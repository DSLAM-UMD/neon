/* contrib/remotexact/remotexact.c */
#include "postgres.h"

#include "access/xact.h"
#include "access/remotexact.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "log.h"
#include "replication/logicalproto.h"
#include "rwset.h"
#include "storage/latch.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapshot.h"
#include "utils/wait_event.h"
#include "miscadmin.h"

#define SingleRegion(region) (UINT64CONST(1) << region)

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUCs */
char	   *remotexact_connstring;

typedef struct CollectedRelationKey
{
	Oid			relid;
} CollectedRelationKey;

typedef struct CollectedRelation
{
	/* Key for the collected_relations table */
	CollectedRelationKey key;

	/* Region this relation belong to */
	int8		region;

	/* Is this relation an index? */
	bool		is_index;

	/* If this relation is a table, did we do a table scan? */
	bool		is_table_scan;

	/* Number of pages if is_index is true, number of tuples otherwise */
	int			nitems;

	/* List of pages read if this is an index, empty if this is a table */
	StringInfoData pages;

	/* List of tuples read if this is an index, empty if this is a table */
	StringInfoData tuples;
} CollectedRelation;

typedef struct RWSetCollectionBuffer
{
	MemoryContext context;

	RWSetHeader header;
	HTAB	   *collected_relations;
	StringInfoData writes;
} RWSetCollectionBuffer;

typedef enum {
	REMOTE_RELKIND_INVALID,
	REMOTE_RELKIND_TABLE,
	REMOTE_RELKIND_INDEX,
} RemoteRelkind;

static RWSetCollectionBuffer *rwset_collection_buffer = NULL;

PGconn	   *xactserver_conn;
bool		xactserver_connected = false;

/*
 * WaitEventSet containing:
 * - WL_SOCKET_READABLE on pageserver_conn,
 * - WL_LATCH_SET on MyLatch, and
 * - WL_EXIT_ON_PM_DEATH.
 */
WaitEventSet *xactserver_conn_wes = NULL;

static void init_rwset_collection_buffer(Oid dbid);
static void rwset_add_region(int region);

static void rx_collect_relation(int region, Oid dbid, Oid relid, char relkind);
static void rx_collect_page(int region, Oid dbid, Oid relid, BlockNumber blkno, char relkind);
static void rx_collect_tuple(int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber tid, char relkind);
static RemoteRelkind get_remote_relkind(char relkind);
static void rx_collect_insert(Relation relation, HeapTuple newtuple);
static void rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple);
static void rx_collect_delete(Relation relation, HeapTuple oldtuple);
static void rx_execute_remote_xact(void);

static CollectedRelation *get_collected_relation(Oid relid, bool create_if_not_found);
static void xactserver_connect(void);
static void xactserver_disconnect(void);
static void check_for_rollback(StringInfo resp_buf);
static int call_PQgetCopyData(char **buffer);
static void clean_up_xact_callback(XactEvent event, void *arg);

static void
init_rwset_collection_buffer(Oid dbid)
{
	MemoryContext old_context;
	HASHCTL		hash_ctl;

	if (rwset_collection_buffer)
	{
		Oid old_dbid = rwset_collection_buffer->header.dbid;

		if (old_dbid != dbid)
			ereport(ERROR,
					errmsg("[remotexact] Remotexact can access only one database"),
					errdetail("old dbid: %u, new dbid: %u", old_dbid, dbid));
		return;
	}

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	rwset_collection_buffer = (RWSetCollectionBuffer *) palloc(sizeof(RWSetCollectionBuffer));
	rwset_collection_buffer->context = TopTransactionContext;

	/* Initialize the header */
	rwset_collection_buffer->header.dbid = dbid;
	/* The current region is always a participant of the transaction */
	rwset_collection_buffer->header.region_set = SingleRegion(current_region);

	/* Initialize a map from relation oid to the read set of the relation */
	hash_ctl.hcxt = rwset_collection_buffer->context;
	hash_ctl.keysize = sizeof(CollectedRelationKey);
	hash_ctl.entrysize = sizeof(CollectedRelation);
	rwset_collection_buffer->collected_relations = hash_create("collected relations",
															   max_predicate_locks_per_xact,
															   &hash_ctl,
															   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Initialize the buffer for the write set */
	initStringInfo(&rwset_collection_buffer->writes);

	MemoryContextSwitchTo(old_context);
}

static void
rwset_add_region(int region)
{
	// Do not collect rwset from main region.
	if (region == GLOBAL_REGION)
		return;
	Assert(RegionIsValid(region));
	Assert(rwset_collection_buffer != NULL);

	/* Mark the proc as executing a remotexact in the procarray before adding
	 * the first remote region to the rwset.
	 * We do this by setting isRemoteXact to ture. Since we are writing to
	 * our own process, we don't need to lock the ProcArray. Also since its
	 * a volatile variable, we know that any subsequent reader will see the
	 * value as true. Any process running a remotexact will idenitfy this
	 * process as executing a remotexact while checking for deadlocks, thus
	 * detecting a potential deadlock. 
	 */
	if (region != current_region &&
		rwset_collection_buffer->header.region_set ==
			SingleRegion(current_region)) {
		MyProc->isRemoteXact = true;
		pg_write_barrier();
	}
    
	/* Set the corresponding region bit in the header */
	rwset_collection_buffer->header.region_set |= SingleRegion(region);
}

static void
rx_collect_relation(int region, Oid dbid, Oid relid, char relkind)
{
	CollectedRelation *relation;
	RemoteRelkind rrelkind;

	// Do not collect rwset from main region.
	if (region == GLOBAL_REGION)
		return;
	rrelkind = get_remote_relkind(relkind);

	Assert(rrelkind != REMOTE_RELKIND_INVALID);

	init_rwset_collection_buffer(dbid);

	relation = get_collected_relation(relid, true);
	relation->region = region;
	relation->is_index = rrelkind == REMOTE_RELKIND_INDEX;
	relation->is_table_scan = rrelkind == REMOTE_RELKIND_TABLE;

	rwset_add_region(region);
}

static void
rx_collect_page(int region, Oid dbid, Oid relid, BlockNumber blkno, char relkind)
{
	CollectedRelation *relation;
	StringInfo	buf = NULL;
	RemoteRelkind rrelkind;
	
	// Do not collect rwset from main region.
	if (region == GLOBAL_REGION)
		return;
	rrelkind = get_remote_relkind(relkind);

	Assert(rrelkind != REMOTE_RELKIND_INVALID);

	if (rrelkind != REMOTE_RELKIND_INDEX)
		return;

	init_rwset_collection_buffer(dbid);

	relation = get_collected_relation(relid, true);
	relation->region = region;
	relation->is_index = true;
	relation->nitems++;

	buf = &relation->pages;
	pq_sendint32(buf, blkno);

	rwset_add_region(region);
}

static void
rx_collect_tuple(int region, Oid dbid, Oid relid, BlockNumber blkno, OffsetNumber offset, char relkind)
{
	CollectedRelation *relation;
	StringInfo	buf = NULL;

	// Do not collect rwset from main region.
	if (region == GLOBAL_REGION)
		return;
	Assert(get_remote_relkind(relkind) == REMOTE_RELKIND_TABLE);

	init_rwset_collection_buffer(dbid);

	relation = get_collected_relation(relid, true);
	relation->region = region;
	relation->is_index = false;
	relation->nitems++;

	buf = &relation->tuples;
	pq_sendint32(buf, blkno);
	pq_sendint16(buf, offset);

	rwset_add_region(region);
}

static RemoteRelkind
get_remote_relkind(char relkind)
{
	switch (relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
			return REMOTE_RELKIND_TABLE; 
		case RELKIND_INDEX:
			return REMOTE_RELKIND_INDEX;
		case RELKIND_VIEW:
		case RELKIND_MATVIEW:
		case RELKIND_COMPOSITE_TYPE:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_PARTITIONED_TABLE:
		case RELKIND_PARTITIONED_INDEX:
			return REMOTE_RELKIND_INVALID;
	}
	return REMOTE_RELKIND_INVALID;
}

static void
rx_collect_insert(Relation relation, HeapTuple newtuple)
{
	StringInfo	buf = NULL;
	int region = RelationGetRegion(relation);
	#if PG_VERSION_NUM >= 150000
		TupleTableSlot *newslot;
	#endif

	if (region == GLOBAL_REGION && current_region != GLOBAL_REGION)
	{	
		remotexact_log(ERROR, "attempting to write to a read-only region");
		return;
	}

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	buf = &rwset_collection_buffer->writes;

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);

#if PG_VERSION_NUM >= 150000
	// TODO(ctring): logicalrep_write_insert in postgres 15 requires a tuple slot,
	//				  so creating it here to make things compile for now. This
	//				  might not be efficient and should be revised.
	newslot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(newtuple, newslot, false);
	/* Encode the insert using the logical replication protocol */
	logicalrep_write_insert(buf,
							InvalidTransactionId,
							relation,
							newslot,
							true /* binary */,
							NULL /* columns */);
	ExecDropSingleTupleTableSlot(newslot);
#else
	/* Encode the insert using the logical replication protocol */
	logicalrep_write_insert(buf, InvalidTransactionId, relation, newtuple, true /* binary */);
#endif

	rwset_add_region(region);
}

static void
rx_collect_update(Relation relation, HeapTuple oldtuple, HeapTuple newtuple)
{
	StringInfo	buf = NULL;
#if PG_VERSION_NUM >= 150000
	TupleTableSlot *oldslot;
	TupleTableSlot *newslot;
#endif

	char		relreplident = relation->rd_rel->relreplident;
	int			region = RelationGetRegion(relation);

	if (region == GLOBAL_REGION && current_region != GLOBAL_REGION)
	{
		remotexact_log(ERROR, "attempting to update to a read-only region");
		return;
	}

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("[remotexact] target relation \"%s\" has neither REPLICA IDENTITY "
						"index nor REPLICA IDENTITY FULL",
						RelationGetRelationName(relation))));
		return;
	}

	buf = &rwset_collection_buffer->writes;

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);

#if PG_VERSION_NUM >= 150000
	// TODO(ctring): logicalrep_write_update in postgres 15 requires tuple slots,
	//				  so creating them here to make things compile for now. This
	//				  might not be efficient and should be revised.
	oldslot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(oldtuple, oldslot, false);
	newslot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(newtuple, newslot, false);
	/* Encode the update using the logical replication protocol */
	logicalrep_write_update(buf,
							InvalidTransactionId,
							relation,
							oldslot,
							newslot,
							true /* binary */,
							NULL /* columns */);
	ExecDropSingleTupleTableSlot(oldslot);
	ExecDropSingleTupleTableSlot(newslot);
#else
	/* Encode the update using the logical replication protocol */
	logicalrep_write_update(buf, InvalidTransactionId, relation, oldtuple, newtuple, true /* binary */);
#endif

	rwset_add_region(region);
}

static void
rx_collect_delete(Relation relation, HeapTuple oldtuple)
{
	StringInfo	buf = NULL;
#if PG_VERSION_NUM >= 150000
	TupleTableSlot *oldslot;
#endif

	char		relreplident = relation->rd_rel->relreplident;
	int			region = RelationGetRegion(relation);

	if (region == GLOBAL_REGION && current_region != GLOBAL_REGION)
	{
		remotexact_log(ERROR, "attempting to delete from a read-only region");
		return;
	}

	init_rwset_collection_buffer(relation->rd_node.dbNode);

	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("[remotexact] target relation \"%s\" has neither REPLICA IDENTITY "
						"index nor REPLICA IDENTITY FULL",
						RelationGetRelationName(relation))));
		return;
	}

	if (oldtuple == NULL)
		return;

	buf = &rwset_collection_buffer->writes;

	/* Starts with the region of the relation */
	pq_sendbyte(buf, region);

#if PG_VERSION_NUM >= 150000
	// TODO(ctring): logicalrep_write_delete in postgres 15 requires a tuple slot,
	//				  so creating it here to make things compile for now. This
	//				  might not be efficient and should be revised.
	oldslot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(oldtuple, oldslot, false);
	/* Encode the delete using the logical replication protocol */
	logicalrep_write_delete(buf, InvalidTransactionId, relation, oldslot, true /* binary */, NULL /* columns */);
	ExecDropSingleTupleTableSlot(oldslot);
#else
	/* Encode the delete using the logical replication protocol */
	logicalrep_write_delete(buf, InvalidTransactionId, relation, oldtuple, true /* binary */);
#endif

	rwset_add_region(region);
}

static void
rx_execute_remote_xact(void)
{
	RWSetHeader			*header;
	CollectedRelation	*relation;
	HASH_SEQ_STATUS		status;
	StringInfoData		buf;
	int		read_len = 0;
	int 	num_read_rels = 0;

	if (rwset_collection_buffer == NULL)
		return;

	header = &rwset_collection_buffer->header;

	/* No need to start a remote xact for a local single-region xact */
	if (header->region_set == SingleRegion(current_region))
		return;

	/* Connect to xact server if not done already */
	if (!xactserver_connected)
		xactserver_connect();

	initStringInfo(&buf);

	/* Assemble the header */
	pq_sendint32(&buf, header->dbid);
	pq_sendint64(&buf, header->region_set);

	/* Cursor now points to where the length of the read section is stored */
	buf.cursor = buf.len;
	/* Read section length will be updated later */
	pq_sendint32(&buf, 0);
	/* Number of read relations will be updated later */
	pq_sendint32(&buf, 0);

	/* Assemble the read set */
	hash_seq_init(&status, rwset_collection_buffer->collected_relations);
	while ((relation = (CollectedRelation *) hash_seq_search(&status)) != NULL)
	{
		Oid			relid = relation->key.relid;
		int8		region = relation->region;
		SnapshotCSN	csn = InvalidXLogRecPtr;
		bool		is_table_scan = relation->is_table_scan;
		int			nitems = relation->nitems;
		StringInfo	items = NULL;
		
		if (RegionIsRemote(region))
			csn = GetRegionLsn(region);

		/* Accumulate the length of the buffer used by each relation */
		read_len -= buf.len;

		if (relation->is_index)
		{
			pq_sendbyte(&buf, 'I');
			items = &relation->pages;
		}
		else
		{
			pq_sendbyte(&buf, 'T');
			items = &relation->tuples;
		}
		pq_sendint32(&buf, relid);
		pq_sendbyte(&buf, region);
		pq_sendint64(&buf, csn);
		pq_sendbyte(&buf, is_table_scan);
		/* Skip sending the tuples if a relation is a table scan. */
		if (!is_table_scan) {
			pq_sendint32(&buf, nitems);
			pq_sendbytes(&buf, items->data, items->len);
		} else {
			pq_sendint32(&buf, 0);
		}
		read_len += buf.len;
		num_read_rels++;
	}

	/* Update the length of the read section */
	*(int *) (buf.data + buf.cursor) = pg_hton32(read_len);
	/* Update the number of the read relations sent */
	*(int *)(buf.data + buf.cursor + sizeof(int)) = pg_hton32(num_read_rels);

	/* Send the buffer to the xact server */
	pq_sendbytes(&buf, rwset_collection_buffer->writes.data, rwset_collection_buffer->writes.len);
	if (PQputCopyData(xactserver_conn, buf.data, buf.len) <= 0 || PQflush(xactserver_conn))
	{
		char *msg = pchomp(PQerrorMessage(xactserver_conn));

		xactserver_disconnect();
		remotexact_log(ERROR, "failed to send read/write set: %s", msg);
	}

	/* Read the response */
	PG_TRY();
	{
		StringInfoData resp_buf;
		int	rc;

		rc = call_PQgetCopyData(&resp_buf.data);
		if (rc >= 0)
		{
			resp_buf.len = rc;
			resp_buf.cursor = 0;
	
			check_for_rollback(&resp_buf);

			PQfreemem(resp_buf.data);
		}
		else if (rc == -1)
			remotexact_log(ERROR, "connection closed by xactserver");
		else if (rc == -2)
			remotexact_log(ERROR, "could not read COPY data: %s", PQerrorMessage(xactserver_conn));
		else
			remotexact_log(ERROR, "unexpected PGgetCopyData return value: %d", rc);
	}
	PG_CATCH();
	{
		xactserver_disconnect();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
check_for_rollback(StringInfo resp_buf)
{
	int rollbacked_by;
	const char *msg;
	bool is_sql_error;

	rollbacked_by = pq_getmsgbyte(resp_buf) - 1;
	/* The first byte is either (region_id + 1), if the transaction was rolled back or 0 otherwise */
	if (rollbacked_by < 0)
		return;

	msg = pq_getmsgstring(resp_buf);
	is_sql_error = pq_getmsgbyte(resp_buf);

	if (is_sql_error)
	{
		char code[5];
		const char *severity;
		const char *detail;
		const char *hint;
		StringInfoData s;

		memcpy(code, pq_getmsgbytes(resp_buf, 5), 5);
		severity = pq_getmsgstring(resp_buf);
		detail = pq_getmsgstring(resp_buf);
		hint = pq_getmsgstring(resp_buf);

		initStringInfo(&s);
		appendStringInfo(&s, "%s: %s.", severity, msg);
		if (detail[0] != '\0')
			appendStringInfo(&s, "DETAIL:  %s", detail);
		if (hint[0] != '\0')
			appendStringInfo(&s, "HINT:  %s", hint);

		PQfreemem(resp_buf->data);
		ereport(ERROR,
				(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
				 errmsg("[remotexact] validation failed during commit"),
				 errdetail("Region %d: \"%s\"", rollbacked_by, s.data)));
	}
	else
	{
		PQfreemem(resp_buf->data);
		ereport(ERROR,
				(errcode(ERRCODE_TRANSACTION_ROLLBACK),
				 errmsg("[remotexact] an error occured during commit"),
				 errdetail("Region %d: \"%s\"", rollbacked_by, msg)));
	}
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 * 
 * Taken from neon/libpagestore.c
 */
static int
call_PQgetCopyData(char **buffer)
{
	int			ret;

retry:
	ret = PQgetCopyData(xactserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(xactserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(xactserver_conn))
				ereport(ERROR, errmsg("could not get response from xactserver: %s",
									  PQerrorMessage(xactserver_conn)));
		}

		goto retry;
	}

	return ret;
}

static CollectedRelation *
get_collected_relation(Oid relid, bool create_if_not_found)
{
	CollectedRelationKey key;
	CollectedRelation *relation;
	bool		found;

	Assert(rwset_collection_buffer);

	key.relid = relid;

	/* Check if the relation is in the map */
	relation = (CollectedRelation *) hash_search(rwset_collection_buffer->collected_relations,
												 &key, HASH_ENTER, &found);
	/* Initialize a new relation entry if not found */
	if (!found) {
		if (create_if_not_found)
		{
			MemoryContext old_context;

			old_context = MemoryContextSwitchTo(rwset_collection_buffer->context);

			relation->nitems = 0;
			relation->region = UNKNOWN_REGION;
			relation->is_index = false;
			relation->is_table_scan = false;
			initStringInfo(&relation->pages);
			initStringInfo(&relation->tuples);

			MemoryContextSwitchTo(old_context);
		}
		else
			relation = NULL;
	}
	return relation;
}

static void
xactserver_connect(void)
{
	int ret;

	Assert(!xactserver_connected);

	xactserver_conn = PQconnectdb(remotexact_connstring);
	if (PQstatus(xactserver_conn) == CONNECTION_BAD)
	{
		char	*msg = pchomp(PQerrorMessage(xactserver_conn));

		PQfinish(xactserver_conn);
		xactserver_conn = NULL;
		
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("[remotexact] could not establish connection to xactserver"),
				 errdetail_internal("%s", msg)));
	}

	/* TODO(ctring): send a more useful starting message */
	ret = PQsendQuery(xactserver_conn, "start");
	if (ret != 1)
	{
		PQfinish(xactserver_conn);
		xactserver_conn = NULL;
		remotexact_log(ERROR, "could not send start command to xactserver");
	}

	xactserver_conn_wes = CreateWaitEventSet(TopMemoryContext, 3);
	AddWaitEventToSet(xactserver_conn_wes, WL_LATCH_SET, PGINVALID_SOCKET,
			  MyLatch, NULL);
	AddWaitEventToSet(xactserver_conn_wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
			  NULL, NULL);
	AddWaitEventToSet(xactserver_conn_wes, WL_SOCKET_READABLE, PQsocket(xactserver_conn), NULL, NULL);

	while (PQisBusy(xactserver_conn))
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(xactserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(xactserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(xactserver_conn));

				PQfinish(xactserver_conn);
				xactserver_conn = NULL;
				FreeWaitEventSet(xactserver_conn_wes);
				xactserver_conn_wes = NULL;

				remotexact_log(ERROR, "could not complete handshake with pageserver: %s", msg);
			}
		}
	}

	remotexact_log(LOG, "connected to '%s'", remotexact_connstring);

	xactserver_connected = true;
}

static void
xactserver_disconnect(void)
{
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 */
	if (xactserver_connected)
	{
		remotexact_log(LOG, "dropping connection to xact server due to error");
		PQfinish(xactserver_conn);
		xactserver_conn = NULL;
		xactserver_connected = false;
	}
	if (xactserver_conn_wes != NULL)
	{
		FreeWaitEventSet(xactserver_conn_wes);
		xactserver_conn_wes = NULL;
	}
}

static void
clean_up_xact_callback(XactEvent event, void *arg)
{
	/* 
	 * Unset the PROC_IS_REMOTEXACT statusFlag for MyProc once the remotexact
	 * completes its execution. We don't need to lock the ProcArray because we
	 * are writing to out own process.
	 */
	MyProc->isRemoteXact = false;
	pg_write_barrier();

	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			rwset_collection_buffer = NULL;
			break;
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			break;
	}
}

static const RemoteXactHook remote_xact_hook =
{
	.collect_tuple = rx_collect_tuple,
	.collect_relation = rx_collect_relation,
	.collect_page = rx_collect_page,
	.collect_insert = rx_collect_insert,
	.collect_update = rx_collect_update,
	.collect_delete = rx_collect_delete,
	.execute_remote_xact = rx_execute_remote_xact
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("remotexact.connstring",
							   "connection string to the transaction server",
							   NULL,
							   &remotexact_connstring,
							   "postgresql://127.0.0.1:10000",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	if (remotexact_connstring && remotexact_connstring[0])
	{
		SetRemoteXactHook(&remote_xact_hook);
		RegisterXactCallback(clean_up_xact_callback, NULL);

		remotexact_log(LOG, "xactserver connection string \"%s\"", remotexact_connstring);
	}
}
