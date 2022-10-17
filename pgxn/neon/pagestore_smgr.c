/*-------------------------------------------------------------------------
 *
 * pagestore_smgr.c
 *
 *
 *
 * Temporary and unlogged rels
 * ---------------------------
 *
 * Temporary and unlogged tables are stored locally, by md.c. The functions
 * here just pass the calls through to corresponding md.c functions.
 *
 * Index build operations that use the buffer cache are also handled locally,
 * just like unlogged tables. Such operations must be marked by calling
 * smgr_start_unlogged_build() and friends.
 *
 * In order to know what relations are permanent and which ones are not, we
 * have added a 'smgr_relpersistence' field to SmgrRelationData, and it is set
 * by smgropen() callers, when they have the relcache entry at hand.  However,
 * sometimes we need to open an SmgrRelation for a relation without the
 * relcache. That is needed when we evict a buffer; we might not have the
 * SmgrRelation for that relation open yet. To deal with that, the
 * 'relpersistence' can be left to zero, meaning we don't know if it's
 * permanent or not. Most operations are not allowed with relpersistence==0,
 * but smgrwrite() does work, which is what we need for buffer eviction.  and
 * smgrunlink() so that a backend doesn't need to have the relcache entry at
 * transaction commit, where relations that were dropped in the transaction
 * are unlinked.
 *
 * If smgrwrite() is called and smgr_relpersistence == 0, we check if the
 * relation file exists locally or not. If it does exist, we assume it's an
 * unlogged relation and write the page there. Otherwise it must be a
 * permanent relation, WAL-logged and stored on the page server, and we ignore
 * the write like we do for permanent relations.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/neon/pagestore_smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/remotexact.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "catalog/pg_class.h"
#include "pagestore_client.h"
#include "storage/smgr.h"
#include "access/xlogdefs.h"
#include "postmaster/interrupt.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/relfilenode.h"
#include "storage/buf_internals.h"
#include "storage/md.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "multiregion.h"
#include "pgstat.h"
#include "catalog/pg_tablespace_d.h"
#include "postmaster/autovacuum.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogutils.h"
#include "access/xlogrecovery.h"
#endif

/*
 * If DEBUG_COMPARE_LOCAL is defined, we pass through all the SMGR API
 * calls to md.c, and *also* do the calls to the Page Server. On every
 * read, compare the versions we read from local disk and Page Server,
 * and Assert that they are identical.
 */
/* #define DEBUG_COMPARE_LOCAL */

#ifdef DEBUG_COMPARE_LOCAL
#include "access/nbtree.h"
#include "storage/bufpage.h"
#include "access/xlog_internal.h"

static char *hexdump_page(char *page);
#endif

#define IS_LOCAL_REL(reln) (reln->smgr_rnode.node.dbNode != 0 && reln->smgr_rnode.node.relNode > FirstNormalObjectId)

const int	SmgrTrace = DEBUG5;

page_server_api *page_server;

/* GUCs */
char	   *page_server_connstring;

/*with substituted password*/
char	   *neon_timeline;
char	   *neon_tenant;
bool		wal_redo = false;
int32		max_cluster_size;
bool		neon_slru_clog;
bool		neon_slru_multixact;
bool		neon_slru_csnlog;

/* unlogged relation build states */
typedef enum
{
	UNLOGGED_BUILD_NOT_IN_PROGRESS = 0,
	UNLOGGED_BUILD_PHASE_1,
	UNLOGGED_BUILD_PHASE_2,
	UNLOGGED_BUILD_NOT_PERMANENT
}			UnloggedBuildPhase;

static SMgrRelation unlogged_build_rel = NULL;
static UnloggedBuildPhase unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;

static void neon_read_at_lsn_multi_region(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
										  int region, XLogRecPtr request_lsn, bool request_latest, char *buffer);

/*
 * Prefetch implementation:
 * Prefetch is performed locally by each backend.
 * There can be up to MAX_PREFETCH_REQUESTS registered using smgr_prefetch
 * before smgr_read. All this requests are appended to primary smgr_read request.
 * It is assumed that pages will be requested in prefetch order.
 * Reading of prefetch responses is delayed until them are actually needed (smgr_read).
 * It make it possible to parallelize processing and receiving of prefetched pages.
 * In case of prefetch miss or any other SMGR request other than smgr_read,
 * all prefetch responses has to be consumed.
 */

#define MAX_PREFETCH_REQUESTS 128

BufferTag	prefetch_requests[MAX_PREFETCH_REQUESTS];
BufferTag	prefetch_responses[MAX_PREFETCH_REQUESTS];
int			n_prefetch_requests;
int			n_prefetch_responses;
int			n_prefetched_buffers;
int			n_prefetch_hits;
int			n_prefetch_misses;
XLogRecPtr	prefetch_lsn;

static void
consume_prefetch_responses(void)
{
	for (int i = n_prefetched_buffers; i < n_prefetch_responses; i++)
	{
		NeonResponse *resp = page_server->receive();

		pfree(resp);
	}
	n_prefetched_buffers = 0;
	n_prefetch_responses = 0;
}

static NeonResponse *
page_server_request(void const *req)
{
	consume_prefetch_responses();
	return page_server->request((NeonRequest *) req);
}

StringInfoData
nm_pack_request(NeonRequest * msg)
{
	StringInfoData s;

	initStringInfo(&s);
	pq_sendbyte(&s, msg->tag);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint8(&s, msg_req->req.region);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint8(&s, msg_req->req.region);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, msg_req->dbNode);

				break;
			}
		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint8(&s, msg_req->req.region);
				pq_sendint32(&s, msg_req->rnode.spcNode);
				pq_sendint32(&s, msg_req->rnode.dbNode);
				pq_sendint32(&s, msg_req->rnode.relNode);
				pq_sendbyte(&s, msg_req->forknum);
				pq_sendint32(&s, msg_req->blkno);

				break;
			}
		case T_NeonGetSlruPageRequest:
			{
				NeonGetSlruPageRequest *msg_req = (NeonGetSlruPageRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint8(&s, msg_req->req.region);
				pq_sendbyte(&s, msg_req->kind);
				pq_sendint32(&s, msg_req->segno);
				pq_sendint32(&s, msg_req->blkno);
				pq_sendbyte(&s, msg_req->check_exists_only);

				break;
			}

			/* pagestore -> pagestore_client. We never need to create these. */
		case T_NeonExistsResponse:
		case T_NeonNblocksResponse:
		case T_NeonGetPageResponse:
		case T_NeonGetSlruPageResponse:
		case T_NeonErrorResponse:
		case T_NeonDbSizeResponse:
		default:
			elog(ERROR, "unexpected neon message tag 0x%02x", msg->tag);
			break;
	}
	return s;
}

NeonResponse *
nm_unpack_response(StringInfo s)
{
	NeonMessageTag tag = pq_getmsgbyte(s);
	NeonResponse *resp = NULL;

	switch (tag)
	{
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = palloc0(sizeof(NeonExistsResponse));

				msg_resp->tag = tag;
				msg_resp->lsn = pq_getmsgint64(s);
				msg_resp->exists = pq_getmsgbyte(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = palloc0(sizeof(NeonNblocksResponse));

				msg_resp->tag = tag;
				msg_resp->lsn = pq_getmsgint64(s);
				msg_resp->n_blocks = pq_getmsgint(s, 4);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetPageResponse:
			{
				NeonGetPageResponse *msg_resp = palloc0(offsetof(NeonGetPageResponse, page) + BLCKSZ);

				msg_resp->tag = tag;
				msg_resp->lsn = pq_getmsgint64(s);
				/* XXX:	should be varlena */
				memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = palloc0(sizeof(NeonDbSizeResponse));

				msg_resp->tag = tag;
				msg_resp->lsn = pq_getmsgint64(s);
				msg_resp->db_size = pq_getmsgint64(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetSlruPageResponse:
			{
				NeonGetSlruPageResponse *msg_resp = palloc0(offsetof(NeonGetSlruPageResponse, page) + BLCKSZ);

				msg_resp->tag = tag;
				msg_resp->lsn = pq_getmsgint64(s);
				msg_resp->seg_exists = pq_getmsgbyte(s);
				msg_resp->page_exists = pq_getmsgbyte(s);
				if (msg_resp->page_exists)
				{
					/* XXX:	should be varlena */
					memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				}
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp;
				size_t		msglen;
				const char *msgtext;

				msgtext = pq_getmsgrawstring(s);
				msglen = strlen(msgtext);

				msg_resp = palloc0(sizeof(NeonErrorResponse) + msglen + 1);
				msg_resp->tag = tag;
				memcpy(msg_resp->message, msgtext, msglen + 1);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

			/*
			 * pagestore_client -> pagestore
			 *
			 * We create these ourselves, and don't need to decode them.
			 */
		case T_NeonExistsRequest:
		case T_NeonNblocksRequest:
		case T_NeonGetPageRequest:
		case T_NeonDbSizeRequest:
		default:
			elog(ERROR, "unexpected neon message tag 0x%02x", tag);
			break;
	}

	return resp;
}

/* dump to json for debugging / error reporting purposes */
char *
nm_to_string(NeonMessage * msg)
{
	StringInfoData s;

	initStringInfo(&s);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"region\": %d", msg_req->req.region);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"region\": %d", msg_req->req.region);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageRequest\"");
				appendStringInfo(&s, ", \"rnode\": \"%u/%u/%u\"",
								 msg_req->rnode.spcNode,
								 msg_req->rnode.dbNode,
								 msg_req->rnode.relNode);
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"blkno\": %u", msg_req->blkno);
				appendStringInfo(&s, ", \"region\": %d", msg_req->req.region);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeRequest\"");
				appendStringInfo(&s, ", \"dbnode\": \"%u\"", msg_req->dbNode);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonGetSlruPageRequest:
			{
				NeonGetSlruPageRequest *msg_req = (NeonGetSlruPageRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruPageRequest\"");
				appendStringInfo(&s, ", \"kind\": %d", msg_req->kind);
				appendStringInfo(&s, ", \"segno\": %d", msg_req->segno);
				appendStringInfo(&s, ", \"blkno\": %u", msg_req->blkno);
				appendStringInfo(&s, ", \"check_exists_only\": %d", msg_req->check_exists_only);
				appendStringInfo(&s, ", \"region\": %d", msg_req->req.region);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = (NeonExistsResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsResponse\"");
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_resp->lsn));
				appendStringInfo(&s, ", \"exists\": %d}",
								 msg_resp->exists);
				appendStringInfoChar(&s, '}');

				break;
			}

		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = (NeonNblocksResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksResponse\"");
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_resp->lsn));
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonGetPageResponse:
			{
				NeonGetPageResponse *msg_resp = (NeonGetPageResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageResponse\"");
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_resp->lsn));
				appendStringInfo(&s, ", \"page\": \"XXX\"}");
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonGetSlruPageResponse:
			{
				NeonGetSlruPageResponse *msg_resp = (NeonGetSlruPageResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruPageResponse\"");
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_resp->lsn));
				appendStringInfo(&s, ", \"seg_exists\": %d", msg_resp->seg_exists);
				appendStringInfo(&s, ", \"page_exists\": %d", msg_resp->page_exists);
				appendStringInfo(&s, ", \"page\": \"XXX\"}");
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp = (NeonErrorResponse *) msg;

				/* FIXME: escape double-quotes in the message */
				appendStringInfoString(&s, "{\"type\": \"NeonErrorResponse\"");
				appendStringInfo(&s, ", \"message\": \"%s\"}", msg_resp->message);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = (NeonDbSizeResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeResponse\"");
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_resp->lsn));
				appendStringInfo(&s, ", \"db_size\": %ld}",
								 msg_resp->db_size);
				appendStringInfoChar(&s, '}');

				break;
			}

		default:
			appendStringInfo(&s, "{\"type\": \"unknown 0x%02x\"", msg->tag);
	}
	return s.data;
}

/*
 * Wrapper around log_newpage() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpage_copy(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blkno,
				 Page page, bool page_std)
{
	PGAlignedBlock copied_buffer;

	memcpy(copied_buffer.data, page, BLCKSZ);
	return log_newpage(rnode, forkNum, blkno, copied_buffer.data, page_std);
}

/*
 * Is 'buffer' identical to a freshly initialized empty heap page?
 */
static bool
PageIsEmptyHeapPage(char *buffer)
{
	PGAlignedBlock empty_page;

	PageInit((Page) empty_page.data, BLCKSZ, 0);

	return memcmp(buffer, empty_page.data, BLCKSZ) == 0;
}

static void
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
	XLogRecPtr	lsn = PageGetLSN(buffer);

	if (ShutdownRequestPending)
		return;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	if (forknum == FSM_FORKNUM && !RecoveryInProgress())
	{
		/* FSM is never WAL-logged and we don't care. */
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&reln->smgr_rnode.node, forknum, blocknum, buffer, false);
		XLogFlush(recptr);
		lsn = recptr;
		ereport(SmgrTrace,
				(errmsg("FSM page %u of relation %u/%u/%u.%u was force logged. Evicted at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}
	else if (forknum == VISIBILITYMAP_FORKNUM && !RecoveryInProgress())
	{
		/*
		 * Always WAL-log vm. We should never miss clearing visibility map
		 * bits.
		 *
		 * TODO Is it too bad for performance? Hopefully we do not evict
		 * actively used vm too often.
		 */
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&reln->smgr_rnode.node, forknum, blocknum, buffer, false);
		XLogFlush(recptr);
		lsn = recptr;

		ereport(SmgrTrace,
				(errmsg("Visibilitymap page %u of relation %u/%u/%u.%u was force logged at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}
	else if (lsn == InvalidXLogRecPtr)
	{
		/*
		 * When PostgreSQL extends a relation, it calls smgrextend() with an
		 * all-zeros pages, and we can just ignore that in Neon. We do need to
		 * remember the new size, though, so that smgrnblocks() returns the
		 * right answer after the rel has been extended. We rely on the
		 * relsize cache for that.
		 *
		 * A completely empty heap page doesn't need to be WAL-logged, either.
		 * The heapam can leave such a page behind, if e.g. an insert errors
		 * out after initializing the page, but before it has inserted the
		 * tuple and WAL-logged the change. When we read the page from the
		 * page server, it will come back as all-zeros. That's OK, the heapam
		 * will initialize an all-zeros page on first use.
		 *
		 * In other scenarios, evicting a dirty page with no LSN is a bad
		 * sign: it implies that the page was not WAL-logged, and its contents
		 * will be lost when it's evicted.
		 */
		if (PageIsNew(buffer))
		{
			ereport(SmgrTrace,
					(errmsg("Page %u of relation %u/%u/%u.%u is all-zeros",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
		else if (PageIsEmptyHeapPage(buffer))
		{
			ereport(SmgrTrace,
					(errmsg("Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
		else
		{
			ereport(PANIC,
					(errmsg("Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
							blocknum,
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum)));
		}
	}
	else
	{
		ereport(SmgrTrace,
				(errmsg("Page %u of relation %u/%u/%u.%u is already wal logged at lsn=%X/%X",
						blocknum,
						reln->smgr_rnode.node.spcNode,
						reln->smgr_rnode.node.dbNode,
						reln->smgr_rnode.node.relNode,
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	/*
	 * Remember the LSN on this page. When we read the page again, we must
	 * read the same or newer version of it.
	 */
	SetLastWrittenLSNForBlock(lsn, reln->smgr_rnode.node, forknum, blocknum);
}

/*
 *	neon_init() -- Initialize private state
 */
void
neon_init(void)
{
	/* noop */
#ifdef DEBUG_COMPARE_LOCAL
	mdinit();
#endif
}

/*
 * GetXLogInsertRecPtr uses XLogBytePosToRecPtr to convert logical insert (reserved) position
 * to physical position in WAL. It always adds SizeOfXLogShortPHD:
 *		seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
 * so even if there are no records on the page, offset will be SizeOfXLogShortPHD.
 * It may cause problems with XLogFlush. So return pointer backward to the origin of the page.
 */
static XLogRecPtr
nm_adjust_lsn(XLogRecPtr lsn)
{
	/*
	 * If lsn points to the beging of first record on page or segment, then
	 * "return" it back to the page origin
	 */
	if ((lsn & (XLOG_BLCKSZ - 1)) == SizeOfXLogShortPHD)
	{
		lsn -= SizeOfXLogShortPHD;
	}
	else if ((lsn & (wal_segment_size - 1)) == SizeOfXLogLongPHD)
	{
		lsn -= SizeOfXLogLongPHD;
	}
	return lsn;
}

/*
 * Return LSN for requesting pages and number of blocks from page server
 */
static XLogRecPtr
neon_get_request_lsn(bool *latest, int region, RelFileNode rnode, ForkNumber forknum, BlockNumber blkno)
{
	XLogRecPtr	lsn;

	if (RecoveryInProgress())
	{
		*latest = false;
		lsn = GetXLogReplayRecPtr(NULL);
		elog(DEBUG1, "neon_get_request_lsn GetXLogReplayRecPtr %X/%X request lsn 0 ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));
	}
	else if (am_walsender)
	{
		*latest = true;
		lsn = InvalidXLogRecPtr;
		elog(DEBUG1, "am walsender neon_get_request_lsn lsn 0 ");
	}

	// Since only keep track of the latest written LSN of the current region, we need to 
	// rely on zenith to find the latest LSN. Hence, it is insufficient to use RegionIsRemote(region)
	// because the global region is also separate from the current region.
	else if (IsMultiRegion() && region != current_region)
	{
		*latest = false;
		lsn = get_region_lsn(region);
		elog(LOG, "get lsn %X/%X for region %d", LSN_FORMAT_ARGS(lsn), region);
		if (lsn == InvalidXLogRecPtr)
			*latest = true;
	}
	else
	{
		XLogRecPtr	flushlsn;

		/*
		 * Use the latest LSN that was evicted from the buffer cache. Any
		 * pages modified by later WAL records must still in the buffer cache,
		 * so our request cannot concern those.
		 */
		*latest = true;
		lsn = GetLastWrittenLSN(rnode, forknum, blkno);
		Assert(lsn != InvalidXLogRecPtr);
		elog(DEBUG1, "neon_get_request_lsn GetLastWrittenLSN lsn %X/%X ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));

		lsn = nm_adjust_lsn(lsn);

		/*
		 * Is it possible that the last-written LSN is ahead of last flush
		 * LSN? Generally not, we shouldn't evict a page from the buffer cache
		 * before all its modifications have been safely flushed. That's the
		 * "WAL before data" rule. However, such case does exist at index
		 * building, _bt_blwritepage logs the full page without flushing WAL
		 * before smgrextend (files are fsynced before build ends).
		 */
#if PG_VERSION_NUM >= 150000
		flushlsn = GetFlushRecPtr(NULL);
#else
		flushlsn = GetFlushRecPtr();
#endif
		if (lsn > flushlsn)
		{
			elog(DEBUG5, "last-written LSN %X/%X is ahead of last flushed LSN %X/%X",
				 (uint32) (lsn >> 32), (uint32) lsn,
				 (uint32) (flushlsn >> 32), (uint32) flushlsn);
			XLogFlush(lsn);
		}
	}

	return lsn;
}

/*
 *	neon_exists() -- Does the physical file exist?
 */
bool
neon_exists(SMgrRelation reln, ForkNumber forkNum)
{
	bool		exists;
	NeonResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:

			/*
			 * We don't know if it's an unlogged rel stored locally, or
			 * permanent rel stored in the page server. First check if it
			 * exists locally. If it does, great. Otherwise check if it exists
			 * in the page server.
			 */
			if (mdexists(reln, forkNum))
				return true;
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdexists(reln, forkNum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(reln->smgr_rnode.node, forkNum, &n_blocks))
	{
		return true;
	}

	/*
	 * \d+ on a view calls smgrexists with 0/0/0 relfilenode. The page server
	 * will error out if you check that, because the whole dbdir for
	 * tablespace 0, db 0 doesn't exists. We possibly should change the page
	 * server to accept that and return 'false', to be consistent with
	 * mdexists(). But we probably also should fix pg_table_size() to not call
	 * smgrexists() with bogus relfilenode.
	 *
	 * For now, handle that special case here.
	 */
	if (reln->smgr_rnode.node.spcNode == 0 &&
		reln->smgr_rnode.node.dbNode == 0 &&
		reln->smgr_rnode.node.relNode == 0)
	{
		return false;
	}

	request_lsn = neon_get_request_lsn(&latest,
					  				   reln->smgr_region,
									   reln->smgr_rnode.node,
									   forkNum,
									   REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonExistsRequest request = {
			.req.tag = T_NeonExistsRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.req.region = reln->smgr_region,
			.rnode = reln->smgr_rnode.node,
		.forknum = forkNum};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonExistsResponse:
			exists = ((NeonExistsResponse *) resp)->exists;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read relation existence of rel %u/%u/%u.%u in region %d from page server at lsn %X/%08X",
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forkNum,
							reln->smgr_region,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}
	pfree(resp);
	return exists;
}

/*
 *	neon_create() -- Create a new relation on neond storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
neon_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrcreate() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdcreate(reln, forkNum, isRedo);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	elog(SmgrTrace, "Create relation %u/%u/%u.%u",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forkNum);

	/*
	 * Newly created relation is empty, remember that in the relsize cache.
	 *
	 * FIXME: This is currently not just an optimization, but required for
	 * correctness. Postgres can call smgrnblocks() on the newly-created
	 * relation. Currently, we don't call SetLastWrittenLSN() when a new
	 * relation created, so if we didn't remember the size in the relsize
	 * cache, we might call smgrnblocks() on the newly-created relation before
	 * the creation WAL record hass been received by the page server.
	 */
	set_cached_relsize(reln->smgr_rnode.node, forkNum, 0);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdcreate(reln, forkNum, isRedo);
#endif
}

/*
 *	neon_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
neon_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/*
	 * Might or might not exist locally, depending on whether it's an unlogged
	 * or permanent relation (or if DEBUG_COMPARE_LOCAL is set). Try to
	 * unlink, it won't do any harm if the file doesn't exist.
	 */
	mdunlink(rnode, forkNum, isRedo);
	if (!RelFileNodeBackendIsTemp(rnode))
	{
		forget_cached_relsize(rnode.node, forkNum);
	}
}

/*
 *	neon_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
neon_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			char *buffer, bool skipFsync)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdextend(reln, forkNum, blkno, buffer, skipFsync);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * Check that the cluster size limit has not been exceeded.
	 *
	 * Temporary and unlogged relations are not included in the cluster size
	 * measured by the page server, so ignore those. Autovacuum processes are
	 * also exempt.
	 */
	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!IsAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetZenithCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file because cluster size limit (%d MB) has been exceeded",
							max_cluster_size),
					 errhint("This limit is defined by neon.max_cluster_size GUC")));
	}

	neon_wallog_page(reln, forkNum, blkno, buffer);
	set_cached_relsize(reln->smgr_rnode.node, forkNum, blkno + 1);

	lsn = PageGetLSN(buffer);
	elog(SmgrTrace, "smgrextend called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forkNum, blkno,
		 (uint32) (lsn >> 32), (uint32) lsn);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdextend(reln, forkNum, blkno, buffer, skipFsync);
#endif
	/*
	 * smgr_extend is often called with an all-zeroes page, so lsn==InvalidXLogRecPtr.
	 * An smgr_write() call will come for the buffer later, after it has been initialized
	 * with the real page contents, and it is eventually evicted from the buffer cache.
	 * But we need a valid LSN to the relation metadata update now.
	 */
	if (lsn == InvalidXLogRecPtr)
	{
		lsn = GetXLogInsertRecPtr();
		SetLastWrittenLSNForBlock(lsn, reln->smgr_rnode.node, forkNum, blkno);
	}
	SetLastWrittenLSNForRelation(lsn, reln->smgr_rnode.node, forkNum);
}

/*
 *  neon_open() -- Initialize newly-opened relation.
 */
void
neon_open(SMgrRelation reln)
{
	/*
	 * We don't have anything special to do here. Call mdopen() to let md.c
	 * initialize itself. That's only needed for temporary or unlogged
	 * relations, but it's dirt cheap so do it always to make sure the md
	 * fields are initialized, for debugging purposes if nothing else.
	 */
	mdopen(reln);

	/* no work */
	elog(SmgrTrace, "[NEON_SMGR] open noop");
}

/*
 *	neon_close() -- Close the specified relation, if it isn't closed already.
 */
void
neon_close(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * Let md.c close it, if it had it open. Doesn't hurt to do this even for
	 * permanent relations that have no local storage.
	 */
	mdclose(reln, forknum);
}


/*
 *	neon_reset_prefetch() -- reoe all previously rgistered prefeth requests
 */
void
neon_reset_prefetch(SMgrRelation reln)
{
	n_prefetch_requests = 0;
}

/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
neon_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* probably shouldn't happen, but ignore it */
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (n_prefetch_requests < MAX_PREFETCH_REQUESTS)
	{
		prefetch_requests[n_prefetch_requests].rnode = reln->smgr_rnode.node;
		prefetch_requests[n_prefetch_requests].forkNum = forknum;
		prefetch_requests[n_prefetch_requests].blockNum = blocknum;
		n_prefetch_requests += 1;
		return true;
	}
	return false;
}

/*
 * neon_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
neon_writeback(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber blocknum, BlockNumber nblocks)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* mdwriteback() does nothing if the file doesn't exist */
			mdwriteback(reln, forknum, blocknum, nblocks);
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwriteback(reln, forknum, blocknum, nblocks);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/* not implemented */
	elog(SmgrTrace, "[NEON_SMGR] writeback noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwriteback(reln, forknum, blocknum, nblocks);
#endif
}

/*
 * While function is defined in the neon extension it's used within neon_test_utils directly.
 * To avoid breaking tests in the runtime please keep function signature in sync.
 */
void
neon_read_at_lsn(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
				 XLogRecPtr request_lsn, bool request_latest, char *buffer)
{
	neon_read_at_lsn_multi_region(rnode, forkNum, blkno, current_region, request_lsn, request_latest, buffer);
}

static void neon_read_at_lsn_multi_region(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
										  int region, XLogRecPtr request_lsn, bool request_latest, char *buffer)
{
	NeonResponse *resp;
	int i;

	/*
	 * Try to find prefetched page. It is assumed that pages will be requested
	 * in the same order as them are prefetched, but some other backend may
	 * load page in shared buffers, so some prefetch responses should be
	 * skipped.
	 */
	for (i = n_prefetched_buffers; i < n_prefetch_responses; i++)
	{
		resp = page_server->receive();
		if (resp->tag == T_NeonGetPageResponse &&
			RelFileNodeEquals(prefetch_responses[i].rnode, rnode) &&
			prefetch_responses[i].forkNum == forkNum &&
			prefetch_responses[i].blockNum == blkno)
		{
			char	   *page = ((NeonGetPageResponse *) resp)->page;

			/*
			 * Check if prefetched page is still relevant. If it is updated by
			 * some other backend, then it should not be requested from smgr
			 * unless it is evicted from shared buffers. In the last case
			 * last_evicted_lsn should be updated and request_lsn should be
			 * greater than prefetch_lsn. Maximum with page LSN is used
			 * because page returned by page server may have LSN either
			 * greater either smaller than requested.
			 */
			if (Max(prefetch_lsn, PageGetLSN(page)) >= request_lsn)
			{
				n_prefetched_buffers = i + 1;
				n_prefetch_hits += 1;
				n_prefetch_requests = 0;
				memcpy(buffer, page, BLCKSZ);
				pfree(resp);
				return;
			}
		}
		pfree(resp);
	}
	n_prefetched_buffers = 0;
	n_prefetch_responses = 0;
	n_prefetch_misses += 1;
	{
		NeonGetPageRequest request = {
			.req.tag = T_NeonGetPageRequest,
			.req.latest = request_latest,
			.req.lsn = request_lsn,
			.req.region = region,
			.rnode = rnode,
			.forknum = forkNum,
			.blkno = blkno,
		};

		if (n_prefetch_requests > 0)
		{
			/* Combine all prefetch requests with primary request */
			page_server->send((NeonRequest *) & request);
			for (i = 0; i < n_prefetch_requests; i++)
			{
				request.rnode = prefetch_requests[i].rnode;
				request.forknum = prefetch_requests[i].forkNum;
				request.blkno = prefetch_requests[i].blockNum;
				prefetch_responses[i] = prefetch_requests[i];
				page_server->send((NeonRequest *) & request);
			}
			page_server->flush();
			n_prefetch_responses = n_prefetch_requests;
			n_prefetch_requests = 0;
			prefetch_lsn = request_lsn;
			resp = page_server->receive();
		}
		else
		{
			resp = page_server->request((NeonRequest *) & request);
		}
	}
	switch (resp->tag)
	{
		case T_NeonGetPageResponse:
			memcpy(buffer, ((NeonGetPageResponse *) resp)->page, BLCKSZ);
			if (RegionIsRemote(region))
			{
				XLogRecPtr lsn = ((NeonGetPageResponse *) resp)->lsn;
				/*
				 * Set the LSN on the page to be equal to the LSN snapshot of the current transaction
				 * so that we don't need to evict the page from local buffer if we use the same LSN
				 * snapshot for the subsequent transactions.
				 * 
				 * When a page is requested to insert tuples, neon may response with an all-zero
				 * page if the page does not exist previously (e.g. the relation is empty). The function
				 * PageIsVerifiedExtended checks either the checksum of a page is valid or the page
				 * is all-zero. If we set the LSN for an all-zero page, that function will consider the
				 * page invalid, so we only set the LSN if the the LSN of the page is not zero, meaning
				 * the page is not an all-zero page. 
				 */
				if (PageGetLSN((Page) buffer) != 0)
					PageSetLSN((Page) buffer, lsn);
			}
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read block %u in rel %u/%u/%u.%u in region %d, from page server at lsn %X/%08X",
							blkno,
							rnode.spcNode,
							rnode.dbNode,
							rnode.relNode,
							region,
							forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	pfree(resp);
}

/*
 *	neon_read() -- Read the specified block from a relation.
 */
void
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
		  char *buffer)
{
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdread(reln, forkNum, blkno, buffer);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	request_lsn = neon_get_request_lsn(&latest, reln->smgr_region, reln->smgr_rnode.node, forkNum, blkno);
	neon_read_at_lsn_multi_region(reln->smgr_rnode.node, forkNum, blkno, reln->smgr_region, request_lsn, latest, buffer);
#ifdef DEBUG_COMPARE_LOCAL
	if (forkNum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		char		mdbuf[BLCKSZ];
		char		mdbuf_masked[BLCKSZ];

		mdread(reln, forkNum, blkno, mdbuf);

		memcpy(pageserver_masked, buffer, BLCKSZ);
		memcpy(mdbuf_masked, mdbuf, BLCKSZ);

		if (PageIsNew(mdbuf))
		{
			if (!PageIsNew(pageserver_masked))
			{
				elog(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 reln->smgr_rnode.node.spcNode,
					 reln->smgr_rnode.node.dbNode,
					 reln->smgr_rnode.node.relNode,
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(buffer));
			}
		}
		else if (PageIsNew(buffer))
		{
			elog(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
				 blkno,
				 reln->smgr_rnode.node.spcNode,
				 reln->smgr_rnode.node.dbNode,
				 reln->smgr_rnode.node.relNode,
				 forkNum,
				 (uint32) (request_lsn >> 32), (uint32) request_lsn,
				 hexdump_page(mdbuf));
		}
		else if (PageGetSpecialSize(mdbuf) == 0)
		{
			/* assume heap */
			RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked, blkno);
			RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

			if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
			{
				elog(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
					 blkno,
					 reln->smgr_rnode.node.spcNode,
					 reln->smgr_rnode.node.dbNode,
					 reln->smgr_rnode.node.relNode,
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf_masked),
					 hexdump_page(pageserver_masked));
			}
		}
		else if (PageGetSpecialSize(mdbuf) == MAXALIGN(sizeof(BTPageOpaqueData)))
		{
			if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf))->btpo_cycleid < MAX_BT_CYCLE_ID)
			{
				/* assume btree */
				RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked, blkno);
				RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
				{
					elog(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 reln->smgr_rnode.node.spcNode,
						 reln->smgr_rnode.node.dbNode,
						 reln->smgr_rnode.node.relNode,
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked),
						 hexdump_page(pageserver_masked));
				}
			}
		}
	}
#endif
}

#ifdef DEBUG_COMPARE_LOCAL
static char *
hexdump_page(char *page)
{
	StringInfoData result;

	initStringInfo(&result);

	for (int i = 0; i < BLCKSZ; i++)
	{
		if (i % 8 == 0)
			appendStringInfo(&result, " ");
		if (i % 40 == 0)
			appendStringInfo(&result, "\n");
		appendStringInfo(&result, "%02x", (unsigned char) (page[i]));
	}

	return result.data;
}
#endif

/*
 *	neon_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
neon_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   char *buffer, bool skipFsync)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
			{
				/* It exists locally. Guess it's unlogged then. */
				mdwrite(reln, forknum, blocknum, buffer, skipFsync);

				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwrite(reln, forknum, blocknum, buffer, skipFsync);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_wallog_page(reln, forknum, blocknum, buffer);

	lsn = PageGetLSN(buffer);
	elog(SmgrTrace, "smgrwrite called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forknum, blocknum,
		 (uint32) (lsn >> 32), (uint32) lsn);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
#endif
}

/*
 *	neon_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
neon_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	NeonResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrnblocks() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdnblocks(reln, forknum);

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(reln->smgr_rnode.node, forknum, &n_blocks))
	{
		elog(SmgrTrace, "cached nblocks for %u/%u/%u.%u: %u blocks",
			 reln->smgr_rnode.node.spcNode,
			 reln->smgr_rnode.node.dbNode,
			 reln->smgr_rnode.node.relNode,
			 forknum, n_blocks);
		return n_blocks;
	}

	request_lsn = neon_get_request_lsn(&latest,
									   reln->smgr_region,
									   reln->smgr_rnode.node,
									   forknum,
									   REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonNblocksRequest request = {
			.req.tag = T_NeonNblocksRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.req.region = reln->smgr_region,
			.rnode = reln->smgr_rnode.node,
			.forknum = forknum,
		};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonNblocksResponse:
			n_blocks = ((NeonNblocksResponse *) resp)->n_blocks;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read relation size of rel %u/%u/%u.%u in region %d from page server at lsn %X/%08X",
							reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							reln->smgr_rnode.node.relNode,
							forknum,
							reln->smgr_region,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}
	update_cached_relsize(reln->smgr_rnode.node, forknum, n_blocks);

	elog(SmgrTrace, "neon_nblocks: rel %u/%u/%u fork %u region %d (request LSN %X/%08X): %u blocks",
		 reln->smgr_rnode.node.spcNode,
		 reln->smgr_rnode.node.dbNode,
		 reln->smgr_rnode.node.relNode,
		 forknum,
		 reln->smgr_region,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 n_blocks);

	pfree(resp);
	return n_blocks;
}

/*
 *	neon_db_size() -- Get the size of the database in bytes.
 */
int64
neon_dbsize(Oid dbNode)
{
	NeonResponse *resp;
	int64		db_size;
	XLogRecPtr	request_lsn;
	bool		latest;
	RelFileNode dummy_node = {InvalidOid, InvalidOid, InvalidOid};

	request_lsn = neon_get_request_lsn(&latest, GLOBAL_REGION, dummy_node, MAIN_FORKNUM, REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonDbSizeRequest request = {
			.req.tag = T_NeonDbSizeRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.dbNode = dbNode,
		};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonDbSizeResponse:
			db_size = ((NeonDbSizeResponse *) resp)->db_size;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read db size of db %u from page server at lsn %X/%08X",
							dbNode,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	elog(SmgrTrace, "neon_dbsize: db %u (request LSN %X/%08X): %ld bytes",
		 dbNode,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 db_size);

	pfree(resp);
	return db_size;
}

/*
 *	neon_truncate() -- Truncate relation to specified number of blocks.
 */
void
neon_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrtruncate() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdtruncate(reln, forknum, nblocks);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	set_cached_relsize(reln->smgr_rnode.node, forknum, nblocks);

	/*
	 * Truncating a relation drops all its buffers from the buffer cache
	 * without calling smgrwrite() on them. But we must account for that in
	 * our tracking of last-written-LSN all the same: any future smgrnblocks()
	 * request must return the new size after the truncation. We don't know
	 * what the LSN of the truncation record was, so be conservative and use
	 * the most recently inserted WAL record's LSN.
	 */
	lsn = GetXLogInsertRecPtr();

	lsn = nm_adjust_lsn(lsn);

	/*
	 * Flush it, too. We don't actually care about it here, but let's uphold
	 * the invariant that last-written LSN <= flush LSN.
	 */
	XLogFlush(lsn);

	/*
	 * Truncate may affect several chunks of relations. So we should either
	 * update last written LSN for all of them, or update LSN for "dummy"
	 * metadata block. Second approach seems more efficient. If the relation
	 * is extended again later, the extension will update the last-written LSN
	 * for the extended pages, so there's no harm in leaving behind obsolete
	 * entries for the truncated chunks.
	 */
	SetLastWrittenLSNForRelation(lsn, reln->smgr_rnode.node, forknum);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdtruncate(reln, forknum, nblocks);
#endif
}

/*
 *	neon_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
neon_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgrimmedsync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdimmedsync(reln, forknum);
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	elog(SmgrTrace, "[NEON_SMGR] immedsync noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}

/*
 * neon_start_unlogged_build() -- Starting build operation on a rel.
 *
 * Some indexes are built in two phases, by first populating the table with
 * regular inserts, using the shared buffer cache but skipping WAL-logging,
 * and WAL-logging the whole relation after it's done. Neon relies on the
 * WAL to reconstruct pages, so we cannot use the page server in the
 * first phase when the changes are not logged.
 */
static void
neon_start_unlogged_build(SMgrRelation reln)
{
	/*
	 * Currently, there can be only one unlogged relation build operation in
	 * progress at a time. That's enough for the current usage.
	 */
	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
		elog(ERROR, "unlogged relation build is already in progress");
	Assert(unlogged_build_rel == NULL);

	ereport(SmgrTrace,
			(errmsg("starting unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	switch (reln->smgr_relpersistence)
	{
		case 0:
			elog(ERROR, "cannot call smgr_start_unlogged_build() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			unlogged_build_rel = reln;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_PERMANENT;
			return;

		default:
			elog(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (smgrnblocks(reln, MAIN_FORKNUM) != 0)
		elog(ERROR, "cannot perform unlogged index build, index is not empty ");

	unlogged_build_rel = reln;
	unlogged_build_phase = UNLOGGED_BUILD_PHASE_1;

	/* Make the relation look like it's unlogged */
	reln->smgr_relpersistence = RELPERSISTENCE_UNLOGGED;

	/*
	 * FIXME: should we pass isRedo true to create the tablespace dir if it
	 * doesn't exist? Is it needed?
	 */
	mdcreate(reln, MAIN_FORKNUM, false);
}

/*
 * neon_finish_unlogged_build_phase_1()
 *
 * Call this after you have finished populating a relation in unlogged mode,
 * before you start WAL-logging it.
 */
static void
neon_finish_unlogged_build_phase_1(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg("finishing phase 1 of unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	if (unlogged_build_phase == UNLOGGED_BUILD_NOT_PERMANENT)
		return;

	Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_1);
	Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

	unlogged_build_phase = UNLOGGED_BUILD_PHASE_2;
}

/*
 * neon_end_unlogged_build() -- Finish an unlogged rel build.
 *
 * Call this after you have finished WAL-logging an relation that was
 * first populated without WAL-logging.
 *
 * This removes the local copy of the rel, since it's now been fully
 * WAL-logged and is present in the page server.
 */
static void
neon_end_unlogged_build(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg("ending unlogged build of relation %u/%u/%u",
					reln->smgr_rnode.node.spcNode,
					reln->smgr_rnode.node.dbNode,
					reln->smgr_rnode.node.relNode)));

	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_PERMANENT)
	{
		RelFileNodeBackend rnode;

		Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_2);
		Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

		/* Make the relation look permanent again */
		reln->smgr_relpersistence = RELPERSISTENCE_PERMANENT;

		/* Remove local copy */
		rnode = reln->smgr_rnode;
		for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			elog(SmgrTrace, "forgetting cached relsize for %u/%u/%u.%u",
				 rnode.node.spcNode,
				 rnode.node.dbNode,
				 rnode.node.relNode,
				 forknum);

			forget_cached_relsize(rnode.node, forknum);
			mdclose(reln, forknum);
			/* use isRedo == true, so that we drop it immediately */
			mdunlink(rnode, forknum, true);
		}
	}

	unlogged_build_rel = NULL;
	unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
}

static void
AtEOXact_neon(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Forget about any build we might have had in progress. The local
			 * file will be unlinked by smgrDoPendingDeletes()
			 */
			unlogged_build_rel = NULL;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
			clear_region_lsns();
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			clear_region_lsns();
			/* fall through */
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
			{
				unlogged_build_rel = NULL;
				unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 (errmsg("unlogged index build was not properly finished"))));
			}
			break;
	}
}

static const struct f_smgr neon_smgr =
{
	.smgr_init = neon_init,
	.smgr_shutdown = NULL,
	.smgr_open = neon_open,
	.smgr_close = neon_close,
	.smgr_create = neon_create,
	.smgr_exists = neon_exists,
	.smgr_unlink = neon_unlink,
	.smgr_extend = neon_extend,
	.smgr_prefetch = neon_prefetch,
	.smgr_reset_prefetch = neon_reset_prefetch,
	.smgr_read = neon_read,
	.smgr_write = neon_write,
	.smgr_writeback = neon_writeback,
	.smgr_nblocks = neon_nblocks,
	.smgr_truncate = neon_truncate,
	.smgr_immedsync = neon_immedsync,

	.smgr_start_unlogged_build = neon_start_unlogged_build,
	.smgr_finish_unlogged_build_phase_1 = neon_finish_unlogged_build_phase_1,
	.smgr_end_unlogged_build = neon_end_unlogged_build,
};

const f_smgr *
smgr_neon(BackendId backend, RelFileNode rnode)
{

	/* Don't use page server for temp relations */
	if (backend != InvalidBackendId)
		return smgr_standard(backend, rnode);
	else
		return &neon_smgr;
}

void
smgr_init_neon(void)
{
	RegisterXactCallback(AtEOXact_neon, NULL);

	smgr_init_standard();
	neon_init();
}

/*
 * SLRU stuff
 */

const char *
slru_kind_to_string(NeonSlruKind kind)
{
	switch (kind)
	{
		case NEON_CLOG:
			return "pg_xact";
		case NEON_MULTI_XACT_MEMBERS:
			return "pg_multixact/members";
		case NEON_MULTI_XACT_OFFSETS:
			return "pg_multixact/offsets";
		case NEON_CSNLOG:
			return "pg_csn";
		default:
			return "invalid";
	}
}

bool
slru_kind_from_string(const char* str, NeonSlruKind* kind)
{
	if (strcmp(str, "pg_xact") == 0)
	{
		*kind = NEON_CLOG;
		return true;
	}
	else if (strcmp(str, "pg_multixact/members") == 0)
	{
		*kind = NEON_MULTI_XACT_MEMBERS;
		return true;
	}
	else if (strcmp(str, "pg_multixact/offsets") == 0)
	{
		*kind = NEON_MULTI_XACT_OFFSETS;
		return true;
	}
	else if (strcmp(str, "pg_csn") == 0)
	{
		*kind = NEON_CSNLOG;
		return true;
	}
	return false;
}

/**
 * neon_slru_kind_check() - Check if the SLRU kind is supported by the pageserver
 */
bool
neon_slru_kind_check(SlruCtl ctl)
{
	const char *dir = ctl->Dir;

	if (strcmp(dir, "pg_xact") == 0 && neon_slru_clog)
	{
		return true;
	}

	if ((strcmp(dir, "pg_multixact/members") == 0 || strcmp(dir, "pg_multixact/offsets") == 0) &&
		neon_slru_multixact)
	{
		return true;
	}

	if (strcmp(dir, "pg_csn") == 0 && neon_slru_csnlog) 
	{
		return true;
	}

	return false;
}

/**
 * neon_slru_read_page() -- Read the specified block from a Simple LRU.
 *
 * NOTE: Never call ereport(ERROR) in here to comply with the behavior expected in slru.c
 */
bool
neon_slru_read_page(SlruCtl ctl, int segno, off_t offset, XLogRecPtr min_lsn, char *buffer)
{
	NeonResponse 				*resp;
	NeonGetSlruPageResponse 	*get_slru_page_resp;
	NeonSlruKind	kind;
	bool			latest;
	XLogRecPtr		request_lsn = min_lsn;
	bool 			read_ok = false;
	// FIXME: select the right region for specific slru kinds
	int				region = current_region;
	RelFileNode dummy_node = {InvalidOid, InvalidOid, InvalidOid};

	if (!slru_kind_from_string(ctl->Dir, &kind))
	{
		ereport(WARNING, errmsg("unexpected slru kind \"%s\"", ctl->Dir));
		return false;
	}

	// FIXME: select the right region for specific slru kinds
	// IF the caller has set a min_lsn, then use the same for the request.
	if (min_lsn == InvalidXLogRecPtr) {
        request_lsn =
            neon_get_request_lsn(&latest, region, dummy_node, MAIN_FORKNUM,
                                REL_METADATA_PSEUDO_BLOCKNO);
    }
	/**
	 * During recovery, if there is no WAL redoing, the function GetXLogReplayRecPtr will return
	 * a lsn 0, causing zenith_get_request_lsn to give out an invalid lsn with "latest" being false,
	 * which is later rejected by the page server.
	 * 
	 * For this corner case to occur, a backend has to request a page during recovery
	 * mode and no WAL redoing actually happens, so this rarely happens with normal backends. 
	 * However, since the startup process calls TrimCLOG and TrimMultiXact towards the end of
	 * the recovery process, this case always happens with the startup process whenever there
	 * is no WAL redoing.
	 * 
	 * It is safe to just grab the latest page here because TrimCLOG and TrimMultiXact are called
	 * after log redoing.
	 */
	if (RecoveryInProgress() && request_lsn == InvalidXLogRecPtr) 
		latest = true;

	{
		NeonGetSlruPageRequest request = {
			.req.tag = T_NeonGetSlruPageRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.req.region = region,
			.kind = kind,
			.segno = segno,
			.blkno = offset,
			.check_exists_only = false
		};

		resp = page_server->request((NeonRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_NeonGetSlruPageResponse:
			get_slru_page_resp = (NeonGetSlruPageResponse *) resp;

			/* see notes about reading truncated segment in recovery in SlruPhysicalWritePage in slru.c */
			if (get_slru_page_resp->seg_exists)
			{
				memcpy(buffer, get_slru_page_resp->page, BLCKSZ);
			}
			else if (InRecovery)
			{
				ereport(LOG,
						(errmsg("segment \"%s/%d\" doesn't exist, reading as zeroes",
								slru_kind_to_string(kind), segno)));
				MemSet(buffer, 0, BLCKSZ);
			}

			read_ok = true;
			break;

		case T_NeonErrorResponse:
			ereport(WARNING,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read block %lu in SLRU %s/%u in region %d, from page server at lsn %X/%08X",
							offset,
							slru_kind_to_string(kind),
							segno,
							region,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(WARNING, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	pfree(resp);

	return read_ok;
}

bool
neon_slru_page_exists(SlruCtl ctl, int segno, off_t offset)
{
	NeonResponse	*resp;
	NeonSlruKind	kind;
	bool			latest;
	XLogRecPtr		request_lsn;
	// FIXME: select the right region for specific slru kinds
	int				region = current_region;
	bool			exists = false;
	RelFileNode dummy_node = {InvalidOid, InvalidOid, InvalidOid};

	if (!slru_kind_from_string(ctl->Dir, &kind))
	{
		ereport(ERROR, errmsg("unexpected slru kind \"%s\"", ctl->Dir));
		return false;
	}

	// FIXME: select the right region for specific slru kinds
	request_lsn = neon_get_request_lsn(&latest, region, dummy_node, MAIN_FORKNUM, REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonGetSlruPageRequest request = {
			.req.tag = T_NeonGetSlruPageRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.req.region = region,
			.kind = kind,
			.segno = segno,
			.blkno = offset,
			.check_exists_only = true
		};

		resp = page_server->request((NeonRequest *) &request);
	}

	switch (resp->tag)
	{
		case T_NeonGetSlruPageResponse:
			exists = ((NeonGetSlruPageResponse *) resp)->page_exists;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not read block %lu in SLRU %s/%u in region %d from page server at lsn %X/%08X",
							offset,
							slru_kind_to_string(kind),
							segno,
							region,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			elog(ERROR, "unexpected response from page server with tag 0x%02x", resp->tag);
	}

	pfree(resp);

	return exists;
} 
