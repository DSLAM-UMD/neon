/*-------------------------------------------------------------------------
 * contrib/remotexact/validate.c
 * 
 * This file contains function to validate the read set in the rwset
 * of a remote transaction.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn_log.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/remotexact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "storage/bufmgr.h"
#include "rwset.h"
#include "validate.h"

void validate_index_scan(RWSetRelation *rw_rel)
{
    Oid relid = rw_rel->relid;
	XidCSN read_csn = rw_rel->csn;
    Relation rel;
    HeapScanDesc scan;
    int nkeys = 0;
    ScanKey keys = NULL;
    int scan_flags = 0;
    int i; 
    RWSetPage *page = NULL;
    Page index_page;
    XLogRecPtr page_lsn = InvalidXLogRecPtr;

    if (!remotexact_validate_index)
        return;

    // This function must only be called for index scans in current_region.
    Assert(rw_rel->region == current_region);
    Assert(rw_rel->is_index && !rw_rel->is_table_scan);

    // Lock in the same mode as SELECT (AccessShareLock).
    // TODO(pooja): To avoid starvation, we might want to use RowShareLock.
    rel = index_open(relid, AccessShareLock);
    scan = (HeapScanDesc)heap_beginscan(rel, SnapshotAny, nkeys, keys, NULL,
                                        scan_flags);

    // For each index page, check if the lsn has been updated.
    for (i = 0; i < rw_rel->n_pages; i++)
    {
        page = &(rw_rel->pages[i]);

        // Advance the hscan to specified block and lock the page for sharing.
        heap_getpageonly(scan, page->blkno);
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

        // Get the page_lsn and unlock the page.
        index_page = BufferGetPage(scan->rs_cbuf);
        page_lsn = PageGetLSN(index_page);
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

        if (page_lsn > read_csn) {
            /* 
             * This page has been updated since the last snapshot, so
             * we need to fail validation. 
             * TODO(pooja): We need to check for each tuple to avoid
             * frequent aborts. 
             */
            break;
        }
    }

    heap_endscan((TableScanDesc)scan);
    index_close(rel, AccessShareLock);

    if (page_lsn > read_csn) {
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("[remotexact] read out-of-date index (relid: %u, blockno: %u, csn: %X/%X, readcsn: %X/%X)",
                        relid,
                        rw_rel->pages[i].blkno,
                        LSN_FORMAT_ARGS(page_lsn),
                        LSN_FORMAT_ARGS(read_csn))));
    }
}

void
validate_table_scan(RWSetRelation *rw_rel)
{
    Oid relid = rw_rel->relid;
    XidCSN read_csn = rw_rel->csn;
    Relation rel;
    TableScanDesc scan;
    HeapScanDesc hscan;
	HeapTuple	htup;
    Snapshot    snapshot = GetActiveSnapshot();

    if (!remotexact_validate_table)
        return;

    // This function must only be called for table scans in current_region.
    Assert(rw_rel->region == current_region);
    Assert(rw_rel->is_table_scan && !rw_rel->is_index);

    rel = table_open(relid, AccessShareLock);

    /*
     * Use SnapshotAny to scan over all tuples
     */
    scan = table_beginscan(rel, SnapshotAny, 0, NULL);
	hscan = (HeapScanDesc) scan;

	while ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        HeapTupleHeader tuple = htup->t_data;
        TransactionId checked_xid = InvalidTransactionId;
        XLogRecPtr tuple_csn = InvalidXLogRecPtr;

        /*
         * Must lock the buffer before checking for visibility
         */
		LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

        /*
         * If a tuple is visible now, it must also be visible to read_csn
         */
        if (HeapTupleSatisfiesVisibility(current_region, htup, snapshot, hscan->rs_cbuf))
        {
            TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple);

            /*
             * Current transaction must not make any modification prior to validation
             */
            Assert(!TransactionIdIsCurrentTransactionId(xmin));

            checked_xid = xmin;
        }
        /*
         * If a tuple is not visible now, it must also not be visible to read_csn.
         * We only need to consider tuples that are committed and then removed
         * as seen by the current snapshot here. In-progress and aborted tuples
         * are never visible to read_csn.
         * 
         * TODO(ctring): There is an edge case where a tuple is removed and
         * then immediately vacuumed after the remote transaction starts but
         * before validation. The physical tuple is no longer available for
         * us to do these checks, resulting in wrong validation. One way to counter
         * this is counting the number of visible tuples while scanning them and
         * compare it with the number of visible tuples during validation.
         */
        else if (HeapTupleHeaderXminCommitted(tuple) &&
                 (HeapTupleHeaderXminFrozen(tuple) ||
                  !XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)))
        {
            TransactionId xmax;

            /*
             * Xmax must be valid because the tuple is invisible because it 
             * was deleted.
             */
            Assert(!(tuple->t_infomask & HEAP_XMAX_INVALID) &&
                   !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

            /*
             * Extract xmax based on whether it is a multixact or not
             */
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
                xmax = HeapTupleGetUpdateXid(current_region, tuple);
            else
                xmax = HeapTupleHeaderGetRawXmax(tuple);

            /*
             * Cannot be the current transaction because it does not make any
             * modification before validation.
             */
            Assert(!TransactionIdIsCurrentTransactionId(xmax));
            
            checked_xid = xmax;
        }

        LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

        /*
         * Translate checked_xid into csn and compare it with csn used for the
         * initial read.
         */
        tuple_csn = CSNLogGetCSNByXid(current_region, checked_xid);
        if (TransactionIdIsValid(checked_xid) &&
            tuple_csn > read_csn)
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("[remotexact] read out-of-date table (relid: %u, blockno: %u, offset: %u, csn: %X/%X, readcsn: %X/%X)",
                            relid,
                            ItemPointerGetBlockNumber(&htup->t_self),
                            ItemPointerGetOffsetNumber(&htup->t_self),
                            LSN_FORMAT_ARGS(tuple_csn),
                            LSN_FORMAT_ARGS(read_csn))));

    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);
}

void
validate_tuple_scan(RWSetRelation *rw_rel)
{
    Oid             relid = rw_rel->relid;
    XidCSN          read_csn = rw_rel->csn;
    Relation        rel;
    int             i;
    Snapshot        snapshot = GetActiveSnapshot();
    HeapTupleData   htup;
    Buffer          buf = InvalidBuffer;
    bool            valid = false;

    if (!remotexact_validate_tuple)
        return;

    // This function must only be called for table scans in current_region.
    Assert(rw_rel->region == current_region);
    Assert(!rw_rel->is_table_scan && !rw_rel->is_index);

    rel = table_open(relid, AccessShareLock);

    for (i = 0; i < rw_rel->n_tuples; i++)
    {
        XLogRecPtr tuple_csn = InvalidXLogRecPtr;

        htup.t_self = rw_rel->tuples[i].tid;

        /* By default, keep_buffer = false to ensure that we don't keep the
         * buffer around when the tuple is not valid. Any tuple that is not
         * active should fail validation.
         */
#if PG_VERSION_NUM >= 150000
        valid = heap_fetch(rel, snapshot, &htup, &buf, false);
#else
        valid = heap_fetch(rel, snapshot, &htup, &buf);
#endif

        if (valid)
        {
            /* The tuple is currently valid, we still need to ensure that it
             * was the same tuple that we read at the remote region. We can
             * do this by making sure that the xact that wrote this tuple
             * committed at or before the read_csn.
             */
            TransactionId xmin = HeapTupleHeaderGetRawXmin(htup.t_data);
            tuple_csn = CSNLogGetCSNByXid(current_region, xmin);
            if (TransactionIdIsValid(xmin) &&
                tuple_csn > read_csn)
                valid = false;

            ReleaseBuffer(buf); 
        }

        if (!valid) {
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("[remotexact] read out-of-date tuple (relid: %u, blockno: %u, offset: %u, csn: %X/%X, readcsn: %X/%X)",
                            relid,
                            ItemPointerGetBlockNumber(&rw_rel->tuples[i].tid),
                            ItemPointerGetOffsetNumber(&rw_rel->tuples[i].tid),
                            LSN_FORMAT_ARGS(tuple_csn),
                            LSN_FORMAT_ARGS(read_csn))));
        }
    }


    table_close(rel, AccessShareLock);
}