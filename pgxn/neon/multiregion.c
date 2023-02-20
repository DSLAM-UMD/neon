/*-------------------------------------------------------------------------
 *
 * multiregion.c
 *	  Handles network communications in a multi-region setup.
 *
 * IDENTIFICATION
 *	 contrib/neon/multiregion.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "multiregion.h"

#include "access/remotexact.h"
#include "catalog/catalog.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "walproposer_utils.h"

/* Track the first LSN we get from the pageserver for a region */
static XLogRecPtr	region_lsns[MAX_REGIONS];

/*
 * Get the LSN of a region. The caller must 
 */
XLogRecPtr
get_region_lsn(int region)
{
	Assert(IsMultiRegion());	

	/*
	 * LSN of the current region is already tracked by postgres
	 */
	AssertArg(region != current_region);
	AssertArg(region < MAX_REGIONS);

	if (region_lsns[region] == InvalidXLogRecPtr)
		region_lsns[region] = neon_get_latest_lsn(region);

	return region_lsns[region];
}

/*
 * Return an array of currently known LSNs of MAX_REGIONS region
 */
XLogRecPtr *
get_all_region_lsns(void)
{
	RelFileNode dummy_node = {InvalidOid, InvalidOid, InvalidOid};

	region_lsns[current_region] =
		GetLastWrittenLSN(dummy_node, MAIN_FORKNUM, REL_METADATA_PSEUDO_BLOCKNO);

	return region_lsns;
}

void
clear_region_lsns(void)
{
	int i;
	for (i = 0; i < MAX_REGIONS; i++)
		region_lsns[i] = InvalidXLogRecPtr;
}
