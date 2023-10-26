/*-------------------------------------------------------------------------
 *
 * validate.h
 *
 * contrib/remotexact/validate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VALIDATE_H
#define VALIDATE_H

#include "postgres.h"

#include "utils/snapshot.h"

/* GUC variables defined in remotexact.c */
extern bool remotexact_validate_index;
extern bool remotexact_validate_table;
extern bool remotexact_validate_tuple;

void validate_index_scan(RWSetRelation *rw_rel);
void validate_table_scan(RWSetRelation *rw_rel);
void validate_tuple_scan(RWSetRelation *rw_rel);

#endif							/* VALIDATE_H */
