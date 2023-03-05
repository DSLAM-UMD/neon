/*-------------------------------------------------------------------------
 *
 * log.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/neon/log.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOG_H
#define LOG_H

#define REMOTEXACT_TAG "[remotexact] "
#define remotexact_log(tag, fmt, ...) ereport(tag,                                  		\
											  (errmsg(REMOTEXACT_TAG fmt, ##__VA_ARGS__), 	\
										 	   errhidestmt(true), errhidecontext(true)))

#endif							/* LOG_H */