#include "queryFS.h"
#include <mysql.h>
#include <stdio.h>
#include <string.h>

#define QUERYFS_SCHEMA_NAME "QueryFS"

#define QUERY_SELECT_CLAUSE "queryId, queryName, UNIX_TIMESTAMP(dbTimeStamp) AS dbTimeStampL"
#define DIR_SELECT_CLAUSE "directoryId, directoryName, UNIX_TIMESTAMP(dbTimeStamp) AS dbTimeStampL"

qfsQuery* qfs_getQuery(int *queryId, const char *name, MYSQL *dbConn);
int queryExists(const char *name, MYSQL *dbConn);
qfsQuery** qfs_getDirectoryDirContents(int dirId, MYSQL *dbConn);
qfsQuery** qfs_getDirectoryFileContents(int dirId, MYSQL *dbConn);
int qfs_getDirectoryIdFromPath(const char *path, MYSQL *dbConn, FILE *logfile);
qfsQuery * qfs_getObjectFromPath(const char *path, MYSQL *dbConn, FILE *logfile);
qfsDir* qfs_getDirectory(int *dirId, char *name, int *parentDirId, MYSQL *dbConn);
void qfs_getQueryResults(int queryId, int fileType, char *outfile, MYSQL *dbConn, FILE *logfile);

