// Based on https://zetcode.com/db/mysqlc/

#include "queryFS.h"
#include "queryFS_mysql.h"

qfsQuery* qfs_getQuery(int *queryId, const char *name, MYSQL *dbConn)
{
	char querySQL[512];
	char whereClause[256];

	if (dbConn == NULL)
		return NULL;

	if (queryId != NULL)
		sprintf(whereClause, "queryId = %d", *queryId);
	else if (name != NULL)
		sprintf(whereClause, "queryName = '%s'", name);
		
	sprintf(querySQL, "SELECT %s FROM queryFS_query WHERE %s", QUERY_SELECT_CLAUSE, whereClause);
//printf("%s\n", querySQL);

	if (mysql_query(dbConn, querySQL))
	{
		printf("Failed to execute query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		printf("Failed to store results for query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	qfsQuery* q = NULL;

	MYSQL_ROW row = mysql_fetch_row(result);
	if (row)
	{
		q = (qfsQuery*)malloc(sizeof(qfsQuery));
		memset(q, 0, sizeof(qfsQuery));

		((qfsItem*)q)->id = atoi(row[0]);
		((qfsItem*)q)->name = strdup(row[1]);
		((qfsItem*)q)->lastUpdateTimestamp = atol(row[2]);
	}

	mysql_free_result(result);

	return q;
}

int queryExists(const char *name, MYSQL *dbConn)
{
	int retVal = 0;
	qfsQuery* query = qfs_getQuery(NULL, name, dbConn);
	if (query != NULL)
	{
		retVal = 1;
		free(query);
	}

	return retVal;
}

qfsQuery** qfs_getDirectoryDirContents(int dirId, MYSQL *dbConn)
{
	unsigned int numContents = 0;
	char querySQL[512];

	if (dbConn == NULL)
		return NULL;

	sprintf(querySQL, "SELECT %s FROM queryFS_directory WHERE parentDirectoryId = %d", DIR_SELECT_CLAUSE, dirId);

	if (mysql_query(dbConn, querySQL))
	{
		printf("Failed to execute query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		printf("Failed to store results for query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	numContents = mysql_num_rows(result);

	qfsQuery** dirContents = (qfsQuery**)malloc(sizeof(qfsQuery*) * (numContents + 1));

	// indicate size by returning a NULL in the final element
	memset(dirContents, 0, sizeof(qfsQuery*) * (numContents + 1));

	int rowNum = 0;

	MYSQL_ROW row = mysql_fetch_row(result);
	while (row)
	{
		qfsQuery* q = (qfsQuery*)malloc(sizeof(qfsQuery));
		memset(q, 0, sizeof(qfsQuery));

		((qfsItem*)q)->id = atoi(row[0]);
		((qfsItem*)q)->name = strdup(row[1]);
		((qfsItem*)q)->lastUpdateTimestamp = atol(row[2]);

		dirContents[rowNum] = q;
		rowNum++;

		row = mysql_fetch_row(result);
	}

	mysql_free_result(result);

	return dirContents;
}

qfsQuery** qfs_getDirectoryFileContents(int dirId, MYSQL *dbConn)
{
	unsigned int numContents = 0;
	char querySQL[512];

	if (dbConn == NULL)
		return NULL;

	sprintf(querySQL, "SELECT %s FROM queryFS_query Q JOIN queryFS_queryDirectoryMap QDM USING(queryId) WHERE directoryId = %d", QUERY_SELECT_CLAUSE, dirId);

	if (mysql_query(dbConn, querySQL))
	{
		printf("Failed to execute query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		printf("Failed to store results for query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	numContents = mysql_num_rows(result);

	qfsQuery** dirContents = (qfsQuery**)malloc(sizeof(qfsQuery*) * (numContents + 1));

	// indicate size by returning a NULL in the final element
	memset(dirContents, 0, sizeof(qfsQuery*) * (numContents + 1));

	int rowNum = 0;

	MYSQL_ROW row = mysql_fetch_row(result);
	while (row)
	{
		qfsQuery* q = (qfsQuery*)malloc(sizeof(qfsQuery));
		memset(q, 0, sizeof(qfsQuery));

		((qfsItem*)q)->id = atoi(row[0]);
		((qfsItem*)q)->name = strdup(row[1]);
		((qfsItem*)q)->lastUpdateTimestamp = atol(row[2]);

		dirContents[rowNum] = q;
		rowNum++;

		row = mysql_fetch_row(result);
	}

	mysql_free_result(result);

	return dirContents;
}

int qfs_getDirectoryIdFromPath(const char *path, MYSQL *dbConn, FILE *logfile)
{
	int dirId = -1;
	char querySQL[512];

	if (dbConn == NULL)
		return -1;

	sprintf(querySQL, "CALL sp_getDirectoryFromPath('%s')", path);
//printf("%s\n", querySQL);

	if (mysql_query(dbConn, querySQL))
	{
		printf("Failed to execute query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return -2;
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		printf("Failed to store results for directory \"%s\"\n", querySQL);
		// Need error logging/handling here
		return -3;
	}

	MYSQL_ROW row = mysql_fetch_row(result);
	if (row)
	{
		dirId = atoi(row[0]);
	}

	mysql_free_result(result);

	// more results? -1 = no, >0 = error, 0 = yes (keep looping)
	int moreResults = mysql_next_result(dbConn);
	if (moreResults > -1)
	{
		MYSQL_RES *result2 = mysql_store_result(dbConn);
		if (result2 != NULL)
		{
			if (logfile != NULL)
			{
				fprintf(logfile, "Unexpected extra results for query \"%s\"? (%d)\n", querySQL, moreResults);
				fflush(logfile);
			}
			mysql_free_result(result2);
		}
		else if (mysql_field_count(dbConn) != 0)
		{
			if (logfile != NULL)
			{
				fprintf(logfile, "Unexpected extra results for query \"%s\"? (%d) fields\n", querySQL, mysql_field_count(dbConn));
				fflush(logfile);
			}
		}
	}

	return dirId;
}

qfsQuery * qfs_getObjectFromPath(const char *path, MYSQL *dbConn, FILE *logfile)
{
	qfsQuery *q = NULL;
	char querySQL[512];

	if (dbConn == NULL)
		return NULL;

	sprintf(querySQL, "CALL sp_getObjectFromPath('%s')", path);

	if(mysql_query(dbConn, querySQL))
	{
		if (logfile != NULL)
			fprintf(logfile, "Failed to execute query \"%s\": %s\n", querySQL, mysql_error(dbConn));
		return NULL;
	}
	else if (logfile != NULL)
	{
		fprintf(logfile, "Successfully executed query \"%s\"\n", querySQL);
		fflush(logfile);
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		if (logfile != NULL)
			fprintf(logfile, "Failed to store results for directory \"%s\"\n", querySQL);
		return NULL;
	}
	else if (logfile != NULL)
	{
		fprintf(logfile, "Successfully stored results for query \"%s\"\n", querySQL);
		fflush(logfile);
	}

	MYSQL_ROW row = mysql_fetch_row(result);
	if (row != NULL)
	{
		if (logfile != NULL)
		{
			fprintf(logfile, "getObject row: %s %s %s\n", row[0], row[1], row[2]);
			fflush(logfile);
		}

		int ftype = atoi(row[1]);
		if (ftype != -1)
		{
			q = (qfsQuery*)malloc(sizeof(qfsQuery));
			((qfsItem*)q)->id = atoi(row[0]);
			((qfsItem*)q)->lastUpdateTimestamp = atol(row[2]);
			q->type = atoi(row[1]);
		}
	}
	else if (logfile != NULL)
	{
		fprintf(logfile, "No results for query \"%s\"\n", querySQL);
		fflush(logfile);
	}

	mysql_free_result(result);

	// more results? -1 = no, >0 = error, 0 = yes (keep looping)
	int moreResults = mysql_next_result(dbConn);
	if (moreResults > -1)
	{
		MYSQL_RES *result2 = mysql_store_result(dbConn);
		if (result2 != NULL)
		{
			if (logfile != NULL)
			{
				fprintf(logfile, "Unexpected extra results for query \"%s\"? (%d)\n", querySQL, moreResults);
				fflush(logfile);
			}
			mysql_free_result(result2);
		}
		else if (mysql_field_count(dbConn) != 0)
		{
			if (logfile != NULL)
			{
				fprintf(logfile, "Unexpected extra results for query \"%s\"? (%d) fields\n", querySQL, mysql_field_count(dbConn));
				fflush(logfile);
			}
		}
	}

	return q;
}

qfsDir* qfs_getDirectory(int *dirId, char *name, int *parentDirId, MYSQL *dbConn)
{
	char querySQL[512];
	char whereClause[256];

	if (dbConn == NULL)
		return NULL;

	if (dirId != NULL)
		sprintf(whereClause, "directoryId = %d", *dirId);
	else if (name != NULL)
		sprintf(whereClause, "directoryName = '%s' AND parentDirectoryId = %d", name, *parentDirId);
	else
		return NULL;

	sprintf(querySQL, "SELECT directoryId, directoryName, parentDirectoryId, UNIX_TIMESTAMP(dbTimeStamp)*1000 AS dbTimeStampL FROM queryFS_directory WHERE %s", whereClause);
//printf("%s\n", querySQL);

	if (mysql_query(dbConn, querySQL))
	{
		printf("Failed to execute query \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	MYSQL_RES *result = mysql_store_result(dbConn);

	if (result == NULL)
	{
		printf("Failed to store results for directory \"%s\"\n", querySQL);
		// Need error logging/handling here
		return NULL;
	}

	qfsDir* q = (qfsDir*)malloc(sizeof(qfsDir));
	memset(q, 0, sizeof(qfsDir));

	MYSQL_ROW row = mysql_fetch_row(result);
	if (row)
	{
		((qfsItem*)q)->id = atoi(row[0]);
		((qfsItem*)q)->name = strdup(row[1]);
		((qfsItem*)q)->lastUpdateTimestamp = atol(row[2]);
	}

	mysql_free_result(result);

	return q;
}

