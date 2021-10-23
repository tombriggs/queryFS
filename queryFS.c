// Based on https://zetcode.com/db/mysqlc/

#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define QUERYFS_SCHEMA_NAME "QueryFS"

typedef struct qfsItem_struct
{
	int id;
	char *name;
	long lastUpdateTimestamp;
} qfsItem;

typedef struct qfsQuery_struct
{
	qfsItem itemInfo;
	int parentId;
	short type;
} qfsQuery;

typedef struct qfsEntry_dir
{
	qfsItem itemInfo;
	qfsItem **childItems;
} qfsDir;

typedef struct queryFS_pdata_s
{
    FILE *logfile;
    MYSQL *dbConn;
} queryFS_pdata;

#define QUERY_SELECT_CLAUSE "queryId, queryName, UNIX_TIMESTAMP(dbTimeStamp) AS dbTimeStampL"
#define DIR_SELECT_CLAUSE "directoryId, directoryName, UNIX_TIMESTAMP(dbTimeStamp) AS dbTimeStampL"

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

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
static struct options {
	const char *host;
	const char *user;
	const char *password;
	int show_help;
} options;

#define OPTION(t, p)                           \
    { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
	OPTION("--host=%s", host),
	OPTION("--user=%s", user),
	OPTION("--password=%s", password),
	OPTION("-h", show_help),
	OPTION("--help", show_help),
	FUSE_OPT_END
};

static void *queryFS_init(struct fuse_conn_info *conn,
			struct fuse_config *cfg)
{
	return NULL;
}

static void queryFS_destroy(void *private_data)
{
	if (private_data != NULL)
	{
		queryFS_pdata * pdata = (queryFS_pdata*)private_data;
		mysql_close(pdata->dbConn);
		if (pdata->logfile != NULL)
			fclose(pdata->logfile);
	}
}

static int queryFS_getattr(const char *path, struct stat *stbuf,
			 struct fuse_file_info *fi)
{
	(void) fi;
	int res = 0;

	MYSQL *dbConn = ((queryFS_pdata*)(fuse_get_context()->private_data))->dbConn;
	FILE *logfile = ((queryFS_pdata*)(fuse_get_context()->private_data))->logfile;

	if (logfile != NULL)
		fprintf(logfile, "queryFS_getattr start: %s\n", path);

	memset(stbuf, 0, sizeof(struct stat));

	// Optimize the simple case
	if (strcmp(path, "/") == 0) 
	{
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	
		if (logfile != NULL)
			fprintf(logfile, "queryFS_getattr returned root dir\n");
	}
	else if (strncmp(path, "/.", 2) == 0)
	{
		// Temporary (?) hack
		if (logfile != NULL)
			fprintf(logfile, "queryFS_getattr: ignoring dot file %s\n", path);
		res = -ENOENT;
	}
	else // if (queryExists(path+1, dbConn)) 
	{
		// qfsQuery* query = qfs_getQuery(NULL, path + 1, dbConn);
		qfsQuery* query = qfs_getObjectFromPath(path, dbConn, logfile);
		// qfsQuery* query = NULL;
		if (query != NULL)
		{
			if (logfile != NULL)
				fprintf(logfile, "queryFS_getattr: found query %s; id: %d type: %d timestamp %ld\n", 
					path,
					((qfsItem*)query)->id,
					query->type,
					((qfsItem*)query)->lastUpdateTimestamp);

			if (query->type == 0)
			{
				stbuf->st_mode = S_IFDIR | 0755;
				stbuf->st_nlink = 2;
			}
			else
			{
				stbuf->st_mode = S_IFREG | 0444;
				stbuf->st_nlink = 1;
				stbuf->st_size = 7; // ??? TODO
			}
			stbuf->st_mtime = ((qfsItem*)query)->lastUpdateTimestamp;

			free(query);
		} 
		else
		{
			if (logfile != NULL)
				fprintf(logfile, "queryFS_getattr: did not find query %s\n", path);

			res = -ENOENT;
		}
	}

	return res;
}

#define EXT_TXT ".txt"
#define EXT_CSV ".csv"
#define EXT_TSV ".tsv"
#define MAX_FILENAME_LEN 256

#define QUERYFS_LOGFILE "/tmp/queryFS.log"

static int queryFS_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi,
			 enum fuse_readdir_flags flags)
{
	(void) offset;
	(void) fi;
	(void) flags;

	const char *extensions[] = {EXT_TXT, EXT_CSV, EXT_TSV};
	int numExtensions = sizeof(extensions) / sizeof(extensions[0]);

	int dirId = 0;
	int parentId = 0;
	char pathPart[MAX_FILENAME_LEN];

	char fileName[MAX_FILENAME_LEN];

	MYSQL *dbConn = ((queryFS_pdata*)(fuse_get_context()->private_data))->dbConn;
	FILE *logfile = ((queryFS_pdata*)(fuse_get_context()->private_data))->logfile;
				
	if (logfile != NULL)
		fprintf(logfile, "queryFS_readdir: path %s start\n", path);

	// The root directory is the easy case
	if (strcmp(path, "/") == 0)
	{
		dirId = 1;
	}
	else
	{
		// Call the DB to find out what directory this is
		dirId = qfs_getDirectoryIdFromPath(path, dbConn, logfile);
		if (dirId < 0)
		{
			if (logfile != NULL)
				fprintf(logfile, "queryFS_readdir: could not find id for path %s\n", path);
			return -ENOENT;
		}
	}


	filler(buf, ".", NULL, 0, 0);
	filler(buf, "..", NULL, 0, 0);
	//filler(buf, options.filename, NULL, 0, 0);

	qfsQuery** dirFileContents = qfs_getDirectoryFileContents(dirId, dbConn);
	if (dirFileContents != NULL)
	{
		qfsQuery** thisFile = dirFileContents;

		// Identify these all as regular files
		struct stat st;
   		memset(&st, 0, sizeof(st));
   		st.st_mode = S_IFREG | 0444;

		while (*thisFile != NULL)
		{
			for (int i = 0; i < numExtensions; i++)
			{
				strcpy(fileName, ((qfsItem*)*thisFile)->name);
				strncat(fileName, extensions[i], MAX_FILENAME_LEN - 1);
				fileName[MAX_FILENAME_LEN - 1] = '\0';

				st.st_mtime = ((qfsItem*)*thisFile)->lastUpdateTimestamp;

				if (logfile != NULL)
					fprintf(logfile, "queryFS_readdir: adding %s, timestamp %ld\n", fileName, st.st_mtime);

				filler(buf, fileName, &st, 0, 0);
			}

			free(*thisFile);

			thisFile++;
		}

		free(dirFileContents);
	}

	qfsQuery** dirDirContents = qfs_getDirectoryDirContents(dirId, dbConn);
	if (dirDirContents != NULL)
	{
		qfsQuery** thisDir = dirDirContents;

		// Identify these all as directories
		struct stat st;
   		memset(&st, 0, sizeof(st));
   		st.st_mode = S_IFDIR | 0444;

		while (*thisDir != NULL)
		{
			st.st_mtime = ((qfsItem*)*thisDir)->lastUpdateTimestamp;

			filler(buf, ((qfsItem*)*thisDir)->name, &st, 0, FUSE_FILL_DIR_PLUS);
			
			free(*thisDir);

			thisDir++;
		}

		free(dirDirContents);
	}

	return 0;
}


static const struct fuse_operations queryFS_oper = {
//	.init           = queryFS_init,
	.getattr	= queryFS_getattr,
	.readdir	= queryFS_readdir,
//	.open		= queryFS_open,
//	.read		= queryFS_read,
	.destroy	= queryFS_destroy,
};

int main(int argc, char **argv)
{
	int ret;

	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	/* Set defaults -- we have to use strdup so that
	   fuse_opt_parse can free the defaults if other
	   values are specified */
	options.host = strdup("localhost");
	options.user = strdup("root");
	options.password = strdup("Temp123!");

	/* Parse options */
	if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
		return 1;

	/* When --help is specified, first print our own file-system
	   specific help text, then signal fuse_main to show
	   additional help (by adding `--help` to the options again)
	   without usage: line (by setting argv[0] to the empty
	   string) */
	if (options.show_help) {
		//show_help(argv[0]);
		fuse_opt_add_arg(&args, "--help");
		args.argv[0][0] = '\0';
	}

	// Open DB connection
	MYSQL *dbConn = mysql_init(NULL);

	if (dbConn == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(dbConn));
		exit(2);
	}

	if (mysql_real_connect(dbConn, options.host, options.user, options.password, QUERYFS_SCHEMA_NAME, 0, NULL, 0) == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(dbConn));
		mysql_close(dbConn);
		exit(2);
	}

	queryFS_pdata * private_data = (queryFS_pdata*)malloc(sizeof(queryFS_pdata));
	private_data->dbConn = dbConn;
	private_data->logfile = fopen(QUERYFS_LOGFILE, "a");

	ret = fuse_main(args.argc, args.argv, &queryFS_oper, private_data);
	fuse_opt_free_args(&args);

	// mysql_close(dbConn);

	return ret;
}

