// Based on https://zetcode.com/db/mysqlc/

#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "queryFS.h"
#include "queryFS_mysql.h"

typedef struct queryFS_pdata_s
{
    FILE *logfile;
    MYSQL *dbConn;
} queryFS_pdata;

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

/*
static void *queryFS_init(struct fuse_conn_info *conn,
			struct fuse_config *cfg)
{
	return NULL;
}
*/

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

#define QUERYFS_FILE_SIZE 1024

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
	else
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
				stbuf->st_size = QUERYFS_FILE_SIZE;
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

#define EXT_CSV ".csv"
#define EXT_TSV ".tsv"
#define MAX_FILENAME_LEN 256

#define QUERYFS_LOGFILE "/tmp/queryFS.log"

const char *extensions[] = {EXT_CSV, EXT_TSV};
int numExtensions = sizeof(extensions) / sizeof(extensions[0]);

static int queryFS_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi,
			 enum fuse_readdir_flags flags)
{
	(void) offset;
	(void) fi;
	(void) flags;

	int dirId = 0;

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

#define OUTFILE_PATH "/tmp/"

int queryFS_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;

	MYSQL *dbConn = ((queryFS_pdata*)(fuse_get_context()->private_data))->dbConn;
	FILE *logfile = ((queryFS_pdata*)(fuse_get_context()->private_data))->logfile;

	if (logfile != NULL)
		fprintf(logfile, "queryFS_open: %s\n", path);

	if ((fi->flags & O_ACCMODE) != O_RDONLY)
        return -EACCES;

	qfsQuery* query = qfs_getObjectFromPath(path, dbConn, logfile);
	if (query != NULL)
	{
		if (logfile != NULL)
			fprintf(logfile, "queryFS_open: found query %s; id: %d type: %d timestamp %ld\n", 
				path,
				((qfsItem*)query)->id,
				query->type,
				((qfsItem*)query)->lastUpdateTimestamp);

		if (query->type == 0)
		{
			// Can't read directories
			retstat = -ENOENT;
		}
		else
		{
			char outFile[MAX_FILENAME_LEN];
			time_t now = time(NULL);

			// Figure out the file type from the extension
			int fileType = EXT_TYPE_CSV;
			if (0 == strcmp(path + strlen(path) - strlen(EXT_TSV), EXT_TSV))
				fileType = EXT_TYPE_TSV;

			sprintf(outFile, "%squeryFS_output_%d_%ld.txt", OUTFILE_PATH, ((qfsItem*)query)->id, now);
			if (logfile != NULL)
				fprintf(logfile, "queryFS_open: writing results (type %d) to file %s\n", fileType, outFile);

			qfs_getQueryResults(((qfsItem*)query)->id, fileType, outFile, dbConn, logfile);
    		fd = open(outFile, fi->flags);
			if (fd < 0)
			{
				if (logfile != NULL)
					fprintf(logfile, "queryFS_open: open failed\n");
			}
			else
			{
				if (logfile != NULL)
					fprintf(logfile, "queryFS_open: outfile #%d\n", fd);
			}
    	
			fi->fh = fd;
		}
	}
	else
		retstat = -ENOENT;
		
	return retstat;
}

int queryFS_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	FILE *logfile = ((queryFS_pdata*)(fuse_get_context()->private_data))->logfile;

	if (logfile != NULL)
		fprintf(logfile, "queryFS_read: %s %ld %ld %ld\n", path, size, offset, fi->fh);

    int r = pread(fi->fh, buf, size, offset);

	if (logfile != NULL)
	{
		fprintf(logfile, "queryFS_read: read %d bytes\n", r);
		fprintf(logfile, "queryFS_read: %s\n", buf);
	}

	return r;
}

int queryFS_release(const char *path, struct fuse_file_info *fi)
{
	FILE *logfile = ((queryFS_pdata*)(fuse_get_context()->private_data))->logfile;

	if (logfile != NULL)
		fprintf(logfile, "queryFS_release: %s\n", path);

    return close(fi->fh);
}

static const struct fuse_operations queryFS_oper = {
//	.init           = queryFS_init,
	.getattr	= queryFS_getattr,
	.readdir	= queryFS_readdir,
	.open		= queryFS_open,
	.read		= queryFS_read,
	.release	= queryFS_release,
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

