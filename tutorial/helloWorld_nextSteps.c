/** @file
 *
 * Compile with:
 *
 *     gcc -Wall helloWorld.c `pkg-config fuse3 --cflags --libs` -o helloWorld
 *
 */


#define FUSE_USE_VERSION 31


#include <fuse.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#define HELLO_WORLD_FILENAME "HelloWorld.txt"

static int helloWorld_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi,
			 enum fuse_readdir_flags flags)
{
	FILE *logFile = (FILE*)(fuse_get_context()->private_data);

	if (logFile != NULL)
		fprintf(logFile, "readdir: %s\n", path);

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0, 0);
	filler(buf, "..", NULL, 0, 0);
	filler(buf, HELLO_WORLD_FILENAME, NULL, 0, 0);

	return 0;
}

#define HELLO_WORLD_CONTENTS "Hello, world!\n"

static int helloWorld_getattr(const char *path, struct stat *stbuf,
			 struct fuse_file_info *fi)
{
	int res = 0;

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	} else if (strcmp(path+1, HELLO_WORLD_FILENAME) == 0) {
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
		stbuf->st_size = strlen(HELLO_WORLD_CONTENTS);
	} else
		res = -ENOENT;

	return res;
}

static int helloWorld_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path+1, HELLO_WORLD_FILENAME) != 0)
		return -ENOENT;

	if ((fi->flags & O_ACCMODE) != O_RDONLY)
		return -EACCES;

	return 0;
}

static int helloWorld_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	if(strcmp(path+1, HELLO_WORLD_FILENAME) != 0)
		return -ENOENT;

	len = strlen(HELLO_WORLD_CONTENTS);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, HELLO_WORLD_CONTENTS + offset, size);
	} else
		size = 0;

	return size;
}

static void helloWorld_destroy(void *private_data)
{
    if (private_data != NULL)
    {
        fclose((FILE*)private_data);
    }
}


static const struct fuse_operations helloWorld_oper = {
	.readdir	= helloWorld_readdir,
	.getattr	= helloWorld_getattr,
	.open		= helloWorld_open,
	.read		= helloWorld_read,
	.destroy    = helloWorld_destroy,
};

int main(int argc, char *argv[])
{
	int ret;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	FILE * logFile = fopen("/tmp/hello.log", "a");

	ret = fuse_main(args.argc, args.argv, &helloWorld_oper, logFile);
	fuse_opt_free_args(&args);
	return ret;
}
