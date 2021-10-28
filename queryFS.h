#ifndef QUERYFS_H
#define QUERYFS_H

#define EXT_TYPE_CSV 0
#define EXT_TYPE_TSV 1

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
#endif

