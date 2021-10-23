queryFS: queryFS.c
	gcc -o queryFS queryFS.c `mysql_config --cflags --libs` `pkg-config fuse3 --cflags --libs`

queryFS_client: queryFS_client.c
	gcc -o queryFS_client queryFS_client.c `mysql_config --cflags --libs`


