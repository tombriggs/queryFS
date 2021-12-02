CC = gcc
CFLAGS = -Wall `mysql_config --cflags` `pkg-config fuse3 --cflags`
LDFLAGS = `mysql_config --libs` `pkg-config fuse3 --libs`
OBJFILES = queryFS.o queryFS_mysql.o
TARGET = queryFS

all: $(TARGET)

$(TARGET): $(OBJFILES)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJFILES) $(LDFLAGS)

clean:
	rm -f $(OBJFILES) $(TARGET) 

mount: $(TARGET)
	rm -f /tmp/queryFS.log 
	mkdir -p mnt
	./queryFS --host=127.0.0.1 --user=root "--password=Temp123!" -s mnt

test: mount
	ls -al mnt
	ls -al mnt/a
	ls -al mnt/b
	ls -al mnt/a/aa
	cat mnt/queryFS_tables.csv
	cat mnt/a/aa/queryFS_schemas.tsv
	sleep 1
	fusermount -u mnt

