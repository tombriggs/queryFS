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

test:
	rm /tmp/queryFS.log 
	./queryFS --host=127.0.0.1 --user=root "--password=Temp123!" -s mnt
	ls -al mnt
	ls -al mnt/a
	ls -al mnt/b
	ls -al mnt/a/aa
	sleep 1
	fusermount -u mnt

