# What is queryFS?
queryFS is a Virtual File System built using FUSE that presents the results of queries from a MySQL database as files in the file system.

## Prerequisites
### libfuse
install libfuse as described at https://github.com/libfuse/libfuse OR on Ubuntu:
    sudo apt install fuse3 libfuse3-3 libfuse3-dev
### A MySQL instance to connect to
If MySQL isn't available already you can use the createMySQLDockerContainer.sh to create a MySQL instance in a Docker container.

## Building
Simply run `make` to build. There are 'mount' and 'test' targets in the Makefile you can edit to make executing queryFS easier as well.
