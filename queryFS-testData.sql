INSERT INTO queryFS_query(queryName, queryText) VALUES('queryFS_tables', 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''QueryFS''');

INSERT INTO queryFS_queryDirectoryMap(queryId, directoryId) VALUES(LAST_INSERT_ID(), 1);

INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('a', 1);
INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('b', 1);
INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('c', 1);

INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('aa', 2);
INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('bb', 3);
INSERT INTO queryFS_directory(directoryName, parentDirectoryId) VALUES('cc', 4);

