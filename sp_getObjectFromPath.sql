
DELIMITER ;

DROP PROCEDURE IF EXISTS QueryFS.sp_getObjectFromPath;

DELIMITER $$

CREATE PROCEDURE QueryFS.sp_getObjectFromPath(IN _path VARCHAR(512))
BEGIN

	DECLARE _pathPart VARCHAR(256);
	DECLARE _pathRemaining VARCHAR(512);
	DECLARE _dirId INTEGER DEFAULT 0;
	DECLARE _parentDirId INTEGER DEFAULT 1;
	DECLARE _pos INTEGER;
	DECLARE _objectType TINYINT DEFAULT -1; -- -1 unknown, 0 dir, 1 file
	DECLARE _lastFour VARCHAR(4);
	DECLARE _lastUsedTimestamp BIGINT;

	IF _path = '/' THEN
		SET _dirId = 1;
		SET _parentDirId = 0;
	ELSEIF SUBSTRING(_path, 1, 1) = '/' THEN
		SET _pathRemaining = SUBSTRING(_path, 2);
		SET _pathPart = _pathRemaining;
-- SELECT _pathRemaining AS pathRemaining;

		SET _pos = INSTR(_pathRemaining, '/');
-- SELECT _pos AS position;

		WHILE _pos > 0 DO
			SET _pathPart = SUBSTRING(_pathRemaining, 1, _pos - 1);
-- SELECT _pathPart AS pathPart;

			SET _dirId = NULL;

			SELECT directoryId
			INTO _dirId
			FROM queryFS_directory 
			WHERE directoryName = _pathPart 
				AND parentDirectoryId = _parentDirId;
-- SELECT _dirId AS dirId;

			IF _dirId IS NOT NULL THEN
				SET _parentDirId = _dirId;
				SET _pathRemaining = SUBSTRING(_pathRemaining, _pos + 1);
				SET _pathPart = _pathRemaining;
				SET _pos = INSTR(_pathRemaining, '/');
			END IF;
		END WHILE;
			
		SET _dirId = NULL;

		-- What's left is the final directory name or file name
		SELECT directoryId, ROUND(UNIX_TIMESTAMP(dbTimeStamp)) 
		INTO _dirId, _lastUsedTimestamp
		FROM queryFS_directory 
		WHERE directoryName = _pathPart 
			AND parentDirectoryId = _parentDirId;

		IF _dirId IS NOT NULL THEN
			SET _objectType = 0; -- dir
		ELSE
			-- See if we can find a file by this name in this directory

			-- Gotta take the file extension off
			SET _lastFour = SUBSTRING(_pathPart, LENGTH(_pathPart) - 3, 4);
			IF _lastFour IN('.txt', '.csv', '.tsv') THEN
				SET _pathPart = SUBSTRING(_pathPart, 1, LENGTH(_pathPart) - 4);
			END IF;
			
			SELECT queryId, ROUND(UNIX_TIMESTAMP(dbTimeStamp)) 
			INTO _dirId, _lastUsedTimestamp
			FROM queryFS_query Q JOIN queryFS_queryDirectoryMap QDM USING(queryId) 
			WHERE queryName = _pathPart
				AND directoryId = _parentDirId;

			IF _dirId IS NOT NULL THEN
				SET _objectType = 1; -- file
			END IF;
		END IF;

	END IF;

	-- IF _dirId IS NOT NULL THEN
		SELECT _dirId AS directoryId, _objectType AS objectType, _lastUsedTimestamp AS dbTimeStampL;
	-- ELSE
		-- SELECT NULL AS directoryId, NULL AS objectType, NULL AS dbTimeStampL WHERE 1=0;
	-- END IF;

END

$$

DELIMITER ;

