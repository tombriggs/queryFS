
DELIMITER ;

DROP PROCEDURE IF EXISTS QueryFS.sp_getDirectoryFromPath;

DELIMITER $$

CREATE PROCEDURE QueryFS.sp_getDirectoryFromPath(IN _path VARCHAR(512))
BEGIN

	DECLARE _pathPart VARCHAR(256);
	DECLARE _pathRemaining VARCHAR(512);
	DECLARE _dirId INTEGER DEFAULT 0;
	DECLARE _parentDirId INTEGER DEFAULT 1;
	DECLARE _pos INTEGER;

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

		-- What's left is the final directory name
		SELECT directoryId
		INTO _dirId
		FROM queryFS_directory 
		WHERE directoryName = _pathPart 
			AND parentDirectoryId = _parentDirId;
	END IF;

	SELECT _dirId AS directoryId;

END

$$

DELIMITER ;

