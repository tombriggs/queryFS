
DELIMITER ;

DROP PROCEDURE IF EXISTS QueryFS.sp_getQueryResult;

DELIMITER $$

CREATE PROCEDURE QueryFS.sp_getQueryResult(IN _queryId INTEGER)
BEGIN

	DECLARE _queryText TEXT;

	SELECT queryText
	INTO _queryText
	FROM queryFS_query
	WHERE queryId = _queryId;

	IF _queryText IS NOT NULL THEN
		SET @sql = _queryText;

		PREPARE queryFSstmt FROM @sql;
		EXECUTE queryFSstmt;
		DEALLOCATE PREPARE queryFSstmt;
	ELSE
		-- Always return something
		SELECT CONCAT('Unknown queryId ', _queryId) AS queryFSerror;
	END IF;

END

$$

DELIMITER ;

