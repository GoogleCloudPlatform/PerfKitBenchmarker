SELECT * FROM sys.databases;

SELECT * FROM sys.master_files;

DECLARE @dbname SYSNAME;
DECLARE @cmd NVARCHAR(300);

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases d
WHERE
  d.state = 0
    OPEN db_cursor;

FETCH NEXT FROM db_cursor INTO @dbname;

WHILE @@FETCH_STATUS
= 0
  BEGIN
SET
  @cmd = 'USE '
    + QUOTENAME(@dbname)
    + '; SELECT DB_NAME() as databasename,* FROM sys.database_scoped_configurations'
      EXEC sp_executesql @cmd;

FETCH NEXT FROM db_cursor INTO @dbname;

END;

CLOSE db_cursor;

DEALLOCATE db_cursor;
