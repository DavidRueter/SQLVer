SET ANSI_NULLS ON
GO
SET ANSI_PADDING ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM sys.triggers WHERE parent_class_desc = 'DATABASE' AND name = N'dtgLogSchemaChanges') BEGIN
  DISABLE TRIGGER [dtgLogSchemaChanges] ON DATABASE
END 

--Create new sqlver schema if needed
IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = 'sqlver') BEGIN
  DECLARE @SQL nvarchar(MAX)
  SET @SQL = 'CREATE SCHEMA [sqlver] AUTHORIZATION [dbo]'
  EXEC(@SQL)
END
GO

--*** sqlver.spinsSysRTLog
IF OBJECT_ID('sqlver.spinsSysRTLog') IS NOT NULL DROP PROCEDURE sqlver.spinsSysRTLog
GO
CREATE PROCEDURE [sqlver].[spinsSysRTLog]
@Msg varchar(MAX) = NULL,
@MsgXML xml = NULL,
@ThreadGUID uniqueidentifier = NULL,
@SPID int = NULL,
@PersistAfterRollback bit = 0
WITH EXECUTE AS OWNER
AS 
BEGIN
  SET NOCOUNT ON
  
  --Added 2/13/2013.  Since this procedure is used for logging messages, including errors, it is possible
  --that this routine may be called in a TRY / CATCH block when there is a doomed transaction.  In such a
  --case this insert would fail.  Since the transaction is doomed anyway, I think that rolling it back here
  --(instead of explicitly within each CATCH block) is cleaner.
  IF XACT_STATE() = -1 BEGIN
    ROLLBACK TRAN
  END  
  
  SET @SPID = COALESCE(@@SPID, @SPID)
  
  IF @PersistAfterRollback = 0 BEGIN
    INSERT INTO sqlver.tblSysRTLog
      (DateLogged, Msg, MsgXML, ThreadGUID, SPID)
    VALUES
      (GETDATE(), @Msg, @MsgXML, @ThreadGUID, @SPID)
  END
  ELSE BEGIN
    /*
    This procedure is designed to allow a caller to provide a message that will be written to an error log table,
    and allow the caller to call it within a transaction.  The provided message will be persisted to the
    error log table even if the transaction is rolled back.
    
    To accomplish this, this procedure utilizes ADO to establish a second database connection (outside
    the transaction context) back into the database to call the dbo.spLogError procedure.
    */
    DECLARE @ConnStr varchar(MAX)
      --connection string for ADO to use to access the database
    SET @ConnStr = 'Provider=SQLNCLI10; DataTypeCompatibility=80; Server=localhost; Database=' + DB_NAME() + '; Uid=sqlverLogger; Pwd=sqlverLoggerPW;'

    DECLARE @SQLCommand varchar(MAX)
    SET @SQLCommand = 'EXEC sqlver.spinsSysRTLog @PersistAfterRollback=0' + 
                      ISNULL(', @Msg=''' + REPLACE(@Msg, CHAR(39), CHAR(39) + CHAR(39)) + '''', '') + 
                      ISNULL(', @ThreadGUID = ''' + CAST(@ThreadGUID AS varchar(100)) + '''', '') + 
                      ISNULL(', @MsgXML = ''' + REPLACE(CAST(@MsgXML AS varchar(MAX)), CHAR(39), CHAR(39) + CHAR(39)) + '''', '') +                      
                      ISNULL(', @SPID = ''' + CAST(@SPID AS varchar(100)) + '''', '') 
                      
    DECLARE @ObjCn int 
      --ADO Connection object  
    DECLARE @ObjRS int    
      --ADO Recordset object returned
      
    DECLARE @RecordCount int   
      --Maximum records to be returned
    SET @RecordCount = 0
     
    DECLARE @ExecOptions int
      --Execute options:  0x80 means to return no records (adExecuteNoRecords) + 0x01 means CommandText is to be evaluted as text
    SET @ExecOptions = 0x81
        
    DECLARE @LastResultCode int = NULL 
       --Last result code returned by an sp_OAxxx procedure.  Will be 0 unless an error code was encountered.
    DECLARE @ErrSource varchar(512)
      --Returned if a COM error is encounterd
    DECLARE @ErrMsg varchar(512)
      --Returned if a COM error is encountered
    
    DECLARE @ErrorMessage varchar(MAX) = NULL
      --our formatted error message


    SET @ErrorMessage = NULL
    SET @LastResultCode = 0
        
      
    BEGIN TRY
      EXEC @LastResultCode = sp_OACreate 'ADODB.Connection', @ObjCn OUT 
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END
    END TRY
    BEGIN CATCH
      SET @ErrorMessage = ERROR_MESSAGE()
    END CATCH
    
    
     BEGIN TRY  
      IF @LastResultCode = 0 BEGIN
       
        EXEC @LastResultCode = sp_OAMethod @ObjCn, 'Open', NULL, @ConnStr
        IF @LastResultCode <> 0 BEGIN
          EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
        END                
      END  
    END TRY
    BEGIN CATCH
      SET @ErrorMessage = ERROR_MESSAGE()
    END CATCH

      
    IF @LastResultCode = 0 BEGIN
      EXEC @LastResultCode = sp_OAMethod @ObjCn, 'Execute', @ObjRS OUTPUT, @SQLCommand, @ExecOptions
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END                
    END
      
    IF @ObjRS IS NOT NULL BEGIN
      BEGIN TRY
        EXEC sp_OADestroy @ObjCn  
      END TRY
      BEGIN CATCH
        --not much we can do...
        SET @LastResultCode = 0
      END CATCH
    END
      
    IF @ObjCn= 1 BEGIN
      BEGIN TRY
        EXEC sp_OADestroy @ObjCn
      END TRY
      BEGIN CATCH
        --not much we can do...
        SET @LastResultCode = 0
      END CATCH
    END    
      
    IF ((@LastResultCode <> 0) OR (@ErrorMessage IS NOT NULL)) BEGIN
      SET @ErrorMessage = 'Error in sqlver.spinsSysRTLog' + ISNULL(': ' + @ErrMsg, '')
      RAISERROR(@ErrorMessage, 16, 1)
    END
  
  END
  
END
GO
--*** sqlver.sputilResultSetAsStr
IF OBJECT_ID('sqlver.sputilResultSetAsStr') IS NOT NULL DROP PROCEDURE sqlver.sputilResultSetAsStr
GO
CREATE PROCEDURE [sqlver].[sputilResultSetAsStr]
@SQL nvarchar(MAX),
@ResultPrefix varchar(MAX) = '',
@ResultSuffix varchar(MAX) = '',
@TrimTrailSuffix bit = 1,
@IncludeLineBreaks bit = 0,
@Result varchar(MAX) OUTPUT
WITH EXECUTE AS OWNER
AS 
BEGIN
  SET NOCOUNT ON
  --note:  statement in @SQL must return only a single column.

  DECLARE @ThisValue varchar(MAX)
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)

  DECLARE @tvValues TABLE (Seq int IDENTITY PRIMARY KEY, ThisValue varchar(MAX))
  INSERT INTO @tvValues (ThisValue)
  EXEC sp_executesql @statement=@SQL
  
  DECLARE curRes CURSOR LOCAL STATIC FOR
  SELECT ThisValue FROM @tvValues ORDER BY Seq

  OPEN curRes
  
  SET @Result = ''
  FETCH curRes INTO @ThisValue
  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @Result = @Result + ISNULL(@ResultPrefix, '') + @ThisValue + ISNULL(@ResultSuffix, '') +
      CASE WHEN @IncludeLineBreaks = 1 THEN @CRLF ELSE '' END
    FETCH curRes INTO @ThisValue
  END
  CLOSE curRes
  DEALLOCATE curRes
    
  WHILE @TrimTrailSuffix = 1 AND PATINDEX('%' + REVERSE(@ResultSuffix) + '%', REVERSE(@Result)) = 1 BEGIN
    SET @Result = SUBSTRING(@Result, 1, (LEN(@Result + 'x') - 1) - (LEN(@ResultSuffix + 'x') - 1))  
  END
END
GO

--*** sqlver.sputilPrintString
IF OBJECT_ID('sqlver.sputilPrintString') IS NOT NULL DROP PROCEDURE sqlver.sputilPrintString
GO
CREATE PROCEDURE [sqlver].[sputilPrintString]
@Buf varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
  DECLARE @S varchar(MAX)
  DECLARE @P int
  SET @P = 1
  
  WHILE @P < LEN(@Buf + 'x') - 1 BEGIN
    SET @S = SUBSTRING(@Buf, @P, 4000)
    PRINT @S + '~'
    SET @P = @P + 4000
  END

END
GO
--*** sqlver.sputilGetRowCounts
IF OBJECT_ID('sqlver.sputilGetRowCounts') IS NOT NULL DROP PROCEDURE sqlver.sputilGetRowCounts
GO
CREATE PROCEDURE [sqlver].[sputilGetRowCounts]
AS 
BEGIN
  SET NOCOUNT ON
  SELECT sch.name, so.name, CAST(si.rows AS bigint) AS rows
  FROM
    sys.objects so
    JOIN sys.schemas sch ON
      so.schema_id = sch.schema_id
    JOIN sys.sysindexes AS si ON 
      so.object_id = si.id AND si.indid < 2
  WHERE
    so.type = 'U'
  ORDER BY
    si.rows DESC  

  SELECT 
   'Total Rows', 
    SUM(CAST(si.rows AS bigint)) AS rows
  FROM
    sys.objects so
    JOIN sys.schemas sch ON
      so.schema_id = sch.schema_id
    JOIN sys.sysindexes AS si ON 
      so.object_id = si.id AND si.indid < 2
  WHERE
    so.type = 'U'
END
GO

--*** sqlver.sputilWriteStringToFile
IF OBJECT_ID('sqlver.sputilWriteStringToFile') IS NOT NULL DROP PROCEDURE sqlver.sputilWriteStringToFile
GO
CREATE PROCEDURE [sqlver].[sputilWriteStringToFile]
---------------------------------------------------------------------------------------------
/*
Procedure to write @FileDate to the specified filename in the servers' filesystem.
From article by Phil Factor
http://www.simple-talk.com/sql/t-sql-programming/reading-and-writing-files-in-sql-server-using-t-sql/
*/
---------------------------------------------------------------------------------------------
@FileData varchar(MAX),
@FilePath varchar(2048),
@Filename varchar(255)
WITH EXECUTE AS CALLER
AS 
BEGIN
  --From article by Phil Factor
  --http://www.simple-talk.com/sql/t-sql-programming/reading-and-writing-files-in-sql-server-using-t-sql/

  SET NOCOUNT ON

  BEGIN TRY
    DECLARE @objFileSystem int
    DECLARE @objTextStream int
    DECLARE @objErrorObject int
    DECLARE @ErrorMessage varchar(1000)
	  DECLARE @Command varchar(1000)
	  DECLARE @hr int
    DECLARE @fileAndPath varchar(80)

    SET @ErrorMessage = 'Opening the File System Object'
    EXECUTE @hr = sp_OACreate  'Scripting.FileSystemObject' , @objFileSystem OUTPUT

    IF RIGHT(@FilePath, 1) <> '\' BEGIN
      SET @FilePath = @FilePath + '\'
    END
    
    SET @FileAndPath = @FilePath + @Filename
    IF @hr = 0 SELECT @objErrorObject = @objFileSystem, @ErrorMessage = 'creating file "' + @FileAndPath + '"'
    IF @hr = 0 EXEC @hr = sp_OAMethod @objFileSystem, 'CreateTextFile', @objTextStream OUTPUT, @FileAndPath, 2, False

    IF @hr = 0 SELECT @objErrorObject = @objTextStream, @ErrorMessage = 'writing to the file "' + @FileAndPath + '"'
    IF @hr = 0 EXEC @hr = sp_OAMethod @objTextStream, 'Write', NULL, @FileData

    IF @hr = 0 SELECT @objErrorObject = @objTextStream, @ErrorMessage = 'closing the file "' + @FileAndPath + '"'
    IF @hr = 0 EXEC @hr = sp_OAMethod  @objTextStream, 'Close'

    IF @hr <> 0 BEGIN
	    DECLARE @Source varchar(255)
		  DECLARE @Description varchar(255)
		  DECLARE @Helpfile varchar(255)
		  DECLARE @HelpID int
  	
	    EXEC sp_OAGetErrorInfo @objErrorObject, @source OUTPUT, @Description OUTPUT, @Helpfile OUTPUT, @HelpID OUTPUT
	    SELECT 
	      @ErrorMessage='Error while ' + 
	      COALESCE(@ErrorMessage, 'doing something') +
			  ', ' + COALESCE(@Description,'')
  			
	    RAISERROR (@ErrorMessage,16,1)
	  END
  	
    EXECUTE sp_OADestroy @objTextStream
  END TRY
  BEGIN CATCH
    PRINT 'Error in sqlver.sputilWriteStringToFile'
    PRINT ERROR_MESSAGE()
    PRINT 'This procedure requires use of COM (OLE Automation) objects.  To enable support, execute the following:'
    PRINT 'EXEC master.dbo.sp_configure ''show advanced options'', 1;'
    PRINT 'RECONFIGURE;'
    PRINT 'EXEC master.dbo.sp_configure ''Ole Automation Procedures'', 1;'
    PRINT 'RECONFIGURE;'    
  END CATCH

END
GO

--*** sqlver.sputilWriteBinaryToFile
IF OBJECT_ID('sqlver.sputilWriteBinaryToFile') IS NOT NULL DROP PROCEDURE sqlver.sputilWriteBinaryToFile
GO
CREATE PROCEDURE [sqlver].[sputilWriteBinaryToFile]
---------------------------------------------------------------------------------------------
/*
Procedure to write binary @FileDate to the specified filename in the server's filesystem.
Similar to sputilWriteStringToFile, but can be used for binary data.
By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
@FileData varbinary(MAX),
@FilePath varchar(2048),
@Filename varchar(255),

@ErrorMsg varchar(MAX) = NULL OUTPUT,
  --NULL unless an error message was encountered
@LastResultCode int = NULL OUTPUT,
  --0 unless an error code was returned by MSXML2.ServerXMLHttp

@SilenceErrors bit = 0
  --If 1, errors are not raised with RAISEERROR(), but caller can check @ErrorMsg.
  --@ErrorMsg will be null if no error was raised.
WITH EXECUTE AS CALLER
AS 
BEGIN 
  SET NOCOUNT ON
  
  DECLARE @Debug bit
  SET @Debug = 0

  IF @FileData IS NULL BEGIN
    RETURN (0)
  END
  
  
  DECLARE @objStream int 
  
  DECLARE @ErrSource varchar(512)
  DECLARE @ErrMsg varchar(512)

  SET @ErrorMsg = NULL

  DECLARE @FileAndPath varchar(512)
  SET @FileAndPath = @FilePath + CASE WHEN(RIGHT(@FilePath, 1) <> '\') THEN '\' ELSE '' END + @Filename

  DECLARE @adTypeBinary int
  SET @adTypeBinary = 1
  
  DECLARE @adTypeText int
  SET @adTypeText = 2
  
  DECLARE @adSaveCreateOverWrite int
  SET @adSaveCreateOverWrite = 2
  
  
  BEGIN TRY
    IF @Debug = 1 PRINT 'About to call sp_OACreate for ADODB.Stream'  
    EXEC @LastResultCode = sp_OACreate 'ADODB.Stream', @objStream OUT 
    IF @LastResultCode <> 0 BEGIN
      EXEC sp_OAGetErrorInfo @objStream, @ErrSource OUTPUT, @ErrMsg OUTPUT 
    END
  END TRY
  BEGIN CATCH
    SET @ErrorMsg = ERROR_MESSAGE()
  END CATCH


  IF @LastResultCode = 0 BEGIN
    BEGIN TRY  
      IF @Debug = 1 PRINT 'About to call sp_OASetProperty for Type'
      EXEC @LastResultCode = sp_OASetProperty @objStream, 'Type', @adTypeBinary
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @objStream, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END        
    END TRY
    BEGIN CATCH
      SET @ErrorMsg = ERROR_MESSAGE()
    END CATCH
  END
  
  IF @LastResultCode = 0 BEGIN
    BEGIN TRY  
      IF @Debug = 1 PRINT 'About to call sp_OAMethod for Open'
      EXEC @LastResultCode = sp_OAMethod @objStream, 'Open'
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @objStream, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END               
    END TRY
    BEGIN CATCH
      SET @ErrorMsg = ERROR_MESSAGE()
    END CATCH   
  END
  
  
  IF @LastResultCode = 0 BEGIN
    BEGIN TRY
      IF @Debug = 1 PRINT 'About to call sp_OAMethod for Write'      
      EXEC @LastResultCode = sp_OAMethod @objStream, 'Write', NULL, @FileData
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @objStream, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END               
    END TRY
    BEGIN CATCH
      SET @ErrorMsg = ERROR_MESSAGE()
    END CATCH   
  END     
          
          
  IF @LastResultCode = 0 BEGIN
    BEGIN TRY
      IF @Debug = 1 PRINT 'About to call sp_OAMethod for SaveToFile'      
      EXEC @LastResultCode = sp_OAMethod @objStream, 'SaveToFile', NULL, @FileAndPath, @adSaveCreateOverWrite
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @objStream, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END               
    END TRY
    BEGIN CATCH
      SET @ErrorMsg = ERROR_MESSAGE()
    END CATCH   
  END                       

  IF @objStream IS NOT NULL BEGIN
    DECLARE @DestroyResultCode int
    EXEC @DestroyResultCode = sp_OADestroy @objStream
  END
  
 
  SET @ErrorMsg = 
    NULLIF(RTRIM(
      ISNULL(@ErrorMsg + ' ', '') + 
      ISNULL('(' + @ErrMsg + ') ', '') + 
      ISNULL('[' + @ErrSource + ']', '')
    ), '')

  IF @ErrorMsg IS NOT NULL BEGIN
    PRINT 'Error in sqlver.sputilWriteBinaryToFile'
    PRINT @ErrorMsg
    PRINT 'FYI, this procedure requires use of COM (OLE Automation) objects.  To enable support, execute the following:'
    PRINT '  EXEC master.dbo.sp_configure ''show advanced options'', 1;'
    PRINT '  RECONFIGURE;'
    PRINT '  EXEC master.dbo.sp_configure ''Ole Automation Procedures'', 1;'
    PRINT '  RECONFIGURE;'    
    PRINT 'User = ' + USER_NAME()
  END

  IF (@ErrorMsg IS NOT NULL) AND (ISNULL(@SilenceErrors, 0) = 0) BEGIN
    RAISERROR (@ErrorMsg, 16, 1)
  END
END
GO

--*** sqlver.tblSysRTLog
IF OBJECT_ID('sqlver.tblSysRTLog') IS NULL BEGIN

CREATE TABLE [sqlver].[tblSysRTLog](
	[SysRTLogId] [int] IDENTITY(1,1) NOT NULL,
	[DateLogged] [datetime] NULL DEFAULT (GETDATE()),
	[Msg] [varchar](max) NULL,
	[MsgXML] [xml] NULL,
	[ThreadGUID] [uniqueidentifier] NULL,
	[SPID] [int] NULL,
 CONSTRAINT [pkSysRTMessages] PRIMARY KEY CLUSTERED 
(
	[SysRTLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

END
GO

--*** sqlver.tblSchemaManifest
IF OBJECT_ID('sqlver.tblSchemaManifest') IS NULL BEGIN

CREATE TABLE [sqlver].[tblSchemaManifest](
	[SchemaManifestId] [int] IDENTITY(1,1) NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[OrigDefinition] [nvarchar](max) NULL,
	[DateAppeared] [datetime] NULL,
	[CreatedByLoginName] [sysname] NOT NULL,
	[DateUpdated] [datetime] NULL,
	[OrigHash] [varbinary](128) NULL,
	[CurrentHash] [varbinary](128) NULL,
	[IsEncrypted] [bit] NULL,
	[StillExists] [bit] NULL,
	[SkipLogging] [bit] NULL,
	[Comments] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[SchemaManifestId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

END
GO

--*** sqlver.tblSchemaLog
IF OBJECT_ID('sqlver.tblSchemaLog') IS NULL BEGIN

CREATE TABLE [sqlver].[tblSchemaLog](
	[SchemaLogId] [int] IDENTITY(1,1) NOT NULL,
	[SPID] [smallint] NULL,
	[EventType] [varchar](50) NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[ObjectType] [varchar](25) NULL,
	[SQLCommand] [nvarchar](max) NULL,
	[EventDate] [datetime] NULL,
	[LoginName] [sysname] NOT NULL,
	[EventData] [xml] NULL,
	[Hash] [varbinary](128) NULL,
	[Comments] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[SchemaLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

END
GO

--*** sqlver.tblNumbers
IF OBJECT_ID('sqlver.tblNumbers') IS NULL BEGIN

CREATE TABLE [sqlver].[tblNumbers](
	[Number] [int] NULL
) ON [PRIMARY]

END
GO

--*** TABLE POPULATE tblNumbers
PRINT 'Populating sqlver.tblNumbers'

TRUNCATE TABLE sqlver.tblNumbers
GO
INSERT INTO sqlver.tblNumbers(Number)
SELECT TOP 99999
  ROW_NUMBER() OVER (ORDER BY so.object_id)
FROM
  sys.all_objects so
  JOIN sys.all_objects so2 ON 1=1    
  --JOIN sys.all_objects so3 ON 1=1     

--*** sqlver.spWhoIsHogging
IF OBJECT_ID('sqlver.spWhoIsHogging') IS NOT NULL DROP PROCEDURE sqlver.spWhoIsHogging
GO
CREATE PROCEDURE [sqlver].[spWhoIsHogging]
@LockType varchar(100) = 'X' --default is eXclusive locks only.  Pass NULL or 'all' to show all locks
AS
BEGIN
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  
  SELECT DISTINCT
    DB_NAME(l.resource_database_id) AS DBName,
    CASE 
      WHEN l.resource_type = 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id) 
      WHEN l.resource_type IN ('KEY', 'PAGE', 'RID') THEN OBJECT_NAME(part.object_id)         
    END AS ObjectName, 
    l.request_session_id AS SPID,     
    sp.loginame AS LoginName,   
    sp.hostname AS HostName,     
    sp.last_batch AS LastBatchTime,   
    sp.open_tran AS SessionOpenTran,
    
    COALESCE(OBJECT_SCHEMA_NAME(tx.objectid, tx.dbid) + '.' + OBJECT_NAME(tx.objectid), tx.text) AS Query, 
        
    sp.blocked AS Blocked,
    
    CASE 
      WHEN er.blocking_session_id > 0 THEN er.blocking_session_id 
    END AS BlockedBySPID,
    CASE er.blocking_session_id
      WHEN 0  Then 'Not Blocked'
      WHEN -2 Then 'Orphaned Distributed Transaction'
      WHEN -3 Then 'Deferred Recovery Transaction'
      WHEN -4 Then 'Latch owner not determined'
    END AS Blocking_Type,    
    
    l.request_mode AS RequestMode,    
    l.request_type AS RequestType,
    l.request_status AS RequestStatus, 
    l.resource_type AS ResourceType, 

    l.resource_subtype AS ResourceSubType,      
    sp.cmd AS Cmd,
 
    sp.waittype AS WaitType,
    sp.waittime AS WaitTime,
    er.total_elapsed_time AS TotalElapsedTime,
    sp.cpu AS CPU,
    sp.physical_io AS PhysicalIO,
    er.reads,
    er.writes,
    er.logical_reads,
    er.Command,    
    sp.program_name AS ProgramName,
    l.request_owner_type,
    l.request_reference_count,
    l.request_lifetime,
    --l.resource_description,
    l.resource_lock_partition,
    CASE er.transaction_isolation_level
      WHEN 0 THEN 'Unspecified'
      WHEN 1 THEN 'Read Uncomitted'
      WHEN 2 THEN 'Read Committed'
      WHEN 3 THEN 'Repeatable'
      WHEN 4 THEN 'Serializable'
      WHEN 5 THEN 'Snapshot'
    END AS TransactionIsolationLevel,       
    er.percent_complete,
    er.estimated_completion_time  
  FROM 
    sys.dm_tran_locks l
    LEFT JOIN sys.sysprocesses sp ON
      l.request_session_id = sp.spid
    LEFT JOIN sys.dm_exec_requests er ON
      sp.spid = er.session_id     
    LEFT JOIN sys.partitions part ON
      l.resource_associated_entity_id  = part.hobt_id                                                  
    OUTER APPLY sys.dm_exec_sql_text(sp.sql_handle) tx    
  WHERE
    DB_NAME(l.resource_database_id) <> 'tempdb' AND
    l.resource_database_id = DB_ID() AND
    sp.spid <> @@SPID AND
    (NULLIF(NULLIF(@LockType, 'all'), '1') IS NULL OR l.request_type = @LockType)
    
  ORDER BY
    DB_NAME(l.resource_database_id),
    l.request_session_id
    
--select * from master.sys.dm_exec_sessions     
    
END
GO

--*** sqlver.sputilGetFileList
IF OBJECT_ID('sqlver.sputilGetFileList') IS NOT NULL DROP PROCEDURE sqlver.sputilGetFileList
GO
CREATE PROCEDURE [sqlver].[sputilGetFileList]
@Path varchar(2048),
@MaxDepth int = NULL,
@IncludeFiles int = 1,
@ReturnFilesOnly bit = 1,
@SuppressResultset bit = 0
AS 
BEGIN
SET NOCOUNT ON

  IF OBJECT_ID('tempdb..#FileListFiles') IS NOT NULL BEGIN
    DROP TABLE #FileListFiles
  END

  CREATE TABLE #FileListFiles (
    FileID int IDENTITY PRIMARY KEY,
    Filename varchar(MAX),
    Path varchar(MAX),
    Depth int,
    IsFile bit,
    ParentFileID int
  )


  CREATE NONCLUSTERED INDEX ixtmpFiles_ParentFileID ON #FileListFiles ([ParentFileID])
  INCLUDE ([FileID],[Filename],[IsFile], [Depth])


  IF OBJECT_ID('tempdb..#FileList') IS NULL BEGIN
    --If caller provided this table, we will simply insert into what they provided  
    CREATE TABLE #FileList (     
      FileID int PRIMARY KEY,      
      FQFilename varchar(MAX),
      Filename varchar(MAX),
      Path varchar(MAX),
      IsFile bit,
      Depth int,
      ParentFileID int          
    )
  END


  INSERT INTO #FileListFiles (
    Filename,
    Depth,
    IsFile
  )
  EXEC xp_dirtree @Path, @MaxDepth, @IncludeFiles


  UPDATE f
  SET ParentFileID = x.ParentFileID
  FROM
    #FileListFiles f
    JOIN (
  SELECT
    f.FileID,
    MAX(f2.FileID) AS ParentFileID
  FROM
    #FileListFiles f
    JOIN #FileListFiles f2 ON
      f2.FileID < f.FileID AND
      f2.Depth < f.Depth AND
      f2.IsFile = 0
    GROUP BY
      f.FileID
  ) x ON
    f.FileID = x.FileID


  ;  
  WITH cte (
    Filename,
    FQFilename,
    Path,
    IsFile,
    Depth,
    FileID,
    ParentFileID 
  )
  AS (
    SELECT
      CASE WHEN f.IsFile = 1 THEN CAST(f.Filename AS varchar(MAX)) END AS Filename,
      CAST(@Path + f.Filename AS varchar(MAX)) AS FQFilename,
      CAST(@Path AS varchar(MAX)) AS Path,
      f.IsFile,
      f.Depth,
      f.FileID,
      f.ParentFileID    
    FROM
      #FileListFiles f
    WHERE
      f.ParentFileID IS NULL

    UNION ALL
    
    SELECT
      CASE WHEN f.IsFile = 1 THEN CAST(f.Filename AS varchar(MAX)) END AS Filename,
      cte.FQFilename + '\' + f.Filename AS FQFilename,
      cte.FQFilename AS Path,
      f.IsFile,
      f.Depth,
      f.FileID,
      f.ParentFileID   
    FROM
      cte cte
      JOIN #FileListFiles f ON
        cte.FileID = f.ParentFileID
  )

  INSERT INTO #FileList (  
    FQFilename,
    Filename,
    Path,
    IsFile,
    Depth,
    FileID,
    ParentFileID     
  )
  SELECT
    cte.FQFilename,
    cte.Filename,
    cte.Path,
    cte.IsFile,
    cte.Depth,
    cte.FileID,
    cte.ParentFileID      
  FROM cte cte
  WHERE
    (ISNULL(@ReturnFilesOnly, 0) = 0 OR cte.IsFile = 1)

    
  IF ISNULL(@SuppressResultset, 0) = 0 BEGIN
    SELECT
      fl.FQFilename,
      fl.Filename,
      fl.Path,
      fl.IsFile,
      fl.Depth,
      fl.FileID,
      fl.ParentFileID       
    FROM #FileList fl
    ORDER BY
      fl.FQFilename    
  END
  
    
END
GO

--*** sqlver.spgetSQLFilegroupsOutOfSpaceAllDBs
IF OBJECT_ID('sqlver.spgetSQLFilegroupsOutOfSpaceAllDBs') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLFilegroupsOutOfSpaceAllDBs
GO
CREATE PROCEDURE [sqlver].[spgetSQLFilegroupsOutOfSpaceAllDBs]
@ListDrives bit = 0,
@ListAllFiles bit = 0,
@MinGigsFree int = 10
AS
BEGIN
  /*
  Returns a list of databases / filegroups that do not have at least one file
  on a drive with at least @MinGigsFree.  (i.e. lists filegroups with insufficient
  disk space to grow).
  
  For each such filegroup, lists all existing files, with an indication of whether
  the drive that file on is "full" (has less than @MinGigsFree available).
  
  Optionally, @ListDrives will return a separate resultset showing all drives
  and an indication of whther each is full.  @ListAllFiles will return a separate
  resultset showing all files, regardless of whether there is insufficient space
  for the filegroup to grow.  
  */

  SET NOCOUNT ON
  IF OBJECT_ID('tempdb..#Drives') IS NOT NULL BEGIN
    DROP TABLE #Drives
  END
    
  CREATE TABLE #Drives (
    Drive varchar(10),
    IsFull bit
  )

  --derived from http://blog.sqlauthority.com/2013/08/02/sql-server-disk-space-monitoring-detecting-low-disk-space-on-server/
  INSERT INTO #Drives (
    Drive,
    IsFull
  )
  SELECT DISTINCT
   --dovs.logical_volume_name AS LogicalName,
   dovs.volume_mount_point AS Drive,
   --CONVERT(INT,dovs.available_bytes/1048576.0) AS FreeSpaceInMB,
   CASE WHEN CONVERT(INT,dovs.available_bytes/1048576.0) < 1024 * @MinGigsFree THEN 1 ELSE 0 END AS IsFull
  FROM
    sys.master_files mf
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.FILE_ID) dovs
  
  
  IF OBJECT_ID('tempdb..#FilesMaxed') IS NOT NULL BEGIN
    DROP TABLE #FilesMaxed
  END  

  CREATE TABLE #FilesMaxed (
    DBName sysname,
    FileGroupName sysname,
    Filename sysname,
    FileSize bigint,
    DriveIsFull bit
  )

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT db.name
  FROM
    sys.databases db
  WHERE
    db.state_desc = 'ONLINE'

  DECLARE @DBName sysname
  DECLARE @SQL nvarchar(MAX)

  OPEN curThis
  FETCH curThis INTO @DBName

  PRINT '>>' + @DBName

  WHILE @@FETCH_STATUS = 0 BEGIN     
    SET @SQL = N'  
    INSERT INTO #FilesMaxed (
      DBName,
      FileGroupName,
      Filename,
      FileSize,
      DriveIsFull
    )
    SELECT
      @DBName AS DBName,
      fg.name AS FileGroupName,
      df.physical_name FileName,
      df.size AS FileSize,
      drv.IsFull
    FROM
      ' + @DBName + '.sys.filegroups fg     
      JOIN ' + @DBName + '.sys.database_files df ON
        fg.data_space_id = df.data_space_id
      JOIN #Drives drv ON
        df.physical_name LIKE drv.Drive + ''%'' COLLATE SQL_Latin1_General_CP1_CI_AS        
    '
    
    EXEC sp_executesql @stmt=@SQL,
      @Params = N'@DBName sysname',
      @DBName = @DBName
          
    FETCH curThis INTO @DBName   
  END   
  CLOSE curThis
  DEALLOCATE curThis
      
     
  IF NOT EXISTS (
    SELECT
      fm.FileGroupName
    FROM
      #FilesMaxed fm
      LEFT JOIN #FilesMaxed fm2 ON
        fm.DBName = fm2.DBName AND
        fm.FileGroupName = fm2.FileGroupName AND
        fm2.DriveIsFull = 0
    WHERE
      fm2.Filename IS NULL
  ) BEGIN
    SELECT
      CAST(NULL AS sysname) AS DBName,
      CAST(NULL AS sysname) AS FileGroupName,
      CAST(NULL AS sysname) AS Filename,
      CAST(NULL AS sysname) AS DriveIsFull,
      'Good!  All Filegroups have room to grow' AS Warning
  END
  ELSE BEGIN     
       
    SELECT
      fm.DBName,
      fm.FileGroupName,
      fm.Filename,
      fm.DriveIsFull,
      'Filegroup has no room to grow' AS Warning
    FROM
      #FilesMaxed fm
      LEFT JOIN #FilesMaxed fm2 ON
        fm.DBName = fm2.DBName AND
        fm.FileGroupName = fm2.FileGroupName AND
        fm2.DriveIsFull = 0
    WHERE
      fm2.Filename IS NULL      
      
  END

  IF @ListDrives = 1 BEGIN
    SELECT
     drv.Drive,
     drv.IsFull
    FROM #Drives drv
    ORDER BY
      drv.Drive
  END

  IF @ListAllFiles = 1 BEGIN
    SELECT fm.*
    FROM
       #FilesMaxed  fm
    ORDER BY
      fm.DBName,
      fm.FileGroupName,
      fm.Filename
  END

END
GO

--*** sqlver.spgetMissingIndexes
IF OBJECT_ID('sqlver.spgetMissingIndexes') IS NOT NULL DROP PROCEDURE sqlver.spgetMissingIndexes
GO
CREATE PROCEDURE [sqlver].[spgetMissingIndexes]
AS
BEGIN
  SELECT 
    dm_mid.database_id AS DatabaseID,
    dm_migs.avg_user_impact*(dm_migs.user_seeks+dm_migs.user_scans) Avg_Estimated_Impact,
    dm_migs.last_user_seek AS Last_User_Seek,
    OBJECT_NAME(dm_mid.OBJECT_ID,dm_mid.database_id) AS [TableName],
    --BEGIN Create Statement Column String Literal
    'CREATE INDEX [ix' +
        REPLACE(OBJECT_NAME(dm_mid.OBJECT_ID,dm_mid.database_id), 'tbl', 'ix')
      + '_' 
      + REPLACE(REPLACE(REPLACE(ISNULL(dm_mid.equality_columns,''),', ','_'),'[',''),']','') 
      + CASE
          WHEN 
            dm_mid.equality_columns IS NOT NULL 
            AND dm_mid.inequality_columns IS NOT NULL 
          THEN '_'
          ELSE ''
        END
      + REPLACE(REPLACE(REPLACE(ISNULL(dm_mid.inequality_columns,''),', ','_'),'[',''),']','')
      + ']'
      + ' ON ' + dm_mid.statement
      + ' (' + ISNULL (dm_mid.equality_columns,'')
      + CASE 
          WHEN
            dm_mid.equality_columns IS NOT NULL
            AND dm_mid.inequality_columns IS NOT NULL 
          THEN ',' 
          ELSE '' 
        END
      + ISNULL (dm_mid.inequality_columns, '')
      + ')'
      + ISNULL (' INCLUDE (' + dm_mid.included_columns + ')', '') AS Create_Statement
    --END Create Statement Column String Literal
  FROM 
    sys.dm_db_missing_index_groups dm_mig
    INNER JOIN sys.dm_db_missing_index_group_stats dm_migs ON 
      dm_migs.group_handle = dm_mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details dm_mid ON 
      dm_mig.index_handle = dm_mid.index_handle
  WHERE 
    dm_mid.database_ID =  DB_ID()
  ORDER BY 
    Avg_Estimated_Impact DESC
END
GO

--*** sqlver.spUninstall
IF OBJECT_ID('sqlver.spUninstall') IS NOT NULL DROP PROCEDURE sqlver.spUninstall
GO

--*** sqlver.spUninstall
IF OBJECT_ID('sqlver.spUninstall') IS NOT NULL DROP PROCEDURE sqlver.spUninstall
GO
CREATE PROCEDURE [sqlver].[spUninstall]
@ReallyRemoveAll bit = 0
AS
BEGIN
  SET NOCOUNT ON
  
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)
  
  IF ISNULL(@ReallyRemoveAll, 0) = 0 BEGIN
    PRINT 'Executing this procedure will remove ALL SqlVer objects from the database ' + DB_NAME() + ' ' +
      'and will therefore PERMANENTLY DELETE ALL VERSION INFORMATION in the tables ' +
      'sqlver.tblSchemaManifest and sqlver.tblSchemaLog.' + @CRLF + @CRLF +
      'If this is really what you want to do, execute this procedure with the @ReallyRemoveAll paramter ' +
      'set to 1, like this:' + @CRLF + @CRLF +
      '  EXEC sqlver.spUninstall @ReallyRemoveAll = 1' + @CRLF + @CRLF +
      'You should probably make a backup of the data in the tables sqlver.tblSchemaManifest and sqlver.tblSchemaLog ' +
      'before you do so, and you should also verify that you are in the correct database.  Do you really want to ' +
      'remove all version information for database ' + DB_NAME() + '??'          
      
    RETURN
  END
  ELSE BEGIN    
    DECLARE @SQL nvarchar(MAX)
      
    IF EXISTS (SELECT * FROM sys.triggers WHERE parent_class_desc = 'DATABASE' AND name = N'dtgLogSchemaChanges') BEGIN
      DISABLE TRIGGER [dtgLogSchemaChanges] ON DATABASE
      DROP TRIGGER [dtgLogSchemaChanges] ON DATABASE
    END  
        
    SELECT 
      @SQL = ISNULL(@SQL + CHAR(10), '') + 'DROP SYNONYM ' + sch.name + '.' + syn.name
    FROM
      sys.synonyms syn
      JOIN sys.objects obj ON
        syn.object_id = obj.object_id 
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
    WHERE
      sch.name = 'sqlver' OR
      syn.base_object_name LIKE '\[sqlver\]%' ESCAPE '\'
      
    PRINT @SQL      
    EXEC (@SQL)
    
    DECLARE curThis CURSOR LOCAL STATIC FOR
    SELECT 
      'DROP ' +      
        CASE obj.type_DESC
          WHEN 'SQL_STORED_PROCEDURE' THEN 'PROCEDURE'
          WHEN 'SQL_SCALAR_FUNCTION' THEN 'FUNCTION'
          WHEN 'SQL_TABLE_VALUED_FUNCTION' THEN 'FUNCTION'
          WHEN 'CLR_SCALAR_FUNCTION' THEN 'FUNCTION'
          WHEN 'CLR_STORED_PROCEDURE' THEN 'PROCEDURE'
          WHEN 'VIEW' THEN 'VIEW'
          WHEN 'USER_TABLE' THEN 'TABLE'
       END +    
       ' ' + sch.name + '.' + obj.name           
    FROM
      sys.objects obj
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
    WHERE    
      sch.name = 'sqlver' AND
      obj.type_DESC IN (    
        'SQL_STORED_PROCEDURE',
        'SQL_SCALAR_FUNCTION',        
        'SQL_TABLE_VALUED_FUNCTION',        
        'CLR_SCALAR_FUNCTION',        
        'CLR_STORED_PROCEDURE',
        'VIEW',
        'USER_TABLE'
      )
    ORDER BY
      CASE obj.type_DESC
        WHEN 'SQL_STORED_PROCEDURE' THEN 1
        WHEN 'SQL_SCALAR_FUNCTION' THEN 2       
        WHEN 'SQL_TABLE_VALUED_FUNCTION' THEN 3
        WHEN 'CLR_SCALAR_FUNCTION' THEN 4
        WHEN 'CLR_STORED_PROCEDURE' THEN 5
        WHEN 'VIEW' THEN 6
        WHEN 'USER_TABLE' THEN 7
     END
      
    OPEN curThis
    FETCH curThis INTO @SQL
    WHILE @@FETCH_STATUS = 0 BEGIN
      EXEC (@SQL)
      FETCH curThis INTO @SQL
    END
    CLOSE curThis
    DEALLOCATE curThis
          
          
    IF EXISTS (SELECT schema_id from sys.schemas WHERE name = 'sqlver') BEGIN
      DROP SCHEMA [sqlver]
    END
    
    PRINT 'All SQLVer objects have been removed'
  END

END
GO


--*** sqlver.spsysCreateSubDir
IF OBJECT_ID('sqlver.spsysCreateSubDir') IS NOT NULL DROP PROCEDURE sqlver.spsysCreateSubDir
GO
CREATE PROCEDURE [sqlver].[spsysCreateSubDir]
@NewPath nvarchar(1024)
--$!ParseMarker
--Note:  comments and code between marker and AS are subject to automatic removal by OpsStream
--©Copyright 2006-2010 by David Rueter, Automated Operations, Inc.
--May be held, used or transmitted only pursuant to an in-force licensing agreement with Automated Operations, Inc.
--Contact info@opsstream.com / 800-964-3646 / 949-264-1555
WITH EXECUTE AS CALLER
AS 
BEGIN
  EXECUTE master.dbo.xp_create_subdir @NewPath 
END
GO

--*** sqlver.spgetSQLSpaceUsedDB
IF OBJECT_ID('sqlver.spgetSQLSpaceUsedDB') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLSpaceUsedDB
GO
CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsedDB]
@objname nvarchar(776) = NULL,		-- The object we want size on.
@updateusage varchar(5) = false		-- Param. for specifying that usage info. should be updated.
AS
/*
Copied from system stored procedure sys.sp_spaceused, but modified to return
a single resultset.  Note that SQL versions after 2012 have a parameter @oneresultset
so this procedure isn't needed for those newer versions
*/

declare @id	int			-- The object id that takes up space
		,@type	character(2) -- The object type.
		,@pages	bigint			-- Working variable for size calc.
		,@dbname sysname
		,@dbsize bigint
		,@logsize bigint
		,@reservedpages  bigint
		,@usedpages  bigint
		,@rowCount bigint

/*
**  Check to see if user wants usages updated.
*/

if @updateusage is not null
	begin
		select @updateusage=lower(@updateusage)

		if @updateusage not in ('true','false')
			begin
				raiserror(15143,-1,-1,@updateusage)
				return(1)
			end
	end
/*
**  Check to see that the objname is local.
*/
if @objname IS NOT NULL
begin

	select @dbname = parsename(@objname, 3)

	if @dbname is not null and @dbname <> db_name()
		begin
			raiserror(15250,-1,-1)
			return (1)
		end

	if @dbname is null
		select @dbname = db_name()

	/*
	**  Try to find the object.
	*/
	SELECT @id = object_id, @type = type FROM sys.objects WHERE object_id = object_id(@objname)

	-- Translate @id to internal-table for queue
	IF @type = 'SQ'
		SELECT @id = object_id FROM sys.internal_tables WHERE parent_id = @id and internal_type = 201 --ITT_ServiceQueue

	/*
	**  Does the object exist?
	*/
	if @id is null
		begin
			raiserror(15009,-1,-1,@objname,@dbname)
			return (1)
		end

	-- Is it a table, view or queue?
	IF @type NOT IN ('U ','S ','V ','SQ','IT')
	begin
		raiserror(15234,-1,-1)
		return (1)
	end
end

/*
**  Update usages if user specified to do so.
*/

if @updateusage = 'true'
	begin
		if @objname is null
			dbcc updateusage(0) with no_infomsgs
		else
			dbcc updateusage(0,@objname) with no_infomsgs
		print ' '
	end

set nocount on

/*
**  If @id is null, then we want summary data.
*/
if @id is null
begin
	select @dbsize = sum(convert(bigint,case when status & 64 = 0 then size else 0 end))
		, @logsize = sum(convert(bigint,case when status & 64 <> 0 then size else 0 end))
		from dbo.sysfiles

	select @reservedpages = sum(a.total_pages),
		@usedpages = sum(a.used_pages),
		@pages = sum(
				CASE
					-- XML-Index and FT-Index internal tables are not considered "data", but is part of "index_size"
					When it.internal_type IN (202,204,211,212,213,214,215,216) Then 0
					When a.type <> 1 Then a.used_pages
					When p.index_id < 2 Then a.data_pages
					Else 0
				END
			)
	from sys.partitions p join sys.allocation_units a on p.partition_id = a.container_id
		left join sys.internal_tables it on p.object_id = it.object_id

	/* unallocated space could not be negative */
	select 
		database_name = db_name(),
		database_size = ltrim(str((convert (dec (15,2),@dbsize) + convert (dec (15,2),@logsize)) 
			* 8192 / 1048576,15,2) + ' MB'),
		'unallocated space' = ltrim(str((case when @dbsize >= @reservedpages then
			(convert (dec (15,2),@dbsize) - convert (dec (15,2),@reservedpages)) 
			* 8192 / 1048576 else 0 end),15,2) + ' MB'),

	/*
	**  Now calculate the summary data.
	**  reserved: sum(reserved) where indid in (0, 1, 255)
	** data: sum(data_pages) + sum(text_used)
	** index: sum(used) where indid in (0, 1, 255) - data
	** unused: sum(reserved) - sum(used) where indid in (0, 1, 255)
	*/
	--dbr select
		reserved = ltrim(str(@reservedpages * 8192 / 1024.,15,0) + ' KB'),
		data = ltrim(str(@pages * 8192 / 1024.,15,0) + ' KB'),
		index_size = ltrim(str((@usedpages - @pages) * 8192 / 1024.,15,0) + ' KB'),
		unused = ltrim(str((@reservedpages - @usedpages) * 8192 / 1024.,15,0) + ' KB')
end

/*
**  We want a particular object.
*/
else
begin
	/*
	** Now calculate the summary data. 
	*  Note that LOB Data and Row-overflow Data are counted as Data Pages.
	*/
	SELECT 
		@reservedpages = SUM (reserved_page_count),
		@usedpages = SUM (used_page_count),
		@pages = SUM (
			CASE
				WHEN (index_id < 2) THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
				ELSE lob_used_page_count + row_overflow_used_page_count
			END
			),
		@rowCount = SUM (
			CASE
				WHEN (index_id < 2) THEN row_count
				ELSE 0
			END
			)
	FROM sys.dm_db_partition_stats
	WHERE object_id = @id;

	/*
	** Check if table has XML Indexes or Fulltext Indexes which use internal tables tied to this table
	*/
	IF (SELECT count(*) FROM sys.internal_tables WHERE parent_id = @id AND internal_type IN (202,204,211,212,213,214,215,216)) > 0 
	BEGIN
		/*
		**  Now calculate the summary data. Row counts in these internal tables don't 
		**  contribute towards row count of original table.
		*/
		SELECT 
			@reservedpages = @reservedpages + sum(reserved_page_count),
			@usedpages = @usedpages + sum(used_page_count)
		FROM sys.dm_db_partition_stats p, sys.internal_tables it
		WHERE it.parent_id = @id AND it.internal_type IN (202,204,211,212,213,214,215,216) AND p.object_id = it.object_id;
	END

	SELECT 
		name = OBJECT_NAME (@id),
		rows = convert (char(11), @rowCount),
		reserved = LTRIM (STR (@reservedpages * 8, 15, 0) + ' KB'),
		data = LTRIM (STR (@pages * 8, 15, 0) + ' KB'),
		index_size = LTRIM (STR ((CASE WHEN @usedpages > @pages THEN (@usedpages - @pages) ELSE 0 END) * 8, 15, 0) + ' KB'),
		unused = LTRIM (STR ((CASE WHEN @reservedpages > @usedpages THEN (@reservedpages - @usedpages) ELSE 0 END) * 8, 15, 0) + ' KB')

end


return (0) -- sp_spaceused
GO

--*** sqlver.spgetSQLSpaceUsedAllDBs
IF OBJECT_ID('sqlver.spgetSQLSpaceUsedAllDBs') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLSpaceUsedAllDBs
GO
CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsedAllDBs]
AS
BEGIN
  IF OBJECT_ID('tempdb..#DBSize') IS NOT NULL BEGIN
    DROP TABLE #DBSize
  END

  CREATE TABLE #DBSize (
    DBName sysname,
    DBTotal varchar(40),
    DBUnallocated varchar(40),  
    DBReserved varchar(40),
    DBData varchar(40),
    DBIndex varchar(40),
    DBUnused varchar(40)
  )

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT db.name
  FROM
    sys.databases db
  WHERE
    db.state_desc = 'ONLINE'  

  DECLARE @DBName sysname
  DECLARE @SQL nvarchar(MAX)

  OPEN curThis
  FETCH curThis INTO @DBName

  WHILE @@FETCH_STATUS = 0 BEGIN
    IF object_id(@DBName + '.sqlver.spgetSQLSpaceUsedDB') IS NOT NULL BEGIN
      SET @SQL = N'INSERT INTO #DBSize EXEC ' + @DBName + '.sqlver.spgetSQLSpaceUsedDB'
      EXEC(@SQL)
    END
      
    FETCH curThis INTO @DBName   
  END   
  CLOSE curThis
  DEALLOCATE curThis
      
  SELECT sz.*
  FROM
    #DBSize sz
  ORDER BY
    LEN(sz.DBTotal) DESC,
    sz.DBTotal DESC

END
GO

--*** sqlver.spgetUnusedIndexes
IF OBJECT_ID('sqlver.spgetUnusedIndexes') IS NOT NULL DROP PROCEDURE sqlver.spgetUnusedIndexes
GO
CREATE PROCEDURE [sqlver].[spgetUnusedIndexes]
AS
BEGIN
  -- Unused Index Script
  -- Original Author: Pinal Dave (C) 2011
  SELECT TOP 500
    s.name AS ObjectSchema,  
    o.name AS ObjectName
    , i.name AS IndexName
    , i.index_id AS IndexID
    , dm_ius.user_seeks AS UserSeek
    , dm_ius.user_scans AS UserScans
    , dm_ius.user_lookups AS UserLookups
    , dm_ius.user_updates AS UserUpdates
    , p.TableRows
    , 'DROP INDEX ' + QUOTENAME(i.name)
    + ' ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(OBJECT_NAME(dm_ius.OBJECT_ID)) AS 'drop statement'
  FROM
    sys.dm_db_index_usage_stats dm_ius
    INNER JOIN sys.indexes i ON i.index_id = dm_ius.index_id AND dm_ius.OBJECT_ID = i.OBJECT_ID
    INNER JOIN sys.objects o ON dm_ius.OBJECT_ID = o.OBJECT_ID
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    INNER JOIN (
      SELECT SUM(p.rows) TableRows, p.index_id, p.OBJECT_ID
      FROM sys.partitions p
      GROUP BY p.index_id, p.OBJECT_ID) p ON
    p.index_id = dm_ius.index_id AND
    dm_ius.OBJECT_ID = p.OBJECT_ID
  WHERE
    OBJECTPROPERTY(dm_ius.OBJECT_ID,'IsUserTable') = 1
    AND dm_ius.database_id = DB_ID()
    AND i.type_desc = 'nonclustered'
    AND i.is_primary_key = 0
    AND i.is_unique_constraint = 0
  ORDER BY
    TableRows DESC
-- (dm_ius.user_seeks + dm_ius.user_scans + dm_ius.user_lookups) ASC
END
GO

--*** sqlver.spShowSlowQueries
IF OBJECT_ID('sqlver.spShowSlowQueries') IS NOT NULL DROP PROCEDURE sqlver.spShowSlowQueries
GO
CREATE PROCEDURE [sqlver].[spShowSlowQueries]
@ClearStatistics bit = 0
AS
BEGIN
  --Based on article by Pinal Dave at http://blog.sqlauthority.com/2009/01/02/sql-server-2008-2005-find-longest-running-query-tsql/

  IF @ClearStatistics = 1 BEGIN
    DBCC FREEPROCCACHE
  END
  

  SELECT DISTINCT TOP 100
    COALESCE(OBJECT_SCHEMA_NAME(t.objectid, t.dbid) + '.' + OBJECT_NAME(t.objectid), t.TEXT) AS Query,
    s.total_elapsed_time / 1000 / 60  AS TotalElapsedTimeMinutes,    
    s.execution_count AS ExecutionCount,
    --s.max_elapsed_time / 1000 / 60 AS MaxElapsedTimeMinutes,  
    ISNULL(s.total_elapsed_time / NULLIF(s.execution_count, 0), 0)  / 1000 / 60 AS AvgElapsedTimeMinutes,
    ISNULL(s.total_elapsed_time / NULLIF(s.execution_count, 0), 0) AvgElapsedTimeMS,    
    s.creation_time AS LogCreatedOn,
    ISNULL(s.execution_count / NULLIF(DATEDIFF(s, s.creation_time, GETDATE()), 0), 0) AS FrequencyPerSec,
    s.total_physical_reads,
    s.last_physical_reads,
    s.total_logical_writes,
    s.last_logical_writes,
    s.total_rows,
    s.last_rows,
    DB_NAME(t.dbid),
    s.*
  FROM
    sys.dm_exec_query_stats s
    CROSS APPLY sys.dm_exec_sql_text( s.sql_handle ) t
  WHERE
    t.dbid = DB_ID()  
  ORDER BY
    TotalElapsedTimeMinutes DESC
END
GO

--*** sqlver.udfGenerateCLRRegisterSQL
IF OBJECT_ID('sqlver.udfGenerateCLRRegisterSQL') IS NOT NULL DROP FUNCTION sqlver.udfGenerateCLRRegisterSQL
GO
CREATE FUNCTION [sqlver].[udfGenerateCLRRegisterSQL](
---------------------------------------------------------------------------------------------
/*
Function to generate the dynamic SQL code needed to secure and register the
specified CLR assembly for use in SQL
By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
@AssemblyName sysname,
@FQFileName varchar(1024)
)
RETURNS varchar(MAX)
AS 
BEGIN
  DECLARE @SQL varchar(MAX)
  SET @SQL = ''
  
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)
  
  DECLARE @AssemblyIdent sysname
  SET @AssemblyIdent = REPLACE(@AssemblyName, '.', '')
    
  DECLARE @DBName sysname
  SET @DBName = DB_NAME()
     
  SET @SQL = @SQL +  
    'IF ASSEMBLYPROPERTY (''' + @AssemblyName + ''', ''MvID'') IS NOT NULL DROP ASSEMBLY [' + @AssemblyName + ']' + @CRLF + 
    'USE master;' + @CRLF +
    'IF EXISTS(SELECT * FROM sys.syslogins WHERE name = ''' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + ''') DROP LOGIN ' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + ';' + @CRLF +
    'IF EXISTS(SELECT * FROM sys.asymmetric_keys WHERE name =''' + @DBName + '#SQLCLRKey_' + @AssemblyIdent + ''') DROP ASYMMETRIC KEY ' + @DBName + '#SQLCLRKey_' + @AssemblyIdent + ';' + @CRLF +    
    'CREATE ASYMMETRIC KEY ' + @DBName + '#SQLCLRKey_' + @AssemblyIdent + ' FROM EXECUTABLE FILE = ''' + @FQFileName + ''';' + @CRLF +
    'CREATE LOGIN ' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + ' FROM ASYMMETRIC KEY ' + @DBName + '#SQLCLRKey_' + @AssemblyIdent + ';' + @CRLF +
    'ALTER LOGIN [' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + '] DISABLE;' + @CRLF +              
    'GRANT EXTERNAL ACCESS ASSEMBLY TO ' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + ';' + @CRLF +
    'GRANT UNSAFE ASSEMBLY TO ' + @DBName + '#SQLCLRLogin_' + @AssemblyIdent + ';' + @CRLF +        
    'USE ' + DB_NAME() + ';' + @CRLF +
    'CREATE ASSEMBLY [' + @AssemblyName + '] FROM ''' + @FQFileName + ''' WITH PERMISSION_SET = UNSAFE;'   

  RETURN @SQL
END
GO

--*** sqlver.udfMakeNumericStrict
IF OBJECT_ID('sqlver.udfMakeNumericStrict') IS NOT NULL DROP FUNCTION sqlver.udfMakeNumericStrict
GO
CREATE FUNCTION [sqlver].[udfMakeNumericStrict](
@Buf varchar(512)
)
RETURNS bigint
AS 
BEGIN
  DECLARE @Result varchar(512)

  SET @Result = ''
  
  DECLARE @i bigint 
  SET @i = 1
  WHILE @i <= LEN(@Buf + 'x') - 1 BEGIN
    IF PATINDEX('%' + SUBSTRING(@Buf, @i, 1) + '%', '01234567890') > 0 SET @Result = @Result + SUBSTRING(@Buf, @i, 1)
    SET @i = @i + 1
  END
  
  SET @Result = REPLACE(@Result, '%', '')

  IF @Result = '' SET @Result = NULL
  
  RETURN @Result
END
GO

--*** sqlver.udfLTRIMSuper
IF OBJECT_ID('sqlver.udfLTRIMSuper') IS NOT NULL DROP FUNCTION sqlver.udfLTRIMSuper
GO
CREATE FUNCTION [sqlver].[udfLTRIMSuper](@S varchar(MAX))
RETURNS varchar(MAX)
AS 
BEGIN
  DECLARE @Result varchar(MAX)
  DECLARE @P int
  SET @P = 1
  WHILE @P <= LEN(@S + 'x') - 1 BEGIN
    IF SUBSTRING(@S, @P, 1) IN  (' ', CHAR(9), CHAR(10), CHAR(13)) BEGIN
      SET @P = @P + 1
    END
    ELSE BEGIN
      BREAK
    END
  END
  
  SET @Result = RIGHT(@S, LEN(@S + 'x') - 1 - @P + 1)
  
  RETURN @Result  
END
GO

--*** udfIsInComment
IF OBJECT_ID('sqlver.udfIsInComment') IS NOT NULL DROP FUNCTION sqlver.udfIsInComment
GO
CREATE FUNCTION [sqlver].[udfIsInComment](
@CharIndex int,
@SQL nvarchar(MAX))
RETURNS BIT
WITH EXECUTE AS OWNER
AS 
BEGIN
  DECLARE @Result bit
  
  IF @CharIndex < 3 BEGIN
    SET @Result = 0
  END
  ELSE IF @CharIndex > LEN(@SQL + 'x') - 1 BEGIN
    SET @Result = NULL
  END
  ELSE BEGIN
    DECLARE @InComment bit
    DECLARE @InBlockComment bit
    
    DECLARE @P int
    DECLARE @C char(1)
    DECLARE @C2 char(1)

    SET @InComment = 0
    SET @InBlockComment = 0
    SET @P = 1
    WHILE @P <= LEN(@SQL + 'x') - 1 - 1 BEGIN
      SET @C = SUBSTRING(@SQL, @P, 1)
      SET @C2 = SUBSTRING(@SQL, @P+1, 1)
    
      IF @InBlockComment = 1 BEGIN
        IF @C + @C2 = '*/' SET @InBlockComment = 0
      END
      ELSE IF @InComment = 1 BEGIN
        IF @C IN (CHAR(13), CHAR(10)) SET @InComment = 0
      END
      ELSE IF @C + @C2 = '/*' BEGIN
        SET @InBlockComment = 1
      END
      ELSE IF @C + @C2 = '--' BEGIN
        SET @InComment = 1
      END
      
      IF @P + 2 >= @CharIndex BEGIN
        BREAK
      END
      ELSE BEGIN    
        SET @P = @P + 1  
      END
    END
    
   SET @Result = CASE WHEN ((@InComment = 1) OR (@InBlockComment = 1)) THEN 1 ELSE 0 END
    
  END
  
  RETURN @Result
END
GO

--*** udfHashBytesNMax
IF OBJECT_ID('sqlver.udfHashBytesNMax') IS NOT NULL DROP FUNCTION sqlver.udfHashBytesNMax
GO
CREATE FUNCTION [sqlver].[udfHashBytesNMax](@Algorithm sysname, @Input nvarchar(MAX))
RETURNS varbinary(MAX)
AS
BEGIN
  DECLARE @Result varbinary(MAX)

  DECLARE @Chunk int
  DECLARE @ChunkSize int
  
  SET @ChunkSize = 4000
  SET @Chunk = 1
  SET @Result = CAST('' AS varbinary(MAX))

  WHILE @Chunk * @ChunkSize < LEN(@Input + 'x') - 1 BEGIN
    --Append the hash for each chunk
    SET @Result = @Result + HASHBYTES(@Algorithm, SUBSTRING(@Input, ((@Chunk - 1) * @ChunkSize) + 1, @ChunkSize))
    SET @Chunk = @Chunk + 1
  END

  --Append the hash for the final partial chunk
  SET @Result =  HASHBYTES(@Algorithm, RIGHT(@Input, LEN(@Input + 'x') - 1 - ((@Chunk - 1) * @ChunkSize)))

  IF @Chunk > 1 BEGIN
    --If we have appended more than one hash, hash the hash.
    --We want to return just normal 160 bit (or whatever the @Algorithm calls for) value,
    --but at the moment we have any number of concatenated hash values in @Result.
    --We therefore need to hash the whole @Result buffer. 
    SET @Result = HASHBYTES(@Algorithm, @Result)    
  END
  
  RETURN @Result
END
GO

--*** udfURLEncode
IF OBJECT_ID('sqlver.udfURLEncode') IS NOT NULL DROP FUNCTION sqlver.udfURLEncode
GO
CREATE FUNCTION [sqlver].[udfURLEncode](
@Buf varchar(MAX)
)
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
/*
// Nested REPLACE statement generated with this code:

SET NOCOUNT ON

DECLARE @Decode bit
SET @Decode = 0

DECLARE @CRLF varchar(5)
SET @CRLF = CHAR(13) + CHAR(10)

DECLARE @tvChars TABLE(
CharID int IDENTITY,
NativeChar nvarchar(10),
EscSeq nvarchar(10)
)
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('%', '%25')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('+', '%2B')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (' ', '+')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (' ', '%20')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('!', '%21')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('#', '%23')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('$', '%24')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('&', '%26')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('(', '%28')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (')', '%29')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('@', '%40')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('`', '%60')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('/', '%2F')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (':', '%3A')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (';', '%3B')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('<', '%3C')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('=', '%3D')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('>', '%3E')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('?', '%3F')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('[', '%5B')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('\', '%5C')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (']', '%5D')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('^', '%5E')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('{', '%7B')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('|', '%7C')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('}', '%7D')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('~', '%7E')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('"', '%22')
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (CHAR(39), '%27') 
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (',', '%2C')

DECLARE curThis CURSOR LOCAL STATIC FOR
SELECT NativeChar, EscSeq FROM @tvChars
ORDER BY CharID

DECLARE @NativeChar nvarchar(10)
DECLARE @EscSeq nvarchar(10)
DECLARE @SQL varchar(2000)

OPEN curThis
FETCH curThis INTO @NativeChar, @EscSeq

SET @SQL = '@Buf' + @CRLF

WHILE @@FETCH_STATUS = 0 BEGIN

  SET @SQL = 'REPLACE(' + @SQL + ',''' + 
    CASE WHEN @Decode = 1 THEN @EscSeq ELSE 
      CASE WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39) ELSE @NativeChar END    
    END +
    ''', ''' + 
    CASE WHEN @Decode = 0 THEN @EscSeq ELSE 
      CASE WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39) ELSE @NativeChar END
    END + 
    ''')'  + @CRLF
  FETCH curThis INTO @NativeChar, @EscSeq
END
CLOSE curThis
DEALLOCATE curThis

PRINT @SQL
*/

RETURN
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Buf
,'%', '%25')
,'+', '%2B')
,' ', '%20')
,'!', '%21')
,'#', '%23')
,'$', '%24')
,'&', '%26')
,'(', '%28')
,')', '%29')
,'@', '%40')
,'`', '%60')
,'/', '%2F')
,':', '%3A')
,';', '%3B')
,'<', '%3C')
,'=', '%3D')
,'>', '%3E')
,'?', '%3F')
,'[', '%5B')
,'\', '%5C')
,']', '%5D')
,'^', '%5E')
,'{', '%7B')
,'|', '%7C')
,'}', '%7D')
,'~', '%7E')
,'"', '%22')
,'''', '%27')
,',', '%2C')

END
GO

--*** udfStripHTML
IF OBJECT_ID('sqlver.udfStripHTML') IS NOT NULL DROP FUNCTION sqlver.udfStripHTML
GO
CREATE FUNCTION [sqlver].[udfStripHTML](@Buf nvarchar(MAX))
RETURNS nvarchar(MAX)
AS
BEGIN
  /*
  Rudimentary function for stripping HTML tags out of a string.
  
  May fail on singletons and single tags other than <br> <hr> and </p>.
  
  Use SQLDOM for a fuller solution if needed.
  */
  
  
  DECLARE @tvStart TABLE (
    Seq int,
    Pos int
  )  
   
  DECLARE @tvEnd TABLE (
    Seq int,
    Pos int
  )    

  DECLARE @TagStart int
  DECLARE @TagEnd int  
  DECLARE @ThisTag nvarchar(254)
  DECLARE @P int


  DECLARE @EOL varchar(5)
  SET @EOL = CHAR(10)

  DECLARE @BufClean nvarchar(MAX)

  --Pre-process, to try to remove singletons
  SET @Buf =
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(     
    REPLACE(
    REPLACE(    
      @Buf
    , '<br>', @EOL)
    , '<br/>', @EOL)
    , '<br />', @EOL)    
    , '<hr>', @EOL)
    , '<hr/>', @EOL)
    , '<hr />', @EOL)    
    , '<p/>', @EOL)    
    , '<p />', @EOL)
          
  
  --Pre-process, to remove <script> blocks
  DELETE FROM @tvStart
  DELETE FROM @tvEnd
  
  IF PATINDEX('%<script%', @Buf) > 0 BEGIN

    INSERT INTO @tvStart (
      Seq,
      Pos
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
      n.Number AS Pos
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf) AND
      SUBSTRING(@Buf, n.Number, LEN('<script')) = '<script'
         

    INSERT INTO @tvEnd (
      Seq,
      Pos
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
      n.Number AS Pos
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf) AND
      SUBSTRING(@Buf, n.Number, LEN('</script>')) = '</script>'   
      
        
      
    DECLARE curThis CURSOR LOCAL STATIC FOR
    SELECT
      x.TagStart,
      x.TagEnd
    FROM (   
      SELECT 
        s1.Pos TagStart,
        e1.Pos AS TagEnd,
        ROW_NUMBER() OVER (PARTITION BY s1.Pos ORDER BY e1.Pos) AS Seq
      FROM
        @tvStart s1
        LEFT JOIN @tvStart s2 ON
          s1.Seq + 1 = s2.Seq
        LEFT JOIN @tvEnd e1 ON
          e1.Pos > s1.Pos AND
          e1.Pos < ISNULL(s2.Pos, LEN(@Buf) + 1)
      ) x    
    WHERE
      x.Seq = 1
    ORDER BY
      x.TagStart    


    SET @P = 1  
    SET @BufClean = ''      
    
    OPEN curThis    

    FETCH curThis INTO @TagStart, @TagEnd

    WHILE @@FETCH_STATUS = 0 BEGIN
      SET @ThisTag = SUBSTRING(@Buf, @TagStart + 1, @TagEnd - @TagStart - 1)

      IF @P < @TagStart BEGIN            
        SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, @TagStart - @P)      
      END
      
      SET @P = @TagEnd + LEN('</script>')

      FETCH curThis INTO @TagStart, @TagEnd  
    END

    CLOSE curThis
    DEALLOCATE curThis

    IF LEN(@Buf) > @P BEGIN
      SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, LEN(@Buf) - @P + 1)
    END
    
    SET @Buf = @BufClean

  END
     
     
  --Pre-process, to remove <-- comment blocks -->
  DELETE FROM @tvStart
  DELETE FROM @tvEnd
  
  IF PATINDEX('%<!--%', @Buf) > 0 BEGIN

    INSERT INTO @tvStart (
      Seq,
      Pos
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
      n.Number AS Pos
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf) AND
      SUBSTRING(@Buf, n.Number, LEN('<!--')) = '<!--'
         

    INSERT INTO @tvEnd (
      Seq,
      Pos
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
      n.Number AS Pos
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf) AND
      SUBSTRING(@Buf, n.Number, LEN('-->')) = '-->'   
      
        
      
    DECLARE curThis CURSOR LOCAL STATIC FOR
    SELECT
      x.TagStart,
      x.TagEnd
    FROM (   
      SELECT 
        s1.Pos TagStart,
        e1.Pos AS TagEnd,
        ROW_NUMBER() OVER (PARTITION BY s1.Pos ORDER BY e1.Pos) AS Seq
      FROM
        @tvStart s1
        LEFT JOIN @tvStart s2 ON
          s1.Seq + 1 = s2.Seq
        LEFT JOIN @tvEnd e1 ON
          e1.Pos > s1.Pos AND
          e1.Pos < ISNULL(s2.Pos, LEN(@Buf) + 1)
      ) x    
    WHERE
      x.Seq = 1
    ORDER BY
      x.TagStart    


    SET @P = 1  
    SET @BufClean = ''      
    
    OPEN curThis    

    FETCH curThis INTO @TagStart, @TagEnd

    WHILE @@FETCH_STATUS = 0 BEGIN
      SET @ThisTag = SUBSTRING(@Buf, @TagStart + 1, @TagEnd - @TagStart - 1)

      IF @P < @TagStart BEGIN            
        SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, @TagStart - @P)      
      END
      
      SET @P = @TagEnd + LEN('-->')

      FETCH curThis INTO @TagStart, @TagEnd  
    END

    CLOSE curThis
    DEALLOCATE curThis

    IF LEN(@Buf) > @P BEGIN
      SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, LEN(@Buf) - @P + 1)
    END
    
    SET @Buf = @BufClean

  END      

  --Process, to remove HTML tags
  DELETE FROM @tvStart
  DELETE FROM @tvEnd
  
  INSERT INTO @tvStart (
    Seq,
    Pos
  )
  SELECT 
    ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
    n.Number AS Pos
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf) AND
    SUBSTRING(@Buf, n.Number, 1) = '<'
       

  INSERT INTO @tvEnd (
    Seq,
    Pos
  )
  SELECT 
    ROW_NUMBER() OVER (ORDER BY n.Number) AS Seq,
    n.Number AS Pos
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf) AND
    SUBSTRING(@Buf, n.Number, 1) = '>'   
     
     
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    x.TagStart,
    x.TagEnd
  FROM (   
    SELECT 
      s1.Pos TagStart,
      e1.Pos AS TagEnd,
      ROW_NUMBER() OVER (PARTITION BY s1.Pos ORDER BY e1.Pos) AS Seq
    FROM
      @tvStart s1
      LEFT JOIN @tvStart s2 ON
        s1.Seq + 1 = s2.Seq
      LEFT JOIN @tvEnd e1 ON
        e1.Pos > s1.Pos AND
        e1.Pos < ISNULL(s2.Pos, LEN(@Buf) + 1)
    ) x    
  WHERE
    x.Seq = 1
  ORDER BY
    x.TagStart    

  SET @P = 1  
  SET @BufClean = ''
      
  OPEN curThis    

  FETCH curThis INTO @TagStart, @TagEnd

  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @ThisTag = SUBSTRING(@Buf, @TagStart + 1, @TagEnd - @TagStart - 1)

    IF @P > 1 AND @ThisTag NOT IN ('HTML', 'HEAD','TITLE', 'BODY', 'SPAN') AND LEFT(@ThisTag, 1) <> '/' BEGIN
      SET @BufClean = @BufClean + @EOL
    END
  
    IF @ThisTag IN ('LI') BEGIN
      SET @BufClean = @BufClean + '* '
    END    
    
    IF @P < @TagStart BEGIN            
      SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, @TagStart - @P)      
    END
    SET @P = @TagEnd + 1
     
    FETCH curThis INTO @TagStart, @TagEnd  
  END

  CLOSE curThis
  DEALLOCATE curThis

  IF LEN(@Buf) > @P BEGIN
    SET @BufClean = @BufClean + SUBSTRING(@Buf, @P, LEN(@Buf) - @P + 1)
  END

  --Replace HTML entities
  SET @BufClean =
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
      @BufClean 
    , '&lt;', '<')
    , '&gt;', '>')
    , '&nbsp;', ' ')  
    , '&quot;', '"')
    , '&rdquo', '"')
    , '&ldquo', '"')   
    , '&apos', '''') 
    , '&rsquo', '''')
    , '&lsquo', '''') 
    , '&amp;', '&')
 

  --Remove leading whitespace
  SET @P = 0
  
  DECLARE @Done bit
  SET @Done = 0
  
  WHILE @P < LEN(@BufClean) AND @Done = 0 BEGIN
    IF SUBSTRING(@BufClean, @P, 1) IN (CHAR(10), CHAR(13), CHAR(9), ' ')  BEGIN
      SET @P = @P + 1
    END
    ELSE BEGIN
      SET @Done = 1
    END
  END

  IF @P > 0 BEGIN
    SET @BufClean = RIGHT(@BufClean, LEN(@BufClean) - @P + 1)
  END
  
  RETURN @BufClean
END  
GO

--*** udfScriptTable
IF OBJECT_ID('sqlver.udfScriptTable') IS NOT NULL DROP FUNCTION sqlver.udfScriptTable
GO
CREATE FUNCTION [sqlver].[udfScriptTable](
@ObjectSchema sysname,
@ObjectName sysname)
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
  --Based on script contributed by Marcello - 25/09/09, in comment to article posted by 
  --Tim Chapman, TechRepublic, 2008/11/20
  --http://www.builderau.com.au/program/sqlserver/soa/Script-Table-definitions-using-TSQL/0,339028455,339293405,00.htm
  
  --Formatting altered by David Rueter (drueter@assyst.com) 2010/05/11 to match
  --script generated by MS SQL Server Management Studio 2005

	DECLARE @Id int,
	@i int,
	@i2 int,
	@Sql varchar(MAX),
	@Sql2 varchar(MAX),
	@f1 varchar(5),
	@f2 varchar(5),
	@f3 varchar(5),
	@f4 varchar(5),
	@T varchar(5)

	SELECT
	  @Id=obj.object_id,
	  @f1 = CHAR(13) + CHAR(10),
	  @f2 = '	',
	  @f3=@f1+@f2,
	  @f4=',' + @f3
	FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
  WHERE
    sch.name LIKE @ObjectSchema AND
    obj.name LIKE @ObjectName    

	IF @Id IS NULL RETURN NULL

	DECLARE @tvData table(
	  Id int identity primary key,
	  D varchar(max) not null,
	  ic int null,
	  re int null,
	  o int not null);

	-- Columns
  WITH c AS(
		SELECT
		  c.column_id,
		  Nr = ROW_NUMBER() OVER (ORDER BY c.column_id),
		  Clr=COUNT(*) OVER(),
			D = QUOTENAME(c.name) + ' ' +
				CASE 
				  WHEN s.name = 'sys' OR c.is_computed=1 THEN '' 
				  ELSE QUOTENAME(s.name) + '.' 
				END +				
				
				CASE
				  WHEN c.is_computed=1 THEN ''
				  WHEN s.name = 'sys' THEN QUOTENAME(t.Name)
				  ELSE QUOTENAME(t.name)
				END +
				
				CASE
				  WHEN ((c.user_type_id <> c.system_type_id) OR (c.is_computed=1)) THEN ''
					WHEN t.Name IN (
					  'xml', 'uniqueidentifier', 'tinyint', 'timestamp', 'time', 'text', 'sysname',
					  'sql_variant', 'smallmoney', 'smallint', 'smalldatetime', 'ntext', 'money',
					  'int', 'image', 'hierarchyid', 'geometry', 'geography', 'float', 'datetimeoffset',
					  'datetime2', 'datetime', 'date', 'bigint', 'bit') THEN ''
					WHEN t.Name in(
					  'varchar','varbinary', 'real', 'nvarchar', 'numeric', 'nchar', 'decimal', 'char', 'binary') THEN
						'(' + ISNULL(CONVERT(varchar, NULLIF(
						CASE WHEN t.Name IN ('numeric', 'decimal') THEN c.precision ELSE c.max_length END, -1)), 'MAX') + 
						ISNULL(',' + CONVERT(varchar, NULLIF(c.scale, 0)), '') + ')'
				  ELSE '??'
			  END + 
			  
				CASE 
				  WHEN ic.object_id IS NOT NULL THEN ' IDENTITY(' + CONVERT(varchar, ic.seed_value) + ',' +
				    CONVERT(varchar,ic.increment_value) + ')'
				  ELSE ''
				END +
				  
				CASE
				  WHEN c.is_computed = 1 THEN 'AS' + cc.definition 
				  WHEN c.is_nullable = 1 THEN ' NULL'
				  ELSE ' NOT NULL'
				END +

				CASE c.is_rowguidcol 
				  WHEN 1 THEN ' rowguidcol'
				  ELSE ''
				END +

				CASE 
				  WHEN d.object_id IS NOT NULL THEN  ' CONSTRAINT ' + QUOTENAME(d.name) + ' DEFAULT ' + d.definition
				  ELSE '' 
				END	

		FROM
		  sys.columns c
      INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
      INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
		  LEFT OUTER JOIN sys.computed_columns cc ON
		    cc.object_id = c.object_id AND
		    cc.column_id = c.column_id

		  LEFT OUTER JOIN sys.default_constraints d ON
		    d.parent_object_id = @id AND
		    d.parent_column_id=c.column_id

		  LEFT OUTER JOIN sys.identity_columns ic ON
		    ic.object_id = c.object_id AND
		    ic.column_id=c.column_id

		WHERE
		  c.object_id=@Id	
  )

  INSERT INTO @tvData(D, o)
  SELECT
    '	' + D + CASE Nr WHEN Clr THEN '' ELSE ',' + @f1 END,
		0
  FROM c
	ORDER by column_id
	

	-- SubObjects
	SET @i=0

	WHILE 1=1 BEGIN

		SELECT TOP 1
		  @i = c.object_id,
		  @T = c.type,
		  @i2=i.index_id
		FROM
		  sys.objects c 
		  LEFT OUTER JOIN sys.indexes i ON
		    i.object_id = @Id AND
		    i.name=c.name
    WHERE
      parent_object_id=@Id AND
      c.object_id>@i AND
      c.type NOT IN ('D', 'TR') --ignore triggers as of 1/15/2012
		ORDER BY c.object_id

		IF @@rowcount=0 BREAK

		IF @T = 'C' BEGIN
		  INSERT INTO @tvData 
			SELECT
			  @f4 + 'CHECK ' +
			    CASE is_not_for_replication 
			      WHEN 1 THEN 'NOT FOR REPLICATION '
			      ELSE ''
			    END + definition, null, null, 10
			FROM
			  sys.check_constraints 
			WHERE object_id=@i
	  END
    ELSE IF @T = 'Pk' BEGIN
		  INSERT INTO @tvData 
			SELECT
			  @f4 + 'CONSTRAINT ' + 
			  QUOTENAME('pk' + REPLACE(@ObjectName, 'tbl', '')) +
			  ' PRIMARY KEY' + ISNULL(' ' + NULLIF(UPPER(i.type_desc),'NONCLUSTERED'), ''),
			  @i2, null, 20			
			FROM sys.indexes i
			WHERE
			  i.object_id=@Id AND i.index_id=@i2
    END
    ELSE IF @T = 'uq' BEGIN
		  INSERT INTO @tvData VALUES(@f4 + 'UNIQUE', @i2, null, 30)
	  END
		ELSE IF @T = 'f' BEGIN
		  INSERT INTO @tvData 
			SELECT
			  @f4 + 'CONSTRAINT ' +  QUOTENAME(f.name) +
			  ' FOREIGN KEY ',
			  -1,
			  @i,
			  40
			FROM
			  sys.foreign_keys f        
      WHERE
        f.object_id=@i
          
      INSERT INTO @tvData 
      SELECT ' REFERENCES ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name), -2, @i, 41
			FROM
			  sys.foreign_keys f
        INNER JOIN sys.objects o ON o.object_id = f.referenced_object_id
        INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
			WHERE
			  f.object_id=@i
			
			INSERT INTO @tvData 
			SELECT ' NOT FOR REPLICATION', -3, @i, 42
			FROM
			  sys.foreign_keys f
			  INNER JOIN sys.objects o ON o.object_id = f.referenced_object_id
			  INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
			WHERE
			  f.object_id = @i AND
			  f.is_not_for_replication=1
    END
    ELSE BEGIN
			INSERT INTO @tvData
			VALUES(@f4 + 'Unknow SubObject [' + @T + ']', null, null, 99)
	  END
	END

  INSERT INTO @tvData
  VALUES(@f1+') ON ' + QUOTENAME('PRIMARY'), null, null, 100)	
  
  -- Indexes
  INSERT INTO @tvData
  SELECT
    @f1 + CHAR(13) + CHAR(10) + 'CREATE ' +
      CASE is_unique WHEN 1 THEN 'UNIQUE ' ELSE '' END +
      UPPER(s.type_desc) + ' INDEX ' + 
      s.name  + ' ON ' +
      QUOTENAME(sc.Name) + '.' + QUOTENAME(o.name),      

    index_id,
    NULL,
    1000
  FROM 
    sys.indexes s
    INNER JOIN sys.objects o ON o.object_id = s.object_id
    INNER JOIN sys.schemas sc ON sc.schema_id = o.schema_id
  WHERE
    s.object_id = @Id AND
    is_unique_constraint = 0 AND
    is_primary_key = 0 AND
    s.type_desc <> 'heap'

  -- Columns
  SET @i=0
  WHILE 1=1 BEGIN
    SELECT TOP 1 
      @i = ic
    FROM
      @tvData
    WHERE
      ic > @i
    ORDER BY ic 

    IF @@ROWCOUNT = 0 BREAK

    SELECT
      @i2=0,
      @Sql=NULL,
      @Sql2=NULL--,
      --@IxCol=NULL
  	    
    WHILE 1=1 BEGIN
	    SELECT 
	      @i2 = index_column_id, 
	            
		    @Sql = CASE c.is_included_column 
		      WHEN 1 THEN @Sql
		      ELSE ISNULL(@Sql + ', ', CHAR(13) + CHAR(10) + '(' + CHAR(13) + CHAR(10)) + '  ' + QUOTENAME(cc.Name) + 
		        CASE c.is_descending_key 
		          WHEN 1 THEN ' DESC'
		          ELSE ' ASC'
		        END
		      END,
			    
		    @Sql2 = CASE c.is_included_column 
		      WHEN 0 THEN @Sql2 
		      ELSE ISNULL(@Sql2 + ', ', CHAR(13) + CHAR(10) + '(' + CHAR(13) + CHAR(10)) + '  ' + QUOTENAME(cc.Name) --+ 
--		        CASE c.is_descending_key 
--		          WHEN 1  THEN ' DESC'
--		          ELSE ' ASC' 
--		        END
		      END

	      FROM
	        sys.index_columns c
	        INNER JOIN sys.columns cc ON
	          c.column_id = cc.column_id AND
	          cc.object_id = c.object_id
	      WHERE
	        c.object_id = @Id AND
	        index_id=@i AND
	        index_column_id > @i2
        ORDER BY
          index_column_id

		    IF @@ROWCOUNT = 0 BREAK
		              
		  END
		  
		  
      UPDATE @tvData
      SET
        D = D + @Sql + CHAR(13) + CHAR(10) + ')' +
        ISNULL(' INCLUDE' + @Sql2 + ')', '') +
        'WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON ' + QUOTENAME('PRIMARY')
        
      WHERE
        ic = @i
    END

  	

    -- References
    SET @i = 0

    WHILE 1 = 1 BEGIN
	    SELECT TOP 1 
	      @i = re
	    FROM
	      @tvData
	    WHERE
	       re > @i
	    ORDER BY re

      IF @@ROWCOUNT = 0 BREAK
  	
	    SELECT
	      @i2=0,
	      @Sql=NULL,
	      @Sql2=NULL
  	  
	    WHILE 1=1 BEGIN
		    SELECT 
		      @i2=f.constraint_column_id, 
			    @Sql = ISNULL(@Sql + ', ', '(') + QUOTENAME(c1.Name),
			    @Sql2 = ISNULL(@Sql2 + ', ', '(') + QUOTENAME(c2.Name)
		    FROM
		      sys.foreign_key_columns f
		      INNER JOIN sys.columns c1 ON
		        c1.column_id = f.parent_column_id AND
		        c1.object_id = f.parent_object_id
		      INNER JOIN sys.columns c2 ON
		        c2.column_id = f.referenced_column_id AND
		        c2.object_id = f.referenced_object_id
		    WHERE
		      f.constraint_object_id = @i AND
		      f.constraint_column_id > @i2
		    ORDER BY
		      f.constraint_column_id

		    IF @@ROWCOUNT = 0 BREAK

		  END

	    UPDATE @tvData 
	    SET
	      D = D + @Sql + ')' --close foreign key
	    WHERE
	      re = @i AND
	      ic = -1

	    UPDATE @tvData
	    SET
	      D = D + @Sql2 + ')'
	    WHERE
	      re = @i AND
	      ic = -2
	  END;

  -- Render
  WITH x AS (
	  SELECT
	    id = d.id-1,
	    D = d.D + ISNULL(d2.D, '')
	  FROM
	    @tvData d
	    LEFT OUTER JOIN @tvData d2 ON
	      d.re = d2.re AND
	      d2.o = 42
	  WHERE
	    d.o = 41		
  )

  UPDATE @tvData
  SET
    D = d.D + x.D
  FROM
    @tvData d
    INNER JOIN x ON x.id=d.id	

  DELETE FROM @tvData
  WHERE
    o IN (41, 42)
    
  SELECT
    @Sql = 'CREATE TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name) + '(' + @f1
  FROM
    sys.objects o
    INNER JOIN sys.schemas s
  ON
    o.schema_id = s.schema_id
  WHERE
    o.object_id = @Id

  SET @i = 0

  WHILE 1 = 1 BEGIN
	  SELECT TOP 1
	    @I = Id,
	    @Sql = @Sql + D 
	  FROM
	    @tvData
	  ORDER BY
	    o,
	    CASE WHEN o=0 THEN RIGHT('0000' + CONVERT(VARCHAR, id), 5)  ELSE D END,
	    id

	  IF @@ROWCOUNT = 0 BREAK

	  DELETE FROM @tvData
	  WHERE
	    id = @i

  END

	RETURN @Sql
END
GO

--*** udfRTRIMSuper
IF OBJECT_ID('sqlver.udfRTRIMSuper') IS NOT NULL DROP FUNCTION sqlver.udfRTRIMSuper
GO
CREATE FUNCTION [sqlver].[udfRTRIMSuper](@S varchar(MAX))
RETURNS varchar(MAX)
AS 
BEGIN
  DECLARE @Result varchar(MAX)
  DECLARE @P int
  SET @P = LEN(@S + 'x') - 1
  WHILE @P >= 1 BEGIN
    IF ISNULL(SUBSTRING(@S, @P, 1), ' ') IN (' ', CHAR(9), CHAR(10), CHAR(13)) BEGIN
      SET @P = @P - 1
    END
    ELSE BEGIN
      BREAK
    END
  END
  
  SET @Result = LEFT(@S, @P)
  
  RETURN @Result  
END
GO

--*** udfURLDecode
IF OBJECT_ID('sqlver.udfURLDecode') IS NOT NULL DROP FUNCTION sqlver.udfURLDecode
GO
CREATE FUNCTION [sqlver].[udfURLDecode](
@Buf varchar(MAX)
)
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
/*
// Nested REPLACE statement generated with this code:
SET NOCOUNT ON

DECLARE @Decode bit
SET @Decode = 1

DECLARE @CRLF varchar(5)
SET @CRLF = CHAR(13) + CHAR(10)

DECLARE @tvChars TABLE(
CharID int IDENTITY,
NativeChar nvarchar(10),
EscSeq nvarchar(10)
)

INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (' ', '+')

DECLARE @i int
SET @i = 1
WHILE @i < 127 BEGIN
  IF @i = 37 SET @i = @i + 1 --skip % for now
  INSERT INTO @tvChars (NativeChar, EscSeq) VALUES (
    CHAR(@i), 
    '%' + RIGHT(master.dbo.fn_varbintohexstr(@i), 2)
  )
  SET @i = @i + 1
END

--insert %
INSERT INTO @tvChars (NativeChar, EscSeq) VALUES ('%', '%25')

DECLARE curThis CURSOR LOCAL STATIC FOR
SELECT NativeChar, EscSeq FROM @tvChars
ORDER BY CharID

DECLARE @NativeChar nvarchar(10)
DECLARE @EscSeq nvarchar(10)
DECLARE @SQL varchar(MAX)

OPEN curThis
FETCH curThis INTO @NativeChar, @EscSeq

SET @SQL = '@Buf' + @CRLF

WHILE @@FETCH_STATUS = 0 BEGIN

  SET @SQL = 'REPLACE(' + @SQL + ',''' + 
    CASE WHEN @Decode = 1 THEN @EscSeq ELSE 
      CASE WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39) ELSE @NativeChar END    
    END +
    ''', ''' + 
    CASE WHEN @Decode = 0 THEN @EscSeq ELSE 
      CASE WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39) ELSE @NativeChar END
    END + 
    ''')'  + @CRLF
  FETCH curThis INTO @NativeChar, @EscSeq
END
CLOSE curThis
DEALLOCATE curThis

PRINT @SQL
*/
RETURN
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Buf
,'+', ' ')
,'%01', '')
,'%02', '')
,'%03', '')
,'%04', '')
,'%05', '')
,'%06', '')
,'%07', '')
,'%08', '')
,'%09', '	')
,'%0a', '
')
,'%0b', '')
,'%0c', '')
,'%0d', '')
,'%0e', '')
,'%0f', '')
,'%10', '')
,'%11', '')
,'%12', '')
,'%13', '')
,'%14', '')
,'%15', '')
,'%16', '')
,'%17', '')
,'%18', '')
,'%19', '')
,'%1a', '')
,'%1b', '')
,'%1c', '')
,'%1d', '')
,'%1e', '')
,'%1f', '')
,'%20', ' ')
,'%21', '!')
,'%22', '"')
,'%23', '#')
,'%24', '$')
,'%26', '&')
,'%27', '''')
,'%28', '(')
,'%29', ')')
,'%2a', '*')
,'%2b', '+')
,'%2c', ',')
,'%2d', '-')
,'%2e', '.')
,'%2f', '/')
,'%30', '0')
,'%31', '1')
,'%32', '2')
,'%33', '3')
,'%34', '4')
,'%35', '5')
,'%36', '6')
,'%37', '7')
,'%38', '8')
,'%39', '9')
,'%3a', ':')
,'%3b', ';')
,'%3c', '<')
,'%3d', '=')
,'%3e', '>')
,'%3f', '?')
,'%40', '@')
,'%41', 'A')
,'%42', 'B')
,'%43', 'C')
,'%44', 'D')
,'%45', 'E')
,'%46', 'F')
,'%47', 'G')
,'%48', 'H')
,'%49', 'I')
,'%4a', 'J')
,'%4b', 'K')
,'%4c', 'L')
,'%4d', 'M')
,'%4e', 'N')
,'%4f', 'O')
,'%50', 'P')
,'%51', 'Q')
,'%52', 'R')
,'%53', 'S')
,'%54', 'T')
,'%55', 'U')
,'%56', 'V')
,'%57', 'W')
,'%58', 'X')
,'%59', 'Y')
,'%5a', 'Z')
,'%5b', '[')
,'%5c', '\')
,'%5d', ']')
,'%5e', '^')
,'%5f', '_')
,'%60', '`')
,'%61', 'a')
,'%62', 'b')
,'%63', 'c')
,'%64', 'd')
,'%65', 'e')
,'%66', 'f')
,'%67', 'g')
,'%68', 'h')
,'%69', 'i')
,'%6a', 'j')
,'%6b', 'k')
,'%6c', 'l')
,'%6d', 'm')
,'%6e', 'n')
,'%6f', 'o')
,'%70', 'p')
,'%71', 'q')
,'%72', 'r')
,'%73', 's')
,'%74', 't')
,'%75', 'u')
,'%76', 'v')
,'%77', 'w')
,'%78', 'x')
,'%79', 'y')
,'%7a', 'z')
,'%7b', '{')
,'%7c', '|')
,'%7d', '}')
,'%7e', '~')
,'%25', '%')

END
GO

--*** udftGetParsedValues
IF OBJECT_ID('sqlver.udftGetParsedValues') IS NOT NULL DROP FUNCTION sqlver.udftGetParsedValues
GO
CREATE FUNCTION [sqlver].[udftGetParsedValues](
  @InputString nvarchar(MAX),
  @Delimiter nchar(1)
)
RETURNS @tvValues TABLE (
  [Value] nvarchar(MAX),
  [Index] int)
WITH EXECUTE AS OWNER
AS 
BEGIN
  IF @Delimiter <> ' ' BEGIN
    SET @Delimiter = NULLIF(@Delimiter, '')
  END
  
  --Remove trailng delimiters
  WHILE RIGHT(@InputString,1) = @Delimiter BEGIN
    SET @InputString = LEFT(@InputString, LEN(@InputString + 'x') - 1 - 1)
  END

  INSERT INTO @tvValues ([Value], [Index])
  SELECT SUBSTRING( @Delimiter + @InputString + @Delimiter, N.Number + 1, 
         CHARINDEX( @Delimiter, @Delimiter + @InputString + @Delimiter, N.Number + 1 ) - N.Number - 1 ),
    ROW_NUMBER() OVER (ORDER BY N.Number)
  FROM sqlver.tblNumbers N
  WHERE
    SUBSTRING( @Delimiter + @InputString + @Delimiter, N.Number, 1 ) = @Delimiter AND
    N.Number < (LEN( @Delimiter + @InputString + @Delimiter + 'x' ) - 1)
  RETURN
END
GO


--*** sqlver.spsysBackupFull
IF OBJECT_ID('sqlver.spsysBackupFull') IS NOT NULL DROP PROCEDURE sqlver.spsysBackupFull
GO
CREATE PROCEDURE [sqlver].[spsysBackupFull]
@PerformCheck bit = 1,  --Performs DBCC CHECKDB
@PerformMaint bit = 1,  --Rebuilds all indexes and statistics
@PerformBU bit = 1,     --Performs actual full backup
@BUPath nvarchar(1024) = 'S:\SQLBackups\Full\',  --Path on the server to store the backup
@FullFileName nvarchar(512) = NULL OUTPUT  --Returns the actual filename
--$!ParseMarker
--Note:  comments and code between marker and AS are subject to automatic removal by OpsStream
--©Copyright 2006-2010 by David Rueter, Automated Operations, Inc.
--May be held, used or transmitted only pursuant to an in-force licensing agreement with Automated Operations, Inc.
--Contact info@opsstream.com / 800-964-3646 / 949-264-1555
WITH EXECUTE AS CALLER
AS 
BEGIN
  DECLARE @Debug bit
  SET @Debug = 1
  
  DECLARE @Msg varchar(MAX)
  
  DECLARE @ThreadGUID uniqueidentifier
  SET @ThreadGUID = NEWID()
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spsysBackupFull: Starting'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END  

  DECLARE @BUFileName varchar(1024)

  DECLARE @DBName sysname
  SET @DBName = DB_NAME()
  
  DECLARE @SQL nvarchar(MAX)

  BEGIN TRY
    IF (@PerformBU = 1) BEGIN
      
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spsysBackupFull: Creating folder for Full backup of ' + @DBName
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
        PRINT @Msg
      END
      
      DECLARE @NewPath varchar(1024)
      SET @NewPath = @BUPath + @DBName 
      EXEC sqlver.spsysCreateSubDir @NewPath = @NewPath
         
         
      IF (@PerformCheck = 1) BEGIN
        IF @Debug = 1 BEGIN
          SET @Msg = 'sqlver.spsysBackupFull: Executing DBCC CHECKDB ON ' + @DBName         
          EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
          PRINT @Msg
        END         
    
        SET @SQL = 'USE [' + @DBName + ']';
        EXEC(@SQL)
        SET @SQL = 'DBCC CHECKDB WITH NO_INFOMSGS'
        EXEC(@SQL)
      END
      
          
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spsysBackupFull: Performing full backup of database ' + @DBName         
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
        PRINT @Msg
      END      
      

      SET @BUFileName = @DBName + '_' + CONVERT(varchar(100), GETDATE(), 112) + 'W'      
      SET @FullFileName = @BUPath + @DBName + '\' + @BUFileName  + '.bak'  
      
   
      SET @SQL = 'BACKUP DATABASE [' + @DBName + '] TO  DISK = N''' + @FullFileName + '''' +
        ' WITH NOFORMAT, NOINIT,  NAME = N''' + @BUFileName + ''', SKIP, REWIND, NOUNLOAD,  STATS = 10'      
      
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spsysBackupFull: ' + @SQL               
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
        PRINT @Msg
      END         
   
      EXEC(@SQL)  
      
    END
    IF (@PerformMaint = 1) BEGIN
      DECLARE @ObjectName varchar(1024)
      DECLARE @IndexName sysname
      DECLARE @ObjectType sysname
      
      SET @SQL = 'USE [' + @DBName + ']';
      EXEC(@SQL)        

      DECLARE curReindex CURSOR LOCAL STATIC FOR
      SELECT
        '[' + sch.name + '].[' + so.name + ']' AS ObjectName,
        '[' + si.name + ']' AS IndexName
      FROM
        sys.indexes si
        JOIN sys.objects so ON
          si.object_id = so.object_id
        JOIN sys.schemas sch ON
          so.schema_id = sch.schema_id
      WHERE
        so.Type IN ('U', 'V') AND
        si.type_desc <> 'HEAP'  
      ORDER BY
        sch.name,
        so.name    
        

      OPEN curReindex
      FETCH curReindex INTO @ObjectName, @IndexName
      WHILE @@FETCH_STATUS = 0 BEGIN
        SET @SQL = 'ALTER INDEX ' + @IndexName + ' ON ' + @ObjectName + ' REBUILD WITH ( FILLFACTOR = 80, PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, SORT_IN_TEMPDB = OFF, ONLINE = OFF )' 
          
       IF @Debug = 1 BEGIN
          SET @Msg = 'sqlver.spsysBackupFull: ' + @SQL               
          EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
          PRINT @Msg
        END           

        BEGIN TRY
          EXEC(@SQL)
        END TRY
        BEGIN CATCH
          SET @Msg = ERROR_MESSAGE()
          
        END CATCH
        FETCH curReindex INTO @ObjectName, @IndexName  
      END
      CLOSE curReindex
      DEALLOCATE curReindex

      DECLARE curUpdStat CURSOR LOCAL STATIC FOR
      SELECT 
        '[' + st.TABLE_SCHEMA+ '].[' + st.TABLE_NAME + ']' AS ObjectName,
        st.TABLE_TYPE AS ObjectType
      FROM 
        INFORMATION_SCHEMA.tables st
      ORDER BY ObjectName

      OPEN curUpdStat
      FETCH curUpdStat INTO @ObjectName, @ObjectType
      WHILE @@FETCH_STATUS = 0 BEGIN
        SET @SQL = 'UPDATE STATISTICS ' + @ObjectName + ' WITH FULLSCAN, ALL'
          --+ CASE WHEN @ObjectType = 'VIEW' THEN ', NORECOMPUTE' ELSE '' END
          
          
        IF @Debug = 1 BEGIN
          SET @Msg = 'sqlver.spsysBackupFull: ' + @SQL               
          EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
          PRINT @Msg
        END   
                
        BEGIN TRY
          EXEC(@SQL)
          
          IF @ObjectName = 'VIEW' BEGIN
            SET @SQL =  'EXEC sp_autostats ' + @ObjectName
            EXEC(@SQL)
          END        
        END TRY
        BEGIN CATCH
          SET @Msg = ERROR_MESSAGE()          
        END CATCH
        FETCH curUpdStat INTO @ObjectName, @ObjectType
      END
      CLOSE curUpdStat
      DEALLOCATE curUpdStat    
    END
  END TRY 
  BEGIN CATCH
    SET @Msg = 'sqlver.spsysBackupFull: Error: ' + ERROR_MESSAGE()
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID    
    PRINT @Msg
  END CATCH  
    
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spsysBackupFull: Finished'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END
      
END
GO

--*** udfParseValueReplace
IF OBJECT_ID('sqlver.udfParseValueReplace') IS NOT NULL DROP FUNCTION sqlver.udfParseValueReplace
GO
CREATE FUNCTION [sqlver].[udfParseValueReplace](
  @InputString varchar(MAX),
  @Delimiter char(1),
  @Index int,
  @NewValue varchar(MAX)
)
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
  DECLARE @Result varchar(MAX)

  DECLARE @tvValues TABLE (
  [Value] varchar(MAX),
  [Index] int)

  --Remove trailng delimiters
  WHILE RIGHT(@InputString,1) = @Delimiter BEGIN
    SET @InputString = LEFT(@InputString, LEN(@InputString + 'x') - 1 - 1)
  END

  INSERT INTO @tvValues ([Value], [Index])
  SELECT SUBSTRING( @Delimiter + @InputString + @Delimiter, N.Number + 1, 
         CHARINDEX( @Delimiter, @Delimiter + @InputString + @Delimiter, N.Number + 1 ) - N.Number - 1 ),
    ROW_NUMBER() OVER (ORDER BY N.Number)
  FROM sqlver.tblNumbers N
  WHERE
    SUBSTRING( @Delimiter + @InputString + @Delimiter, N.Number, 1 ) = @Delimiter AND
    N.Number < Len( @Delimiter + @InputString + @Delimiter + 'x' ) - 1

  UPDATE @tvValues SET [Value] = @NewValue WHERE [Index] = @Index

  DECLARE curThis CURSOR STATIC LOCAL FOR
  SELECT [Value] FROM @tvValues ORDER BY [Index]

  DECLARE @ThisValue varchar(MAX)

  SET @Result = ''

  OPEN curThis
  FETCH curThis INTO @ThisValue

  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @Result = @Result + @ThisValue + @Delimiter
    FETCH curThis INTO @ThisValue    
  END
  CLOSE curThis
  DEALLOCATE curThis

  IF LEN(@Result + 'x') - 1 > 0 BEGIN       
    SET @Result = LEFT(@Result, LEN(@Result + 'x') - 1 - 1)  
  END

  RETURN @Result

END
GO

--*** spShowRTLog
IF OBJECT_ID('sqlver.spShowRTLog') IS NOT NULL DROP PROCEDURE sqlver.spShowRTLog
GO
CREATE PROCEDURE [sqlver].[spShowRTLog]
@MsgLike varchar(MAX) = NULL
AS
BEGIN
  SET NOCOUNT ON
  
  SELECT TOP 5000
    rt.*
  FROM 
    sqlver.tblSysRTLog (NOLOCK) rt
  WHERE
    @MsgLike IS NULL OR
    rt.Msg LIKE @MsgLike + '%'
  ORDER BY
    rt.SysRTLogID DESC  
END
GO

--*** spgetSSRSDatasets
IF OBJECT_ID('sqlver.spgetSSRSDatasets') IS NOT NULL DROP PROCEDURE sqlver.spgetSSRSDatasets
GO
CREATE PROCEDURE [sqlver].[spgetSSRSDatasets]
AS
BEGIN
  --From:  http://bretstateham.com/extracting-ssrs-report-rdl-xml-from-the-reportserver-database/


  --The first CTE gets the content as a varbinary(max)
  --as well as the other important columns for all reports,
  --data sources and shared datasets.
  WITH ItemContentBinaries AS
    (
      SELECT
         ItemID,
         Name,
        [Type],
        CASE Type
           WHEN 2 THEN 'Report'
           WHEN 5 THEN 'Data Source'
           WHEN 7 THEN 'Report Part'
           WHEN 8 THEN 'Shared Dataset'
           ELSE 'Other'
         END AS TypeDescription,
        CONVERT(varbinary(max),Content) AS Content
      FROM
        ReportServer.dbo.Catalog
      WHERE Type IN (2,5,7,8)
    ),
  
    --The second CTE strips off the BOM if it exists...
    ItemContentNoBOM AS
    (
      SELECT
         ItemID,
         Name,
         [Type],
         TypeDescription,
        CASE
           WHEN LEFT(Content,3) = 0xEFBBBF
             THEN CONVERT(varbinary(max),SUBSTRING(Content,4,LEN(Content)))
           ELSE
             Content
         END AS Content
      FROM
        ItemContentBinaries
    ),
  
  --The old outer query is now a CTE to get the content in its xml form only...
    ItemContentXML AS
    (
      SELECT
         ItemID,
         Name,[Type],TypeDescription,
         CONVERT(xml,Content) AS ContentXML
     FROM
       ItemContentNoBOM
    )
    
    
  --now use the XML data type to extract the queries, and their command types and text....
  SELECT
       --ixml.ItemID,
       ixml.Name,
       --[Type],
       ixml.TypeDescription,
       --ContentXML,
      ISNULL(Query.value('(./*:CommandType/text())[1]','nvarchar(1024)'),'Query') AS CommandType,
      Query.value('(./*:DataSourceName/text())[1]','nvarchar(max)') AS DataSource,      
      Query.value('(./*:CommandText/text())[1]','nvarchar(max)') AS CommandText,
      xl.LastRan--,
      --ixml.ContentXML
  FROM
    ItemContentXML ixml
    
    --Get all the Query elements (The "*:" ignores any xml namespaces)
    CROSS APPLY ixml.ContentXML.nodes('//*:Query') Queries(Query)
    
    --Get most recent start time
    LEFT JOIN (
      SELECT
        xl.ReportID,
        MAX(xl.TimeStart) AS LastRan
      FROM
        ReportServer.dbo.ExecutionLog xl
      GROUP BY
        xl.ReportID) xl ON
      ixml.ItemId = xl.ReportId
  ORDER BY
    CASE WHEN LastRan IS NOT NULL THEN 1 ELSE 2 END,
    Name,
    CommandText

END
GO

--*** spgetSQLSpaceUsed
IF OBJECT_ID('sqlver.spgetSQLSpaceUsed') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLSpaceUsed
GO
CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsed]
AS 
BEGIN
  SET NOCOUNT ON

  DECLARE @Q char
  SET @Q = char(39)

  IF OBJECT_ID('tempdb..#SpaceUsed') IS NOT NULL BEGIN
    DROP TABLE #SpaceUsed
  END

  CREATE TABLE #SpaceUsed (
    schemaname sysname NULL,
    name sysname,
    rows bigint,
    reserved varchar(20),
    data varchar(20),
    index_size varchar(20),
    unused varchar(20),
    reserved_bytes bigint,
    data_bytes bigint,
    index_bytes bigint,
    unused_bytes bigint
  )

  DECLARE @SQL varchar(MAX)
  DECLARE @SchemaName sysname

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT 'EXEC sp_spaceused ' +  @Q + sch.name + '.' + so.name + @Q, sch.name AS SchemaName
  FROM
    sys.objects so
    JOIN sys.schemas sch ON
      so.schema_id = sch.schema_id
  WHERE
    so.type_desc IN ('SERVICE_QUEUE', 'USER_TABLE') OR
    ((so.type_desc = 'VIEW') AND
     EXISTS (SELECT object_id FROM sys.indexes where object_id = so.object_id))

  OPEN curThis
  FETCH curThis INTO @SQL, @SchemaName

  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @SQL = 'INSERT INTO #SpaceUsed (name, rows, reserved, data, index_size, unused) ' + @SQL
    EXEC(@SQL)
    
    UPDATE #SpaceUsed
    SET
      schemaname = @SchemaName
    WHERE
      schemaname IS NULL
    FETCH curThis INTO @SQL, @SchemaName
  END


  UPDATE #SpaceUsed
  SET
    reserved_bytes = sqlver.udfMakeNumericStrict(Reserved) * 
      CASE 
        WHEN PATINDEX('%KB%', reserved) > 0 THEN 1024
        WHEN PATINDEX('%MB%', reserved) > 0 THEN 1024 * 1024
        WHEN PATINDEX('%GB%', reserved) > 0 THEN 1024 * 1024 * 1024
        ELSE 1
      END,
          
    data_bytes = sqlver.udfMakeNumericStrict(data) * 
      CASE 
        WHEN PATINDEX('%KB%', data) > 0 THEN 1024
        WHEN PATINDEX('%MB%', data) > 0 THEN 1024 * 1024
        WHEN PATINDEX('%GB%', data) > 0 THEN 1024 * 1024 * 1024
        ELSE 1
      END,
      
    index_bytes = sqlver.udfMakeNumericStrict(index_size) * 
      CASE 
        WHEN PATINDEX('%KB%', index_size) > 0 THEN 1024
        WHEN PATINDEX('%MB%', index_size) > 0 THEN 1024 * 1024
        WHEN PATINDEX('%GB%', index_size) > 0 THEN 1024 * 1024 * 1024
        ELSE 1
      END,
      
    unused_bytes = sqlver.udfMakeNumericStrict(unused) * 
      CASE 
        WHEN PATINDEX('%KB%', unused) > 0 THEN 1024
        WHEN PATINDEX('%MB%', unused) > 0 THEN 1024 * 1024
        WHEN PATINDEX('%GB%', unused) > 0 THEN 1024 * 1024 * 1024
        ELSE 1
      END                             
         
         

  SELECT * FROM #SpaceUsed
  ORDER BY reserved_bytes DESC

  DROP TABLE #SpaceUsed
END
GO

--*** spsysBuildCLRAssemblyInfo
IF OBJECT_ID('sqlver.spsysBuildCLRAssemblyInfo') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLRAssemblyInfo
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLRAssemblyInfo]
@PerformDropAll bit = 0
AS 
BEGIN
  SELECT
    a.name AS AssemblyName,
    sch.name + '.' + obj.name AS ObjectName,
    
    'DROP ' + 
    CASE obj.type
      WHEN 'FS' THEN 'FUNCTION'
      WHEN 'FT' THEN 'FUNCTION'
      WHEN 'PC' THEN 'PROCEDURE'
    END + ' ' + 
    sch.name + '.' + obj.name AS DropStatement
      
  FROM
    sys.assembly_modules m
    JOIN sys.assemblies a oN
      m.assembly_id = a.assembly_id
    JOIN sys.objects obj ON
      m.object_id = obj.object_id
    JOIN sys.schemas sch ON 
      obj.schema_id = sch.schema_id
     
      
  ;
  WITH cte AS (      
    --assemblies with nothing dependding on them
    SELECT
      1 AS Level,
      a.name AS AssemblyName,
      a.is_visible,
      a.create_date,
      a.modify_date,
      a.assembly_id,
      ar.referenced_assembly_id,
      a.is_user_defined,
      CAST(NULL AS varchar(MAX)) AS DependsOn,
      'DROP ASSEMBLY [' + a.name + ']' AS DeleteStatement
    FROM
      sys.assemblies a
      LEFT JOIN sys.assembly_references ar ON
        a.assembly_id = ar.referenced_assembly_id
    WHERE
      ar.assembly_id IS NULL
       
    UNION ALL
         
    SELECT
      cte.Level + 1, 
      cte.AssemblyName,
      a2.is_visible,
      a2.create_date,
      a2.modify_date,
      a2.assembly_id,
      ar.referenced_assembly_id,
      a2.is_user_defined,  
      CAST(ISNULL(cte.DependsOn + ';', '') + a2.Name AS varchar(MAX)),
      'DROP ASSEMBLY [' + cte.AssemblyName + ']' AS DeleteStatement
    FROM
      cte
      JOIN sys.assembly_references ar ON
        cte.assembly_id = ar.assembly_id
      JOIN sys.assemblies a2 ON 
        ar.referenced_assembly_id = a2.assembly_id
    WHERE
      PATINDEX('%' + a2.Name + '%', ISNULL(cte.DependsOn, '')) = 0        
  )

  SELECT
    x.AssemblyName,
    x.DependsOn,
    x.DeleteStatement
  FROM
    (
    SELECT
      cte.AssemblyName,
      cte.DependsOn,
      cte.Level,
      cte.is_user_defined,
      cte.DeleteStatement,
      ROW_NUMBER() OVER (PARTITION BY cte.AssemblyName ORDER BY cte.Level DESC, LEN(DependsOn) DESC) AS Seq
    FROM
      cte cte
    ) x
  WHERE
    x.is_user_defined = 1 AND
    x.Seq = 1
  ORDER BY
    x.AssemblyName  
    
    
  IF @PerformDropAll = 1 BEGIN
    DECLARE @SQL varchar(MAX)
    DECLARE @SQL2 varchar(MAX)
    
    
    SET @SQL =
      'SELECT     
        ''DROP '' + 
        CASE obj.type
          WHEN ''FS'' THEN ''FUNCTION''
          WHEN ''FT'' THEN ''FUNCTION''
          WHEN ''PC'' THEN ''PROCEDURE''
        END + '' '' + 
        sch.name + ''.'' + obj.name AS DropStatement
          
      FROM
        sys.assembly_modules m
        JOIN sys.assemblies a oN
          m.assembly_id = a.assembly_id
        JOIN sys.objects obj ON
          m.object_id = obj.object_id
        JOIN sys.schemas sch ON 
          obj.schema_id = sch.schema_id
      '
    EXEC sqlver.sputilResultSetAsStr @SQL = @SQL, @IncludeLineBreaks = 1, @Result = @SQL2 OUTPUT

    PRINT @SQL2
    EXEC (@SQL2)
    
    ----------------------


    DECLARE @ThisDeleteStatement varchar(MAX)

    WHILE EXISTS (SELECT a.name FROM sys.assemblies a WHERE a.is_user_defined = 1) BEGIN
          
      SET @SQL = '     
      ;
      WITH cte AS (      
        --assemblies with nothing dependding on them
        SELECT
          1 AS Level,
          a.name AS AssemblyName,     
          a.assembly_id,
          ar.referenced_assembly_id,
          a.is_user_defined,
          CAST(NULL AS varchar(MAX)) AS DependsOn,
          ''DROP ASSEMBLY ['' + a.name + '']'' AS DeleteStatement
        FROM
          sys.assemblies a
          LEFT JOIN sys.assembly_references ar ON
            a.assembly_id = ar.referenced_assembly_id
        WHERE
          ar.assembly_id IS NULL
           
        UNION ALL
             
        SELECT
          cte.Level + 1, 
          cte.AssemblyName,
          a2.assembly_id,
          ar.referenced_assembly_id,
          a2.is_user_defined,  
          CAST(ISNULL(cte.DependsOn + '';'', '''') + a2.Name AS varchar(MAX)),
          ''DROP ASSEMBLY ['' + cte.AssemblyName + '']'' AS DeleteStatement
        FROM
          cte
          JOIN sys.assembly_references ar ON
            cte.assembly_id = ar.assembly_id
          JOIN sys.assemblies a2 ON 
            ar.referenced_assembly_id = a2.assembly_id
        WHERE
          PATINDEX(''%'' + a2.Name + ''%'', ISNULL(cte.DependsOn, '''')) = 0        
      )

      SELECT
        x.DeleteStatement
      FROM
        (
        SELECT
          cte.AssemblyName,
          cte.DependsOn,
          cte.Level,
          cte.is_user_defined,
          cte.DeleteStatement,
          ROW_NUMBER() OVER (PARTITION BY cte.AssemblyName ORDER BY cte.Level DESC, LEN(DependsOn) DESC) AS Seq
        FROM
          cte cte
        ) x
      WHERE
        x.is_user_defined = 1 AND
        x.Seq = 1
      ORDER BY
        x.Level,
        x.AssemblyName
      '
    
      
      EXEC sqlver.sputilResultSetAsStr @SQL = @SQL, @IncludeLineBreaks = 1, @Result = @SQL2 OUTPUT

      PRINT @SQL2
      EXEC (@SQL2)
    END       
                        
  END    
END
GO

--*** spsysBuildCLRAssemblyCache
IF OBJECT_ID('sqlver.spsysBuildCLRAssemblyCache') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLRAssemblyCache
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLRAssemblyCache]
@TargetPath nvarchar(1024) = 'C:\Temp\AssemblyCache\',
@AssemblyPath nvarchar(1024) = 'C:\Temp\AssemblyLibrary\'
AS 
BEGIN
  SET NOCOUNT ON

  /*
  Often SQL CLR assemblies will depend upon other assemblies (i.e. "using XXXX" references
  in C#).
  
  We need to load all dependency assemblies into SQL when we load such an assembly.
  
  We could explicitly load each assembly specified in the "using XXXX" statements in C#,
  but this is not ideal:  1) this does not include dependencies of those dependencies 
  (i.e. grandchildren, great-grandchildren, etc.), 2) this requires manual maintenances,
  and 3) this will make each of the dependency assemblies "visible" in SQL CLR (meaning
  that functions and procedures could use them--which is not ideal from a security and
  surface area standpoint.
  
  Instead, .NET (SQL CLR) can figure out the dependencies for us automatically.  Except,
  SQL needs to have access to the dependency assembly .DLL files in order for this to work.
  
  For Windows .NET assemblies (i.e. not SQL CLR), .NET can use the GAC (Global Assembly
  Cache) in Windows to find the needed .DLL files (assuming they are registed in the GAC).
  
  For SQL CLR .NET assemblies, .NET will NOT look to the Windows GAC, but will instead
  look to SQL's internal GAC.  SQL's GAC contains a much smaller set of assemblies, as
  only certain assemblies are supported by Microsoft for use in SQL.
  
  You can use automatic dependency loading with unsupported assemblies by copying the
  needed dependency .DLL files to the folder that contains the primary assembly you
  are trying to load.
  
  However, locating the right .DLL file is a little challenging, because there are
  many versions of each .DLL file in the C:\Windows folder.  (Separate .DLL files for
  32 vs. 64-bit, for each version of the .NET Framework (1, 2, 3.5, 4, etc.), plus 
  multiple copies for Windows SxS, plus...still other copies.)  Without the correct
  required .DLL with the exact version, with the exact signature, your assembly will
  not load.
  
  This procedure (sqlver.spSysBuildCLRAssemblies) takes the approach of copying
  ALL of the Windows GAC .DLLs to a single cache folder that is used for loading the
  assemblies into the database.  Once the CREATE ASSEMBLY work is done, the cache
  folder can be deleted.
  
  Execute this procedure before executing the sqlver.spsysBuildCLR_xxx procedure
  (if the assembly being built has dependences that you want to satisfy automatically).   
  */


  IF OBJECT_ID('tempdb..#SourcePaths') IS NOT NULL BEGIN
    DROP TABLE #SourcePaths
  END
  
  CREATE TABLE #SourcePaths (
    Seq int PRIMARY KEY,
    FQPath nvarchar(MAX)
  )
  
  INSERT INTO #SourcePaths (
    Seq,
    FQPath
  )      
  VALUES
    (1, 'C:\Windows\assembly\GAC_MSIL\'),
    (2, 'C:\Windows\assembly\GAC_64\'),
    (3, @AssemblyPath)    
    
  --  (1, 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\'),
  --  (2, 'C:\Windows\Microsoft.NET\Framework64\v2.0.50727\'),
  --  (3, 'C:\Windows\Microsoft.NET\Framework64\v3.0\WPF\'),
  --  (4, @AssemblyPath)    
  

  IF OBJECT_ID('tempdb..#ToCopy') IS NOT NULL BEGIN
    DROP TABLE #ToCopy
  END


  CREATE TABLE #ToCopy (
    Seq int,
    FQFilename nvarchar(MAX)
  )  

  IF OBJECT_ID('tempdb..#FileList') IS NOT NULL BEGIN
    DROP TABLE #FileList
  END

  CREATE TABLE #FileList (     
    FileID int PRIMARY KEY,      
    FQFilename varchar(MAX),
    Filename varchar(MAX),
    Path varchar(MAX),
    IsFile bit,
    Depth int,
    ParentFileID int          
  )

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    spth.Seq,
    spth.FQPath
  FROM
    #SourcePaths spth
  ORDER BY
    spth.Seq

  DECLARE @ThisSeq int
  DECLARE @ThisPath nvarchar(1024)
  
  OPEN curThis
  FETCH curThis INTO @ThisSeq, @ThisPath
  WHILE @@FETCH_STATUS = 0 BEGIN
  

    EXEC sqlver.sputilGetFileList @Path = @ThisPath, @SuppressResultset = 1
    
    INSERT INTO #ToCopy (
      Seq,
      FQFilename
    )
    SELECT
      @ThisSeq,
      fl.FQFilename
    FROM
      #FileList fl
    WHERE
      PATINDEX('%.dll', fl.FQFilename) > 0       
      
    TRUNCATE TABLE #FileList
        
    FETCH curThis INTO @ThisSeq, @ThisPath  
  END
  CLOSE curThis
  DEALLOCATE curThis      
         
  DECLARE @SQL varchar(MAX)
  
  SET @SQL = 'SELECT tc.fqFilename FROM #ToCopy tc'

  DECLARE @ResultPrefix nvarchar(1024)
  SET @ResultPrefix = 'COPY "'
  
  DECLARE @ResultSuffix nvarchar(1024)
  SET @ResultSuffix = '" ' + @TargetPath
  
  DECLARE @CopyScript nvarchar(MAX)
  
  EXEC sqlver.sputilResultsetAsStr @SQL = @SQL, @ResultPrefix = @ResultPrefix, @ResultSuffix = @ResultSuffix, @TrimTrailSuffix = 0, @IncludeLineBreaks = 1, @Result = @CopyScript OUTPUT
  

  EXEC sqlver.sputilWriteStringToFile 
    @FileData = @CopyScript,
    @FilePath = @TargetPath,
    @FileName = 'CopySystemDLLs.bat'    
   
   
    --Temporarily enable xp_cmdshell support so we can build the source code
    DECLARE @OrigSupport_XPCmdShell bit
    SELECT @OrigSupport_XPCmdShell = CONVERT(bit, value) FROM sys.configurations WHERE name = 'xp_cmdshell'
    IF ISNULL(@OrigSupport_XPCmdShell, 0) = 0 BEGIN
      --IF @Debug = 1 PRINT '***Temporarily enabling xp_cmdshell support'  
      SET @SQL = '
        EXEC master.dbo.sp_configure ''show advanced options'', 1;
        RECONFIGURE;
        EXEC master.dbo.sp_configure ''xp_cmdshell'', 1;
        RECONFIGURE;'
      EXEC(@SQL)
    END
    
    --Execute the temporary batch file
    --IF @Debug = 1 PRINT '***Executing batch file to build C# source into .dll'
    SET @SQL = 'EXEC xp_cmdshell "' + @TargetPath + 'CopySystemDLLs.bat' + '";'
    EXEC(@SQL)      
    
    --Clean up by deleting temporary files
    --IF @Debug = 1 PRINT '***Deleting batch file'
    SET @SQL = 'EXEC xp_cmdshell "del ' + @TargetPath + 'CopySystemDLLs.bat' + '";'
    EXEC(@SQL)
    
    --Disable xp_cmdshell support
    IF ISNULL(@OrigSupport_XPCmdShell, 0) = 0  BEGIN
      --IF @Debug = 1 PRINT '***Disabling xp_cmdshell support'
      SET @SQL = '
        EXEC master.dbo.sp_configure ''show advanced options'', 1;
        RECONFIGURE;
        EXEC master.dbo.sp_configure ''xp_cmdshell'', 0;
        RECONFIGURE;'  
      EXEC(@SQL)
    END           

                
END
GO

--*** spBuildManifest
IF OBJECT_ID('sqlver.spBuildManifest') IS NOT NULL DROP PROCEDURE sqlver.spBuildManifest
GO
CREATE PROCEDURE [sqlver].[spBuildManifest]
AS
BEGIN
  SET NOCOUNT ON 
  
  INSERT INTO sqlver.tblSchemaManifest(  
    ObjectName,
    SchemaName,
    DatabaseName,  
    OrigDefinition,
    DateAppeared,
    CreatedByLoginName,
    DateUpdated,
    OrigHash,
    CurrentHash,
    IsEncrypted,
    StillExists,
    SkipLogging,
    Comments
  )
  SELECT
    so.name,
    sch.name,
    DB_NAME(),
    CASE 
      WHEN so.type_desc = 'USER_TABLE' THEN sqlver.udfScriptTable(sch.name, so.name)
      ELSE OBJECT_DEFINITION(so.object_id)
    END AS OrigDefinition,
    so.create_date,
    'Before SQLVer',
    so.modify_date,
    sqlver.udfHashBytesNMax('SHA1',
      CASE 
        WHEN so.type_desc = 'USER_TABLE' THEN sqlver.udfScriptTable(sch.name, so.name)
        ELSE OBJECT_DEFINITION(so.object_id)
      END
    ) AS OrigHash,
    
    sqlver.udfHashBytesNMax('SHA1',
      CASE 
        WHEN so.type_desc = 'USER_TABLE' THEN sqlver.udfScriptTable(sch.name, so.name)
        ELSE OBJECT_DEFINITION(so.object_id)
      END
    ) AS CurrentHash,    
        
    0 AS IsEnrypted,
    1 AS StillExists,
    0 AS SkipLogging,
    'Found on ' + CAST(GETDATE() AS varchar(100)) + ' by sqlver.spBuildManifest'    
  FROM
    sys.objects so
    JOIN sys.schemas sch ON
      sch.schema_ID = so.schema_ID
    LEFT JOIN sqlver.tblSchemaManifest m ON
      sch.name = m.SchemaName AND
      so.name = m.ObjectName
    WHERE so.type_desc IN (
        'SQL_SCALAR_FUNCTION',
        'SQL_STORED_PROCEDURE',
        'SQL_TABLE_VALUED_FUNCTION',
        'SQL_TRIGGER',
        'USER_TABLE',
        'VIEW',
        'SYNONYM') AND
      m.SchemaManifestId IS NULL    
    ORDER BY 
      sch.name,
      so.name  
END
GO

--*** spVersion
IF OBJECT_ID('sqlver.spVersion') IS NOT NULL DROP PROCEDURE sqlver.spVersion
GO
CREATE PROCEDURE [sqlver].[spVersion]
@ObjectName nvarchar(512) = NULL,
@MaxVersions int = NULL,
@ChangedSince datetime = NULL,
@SchemaLogId int = NULL,
@SortByName bit = 0
AS
BEGIN
  SET NOCOUNT ON
  
  DECLARE @TargetDBName sysname
  DECLARE @TargetSchemaName sysname
  DECLARE @TargetObjectName sysname
  
  SET @TargetDBName = ISNULL(PARSENAME(@ObjectName, 3), DB_NAME())
  SET @TargetSchemaName = ISNULL(PARSENAME(@ObjectName, 2), '%')
  SET @TargetObjectName = ISNULL(PARSENAME(@ObjectName, 1), '%')

  SELECT
    x.Object,
    x.LastUpdate,
    x.LastUpdateBy,
    x.Comments,
    x.SQLCommand,
    x.DateAppeared,
    x.SchemaLogId,
    x.Hash
  FROM (
    SELECT
      COALESCE(
        l.DatabaseName + '.' + l.SchemaName + '.' + l.ObjectName,
        m.DatabaseName + '.' + m.SchemaName + '.' + m.ObjectName) AS Object,
      m.DateAppeared,
      l.SchemaLogId,
      COALESCE(l.EventDate, m.DateUpdated) AS LastUpdate,
      COALESCE(l.LoginName, m.CreatedByLoginName) AS LastUpdateBy,
      COALESCE(l.Comments, m.Comments) AS Comments,
      COALESCE(l.Hash, m.CurrentHash) AS Hash,
      COALESCE(l.SQLCommand, m.OrigDefinition) AS SQLCommand,
      CASE 
        WHEN l.SchemaLogID IS NULL THEN 0 
        ELSE ROW_NUMBER() OVER (PARTITION BY l.DatabaseName, l.SchemaName, l.ObjectName ORDER BY l.SchemaLogId DESC)
      END AS Seq
    FROM
      sqlver.tblSchemaLog l
      FULL OUTER JOIN sqlver.tblSchemaManifest m ON
        l.SchemaName = m.SchemaName AND
        l.ObjectName = m.ObjectName
    WHERE
      COALESCE(l.DatabaseName, m.DatabaseName) LIKE @TargetDBName AND
      COALESCE(l.SchemaName, m.SchemaName) LIKE @TargetSchemaName AND
      COALESCE(l.ObjectName, m.ObjectName) LIKE @TargetObjectName AND
      
      (@ChangedSince IS NULL OR COALESCE(l.EventDate, m.DateUpdated) >= @ChangedSince) AND
      (@SchemaLogId IS NULL OR l.SchemaLogId = @SchemaLogId)
    ) x
  WHERE
    (
     (@MaxVersions IS NULL AND x.Seq < 2) OR
     (x.Seq < = @MaxVersions)
    )
  ORDER BY
    CASE 
      WHEN @SortByName = 1 THEN x.Object
    END,
    CASE    
      WHEN @ChangedSince IS NOT NULL OR 
        (
        @ObjectName IS NULL AND
        @MaxVersions IS NULL AND
        @ChangedSince IS NULL AND
        @SchemaLogId IS NULL
        )
        THEN x.LastUpdate
    END DESC,
    CASE 
      WHEN @SortByName = 0 THEN x.Object
    END
    
    
    
  IF @SchemaLogId IS NOT NULL BEGIN
    DECLARE @Buf nvarchar(MAX)
    SELECT @Buf = l.SQLCommand
    FROM
      sqlver.tblSchemaLog l
    WHERE
      l.SchemaLogID = @SchemaLogID
      
   EXEC sqlver.sputilPrintString @Buf
          
  END
END
GO

--*** sputilFindInCode
IF OBJECT_ID('sqlver.sputilFindInCode') IS NOT NULL DROP PROCEDURE sqlver.sputilFindInCode
GO
CREATE PROCEDURE [sqlver].[sputilFindInCode]
@TargetString varchar(254),
@TargetSchema sysname = NULL
AS 
BEGIN
  SET NOCOUNT ON
  
  DECLARE @Msg varchar(MAX)
  
  DECLARE @PreLen int
  SET @PreLen = 40
  
  DECLARE @PostLen int
  SET @PostLen = 40
  
  SELECT DISTINCT
    sch.name AS SchemaName,
    so.name AS ObjectName, 
    SUBSTRING(sysmod.definition, 
      CASE WHEN PATINDEX('%' + @TargetString + '%', sysmod.definition) - @PreLen < 1 
        THEN 1 
        ELSE PATINDEX('%' + @TargetString + '%', sysmod.definition) - @PreLen
      END,
       
      CASE WHEN PATINDEX('%' + @TargetString + '%', sysmod.definition) + LEN(@TargetString + 'x') - 1 + @PreLen + @PostLen > LEN(sysmod.definition + 'x') - 1
        THEN LEN(sysmod.definition + 'x') - 1 - PATINDEX('%' + @TargetString + '%', sysmod.definition) + 1
        ELSE LEN(@TargetString + 'x') - 1 + @PreLen + @PostLen
      END) AS Context
  INTO #Results
  FROM
    sys.objects so
    JOIN sys.schemas sch ON so.schema_id = sch.schema_id
    JOIN sys.sql_modules  sysmod ON so.object_id = sysmod.object_id
  WHERE 
    ((@TargetSchema IS NULL) OR (sch.name = @TargetSchema)) AND
    (PATINDEX('%' + @TargetString + '%', sysmod.definition) > 0) 
    
  BEGIN TRY 
  INSERT INTO #Results (
    SchemaName,
    ObjectName,
    Context
  )
  SELECT DISTINCT
    CAST('**SQL Agent Job***' AS sysname)  collate database_default AS SchemaName,
    CAST(sysj.name + ' | Step: ' +  sysjs.step_name + ' (' + CAST(sysjs.step_id AS varchar(100)) + ')' AS sysname) collate database_default AS ObjectName,
    
    SUBSTRING(sysjs.command, 
      CASE WHEN PATINDEX('%' + @TargetString + '%', sysjs.command) - @PreLen < 1 
        THEN 1 
        ELSE PATINDEX('%' + @TargetString + '%', sysjs.command) - @PreLen
      END,
       
      CASE WHEN PATINDEX('%' + @TargetString + '%', sysjs.command) + LEN(@TargetString + 'x') - 1 + @PreLen + @PostLen > LEN(sysjs.command + 'x') - 1
        THEN LEN(sysjs.command + 'x') - 1 - PATINDEX('%' + @TargetString + '%', sysjs.command) + 1
        ELSE LEN(@TargetString + 'x') - 1 + @PreLen + @PostLen
      END) collate database_default AS Context     
  FROM
    msdb.dbo.sysjobs sysj
    JOIN msdb.dbo.sysjobsteps sysjs ON
      sysj.job_id = sysjs.job_id      
  WHERE 
    (PATINDEX('%' + @TargetString + '%', sysjs.command) > 0)
  END TRY
  BEGIN CATCH
    SET @Msg='sqlver.sputilFindInCode could not search SQL Agent jobs: ' + ERROR_MESSAGE() 
  END CATCH
        
  SELECT
    SchemaName,
    ObjectName,
    Context
  FROM
    #Results
  ORDER BY
    SchemaName,
    ObjectName
END
GO

--*** udfParseValue
IF OBJECT_ID('sqlver.udfParseValue') IS NOT NULL DROP FUNCTION sqlver.udfParseValue
GO
CREATE FUNCTION [sqlver].[udfParseValue] (
  @InputString nvarchar(MAX),
  @ValueIndex int,
  @Delimiter nchar(1) = ','
  )
RETURNS nvarchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
  RETURN (
    SELECT [Value]
    FROM sqlver.udftGetParsedValues(@InputString, @Delimiter)
    WHERE
      [Index] = @ValueIndex
  )
END
GO

--*** udfParseVarValue
IF OBJECT_ID('sqlver.udfParseVarValue') IS NOT NULL DROP FUNCTION sqlver.udfParseVarValue
GO
CREATE FUNCTION [sqlver].[udfParseVarValue](
@Buf varchar(MAX),
@VarName varchar(254),
@Delim char(1))
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN
  DECLARE @Result varchar(MAX)
  
  DECLARE @i int
  SET @i = 1
  DECLARE @s varchar(MAX)
  
  SET @s = ''
  WHILE (@s IS NOT NULL) AND (@Result IS NULL) BEGIN
    SET @s = sqlver.udfParseValue(@Buf, @i, @Delim)
    IF LEFT(@s, 1) IN ('&', '?') SET @s = RIGHT(@s, LEN(@s + 'x') - 1 - 1)
    IF PATINDEX(@VarName + '=%', @s) = 1 SET @Result = RIGHT(@s, LEN(@s + 'x') - 1 - PATINDEX('%=%', @s))
    SET @i = @i + 1
  END
  
  IF @Delim = '&' BEGIN
    --assume value is URL-encoded
    SET @Result = sqlver.udfURLDecode(@Result)
  END

  IF RTRIM(@Result) = '' SET @Result = NULL
  
  RETURN @Result
END
GO

--*** udfParseVarRemove
IF OBJECT_ID('sqlver.udfParseVarRemove') IS NOT NULL DROP FUNCTION sqlver.udfParseVarRemove
GO
CREATE FUNCTION [sqlver].[udfParseVarRemove](
@Buf varchar(MAX),
@VarName varchar(254),
@Delim char(1))
RETURNS varchar(MAX)
WITH EXECUTE AS OWNER
AS 
BEGIN 
  DECLARE @i int
  SET @i = 1
  DECLARE @s varchar(MAX)
  
  SET @s = ''
  WHILE (@s IS NOT NULL) BEGIN
    SET @s = sqlver.udfParseValue(@Buf, @i, @Delim)
    IF LEFT(@s, 1) IN ('&', '?') SET @s = RIGHT(@s, LEN(@s + 'x') - 1 - 1)
    IF PATINDEX(@VarName + '=%', @s) = 1 BEGIN
      SET @Buf = REPLACE(@Buf, @s + @Delim, '')
      SET @Buf = REPLACE(@Buf, @s, '')      
    END
    ELSE BEGIN  
      SET @i = @i + 1
    END
  END
  
  WHILE RIGHT(@Buf, 1) = '&' BEGIN
    SET @Buf = LEFT(@Buf, LEN(@Buf) -1)
  END
  
  RETURN @Buf
END
GO

--*** spsysBuildCLRAssembly
IF OBJECT_ID('sqlver.spsysBuildCLRAssembly') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLRAssembly
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLRAssembly]
---------------------------------------------------------------------------------------------
/*
Procedure to build and register a CLR assembly from C# source
By David Rueter (drueter@assyst.com), 5/1/2013

PREREQUISITE:

The sn.exe and csc.exe utilities are part of the "Windows SDK for Windows Server 2008 and 
.NET Framework 3.5 ", available as a free download at
http://www.microsoft.com/en-us/download/details.aspx?id=11310

NOTES:
SQL Server 2005 and 2008 CLR support is limited to .NET Framework 3.5. SQL Server 2012 introduces
support for .NET Framework 4.0, but can run .NET Framework 3.5. This procedure uses .NET
Framework 3.5 which is our only option on SQL 2005, 2008, and 2008 R2.

See:  http://www.sqlservercentral.com/articles/SQLCLR/98177/ for more information.
*/
---------------------------------------------------------------------------------------------
  @AssemblyName sysname,    
    --Name of the assembly
  @FileName varchar(512),
    --FileName to use when writing out C# source code
  @FilePath varchar(1024),
    --Path to folder where .DDL will be built.  Path also used for
    --temporary files during this deployment script.
  @DropWrapperSQL varchar(MAX),
    --SQL code to drop the wrapper functions or procedures that expose
    --the assembly's routines  
  @CreateWrapperSQL varchar(MAX),
    --SQL code to create the wrapper functions or procedures that expose
    --the assembly's routines    
  @SourceCode varchar(MAX),
    --C# source code
  @SkipAssemblyItself bit = 0
    --Use when you need to manually add the assembly to a server.  If set, this procedure will onlyd
    --do the supporting work (and will not actually compile @SourceCode or register the assembly)  

/*
  Note:  if the assembly references other assemblies, the caller should first
  create and populate a temporary table as follows:
    CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
*/
WITH EXECUTE AS CALLER
AS 
BEGIN
  SET NOCOUNT ON
  
  DECLARE @Debug bit
  --Set @Debug = 1 to enable verbose PRINT output
  SET @Debug = 1 

  DECLARE @ReRegisterExisting bit
  --Indicates that if referenced assemblies are already registered that
  --they should be dropped and re-registered.  (The main assembly
  --we are building will always be dropped and re-registereed regardless
  --of this setting.)
  SET @ReRegisterExisting = 0

  --------------------------------------------------------------------------------
  /*
  Do the work of registering referenced assemblies and building C# code.
  
  You should not need to edit this section.
  */

  DECLARE @PathToSN sysname
  --SET @PathToSN = '"C:\Program Files\Microsoft SDKs\Windows\v6.1\Bin\sn.exe"'
  SET @PathToSN = '"C:\Temp\MSTools\sn.exe"'
  
 
  DECLARE @DBName sysname
  SET @DBName = DB_NAME()
  
  
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)  


  DECLARE @SQL varchar(MAX)

 
  --Enable CLR supoprt
  IF @Debug = 1 PRINT '***Enabling CLR support'  
  DECLARE @OrigSupport_CLR bit
  SELECT @OrigSupport_CLR = CONVERT(bit, value) FROM sys.configurations WHERE name = 'clr enabled'
  IF ISNULL(@OrigSupport_CLR, 0) = 0 BEGIN
    SET @SQL ='
    EXEC master.dbo.sp_configure ''show advanced options'', 1;
    RECONFIGURE;
    EXEC master.dbo.sp_configure ''clr'', 1;
    RECONFIGURE;'
    IF @Debug = 1 PRINT @SQL  
    EXEC(@SQL)
  END
  
  
  IF @DropWrapperSQL IS NOT NULL BEGIN
    IF @Debug = 1 BEGIN
      PRINT '***Dropping wrapper SQL objects (functions, stored procs, etc.)'
      PRINT @DropWrapperSQL
    END  
    
    DECLARE curDropWrap CURSOR LOCAL STATIC FOR
    SELECT sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(pv.Value)) AS DropWrap
    FROM sqlver.udftGetParsedValues(@DropWrapperSQL, '~') pv
      
    OPEN curDropWrap
    FETCH curDropWrap INTO @DropWrapperSQL
    WHILE @@FETCH_STATUS = 0 BEGIN
      EXEC(@DropWrapperSQL)
      FETCH curDropWrap INTO @DropWrapperSQL
    END
    CLOSE curDropWrap
    DEALLOCATE curDropWrap           
  END  
  
  IF ISNULL(@SkipAssemblyItself, 0) = 0 BEGIN
    --Drop assembly if it exists
    IF EXISTS (SELECT * FROM sys.assemblies asms WHERE asms.name = @AssemblyName and is_user_defined = 1) BEGIN  
    --IF ASSEMBLYPROPERTY (@AssemblyName, 'MvID') IS NOT NULL BEGIN
      SET @SQL  = 'DROP ASSEMBLY [' + @AssemblyName + '];'
      IF @Debug = 1 BEGIN
        PRINT '***Dropping existing assembly'
        PRINT @SQL
      END
      EXEC(@SQL)
    END
  END
  


  DECLARE @DropSQL varchar(MAX)
  
  DECLARE @References varchar(MAX)

  SET @References = ''       
  
  DECLARE @RefSQL varchar(MAX)
  SET @RefSQL = '
    SELECT
      REVERSE(sqlver.udfParseValue(REVERSE(FQFileName), 1, ''\''))
    FROM #References
    WHERE
      AddToCompilerRefs = 1
    ORDER BY
      RefSequence
  '

  EXEC sqlver.sputilResultsetAsStr
    @SQL = @RefSQL,
    @ResultSuffix = ' ',
    @TrimTrailSuffix = 1,
    @Result = @References OUTPUT
    
  IF NULLIF(RTRIM(@References), '') IS NOT NULL BEGIN
    SET @References = ISNULL(' /reference:' + @References + ' ', '')
  END
      
  
  --Register each assembly referenced by the new CLR assembly.  
  IF OBJECT_ID('tempdb..#References') IS NOT NULL BEGIN
    DECLARE curThis CURSOR LOCAL STATIC FOR
    SELECT 
      AssemblyName,
      FQFileName,
      CASE WHEN PATINDEX('%SYSTEM%', AssemblyName) > 0 THEN 'MSDOTNET' ELSE REPLACE(COALESCE(IdentifierRoot, FQFileName), '\', '/') END
    FROM #References
    ORDER BY RefSequence    
    
    DECLARE @ThisAssemblyName sysname
    DECLARE @ThisFQFileName varchar(1024)
    DECLARE @ThisObjectName varchar(1024)
    
    
    DECLARE @Include bit

    SET @DropSQL = ''  
    SET @SQL = ''
    
    OPEN curThis
    FETCH curThis INTO @ThisAssemblyName, @ThisFQFileName, @ThisObjectName
    WHILE @@FETCH_STATUS = 0 BEGIN
      IF ASSEMBLYPROPERTY (@ThisAssemblyName, 'MvID') IS NOT NULL BEGIN
        --Assembly already exists
        SET @Include = @ReRegisterExisting
      END
      ELSE BEGIN
        SET @Include = 1
      END
           
      /*
      The problem with references are that multiple referenced .DLL's may be signed with the same key
      (such as is the case with .NET Framework assemblies).  This means that creating the asymmetric
      key for the first referenced .DLL will work, but asymmetric keys cannot be created for subsequent
      referenced .DLL's because the public key (derived from the strong name of the .DLL's) already exists
      in the first asymmetric key.
      
      So, naming convention alone is not enough to tell us whether an asymmetric key needs to be created
      for a referenced assembly.  Instead, we need to either create a "temporary" key, and compare the
      resulting public_key column in sys.asymmetric_keys with public keys in master.sys.asymmetric_keys,
      or simply trap and ignore any error on the create statement (and assume that the failure was
      due to an existing key).      
      */     
      
      
  --        'IF NOT EXISTS (SELECT asymmetric_key_id FROM sys.asymmetric_keys WHERE name = ''SQLCLRKey#' + @ThisObjectName + ''') ' +       
PRINT '****' +           'CREATE ASSEMBLY [' + @ThisAssemblyName + '] FROM ''' + @ThisFQFileName + ''' WITH PERMISSION_SET = UNSAFE;'         

      IF @Include = 1 BEGIN 
        PRINT '***** Need to include assembly ' + ISNULL(@ThisAssemblyName, 'NULL') 
        
        SET @DropSQL = 
          'USE MASTER;' + @CRLF +        

          'BEGIN TRY' + @CRLF +
            'DROP LOGIN [SQLCLRLogin#' + @ThisObjectName + ']' + @CRLF + 
          'END TRY BEGIN CATCH PRINT ERROR_MESSAGE() + CHAR(13) + ''Could not drop login [SQLCLRLogin#' + @ThisObjectName + ']'' END CATCH' + @CRLF +
                    
          'BEGIN TRY' + @CRLF +
            'IF EXISTS (SELECT asymmetric_key_id FROM sys.asymmetric_keys WHERE name = ''SQLCLRKey#' + @ThisObjectName + ''') DROP ASYMMETRIC KEY [SQLCLRKey#' + @ThisObjectName + '];' + @CRLF +
          'END TRY BEGIN CATCH PRINT ERROR_MESSAGE() + CHAR(13) + ''Could not drop asymmetric key for ' + ISNULL(@ThisObjectName, 'NULL') + ''' END CATCH' + @CRLF +
          'USE ' + @DBName + ';' + @CRLF +          
          'BEGIN TRY' + @CRLF +          
            --'IF ASSEMBLYPROPERTY (''' + @ThisAssemblyName + ''', ''MvID'') IS NOT NULL DROP ASSEMBLY [' + @ThisAssemblyName + '];' + @CRLF +
            'IF EXISTS (SELECT * FROM sys.assemblies asms WHERE asms.name = ''' + @ThisAssemblyName + ''' and is_user_defined = 1) DROP ASSEMBLY [' + @ThisAssemblyName + '];' + @CRLF +          
          'END TRY BEGIN CATCH PRINT ERROR_MESSAGE() + CHAR(13) + ''Could not drop assembly ' + ISNULL(@ThisAssemblyName, 'NULL') + ''' END CATCH' + @CRLF +
          ISNULL(@DropSQL, '')
             
        
        SET @SQL = ISNULL(@SQL, '') +
          'USE MASTER;' + @CRLF +
          'BEGIN TRY' + @CRLF +
             'CREATE ASYMMETRIC KEY [SQLCLRKey#' + @ThisObjectName + '] FROM EXECUTABLE FILE = ''' + @ThisFQFileName + '''' + @CRLF +            
             'IF NOT EXISTS (SELECT sid FROM sys.syslogins WHERE name = ''SQLCLRLogin#' + @ThisObjectName + ''')' + @CRLF + 
             'CREATE LOGIN [SQLCLRLogin#' + @ThisObjectName + '] FROM ASYMMETRIC KEY [SQLCLRKey#' + @ThisObjectName + ']' + @CRLF +          
             'ALTER LOGIN [SQLCLRLogin#' + @ThisObjectName + '] DISABLE;' + @CRLF +
             'GRANT EXTERNAL ACCESS ASSEMBLY TO [SQLCLRLogin#' + @ThisObjectName + '];' + @CRLF + 
             'GRANT UNSAFE ASSEMBLY TO [SQLCLRLogin#' + @ThisObjectName + '];' + @CRLF +
          'END TRY BEGIN CATCH PRINT ERROR_MESSAGE() + CHAR(13) + ''Could not create asymmetric key or login for  ' + ISNULL(@ThisObjectName, 'NULL') + ''' END CATCH' + @CRLF +

          'BEGIN TRY' + @CRLF +        
            'USE ' + @DBName + ';' + @CRLF +       
            'CREATE ASSEMBLY [' + @ThisAssemblyName + '] FROM ''' + @ThisFQFileName + ''' WITH PERMISSION_SET = UNSAFE;' + @CRLF +                      
          'END TRY BEGIN CATCH PRINT ERROR_MESSAGE() + CHAR(13) + ''Could not create assembly ' + ISNULL(@ThisAssemblyName, 'NULL') + ''' END CATCH' + @CRLF             
      END
    
      --SET @References = @References +  ' /reference:' + @ThisFQFileName
      FETCH curThis INTO @ThisAssemblyName, @ThisFQFileName, @ThisObjectName 
    END
    CLOSE curThis
    DEALLOCATE curThis            
  END
  
  IF @Debug = 1 BEGIN
    PRINT '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    PRINT '***Dropping existing assemblies'    
    EXEC sqlver.sputilPrintString @DropSQL        
    PRINT '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'    
  END
  EXEC(@DropSQL)

  IF @Debug = 1 BEGIN
    PRINT '***Registering and securing referenced assemblies'
    EXEC sqlver.sputilPrintString @SQL
  END
  EXEC(@SQL)
    
  --Temporarily enable COM support (for local file access)
  DECLARE @OrigSupport_COM bit
  SELECT @OrigSupport_COM = CONVERT(bit, value) FROM sys.configurations WHERE name = 'Ole Automation Procedures'
  IF ISNULL(@OrigSupport_COM, 0) = 0 BEGIN
    IF @Debug = 1 PRINT '***Temporarily setting up COM support'  
    SET @SQL = '
      EXEC sp_configure ''show advanced options'', 1;
      RECONFIGURE;
      EXEC sp_configure ''Ole Automation Procedures'', 1;
      RECONFIGURE;'   
    IF @Debug = 1 PRINT @SQL     
    EXEC(@SQL)
       
    SET @SQL =
      'USE master;' + @CRLF +
      'BEGIN TRY' + @CRLF +
      'GRANT EXEC ON sp_OACreate TO [' + SUSER_NAME() + '];' + @CRLF +
      'GRANT EXEC ON sp_OAGetProperty TO [' + SUSER_NAME() + '];' +@CRLF +
      'GRANT EXEC ON sp_OASetProperty TO [' + SUSER_NAME() + '];' +@CRLF +
      'GRANT EXEC ON sp_OAMethod TO [' + SUSER_NAME() + '];' + @CRLF +
      'GRANT EXEC ON sp_OAGetErrorInfo TO [' + SUSER_NAME() + '];' +@CRLF +
      'GRANT EXEC ON sp_OADestroy TO [' + SUSER_NAME() + '];' + @CRLF + 
      'END TRY' + @CRLF +
      'BEGIN CATCH' + @CRLF +
      'PRINT ''Could not grant rights.  Attempting to proceed anyway.''' + @CRLF +
      'END CATCH' + @CRLF +
      'USE ' + @DBName + ';'

    IF @Debug = 1 PRINT @SQL    
    EXEC(@SQL)    
  END

  
  IF ISNULL(@SkipAssemblyItself, 0) = 0 BEGIN
    --Write out C# file    
    IF @Debug = 1 PRINT '***Writing out C# File'

    SET @SourceCode = '
      //Generated by sqlver.spsysRebuildCLR' + @CRLF +
      @SourceCode

    IF @Debug = 1 PRINT 'EXEC sqlver.sputilWriteStringToFile... (' + @FilePath + @FileName + ')'
    EXEC sqlver.sputilWriteStringToFile 
      @FileData = @SourceCode,
      @FilePath = @FilePath,
      @FileName = @FileName


    --Temporarily enable xp_cmdshell support so we can build the source code
    DECLARE @OrigSupport_XPCmdShell bit
    SELECT @OrigSupport_XPCmdShell = CONVERT(bit, value) FROM sys.configurations WHERE name = 'xp_cmdshell'
    IF ISNULL(@OrigSupport_XPCmdShell, 0) = 0 BEGIN
      IF @Debug = 1 PRINT '***Temporarily enabling xp_cmdshell support'  
      SET @SQL = '
        EXEC master.dbo.sp_configure ''show advanced options'', 1;
        RECONFIGURE;
        EXEC master.dbo.sp_configure ''xp_cmdshell'', 1;
        RECONFIGURE;'
      IF @Debug = 1 PRINT @SQL    
      EXEC(@SQL)
    END


    --Create the temporary batch file we will call
    IF @Debug = 1 PRINT '***Writing batch file to build C# source into .dll' 

    DECLARE @ThisDriveLetter varchar(10)
    DECLARE @ThisPath varchar(MAX)
    
    SET @ThisDriveLetter = sqlver.udfParseValue(@FilePath, 1, ':') + ':'
    SET @ThisPath = sqlver.udfParseValue(@FilePath, 2, ':')
    

    DECLARE @Command varchar(2048)
    SET @Command = 
    @ThisDriveLetter + @CRLF +
    'cd ' + @ThisPath + @CRLF + 
    @PathToSN + ' -k ' + @FilePath +  REPLACE(@FileName, '.cs', '.snk') + @CRLF +
     '"C:\Windows\Microsoft.NET\Framework\v3.5\csc" /t:library' +  
     @References + 
     ' /out:' + @FilePath + REPLACE(@FileName, '.cs', '.dll') + 
     ' /keyfile:' + @FilePath +  REPLACE(@FileName, '.cs', '.snk') + 
     ' ' + @FilePath + @FileName   

    IF @Debug = 1 PRINT 'EXEC sqlver.sputilWriteStringToFile... (' + @FilePath + 'tmp.bat)'
    EXEC sqlver.sputilWriteStringToFile 
      @FileData = @Command,
      @FilePath = @FilePath,
      @FileName = 'tmp.bat'   
    
    --Execute the temporary batch file
    IF @Debug = 1 PRINT '***Executing batch file to build C# source into .dll'
    SET @SQL = 'EXEC xp_cmdshell "' + @FilePath + 'tmp.bat' + '";'
    IF @Debug = 1 PRINT @SQL
    EXEC(@SQL)  

    --Clean up by deleting temporary files
    IF @Debug = 1 PRINT '***Deleting batch file'
    SET @SQL = 'EXEC xp_cmdshell "del ' + @FilePath + 'tmp.bat' + '"'
    IF @Debug = 1 PRINT @SQL
    EXEC(@SQL)  
    
    
    IF @Debug = 1 PRINT '***Deleting .snk file'
    SET @SQL = 'EXEC xp_cmdshell "del ' + @FilePath + REPLACE(@FileName, '.cs', '.snk') + '"'
    IF @Debug = 1 PRINT @SQL
    EXEC(@SQL)  
    
    IF @Debug = 1 PRINT '***Deleting .cs file'
    SET @SQL = 'EXEC xp_cmdshell "del ' + @FilePath + @Filename + '"'
    IF @Debug = 1 PRINT @SQL
    EXEC(@SQL)      


    --Disable xp_cmdshell support
    IF ISNULL(@OrigSupport_XPCmdShell, 0) = 0  BEGIN
      IF @Debug = 1 PRINT '***Disabling xp_cmdshell support'
      SET @SQL = '
        EXEC master.dbo.sp_configure ''show advanced options'', 1;
        RECONFIGURE;
        EXEC master.dbo.sp_configure ''xp_cmdshell'', 0;
        RECONFIGURE;'
      IF @Debug = 1 PRINT @SQL    
      EXEC(@SQL)
    END        
       
    --Disable COM support      
    IF ISNULL(@OrigSupport_COM, 0) = 0  BEGIN
      IF @Debug = 1 PRINT '***Disabling COM support'
      SET @SQL = '
        EXEC master.dbo.sp_configure ''show advanced options'', 1;
        RECONFIGURE;
        EXEC master.dbo.sp_configure ''Ole Automation Procedures'', 0;
        RECONFIGURE;'
      IF @Debug = 1 PRINT @SQL    
      EXEC(@SQL)
    END
    
  END
  

  -----------------------------------------------------------------------------------------        
  /*
  Register the new assembly, and create wrapper function (or stored procedure)
  
  Edit this code as necessary to include your own names (i.e. see sql.udfRenderPDF,
  PDFCLR, SQLCLRLogon_PDFCLR, 
    */
          
  IF ISNULL(@SkipAssemblyItself, 0) = 0 BEGIN
    IF @Debug = 1 PRINT '***Registering newly-built assembly ' + @AssemblyName    
    SET @SQL = sqlver.udfGenerateCLRRegisterSQL(@AssemblyName, @FilePath + REPLACE(@FileName, '.cs', '.dll'))
    IF @Debug = 1 PRINT @SQL
    EXEC(@SQL)  
  END


  IF @CreateWrapperSQL IS NOT NULL BEGIN
    IF @Debug = 1 BEGIN
      PRINT '***Creating wrapper SQL objects (functions, stored procs, etc.)'
      PRINT @CreateWrapperSQL
    END
      
    DECLARE curCreateWrap CURSOR LOCAL STATIC FOR
    SELECT sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(pv.Value)) AS CreateWrap
    FROM sqlver.udftGetParsedValues(@CreateWrapperSQL, '~') pv
      
    OPEN curCreateWrap
    FETCH curCreateWrap INTO @CreateWrapperSQL
    WHILE @@FETCH_STATUS = 0 BEGIN
      EXEC(@CreateWrapperSQL)
      FETCH curCreateWrap INTO @CreateWrapperSQL
    END
    CLOSE curCreateWrap
    DEALLOCATE curCreateWrap     
  END
      
  -----------------------------------------------------------------------------------------
  
  
  PRINT 
  @CRLF + @CRLF +
  '***Finished building and registering the ' + @AssemblyName + ' CLR Assembly and related objects.' 
  
END
GO

--*** spsysBuildCLR_SendMail
IF OBJECT_ID('sqlver.spsysBuildCLR_SendMail') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLR_SendMail
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLR_SendMail]
---------------------------------------------------------------------------------------------
/*
Procedure to demonstrate use of sqlver.spsysBuildCLRAssembly to build and register a CLR
assembly in SQL without the use of Visual Studio.

This is just a sample:  you can use this as a template to create your own procedures
to register your own CLR assemblies.

By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
AS 
BEGIN
  SET NOCOUNT ON
  
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\Temp\AssemblyCache\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System, 'C:\Windows\Microsoft.NET\Framework64\v2.0.50727\System.dll')
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.udfGetMIMEType_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfGetMIMEType_CLR;
    END    
    
    IF OBJECT_ID(''sqlver.sputilSendMail_CLR'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilSendMail_CLR;
    END
    
    IF OBJECT_ID(''sqlver.udfBase64Encode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfBase64Encode_CLR;
    END
    '      

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE FUNCTION sqlver.udfGetMIMEType_CLR (
      @Filename nvarchar(MAX)
    )      
    RETURNS nvarchar(MAX)
    AS
      EXTERNAL NAME [SendMail_SQLCLR].[Functions].[GetMIMETypeFromFilename]  
    ~      
    CREATE FUNCTION sqlver.udfBase64Encode_CLR (
      @Buf varbinary(MAX)
    )
    RETURNS nvarchar(MAX)
    AS
      EXTERNAL NAME [SendMail_SQLCLR].[Functions].[Base64Encode]
    ~  
    CREATE PROCEDURE sqlver.sputilSendMail_CLR      
      @From nvarchar(4000) = NULL, 
      @FromFriendly nvarchar(4000) = NULL,      
      @To   nvarchar(MAX),  --note:  you can specify friendly name like this ''Steve Friday <something@changeme.com> and can use a comma or semicolon separated list'' 
      @Subject nvarchar(MAX), 
      @CC nvarchar(4000) = NULL,
      @BCC nvarchar(4000) = NULL,

      @TextBody nvarchar(MAX), --use this if sending text
      @HTMLBody nvarchar(MAX),  --use this if sending HTML

      @ServerAddress nvarchar(MAX),
      @ServerPort int = 25,
      @EnableSSL bit = 0,
      @User nvarchar(MAX),
      @Password nvarchar(MAX),
      
      @AttachFilename nvarchar(4000) = NULL, --If @AttachData is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
      @AttachData varbinary(MAX) --Binary data to include as an attachment                
    AS
      --NOTE: We would like to have some of these parameters such as @AttachData default to NULL,
      --but then we cannot use varchar(MAX) or varbinary(MAX).  It is for this reason that we  also
      --are using nvarchar(4000) on some parameters:  these can be changed to nvarchar(MAX) to support
      --longer values, but then we cannot use default values.
      EXTERNAL NAME [SendMail_SQLCLR].[Procedures].[SendMail]                 
    '
      

  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.IO;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;

using System.Data.SqlTypes;

public partial class Functions
{
    public static string GetMIMETypeFromFilename(string filename)
    {
        //string paramFilename = filename.IsNull ? null : Convert.ToString(filename);
        string paramFilename = filename;
        string thisExtension = Path.GetExtension(paramFilename);
        string result = "text/plain";

        var dictMime = new Dictionary<string, string>
        //from lists at http://www.freeformatter.com/mime-types-list.html
        //and http://www.utoronto.ca/webdocs/HTMLdocs/Book/Book-3ed/appb/mimetype.html#text
        {
            { ".jpg","image/jpeg" },
            { ".3g2","video/3gpp2" },
            { ".3gp","video/3gpp" },
            { ".7z","application/x-7z-compressed" },
            { ".ai","application/postscript" },
            { ".aif","audio/x-aiff" },
            { ".air","application/vnd.adobe.air-application-installer-package+zip" },
            { ".apk","application/vnd.android.package-archive" },
            { ".asf","video/x-ms-asf" },
            { ".avi","video/x-msvideo" },
            { ".bmp","image/bmp" },
            { ".cab","application/vnd.ms-cab-compressed" },
            { ".chm","application/vnd.ms-htmlhelp" },
            { ".doc","application/msword" },
            { ".docm","application/vnd.ms-word.document.macroenabled.12" },
            { ".docx","application/vnd.openxmlformats-officedocument.wordprocessingml.document" },
            { ".dotm","application/vnd.ms-word.template.macroenabled.12" },
            { ".dotx","application/vnd.openxmlformats-officedocument.wordprocessingml.template" },
            { ".dts","audio/vnd.dts" },
            { ".dwf","model/vnd.dwf" },
            { ".dwg","image/vnd.dwg" },
            { ".dxf","image/vnd.dxf" },
            { ".eml","message/rfc822" },
            { ".eps","application/postscript" },
            { ".exe","application/x-msdownload" },
            { ".gif","image/gif" },
            { ".gtar","application/x-gtar" },
            { ".hlp","application/winhlp" },
            { ".hqx","application/mac-binhex40" },
            { ".htm","text/html" },
            { ".html","text/html" },
            { ".icc","application/vnd.iccprofile" },
            { ".ico","image/x-icon" },
            { ".ics","text/calendar" },
            { ".jar","application/java-archive" },
            { ".java","text/x-java-source,java" },
            { ".jnlp","application/x-java-jnlp-file" },
            { ".jpeg","image/jpeg" },
            { ".jpgv","video/jpeg" },
            { ".js","application/javascript" },
            { ".json","application/json" },
            { ".kml","application/vnd.google-earth.kml+xml" },
            { ".kmz","application/vnd.google-earth.kmz" },
            { ".ktx","image/ktx" },
            { ".latex","application/x-latex" },
            { ".m3u","audio/x-mpegurl" },
            { ".mdb","application/x-msaccess" },
            { ".mid","audio/midi" },
            { ".mny","application/x-msmoney" },
            { ".mov","video/quicktime" },
            { ".mp3","audio/mpeg" },
            { ".mp4","video/mp4" },
            { ".mp4a","audio/mp4" },
            { ".mpeg","video/mpeg" },
            { ".mpg","video/mpeg" },
            { ".mpga","audio/mpeg" },
            { ".mpkg","application/vnd.apple.installer+xml" },
            { ".mpp","application/vnd.ms-project" },
            { ".onetoc","application/onenote" },
            { ".pcl","application/vnd.hp-pcl" },
            { ".pcx","image/x-pcx" },
            { ".pdf","application/pdf" },
            { ".pgp","application/pgp-signature" },
            { ".pl","application/x-perl" },
            { ".png","image/png" },
            { ".potm","application/vnd.ms-powerpoint.template.macroenabled.12" },
            { ".potx","application/vnd.openxmlformats-officedocument.presentationml.template" },
            { ".ppam","application/vnd.ms-powerpoint.addin.macroenabled.12" },
            { ".ppd","application/vnd.cups-ppd" },
            { ".ppsm","application/vnd.ms-powerpoint.slideshow.macroenabled.12" },
            { ".ppsx","application/vnd.openxmlformats-officedocument.presentationml.slideshow" },
            { ".ppt","application/vnd.ms-powerpoint" },
            { ".pptm","application/vnd.ms-powerpoint.presentation.macroenabled.12" },
            { ".pptx","application/vnd.openxmlformats-officedocument.presentationml.presentation" },
            { ".ps","application/postscript" },
            { ".psd","image/vnd.adobe.photoshop" },
            { ".pub","application/x-mspublisher" },
            { ".qfx","application/vnd.intu.qfx" },
            { ".qt","video/quicktime" },
            { ".qxd","application/vnd.quark.quarkxpress" },
            { ".ram","audio/x-pn-realaudio" },
            { ".rm","application/vnd.rn-realmedia" },
            { ".rsd","application/rsd+xml" },
            { ".rss","application/rss+xml" },
            { ".rtf","application/rtf" },
            { ".rtx","text/richtext" },
            { ".sh","application/x-sh" },
            { ".sit","application/x-stuffit" },
            { ".sitx","application/x-stuffitx" },
            { ".svg","image/svg+xml" },
            { ".swf","application/x-shockwave-flash" },
            { ".tar","application/x-tar" },
            { ".tcl","application/x-tcl" },
            { ".tif","image/tiff" },
            { ".tiff","image/tiff" },
            { ".torrent","application/x-bittorrent" },
            { ".tsv","text/tab-separated-values" },
            { ".ttf","application/x-font-ttf" },
            { ".txt","text/plain" },
            { ".vsd","application/vnd.visio" },
            { ".wav","audio/x-wav" },
            { ".weba","audio/webm" },
            { ".webm","video/webm" },
            { ".wma","audio/x-ms-wma" },
            { ".wmd","application/x-ms-wmd" },
            { ".wmf","application/x-msmetafile" },
            { ".wmv","video/x-ms-wmv" },
            { ".woff","application/x-font-woff" },
            { ".wpd","application/vnd.wordperfect" },
            { ".wps","application/vnd.ms-works" },
            { ".wri","application/x-mswrite" },
            { ".wvx","video/x-ms-wvx" },
            { ".xap","application/x-silverlight-app" },
            { ".xhtml","application/xhtml+xml" },
            { ".xif","image/vnd.xiff" },
            { ".xlam","application/vnd.ms-excel.addin.macroenabled.12" },
            { ".xls","application/vnd.ms-excel" },
            { ".xlsm","application/vnd.ms-excel.sheet.macroenabled.12" },
            { ".xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" },
            { ".xml","application/xml" },
            { ".xps","application/vnd.ms-xpsdocument" },
            { ".zip","application/zip" }
        };

        if (dictMime.ContainsKey(paramFilename))
        {
            result = dictMime[paramFilename];
        }

        return result;
    }
    
      
  //A function we want to expose in the SQL CLR assembly
  //(return string containing Base64 representation of binary data)
  public static string Base64Encode(SqlBytes AttachData)
  {
      return Convert.ToBase64String(AttachData.Buffer);
  }    
}


        
public partial class Procedures
{
    public static void SendMail(
        SqlString From,
        SqlString FromFriendly,
        SqlString To,
        SqlString Subject,
        SqlString CC,
        SqlString BCC,
        SqlString TextBody,
        SqlString HTMLBody,

        SqlString ServerAddress,
        SqlInt32 ServerPort, //port 465 for SSL, or 25,
        SqlBoolean EnableSSL,
        SqlString User,
        SqlString Password,

        SqlString AttachFilename,
        SqlBytes AttachData
    )
    {

        Func<MailAddressCollection, string, MailAddressCollection> add_addresses = null;
        add_addresses = (MailAddressCollection thisAddrCollection, string thisAddrString) =>
        {
            if (thisAddrString != null && thisAddrString.Trim() != "")
            {
                string[] toAddresses = thisAddrString.Replace(",", ";").Split('';'');
                foreach (string thisAddrStr in toAddresses)
                {
                    string thisAddr = null;
                    string thisFriendly = null;
                    if (thisAddrStr.IndexOf("<") > 0)
                    {
                        var thisAddrParts = thisAddrStr.Split(''<'');
                        thisFriendly = thisAddrParts[0].Trim();
                        thisAddr = thisAddrParts[1].Replace(">", "").Trim();
                    }
                    else
                    {
                        thisAddr = thisAddrStr;
                        thisFriendly = null;
                    }
                    thisAddrCollection.Add(new MailAddress(thisAddr, thisFriendly));
                }
            }

            return thisAddrCollection;
        };

        //convert SQL type parameters into C# types
        string paramFrom = From.IsNull ? null : Convert.ToString(From);
        string paramFromFriendly = FromFriendly.IsNull ? null : Convert.ToString(FromFriendly);
        string paramTo = To.IsNull ? null : Convert.ToString(To);
        string paramSubject = Subject.IsNull ? null : Convert.ToString(Subject);
        string paramCC = CC.IsNull ? null : Convert.ToString(CC);
        string paramBCC = BCC.IsNull ? null : Convert.ToString(BCC);
        string paramTextBody = TextBody.IsNull ? null : Convert.ToString(TextBody);
        string paramHTMLBody = HTMLBody.IsNull ? null : Convert.ToString(HTMLBody);

        string paramServerAddress = ServerAddress.IsNull ? null : Convert.ToString(ServerAddress);

        Int32 paramServerPort = ServerPort.IsNull ? 25 : Convert.ToInt32(ServerPort.Value);

        Boolean paramEnableSSL = EnableSSL.IsNull ? false : Convert.ToBoolean(EnableSSL.Value);

        string paramUser = User.IsNull ? null : Convert.ToString(User);
        string paramPassword = Password.IsNull ? null : Convert.ToString(Password);

        string paramAttachFilename = AttachFilename.IsNull ? null : Convert.ToString(AttachFilename);
        //we don''t need to convert AttachData;


        using (var thisMailMessage = new MailMessage())
        {
            thisMailMessage.From = new MailAddress(paramFrom, paramFromFriendly);

            add_addresses(thisMailMessage.To, paramTo);
            add_addresses(thisMailMessage.CC, paramCC);
            add_addresses(thisMailMessage.Bcc, paramBCC);

            thisMailMessage.Subject = paramSubject;
            thisMailMessage.Body = paramTextBody;

            if (paramHTMLBody == "" || paramHTMLBody == null)
            {
                thisMailMessage.Body = paramTextBody;
                thisMailMessage.IsBodyHtml = false;
            }
            else {
                if (paramTextBody == null || paramTextBody.Trim() == "")
                {
                    //HTML body, with no text body
                    thisMailMessage.Body = paramHTMLBody;
                    thisMailMessage.IsBodyHtml = true;
                }
                else {
                    thisMailMessage.Body = paramTextBody;
                    thisMailMessage.IsBodyHtml = false;

                    //HTML body and text body: add HTML as alternate
                    ContentType mimeType = new System.Net.Mime.ContentType("text/html");

                    AlternateView alternate = AlternateView.CreateAlternateViewFromString(paramHTMLBody, mimeType);
                    thisMailMessage.AlternateViews.Add(alternate);
                }
            }  

            if (!AttachData.IsNull && AttachData != null)
            {
                MemoryStream attachDataStream = new MemoryStream(AttachData.Buffer);
                Attachment thisAttach = null;
                
                if (paramAttachFilename != null && paramAttachFilename.Trim() != "")
                {
                  //note:  AttachmentFileName is used as name for the binary data passed in                
                  thisAttach = new Attachment(attachDataStream, paramAttachFilename, Convert.ToString(Functions.GetMIMETypeFromFilename(paramAttachFilename)));
                }
                else
                {
                  thisAttach = new Attachment(attachDataStream, paramAttachFilename);                              
                }
                thisMailMessage.Attachments.Add(thisAttach);
            }
            else
            {
                //no AttachData provided
                if (paramAttachFilename != null && paramAttachFilename.Trim() != "")
                {
                    FileStream attachDataStream = new FileStream(paramAttachFilename, FileMode.Open);
                    //Note:  AttachmentFileName specifies fiel to read from
                    Attachment thisAttach = new Attachment(attachDataStream, paramAttachFilename, Convert.ToString(Functions.GetMIMETypeFromFilename(paramAttachFilename)));
                    thisMailMessage.Attachments.Add(thisAttach);
                }
            }



            var smtp = new SmtpClient
            {
                Host = paramServerAddress,
                Port = paramServerPort,
                EnableSsl = paramEnableSSL,
                DeliveryMethod = SmtpDeliveryMethod.Network,
                UseDefaultCredentials = false,
                Credentials = new NetworkCredential(paramUser, paramPassword)
            };

            smtp.Send(thisMailMessage);

        }

    }
}
//------end of CLR Source------
'    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'SendMail_SQLCLR',
    @FileName = 'SendMail_SQLCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END
GO

--*** spsysBuildCLR_GetHTTP
IF OBJECT_ID('sqlver.spsysBuildCLR_GetHTTP') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLR_GetHTTP
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLR_GetHTTP]
---------------------------------------------------------------------------------------------
/*
Procedure to demonstrate use of sqlver.spsysBuildCLRAssembly to build and register a CLR
assembly in SQL without the use of Visual Studio.

This is just a sample:  you can use this as a template to create your own procedures
to register your own CLR assemblies.

By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
--$!ParseMarker
--Note:  comments and code between marker and AS are subject to automatic removal by OpsStream
--©Copyright 2006-2010 by David Rueter, Automated Operations, Inc.
--May be held, used or transmitted only pursuant to an in-force licensing agreement with Automated Operations, Inc.
--Contact info@opsstream.com / 800-964-3646 / 949-264-1555
AS 
BEGIN
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\Temp\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Drawing.dll')
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Windows.Forms', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Windows.Forms.dll')  
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.sputilGetHTTP'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilGetHTTP;
    END

    IF OBJECT_ID(''sqlver.sputilGetHTTP_CLR'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilGetHTTP_CLR;
    END
            
    IF OBJECT_ID(''sqlver.udfURLEncode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfURLEncode_CLR;
    END    
    
    IF OBJECT_ID(''sqlver.udfURLDecode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfURLDecode_CLR;
    END       
    
  '

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE PROCEDURE sqlver.sputilGetHTTP_CLR
      @URL nvarchar(MAX),
        --URL to retrieve data from
      @HTTPMethod nvarchar(40) = ''GET'',
        --can be either GET or POST
      @ContentType nvarchar(254)= ''text/http'',
        --set to ''application/x-www-form-urlencoded'' for POST, etc.  
      @Cookies nvarchar(MAX) OUTPUT,
        --string containing name=value,name=value list of cookies and values
      @DataToSend nvarchar(MAX), 
        --data to post, if @HTTPMethod = ''POST''
      @DataToSendBin varbinary(MAX),
        --data to post (binary)...if @DataToSend is not provided
      @Headers nvarchar(MAX) OUTPUT,
        --Headers to include with the request / headers returned with the response
        --CRLF terminated list of Name: Value strings
      @User nvarchar(512) = NULL,
        --If provided, use this value for the HTTP authentication user name
      @Password nvarchar(512) = NULL,
        --If provided, use this value for the HTTP authentication password        
      @UserAgent nvarchar(512) = ''SQLCLR'',
        --If provided, use this value for the HTTP UserAgent header           
      @HTTPStatus int = NULL OUTPUT,
        --HTTP Status Code (200=OK, 404=Not Found, etc.)
      @HTTPStatusText nvarchar(4000) = NULL OUTPUT,  
        --HTTP status code description
      @RedirURL nvarchar(4000) = NULL OUTPUT,
        --Redirect URL
      @ResponseBinary varbinary(MAX) OUTPUT,
        --Full binary data returned by remote HTTP server

      @ErrorMsg nvarchar(MAX) OUTPUT
        --NULL unless an error message was encountered   
    AS
      EXTERNAL NAME [GetHTTPCLR_SQLCLR].[Procedures].[HTTPGet]
      
    ~
        
    CREATE PROCEDURE sqlver.sputilGetHTTP
      @URL nvarchar(MAX),
        --URL to retrieve data from
      @HTTPMethod nvarchar(40) = ''GET'',
        --can be either GET or POST
      @ContentType nvarchar(254)= ''text/http'',
        --set to ''''application/x-www-form-urlencoded'''' for POST, etc.  
      --@Cookies nvarchar(MAX) OUTPUT,
        --string containing name=value,name=value list of cookies and values
      --@DataToSend nvarchar(MAX), 
        --data to post, if @HTTPMethod = ''''POST''''
      --@DataToSendBin varbinary(MAX),
        --data to post (binary)...if @DataToSend is not provided
      --@Headers nvarchar(MAX) OUTPUT,
        --Headers to include with the request / headers returned with the response
        --CRLF terminated list of Name: Value strings
      @User nvarchar(512) = NULL,
        --If provided, use this value for the HTTP authentication user name
      @Password nvarchar(512) = NULL,
        --If provided, use this value for the HTTP authentication password        
      @UserAgent nvarchar(512) = ''SQLCLR'',
        --If provided, use this value for the HTTP UserAgent header           
      @HTTPStatus int = NULL OUTPUT,
        --HTTP Status Code (200=OK, 404=Not Found, etc.)
      @HTTPStatusText nvarchar(4000) = NULL OUTPUT,  
        --HTTP status code description
      @RedirURL nvarchar(4000) = NULL OUTPUT,
        --Redirect URL
      @ResponseBinary varbinary(MAX) OUTPUT
        --Full binary data returned by remote HTTP server

      --@ErrorMsg nvarchar(MAX) OUTPUT
        --NULL unless an error message was encountered   

    AS
    BEGIN
      /*
      Simplified procedure to initiate an HTTP request.
      
      Does not support @Cookies, @DataToSend, @DataToSendBin, or @Headers
      If these are needed, call sqlver.sputilGetHTTP_CLR directly.
      
      (SQL does not allow us to assign default values to long paramaters such as varchar(MAX))
      */      

      DECLARE @ErrorMessage nvarchar(MAX)
      
      EXEC sqlver.sputilGetHTTP_CLR
        @URL = @URL,
        @HTTPMethod = @HTTPMethod,
        @ContentType = @ContentType,
        
        @Cookies = NULL,
        @DataToSend = NULL,
        @DataToSendBin = NULL,
        @Headers = NULL,
        
        @User = @User,
        @Password = @Password,
        @UserAgent = @UserAgent,
        
        @HTTPStatus = @HTTPStatus OUTPUT,
        @HTTPStatusText = @HTTPStatusText OUTPUT,
        @RedirURL = @RedirURL OUTPUT,  
        @ResponseBinary = @ResponseBinary OUTPUT,
        
        @ErrorMsg = @ErrorMessage OUTPUT
        
      IF NULLIF(RTRIM(@ErrorMessage), '''') IS NOT NULL BEGIN
        RAISERROR(''Error in sqlver.sputilGetHTTP: %s'', 16, 1, @ErrorMessage)
      END
      
    END    
    
    ~
        
    CREATE FUNCTION sqlver.udfURLEncode_CLR(
      @Buf nvarchar(MAX)
    )
    RETURNS nvarchar(MAX) WITH EXECUTE AS CALLER
    AS
    EXTERNAL NAME [GetHTTPCLR_SQLCLR].[Functions].[DBRUrlEncode]
    
    ~
    
    CREATE FUNCTION sqlver.udfURLDecode_CLR(
      @Buf nvarchar(MAX)
    )
    RETURNS nvarchar(MAX) WITH EXECUTE AS CALLER
    AS
    EXTERNAL NAME [GetHTTPCLR_SQLCLR].[Functions].[DBRUrlDecode]    

    '
      

  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------

using System;
using System.Net;
using System.IO;

using System.Collections.Generic;
using System.Linq;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

using System.Text.RegularExpressions;
using System.Text;

public partial class Functions
{
    
    // A local UrlEncode, because we cannot use System.Web in SQL
    // UrlEncode by David Rueter (drueter@assyst.com)
    
    // DBRUrlEncode and DBRUrlDecode are compatible with various
    // well-used percent-encoding routines, such as encodeURIComponent
    // in Javascript, and urlparse.quote in Python 
    
        
    public static SqlString DBRUrlEncode(SqlString buf)
    {        
        if (buf.IsNull) {
          return SqlString.Null;
        }
        else {    
          string paramBuf = System.Text.Encoding.UTF8.GetString(Encoding.Convert(System.Text.Encoding.Unicode, System.Text.Encoding.UTF8, buf.GetUnicodeBytes()));  
          
          // Note:  buf.ToString() seems to produce the same results as the above, 
          // but we want be very clear that we are expecting buf to contain a Unicode string
          // in UCS-2 (pre-SQL2012) or UTF-16 (SQL2012 and later), and that we expect it to
          // be converted to UTF-8
                                                                                                                
          string output = "";        
          int p = 0;             
        
          // Set up regex to find special characters not in a-z A-Z 0-9 _ .
          Regex regex = new Regex("([^a-zA-Z0-9_.])");

          Match match = regex.Match(paramBuf);
          while (match.Success)
          {
              // Output the portion of the string up to the matched special character
              output += paramBuf.Substring(p, match.Index - p);              
              
              // We do not know how many bytes this character uses--could be 1-4
              // So we convert the character to a byte array, and then walk through the array
              
              byte[] specialBytes = Encoding.UTF8.GetBytes(paramBuf[match.Index].ToString());
                            
              for(int i = 0; i < specialBytes.Length; i ++)
              {
                string hexval = "%" + specialBytes[i].ToString("X2");
                output += hexval.ToUpper();
              }
              
              p = match.Index + 1;

              match = match.NextMatch();
          }

          if (p < paramBuf.Length)
          {
              output += paramBuf.Substring(p);
          }
          
          // Now convert UTF-8 string to Unicode and return
          return System.Text.Encoding.Unicode.GetString(Encoding.Convert(System.Text.Encoding.UTF8, System.Text.Encoding.Unicode, Encoding.UTF8.GetBytes(output)));                              
          
        }
     
    }
    

    // A local UrlDecode, because we cannot use System.Web in SQL
    // UrlDecode by David Rueter (drueter@assyst.com)
    
    // DBRUrlEncode and DBRUrlDecode are compatible with various
    // well-used percent-encoding routines, such as encodeURIComponent
    // in Javascript, and urlparse.quote in Python 
    
            
    public static SqlString DBRUrlDecode(SqlString buf)
    {    
        if (buf.IsNull) {
          return SqlString.Null;
        }
        else {

          string paramBuf = System.Text.Encoding.ASCII.GetString(buf.GetNonUnicodeBytes());          
          
          // Note:  buf.ToString() seems to produce the same results as the above, 
          // but we want be very clear that weare expecting buf to contain an ASCII
          // string.  We expect the string to have all non-ASCII charecters "percent
          // encoded".
          

          //Allocate storage for the output data.  1 UTF-8 character can require up to 4 bytes
          //to store, hence the paramBufLength * 4
          
          byte[] output = new byte[paramBuf.Length * 4];
                   
          int p = 0; //position, of the incoming string
          int op = 0; //outbound position, of the outgoing array of bytes        

          //Set up a regex to find %HH matches, where HH refers to two hexidecimal digits
          Regex regex = new Regex(@"([%][A-Fa-f0-9]{2})");

          Match match = regex.Match(paramBuf);
          while (match.Success)
          {

            //Get the chunk of the string up to the %HH match
            string chunk = paramBuf.Substring(p, match.Index - p);
            
            //Convert the string chunk to an array of bytes
            byte[] chunkBytes = Encoding.UTF8.GetBytes(chunk);            
            
            //Copy the chunk to the output array of bytes, and increment the output position
            System.Buffer.BlockCopy(chunkBytes, 0, output, op, chunkBytes.Length);
            op += chunkBytes.Length;

            //Convert the HH hex digits to a byte, and write to the output array of bytes  
            string hexVal = paramBuf.Substring(match.Index + 1, 2);                    
            output[op] = Convert.ToByte(hexVal, 16);
            op += 1;
            
            //Increment the position on the incoming string      
            p = match.Index + 3;                  

            //Find next %HH match
            match = match.NextMatch();
          }

          //We are done with %HH matches
          if (p < paramBuf.Length)
          {
              //If there are remaining characters after the last %HH, copy
              //those to the output array of bytes
              
              string chunk = paramBuf.Substring(p);
              byte[] chunkBytes = Encoding.UTF8.GetBytes(chunk);
              System.Buffer.BlockCopy(chunkBytes, 0, output, op, chunkBytes.Length);
              op += chunkBytes.Length;                            
          }
          
          // The output array is longer than we need--trim it to the correct length          
          Array.Resize(ref output, op);                  
          
          // output now has the correct bytes, but needs the correct character encoding
          // Convert bytes from UTF-8 encoding that we used here, to the Unicode encoding that SQL prefers          
          output = Encoding.Convert(System.Text.Encoding.UTF8, System.Text.Encoding.Unicode, output);
          
          //Return the output bytes as an actual Unicode string from the output bytes          
          return System.Text.Encoding.Unicode.GetString(output);
                    
      }
    } 
}


public partial class Procedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void HTTPGet(
      SqlString URL,
      SqlString HTTPMethod,
      SqlString ContentType,
      ref SqlString Cookies,
      SqlString DataToSend,
      SqlBytes DataToSendBin,
      ref SqlString Headers,
      SqlString User,
      SqlString Password,
      SqlString UserAgent,

      out SqlInt32 HTTPStatus,
      out SqlString HTTPStatusText,
      out SqlString RedirURL,
      out SqlBinary ResponseBinary,
      out SqlString ErrorMsg

    )
    {
        string paramURL = Convert.ToString(URL);
        string paramHTTPMethod = Convert.ToString(HTTPMethod);
        string paramContentType = Convert.ToString(ContentType);
        string paramDataToSend = Convert.ToString(DataToSend);
        string paramHeaders = Convert.ToString(Headers);
        string paramUser = Convert.ToString(User);
        string paramPassword = Convert.ToString(Password);
        string paramUserAgent = Convert.ToString(UserAgent);

        string paramCookies = Convert.ToString(Cookies);
        string paramErrorMsg = "";


        byte[] binData = new byte[1];
        byte[] buffer = new byte[4096];

        Int32 responseStatusCode = 0;
        string responseStatusDescription = null;
        string responseRedirURL = null;

        HttpWebRequest request = null;
        HttpWebResponse response = null;

        Stream responseStream = null;

        try
        {
            request = (HttpWebRequest)WebRequest.Create(paramURL);

            //assign cookies that were passed in
            CookieContainer thisCookieContainer = new CookieContainer();
            thisCookieContainer.SetCookies(new Uri(paramURL), paramCookies);

            request.CookieContainer = thisCookieContainer;

            request.AllowAutoRedirect = false;

            if (paramUserAgent == "chrome")
            {
                //can pretend to be Chrome:            
                request.UserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36";
            }
            else
            {
                request.UserAgent = paramUserAgent;
            }

            if ((paramUser.Length > 0) && (paramPassword.Length > 0))
            {
                request.Credentials = new System.Net.NetworkCredential(paramUser, paramPassword);
            }

            if (paramHTTPMethod.Length == 0)
            {
                request.Method = "GET";
            }
            else
            {
                request.Method = paramHTTPMethod; //PUT/POST/GET/DELETE
            }

            request.ContentType = paramContentType;


            //http://stackoverflow.com/questions/4982104/c-sharp-split-return-key-value-pairs-in-an-array

            //dict thisHeaderDict = paramHeaders.Split(new string[] {"\r\n" }, StringSplitOptions.None)
            //                        .Select(x => x.Split('':''))
            //                        .ToDictionary(x => x[0], x => x[1]);

            var thisHeaderDict = paramHeaders.Split(new string[] { "\r\n" }, StringSplitOptions.None)
                                     .Select(x => x.Split('':''))
                                     .Where(x => x.Length > 1 && !String.IsNullOrEmpty(x[0].Trim()) && !String.IsNullOrEmpty(x[1].Trim()))
                                     .ToDictionary(x => x[0].Trim(), x => x[1].Trim());


            foreach (KeyValuePair<string, string> entry in thisHeaderDict)
            {
                switch (entry.Key.ToLower())
                {
                    //see: https://msdn.microsoft.com/en-us/library/system.net.httpwebrequest.headers(v=vs.110).aspx

                    case "accept":
                        request.Accept = entry.Value;
                        break;
                    case "connection":
                        if (entry.Value.EndsWith("Keep-alive"))
                        {
                            request.KeepAlive = true;
                        }
                        request.Connection = entry.Value;
                        break;
                    case "content-length":
                        request.ContentLength = int.Parse(entry.Value);
                        break;
                    case "content-type":
                        request.ContentType = entry.Value;
                        break;
                    case "expect":
                        //request.Expect = entry.Value;
                        request.ServicePoint.Expect100Continue = false;
                        break;
                    case "date":
                        //Note:  cannot be set in .Net 3.5  Defaults to system current date
                        //request.Date = entry.Value;
                        break;
                    case "host":
                        //Note:  cannot be set in .Net 3.5  Defaults to system current host information
                        //request.Host = entry.Value;
                        break;
                    case "if-modified-since":
                        request.IfModifiedSince = Convert.ToDateTime(entry.Value);
                        break;
                    case "range":
                        var rangeParts = entry.Value.Split(''-'');
                        request.AddRange(int.Parse(Regex.Replace(rangeParts[0], "[^0-9 _]", "")), int.Parse(Regex.Replace(rangeParts[1], "[^0-9 _]", "")));
                        break;
                    case "referer":
                        request.Referer = entry.Value;
                        break;
                    case "transfer-encoding":
                        request.SendChunked = true;
                        request.TransferEncoding = entry.Value;
                        break;
                    case "user-agent":
                        request.UserAgent = entry.Value;
                        break;

                    default:
                        request.Headers.Add(entry.Key, entry.Value);
                        break;
                }
            }


            if ((paramHTTPMethod.ToUpper() == "POST") && (paramDataToSend.Length > 0 || DataToSendBin.Length > 0))
            {
                paramErrorMsg = "DEBUG1";
                //convert string paramDataToSend to byte array
                byte[] binSendData;
                if (paramDataToSend.Length > 0)
                {
                    binSendData = System.Text.Encoding.Default.GetBytes(paramDataToSend);
                }
                else {
                    binSendData = DataToSendBin.Buffer;
                }

                paramErrorMsg = "DEBUG1a";

                //set ContentLength
                request.ContentLength = binSendData.Length;

                paramErrorMsg = "DEBUG1b";

                //get stream object for the request
                Stream dataStream = request.GetRequestStream();

                paramErrorMsg = "DEBUG1c";

                //write byte array to the stream
                dataStream.Write(binSendData, 0, binSendData.Length);

                paramErrorMsg = "DEBUG1d";

                //close the stream
                dataStream.Close();

                paramErrorMsg = "DEBUG1e";
            }
            else
            {
                request.ContentLength = 0;
            }

            paramErrorMsg = "DEBUG2";


            response = (HttpWebResponse)request.GetResponse();

            paramErrorMsg = "DEBUG3";

            responseStatusCode = Convert.ToInt32(response.StatusCode);
            responseStatusDescription = response.StatusDescription;

            if (responseStatusCode == 301 | responseStatusCode == 302)
            {
                responseRedirURL = response.Headers["Location"];
            }

            paramErrorMsg = "DEBUG4";


            //Merge response cookies into our CookieContainer
            foreach (Cookie cook in response.Cookies)
            {
                if (thisCookieContainer.GetCookies(new Uri(paramURL))["Name"] != null)
                {
                    thisCookieContainer.GetCookies(new Uri(paramURL))["Name"].Value = cook.Value;
                    thisCookieContainer.GetCookies(new Uri(paramURL))["Name"].Expires = cook.Expires;
                }
                else
                {
                    thisCookieContainer.Add(cook);
                }
            }


            //Write cookies out to string
            string paramCookieStr = "";
            foreach (Cookie cook in thisCookieContainer.GetCookies(new Uri(paramURL)))
            {
                paramCookieStr = paramCookieStr + cook.ToString() + "; " + "Path=/,"; // + cook.Path + ",";
            }
            if (paramCookieStr.Length > 0)
            {
                paramCookieStr = paramCookieStr.Substring(0, paramCookieStr.Length - 1);
            }
            paramCookies = paramCookieStr;


            paramHeaders = "";
            for (int i = 0; i < response.Headers.Count; ++i)
            {
                paramHeaders = paramHeaders + response.Headers.Keys[i] + ":" + response.Headers[i] + "\r\n";
            }

            paramErrorMsg = "DEBUG4b";

            responseStream = response.GetResponseStream();

            paramErrorMsg = "DEBUG5";

            using (MemoryStream memoryStream = new MemoryStream())
            {
                int count = 0;
                do
                {
                    count = responseStream.Read(buffer, 0, buffer.Length);
                    memoryStream.Write(buffer, 0, count);

                } while (count != 0);

                binData = memoryStream.ToArray();

            }

            string strData = System.Text.Encoding.Default.GetString(binData);

            paramErrorMsg = "DEBUG6";


            response.Close();
            responseStream.Dispose();

            paramErrorMsg = "DEBUG7";

        }

        catch (WebException ex)
        {
            SqlContext.Pipe.Send(ex.Message.ToString());

            response = (HttpWebResponse)ex.Response;
            responseStatusCode = Convert.ToInt32(response.StatusCode);
            responseStatusDescription = response.StatusDescription;

            //get error resopnse data
            responseStream = response.GetResponseStream();
            using (MemoryStream memoryStream = new MemoryStream())
            {
                int count = 0;
                do
                {
                    count = responseStream.Read(buffer, 0, buffer.Length);
                    memoryStream.Write(buffer, 0, count);

                } while (count != 0);

                binData = memoryStream.ToArray();

            }
            response.Close();
            responseStream.Dispose();

            //string strData = System.Text.Encoding.Default.GetString(binData);                                    

            paramErrorMsg = ex.Message.ToString();

        }


        catch (NotSupportedException ex)
        {
            paramErrorMsg = "The request cache validator indicated that the response for this request can be served from the cache; however, this request includes data to be sent to the server. Requests that send data must not use the cache. This exception can occur if you are using a custom cache validator that is incorrectly implemented.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (ProtocolViolationException ex)
        {
            paramErrorMsg = "Method is GET or HEAD, and either ContentLength is greater or equal to zero or SendChunked is true. -or- KeepAlive is true, AllowWriteStreamBuffering is false, ContentLength is -1, SendChunked is false, and Method is POST or PUT.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (InvalidOperationException ex)
        {
            paramErrorMsg = "The stream is already in use by a previous call to BeginGetResponse. -or- TransferEncoding is set to a value and SendChunked is false.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (UriFormatException ex)
        {
            paramErrorMsg = "Invalid URI: The Uri string is too long. (" + paramURL + ")(" + paramErrorMsg + ")";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        //Assign values to output parameters

        if (paramErrorMsg.StartsWith("DEBUG"))
        {
            ErrorMsg = SqlString.Null;
        }
        else
        {
            ErrorMsg = paramErrorMsg;
        }

        ResponseBinary = binData;

        HTTPStatus = responseStatusCode;
        HTTPStatusText = responseStatusDescription;

        if (responseRedirURL == null)
        {
            RedirURL = SqlString.Null;
        }
        else
        {
            RedirURL = responseRedirURL;
        }

        Cookies = paramCookies;
        Headers = paramHeaders;

    }

}

//------end of CLR Source------
'

    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'GetHTTPCLR_SQLCLR',
    @FileName = 'GetHTTPCLR_SQLCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END
GO

--*** spsysBuildCLR_FTPCLR
IF OBJECT_ID('sqlver.spsysBuildCLR_FTPCLR') IS NOT NULL DROP PROCEDURE sqlver.spsysBuildCLR_FTPCLR
GO
CREATE PROCEDURE [sqlver].[spsysBuildCLR_FTPCLR]
@FilePath varchar(1024) = 'C:\Temp\',
@FileName varchar(1024) = 'FTPCLR_SQLCLR.dll',
@BuildFromSource bit = 1
AS 
BEGIN
  DECLARE @AssemblyName sysname
  SET @AssemblyName = 'FTPCLR'
  
  IF @BuildFromSource = 1 BEGIN
    SET @FileName = REPLACE(@FileName, '.dll', '.cs')
  END
  
  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))  
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Drawing.dll')
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.sputilFTPUpload_CLR'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilFTPUpload_CLR;
    END
    
    IF OBJECT_ID(''sqlver.sputilFTPDownload_CLR'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilFTPDownload_CLR;
    END    
  '

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '         
    CREATE PROCEDURE sqlver.sputilFTPUpload_CLR
    @ftpHost nvarchar(MAX),
    @ftpUserName nvarchar(MAX),
    @ftpPassword nvarchar(MAX),
    @localFile nvarchar(MAX),
    @remoteFile nvarchar(MAX),
    @ftpContent varbinary(MAX),
    @resultText nvarchar(MAX) OUTPUT,
    @hadError bit OUTPUT
    AS
    EXTERNAL NAME [FTPCLR].[Functions].[ftpUpload]    

    ~
    CREATE PROCEDURE sqlver.sputilFTPDownload_CLR
    @ftpHost nvarchar(MAX),
    @ftpUserName nvarchar(MAX), 
    @ftpPassword nvarchar(MAX), 
    @localFile nvarchar(MAX),
    @remoteFile nvarchar(MAX),
    @ModeBinary bit,    
    @ftpContent varbinary(MAX) OUTPUT,
    @resultText nvarchar(MAX) OUTPUT,
    @hadError bit OUTPUT
    AS 
    EXTERNAL NAME [FTPCLR].[Functions].[ftpDownload]        
  '  
       
  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
  //------start of CLR Source------
  using System;
  using System.Data;
  using System.Data.SqlClient;
  using System.Data.SqlTypes;
  using Microsoft.SqlServer.Server;
  using System.IO;
  using System.Xml;
  using System.Linq;
  using System.Xml.Linq;
  using System.Security;
  
  using System.Net;
  using System.Text;

  //from AssemblyInfo.cs
  using System.Reflection;
  using System.Runtime.CompilerServices;
  using System.Runtime.InteropServices;
  using System.Data.Sql;

  // General Information about an assembly is controlled through the following
  // set of attributes. Change these attribute values to modify the information
  // associated with an assembly.
  [assembly: AssemblyTitle("FTPCLR")]
  [assembly: AssemblyDescription("Allow FTP upload and download via SQL CLR Functions.  Generated automatically by sqlver.spsysRebuildCLR_FTPCLR")]
  [assembly: AssemblyConfiguration("")]
  [assembly: AssemblyCompany("David Rueter")]
  [assembly: AssemblyProduct("FTPCLR")]
  [assembly: AssemblyCopyright("public domain")]
  [assembly: AssemblyTrademark("drueter@assyst.com")]
  [assembly: AssemblyCulture("")]

  [assembly: ComVisible(false)]

  //
  // Version information for an assembly consists of the following four values:
  //
  //      Major Version
  //      Minor Version
  //      Build Number
  //      Revision
  //
  // You can specify all the values or you can default the Revision and Build Numbers
  // by using the ''*'' as shown below:
  [assembly: AssemblyVersion("1.0.*")]


  [assembly: AllowPartiallyTrustedCallers]

  public partial class Functions
  {
       [Microsoft.SqlServer.Server.SqlFunction]
      public static void ftpUpload(
          SqlString ftpHost,
          SqlString ftpUserName,
          SqlString ftpPassword,
          SqlString localFile,
          SqlString remoteFile,
          SqlBytes ftpContent,          
          out SqlString resultText,
          out SqlBoolean hadError)
      {
          resultText = null;
          hadError = SqlBoolean.False;
          
          
          //Fix remote file names
          if (remoteFile.ToString().Substring(1, 1) != "/")
              remoteFile = "/" + remoteFile.ToString();
          remoteFile = remoteFile.ToString().Replace(@"\", @"/");

          try
          {
              // Get the object used to communicate with the server.
              FtpWebRequest request = (FtpWebRequest)WebRequest.Create(new Uri("ftp://" + ftpHost.ToString() + remoteFile.ToString()));
              request.Method = WebRequestMethods.Ftp.UploadFile;

              // Logon to the FTP Server with the given credidentials 
              request.Credentials = new NetworkCredential(ftpUserName.ToString(), ftpPassword.ToString());

              byte[] fileContents = null;
               
              if (!localFile.IsNull) {
                // Copy the contents of the file to the request stream.
                StreamReader sourceStream = new StreamReader(localFile.ToString());                            
                fileContents = Encoding.UTF8.GetBytes(sourceStream.ReadToEnd());
                request.ContentLength = fileContents.Length;                
                sourceStream.Close();
              }
              else {
                if (!ftpContent.IsNull) {
                  MemoryStream sourceStream = new MemoryStream();

                  int buffersize = 2048;
                  int readCount;
                  byte[] buffer = new byte[buffersize];
                
                  readCount = ftpContent.Stream.Read(buffer, 0, buffersize);
                  while (readCount > 0)
                  {
                    sourceStream.Write(buffer, 0, readCount);
                    readCount = ftpContent.Stream.Read(buffer, 0, buffersize);
                  }
                
                  fileContents = sourceStream.ToArray(); 
                  request.ContentLength = fileContents.Length;                                         
                  sourceStream.Close(); 
                }             
              }                           
              
              if (fileContents != null) {
                Stream requestStream = request.GetRequestStream();
                requestStream.Write(fileContents, 0, fileContents.Length);
                requestStream.Close();

                //Store the response
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                //Return the response
                resultText = response.StatusDescription.ToString();
              }
              else {
                resultText = "Nothing to upload.";
              }
          }
          catch (Exception ex)
          {
              //Return the exception  
              hadError = SqlBoolean.True;        
              resultText = ex.Message;
          }
      }
       
  
      [Microsoft.SqlServer.Server.SqlFunction]
      public static void ftpDownload(
        SqlString ftpHost,
        SqlString ftpUserName,
        SqlString ftpPassword,
        SqlString localFile,
        SqlString remoteFile,
        SqlBoolean ftpModeBinary,
        out SqlBytes ftpContent,        
        out SqlString resultText,
        out SqlBoolean hadError
      )
      {
          ftpContent = null;
          resultText = null;
          hadError = SqlBoolean.False;
          
          //Fix remote file names
          if (remoteFile.ToString().Substring(1, 1) != "/")
              remoteFile = "/" + remoteFile.ToString();
          remoteFile = remoteFile.ToString().Replace(@"\",@"/");

          FtpWebRequest reqFtp;
          try
          {
              reqFtp = (FtpWebRequest)FtpWebRequest.Create(new Uri("ftp://" + ftpHost.ToString() + remoteFile.ToString()));
              reqFtp.Method = WebRequestMethods.Ftp.DownloadFile;
              
              //Set Transfer Mode
              reqFtp.UseBinary = ftpModeBinary.IsTrue;

              //Set Logon Credidentials
              reqFtp.Credentials = new NetworkCredential(ftpUserName.ToString(), ftpPassword.ToString());
              FtpWebResponse response = (FtpWebResponse)reqFtp.GetResponse();
              
              //Create Stream to save file
              Stream ftpStream = response.GetResponseStream();

              if (!localFile.IsNull) {
                FileStream outputStream = new FileStream(localFile.ToString(), FileMode.Create);
                
                int buffersize = 2048;
                int readCount;
                byte[] buffer = new byte[buffersize];
                
                readCount = ftpStream.Read(buffer, 0, buffersize);
                while (readCount > 0) {
                    outputStream.Write(buffer, 0, readCount);
                    readCount = ftpStream.Read(buffer, 0, buffersize);
                }  
                outputStream.Close();                                           
              }
              else
              {              
                //return the downloaded data in ftpContent parameter
                
                MemoryStream outputStream = new MemoryStream();
   
                int buffersize = 2048;
                int readCount;
                byte[] buffer = new byte[buffersize];
                
                readCount = ftpStream.Read(buffer, 0, buffersize);
                while (readCount > 0)
                { //Save data to file.
                    outputStream.Write(buffer, 0, readCount);
                    readCount = ftpStream.Read(buffer, 0, buffersize);
                }

                //Return the results
                ftpContent = (new SqlBytes(outputStream.ToArray()));
                
                outputStream.Close();
              }
              
              
              
              resultText = response.StatusDescription.ToString();
              
              
              //Tidy up...                           
              ftpStream.Close();
              response.Close();
          }
          catch (Exception ex)
          {
              // Return the exception
              ftpContent = null;
              hadError = SqlBoolean.True;
              resultText = ex.Message;              
          }
                
      }
  };
  //------end of CLR Source------  '

    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = @AssemblyName,
    @FileName = @FileName,
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END
GO

--*** sputilExecInOtherConnection
IF OBJECT_ID('sqlver.sputilExecInOtherConnection') IS NOT NULL DROP PROCEDURE sqlver.sputilExecInOtherConnection
GO
CREATE PROCEDURE [sqlver].[sputilExecInOtherConnection]
@SQLCommand nvarchar(MAX),
@Server sysname = 'localhost',
@Username sysname = 'sqlverLogger',
@Password sysname = 'sqlverLoggerPW'
AS 
BEGIN
  /*
  This procedure is designed to allow a caller to provide a message that will be written to an error log table,
  and allow the caller to call it within a transaction.  The provided message will be persisted to the
  error log table even if the transaction is rolled back.
  
  To accomplish this, this procedure utilizes ADO to establish a second database connection (outside
  the transaction context) back into the database to execute the SQL in @SQL.
  */

  DECLARE @ConnStr varchar(MAX)
    --connection string for ADO to use to access the database
  SET @ConnStr = 'Provider=SQLNCLI10; DataTypeCompatibility=80; Server=' + @Server + '; Database=' + DB_NAME() + '; Uid=' + @Username + '; Pwd=' + @Password + ';'
  
  DECLARE @ObjCn int 
    --ADO Connection object  
  DECLARE @ObjRS int    
    --ADO Recordset object returned
    
  DECLARE @RecordCount int   
    --Maximum records to be returned
  SET @RecordCount = 0
   
  DECLARE @ExecOptions int
    --Execute options:  0x80 means to return no records (adExecuteNoRecords) + 0x01 means CommandText is to be evaluted as text
  SET @ExecOptions = 0x81
      
  DECLARE @LastResultCode int = NULL 
     --Last result code returned by an sp_OAxxx procedure.  Will be 0 unless an error code was encountered.
  DECLARE @ErrSource varchar(512)
    --Returned if a COM error is encounterd
  DECLARE @ErrMsg varchar(512)
    --Returned if a COM error is encountered
  
  DECLARE @ErrorMessage varchar(MAX) = NULL
    --our formatted error message


  SET @ErrorMessage = NULL
  SET @LastResultCode = 0
      
    
  BEGIN TRY
    EXEC @LastResultCode = sp_OACreate 'ADODB.Connection', @ObjCn OUT 
    IF @LastResultCode <> 0 BEGIN
      EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
    END
  END TRY
  BEGIN CATCH
    SET @ErrorMessage = ERROR_MESSAGE()
  END CATCH
  
  
   BEGIN TRY  
    IF @LastResultCode = 0 BEGIN
     
      EXEC @LastResultCode = sp_OAMethod @ObjCn, 'Open', NULL, @ConnStr
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END                
    END  
  END TRY
  BEGIN CATCH
    SET @ErrorMessage = ERROR_MESSAGE()
  END CATCH

    
  IF @LastResultCode = 0 BEGIN
    EXEC @LastResultCode = sp_OAMethod @ObjCn, 'Execute', @ObjRS OUTPUT, @SQLCommand, @ExecOptions
    IF @LastResultCode <> 0 BEGIN
      EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
    END                
  END
    
  IF @ObjRS IS NOT NULL BEGIN
    BEGIN TRY
      EXEC sp_OADestroy @ObjCn  
    END TRY
    BEGIN CATCH
      --not much we can do...
      SET @LastResultCode = 0
    END CATCH
  END
    
  IF @ObjCn= 1 BEGIN
    BEGIN TRY
      EXEC sp_OADestroy @ObjCn
    END TRY
    BEGIN CATCH
      --not much we can do...
      SET @LastResultCode = 0
    END CATCH
  END    
    
  IF ((@LastResultCode <> 0) OR (@ErrorMessage IS NOT NULL)) BEGIN
    SET @ErrorMessage = 'Error in sqlver.sputilExecInOtherConnection' + ISNULL(': ' + @ErrMsg, '')
    RAISERROR(@ErrorMessage, 16, 1)
    RETURN(2001)
  END
  
END
GO


--*** udfMath_deg2rad
IF OBJECT_ID('sqlver.udfMath_deg2rad') IS NOT NULL DROP FUNCTION sqlver.udfMath_deg2rad
GO
CREATE FUNCTION [sqlver].[udfMath_deg2rad](
@deg float)
RETURNS float
AS 
BEGIN
  DECLARE @Result float
  SET @Result = @deg * PI() / 180
  RETURN @Result
END
GO

--*** udfMath_rad2deg
IF OBJECT_ID('sqlver.udfMath_rad2deg') IS NOT NULL DROP FUNCTION sqlver.udfMath_rad2deg
GO
CREATE FUNCTION [sqlver].[udfMath_rad2deg](
@rad float)
RETURNS float
AS 
BEGIN
  DECLARE @Result float
  SET @Result = @rad * 180 / PI()
  RETURN @Result
END
GO

--*** udfDistanceFromCoordinates
IF OBJECT_ID('sqlver.udfDistanceFromCoordinates') IS NOT NULL DROP FUNCTION sqlver.udfDistanceFromCoordinates
GO
CREATE FUNCTION [sqlver].[udfDistanceFromCoordinates](
@LatitudeA float,
@LongitudeA float,
@LatitudeB float,
@LongitudeB float,
@Unit char)
RETURNS float
AS 
BEGIN
  DECLARE @Result float

  --This routine calculates the distance between two points
  --(given the  latitude/longitude of those points). It is being 
  --used to calculate  distance between two ZIP Codes or Postal 
  --Codes using our    ZIPCodeWorld(TM) and PostalCodeWorld(TM)
  -- products.

  --Definitions                                                    
  -- South latitudes are negative, east longitudes are positive           

  --Passed to function                                                   
  --@LatitudeA , @LongitudeA = Latitude and Longitude of point 1 
  --(in decimal degrees) 
  --@LatitudeB, @LongitudeB = Latitude and Longitude of point 2 
  --(in decimal degrees) 
  --unit = the unit you desire for results   
  --where 'M' is statute miles (default)
  --''K' is kilometers    --
  --'N' is nautical miles  
  --United States ZIP Code/ Canadian Postal Code databases with 
  --latitude & longitude are available at 
  --http//www.zipcodeworld.com               
  --For enquiries, please contact sales@zipcodeworld.com   

  --Official Web site http//www.zipcodeworld.com                         
  --Hexa Software Development Center ¸ All Rights Reserved 2003            

  DECLARE @Theta float
  DECLARE @Dist float
  SET @Result = NULL
  IF
    @LatitudeA IS NULL OR
    @LongitudeA IS NULL OR 
    @LatitudeB IS NULL OR
    @LongitudeB IS NULL RETURN @Result

  IF ((@LatitudeA = @LatitudeB) AND (@LongitudeA = @LongitudeB)) RETURN 0

  SET @Theta = @LongitudeA - @LongitudeB
  SET @Dist = SIN(sqlver.udfMath_deg2rad(@LatitudeA )) * SIN(sqlver.udfMath_deg2rad(@LatitudeB)) + COS(sqlver.udfMath_deg2rad(@LatitudeA )) * COS(sqlver.udfMath_deg2rad(@LatitudeB)) * COS(sqlver.udfMath_deg2rad(@Theta))
  SET @Dist = ACOS(@Dist)
  SET @Dist = sqlver.udfMath_rad2deg(@Dist)
  SET @Result = @Dist * 60 * 1.1515
  IF UPPER(@Unit) = 'K' SET @Result = @Result * 1.609344
  ELSE IF UPPER(@Unit) = 'N' SET @Result = @Result * 0.8684
 
  RETURN @Result
END
GO

--*** spgetSQLProgress
IF OBJECT_ID('sqlver.spgetSQLProgress') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLProgress
GO
CREATE PROCEDURE [sqlver].[spgetSQLProgress]
AS
BEGIN
  SELECT
    er.session_id, er.command, er.percent_complete
  FROM
    sys.dm_exec_requests er
  WHERE
    er.command like 'DBCC%'
END
GO


--*** spgetDBsWithSQLVer
IF OBJECT_ID('sqlver.spgetDBsWithSQLVer') IS NOT NULL DROP PROCEDURE sqlver.spgetDBsWithSQLVer
GO
CREATE PROCEDURE [sqlver].[spgetDBsWithSQLVer]
AS
BEGIN
  CREATE TABLE #DBs (DBName sysname)
  
  DECLARE @SQL varchar(MAX) 
  SELECT @SQL = 'USE ? INSERT INTO #DBs (DBName) SELECT DB_NAME() FROM sys.schemas WHERE name = ''sqlver'''
  EXEC sp_MSforeachdb @SQL
  
  SELECT DBName FROM #DBs
  WHERE DBName <> 'msdb'
  ORDER BY DBName  
  
  DROP TABLE #DBs
END
GO

--*** spgetAllDBsBackupStatus
IF OBJECT_ID('sqlver.spgetAllDBsBackupStatus') IS NOT NULL DROP PROCEDURE sqlver.spgetAllDBsBackupStatus
GO
CREATE PROCEDURE [sqlver].[spgetAllDBsBackupStatus]
AS
BEGIN
  SET NOCOUNT ON
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  
  CREATE TABLE #SQLVerDBs (DBName sysname)
  
  CREATE TABLE #Log (DateLogged datetime, DBName sysname, Msg varchar(MAX))
  CREATE TABLE #Progress (DBName sysname, session_id int, command NVARCHAR(MAX), percent_complete float)
 
  INSERT INTO #SqlVerDBs(DBName) EXEC sqlver.spgetDBsWithSQLVer
 
  DECLARE @SQL varchar(MAX) 
  DECLARE @ThisDB sysname
  
  DECLARE curThis CURSOR LOCAL STATIC FOR SELECT DBName FROM #SQLVerDBs
  OPEN curThis
  FETCH curThis INTO @ThisDB
  
  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @SQL = 'INSERT INTO #Log (DBName, DateLogged, Msg) ' + 
      'SELECT ''' + @ThisDB + ''', DateLogged, Msg FROM ' + @ThisDB + '.sqlver.tblSysRTLog ' +
      'WHERE DateLogged > CAST(GETDATE() AS Date) AND ' +
      'Msg LIKE ''sqlver.spsysBackupFull%'''
      
    EXEC(@SQL)
    FETCH curThis INTO @ThisDB      
  END
  
  CLOSE curThis
  DEALLOCATE curThis

  INSERT INTO #Progress (DBName, session_id, command, percent_complete)
  SELECT DB_NAME(er.database_id), er.session_id, er.command, er.percent_complete
  FROM sys.dm_exec_requests er
  WHERE er.command like 'DBCC%'
  
  SELECT * FROM #Log ORDER BY DateLogged DESC
  SELECT * FROM #Progress ORDER BY DBName
  
  DROP TABLE #Log
  DROP TABLE #Progress
END
GO


--*** spgetSQLLocks
IF OBJECT_ID('sqlver.spgetSQLLocks') IS NOT NULL DROP PROCEDURE sqlver.spgetSQLLocks
GO
CREATE PROCEDURE [sqlver].[spgetSQLLocks]
@ExclusiveOnly bit = 1
AS
BEGIN
  SET NOCOUNT ON
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    
  CREATE TABLE #Locks (
	  [DBName] [nvarchar](256) NULL,
	  [ObjectName] [sysname] NULL,
	  [IndexName] [sysname] NULL,
	  [Executed] [nvarchar](MAX) NULL,
	  [ExecutedBy] [nchar](256) NULL,
	  [hostname] [nchar](256) NULL,
	  [loginame] [nchar](256) NULL,
	  [nt_domain] [nchar](256) NULL,
	  [nt_username] [nchar](256) NULL,
	  [blocked] [smallint] NULL,
	  [cpu] [int] NULL,
	  [physical_io] [bigint] NULL,
	  [resource_type] [nvarchar](120) NULL,
	  [resource_subtype] [nvarchar](120) NULL,
	  [resource_database_id] [int] NULL,
	  [resource_description] [nvarchar](512) NULL,
	  [resource_associated_entity_id] [bigint] NULL,
	  [resource_lock_partition] [int] NULL,
	  [request_mode] [nvarchar](120) NULL,
	  [request_type] [nvarchar](120) NULL,
	  [request_status] [nvarchar](120) NULL,
	  [request_reference_count] [smallint] NULL,
	  [request_lifetime] [int] NULL,
	  [request_session_id] [int] NULL,
	  [request_exec_context_id] [int] NULL,
	  [request_request_id] [int] NULL,
	  [request_owner_type] [nvarchar](120) NULL,
	  [request_owner_id] [bigint] NULL,
	  [request_owner_guid] [uniqueidentifier] NULL,
	  [request_owner_lockspace_id] [nvarchar](64) NULL,
	  [lock_owner_address] [varbinary](8) NULL
  )
  
  INSERT INTO #Locks
  SELECT
    DB_NAME(l.resource_database_id) AS DBName,  
    CAST(NULL AS sysname) AS ObjectName,
    CAST(NULL AS sysname) AS IndexName,
      COALESCE(
      OBJECT_NAME(st.objectid, l.resource_database_id),
      st.text) AS Executed,   
    sp.program_name AS ExecutedBy,     
    sp.hostname,
    sp.loginame,
    sp.nt_domain,
    sp.nt_username,
    sp.blocked,
    sp.cpu,
    sp.physical_io,
    l.*
  FROM
    sys.dm_tran_locks L
    JOIN sys.dm_exec_sessions ES ON ES.session_id = L.request_session_id
    JOIN sys.dm_tran_session_transactions TST ON ES.session_id = TST.session_id
    JOIN sys.dm_tran_active_transactions AT ON TST.transaction_id = AT.transaction_id
    JOIN sys.dm_exec_connections CN ON CN.session_id = ES.session_id
    CROSS APPLY sys.dm_exec_sql_text(CN.most_recent_sql_handle) AS ST  
  
    JOIN sys.sysprocesses sp ON
      l.request_session_id = sp.spid

  WHERE
    l.resource_database_id<> DB_ID('tempdb') AND
    (@ExclusiveOnly = 0 OR request_mode = 'X')
    
  
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT DISTINCT
    l.resource_database_id
  FROM
    #Locks l
       
  DECLARE @ThisDBID int
  DECLARE @SQL nvarchar(MAX)
  
  OPEN curThis
  FETCH curThis INTO @ThisDBID
  
  WHILE @@FETCH_STATUS = 0 BEGIN
  
    SET @SQL = 'UPDATE l
    SET
      ObjectName = COALESCE(o1.name, o2.name),
      IndexName = ix1.name
    FROM
      #Locks l
      LEFT JOIN ' + DB_NAME(@ThisDBID) + '.sys.partitions p ON
        p.hobt_id = 
          CASE WHEN l.resource_type IN (''PAGE'', ''KEY'', ''RID'', ''HOBT'') THEN l.resource_associated_entity_id END
      LEFT JOIN ' + DB_NAME(@ThisDBID) + '.sys.objects o1 ON
        p.object_id = o1.object_id
      LEFT JOIN ' + DB_NAME(@ThisDBID) + '.sys.indexes ix1 ON
        o1.object_id = ix1.object_id AND
        p.index_id = ix1.index_id
        
      LEFT JOIN sys.objects o2 ON
        o2.object_id = 
          CASE WHEN l.resource_type = ''OBJECT'' THEN l.resource_associated_entity_id END
    WHERE
      l.resource_database_id = ' + CAST(@ThisDBID AS varchar(100))
      
    EXEC(@SQL)     
      
    FETCH curThis INTO @ThisDBID
  END
  CLOSE curThis
  DEALLOCATE curThis


  SELECT DISTINCT 
    l.ExecutedBy,
    CAST(NULL AS sysname) AS JobName
  INTO #AgentJobs
  FROM
    #Locks l
  WHERE
    l.ExecutedBy LIKE 'SQLAgent%'
    
    
  UPDATE aj
  SET
    JobName = 'SQLAgent ' + sj.name
  FROM
    #AgentJobs aj
    
    LEFT JOIN msdb.dbo.sysjobs sj ON
      sj.job_id = 
        CAST(
          '0x' +
          RTRIM(sqlver.udfParseValue(sqlver.udfParseValue(aj.ExecutedBy, 2, 'x'), 1, ':'))
        AS varbinary(128))
  
 
   UPDATE l
   SET
     ExecutedBy = aj.JobName
   FROM
     #Locks l
     JOIN #AgentJobs aj ON
       l.ExecutedBy = aj.ExecutedBy

    
  SELECT
    l.DBName,
    l.ObjectName,
    l.request_session_id AS SPID,  
    l.Executed,  
    l.ExecutedBy,    
    l.IndexName,
    l.request_Mode AS Mode,
    l.resource_type AS ResourceType,
    l.blocked AS Blocked,
    l.loginame AS LoginName,
    l.hostname AS Hostname,
    l.cpu AS CPU,
    l.physical_io AS PhysicalIO,
    l.resource_associated_entity_id
  FROM
     #Locks l
  ORDER BY
    l.DBName,
    l.request_session_id
  
  DROP TABLE #Locks    

END
GO


--*** dtgLogSchemaChanges
IF EXISTS (SELECT * FROM sys.triggers WHERE parent_class_desc = 'DATABASE' AND name = N'dtgLogSchemaChanges') BEGIN
  DISABLE TRIGGER [dtgLogSchemaChanges] ON DATABASE
  DROP TRIGGER [dtgLogSchemaChanges] ON DATABASE
END
GO
SET ANSI_NULLS ON
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dtgLogSchemaChanges] ON DATABASE
FOR
  create_procedure, alter_procedure, drop_procedure,
  create_table, alter_table, drop_table,
  create_view, alter_view, drop_view,
  create_function, alter_function, drop_function,
  create_index, alter_index, drop_index,
  create_trigger, alter_trigger, drop_trigger,
  create_synonym, drop_synonym
AS
BEGIN
  --Logs schema changes to sqlver.tblSchemaLog
  SET NOCOUNT ON
  
  DECLARE @Debug bit
  SET @Debug = 0
  
  DECLARE @Visible bit
  SET @Visible = 1
  
  IF @Debug = 1 BEGIN
    PRINT 'dtgLogSchemaChanges: Starting'
  END
  
  BEGIN TRY
    --retrieve trigger event data
    DECLARE @EventData xml
    SET @EventData = EVENTDATA()
    
    DECLARE @SkipLogging bit
    DECLARE @IsEncrypted bit
    
    DECLARE @DatabaseName sysname
    DECLARE @SchemaName sysname
    DECLARE @ObjectName sysname
    DECLARE @EventType varchar(50)
    DECLARE @ObjectType varchar(25)
    DECLARE @QualifiedName varchar(775)
    DECLARE @ObjectId int
    DECLARE @SQLFromEvent nvarchar(MAX)
    DECLARE @SQLForHash nvarchar(MAX)
    DECLARE @SQLWithStrippedComment nvarchar(MAX)
    
    DECLARE @SPID smallint
    DECLARE @LoginName sysname
    DECLARE @EventDate datetime
    
    DECLARE @Comments nvarchar(MAX)
    
    DECLARE @HasEmbeddedComment bit
    SET @HasEmbeddedComment = 0
    
    DECLARE @NeedExec bit
    SET @NeedExec = 0
    
    DECLARE @Buf nvarchar(MAX)
    DECLARE @P int
    
    --grab values from event XML
    SET @ObjectType = @EventData.value('(/EVENT_INSTANCE/ObjectType)[1]', 'varchar(25)')
    SET @DatabaseName = @EventData.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'sysname')
    SET @SchemaName = @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname')
    SET @SPID = @EventData.value('(/EVENT_INSTANCE/SPID)[1]', 'smallint');
    
    SET @ObjectName = CASE
                        WHEN @ObjectType = 'INDEX' THEN @EventData.value('(/EVENT_INSTANCE/TargetObjectName)[1]', 'sysname')
                        ELSE @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname')
                      END

    SET @EventType = @EventData.value('(/EVENT_INSTANCE/EventType)[1]', 'varchar(50)')
    SET @LoginName = @EventData.value('(/EVENT_INSTANCE/LoginName)[1]', 'sysname')
    SET @EventDate = COALESCE(@EventData.value('(/EVENT_INSTANCE/PostTime)[1]', 'datetime'), GETDATE())
    
    SET @SQLFromEvent = @EventData.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'nvarchar(MAX)')
    SET @QualifiedName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
    SET @ObjectId = OBJECT_ID(@QualifiedName) 
    
    IF @SQLFromEvent LIKE 'ALTER INDEX%' AND
       PATINDEX('%REBUILD WITH%', @SQLFromEvent) > 0 BEGIN
      SET @SkipLogging = 1
      --We don't want to log index rebuilds
    END                   
    
    IF @ObjectName = 'dtgLogSchemaChanges' BEGIN
      IF @Debug = 1 PRINT 'Exiting dtgLogSchemaChanges ON ' + @SchemaName + '.' + @ObjectName + ' because @SkipLogging = 1'
      RETURN
    END
   
    SET @IsEncrypted = CASE WHEN @SQLFromEvent = '--ENCRYPTED--' THEN 1 ELSE 0 END
    IF @IsEncrypted = 1 BEGIN
      --We will assume that the DDL for the object is being updated.
      --Since we can't calculate a hash on the actual statement, we'll calculate a
      --hash on a new GUID to force a unique hash.  This way this event will
      --be treated as a new update that needs to be logged.
      SET @SQLFromEvent = CAST(NEWID() AS nvarchar(MAX))
    END
    
    SET @P = PATINDEX('%/*/%', @SQLFromEvent)
     
    IF @P > 0 BEGIN      
      DECLARE @SQLLen int
      SET @SQLLen = LEN(@SQLFromEvent + 'x') - 1 
      SET @Buf = RIGHT(@SQLFromEvent, @SQLLen - @P + 1 - LEN('/*/'))   
      SET @Buf = LEFT(@Buf, PATINDEX('%*/%', @Buf) - 1)
      SET @Comments = ISNULL(@Comments + ' | ', '') + ISNULL(NULLIF(RTRIM(@Buf), ''), '')

      SET @HasEmbeddedComment = 1
      
      SET @SQLWithStrippedComment = LEFT(@SQLFromEvent, @P - 1) + SUBSTRING(@SQLFromEvent, @P + LEN(@Buf) +  7, @SQLLen)        
    END
        
         
    IF @ObjectType = 'TABLE' BEGIN
      --Retrieve the complete definition of the table
      SET @SQLForHash = sqlver.udfScriptTable(@SchemaName, @ObjectName)
    END
    ELSE BEGIN
      --Use the SQL that was in the statement
      IF @HasEmbeddedComment = 1 BEGIN
        SET @SQLForHash = @SQLWithStrippedComment
      END
      ELSE BEGIN
        SET @SQLForHash = @SQLFromEvent
      END
    END
    SET @SQLForHash = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(
      REPLACE(
        REPLACE(@SQLForHash, 'ALTER ' + @ObjectType, 'CREATE ' + @ObjectType), --always base hash on the create statement
        'CREATE ' + @ObjectType, 'CREATE ' + @ObjectType   --to ensure case of object type is consistent
      )
    ))
    
    
    DECLARE @LastSchemaLogId int
    DECLARE @SchemaLogId int
    
    DECLARE @ManifestId int

    DECLARE @CalculatedHash varbinary(128)
    DECLARE @StoredHash varbinary(128)
    DECLARE @StoredHashManifest varbinary(128)
    
    
    SET @IsEncrypted = CASE WHEN @SQLFromEvent = '--ENCRYPTED--' THEN 1 ELSE 0 END
    
    
    --Retrieve manifest data
    IF @Debug = 1 BEGIN
      PRINT 'dtgLogSchemaChanges: Retrieving from sqlver.tblSchemaManifest'
    END
  
    SELECT
      @ManifestId = m.SchemaManifestId,
      @StoredHashManifest = m.CurrentHash,
      @SkipLogging = m.SkipLogging
    FROM
      sqlver.tblSchemaManifest m
    WHERE
      m.SchemaName = @SchemaName AND
      m.ObjectName = @ObjectName
      

    SET @SkipLogging = ISNULL(@SkipLogging, 0)
    
    SELECT
      @LastSchemaLogId = MAX(schl.SchemaLogId)
    FROM
      sqlver.tblSchemaLog schl
    WHERE
      schl.SchemaName = @SchemaName AND
      schl.ObjectName = @ObjectName        
    
    
    IF @SkipLogging = 0 BEGIN    
      IF @SQLWithStrippedComment IS NOT NULL AND @ObjectType <> 'TABLE' BEGIN
        SET @NeedExec = 1
      END
          
      SELECT @StoredHash = schl.Hash
      FROM
        sqlver.tblSchemaLog schl
      WHERE
        @LastSchemaLogId = schl.SchemaLogId
        
      SET @StoredHash = COALESCE(@StoredHash, @StoredHashManifest)
      
      
      IF @Debug = 1 BEGIN
        PRINT 'dtgLogSchemaChanges: Calculating hash'
      END
      
      SET @CalculatedHash =  sqlver.udfHashBytesNMax('SHA1', @SQLForHash)
      
      IF (@CalculatedHash = @StoredHash) BEGIN
        --Hash matches.  Nothing has changed.
        IF @Debug = 1 BEGIN
          PRINT 'dtgLogSchemaChanges: Hash matches.  Nothing has changed.'
        END
        
        IF @Comments IS NOT NULL BEGIN
          UPDATE sqlver.tblSchemaLog
          SET
            Comments = Comments + ' | ' + @Comments
          WHERE
            SchemaLogId = @LastSchemaLogId
        END      
                    
        SET @SkipLogging = 1
      END


      IF @ManifestId IS NULL BEGIN
        IF @Debug = 1 BEGIN
          PRINT 'dtgLogSchemaChanges: Inserting into sqlver.tblSchemaManifest'
        END
        
        INSERT INTO sqlver.tblSchemaManifest(  
          ObjectName,
          SchemaName,
          DatabaseName,  
          OrigDefinition,
          DateAppeared,
          CreatedByLoginName,
          DateUpdated,
          OrigHash,
          CurrentHash,
          IsEncrypted,
          StillExists,
          SkipLogging,
          Comments             
        )
        VALUES (
          @ObjectName,
          @SchemaName,
          @DatabaseName,  
          @SQLForHash,
          @EventDate,
          @LoginName,
          @EventDate,
          @CalculatedHash,
          @CalculatedHash,
          @IsEncrypted,
          1,
          @NeedExec,
          @Comments 
        )
        
        SET @ManifestId = SCOPE_IDENTITY()  
      END
      ELSE BEGIN
        IF @Debug = 1 BEGIN
          PRINT 'dtgLogSchemaChanges: Updating sqlver.tblSchemaManifest'
        END
        
        UPDATE sqlver.tblSchemaManifest
        SET
          DateUpdated = @EventDate,
          CurrentHash = @CalculatedHash,
          IsEncrypted = @IsEncrypted,
          StillExists = CASE WHEN OBJECT_ID(@SchemaName + '.' + @ObjectName) IS NOT NULL THEN 1 ELSE 0 END,
          SkipLogging = @NeedExec
        WHERE
          SchemaManifestId = @ManifestId               
      END            
    
      
      IF @SkipLogging = 0 BEGIN
        IF @Debug = 1 BEGIN
          PRINT 'dtgLogSchemaChanges: Inserting into sqlver.tblSchemaLog'
        END      
        
        INSERT INTO sqlver.tblSchemaLog (
          SPID,
          EventType,
          ObjectName,
          SchemaName,
          DatabaseName, 
          ObjectType,
          SQLCommand,
          EventDate,
          LoginName,
          EventData,
          Comments,
          Hash
        )
        VALUES (
          COALESCE(@SPID, @@SPID),
          @EventType,
          @ObjectName,
          @SchemaName,
          @DatabaseName, 
          @ObjectType,
          @SQLFromEvent,
          @EventDate,
          @LoginName,
          @EventData,
          @Comments,
          @CalculatedHash                
        )
    
        SET @SchemaLogId = SCOPE_IDENTITY()
        SET @StoredHash = @CalculatedHash            
        
      END    
      
      IF @NeedExec = 1 BEGIN        
        SET @SQLFromEvent = REPLACE(@SQLFromEvent, 'CREATE ' + @ObjectType, 'ALTER ' + @ObjectType)
        EXEC (@SQLFromEvent)   
        
        UPDATE sqlver.tblSchemaManifest
        SET
          SkipLogging = 0
        WHERE
          SchemaManifestId = @ManifestId             
      END      
    END  
    
    
    IF @Visible = 1 BEGIN      
      PRINT 'Changes to ' + @DatabaseName + '.' + @SchemaName + '.' + @ObjectName + ' successfully logged by SQLVer'      
    END
  END TRY
  BEGIN CATCH
    PRINT 'Error logging DDL changes in database trigger dtgLogSchemaChanges: ' + ERROR_MESSAGE()
    PRINT 'Your DDL statement may have been successfully processed, but changes were not logged by the version tracking system.'
  END CATCH
  
  IF @Debug = 1 BEGIN
    PRINT 'dtgLogSchemaChanges: Finished'
  END  
  
END
GO
DISABLE TRIGGER [dtgLogSchemaChanges] ON DATABASE
GO
ENABLE TRIGGER [dtgLogSchemaChanges] ON DATABASE
GO

--*** LOGIN sqlverLogger 
IF NOT EXISTS (SELECT sid FROM sys.syslogins WHERE name = 'sqlverLogger') BEGIN
  CREATE LOGIN [sqlverLogger] WITH PASSWORD=N'sqlverLoggerPW', DEFAULT_DATABASE=[master], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
END
GO
ALTER LOGIN [sqlverLogger] ENABLE
GO

--*** USER sqlverLogger 
IF NOT EXISTS (SELECT uid FROM sys.sysusers WHERE name = 'sqlverLogger') BEGIN
  CREATE USER [sqlverLogger] FOR LOGIN [sqlverLogger] WITH DEFAULT_SCHEMA=[sqlver]
END
GO
GRANT EXEC ON [sqlver].[spinsSysRTLog] TO [sqlverLogger]
GO

  IF NOT EXISTS(
    SELECT 
      syn.name
    FROM
      sys.synonyms syn
      JOIN sys.objects obj ON
        syn.object_id = obj.object_id 
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
    WHERE
      sch.name = 'sqlver' AND
      syn.name = 'find'
  ) BEGIN  
    CREATE SYNONYM [sqlver].[find] FOR [sqlver].[sputilFindInCode]    
  END
  
      
  IF NOT EXISTS(
    SELECT 
      syn.name
    FROM
      sys.synonyms syn
      JOIN sys.objects obj ON
        syn.object_id = obj.object_id 
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
    WHERE
      sch.name = 'sqlver' AND
      syn.name = 'ver'
  ) BEGIN  
    CREATE SYNONYM [sqlver].[ver] FOR [sqlver].[spVersion]    
  END
  
  IF NOT EXISTS(
    SELECT 
      syn.name
    FROM
      sys.synonyms syn
      JOIN sys.objects obj ON
        syn.object_id = obj.object_id 
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
    WHERE
      sch.name = 'sqlver' AND
      syn.name = 'RTLog'
  ) BEGIN  
    CREATE SYNONYM [sqlver].[RTLog] FOR [sqlver].[spinsSysRTLog]   
  END  

GO

--*** Permissions
DECLARE @SQL nvarchar(MAX)

IF EXISTS (SELECT name FROM sys.database_principals WHERE name = 'opsstream_sys') BEGIN
  SELECT
    @SQL = ISNULL(@SQL + CHAR(10), '') + 
    'GRANT ' +
      CASE obj.type_Desc
        WHEN 'SQL_TABLE_VALUED_FUNCTION' THEN 'SELECT'
        WHEN 'CLR_SCALAR_FUNCTION' THEN 'EXEC'
        WHEN 'SQL_SCALAR_FUNCTION' THEN 'EXEC'
        WHEN 'SQL_STORED_PROCEDURE' THEN 'EXEC'
        WHEN 'CLR_STORED_PROCEDURE' THEN 'EXEC'
        ELSE 'UNKOWN'
      END + ' ON ' + sch.name + '.' + obj.Name + ' TO opsstream_sys'
  FROM
    sys.objects obj
    JOIN sys.schemas sch ON
      obj.schema_id = sch.schema_id
  WHERE
    sch.name = 'sqlver' AND
    obj.type_DESC IN (    
      'SQL_TABLE_VALUED_FUNCTION',
      'CLR_SCALAR_FUNCTION',
      'SQL_SCALAR_FUNCTION',
      'SQL_STORED_PROCEDURE',
      'CLR_STORED_PROCEDURE'
    )    
  EXEC(@SQL)
END

GO

--*** Help Text
PRINT '
SQLVer has been installed.  To register CLR objects, do the following:
On the SQL server, create the following directories:
  C:\Temp
  C:\Temp\AssemblyCache
  C:\Temp\AssemblyLibrary
  C:\Temp\MSTools

Copy SN.exe and SN.exe.config (from the .NET 3.5 SDK) into C:\Temp\MSTools

In SQL, execute the following:

EXEC sqlver.spsysBuildCLRAssemblyCache
EXEC sqlver.spsysBuildCLR_GetHTTP

Optionally, you can create the following synonyms to make it more convenient to use SQLVer:

CREATE SYNONYM dbo.find FOR sqlver.sputilFindInCode
CREATE SYNONYM dbo.RTLog FOR sqlver.spinsSysRTLog
CREATE SYNONYM dbo.ver FOR sqlver.spVersion

CREATE SYNONYM dbo.whoHog FOR sqlver.spWhoIsHogging
CREATE SYNONYM dbo.printStr FOR sqlver.sputilPrintString
CREATE SYNONYM dbo.showSlow FOR sqlver.spShowSlowQueries
CREATE SYNONYM dbo.showRT FOR sqlver.spShowRTLog


To drop these synonyms:

DROP SYNONYM dbo.find
DROP SYNONYM dbo.RTLog
DROP SYNONYM dbo.ver

DROP SYNONYM dbo.whoHog
DROP SYNONYM dbo.printStr
DROP SYNONYM dbo.showSlow
DROP SYNONYM dbo.showRT

If in the future you want to uninstall SQLVer, you can execute:

EXEC sqlver.spUninstall

'
