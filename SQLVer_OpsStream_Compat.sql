/*
This script makes changes to an OpsStream database to allow it to use SQLVer objects instead of native OpsStream objects.

(Many SQLVer objects started out as OpsStream-specific objects.)

This script is not needed for a general SQLVer installation.  It is needed only if installing SQLVer into an OpsStream database.
*/


CREATE SYNONYM sqlver.vwMasterSchemaLog FOR [MASTER.OPSSTREAM.COM,24849].osMaster.sqlver.vwSchemaLog
CREATE SYNONYM sqlver.vwMasterSchemaManifest FOR [MASTER.OPSSTREAM.COM,24849].osMaster.sqlver.vwSchemaManifest
CREATE SYNONYM sqlver.spMasterExecuteSQL FOR [MASTER.OPSSTREAM.COM,24849].osMaster.dbo.sp_executesql
CREATE SYNONYM sqlver.spMasterSchemaObjectDefinition FOR [MASTER.OPSSTREAM.COM,24849].osMaster.sqlver.spsysSchemaObjectDefinition


CREATE TABLE #ToDrop (
  Seq int IDENTITY,
  DropSchema sysname NULL,
  DropObject sysname NULL
)

INSERT INTO #ToDrop (DropSchema, DropObject)
VALUES 
  ('opsstream', 'sputilGetHTTP_CLR'),
  ('opsstream', 'udfURLEncode_CLR'),
  ('opsstream', 'udfURLDecode_CLR'), 
  ('opsstream', 'udfGetMIMEType_CLR'),
  ('opsstream', 'udfBase64Encode_CLR'),
  ('opsstream', 'sputilSendMail_CLR'),
  ('dbo', 'Find')

 
DECLARE @DropSchema sysname
DECLARE @DropObject sysname
DECLARE @SQL nvarchar(MAX)

DECLARE curThis CURSOR LOCAL STATIC FOR
SELECT
  tmp.DropSchema,
  tmp.DropObject
FROM
  #ToDrop tmp
ORDER BY
  tmp.Seq
  
OPEN curThis
FETCH curThis INTO @DropSchema, @DropObject

  WHILE @@FETCH_STATUS = 0 BEGIN
  
    SET @SQL =
      'IF EXISTS (SELECT * FROM sys.synonyms syn JOIN sys.schemas sch ON syn.schema_id = sch.schema_id WHERE sch.name = ''' + @DropSchema + ''' AND syn.name = ''' + @DropObject + ''') DROP SYNONYM ' + @DropSchema + '.' + @DropObject+ 
      ' ELSE IF OBJECT_ID(''' + @DropSchema + '.' + @DropObject + ''') IS NOT NULL DROP PROCEDURE ' + @DropSchema + '.' + @DropObject
      
    PRINT @SQL
          
    EXEC(@SQL)
  
  FETCH curThis INTO @DropSchema, @DropObject
END

CLOSE curThis
DEALLOCATE curThis
  


EXEC sqlver.spsysBuildCLRAssemblyCache
EXEC sqlver.spsysBuildCLR_GetHTTP   
EXEC sqlver.spsysBuildCLR_SendMail


DECLARE @OldSchemaName sysname
DECLARE @OldObjectName sysname
DECLARE @NewSchemaName sysname
DECLARE @NewObjectName sysname

CREATE TABLE #ToReplace (
  Seq int IDENTITY,
  OldSchemaName sysname NULL,
  OldObjectName sysname NULL,
  NewSchemaName sysname NULL,
  NewObjectName sysname NULL
)


INSERT INTO #ToReplace (NewObjectName, OldObjectName)
VALUES
  ('spsysBackupFull', 'spsysBackupWeekly'),
  ('udftGetParsedValues', 'udfGetParsedValues'),
  ('udfParseValue', 'parseValue'),
  ('udfParseVarValue', 'parseVarValue'),
  ('udfParseVarRemove', 'parseVarRemove'),
  ('udfParseValueReplace', 'parseValueReplace'),
  ('spinsSysRTLog', 'spinsSysRTMessage')
    

INSERT INTO #ToReplace (NewObjectName)
VALUES
  ('sputilGetHTTP_CLR'),
  ('udfURLEncode_CLR'),
  ('udfURLDecode'),
  ('udfGetMIMEType_CLR'),
  ('udfBase64Encode_CLR'),
  ('sputilSendMail_CLR'),
  ('sputilResultSetAsStr'),  
  ('sputilPrintString'),  
  ('sputilGetRowCounts'),  
  ('sputilWriteStringToFile'),
  ('sputilWriteBinaryToFile'),
  ('spWhoIsHogging'),    
  ('sputilGetFileList'),
  ('spgetSQLFilegroupsOutOfSpaceAllDBs'),  
  ('spgetMissingIndexes'),  
  ('spUninstall'),  
  ('spsysCreateSubDir'),
  ('spgetSQLSpaceUsedDB'),
  ('spgetSQLSpaceUsedAllDBs'),
  ('spgetUnusedIndexes'),  
  ('spShowSlowQueries'),    
  ('udfGenerateCLRRegisterSQL'),
  ('udfMakeNumericStrict'),  
  ('udfGenerateCLRRegisterSQL'),  
  ('udfMakeNumericStrict'),  
  ('udfLTRIMSuper'),
  ('udfIsInComment'),
  ('udfHashBytesNMax'),  
  ('udfURLEncode'),  
  ('udfStripHTML'),
  ('udfScriptTable'),
  ('udfRTRIMSuper'),
  ('udfURLDecode'),  
  ('spShowRTLog'),
  ('spgetSSRSDatasets'),    
  ('spgetSQLSpaceUsed'),    
  ('spsysBuildCLRAssemblyInfo'),  
  ('spsysBuildCLRAssemblyCache'),  
  ('spBuildManifest'),    
  ('spVersion'),
  ('sputilFindInCode'), 
  ('spsysBuildCLRAssembly'),    
  ('spsysBuildCLR_SendMail'),
  ('spsysBuildCLR_GetHTTP'),
  ('spsysBuildCLR_FTPCLR'),    
  ('sputilExecInOtherConnection'),  
  ('udfMath_deg2rad'),    
  ('udfMath_rad2deg'),
  ('udfDistanceFromCoordinates'),
  ('sputilGetHTTP')
  
  
  
UPDATE #ToReplace
SET
  OldSchemaName = ISNULL(OldSchemaName, 'opsstream'),
  OldObjectName = ISNULL(OldObjectName, NewObjectName),
  NewSchemaName = ISNULL(NewSchemaName, 'sqlver')
  

DECLARE curThis CURSOR STATIC LOCAL FOR
SELECT
  tmp.OldSchemaName,
  tmp.OldObjectName,
  tmp.NewSchemaName,
  tmp.NewObjectName
FROM
  #ToReplace tmp
ORDER BY
  tmp.Seq
  
OPEN curThis
FETCH curThis INTO
  @OldSchemaName,
  @OldObjectName,
  @NewSchemaName,
  @NewObjectName
  
WHILE @@FETCH_STATUS = 0 BEGIN
  
  SET @SQL =
    'IF EXISTS (SELECT * FROM sys.synonyms syn JOIN sys.schemas sch ON syn.schema_id = sch.schema_id WHERE sch.name = ''' + @OldSchemaName + ''' AND syn.name = ''' + @OldObjectName + ''') DROP SYNONYM ' + @OldSchemaName + '.' + @OldObjectName + 
    ' ELSE IF OBJECT_ID(''' + @OldSchemaName + '.' + @OldObjectName + ''') IS NOT NULL DROP ' + 
    CASE
      WHEN @NewObjectName LIKE 'sp%' THEN 'PROCEDURE'
      WHEN @NewObjectName LIKE 'udf%' THEN 'FUNCTION'
    END + ' ' + @OldSchemaName + '.' + @OldObjectName + CHAR(10) +
    'CREATE SYNONYM ' + @OldSchemaName + '.' + @OldObjectName + ' FOR ' +  @NewSchemaName + '.' + @NewObjectName
    
  PRINT @SQL
    
  EXEC(@SQL)
        
  FETCH curThis INTO
    @OldSchemaName,
    @OldObjectName,
    @NewSchemaName,
    @NewObjectName
END

CLOSE curThis
DEALLOCATE curThis    


CREATE SYNONYM dbo.find FOR sqlver.sputilFindInCode


DROP TABLE #ToDrop
DROP TABLE #ToReplace

