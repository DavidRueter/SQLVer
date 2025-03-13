--SQLVer generated on Mar 12 2025 10:08PM

/*
SQLVer
©Copyright 2006-2025 by David Rueter (drueter@assyst.com)
See:  https://github.com/davidrueter/sqlver

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


PRINT 'Installing SQLVer'
GO


SET ANSI_NULLS ON
GO


SET QUOTED_IDENTIFIER ON
GO


IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'dtgSQLVerLogSchemaChanges' AND parent_class = 0) BEGIN
 DROP TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE
END
GO


IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sqlver') BEGIN
DECLARE @SQL nvarchar(MAX)
SET @SQL = 'CREATE SCHEMA [sqlver] '
EXEC(@SQL)
END
GO


IF OBJECT_ID('[sqlver].[tblNumbers]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblNumbers](
	[Number] [int] NOT NULL,
	CONSTRAINT [pkNumbers] PRIMARY KEY CLUSTERED
(
  [Number] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblNumbers] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblNumbers]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblNumbers](
	[Number] [int] NOT NULL,
	CONSTRAINT [pkNumbers] PRIMARY KEY CLUSTERED
(
  [Number] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblNumbers]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF NOT EXISTS(SELECT TOP 1 Number FROM sqlver.tblNumbers) BEGIN
INSERT INTO sqlver.tblNumbers (Number)
SELECT TOP 200000
ROW_NUMBER() OVER (ORDER BY a.number, b.number)
FROM
  master..spt_values a
 JOIN master..spt_values b ON 1 = 1
END
ELSE BEGIN
PRINT 'WARNING: SQLVer requires that table sqlver.tblNumbers contain unique sequential integers from 1 to 200000.  Fewer rows may cause unexpected results.  More rows may be OK, but may degrade performance of certain functions.'
END

GO


IF OBJECT_ID('[sqlver].[sputilPrintString]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilPrintString]
END
GO

CREATE PROCEDURE [sqlver].[sputilPrintString]
@Buf varchar(MAX),
@Help bit = 0

WITH EXECUTE AS OWNER
--$!SQLVer Sep 13 2022 12:46PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @Help = 1 BEGIN

    DECLARE @CRLF nvarchar(5)
    SET @CRLF = CHAR(13) + CHAR(10)

    PRINT
      CONCAT(
        'WARNING:  You must search-and-replace the string printed here to replace:', @CRLF,
        '    ~-~{CR}{LF}', @CRLF,
        'with an empty string.', @CRLF,  @CRLF,
        'For example, using T-SQL:', @CRLF,  @CRLF,
        '    REPLACE(@Buf, CHAR(126) + CHAR(45) + CHAR(126) + CHAR(13) + CHAR(10), '''')', @CRLF,  @CRLF,
        'Or using SSMS, open Find and Replace (i.e. with CTRL-H), click the .* icon (to enable regular expressions), and search for:', @CRLF,  @CRLF,
        '    \x7e\x2d\x7e\x0d\x0a', @CRLF, @CRLF,
        '(This is due to a limitation of the T-SQL PRINT statement that does not provide a way to print long strings or to suppress CR LF.)', @CRLF,
        '********************************************'
      )
  END

  DECLARE @S varchar(MAX)
  DECLARE @P int
  SET @P = 1
  
  WHILE @P < LEN(@Buf + 'x') - 1 BEGIN
    SET @S = SUBSTRING(@Buf, @P, 4000)
    PRINT CONCAT(@S, CHAR(126), '-', CHAR(126))  --CHAR(126) is tilde character
    SET @P = @P + 4000
  END

END

GO


IF OBJECT_ID('[sqlver].[sputilResultSetAsStr]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilResultSetAsStr]
END
GO

CREATE PROCEDURE [sqlver].[sputilResultSetAsStr]
@SQL nvarchar(MAX),
@ResultPrefix nvarchar(MAX) = '',
@ResultSuffix nvarchar(MAX) = '',
@TrimTrailSuffix bit = 1,
@IncludeLineBreaks bit = 0,
@Result nvarchar(MAX) OUTPUT

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  --note:  statement in @SQL must return only a single column.

  DECLARE @ThisValue nvarchar(MAX)
  DECLARE @CRLF nvarchar(5)
  SET @CRLF = NCHAR(13) + NCHAR(10)

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
    
  WHILE @TrimTrailSuffix = 1 AND LEN(@ResultSuffix) > 0 AND PATINDEX('%' + REVERSE(@ResultSuffix) + '%', REVERSE(@Result)) = 1 BEGIN
    SET @Result = SUBSTRING(@Result, 1, (LEN(@Result + 'x') - 1) - (LEN(@ResultSuffix + 'x') - 1))  
  END
END

GO


IF OBJECT_ID('[sqlver].[udfFindInSQL]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfFindInSQL]
END
GO

CREATE FUNCTION [sqlver].[udfFindInSQL](
@TargetStr nvarchar(MAX),
@SQL nvarchar(MAX),
@StartPos int)
RETURNS int

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Chunk nvarchar(MAX)
  DECLARE @Found bit
  DECLARE @P int
  DECLARE @Delims varchar(40)
  SET @Delims =  ' ' + ';' + CHAR(13) + CHAR(10) + CHAR(9)
  
  SET @Found = 0        
  SET @P = 0        
  
  IF @StartPos IS NULL BEGIN
    SET @StartPos = 0
  END
  
  SET @SQL = @SQL + CHAR(13)
  
  SET @Chunk = RIGHT(@SQL, LEN(@SQL) - @StartPos)
  WHILE (@Found = 0) AND (@P IS NOT NULL) BEGIN
    SET @P = PATINDEX('%' + @TargetStr + '[' + @Delims + ']%', @Chunk)

    IF @P = 0 BEGIN
      --Didn't find @TargetStr immediately followed by a delimiter.
      --Try immediately followed by inline comment.
      SET @P = PATINDEX('%' + @TargetStr + '--%', @Chunk)
    END
                
    IF @P = 0 BEGIN
      --Didn't find @TargetStr immediately followed by a delimiter or inline comment.
      --Try immediately followed by block comment.
      SET @P = PATINDEX('%' + @TargetStr + '/*%', @Chunk)
    END      

    IF (@P > 0) BEGIN
      IF 
        --sqlver.udfIsInComment(@StartPos + @P, @SQL) = 0 AND
        --udfSQLTerm checks for comments
        sqlver.udfSQLTerm(@StartPos + @P, @SQL) LIKE 'Word:%' BEGIN
        SET @Found = 1
        BREAK
      END
      ELSE BEGIN
        SET @StartPos = @StartPos + @P
        SET @Chunk = RIGHT(@SQL, LEN(@SQL) - @StartPos)
      END
    END
    ELSE BEGIN
      SET @P = NULL
    END              
  END
  
  RETURN ISNULL(@StartPos + @P, 0)
END

GO


IF OBJECT_ID('[sqlver].[udfHashBytesNMax]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfHashBytesNMax]
END
GO

CREATE FUNCTION [sqlver].[udfHashBytesNMax](@Algorithm sysname = 'SHA2_256', @Input nvarchar(MAX))
RETURNS varbinary(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @Algorithm IS NULL BEGIN
    SET @Algorithm = 'SHA2_256'
  END

  DECLARE @Result varbinary(MAX)

  DECLARE @Chunk int
  DECLARE @ChunkSize int
  DECLARE @ChunkInput nvarchar(MAX)
  
  SET @ChunkSize = 4000
  SET @Chunk = 1
  SET @Result = CAST('' AS varbinary(MAX))

  WHILE @Chunk * @ChunkSize < LEN(@Input + 'x') - 1 BEGIN
    --Append the hash for each chunk
    SET @ChunkInput = SUBSTRING(@Input, ((@Chunk - 1) * @ChunkSize) + 1, @ChunkSize)
    SET @Result = @Result + HASHBYTES(@Algorithm, @ChunkInput)
    SET @Chunk = @Chunk + 1
  END

  --Append the hash for the final partial chunk
  SET @ChunkInput = RIGHT(@Input, LEN(@Input + 'x') - 1 - ((@Chunk - 1) * @ChunkSize))
  SET @Result = @Result + HASHBYTES(@Algorithm, @CHunkInput)

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


IF OBJECT_ID('[sqlver].[udfIsInComment]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfIsInComment]
END
GO

CREATE FUNCTION [sqlver].[udfIsInComment](
@CharIndex int,
@SQL nvarchar(MAX))
RETURNS BIT

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfLTRIMSuper]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfLTRIMSuper]
END
GO

CREATE FUNCTION [sqlver].[udfLTRIMSuper](@S varchar(MAX))
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfRTRIMSuper]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfRTRIMSuper]
END
GO

CREATE FUNCTION [sqlver].[udfRTRIMSuper](@S varchar(MAX))
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfScriptTable]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfScriptTable]
END
GO

CREATE FUNCTION [sqlver].[udfScriptTable](
@SchemaName sysname, --can contain schema.name if @ObjectName is NULL
@ObjectName sysname = NULL)   --can be NULL
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Aug  3 2021  9:53AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Based on script contributed by Marcello - 25/09/09, in comment to article posted by 
  --Tim Chapman, TechRepublic, 2008/11/20
  --http://www.builderau.com.au/program/sqlserver/soa/Script-Table-definitions-using-TSQL/0,339028455,339293405,00.htm
  
  --Formatting altered by David Rueter (drueter@assyst.com) 2010/05/11 to match
  --script generated by MS SQL Server Management Studio 2005

  IF @ObjectName IS NULL BEGIN
    SET @ObjectName = PARSENAME(@SchemaName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 2)
  END
  ELSE BEGIN
    SET @ObjectName = PARSENAME(@ObjectName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 1)
  END
  

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
    @f2 = CHAR(9),
    @f3=@f1+@f2,
    @f4=',' + @f3
  FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
  WHERE
    sch.name LIKE @SchemaName AND
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
            CASE
             WHEN t.Name IN ('numeric', 'decimal') THEN c.precision
             WHEN c.max_length = -1 THEN c.max_length
             WHEN t.Name IN ('nchar', 'nvarchar') THEN c.max_length / 2
             ELSE c.max_length
            END, -1)), 'MAX') + 
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
    CHAR(9) + D + CASE Nr WHEN Clr THEN '' ELSE ',' + @f1 END,
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
--            CASE c.is_descending_key 
--              WHEN 1  THEN ' DESC'
--              ELSE ' ASC' 
--            END
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


IF OBJECT_ID('[sqlver].[udfScriptType]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfScriptType]
END
GO

CREATE FUNCTION [sqlver].[udfScriptType](
@SchemaName sysname, --can contain schema.name if @ObjectName is NULL
@ObjectName sysname   --can be NULL
)
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  IF @ObjectName IS NULL BEGIN
    SET @ObjectName = PARSENAME(@SchemaName, 1)
    SET @Schemaname = PARSENAME(@SchemaName, 2)
  END
  ELSE BEGIN
    SET @ObjectName = PARSENAME(@ObjectName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 1)
  END

  DECLARE @TypeDef nvarchar(MAX)
  DECLARE @ColDef nvarchar(MAX)
  DECLARE @TypeID int
  DECLARE @IsTableType bit
  DECLARE @FQObjName nvarchar(512)

  SELECT
    @TypeDef = 
      'CREATE TYPE [' + sch.name + '].[' + typ.name + '] AS ' +
      CASE WHEN typ.is_table_type = 1 THEN 'TABLE(' ELSE 'UNKNOWN' END,

    @TypeID = typ.user_type_id,
    @IsTableType = typ.is_table_type
  FROM
    sys.types typ
    JOIN sys.schemas sch ON
      typ.schema_id = sch.schema_id
  WHERE
    typ.is_user_defined = 1 AND
    sch.name = @SchemaName AND
    typ.name = @ObjectName


  IF @IsTableType = 1 BEGIN

    SELECT
      @ColDef = ISNULL(@ColDef + ',' + NCHAR(13) + NCHAR(10), '') +
        '  ' + x.coldef
    FROM
      (
      SELECT
        tt.user_type_id,
        col.column_id,
        '[' + col.name + '] ' +
        '[' + typ.name + ']' + 
          CASE
            WHEN typ.name IN ('decimal', 'numeric')
              THEN '(' + CAST(col.precision AS varchar(100)) + ', ' + CAST(col.scale AS varchar(100)) + ')'
            WHEN typ.name IN ('char', 'nchar', 'binary', 'varchar', 'nvarchar')
              THEN '(' + ISNULL(CAST(NULLIF(col.max_length, -1) / 
                CASE
                  WHEN typ.name IN ('nchar', 'nvarchar') THEN 2
                  ELSE 1
                END AS varchar(100)), 'max') + ')'
            ELSE ''
          END +
          CASE
            WHEN typ.is_nullable = 1 THEN ' NULL'
            ELSE ''
          END AS coldef
      FROM
        sys.table_types tt
        JOIN sys.columns col ON
          tt.type_table_object_id = col.object_id 
        JOIN sys.types typ ON
          col.user_type_id = typ.user_type_id
      ) x
    WHERE
      x.user_type_id = @TypeID

    SET @TypeDef = @TypeDef + NCHAR(13) + NCHAR(10) + ISNULL(@ColDef + 
    NCHAR(13) + NCHAR(10) + '  )', '')
  END

  RETURN @TypeDef
END

GO


IF OBJECT_ID('[sqlver].[udfSQLTerm]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfSQLTerm]
END
GO

CREATE FUNCTION [sqlver].[udfSQLTerm](
@CharIndex int,
@SQL nvarchar(MAX))
RETURNS varchar(30)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result varchar(30)
  
  --Given a string @SQL, determine the type of term that postion
  --@CharIndex is in.
  SET @SQL = RTRIM(@SQL)
  
  IF @CharIndex > LEN(@SQL) BEGIN
    SET @Result = NULL
  END
  ELSE BEGIN
    DECLARE @Mode int
      --1 = String Literal
      --2 = Quoted Identifier
      --3 = Variable Name
      --4 = Comment
      --5 = Block Comment
      --currently cannot distinguish unquoted identifier from command 

    DECLARE @PrevMode int
      
    DECLARE @IsWhitespace bit
    DECLARE @IsOperator bit
     
    DECLARE @P int
    DECLARE @C char(1)
    DECLARE @C2 CHAR(1)
    
    DECLARE @WordPos int
    SET @WordPos = 0
       
    SET @Mode = 0    
    SET @P = 1
    WHILE @P <= @CharIndex BEGIN
      SET @C = SUBSTRING(@SQL, @P, 1)
      
      IF @P < LEN(@SQL) BEGIN
        SET @C2 = SUBSTRING(@SQL, @P + 1, 1)
      END
      ELSE BEGIN
        SET @C2 = NULL
      END
      
      IF (@Mode = 0) BEGIN
        IF @C IN (CHAR(39), '[', '@', '-', '/') BEGIN
          IF @C = CHAR(39) SET @Mode = 1
          ELSE IF @C = '[' SET @Mode = 2
          ELSE IF @C = '@' SET @Mode = 3
          ELSE IF @C = '-' AND @C2 = '-' BEGIN
            SET @Mode = 4
            SET @P = @P + 1
          END
          ELSE IF @C = '/' AND @C2 = '*' BEGIN
            SET @Mode = 5
            SET @P = @P + 1
          END
        END
        ELSE BEGIN
          SET @IsWhitespace = CASE WHEN @C IN (' ', CHAR(9), CHAR(10), CHAR(13)) THEN 1 ELSE 0 END
          SET @IsOperator = CASE WHEN @C IN ('+', '-', '*', '/', '=', '.') THEN 1 ELSE 0 END
        END
      END
      ELSE IF (@Mode = 1) AND (@C = CHAR(39)) SET @Mode = 0
      ELSE IF (@Mode = 2) AND (@C = ']') SET @Mode = 0
      ELSE IF (@Mode = 3) AND (@C IN ('=', ' ')) SET @Mode = 0
      ELSE IF (@Mode = 4) AND (@C = CHAR(13)) SET @Mode = 0
      ELSE IF (@Mode = 5) AND (@C = '*') AND (@C2 = '/') SET @Mode = 0

      IF (@IsWhitespace = 1) OR (@IsOperator = 1) BEGIN
        SET @WordPos = 0
      END
      ELSE IF (@Mode <> @PrevMode) BEGIN
        SET @WordPos = 1
      END
      ELSE BEGIN 
        SET @WordPos = @WordPos + 1        
      END

      SET @PrevMode = @Mode      
      SET @P = @P + 1
    END
    
    IF @Mode = 0 BEGIN
      IF @IsWhitespace = 1 SET @Result = 'Whitespace'
      ELSE IF @IsOperator = 1 SET @Result = 'Operator'
      ELSE SET @Result = 'Word' + ':' + ISNULL(CAST(@WordPos AS varchar(100)), 'NULL')
    END
    ELSE BEGIN
      IF @Mode = 1 SET @Result = 'Literal'
      ELSE IF @Mode = 2 SET @Result = 'QuotedIdent'
      ELSE IF @Mode = 3 SET @Result = 'VariableName'
      ELSE IF @Mode IN (4, 5) SET @Result = 'Comment'
    END
     
  END
  
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfStripSQLCommentsExcept]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfStripSQLCommentsExcept]
END
GO

CREATE FUNCTION [sqlver].[udfStripSQLCommentsExcept](
@SQL nvarchar(MAX),
@ExceptStartsWith nvarchar(MAX)
)
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @L int
  SET @L = LEN(@SQL)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RETURN CAST('Error in sqlver.udfStripSQLCommentsExcept:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in slqlver.tblNumbers.' AS int)
  END

  --Make sure there is an EOL at the end
  SET @SQL = @SQL + CHAR(13)

  /*
  This table variable has a row for each character in the string,
  along with flags that describe the character.  EndPos pertains
  to certain character runs such as comments.
  */
  DECLARE @tvSQL TABLE (
    Pos int PRIMARY KEY,
    EndPos int,
    ThisChar nchar,  
    --StartQIdent bit DEFAULT (0),  --Quoted identifiers like [SomeObject]
    --StartVar bit DEFAULT (0),     --Variables like @SomeVar
    --StartLit bit DEFAULT (0),     --String literals like 'some text'
    StartCom1 bit DEFAULT (0),      --Start of comment like --some comment
    StartCom2 bit DEFAULT (0),      --Start of block comment like /* some block comment */
    --IsWhite bit DEFAULT (0),      --Whitespace (space, tab, CR, LF)
    --IsOper  bit DEFAULT (0),      --SQL operator like + - = etc.
    IsComment bit DEFAULT (0),      --Comment character
    --IsEOL bit DEFAULT (0),        --End of Line (CR LF ;)
    ToStrip bit DEFAULT(0)          --This character should be stripped from results
  )

  INSERT INTO @tvSQL (
    Pos,
    ThisChar
  )
  SELECT
    n.Number,
    SUBSTRING(@SQL, n.Number, 1)
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@SQL + 'x') - 1    
    
  UPDATE t1
    SET 
      --StartQIdent = CASE WHEN t_qident.Pos IS NOT NULL THEN 1 ELSE 0 END,
      --StartVar = CASE WHEN t_var.Pos IS NOT NULL THEN 1 ELSE 0 END,
      StartCom1 = CASE WHEN t_com1.Pos IS NOT NULL THEN 1 ELSE 0 END,
      StartCom2 = CASE WHEN t_com2.Pos IS NOT NULL THEN 1 ELSE 0 END
      --IsWhite = CASE WHEN t_white.Pos IS NOT NULL THEN 1 ELSE 0 END,
      --IsOper = CASE WHEN t_oper.Pos IS NOT NULL THEN 1 ELSE 0 END,
      --IsEOL = CASE WHEN t_EOL.Pos IS NOT NULL THEN 1 ELSE 0 END,
      --EndPos = CASE WHEN t_qident.Pos IS NOT NULL THEN t_qident.EndPos END
  FROM 
    @tvSQL t1

    ----detect variable
    --LEFT JOIN @tvSQL t_var ON
    --  t1.Pos = t_var.Pos AND
    --  t1.ThisChar = '@'  

    ----detect quoted identifier
    --LEFT JOIN (
    --  SELECT
    --    t1.Pos,
    --    MIN(t2.Pos) AS EndPos
    --  FROM
    --    @tvSQL t1  
    --    LEFT JOIN @tvSQL t2 ON
    --      t1.Pos < t2.Pos AND
    --      t2.ThisChar = ']'    
    --  WHERE
    --    t1.ThisChar = '['
    --  GROUP BY
    --    t1.Pos
    --  ) t_qident ON
    --    t1.Pos = t_qident.Pos AND
    --    t1.ThisChar = '['             
    
    -- detect single-line comment
    LEFT JOIN (
      SELECT
        t1.Pos
      FROM    
        @tvSQL t1
        JOIN @tvSQL t1a ON
          t1.Pos + 1 = t1a.Pos AND
          t1.ThisChar = '-' AND
          t1a.ThisChar = '-'
     ) t_com1 ON
       t1.Pos = t_com1.Pos          
            
    -- detect block comment
    LEFT JOIN (
      SELECT
        t1.Pos
      FROM    
        @tvSQL t1
        JOIN @tvSQL t1a ON
          t1.Pos + 1 = t1a.Pos AND
          t1.ThisChar = '/' AND
          t1a.ThisChar = '*'
     ) t_com2 ON
       t1.Pos = t_com2.Pos              
           
    -- detect whitespace
    --LEFT JOIN @tvSQL t_white ON
    --  t1.Pos = t_white.Pos AND    
    --  t1.ThisChar IN (' ', CHAR(9), CHAR(10), CHAR(13))        
      
    -- detect SQL operator
    --LEFT JOIN @tvSQL t_oper ON
    --  t1.Pos = t_oper.Pos AND
    --  (
    --   (t1.ThisChar IN ('+', '*', '=', '.', ';', '(', ')')) OR
    --   (t1.ThisChar = '-' AND t_com1.Pos IS NULL) OR
    --   (t1.ThisChar = '/' AND t_com2.Pos IS NULL)
    --  )
    
    -- detect EOL
    --LEFT JOIN @tvSQL t_EOL ON
    --  t1.Pos = t_EOL.Pos AND
    --  t1.ThisChar IN (';', CHAR(13), CHAR(10))
       
  --identify end of single-line comment (up to but not including first EOL)
  UPDATE @tvSQL
  SET EndPos = Pos + PATINDEX('%[' + CHAR(13) + CHAR(10) + ']%', STUFF(@SQL, 1, Pos, ''))
  WHERE
    StartCom1 = 1
    
  --identify end of block comment
  UPDATE @tvSQL
  SET EndPos = Pos + PATINDEX('%*/%', STUFF(@SQL, 1, Pos, '')) + 2
  WHERE
    StartCom2 = 1
    
  --flag individual comment characters
  UPDATE t2
  SET
    IsComment = 1,
    ToStrip = 1
  FROM
    @tvSQL t1
    JOIN @tvSQL t2 ON
      t2.Pos >= t1.Pos AND
      t2.Pos <= t1.EndPos    
  WHERE
    (t1.StartCom1 = 1 OR t1.StartCom2 = 1)


  --unflag comments that are like @ExceptStartsWith
  IF @ExceptStartsWith IS NOT NULL BEGIN
    UPDATE t3
    SET
      ToStrip = 0
    FROM
      (
      SELECT
        t1.Pos,
        t1.EndPos,
          (SELECT
            '' + t2.ThisChar
            FROM @tvSQL t2
            WHERE
              t2.Pos >= t1.Pos + 2 AND
              t2.Pos <= t1.EndPos - CASE WHEN t1.StartCom2 = 1 THEN 2 ELSE 0 END 
            ORDER BY
              t2.Pos
            FOR XML PATH(''), TYPE
          ).value('.', 'nvarchar(MAX)') AS CommentBuf
      FROM
        @tvSQL t1
      WHERE
       (t1.StartCom1 = 1 OR t1.StartCom2 = 1)
      GROUP BY
        t1.Pos,
        t1.EndPos,
        t1.StartCom2
      ) x
      JOIN @tvSQL t3 ON
        t3.Pos >= x.Pos AND
        t3.Pos <= x.EndPos
    WHERE
      x.CommentBuf LIKE @ExceptStartsWith + '%'
  END
  
  DECLARE @Buf nvarchar(MAX)

  SET @Buf =
   (
    SELECT
      t1.ThisChar + ''
    FROM
      @tvSQL t1
    WHERE
      ToStrip = 0
    FOR XML PATH(''), TYPE
   ).value('.', 'nvarchar(MAX)')


  --Remove EOL that we added at the end  
  RETURN LEFT(@Buf, LEN(@Buf + 'x') - 1)  
END

GO


IF OBJECT_ID('[sqlver].[udfStripSQLComments]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfStripSQLComments]
END
GO

CREATE FUNCTION [sqlver].[udfStripSQLComments](
@SQL nvarchar(MAX)
)
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @L int
  SET @L = LEN(@SQL)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RETURN CAST('Error in sqlver.udfStripSQLComments:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.' AS int)
  END

  --Make sure there is an EOL at the end
  SET @SQL = @SQL + CHAR(13)

  DECLARE @tvSQL TABLE (
    Pos int PRIMARY KEY,
    EndPos int,
    ThisChar nchar,  
    StartQIdent bit DEFAULT (0),  
    StartVar bit DEFAULT (0),
    StartLit bit DEFAULT (0),
    StartCom1 bit DEFAULT (0),
    StartCom2 bit DEFAULT (0),
    IsWhite bit DEFAULT (0),
    IsOper  bit DEFAULT (0),
    IsComment bit DEFAULT (0),
    IsEOL bit DEFAULT (0)
  )

  INSERT INTO @tvSQL (
    Pos,
    ThisChar
  )
  SELECT
    n.Number,
    SUBSTRING(@SQL, n.Number, 1)
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@SQL + 'x') - 1
    
    
  UPDATE t1
    SET 
      StartQIdent = CASE WHEN t_qident.Pos IS NOT NULL THEN 1 ELSE 0 END,
      StartVar = CASE WHEN t_var.Pos IS NOT NULL THEN 1 ELSE 0 END,
      StartCom1 = CASE WHEN t_com1.Pos IS NOT NULL THEN 1 ELSE 0 END,
      StartCom2 = CASE WHEN t_com2.Pos IS NOT NULL THEN 1 ELSE 0 END,
      IsWhite = CASE WHEN t_white.Pos IS NOT NULL THEN 1 ELSE 0 END,
      IsOper = CASE WHEN t_oper.Pos IS NOT NULL THEN 1 ELSE 0 END,
      IsEOL = CASE WHEN t_EOL.Pos IS NOT NULL THEN 1 ELSE 0 END,
      EndPos = CASE WHEN t_qident.Pos IS NOT NULL THEN t_qident.EndPos END
  FROM 
    @tvSQL t1

    LEFT JOIN @tvSQL t_var ON
      t1.Pos = t_var.Pos AND
      t1.ThisChar = '@'  

    LEFT JOIN (
      SELECT
        t1.Pos,
        MIN(t2.Pos) AS EndPos
      FROM
        @tvSQL t1  
        LEFT JOIN @tvSQL t2 ON
          t1.Pos < t2.Pos AND
          t2.ThisChar = ']'    
      WHERE
        t1.ThisChar = '['
      GROUP BY
        t1.Pos
      ) t_qident ON
        t1.Pos = t_qident.Pos AND
        t1.ThisChar = '['             
    
    LEFT JOIN (
      SELECT
        t1.Pos
      FROM    
        @tvSQL t1
        JOIN @tvSQL t1a ON
          t1.Pos + 1 = t1a.Pos AND
          t1.ThisChar = '-' AND
          t1a.ThisChar = '-'
     ) t_com1 ON
       t1.Pos = t_com1.Pos          
            
    LEFT JOIN (
      SELECT
        t1.Pos
      FROM    
        @tvSQL t1
        JOIN @tvSQL t1a ON
          t1.Pos + 1 = t1a.Pos AND
          t1.ThisChar = '/' AND
          t1a.ThisChar = '*'
     ) t_com2 ON
       t1.Pos = t_com2.Pos              
           
    LEFT JOIN @tvSQL t_white ON
      t1.Pos = t_white.Pos AND    
      t1.ThisChar IN (' ', CHAR(9), CHAR(10), CHAR(13))        
      
    LEFT JOIN @tvSQL t_oper ON
      t1.Pos = t_oper.Pos AND
      (
       (t1.ThisChar IN ('+', '*', '=', '.', ';', '(', ')')) OR
       (t1.ThisChar = '-' AND t_com1.Pos IS NULL) OR
       (t1.ThisChar = '/' AND t_com2.Pos IS NULL)
      )
    
    LEFT JOIN @tvSQL t_EOL ON
      t1.Pos = t_EOL.Pos AND
      t1.ThisChar IN (';', CHAR(13), CHAR(10))
       
      
  UPDATE @tvSQL
  SET EndPos = Pos + PATINDEX('%[' + CHAR(13) + CHAR(10) + ']%', RIGHT(@SQL, LEN(@SQL + 'x') - 1 - Pos - 1 - 1)) + 2
  WHERE
    StartCom1 = 1
    
  UPDATE @tvSQL
  SET EndPos = Pos + PATINDEX('%*/%', RIGHT(@SQL, LEN(@SQL + 'x') - 1 - Pos - 1)) + 2
  WHERE
    StartCom2 = 1
    
  UPDATE t2
  SET IsComment = 1
  FROM
    @tvSQL t1
    JOIN @tvSQL t2 ON
      t2.Pos >= t1.Pos AND
      t2.Pos <= t1.EndPos    
  WHERE
    (t1.StartCom1 = 1 OR t1.StartCom2 = 1)
   

  DECLARE @Buf nvarchar(MAX)

  SET @Buf = ''

  SELECT @Buf = @Buf + t1.ThisChar
  FROM
    @tvSQL t1
  WHERE
    IsComment = 0

  --Remove EOL that we added at the end  
  RETURN LEFT(@Buf, LEN(@Buf + 'x') - 1)  
END

GO


IF OBJECT_ID('[sqlver].[udfSubstrToDelims]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfSubstrToDelims]
END
GO

CREATE FUNCTION [sqlver].[udfSubstrToDelims](
@Str nvarchar(MAX),
@StartAt int,
@Delims nvarchar(MAX)
)
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result nvarchar(MAX)

  IF @Delims = '\eol' BEGIN
    SET @Delims = ';' + CHAR(13) + CHAR(10) + CHAR(9)
  END

  DECLARE @P int

  SET @P = LEN(ISNULL(@Str, '') + 'x') - 1

  IF @StartAt > 0 BEGIN
    SET @P = @P - @StartAt + 1
  END

  IF @P > 0 BEGIN
    SET @Result = RIGHT(@Str, @P)
  END
  ELSE BEGIN
    SET @Result = @Str
  END

  SET @P = PATINDEX('%[' + @Delims + ']%', @Result)

  IF @P > 0 BEGIN
    SET @Result = LEFT(@Result, @P - 1)
  END

  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udftGetParsedValues]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftGetParsedValues]
END
GO

CREATE FUNCTION [sqlver].[udftGetParsedValues](
  @InputString nvarchar(MAX),
  @Delimiter nchar(1)
)
RETURNS @tvValues TABLE (
  [Value] nvarchar(MAX),
  [Index] int)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @L int
  SET @L = LEN(@InputString)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), NULL) BEGIN
    INSERT INTO @tvValues ([Value], [Index])
    VALUES ('Error in sqlver.udftGetParsedValues:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.', -1)
  END

  --Remove trailing delimiters
  WHILE RIGHT(@InputString,1) = @Delimiter BEGIN
    SET @InputString = LEFT(@InputString, LEN(@InputString + 'x') - 1 - 1)
  END
  SET @InputString = @Delimiter + @InputString + @Delimiter

  INSERT INTO @tvValues ([Value], [Index])
  SELECT
    SUBSTRING(
      @InputString,
      N.Number + 1, 
      CHARINDEX( @Delimiter, @InputString, N.Number + 1 ) - N.Number - 1
    ),

    ROW_NUMBER() OVER (ORDER BY N.Number)

  FROM sqlver.tblNumbers N
  WHERE
    SUBSTRING(@InputString, N.Number, 1 ) = @Delimiter AND
    N.Number < (LEN(@InputString + 'x' ) - 1)
  RETURN
END

GO


IF OBJECT_ID('[sqlver].[tblSchemaLog]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblSchemaLog](
	[SchemaLogId] [int] IDENTITY(1,1) NOT NULL,
	[SPID] [smallint] NULL,
	[EventType] [varchar](50) NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[ObjectType] [varchar](25) NULL,
	[SQLCommand] [nvarchar](MAX) NULL,
	[EventDate] [datetime] NULL,
	[LoginName] [sysname] NOT NULL,
	[EventData] [xml] NULL,
	[Hash] [varbinary](128) NULL,
	[Comments] [nvarchar](MAX) NULL,
	[UserID] [int] NULL,
	[SQLFullTable] [nvarchar](MAX) NULL,
	CONSTRAINT [pkSchemaLog] PRIMARY KEY CLUSTERED
(
  [SchemaLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSchemaLog_ObjectName_SchemaName ON [sqlver].[tblSchemaLog]
(
  [ObjectName] ASC,   [SchemaName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblSchemaLog] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblSchemaLog]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblSchemaLog](
	[SchemaLogId] [int] IDENTITY(1,1) NOT NULL,
	[SPID] [smallint] NULL,
	[EventType] [varchar](50) NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[ObjectType] [varchar](25) NULL,
	[SQLCommand] [nvarchar](MAX) NULL,
	[EventDate] [datetime] NULL,
	[LoginName] [sysname] NOT NULL,
	[EventData] [xml] NULL,
	[Hash] [varbinary](128) NULL,
	[Comments] [nvarchar](MAX) NULL,
	[UserID] [int] NULL,
	[SQLFullTable] [nvarchar](MAX) NULL,
	CONSTRAINT [pkSchemaLog] PRIMARY KEY CLUSTERED
(
  [SchemaLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSchemaLog_ObjectName_SchemaName ON [sqlver].[tblSchemaLog]
(
  [ObjectName] ASC,   [SchemaName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblSchemaLog]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[tblSchemaManifest]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblSchemaManifest](
	[SchemaManifestId] [int] IDENTITY(1,1) NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[OrigDefinition] [nvarchar](MAX) NULL,
	[DateAppeared] [datetime] NULL,
	[CreatedByLoginName] [sysname] NULL,
	[DateUpdated] [datetime] NULL,
	[OrigHash] [varbinary](128) NULL,
	[CurrentHash] [varbinary](128) NULL,
	[IsEncrypted] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsEncrypted] DEFAULT ((0)),
	[StillExists] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__StillExists] DEFAULT ((0)),
	[SkipLogging] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__SkipLogging] DEFAULT ((0)),
	[Comments] [nvarchar](MAX) NULL,
	[ObjectType] [sysname] NULL,
	[IsGenerated] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsGenerated] DEFAULT ((0)),
	[IsUserDefined] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsUserDefined] DEFAULT ((0)),
	[HasError] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__HasError] DEFAULT ((0)),
	[ErrorMessage] [varchar](MAX) NULL,
	[UpdateAvail] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__UpdateAvail] DEFAULT ((0)),
	[UpdateHash] [varbinary](128) NULL,
	[UpdateDefinition] [nvarchar](MAX) NULL,
	[InhibitUpdate] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__InhibitUpdate] DEFAULT ((0)),
	[UpdateBatchGUID] [uniqueidentifier] NULL,
	[IncludeInQueryBuilder] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IncludeInQueryBuilder] DEFAULT ((0)),
	[ColumnDefinition] [nvarchar](MAX) NULL,
	[ExecuteAs] [sysname] NULL,
	[WriteProtected] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__WriteProtected] DEFAULT ((0)),
	[ForceSchemaBinding] [bit] NULL,
	[ObjectCategory] [int] NULL,
	[ExcludeFromSync] [bit] NULL,
	CONSTRAINT [pkSchemaManifest] PRIMARY KEY CLUSTERED
(
  [SchemaManifestId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_ObjectName_SchemaName ON [sqlver].[tblSchemaManifest]
(
  [ObjectName] ASC,   [SchemaName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_SchemaName_StillExists ON [sqlver].[tblSchemaManifest]
(
  [SchemaName] ASC,   [StillExists] ASC
) INCLUDE
(
  [SchemaManifestId],   [ObjectName],   [ObjectType])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_StillExists_ObjectType ON [sqlver].[tblSchemaManifest]
(
  [StillExists] ASC,   [ObjectType] ASC
) INCLUDE
(
  [SchemaManifestId],   [ObjectName],   [SchemaName])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSchemaManifest_ObjectName_StillExists_ObjectType ON [sqlver].[tblSchemaManifest]
(
  [ObjectName] ASC,   [StillExists] ASC,   [ObjectType] ASC
) INCLUDE
(
  [SchemaManifestId],   [SchemaName])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblSchemaManifest] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblSchemaManifest]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblSchemaManifest](
	[SchemaManifestId] [int] IDENTITY(1,1) NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[OrigDefinition] [nvarchar](MAX) NULL,
	[DateAppeared] [datetime] NULL,
	[CreatedByLoginName] [sysname] NULL,
	[DateUpdated] [datetime] NULL,
	[OrigHash] [varbinary](128) NULL,
	[CurrentHash] [varbinary](128) NULL,
	[IsEncrypted] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsEncrypted] DEFAULT ((0)),
	[StillExists] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__StillExists] DEFAULT ((0)),
	[SkipLogging] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__SkipLogging] DEFAULT ((0)),
	[Comments] [nvarchar](MAX) NULL,
	[ObjectType] [sysname] NULL,
	[IsGenerated] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsGenerated] DEFAULT ((0)),
	[IsUserDefined] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IsUserDefined] DEFAULT ((0)),
	[HasError] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__HasError] DEFAULT ((0)),
	[ErrorMessage] [varchar](MAX) NULL,
	[UpdateAvail] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__UpdateAvail] DEFAULT ((0)),
	[UpdateHash] [varbinary](128) NULL,
	[UpdateDefinition] [nvarchar](MAX) NULL,
	[InhibitUpdate] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__InhibitUpdate] DEFAULT ((0)),
	[UpdateBatchGUID] [uniqueidentifier] NULL,
	[IncludeInQueryBuilder] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__IncludeInQueryBuilder] DEFAULT ((0)),
	[ColumnDefinition] [nvarchar](MAX) NULL,
	[ExecuteAs] [sysname] NULL,
	[WriteProtected] [bit] NOT NULL CONSTRAINT [dfSchemaManifest__WriteProtected] DEFAULT ((0)),
	[ForceSchemaBinding] [bit] NULL,
	[ObjectCategory] [int] NULL,
	[ExcludeFromSync] [bit] NULL,
	CONSTRAINT [pkSchemaManifest] PRIMARY KEY CLUSTERED
(
  [SchemaManifestId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_ObjectName_SchemaName ON [sqlver].[tblSchemaManifest]
(
  [ObjectName] ASC,   [SchemaName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_SchemaName_StillExists ON [sqlver].[tblSchemaManifest]
(
  [SchemaName] ASC,   [StillExists] ASC
) INCLUDE
(
  [SchemaManifestId],   [ObjectName],   [ObjectType])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixixSchemaManifest_StillExists_ObjectType ON [sqlver].[tblSchemaManifest]
(
  [StillExists] ASC,   [ObjectType] ASC
) INCLUDE
(
  [SchemaManifestId],   [ObjectName],   [SchemaName])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSchemaManifest_ObjectName_StillExists_ObjectType ON [sqlver].[tblSchemaManifest]
(
  [ObjectName] ASC,   [StillExists] ASC,   [ObjectType] ASC
) INCLUDE
(
  [SchemaManifestId],   [SchemaName])WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblSchemaManifest]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[spsysSchemaExistSync]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaExistSync]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaExistSync]
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Flag missing objects
  UPDATE schm
  SET
    StillExists = 0
  FROM
    sqlver.tblSchemaManifest schm 
    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id AND
      schm.ObjectName = obj.name

  WHERE
    schm.ObjectType NOT IN ('SYNONYM', 'TRIGGER', 'TYPE') AND
    schm.StillExists = 1 AND
    obj.object_id IS NULL

  UPDATE schm
  SET
    StillExists = 0
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.synonyms syn ON
      schm.ObjectName = syn.name AND
      sch.schema_id = syn.schema_id
  WHERE
    schm.ObjectType = 'SYNONYM' AND
    schm.StillExists = 1 AND
    syn.object_id IS NULL


  UPDATE schm
  SET
    StillExists = 0
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.types typ ON
      schm.ObjectName = typ.name AND
      sch.schema_id = typ.schema_id
  WHERE
    schm.ObjectType = 'TYPE' AND
    schm.StillExists = 1 AND
    typ.user_type_id IS NULL


  UPDATE schm
  SET
    StillExists = 0
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.triggers tg ON
      schm.ObjectName = tg.name
    LEFT JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id AND
      tg.object_id = obj.object_id

  WHERE
    schm.ObjectType = 'TRIGGER' AND
    schm.StillExists = 1 AND
    (tg.object_id IS NULL OR (tg.parent_class <> 0 AND obj.object_id IS NULL))



  --Flag present objects
  UPDATE schm
  SET
    StillExists = 1
  FROM
    sqlver.tblSchemaManifest schm 
    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id AND
      schm.ObjectName = obj.name

  WHERE
    schm.ObjectType NOT IN ('SYNONYM', 'TRIGGER','TYPE') AND
    schm.StillExists = 0 AND
    obj.object_id IS NOT NULL


  UPDATE schm
  SET
    StillExists = 1
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.synonyms syn ON
      schm.ObjectName = syn.name AND
      sch.schema_id = syn.schema_id
  WHERE
    schm.ObjectType = 'SYNONYM' AND
    schm.StillExists = 0 AND
    syn.object_id IS NOT NULL


  UPDATE schm
  SET
    StillExists = 1
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.types typ ON
      schm.ObjectName = typ.name AND
      sch.schema_id = typ.schema_id
  WHERE
    schm.ObjectType = 'TYPE' AND
    schm.StillExists = 0 AND
    typ.user_type_id IS NOT NULL

  UPDATE schm
  SET
    StillExists = 1
  FROM
    sqlver.tblSchemaManifest schm 

    LEFT JOIN sys.schemas sch ON
      schm.SchemaName = sch.name 
    LEFT JOIN sys.triggers tg ON
      schm.ObjectName = tg.name
    LEFT JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id AND
      tg.object_id = obj.object_id

  WHERE
    schm.ObjectType = 'TRIGGER' AND
    schm.StillExists = 0 AND
    tg.object_id IS NOT NULL AND (tg.parent_class = 0 OR obj.object_id IS NOT NULL)

END

GO


IF OBJECT_ID('[sqlver].[spusrSchemaObjectCategorize]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spusrSchemaObjectCategorize]
END
GO

CREATE PROCEDURE [sqlver].[spusrSchemaObjectCategorize]
@ObjectManifestId int = NULL
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  If this procedure exists, it is called by the dtgSQLVerLogSchemaChange
  to categorize objects.

  It may also be called manually.

  Modify this procedure as needed to set sqlver.tblSchemaManifest.ObjectCategory
  as you see fit.

  Object categories can be passed as a comma-separated list into @ObjectCategories
  when calling sqlver.spsysSchemaVersionUpdateFromMaster... to indicate which
  categories of objects should be updated.
  */

  UPDATE schm
  SET
      ObjectCategory = 
      CASE
        WHEN
          (
            schm.SchemaName = 'opsstream' AND

            (
              schm.ObjectName = 'vwQXDLabelUsers'

              OR
              (
                PATINDEX('%$$%', schm.ObjectName) = 0 AND

                (
                PATINDEX('%QXD[_]%', schm.ObjectName) > 0 OR
                PATINDEX('%tblQXDH[_]%', schm.ObjectName) > 0 OR
                PATINDEX('%QXDix[_]%', schm.ObjectName) > 0 OR
                PATINDEX('%QXDixnuq[_]%', schm.ObjectName) > 0 OR          
                PATINDEX('%QXDLabel[_]%', schm.ObjectName) > 0
                ) AND

                (
                x.ParentObject LIKE 'tblQXD%' OR
                x.ParentObject LIKE 'tblQXDH[_]%' OR 
                x.ParentObject LIKE 'vwQXD%' OR
                x.ParentObject LIKE 'spgetQXD%' OR
                x.ParentObject LIKE 'spinsQXD%' OR
                x.ParentObject LIKE 'spupdQXD%' OR
                x.ParentObject LIKE 'spdelQXD%' OR
                x.ParentObject LIKE 'vwQXDLabel%' OR
                x.ParentObject LIKE 'vwQXDix%'
                )
              )
            )
          )
          THEN 1001
        WHEN schm.ObjectName = 'dtgSQLVerLogSchemaChanges' AND NULLIF(RTRIM(SchemaName), '') IS NULL THEN 1003
        WHEN schm.SchemaName = 'opsstream' THEN 1000
        WHEN schm.SchemaName = 'opsusr' THEN 1002
        WHEN schm.SchemaName = 'sqlver' THEN 1003
        WHEN schm.SchemaName IN ('geonames', 'sdom', 'sws', 'theas') THEN 1004
        ELSE 0        
      END
  FROM
    sqlver.tblSchemaManifest schm
    JOIN (
      SELECT
        schm.SchemaManifestID,
        COALESCE(sch.name, schm.SchemaName) AS ParentSchema,
        COALESCE(obj.name, schm.ObjectName) AS ParentObject
      FROM
        sqlver.tblSchemaManifest schm

        LEFT JOIN sys.triggers tg ON
          schm.ObjectName = tg.name 
        LEFT JOIN sys.schemas sch ON
          schm.SchemaName = sch.name
        LEFT JOIN sys.objects obj ON
          tg.parent_id = obj.object_id AND
          sch.schema_id = obj.schema_id
      WHERE
        (@ObjectManifestId IS NULL OR
         schm.SchemaManifestID = @ObjectManifestId 
        )
      ) x ON
        schm.SchemaManifestID = x.SchemaManifestID
    WHERE
      (@ObjectManifestId IS NULL OR
       schm.SchemaManifestID = @ObjectManifestId
      )

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaProcessObject]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaProcessObject]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaProcessObject]
@SchemaName sysname = NULL,
@ObjectName sysname = NULL,
@EventData xml = NULL,
@ForceSchemaBinding bit = NULL,
@ForceExecuteAs bit = NULL,
@SkipExec bit = 0
AS
BEGIN
  --You may set @Debug = 1 to output verbose debugging messages from this trigger.
  DECLARE @Debug bit
  SET @Debug = 0

  /*
  DATABASE trigger to log DDL changes to sqlver.tblSchemaLog

  1) Object will be added to sqlver.tblSchemaManifest if it does
     not exist

  2) WriteProtect and SkipLogging flags in sqlver.tblSchemaManifest
     will be respected

  3) Log entry will be added to sqlver.tblSchemaLog if applicable
     and if hash is different from previous hash

  4) Hash is always generated on the "CREATE xxx" version of the
     the SQL statement, but the actual statement ("ALTER xxx" or
     "CREATE xxx") is stored in sqlver.tblSchemaLog

  5) If the object is a table or an index, the hash will be based
     upon the full script for the CREATE TABLE, incluing all indexes.
     This generated script will also be saved in:
     sqlver.tblSchemaLog.SQLFullTable

  5) May include a special block comment that starts with /ver

     Such a comment will be stored in sqlver.tblSchemaLog but will be
     stripped from the code prior to generating hash and storing
     in sqlver.tblSchemaLog  (useful for version-specific change log
     messages)

     Only the first such comment in the code block will be logged.
     If special comment is present but hash is unchanged, comment will
     be appended to prior comment in prior log entry.  (Allows appending
     a comment after the object has been updated)

     Note that such a comment will cause the DDL statement (after stripping
     the comment) to be executed

     If this comment includes /manifest, the comment text prior to
     /manifest will be logged to sqlver.tblSchemaLog but the subsequent
     text will be saved to sqlver.tblSchemaManifest, overwriting any
     value stored in sqlver.tblSchemaManifest.Comments  (useful for storing
     object-level notes).  You may also manually update sqlver.tblSchemaManifest
     if you like.

  6) Alter this trigger to set @RequireVerComment = 1 to throw an error
     if a /ver comment is not provided

  7) Alter this trigger to set @Visible=false to disable output of
     messages

  8) If object is encrypted (WITH ENCRYPTION) the change will be logged,
     but the actual code will be recorded as a new GUID (since the
     code is not available due to encryption)

  9) May insert a copyright message in @tvCopyrightMsgs, or multiple
     messages--each for a specific schema name.  The applicable message
     will be injected into the SQL statement.
  
  */
  SET NOCOUNT ON

  IF @EventData IS NULL AND OBJECT_ID(CONCAT(@SchemaName, '.', @ObjectName)) IS NULL BEGIN
    RETURN  --Nothing to do. Exit
  END

  --Set @Visible = 1 to PRINT a SQLVer message each time an object is modified.
  --SET @Visible = 0 to have SQLVer work without outputting PRINT messages

  DECLARE @Visible bit
  IF @@NESTLEVEL <= 2 BEGIN
    --Running due to an interactive DDL command
    SET @Visible = 1
  END
  ELSE BEGIN
    --Running due to DDL from a stored procedure
    SET @Visible = 0
  END

  IF @ForceSchemaBinding IS NOT NULL AND @SkipExec = 1 BEGIN
    SET @SkipExec = 0
  END

  DECLARE @Nested bit
  DECLARE @ChangeDetected bit

  --Set @RequireVerComment to force each object change to include a logged version comment
  DECLARE @RequireVerComment bit
  SET @RequireVerComment = 0

  DECLARE @tvCopyrightMsgs TABLE (
    SchemaName sysname NULL,
    CopyrightMsg nvarchar(MAX)
  )

  /*
  Different schemas may need different copyright information added to comments in
  each object.  You could create a table that contains these messages, but a this
  point this trigger simply uses the hard-coded messages below.
  */

  INSERT INTO @tvCopyrightMsgs (SchemaName, CopyrightMsg)
  VALUES
    --Default message (specified with SchemaName = '*')
    ('*', ''),

    --Schema-specific messages
    (
    'sqlver',
'--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)'
    ),

    (
    'sdom',
'--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqldom)'
    ),

    (
    'theas',
'--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/theas)'
    ),

    (
    'sws',
'--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --SWS (SQLWebShim) is a deprecated way of using SQL to generate HTML responses'
    )

  IF EXISTS(SELECT schema_id FROM sys.schemas WHERE name = 'opsstream') BEGIN
    INSERT INTO @tvCopyrightMsgs (SchemaName, CopyrightMsg)
    VALUES
    (
    'opsstream',
'--©Copyright 2006-2019 by David Rueter, Automated Operations, Inc.
--May be held, used or transmitted only pursuant to an in-force licensing agreement with Automated Operations, Inc.
--Contact info@opsstream.com / 800-964-3646 / 949-264-1555'
    )
  END
  --------------------------------------------------------------

  DECLARE @Msg nvarchar(MAX)
   
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spSysSchemaProcessObject: Starting'
    PRINT @Msg
  END
  
  DECLARE @CRLF nvarchar(5)
  SET @CRLF = NCHAR(13) + NCHAR(10)

  BEGIN TRY
    --retrieve trigger event data
    --DECLARE @EventData xml
    --SET @EventData = EVENTDATA()
    

    DECLARE @SkipLogging bit
    DECLARE @IsEncrypted bit
    
    DECLARE @DatabaseName sysname
    --DECLARE @SchemaName sysname
    --DECLARE @ObjectName sysname
    DECLARE @IndexName sysname
    DECLARE @EventType varchar(50)
    DECLARE @ObjectType varchar(25)
    DECLARE @QualifiedName varchar(775)
    DECLARE @ObjectId int
    DECLARE @OrigSQLFromEvent nvarchar(MAX)
    DECLARE @SQLFromEvent nvarchar(MAX)
    DECLARE @SQLForHash nvarchar(MAX)
    DECLARE @SQLStripped nvarchar(MAX)
    
    DECLARE @SPID smallint
    DECLARE @LoginName sysname
    DECLARE @EventDate datetime

    DECLARE @LastSchemaLogId int
    DECLARE @SchemaLogId int
    
    DECLARE @ManifestId int
    DECLARE @OrigDefinitionIsNull int

    DECLARE @CalculatedHash varbinary(128)
    DECLARE @StoredHash varbinary(128)
    DECLARE @StoredHashManifest varbinary(128)
    DECLARE @WriteProtected bit
    DECLARE @StoredExecuteAs sysname
    DECLARE @ExistingObject bit
    DECLARE @StoredSkipLogging bit
    --DECLARE @ForceSchemaBinding bit


    DECLARE @ThisComment nvarchar(MAX)    
    DECLARE @ThisManifestComment nvarchar(MAX)
    DECLARE @Comments nvarchar(MAX)
    DECLARE @CommentAlreadyExists bit

    
    DECLARE @HasEmbeddedComment bit
    SET @HasEmbeddedComment = 0
    
    DECLARE @NeedExec bit
    SET @NeedExec = 0
    
    DECLARE @Buf nvarchar(MAX)
    DECLARE @P int
    DECLARE @P1 int
    DECLARE @P2 int
    DECLARE @PWithClause int
    DECLARE @WithClause nvarchar(MAX)
    DECLARE @PAs int
    DECLARE @PMarker int
    DECLARE @PMarker2 int

    DECLARE @SVMarker nvarchar(MAX)
    SET @SVMarker = '--$!' + 'SQLVer ' + ISNULL(CAST(GETDATE() AS varchar(100)), '') + ISNULL(' by ' + SYSTEM_USER, '') + @CRLF
    
    DECLARE @UserID int

    SET @SkipLogging = 0

    --Get OpsStream user, if applicable
    IF OBJECT_ID('opsstream.vwSysCurUser') > 0 BEGIN
      SELECT @UserID = UserID FROM opsstream.vwSysCurUser
    END

    IF @EventData IS NULL AND (@SchemaName IS NOT NULL OR @ObjectName IS NOT NULL) BEGIN
      IF @Debug = 1 PRINT 'NOT using XML event data'

      --Allow @SchemaName to contain schema.object or [schema].[object]
      IF @ObjectName IS NULL BEGIN
        SET @ObjectName = PARSENAME(@SchemaName, 1)
        SET @SchemaName = PARSENAME(@SchemaName, 2)
      END
      ELSE BEGIN
        SET @ObjectName = PARSENAME(@ObjectName, 1)
        SET @SchemaName = PARSENAME(@SchemaName, 1)
      END
            
      SET @DatabaseName = DB_NAME()

      SET @SPID = @@SPID
      SET @EventType = 'REPARSE'
      SET @LoginName = SYSTEM_USER
      SET @EventDate = GETDATE()

      IF @ObjectName = 'dtgSQLVerLogSchemaChanges' AND NULLIF(RTRIM(@SchemaName), '') IS NULL BEGIN
        SELECT
          @SchemaName = '',
          @ObjectId = tg.object_id,
          @ObjectType = 'TRIGGER',
          @SQLFromEvent = smod.[definition] 
        FROM
          sys.triggers tg
          LEFT JOIN sys.sql_modules smod ON
            tg.object_id = smod.object_id
        WHERE
          tg.name = @ObjectName AND
          tg.parent_class = 0
      END
   
      ELSE BEGIN
        SET @ObjectId = NULL

        --See if it is a synonym
        SELECT
          @ObjectId = syn.object_id,
          @ObjectType = 'SYNONYM',
          @SQLFromEvent = 'CREATE SYNONYM ' + '[' + sch.name + '].[' + syn.name + '] FOR ' + syn.base_object_name
        FROM
          sys.synonyms syn
          JOIN sys.schemas sch ON
            syn.schema_id = sch.schema_id
        WHERE
          sch.name = @SchemaName AND
          syn.name = @ObjectName 

        --See if it is a type
        SELECT
          @ObjectId = typ.system_type_id,
          @ObjectType = 'TYPE',
          @SQLFromEvent = sqlver.udfScriptType(@SchemaName, @ObjectName)
        FROM
          sys.types typ
          JOIN sys.schemas sch ON
            typ.schema_id = sch.schema_id
        WHERE
          sch.name = @SchemaName AND
          typ.name = @ObjectName 

        --Otherwise try to figure out the type
        IF @ObjectId IS NULL BEGIN
          SELECT
            @ObjectId = obj.object_id,
            @ObjectType = 
              CASE obj.[Type]
                WHEN 'FN' THEN 'FUNCTION' --  SQL_SCALAR_FUNCTION
                --'FS',--  CLR_SCALAR_FUNCTION
                --'FT',--  CLR_TABLE_VALUED_FUNCTION
                WHEN 'IF' THEN 'FUNCTION'  --SQL_INLINE_TABLE_VALUED_FUNCTION
                WHEN 'P' THEN 'PROCEDURE'  --SQL_STORED_PROCEDURE
                --'PC',--  CLR_STORED_PROCEDURE
                WHEN 'V' THEN 'VIEW'   --VIEW
                WHEN 'TF' THEN 'FUNCTION' --SQL_TABLE_VALUED_FUNCTION
                WHEN 'TR' THEN 'TRIGGER' --SQL_TRIGGER
                WHEN 'U' THEN 'TABLE' -- USER_TABLE
                WHEN 'SN' THEN 'SYNONYM' --SYNONYM           
              END,
            @SQLFromEvent = CASE WHEN obj.[Type] IN ('U') THEN sqlver.udfScriptTable(@SchemaName, @ObjectName) ELSE smod.[definition] END
          FROM
            sys.schemas sch
            JOIN sys.objects obj ON
              sch.schema_id = obj.schema_id
            LEFT JOIN sys.sql_modules smod ON
              obj.object_id = smod.object_id
          WHERE
            sch.name = @SchemaName AND
            obj.name = @ObjectName AND
            obj.type IN (
              'FN',--  SQL_SCALAR_FUNCTION
              --'FS',--  CLR_SCALAR_FUNCTION
              --'FT',--  CLR_TABLE_VALUED_FUNCTION
              'IF',--  SQL_INLINE_TABLE_VALUED_FUNCTION
              'P', --   SQL_STORED_PROCEDURE
              --'PC',--  CLR_STORED_PROCEDURE
              'V', --   VIEW
              'TF',--  SQL_TABLE_VALUED_FUNCTION
              'TR', --SQL_TRIGGER
              'U',  -- USER_TABLE
              'SN' --SYNONYM
            )
        END
      END
    END
    ELSE BEGIN

      IF @Debug = 1 BEGIN
        PRINT 'Getting values from event XML'
      END

      --grab values from event XML
      SET @ObjectType = @EventData.value('(/EVENT_INSTANCE/ObjectType)[1]', 'varchar(25)')
      SET @DatabaseName = @EventData.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'sysname')
      SET @SchemaName = @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname')
      SET @SPID = @EventData.value('(/EVENT_INSTANCE/SPID)[1]', 'smallint');
    
      SET @ObjectName = CASE
                          WHEN @ObjectType = 'INDEX' THEN @EventData.value('(/EVENT_INSTANCE/TargetObjectName)[1]', 'sysname')
                          ELSE @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname')
                        END

      IF @ObjectType = 'INDEX' BEGIN
        SET @IndexName = @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname')
      END

      SET @EventType = @EventData.value('(/EVENT_INSTANCE/EventType)[1]', 'varchar(50)')
      SET @LoginName = @EventData.value('(/EVENT_INSTANCE/LoginName)[1]', 'sysname')
      SET @EventDate = COALESCE(@EventData.value('(/EVENT_INSTANCE/PostTime)[1]', 'datetime'), GETDATE())
    
      SET @SQLFromEvent = @EventData.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'nvarchar(MAX)')

      SET @QualifiedName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
      SET @ObjectId = OBJECT_ID(@QualifiedName) 
    

      IF @Debug = 1 BEGIN
        PRINT 'Done getting values from event XML'
      END

    END



    SET @QualifiedName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)  

    --Switch ALTER to CREATE for hash
    --Find the first ALTER that is in uncommented SQL code

    SET @P = sqlver.udfFindInSQL('ALTER', @SQLFromEvent, 0)
    SET @P2 = sqlver.udfFindInSQL('CREATE', @SQLFromEvent, 0)

    IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
      SET @SQLFromEvent = STUFF(@SQLFromEvent, @P, LEN('ALTER'), 'CREATE')
    END

    --fetch appropriate copyright message
    DECLARE @CopyrightMsg nvarchar(MAX)

    SELECT
      @CopyrightMsg = cm.CopyrightMsg
    FROM
      @tvCopyrightMsgs cm
    WHERE
      cm.SchemaName = @SchemaName 

    IF @CopyrightMsg IS NULL BEGIN
      --Get default message
      SELECT
      @CopyrightMsg = cm.CopyrightMsg
    FROM
      @tvCopyrightMsgs cm
    WHERE
      cm.SchemaName = '*'
    END


    SET @CopyrightMsg = ISNULL(NULLIF(sqlver.udfRTRIMSuper(@CopyrightMsg), '') + @CRLF, '') + 
      '--Note: Comments after $!' + 'SQLVer and before AS are subject to automatic removal'

    /*
    We need to manipulate this SQL statement.  Certain things should
    be excluded from the hash calculation, certain things (like
    WITH SCHEMABINDING) may need to be added or removed, and certain
    things should be consistently added to comments.
      1) Strip out the WITH clause that occurs before the AS
         before calculating the hash.  (Other WITH clauses, such
         as CTEs should not be affected)
         , but include WITH before executing
      2) Add in generated WITH clause as needed
      3) Remove existing comments that occur before the AS
      4) Add in copyright message in comment before the AS
      5) Change ALTER to CREATE for the hash calculation
      6) Change CREATE to ALTER for the actual updating of the
         object's code

    Finding the right 'AS' and the right 'WITH' is tricky, as these can
    occur in multiple places.  Once we find these, we replace them with
    temporary tokens of '{{!' + 'AS!}}' and '{{!' + 'WITH!}}' to make it easier to
    perform the required string manipulation.  Then these temporary tokens
    are replaced with AS and WITH prior to executing the ALTER SQL.
    
    NOTE:  Whenver these temporary tokens are embedded as string literals
    here in this code, we "escape" then as '{{!' + 'AS!}}' so that this
    code does not self-modify itself in a bad way!
    */

    --Trim leading and trailing whitespace
    SET @SQLFromEvent = sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(@SQLFromEvent))
    SET @OrigSQLFromEvent = @SQLFromEvent

    --Search the SQL code to find WITH clause.  We are looking for the first
    --WITH that occurs in uncommented SQL code...but we realize that such
    --a WITH may occur after AS...in which case it is a false-positive that
    --should be ignored.  We will make that determination later
    SET @PWithClause = sqlver.udfFindInSQL('WITH', @SQLFromEvent, 0)

    --If we found a WITH, assume that the WITH clause is on a single line.
    --This is a SQLVer limitation:  a module-level WITH (i.e. WITH before the AS)
    --MUST be confined to a single line.
    IF @PWithClause > 0 BEGIN
      SET @WithClause = sqlver.udfSubstrToDelims(@SQLFromEvent, @PWithClause, '\eol')
    END

    --Search the SQL code to find the first AS in uncomment code
    SET @PAs = sqlver.udfFindInSQL('AS', @SQLFromEvent, 0)
    IF @PAs >= @PWithClause AND @PAs < @PWithClause + LEN(@WithClause) BEGIN
      --an AS in the WITH clause (such as WITH EXECUTE AS OWNER) does not count
      SET @PAs = sqlver.udfFindInSQL('AS', @SQLFromEvent, @PWithClause + LEN(@WithClause) - 1)
    END

    --Ignore the WITH clause if it comes after the AS
    IF @PWithClause > @PAs BEGIN
      --WITH clause is a false-positive, such as a WITH in a CTE
      SET @PWithClause = NULL
      SET @WithClause = NULL
    END

    DECLARE @BeforeAs nvarchar(MAX)

    --If we found an AS, perform the string manipulation
    IF @PAs > 0 BEGIN

      SET @SQLFromEvent = sqlver.udfRTRIMSuper(LEFT(@SQLFromEvent, @PAs -1)) +  '{{!' + 'AS!}}' + @CRLF +
        sqlver.udfLTRIMSuper(RIGHT(@SQLFromEvent, LEN(@SQLFromEvent) - @PAs + 1 - 2))

      SET @PAs = PATINDEX('%{{!' + 'AS!}}%', @SQLFromEvent)

      --Gather all the code that occurs before the AS
      SET @BeforeAs = sqlver.udfRTRIMSuper(LEFT(@SQLFromEvent, @PAs - 1))

      --Note:  could perform modifidations on @BeforeAs here, such as to remove old-style strings
      --IF @ObjectName <> 'sqlver.spSysSchemaProcessObject' BEGIN
      --  SET @BeforeAs = REPLACE(@BeforeAs, @CopyrightMsg, '')
      --END

      SET @PMarker = PATINDEX('%--$!' + 'SQLVer%', @BeforeAs)

      --handle old-style marker
      SET @PMarker2 = NULLIF(PATINDEX('%--$!' + 'ParseMarker%', @BeforeAs), 0)
      IF @PMarker2 < @PMarker BEGIN
        SET @PMarker = @PMarker2
      END

      IF @PMarker > 0 BEGIN
        --Replace with '{{!' + 'ParseMarker!}}'
        SET @BeforeAs = sqlver.udfRTRIMSuper(LEFT(@BeforeAs, @PMarker - 1)) +
          @CRLF + '{{!' + 'ParseMarker!}}' + 
          ISNULL( 
            NULLIF(
              sqlver.udfRTRIMSuper(
                sqlver.udfLTRIMSuper(
                  sqlver.udfStripSQLCommentsExcept(
                    SUBSTRING(@BeforeAs, @PMarker, LEN(@BeforeAs)),
                    '$$'
                  ) --strip all comments before the AS, EXCEPT for comments that start with $$
                )
              )
            , '')
            + @CRLF
          , '')       
      END
      ELSE BEGIN
        SET @BeforeAs = sqlver.udfRTRIMSuper(@BeforeAs) + @CRLF + '{{!' + 'ParseMarker!}}'
      END

      IF @PWithClause > 0 BEGIN
        --We already found a WITH clause before AS above.  But now we need to find
        --the position of the WITH within @BeforeAs, because comments and other whitespace
        --has now been trimmed
        SET @PWithClause = sqlver.udfFindInSQL('WITH', @BeforeAs, 0)
        SET @WithClause = sqlver.udfSubstrToDelims(@BeforeAs, @PWithClause, '\eol')

        --Replace the WITH with the temporary token '{{!' + 'WITH!}}'
        DECLARE @AfterWith nvarchar(MAX)
        SET @AfterWith = sqlver.udfLTRIMSuper(RIGHT(@BeforeAs, LEN(@BeforeAs + 'x') - 1 - (@PWithClause + LEN(@WithClause + 'x') - 1 ) + 1 ))         

        SET @BeforeAs = sqlver.udfRTRIMSuper(LEFT(@BeforeAs, @PWithClause -1)) + @CRLF + 
                        '{{!' + 'WITH!}}' +
                         ISNULL(@AfterWith, '')
      END
   
      --update @SQLFromEvent with the updated @BeforeAS
      SET @SQLFromEvent = ISNULL(@BeforeAs, '') + RIGHT(@SQLFromEvent, LEN(@SQLFromEvent + 'x') - 1 - @PAs + 1)

      --update the position of AS and WITH within the updated @SQLFromEvent string
      SET @PAs = PATINDEX('%{{!' + 'AS!}}%', @SQLFromEvent)
      SET @PWithClause = PATINDEX('%{{!' + 'WITH!}}%', @SQLFromEvent)

    END

    /*
    SQLVer wants to allow SCHEMABINDING to be controlled by a
    flag in sqlver.tblSchemaLog.

    Note that if sqlver.tblSchemaManifest.ForceSchemaBinding is not null
    the SCHEMABINDING may be changed from from what the original DDL
    statement had.
    */

    --Determine if the DDL statement specifies WITH SCHEMABINDING
    DECLARE @HasSchemabinding bit
    SET @HasSchemabinding = 0
    IF PATINDEX('%SCHEMABINDING%', @WithClause) > 0 BEGIN
      SET @HasSchemabinding = 1
    END

    --WITH EXECUTE AS does not affect the version hash, but we
    --do store this in sqlver.tblSchemaManifest, and we do warn
    --if the DDL makes a change to this.
    DECLARE @ExecuteAs sysname
    SET @P = PATINDEX('%EXECUTE AS%', @WithClause)
    IF @P > 0 BEGIN
      SET @ExecuteAs = sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(SUBSTRING(@WithClause, @P + LEN('EXECUTE AS'), LEN(@WithClause))))

      SET @P2 = PATINDEX('%[; ,' + @CRLF + ']%', @ExecuteAs)

      IF @P2 > 0 BEGIN
        SET @ExecuteAs = LEFT(@ExecuteAs, @P2)
      END
      SET @ExecuteAs = sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(@ExecuteAs))
    END


    --Retrieve manifest data
    IF @Debug = 1 BEGIN
      SET @Msg = 'sqlver.spSysSchemaProcessObject: Retrieving from sqlver.tblSchemaManifest'
      PRINT @Msg
    END
  
    SELECT
      @ManifestId = m.SchemaManifestId,
      @StoredHashManifest = m.CurrentHash,
      @StoredSkipLogging = ISNULL(m.SkipLogging, 0),
      @WriteProtected = ISNULL(m.WriteProtected, 0),
      @StoredExecuteAs = NULLIF(RTRIM(m.ExecuteAs), ''),
      @ForceSchemaBinding = COALESCE(@ForceSchemaBinding, m.ForceSchemaBinding),
      @ForceExecuteAs = COALESCE(@ForceExecuteAs, CASE WHEN m.ExecuteAs LIKE '!%' THEN 1 ELSE 0 END), --!OWNER or !Caller means force
      @ExistingObject = ISNULL(m.StillExists, 0),
      @OrigDefinitionIsNull = CASE WHEN NULLIF(RTRIM(REPLACE(m.OrigDefinition, '--ENCRYPTED--', '')), '') IS NULL THEN 1 ELSE 0 END
    FROM
      sqlver.tblSchemaManifest m
    WHERE
      m.SchemaName = @SchemaName AND
      m.ObjectName = @ObjectName

      
    IF @WriteProtected = 1 BEGIN
      --sqlver.tblSchemamanifest.WriteProtected is set, so throw an
      --error to roll back this modification
      SET @Msg = 'Cannot modify ' + @DatabaseName + '.' + @SchemaName + '.' + @ObjectName + ' because object is flagged as write-protected by SQLVer.  See sqlver.tblSchemaManifest'  
      RAISERROR(@Msg, 16, 1)
      RETURN
    END
    
    IF @Debug = 1 BEGIN
      PRINT '@ObjectType=' + ISNULL(@ObjectType, 'NULL')
    END

    --Determine if we should skip the logging of this DDL change
    IF @StoredSkipLogging = 1 BEGIN
      SET @SkipLogging = @StoredSkipLogging
    END
    
    IF @SQLFromEvent LIKE 'ALTER INDEX%' BEGIN
      SET @P1 = sqlver.udfFindInSQL('REBUILD', @SQLFromEvent, 0)

      IF @P1 > 0 AND @PWithClause > @P1 BEGIN
        SET @SkipLogging = 1
        --We don't want to log index rebuilds
      END
    END 
    
    --IF @ObjectType = 'TABLE' AND PATINDEX('%sqlver[_][_]reparse%', @SQLFromEvent) > 0 BEGIN
    --  SET @SkipLogging = 1
    --END                

    SET @IsEncrypted = CASE WHEN @SQLFromEvent = '--ENCRYPTED--' THEN 1 ELSE 0 END
    
    IF @ObjectType IN ('TABLE', 'INDEX') BEGIN
      --Retrieve the complete definition of the table
      SET @SQLForHash = sqlver.udfScriptTable(@SchemaName, @ObjectName)
    END

    ELSE IF @IsEncrypted = 1 BEGIN
      --We will assume that the DDL for the object is being updated.
      --Since we can't calculate a hash on the actual statement, we'll calculate a
      --hash on a new GUID to force a unique hash.  This way this event will
      --be treated as a new update that needs to be logged.
      SET @SQLForHash = CAST(NEWID() AS nvarchar(MAX))
    END

    ELSE BEGIN
      SET @SQLStripped = @SQLFromEvent

      DECLARE @IncludeMarker nvarchar(MAX)

      DECLARE @PInclude int
      DECLARE @PIncludeEnd int
      DECLARE @ThisInclude nvarchar(MAX)
      DECLARE @ThisIncludeExec nvarchar(MAX)
      DECLARE @ThisIncludeResult nvarchar(MAX)

    --xxxxxxxxxxxxxxxx

    --@SQLStripped is ready, except for special SQLVer processing

      IF ISNULL(@SchemaName, '') <> 'sqlver' AND
        ISNULL(@ObjectName, '') <> 'spsysSchemaProcessObject' BEGIN

        SET @IncludeMarker = '--$$SQLVer:Include:'

        SET @PInclude = PATINDEX('%' + @IncludeMarker + '%', @SQLStripped)
        
        WHILE @PInclude > 0 BEGIN
          BEGIN TRY
            SET @NeedExec = 1
            SET @ThisInclude = sqlver.udfSubstrToDelims(@SQLStripped, @PInclude, NCHAR(10)) + NCHAR(10)

            SET @PIncludeEnd = 
              ISNULL(
              NULLIF(PATINDEX(
                '%' + REPLACE(@ThisInclude, '$$', '$End') + '%',
                SUBSTRING(@SQLStripped, @PInclude, LEN(@SQLStripped))
              ), 0) +
              LEN(REPLACE(@ThisInclude, '$$', '$End')) + LEN(@CRLF) -3, 0)

            SET @ThisIncludeExec = REPLACE(@ThisInclude, @IncludeMarker, '')
            EXEC sqlver.sputilResultSetAsStr @SQL = @ThisIncludeExec, @Result = @ThisIncludeResult OUTPUT

            SET @ThisIncludeResult = 
              REPLACE(@ThisInclude, '$$', '$!') +
              ISNULL(@ThisIncludeResult, '') + @CRLF +
              REPLACE(@ThisInclude, '$$', '$End')

            IF @PIncludeEnd > 0 BEGIN
              SET @SQLStripped = STUFF(@SQLStripped, @PInclude, @PIncludeEnd, @ThisIncludeResult)
            END
            ELSE BEGIN 
              SET @SQLStripped = STUFF(@SQLStripped, @PInclude, LEN(@ThisInclude), @ThisIncludeResult)
            END

            SET @PInclude = PATINDEX('%' + @IncludeMarker + '%', @SQLStripped)
          END TRY
          BEGIN CATCH
            PRINT 'sqlver.spSysSchemaProcessObject: Error processing $$SQLVer:Include (' + ISNULL(@ThisInclude, '') + ') ' + ERROR_MESSAGE()
            SET @PInclude = 0
          END CATCH
        END

        SET @SQLStripped = REPLACE(@SQLStripped,  REPLACE(@IncludeMarker, '$$', '$!'), @IncludeMarker) 

      END

    --xxxxxxxxxxxxxxxxxx

      
      /*
      SQLVer looks for special block comments that begin with /ver

      These special comments are designed to be version-specific
      comments (such as for a change log).

      For these special comments, we remove the comments from the code,
      but store them in the table sqlver.tblSchemaLog
      */

      --Strip out special SQLVer comments
      DECLARE @Marker nvarchar(10)
      SET @Marker = '/*' + CHAR(47) + 'ver'

      SET @P = PATINDEX('%' + @Marker + '%', @SQLStripped)

      IF @P > 0 BEGIN      
        DECLARE @SQLLen int

        SET @SQLLen = LEN(@SQLStripped + 'x') - 1 
        SET @Buf = RIGHT(@SQLStripped, @SQLLen - @P + 1 - LEN(@Marker))   

        SET @P2 = PATINDEX('%*' + '/%', @Buf)
        SET @Buf = LEFT(@Buf, @P2 - 1)
        SET @ThisComment = ISNULL(NULLIF(RTRIM(@Buf), ''), '')

        DECLARE @SQLBeforeComment nvarchar(MAX)
        DECLARE @SQLAfterComment nvarchar(MAX)
        SET @SQLBeforeComment = LEFT(@SQLStripped, @P - 1)
        SET @SQLAfterComment = SUBSTRING(@SQLStripped, @P + LEN(@Marker) + LEN(@ThisComment + 'x') + LEN('/**/') - 1, LEN(@SQLStripped))

        --Remove extraneous CRLF
        IF RIGHT(@SQLBeforeComment, LEN(@CRLF + 'x') - 1) = @CRLF AND
            LEFT(@SQLAfterComment, LEN(@CRLF + 'x') - 1) = @CRLF BEGIN
          SET @SQLAfterComment = SUBSTRING(@SQLAfterComment, LEN(@CRLF + 'x') - 1 + 1, LEN(@SQLAfterComment))
        END
         

        IF @Debug = 1 BEGIN
          PRINT '>>>>@SQLBeforeComment:' + @SQLBeforeComment
          PRINT '>>>>@SQLAfterComment:' + @SQLAfterComment
        END

        SET @SQLStripped = 
              sqlver.udfRTRIMSuper(
                @SQLBeforeComment +
                @SQLAfterComment
              )   

        SET @ThisComment = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(@ThisComment))
        SET @ThisComment = NULLIF(@ThisComment, '')

      
        /*
        Within a special /ver comment, SQLVer looks for a special comment that
        begins with /manifest.

        This special comment is understood not to be version-specific, but
        rather object-specific, to be stored in sqlver.tblObjectManifest.
        
        This comment is stripped out of the version-specific comment.
        */

        SET @P2 = PATINDEX('%/' + 'manifest%', @ThisComment)
        IF @P2 > 0 BEGIN
          SET @ThisManifestComment = sqlver.udfLTRIMSuper(RIGHT(@ThisComment, LEN(@ThisComment + 'x') - 1 - @P2 - LEN('/manifest')))
          SET @ThisComment = sqlver.udfRTRIMSuper(LEFT(@ThisComment, @P2 -1))
        END

        IF @ThisComment IS NOT NULL BEGIN
          SET @Comments = ISNULL(@Comments + ' | ', '') + @ThisComment

          SET @HasEmbeddedComment = 1      
        END
      END

      IF @RequireVerComment = 1 AND ISNULL(@HasEmbeddedComment, 0) = 0 BEGIN
        SET @Msg = 'You must include a version comment regarding your changes.  Add a block comment that starts with /ver such as:' + @CRLF +
        '/*' + CHAR(47) + 'ver' + @CRLF + 
        'Changed xxx to because yyy.' + @CRLF +
        '*/'
        RAISERROR(@Msg, 16, 1)
      END

      --Now strip out WITH clause for hash
      SET @SQLForHash = REPLACE(REPLACE(
                          @SQLStripped, @CRLF + '{{!' + 'WITH!}}', ''),
                          '{{!' + 'AS!}}', @CRLF + 'AS')

    END    
    

    /*
    We want the version hash to always be calculated on a CREATE
    and never an ALTER.
    
    For example, if an object is created, then altered, then
    altered again to remove the first changes, the resulting
    hash should match the original hash from when the object
    was created.
    */

    --Switch ALTER to CREATE for hash
    --Find the first ALTER that is in uncommented SQL code
    SET @P = sqlver.udfFindInSQL('ALTER', @SQLForHash, 0)
    SET @P2 = sqlver.udfFindInSQL('CREATE', @SQLForHash, 0)

    IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
      SET @SQLForHash = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(LEFT(@SQLForHash, @P - 1) + 'CREATE' + RIGHT(@SQLForHash, LEN(@SQLForHash) - LEN('ALTER'))))
    END

    SELECT
      @LastSchemaLogId = MAX(schl.SchemaLogId)
    FROM
      sqlver.tblSchemaLog schl
    WHERE
      schl.SchemaName = @SchemaName AND
      schl.ObjectName = @ObjectName          
 
  
    IF @ObjectType NOT IN ('TABLE', 'INDEX', 'TYPE') AND
        (@ThisComment IS NOT NULL OR
        @ThisManifestComment IS NOT NULL OR
        NULLIF(RTRIM(@CopyrightMsg), '') IS NOT NULL OR
        @ForceSchemaBinding IS NOT NULL) BEGIN
      SET @NeedExec = 1
    END
          
    SELECT @StoredHash = schl.Hash
    FROM
      sqlver.tblSchemaLog schl
    WHERE
      @LastSchemaLogId = schl.SchemaLogId
        
    SET @StoredHash = COALESCE(@StoredHash, @StoredHashManifest)
      
      
    IF @Debug = 1 BEGIN
      SET @Msg = 'sqlver.spSysSchemaProcessObject: Calculating hash'
      PRINT @Msg
    END

    IF @EventType NOT LIKE 'DROP%' BEGIN
      SET @CalculatedHash = NULL
      SET @CalculatedHash =  sqlver.udfHashBytesNMax(DEFAULT, @SQLForHash)
    END
      
    IF (@CalculatedHash = @StoredHash) BEGIN
      --Hash matches.  Nothing has changed.
      SET @ChangeDetected = 0

      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spSysSchemaProcessObject: Hash matches.  Nothing has changed.'
        PRINT @Msg
      END
        
      IF @Comments IS NOT NULL BEGIN
        SELECT
          @CommentAlreadyExists = 1
        FROM
          sqlver.tblSchemaLog schl
        WHERE
          schl.SchemaLogId = @LastSchemaLogId AND
          @Comments IN (SELECT RTRIM(LTRIM([Value])) FROM sqlver.udftGetParsedValues(schl.Comments, '|'))

        IF ISNULL(@CommentAlreadyExists, 0) = 0 BEGIN
        UPDATE schl
          SET
            Comments = Comments + ' | ' + @Comments
          FROM
            sqlver.tblSchemaLog schl
          WHERE
            schl.SchemaLogId = @LastSchemaLogId
        END

      END      
    
      UPDATE sqlver.tblSchemaManifest
      SET
        ObjectType = @ObjectType       
      WHERE
        SchemaManifestId = @ManifestId AND
        ISNULL(ObjectType, '') <> @ObjectType   
                    
      SET @SkipLogging = 1
    END
    ELSE BEGIN
      SET @ChangeDetected = 1
    END



    DECLARE @IsGenerated bit
    SET @IsGenerated = 0

    SELECT
      @IsGenerated = 1
    FROM
      sys.schemas sch
      JOIN sys.objects obj ON
        sch.schema_id = obj.schema_id
    WHERE
      sch.name = @SchemaName AND
      obj.name = @ObjectName AND
      (
        obj.type_desc LIKE 'CLR%' OR
        (sch.name = 'opsstream' AND obj.name LIKE 'vwQXDLabel[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'vwQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'vwQXDix[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'vwQXDixnuq[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'spgetQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'spinsQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'spdelQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'spupdQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'tguitblQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'tgiivwQXD[_]%') OR
        (sch.name = 'opsstream' AND obj.name LIKE 'vwQXDLabelUsers')
      )


    IF @ManifestId IS NULL BEGIN
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spSysSchemaProcessObject: Inserting into sqlver.tblSchemaManifest'
        PRINT @Msg
      END
        
      INSERT INTO sqlver.tblSchemaManifest(  
        ObjectName,
        SchemaName,
        DatabaseName,
        ObjectType,
        OrigDefinition,
        DateAppeared,
        CreatedByLoginName,
        DateUpdated,
        OrigHash,
        CurrentHash,
        IsEncrypted,
        ExecuteAs,
        StillExists,
        SkipLogging,
        Comments,
        IsGenerated           
      )
      VALUES (
        @ObjectName,
        @SchemaName,
        @DatabaseName,  
        @ObjectType,
        @OrigSQLFromEvent, --@SQLForHash,
        @EventDate,
        @LoginName,
        @EventDate,
        @CalculatedHash,
        @CalculatedHash,
        @IsEncrypted,
        @ExecuteAs,
        1, --Note that we update all the StillExists flags below 
        0,
        @ThisManifestComment,
        @IsGenerated
      )
        
      SET @ManifestId = SCOPE_IDENTITY()  

      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spSysSchemaProcessObject: New ManifestID=' + ISNULL(CAST(@ManifestID AS varchar(100)), 'NULL')
        PRINT @Msg
      END
    END
    ELSE BEGIN
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spSysSchemaProcessObject: Updating sqlver.tblSchemaManifest (@ManifestID=' + ISNULL(CAST(@ManifestID AS varchar(100)), 'NULL') + ')'
        PRINT @Msg
      END
        
      UPDATE sqlver.tblSchemaManifest
      SET
        ObjectType = @ObjectType,
        DateUpdated = @EventDate,
        CurrentHash = @CalculatedHash,
        IsEncrypted = @IsEncrypted,
        ForceSchemaBinding = NULL,
        ExecuteAs = @ExecuteAs,
        Comments = @ThisManifestComment,
        IsGenerated = @IsGenerated
      WHERE
        SchemaManifestId = @ManifestId              
    END
    
    --Categorize object
    IF OBJECT_ID('sqlver.spusrSchemaObjectCategorize') IS NOT NULL BEGIN
      BEGIN TRY
        EXEC sqlver.spusrSchemaObjectCategorize @ObjectManifestId = @ManifestId 
      END TRY
      BEGIN CATCH
        PRINT 'sqlver.spSysSchemaProcessObject: Error when calling sqlver.spusrSchemaObjectCategorize. ' + ERROR_MESSAGE()
      END CATCH
    END                      

    IF  @OrigDefinitionIsNull = 1 AND @ObjectType = 'TABLE' BEGIN
      UPDATE sqlver.tblSchemaManifest
      SET
        OrigDefinition = @OrigSQLFromEvent
      WHERE
        SchemaManifestId = @ManifestId 
    END

    IF @SkipLogging = 0 BEGIN
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spSysSchemaProcessObject: Inserting into sqlver.tblSchemaLog'
        PRINT @Msg
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
        Hash,
        UserID,
        SQLFullTable
      )
      VALUES (
        COALESCE(@SPID, @@SPID),
        @EventType,
        @ObjectName,
        @SchemaName,
        @DatabaseName, 
        @ObjectType,
        @OrigSQLFromEvent, --@SQLStripped,
        @EventDate,
        @LoginName,
        @EventData,
        @Comments,
        @CalculatedHash,
        @UserID,
        CASE WHEN @ObjectType IN ('TABLE', 'INDEX') THEN @SQLForHash END                
      )
    
      SET @SchemaLogId = SCOPE_IDENTITY()
      SET @StoredHash = @CalculatedHash            
        
    END    

    IF @Debug = 1 BEGIN
      PRINT '@NeedExec=' + ISNULL(CAST(@NeedExec AS varchar(100)), 'NULL')
    END

    IF @EventType NOT LIKE 'DROP%' AND
       @ObjectType <> 'SYNONYM' AND
       @NeedExec = 1 AND
       ISNULL(@SkipExec, 0) = 0 BEGIN   
      DECLARE @SQLForExec nvarchar(MAX)
      SET @SQLForExec = @SQLStripped

      --Switch CREATE to ALTER to execute DDL
      SET @P = sqlver.udfFindInSQL('CREATE', @SQLForExec, 0)
      SET @P2 = sqlver.udfFindInSQL('ALTER', @SQLForExec, 0)
      IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
        SET @SQLForExec= LEFT(@SQLForExec, @P - 1) + 'ALTER' + RIGHT(@SQLForExec, LEN(@SQLForExec) - LEN('CREATE'))
      END

      
      IF @ForceSchemaBinding = 1 AND @HasSchemabinding = 0 BEGIN
        SET @WithClause = ISNULL(NULLIF(RTRIM(@WithClause), '') + ', ', 'WITH ') + 'SCHEMABINDING'
      END
      ELSE IF @ForceSchemaBinding = 0 AND @HasSchemabinding = 1 BEGIN
        DECLARE @NewWithClause nvarchar(MAX)

        SELECT
          @NewWithClause = ISNULL(@NewWithClause + ',', '') + sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(REPLACE(pv.[Value], 'WITH', '')))
        FROM
          sqlver.udftGetParsedValues(@WithClause, ',') pv
        WHERE
          pv.[Value] NOT LIKE '%SCHEMABINDING%'
        ORDER BY
          pv.[Index]
            
        IF @NewWithClause IS NOT NULL BEGIN
          SET @WithClause = 'WITH ' + @NewWithClause
        END
        ELSE BEGIN
          SET @WithClause = NULL
        END
      END


      IF COALESCE(NULLIF(RTRIM(@StoredExecuteAs), ''), NULLIF(RTRIM(@ExecuteAs), '')) IS NOT NULL BEGIN
        IF @ExecuteAs IS NULL BEGIN
          --If ExecuteAs is stored in sqlver.tblSchemaManifest but is not provided in WITH clause,
          --add EXECUTE AS.
          SET @WithClause = ISNULL(NULLIF(RTRIM(@WithClause), '') + ', ', 'WITH ') + 'EXECUTE AS ' + @StoredExecuteAs + @CRLF
        END
        ELSE BEGIN
          IF @ForceExecuteAs = 1 BEGIN
            --replace the WITH EXECUTE AS xxx with what is stored in sqlver.tblSchemaManifest
            SET @WithClause = REPLACE(@WithClause, 'EXECUTE AS ' + @ExecuteAs, 'EXECUTE AS ' + @StoredExecuteAs)
          END
        END
      END
       
      SET @WithClause = @CRLF + ISNULL(NULLIF(sqlver.udfRTRIMSuper(@WithClause), '') + @CRLF, '') 
        
      --Add in WITH token
      IF PATINDEX('%{{!' + 'WITH!}}%', @SQLForExec) = 0 BEGIN
        SET @SQLForExec = REPLACE(@SQLForExec, '{{!' + 'AS!}}','{{!' + 'WITH!}}{{!' + 'AS!}}')   
      END

      SET @SQLForExec = REPLACE(@SQLForExec, '{{!' + 'WITH!}}', ISNULL(NULLIF(RTRIM(@WithClause), ''), ''))
      SET @SQLForExec = REPLACE(@SQLForExec, '{{!' + 'AS!}}',
        ISNULL(NULLIF(RTRIM(@CopyrightMsg), ''), '') +
        @CRLF + 'AS')      

      SET @SQLForExec = REPLACE(@SQLForExec, '{{!' + 'ParseMarker!}}', @SVMarker)

      IF @EventType NOT IN ('create_synonym') AND
         @EventType NOT LIKE 'drop%' AND
         @ObjectName <> 'spsysSchemaProcessObject' BEGIN

        IF OBJECT_ID('tempdb..#SQLVerWork') IS NULL BEGIN
          CREATE TABLE #SQLVerWork (
            SchemaName sysname NULL,
            ObjectName sysname NULL
          )
        END
     

        IF OBJECT_ID('tempdb..#SQLVerWork') IS NOT NULL BEGIN
          SELECT
            @Nested = 1 
          FROM
            #SQLVerWork (NOLOCK) wrk
          WHERE
            ISNULL(wrk.SchemaName, '') = ISNULL(@SchemaName, '') AND
            wrk.ObjectName = @ObjectName
        END

        IF ISNULL(@Nested, 0) = 0 BEGIN
          IF OBJECT_ID('tempdb..#SQLVerWork') IS NOT NULL BEGIN
            INSERT INTO #SQLVerWork (
              SchemaName,
              ObjectName
              )
            SELECT
              @SchemaName,
              @ObjectName
 
          END

          BEGIN TRY
            IF @Debug = 1 BEGIN
              PRINT '>>>Executing: '
              PRINT ISNULL(@SQLForExec, 'NULL')
            END    
            EXEC (@SQLForExec)

            --process special Reprocess directives to allow regeneration of dependent objects 
            IF ISNULL(@SchemaName, '') <> 'sqlver' AND
              ISNULL(@ObjectName, '') <> 'spsysSchemaProcessObject' BEGIN
              SET @IncludeMarker = '--$$SQLVer:Reprocess:'

              SET @PInclude = PATINDEX('%' + @IncludeMarker + '%', @SQLStripped)

              WHILE @PInclude > 0 BEGIN
                BEGIN TRY

                  SET @ThisInclude = sqlver.udfSubstrToDelims(@SQLStripped, @PInclude, NCHAR(10)) + NCHAR(10)

                  SET @ThisIncludeExec = REPLACE(sqlver.udfRTRIMSuper(@ThisInclude), @IncludeMarker, REPLACE(@IncludeMarker, 'Reprocess', 'Include'))
                  EXEC sqlver.spsysReprocessObjects @TargetStr = @ThisIncludeExec

                  SET @SQLStripped = STUFF(@SQLStripped, @PInclude, LEN(@ThisInclude), '')

                  SET @PInclude = PATINDEX('%' + @IncludeMarker + '%', @SQLStripped)
                END TRY
                BEGIN CATCH
                  PRINT 'sqlver.spSysSchemaProcessObject: Error processing $$SQLVer:Reprocess (' + ISNULL(@ThisInclude, '') + ') ' + ERROR_MESSAGE()
                  SET @PInclude = 0
                END CATCH
              END
            END

          END TRY
          BEGIN CATCH
            PRINT '...Error while SQLVer was re-executing definition of ' + ISNULL(@SchemaName + '.', '') + ISNULL(@ObjectName, 'NULL') +
                  ': ' + ERROR_MESSAGE()
            PRINT '>>>>'
            EXEC sqlver.sputilPrintString @SQLForExec
            PRINT '<<<<'
            PRINT ''
            PRINT ''
          END CATCH
        END

        IF OBJECT_ID('tempdb..#SQLVerWork') IS NOT NULL BEGIN
          DELETE FROM #SQLVerWork
          WHERE
            ISNULL(SchemaName, '') = ISNULL(@SchemaName, '') AND
            ObjectName = @ObjectName          
        END       
      END
        
    END  

    IF @Visible = 1 AND ISNULL(@Nested, 0) = 0 BEGIN  
      IF @ExistingObject = 1 BEGIN

        IF @ChangeDetected = 0 BEGIN
          --no changes detected
          IF @ForceSchemaBinding IS NOT NULL BEGIN
            SET @Msg = 'Schemabinding forced ' + 
              CASE WHEN @ForceSchemaBinding = 1 THEN 'on' ELSE 'off' END +
              ' for {{Obj}} by SQLVer'          
          END
          ELSE IF @EventType = 'REPARSE' BEGIN
            SET @Msg = '{{Obj}} reprocessed by SQLVer'
          END
          ELSE BEGIN
            SET @Msg = 'No changes to {{Obj}} detected by SQLVer'
          END
        END
        ELSE BEGIN
          --changes detected
          SET @Msg = 'Changes to {{Obj}} detected by SQLVer {{Hash}}'
        END
      END
      ELSE BEGIN
        --new object
        SET @Msg = 'New object {{Obj}} detected by SQLVer {{Hash}}'
      END

      SET @Msg = REPLACE(@Msg, '{{Obj}}', @DatabaseName + '.' + ISNULL(NULLIF(RTRIM(@SchemaName), '') + '.', '') + @ObjectName)
      SET @Msg = REPLACE(@Msg, '{{Hash}}',
                        CASE
                          WHEN @EventType NOT LIKE 'DROP%'
                            THEN ISNULL('(Hash: ' +  master.dbo.fn_varbintohexstr(@CalculatedHash) + ')', '')
                          ELSE ''
                        END)

      PRINT ISNULL(@Msg, 'Problem with SQLVer logging.')

      --Print warnings
      IF @IsEncrypted = 1 BEGIN
        PRINT 'WARNING: SQL object is encrypted, so code NOT saved to the change log.'
      END   

      /*
      IF @SkipLogging = 1 BEGIN   
        IF @StoredSkipLogging = 1 BEGIN
          PRINT 'Changes not logged by SQLVer, due to SkipLogging flag in sqlver.tblSchemaManifest'
        END
        ELSE BEGIN
          SET @Msg = 'Changes not logged by SQLVer, due to @SkipLogging = 1'
        END
      END
      */

      --IF @ExistingObject = 1 AND ISNULL(@ExecuteAs, '') <> ISNULL(@StoredExecuteAs, '') BEGIN
      --  PRINT 'WARNING: Security context changed via EXECUTE AS.'
      --END      

    END

    --Update StillExists for ALL objects
    EXEC [sqlver].[spsysSchemaExistSync]
 
  END TRY
  BEGIN CATCH
    SET @Msg = 'Error logging DDL changes in database trigger sqlver.spSysSchemaProcessObject: ' + ERROR_MESSAGE()
    PRINT @Msg

    SET @Msg = '>>>' + @SQLForExec
    EXEC sqlver.sputilPrintString @Msg
  END CATCH
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spSysSchemaProcessObject: Finished'
    PRINT @Msg
  END    

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaProcessAll]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaProcessAll]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaProcessAll]
@SkipExec bit = 1
--$!SQLVer Jul  2 2024  8:08AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @SchemaName sysname
  DECLARE @ObjectName sysname
  DECLARE @ForceSchemaBinding bit

  --DDL triggers
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    NULL AS SchemaName,
    tg.name AS ObjectName,
    NULL AS ForceSchemaBinding
  FROM
    sys.triggers tg    
  WHERE
    tg.parent_class = 0    
  ORDER BY 
    tg.name

  OPEN curThis
  FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding

  WHILE @@FETCH_STATUS = 0 BEGIN
    BEGIN TRY
      --PRINT 'SQLVer is processing ' + ISNULL(@SchemaName, '') + '.' + @ObjectName 
      EXEC sqlver.spSysSchemaProcessObject @SchemaName = @SchemaName, @ObjectName = @ObjectName, @ForceSchemaBinding = @ForceSchemaBinding, @SkipExec = @SkipExec
    END TRY
    BEGIN CATCH
      PRINT '***SQLVer could not process trigger ' + ISNULL(@SchemaName, '') + '.' + @ObjectName + ': ' + ERROR_MESSAGE()
    END CATCH
    FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding
  END
  CLOSE curThis
  DEALLOCATE curThis

  --Other objects
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    sch.name AS SchemaName,
    obj.name AS ObjectName,
    NULL AS ForceSchemaBinding
  FROM
    sys.objects obj
    JOIN sys.schemas sch ON
      obj.schema_id = sch.schema_id
  WHERE
    obj.type IN (
      'FN',--	SQL_SCALAR_FUNCTION
      --'FS',--	CLR_SCALAR_FUNCTION
      --'FT',--	CLR_TABLE_VALUED_FUNCTION
      'IF',--	SQL_INLINE_TABLE_VALUED_FUNCTION
      'P', -- SQL_STORED_PROCEDURE
      --'PC',--	CLR_STORED_PROCEDURE
      'V', -- VIEW
      'TF',-- SQL_TABLE_VALUED_FUNCTION
      'TR',-- SQL_TRIGGER
      'U'  -- USER_TABLE
    )
  ORDER BY 
    sch.name,
    obj.type,
    obj.name

  OPEN curThis
  FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding

  WHILE @@FETCH_STATUS = 0 BEGIN
    BEGIN TRY
      --PRINT 'SQLVer is processing ' + ISNULL(@SchemaName, '') + '.' + @ObjectName 
      EXEC sqlver.spSysSchemaProcessObject @SchemaName = @SchemaName, @ObjectName = @ObjectName, @ForceSchemaBinding = @ForceSchemaBinding, @SkipExec = @SkipExec
    END TRY
    BEGIN CATCH
      PRINT '***SQLVer could not process object ' + ISNULL(@SchemaName, '') + '.' + @ObjectName + ': ' + ERROR_MESSAGE()
    END CATCH
    FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding
  END
  CLOSE curThis
  DEALLOCATE curThis

  --Synonyms
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    sch.name AS SchemaName,
    syn.name AS ObjectName,
    NULL AS ForceSchemaBinding
  FROM
    sys.synonyms syn  
    JOIN sys.schemas sch ON 
      syn.schema_id = sch.schema_id    
  ORDER BY 
    syn.name

  OPEN curThis
  FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding

  WHILE @@FETCH_STATUS = 0 BEGIN
    BEGIN TRY
      --PRINT 'SQLVer is processing ' + ISNULL(@SchemaName, '') + '.' + @ObjectName 
      EXEC sqlver.spSysSchemaProcessObject @SchemaName = @SchemaName, @ObjectName = @ObjectName, @ForceSchemaBinding = @ForceSchemaBinding, @SkipExec = @SkipExec
    END TRY
    BEGIN CATCH
      PRINT '***SQLVer could not process synonym ' + ISNULL(@SchemaName, '') + '.' + @ObjectName + ': ' + ERROR_MESSAGE()
    END CATCH
    FETCH curThis INTO @SchemaName, @ObjectName, @ForceSchemaBinding
  END
  CLOSE curThis
  DEALLOCATE curThis
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaObjectCompare]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaObjectCompare]
END
GO

CREATE PROCEDURE sqlver.spsysSchemaObjectCompare
@Hash1 varbinary(128),
@Hash2 varbinary(128)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @SchemaName sysname
  DECLARE @ObjectName sysname
  DECLARE @ObjectType sysname

  DECLARE @Buf1 nvarchar(MAX)
  DECLARE @Buf2 nvarchar(MAX)
  DECLARE @Date1 datetime
  DECLARE @Date2 datetime

  SELECT
    @SchemaName = COALESCE(schl1.SchemaName, schm1.SchemaName, schm1a.SchemaName, schl2.Schemaname, schm2.SchemaName, schm2a.SchemaName),
    @ObjectName = COALESCE(schl1.ObjectName, schm1.ObjectName, schm1a.ObjectName, schl2.Objectname, schm2.ObjectName, schm2a.ObjectName),

    @Buf1 = COALESCE(schl1.SQLCommand, schm1.OrigDefinition),
    @Buf2 = COALESCE(schl2.SQLCommand, schm2.OrigDefinition),

    @Date1 = COALESCE(schl1.EventDate, schm1.DateAppeared),
    @Date2 = COALESCE(schl2.EventDate, schm2.DateAppeared)
  FROM
    (SELECT 1 AS Placeholder) x 
    LEFT JOIN sqlver.tblSchemaLog schl1 ON schl1.[Hash] = @Hash1
    LEFT JOIN sqlver.tblSchemaManifest schm1 ON schm1.OrigHash = @Hash1 AND schl1.SchemaLogID IS NULL
    LEFT JOIN sqlver.tblSchemaManifest schm1a ON schm1a.OrigHash = @Hash1 AND schm1a.SchemaName = @SchemaName AND schm1a.ObjectName = @ObjectName

    LEFT JOIN sqlver.tblSchemaLog schl2 ON schl2.[Hash] = @Hash2
    LEFT JOIN sqlver.tblSchemaManifest schm2 ON schm2.CurrentHash = @Hash2 AND schl2.SchemaLogID IS NULL
    LEFT JOIN sqlver.tblSchemaManifest schm2a ON schm1a.OrigHash = @Hash2 AND schm2a.SchemaName = @SchemaName AND schm2a.ObjectName = @ObjectName

  SELECT
    @ObjectType = COALESCE(schm1.ObjectType, schm2.ObjectType)
  FROM
    (SELECT 1 AS Placeholder) x 
    LEFT JOIN sqlver.tblSchemaManifest schm1 ON schm1.SchemaName = @SchemaName AND schm1.ObjectName = @ObjectName
    LEFT JOIN sqlver.tblSchemaManifest schm2 ON schm2.SchemaName = @SchemaName AND schm2.ObjectName = @ObjectName
  SELECT
    @SchemaName AS SchemaName,
    @ObjectName AS ObjectName,
    @ObjectType AS ObjectType,
    @Date1 AS Date1,
    @Date2 AS Date2

  IF @ObjectType = 'TABLE' BEGIN
    DECLARE @Src1 nvarchar(MAX)
    DECLARE @Src2 nvarchar(MAX)

    SET @Src1 = sqlver.udfScriptTable(@SchemaName, @ObjectName)
    SET @Src2 = sqlver.udfScriptTable(@SchemaName, @ObjectName)

    PRINT '***Table Defintion 1: '
    EXEC sqlver.sputilPrintString @Src1

    PRINT '***Table Defintion 2: '
    EXEC sqlver.sputilPrintString @Src2

  END

  IF OBJECT_ID('sqlver.udftGetDiffs_CLR') IS NOT NULL BEGIN
    SELECT *
    FROM
      sqlver.udftGetDiffs_CLR(@Buf1, @Buf2)
  END

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaObjectDefinition]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaObjectDefinition]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaObjectDefinition]
@SchemaName sysname,
@ObjectName sysname,
@CurrentHash varbinary(128) = NULL,
@ChunkID int = NULL,
@MaxChunkID int = NULL OUTPUT,
@DefinitionChunk varchar(8000) = NULL OUTPUT,
@Definition varchar(MAX) = NULL OUTPUT,
@Debug bit = 0

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  --Gets object definition in chunks
  
  DECLARE @Buf varchar(MAX)
  DECLARE @BufFrag varchar(MAX)
  
  DECLARE @ObjectType sysname
  
      
  IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Starting for ' + ISNULL(@SchemaName, '') + '.' + ISNULL(@ObjectName, '')
   
  IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): @CurrentHash =' + ISNULL(master.dbo.fn_varbintohexstr(@CurrentHash), 'NULL')


  SELECT @ObjectType = om.ObjectType
  FROM
    sqlver.tblSchemaManifest om
  WHERE
    om.SchemaName = @SchemaName AND
    om.ObjectName = @ObjectName
    
  DECLARE @CurVer int --sqlver.tblSchemaLog.SchemaLogID for the current version
  DECLARE @StartVer int --starting ID for iterating to build ALTER concat (WHERE SchemaLogID > @StartVer)
  SET @Buf = ''

  IF @ObjectType = 'TYPE' BEGIN
    IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Object is TYPE'
    IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): @CurrentHash=' + ISNULL(master.dbo.fn_varbintohexstr(@CurrentHash), 'NULL')
    
    SET @Buf = sqlver.udfScriptType(@SchemaName, @ObjectName)
  END

  ELSE IF @ObjectType = 'TABLE' BEGIN
    IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Object is TABLE'
    IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): @CurrentHash=' + ISNULL(master.dbo.fn_varbintohexstr(@CurrentHash), 'NULL')
         
    --Try to find version that caller currently has (in tblSysSchemaLog)
    SELECT 
      @CurVer = x.SchemaLogID
    FROM
      (
      SELECT
        schl.SchemaLogID,
        ROW_NUMBER() OVER (PARTITION BY schl.SchemaName, schl.ObjectName, schl.Hash ORDER BY schl.SchemaLogID DESC) AS Seq
      FROM 
        sqlver.tblSchemaLog schl
      WHERE
        schl.SchemaName = @SchemaName AND
        schl.ObjectName = @ObjectName AND
        schl.Hash = @CurrentHash
      ) x
    WHERE
      x.Seq = 1
     
    
    IF @CurVer IS NULL BEGIN
       --Caller has no known version.  Provide the current definition.
      IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): No known version'
     
      SET @Buf = sqlver.udfScriptTable(@SchemaName, @ObjectName)

    END
    ELSE BEGIN
      --Caller has a known version.  We'll concatenate the deltas together, starting
      --with the next version.
      IF @Debug = 1 PRINT '@CurVer=' + CAST(@CurVer AS varchar(100))
      SET @Buf = ''

      DECLARE @ThisDelta varchar(MAX)

      DECLARE curDeltas CURSOR LOCAL STATIC FOR
      SELECT schl.SQLCommand
      FROM
        sqlver.tblSchemaLog schl
      WHERE  
        schl.SchemaName = @SchemaName AND
        schl.ObjectName = @ObjectName AND  
        schl.SchemaLogID > @CurVer AND
        schl.EventType <> 'REPARSE'

        
      OPEN curDeltas
      FETCH curDeltas INTO @ThisDelta
      
      WHILE @@FETCH_STATUS = 0 BEGIN
        IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Found @ThisDelta=' + ISNULL(CAST(@ThisDelta AS varchar(100)), 'NULL')    
    
        SET @Buf = ISNULL(@Buf , '') + ISNULL(@ThisDelta + CHAR(13) + CHAR(10), '') 
        FETCH curDeltas INTO @ThisDelta      
      END
      
      CLOSE curDeltas
      DEALLOCATE curDeltas

    END        
        
  END
  
  ELSE BEGIN
    IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Not a table.  Simple object DDL.'

    SELECT 
      @Buf = COALESCE(schl_max.SQLCommand, om.OrigDefinition)
    FROM 
      sqlver.tblSchemaManifest om
      LEFT JOIN (
        SELECT
          schl.SchemaName,
          schl.ObjectName,
          schl.SQLCommand,
          schl.SchemaLogID,
          ROW_NUMBER() OVER (PARTITION BY schl.SchemaName, schl.ObjectName ORDER BY schl.SchemaLogID DESC) AS Seq
        FROM
          sqlver.tblSchemaLog schl
        ) schl_max ON
        om.SchemaName = schl_max.SchemaName AND
        om.ObjectName = schl_max.ObjectName AND
        schl_max.Seq = 1
    WHERE
      om.SchemaName = @SchemaName AND
      om.ObjectName = @ObjectName
  
  END
  
  IF @Debug = 1 BEGIN
    PRINT 'Final @Buf='
    EXEC sqlver.sputilPrintString @Buf
  END

  IF @Debug = 1 PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Starting to chunk.  @Buf=' + ISNULL(CAST(@Buf AS varchar(100)), 'NULL')
  
 
  DECLARE @tvOut TABLE (
    BufID int IDENTITY,
    Buf varchar(8000)
  )
 
  DECLARE @i int
  
  SET @i = 1
  WHILE @i <= LEN(@Buf + 'x') - 1 BEGIN
    IF @Debug = 1 BEGIN
      PRINT 'spsysSchemaObjectDefinition (' + DB_NAME() + '): Chunk #' + CAST(@i AS varchar(100)) + ': ' + SUBSTRING(@Buf, @i, 8000)
    END
    
    INSERT INTO @tvOut (Buf)
    VALUES (SUBSTRING(@Buf, @i, 8000))
    
    SET @i = @i + 8000
  END 
  
  
  SELECT @MaxChunkID = MAX(BufID) FROM @tvOut
  
  SET @DefinitionChunk = NULL
  SET @Definition = @Buf  
  
  IF @Debug = 1 BEGIN
    SELECT 'tvOut Chunk', * FROM @tvOut
  END
  

  IF @Debug = 1 PRINT '@ChunkID = ' + ISNULL(CAST(@ChunkID AS varchar(100)), 'NULL')

  IF @ChunkID IS NOT NULL BEGIN
    SELECT @DefinitionChunk = tv.Buf
    FROM @tvOut tv
    WHERE 
      tv.BufID = @ChunkID
  END

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaObjectVersionsXML]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaObjectVersionsXML]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaObjectVersionsXML]
@BufID int = NULL,
@MaxBufID int = NULL OUTPUT,
@Result varchar(8000) = NULL OUTPUT,
@ObjectCategories varchar(8000) = NULL

WITH EXECUTE AS OWNER
--$!SQLVer Sep 27 2022  2:12PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON  
/*
This is a little tricky due to limitations imposed on us by accessing data from a remote server:

  1) Though we can execute a stored procedure on the remote server that returns a resultset,
     we can't insert those results into a table (including a table variable) on the local
     server unless DTC is enabled.

  2) We cannot execute or select from a user-defined function on the remote server.

  3) We cannot select XML or varchar(MAX) data types

So how can we get a resultset from the remote server without requiring configuration of DTC?

We use XML, but on the remote server we cast it to varchar, and break it into 8000 byte chunks.
The remote stored procedure will tell us how many chunks there are, and let us return individual
chunks by index.

We can then retrieve each chunk, and assemble in a variable locally.  Then we can cast back to
XML, and then select the data.

Not the most efficient way of doing things...but it avoids the need for special configuration
of DTC on customer machines.
*/

  DECLARE @Debug bit
  SET @Debug = 0
  
  IF @Debug = 1 BEGIN
    PRINT 'spsysSchemaObjectVersionsXML: Starting (' + DB_NAME() + ')'
    IF DB_NAME() = 'osMaster' PRINT 'Warning: Debug messages may be displayed in reverse order when coming from a linked server.'
  END;

  EXEC sqlver.spsysSchemaExistSync

  DECLARE @XML xml

  SET @XML = (
    SELECT
      cver.SchemaName,
      cver.ObjectName,
      cver.ObjectType,
      cver.ObjectCategory,
      cver.Hash,
      cver.VersionDate,
      cver.EventType,
      cver.StillExists

    FROM (
      SELECT
          om.SchemaName,
          om.ObjectName,
          om.ObjectType,
          om.ObjectCategory,
          CAST(master.dbo.fn_varbintohexstr(om.CurrentHash) AS varchar(100)) AS [Hash],
          COALESCE(schl.EventDate, om.DateAppeared) AS VersionDate,
          ROW_NUMBER() OVER (PARTITION BY om.SchemaName, om.ObjectName ORDER BY schl.SchemaLogID DESC) AS Seq,
          schl.EventType,
          om.StillExists
        FROM 
          sqlver.tblSchemaManifest om
          LEFT JOIN sqlver.tblSchemaLog schl ON
            om.SchemaName = schl.SchemaName AND
            om.ObjectName = schl.ObjectName AND
            (om.CurrentHash IS NULL OR om.CurrentHash = schl.Hash)
            --experimental:  CurrentHash may be null in the case of a DROP
            --om.CurrentHash = schl.Hash
          LEFT JOIN sqlver.udftGetParsedValues(@ObjectCategories, ',') pv ON
            om.ObjectCategory = pv.[Value]      

        WHERE
          (NULLIF(RTRIM(@ObjectCategories), '') IS NULL OR pv.[Value] IS NOT NULL) AND

          (
           om.SchemaName <> 'opsstream' OR
           (om.ObjectName NOT LIKE 'typQXD[_]%' OR om.ObjectName ='typQXD_$$')
          ) AND

          (om.StillExists = 1 OR schl.EventType LIKE 'DROP%')
      ) cver
    WHERE cver.Seq = 1   
      ORDER BY
         CASE cver.ObjectName
           WHEN 'dtgSQLVerLogSchemaChanges' THEN 1
           ELSE 9
         END,
         CASE cver.ObjectType
           WHEN 'USER_TABLE' THEN 0
           WHEN 'VIEW' THEN 1 
           WHEN 'SQL_TRIGGER' THEN 2 
           WHEN 'SQL_SCALAR_FUNCTION' THEN 3 
           WHEN 'SQL_TABLE_VALUED_FUNCTION' THEN 4     
           WHEN 'SQL_STORED_PROCEDURE' THEN 5
           ELSE 9
         END,                
         cver.schemaName,
         cver.ObjectName  
  FOR XML PATH ('OSDBObject'), TYPE, BINARY BASE64
  ) 
  

  IF @Debug = 1 BEGIN
    PRINT 'spsysSchemaObjectVersionsXML: HaveXML'
    SELECT @XML AS [XML]
  END
 
  
  DECLARE @Buf varchar(MAX)
  SET @Buf = CAST(@XML AS varchar(MAX))
 
  IF @Debug = 1  PRINT 'spsysSchemaObjectVersionsXML: Casted XML to @Buf.'

 
  DECLARE @tvOut TABLE (
    BufID int IDENTITY,
    Buf varchar(8000)
  )
  
  DECLARE @i int
  
  IF @Debug = 1 PRINT 'spsysSchemaObjectVersionsXML: LEN(@Buf) = ' + CAST(LEN(@Buf) AS varchar(100))
  
  SET @i = 0
  WHILE @i <= LEN(@Buf) BEGIN
    IF @Debug = 1 PRINT CAST(@i AS varchar(100)) + ':  ' + @Buf
    INSERT INTO @tvOut (Buf)
    VALUES (SUBSTRING(@Buf, @i, 8000))
   
    SET @i = @i + 8000
  END 
  
  
  SELECT @MaxBufID = MAX(BufID) FROM @tvOut
  
  IF @BufID IS NULL BEGIN
    SELECT * FROM @tvOut
  END
  ELSE BEGIN
    SELECT @Result = Buf
    FROM @tvOut
    WHERE 
      BufID = @BufID
  END
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaMaster_ObjectDefinition]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaMaster_ObjectDefinition]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaMaster_ObjectDefinition]
@SchemaName sysname,
@ObjectName sysname,
@CurrentHash varbinary(128),
@Definition varchar(MAX) OUTPUT

WITH EXECUTE AS CALLER
--$!SQLVer Aug  3 2021  9:26AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --We need to get schema version information from the OpsStream Master instance.
  SET NOCOUNT ON
  
  DECLARE @Debug bit
  SET @Debug = 0

  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): Starting'

  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): @SchemaName = ' + ISNULL(@SchemaName, 'NULL')
  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): @ObjectName = ' + ISNULL(@ObjectName, 'NULL')
  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): @CurrentHash = ' + ISNULL(master.dbo.fn_varbintohexstr(@CurrentHash), 'NULL')

  DECLARE @MaxChunkID int
  DECLARE @Chunk varchar(8000)
  DECLARE @i int


  DECLARE @ObjectType sysname
  
  SELECT
    @ObjectType = om.ObjectType
  FROM
    sqlver.tblSchemaManifest om   
  WHERE
    om.SchemaName = @SchemaName AND
    om.ObjectName = @ObjectName

  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): @ObjectType = ' + ISNULL(@ObjectType, 'NULL')

  --IF ISNULL(@ObjectType, '') NOT IN ('TABLE', 'INDEX') BEGIN
  --  SET @CurrentHash = NULL
  --END

  SET @i = 1

  IF @Debug = 1 PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): Calling EXEC sqlver.spMasterSchemaObjectDefinition'

  --Note:  sqlver.spMasterSchemaObjectDefinition is a synonym you must create to point to the remote repository database sqlver.spsysSchemaObjectDefinition
  EXEC sqlver.spMasterSchemaObjectDefinition
    @SchemaName = @SchemaName,
    @ObjectName = @ObjectName,
    @CurrentHash = @CurrentHash,    
    @ChunkID = @i,
    @MaxChunkID = @MaxChunkID OUTPUT,
    @DefinitionChunk = @Chunk OUTPUT


  SET @Definition = CAST(@Chunk AS varchar(MAX))

  WHILE @i < @MaxChunkID BEGIN
    
    SET @i = @i + 1
    
    --Note:  sqlver.spMasterSchemaObjectDefinition is a synonym you must create to point to the remote master repository database
    EXEC sqlver.spMasterSchemaObjectDefinition
      @SchemaName = @SchemaName,
      @ObjectName = @ObjectName,
      @CurrentHash = @CurrentHash,        
      @ChunkID = @i,
      @MaxChunkID = @MaxChunkID OUTPUT,      
      @DefinitionChunk = @Chunk OUTPUT

    SET @Definition = @Definition + CAST(@Chunk AS varchar(MAX))    
  END
  
  IF @Debug = 1 BEGIN
    PRINT 'spsysSchemaMaster_ObjectDefinition (' + DB_NAME() + '): Final'
    PRINT '>>>>>>>>>>>>>>>>>>'
    EXEC sqlver.sputilPrintString @Definition
    PRINT '<<<<<<<<<<<<<<<<<<'
  END
    

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaMaster_VersionsXML]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaMaster_VersionsXML]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaMaster_VersionsXML]
@XML xml OUTPUT,
@ObjectCategories varchar(8000) = NULL

WITH EXECUTE AS CALLER
--$!SQLVer Aug  3 2021  9:26AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  --We need to get schema version information from the OpsStream Master instance.
  
  DECLARE @Debug bit
  SET @Debug = 0
  
  IF @Debug = 1 PRINT 'spsysSchemaMaster_VersionsXML: Starting'
  
  DECLARE @Buf varchar(MAX)

  DECLARE @MaxBufID int
  DECLARE @BufFrag varchar(8000)
  DECLARE @i int
  
  
  SET @Buf = ''

  SET @i = 1
  --Note: sqlver.spMasterSchemaObjectVersionsXML is a synonym that you must create to point to the remote master database sqlver.spsysSchemaObjectVersionsXML
  EXEC sqlver.spMasterSchemaObjectVersionsXML
    @BufID = @i, @MaxBufID = @MaxBufID OUTPUT, @Result = @BufFrag OUTPUT, @ObjectCategories = @ObjectCategories
  SET @Buf = @BufFrag

  WHILE @i < @MaxBufID BEGIN
    SET @i = @i + 1
    EXEC sqlver.spMasterSchemaObjectVersionsXML
      @BufID = @i, @MaxBufID = @MaxBufID OUTPUT, @Result = @BufFrag OUTPUT, @ObjectCategories = @ObjectCategories

    SET @Buf = @Buf + @BufFrag
  END

  SET @XML = CAST(@Buf AS xml)
  
  IF @Debug = 1 SELECT 'spsysSchemaMaster_VersionsXML', @XML AS XML
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaVersionUpdateFromMaster]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaVersionUpdateFromMaster]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaVersionUpdateFromMaster]
@PerformUpdate bit = 0,
@ObjectCategories varchar(8000) = '0,1000,1003,1004',
@ProcessDrops bit = 0,
@Debug bit = 0

WITH EXECUTE AS CALLER
--$!SQLVer Sep 27 2022  2:33PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  IF @Debug = 1 PRINT 'spsysSchemaVersionUpdateFromMaster: Starting'
  
  IF DB_NAME() = 'osMaster' BEGIN
    RAISERROR('Error in spsysSchemaVersionUpdateFromMaster: Cannot synchronize osMaster with itself.', 16, 1)
    RETURN(1002)
  END

  --Do some quick housekeeping on tblSchemaManifest
  EXEC sqlver.spsysSchemaExistSync

   
  DECLARE @tvMasterVersions TABLE (
    SchemaName sysname NULL,
    ObjectName sysname,
    ObjectType sysname NULL,
    ObjectCategory sysname,
    VersionDate datetime,
    Version int,
    [Hash] varchar(100),
    EventType sysname,
    StillExists bit)
    
  --Retrieve current version information from Master (for all objects)  
  IF @Debug = 1 PRINT 'spsysSchemaVersionUpdateFromMaster: Calling spsysSchemaMaster_VersionsXML'  
  DECLARE @XML xml
  EXEC sqlver.spsysSchemaMaster_VersionsXML @ObjectCategories = @ObjectCategories, @XML = @XML OUTPUT
  
  IF @Debug = 1 SELECT 'spsysSchemaVersionUpdateFromMaster', @XML
  
  --Digest XML version data from master into a table 
  INSERT INTO @tvMasterVersions (
    SchemaName,
    ObjectName,
    ObjectType,
    ObjectCategory,
    VersionDate,
    Version,
    [Hash],
    EventType,
    StillExists)

	SELECT
	  nref.value('SchemaName[1]', 'sysname') AS SchemaName,
	  nref.value('ObjectName[1]', 'sysname') AS ObjectName,
	  nref.value('ObjectType[1]', 'sysname') AS ObjectType,
	  nref.value('ObjectCategory[1]', 'int') AS ObjectType,
	  nref.value('VersionDate[1]', 'datetime') AS VersionDate,
	  nref.value('Version[1]', 'int') AS Version,
	  nref.value('Hash[1]', 'varchar(100)') AS [Hash],
    ISNULL(nref.value('EventType[1]', 'sysname'), 'UNKONWN') AS EventType,
    nref.value('StillExists[1]', 'bit') AS StillExists
	FROM @XML.nodes('//OSDBObject') AS R(nref)
	ORDER BY
	  CASE nref.value('ObjectName[1]', 'sysname')
      WHEN 'tblSysSchemaManifest' THEN 1    
      WHEN 'tblSysSchemaLog' THEN 2
	    WHEN 'spsysSchemaVersionUpdateFromMaster' THEN 6	    
	    ELSE 10
	  END
	    
  	

  --walk through each object
  IF @Debug = 1 PRINT 'spsysSchemaVersionUpdateFromMaster: Walking through objects'  
    
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    mv.SchemaName,
    mv.ObjectName,
    mv.ObjectType,
    CASE WHEN om.StillExists = 1 THEN master.dbo.fn_varbintohexstr(om.CurrentHash) ELSE NULL END AS LocalHash,
    mv.[Hash] AS MasterHash,
    COALESCE(schl.EventDate, om.DateAppeared) AS VersionDate_Local,
    mv.VersionDate AS VersionDate_Master,
    CASE WHEN om.StillExists = 1 THEN om.CurrentHash ELSE NULL END AS LocalHashBin,
    mv.EventType,
    mv.StillExists AS MasterStillExists,
    ISNULL(om.IsGenerated, 0)

  FROM
    @tvMasterVersions mv
    LEFT JOIN sqlver.tblSchemaManifest om ON
      mv.SchemaName = om.SchemaName AND
      mv.ObjectName = om.ObjectName

    LEFT JOIN (
      SELECT
        schl.SchemaName,
        schl.ObjectName,
        schl.Hash,
        MAX(schl.EventDate) AS EventDate
      FROM
        sqlver.tblSchemaLog schl
      GROUP BY
        schl.SchemaName,
        schl.ObjectName,
        schl.Hash) schl ON 
        om.SchemaName = schl.SchemaName AND
      om.ObjectName = schl.ObjectName AND
      om.CurrentHash = schl.Hash
  WHERE
    (mv.StillExists = 1 OR @ProcessDrops = 1) AND
    (ISNULL(om.ExcludeFromSync, 0) = 0) AND 

    (
     (om.CurrentHash IS NULL AND mv.StillExists = 1) OR --Master object exists but is not present locally
     (mv.Hash <> ISNULL(master.dbo.fn_varbintohexstr(om.CurrentHash), '')) --Master object version is different from what we have locally
    )
  ORDER BY
    CASE mv.ObjectName
      WHEN 'tblSchemaManifest' THEN 1    
      WHEN 'tblSchemaLog' THEN 2
      WHEN 'dtgSQLVerLogSchemaChanges' THEN 4    
	    WHEN 'spsysSchemaVersionUpdateFromMaster' THEN 6	      
      ELSE 10
    END,  
    --tables first
    CASE mv.ObjectType
       WHEN 'TABLE' THEN 0
       WHEN 'VIEW' THEN 1 
       WHEN 'SQL_TRIGGER' THEN 2 
       WHEN 'SQL_SCALAR_FUNCTION' THEN 3 
       WHEN 'SQL_TABLE_VALUED_FUNCTION' THEN 4     
       WHEN 'SQL_INLINE_TABLE_VALUED_FUNCTION' THEN 5
       WHEN 'SQL_STORED_PROCEDURE' THEN 6
       ELSE 7
    END,
    --new objects
    CASE WHEN om.SchemaManifestID IS NULL THEN 0 ELSE 1 END,
    om.SchemaName,
    om.ObjectName
    

  DECLARE @SQL nvarchar(MAX)
  DECLARE @DropSQL nvarchar(MAX)  
  DECLARE @ThisSchemaName sysname
  DECLARE @ThisObjectName sysname
  DECLARE @ThisObjectType sysname
  DECLARE @LocalHash varchar(100)
  DECLARE @MasterHash varchar(100)
  DECLARE @LocalDate datetime
  DECLARE @MasterDate datetime
  DECLARE @LocalHashBin varbinary(128)
  DECLARE @UpdateCount int
  DECLARE @UpdateAppliedCount int
  DECLARE @EventType sysname
  DECLARE @MasterStillExists bit
  DECLARE @IsGenerated bit

  DECLARE @tvSQLLines TABLE (
    Id int,
    SQLLine nvarchar(MAX),
    IsFK bit
  )

  DECLARE @SQLFK nvarchar(MAX)
  DECLARE @ThisSQLFK nvarchar(MAX)


  OPEN curThis
  FETCH curThis INTO 
    @ThisSchemaName,
    @ThisObjectName,
    @ThisObjectType,
    @LocalHash,
    @MasterHash,
    @LocalDate,
    @MasterDate,
    @LocalHashBin,
    @EventType,
    @MasterStillExists,
    @IsGenerated

  SET @UpdateCount = 0
  SET @UpdateAppliedCount = 0

  WHILE @@FETCH_STATUS = 0 BEGIN 
    SET @UpdateCount = @UpdateCount + 1

    IF @Debug = 1 BEGIN
      PRINT 'spsysSchemaVersionUpdateFromMaster: ******' + @ThisSchemaName + '.' + @ThisObjectName 
      PRINT 'spsysSchemaVersionUpdateFromMaster: @LocalHash = ' + ISNULL(master.dbo.fn_varbintohexstr(@LocalHashBin) , 'NULL')
      PRINT 'spsysSchemaVersionUpdateFromMaster: Calling spsysSchemaMaster_ObjectDefinition'
    END

    SET @SQL = NULL
    DELETE FROM @tvSQLLines

    EXEC sqlver.spsysSchemaMaster_ObjectDefinition
      @SchemaName = @ThisSchemaName,
      @ObjectName = @ThisObjectName,
      @CurrentHash = @LocalHashBin,
      @Definition = @SQL OUTPUT


    IF @ThisObjectType = 'TABLE' BEGIN

      IF @LocalHash IS NULL BEGIN     

        /*
        The problem with tables is that they can have foreign key constraints
        (dependencies upon other tables).  This means that we will not be able
        to create these constraints until the referenced table exists.

        So we will use string manipulation to comment out the foreign key
        constraints, and will save a copy of these constraints to execute later
        after all other table changes have been applied.

        To do this, we will split @SQL into individual lines.
        */

        IF @SQL IS NOT NULL BEGIN
          IF @Debug = 1 BEGIN
            PRINT '***spsysSchemaVersionUpdateFromMaster: Orig SQL to create table ' + @ThisSchemaName + '.' + @ThisObjectName + '>>1>'
            EXEC sqlver.sputilPrintString @SQL
            PRINT '<1<<'
          END         
        END

        INSERT INTO @tvSQLLines (
          Id,
          SQLLine,
          IsFK
        )

        SELECT
          pv.[Index],
          NULLIF(sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper([Value])), ''),
          0
        FROM
          sqlver.udftGetParsedValues(@SQL, CHAR(13)) pv      
          
        SET @SQL = NULL              

        UPDATE lns
        SET
          IsFK = 1
        FROM
          @tvSQLLines lns
        WHERE
          lns.SQLLine LIKE 'CONSTRAINT%' AND
          PATINDEX('%FOREIGN KEY%', lns.SQLLine) > 0

        UPDATE lns
        SET
          SQLLine = LEFT(lns.SQLLine, LEN(lns.SQLLine) - 1)
        FROM
          @tvSQLLines lns
        WHERE
          lns.IsFK = 1 AND
          RIGHT(lns.SQLLine, 1) = ','
        

        SELECT
          @SQL = ISNULL(@SQL, '') + ISNULL(lns.SQLLine + CHAR(13) + CHAR(10), '')
        FROM
          @tvSQLLines lns
        WHERE
          lns.IsFK = 0
        ORDER BY
          lns.Id


        SELECT
          @ThisSQLFK = ISNULL(@ThisSQLFK, '') +
            'BEGIN TRY' + CHAR(13) + CHAR(10) +              
            'ALTER TABLE [' + @ThisSchemaName + '].[' + @ThisObjectName + '] ADD ' + 
            ISNULL(lns.SQLLine + CHAR(13) + CHAR(10), '') + 
            'END TRY' + CHAR(13) + CHAR(10) + 
            'BEGIN CATCH' + CHAR(13) + CHAR(10) + 
            'PRINT ''$$$Error creating foreign keys for ' + ISNULL(@ThisSchemaName + '.', '') + ISNULL(@ThisObjectName, 'NULL') + ': '' + ERROR_MESSAGE()' +  CHAR(13) + CHAR(10) +
            'END CATCH' + CHAR(13) + CHAR(10) 
        FROM
          @tvSQLLines lns
        WHERE
          lns.IsFK = 1
        ORDER BY
          lns.Id


        IF @SQL IS NOT NULL BEGIN
          IF @Debug = 1 BEGIN
            PRINT '***spsysSchemaVersionUpdateFromMaster: Will create table ' + @ThisSchemaName + '.' + @ThisObjectName + '>>2>'
            EXEC sqlver.sputilPrintString @SQL
            PRINT '<2<<'
          END         
        END
            
      END
    END
     
    --Print object information
    IF @IsGenerated = 0 AND (@EventType NOT LIKE 'DROP%' OR @LocalHash IS NOT NULL) BEGIN
      PRINT    
        '****' + ISNULL(@ThisSchemaName, '') + '.' + ISNULL(@ThisObjectName, 'NULL') + ISNULL(' (' + @ThisObjectType + ')', '') + 
        ' LocalDate: ' + ISNULL(CAST(@LocalDate AS varchar(100)), 'N/A') + '  MasterDate: ' + ISNULL(CAST(@MasterDate AS varchar(100)), 'N/A') + ' ' +
        ' EXEC sqlver.spsysSchemaObjectCompareMaster ' + ISNULL(CAST(@LocalHash AS varchar(100)), 'NULL') + ', ' + ISNULL(CAST(@MasterHash AS varchar(100)), 'NULL')
    END
 
    IF @Debug = 1 PRINT 'spsysSchemaVersionUpdateFromMaster: @ThisObjectType = ' + ISNULL(@ThisObjectType, 'NULL')

    DECLARE @P int
    DECLARE @P2 INT

    IF @Debug = 1 BEGIN
      PRINT 'spsysSchemaVersionUpdateFromMaster: @EventType = ' + ISNULL(@EventType, 'NULL')
    END

    IF @IsGenerated = 1 OR (@EventType LIKE 'DROP%' AND @LocalHash IS NULL) BEGIN
      --Ignore:  no need to drop object, because it doesn't exist
      SET @SQL = NULL
      SET @UpdateCount = @UpdateCount - 1
      IF @Debug = 1 PRINT @ThisSchemaName + '.' +  @ThisObjectName + ' (' + @EventType + ') is not needed (StillExists = 0)'
         
    END
    ELSE IF (@ThisObjectType IN ('SYNONYM', 'TYPE')) BEGIN
      --Switch ALTER to CREATE
      --Find the first ALTER that is in uncommented SQL code
      SET @P = sqlver.udfFindInSQL('ALTER', @SQL, 0)
      SET @P2 = sqlver.udfFindInSQL('CREATE', @SQL, 0)

      IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
        SET @SQL = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(LEFT(@SQL, @P - 1) + 'CREATE' + RIGHT(@SQL, LEN(@SQL) - LEN('ALTER'))))
      END

      --can't ALTER synonyms or types, so need to drop and re-create
      IF @LocalHash IS NOT NULL BEGIN
        SET @SQL = CONCAT('DROP ', @ThisObjectType, ' ', @ThisSchemaName, '.', @ThisObjectName) + CHAR(13) + CHAR(10) + @SQL
      END
    END
    ELSE IF (@ThisObjectType NOT IN ('TABLE', 'INDEX')) BEGIN
      IF @LocalHash IS NULL BEGIN
        --Object does not yet exist.  Switch ALTER to CREATE
        --Find the first ALTER that is in uncommented SQL code
        SET @P = sqlver.udfFindInSQL('ALTER', @SQL, 0)
        SET @P2 = sqlver.udfFindInSQL('CREATE', @SQL, 0)

        IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
          SET @SQL = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(LEFT(@SQL, @P - 1) + 'CREATE' + RIGHT(@SQL, LEN(@SQL) - LEN('ALTER'))))
        END
      END
      ELSE BEGIN
        --Object exists.  Switch CREATE to ALTER
        --Find the first ALTER that is in uncommented SQL code
        SET @P = sqlver.udfFindInSQL('CREATE', @SQL, 0)
        SET @P2 = sqlver.udfFindInSQL('ALTER', @SQL, 0)

        IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
          SET @SQL = sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(LEFT(@SQL, @P - 1) + 'ALTER' + RIGHT(@SQL, LEN(@SQL) - LEN('CREATE'))))
        END
      END
    END


    IF @ThisObjectName NOT LIKE 'bak%' AND
      @ThisObjectName NOT LIKE 'tmp%' BEGIN    
        
      
      IF @PerformUpdate = 1 AND @SQL IS NOT NULL BEGIN      
        
        BEGIN TRY
          IF @Debug = 1 BEGIN
            PRINT 'spsysSchemaVersionUpdateFromMaster: About to execute @SQL' + '>>3>'
            EXEC sqlver.sputilPrintString @SQL
            PRINT '<3<<'
          END
                            
          EXEC(@SQL)
          SET @UpdateAppliedCount = @UpdateAppliedCount + 1

          IF @Debug = 1 PRINT 'spsysSchemaVersionUpdateFromMaster: Done with execute @SQL'   
        END TRY
        BEGIN CATCH
          PRINT 'spsysSchemaVersionUpdateFromMaster: Error when executing @SQL: ' + ERROR_MESSAGE()
          PRINT '>>>'
          EXEC sqlver.sputilPrintString @SQL
        END CATCH  
            
      END
    END

    IF @PerformUpdate = 1 AND @ThisSQLFK IS NOT NULL BEGIN

      SET @SQLFK = ISNULL(@SQLFK, '') + ISNULL(@ThisSQLFK + CHAR(13) + CHAR(10), '')
      SET @ThisSQLFK = NULL
    END
    

    FETCH curThis INTO 
      @ThisSchemaName,
      @ThisObjectName,
      @ThisObjectType,
      @LocalHash,
      @MasterHash,
      @LocalDate,
      @MasterDate,
      @LocalHashBin,
      @EventType,
      @MasterStillExists,
      @IsGenerated
  END
  CLOSE curThis
  DEALLOCATE curThis


  IF @SQLFK IS NOT NULL BEGIN
    --Now we can create foreign keys

    IF @Debug = 1 BEGIN
      PRINT 'spsysSchemaVersionUpdateFromMaster: Executing SQL to create foreign keys >>4>'
      EXEC sqlver.sputilPrintString @SQLFK
      PRINT '<4<<'
    END

    BEGIN TRY
      EXEC(@SQLFK)
    END TRY
    BEGIN CATCH
      PRINT 'spsysSchemaVersionUpdateFromMaster: Error while creating foreign keys: ' + ERROR_MESSAGE()
      --PRINT '>>>' + ISNULL(@SQLFK, 'NULL')      
    END CATCH
  END


  SELECT   
    om.SchemaName,
    om.ObjectName,
    om.ObjectType,
    'No longer exists in master' AS Note
  FROM
    sqlver.tblSchemaManifest om
    LEFT JOIN sqlver.udftGetParsedValues(@ObjectCategories, ',') pv ON
      om.ObjectCategory = pv.[Value]      
    LEFT JOIN @tvMasterVersions mv ON
      om.SchemaName = mv.SchemaName AND
      om.ObjectName = mv.ObjectName
  WHERE
    om.StillExists = 1 AND
    ISNULL(om.ExcludeFromSync, 0) = 0 AND
    mv.ObjectName IS NULL AND
    (NULLIF(RTRIM(@ObjectCategories), '') IS NULL OR pv.[Value] IS NOT NULL)


  IF @UpdateCount = 0 BEGIN
    PRINT 'spsysSchemaVersionUpdateFromMaster: Nothing needs to be updated.'
  END
  ELSE BEGIN
    PRINT 'spsysSchemaVersionUpdateFromMaster: ' + ISNULL(CAST(@UpdateCount AS varchar(100)), 'No') + ' update' +
      CASE WHEN @UpdateCount <> 1 THEN 's' ELSE '' END + ' found.'
    PRINT 'spsysSchemaVersionUpdateFromMaster: ' + ISNULL(CAST(@UpdateAppliedCount AS varchar(100)), 'No') + ' update' + 
      CASE WHEN @UpdateAppliedCount <> 1 THEN 's' ELSE '' END + ' applied.'
  END

END

GO


IF OBJECT_ID('[sqlver].[spUninstall]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spUninstall]
END
GO

CREATE PROCEDURE [sqlver].[spUninstall]
@ReallyRemoveAll bit = 0
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spsysGenerateSQLVer]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysGenerateSQLVer]
END
GO

CREATE PROCEDURE [sqlver].[spsysGenerateSQLVer]
@EssentialOnly bit = 0,
@Buf nvarchar(MAX) = NULL OUTPUT,
@ReturnResultset bit = 0
--$!SQLVer Mar 12 2025 10:08PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @FinalBuf nvarchar(MAX)

  DECLARE @SQL nvarchar(MAX)
  DECLARE @IntroBanner nvarchar(MAX)
  DECLARE @PostRunSQL nvarchar(MAX)

  DECLARE @LicenseBody nvarchar(MAX)
  SET @LicenseBody =
'
/*
SQLVer
©Copyright 2006-2025 by David Rueter (drueter@assyst.com)
See:  https://github.com/davidrueter/sqlver

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
'

  SET @IntroBanner =
    '--SQLVer' + CASE WHEN @EssentialOnly = 1 THEN 'Essential' ELSE '' END +
    ' generated on ' + CAST(GETDATE() AS varchar(100)) + NCHAR(13) + NCHAR(10) +

    @LicenseBody +

    NCHAR(13) + NCHAR(10) + 
    NCHAR(13) + NCHAR(10) 


  SET @PostRunSQL = NCHAR(13) + NCHAR(10) +  '

PRINT ''Done processing all database objects.  SQLVer is now ready for normal use.''
PRINT ''''
'

+ CASE WHEN ISNULL(@EssentialOnly, 0) = 0 THEN
'
PRINT ''''
PRINT ''If you like, you can now build one or more of these CLR assemblies:''
PRINT ''''
PRINT ''
/*
EXEC sqlver.spsysBuildCLR_DiffMatch
EXEC sqlver.spsysBuildCLR_GetHTTP
EXEC sqlver.spsysBuildCLR_SendMail
EXEC sqlver.spsysBuildCLR_FTP
EXEC sqlver.spsysBuildCLR_SQLVerUtil
*/
''
'

 ELSE '' END




  CREATE TABLE #essential (
    FQObjName nvarchar(512),
    Seq int
  )

  INSERT INTO #essential (
    FQObjName,
    Seq
  )
  VALUES
    ('req', 10),
    ('[sqlver].[tblNumbers]', 20),
    ('popnum', 30),
    ('[sqlver].[sputilPrintString]', 40),
    ('[sqlver].[sputilResultSetAsStr]', 50),
    ('[sqlver].[udfFindInSQL]', 80),
    ('[sqlver].[udfHashBytesNMax]', 90),
    ('[sqlver].[udfIsInComment]', 100),
    ('[sqlver].[udfLTRIMSuper]', 110),
    ('[sqlver].[udfRTRIMSuper]', 120),
    ('[sqlver].[udfScriptTable]', 130),
    ('[sqlver].[udfScriptType]', 135),
    ('[sqlver].[udfSQLTerm]', 140),
    ('[sqlver].[udfStripSQLComments]', 150),
    ('[sqlver].[udfStripSQLCommentsExcept]', 150),
    ('[sqlver].[udfSubstrToDelims]', 160),
    ('[sqlver].[udftGetParsedValues]', 170),

    ('[sqlver].[tblSchemaLog]', 174),
    ('[sqlver].[tblSchemaManifest]', 178),

    ('[sqlver].[spsysSchemaExistSync]', 180),
    ('[sqlver].[spusrSchemaObjectCategorize]', 190),
    ('[sqlver].[spsysSchemaProcessObject]', 200),
    ('[sqlver].[spsysSchemaProcessAll]', 210),
    ('[sqlver].[spsysSchemaObjectCompare]', 220),
    ('[sqlver].[spsysSchemaObjectDefinition]', 230),
    ('[sqlver].[spsysSchemaObjectVersionsXML]', 240),
    ('[sqlver].[spsysSchemaMaster_ObjectDefinition]', 250),
    ('[sqlver].[spsysSchemaMaster_VersionsXML]', 260),
    ('[sqlver].[spsysSchemaVersionUpdateFromMaster]', 270),
    ('[sqlver].[spUninstall]', 280),
    ('[sqlver].[spsysGenerateSQLVer]', 290),
    ('[dtgSQLVerLogSchemaChanges]', 300)


  IF OBJECT_ID('tempdb..#scripts') IS NOT NULL BEGIN
    DROP TABLE #scripts
  END

  CREATE TABLE #scripts (
    Id int IDENTITY,
    Def nvarchar(MAX),
    FQObjName sysname NULL,
    ObjectType sysname NULL,
    Seq int,
    IsEssential bit,
    EsSeq int
  )

  
  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  SELECT 'req', 'PRINT ''Installing SQLVer'''


  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  SELECT 'req', 'SET ANSI_NULLS ON'


  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  SELECT 'req', 'SET QUOTED_IDENTIFIER ON'


  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  SELECT 'req',
    'IF EXISTS (SELECT * FROM sys.triggers WHERE name = ''dtgSQLVerLogSchemaChanges'' AND parent_class = 0) BEGIN' + NCHAR(13) + NCHAR(10) +
    ' DROP TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE' + NCHAR(13) + NCHAR(10) +
    'END'

  INSERT INTO #scripts (
    FQObjName,
    ObjectType,
    Def
  )
  SELECT 'req', 'SCHEMA',
    'IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = ''sqlver'') BEGIN' + NCHAR(13) + NCHAR(10) +
    'DECLARE @SQL nvarchar(MAX)' + NCHAR(13) + NCHAR(10) +
    'SET @SQL = ''CREATE SCHEMA [sqlver] ''' + NCHAR(13) + NCHAR(10) +
    'EXEC(@SQL)' + NCHAR(13) + NCHAR(10) +
    'END'


  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  SELECT
    QUOTENAME(sch.name) + '.' + QUOTENAME(typ.name) AS FQObjName,    
    sqlver.udfScriptType(sch.name, typ.name)
  FROM
    sys.types typ
    JOIN sys.schemas sch ON
      typ.schema_id = sch.schema_id
  WHERE
    typ.is_user_defined = 1 AND
    sch.name = 'sqlver'
  ORDER BY
    sch.name,
    typ.name
  

  INSERT INTO #scripts (
    FQObjName,
    Def
  )
  VALUES ('popnum', 
  'IF NOT EXISTS(SELECT TOP 1 Number FROM sqlver.tblNumbers) BEGIN' + NCHAR(13) + NCHAR(10) +
  'INSERT INTO sqlver.tblNumbers (Number)' + NCHAR(13) + NCHAR(10) +
  'SELECT TOP 200000' + NCHAR(13) + NCHAR(10) +
  'ROW_NUMBER() OVER (ORDER BY a.number, b.number)' + NCHAR(13) + NCHAR(10) +
  'FROM' + NCHAR(13) + NCHAR(10) +
  '  master..spt_values a' + NCHAR(13) + NCHAR(10) +
  ' JOIN master..spt_values b ON 1 = 1' + NCHAR(13) + NCHAR(10) +
  'END' + NCHAR(13) + NCHAR(10) +
  'ELSE BEGIN' + NCHAR(13) + NCHAR(10) +
  'PRINT ''WARNING: SQLVer requires that table sqlver.tblNumbers contain unique sequential integers from 1 to 200000.  Fewer rows may cause unexpected results.  More rows may be OK, but may degrade performance of certain functions.''' +
  NCHAR(13) + NCHAR(10) +
  'END' + NCHAR(13) + NCHAR(10)
  )


  INSERT INTO #scripts (
    FQObjName,
    ObjectType,
    Def
  )
  SELECT
    QUOTENAME(sch.name) + '.' + QUOTENAME(obj.name),

    obj.type,

    CAST(
    CASE obj.type
      WHEN 'U' THEN sqlver.udfScriptTable(sch.name, obj.name)
      WHEN 'SN' THEN N'CREATE SYNONYM [' + sch.name + N'].[' + obj.name + N'] FOR ' + syn.base_object_name
      ELSE sqlver.udfRTRIMSuper(sqlver.udfLTRIMSuper(sm.definition))
    END AS nvarchar(MAX)) +

    NCHAR(13) + NCHAR(10)
  FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
    LEFT JOIN sys.sql_modules sm ON
      obj.object_id = sm.object_id

    LEFT JOIN sys.synonyms syn ON
      sch.schema_id = syn.schema_id AND
      obj.object_id = syn.object_id

  WHERE
    (
     sch.name = 'sqlver' OR
     obj.name = 'sqlver.spSysSchemaProcessObject' OR
     (obj.type = 'SN' AND PARSENAME(syn.base_object_name, 2) = 'sqlver' AND sch.name IN ('dbo', 'sqlver', 'geonames', 'sdom'))
    ) AND
    obj.type NOT IN (
      'D', --  DEFAULT_CONSTRAINT
      'F',   --FOREIGN_KEY_CONSTRAINT
      'FS',  --CLR_SCALAR_FUNCTION
      'FT',  --CLR_TABLE_VALUED_FUNCTION
      'IT',  --INTERNAL_TABLE
      'PC',  --CLR_STORED_PROCEDURE
      'PK',  --PRIMARY_KEY_CONSTRAINT
      'S',  --SYSTEM_TABLE
      'SQ',  --SERVICE_QUEUE
      'TT',  --TYPE_TABLE
      'UQ'  --UNIQUE_CONSTRAINT
      )
  ORDER BY
    CASE obj.type
      WHEN 'U' THEN 1   --USER_TABLE
      WHEN 'V' THEN 2   --VIEW
      WHEN 'IF' THEN 3  --SQL_INLINE_TABLE_VALUED_FUNCTION
      WHEN 'FN' THEN 4  --SQL_SCALAR_FUNCTION
      WHEN 'TF' THEN 5  --SQL_TABLE_VALUED_FUNCTION
      WHEN 'P' THEN 6   --SQL_STORED_PROCEDURE
      WHEN 'SN' THEN 7  --SYNONYM
      WHEN 'TR' THEN 8  --SQL_TRIGGER
    END


  INSERT INTO #scripts (
    FQObjName,
    ObjectType,
    Def
  )
  SELECT
    QUOTENAME(tg.name),
    'TR',
    sm.definition
  FROM
    sys.triggers tg
    LEFT JOIN sys.sql_modules sm ON
      tg.object_id = sm.object_id
  WHERE
    tg.parent_class = 0
  ORDER BY
    tg.name

  --update #scripts to flag IsEssential and update EsSeq
  UPDATE scr
  SET
    EsSeq = es.Seq,
    IsEssential = CASE WHEN es.FQObjName IS NOT NULL THEN 1 ELSE 0 END
  FROM
    #scripts scr 
    LEFT JOIN #essential es ON
      scr.FQObjName = es.FQObjName


  --update #scripts to set Seq
  UPDATE scr
  SET
    Seq = x.Seq
  FROM
    (
    SELECT
      scr.Id,
      DENSE_RANK() OVER (ORDER BY CASE WHEN scr.IsEssential = 1 THEN 1 ELSE 2 END, scr.EsSeq, scr.Id) AS Seq
    FROM
      #scripts scr     
     ) x
    JOIN #scripts scr ON
      x.Id = scr.Id


  --SQL script that will be run by sqlver.sputilResultSetAsStr to concatenate the individual script elements into the final ouput string
  SET @SQL =
    'SELECT
    
      CASE scr.ObjectType WHEN ''U'' THEN ''IF OBJECT_ID('' + CHAR(39) + scr.FQObjName + CHAR(39) + '') IS NULL BEGIN'' + NCHAR(13) + NCHAR(10) ELSE '''' END +

      ISNULL(
      ''IF OBJECT_ID('' + CHAR(39) + scr.FQObjName + CHAR(39) + '') IS NOT NULL BEGIN'' + NCHAR(13) + NCHAR(10) +
      ''  DROP '' +
      CASE scr.ObjectType
        WHEN ''P'' THEN ''PROCEDURE''
        WHEN ''V'' THEN ''VIEW''
        WHEN ''SN'' THEN ''SYNONYM''
        WHEN ''IF'' THEN ''FUNCTION''
        WHEN ''FN'' THEN ''FUNCTION''
        WHEN ''TF'' THEN ''FUNCTION''
        WHEN ''TR'' THEN ''TRIGGER''
      END +
      '' '' + scr.FQObjName  + NCHAR(13) + NCHAR(10) +
      ''END'' + NCHAR(13) + NCHAR(10) +
      ''GO'' + NCHAR(13) + NCHAR(10) + 
      NCHAR(13) + NCHAR(10)
      , '''') + 

      scr.Def +

      NCHAR(13) + NCHAR(10) +

      CASE scr.ObjectType WHEN ''U'' THEN ''END

      ELSE BEGIN
      PRINT ''''WARNING:  Table '' + scr.FQObjName + '' already exists.''''
      PRINT ''''It would be best if you can drop this table and then re-execute this script to re-create it.''''
      PRINT ''''   DROP TABLE '' + scr.FQObjName + ''''''
      PRINT ''''If you do not drop this table, you may need to alter it manually.''''
      PRINT ''''New:''''
      PRINT ''''>>>>''''
      PRINT '''''' + scr.Def + ''''''
      PRINT ''''<<<<''''
      PRINT ''''''''
      PRINT ''''Existing: ''''
      PRINT ''''>>>>''''
      PRINT '' + ''sqlver.udfScriptTable('' + CHAR(39) + scr.FQObjName + CHAR(39) + '', NULL)'' + ''
      PRINT ''''<<<<''''
      PRINT ''''''''
      END'' + NCHAR(13) + NCHAR(10)' + 
      'ELSE '''' END +

      N''GO'' + NCHAR(13) + NCHAR(10) + NCHAR(13) + NCHAR(10)

    FROM
      #scripts scr
    ' +
    CASE WHEN @EssentialOnly = 1 THEN 'WHERE scr.IsEssential = 1' ELSE '' END +
    '
    ORDER BY
      scr.Seq'


  EXEC sqlver.sputilResultSetAsStr
    @SQL = @SQL,
    @IncludeLineBreaks = 1,
    @Result = @FinalBuf OUTPUT

  SET @FinalBuf = @IntroBanner +  @FinalBuf + @PostRunSQL  

  BEGIN TRY
    --Write the script to a file (on the server)
    DECLARE @Filename sysname
    SET @Filename =
         'SQLVer' + CASE WHEN @EssentialOnly = 1 THEN 'Essential' ELSE '' END + '_' +
          RIGHT('0000' + CAST(DATEPART(year, GETDATE()) AS varchar(100)), 4) +
          RIGHT('0' + CAST(DATEPART(month, GETDATE()) AS varchar(100)), 2) +
          RIGHT('0' + CAST(DATEPART(day, GETDATE()) AS varchar(100)), 2) + 
         '.sql'

    EXEC sqlver.sputilWriteStringToFile
            @FileData = @FinalBuf,
            @FilePath = 'C:\SQLVer\',
            @Filename = @Filename,
            @ErrorMsg = NULL

  END TRY
  BEGIN CATCH
    PRINT CONCAT('sqlver.spsysGenerateSQLVer could not output SQLVer script to C:\SQLVer\', @Filename, ' on the SQL server')
  END CATCH

  SET @Buf = @FinalBuf

  --Print the script out.  Note that the printed output will have a CHAR(126) tilde
  --character sequence ~-~ at the end of each 8000 characters:  you must manually remove
  --that and the following CR/LF before executing the printed string
  --(i.e Find the CHAR(126), then Delete Delete)
  EXEC sqlver.sputilPrintString @Buf

  DECLARE @CRLF nvarchar(6)
  SET @CRLF = CHAR(13) + CHAR(10)

  PRINT CONCAT(
    '/*', @CRLF,
    'WARNING:  You must search-and-replace the string printed here to replace:', @CRLF,
    '    ~-~{CR}{LF}', @CRLF,
    'with an empty string.', @CRLF,  @CRLF,
    'For example, using T-SQL:', @CRLF,  @CRLF,
    '    REPLACE(@Buf, CHAR(126) + CHAR(45) + CHAR(126) + CHAR(13) + CHAR(10), '''')', @CRLF,  @CRLF,
    'Or using SSMS, open Find and Replace (i.e. with CTRL-H), click the .* icon (to enable regular expressions), and search for:', @CRLF,  @CRLF,
    '    \x7e\x2d\x7e\x0d\x0a', @CRLF, @CRLF, @CRLF, @CRLF,
    '(This is due to a limitation of the T-SQL PRINT statement that does not provide a way to print long strings or to suppress CR LF.)', @CRLF,
    '*/'
  )


  IF @ReturnResultset = 1 BEGIN  
    SELECT
      scr.Seq,
      scr.FQObjName,
      scr.Def
    FROM
      #scripts scr
    WHERE
      (ISNULL(@EssentialOnly, 0) = 0 OR scr.IsEssential = 1)
    ORDER BY
      scr.Seq
  END
    
END

GO


IF OBJECT_ID('[dtgSQLVerLogSchemaChanges]') IS NOT NULL BEGIN
  DROP TRIGGER [dtgSQLVerLogSchemaChanges]
END
GO

CREATE TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE
FOR
  create_procedure, alter_procedure, drop_procedure,
  create_table, alter_table, drop_table,
  create_view, alter_view, drop_view,
  create_function, alter_function, drop_function,
  create_index, alter_index, drop_index,
  create_trigger, alter_trigger, drop_trigger,
  create_synonym, drop_synonym,
  create_type, drop_type
--$!SQLVer Nov 13 2024 10:12AM by sa

--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET XACT_ABORT OFF;

  DECLARE @Msg nvarchar(MAX)

  BEGIN TRY
    DECLARE @EventData xml
    SET @EventData = EVENTDATA()

    EXEC sqlver.spsysSchemaProcessObject @EventData = @EventData
  END TRY
  BEGIN CATCH
    SET @Msg = CONCAT('Warning: SQLVer encountered an error: ', ERROR_MESSAGE())

    IF (SELECT sysdb.user_access_desc FROM sys.databases sysdb WHERE sysdb.name = DB_NAME()) = 'SINGLE_USER' BEGIN
      SET @Msg = CONCAT(
        @Msg,
        ' This may be due to the fact that the database is currently in SINGLE_USER mode.',
        ' If the problem persists you could try temporarily disabling the SQLVer Database Trigger',
        ' with: ', CHAR(13) + CHAR(10), 'DISABLE TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE', CHAR(13) + CHAR(10),
        ' (but be sure to re-enable with ENABLE TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE when done.)'
        )
    END

    PRINT @Msg
  END CATCH
END
GO


CREATE TYPE [sqlver].[typDictTable] AS TABLE(
  [ID] [int] NULL,
  [ParamName] [nvarchar](254) NULL,
  [ParamValue] [nvarchar](max) NULL
  )
GO


IF OBJECT_ID('[sqlver].[tblSecureValues]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblSecureValues](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[KeyName] [sysname] NOT NULL,
	[DateUpdated] [datetime] NULL CONSTRAINT [dfSecureValues__DateUpdated] DEFAULT (getdate()),
	[SecureValue] [varbinary](8000) NULL,
	CONSTRAINT [pkSecureValues] PRIMARY KEY CLUSTERED
(
  [id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSecureValue_KeyName ON [sqlver].[tblSecureValues]
(
  [KeyName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE UNIQUE NONCLUSTERED INDEX ixSecureValues_KeyName ON [sqlver].[tblSecureValues]
(
  [KeyName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblSecureValues] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblSecureValues]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblSecureValues](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[KeyName] [sysname] NOT NULL,
	[DateUpdated] [datetime] NULL CONSTRAINT [dfSecureValues__DateUpdated] DEFAULT (getdate()),
	[SecureValue] [varbinary](8000) NULL,
	CONSTRAINT [pkSecureValues] PRIMARY KEY CLUSTERED
(
  [id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSecureValue_KeyName ON [sqlver].[tblSecureValues]
(
  [KeyName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE UNIQUE NONCLUSTERED INDEX ixSecureValues_KeyName ON [sqlver].[tblSecureValues]
(
  [KeyName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblSecureValues]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[tblTempTables]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblTempTables](
	[TempTableID] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [sysname] NOT NULL,
	[FoundInProc_ObjectID] [int] NULL,
	[FoundInProc_FQName] [nvarchar](512) NULL,
	[FirstStartPos] [int] NULL,
	CONSTRAINT [pkTempTables] PRIMARY KEY CLUSTERED
(
  [TempTableID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblTempTables] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblTempTables]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblTempTables](
	[TempTableID] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [sysname] NOT NULL,
	[FoundInProc_ObjectID] [int] NULL,
	[FoundInProc_FQName] [nvarchar](512) NULL,
	[FirstStartPos] [int] NULL,
	CONSTRAINT [pkTempTables] PRIMARY KEY CLUSTERED
(
  [TempTableID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblTempTables]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[tblDeploymentRepository]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblDeploymentRepository](
	[Filename] [varchar](255) NULL,
	[Description] [varchar](255) NULL,
	[FileData] [varbinary](MAX) NULL
) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblDeploymentRepository] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblDeploymentRepository]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblDeploymentRepository](
	[Filename] [varchar](255) NULL,
	[Description] [varchar](255) NULL,
	[FileData] [varbinary](MAX) NULL
) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblDeploymentRepository]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[tblSysRTLog]') IS NULL BEGIN
CREATE TABLE [sqlver].[tblSysRTLog](
	[SysRTLogId] [int] IDENTITY(1,1) NOT NULL,
	[DateLogged] [datetime] NULL CONSTRAINT [dfSysRTLog__DateLogged] DEFAULT (getdate()),
	[Msg] [nvarchar](MAX) NULL,
	[MsgXML] [xml] NULL,
	[ThreadGUID] [uniqueidentifier] NULL,
	[SPID] [int] NULL,
	CONSTRAINT [pkSysRTLog] PRIMARY KEY CLUSTERED
(
  [SysRTLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_DateLogged ON [sqlver].[tblSysRTLog]
(
  [DateLogged] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_RTLogID ON [sqlver].[tblSysRTLog]
(
  [SysRTLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_ThreadGUID ON [sqlver].[tblSysRTLog]
(
  [ThreadGUID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

END

      ELSE BEGIN
      PRINT 'WARNING:  Table [sqlver].[tblSysRTLog] already exists.'
      PRINT 'It would be best if you can drop this table and then re-execute this script to re-create it.'
      PRINT '   DROP TABLE [sqlver].[tblSysRTLog]'
      PRINT 'If you do not drop this table, you may need to alter it manually.'
      PRINT 'New:'
      PRINT '>>>>'
      PRINT 'CREATE TABLE [sqlver].[tblSysRTLog](
	[SysRTLogId] [int] IDENTITY(1,1) NOT NULL,
	[DateLogged] [datetime] NULL CONSTRAINT [dfSysRTLog__DateLogged] DEFAULT (getdate()),
	[Msg] [nvarchar](MAX) NULL,
	[MsgXML] [xml] NULL,
	[ThreadGUID] [uniqueidentifier] NULL,
	[SPID] [int] NULL,
	CONSTRAINT [pkSysRTLog] PRIMARY KEY CLUSTERED
(
  [SysRTLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_DateLogged ON [sqlver].[tblSysRTLog]
(
  [DateLogged] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_RTLogID ON [sqlver].[tblSysRTLog]
(
  [SysRTLogId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX ixSysRTLog_ThreadGUID ON [sqlver].[tblSysRTLog]
(
  [ThreadGUID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
'
      PRINT '<<<<'
      PRINT ''
      PRINT 'Existing: '
      PRINT '>>>>'
      PRINT sqlver.udfScriptTable('[sqlver].[tblSysRTLog]', NULL)
      PRINT '<<<<'
      PRINT ''
      END
GO


IF OBJECT_ID('[sqlver].[vwSchemaLog]') IS NOT NULL BEGIN
  DROP VIEW [sqlver].[vwSchemaLog]
END
GO

CREATE VIEW sqlver.vwSchemaLog
--$!SQLVer Dec  2 2020  1:58PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
SELECT
  schl.SchemaLogID,
  schl.EventType,
  schl.ObjectName,
  schl.SchemaName,
  schl.ObjectType,
  schl.SQLCommand,
  schl.EventDate,
  schl.Hash,
  schl.Comments,
  schl.SQLFullTable
FROM
  sqlver.tblSchemaLog schl

GO


IF OBJECT_ID('[sqlver].[vwSchemaManifest]') IS NOT NULL BEGIN
  DROP VIEW [sqlver].[vwSchemaManifest]
END
GO

CREATE VIEW sqlver.vwSchemaManifest
--$!SQLVer Dec  2 2020  2:04PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
SELECT
  schm.SchemaManifestID,
  schm.ObjectName,
  schm.SchemaName,
  schm.OrigDefinition,
  schm.DateAppeared,
  schm.DateUpdated,
  schm.OrigHash,
  schm.CurrentHash,
  schm.IsEncrypted,
  schm.StillExists,
  schm.SkipLogging,
  schm.Comments,
  schm.ObjectType,
  schm.IsGenerated,
  schm.IsUserDefined,
  schm.HasError,
  schm.ErrorMessage,
  schm.UpdateAvail,
  schm.UpdateHash,
  schm.UpdateDefinition,
  schm.InhibitUpdate,
  schm.UpdateBatchGUID,
  schm.IncludeInQueryBuilder,
  schm.ColumnDefinition,
  schm.ExecuteAs,
  schm.WriteProtected,
  schm.ForceSchemaBinding,
  schm.ObjectCategory,
  schl.SQLCommand AS CurrentDefinition
FROM
  sqlver.tblSchemaManifest schm
  LEFT JOIN sqlver.tblSchemaLog schl ON
    schm.CurrentHash = schl.Hash

GO


IF OBJECT_ID('[sqlver].[udfMaxInt]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfMaxInt]
END
GO

CREATE FUNCTION sqlver.udfMaxInt(@var sql_variant)
RETURNS bigint
--$!SQLVer Oct 14 2024  7:57AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql?view=sql-server-ver16
  --https://stackoverflow.com/questions/2699975/getting-maximum-value-of-float-in-sql-programmatically

  SET @var = ISNULL(@var, 0.0)
  
  RETURN CASE CAST(SQL_VARIANT_PROPERTY(@var, 'BaseType') aS sysname)
    WHEN 'bigint' THEN CAST(9223372036854775807 AS bigint)
    WHEN 'int' THEN CAST(2147483647 AS int)
    WHEN 'smallint' THEN 	CAST(32767 AS int)
    WHEN 'tinyint' THEN CAST(127 AS int)
    WHEN 'bit' THEN CAST(1 AS bit)
  END
END

GO


IF OBJECT_ID('[sqlver].[udfTrimLead]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfTrimLead]
END
GO

CREATE FUNCTION [sqlver].[udfTrimLead](@Buf nvarchar(4000), @TargetChars nvarchar(5) = '0')
RETURNS nvarchar(4000)
--$!SQLVer Oct 12 2022  1:12PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @P int

  SELECT @P = MIN(x.Number)
  FROM
  (
  SELECT
    n.Number,
    SUBSTRING(@Buf, n.Number, 1) AS ThisChar
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf)
  ) x
  WHERE
    ASCII(x.ThisChar) >= 32 AND
    CHARINDEX(x.ThisChar, ISNULL(@TargetChars, '0')) = 0

  RETURN RIGHT(@Buf, LEN(@Buf) - @P + CASE WHEN SUBSTRING(@Buf, LEN(@Buf) - @P + 1, 1) = '.' THEN 0 ELSE 1 END)
END

GO


IF OBJECT_ID('[sqlver].[udfScriptIndexDrop]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfScriptIndexDrop]
END
GO

CREATE FUNCTION sqlver.udfScriptIndexDrop(
@SchemaName sysname,
@ObjectName sysname = NULL
)
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  IF @ObjectName IS NULL BEGIN
    SET @ObjectName = PARSENAME(@SchemaName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 2)
  END
  ELSE BEGIN
    SET @ObjectName = PARSENAME(@ObjectName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 1)
  END

  DECLARE @SQL nvarchar(MAX)
  SELECT
    @SQL = ISNULL(@SQL, '') +
    'DROP INDEX ' + QUOTENAME(ix.name) + ' ON ' + QUOTENAME(sch.name) + '.' + QUOTENAME(tab.name) + NCHAR(13) + NCHAR(10) 
  FROM
    sys.tables tab
    JOIN sys.schemas sch ON
      tab.schema_id = sch.schema_id
    JOIN sys.indexes ix ON
      tab.object_id = ix.object_id
  WHERE
    ix.type_desc = 'NONCLUSTERED' AND
    tab.type_desc = 'USER_TABLE' AND
    sch.name = @SchemaName AND
    tab.name = @ObjectName

  RETURN(@SQL)
END

GO


IF OBJECT_ID('[sqlver].[udfDecimalToFraction]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfDecimalToFraction]
END
GO

CREATE FUNCTION [sqlver].[udfDecimalToFraction](@DecNum decimal(18,8), @Denom tinyint)
RETURNS varchar(10)
--$!SQLVer Mar 13 2024  5:18PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  Returns a string containing the value of @DecNum expressed in
  fractional notation, with the precision of 1/@Denom.

  For example, for 16ths, set @Denom to 16

  Will try to simplify the fraction if @Simplify = 1
  */
  DECLARE @Result varchar(10)
  DECLARE @Simplify bit
  SET @Simplify = 1

  DECLARE @WholePart bigint
  SET @WholePart = CAST(@DecNum AS bigint)

  DECLARE @FracPart decimal(9,8)
  SET @FracPart = ABS(@DecNum % 1)

  DECLARE @FracString nvarchar(10)

  DECLARE @Numer int
  SET @Numer = ROUND(@FracPart * @Denom, 0)

  IF @Simplify = 1 BEGIN
    WHILE @Denom > 2 AND @Numer % 2 = 0 AND @Denom % 2 = 0 BEGIN
      SET @Denom = @Denom / 2
      SET @Numer = @Numer / 2
    END
  END

  IF @Numer / @Denom = 1 BEGIN
    SET @WholePart = @WholePart + 1
    SET @Numer = 0
  END

  SET @FracString =
    CASE
      WHEN @Numer > 0 THEN CONCAT(@Numer, '/', @Denom)
      ELSE ''
    END

  IF @WholePart = 0 BEGIN
    SET @Result = CASE WHEN @Numer = 0 THEN '0' ELSE @FracString END
  END
  ELSE BEGIN
    SET @Result = CONCAT(CAST(@WholePart AS varchar(10)), ' ', @FracString)
  END

  RETURN @Result

  /*
  --An alternate approach, hard-coded to 16ths of an inch:
  DECLARE @Result varchar(10)

  DECLARE @WholePart bigint
  SET @WholePart = CAST(@DecNum AS bigint)

  DECLARE @FracPart decimal(9,8)
  SET @FracPart = ABS(@DecNum % 1)

  DECLARE @FracString nvarchar(10)
  SET @FracString =
  CASE 
    WHEN @FracPart > 0.03125 AND @FracPart <= 0.09375 THEN '1/16'
    WHEN @FracPart > 0.09375 And @FracPart <= 0.15625 THEN '1/8'
    WHEN @FracPart > 0.15625 And @FracPart <= 0.21875 THEN '3/16'
    WHEN @FracPart > 0.21875 And @FracPart <= 0.28125 THEN '1/4'
    WHEN @FracPart > 0.28125 And @FracPart <= 0.34375 THEN '5/16'
    WHEN @FracPart > 0.34375 And @FracPart <= 0.40625 THEN '3/8'
    WHEN @FracPart > 0.40625 And @FracPart <= 0.46875 THEN '7/16'
    WHEN @FracPart > 0.46875 And @FracPart <= 0.53125 THEN '1/2'
    WHEN @FracPart > 0.53125 And @FracPart <= 0.59375 THEN '9/16'
    WHEN @FracPart > 0.59375 And @FracPart <= 0.65625 THEN '5/8'
    WHEN @FracPart > 0.65625 And @FracPart <= 0.71875 THEN '11/16'
    WHEN @FracPart > 0.71875 And @FracPart <= 0.78125 THEN '3/4'
    WHEN @FracPart > 0.78125 And @FracPart <= 0.84375 THEN '13/16'
    WHEN @FracPart > 0.84375 And @FracPart <= 0.90625 THEN '7/8'
    WHEN @FracPart > 0.90625 And @FracPart <= 0.96875 THEN '15/16'
    WHEN @FracPart <= 0.03125 THEN ''
  END


  IF @FracPart > 0.96875 BEGIN
    SET @WholePart = @WholePart +
      CASE
        WHEN @WholePart < 0 THEN -1
        ELSE 1
      END
  END

  IF @WholePart = 0 BEGIN
    SET @Result = @FracString
  END
  ELSE BEGIN
    SET @Result = CONCAT(CAST(@WholePart AS varchar(10)), ' ', @FracString)
  END

  RETURN @Result
  */
END

GO


IF OBJECT_ID('[sqlver].[udfCurrentTimeZoneName]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCurrentTimeZoneName]
END
GO

CREATE FUNCTION sqlver.udfCurrentTimeZoneName()
RETURNS sysname

WITH EXECUTE AS OWNER
--$!SQLVer Sep 10 2021  7:46AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  To use the AT TIME ZONE clause we often need to know the
  server's timezone.  While SQL provides CURRENT_TIMEZONE() it does not
  currently (as of SQL2019) provide CURRENT_TIMEZONE_ID()

  So we need to get the current timezone from the registry.

  https://blog.sqlauthority.com/2014/02/15/sql-server-get-current-timezone-name-in-sql-server/
  https://github.com/bootstrap-vue/bootstrap-vue/issues/5842
  https://docs.microsoft.com/en-us/sql/t-sql/functions/current-timezone-id-transact-sql?view=sql-server-ver15
  https://docs.microsoft.com/en-us/sql/t-sql/functions/current-timezone-transact-sql?view=sql-server-ver15
  https://stackoverflow.com/questions/64735380/how-to-find-current-timezone-id

  */

  DECLARE @TimeZone VARCHAR(50)
  EXEC MASTER.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
  'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
  'TimeZoneKeyName',@TimeZone OUT
  RETURN @TimeZone
END

GO


IF OBJECT_ID('[sqlver].[udfHasTimeZone]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfHasTimeZone]
END
GO

CREATE FUNCTION sqlver.udfHasTimeZone(
@TimeStr varchar(40)
)
RETURNS bit
--$!SQLVer Sep 10 2021  7:46AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  When working with ISO8601 date/time strings, the string may or may not contain
  the timezone.
     '2021-09-06T13:06:46-07:00' indicates the timezone is GMT -7 hours
     '2021-09-06T13:06:46' does not provide a timezone
  This funtion returns a bit:
    1 indicates that timezone information is present,
    0 indicates that timezone information is not present
  */
  DECLARE @TZMinus int
  DECLARE @TZPlus int
  DECLARE @TimeT int
  DECLARE @Result bit
  DECLARE @RT varchar(40)

  SET @RT = REVERSE(@TimeStr)

  SET @TZMinus = CHARINDEX('-', @RT)
  SET @TZPlus = CHARINDEX('+', @RT)
  SET @TimeT = CHARINDEX('T', @RT)

  IF COALESCE(NULLIF(@TZMinus, 0), NULLIF(@TZPlus, 0)) < @TimeT BEGIN
    SET @Result = 1
  END
  ELSE BEGIN
    SET @Result = 0
  END

  RETURN @Result

END

GO


IF OBJECT_ID('[sqlver].[udfISO8601ToDateTime]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfISO8601ToDateTime]
END
GO

CREATE FUNCTION sqlver.udfISO8601ToDateTime(
@TimeStr varchar(40)
)
RETURNS datetime
--$!SQLVer Sep 10 2021  7:46AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  @TimeStr contains a date/time string formatted as per ISO8601.
  It may or may not contain timezone information, such as:
    '2021-09-06T13:06:46-07:00'
    yyyy-MM-ddThh:mm:ss.fffZ (no spaces)
   or
    '2021-09-06T13:06:46'
     yyyy-mm-ddThh:mi:ss.mmm (no spaces)

  If timezone is specified, we will convert the time to the server's
  timezone.

  https://docs.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver15
  https://www.mssqltips.com/sqlservertip/1145/date-and-time-conversions-using-sql-server/
  https://docs.microsoft.com/en-us/sql/t-sql/queries/at-time-zone-transact-sql?view=sql-server-ver15
  https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-time-zone-info-transact-sql?view=sql-server-ver15
  https://database.guide/get-the-current-time-zone-of-the-server-in-sql-server-t-sql/
  https://docs.microsoft.com/en-us/sql/t-sql/functions/current-timezone-id-transact-sql?view=sql-server-ver15
  https://github.com/bootstrap-vue/bootstrap-vue/issues/5842
  */

  DECLARE @Result datetime

  IF sqlver.udfHasTimeZone(@TimeStr) = 1 BEGIN
    SET @Result = 
      CAST(CONVERT(datetimeoffset,@TimeStr,127)
      AT TIME ZONE sqlver.udfCurrentTimeZoneName()
      AS datetime)
  END
  ELSE BEGIN
    SET @Result =
      CAST(CONVERT(datetimeoffset,@TimeStr,126)
      AS datetime)
  END

  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrTo]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrTo]
END
GO

CREATE FUNCTION sqlver.udfCopyStrTo(@InputStr nvarchar(MAX), @Delimiter nchar(1))
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Jan 14 2022  9:07AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN
    LEFT(@InputStr, ISNULL(NULLIF(CHARINDEX(@Delimiter, @InputStr) - 1, -1), LEN(@InputStr)))
END

GO


IF OBJECT_ID('[sqlver].[udfCleanAlphaNumOnly]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCleanAlphaNumOnly]
END
GO

CREATE FUNCTION [sqlver].[udfCleanAlphaNumOnly](
@InStr varchar(MAX)
)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Jul 14 2021  8:08AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @L int
  SET @L = LEN(@InStr)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RETURN CAST('Error in sqlver.udfCleanAlphaNumOnly:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.' AS int)
  END

  DECLARE @Buf varchar(MAX)
  
  SELECT
    @Buf = ISNULL(@Buf, '') +
      CASE
        WHEN ASCII(SUBSTRING(@InStr, num.Number, 1)) < 32 THEN '' 
        WHEN ASCII(SUBSTRING(@InStr, num.Number, 1)) < 48 OR PATINDEX('%' + SUBSTRING(@InStr, num.Number, 1) + '%', '01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ') = 0 THEN ''        
        ELSE SUBSTRING(@InStr, num.Number, 1)
      END
  FROM
    sqlver.tblNumbers num
  WHERE
    num.Number <= LEN(@InStr)
    
  --SET @Buf = REPLACE(REPLACE(@Buf, '__', '_'), ' ', '') 
  
  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfScriptTable2]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfScriptTable2]
END
GO

CREATE FUNCTION [sqlver].[udfScriptTable2](
@SchemaName sysname, --can contain schema.name if @ObjectName is NULL
@ObjectName sysname = NULL, --can be NULL
@indexesOnly bit = 0,
@DropIndexesOnly bit = 0,
@ExcludeIndexes bit = 0
)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Aug  3 2021  9:53AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Based on script contributed by Marcello - 25/09/09, in comment to article posted by 
  --Tim Chapman, TechRepublic, 2008/11/20
  --http://www.builderau.com.au/program/sqlserver/soa/Script-Table-definitions-using-TSQL/0,339028455,339293405,00.htm
  
  --Formatting altered by David Rueter (drueter@assyst.com) 2010/05/11 to match
  --script generated by MS SQL Server Management Studio 2005

  IF @ObjectName IS NULL BEGIN
    SET @ObjectName = PARSENAME(@SchemaName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 2)
  END
  ELSE BEGIN
    SET @ObjectName = PARSENAME(@ObjectName, 1)
    SET @SchemaName = PARSENAME(@SchemaName, 1)
  END
  

  DECLARE @id int,
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
    @id=obj.object_id,
    @f1 = CHAR(13) + CHAR(10),
    @f2 = CHAR(9),
    @f3=@f1+@f2,
    @f4=',' + @f3
  FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
  WHERE
    sch.name LIKE @SchemaName AND
    obj.name LIKE @ObjectName    

  IF @id IS NULL RETURN NULL

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
            CASE
             WHEN t.Name IN ('numeric', 'decimal') THEN c.precision
             WHEN c.max_length = -1 THEN c.max_length
             WHEN t.Name IN ('nchar', 'nvarchar') THEN c.max_length / 2
             ELSE c.max_length
            END, -1)), 'MAX') + 
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
      c.object_id=@id
  )

  INSERT INTO @tvData(D, o)
  SELECT
    CHAR(9) + D + CASE Nr WHEN Clr THEN '' ELSE ',' + @f1 END,
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
        i.object_id = @id AND
        i.name=c.name
    WHERE
      parent_object_id=@id AND
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
        i.object_id=@id AND i.index_id=@i2
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
    CASE
      WHEN @DropIndexesOnly = 1 THEN
        @f1 + CHAR(13) + CHAR(10) + 'DROP INDEX ' +
        s.name  + ' ON ' +
        QUOTENAME(sc.Name) + '.' + QUOTENAME(o.name)
      ELSE
        @f1 + CHAR(13) + CHAR(10) + 'CREATE ' +
        CASE is_unique WHEN 1 THEN 'UNIQUE ' ELSE '' END +
        UPPER(s.type_desc) + ' INDEX ' + 
        s.name  + ' ON ' +
        QUOTENAME(sc.Name) + '.' + QUOTENAME(o.name)
    END,      

    index_id,
    NULL,
    1000
  FROM 
    sys.indexes s
    INNER JOIN sys.objects o ON o.object_id = s.object_id
    INNER JOIN sys.schemas sc ON sc.schema_id = o.schema_id
  WHERE
    s.object_id = @id AND
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
      --@ixCol=NULL
        

    IF ISNULL(@DropIndexesOnly, 0) = 0 BEGIN
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
  --            CASE c.is_descending_key 
  --              WHEN 1  THEN ' DESC'
  --              ELSE ' ASC' 
  --            END
            END

          FROM
            sys.index_columns c
            INNER JOIN sys.columns cc ON
              c.column_id = cc.column_id AND
              cc.object_id = c.object_id
          WHERE
            c.object_id = @id AND
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


   SELECT
     @i=0,
     @Sql=NULL;


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
    
  IF ISNULL(@indexesOnly, 0) = 0 AND ISNULL(@DropIndexesOnly, 0) = 0 BEGIN
    SELECT
      @Sql = 'CREATE TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name) + '(' + @f1
    FROM
      sys.objects o
      INNER JOIN sys.schemas s
    ON
      o.schema_id = s.schema_id
    WHERE
      o.object_id = @id
  END

  SET @i = 0

  WHILE 1 = 1 BEGIN
    SELECT TOP 1
      @i = tv.Id,
      @Sql = ISNULL(@Sql, '') + tv.D 
    FROM
      @tvData tv
    WHERE
      ((ISNULL(@indexesOnly, 0) = 0 AND ISNULL(@DropIndexesOnly, 0) = 0) OR tv.o >= 1000) AND
      (ISNULL(@ExcludeIndexes, 0) = 0 OR tv.o < 1000)
    ORDER BY
      tv.o,
      CASE WHEN tv.o=0 THEN RIGHT('0000' + CONVERT(VARCHAR, tv.id), 5)  ELSE tv.D END,
      tv.id

    IF @@ROWCOUNT = 0 BREAK

    DELETE FROM @tvData
    WHERE
      id = @i

  END

  RETURN @Sql
END

GO


IF OBJECT_ID('[sqlver].[udfGetSecureValue]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfGetSecureValue]
END
GO

CREATE FUNCTION sqlver.udfGetSecureValue(@KeyName sysname)
RETURNS nvarchar(4000)
--$!SQLVer Sep 13 2022 11:30AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @PlainValue nvarchar(4000) = NULL
  DECLARE @PlainValueBin varbinary(8000) = NULL
  DECLARE @CryptKey nvarchar(1024) = NULL

  IF @CryptKey IS NULL BEGIN
    SELECT
      --@CryptKey = ENCRYPTBYPASSPHRASE('sqlver', sv.SecureValue)
      @CryptKey =sv.SecureValue
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.id = '0'
  END

  DECLARE @SVID int

  SELECT @SVID = sv.id
  FROM
    sqlver.tblSecureValues sv
  WHERE
    sv.KeyName = @KeyName


  IF @SVID IS NULL BEGIN
    SET @PlainValueBin = NULL
  END
  ELSE BEGIN
    SELECT
      @PlainValueBin = DECRYPTBYPASSPHRASE(@CryptKey, sv.SecureValue)
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.ID = @SVID
  END

  SET @PlainValue = CAST(@PlainValueBin AS nvarchar(4000))

  RETURN @PlainValue

END

GO


IF OBJECT_ID('[sqlver].[udfParseValueQ]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfParseValueQ]
END
GO

CREATE FUNCTION [sqlver].[udfParseValueQ] (
  @InputString nvarchar(MAX),
  @ValueIndex int,
  @Delimiter nchar(1) = ',',
  @Quote nchar(1) = '`',
  @StripQuote bit = 0
  )
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN (
    SELECT [Value]
    FROM sqlver.udftGetParsedValuesQ(@InputString, @Delimiter, @Quote, @StripQuote)
    WHERE
      [Index] = @ValueIndex
  )
END

GO


IF OBJECT_ID('[sqlver].[udfFixStringEncoding]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfFixStringEncoding]
END
GO

CREATE FUNCTION sqlver.udfFixStringEncoding (@InputText varchar(MAX))
RETURNS varchar(MAX)
--$!SQLVer Mar 11 2025  3:27PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  -- Fix common UTF-8 misinterpretation issues
  SET @InputText = REPLACE(@InputText, 'â€œ', '"'); -- Open Smart Double Quote
  SET @InputText = REPLACE(@InputText, 'â€', '"'); -- Close Smart Double Quote
  SET @InputText = REPLACE(@InputText, 'â€™', ''''); -- Apostrophe / Right Single Quote
  SET @InputText = REPLACE(@InputText, 'â€˜', ''''); -- Left Single Quote
  SET @InputText = REPLACE(@InputText, 'â€”', '--'); -- Em Dash
  SET @InputText = REPLACE(@InputText, 'â€“', '--'); -- En Dash
  SET @InputText = REPLACE(@InputText, 'â€¦', '...'); -- Ellipsis
  SET @InputText = REPLACE(@InputText, 'â€¢', '*'); -- Bullet Point
  SET @InputText = REPLACE(@InputText, 'â„¢', '(TM)'); -- Trademark Symbol
  SET @InputText = REPLACE(@InputText, 'â€', '"'); -- Generic Double Quote Issue
  SET @InputText = REPLACE(@InputText, 'Ã©', 'é'); -- Latin Small Letter e with Acute
  SET @InputText = REPLACE(@InputText, 'Ã¨', 'è'); -- Latin Small Letter e with Grave
  SET @InputText = REPLACE(@InputText, 'Ã', 'A'); -- Miscellaneous A corruption
  SET @InputText = REPLACE(@InputText, 'Ã±', 'ñ'); -- Latin Small Letter n with Tilde
  SET @InputText = REPLACE(@InputText, 'Ã³', 'ó'); -- Latin Small Letter o with Acute
  SET @InputText = REPLACE(@InputText, 'Ã¡', 'á'); -- Latin Small Letter a with Acute
  SET @InputText = REPLACE(@InputText, 'Ãº', 'ú'); -- Latin Small Letter u with Acute
  SET @InputText = REPLACE(@InputText, 'Ã?Â©', 'é'); -- Additional encoding for é
    
  -- Fix non-ASCII symbols to plain text equivalents
  SET @InputText = REPLACE(@InputText, N'“', '"'); -- Left Double Quote
  SET @InputText = REPLACE(@InputText, N'”', '"'); -- Right Double Quote
  SET @InputText = REPLACE(@InputText, N'‘', ''''); -- Left Single Quote
  SET @InputText = REPLACE(@InputText, N'’', ''''); -- Right Single Quote
  SET @InputText = REPLACE(@InputText, N'–', '-'); -- En Dash
  SET @InputText = REPLACE(@InputText, N'—', '-'); -- Em Dash
  SET @InputText = REPLACE(@InputText, N'…', '...'); -- Ellipsis
  SET @InputText = REPLACE(@InputText, N'«', '"'); -- Left Angle Quote
  SET @InputText = REPLACE(@InputText, N'»', '"'); -- Right Angle Quote
  SET @InputText = REPLACE(@InputText, N'-', '-'); -- Non-Breaking Hyphen
  SET @InputText = REPLACE(@InputText, N'©', '(c)'); -- Copyright Symbol
  SET @InputText = REPLACE(@InputText, N'®', '(R)'); -- Registered Symbol
  SET @InputText = REPLACE(@InputText, N'™', '(TM)'); -- Trademark Symbol
  SET @InputText = REPLACE(@InputText, N'°', ' degrees'); -- Degree Symbol
  SET @InputText = REPLACE(@InputText, N'½', '1/2'); -- Half Fraction
  SET @InputText = REPLACE(@InputText, N'¼', '1/4'); -- Quarter Fraction
  SET @InputText = REPLACE(@InputText, N'¾', '3/4'); -- Three-Quarter Fraction
  SET @InputText = REPLACE(@InputText, N'†', '*'); -- Dagger
  SET @InputText = REPLACE(@InputText, N'‡', '**'); -- Double Dagger
    
  RETURN @InputText;
END;

GO


IF OBJECT_ID('[sqlver].[udfReplaceList]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfReplaceList]
END
GO

CREATE FUNCTION [sqlver].[udfReplaceList](
@String nvarchar(MAX),
@CharMap nvarchar(MAX) --comma-delimited list of oldchar:newchar pairs
)
RETURNS nvarchar(MAX)
--$!SQLVer Jan 19 2024 11:48AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @tvCharMap TABLE (
    OldChar nvarchar(10),
    NewChar nvarchar(10)
  )

  INSERT INTO @tvCharMap (
    OldChar,
    NewChar
  )
  SELECT
    sqlver.udfParseValue(pv.[Value], 1, ':'),
    sqlver.udfParseValue(pv.[Value], 2, ':')
  FROM
    sqlver.udftGetParsedValues(@CharMap, ',') pv
  WHERE
    pv.[Value] IS NOT NULL

  DECLARE @Buf nvarchar(MAX)
  SET @Buf = @String

  DECLARE @OldChar nvarchar(10)
  DECLARE @NewChar nvarchar(10)


  DECLARE curThis CURSOR LOCAL STATIC
  FOR
  SELECT OldChar, NewChar FROM @tvCharMap

  OPEN curThis
  FETCH curThis INTO @OldChar, @NewChar
  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @Buf = REPLACE(@Buf, @OldChar, @NewChar)
    FETCH curThis INTO @OldChar, @NewChar
  END
  CLOSE curThis
  DEALLOCATE curThis

  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfTrim]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfTrim]
END
GO

CREATE FUNCTION [sqlver].[udfTrim](@Buf nvarchar(4000), @TargetChars nvarchar(5) = '0', @Cmd varchar(5) = 'R')
RETURNS nvarchar(4000)
--$!SQLVer Jan 20 2022  7:20AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  /*
  Trims whitespace (< ASCII 32)

 --Additionally:
  @TargetChars can contain one or more regular characters to remove

  @Cmd can contain one or more of:
    R -- Trim from Right
    L -- Trim from Left
    D -- Trim trailing decimal (Right trim only)
  */


  IF @Cmd IS NULL BEGIN
    SET @Cmd = 'R'
  END

  DECLARE @P int

  IF CHARINDEX('L', @Cmd) > 0 BEGIN

    SELECT @P = MIN(x.Number)
    FROM
    (
    SELECT
      n.Number,
      SUBSTRING(@Buf, n.Number, 1) AS ThisChar
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf + 'x') - 1
    ) x
    WHERE
      ASCII(x.ThisChar) >= 32 AND
      CHARINDEX(x.ThisChar, ISNULL(@TargetChars, '0')) = 0

    SET @Buf = RIGHT(@Buf, LEN(@Buf + 'x') - 1 - @P + 1)

  END

  IF CHARINDEX('R', @Cmd) > 0 BEGIN

    SELECT @P = MIN(x.Number)
    FROM
    (
    SELECT
      n.Number,
      SUBSTRING(REVERSE(@Buf), n.Number, 1) AS ThisChar
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf + 'x') - 1
    ) x
    WHERE
      ASCII(x.ThisChar) >= 32 AND
      CHARINDEX(x.ThisChar, ISNULL(@TargetChars, '0')) = 0

    RETURN LEFT(
      @Buf, LEN(@Buf + 'x') - 1 - @P + 1
      + CASE WHEN CHARINDEX('D', @Cmd) > 0 AND SUBSTRING(@Buf, LEN(@Buf + 'x') - 1 - @P + 1, 1) = '.' THEN -1 ELSE 0 END
    )

  END

  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfDictionaryLookup]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfDictionaryLookup]
END
GO

CREATE FUNCTION [sqlver].[udfDictionaryLookup](
@ParamName nvarchar(254),
@TVData sqlver.typDictTable READONLY
)
RETURNS nvarchar(MAX)
--$!SQLVer Sep 27 2022  2:42PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --retrieve the value associated with @ParamName
  --from the provided @TVData table containing name-value
  --pairs
  DECLARE @Result nvarchar(MAX)
  
  SELECT
    @Result = dict.ParamValue
  FROM
    @TVData dict
  WHERE
    dict.ParamName = @ParamName
    
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfHashBytesBinMax]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfHashBytesBinMax]
END
GO

CREATE FUNCTION [sqlver].[udfHashBytesBinMax](@Algorithm sysname = 'SHA2_256', @Input varbinary(MAX))
RETURNS varbinary(MAX)
--$!SQLVer Jul  9 2024  4:30AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  WARNINGS:
  1) If running SQL 2016 or later the built-in HASHBYTES() supports > 8000 characters,
     so this function may not be needed.
  2) This function works reliably, but produces different results than the built-in HASHBYTES()
  3) This function is needed for backwards-compabililty with hashes stored in sqlver.tblSchemaLog
  4) When using HASHBYTES() you should take care to pass in varbinary
     (and not varchar or nvarchar) as results vary depending on data type
  5) The results of this function do not match the results of the built-in HASHBYTES function
      5a) This function has a flaw: the last chunk of data (i.e. LEN() MOD 4000) gets handled
          as an nvarchar instead of as a varbinary.  Thus the results of this function
          are different than HASHBYTES()
      5b) This function concatenates the hashes of each 4K chunk and then hashes that.  But
          the implementation of HASHBYTES() seems to handle this slightly differently...leading to
          different results.
  */
  IF @Algorithm IS NULL BEGIN
    SET @Algorithm = 'SHA2_256'
  END

  IF NULLIF(PATINDEX('%|' + @Algorithm + '|%', '|MD2|MD4|MD5|SHA|SHA1|SHA2_256|SHA2_512|'), 0) IS NULL BEGIN
    RETURN CAST('Error in sqlver.udfHashBytesBinMax: ' + ISNULL(@Algorithm, 'NULL') + ' is not a valid value for @Algorithm.' as int)
  END
  
  DECLARE @Result varbinary(MAX)

  DECLARE @Chunk int
  DECLARE @ChunkSize int
  DECLARE @ChunkInput nvarchar(MAX)
  
  SET @ChunkSize = 4000
  SET @Chunk = 1
  SET @Result = CAST('' AS varbinary(MAX))

  WHILE @Chunk * @ChunkSize < LEN(@Input) BEGIN
    --Append the hash for each chunk
    SET @ChunkInput = SUBSTRING(@Input, ((@Chunk - 1) * @ChunkSize) + 1, @ChunkSize)
    SET @Result = @Result + HASHBYTES(@Algorithm, @ChunkInput)
    SET @Chunk = @Chunk + 1
  END

  --Append the hash for the final partial chunk
  SET @ChunkInput = RIGHT(@Input, LEN(@Input) - ((@Chunk - 1) * @ChunkSize))
  SET @Result = @Result + HASHBYTES(@Algorithm, @ChunkInput)

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


IF OBJECT_ID('[sqlver].[udfLPad]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfLPad]
END
GO

CREATE FUNCTION [sqlver].[udfLPad] (
   @ThisString varchar(254),
   @PadChar varchar(1),
   @Length int
  )
RETURNS varchar(254)
--$!SQLVer Jul  9 2021 12:23PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @MaxLen int

  DECLARE @Return varchar(254)
  
  SET @ThisString = RIGHT(@ThisString, @Length) --truncate long string

  SET @Return = Replicate(@PadChar, @Length - LEN(@ThisString + 'x') + 1) + @ThisString

  RETURN @Return
END

GO


IF OBJECT_ID('[sqlver].[udfParseVarValue]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfParseVarValue]
END
GO

CREATE FUNCTION [sqlver].[udfParseVarValue](
@Buf nvarchar(MAX),
@VarName nvarchar(254),
@Delim nchar(1))
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Oct 23 2024  4:36PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result nvarchar(MAX)

  SELECT
    @Result = pv.[Value]
  FROM 
    sqlver.udftGetParsedValues(@Buf, @Delim) pv
  WHERE
    pv.[Value] LIKE @VarName + '=%'


  SET @Result = SUBSTRING(@Result, PATINDEX('%=%', @Result) + 1, LEN(@Result))
 
  DECLARE @Decode bit
  SET @Decode = 0

  IF @Delim = '&' BEGIN
    --assume value is URL-encoded
    SET @Decode = 1
  END

  IF @Decode = 1 BEGIN
    SET @Result = sqlver.udfURLDecode(@Result)
  END

  IF RTRIM(@Result) = '' SET @Result = NULL

  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfStripTempTablePrefixes]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfStripTempTablePrefixes]
END
GO

CREATE FUNCTION [sqlver].[udfStripTempTablePrefixes](@Buf nvarchar(MAX))
RETURNS nvarchar(MAX)
--$!SQLVer Sep  9 2022 12:45PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Buf) = 1 BEGIN
    SET @ObjID = CAST(@Buf AS int)
  END
  ELSE IF LEN(@Buf) < 254 BEGIN
    SET @ObjID = OBJECT_ID(@Buf)
  END
  
  IF @ObjID IS NOT NULL BEGIN
     SET @Buf = OBJECT_DEFINITION(@ObjID)
  END


  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT DISTINCT
    '#' + sqlver.udfCopyStrTo(SUBSTRING(@Buf, n.Number + 1, 254), '#')
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf) AND
    SUBSTRING(@Buf, n.Number, 4) = '#___'

  DECLARE @ThisPrefix nvarchar(100)

  OPEN curThis
  FETCH curThis INTO @ThisPrefix

  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @Buf = REPLACE(@Buf, @ThisPrefix, '')
    FETCH curThis INTO @ThisPrefix
  END
  CLOSE curThis
  DEALLOCATE curThis

  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfParseVarRemove]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfParseVarRemove]
END
GO

CREATE FUNCTION [sqlver].[udfParseVarRemove](
@Buf varchar(MAX),
@VarName varchar(254),
@Delim char(1))
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfHMAC]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfHMAC]
END
GO

CREATE FUNCTION [sqlver].[udfHMAC] (
@Buf varbinary(8000),
@SecretKey varbinary(MAX),
@Algorithm sysname = 'SHA2_256'
)
RETURNS varbinary(MAX)
--$!SQLVer Oct 23 2024  4:33PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --More information:

  --https://en.wikipedia.org/wiki/HMAC
  --https://datatracker.ietf.org/doc/html/rfc2104#section-2
  --https://medium.com/@short_sparrow/how-hmac-works-step-by-step-explanation-with-examples-f4aff5efb40e
  --https://www.freeformatter.com/hmac-generator.html#before-output
  --https://cryptii.com/pipes/hmac
  --I wrote this function and then subsequently discovered: https://gist.github.com/rmalayter/3130462

  --Validate @Algorithm
  IF NULLIF(PATINDEX('%|' + @Algorithm + '|%', '|SHA2_256|SHA2_512|'), 0) IS NULL BEGIN
    RETURN CAST('Error in sqlver.udfHMAC: ' + ISNULL(@Algorithm, 'NULL') + ' is not a valid value for @Algorithm.' as int)
  END

  --Validate SQL Version
  DECLARE @SQLVersion int
  SET @SQLVersion = sqlver.udfParseValue(CAST(SERVERPROPERTY('productversion') AS varchar(100)), 1, '.')
  IF @SQLVersion < 13 BEGIN  --lower than SQL 2016  See: https://learn.microsoft.com/en-us/troubleshoot/sql/releases/download-and-install-latest-updates#sql-server-complete-version-list-tables
    RETURN CAST('Error in sqlver.udfHMAC: Not supported on your version of MSSQL (due to HASHBYTES limitations).' as int)
  END
 
  DECLARE @BlockLength int --byte length of blocks
  SET @BlockLength = 64  --default to 64 bytes (512 bits)
  
  IF PATINDEX('%256%', @Algorithm) > 0 BEGIN
    SET @BlockLength = 64
  END
  IF PATINDEX('%512%', @Algorithm) > 0 BEGIN
    SET @BlockLength = 128
  END
 

  DECLARE @IPad tinyint
  SET @IPad =0x36

  DECLARE @OPad tinyint
  SET @OPad = 0x5C

  DECLARE @PaddedKey varbinary(MAX)

  DECLARE @IKey varbinary(MAX)
  DECLARE @OKey varbinary(MAX)

	DECLARE @i integer

  --Convert @SecretKey to the proper length @PaddedKey
	IF LEN(@SecretKey) > @BlockLength  BEGIN
    SET @PaddedKey = HASHBYTES(@Algorithm, @SecretKey) --hash the long key value to shorten it
  END
	ELSE BEGIN
 		SET @PaddedKey = SUBSTRING(@SecretKey + CAST('' AS binary(2048)), 1, @BlockLength) --otherwise pad it out with zeros
  END

	SET @i = 1
  SET @IKey = CAST('' AS varbinary(2048))
  SET @OKey = CAST('' AS varbinary(2048))

	WHILE @i <= @BlockLength
	BEGIN
		SET @IKey = @IKey + CAST((SUBSTRING(@PaddedKey, @i, 1) ^ @IPad) AS varbinary(2048))
    SET @OKey = @OKey + CAST((SUBSTRING(@PaddedKey, @i, 1) ^ @OPad) AS varbinary(2048))
		SET @i = @i + 1
	END

	RETURN HASHBYTES(@Algorithm , @OKey + HASHBYTES(@Algorithm , @IKey + @Buf))
END

GO


IF OBJECT_ID('[sqlver].[udfMath_rad2deg]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfMath_rad2deg]
END
GO

CREATE FUNCTION [sqlver].[udfMath_rad2deg](
@rad float)
RETURNS float
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result float
  SET @Result = @rad * 180 / PI()
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfReplaceRight]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfReplaceRight]
END
GO

CREATE FUNCTION [sqlver].[udfReplaceRight](
@Buf varchar(MAX),
@TrailChars varchar(MAX),
@ReplaceWith varchar(MAX),
@EmptyStrAsNull bit)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Jan 19 2024 11:47AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result varchar(MAX)
  SET @Result = @Buf
  
  WHILE @Result LIKE '%'  +@TrailChars BEGIN
    SET @Result = LEFT(@Result, LEN(@Result + 'x') - 1 - LEN(@TrailChars + 'x') + 1)
  END

  IF @EmptyStrAsNull = 1 BEGIN
    SET @Result = NULLIF(@Result, '')
  END
   
  IF @Buf + 'x' <> @Result + 'x' BEGIN
    SET @Result = @Result + ISNULL(@ReplaceWith, '')
  END
   
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfDistanceFromCoordinates]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfDistanceFromCoordinates]
END
GO

CREATE FUNCTION [sqlver].[udfDistanceFromCoordinates](
@LatitudeA float,
@LongitudeA float,
@LatitudeB float,
@LongitudeB float,
@Unit char)
RETURNS float
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfReplaceLeft]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfReplaceLeft]
END
GO

CREATE FUNCTION [sqlver].[udfReplaceLeft](
@Buf varchar(MAX),
@LeadChars varchar(MAX),
@ReplaceWith varchar(MAX),
@EmptyStrAsNull bit)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Jan 19 2024 11:47AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result varchar(MAX)
  SET @Result = @Buf
  
  WHILE @Result LIKE @LeadChars + '%' BEGIN
    SET @Result = RIGHT(@Result, LEN(@Result + 'x') - 1 - LEN(@LeadChars + 'x') + 1)
  END

  IF @EmptyStrAsNull = 1 BEGIN
    SET @Result = NULLIF(@Result, '')
  END
   
  IF @Buf + 'x' <> @Result + 'x' BEGIN
    SET @Result = ISNULL(@ReplaceWith, '') + @Result
  END
   
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfMath_deg2rad]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfMath_deg2rad]
END
GO

CREATE FUNCTION [sqlver].[udfMath_deg2rad](
@deg float)
RETURNS float
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result float
  SET @Result = @deg * PI() / 180
  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfParseValueReplace]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfParseValueReplace]
END
GO

CREATE FUNCTION [sqlver].[udfParseValueReplace](
  @InputString varchar(MAX),
  @Delimiter char(1),
  @Index int,
  @NewValue varchar(MAX)
)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @L int
  SET @L = LEN(@InputString)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RETURN CAST('Error in sqlver.udfParseValueReplace:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.' AS int)
  END

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


IF OBJECT_ID('[sqlver].[udfColDef]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfColDef]
END
GO

CREATE FUNCTION [sqlver].[udfColDef](@Object sysname)
RETURNS nvarchar(MAX)
--$!SQLVer Sep  5 2022  8:47PM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @Buf nvarchar(MAX)

  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Object) = 0 BEGIN
    SET @ObjID = OBJECT_ID(@Object)
  END

  SELECT @Buf = 
    STRING_AGG(
      x.ColDef,
      ',' + CHAR(13) + CHAR(10))
      WITHIN GROUP (ORDER BY x.Seq)
  FROM (
    SELECT
      col.column_id AS Seq,

      CONCAT(
        col.name,
        ' ',
        typ.name,
        CASE
          WHEN typ.name IN ('numeric', 'decimal') THEN '(' + CAST(col.precision AS varchar(100)) + ', ' + CAST(col.scale AS varchar(100)) + ')'
          WHEN typ.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'varbinary') AND col.max_length > 0 THEN '(' + CAST(col.max_length AS varchar(100)) + ')'
          WHEN typ.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'varbinary') AND col.max_length = -1 THEN '(MAX)'
          ELSE ''
        END,
        CASE WHEN col.is_nullable = 0 THEN ' NOT NULL' ELSE '' END,
        CASE WHEN ixc.object_id IS NOT NULL THEN ' PRIMARY KEY' ELSE '' END
      ) AS ColDef
    FROM
      sys.tables tab
      JOIN sys.schemas sch ON
        tab.schema_id = sch.schema_id
      JOIN sys.columns col ON
        tab.object_id = col.object_id
      JOIN sys.types typ ON
        col.user_type_id = typ.user_type_id
      LEFT JOIN sys.indexes ix ON
        tab.object_id = ix.object_id AND
        ix.is_primary_key = 1

      LEFT JOIN sys.index_columns ixc ON
        ix.object_id = ixc.object_id AND
        ix.index_id = ixc.index_id and
        col.column_id = ixc.column_id
    WHERE
      tab.object_id = @ObjID
  ) x


  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfURLEncode]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfURLEncode]
END
GO

CREATE FUNCTION [sqlver].[udfURLEncode](
@Buf varchar(MAX)
)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfBase64Decode]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfBase64Decode]
END
GO

CREATE FUNCTION sqlver.udfBase64Decode(
@Encoded varchar(MAX)
)
RETURNS varbinary(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN CAST('' AS xml).value('xs:base64Binary(sql:variable("@Encoded"))', 'varbinary(MAX)')
END

GO


IF OBJECT_ID('[sqlver].[udfStripHTML]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfStripHTML]
END
GO

CREATE FUNCTION [sqlver].[udfStripHTML](@Buf nvarchar(MAX))
RETURNS nvarchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  Rudimentary function for stripping HTML tags out of a string.
  
  May fail on singletons and single tags other than <br> <hr> and </p>.
  
  Use SQLDOM for a fuller solution if needed.
  */
  
  DECLARE @L int
  SET @L = LEN(@Buf)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RETURN CAST('Error in sqlver.udfStripHTML:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.' AS int)
  END
  
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


IF OBJECT_ID('[sqlver].[udfBase64DecodeStr]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfBase64DecodeStr]
END
GO

CREATE FUNCTION sqlver.udfBase64DecodeStr(
@Encoded varchar(MAX)
)
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN CAST(sqlver.udfBase64Decode(@Encoded) AS varchar(MAX))
END

GO


IF OBJECT_ID('[sqlver].[udfBase64Encode]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfBase64Encode]
END
GO

CREATE FUNCTION sqlver.udfBase64Encode(
@SourceBin varbinary(MAX)
)
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN CAST('' AS xml).value('xs:base64Binary(sql:variable("@SourceBin"))', 'varchar(MAX)')
END

GO


IF OBJECT_ID('[sqlver].[udfBase64EncodeStr]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfBase64EncodeStr]
END
GO

CREATE FUNCTION sqlver.udfBase64EncodeStr(
@SourceStr varchar(MAX)
)
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN sqlver.udfBase64Encode(CAST(@SourceStr AS varbinary(MAX)))
END

GO


IF OBJECT_ID('[sqlver].[udfURLDecode]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfURLDecode]
END
GO

CREATE FUNCTION [sqlver].[udfURLDecode](
@Buf varchar(MAX)
)
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Oct 23 2024  4:37PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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

  SET @SQL = 'REPLACE(' + @SQL + ',' + 
    CASE WHEN @Decode = 1 THEN @EscSeq ELSE 
      CASE 
        WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39)
        WHEN @NativeChar = CHAR(9) THEN 'CHAR(9)'
        WHEN @NativeChar = CHAR(10) THEN 'CHAR(10)'
        WHEN @NativeChar = CHAR(13) THEN 'CHAR(13)'
        ELSE CHAR(39) + @NativeChar + CHAR(39) END    
    END +
    ', ' + 
    CASE WHEN @Decode = 0 THEN @EscSeq ELSE 
      CASE
        WHEN @NativeChar = CHAR(39) THEN CHAR(39) + CHAR(39)
        WHEN @NativeChar = CHAR(9) THEN 'CHAR(9)'
        WHEN @NativeChar = CHAR(10) THEN 'CHAR(10)'
        WHEN @NativeChar = CHAR(13) THEN 'CHAR(13)'
        ELSE CHAR(39) + @NativeChar + CHAR(39) END
    END + 
    ')'  + @CRLF
  FETCH curThis INTO @NativeChar, @EscSeq
END
CLOSE curThis
DEALLOCATE curThis

PRINT @SQL
*/
RETURN
--REPLACE(REPLACE(
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Buf
,'+', ' ')
,'%01', '?')
,'%02', '?')
,'%03', '?')
,'%04', '?')
,'%05', '?')
,'%06', '?')
,'%07', '?')
,'%08', '?')
--,'%09', CHAR(9))
--,'%0a', CHAR(10))
,'%0b', '?')
,'%0c', '?')
,'%0d', '')
,'%0e', '?')
,'%0f', '?')
,'%10', '?')
,'%11', '?')
,'%12', '?')
,'%13', '?')
,'%14', '?')
,'%15', '?')
,'%16', '?')
,'%17', '?')
,'%18', '?')
,'%19', '?')
,'%1a', '?')
,'%1b', '?')
,'%1c', '?')
,'%1d', '?')
,'%1e', '?')
,'%1f', '?')
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


IF OBJECT_ID('[sqlver].[udfIsInt]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfIsInt]
END
GO

CREATE FUNCTION sqlver.udfIsInt(@Buf varchar(254))
RETURNS bit
--$!SQLVer Sep  3 2022  5:49AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result bit
  SET @Result = 0

  IF LEFT(@Buf, 1) = '-' BEGIN
    SET @Buf = RIGHT(@Buf, LEN(@Buf + 'x') - 1 -1)
  END

  IF PATINDEX('%[^-0-9]%', @Buf) = 0 BEGIN
    SET @Result = 1
  END

  RETURN @Result

END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrToNonAlphaNum]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrToNonAlphaNum]
END
GO

CREATE FUNCTION sqlver.udfCopyStrToNonAlphaNum(@InputStr nvarchar(MAX))
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Sep  3 2022  5:49AM by sa
--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN
  LEFT(@InputStr, 
    COALESCE(
      NULLIF(PATINDEX('%[^_0-9A-Za-z]%', @InputStr), 0) - 1,
      LEN(@InputStr + 'x') -1
    )
  )
END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrToNonIdent]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrToNonIdent]
END
GO

CREATE FUNCTION [sqlver].[udfCopyStrToNonIdent](@InputStr nvarchar(MAX))
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Sep  3 2022  5:49AM by sa
--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN
  LEFT(@InputStr, 
    COALESCE(
      NULLIF(PATINDEX('%[^_#0-9A-Za-z]%', @InputStr), 0) - 1,
      LEN(@InputStr + 'x') -1
    )
  )
END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrToWhite]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrToWhite]
END
GO

CREATE FUNCTION sqlver.udfCopyStrToWhite(@InputStr nvarchar(MAX))
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Sep  3 2022  5:49AM by sa
--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN
  LEFT(@InputStr, 
    COALESCE(
      NULLIF(PATINDEX('%[ ' + CHAR(9) + CHAR(10) + CHAR(13) + ']%', @InputStr), 0) - 1,
      LEN(@InputStr + 'x') -1
    )
  )
END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrAfter]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrAfter]
END
GO

CREATE FUNCTION [sqlver].[udfCopyStrAfter](
@InputStr nvarchar(MAX),
@Delimeter nvarchar(40)
)
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Aug  3 2021  9:26AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @P int
  SET @P = PATINDEX('%' + REPLACE(@Delimeter, '_', '[_]') + '%', @InputStr)
  RETURN CASE WHEN @P > 0 THEN SUBSTRING(@InputStr, @P + LEN(@Delimeter + 'x') - 1, LEN(@InputStr + 'x') - 1) END
END

GO


IF OBJECT_ID('[sqlver].[udfCopyStrPriorWord]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfCopyStrPriorWord]
END
GO

CREATE FUNCTION sqlver.udfCopyStrPriorWord(@Buf nvarchar(MAX), @P int)
RETURNS nvarchar(MAX)
--$!SQLVer Sep  3 2022  5:49AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET @Buf = REVERSE(sqlver.udfCopyStrToWhite(sqlver.udfLTRIMSuper(REVERSE(LEFT(@Buf, @P - 1)))))
  RETURN @Buf
END

GO


IF OBJECT_ID('[sqlver].[udfParseValue]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfParseValue]
END
GO

CREATE FUNCTION [sqlver].[udfParseValue] (
  @InputString nvarchar(MAX),
  @ValueIndex int,
  @Delimiter nchar(1) = ','
  )
RETURNS nvarchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfDecimalToFraction2]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfDecimalToFraction2]
END
GO

CREATE FUNCTION sqlver.udfDecimalToFraction2(@DecNum decimal(18,8))
RETURNS varchar(10)
--$!SQLVer Mar 13 2024  4:14PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  Returns a string containing the value of @DecNum expressed in fractional
  notation with precision to 1/16.
  */
  DECLARE @Result varchar(10)

  DECLARE @WholePart bigint
  SET @WholePart = CAST(@DecNum AS bigint)

  DECLARE @FracPart decimal(9,8)
  SET @FracPart = ABS(@DecNum % 1)

  DECLARE @FracString nvarchar(10)
  SET @FracString =
  CASE 
    WHEN @FracPart > 0.03125 AND @FracPart <= 0.09375 THEN '1/16'
    WHEN @FracPart > 0.09375 And @FracPart <= 0.15625 THEN '1/8'
    WHEN @FracPart > 0.15625 And @FracPart <= 0.21875 THEN '3/16'
    WHEN @FracPart > 0.21875 And @FracPart <= 0.28125 THEN '1/4'
    WHEN @FracPart > 0.28125 And @FracPart <= 0.34375 THEN '5/16'
    WHEN @FracPart > 0.34375 And @FracPart <= 0.40625 THEN '3/8'
    WHEN @FracPart > 0.40625 And @FracPart <= 0.46875 THEN '7/16'
    WHEN @FracPart > 0.46875 And @FracPart <= 0.53125 THEN '1/2'
    WHEN @FracPart > 0.53125 And @FracPart <= 0.59375 THEN '9/16'
    WHEN @FracPart > 0.59375 And @FracPart <= 0.65625 THEN '5/8'
    WHEN @FracPart > 0.65625 And @FracPart <= 0.71875 THEN '11/16'
    WHEN @FracPart > 0.71875 And @FracPart <= 0.78125 THEN '3/4'
    WHEN @FracPart > 0.78125 And @FracPart <= 0.84375 THEN '13/16'
    WHEN @FracPart > 0.84375 And @FracPart <= 0.90625 THEN '7/8'
    WHEN @FracPart > 0.90625 And @FracPart <= 0.96875 THEN '15/16'
    WHEN @FracPart <= 0.03125 THEN ''
  END


  IF @FracPart > 0.96875 BEGIN
    SET @WholePart = @WholePart +
      CASE
        WHEN @WholePart < 0 THEN -1
        ELSE 1
      END
  END

  IF @WholePart = 0 BEGIN
    SET @Result = @FracString
  END
  ELSE BEGIN
    SET @Result = CONCAT(CAST(@WholePart AS varchar(10)), ' ', @FracString)
  END

  RETURN @Result
END

GO


IF OBJECT_ID('[sqlver].[udfTrimTrail]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfTrimTrail]
END
GO

CREATE FUNCTION [sqlver].[udfTrimTrail](@Buf nvarchar(4000), @TrailChars nvarchar(5) = '0')
RETURNS nvarchar(4000)
--$!SQLVer Jan 20 2022  7:20AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @P int

  SELECT @P = MIN(x.Number)
  FROM
  (
  SELECT
    n.Number,
    SUBSTRING(REVERSE(@Buf), n.Number, 1) AS ThisChar
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf + 'x') - 1
  ) x
  WHERE
    ASCII(x.ThisChar) >= 32 AND
    CHARINDEX(x.ThisChar, ISNULL(@TrailChars, '0')) = 0

    RETURN LEFT(
      @Buf, LEN(@Buf + 'x') - 1 - @P + 1
      + CASE WHEN SUBSTRING(@Buf, LEN(@Buf + 'x') - 1 - @P + 1, 1) = '.' THEN -1 ELSE 0 END
    )
END

GO


IF OBJECT_ID('[sqlver].[udfStrToGUID]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfStrToGUID]
END
GO

CREATE FUNCTION [sqlver].[udfStrToGUID](
@Str varchar(100))
RETURNS uniqueidentifier

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Result uniqueidentifier
  SET @Result = NULL
  
  SET @Str=LTRIM(RTRIM(@Str))
  
--  IF LEN(@Str) <> 36 BEGIN
--    RAISERROR('Error in strToGUID:  Input string must be 32 characters long', 16, 1)
--    RETURN NULL
--  END

  SET @Str = REPLACE(REPLACE(REPLACE(@Str, '-', ''), '{', ''), '}', '')  

  DECLARE @InvalidStr bit
  SET @InvalidStr = 0
  
  IF LEN(@Str) <> 32 BEGIN
    SET @InvalidStr = 1
  END
  ELSE BEGIN
    DECLARE @i int
    SET @i = 1
    WHILE @i < LEN(@Str) BEGIN
      IF PATINDEX('%' + SUBSTRING(@Str, @i, 1) + '%', '0123456789ABCDEF') = 0 BEGIN
        SET @InvalidStr = 1
        BREAK
      END
      SET @i = @i + 1
    END
  END
  
  
  IF @InvalidStr = 1 BEGIN
--    RAISERROR('Error in strToGUID:  Input string must contain only valid hexadecimal characters (0123456789ABCDEF)', 16, 1)
    SET @Result = NULL
  END
  ELSE BEGIN
      
    DECLARE @Buf varchar(100)
    SET @Buf = 
        LEFT(@Str, 8) + '-' + 
        SUBSTRING(@Str, 9, 4) + '-' +
        SUBSTRING(@Str, 13, 4) + '-' +      
        SUBSTRING(@Str, 17, 4) + '-' +        
        RIGHT(@Str, 12)
        
  --  BEGIN TRY
      SET @Result = CAST(@Buf AS uniqueidentifier)
  --  END TRY
  --  BEGIN CATCH
  --    RAISERROR('Error in strToGUID:  Input string could not be cast to an uniqueidentifier (GUID)', 16, 1)
  --  END CATCH
  END

  
  RETURN @Result
  
END

GO


IF OBJECT_ID('[sqlver].[udfGUIDToStr]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfGUIDToStr]
END
GO

CREATE FUNCTION [sqlver].[udfGUIDToStr](
@GUID uniqueidentifier)
RETURNS varchar(100)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN REPLACE(CAST(@GUID AS varchar(100)), '-', '')
END

GO


IF OBJECT_ID('[sqlver].[udfScriptTable_TempDB]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfScriptTable_TempDB]
END
GO

CREATE FUNCTION [sqlver].[udfScriptTable_TempDB](
@ObjectName sysname = NULL)   --can be NULL
RETURNS varchar(MAX)

WITH EXECUTE AS OWNER
--$!SQLVer Aug  3 2021  9:53AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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
  @Sql nvarchar(MAX),
  @Sql2 nvarchar(MAX),
  @f2 varchar(5),
  @f3 varchar(5),
  @f4 varchar(5),
  @T varchar(5)

  DECLARE @ActualObjectName sysname
  DECLARE @CRLF nvarchar(5)
  SET @CRLF = NCHAR(13) + NCHAR(10)

  SELECT
    @Id=obj.object_id,
    @ActualObjectName=obj.name,
    @f2 = CHAR(9),
    @f3=@CRLF+@f2,
    @f4=',' + @f3
  FROM
    tempdb.sys.objects obj
  WHERE
  obj.object_id = OBJECT_ID(@ObjectName)
  
  SET @ObjectName = REPLACE(@ObjectName, 'tempdb..', '')

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
            CASE
             WHEN t.Name IN ('numeric', 'decimal') THEN c.precision
             WHEN c.max_length = -1 THEN c.max_length
             WHEN t.Name IN ('nchar', 'nvarchar') THEN c.max_length / 2
             ELSE c.max_length
            END, -1)), 'MAX') + 
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
      tempdb.sys.columns c
      INNER JOIN tempdb.sys.types t ON t.user_type_id = c.user_type_id
      INNER JOIN tempdb.sys.schemas s ON s.schema_id = t.schema_id
      LEFT OUTER JOIN tempdb.sys.computed_columns cc ON
        cc.object_id = c.object_id AND
        cc.column_id = c.column_id

      LEFT OUTER JOIN tempdb.sys.default_constraints d ON
        d.parent_object_id = @id AND
        d.parent_column_id=c.column_id

      LEFT OUTER JOIN tempdb.sys.identity_columns ic ON
        ic.object_id = c.object_id AND
        ic.column_id=c.column_id

    WHERE
      c.object_id=@Id  
  )

  INSERT INTO @tvData(D, o)
  SELECT
    CHAR(9) + D + CASE Nr WHEN Clr THEN '' ELSE ',' + @CRLF END,
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
      tempdb.sys.objects c 
      LEFT OUTER JOIN tempdb.sys.indexes i ON
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
        tempdb.sys.check_constraints 
      WHERE object_id=@i
    END
    ELSE IF @T = 'Pk' BEGIN
      INSERT INTO @tvData 
      SELECT
        @f4 + 'CONSTRAINT ' + 
        QUOTENAME('pk' + REPLACE(@ObjectName, 'tbl', '')) +
        ' PRIMARY KEY' + ISNULL(' ' + NULLIF(UPPER(i.type_desc),'NONCLUSTERED'), ''),
        @i2, null, 20      
      FROM tempdb.sys.indexes i
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
        tempdb.sys.foreign_keys f        
      WHERE
        f.object_id=@i
          
      INSERT INTO @tvData 
      SELECT ' REFERENCES ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name), -2, @i, 41
      FROM
        tempdb.sys.foreign_keys f
        INNER JOIN tempdb.sys.objects o ON o.object_id = f.referenced_object_id
        INNER JOIN tempdb.sys.schemas s ON s.schema_id = o.schema_id
      WHERE
        f.object_id=@i
      
      INSERT INTO @tvData 
      SELECT ' NOT FOR REPLICATION', -3, @i, 42
      FROM
        tempdb.sys.foreign_keys f
        INNER JOIN tempdb.sys.objects o ON o.object_id = f.referenced_object_id
        INNER JOIN tempdb.sys.schemas s ON s.schema_id = o.schema_id
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
  VALUES(@CRLF+') ON ' + QUOTENAME('PRIMARY'), null, null, 100)  
  
  -- Indexes
  INSERT INTO @tvData
  SELECT
    @CRLF + CHAR(13) + CHAR(10) + 'CREATE ' +
      CASE is_unique WHEN 1 THEN 'UNIQUE ' ELSE '' END +
      UPPER(s.type_desc) + ' INDEX ' + 
      s.name  + ' ON ' +
      QUOTENAME(sc.Name) + '.' + QUOTENAME(o.name),      

    index_id,
    NULL,
    1000
  FROM 
    tempdb.sys.indexes s
    INNER JOIN tempdb.sys.objects o ON o.object_id = s.object_id
    INNER JOIN tempdb.sys.schemas sc ON sc.schema_id = o.schema_id
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
--            CASE c.is_descending_key 
--              WHEN 1  THEN ' DESC'
--              ELSE ' ASC' 
--            END
          END

        FROM
          tempdb.sys.index_columns c
          INNER JOIN tempdb.sys.columns cc ON
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
          tempdb.sys.foreign_key_columns f
          INNER JOIN tempdb.sys.columns c1 ON
            c1.column_id = f.parent_column_id AND
            c1.object_id = f.parent_object_id
          INNER JOIN tempdb.sys.columns c2 ON
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
    @Sql = 'CREATE TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(o.name) + '(' + @CRLF
  FROM
    tempdb.sys.objects o
    INNER JOIN tempdb.sys.schemas s
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

  SET @SQL = REPLACE(@SQL, @ActualObjectName, @ObjectName)
  RETURN @Sql
END

GO


IF OBJECT_ID('[sqlver].[udfSecondsToChar]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfSecondsToChar]
END
GO

CREATE FUNCTION [sqlver].[udfSecondsToChar] (@ThisSec bigint, @ForceDay bit)
RETURNS varchar(100)

WITH EXECUTE AS OWNER
--$!SQLVer Mar  3 2025 12:04PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Return varchar(100)
  DECLARE @day int
  DECLARE @hr int
  DECLARE @min int
  DECLARE @sec int
  
  DECLARE @IsNegative bit
  
  IF @ThisSec < 0 SET @IsNegative = 1
  
  SET @ThisSec = ABS(@ThisSec)
  
  SET @day = ROUND(@ThisSec / 86400, 0) 
  SET @ThisSec = @ThisSec - (86400 * @day)
  
  SET @hr = ROUND(@ThisSec / 3600, 0)
  SET @ThisSec = @ThisSec - (3600 * @hr)
  
  SET @min = ROUND(@ThisSec / 60, 0)
  SET @ThisSec = @ThisSec - (60 * @min)
  
  SET @sec = @ThisSec
  
  SET @Return = CAST(@hr AS varchar(10))
  IF @hr < 10 SET @Return = RIGHT('00' + CAST(@hr AS varchar(10)), 2)
  SET @Return = @Return +
      ':' + RIGHT('00' + CAST(@min AS varchar(10)), 2) + 
      ':' + RIGHT('00' + CAST(@sec AS varchar(10)), 2)
  IF @ForceDay = 1 OR @day > 0 SET @Return = CAST(@day AS varchar(10)) + 'd ' + @Return
 
  IF @IsNegative = 1 SET @Return = '-' + @Return
  
  RETURN @Return
END

GO


IF OBJECT_ID('[sqlver].[udfMakeNumericStrict]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfMakeNumericStrict]
END
GO

CREATE FUNCTION [sqlver].[udfMakeNumericStrict](
@Buf varchar(512)
)
RETURNS bigint
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udfSecureValue]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfSecureValue]
END
GO

CREATE FUNCTION [sqlver].[udfSecureValue] (
@KeyName sysname,
@CryptKey nvarchar(1024) = NULL
)
RETURNS nvarchar(4000)
--$!SQLVer Aug 18 2022 10:41AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @PlainValueBin varbinary(8000)
  DECLARE @PlainValue nvarchar(4000)

  IF @CryptKey IS NULL BEGIN
    SELECT
      --@CryptKey = ENCRYPTBYPASSPHRASE('sqlver', sv.SecureValue)
      @CryptKey =sv.SecureValue
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.id = '0'

  END

  DECLARE @SVID int

  SELECT @SVID = sv.id
  FROM
    sqlver.tblSecureValues sv
  WHERE
    sv.KeyName = @KeyName

  IF @SVID IS NULL BEGIN
    SET @PlainValueBin = NULL
  END
  ELSE BEGIN
    SELECT
      @PlainValueBin = DECRYPTBYPASSPHRASE(@CryptKey, sv.SecureValue)
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.ID = @SVID
  END

  SET @PlainValue = CAST(@PlainValueBin AS nvarchar(4000))

  RETURN @PlainValue

END

GO


IF OBJECT_ID('[sqlver].[udfRTRIMZeros]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfRTRIMZeros]
END
GO

CREATE FUNCTION [sqlver].[udfRTRIMZeros] (
@Num NUMERIC(38,12)
)
RETURNS varchar(100)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  RETURN replace(rtrim(replace(replace(rtrim(replace(CAST(@Num AS varchar(100)),'0', ' ')), ' ', '0'), '.', ' ')), ' ', '.')
END

GO


IF OBJECT_ID('[sqlver].[udfGenerateCLRRegisterSQL]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udfGenerateCLRRegisterSQL]
END
GO

CREATE FUNCTION [sqlver].[udfGenerateCLRRegisterSQL](

@AssemblyName sysname,
@FQFileName varchar(1024)
)
RETURNS varchar(MAX)
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[udftFindExec]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftFindExec]
END
GO

CREATE FUNCTION sqlver.udftFindExec(
  @Buf nvarchar(MAX)
)
RETURNS @tvResults TABLE (ExecProc sysname, StartPos int, ResultContext nvarchar(254))
--$!SQLVer Sep  3 2022  5:49AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Buf) = 1 BEGIN
    SET @ObjID = CAST(@Buf AS int)
  END
  ELSE IF LEN(@Buf) < 254 BEGIN
    SET @ObjID = OBJECT_ID(@Buf)
  END

  IF @ObjID IS NOT NULL BEGIN
    SET @Buf =  OBJECT_DEFINITION(@ObjID)
  END


  INSERT INTO @tvResults
  SELECT
    sqlver.udfCopyStrToWhite(SUBSTRING(@Buf, n.Number + 5, 254)) AS ExecProc,
    n.Number,
    SUBSTRING(@Buf, n.Number - 10, 254) AS ResultContext
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= LEN(@Buf + 'x') - 1 AND
    SUBSTRING(@Buf, n.Number, 5) = 'EXEC '

  RETURN
END

GO


IF OBJECT_ID('[sqlver].[udftFindTempTables]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftFindTempTables]
END
GO

CREATE FUNCTION [sqlver].[udftFindTempTables](
  @Buf nvarchar(MAX)
)
RETURNS @tvResults TABLE (TempTable sysname, StartPos int, ResultContext nvarchar(254))
--$!SQLVer Sep  3 2022  5:49AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Buf) = 1 BEGIN
    SET @ObjID = CAST(@Buf AS int)
  END
  ELSE IF LEN(@Buf) < 254 BEGIN
    SET @ObjID = OBJECT_ID(@Buf)
  END

  IF @ObjID IS NOT NULL BEGIN
    SET @Buf =  OBJECT_DEFINITION(@ObjID)
  END


  INSERT INTO @tvResults
  SELECT
    sqlver.udfCopyStrToNonIdent(SUBSTRING(@Buf, x.Number, 254)) AS TempTable,
    x.Number AS StartPos,
    SUBSTRING(@Buf, x.Number - 10, 254) AS ResultContext
  FROM
    (
    SELECT
      n.Number
    FROM
      sqlver.tblNumbers n
    WHERE
      n.Number <= LEN(@Buf) AND
      SUBSTRING(@Buf, n.Number, 1) = '#' AND
      ASCII(SUBSTRING(@Buf, n.Number - 1, 1)) IN (9, 10, 13, 32, 46, 59) AND
      NOT sqlver.udfIsInComment(n.Number, @Buf) = 1
    ) x
  WHERE
    SUBSTRING(@Buf, x.Number, 254) NOT LIKE '#[_][_][_]%'

  --Delete false positives
  DELETE FROM @tvResults
  WHERE
    sqlver.udfCopyStrPriorWord(OBJECT_DEFINITION(@ObjID), StartPos) IN ('PROCEDURE', 'FUNCTION', 'EXEC')
      --INDEX cannot be excluded, due to two-part names like CREATE INDEX #mytable.#myindex

  RETURN
END

GO


IF OBJECT_ID('[sqlver].[udftGetParsedValuesQ]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftGetParsedValuesQ]
END
GO

CREATE FUNCTION [sqlver].[udftGetParsedValuesQ](
  --Q = Quote.  Delimiters embedded inside a quoted string are ignored 
  --'abc,`def,ghi` results in "abc" and "def,ghi" (with @Quote = '`')
  @InputString nvarchar(MAX),
  @Delimiter nchar(1) = ',',
  @Quote nchar(1) = N'`',
  @StripQuote bit = 0
)
RETURNS @tvValues TABLE (
  [Value] nvarchar(MAX),
  [Index] int)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @Quote IS NULL BEGIN
    SET @Quote = N'`'
  END

  DECLARE @L int
  SET @L = LEN(@InputString)
  IF @L > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), NULL) BEGIN
    INSERT INTO @tvValues ([Value], [Index])
    VALUES ('Error in sqlver.udftGetParsedValuesQ:  String length (' + CAST(@L AS varchar(100)) + ') exceeds maximum number in sqlver.tblNumbers.', -1)
  END

   --New 5/23/2020:  respect backtick quote delimited strings like `hello` and
  --ignore embedded delimiters
  DECLARE @tvQStrs TABLE(seq int IDENTITY, StartPos int, EndPos int)

  IF CHARINDEX(@Quote, @InputString) > 0 BEGIN
    --there are backtick quotes, which indicate we are to ignore embedded delimiters
    INSERT INTO @tvQStrs (StartPos)
    SELECT
      n.Number
    FROM
      sqlver.tblNumbers N
    WHERE
      n.Number <= LEN(@InputString + 'x') - 1 AND
      SUBSTRING(@InputString, n.Number, 1) = @Quote

    UPDATE Q
    SET
      EndPos = Q2.StartPos
    FROM
      @tvQStrs Q
      JOIN @tvQStrs Q2 ON
        Q.Seq + 1 = Q2.Seq
    WHERE
      Q2.Seq % 2 = 0

    DELETE FROM @tvQStrs
    WHERE
      Seq % 2 = 0      

  END

  --Remove trailing delimiters
  WHILE RIGHT(@InputString,1) = @Delimiter AND
    NOT EXISTS(
      SELECT seq
      FROM @tvQStrs
      WHERE EndPos IS NULL) BEGIN
    SET @InputString = LEFT(@InputString, LEN(@InputString + 'x') - 1 - 1)
  END

  SET @InputString = @Delimiter + @InputString + @Delimiter

  UPDATE @tvQStrs SET 
    StartPos = StartPos + 1,
    EndPos = ISNULL(EndPos + 1, LEN(@InputString + 'x') - 1)
  WHERE
    EndPos IS NULL

  DECLARE @tvDelims TABLE(seq int IDENTITY, Pos int, NextPos int)
  INSERT INTO @tvDelims (Pos)
  SELECT
    n.Number
  FROM
    sqlver.tblNumbers n
    LEFT JOIN @tvQStrs Q ON
      n.Number > Q.StartPos AND n.Number <= Q.EndPos
  WHERE
    n.Number < LEN(@InputString + 'x') - 1 AND
    Q.Seq IS NULL AND
    SUBSTRING(@InputString, n.Number, 1) = @Delimiter


  UPDATE d
  SET
    NextPos = ISNULL(d2.Pos, LEN(@InputString + 'x') - 1)
  FROM
    @tvDelims d
    LEFT JOIN @tvDelims d2 ON 
     d.Seq + 1 = d2.Seq

  INSERT INTO @tvValues ([Value], [Index])
  SELECT
    CASE WHEN @StripQuote = 1 THEN
      REPLACE(
      SUBSTRING(
        @InputString,
        d.Pos + 1,
        d.NextPos - d.Pos - 1
      ), @Quote, '')
    ELSE
      SUBSTRING(
        @InputString,
        d.Pos + 1,
        d.NextPos - d.Pos - 1
      )
    END,

    d.Seq
  FROM 
    @tvDelims d
  WHERE
    d.NextPos IS NOT NULL

  RETURN
END

GO


IF OBJECT_ID('[sqlver].[udftGetCalendarMonths]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftGetCalendarMonths]
END
GO

CREATE FUNCTION [sqlver].[udftGetCalendarMonths] (
@StartDate datetime,
@EndDate datetime
)
RETURNS @Months TABLE
( 
  MonthIndex int,
  StartDate datetime,
  EndDate datetime
)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Truncate time portion of date to get to midnight
  SET @StartDate = CAST(CAST(@StartDate AS date) AS datetime)
  SET @EndDate = CAST(CAST(@EndDate AS date) AS datetime)

  INSERT INTO @Months (
    MonthIndex,
    StartDate,
    EndDate
    )
  SELECT
    n.Number,
    DATEADD(month, n.Number - 1, @StartDate),
    DATEADD(day, -1, DATEADD(month, n.Number, @StartDate))
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= DATEDIFF(month, @StartDate, @EndDate - 1)

  RETURN
END

GO


IF OBJECT_ID('[sqlver].[udftGetCalendarDays]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftGetCalendarDays]
END
GO

CREATE FUNCTION [sqlver].[udftGetCalendarDays] (
@StartDate datetime,
@EndDate datetime
)
RETURNS @Days TABLE
( 
  DayIndex int,
  [Date] datetime
)

WITH EXECUTE AS OWNER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Truncate time portion of date to get to midnight
  SET @StartDate = CAST(CAST(@StartDate AS date) AS datetime)
  SET @EndDate = CAST(CAST(@EndDate AS date) AS datetime)

  INSERT INTO @Days (
    DayIndex,
    [Date]
    )
  SELECT
    n.Number,
    DATEADD(day,  n.Number - 1, @StartDate)
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= DATEDIFF(day, @StartDate, @EndDate - 1)

  RETURN
END

GO


IF OBJECT_ID('[sqlver].[udftGetParamInfo]') IS NOT NULL BEGIN
  DROP FUNCTION [sqlver].[udftGetParamInfo]
END
GO

CREATE FUNCTION [sqlver].[udftGetParamInfo](
@ObjectSchema sysname = NULL,
@ObjectName sysname = NULL,
@SQL nvarchar(MAX) = NULL
)
RETURNS @tvParams TABLE (
    ParamID int PRIMARY KEY,
    RawDefinition varchar(MAX) NULL,
    ParamName sysname NULL,
    DataType sysname NULL,
    HasDefaultValue bit NULL,
    DefaultValue varchar(MAX) NULL
    )
--$!SQLVer Dec 10 2020 11:34AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @ObjectName IS NOT NULL BEGIN
    SET @SQL = OBJECT_DEFINITION(OBJECT_ID(@ObjectSchema + '.' + @ObjectName))
  END

  DECLARE @Buf nvarchar(MAX)
  DECLARE @P int

  SET @P = sqlver.udfFindInSQL('AS', @SQL, 0)
  IF @P > 0 BEGIN
    SET @Buf = LEFT(@SQL, @P - 1)
  END
  
  SET @P = sqlver.udfFindInSQL('WITH', @Buf, 0)
  IF @P > 0 BEGIN
    SET @Buf = LEFT(@Buf, @P - 1)    
  END

  SET @P = PATINDEX('%@%', @SQL)
  IF @P > 0 BEGIN
    SET @Buf = SUBSTRING(@Buf, @P, LEN(@Buf))
  END

  SET @Buf = sqlver.udfStripSQLComments(@Buf)
  
  IF @Buf IS NOT NULL BEGIN
    INSERT INTO @tvParams (
      ParamID,
      RawDefinition
    )
    SELECT
      udf.[Index],
      sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(udf.Value))
    FROM
      sqlver.udftGetParsedValues(@Buf, ',') udf

    UPDATE @tvParams
    SET 
      HasDefaultValue = CASE WHEN PATINDEX('%=%', RawDefinition) > 0 THEN 1 ELSE 0 END,
      ParamName = LEFT(RawDefinition, PATINDEX('% %', RawDefinition) - 1),
      DataType = sqlver.udfParseValue(RawDefinition, 2, ' ')
      
    UPDATE @tvParams
    SET DefaultValue = NULLIF(RTRIM(LTRIM(SUBSTRING(RawDefinition, PATINDEX('%=%', RawDefinition) + 1, LEN(RawDefinition)))), 'NULL')
    WHERE
      HasDefaultValue = 1
  END
      
  RETURN    
END

GO


IF OBJECT_ID('[sqlver].[sputilAuthy]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilAuthy]
END
GO

CREATE PROCEDURE [sqlver].[sputilAuthy]
@Email nvarchar(128) = NULL,
@Phone nvarchar(40) = NULL,
@CountryCode nvarchar(10) = NULL,
@Action varchar(40), --getuser, sendtoken, verifytoken
@AuthyAPIKey nvarchar(40) = '2b1eGaK3SXo7BkkfKGc7vfBylEeNJxM0',
@AuthyToken nvarchar(40) = NULL,
@AuthyUserID nvarchar(40) = NULL OUTPUT,
@AuthyMessage nvarchar(4000) = NULL OUTPUT,
@ErrorMessage nvarchar(4000) = NULL OUTPUT,
@Success bit = NULL OUTPUT,
@SuppressResultset bit = 0
--$!SQLVer May 12 2021  6:26PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Debug bit
  SET @Debug = 1

  DECLARE @Msg nvarchar(MAX)

  DECLARE @ThreadGUID uniqueidentifier
  SET @ThreadGUID = NEWID()

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Starting'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END


  SET @Action = NULLIF(RTRIM(@Action), '')
  SET @Email = NULLIF(RTRIM(@Email), '')
  SET @Phone = NULLIF(RTRIM(@Phone), '')
  SET @CountryCode = NULLIF(RTRIM(@CountryCode), '')
  SET @AuthyUserID = NULLIF(RTRIM(@AuthyUserID), '')
  SET @AuthyToken = NULLIF(RTRIM(@AuthyToken), '')

  IF @Action IS NULL OR @Action NOT IN ('getuser', 'sendtoken', 'verifytoken') BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Error:  Invalid @Action specified.  Must be one of:  getuser, sendtoken, verifytoken'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END

  IF @Action = 'getuser' AND @Email + @Phone + @CountryCode IS NULL BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Error: You must provide values for @Email, @Phone and @CountryCode'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END

  IF @Action = 'sendtoken' AND @AuthyUserID IS NULL BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Error: You must provide value for @AuthyUserID'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END


  IF @Action = 'verifytoken' AND @AuthyUserID + @AuthyToken IS NULL BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Error: You must provide value for @AuthyUserID and @AuthyToken'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END


  DECLARE @URL nvarchar(512)
  DECLARE @HTTPMethod nvarchar(40) = 'GET'

  DECLARE @Headers nvarchar(MAX) = 'X-Authy-API-Key: {APIKEY}'
  SET @Headers = REPLACE(@Headers, '{APIKEY}', @AuthyAPIKey)

  DECLARE @FormData nvarchar(MAX)

  IF @Action = 'getuser' BEGIN
    SET @URL = 'https://api.authy.com/protected/json/users/new'
    SET @HTTPMethod = 'POST'

    SET @FormData = 'user[email]={EMAIL}&user[cellphone]={CELLPHONE}&user[country_code]={COUNTRYCODE}'
    SET @FormData = REPLACE(REPLACE(REPLACE(@FormData,
      '{EMAIL}', sqlver.udfURLEncode(@Email)),
      '{CELLPHONE}', sqlver.udfURLEncode(@Phone)),
      '{COUNTRYCODE}', sqlver.udfURLEncode(@CountryCode))
  END

  ELSE IF @Action = 'sendtoken' BEGIN
    SET @URL = 'https://api.authy.com/protected/json/sms/{AUTHYID}'
    SET @URL = REPLACE(@URL, '{AUTHYID}', @AuthyUserID)

    SET @HTTPMethod = 'GET'
  END

  ELSE IF @Action = 'verifytoken' BEGIN
    SET @URL = 'https://api.authy.com/protected/json/verify/{TOKEN}/{AUTHYID}'
    SET @URL = REPLACE(@URL, '{TOKEN}', @AuthyToken)
    SET @URL = REPLACE(@URL, '{AUTHYID}', @AuthyUserID)

    SET @HTTPMethod = 'GET'
  END

  DECLARE @Buf varbinary(MAX)

  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @HTTPMethod = @HTTPMethod,
    @ContentType = 'application/x-www-form-urlencoded',
    @Headers = @Headers,
    @Cookies = NULL,
    @DataToSend = @FormData,
    @DataToSendBin = NULL,
    @UserAgent = 'curl/7.55.1',
    @ErrorMsg = @ErrorMessage OUTPUT,
    @ResponseBinary=@Buf OUTPUT

  DECLARE @JSON nvarchar(MAX)

  DECLARE @SuccessStr nvarchar(MAX)

  IF @ErrorMessage IS NULL BEGIN
    SET @JSON = CAST(@Buf AS varchar(MAX))
    SET @SuccessStr = JSON_VALUE (@JSON , '$.success')
    SET @Success = CASE WHEN @SuccessStr IN ('true', '1', 'ok') THEN 1 ELSE 0 END

    IF @Action = 'getuser' BEGIN
      SET @AuthyUserID = JSON_VALUE (@JSON , '$.user.id')
    END

    SET @AuthyMessage = JSON_VALUE (@JSON , '$.message') 
  END

  SET @Success = ISNULL(@Success, 0)

  IF ISNULL(@SuppressResultset, 0) = 0 BEGIN
    SELECT
      @Success AS Success,
      @AuthyUserID AS AuthyUserID,
      @SuccessStr AS AuthySuccess,
      @AuthyMessage AS AuthyMessage,
      @JSON AS ResponseJSON,
      @ErrorMessage AS HTTPErrorMessage,
      GETDATE() AS ResponseTime
  END

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.sputilAuthy: Finished'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END

END

GO


IF OBJECT_ID('[sqlver].[spgetSQLProcesses]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLProcesses]
END
GO

CREATE PROCEDURE sqlver.spgetSQLProcesses
@AllDBs bit = 0
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SELECT
    t.text,
    syspr.*
  FROM 
    sys.sysprocesses syspr
    CROSS APPLY sys.dm_exec_sql_text(syspr.sql_handle) t
  WHERE
    (@AllDBs = 1 OR syspr.dbid = DB_ID()) AND
    syspr.status <> 'sleeping' AND
    syspr.spid <> @@SPID AND
    syspr.lastwaittype NOT LIKE 'BROKER%'
  ORDER BY
    syspr.physical_io DESC
END

GO


IF OBJECT_ID('[sqlver].[spinsNumbers]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spinsNumbers]
END
GO

CREATE PROCEDURE sqlver.spinsNumbers
@MaxNumber bigint
--$!SQLVer Oct 25 2021  9:23AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  INSERT INTO sqlver.tblNumbers (Number)
  SELECT x.Number
  FROM
    (
    SELECT TOP (@MaxNumber) mx.MaxNumber + ROW_NUMBER() OVER (ORDER BY obj3.OBJECT_ID) AS Number
    FROM
      (
        SELECT MAX(Number) MaxNumber FROM sqlver.tblNumbers
      ) mx
      JOIN sys.objects obj1 ON 1=1
      JOIN sys.objects obj2 ON 1=1
      JOIN sys.objects obj3 ON 1=1
    ) x
  WHERE
    x.Number <= @MaxNumber
END

GO


IF OBJECT_ID('[sqlver].[spsysGrant]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysGrant]
END
GO

CREATE PROCEDURE [sqlver].[spsysGrant]
@SchemaName sysname = 'sqlver',
@GrantTo sysname = 'opsstream_sys'
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @SQL nvarchar(MAX)

  SET @SQL = 'GRANT EXEC ON sqlver.spinsSysRTLog TO sqlverLogger'

  SELECT @SQL = ISNULL(@SQL + NCHAR(13) + NCHAR(10), '') + 
    'GRANT ' +
    CASE obj.[type]
      WHEN 'U' THEN 'SELECT'
      WHEN 'V' THEN 'SELECT'
      WHEN 'TF' THEN 'SELECT'
      WHEN 'IF' THEN 'SELECT'
      ELSE 'EXEC'
    END +
    ' ON ' + 
    QUOTENAME(sch.name) + '.' + QUOTENAME(obj.name) + 
    ' TO ' + QUOTENAME(@GrantTo)
  FROM
    sys.objects obj
    JOIN sys.schemas sch ON
      obj.schema_id = sch.schema_id
  WHERE
    (@SchemaName IS NULL OR sch.name = @SchemaName) AND
    sch.name <> 'sys' AND

    obj.type IN (
      --'D ', --DEFAULT_CONSTRAINT
      --'F ', --FOREIGN_KEY_CONSTRAINT
      'FN', --SQL_SCALAR_FUNCTION
      'FS', --CLR_SCALAR_FUNCTION
      'IF', --SQL_INLINE_TABLE_VALUED_FUNCTION
      --'IT', --INTERNAL_TABLE
      'P ', --SQL_STORED_PROCEDURE
      'PC', --CLR_STORED_PROCEDURE
      --'PK', --PRIMARY_KEY_CONSTRAINT
      --'S ', --SYSTEM_TABLE
      --'SN', --SYNONYM
      --'SQ', --SERVICE_QUEUE
      'TF', --SQL_TABLE_VALUED_FUNCTION
      --'TR', --SQL_TRIGGER
      --'TT', --TYPE_TABLE
      --'U ', --USER_TABLE
      --'UQ', --UNIQUE_CONSTRAINT
      'V ' --VIEW
    )
      


  SELECT @SQL = ISNULL(@SQL + NCHAR(13) + NCHAR(10), '') + 
    'GRANT ' +
    CASE obj.[type]
      WHEN 'U' THEN 'SELECT'
      WHEN 'V' THEN 'SELECT'
      WHEN 'TF' THEN 'SELECT'
      WHEN 'IF' THEN 'SELECT'
      ELSE 'EXEC'
    END +
    ' ON ' + 
    QUOTENAME(sch.name) + '.' + QUOTENAME(syn.name) + 
    ' TO ' + QUOTENAME(@GrantTo)

  FROM
    sys.synonyms syn
    JOIN sys.schemas sch ON
      syn.schema_id = sch.schema_id
    JOIN sys.objects obj ON
      obj.object_id = OBJECT_ID(syn.base_object_name)
  WHERE
    (@SchemaName IS NULL OR sch.name = @SchemaName) AND
    sch.name <> 'sys' AND

    obj.type IN (
      --'D ', --DEFAULT_CONSTRAINT
      --'F ', --FOREIGN_KEY_CONSTRAINT
      'FN', --SQL_SCALAR_FUNCTION
      'FS', --CLR_SCALAR_FUNCTION
      'IF', --SQL_INLINE_TABLE_VALUED_FUNCTION
      --'IT', --INTERNAL_TABLE
      'P ', --SQL_STORED_PROCEDURE
      'PC', --CLR_STORED_PROCEDURE
      --'PK', --PRIMARY_KEY_CONSTRAINT
      --'S ', --SYSTEM_TABLE
      --'SN', --SYNONYM
      --'SQ', --SERVICE_QUEUE
      'TF', --SQL_TABLE_VALUED_FUNCTION
      --'TR', --SQL_TRIGGER
      --'TT', --TYPE_TABLE
      --'U ', --USER_TABLE
      --'UQ', --UNIQUE_CONSTRAINT
      'V ' --VIEW
    )

  EXEC sqlver.sputilPrintString @SQL

  EXEC(@SQL)

END

GO


IF OBJECT_ID('[sqlver].[sputilResizeImage]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilResizeImage]
END
GO

CREATE PROCEDURE [sqlver].[sputilResizeImage]
@OrigImage varbinary(MAX),
@Filename nvarchar(1024) = 'MyImage.jpg',
@ImageContentType varchar(254) = 'image/jpeg',
@TargetWidth int = 0,
@TargetHeight int = 0,
@URL nvarchar(1024) = 'http://localhost:24800/DoCLR',
@ResizedImage varbinary(MAX) OUT
--$!SQLVer Sep 28 2021 11:44AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  DECLARE @Buf varbinary(MAX)

  DECLARE @MultipartBoundary varchar(100)
  SET @MultipartBoundary = LOWER(LEFT(REPLACE(CAST(NEWID() AS varchar(100)), '-', ''), 16))   
  SET @MultipartBoundary = sqlver.udfLPad(@MultipartBoundary, '-', 40)

  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)

  DECLARE @Headers varchar(MAX)
  DECLARE @ContentType varchar(254)
  DECLARE @DataToSendBin varbinary(MAX)
  DECLARE @DataToSend varchar(MAX)
  
  SET @ImageContentType = COALESCE(@ImageContentType, 'application/octet-stream')

  SET @ContentType = 'multipart/form-data; boundary=' + @MultipartBoundary

  SET @Headers = 
    'Content-Length: {{$LENGTH}}' + @CRLF
                  
  SET @DataToSend =
    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="methodToCall"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF +
    'ResizeImage' +
    @CRLF +

    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="targetWidth"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF +
    CAST(ISNULL(@TargetWidth, 0) AS varchar(100)) +
    @CRLF +

    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="targetHeight"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF +
    CAST(ISNULL(@TargetHeight, 0) AS varchar(100)) +
    @CRLF +

    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="origImage"; filename="' +  @Filename + '"' + @CRLF +
    'Content-Type: ' + @ImageContentType + @CRLF +
    'Content-Transfer-Encoding: binary' + @CRLF +
     @CRLF


  SET @DataToSendBin =
    CAST(@DataToSend AS varbinary(MAX)) +
    @OrigImage +
    CAST(@CRLF +'--' + @MultipartBoundary + '--' + @CRLF AS varbinary(MAX))


  DECLARE @DataLen int
  SET @DataLen = DATALENGTH(@DataToSendBin)

  SET @DataToSend = NULL

  SET @Headers = REPLACE(@Headers, '{{$LENGTH}}', CAST(ISNULL(@DataLen, 0) AS varchar(100)))

  DECLARE @HTTPStatus int
  DECLARE @RedirURL nvarchar(1024)
  DECLARE @RXBuf varbinary(MAX)
  DECLARE @ErrorMsg nvarchar(MAX)

  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @HTTPMethod = 'POST',  
    @ContentType = @ContentType,
    @Cookies = NULL,
    @DataToSend = NULL,
    @DataToSendBin = @DataToSendBin,
    @Headers = @Headers,
    @User = NULL,
    @Password = NULL,
    @UserAgent = 'OpsStream SQL',
    @AllowOldTLS = 0,
    @SSLProtocol = NULL,
    @HTTPStatus = @HTTPStatus OUTPUT,
    @HTTPStatusText = NULL,
    @RedirURL = @RedirURL OUTPUT,  
    @ResponseBinary = @ResizedImage OUTPUT,
    @ErrorMsg = @ErrorMsg OUTPUT  

END

GO


IF OBJECT_ID('[sqlver].[spinsSysRTLog]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spinsSysRTLog]
END
GO

CREATE PROCEDURE [sqlver].[spinsSysRTLog]
@Msg nvarchar(MAX) = NULL,
@MsgXML xml = NULL,
@ThreadGUID uniqueidentifier = NULL,
@SPID int = NULL,
@PersistAfterRollback bit = 0,
@PrintToo bit = 0

WITH EXECUTE AS OWNER
--$!SQLVer Nov 13 2024 10:16AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
 
  --Comment out the following line to enable the Persist After Rollback
  --functionality.  But first test the connection string set below.
  --SET @PersistAfterRollback = 0


  --If database is in SINGLE_USER mode, @PersistAfterRollback=1 cannot be used because by definition that requires an additional database connection
  /*
  --Enable this check if you want...but it does add a bit of overhead due to having to query sys.databases
  IF @PersistAfterRollback = 1 AND (SELECT sysdb.user_access_desc FROM sys.databases sysdb WHERE sysdb.name = DB_NAME()) = 'SINGLE_USER' BEGIN
    PRINT 'sqlver.spinsSysRTLog: Forcing @PersistAfterRollback=0 because database is in SINGLE_USER mode'
    SET @PersistAfterRollback = 0
  END
  */

  DECLARE @ConnStr varchar(MAX)
  --connection string for ADO to use to access the database

  --Replace NULL with your actual connection string
  SET @ConnStr = NULL

  IF @ConnStr IS NULL BEGIN
    --Fallback connection string.
    --This connection string is just a guess.  You should specify it above.
    --For example you may instead want to connect with something like:
    --  Server=localhost,1433 
    --SET @ConnStr = 'Provider=SQLNCLI11; Server=' + CONVERT(sysname, SERVERPROPERTY('servername')) + '; Database=' + DB_NAME() + '; Uid=sqlverLogger; Pwd=sqlverLoggerPW;'
    SET @ConnStr = 'Provider=MSOLEDBSQL; Server=' + CONVERT(sysname, SERVERPROPERTY('servername')) + '; Database=' + DB_NAME() + '; Uid=sqlverLogger; Pwd=sqlverLoggerPW;'
  END
  --SET @ConnStr = 'Provider=SQLNCLI11; Server=' + @@servername + '; Database=' + DB_NAME() + '; Uid=sqlverLogger; Pwd=sqlverLoggerPW;'

  --Added 2/13/2013.  Since this procedure is used for logging messages, including errors, it is possible
  --that this routine may be called in a TRY / CATCH block when there is a doomed transaction.  In such a
  --case this insert would fail.  Since the transaction is doomed anyway, I think that rolling it back here
  --(instead of explicitly within each CATCH block) is cleaner.
  IF XACT_STATE() = -1 BEGIN
    ROLLBACK TRAN
  END  
  
  SET @SPID = COALESCE(@SPID, @@SPID)
  
  IF @PersistAfterRollback = 0 BEGIN
    INSERT INTO sqlver.tblSysRTLog
      (DateLogged, Msg, MsgXML, ThreadGUID, SPID)
    VALUES
      (GETDATE(), @Msg, @MsgXML, @ThreadGUID, @SPID)

    IF @PrintToo = 1 BEGIN
      PRINT CAST(GETDATE() AS varchar(100)) + '  ' + @Msg
    END
  END
  ELSE BEGIN
    /*
    This procedure is designed to allow a caller to provide a message that will be written to an error log table,
    and allow the caller to call it within a transaction.  The provided message will be persisted to the
    error log table even if the transaction is rolled back.
    
    To accomplish this, this procedure utilizes ADO to establish a second database connection (outside
    the transaction context) back into the database to call the dbo.spLogError procedure.
    */

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
    
    DECLARE @LastCommand varchar(128)    
      
    BEGIN TRY
      SET @LastCommand = 'sp_OACreate ''ADODB.Connection'''
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
      SET @LastCommand = 'sp_OAMethod ''Open'''       
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
      SET @LastCommand = 'sp_OAMethod ''Execute'''    
      EXEC @LastResultCode = sp_OAMethod @ObjCn, 'Execute', @ObjRS OUTPUT, @SQLCommand, @ExecOptions
      IF @LastResultCode <> 0 BEGIN
        EXEC sp_OAGetErrorInfo @ObjCn, @ErrSource OUTPUT, @ErrMsg OUTPUT 
      END                
    END
      
    IF @ObjRS IS NOT NULL BEGIN
      BEGIN TRY
        SET @LastCommand = 'sp_OADestroy @ObjRS'
        EXEC sp_OADestroy @ObjRS 
      END TRY
      BEGIN CATCH
        --not much we can do...
        SET @LastResultCode = 0
      END CATCH
    END
      
    IF @ObjCn IS NOT NULL BEGIN
      BEGIN TRY
        SET @LastCommand = 'sp_OADestroy @ObjCn'
        EXEC sp_OADestroy @ObjCn
      END TRY
      BEGIN CATCH
        --not much we can do...
        SET @LastResultCode = 0
      END CATCH
    END    
      
    IF ((@LastResultCode <> 0) OR (@ErrorMessage IS NOT NULL)) BEGIN
      SET @ErrorMessage = CONCAT(
        'Error in sqlver.spinsSysRTLog:',
        ISNULL(' @ErrMsg=' + @ErrMsg, '') ,
        ISNULL(' @LastErrorCode=' + CAST(@LastResultCode AS varchar(100)), ''),
        ISNULL(' @ErrorMessage=' + @ErrorMessage, ''),
        ISNULL(' @LastCommand=' + @LastCommand, ''),
        ' while trying to log: ''',
        ISNULL(@Msg, '') + '''',
        ' @ThreadGUID=' + CAST(@ThreadGUID AS varchar(100))
        )
      RAISERROR(@ErrorMessage, 16, 1)
    END
  
  END
  
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaFKEnable]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaFKEnable]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaFKEnable]
@Enable bit = 1
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Msg nvarchar(MAX)

  SET @Msg = 'sqlver.spsysSchemaFKEnable:  Disabling all foregin key constraints in database ' + DB_NAME()

  IF @Enable = 1 BEGIN
    SET @Msg = REPLACE(@Msg, 'Disabling', 'Enabling')
  END

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT 
    '[' + sch.name + '].[' + t.name + ']' TableName
  FROM
    sys.tables t
    JOIN sys.schemas sch ON
      t.schema_id = sch.schema_id
  ORDER BY
    sch.name,
    t.name

  DECLARE @ThisTablename sysname
  DECLARE @SQL nvarchar(MAX)

  OPEN curThis
  FETCH curThis INTO @ThisTableName
  WHILE @@FETCH_STATUS = 0 BEGIN

    SET @SQL = 'ALTER TABLE ' + @ThisTableName + ' NOCHECK CONSTRAINT all'

    IF @Enable = 1 BEGIN
      SET @SQL = REPLACE(@SQL, 'NOCHECK', 'CHECK')
    END

    BEGIN TRY
      PRINT @SQL
      EXEC (@SQL)
      --PRINT @ThisTableName
    END TRY
    BEGIN CATCH
      PRINT '***Error on ' + @ThisTableName + ': ' + ERROR_MESSAGE()
    END CATCH

    FETCH curThis INTO @ThisTableName
  END
  CLOSE curThis
  DEALLOCATE curThis

END

GO


IF OBJECT_ID('[sqlver].[sputilFormatError]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilFormatError]
END
GO

CREATE PROCEDURE [sqlver].[sputilFormatError]
@MessageRaw nvarchar(MAX),
@MessagePretty nvarchar(MAX) = NULL OUTPUT,
@Result nvarchar(MAX) = NULL OUTPUT,
@SuppressResultset bit = 1
--$!SQLVer Mar 13 2022  7:36PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --Crude string parsing to try to make sense out of certain complicated error messages

  BEGIN TRY

    DECLARE @P_DupKeyRow int
    SET @P_DupKeyRow = PATINDEX('%Cannot insert duplicate key row%', @MessageRaw)

    IF @P_DupKeyRow = 0 BEGIN
      SET @MessagePretty = @MessageRaw
    END
    ELSE BEGIN

      DECLARE @Buf_IndexName nvarchar(MAX)
      DECLARE @Buf_QDMID nvarchar(MAX)
      SET @Buf_IndexName = sqlver.udfParseValue(@MessageRaw, 2, CHAR(39))

      SET @Buf_QDMID = @Buf_IndexName
      DECLARE @QDMID int
      SET @QDMID = CAST(RIGHT(@Buf_QDMID, PATINDEX('%[_]%', REVERSE(@Buf_QDMID)) - 1) AS int)

      DECLARE @FieldName sysname

      DECLARE @QuestCodeErr varchar(40)
  
      DECLARE @RefQDMID int

      SELECT
        @FieldName = qdm.FieldName,
        @QuestCodeErr = qd.QuestCode,
        @RefQDMID = qdm.Referenced_QuestDefMetaID
      FROM
        opsstream.tblQuestDefMeta qdm
        JOIN opsstream.tblQuestDefs qd ON
          qdm.QuestDefID = qd.QuestDefID
      WHERE
        qdm.QuestDefMetaID = @QDMID

      DECLARE @RefQuestCodeErr varchar(40)
      DECLARE @RefFieldName sysname

      SELECT
        @RefQuestCodeErr = qd.QuestCode,
        @RefFieldName = qdm.FieldName
      FROM
        opsstream.tblQuestDefMeta qdm
        JOIN opsstream.tblQuestDefs qd ON
          qdm.QuestDefID = qd.QuestDefID
      WHERE
        qdm.QuestDefMetaID = @RefQDMID


      DECLARE @Buf_BadVals nvarchar(MAX)
      SET @Buf_BadVals = sqlver.udfParseValue(@MessageRaw, 2, '(')

      SET @Buf_BadVals = REPLACE(LEFT(@Buf_BadVals, PATINDEX('%)%', @Buf_BadVals) - 1), ' ', '')

      DECLARE @BadContextQuestID int
      DECLARE @BadVal varchar(1024)

      SET @BadVal = sqlver.udfParseValue(@Buf_BadVals, 2, ',')

      IF @BadVal IS NOT NULL BEGIN
        SET @BadContextQuestID = sqlver.udfParseValue(@Buf_BadVals, 1, ',')
      END
      ELSE BEGIN
        SET @BadVal = sqlver.udfParseValue(@Buf_BadVals, 1, ',')
      END


      IF @RefQuestCodeErr IS NOT NULL BEGIN
        DECLARE @SQLBadVal nvarchar(MAX)

        SET @SQLBadVal = 'SELECT @BadVal = ' + @RefFieldName + ' FROM opsstream.vwQXD_' + @RefQuestCodeErr + ' WHERE QuestID = ' + @BadVal
        EXEC sp_executesql @stmt = @SQLBadVal, @params = N'@BadVal nvarchar(1024) OUTPUT', @BadVal = @BadVal OUTPUT
      END


      SET @MessagePretty =
        'Field ' + @QuestCodeErr + '.' + @FieldName + ' must be unique' + ', but ' +
        'value "' + @BadVal + '" is present in multiple input rows.'

      --@MessagePretty now contains a more friendly message in some cases
      SET @Result = @MessagePretty
        
    END
  END TRY
  BEGIN CATCH
    SET @Result = @MessageRaw
  END CATCH

  --EXEC sqlver.sputilPrintString @Result

  IF @SuppressResultset = 0 BEGIN
    SELECT @Result AS Result
  END
END

GO


IF OBJECT_ID('[sqlver].[spLastModified]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spLastModified]
END
GO

CREATE PROCEDURE [sqlver].[spLastModified]
--$!SQLVer Oct 23 2024  4:58PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --NOTE:  intentionally returns the EARLIEST date for a given hash.
  --In other words:  if an object was changed, and that change was reverted, the
  --date would remain the original date (prior to the change and the reversion)

  SELECT
    x.SchemaName,
    x.ObjectName,
    x.ObjectType,
    x.CurrentHash,
    x.EventDate AS DateLastModified
  FROM 
    (
    SELECT
      om.SchemaName,
      om.ObjectName,
      om.ObjectType,
      om.CurrentHash,
      schl.EventDate,
      ROW_NUMBER() OVER (PARTITION BY om.SchemaName, om.ObjectName ORDER BY schl.SchemaLogID) AS Seq
    FROM
      sqlver.tblSchemaManifest om
      JOIN sqlver.tblSchemaLog schl ON
        om.SchemaName = schl.SchemaName AND
        om.ObjectName = schl.ObjectName AND
        om.CurrentHash = schl.Hash
    ) x
  WHERE
    x.Seq = 1
  ORDER BY
    x.EventDate DESC
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaUpdateColumnDefs]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaUpdateColumnDefs]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaUpdateColumnDefs]
@ObjectName sysname,
@SchemaName sysname,
@PerformUpdate bit = 1,
@Print bit = 0

WITH EXECUTE AS OWNER
--$!SQLVer Dec  9 2020  3:36PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @SQL varchar(MAX)
  DECLARE @ColumnBlock nvarchar(MAX)

  
  DECLARE @IsProcedure bit
  SET @IsProcedure = 0
  
  SELECT @IsProcedure = 1
  FROM
    sys.objects so
  WHERE
    so.object_id = OBJECT_ID(@SchemaName + '.' + @ObjectName) AND
    so.type_desc = 'SQL_STORED_PROCEDURE'
    
  DECLARE @IsSelectable bit
  SET @IsSelectable = 0
  
  SELECT @IsSelectable = 1
  FROM
    sys.objects so
  WHERE
    so.object_id = OBJECT_ID(@SchemaName + '.' + @ObjectName) AND
    so.type_desc IN (
      'USER_TABLE',
      'VIEW'--,
--      'SQL_TABLE_VALUED_FUNCTION'
    )
        

  IF @IsProcedure = 1 BEGIN        
    DECLARE @Params varchar(MAX)
    SET @Params = ''
        
    SELECT @Params = @Params + par.ParamName + '=NULL,'
    FROM sqlver.udftGetParamInfo(@SchemaName, @ObjectName, NULL) par
    WHERE
      par.HasDefaultValue = 0
      
    IF RIGHT(@Params,1) = ',' BEGIN
      SET @Params = LEFT(@Params, LEN(@Params + 'x') - 1 - 1)
    END    
          
    SET @SQL = 'EXEC ' + @SchemaName + '.' + @ObjectName + ' ' + @Params
  END
  ELSE IF @IsSelectable = 1 BEGIN
    SET @SQL = 'SELECT * FROM ' + @SchemaName + '.' + @ObjectName + ' WHERE 1=0'
  END


  IF @SQL IS NOT NULL BEGIN
    EXEC sqlver.sputilGetColumnBlock
      @SQLStatement = @SQL,
      @Format = 'coldef',
      @ColumnBlock = @ColumnBlock OUTPUT

    IF @PerformUpdate = 1 BEGIN    
      UPDATE schm
      SET ColumnDefinition = @ColumnBlock
      FROM
        sqlver.tblSchemaManifest schm
      WHERE
        schm.SchemaName = @SchemaName AND
        schm.ObjectName = @ObjectName
    END
    
    IF @Print = 1 BEGIN
      PRINT @ColumnBlock
    END            
  END      
END

GO


IF OBJECT_ID('[sqlver].[sputilGetColumnBlock]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetColumnBlock]
END
GO

CREATE PROCEDURE [sqlver].[sputilGetColumnBlock]
@SQLStatement varchar(MAX),
@Format varchar(40) = 'collist',
@ColPrefix varchar(40) = NULL,
@VarPrefix varchar(40) = NULL,
@TempTableName sysname = NULL,
@ColumnBlock varchar(MAX) = NULL OUTPUT,
@IncludeQuotes bit = 0,
@InhibitResultset bit = 1,
@InhibitPrint bit = 0

WITH EXECUTE AS CALLER
--$!SQLVer Apr 20 2021  7:01AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  SET @Format = LOWER(@Format)

  PRINT 'Other choices for @Format: '
  PRINT '  vardef coldef setvar setcol temptable varlist collist'
  PRINT ''
  PRINT 'Optional paramters: '
  PRINT '@SQLStatement varchar(MAX),
@Format varchar(40) = ''collist'',
@ColPrefix varchar(40) = NULL,
@VarPrefix varchar(40) = NULL,
@TempTableName sysname = NULL,
@ColumnBlock varchar(MAX) = NULL OUTPUT,
@IncludeQuotes bit = 0,
@InhibitResultset bit = 1,
@InhibitPrint bit = 0'
  PRINT ''
  PRINT ''

  IF @Format NOT IN ('vardef', 'coldef', 'setvar', 'setcol', 'temptable', 'varlist', 'collist') BEGIN
    RAISERROR('Error in sqlver.sputilGetColumnBlocks: Parameter @Format has an invald value.  Value must be one of the following: (vardef, coldef, setvar, setcol, varlist, collist, temptable).', 16, 1)
  END
  
  IF @Format = 'temptable' AND @TempTableName IS NULL BEGIN
    RAISERROR('Error in sqlver.sputilGetColumnBlocks: No value specified for paramter @TempTableName, and @Format was set to temptable.', 16, 1)
  END

  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)  

  DECLARE @tvColInfo TABLE (
    ORDINAL_POSITION int,
    COLUMN_NAME sysname,
    DATA_TYPE sysname,
    CHARACTER_MAXIMUM_LENGTH int,
    NUMERIC_PRECISION tinyint,
    NUMERIC_SCALE int,
    IS_NULLABLE varchar(3))

  DECLARE @ORDINAL_POSITION int
  DECLARE @COLUMN_NAME sysname
  DECLARE @DATA_TYPE sysname
  DECLARE @CHARACTER_MAXIMUM_LENGTH int
  DECLARE @NUMERIC_PRECISION tinyint
  DECLARE @NUMERIC_SCALE int
  DECLARE @IS_NULLABLE varchar(3)    

  IF OBJECT_ID(@SQLStatement) IS NOT NULL BEGIN
    --for convenience:  if a tablename (or view, or table-valued function) is passed in, assume SELECT *'
    SET @SQLStatement = 'SELECT * FROM ' + @SQLStatement + ' WHERE 1=0'
  END
  ELSE IF OBJECT_ID('opsstream.tblQuestDefs') IS NOT NULL BEGIN
    IF EXISTS (SELECT * FROM opsstream.tblQuestDefs WHERE QuestCode = @SQLStatement) BEGIN
      SELECT @SQLStatement = 'SELECT * FROM opsstream.tblQXD_' + @SQLStatement + ' WHERE 1=0'
    END
  END

  INSERT INTO @tvColInfo    
  EXEC sqlver.sputilGetColumnInfo @SQL = @SQLStatement

  DECLARE curCols CURSOR LOCAL STATIC FOR
  SELECT * FROM @tvColInfo

  OPEN curCols

  FETCH curCols INTO
    @ORDINAL_POSITION,
    @COLUMN_NAME,
    @DATA_TYPE,
    @CHARACTER_MAXIMUM_LENGTH,
    @NUMERIC_PRECISION,
    @NUMERIC_SCALE,
    @IS_NULLABLE

  SET @ColumnBlock = ''

  WHILE @@FETCH_STATUS = 0 BEGIN
    SET @ColumnBlock = @ColumnBlock + 
      CASE @Format
        WHEN 'vardef' THEN
          ISNULL(@VarPrefix, '') + '@' + @COLUMN_NAME + ' ' + @DATA_TYPE +
          CASE 
            WHEN @DATA_TYPE = 'numeric' THEN '(' + CAST(@NUMERIC_PRECISION AS varchar(100)) + ', ' + CAST(@NUMERIC_SCALE AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH > 0 THEN '(' + CAST(@CHARACTER_MAXIMUM_LENGTH AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)'          
            ELSE ''
          END
        WHEN 'coldef' THEN
          @COLUMN_NAME + ' ' + @DATA_TYPE +
          CASE 
            WHEN @DATA_TYPE = 'numeric' THEN '(' + CAST(@NUMERIC_PRECISION AS varchar(100)) + ', ' + CAST(@NUMERIC_SCALE AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH > 0 THEN '(' + CAST(@CHARACTER_MAXIMUM_LENGTH AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)'               
            ELSE ''
          END
        WHEN 'setvar' THEN 
          '@' + @COLUMN_NAME + ' = ' + ISNULL(@ColPrefix, '') + @COLUMN_NAME     
        WHEN 'setcol' THEN
          ISNULL(@ColPrefix, '') + @COLUMN_NAME + ' = @' + @COLUMN_NAME     
        WHEN 'temptable' THEN
          @COLUMN_NAME + ' ' + @DATA_TYPE +
          CASE 
            WHEN @DATA_TYPE = 'numeric' THEN '(' + CAST(@NUMERIC_PRECISION AS varchar(100)) + ', ' + CAST(@NUMERIC_SCALE AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH > 0 THEN '(' + CAST(@CHARACTER_MAXIMUM_LENGTH AS varchar(100)) + ')'
            WHEN @CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)'               
            ELSE ''
          END
        WHEN 'varlist' THEN
          '@' + @COLUMN_NAME 
        WHEN 'collist' THEN
          CASE WHEN @IncludeQuotes = 1 THEN '[' ELSE '' END +
          ISNULL(@ColPrefix, '') + @COLUMN_NAME +
          CASE WHEN @IncludeQuotes = 1 THEN ']' ELSE '' END           
      END + CASE WHEN @Format = 'vardef' AND @VarPrefix = 'DECLARE ' THEN '' ELSE ',' END + @CRLF    
          
      
    FETCH curCols INTO
      @ORDINAL_POSITION,
      @COLUMN_NAME,
      @DATA_TYPE,
      @CHARACTER_MAXIMUM_LENGTH,
      @NUMERIC_PRECISION,
      @NUMERIC_SCALE,
      @IS_NULLABLE    
      
  END

  CLOSE curCols
  
  IF LEFT(REVERSE(@ColumnBlock), 3) = CHAR(10) + CHAR(13) + ',' BEGIN
    SET @ColumnBlock = LEFT(@ColumnBlock, LEN(@ColumnBlock + 'x') - 1 - LEN(',' + @CRLF + 'x') + 1)
  END

  IF @Format = 'temptable' BEGIN
    SET @ColumnBlock = 'CREATE TABLE #' + @TempTableName + ' (' + @CRLF + @ColumnBlock + ')' + @CRLF 
  END
  
  IF ISNULL(@InhibitResultset, 0) = 0 BEGIN
    SELECT @ColumnBlock AS ColumnBlock
  END
  
  IF ISNULL(@InhibitPrint, 0) = 0 BEGIN
   EXEC sqlver.sputilPrintString @ColumnBlock
  END

END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaShowDiffs]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaShowDiffs]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaShowDiffs]
@ObjectName sysname = NULL,
@SchemaName sysname = NULL,
@MaxDays int = 30, --Include objects that have changed in the past @MaxDays
@StartDate datetime = NULL, --Include objects that have changed since @StartDate
@CompareOlderThanDays int = NULL, --Compare objects with version older than this.  (NULL for most recent prior version)
@CompareOlderStartDate datetime = NULL --Compare objects with version older than this.  (NULL for most recent prior version)
--$!SQLVer Dec  4 2020  1:39PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN  
  SET NOCOUNT ON
  
  IF @StartDate IS NOT NULL BEGIN
    SET @MaxDays = DATEDIFF(day, @StartDate, GETDATE())
  END
  ELSE BEGIN
    SET @StartDate = CAST(ROUND(CAST(DATEADD(day, -1 * @MaxDays, GETDATE()) AS float), 0) AS datetime)
  END

  IF @CompareOlderStartDate IS NOT NULL OR @CompareOlderThanDays IS NOT NULL BEGIN
    IF @CompareOlderStartDate IS NOT NULL BEGIN
      SET @CompareOlderThanDays  = DATEDIFF(day, @CompareOlderStartDate, GETDATE())    
    END
    ELSE BEGIN
      SET @CompareOlderStartDate = CAST(ROUND(CAST(DATEADD(day, -1 * @CompareOlderThanDays, GETDATE()) AS float), 0) AS datetime)
    END
  END

  DECLARE @SQL1 nvarchar(MAX)
  DECLARE @SQL2 nvarchar(MAX)
  
  ;
  WITH cteSchl (
    SchemaName,
    ObjectName,
    Seq,
    SqlCommand,
    EventDate,
    SchemaLogID,
    LoginName,
    UserName,
    [Hash]
    )
  AS
  (  
  SELECT
    schl.SchemaName,
    schl.ObjectName,
    ROW_NUMBER() OVER (PARTITION BY schl.SchemaName, schl.ObjectName ORDER BY schl.SchemaLogID DESC) AS Seq,
    schl.SqlCommand,
    schl.EventDate,
    schl.SchemaLogID,
    schl.LoginName,
    u.UserName,
    schl.Hash    
  FROM
    sqlver.tblSchemaLog schl
    LEFT JOIN opsstream.tblUsers u ON
      schl.UserID = u.UserID    
  WHERE
    ((@SchemaName IS NULL) OR (schl.SchemaName = @SchemaName)) AND
    ((@ObjectName IS NULL) OR (schl.ObjectName = @ObjectName))
  )  
  
  SELECT
    a.SchemaName,
    a.ObjectName,
    a.EventDate AS LastEditDate,
    a.LoginName AS LastEditLogin,
    a.UserName AS LastEditOSUser,
    a.SchemaLogID AS LastEditID,

    b.EventDate AS PrevEditDate,
    b.LoginName AS PrevEditLogin,
    b.UserName AS PrevEditOSUser,
    b.SchemaLogID AS PrevEditID    
    
    INTO #Changes
  FROM 
    cteSchl a
    
    LEFT JOIN (
      SELECT
        schl.ObjectName,
        schl.SchemaName,
        MAX(schl.SchemaLogID) AS MaxID
      FROM
        sqlver.tblSchemaLog schl
      WHERE
        schl.EventDate < @CompareOlderStartDate 
      GROUP BY
        schl.ObjectName,
        schl.SchemaName) cmax ON
      a.SchemaName = cmax.SchemaName AND
      a.ObjectName = cmax.ObjectName
      
    JOIN cteSchl b ON
      a.SchemaName = b.SchemaName AND
      a.ObjectName = b.ObjectName AND
      a.Seq = 1 AND
      ((@CompareOlderStartDate IS NULL AND b.Seq = 2) OR
       (b.SchemaLogID = cmax.MaxID)) AND
      b.SchemaLogID < a.SchemaLogID 
  WHERE
    a.EventDate >= @StartDate

  SELECT TOP 1
    @SQL2 = schl1.SqlCommand,
    @SQL1 = schl2.SqlCommand
  FROM
    #Changes ch
    JOIN sqlver.tblSchemaLog schl1 ON
      ch.LastEditID = schl1.SchemaLogID
    JOIN sqlver.tblSchemaLog schl2 ON
      ch.PrevEditID = schl2.SchemaLogID
  ORDER BY
    ch.LastEditDate DESC      
            
  SELECT * 
  FROM #Changes ch
  ORDER BY
    ch.LastEditDate DESC    
  
  SELECT * FROM sqlver.udftGetDiffs_CLR(@SQL1, @SQL2)  
END

GO


IF OBJECT_ID('[sqlver].[sputilGetHTTP]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetHTTP]
END
GO

CREATE PROCEDURE [sqlver].[sputilGetHTTP]
  @URL nvarchar(MAX),
    --URL to retrieve data from
  @HTTPMethod nvarchar(40) = 'GET',
    --can be either GET or POST
  @ContentType nvarchar(254)= 'text/html' OUTPUT,
    --set to 'application/x-www-form-urlencoded' for POST, etc.  
    --If provided in the response headers, the will be set to the Content-Type value in the response
  --@Cookies nvarchar(MAX) OUTPUT,
    --string containing name=value,name=value list of cookies and values
  --@DataToSend nvarchar(MAX), 
    --data to post, if @HTTPMethod = 'POST'
  --@DataToSendBin varbinary(MAX),
    --data to post (binary)...if @DataToSend is not provided
  --@Headers nvarchar(MAX) OUTPUT,
    --Headers to include with the request / headers returned with the response
    --CRLF terminated list of Name: Value strings
  @User nvarchar(512) = NULL,
    --If provided, use this value for the HTTP authentication user name
  @Password nvarchar(512) = NULL,
    --If provided, use this value for the HTTP authentication password        
  @UserAgent nvarchar(512) = 'SQLCLR',
    --If provided, use this value for the HTTP UserAgent header           
  @HTTPStatus int = NULL OUTPUT,
    --HTTP Status Code (200=OK, 404=Not Found, etc.)
  @HTTPStatusText nvarchar(4000) = NULL OUTPUT,  
    --HTTP status code description
  @RedirURL nvarchar(4000) = NULL OUTPUT,
    --Redirect URL
  @ResponseBinary varbinary(MAX) OUTPUT,
    --Full binary data returned by remote HTTP server
        
  @AutoFollowRedir bit = 1,
    --If response indicates a redirect, re-initate an HTTP request to that @RedirURL
        
  @Filename nvarchar(MAX) = NULL OUTPUT,
    --If provided in the response headers, the filename from the Content-Disposition value
  @LastModified nvarchar(MAX) = NULL OUTPUT,
    --If provided in the response headers, the Last-Modified value
  @LastModifiedDate datetime = NULL OUTPUT,
    --If provided in the response headers, the Last-Modified value cast as a datetime.
    --Does not perform any timezone offset calculations (i.e. usually GMT)    

  @ReturnHeaders bit = 0,
    --If set, any response headers are returned in a resultset

  @URLRoot nvarchar(MAX) = NULL
    --absolute URL to prepend to @RedirURL if needed  
        
  --@ErrorMsg nvarchar(MAX) OUTPUT
    --NULL unless an error message was encountered
--$!SQLVer Mar  8 2025 10:03PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  /*
  Simplified procedure to initiate an HTTP request.
      
  Does not support @Cookies, @DataToSend, @DataToSendBin, or @Headers
  If these are needed, call sqlver.sputilGetHTTP_CLR directly.
      
  (SQL does not allow us to assign default values to long paramaters such as varchar(MAX))
  */      

  DECLARE @Headers nvarchar(MAX)
  DECLARE @Header varchar(MAX)
  DECLARE @Cookies nvarchar(MAX)
  DECLARE @AllowOldTLS bit
 
  SET @AllowOldTLS = 0
        
  DECLARE @ErrorMessage nvarchar(MAX)
      
  DECLARE @tvPV TABLE(Id int, Value nvarchar(MAX))

    
       
  DECLARE @Done bit
  SET @Done = 0
        
  WHILE @Done = 0 BEGIN
      
    SET @RedirURL = NULL
    SET @Headers = NULL
    DELETE FROM @tvPV
                   
    EXEC sqlver.sputilGetHTTP_CLR
      @URL = @URL,
      @HTTPMethod = @HTTPMethod,
      @ContentType = @ContentType,
            
      @Cookies = @Cookies OUTPUT,
      @DataToSend = NULL,
      @DataToSendBin = NULL,
      @Headers = @Headers OUTPUT,
            
      @User = @User,
      @Password = @Password,
      @UserAgent = @UserAgent,
            
      @HTTPStatus = @HTTPStatus OUTPUT,
      @HTTPStatusText = @HTTPStatusText OUTPUT,
      @RedirURL = @RedirURL OUTPUT,  
      @ResponseBinary = @ResponseBinary OUTPUT,
            
      @ErrorMsg = @ErrorMessage OUTPUT

    IF NULLIF(RTRIM(@ErrorMessage), '') IS NOT NULL BEGIN
      RAISERROR('Error in sqlver.sputilGetHTTP: %s', 16, 1, @ErrorMessage)
    END        

    IF @HTTPStatus = 500 BEGIN
      PRINT 'sqlver.sputilGetHTTP: HTTPStatus = 500'
      PRINT CAST(@ResponseBinary AS varchar(MAX))
    END  
        
    INSERT INTO @tvPV (Id, Value)
    SELECT
      [Index],
      Value
    FROM
      sqlver.udftGetParsedValues(@Headers, CHAR(10))
          
          
    SELECT 
      @Header = sqlver.udfRTRIMSuper(pv.Value)
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE 'Content-Disposition:%'
        
    SET @Filename = sqlver.udfParseValue(sqlver.udfParseValue(@Header, 2, ';'), 2, '=')
        
        
    SELECT 
      @LastModified = REPLACE(sqlver.udfRTRIMSuper(pv.Value), 'Last-Modified:', '')
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE 'Last-Modified:%'

        
    SET @LastModified = LTRIM(sqlver.udfParseValue(@LastModified, 2, ','))
    SET @LastModified = LEFT(@LastModified, LEN(@LastModified) - 4)
    SET @LastModifiedDate = CAST(@LastModified AS datetime)
        
        
    SELECT 
      @ContentType = REPLACE(sqlver.udfRTRIMSuper(pv.Value), 'Content-Type:', '')
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE 'Content-Type:%'                       
        
    IF @AutoFollowRedir = 0 OR @RedirURL IS NULL BEGIN
      SET @Done = 1
    END
    ELSE BEGIN
    
      IF @RedirURL LIKE '/%' BEGIN
        SET @URLRoot = REPLACE(@URL, '//', '@@')
        SET @URLRoot = LEFT(@URLRoot, CHARINDEX('/', @URLRoot) - 1)
        SET @URLRoot = REPLACE(@URLRoot, '@@', '//')
      END
      ELSE IF @RedirURL LIKE 'http%' BEGIN
        SET @URLRoot = ''
      END
      ELSE BEGIN
        SET @URLRoot = LEFT(@URL, LEN(@URL) - CHARINDEX('/', REVERSE(@URL) + 1))
      END
    
      SET @URL = ISNULL(@URLRoot, '') +  @RedirURL  

    END           
          
  END         
      
  IF @ReturnHeaders = 1 BEGIN
    SELECT * FROM sqlver.udftGetParsedValues(@Headers, char(10))    
  END
END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_ImageTools]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_ImageTools]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_ImageTools]
---------------------------------------------------------------------------------------------
/*
Procedure to demonstrate use of opsstream.spsysBuildCLRAssembly to build and register a CLR
assembly in SQL without the use of Visual Studio.

This is just a sample:  you can use this as a template to create your own procedures
to register your own CLR assemblies.

By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  PRINT '***CAN NO LONGER USE SQLCLR_ImageTools in SQLCLR***'
  PRINT 'This has been deprecated, due to incompatibility of '
  PRINT 'the .NET 4.0 version of System.Image.dll which now'
  PRINT 'contains native code, and hence cannot be loaded into'
  PRINT 'SQLCLR.'
  PRINT ''
  PRINT 'Consider uisng the SQLVerCLR web server to host this'
  PRINT 'assembly''s functionality.'
  RAISERROR('Assembly SQLCLR_ImageTools is not supported and cannot proceed.', 16, 1)
  RETURN 1002

  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'

  DECLARE @FilePathAssemblyCache varchar(1024)
  SET @FilePathAssemblyCache= 'C:\SQLVer\AssemblyCache\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  INSERT INTO #References (AssemblyName, FQFileName, AddToCompilerRefs) VALUES ('System.Drawing', @FilePathAssemblyCache + 'System.Drawing.dll', 1)
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '   
    IF OBJECT_ID(''sqlver.sputilResizeImage_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfResizeImage_CLR;
    END
    '      

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '    
    CREATE FUNCTION sqlver.udfResizeImage_CLR (
        /*
        Will proportionally resize a bitmap image.
        Optionally can crop the image to the specified region before resizing.
        */
      
        @sourceImageBytes varbinary(MAX),
        --the source image bitmap, as a byte array

        --Specify target image dimensions.  Note that we will preserve the original
        --aspect ratio (i.e. scale proportionally)...so the final width or height
        --may be different from the target.  (i.e. scaling a 100x300 image to 
        --300x600 would result in a 200x600 image, as we are constrained by the
        --600 pixel height--which means we can only scale 2x, which means 200 pixel
        --width in this case.
        --You may specify only a single dimension if desired--and let the other dimension
        --be automatically calculated.  (0 means automatically calculate)
        @targetWidth int = 0,
        @targetHeight int = 0,

        --Optionally specify coordinates defining an area of the source image to crop to.
        --Crop coordinate are absolute, for a starting point and an ending point on the
        --original image.  Leave parameters set to 0 for no cropping.
        @cropStartX int = 0,
        @cropStartY int = 0,
        @cropEndX int = 0,
        @cropEndY int = 0
        ) 
    RETURNS varbinary(MAX)                       
    AS
      --NOTE: We would like to have some of these parameters such as @AttachData default to NULL,
      --but then we cannot use varchar(MAX) or varbinary(MAX).  It is for this reason that we  also
      --are using nvarchar(4000) on some parameters:  these can be changed to nvarchar(MAX) to support
      --longer values, but then we cannot use default values.
      EXTERNAL NAME [SQLCLR_ImageTools].[Functions].[ResizeImage]                 
    '
      

  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.IO;
using System.Drawing;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

public partial class Functions
{
    public static SqlBinary ResizeImage(
      /*
      Will proportionally resize a bitmap image.
      Optionally can crop the image to the specified region before resizing.
      */

      SqlBinary sourceImageBytes,
      //the source image bitmap, as a byte array

      //Specify target image dimensions.  Note that we will preserve the original
      //aspect ratio (i.e. scale proportionally)...so the final width or height
      //may be different from the target.  (i.e. scaling a 100x300 image to 
      //300x600 would result in a 200x600 image, as we are constrained by the
      //600 pixel height--which means we can only scale 2x, which means 200 pixel
      //width in this case.
      //You may specify only a single dimension if desired--and let the other dimension
      //be automatically calculated.  (0 means automatically calculate)
      SqlInt32 targetWidth,
      SqlInt32 targetHeight,

      //Optionally specify coordinates defining an area of the source image to crop to.
      //Crop coordinate are absolute, for a starting point and an ending point on the
      //original image.  Leave parameters set to 0 for no cropping.
      SqlInt32 cropStartX,
      SqlInt32 cropStartY,
      SqlInt32 cropEndX,
      SqlInt32 cropEndY
    )
    {
        byte[] paramSourceImageBytes = (byte[])sourceImageBytes;

        int paramTargetWidth = (int)targetWidth;
        int paramTargetHeight = (int)targetHeight;
        int paramCropStartX = (int)cropStartX;
        int paramCropStartY = (int)cropStartY;
        int paramCropEndX = (int)cropEndX;
        int paramCropEndY = (int)cropEndY;

        //copy paramSourceImageBytes to a stream
        using (var imageStream = new MemoryStream(paramSourceImageBytes))
        {   //copy imageStream to a sourceImage System.Drawing.Image object
            using (var sourceImage = System.Drawing.Image.FromStream(imageStream))
            {
                int sourceWidth = 0;
                int sourceHeight = 0;

                bool needCrop = false;

                if (paramCropStartX + paramCropStartY + paramCropEndX + paramCropEndY == 0)
                {
                    //Croping is not specified:  sourceWidth and sourceHeight should match
                    //the full size of the sourceImage
                    needCrop = false;
                    sourceWidth = sourceImage.Width;
                    sourceHeight = sourceImage.Height;
                }
                else
                {
                    //Croping is specified:  sourceWidth and sourceHeight will be determined
                    //by the cropped region (not the full size of the sourceImage)
                    needCrop = true;
                    sourceWidth = paramCropEndX - paramCropStartX;
                    sourceHeight = paramCropEndY - paramCropStartY;
                }

                int sourceX = paramCropStartX;
                int sourceY = paramCropStartY;

                int destX = 0;
                int destY = 0;

                float nPercent = 0;
                float nPercentW = 0;
                float nPercentH = 0;

                if (paramTargetWidth > 0)
                {
                    nPercentW = ((float)paramTargetWidth / (float)sourceWidth);
                }

                if (paramTargetHeight > 0)
                {
                    nPercentH = ((float)paramTargetHeight / (float)sourceHeight);
                }

                //the size of the destination is determined by the the smallest scale
                if (nPercentW > 0 && ((nPercentH <= 0) || (nPercentW < nPercentH)))
                {
                    nPercent = nPercentW;
                }
                else
                {
                    nPercent = nPercentH;
                }

                if (nPercent <= 0)
                {
                    nPercent = 1;
                }


                int destWidth = (int)((float)sourceWidth * nPercent);
                int destHeight = (int)((float)sourceHeight * nPercent);


                //create a new bitmap object to hold the resized image
                using (var destImage = new System.Drawing.Bitmap(destWidth, destHeight, sourceImage.PixelFormat))
                {
                    //create a graphics object to let us draw on the destination image
                    using (var this_graphic = System.Drawing.Graphics.FromImage(destImage))
                    {
                        //use new graphics object to draw on the destination image
                        this_graphic.CompositingQuality = System.Drawing.Drawing2D.CompositingQuality.HighQuality;
                        this_graphic.SmoothingMode = System.Drawing.Drawing2D.SmoothingMode.HighQuality;
                        this_graphic.InterpolationMode = System.Drawing.Drawing2D.InterpolationMode.HighQualityBicubic;

                        var dest_rectangle = new System.Drawing.Rectangle(destX, destY, destX + destWidth, destY + destHeight);

                        if (needCrop)
                        {
                            //draw cropped region from crop_rectangle scaled to dest_rect                    
                            System.Drawing.Rectangle crop_rectangle = new System.Drawing.Rectangle(paramCropStartX, paramCropStartY, paramCropEndX, paramCropEndY);
                            this_graphic.DrawImage(sourceImage, dest_rectangle, crop_rectangle, GraphicsUnit.Pixel);
                        }
                        else
                        {
                            //draw the entire sourceImage scaled to dest_rect
                            this_graphic.DrawImage(sourceImage, dest_rectangle);
                        }
                    }

                    using (var ms = new MemoryStream())
                    {
                        destImage.Save(ms, System.Drawing.Imaging.ImageFormat.Jpeg);
                        return ms.ToArray();
                    }
                }

            }

        }
    }
}

//------end of CLR Source------
'    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'SQLCLR_ImageTools',
    @FileName = 'SQLCLR_ImageTools.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_ActiveDir]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_ActiveDir]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_ActiveDir]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'
  

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  INSERT INTO #References (AssemblyName, FQFileName, AddToCompilerRefs) VALUES ('System.DirectoryServices', 'C:\SQLVer\AssemblyCache\System.DirectoryServices.dll', 1)   

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.udfAuthenticateAD_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfAuthenticateAD_CLR;
    END'   

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE FUNCTION sqlver.udfAuthenticateAD_CLR (     
      @UserName nvarchar(512),
      @Password nvarchar(512),
      @Domain nvarchar(512)            
    )
    RETURNS uniqueidentifier
    AS
      EXTERNAL NAME [ActiveDirCLR].[AD].[AuthenticateAD]'

      
  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.IO;
using System.Net;
using System.Xml;
//using System.Text.RegularExpressions;
using System.DirectoryServices;
/////////////////////////////////////
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;


// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("ActiveDirCLR")]
[assembly: AssemblyDescription("drueter@assyst.com (David Rueter)")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("OpsStream")]
[assembly: AssemblyProduct("ActiveDirCLR")]
[assembly: AssemblyCopyright("Copyright 2014 David Rueter. All Rights Reserved.")]
[assembly: AssemblyTrademark("David Rueter")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("E11FF6CE-BD84-451E-BEF1-1829181AA443")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the ''''*'''' as shown below:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]

public partial class AD
{
    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlGuid AuthenticateAD(SqlString userName, SqlString password, SqlString path)
    {
        bool authentic = false;
        Guid userGUID = Guid.Empty;
        try
        {
            String p = path.ToString();
            String un = userName.ToString();
            String pw = password.ToString();

            DirectoryEntry entry = new DirectoryEntry(p, un, pw);

            DirectorySearcher searcher = new DirectorySearcher(entry, "(sAMAccountName=" + un + ")");
            SearchResult searchResult = searcher.FindOne();

            if (searchResult != null)
            {
                //http://support.microsoft.com/kb/327442
                //It is not safe to simply use .FindOne().GetDirectoryEntry()

                String userPath = searchResult.Path;
                DirectoryEntry userEntry = new DirectoryEntry(searchResult.Path, un, pw);
                userGUID = userEntry.Guid;
                authentic = true;
            }
        }
        catch (DirectoryServicesCOMException) { }

        if (authentic)
        {
            return new SqlGuid(userGUID);
        }
        else
        {
            return SqlGuid.Null;
        }
    }
};
//------end of CLR Source------
'


  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'ActiveDirCLR',
    @FileName = 'ActiveDirCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_DiffMatch]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_DiffMatch]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_DiffMatch]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'
  

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Windows.Forms', 'C:\WINDOWS\Microsoft.NET\Framework64\v2.0.50727\System.Windows.Forms.dll')  
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.udfDiffMatchHTML_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfDiffMatchHTML_CLR;
    END
    IF OBJECT_ID(''sqlver.UrlEncode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.UrlEncode_CLR;
    END
    IF OBJECT_ID(''sqlver.UrlDecode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.UrlDecode_CLR;
    END    
    IF OBJECT_ID(''sqlver.udftGetDiffs_CLR '') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udftGetDiffs_CLR ;
    END'

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE FUNCTION sqlver.udfDiffMatchHTML_CLR (     
      @Text1 nvarchar(MAX),
        --First text value to compare
      @Text2 nvarchar(MAX)
        --Second text value to compare              
    )
    RETURNS nvarchar(MAX)
    --Returns Differences between Text1 and Text2, formatted in HTML    
    AS
      EXTERNAL NAME [DiffMatchCLR].[DiffMatch].[DiffMatchHTML]
      
      
    ~
    CREATE FUNCTION sqlver.UrlEncode_CLR (     
      @s nvarchar(MAX)          
    )
    RETURNS nvarchar(MAX)  
    AS
      EXTERNAL NAME [DiffMatchCLR].[DiffMatch].[UrlEncode]
      
      
    ~
    CREATE FUNCTION sqlver.UrlDecode_CLR (     
      @s nvarchar(MAX)          
    )
    RETURNS nvarchar(MAX)  
    AS
      EXTERNAL NAME [DiffMatchCLR].[DiffMatch].[UrlEncode]
    
    ~

    CREATE FUNCTION sqlver.udftGetDiffs_CLR(@text1 nvarchar(MAX), @text2 nvarchar(MAX))
    RETURNS TABLE (
      diffSequence int,
      diffText nvarchar(MAX),
      diffOperation nvarchar(40),
      diffIndication nvarchar(40)
    )
    AS
    EXTERNAL NAME [DiffMatchCLR].[DiffMatch].[GetDiffs]'

      
  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.IO;
using System.Net;
using System.Xml;
using System.Text.RegularExpressions;
/////////////////////////////////////
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;


// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("DiffMatchCLR")]
[assembly: AssemblyDescription("raser@google.com (Neil Fraser)")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Google")]
[assembly: AssemblyProduct("DiffMatchCLR")]
[assembly: AssemblyCopyright("Copyright 2008 Google Inc. All Rights Reserved.")]
[assembly: AssemblyTrademark("Google")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("60C546BD-AAEB-456D-9700-F6DA10012FB1")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the ''*'' as shown below:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]

/////////////////////////////////////
/*
 * Copyright 2008 Google Inc. All Rights Reserved.
 * Author: fraser@google.com (Neil Fraser)
 * Author: anteru@developer.shelter13.net (Matthaeus G. Chajdas)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Diff Match and Patch
 * http://code.google.com/p/google-diff-match-patch/
 */

namespace DiffMatchPatch
{
    internal static class CompatibilityExtensions
    {
        // JScript splice function
        public static List<T> Splice<T>(this List<T> input, int start, int count,
            params T[] objects)
        {
            List<T> deletedRange = input.GetRange(start, count);
            input.RemoveRange(start, count);
            input.InsertRange(start, objects);

            return deletedRange;
        }

        // Java substring function
        public static string JavaSubstring(this string s, int begin, int end)
        {
            return s.Substring(begin, end - begin);
        }

        //A local UrlEncode, because we can''t use System.Web in SQL
        //UrlEncode by David Rueter (drueter@assyst.com)
        public static string UrlEncode(string s)
        {
            string output = "";
            int p = 0;

            Regex regex = new Regex("([^a-zA-Z0-9_.])");

            Match match = regex.Match(s);
            while (match.Success)
            {
                if (match.Index > p)
                {
                    output += s.Substring(p, match.Index - p);
                }
                if (match.Value[0] == '' '')
                {
                    output += ''+'';
                }
                else
                {
                    string hexVal = "%" + String.Format("{0:X2}", (int)match.Value[0]);
                    output += hexVal.ToUpper();
                }
                p = match.Index + 1;

                match = match.NextMatch();
            }

            if (p < s.Length)
            {
                output += s.Substring(p);
            }

            return output;
        }

        //A local UrlDecode, because we can''t use System.Web in SQL
        //UrlDecode by David Rueter (drueter@assyst.com)
        public static string UrlDecode(string s)
        {
            string output = "";
            int p = 0;

            Regex regex = new Regex("([%+])");

            Match match = regex.Match(s);
            while (match.Success)
            {
                if (match.Index > p)
                {
                    output += s.Substring(p, match.Index - p);
                }
                if (match.Value[0] == ''+'')
                {
                    output += '' '';
                    p = match.Index + 1;
                }
                else
                {
                    string hexVal = match.Value.Substring(1, 2);
                    output += int.Parse(hexVal);
                    p = match.Index + 3;
                }

                match = match.NextMatch();
            }

            if (p < s.Length)
            {
                output += s.Substring(p);
            }

            return output;
        }

    }

    /**-
     * The data structure representing a diff is a List of Diff objects:
     * {Diff(Operation.DELETE, "Hello"), Diff(Operation.INSERT, "Goodbye"),
     *  Diff(Operation.EQUAL, " world.")}
     * which means: delete "Hello", add "Goodbye" and keep " world."
     */
    public enum Operation
    {
        DELETE, INSERT, EQUAL
    }


    /**
     * Class representing one diff operation.
     */
    public class Diff
    {
        public Operation operation;
        // One of: INSERT, DELETE or EQUAL.
        public string text;
        // The text associated with this diff operation.



        /**
         * Constructor.  Initializes the diff with the provided values.
         * @param operation One of INSERT, DELETE or EQUAL.
         * @param text The text being applied.
         */
        public Diff(Operation operation, string text)
        {
            // Construct a diff with the specified operation and text.
            this.operation = operation;
            this.text = text;
        }

        /**
         * Display a human-readable version of this Diff.
         * @return text version.
         */
        public override string ToString()
        {
            string prettyText = this.text.Replace(''\n'', ''\u00b6'');
            return "Diff(" + this.operation + ",\"" + prettyText + "\")";
        }

        /**
         * Is this Diff equivalent to another Diff?
         * @param d Another Diff to compare against.
         * @return true or false.
         */
        public override bool Equals(Object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to Diff return false.
            Diff p = obj as Diff;
            if ((System.Object)p == null)
            {
                return false;
            }

            // Return true if the fields match.
            return p.operation == this.operation && p.text == this.text;
        }

        public bool Equals(Diff obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // Return true if the fields match.
            return obj.operation == this.operation && obj.text == this.text;
        }

        public override int GetHashCode()
        {
            return text.GetHashCode() ^ operation.GetHashCode();
        }
    }


    /**
     * Class representing one patch operation.
     */
    public class Patch
    {
        public List<Diff> diffs = new List<Diff>();
        public int start1;
        public int start2;
        public int length1;
        public int length2;

        /**
         * Emmulate GNU diff''s format.
         * Header: @@ -382,8 +481,9 @@
         * Indicies are printed as 1-based, not 0-based.
         * @return The GNU diff string.
         */
        public override string ToString()
        {
            string coords1, coords2;
            if (this.length1 == 0)
            {
                coords1 = this.start1 + ",0";
            }
            else if (this.length1 == 1)
            {
                coords1 = Convert.ToString(this.start1 + 1);
            }
            else
            {
                coords1 = (this.start1 + 1) + "," + this.length1;
            }
            if (this.length2 == 0)
            {
                coords2 = this.start2 + ",0";
            }
            else if (this.length2 == 1)
            {
                coords2 = Convert.ToString(this.start2 + 1);
            }
            else
            {
                coords2 = (this.start2 + 1) + "," + this.length2;
            }
            StringBuilder text = new StringBuilder();
            text.Append("@@ -").Append(coords1).Append(" +").Append(coords2)
                .Append(" @@\n");
            // Escape the body of the patch with %xx notation.
            foreach (Diff aDiff in this.diffs)
            {
                switch (aDiff.operation)
                {
                    case Operation.INSERT:
                        text.Append(''+'');
                        break;
                    case Operation.DELETE:
                        text.Append(''-'');
                        break;
                    case Operation.EQUAL:
                        text.Append('' '');
                        break;
                }

                text.Append(CompatibilityExtensions.UrlEncode(aDiff.text).Replace(''+'', '' '')).Append("\n");
            }

            return diff_match_patch.unescapeForEncodeUriCompatability(
                text.ToString());
        }
    }


    /**
     * Class containing the diff, match and patch methods.
     * Also Contains the behaviour settings.
     */
    public class diff_match_patch
    {
        // Defaults.
        // Set these on your diff_match_patch instance to override the defaults.

        // Number of seconds to map a diff before giving up (0 for infinity).
        public float Diff_Timeout = 1.0f;
        // Cost of an empty edit operation in terms of edit characters.
        public short Diff_EditCost = 4;
        // At what point is no match declared (0.0 = perfection, 1.0 = very loose).
        public float Match_Threshold = 0.5f;
        // How far to search for a match (0 = exact location, 1000+ = broad match).
        // A match this many characters away from the expected location will add
        // 1.0 to the score (0.0 is a perfect match).
        public int Match_Distance = 1000;
        // When deleting a large block of text (over ~64 characters), how close
        // do the contents have to be to match the expected contents. (0.0 =
        // perfection, 1.0 = very loose).  Note that Match_Threshold controls
        // how closely the end points of a delete need to match.
        public float Patch_DeleteThreshold = 0.5f;
        // Chunk size for context length.
        public short Patch_Margin = 4;

        // The number of bits in an int.
        private short Match_MaxBits = 32;


        //  DIFF FUNCTIONS


        /**
         * Find the differences between two texts.
         * Run a faster, slightly less optimal diff.
         * This method allows the ''checklines'' of diff_main() to be optional.
         * Most of the time checklines is wanted, so default to true.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @return List of Diff objects.
         */
        public List<Diff> diff_main(string text1, string text2)
        {
            return diff_main(text1, text2, true);
        }

        /**
         * Find the differences between two texts.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param checklines Speedup flag.  If false, then don''t run a
         *     line-level diff first to identify the changed areas.
         *     If true, then run a faster slightly less optimal diff.
         * @return List of Diff objects.
         */
        public List<Diff> diff_main(string text1, string text2, bool checklines)
        {
            // Set a deadline by which time the diff must be complete.
            DateTime deadline;
            if (this.Diff_Timeout <= 0)
            {
                deadline = DateTime.MaxValue;
            }
            else
            {
                deadline = DateTime.Now +
                    new TimeSpan(((long)(Diff_Timeout * 1000)) * 10000);
            }
            return diff_main(text1, text2, checklines, deadline);
        }

        /**
         * Find the differences between two texts.  Simplifies the problem by
         * stripping any common prefix or suffix off the texts before diffing.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param checklines Speedup flag.  If false, then don''t run a
         *     line-level diff first to identify the changed areas.
         *     If true, then run a faster slightly less optimal diff.
         * @param deadline Time when the diff should be complete by.  Used
         *     internally for recursive calls.  Users should set DiffTimeout
         *     instead.
         * @return List of Diff objects.
         */
        private List<Diff> diff_main(string text1, string text2, bool checklines,
            DateTime deadline)
        {
            // Check for null inputs not needed since null can''t be passed in C#.

            // Check for equality (speedup).
            List<Diff> diffs;
            if (text1 == text2)
            {
                diffs = new List<Diff>();
                if (text1.Length != 0)
                {
                    diffs.Add(new Diff(Operation.EQUAL, text1));
                }
                return diffs;
            }

            // Trim off common prefix (speedup).
            int commonlength = diff_commonPrefix(text1, text2);
            string commonprefix = text1.Substring(0, commonlength);
            text1 = text1.Substring(commonlength);
            text2 = text2.Substring(commonlength);

            // Trim off common suffix (speedup).
            commonlength = diff_commonSuffix(text1, text2);
            string commonsuffix = text1.Substring(text1.Length - commonlength);
            text1 = text1.Substring(0, text1.Length - commonlength);
            text2 = text2.Substring(0, text2.Length - commonlength);

            // Compute the diff on the middle block.
            diffs = diff_compute(text1, text2, checklines, deadline);

            // Restore the prefix and suffix.
            if (commonprefix.Length != 0)
            {
                diffs.Insert(0, (new Diff(Operation.EQUAL, commonprefix)));
            }
            if (commonsuffix.Length != 0)
            {
                diffs.Add(new Diff(Operation.EQUAL, commonsuffix));
            }

            diff_cleanupMerge(diffs);
            return diffs;
        }

        /**
         * Find the differences between two texts.  Assumes that the texts do not
         * have any common prefix or suffix.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param checklines Speedup flag.  If false, then don''t run a
         *     line-level diff first to identify the changed areas.
         *     If true, then run a faster slightly less optimal diff.
         * @param deadline Time when the diff should be complete by.
         * @return List of Diff objects.
         */
        private List<Diff> diff_compute(string text1, string text2,
                                        bool checklines, DateTime deadline)
        {
            List<Diff> diffs = new List<Diff>();

            if (text1.Length == 0)
            {
                // Just add some text (speedup).
                diffs.Add(new Diff(Operation.INSERT, text2));
                return diffs;
            }

            if (text2.Length == 0)
            {
                // Just delete some text (speedup).
                diffs.Add(new Diff(Operation.DELETE, text1));
                return diffs;
            }

            string longtext = text1.Length > text2.Length ? text1 : text2;
            string shorttext = text1.Length > text2.Length ? text2 : text1;
            int i = longtext.IndexOf(shorttext, StringComparison.Ordinal);
            if (i != -1)
            {
                // Shorter text is inside the longer text (speedup).
                Operation op = (text1.Length > text2.Length) ?
                    Operation.DELETE : Operation.INSERT;
                diffs.Add(new Diff(op, longtext.Substring(0, i)));
                diffs.Add(new Diff(Operation.EQUAL, shorttext));
                diffs.Add(new Diff(op, longtext.Substring(i + shorttext.Length)));
                return diffs;
            }

            if (shorttext.Length == 1)
            {
                // Single character string.
                // After the previous speedup, the character can''t be an equality.
                diffs.Add(new Diff(Operation.DELETE, text1));
                diffs.Add(new Diff(Operation.INSERT, text2));
                return diffs;
            }

            // Check to see if the problem can be split in two.
            string[] hm = diff_halfMatch(text1, text2);
            if (hm != null)
            {
                // A half-match was found, sort out the return data.
                string text1_a = hm[0];
                string text1_b = hm[1];
                string text2_a = hm[2];
                string text2_b = hm[3];
                string mid_common = hm[4];
                // Send both pairs off for separate processing.
                List<Diff> diffs_a = diff_main(text1_a, text2_a, checklines, deadline);
                List<Diff> diffs_b = diff_main(text1_b, text2_b, checklines, deadline);
                // Merge the results.
                diffs = diffs_a;
                diffs.Add(new Diff(Operation.EQUAL, mid_common));
                diffs.AddRange(diffs_b);
                return diffs;
            }

            if (checklines && text1.Length > 100 && text2.Length > 100)
            {
                return diff_lineMode(text1, text2, deadline);
            }

            return diff_bisect(text1, text2, deadline);
        }

        /**
         * Do a quick line-level diff on both strings, then rediff the parts for
         * greater accuracy.
         * This speedup can produce non-minimal diffs.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param deadline Time when the diff should be complete by.
         * @return List of Diff objects.
         */
        private List<Diff> diff_lineMode(string text1, string text2,
                                         DateTime deadline)
        {
            // Scan the text on a line-by-line basis first.
            Object[] b = diff_linesToChars(text1, text2);
            text1 = (string)b[0];
            text2 = (string)b[1];
            List<string> linearray = (List<string>)b[2];

            List<Diff> diffs = diff_main(text1, text2, false, deadline);

            // Convert the diff back to original text.
            diff_charsToLines(diffs, linearray);
            // Eliminate freak matches (e.g. blank lines)
            diff_cleanupSemantic(diffs);

            // Rediff any replacement blocks, this time character-by-character.
            // Add a dummy entry at the end.
            diffs.Add(new Diff(Operation.EQUAL, string.Empty));
            int pointer = 0;
            int count_delete = 0;
            int count_insert = 0;
            string text_delete = string.Empty;
            string text_insert = string.Empty;
            while (pointer < diffs.Count)
            {
                switch (diffs[pointer].operation)
                {
                    case Operation.INSERT:
                        count_insert++;
                        text_insert += diffs[pointer].text;
                        break;
                    case Operation.DELETE:
                        count_delete++;
                        text_delete += diffs[pointer].text;
                        break;
                    case Operation.EQUAL:
                        // Upon reaching an equality, check for prior redundancies.
                        if (count_delete >= 1 && count_insert >= 1)
                        {
                            // Delete the offending records and add the merged ones.
                            diffs.RemoveRange(pointer - count_delete - count_insert,
                                count_delete + count_insert);
                            pointer = pointer - count_delete - count_insert;
                            List<Diff> a =
                                this.diff_main(text_delete, text_insert, false, deadline);
                            diffs.InsertRange(pointer, a);
                            pointer = pointer + a.Count;
                        }
                        count_insert = 0;
                        count_delete = 0;
                        text_delete = string.Empty;
                        text_insert = string.Empty;
                        break;
                }
                pointer++;
            }
            diffs.RemoveAt(diffs.Count - 1);  // Remove the dummy entry at the end.

            return diffs;
        }

        /**
         * Find the ''middle snake'' of a diff, split the problem in two
         * and return the recursively constructed diff.
         * See Myers 1986 paper: An O(ND) Difference Algorithm and Its Variations.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param deadline Time at which to bail if not yet complete.
         * @return List of Diff objects.
         */
        protected List<Diff> diff_bisect(string text1, string text2,
            DateTime deadline)
        {
            // Cache the text lengths to prevent multiple calls.
            int text1_length = text1.Length;
            int text2_length = text2.Length;
            int max_d = (text1_length + text2_length + 1) / 2;
            int v_offset = max_d;
            int v_length = 2 * max_d;
            int[] v1 = new int[v_length];
            int[] v2 = new int[v_length];
            for (int x = 0; x < v_length; x++)
            {
                v1[x] = -1;
                v2[x] = -1;
            }
            v1[v_offset + 1] = 0;
            v2[v_offset + 1] = 0;
            int delta = text1_length - text2_length;
            // If the total number of characters is odd, then the front path will
            // collide with the reverse path.
            bool front = (delta % 2 != 0);
            // Offsets for start and end of k loop.
            // Prevents mapping of space beyond the grid.
            int k1start = 0;
            int k1end = 0;
            int k2start = 0;
            int k2end = 0;
            for (int d = 0; d < max_d; d++)
            {
                // Bail out if deadline is reached.
                if (DateTime.Now > deadline)
                {
                    break;
                }

                // Walk the front path one step.
                for (int k1 = -d + k1start; k1 <= d - k1end; k1 += 2)
                {
                    int k1_offset = v_offset + k1;
                    int x1;
                    if (k1 == -d || k1 != d && v1[k1_offset - 1] < v1[k1_offset + 1])
                    {
                        x1 = v1[k1_offset + 1];
                    }
                    else
                    {
                        x1 = v1[k1_offset - 1] + 1;
                    }
                    int y1 = x1 - k1;
                    while (x1 < text1_length && y1 < text2_length
                          && text1[x1] == text2[y1])
                    {
                        x1++;
                        y1++;
                    }
                    v1[k1_offset] = x1;
                    if (x1 > text1_length)
                    {
                        // Ran off the right of the graph.
                        k1end += 2;
                    }
                    else if (y1 > text2_length)
                    {
                        // Ran off the bottom of the graph.
                        k1start += 2;
                    }
                    else if (front)
                    {
                        int k2_offset = v_offset + delta - k1;
                        if (k2_offset >= 0 && k2_offset < v_length && v2[k2_offset] != -1)
                        {
                            // Mirror x2 onto top-left coordinate system.
                            int x2 = text1_length - v2[k2_offset];
                            if (x1 >= x2)
                            {
                                // Overlap detected.
                                return diff_bisectSplit(text1, text2, x1, y1, deadline);
                            }
                        }
                    }
                }

                // Walk the reverse path one step.
                for (int k2 = -d + k2start; k2 <= d - k2end; k2 += 2)
                {
                    int k2_offset = v_offset + k2;
                    int x2;
                    if (k2 == -d || k2 != d && v2[k2_offset - 1] < v2[k2_offset + 1])
                    {
                        x2 = v2[k2_offset + 1];
                    }
                    else
                    {
                        x2 = v2[k2_offset - 1] + 1;
                    }
                    int y2 = x2 - k2;
                    while (x2 < text1_length && y2 < text2_length
                        && text1[text1_length - x2 - 1]
                        == text2[text2_length - y2 - 1])
                    {
                        x2++;
                        y2++;
                    }
                    v2[k2_offset] = x2;
                    if (x2 > text1_length)
                    {
                        // Ran off the left of the graph.
                        k2end += 2;
                    }
                    else if (y2 > text2_length)
                    {
                        // Ran off the top of the graph.
                        k2start += 2;
                    }
                    else if (!front)
                    {
                        int k1_offset = v_offset + delta - k2;
                        if (k1_offset >= 0 && k1_offset < v_length && v1[k1_offset] != -1)
                        {
                            int x1 = v1[k1_offset];
                            int y1 = v_offset + x1 - k1_offset;
                            // Mirror x2 onto top-left coordinate system.
                            x2 = text1_length - v2[k2_offset];
                            if (x1 >= x2)
                            {
                                // Overlap detected.
                                return diff_bisectSplit(text1, text2, x1, y1, deadline);
                            }
                        }
                    }
                }
            }
            // Diff took too long and hit the deadline or
            // number of diffs equals number of characters, no commonality at all.
            List<Diff> diffs = new List<Diff>();
            diffs.Add(new Diff(Operation.DELETE, text1));
            diffs.Add(new Diff(Operation.INSERT, text2));
            return diffs;
        }

        /**
         * Given the location of the ''middle snake'', split the diff in two parts
         * and recurse.
         * @param text1 Old string to be diffed.
         * @param text2 New string to be diffed.
         * @param x Index of split point in text1.
         * @param y Index of split point in text2.
         * @param deadline Time at which to bail if not yet complete.
         * @return LinkedList of Diff objects.
         */
        private List<Diff> diff_bisectSplit(string text1, string text2,
            int x, int y, DateTime deadline)
        {
            string text1a = text1.Substring(0, x);
            string text2a = text2.Substring(0, y);
            string text1b = text1.Substring(x);
            string text2b = text2.Substring(y);

            // Compute both diffs serially.
            List<Diff> diffs = diff_main(text1a, text2a, false, deadline);
            List<Diff> diffsb = diff_main(text1b, text2b, false, deadline);

            diffs.AddRange(diffsb);
            return diffs;
        }

        /**
         * Split two texts into a list of strings.  Reduce the texts to a string of
         * hashes where each Unicode character represents one line.
         * @param text1 First string.
         * @param text2 Second string.
         * @return Three element Object array, containing the encoded text1, the
         *     encoded text2 and the List of unique strings.  The zeroth element
         *     of the List of unique strings is intentionally blank.
         */
        public Object[] diff_linesToChars(string text1, string text2)
        {
            List<string> lineArray = new List<string>();
            Dictionary<string, int> lineHash = new Dictionary<string, int>();
            // e.g. linearray[4] == "Hello\n"
            // e.g. linehash.get("Hello\n") == 4

            // "\x00" is a valid character, but various debuggers don''t like it.
            // So we''ll insert a junk entry to avoid generating a null character.
            lineArray.Add(string.Empty);

            string chars1 = diff_linesToCharsMunge(text1, lineArray, lineHash);
            string chars2 = diff_linesToCharsMunge(text2, lineArray, lineHash);
            return new Object[] { chars1, chars2, lineArray };
        }

        /**
         * Split a text into a list of strings.  Reduce the texts to a string of
         * hashes where each Unicode character represents one line.
         * @param text String to encode.
         * @param lineArray List of unique strings.
         * @param lineHash Map of strings to indices.
         * @return Encoded string.
         */
        private string diff_linesToCharsMunge(string text, List<string> lineArray,
                                              Dictionary<string, int> lineHash)
        {
            int lineStart = 0;
            int lineEnd = -1;
            string line;
            StringBuilder chars = new StringBuilder();
            // Walk the text, pulling out a Substring for each line.
            // text.split(''\n'') would would temporarily double our memory footprint.
            // Modifying text would create many large strings to garbage collect.
            while (lineEnd < text.Length - 1)
            {
                lineEnd = text.IndexOf(''\n'', lineStart);
                if (lineEnd == -1)
                {
                    lineEnd = text.Length - 1;
                }
                line = text.JavaSubstring(lineStart, lineEnd + 1);
                lineStart = lineEnd + 1;

                if (lineHash.ContainsKey(line))
                {
                    chars.Append(((char)(int)lineHash[line]));
                }
                else
                {
                    lineArray.Add(line);
                    lineHash.Add(line, lineArray.Count - 1);
                    chars.Append(((char)(lineArray.Count - 1)));
                }
            }
            return chars.ToString();
        }

        /**
         * Rehydrate the text in a diff from a string of line hashes to real lines
         * of text.
         * @param diffs List of Diff objects.
         * @param lineArray List of unique strings.
         */
        public void diff_charsToLines(ICollection<Diff> diffs,
                        List<string> lineArray)
        {
            StringBuilder text;
            foreach (Diff diff in diffs)
            {
                text = new StringBuilder();
                for (int y = 0; y < diff.text.Length; y++)
                {
                    text.Append(lineArray[diff.text[y]]);
                }
                diff.text = text.ToString();
            }
        }

        /**
         * Determine the common prefix of two strings.
         * @param text1 First string.
         * @param text2 Second string.
         * @return The number of characters common to the start of each string.
         */
        public int diff_commonPrefix(string text1, string text2)
        {
            // Performance analysis: http://neil.fraser.name/news/2007/10/09/
            int n = Math.Min(text1.Length, text2.Length);
            for (int i = 0; i < n; i++)
            {
                if (text1[i] != text2[i])
                {
                    return i;
                }
            }
            return n;
        }

        /**
         * Determine the common suffix of two strings.
         * @param text1 First string.
         * @param text2 Second string.
         * @return The number of characters common to the end of each string.
         */
        public int diff_commonSuffix(string text1, string text2)
        {
            // Performance analysis: http://neil.fraser.name/news/2007/10/09/
            int text1_length = text1.Length;
            int text2_length = text2.Length;
            int n = Math.Min(text1.Length, text2.Length);
            for (int i = 1; i <= n; i++)
            {
                if (text1[text1_length - i] != text2[text2_length - i])
                {
                    return i - 1;
                }
            }
            return n;
        }

        /**
         * Determine if the suffix of one string is the prefix of another.
         * @param text1 First string.
         * @param text2 Second string.
         * @return The number of characters common to the end of the first
         *     string and the start of the second string.
         */
        protected int diff_commonOverlap(string text1, string text2)
        {
            // Cache the text lengths to prevent multiple calls.
            int text1_length = text1.Length;
            int text2_length = text2.Length;
            // Eliminate the null case.
            if (text1_length == 0 || text2_length == 0)
            {
                return 0;
            }
            // Truncate the longer string.
            if (text1_length > text2_length)
            {
                text1 = text1.Substring(text1_length - text2_length);
            }
            else if (text1_length < text2_length)
            {
                text2 = text2.Substring(0, text1_length);
            }
            int text_length = Math.Min(text1_length, text2_length);
            // Quick check for the worst case.
            if (text1 == text2)
            {
                return text_length;
            }

            // Start by looking for a single character match
            // and increase length until no match is found.
            // Performance analysis: http://neil.fraser.name/news/2010/11/04/
            int best = 0;
            int length = 1;
            while (true)
            {
                string pattern = text1.Substring(text_length - length);
                int found = text2.IndexOf(pattern, StringComparison.Ordinal);
                if (found == -1)
                {
                    return best;
                }
                length += found;
                if (found == 0 || text1.Substring(text_length - length) ==
                    text2.Substring(0, length))
                {
                    best = length;
                    length++;
                }
            }
        }

        /**
         * Do the two texts share a Substring which is at least half the length of
         * the longer text?
         * This speedup can produce non-minimal diffs.
         * @param text1 First string.
         * @param text2 Second string.
         * @return Five element String array, containing the prefix of text1, the
         *     suffix of text1, the prefix of text2, the suffix of text2 and the
         *     common middle.  Or null if there was no match.
         */

        protected string[] diff_halfMatch(string text1, string text2)
        {
            if (this.Diff_Timeout <= 0)
            {
                // Don''t risk returning a non-optimal diff if we have unlimited time.
                return null;
            }
            string longtext = text1.Length > text2.Length ? text1 : text2;
            string shorttext = text1.Length > text2.Length ? text2 : text1;
            if (longtext.Length < 4 || shorttext.Length * 2 < longtext.Length)
            {
                return null;  // Pointless.
            }

            // First check if the second quarter is the seed for a half-match.
            string[] hm1 = diff_halfMatchI(longtext, shorttext,
                                           (longtext.Length + 3) / 4);
            // Check again based on the third quarter.
            string[] hm2 = diff_halfMatchI(longtext, shorttext,
                                           (longtext.Length + 1) / 2);
            string[] hm;
            if (hm1 == null && hm2 == null)
            {
                return null;
            }
            else if (hm2 == null)
            {
                hm = hm1;
            }
            else if (hm1 == null)
            {
                hm = hm2;
            }
            else
            {
                // Both matched.  Select the longest.
                hm = hm1[4].Length > hm2[4].Length ? hm1 : hm2;
            }

            // A half-match was found, sort out the return data.
            if (text1.Length > text2.Length)
            {
                return hm;
                //return new string[]{hm[0], hm[1], hm[2], hm[3], hm[4]};
            }
            else
            {
                return new string[] { hm[2], hm[3], hm[0], hm[1], hm[4] };
            }
        }

        /**
         * Does a Substring of shorttext exist within longtext such that the
         * Substring is at least half the length of longtext?
         * @param longtext Longer string.
         * @param shorttext Shorter string.
         * @param i Start index of quarter length Substring within longtext.
         * @return Five element string array, containing the prefix of longtext, the
         *     suffix of longtext, the prefix of shorttext, the suffix of shorttext
         *     and the common middle.  Or null if there was no match.
         */
        private string[] diff_halfMatchI(string longtext, string shorttext, int i)
        {
            // Start with a 1/4 length Substring at position i as a seed.
            string seed = longtext.Substring(i, longtext.Length / 4);
            int j = -1;
            string best_common = string.Empty;
            string best_longtext_a = string.Empty, best_longtext_b = string.Empty;
            string best_shorttext_a = string.Empty, best_shorttext_b = string.Empty;
            while (j < shorttext.Length && (j = shorttext.IndexOf(seed, j + 1,
                StringComparison.Ordinal)) != -1)
            {
                int prefixLength = diff_commonPrefix(longtext.Substring(i),
                                                     shorttext.Substring(j));
                int suffixLength = diff_commonSuffix(longtext.Substring(0, i),
                                                     shorttext.Substring(0, j));
                if (best_common.Length < suffixLength + prefixLength)
                {
                    best_common = shorttext.Substring(j - suffixLength, suffixLength)
                        + shorttext.Substring(j, prefixLength);
                    best_longtext_a = longtext.Substring(0, i - suffixLength);
                    best_longtext_b = longtext.Substring(i + prefixLength);
                    best_shorttext_a = shorttext.Substring(0, j - suffixLength);
                    best_shorttext_b = shorttext.Substring(j + prefixLength);
                }
            }
            if (best_common.Length * 2 >= longtext.Length)
            {
                return new string[]{best_longtext_a, best_longtext_b,
            best_shorttext_a, best_shorttext_b, best_common};
            }
            else
            {
                return null;
            }
        }

        /**
         * Reduce the number of edits by eliminating semantically trivial
         * equalities.
         * @param diffs List of Diff objects.
         */
        public void diff_cleanupSemantic(List<Diff> diffs)
        {
            bool changes = false;
            // Stack of indices where equalities are found.
            Stack<int> equalities = new Stack<int>();
            // Always equal to equalities[equalitiesLength-1][1]
            string lastequality = null;
            int pointer = 0;  // Index of current position.
            // Number of characters that changed prior to the equality.
            int length_insertions1 = 0;
            int length_deletions1 = 0;
            // Number of characters that changed after the equality.
            int length_insertions2 = 0;
            int length_deletions2 = 0;
            while (pointer < diffs.Count)
            {
                if (diffs[pointer].operation == Operation.EQUAL)
                {  // Equality found.
                    equalities.Push(pointer);
                    length_insertions1 = length_insertions2;
                    length_deletions1 = length_deletions2;
                    length_insertions2 = 0;
                    length_deletions2 = 0;
                    lastequality = diffs[pointer].text;
                }
                else
                {  // an insertion or deletion
                    if (diffs[pointer].operation == Operation.INSERT)
                    {
                        length_insertions2 += diffs[pointer].text.Length;
                    }
                    else
                    {
                        length_deletions2 += diffs[pointer].text.Length;
                    }
                    // Eliminate an equality that is smaller or equal to the edits on both
                    // sides of it.
                    if (lastequality != null && (lastequality.Length
                        <= Math.Max(length_insertions1, length_deletions1))
                        && (lastequality.Length
                            <= Math.Max(length_insertions2, length_deletions2)))
                    {
                        // Duplicate record.
                        diffs.Insert(equalities.Peek(),
                                     new Diff(Operation.DELETE, lastequality));
                        // Change second copy to insert.
                        diffs[equalities.Peek() + 1].operation = Operation.INSERT;
                        // Throw away the equality we just deleted.
                        equalities.Pop();
                        if (equalities.Count > 0)
                        {
                            equalities.Pop();
                        }
                        pointer = equalities.Count > 0 ? equalities.Peek() : -1;
                        length_insertions1 = 0;  // Reset the counters.
                        length_deletions1 = 0;
                        length_insertions2 = 0;
                        length_deletions2 = 0;
                        lastequality = null;
                        changes = true;
                    }
                }
                pointer++;
            }

            // Normalize the diff.
            if (changes)
            {
                diff_cleanupMerge(diffs);
            }
            diff_cleanupSemanticLossless(diffs);

            // Find any overlaps between deletions and insertions.
            // e.g: <del>abcxxx</del><ins>xxxdef</ins>
            //   -> <del>abc</del>xxx<ins>def</ins>
            // e.g: <del>xxxabc</del><ins>defxxx</ins>
            //   -> <ins>def</ins>xxx<del>abc</del>
            // Only extract an overlap if it is as big as the edit ahead or behind it.
            pointer = 1;
            while (pointer < diffs.Count)
            {
                if (diffs[pointer - 1].operation == Operation.DELETE &&
                    diffs[pointer].operation == Operation.INSERT)
                {
                    string deletion = diffs[pointer - 1].text;
                    string insertion = diffs[pointer].text;
                    int overlap_length1 = diff_commonOverlap(deletion, insertion);
                    int overlap_length2 = diff_commonOverlap(insertion, deletion);
                    if (overlap_length1 >= overlap_length2)
                    {
                        if (overlap_length1 >= deletion.Length / 2.0 ||
                            overlap_length1 >= insertion.Length / 2.0)
                        {
                            // Overlap found.
                            // Insert an equality and trim the surrounding edits.
                            diffs.Insert(pointer, new Diff(Operation.EQUAL,
                                insertion.Substring(0, overlap_length1)));
                            diffs[pointer - 1].text =
                                deletion.Substring(0, deletion.Length - overlap_length1);
                            diffs[pointer + 1].text = insertion.Substring(overlap_length1);
                            pointer++;
                        }
                    }
                    else
                    {
                        if (overlap_length2 >= deletion.Length / 2.0 ||
                            overlap_length2 >= insertion.Length / 2.0)
                        {
                            // Reverse overlap found.
                            // Insert an equality and swap and trim the surrounding edits.
                            diffs.Insert(pointer, new Diff(Operation.EQUAL,
                                deletion.Substring(0, overlap_length2)));
                            diffs[pointer - 1].operation = Operation.INSERT;
                            diffs[pointer - 1].text =
                                insertion.Substring(0, insertion.Length - overlap_length2);
                            diffs[pointer + 1].operation = Operation.DELETE;
                            diffs[pointer + 1].text = deletion.Substring(overlap_length2);
                            pointer++;
                        }
                    }
                    pointer++;
                }
                pointer++;
            }
        }

        /**
         * Look for single edits surrounded on both sides by equalities
         * which can be shifted sideways to align the edit to a word boundary.
         * e.g: The c<ins>at c</ins>ame. -> The <ins>cat </ins>came.
         * @param diffs List of Diff objects.
         */
        public void diff_cleanupSemanticLossless(List<Diff> diffs)
        {
            int pointer = 1;
            // Intentionally ignore the first and last element (don''t need checking).
            while (pointer < diffs.Count - 1)
            {
                if (diffs[pointer - 1].operation == Operation.EQUAL &&
                  diffs[pointer + 1].operation == Operation.EQUAL)
                {
                    // This is a single edit surrounded by equalities.
                    string equality1 = diffs[pointer - 1].text;
                    string edit = diffs[pointer].text;
                    string equality2 = diffs[pointer + 1].text;

                    // First, shift the edit as far left as possible.
                    int commonOffset = this.diff_commonSuffix(equality1, edit);
                    if (commonOffset > 0)
                    {
                        string commonString = edit.Substring(edit.Length - commonOffset);
                        equality1 = equality1.Substring(0, equality1.Length - commonOffset);
                        edit = commonString + edit.Substring(0, edit.Length - commonOffset);
                        equality2 = commonString + equality2;
                    }

                    // Second, step character by character right,
                    // looking for the best fit.
                    string bestEquality1 = equality1;
                    string bestEdit = edit;
                    string bestEquality2 = equality2;
                    int bestScore = diff_cleanupSemanticScore(equality1, edit) +
                        diff_cleanupSemanticScore(edit, equality2);
                    while (edit.Length != 0 && equality2.Length != 0
                        && edit[0] == equality2[0])
                    {
                        equality1 += edit[0];
                        edit = edit.Substring(1) + equality2[0];
                        equality2 = equality2.Substring(1);
                        int score = diff_cleanupSemanticScore(equality1, edit) +
                            diff_cleanupSemanticScore(edit, equality2);
                        // The >= encourages trailing rather than leading whitespace on
                        // edits.
                        if (score >= bestScore)
                        {
                            bestScore = score;
                            bestEquality1 = equality1;
                            bestEdit = edit;
                            bestEquality2 = equality2;
                        }
                    }

                    if (diffs[pointer - 1].text != bestEquality1)
                    {
                        // We have an improvement, save it back to the diff.
                        if (bestEquality1.Length != 0)
                        {
                            diffs[pointer - 1].text = bestEquality1;
                        }
                        else
                        {
                            diffs.RemoveAt(pointer - 1);
                            pointer--;
                        }
                        diffs[pointer].text = bestEdit;
                        if (bestEquality2.Length != 0)
                        {
                            diffs[pointer + 1].text = bestEquality2;
                        }
                        else
                        {
                            diffs.RemoveAt(pointer + 1);
                            pointer--;
                        }
                    }
                }
                pointer++;
            }
        }

        /**
         * Given two strings, comAdde a score representing whether the internal
         * boundary falls on logical boundaries.
         * Scores range from 6 (best) to 0 (worst).
         * @param one First string.
         * @param two Second string.
         * @return The score.
         */
        private int diff_cleanupSemanticScore(string one, string two)
        {
            if (one.Length == 0 || two.Length == 0)
            {
                // Edges are the best.
                return 6;
            }

            // Each port of this function behaves slightly differently due to
            // subtle differences in each language''s definition of things like
            // ''whitespace''.  Since this function''s purpose is largely cosmetic,
            // the choice has been made to use each language''s native features
            // rather than force total conformity.
            char char1 = one[one.Length - 1];
            char char2 = two[0];
            bool nonAlphaNumeric1 = !Char.IsLetterOrDigit(char1);
            bool nonAlphaNumeric2 = !Char.IsLetterOrDigit(char2);
            bool whitespace1 = nonAlphaNumeric1 && Char.IsWhiteSpace(char1);
            bool whitespace2 = nonAlphaNumeric2 && Char.IsWhiteSpace(char2);
            bool lineBreak1 = whitespace1 && Char.IsControl(char1);
            bool lineBreak2 = whitespace2 && Char.IsControl(char2);
            bool blankLine1 = lineBreak1 && BLANKLINEEND.IsMatch(one);
            bool blankLine2 = lineBreak2 && BLANKLINESTART.IsMatch(two);

            if (blankLine1 || blankLine2)
            {
                // Five points for blank lines.
                return 5;
            }
            else if (lineBreak1 || lineBreak2)
            {
                // Four points for line breaks.
                return 4;
            }
            else if (nonAlphaNumeric1 && !whitespace1 && whitespace2)
            {
                // Three points for end of sentences.
                return 3;
            }
            else if (whitespace1 || whitespace2)
            {
                // Two points for whitespace.
                return 2;
            }
            else if (nonAlphaNumeric1 || nonAlphaNumeric2)
            {
                // One point for non-alphanumeric.
                return 1;
            }
            return 0;
        }

        // Define some regex patterns for matching boundaries.
        private Regex BLANKLINEEND = new Regex("\\n\\r?\\n\\Z");
        private Regex BLANKLINESTART = new Regex("\\A\\r?\\n\\r?\\n");

        /**
         * Reduce the number of edits by eliminating operationally trivial
         * equalities.
         * @param diffs List of Diff objects.
         */
        public void diff_cleanupEfficiency(List<Diff> diffs)
        {
            bool changes = false;
            // Stack of indices where equalities are found.
            Stack<int> equalities = new Stack<int>();
            // Always equal to equalities[equalitiesLength-1][1]
            string lastequality = string.Empty;
            int pointer = 0;  // Index of current position.
            // Is there an insertion operation before the last equality.
            bool pre_ins = false;
            // Is there a deletion operation before the last equality.
            bool pre_del = false;
            // Is there an insertion operation after the last equality.
            bool post_ins = false;
            // Is there a deletion operation after the last equality.
            bool post_del = false;
            while (pointer < diffs.Count)
            {
                if (diffs[pointer].operation == Operation.EQUAL)
                {  // Equality found.
                    if (diffs[pointer].text.Length < this.Diff_EditCost
                        && (post_ins || post_del))
                    {
                        // Candidate found.
                        equalities.Push(pointer);
                        pre_ins = post_ins;
                        pre_del = post_del;
                        lastequality = diffs[pointer].text;
                    }
                    else
                    {
                        // Not a candidate, and can never become one.
                        equalities.Clear();
                        lastequality = string.Empty;
                    }
                    post_ins = post_del = false;
                }
                else
                {  // An insertion or deletion.
                    if (diffs[pointer].operation == Operation.DELETE)
                    {
                        post_del = true;
                    }
                    else
                    {
                        post_ins = true;
                    }
                    /*
                     * Five types to be split:
                     * <ins>A</ins><del>B</del>XY<ins>C</ins><del>D</del>
                     * <ins>A</ins>X<ins>C</ins><del>D</del>
                     * <ins>A</ins><del>B</del>X<ins>C</ins>
                     * <ins>A</del>X<ins>C</ins><del>D</del>
                     * <ins>A</ins><del>B</del>X<del>C</del>
                     */
                    if ((lastequality.Length != 0)
                        && ((pre_ins && pre_del && post_ins && post_del)
                        || ((lastequality.Length < this.Diff_EditCost / 2)
                        && ((pre_ins ? 1 : 0) + (pre_del ? 1 : 0) + (post_ins ? 1 : 0)
                        + (post_del ? 1 : 0)) == 3)))
                    {
                        // Duplicate record.
                        diffs.Insert(equalities.Peek(),
                                     new Diff(Operation.DELETE, lastequality));
                        // Change second copy to insert.
                        diffs[equalities.Peek() + 1].operation = Operation.INSERT;
                        equalities.Pop();  // Throw away the equality we just deleted.
                        lastequality = string.Empty;
                        if (pre_ins && pre_del)
                        {
                            // No changes made which could affect previous entry, keep going.
                            post_ins = post_del = true;
                            equalities.Clear();
                        }
                        else
                        {
                            if (equalities.Count > 0)
                            {
                                equalities.Pop();
                            }

                            pointer = equalities.Count > 0 ? equalities.Peek() : -1;
                            post_ins = post_del = false;
                        }
                        changes = true;
                    }
                }
                pointer++;
            }

            if (changes)
            {
                diff_cleanupMerge(diffs);
            }
        }

        /**
         * Reorder and merge like edit sections.  Merge equalities.
         * Any edit section can move as long as it doesn''t cross an equality.
         * @param diffs List of Diff objects.
         */
        public void diff_cleanupMerge(List<Diff> diffs)
        {
            // Add a dummy entry at the end.
            diffs.Add(new Diff(Operation.EQUAL, string.Empty));
            int pointer = 0;
            int count_delete = 0;
            int count_insert = 0;
            string text_delete = string.Empty;
            string text_insert = string.Empty;
            int commonlength;
            while (pointer < diffs.Count)
            {
                switch (diffs[pointer].operation)
                {
                    case Operation.INSERT:
                        count_insert++;
                        text_insert += diffs[pointer].text;
                        pointer++;
                        break;
                    case Operation.DELETE:
                        count_delete++;
                        text_delete += diffs[pointer].text;
                        pointer++;
                        break;
                    case Operation.EQUAL:
                        // Upon reaching an equality, check for prior redundancies.
                        if (count_delete + count_insert > 1)
                        {
                            if (count_delete != 0 && count_insert != 0)
                            {
                                // Factor out any common prefixies.
                                commonlength = this.diff_commonPrefix(text_insert, text_delete);
                                if (commonlength != 0)
                                {
                                    if ((pointer - count_delete - count_insert) > 0 &&
                                      diffs[pointer - count_delete - count_insert - 1].operation
                                          == Operation.EQUAL)
                                    {
                                        diffs[pointer - count_delete - count_insert - 1].text
                                            += text_insert.Substring(0, commonlength);
                                    }
                                    else
                                    {
                                        diffs.Insert(0, new Diff(Operation.EQUAL,
                                            text_insert.Substring(0, commonlength)));
                                        pointer++;
                                    }
                                    text_insert = text_insert.Substring(commonlength);
                                    text_delete = text_delete.Substring(commonlength);
                                }
                                // Factor out any common suffixies.
                                commonlength = this.diff_commonSuffix(text_insert, text_delete);
                                if (commonlength != 0)
                                {
                                    diffs[pointer].text = text_insert.Substring(text_insert.Length
                                        - commonlength) + diffs[pointer].text;
                                    text_insert = text_insert.Substring(0, text_insert.Length
                                        - commonlength);
                                    text_delete = text_delete.Substring(0, text_delete.Length
                                        - commonlength);
                                }
                            }
                            // Delete the offending records and add the merged ones.
                            if (count_delete == 0)
                            {
                                diffs.Splice(pointer - count_insert,
                                    count_delete + count_insert,
                                    new Diff(Operation.INSERT, text_insert));
                            }
                            else if (count_insert == 0)
                            {
                                diffs.Splice(pointer - count_delete,
                                    count_delete + count_insert,
                                    new Diff(Operation.DELETE, text_delete));
                            }
                            else
                            {
                                diffs.Splice(pointer - count_delete - count_insert,
                                    count_delete + count_insert,
                                    new Diff(Operation.DELETE, text_delete),
                                    new Diff(Operation.INSERT, text_insert));
                            }
                            pointer = pointer - count_delete - count_insert +
                                (count_delete != 0 ? 1 : 0) + (count_insert != 0 ? 1 : 0) + 1;
                        }
                        else if (pointer != 0
                          && diffs[pointer - 1].operation == Operation.EQUAL)
                        {
                            // Merge this equality with the previous one.
                            diffs[pointer - 1].text += diffs[pointer].text;
                            diffs.RemoveAt(pointer);
                        }
                        else
                        {
                            pointer++;
                        }
                        count_insert = 0;
                        count_delete = 0;
                        text_delete = string.Empty;
                        text_insert = string.Empty;
                        break;
                }
            }
            if (diffs[diffs.Count - 1].text.Length == 0)
            {
                diffs.RemoveAt(diffs.Count - 1);  // Remove the dummy entry at the end.
            }

            // Second pass: look for single edits surrounded on both sides by
            // equalities which can be shifted sideways to eliminate an equality.
            // e.g: A<ins>BA</ins>C -> <ins>AB</ins>AC
            bool changes = false;
            pointer = 1;
            // Intentionally ignore the first and last element (don''t need checking).
            while (pointer < (diffs.Count - 1))
            {
                if (diffs[pointer - 1].operation == Operation.EQUAL &&
                  diffs[pointer + 1].operation == Operation.EQUAL)
                {
                    // This is a single edit surrounded by equalities.
                    if (diffs[pointer].text.EndsWith(diffs[pointer - 1].text,
                        StringComparison.Ordinal))
                    {
                        // Shift the edit over the previous equality.
                        diffs[pointer].text = diffs[pointer - 1].text +
                            diffs[pointer].text.Substring(0, diffs[pointer].text.Length -
                                                          diffs[pointer - 1].text.Length);
                        diffs[pointer + 1].text = diffs[pointer - 1].text
                            + diffs[pointer + 1].text;
                        diffs.Splice(pointer - 1, 1);
                        changes = true;
                    }
                    else if (diffs[pointer].text.StartsWith(diffs[pointer + 1].text,
                      StringComparison.Ordinal))
                    {
                        // Shift the edit over the next equality.
                        diffs[pointer - 1].text += diffs[pointer + 1].text;
                        diffs[pointer].text =
                            diffs[pointer].text.Substring(diffs[pointer + 1].text.Length)
                            + diffs[pointer + 1].text;
                        diffs.Splice(pointer + 1, 1);
                        changes = true;
                    }
                }
                pointer++;
            }
            // If shifts were made, the diff needs reordering and another shift sweep.
            if (changes)
            {
                this.diff_cleanupMerge(diffs);
            }
        }

        /**
         * loc is a location in text1, comAdde and return the equivalent location in
         * text2.
         * e.g. "The cat" vs "The big cat", 1->1, 5->8
         * @param diffs List of Diff objects.
         * @param loc Location within text1.
         * @return Location within text2.
         */
        public int diff_xIndex(List<Diff> diffs, int loc)
        {
            int chars1 = 0;
            int chars2 = 0;
            int last_chars1 = 0;
            int last_chars2 = 0;
            Diff lastDiff = null;
            foreach (Diff aDiff in diffs)
            {
                if (aDiff.operation != Operation.INSERT)
                {
                    // Equality or deletion.
                    chars1 += aDiff.text.Length;
                }
                if (aDiff.operation != Operation.DELETE)
                {
                    // Equality or insertion.
                    chars2 += aDiff.text.Length;
                }
                if (chars1 > loc)
                {
                    // Overshot the location.
                    lastDiff = aDiff;
                    break;
                }
                last_chars1 = chars1;
                last_chars2 = chars2;
            }
            if (lastDiff != null && lastDiff.operation == Operation.DELETE)
            {
                // The location was deleted.
                return last_chars2;
            }
            // Add the remaining character length.
            return last_chars2 + (loc - last_chars1);
        }

        /**
         * Convert a Diff list into a pretty HTML report.
         * @param diffs List of Diff objects.
         * @return HTML representation.
         */
        public string diff_prettyHtml(List<Diff> diffs)
        {
            StringBuilder html = new StringBuilder();
            foreach (Diff aDiff in diffs)
            {
                string text = aDiff.text.Replace("&", "&amp;").Replace("<", "&lt;")
                  .Replace(">", "&gt;").Replace("\n", "&para;<br>");
                switch (aDiff.operation)
                {
                    case Operation.INSERT:
                        html.Append("<ins style=\"background:#e6ffe6;\">").Append(text)
                            .Append("</ins>");
                        break;
                    case Operation.DELETE:
                        html.Append("<del style=\"background:#ffe6e6;\">").Append(text)
                            .Append("</del>");
                        break;
                    case Operation.EQUAL:
                        html.Append("<span>").Append(text).Append("</span>");
                        break;
                }
            }
            return html.ToString();
        }

        /**
         * Compute and return the source text (all equalities and deletions).
         * @param diffs List of Diff objects.
         * @return Source text.
         */
        public string diff_text1(List<Diff> diffs)
        {
            StringBuilder text = new StringBuilder();
            foreach (Diff aDiff in diffs)
            {
                if (aDiff.operation != Operation.INSERT)
                {
                    text.Append(aDiff.text);
                }
            }
            return text.ToString();
        }

        /**
         * Compute and return the destination text (all equalities and insertions).
         * @param diffs List of Diff objects.
         * @return Destination text.
         */
        public string diff_text2(List<Diff> diffs)
        {
            StringBuilder text = new StringBuilder();
            foreach (Diff aDiff in diffs)
            {
                if (aDiff.operation != Operation.DELETE)
                {
                    text.Append(aDiff.text);
                }
            }
            return text.ToString();
        }

        /**
         * Compute the Levenshtein distance; the number of inserted, deleted or
         * substituted characters.
         * @param diffs List of Diff objects.
         * @return Number of changes.
         */
        public int diff_levenshtein(List<Diff> diffs)
        {
            int levenshtein = 0;
            int insertions = 0;
            int deletions = 0;
            foreach (Diff aDiff in diffs)
            {
                switch (aDiff.operation)
                {
                    case Operation.INSERT:
                        insertions += aDiff.text.Length;
                        break;
                    case Operation.DELETE:
                        deletions += aDiff.text.Length;
                        break;
                    case Operation.EQUAL:
                        // A deletion and an insertion is one substitution.
                        levenshtein += Math.Max(insertions, deletions);
                        insertions = 0;
                        deletions = 0;
                        break;
                }
            }
            levenshtein += Math.Max(insertions, deletions);
            return levenshtein;
        }

        /**
         * Crush the diff into an encoded string which describes the operations
         * required to transform text1 into text2.
         * E.g. =3\t-2\t+ing  -> Keep 3 chars, delete 2 chars, insert ''ing''.
         * Operations are tab-separated.  Inserted text is escaped using %xx
         * notation.
         * @param diffs Array of Diff objects.
         * @return Delta text.
         */
        public string diff_toDelta(List<Diff> diffs)
        {
            StringBuilder text = new StringBuilder();
            foreach (Diff aDiff in diffs)
            {
                switch (aDiff.operation)
                {
                    case Operation.INSERT:
                        text.Append("+").Append(CompatibilityExtensions.UrlEncode(aDiff.text).Replace(''+'', '' '')).Append("\t");
                        break;
                    case Operation.DELETE:
                        text.Append("-").Append(aDiff.text.Length).Append("\t");
                        break;
                    case Operation.EQUAL:
                        text.Append("=").Append(aDiff.text.Length).Append("\t");
                        break;
                }
            }
            string delta = text.ToString();
            if (delta.Length != 0)
            {
                // Strip off trailing tab character.
                delta = delta.Substring(0, delta.Length - 1);
                delta = unescapeForEncodeUriCompatability(delta);
            }
            return delta;
        }

        /**
         * Given the original text1, and an encoded string which describes the
         * operations required to transform text1 into text2, comAdde the full diff.
         * @param text1 Source string for the diff.
         * @param delta Delta text.
         * @return Array of Diff objects or null if invalid.
         * @throws ArgumentException If invalid input.
         */
        public List<Diff> diff_fromDelta(string text1, string delta)
        {
            List<Diff> diffs = new List<Diff>();
            int pointer = 0;  // Cursor in text1
            string[] tokens = delta.Split(new string[] { "\t" },
                StringSplitOptions.None);
            foreach (string token in tokens)
            {
                if (token.Length == 0)
                {
                    // Blank tokens are ok (from a trailing \t).
                    continue;
                }
                // Each token begins with a one character parameter which specifies the
                // operation of this token (delete, insert, equality).
                string param = token.Substring(1);
                switch (token[0])
                {
                    case ''+'':
                        // decode would change all "+" to " "
                        param = param.Replace("+", "%2b");

                        param = CompatibilityExtensions.UrlDecode(param);
                        //} catch (UnsupportedEncodingException e) {
                        //  // Not likely on modern system.
                        //  throw new Error("This system does not support UTF-8.", e);
                        //} catch (IllegalArgumentException e) {
                        //  // Malformed URI sequence.
                        //  throw new IllegalArgumentException(
                        //      "Illegal escape in diff_fromDelta: " + param, e);
                        //}
                        diffs.Add(new Diff(Operation.INSERT, param));
                        break;
                    case ''-'':
                    // Fall through.
                    case ''='':
                        int n;
                        try
                        {
                            n = Convert.ToInt32(param);
                        }
                        catch (FormatException e)
                        {
                            throw new ArgumentException(
                                "Invalid number in diff_fromDelta: " + param, e);
                        }
                        if (n < 0)
                        {
                            throw new ArgumentException(
                                "Negative number in diff_fromDelta: " + param);
                        }
                        string text;
                        try
                        {
                            text = text1.Substring(pointer, n);
                            pointer += n;
                        }
                        catch (ArgumentOutOfRangeException e)
                        {
                            throw new ArgumentException("Delta length (" + pointer
                                + ") larger than source text length (" + text1.Length
                                + ").", e);
                        }
                        if (token[0] == ''='')
                        {
                            diffs.Add(new Diff(Operation.EQUAL, text));
                        }
                        else
                        {
                            diffs.Add(new Diff(Operation.DELETE, text));
                        }
                        break;
                    default:
                        // Anything else is an error.
                        throw new ArgumentException(
                            "Invalid diff operation in diff_fromDelta: " + token[0]);
                }
            }
            if (pointer != text1.Length)
            {
                throw new ArgumentException("Delta length (" + pointer
                    + ") smaller than source text length (" + text1.Length + ").");
            }
            return diffs;
        }


        //  MATCH FUNCTIONS


        /**
         * Locate the best instance of ''pattern'' in ''text'' near ''loc''.
         * Returns -1 if no match found.
         * @param text The text to search.
         * @param pattern The pattern to search for.
         * @param loc The location to search around.
         * @return Best match index or -1.
         */
        public int match_main(string text, string pattern, int loc)
        {
            // Check for null inputs not needed since null can''t be passed in C#.

            loc = Math.Max(0, Math.Min(loc, text.Length));
            if (text == pattern)
            {
                // Shortcut (potentially not guaranteed by the algorithm)
                return 0;
            }
            else if (text.Length == 0)
            {
                // Nothing to match.
                return -1;
            }
            else if (loc + pattern.Length <= text.Length
            && text.Substring(loc, pattern.Length) == pattern)
            {
                // Perfect match at the perfect spot!  (Includes case of null pattern)
                return loc;
            }
            else
            {
                // Do a fuzzy compare.
                return match_bitap(text, pattern, loc);
            }
        }

        /**
         * Locate the best instance of ''pattern'' in ''text'' near ''loc'' using the
         * Bitap algorithm.  Returns -1 if no match found.
         * @param text The text to search.
         * @param pattern The pattern to search for.
         * @param loc The location to search around.
         * @return Best match index or -1.
         */
        protected int match_bitap(string text, string pattern, int loc)
        {
            // assert (Match_MaxBits == 0 || pattern.Length <= Match_MaxBits)
            //    : "Pattern too long for this application.";

            // Initialise the alphabet.
            Dictionary<char, int> s = match_alphabet(pattern);

            // Highest score beyond which we give up.
            double score_threshold = Match_Threshold;
            // Is there a nearby exact match? (speedup)
            int best_loc = text.IndexOf(pattern, loc, StringComparison.Ordinal);
            if (best_loc != -1)
            {
                score_threshold = Math.Min(match_bitapScore(0, best_loc, loc,
                    pattern), score_threshold);
                // What about in the other direction? (speedup)
                best_loc = text.LastIndexOf(pattern,
                    Math.Min(loc + pattern.Length, text.Length),
                    StringComparison.Ordinal);
                if (best_loc != -1)
                {
                    score_threshold = Math.Min(match_bitapScore(0, best_loc, loc,
                        pattern), score_threshold);
                }
            }

            // Initialise the bit arrays.
            int matchmask = 1 << (pattern.Length - 1);
            best_loc = -1;

            int bin_min, bin_mid;
            int bin_max = pattern.Length + text.Length;
            // Empty initialization added to appease C# compiler.
            int[] last_rd = new int[0];
            for (int d = 0; d < pattern.Length; d++)
            {
                // Scan for the best match; each iteration allows for one more error.
                // Run a binary search to determine how far from ''loc'' we can stray at
                // this error level.
                bin_min = 0;
                bin_mid = bin_max;
                while (bin_min < bin_mid)
                {
                    if (match_bitapScore(d, loc + bin_mid, loc, pattern)
                        <= score_threshold)
                    {
                        bin_min = bin_mid;
                    }
                    else
                    {
                        bin_max = bin_mid;
                    }
                    bin_mid = (bin_max - bin_min) / 2 + bin_min;
                }
                // Use the result from this iteration as the maximum for the next.
                bin_max = bin_mid;
                int start = Math.Max(1, loc - bin_mid + 1);
                int finish = Math.Min(loc + bin_mid, text.Length) + pattern.Length;

                int[] rd = new int[finish + 2];
                rd[finish + 1] = (1 << d) - 1;
                for (int j = finish; j >= start; j--)
                {
                    int charMatch;
                    if (text.Length <= j - 1 || !s.ContainsKey(text[j - 1]))
                    {
                        // Out of range.
                        charMatch = 0;
                    }
                    else
                    {
                        charMatch = s[text[j - 1]];
                    }
                    if (d == 0)
                    {
                        // First pass: exact match.
                        rd[j] = ((rd[j + 1] << 1) | 1) & charMatch;
                    }
                    else
                    {
                        // Subsequent passes: fuzzy match.
                        rd[j] = ((rd[j + 1] << 1) | 1) & charMatch
                            | (((last_rd[j + 1] | last_rd[j]) << 1) | 1) | last_rd[j + 1];
                    }
                    if ((rd[j] & matchmask) != 0)
                    {
                        double score = match_bitapScore(d, j - 1, loc, pattern);
                        // This match will almost certainly be better than any existing
                        // match.  But check anyway.
                        if (score <= score_threshold)
                        {
                            // Told you so.
                            score_threshold = score;
                            best_loc = j - 1;
                            if (best_loc > loc)
                            {
                                // When passing loc, don''t exceed our current distance from loc.
                                start = Math.Max(1, 2 * loc - best_loc);
                            }
                            else
                            {
                                // Already passed loc, downhill from here on in.
                                break;
                            }
                        }
                    }
                }
                if (match_bitapScore(d + 1, loc, loc, pattern) > score_threshold)
                {
                    // No hope for a (better) match at greater error levels.
                    break;
                }
                last_rd = rd;
            }
            return best_loc;
        }

        /**
         * Compute and return the score for a match with e errors and x location.
         * @param e Number of errors in match.
         * @param x Location of match.
         * @param loc Expected location of match.
         * @param pattern Pattern being sought.
         * @return Overall score for match (0.0 = good, 1.0 = bad).
         */
        private double match_bitapScore(int e, int x, int loc, string pattern)
        {
            float accuracy = (float)e / pattern.Length;
            int proximity = Math.Abs(loc - x);
            if (Match_Distance == 0)
            {
                // Dodge divide by zero error.
                return proximity == 0 ? accuracy : 1.0;
            }
            return accuracy + (proximity / (float)Match_Distance);
        }

        /**
         * Initialise the alphabet for the Bitap algorithm.
         * @param pattern The text to encode.
         * @return Hash of character locations.
         */
        protected Dictionary<char, int> match_alphabet(string pattern)
        {
            Dictionary<char, int> s = new Dictionary<char, int>();
            char[] char_pattern = pattern.ToCharArray();
            foreach (char c in char_pattern)
            {
                if (!s.ContainsKey(c))
                {
                    s.Add(c, 0);
                }
            }
            int i = 0;
            foreach (char c in char_pattern)
            {
                int value = s[c] | (1 << (pattern.Length - i - 1));
                s[c] = value;
                i++;
            }
            return s;
        }


        //  PATCH FUNCTIONS


        /**
         * Increase the context until it is unique,
         * but don''t let the pattern expand beyond Match_MaxBits.
         * @param patch The patch to grow.
         * @param text Source text.
         */
        protected void patch_addContext(Patch patch, string text)
        {
            if (text.Length == 0)
            {
                return;
            }
            string pattern = text.Substring(patch.start2, patch.length1);
            int padding = 0;

            // Look for the first and last matches of pattern in text.  If two
            // different matches are found, increase the pattern length.
            while (text.IndexOf(pattern, StringComparison.Ordinal)
                != text.LastIndexOf(pattern, StringComparison.Ordinal)
                && pattern.Length < Match_MaxBits - Patch_Margin - Patch_Margin)
            {
                padding += Patch_Margin;
                pattern = text.JavaSubstring(Math.Max(0, patch.start2 - padding),
                    Math.Min(text.Length, patch.start2 + patch.length1 + padding));
            }
            // Add one chunk for good luck.
            padding += Patch_Margin;

            // Add the prefix.
            string prefix = text.JavaSubstring(Math.Max(0, patch.start2 - padding),
              patch.start2);
            if (prefix.Length != 0)
            {
                patch.diffs.Insert(0, new Diff(Operation.EQUAL, prefix));
            }
            // Add the suffix.
            string suffix = text.JavaSubstring(patch.start2 + patch.length1,
                Math.Min(text.Length, patch.start2 + patch.length1 + padding));
            if (suffix.Length != 0)
            {
                patch.diffs.Add(new Diff(Operation.EQUAL, suffix));
            }

            // Roll back the start points.
            patch.start1 -= prefix.Length;
            patch.start2 -= prefix.Length;
            // Extend the lengths.
            patch.length1 += prefix.Length + suffix.Length;
            patch.length2 += prefix.Length + suffix.Length;
        }

        /**
         * Compute a list of patches to turn text1 into text2.
         * A set of diffs will be computed.
         * @param text1 Old text.
         * @param text2 New text.
         * @return List of Patch objects.
         */
        public List<Patch> patch_make(string text1, string text2)
        {
            // Check for null inputs not needed since null can''t be passed in C#.
            // No diffs provided, comAdde our own.
            List<Diff> diffs = diff_main(text1, text2, true);
            if (diffs.Count > 2)
            {
                diff_cleanupSemantic(diffs);
                diff_cleanupEfficiency(diffs);
            }
            return patch_make(text1, diffs);
        }

        /**
         * Compute a list of patches to turn text1 into text2.
         * text1 will be derived from the provided diffs.
         * @param diffs Array of Diff objects for text1 to text2.
         * @return List of Patch objects.
         */
        public List<Patch> patch_make(List<Diff> diffs)
        {
            // Check for null inputs not needed since null can''t be passed in C#.
            // No origin string provided, comAdde our own.
            string text1 = diff_text1(diffs);
            return patch_make(text1, diffs);
        }

        /**
         * Compute a list of patches to turn text1 into text2.
         * text2 is ignored, diffs are the delta between text1 and text2.
         * @param text1 Old text
         * @param text2 Ignored.
         * @param diffs Array of Diff objects for text1 to text2.
         * @return List of Patch objects.
         * @deprecated Prefer patch_make(string text1, List<Diff> diffs).
         */
        public List<Patch> patch_make(string text1, string text2,
            List<Diff> diffs)
        {
            return patch_make(text1, diffs);
        }

        /**
         * Compute a list of patches to turn text1 into text2.
         * text2 is not provided, diffs are the delta between text1 and text2.
         * @param text1 Old text.
         * @param diffs Array of Diff objects for text1 to text2.
         * @return List of Patch objects.
         */
        public List<Patch> patch_make(string text1, List<Diff> diffs)
        {
            // Check for null inputs not needed since null can''t be passed in C#.
            List<Patch> patches = new List<Patch>();
            if (diffs.Count == 0)
            {
                return patches;  // Get rid of the null case.
            }
            Patch patch = new Patch();
            int char_count1 = 0;  // Number of characters into the text1 string.
            int char_count2 = 0;  // Number of characters into the text2 string.
            // Start with text1 (prepatch_text) and apply the diffs until we arrive at
            // text2 (postpatch_text). We recreate the patches one by one to determine
            // context info.
            string prepatch_text = text1;
            string postpatch_text = text1;
            foreach (Diff aDiff in diffs)
            {
                if (patch.diffs.Count == 0 && aDiff.operation != Operation.EQUAL)
                {
                    // A new patch starts here.
                    patch.start1 = char_count1;
                    patch.start2 = char_count2;
                }

                switch (aDiff.operation)
                {
                    case Operation.INSERT:
                        patch.diffs.Add(aDiff);
                        patch.length2 += aDiff.text.Length;
                        postpatch_text = postpatch_text.Insert(char_count2, aDiff.text);
                        break;
                    case Operation.DELETE:
                        patch.length1 += aDiff.text.Length;
                        patch.diffs.Add(aDiff);
                        postpatch_text = postpatch_text.Remove(char_count2,
                            aDiff.text.Length);
                        break;
                    case Operation.EQUAL:
                        if (aDiff.text.Length <= 2 * Patch_Margin
                            && patch.diffs.Count() != 0 && aDiff != diffs.Last())
                        {
                            // Small equality inside a patch.
                            patch.diffs.Add(aDiff);
                            patch.length1 += aDiff.text.Length;
                            patch.length2 += aDiff.text.Length;
                        }

                        if (aDiff.text.Length >= 2 * Patch_Margin)
                        {
                            // Time for a new patch.
                            if (patch.diffs.Count != 0)
                            {
                                patch_addContext(patch, prepatch_text);
                                patches.Add(patch);
                                patch = new Patch();
                                // Unlike Unidiff, our patch lists have a rolling context.
                                // http://code.google.com/p/google-diff-match-patch/wiki/Unidiff
                                // Update prepatch text & pos to reflect the application of the
                                // just completed patch.
                                prepatch_text = postpatch_text;
                                char_count1 = char_count2;
                            }
                        }
                        break;
                }

                // Update the current character count.
                if (aDiff.operation != Operation.INSERT)
                {
                    char_count1 += aDiff.text.Length;
                }
                if (aDiff.operation != Operation.DELETE)
                {
                    char_count2 += aDiff.text.Length;
                }
            }
            // Pick up the leftover patch if not empty.
            if (patch.diffs.Count != 0)
            {
                patch_addContext(patch, prepatch_text);
                patches.Add(patch);
            }

            return patches;
        }

        /**
         * Given an array of patches, return another array that is identical.
         * @param patches Array of Patch objects.
         * @return Array of Patch objects.
         */
        public List<Patch> patch_deepCopy(List<Patch> patches)
        {
            List<Patch> patchesCopy = new List<Patch>();
            foreach (Patch aPatch in patches)
            {
                Patch patchCopy = new Patch();
                foreach (Diff aDiff in aPatch.diffs)
                {
                    Diff diffCopy = new Diff(aDiff.operation, aDiff.text);
                    patchCopy.diffs.Add(diffCopy);
                }
                patchCopy.start1 = aPatch.start1;
                patchCopy.start2 = aPatch.start2;
                patchCopy.length1 = aPatch.length1;
                patchCopy.length2 = aPatch.length2;
                patchesCopy.Add(patchCopy);
            }
            return patchesCopy;
        }

        /**
         * Merge a set of patches onto the text.  Return a patched text, as well
         * as an array of true/false values indicating which patches were applied.
         * @param patches Array of Patch objects
         * @param text Old text.
         * @return Two element Object array, containing the new text and an array of
         *      bool values.
         */
        public Object[] patch_apply(List<Patch> patches, string text)
        {
            if (patches.Count == 0)
            {
                return new Object[] { text, new bool[0] };
            }

            // Deep copy the patches so that no changes are made to originals.
            patches = patch_deepCopy(patches);

            string nullPadding = this.patch_addPadding(patches);
            text = nullPadding + text + nullPadding;
            patch_splitMax(patches);

            int x = 0;
            // delta keeps track of the offset between the expected and actual
            // location of the previous patch.  If there are patches expected at
            // positions 10 and 20, but the first patch was found at 12, delta is 2
            // and the second patch has an effective expected position of 22.
            int delta = 0;
            bool[] results = new bool[patches.Count];
            foreach (Patch aPatch in patches)
            {
                int expected_loc = aPatch.start2 + delta;
                string text1 = diff_text1(aPatch.diffs);
                int start_loc;
                int end_loc = -1;
                if (text1.Length > this.Match_MaxBits)
                {
                    // patch_splitMax will only provide an oversized pattern
                    // in the case of a monster delete.
                    start_loc = match_main(text,
                        text1.Substring(0, this.Match_MaxBits), expected_loc);
                    if (start_loc != -1)
                    {
                        end_loc = match_main(text,
                            text1.Substring(text1.Length - this.Match_MaxBits),
                            expected_loc + text1.Length - this.Match_MaxBits);
                        if (end_loc == -1 || start_loc >= end_loc)
                        {
                            // Can''t find valid trailing context.  Drop this patch.
                            start_loc = -1;
                        }
                    }
                }
                else
                {
                    start_loc = this.match_main(text, text1, expected_loc);
                }
                if (start_loc == -1)
                {
                    // No match found.  :(
                    results[x] = false;
                    // Subtract the delta for this failed patch from subsequent patches.
                    delta -= aPatch.length2 - aPatch.length1;
                }
                else
                {
                    // Found a match.  :)
                    results[x] = true;
                    delta = start_loc - expected_loc;
                    string text2;
                    if (end_loc == -1)
                    {
                        text2 = text.JavaSubstring(start_loc,
                            Math.Min(start_loc + text1.Length, text.Length));
                    }
                    else
                    {
                        text2 = text.JavaSubstring(start_loc,
                            Math.Min(end_loc + this.Match_MaxBits, text.Length));
                    }
                    if (text1 == text2)
                    {
                        // Perfect match, just shove the Replacement text in.
                        text = text.Substring(0, start_loc) + diff_text2(aPatch.diffs)
                            + text.Substring(start_loc + text1.Length);
                    }
                    else
                    {
                        // Imperfect match.  Run a diff to get a framework of equivalent
                        // indices.
                        List<Diff> diffs = diff_main(text1, text2, false);
                        if (text1.Length > this.Match_MaxBits
                            && this.diff_levenshtein(diffs) / (float)text1.Length
                            > this.Patch_DeleteThreshold)
                        {
                            // The end points match, but the content is unacceptably bad.
                            results[x] = false;
                        }
                        else
                        {
                            diff_cleanupSemanticLossless(diffs);
                            int index1 = 0;
                            foreach (Diff aDiff in aPatch.diffs)
                            {
                                if (aDiff.operation != Operation.EQUAL)
                                {
                                    int index2 = diff_xIndex(diffs, index1);
                                    if (aDiff.operation == Operation.INSERT)
                                    {
                                        // Insertion
                                        text = text.Insert(start_loc + index2, aDiff.text);
                                    }
                                    else if (aDiff.operation == Operation.DELETE)
                                    {
                                        // Deletion
                                        text = text.Remove(start_loc + index2, diff_xIndex(diffs,
                                            index1 + aDiff.text.Length) - index2);
                                    }
                                }
                                if (aDiff.operation != Operation.DELETE)
                                {
                                    index1 += aDiff.text.Length;
                                }
                            }
                        }
                    }
                }
                x++;
            }
            // Strip the padding off.
            text = text.Substring(nullPadding.Length, text.Length
                - 2 * nullPadding.Length);
            return new Object[] { text, results };
        }

        /**
         * Add some padding on text start and end so that edges can match something.
         * Intended to be called only from within patch_apply.
         * @param patches Array of Patch objects.
         * @return The padding string added to each side.
         */
        public string patch_addPadding(List<Patch> patches)
        {
            short paddingLength = this.Patch_Margin;
            string nullPadding = string.Empty;
            for (short x = 1; x <= paddingLength; x++)
            {
                nullPadding += (char)x;
            }

            // Bump all the patches forward.
            foreach (Patch aPatch in patches)
            {
                aPatch.start1 += paddingLength;
                aPatch.start2 += paddingLength;
            }

            // Add some padding on start of first diff.
            Patch patch = patches.First();
            List<Diff> diffs = patch.diffs;
            if (diffs.Count == 0 || diffs.First().operation != Operation.EQUAL)
            {
                // Add nullPadding equality.
                diffs.Insert(0, new Diff(Operation.EQUAL, nullPadding));
                patch.start1 -= paddingLength;  // Should be 0.
                patch.start2 -= paddingLength;  // Should be 0.
                patch.length1 += paddingLength;
                patch.length2 += paddingLength;
            }
            else if (paddingLength > diffs.First().text.Length)
            {
                // Grow first equality.
                Diff firstDiff = diffs.First();
                int extraLength = paddingLength - firstDiff.text.Length;
                firstDiff.text = nullPadding.Substring(firstDiff.text.Length)
                    + firstDiff.text;
                patch.start1 -= extraLength;
                patch.start2 -= extraLength;
                patch.length1 += extraLength;
                patch.length2 += extraLength;
            }

            // Add some padding on end of last diff.
            patch = patches.Last();
            diffs = patch.diffs;
            if (diffs.Count == 0 || diffs.Last().operation != Operation.EQUAL)
            {
                // Add nullPadding equality.
                diffs.Add(new Diff(Operation.EQUAL, nullPadding));
                patch.length1 += paddingLength;
                patch.length2 += paddingLength;
            }
            else if (paddingLength > diffs.Last().text.Length)
            {
                // Grow last equality.
                Diff lastDiff = diffs.Last();
                int extraLength = paddingLength - lastDiff.text.Length;
                lastDiff.text += nullPadding.Substring(0, extraLength);
                patch.length1 += extraLength;
                patch.length2 += extraLength;
            }

            return nullPadding;
        }

        /**
         * Look through the patches and break up any which are longer than the
         * maximum limit of the match algorithm.
         * Intended to be called only from within patch_apply.
         * @param patches List of Patch objects.
         */
        public void patch_splitMax(List<Patch> patches)
        {
            short patch_size = this.Match_MaxBits;
            for (int x = 0; x < patches.Count; x++)
            {
                if (patches[x].length1 <= patch_size)
                {
                    continue;
                }
                Patch bigpatch = patches[x];
                // Remove the big old patch.
                patches.Splice(x--, 1);
                int start1 = bigpatch.start1;
                int start2 = bigpatch.start2;
                string precontext = string.Empty;
                while (bigpatch.diffs.Count != 0)
                {
                    // Create one of several smaller patches.
                    Patch patch = new Patch();
                    bool empty = true;
                    patch.start1 = start1 - precontext.Length;
                    patch.start2 = start2 - precontext.Length;
                    if (precontext.Length != 0)
                    {
                        patch.length1 = patch.length2 = precontext.Length;
                        patch.diffs.Add(new Diff(Operation.EQUAL, precontext));
                    }
                    while (bigpatch.diffs.Count != 0
                        && patch.length1 < patch_size - this.Patch_Margin)
                    {
                        Operation diff_type = bigpatch.diffs[0].operation;
                        string diff_text = bigpatch.diffs[0].text;
                        if (diff_type == Operation.INSERT)
                        {
                            // Insertions are harmless.
                            patch.length2 += diff_text.Length;
                            start2 += diff_text.Length;
                            patch.diffs.Add(bigpatch.diffs.First());
                            bigpatch.diffs.RemoveAt(0);
                            empty = false;
                        }
                        else if (diff_type == Operation.DELETE && patch.diffs.Count == 1
                          && patch.diffs.First().operation == Operation.EQUAL
                          && diff_text.Length > 2 * patch_size)
                        {
                            // This is a large deletion.  Let it pass in one chunk.
                            patch.length1 += diff_text.Length;
                            start1 += diff_text.Length;
                            empty = false;
                            patch.diffs.Add(new Diff(diff_type, diff_text));
                            bigpatch.diffs.RemoveAt(0);
                        }
                        else
                        {
                            // Deletion or equality.  Only take as much as we can stomach.
                            diff_text = diff_text.Substring(0, Math.Min(diff_text.Length,
                                patch_size - patch.length1 - Patch_Margin));
                            patch.length1 += diff_text.Length;
                            start1 += diff_text.Length;
                            if (diff_type == Operation.EQUAL)
                            {
                                patch.length2 += diff_text.Length;
                                start2 += diff_text.Length;
                            }
                            else
                            {
                                empty = false;
                            }
                            patch.diffs.Add(new Diff(diff_type, diff_text));
                            if (diff_text == bigpatch.diffs[0].text)
                            {
                                bigpatch.diffs.RemoveAt(0);
                            }
                            else
                            {
                                bigpatch.diffs[0].text =
                                    bigpatch.diffs[0].text.Substring(diff_text.Length);
                            }
                        }
                    }
                    // Compute the head context for the next patch.
                    precontext = this.diff_text2(patch.diffs);
                    precontext = precontext.Substring(Math.Max(0,
                        precontext.Length - this.Patch_Margin));

                    string postcontext = null;
                    // Append the end context for this patch.
                    if (diff_text1(bigpatch.diffs).Length > Patch_Margin)
                    {
                        postcontext = diff_text1(bigpatch.diffs)
                            .Substring(0, Patch_Margin);
                    }
                    else
                    {
                        postcontext = diff_text1(bigpatch.diffs);
                    }

                    if (postcontext.Length != 0)
                    {
                        patch.length1 += postcontext.Length;
                        patch.length2 += postcontext.Length;
                        if (patch.diffs.Count != 0
                            && patch.diffs[patch.diffs.Count - 1].operation
                            == Operation.EQUAL)
                        {
                            patch.diffs[patch.diffs.Count - 1].text += postcontext;
                        }
                        else
                        {
                            patch.diffs.Add(new Diff(Operation.EQUAL, postcontext));
                        }
                    }
                    if (!empty)
                    {
                        patches.Splice(++x, 0, patch);
                    }
                }
            }
        }

        /**
         * Take a list of patches and return a textual representation.
         * @param patches List of Patch objects.
         * @return Text representation of patches.
         */
        public string patch_toText(List<Patch> patches)
        {
            StringBuilder text = new StringBuilder();
            foreach (Patch aPatch in patches)
            {
                text.Append(aPatch);
            }
            return text.ToString();
        }

        /**
         * Parse a textual representation of patches and return a List of Patch
         * objects.
         * @param textline Text representation of patches.
         * @return List of Patch objects.
         * @throws ArgumentException If invalid input.
         */
        public List<Patch> patch_fromText(string textline)
        {
            List<Patch> patches = new List<Patch>();
            if (textline.Length == 0)
            {
                return patches;
            }
            string[] text = textline.Split(''\n'');
            int textPointer = 0;
            Patch patch;
            Regex patchHeader
                = new Regex("^@@ -(\\d+),?(\\d*) \\+(\\d+),?(\\d*) @@$");
            Match m;
            char sign;
            string line;
            while (textPointer < text.Length)
            {
                m = patchHeader.Match(text[textPointer]);
                if (!m.Success)
                {
                    throw new ArgumentException("Invalid patch string: "
                        + text[textPointer]);
                }
                patch = new Patch();
                patches.Add(patch);
                patch.start1 = Convert.ToInt32(m.Groups[1].Value);
                if (m.Groups[2].Length == 0)
                {
                    patch.start1--;
                    patch.length1 = 1;
                }
                else if (m.Groups[2].Value == "0")
                {
                    patch.length1 = 0;
                }
                else
                {
                    patch.start1--;
                    patch.length1 = Convert.ToInt32(m.Groups[2].Value);
                }

                patch.start2 = Convert.ToInt32(m.Groups[3].Value);
                if (m.Groups[4].Length == 0)
                {
                    patch.start2--;
                    patch.length2 = 1;
                }
                else if (m.Groups[4].Value == "0")
                {
                    patch.length2 = 0;
                }
                else
                {
                    patch.start2--;
                    patch.length2 = Convert.ToInt32(m.Groups[4].Value);
                }
                textPointer++;

                while (textPointer < text.Length)
                {
                    try
                    {
                        sign = text[textPointer][0];
                    }
                    catch (IndexOutOfRangeException)
                    {
                        // Blank line?  Whatever.
                        textPointer++;
                        continue;
                    }
                    line = text[textPointer].Substring(1);
                    line = line.Replace("+", "%2b");
                    line = CompatibilityExtensions.UrlDecode(line);
                    if (sign == ''-'')
                    {
                        // Deletion.
                        patch.diffs.Add(new Diff(Operation.DELETE, line));
                    }
                    else if (sign == ''+'')
                    {
                        // Insertion.
                        patch.diffs.Add(new Diff(Operation.INSERT, line));
                    }
                    else if (sign == '' '')
                    {
                        // Minor equality.
                        patch.diffs.Add(new Diff(Operation.EQUAL, line));
                    }
                    else if (sign == ''@'')
                    {
                        // Start of next patch.
                        break;
                    }
                    else
                    {
                        // WTF?
                        throw new ArgumentException(
                            "Invalid patch mode ''" + sign + "'' in: " + line);
                    }
                    textPointer++;
                }
            }
            return patches;
        }

        /**
         * Unescape selected chars for compatability with JavaScript''s encodeURI.
         * In speed critical applications this could be dropped since the
         * receiving application will certainly decode these fine.
         * Note that this function is case-sensitive.  Thus "%3F" would not be
         * unescaped.  But this is ok because it is only called with the output of
         * HttpUtility.UrlEncode which returns lowercase hex.
         *
         * Example: "%3f" -> "?", "%24" -> "$", etc.
         *
         * @param str The string to escape.
         * @return The escaped string.
         */
        public static string unescapeForEncodeUriCompatability(string str)
        {
            return str.Replace("%21", "!").Replace("%7e", "~")
                .Replace("%27", "''").Replace("%28", "(").Replace("%29", ")")
                .Replace("%3b", ";").Replace("%2f", "/").Replace("%3f", "?")
                .Replace("%3a", ":").Replace("%40", "@").Replace("%26", "&")
                .Replace("%3d", "=").Replace("%2b", "+").Replace("%24", "$")
                .Replace("%2c", ",").Replace("%23", "#");
        }
    }
}

/////////////////////////////////////
public partial class DiffMatch
{

    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlChars DiffMatchHTML(SqlChars text1, SqlChars text2)
    {
        string result;
        try
        {
            DiffMatchPatch.diff_match_patch diff = new DiffMatchPatch.diff_match_patch();


            var a = diff.diff_linesToChars(new string(text1.Value), new string(text2.Value));
            string lineText1 = (string)a[0];
            string lineText2 = (string)a[1];
            List<string> lineArray = (List<string>)a[2];

            var diffs = diff.diff_main(lineText1, lineText2, false);

            // Use cleaupSemantic here if you want to stay at the line level.
            diff.diff_cleanupSemantic(diffs);

            diff.diff_charsToLines(diffs, lineArray);


            //                var diffs = diff.diff_main(new string(text1.Value), new string(text2.Value));
            //                diff.diff_cleanupSemantic(diffs);
            var html = diff.diff_prettyHtml(diffs);

            //Return the response
            result = html;
        }
        catch (Exception ex)
        {
            //Return the exception 
            result = ex.Message;
        }
        return new SqlChars(result);
    }


    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlChars UrlEncode(SqlChars s)
    {
        return new SqlChars(DiffMatchPatch.CompatibilityExtensions.UrlEncode(new string(s.Value)));
    }


    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlChars UrlDecode(SqlChars s)
    {
        return new SqlChars(DiffMatchPatch.CompatibilityExtensions.UrlDecode(new string(s.Value)));
    }


    private class DiffInfo
    {
        public SqlInt32 DiffSequence;
        public SqlChars DiffText;
        public SqlString DiffOperation;
        public SqlString DiffIndication;
        public DiffInfo(SqlInt32 diffSequence, SqlChars diffText, SqlString diffOperation,
        SqlString diffIndication)
        {
            DiffSequence = diffSequence;
            DiffText = diffText;
            DiffOperation = diffOperation;
            DiffIndication = diffIndication;
        }
    }
    [Microsoft.SqlServer.Server.SqlFunction(
        FillRowMethodName = "FillDiffArrayList",
        TableDefinition = "diffSequence int, diffText nvarchar(MAX), diffOperation nvarchar(40), diffIndication nvarchar(40)")]
    public static IEnumerable GetDiffs (SqlChars text1, SqlChars text2)
    {
        try
        {
            DiffMatchPatch.diff_match_patch diff = new DiffMatchPatch.diff_match_patch();

            var a = diff.diff_linesToChars(new string(text1.Value), new string(text2.Value));
            string lineText1 = (string)a[0];
            string lineText2 = (string)a[1];
            List<string> lineArray = (List<string>)a[2];

            var diffs = diff.diff_main(lineText1, lineText2, false);

            // Use cleaupSemantic here if you want to stay at the line level.
            diff.diff_cleanupSemantic(diffs);

            diff.diff_charsToLines(diffs, lineArray);

            ArrayList diffsArrayList = new ArrayList();

            string thisDiffOperation = null;
            string thisDiffIndicator = null;

            foreach (DiffMatchPatch.Diff thisDiff in diffs)
            {

                switch (thisDiff.operation)
                {
                    case DiffMatchPatch.Operation.INSERT:
                        thisDiffOperation = "Insert";
                        thisDiffIndicator = ">>>";
                        break;
                    case DiffMatchPatch.Operation.DELETE:
                        thisDiffOperation = "Delete";
                        thisDiffIndicator = "<<<";
                        break;
                    case DiffMatchPatch.Operation.EQUAL:
                        thisDiffOperation = "Equal";
                        thisDiffIndicator = "===";
                        break;
                }
                diffsArrayList.Add(new DiffInfo(
                    new SqlInt32(diffsArrayList.Count + 1),
                    new SqlChars(thisDiff.text),
                    new SqlString(thisDiffOperation),
                    new SqlString(thisDiffIndicator))
                );
            }

            return diffsArrayList;
        }
        catch
        {
            return null;
        }
    }

    //FillRow method. The method name has been specified above as 
    //a SqlFunction attribute property
    public static void FillDiffArrayList(
        object objDiff,
        out SqlInt32 diffSequence,
        out SqlChars diffText,
        out SqlString diffOperation, 
        out SqlString diffIndicator)
    {
        DiffInfo di= (DiffInfo)objDiff;
        diffSequence = di.DiffSequence;
        diffText = di.DiffText;
        diffOperation = di.DiffOperation;
        diffIndicator = di.DiffIndication;
    }

};
//------end of CLR Source------
'


  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'DiffMatchCLR',
    @FileName = 'DiffMatchCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_SQLVerUtil]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_SQLVerUtil]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_SQLVerUtil]
--$!SQLVer Jun  1 2022  6:28AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'
  
  DECLARE @AssemblyName sysname
  SET @AssemblyName = 'SQLVerUtilCLR'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))


  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.GetMP3Info_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.GetMP3Info_CLR;
    END

    IF OBJECT_ID(''sqlver.udfCheckHost_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfCheckHost_CLR;
    END

    IF OBJECT_ID(''sqlver.udfPing_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfPing_CLR;
    END

    '

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    
    CREATE FUNCTION sqlver.GetMP3Info_CLR (     
      @MP3Data varbinary(MAX)          
    )
    RETURNS nvarchar(MAX)  
    AS
      EXTERNAL NAME [' + @AssemblyName + '].[Functions].[GetMP3Info]

    ~

    CREATE FUNCTION sqlver.udfCheckHost_CLR (     
      @Hostname sysname,
      @intPort smallint
    )
    RETURNS bit
    AS
      EXTERNAL NAME [' + @AssemblyName + '].[Functions].[CheckHost]

    ~

    CREATE FUNCTION sqlver.udfPing_CLR (     
      @Hostname sysname          
    )
    RETURNS bit  
    AS
      EXTERNAL NAME [' + @AssemblyName + '].[Functions].[Ping]

  '



      
  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

/////////////////////////////////////
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/////////////////////////////////////
using System.Net;
using System.Net.NetworkInformation;
// for Ping

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("SQLVerUtilCLR")]
[assembly: AssemblyDescription("drueter@assyst.com (David Rueter)")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("OpsStream, LLC")]
[assembly: AssemblyProduct("SQLVerUtilCLR")]
[assembly: AssemblyCopyright("Copyright 2017 OpsStream, LLC  All Rights Reserved.")]
[assembly: AssemblyTrademark("OpsStream")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("4A7C6334-DB7F-447E-83CA-BE073A0CF536")]


// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the ''*'' as shown below:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("1.0.0.0")]
[assembly: AssemblyFileVersion("1.0.0.0")]


  public partial class Functions
{

    public class MP3Info
    {
        /*
        Big thanks to:
        Robert Wlodarczyk
        https://www.linkedin.com/in/robertwlodarczyk
        Reading MP3 Headers 
        http://web.archive.org/web/20080801005127/http://www.devhood.com/tutorials/tutorial_details.aspx?tutorial_id=79
        */
        
        
        // Public variables for storing the information about the MP3
        public int intBitRate;
        public string strFileName;
        public long lngFileSize;
        public int intFrequency;
        public string strMode;
        public int intLength;
        public string strLengthFormatted;

        // Private variables used in the process of reading in the MP3 files
        private ulong bithdr;
        private bool boolVBitRate;
        private int intVFrames;


        public bool ReadMP3Information(byte[] MP3Data)
        {
            MemoryStream fs = new MemoryStream(MP3Data);
            strFileName = "{stream}";


            // Set the file size
            lngFileSize = fs.Length;

            byte[] bytHeader = new byte[4];
            byte[] bytVBitRate = new byte[12];
            int intPos = 0;

            // Keep reading 4 bytes from the header until we know for sure that in 
            // fact it is an MP3
            do
            {
                fs.Position = intPos;
                fs.Read(bytHeader, 0, 4);
                intPos++;
                LoadMP3Header(bytHeader);
            }
            while (!IsValidHeader() && (fs.Position != fs.Length));

            // If the current file stream position is equal to the length, 
            // that means that we have read the entire file and it is not a valid MP3 file
            if (fs.Position != fs.Length)
            {
                intPos += 3;

                if (getVersionIndex() == 3)    // MPEG Version 1
                {
                    if (getModeIndex() == 3)    // Single Channel
                    {
                        intPos += 17;
                    }
                    else
                    {
                        intPos += 32;
                    }
                }
                else                        // MPEG Version 2.0 or 2.5
                {
                    if (getModeIndex() == 3)    // Single Channel
                    {
                        intPos += 9;
                    }
                    else
                    {
                        intPos += 17;
                    }
                }

                // Check to see if the MP3 has a variable bitrate
                fs.Position = intPos;
                fs.Read(bytVBitRate, 0, 12);
                boolVBitRate = LoadVBRHeader(bytVBitRate);

                // Once the file is read in, then assign the properties of the file to the public variables
                intBitRate = getBitrate();
                intFrequency = getFrequency();
                strMode = getMode();
                intLength = getLengthInSeconds();
                strLengthFormatted = getFormattedLength();
                fs.Close();
                return true;
            }
            return false;
        }

        private void LoadMP3Header(byte[] c)
        {
            // this thing is quite interesting, it works like the following
            // c[0] = 00000011
            // c[1] = 00001100
            // c[2] = 00110000
            // c[3] = 11000000
            // the operator << means that we will move the bits in that direction
            // 00000011 << 24 = 00000011000000000000000000000000
            // 00001100 << 16 =         000011000000000000000000
            // 00110000 << 24 =                 0011000000000000
            // 11000000       =                         11000000
            //                +_________________________________
            //                  00000011000011000011000011000000
            bithdr = (ulong)(((c[0] & 255) << 24) | ((c[1] & 255) << 16) | ((c[2] & 255) << 8) | ((c[3] & 255)));
        }

        private bool LoadVBRHeader(byte[] inputheader)
        {
            // If it is a variable bitrate MP3, the first 4 bytes will read Xing
            // since they are the ones who added variable bitrate-edness to MP3s
            if (inputheader[0] == 88 && inputheader[1] == 105 &&
                inputheader[2] == 110 && inputheader[3] == 103)
            {
                int flags = (int)(((inputheader[4] & 255) << 24) | ((inputheader[5] & 255) << 16) | ((inputheader[6] & 255) << 8) | ((inputheader[7] & 255)));
                if ((flags & 0x0001) == 1)
                {
                    intVFrames = (int)(((inputheader[8] & 255) << 24) | ((inputheader[9] & 255) << 16) | ((inputheader[10] & 255) << 8) | ((inputheader[11] & 255)));
                    return true;
                }
                else
                {
                    intVFrames = -1;
                    return true;
                }
            }
            return false;
        }

        private bool IsValidHeader()
        {
            return (((getFrameSync() & 2047) == 2047) &&
                    ((getVersionIndex() & 3) != 1) &&
                    ((getLayerIndex() & 3) != 0) &&
                    ((getBitrateIndex() & 15) != 0) &&
                    ((getBitrateIndex() & 15) != 15) &&
                    ((getFrequencyIndex() & 3) != 3) &&
                    ((getEmphasisIndex() & 3) != 2));
        }

        private int getFrameSync()
        {
            return (int)((bithdr >> 21) & 2047);
        }

        private int getVersionIndex()
        {
            return (int)((bithdr >> 19) & 3);
        }

        private int getLayerIndex()
        {
            return (int)((bithdr >> 17) & 3);
        }

        private int getProtectionBit()
        {
            return (int)((bithdr >> 16) & 1);
        }

        private int getBitrateIndex()
        {
            return (int)((bithdr >> 12) & 15);
        }

        private int getFrequencyIndex()
        {
            return (int)((bithdr >> 10) & 3);
        }

        private int getPaddingBit()
        {
            return (int)((bithdr >> 9) & 1);
        }

        private int getPrivateBit()
        {
            return (int)((bithdr >> 8) & 1);
        }

        private int getModeIndex()
        {
            return (int)((bithdr >> 6) & 3);
        }

        private int getModeExtIndex()
        {
            return (int)((bithdr >> 4) & 3);
        }

        private int getCoprightBit()
        {
            return (int)((bithdr >> 3) & 1);
        }

        private int getOrginalBit()
        {
            return (int)((bithdr >> 2) & 1);
        }

        private int getEmphasisIndex()
        {
            return (int)(bithdr & 3);
        }

        private double getVersion()
        {
            double[] table = { 2.5, 0.0, 2.0, 1.0 };
            return table[getVersionIndex()];
        }

        private int getLayer()
        {
            return (int)(4 - getLayerIndex());
        }

        private int getBitrate()
        {
            // If the file has a variable bitrate, then we return an integer average bitrate,
            // otherwise, we use a lookup table to return the bitrate
            if (boolVBitRate)
            {
                double medFrameSize = (double)lngFileSize / (double)getNumberOfFrames();
                return (int)((medFrameSize * (double)getFrequency()) / (1000.0 * ((getLayerIndex() == 3) ? 12.0 : 144.0)));
            }
            else
            {
                int[,,] table =        {
                                { // MPEG 2 & 2.5
                                    {0,  8, 16, 24, 32, 40, 48, 56, 64, 80, 96,112,128,144,160,0}, // Layer III
                                    {0,  8, 16, 24, 32, 40, 48, 56, 64, 80, 96,112,128,144,160,0}, // Layer II
                                    {0, 32, 48, 56, 64, 80, 96,112,128,144,160,176,192,224,256,0}  // Layer I
                                },
                                { // MPEG 1
                                    {0, 32, 40, 48, 56, 64, 80, 96,112,128,160,192,224,256,320,0}, // Layer III
                                    {0, 32, 48, 56, 64, 80, 96,112,128,160,192,224,256,320,384,0}, // Layer II
                                    {0, 32, 64, 96,128,160,192,224,256,288,320,352,384,416,448,0}  // Layer I
                                }
                                };

                return table[getVersionIndex() & 1, getLayerIndex() - 1, getBitrateIndex()];
            }
        }

        private int getFrequency()
        {
            int[,] table =    {
                            {32000, 16000,  8000}, // MPEG 2.5
                            {    0,     0,     0}, // reserved
                            {22050, 24000, 16000}, // MPEG 2
                            {44100, 48000, 32000}  // MPEG 1
                        };

            return table[getVersionIndex(), getFrequencyIndex()];
        }

        private string getMode()
        {
            switch (getModeIndex())
            {
                default:
                    return "Stereo";
                case 1:
                    return "Joint Stereo";
                case 2:
                    return "Dual Channel";
                case 3:
                    return "Single Channel";
            }
        }

        private int getLengthInSeconds()
        {
            // "intKilBitFileSize" made by dividing by 1000 in order to match the "Kilobits/second"
            int intKiloBitFileSize = (int)((8 * lngFileSize) / 1000);
            return (int)(intKiloBitFileSize / getBitrate());
        }

        private string getFormattedLength()
        {
            // Complete number of seconds
            int s = getLengthInSeconds();

            // Seconds to display
            int ss = s % 60;

            // Complete number of minutes
            int m = (s - ss) / 60;

            // Minutes to display
            int mm = m % 60;

            // Complete number of hours
            int h = (m - mm) / 60;

            // Make "hh:mm:ss"
            return h.ToString("D2") + ":" + mm.ToString("D2") + ":" + ss.ToString("D2");
        }

        private int getNumberOfFrames()
        {
            // Again, the number of MPEG frames is dependant on whether it is a variable bitrate MP3 or not
            if (!boolVBitRate)
            {
                double medFrameSize = (double)(((getLayerIndex() == 3) ? 12 : 144) * ((1000.0 * (float)getBitrate()) / (float)getFrequency()));
                return (int)(lngFileSize / medFrameSize);
            }
            else
                return intVFrames;
        }
    }


    [Microsoft.SqlServer.Server.SqlFunction]

    //A function to return information about an MP3 file
    public static string GetMP3Info(byte[] MP3Data)
    {
        string output = "";

        MP3Info mp3info = new MP3Info();
        bool boolIsMP3 = mp3info.ReadMP3Information(MP3Data);
        if (boolIsMP3)
        {
            output =
                "DurationFormatted=" + mp3info.strLengthFormatted + "&" +
                "DurationSeconds=" + mp3info.intLength.ToString() + "&" +
                "BitRate=" + mp3info.intBitRate.ToString() + "&" +
                "Frequency=" + mp3info.intFrequency.ToString() + "&" +
                "Mode=" + mp3info.strMode + "&" +
                "FilseSize=" + mp3info.lngFileSize.ToString();
        }

        return output;
    }


    [Microsoft.SqlServer.Server.SqlFunction]

        //A function to check whether a particular host is accessible.
        //If a value for intPort is specified, result will be true if a socket connection could be opened.
        //If a value for intPort is not specified, result will be true if a ping was successfull.
        public static SqlBoolean CheckHost(SqlString strHostname, SqlInt16 intPort)
        {
            Boolean success = false;
            String output = "";

            IPAddress ipa;
            try
            {
                ipa = Dns.GetHostAddresses(strHostname.Value)[0];
                output += "Retrieved IP address:" + ipa.ToString();

                try
                {
                    if (!intPort.IsNull && intPort.Value > 0)
                    {
                        System.Net.Sockets.Socket sock = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);

                        //sock.Connect(ipa, intPort.Value);
                        // Connect using a timeout (5 seconds)
                        IAsyncResult asyncConn = sock.BeginConnect(ipa, intPort.Value, null, null);

                        bool connected = asyncConn.AsyncWaitHandle.WaitOne(5000, true);

                        if (!sock.Connected)
                        {
                            output += "Socket connection timed out.  Could not connect.";
                            //sock.EndConnect(asyncConn);
                        }
                        else
                        {
                            output += "Socket connected:  Everything looks good!";
                            success = true;
                        }

                        sock.Close();

                    }
                    else
                    {
                        // no port specified, so just ping
                        var ping = new Ping();

                        var reply = ping.Send(strHostname.Value, 5); //timeout in seconds
                        if ((reply != null) && (reply.Status == IPStatus.Success))
                        {
                            output += "Ping successful";
                            success = true;
                        }
                    }

                }
                catch (System.Net.Sockets.SocketException ex)
                {
                    output += "Error opening socket: " + ex.Message + " (" + ex.ErrorCode.ToString() + ")";
                }
            }
            catch (Exception ex)
            {
                output += "Failure in DNS " + ex.Message;

                try
                {
                    var ping = new Ping();
                    var reply = ping.Send("9.9.9.9", 5); //timeout in seconds
                    if ((reply != null) && (reply.Status == IPStatus.Success))
                    {
                        output += "but DNS Server Ping Success";
                    }
                    else
                    {
                        output += "and DNS Server Ping Failure:  Is the internet connection broken?";
                    }
                }
                catch (Exception)
                {
                    output += "Unknown failure";
                }
            }

            // Now convert UTF-8 string to Unicode and return

            return SqlBoolean.Parse(success.ToString());
            //return System.Text.Encoding.Unicode.GetString(Encoding.Convert(System.Text.Encoding.UTF8, System.Text.Encoding.Unicode, Encoding.UTF8.GetBytes(output)));
        }



    [Microsoft.SqlServer.Server.SqlFunction]

    //A function to issue a TCP/IP ping to a host
    public static SqlBoolean Ping(SqlString strHostname)
    {
        try
        {
            var ping = new Ping();
            var reply = ping.Send(strHostname.Value, 5); //timeout in seconds
            return (reply != null) && (reply.Status == IPStatus.Success);
        }
        catch (Exception)
        {
            return false;
        }
    }
}
//------end of CLR Source------
'

  DECLARE @CSFilename sysname
  SET @CSFilename = @AssemblyName + '.cs'

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = @AssemblyName,
    @FileName = @CSFilename,
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END

GO


IF OBJECT_ID('[sqlver].[spgetWhatChanged]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetWhatChanged]
END
GO

CREATE PROCEDURE sqlver.spgetWhatChanged
@On datetime = NULL,
@DayPad int = NULL,
@StartDate datetime = NULL,
@EndDate datetime = NULL
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @On IS NOT NULL BEGIN
    SET @StartDate = CAST(@On AS date)
    SET @EndDate = @StartDate + 1
  END

  IF @DayPad IS NOT NULL BEGIN
    SET @StartDate = DATEADD(day, @DayPad * -1, @StartDate)
    SET @EndDate = DATEADD(day, @DayPad, @EndDate)
  END

  SELECT *
  FROM
  (
    SELECT
      sm.SchemaName,
      sm.ObjectName,
      Min(schl.EventDate) AS FirstMod,
      MAX(schl.EventDate) AS LastMod
    FROM
      sqlver.tblSchemaLog schl
      JOIN sqlver.tblSchemaManifest sm ON
        schl.SchemaName = sm.SchemaName AND
        schl.ObjectName = sm.ObjectName
    WHERE
      sm.IsGenerated = 0 AND
      (@StartDate IS NULL OR schl.EventDate > @StartDate) AND
      (@EndDate IS NULL OR schl.EventDate < @EndDate + 1) 
    GROUP BY
      sm.SchemaName,
      sm.ObjectName
  ) x
  ORDER BY
    x.LastMod DESC
END

GO


IF OBJECT_ID('[sqlver].[spsysGeonamesCreate]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysGeonamesCreate]
END
GO

CREATE PROCEDURE [sqlver].[spsysGeonamesCreate]

WITH EXECUTE AS CALLER
--$!SQLVer Jan 11 2022 11:02AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN  
  SET NOCOUNT ON

  DECLARE @Debug bit
  SET @Debug = 1

  /*
  --Note:  to modify this procedure you may need to execute the following
  --prior to the CREATE PROCEDURE / ALTER PROCEDURE statement

  DROP TABLE geonames.tblUSPostal
  GO
  CREATE TABLE geonames.tblUSPostal (
    GeoNameID int, GeoZipID int IDENTITY,
    CountryCode char(2),-- iso country code, 2 characters
    PostalCode varchar(20),
    PlaceName varchar(180),
    Admin1Name_State varchar(100),       --1. order subdivision (state)
    Admin1Code_State varchar(20),        --1. order subdivision (state)
    Admin2Name_County varchar(100),      --2. order subdivision (county/province)
    Admin2Code_County varchar(20),       --2. order subdivision (county/province)
    Admin3Name_Subdivision varchar(100), --3. order subdivision (community)
    Admin3Code_Subdivision varchar(20),  --3. order subdivision (community)
    Latitude decimal(10, 6),             --estimated latitude (wgs84)
    Longitude decimal(10,6),             --estimated longitude (wgs84)
    Accuracy tinyint                     --accuracy of lat/lng from 1=estimated to 6=centroid
  )
  GO
  */
  
  DECLARE @Msg varchar(MAX)
  
  DECLARE @ThreadGUID uniqueidentifier
  SET @ThreadGUID = NEWID()

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Starting'
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END

  --Download file US.Zip from:  http://download.geonames.org/export/zip/
  --(for zipcode data)
  --AND
  --Download file US.Zip from:  http://http://download.geonames.org/export/dump/
  --(for gazeteer data)


  DECLARE @FilePath varchar(2048)
  SET @FilePath = 'C:\SQLVer\Temp\'
  
  DECLARE @Filename varchar(255)
  DECLARE @URL nvarchar(MAX)
  DECLARE @HTTPStatus int
  DECLARE @ErrorMessage nvarchar(MAX)
  DECLARE @BinBuf varbinary(MAX) 
 
  DECLARE @TableName sysname
  DECLARE @SQL nvarchar(MAX)
  DECLARE @SchemaID int   


  DECLARE @tvConfig TABLE (
    name nvarchar(35),
    minimum int,
    maximum int,
    config_value int,
    run_value int
  )    
        
  DECLARE @OrigOptValue_ShowAdvanced int
  DECLARE @OrigOptValue_OleAutomation int
  DECLARE @OrigOptValue_xp_cmdshell int
        
  DELETE FROM @tvConfig
          
  INSERT INTO @tvConfig  
  EXEC sp_configure 'show advanced options'
        
  SELECT @OrigOptValue_ShowAdvanced = run_value FROM @tvConfig
        
  IF @OrigOptValue_ShowAdvanced = 0 BEGIN
    EXEC sp_configure 'show advanced options', 1    
    RECONFIGURE
  END      

  DELETE FROM @tvConfig
            
  INSERT INTO @tvConfig  
  EXEC sp_configure 'Ole Automation Procedures'
      
  SELECT @OrigOptValue_OleAutomation = run_value FROM @tvConfig  
        
  IF @OrigOptValue_OleAutomation = 0 BEGIN    
    EXEC sp_configure 'Ole Automation Procedures', 1
    RECONFIGURE
  END  

  DELETE FROM @tvConfig
            
  INSERT INTO @tvConfig  
  EXEC sp_configure 'xp_cmdshell'
      
  SELECT @OrigOptValue_xp_cmdshell = run_value FROM @tvConfig  
        
  IF @OrigOptValue_xp_cmdshell = 0 BEGIN    
    EXEC sp_configure 'xp_cmdshell', 1
    RECONFIGURE
  END        
      
 SET NOCOUNT OFF

  SELECT
    @SchemaID = sch.schema_id
  FROM
    sys.schemas sch
  WHERE
    sch.name = 'geonames'
    
  IF @SchemaID IS NULL BEGIN
    SET @SQL = 'CREATE SCHEMA geonames AUTHORIZATION dbo'
    EXEC(@SQL)
  END  

  IF OBJECT_ID('geonames.tblUSPostal') IS NOT NULL BEGIN
    DROP TABLE geonames.tblUSPostal
  END
    
  CREATE TABLE geonames.tblUSPostal (
    --GeoNameID int, GeoZipID int IDENTITY,
    CountryCode char(2),-- iso country code, 2 characters
    PostalCode varchar(20),
    PlaceName varchar(180),
    Admin1Name_State varchar(100),       --1. order subdivision (state)
    Admin1Code_State varchar(20),        --1. order subdivision (state)
    Admin2Name_County varchar(100),      --2. order subdivision (county/province)
    Admin2Code_County varchar(20),       --2. order subdivision (county/province)
    Admin3Name_Subdivision varchar(100), --3. order subdivision (community)
    Admin3Code_Subdivision varchar(20),  --3. order subdivision (community)
    Latitude decimal(10, 6),             --estimated latitude (wgs84)
    Longitude decimal(10,6),             --estimated longitude (wgs84)
    Accuracy tinyint                     --accuracy of lat/lng from 1=estimated to 6=centroid
  )


  SET @URL = 'http://download.geonames.org/export/zip/US.zip'
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Starting download of ' + ISNULL(@URL, 'NULL')
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END  
  
  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @Cookies = NULL,
    @DataToSend = NULL,
    @DataToSendBin = NULL,
    @Headers = NULL,
    @ResponseBinary = @BinBuf OUTPUT,
    @HTTPStatus = @HTTPStatus OUTPUT,
    @ErrorMsg = @ErrorMessage OUTPUT
  

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: @HTTPStatus=' + CAST(@HTTPStatus AS varchar(100)) +
                ' @ErrorMessage=' + ISNULL(@ErrorMessage, 'NULL') + 
                ' LEN(@BinBuf)=' + CAST(LEN(@BinBuf) AS varchar(100))
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END  
  
  IF @ErrorMessage IS NOT NULL BEGIN
    SET @ErrorMessage = 'sqlver.spGeonamesCreate: Error downloading ' + ISNULL(@URL, 'NULL') + ': ' + @ErrorMessage
    
    EXEC sqlver.spinsSysRTLog @Msg = @ErrorMessage, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    
    RAISERROR(@ErrorMessage, 16, 1)
    RETURN 2001
  END
  ELSE BEGIN
    SET @Filename = 'USPostal.zip' 
    
    IF @Debug = 1 BEGIN
      SET @Msg = 'sqlver.spGeonamesCreate: Writing downloaded data to ' + ISNULL(@FilePath, 'NULL') + ISNULL(@Filename, 'NULL')
      RAISERROR(@Msg, 0, 1) WITH NOWAIT
      EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
    END    

    EXEC sqlver.sputilWriteBinaryToFile
      @FileData = @BinBuf,
      @FilePath = @FilePath,
      @FileName = @Filename,
      @ErrorMsg = @ErrorMessage OUTPUT 




    
    IF @ErrorMessage IS NOT NULL BEGIN    
      SET @ErrorMessage = 'sqlver.spGeonamesCreate: Error writing file to ' + ISNULL(@URL, 'NULL') + ': ' + @ErrorMessage
 
      EXEC sqlver.spinsSysRTLog @Msg = @ErrorMessage, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1    
    
      RAISERROR(@ErrorMessage, 16, 1)
      RETURN 2001
    END             
    ELSE BEGIN    
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spGeonamesCreate: Unzipping ' + ISNULL(@Filename, 'NULL')
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
      END  
      
      SET NOCOUNT ON
      SET @SQL = 'EXEC xp_cmdshell ''cd . && "C:\Program Files\7-Zip\7z.exe" e ' + @FilePath + @Filename + ' -o' + @FilePath + ' -y -r'''
      EXEC(@SQL) 
      SET NOCOUNT OFF
    END
  END


  SET @TableName = 'geonames.tblUSPostal'
  SET @Filename = 'US.txt'
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Performing BULK INSERT of ' + ISNULL(@Filename, 'NULL') + ' into ' + ISNULL(@TableName, 'NULL')
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END   
  
  SET @SQL = 
    'BULK INSERT ' + @TableName + ' FROM ''' + @FilePath + @FileName + ''' WITH (FIELDTERMINATOR = ''0x09'', ROWTERMINATOR = ''0x0a'', TABLOCK)'

  EXEC(@SQL)

  ALTER TABLE geonames.tblUSPostal ADD GeoNameID int, GeoZipID int IDENTITY
  
  -------------------------------------------------  
  
  IF OBJECT_ID('geonames.tblUS') IS NOT NULL BEGIN
    DROP TABLE geonames.tblUS
  END

  CREATE TABLE geonames.tblUS (
    GeoNameID int PRIMARY KEY,             --integer id of record in geonames database
    GeoName nvarchar(200),                 --name of geographical point (utf8)
    GeoNameASCII varchar(200),             --name of geographical point in plain ascii characters
    AlternateNames nvarchar(MAX),           --alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table
    Latitude decimal(10, 6),               --latitude in decimal degrees (wgs84)
    Longitude decimal(10,6),               --longitude in decimal degrees (wgs84)
    FeatureClass char(1),                  --http://www.geonames.org/export/codes.html
    FeatureCode varchar(10),               --http://www.geonames.org/export/codes.html
    CountryCode char(2),                   --ISO-3166 2-letter country code, 2 characters
    CC2 nvarchar(200),                     --alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
    Admin1Code_State varchar(20),          --fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code
    Admin2Code_County varchar(80),         --code for the second administrative division, a county in the US, see file admin2Codes.txt
    Admin3Code_SubDivision varchar(20),    --code for third level administrative division
    Admin4Code varchar(20),                --code for fourth level administrative division
    [Population] bigint,                   --bigint (8 byte int) 
    Elevation int,                         --in meters
    DEM int,                               --digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
    Timezone varchar(40),                  --the iana timezone id (see file timeZone.txt)
    ModificationDate date                  --date of last modification in yyyy-MM-dd format
  )


  SET @URL = 'http://download.geonames.org/export/dump/US.zip'

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Starting download of ' + ISNULL(@URL, 'NULL')
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END  
  
  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @Cookies = NULL,
    @DataToSend = NULL,
    @DataToSendBin = NULL,
    @Headers = NULL,
    @ResponseBinary = @BinBuf OUTPUT,
    @HTTPStatus = @HTTPStatus OUTPUT,    
    @ErrorMsg = @ErrorMessage OUTPUT

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: @HTTPStatus=' + CAST(@HTTPStatus AS varchar(100)) +
                ' @ErrorMessage=' + ISNULL(@ErrorMessage, 'NULL') +
                ' LEN(@BinBuf)=' + CAST(LEN(@BinBuf) AS varchar(100))
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END

  IF @ErrorMessage IS NOT NULL BEGIN
    SET @ErrorMessage = 'sqlver.spGeonamesCreate: Error downloading ' + ISNULL(@URL, 'NULL') + ': ' + @ErrorMessage
    
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
        
    RAISERROR(@ErrorMessage, 16, 1)
    RETURN 2001
  END
  ELSE BEGIN   
    SET @Filename = 'US.zip' 

    IF @Debug = 1 BEGIN
      SET @Msg = 'sqlver.spGeonamesCreate: Writing downloaded data to ' + ISNULL(@Filename, 'NULL')
      RAISERROR(@Msg, 0, 1) WITH NOWAIT
      EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
    END
             
    EXEC sqlver.sputilWriteBinaryToFile
      @FileData = @BinBuf,
      @FilePath = @FilePath,
      @FileName = @Filename,
      @ErrorMsg = @ErrorMessage OUTPUT 
    
    IF @ErrorMessage IS NOT NULL BEGIN    
      SET @ErrorMessage = 'sqlver.spGeonamesCreate: Error writing file to' + ISNULL(@FilePath + @Filename, 'NULL') + ': ' + @ErrorMessage

      EXEC sqlver.spinsSysRTLog @Msg = @ErrorMessage, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
       
      RAISERROR(@ErrorMessage, 16, 1)
      RETURN 2001
    END
    ELSE BEGIN
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spGeonamesCreate: Unzipping ' + ISNULL(@Filename, 'NULL')
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
      END    
        
      SET NOCOUNT ON
      SET @SQL = 'EXEC xp_cmdshell ''cd . && "C:\Program Files\7-Zip\7z.exe" e ' + @FilePath + @Filename + ' -o' + @FilePath + ' -y -r'''
      EXEC(@SQL) 
      SET NOCOUNT OFF
    END    
  END
  

  SET @TableName = 'geonames.tblUS'
  SET @Filename = 'US.txt'  

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Performing BULK INSERT of ' + ISNULL(@Filename, 'NULL') + ' into ' + ISNULL(@TableName, 'NULL')
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END    
  
  SET @SQL = 
    'BULK INSERT ' + @TableName + ' FROM ''' + @FilePath + @FileName + ''' WITH (FIELDTERMINATOR = ''0x09'', ROWTERMINATOR = ''0x0a'', TABLOCK)'

  EXEC(@SQL)
  
  ------------------
  
  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Creating indexes'
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END 
  
  CREATE INDEX ix_USPostal_GeoNameID ON geonames.tblUSPostal(GeoNameID)

  CREATE INDEX ix_USPostal_PlaceName ON geonames.tblUSPostal (PlaceName)
  CREATE INDEX ix_USPostal_PostalCode ON geonames.tblUSPostal (PostalCode)
  CREATE INDEX ix_USPostal_County ON geonames.tblUSPostal  (Admin1Code_State, Admin2Code_County)
  
  ------------------

  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Matching Zip and Geo to update geonames.tblUSPostal.GeoNameID'
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END 
  
  --Update "easy" matches on name and state
  UPDATE z
  SET
    GeoNameID = gn.GeoNameID    
  FROM
    geonames.tblUSPostal z
    LEFT JOIN geonames.tblUS gn ON
      z.PlaceName = gn.GeoNameASCII AND
      z.Admin2Code_County = gn.Admin2Code_County AND
      z.Admin1Code_State = gn.Admin1Code_State          
  WHERE
    z.GeoNameID IS NULL


  --Fuzzy Update #1:  State matches, and zip's county = GeoName, then find closest distance
  UPDATE z
  SET
    GeoNameID = x.GeoNameID
  FROM 
    (
    SELECT
      z.GeoZipID,
      gn.GeoNameID, 
      ROW_NUMBER() OVER (PARTITION BY z.GeoZipID ORDER BY sqlver.udfDistanceFromCoordinates(z.Latitude, z.Longitude, gn.Latitude, gn.Longitude, 'M')) Seq
    FROM
      geonames.tblUSPostal z
      JOIN geonames.tblUS gn ON
        --z.Admin2Code_County = gn.Admin2Code_County AND
        z.Admin1Code_State = gn.Admin1Code_State AND
        z.Admin2Name_County = gn.GeoName
    WHERE
      z.GeoNameID IS NULL) x
        
    JOIN geonames.tblUSPostal z ON
      x.GeoZipID = z.GeoZipID
  WHERE
    x.Seq = 1 AND
    z.GeoNameID IS NULL
    

  --Fuzzy Update #2:  State and county matches,then find closest distance
  UPDATE z
  SET
    GeoNameID = x.GeoNameID
  FROM 
    (
    SELECT
      z.GeoZipID,
      gn.GeoNameID, 
      ROW_NUMBER() OVER (PARTITION BY z.GeoZipID ORDER BY sqlver.udfDistanceFromCoordinates(z.Latitude, z.Longitude, gn.Latitude, gn.Longitude, 'M')) Seq
    FROM
      geonames.tblUSPostal z
      JOIN geonames.tblUS gn ON
        z.Admin2Code_County = gn.Admin2Code_County AND
        z.Admin1Code_State = gn.Admin1Code_State 
    WHERE
      z.GeoNameID IS NULL) x
        
    JOIN geonames.tblUSPostal z ON
      x.GeoZipID = z.GeoZipID
  WHERE
    x.Seq = 1 AND
   z.GeoNameID IS NULL     
      
      
  --Fuzzy Update #3: County matches,then find closest distance
  UPDATE z
  SET
    GeoNameID = x.GeoNameID
  FROM (  
  SELECT
    z.GeoZipID,
    gn.GeoNameID, 
    ROW_NUMBER() OVER (PARTITION BY z.GeoZipID ORDER BY sqlver.udfDistanceFromCoordinates(z.Latitude, z.Longitude, gn.Latitude, gn.Longitude, 'M')) Seq
  FROM
    geonames.tblUSPostal z
    JOIN geonames.tblUS gn ON
      z.Admin2Code_County = gn.Admin2Code_County
  WHERE
    z.GeoNameID IS NULL
  ) x
    
    JOIN geonames.tblUSPostal z ON
      x.Seq = 1 AND
      x.GeoZipID = z.GeoZipID
  WHERE
    z.GeoNameID IS NULL

    
    
  SELECT 'Still Unmatched', z.*
  FROM
    geonames.tblUSPostal z 
  WHERE
    z.GeoNameID IS NULL   

  IF @OrigOptValue_xp_cmdshell = 0 BEGIN    
    EXEC sp_configure 'xp_cmdshell', 0
    RECONFIGURE
  END        

  IF @OrigOptValue_OleAutomation = 0 BEGIN    
    EXEC sp_configure 'Ole Automation Procedures', 0
    RECONFIGURE
  END  

  IF @OrigOptValue_ShowAdvanced = 0 BEGIN
    EXEC sp_configure 'show advanced options', 0   
    RECONFIGURE
  END


  IF OBJECT_ID('[geonames].[udftMilesFromZip]') IS NULL BEGIN
  SET @SQL = 
'CREATE FUNCTION [geonames].[udftMilesFromZip](
@Zip1 varchar(5),
@Zip2 varchar(5))
RETURNS TABLE
AS
RETURN (
  SELECT 
    zip1.PostalCode AS Zip1, 
    zip2.PostalCode AS Zip2,
    sqlver.udfDistanceFromCoordinates(
      zip1.Latitude, zip1.Longitude,
      zip2.Latitude, zip2.Longitude, ''M'') AS Miles
  FROM
    geonames.tblUSPostal zip1
    JOIN geonames.tblUSPostal zip2 ON
      zip1.PostalCode = @Zip1 AND
      zip2.PostalCode = @Zip2  
)'
    EXEC (@SQL)

    IF EXISTS(SELECT schema_id FROM sys.schemas WHERE name = 'opsstream') AND 
      EXISTS (SELECT principal_id from sys.server_principals WHERE name = 'opsstream_sys') BEGIN

      GRANT SELECT ON geonames.tblUS TO opsstream_sys
      GRANT SELECT ON geonames.tblUSPostal to opsstream_sys
      GRANT SELECT ON geonames.udftMilesFromZip TO opsstream_sys
    END

  END


  IF @Debug = 1 BEGIN
    SET @Msg = 'sqlver.spGeonamesCreate: Finished'
    RAISERROR(@Msg, 0, 1) WITH NOWAIT
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
  END
    
END

GO


IF OBJECT_ID('[sqlver].[spsysSchemaObjectCompareMaster]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaObjectCompareMaster]
END
GO

CREATE PROCEDURE [sqlver].[spsysSchemaObjectCompareMaster]
@Hash1 varbinary(128) = NULL,
@MasterHash varbinary(128) = NULL,
@SchemaName sysname = NULL,
@ObjectName sysname = NULL
--$!SQLVer Aug  3 2021  9:26AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @ObjectType sysname

  DECLARE @SrcLocal nvarchar(MAX)
  DECLARE @SrcMaster nvarchar(MAX)
  DECLARE @LocalDate datetime
  DECLARE @MasterDate datetime

  SELECT
    @SchemaName = COALESCE(@SchemaName, schl1.SchemaName, schm1.SchemaName, schm2.SchemaName, schl2.SchemaName, schm3.SchemaName),
    @ObjectName = COALESCE(@ObjectName, schl1.ObjectName, schm1.ObjectName, schm2.ObjectName, schl2.Objectname, schm3.ObjectName),

    @SrcLocal = COALESCE(schl1.SQLCommand, schl2.SQLCommand),--COALESCE(schl1.SQLCommand, schm1.CurrentDefinition),
    @SrcMaster = COALESCE(schl3.SQLCommand, schm3.CurrentDefinition),

    @LocalDate = COALESCE(schl1.EventDate, schl2.EventDate, schm1.DateUpdated, schm2.DateUpdated, schm1.DateAppeared, schm2.DateAppeared),
    @MasterDate = COALESCE(schl3.EventDate, schm3.DateUpdated)
  FROM
    (SELECT 1 AS Placeholder) x 
    LEFT JOIN sqlver.tblSchemaLog schl1 ON schl1.[Hash] = @Hash1
    LEFT JOIN sqlver.tblSchemaManifest schm1 ON schm1.OrigHash = @Hash1 AND schl1.SchemaLogID IS NULL
    LEFT JOIN sqlver.tblSchemaManifest schm2 ON schm2.SchemaName = @SchemaName AND schm2.ObjectName = @ObjectName
    LEFT JOIN sqlver.tblSchemaLog schl2 ON schm2.CurrentHash = schl2.Hash

    --note:  sqlver.vwMasterSchemaLog and sqlver.vwMasterSchemaManifest are synonyms you must set up to point to the remote repository
    LEFT JOIN sqlver.vwMasterSchemaLog schl3 ON schl3.[Hash] = @MasterHash
    LEFT JOIN sqlver.vwMasterSchemaManifest schm3 ON schm3.SchemaName = @SchemaName AND schm3.ObjectName = @ObjectName

  SELECT
    @ObjectType = COALESCE(schm1.ObjectType, schm2.ObjectType)
  FROM
    (SELECT 1 AS Placeholder) x 
    LEFT JOIN sqlver.tblSchemaManifest schm1 ON schm1.SchemaName = @SchemaName AND schm1.ObjectName = @ObjectName
    LEFT JOIN sqlver.vwMasterSchemaManifest schm2 ON schm2.SchemaName = @SchemaName AND schm2.ObjectName = @ObjectName


  SELECT
    @SchemaName AS SchemaName,
    @ObjectName AS ObjectName,
    @ObjectType AS ObjectType,
    @LocalDate AS LocalDate,
    @MasterDate AS MasterDate

  IF @ObjectType = 'TABLE' BEGIN

    SET @SrcLocal  = sqlver.udfScriptTable(@SchemaName, @ObjectName)

    DECLARE @tvSrcMaster TABLE (ObjectDefinition nvarchar(MAX))

    INSERT INTO @tvSrcMaster
    --Note:  sqlver.spMasterExecuteSQL is a synonym you must set up to point to the remote master database dbo.sp_executesql
    EXEC sqlver.spMasterExecuteSQL @stmt = N'SELECT sqlver.udfScriptTable(@SchemaName, @ObjectName)', @params = N'@SchemaName sysname, @ObjectName sysname', @SchemaName = @SchemaName, @ObjectName = @ObjectName

    SELECT @SrcMaster = ObjectDefinition FROM @tvSrcMaster
  END


  PRINT '-------------------------------------'
  PRINT '***Master: '
  EXEC sqlver.sputilPrintString @SrcMaster

  PRINT '-------------------------------------'
  PRINT '***Local: '
  EXEC sqlver.sputilPrintString @SrcLocal


  IF OBJECT_ID('sqlver.udftGetDiffs_CLR') IS NOT NULL BEGIN
    SELECT *
    FROM
      sqlver.udftGetDiffs_CLR(@SrcLocal, @SrcMaster)
  END

END

GO


IF OBJECT_ID('[sqlver].[spgetSQLLocks]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLLocks]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLLocks]
@ExclusiveOnly bit = 1
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spgetLastModified]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetLastModified]
END
GO

CREATE PROCEDURE [sqlver].[spgetLastModified]
--$!SQLVer Oct  3 2023  8:32AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --NOTE:  intentionally returns the EARLIEST date for a given hash.
  --In other words:  if an object was changed, and that change was reverted, the
  --date would remain the original date (prior to the change and the reversion)

  SELECT
    x.SchemaName,
    x.ObjectName,
    x.ObjectType,
    x.CurrentHash,
    x.EventDate AS DateLastModified
  FROM 
    (
    SELECT
      om.SchemaName,
      om.ObjectName,
      om.ObjectType,
      om.CurrentHash,
      schl.EventDate,
      ROW_NUMBER() OVER (PARTITION BY om.SchemaName, om.ObjectName ORDER BY schl.SchemaLogID) AS Seq
    FROM
      sqlver.tblSchemaManifest om
      JOIN sqlver.tblSchemaLog schl ON
        om.SchemaName = schl.SchemaName AND
        om.ObjectName = schl.ObjectName AND
        om.CurrentHash = schl.Hash
    ) x
  WHERE
    x.Seq = 1
  ORDER BY
    x.EventDate DESC
END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_PDFCLR]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_PDFCLR]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_PDFCLR]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  PRINT '***CAN NO LONGER USE PDFCLR in SQLCLR***'
  PRINT 'This has been deprecated, due to incompatibility of '
  PRINT 'the .NET 4.0 version of System.Image.dll which now'
  PRINT 'contains native code, and hence cannot be loaded into'
  PRINT 'SQLCLR.'
  PRINT ''
  PRINT 'Consider uisng the SQLVerCLR web server to host this'
  PRINT 'assembly''s functionality.'
  RAISERROR('Assembly PDFCLR is not supported and cannot proceed.', 16, 1)
  RETURN 1002

  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Drawing.dll')
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Windows.Forms', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Windows.Forms.dll')  
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('iTextSharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.udfRenderPDF'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfRenderPDF;
    END
  '

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE FUNCTION [sqlver].[udfRenderPDF](
    @TemplatePDF varbinary(MAX),
    @FieldsXML xml
    )
    RETURNS [varbinary](max) WITH EXECUTE AS CALLER
    AS 
    EXTERNAL NAME [PDFCLR].[Functions].[RenderPDF]
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
using iTextSharp.text;
using iTextSharp.text.pdf;
using System.IO;
using System.Xml;
using System.Linq;
using System.Xml.Linq;
using System.Security;

//from AssemblyInfo.cs
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Data.Sql;

// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("PDFCLR")]
[assembly: AssemblyDescription("Render PDF documents in a SQL CLR Function.  Generated automatically by opsstream.spsysRebuildCLR_PDFCLR")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("OpsStream")]
[assembly: AssemblyProduct("PDFCLR")]
[assembly: AssemblyCopyright("Copyright ©  2013")]
[assembly: AssemblyTrademark("OpsStream")]
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
    public static SqlBytes RenderPDF(
        SqlBytes templatePDF,
        SqlXml fieldsXML
        )
    {
        // Put your code here
        ///////////////////////////////////////////////
        if (templatePDF.IsNull) {
          throw new Exception("Error in CLR function RenderPDF: Parameter templatePDF must contain a valid PDF document.");          
        }
        
        if (fieldsXML.IsNull) { 
          throw new Exception("Error in CLR function RenderPDF: Parameter fieldsXML must contain a valid XML document.");          
        }        
        
        using (MemoryStream outputPDFStream = new MemoryStream())
        {

            BaseFont f_cb = BaseFont.CreateFont("c:\\windows\\fonts\\ARIALBD.ttf", BaseFont.CP1252, BaseFont.NOT_EMBEDDED);
            BaseFont f_cn = BaseFont.CreateFont("c:\\windows\\fonts\\ARIAL.ttf", BaseFont.CP1252, BaseFont.NOT_EMBEDDED);

            //create new output document
            Document doc1 = new Document();
            PdfCopy writer = new PdfCopy(doc1, outputPDFStream);
            doc1.Open();


                    //Process the fields specified in the fieldsXML parameter
                    XmlReader pagesXMLReader = fieldsXML.CreateReader();
                    //XmlReader pagesXMLReader = XmlReader.Create(new StringReader(fieldsXML));

                    pagesXMLReader.MoveToContent();

                    while (pagesXMLReader.ReadToFollowing("Page"))
                    {
                        //create a new single-page PDF document by overlaying text over template    

                        //for loading the PDF template
                        MemoryStream thisTemplatePDF = new MemoryStream(templatePDF.Buffer);
                        PdfReader readerTemplate = new PdfReader(thisTemplatePDF.ToArray());

                        //for the output of the manipulated single-page PDF document
                        MemoryStream thisPageStream = new MemoryStream();

                        //for overlaying text over the template
                        PdfStamper stamper = new PdfStamper(readerTemplate, thisPageStream);

                        //for reading the newly-generated single-page PDF document for inclusion in the main output document
                        PdfReader readerThisPage = null;

                        //for directly writing to the single-page PDF document
                        PdfContentByte canvas;


                        canvas = stamper.GetOverContent(1);

                        canvas.SaveState();
                        canvas.BeginText();

                        XmlReader fieldsXMLReader = pagesXMLReader.ReadSubtree();

                        fieldsXMLReader.ReadToDescendant("Fields");
                        while (fieldsXMLReader.ReadToFollowing("Field"))
                        {
                            string thisValue;
                            int thisXPos;
                            int thisYPos;
                            int thisFontSize;

                            XmlReader thisSubtreeXMLReader = fieldsXMLReader.ReadSubtree();
                            thisSubtreeXMLReader.MoveToContent();
                            //Note:  We should always be on an element, however if we do not call
                            //.MoveToContent the XNode.ReadFrom throws an error below:
                            //"The XmlReader state should be Interactive."  Calling
                            //.MoveToContent avoids this error.
                            
                            XElement thisFieldNode = (XElement)XNode.ReadFrom(thisSubtreeXMLReader);
                            
                            //Note:  XNode.ReadFrom advances the reader, which is a pain when we
                            //are trying to loop through XML reader (such as our loop
                            //      while (fieldsXMLReader.ReadToFollowing("Field"))
                            //above.  This leads to only every-other Field element being processed.
                            //Consequently, we use the thisSubtreeXMLReader, so that XNode.ReadFrom
                            //does not mess up our position in the fieldsXMLReader.

                            thisValue =
                                thisFieldNode
                                .Elements("TextValue")
                                .Nodes()
                                .OfType<XText>()
                                .First()
                                .Value;

                            thisXPos = Convert.ToInt32(
                                thisFieldNode
                                .Elements("XPos")
                                .Nodes()
                                .OfType<XText>()
                                .First()
                                .Value);

                            thisYPos = Convert.ToInt32(
                                thisFieldNode
                                .Elements("YPos")
                                .Nodes()
                                .OfType<XText>()
                                .First()
                                .Value);

                            thisFontSize = Convert.ToInt32(
                                thisFieldNode
                                .Elements("FontSize")
                                .Nodes()
                                .OfType<XText>()
                                .First()
                                .Value);


                            canvas.SetFontAndSize(f_cn, thisFontSize);
                            canvas.SetTextMatrix(thisXPos, thisYPos);
                            canvas.ShowText(thisValue);
                            
                            thisSubtreeXMLReader.Close();
                        }

                        canvas.EndText();
                        canvas.RestoreState();

                        //Close the stamper to render the new single-page PDF document.
                        //Note that this closes the output stream too.
                        stamper.Close();

                        //Instantiate a new reader to read the newly-created single-page PDF document.
                        //Since the stream has been closed, we need to read directly from the byte array
                        //returned by .ToArray() of the closed stream.  See:
                        //  http://itext-general.2136553.n4.nabble.com/PDFStamper-weird-situation-td4658458.html#a4658459
                        //  http://msdn.microsoft.com/en-us/library/system.io.memorystream.toarray(v=vs.85).aspx
                        readerThisPage = new PdfReader(thisPageStream.ToArray());

                        //Add newly-created single-page PDF document to the main output PDF document
                        writer.AddPage(writer.GetImportedPage(readerThisPage, 1));

                        stamper.Dispose();
                        readerThisPage.Dispose();
                        thisPageStream.Dispose();

                        readerTemplate.Dispose();
                        thisTemplatePDF.Dispose();

                        fieldsXMLReader.Close();
                    }
                    pagesXMLReader.Close();
              

                //Close the main output PDF document
                doc1.Close();

                writer.Dispose();
                doc1.Dispose();
            
            return (new SqlBytes(outputPDFStream.ToArray()));

        }
        ///////////////////////////////////////////////

    }
};
  //------end of CLR Source------  '

    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'PDFCLR',
    @FileName = 'PDFCLR_SQLCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

END

GO


IF OBJECT_ID('[sqlver].[spactOpenAI_Chat]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spactOpenAI_Chat]
END
GO

CREATE PROCEDURE [sqlver].[spactOpenAI_Chat]
  @PromptText varchar(MAX),
  @SystemInstruction varchar(MAX) = NULL,
  @ChatResponse varchar(MAX) = NULL OUTPUT,
  @JSONResponse varchar(MAX) = NULL OUTPUT,
  @PrintToo bit = 1,
  @Debug bit = 0
--$!SQLVer Mar 12 2025  8:59PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  --DECLARE @Debug bit = 1
  DECLARE @Msg varchar(MAX)
  DECLARE @ThreadGUID uniqueidentifier = NEWID()
  DECLARE @MN varchar(100) = 'sqlver.spactOpenAI_Chat: '

  DECLARE @Log bit
  SET @Log = 1

  IF @Debug = 1 BEGIN
    SET @Msg = CONCAT(@MN, 'Starting')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

  IF NULLIF(RTRIM(@PromptText), '') IS NULL BEGIN
    SET @Msg = CONCAT(@MN, 'Error: No prompt was provided in @PromptText ')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1001)
  END

  /*
  -- Ensure user is logged in to OpsStream
  IF NOT EXISTS (SELECT 1 FROM opsstream.vwSysCurUser) 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Error: Cannot proceed without being logged into OpsStream.')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1001)
  END
  */

  -- Retrieve OpenAI API key securely
  DECLARE @ApiKey varchar(255)
  SET @ApiKey = sqlver.udfGetSecureValue('OpenAI_APIKey')
  
  IF @ApiKey IS NULL
  BEGIN
    SET @Msg = CONCAT(@MN,
      'Error: OpenAI API key not found.', CHAR(13), CHAR(10),
      'Add you key by executing this in the SQL database: ', CHAR(13), CHAR(10),
      '  EXEC sqlver.spStoreSecureValue ''OpenAI_APIKey'', ''{your key here}''', CHAR(13), CHAR(10),
      'Note that you obtain your API key at:', CHAR(13), CHAR(10),
      '  https://platform.openai.com/settings/organization/api-keys'
      )
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1003)
  END

  -- Prepare API URL
  DECLARE @URL varchar(4000) = 'https://api.openai.com/v1/chat/completions'

  -- Prepare JSON payload
  IF @SystemInstruction IS NULL BEGIN
    SET @SystemInstruction = sqlver.udfGetSecureValue('OpenAI_DefaultInstruction')

    IF @SystemInstruction IS NULL BEGIN
        SET @Msg = CONCAT(@MN,
      'While not required, System Instruction was not found.', CHAR(13), CHAR(10),
      'This optional instruction to the GPT influences the chat response.', CHAR(13), CHAR(10),
      'You may provide this as a parameter, such as:',
      '  EXEC sqlver.spactOpenAI_Chat ''What is greater than 1?'', @SystemInstruction = ''You are a poet''', CHAR(13), CHAR(10),
      'You may also save a default System Instruction like this:', CHAR(13), CHAR(10),
      '  EXEC sqlver.spStoreSecureValue ''OpenAI_DefaultInstruction'', ''You are a poet''', CHAR(13), CHAR(10),
      'You can pass in @SystemInstruction = '''' if you want to suppress this message without providing a value.'
      )
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    END
  END


  DECLARE @JSONPayload varchar(MAX)
  SET @JSONPayload = '{"model": "gpt-4-turbo", "temperature": 0, "messages": [' + 
    CASE WHEN @SystemInstruction IS NOT NULL THEN '{"role": "system", "content": "' + REPLACE(@SystemInstruction, '"', '\"') + '"}, ' ELSE '' END +
    '{"role": "user", "content": "' + REPLACE(@PromptText, '"', '\"') + '"}]}'

  -- Prepare Authorization Header
  DECLARE @Headers varchar(MAX)
  SET @Headers = CONCAT('Authorization: Bearer ', @ApiKey, CHAR(13) + CHAR(10),
                        'Content-Type: application/json', CHAR(13) + CHAR(10))

  DECLARE @BinBuf varbinary(MAX)
  DECLARE @HTTPStatus int
  DECLARE @Cookies varchar(MAX)
  DECLARE @RedirURL varchar(4000)
  DECLARE @ErrorMsg varchar(MAX)

  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, '@Headers=', @Headers)
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END
  
  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, '@JSONPayload=', @JSONPayload)
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Performing HTTP POST to get chat response.')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

  IF @Log = 1 BEGIN
    SET @Msg = CONCAT(@MN, ' Sending this prompt: ', @PromptText)
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END
  
  BEGIN TRY
    -- Perform HTTP POST request
    EXEC sqlver.sputilGetHTTP_CLR
      @URL = @URL,
      @HTTPMethod = 'POST',
      @Headers = @Headers,
      @DataToSend = @JSONPayload,
      @DataToSendBin = NULL,
      @Cookies = @Cookies OUTPUT,
      @HTTPStatus = @HTTPStatus OUTPUT,
      @RedirURL = @RedirURL OUTPUT,
      @ResponseBinary = @BinBuf OUTPUT,
      @ErrorMsg = @ErrorMsg OUTPUT

    IF @Debug = 1 BEGIN
      SET @Msg = CONCAT(@MN, 'Response received: @HTTPStatus=', @HTTPStatus)
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    END

    IF NULLIF(RTRIM(@ErrorMsg), '') IS NOT NULL BEGIN
      SET @Msg = CONCAT(@MN, '@ErrorMsg=', @ErrorMsg)
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    END

    -- Convert binary response to JSON
    --DECLARE @JSONResponse varchar(MAX)
    SET @JSONResponse = CAST(@BinBuf AS varchar(MAX))

    IF @Debug = 1 BEGIN
      SET @Msg = CONCAT(@MN, '@JSONResponse=', @JSONResponse)
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
    END

    SELECT @ChatResponse  = j.ChatResponse FROM OPENJSON(@JSONResponse) WITH (ChatResponse  varchar(MAX) '$.choices[0].message.content') j

    IF @Debug = 1 BEGIN
      SET @Msg = CONCAT(@MN, '@ChatResponse=', @ChatResponse)
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
    END

    IF @ChatResponse IS NULL BEGIN
      SET @Msg = CONCAT(@MN, 'Error: No response text returned from OpenAI.')
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1

      IF @PrintToo = 1 BEGIN
        EXEC sqlver.sputilPrintString 'No response text returned from OpenAI.'
      END

      RAISERROR(@Msg, 16, 1)
      RETURN (1004)
    END

    IF @PrintToo = 1 BEGIN
      EXEC sqlver.sputilPrintString @ChatResponse
    END

  END TRY
  BEGIN CATCH
    SET @Msg = CONCAT(@MN, 'Error while processing response: ', ERROR_MESSAGE())
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RETURN (1005)
  END CATCH

  IF @Debug = 1 BEGIN
    SET @Msg = CONCAT(@MN, 'Finished')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

END

GO


IF OBJECT_ID('[sqlver].[spactOpenAI_TranscribeAudio]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spactOpenAI_TranscribeAudio]
END
GO

CREATE PROCEDURE [sqlver].[spactOpenAI_TranscribeAudio]
  @AttachmentGUID uniqueidentifier,
  @JSON varchar(MAX) = NULL OUTPUT, 
  @TranscriptText varchar(MAX) = NULL OUTPUT
--$!SQLVer Mar 12 2025  9:03PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @Debug bit = 0
  DECLARE @Msg varchar(MAX)
  DECLARE @ThreadGUID uniqueidentifier = NEWID()
  DECLARE @MN varchar(100) = 'sqlver.spactOpenAI_TranscribeAudio: '

  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Starting')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

  -- Ensure user is logged in to OpsStream
  IF NOT EXISTS (SELECT 1 FROM opsstream.vwSysCurUser) 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Error: Cannot proceed without being logged into OpsStream.')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1001)
  END

  -- Retrieve audio data from opsstream.tblQuestAttachments
  DECLARE @AudioData varbinary(MAX)
  DECLARE @Filename varchar(254)
  DECLARE @FileType varchar(255)

  SELECT @AudioData = AttachmentData,
         @Filename = Filename,
         @FileType = Filetype
  FROM opsstream.tblQuestAttachments
  WHERE AttachmentGUID = @AttachmentGUID


  IF @AudioData IS NULL
  BEGIN
    SET @Msg = CONCAT(@MN, 'Error: No audio data found for AttachmentGUID ', @AttachmentGUID)
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1002)
  END

  -- Prepare API URL
  DECLARE @URL varchar(4000) = 'https://api.openai.com/v1/audio/transcriptions'

  -- Retrieve OpenAI API key securely
  DECLARE @ApiKey varchar(255) --note that new project-based keys can be 164 characters or longer

  SET @ApiKey = sqlver.udfGetSecureValue('OpenAI_APIKey')
  
  IF @ApiKey IS NULL
  BEGIN
    SET @Msg = CONCAT(@MN,
      'Error: OpenAI API key not found.', CHAR(13), CHAR(10),
      'Add you key by executing this in the SQL database: ', CHAR(13), CHAR(10),
      '  EXEC sqlver.spStoreSecureValue ''OpenAI_APIKey'', ''{your key here}''', CHAR(13), CHAR(10),
      'Note that you obtain your API key at:', CHAR(13), CHAR(10),
      '  https://platform.openai.com/settings/organization/api-keys'
      )
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN (1003)
  END

  -- Prepare multipart form data

  --boundary must be less than 70 bytes long and contain only 7-bit US-ASCII printable characters
    
  DECLARE @Boundary varchar(70)
  SET @Boundary = 'opsstream' + LOWER(LEFT(REPLACE(CAST(NEWID() AS varchar(70)), '-', ''), 16))
  
  --Alternate ways of generating a boundary value:
    --SET @MultipartBoundary = CAST(DATEDIFF(s, '19700101', GETDATE()) AS varchar(100))
    --SET @MultipartBoundary = opsstream.randomString(16)
    
  SET @Boundary = opsstream.LPad(@Boundary, '-', 40) --we are padding to a total length of only 40 characters...30 characters less than the maximum



  DECLARE @CRLF varchar(5) = CHAR(13) + CHAR(10)
  DECLARE @DataToSend varchar(MAX)
  DECLARE @DataToSendBin varbinary(MAX)

  -- Prepare Authorization Header
  DECLARE @Headers varchar(MAX)
  SET @Headers = CONCAT('Authorization: Bearer ', '{APIKey}', @CRLF, 
                        'Content-Type: multipart/form-data; ',
                        'boundary=', @Boundary, @CRLF, @CRLF)

  SET @Headers= REPLACE(@Headers, '{APIKEY}', @ApiKey)
                    
  SET @DataToSend = CONCAT(

    --Field "file" to hold the model name
    '--', @Boundary, @CRLF +
    'Content-Disposition: form-data; name="model";', @CRLF,
    @CRLF, --Extra CRLF is REQUIRED!!!
    'whisper-1', @CRLF,
    
    --Field "file" to hold the binary file
    '--', @Boundary, @CRLF +
    'Content-Disposition: form-data; name="file"; filename="', @Filename, '"', @CRLF,
    'Content-Type: ', @FileType, @CRLF,
    --'Content-Type: application/octet-stream', @CRLF,
    --'Content-Type: text/plain', @CRLF,
    'Content-Transfer-Encoding: binary', @CRLF,
     @CRLF --Extra CRLF is REQUIRED!!!
    
     )

  --Add binary data payload
  SET @DataToSendBin = CAST(@DataToSend AS varbinary(MAX)) + @AudioData

  SET @DataToSend = NULL --make sure we are using @DataToSendBin and not @DataToSend

  --Closing boundary
  SET @DataToSendBin = @DataToSendBin +
    CAST(@CRLF +'--' + @Boundary + '--' + @CRLF AS varbinary(MAX)) --Extra CRLF is REQUIRED!!!


  DECLARE @BinBuf varbinary(MAX)
  DECLARE @HTTPStatus int
  DECLARE @Cookies varchar(MAX)
  DECLARE @RedirURL varchar(4000)
  DECLARE @ErrorMsg varchar(MAX)

  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Performing HTTP POST to transcribe audio.')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END
  

  BEGIN TRY
    -- Perform HTTP POST request
    EXEC sqlver.sputilGetHTTP_CLR
      @URL = @URL,
      @HTTPMethod = 'POST',
      @Headers = @Headers,
      @DataToSend = NULL, --@DataToSend,
      @DataToSendBin = @DataToSendBin,
      @Cookies = @Cookies OUTPUT,
      @HTTPStatus = @HTTPStatus OUTPUT,
      @RedirURL = @RedirURL OUTPUT,
      @ResponseBinary = @BinBuf OUTPUT,
      @ErrorMsg = @ErrorMsg OUTPUT

    IF @Debug = 1 
    BEGIN
      SET @Msg = CONCAT(@MN, 'Response received: @HTTPStatus=', @HTTPStatus, '; @Headers=', @Headers, '; @RedirURL=', @RedirURL);
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    END

    -- Convert binary response to JSON
    SET @JSON = CAST(@BinBuf AS varchar(MAX))


    -- Extract transcript from JSON response
    --SET @TranscriptText = JSON_VALUE(@JSON, '$.text')  --will not work if the transcript is more than 4000 characters long.
    SELECT @TranscriptText = j.TranscriptText FROM OPENJSON(@JSON) WITH (TranscriptText varchar(MAX) '$.text') j

    IF @TranscriptText IS NULL
    BEGIN
      SET @Msg = CONCAT(@MN, 'Error: No transcript returned from OpenAI.')
      EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
      RAISERROR(@Msg, 16, 1)
      RETURN (1004)
    END


  END TRY
  BEGIN CATCH
    SET @Msg = CONCAT(@MN, 'Error while processing response: ', ERROR_MESSAGE())
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RETURN (1005)
  END CATCH

  IF @Debug = 1 
  BEGIN
    SET @Msg = CONCAT(@MN, 'Finished')
    EXEC sqlver.spinsSysRTLog @Msg=@Msg, @ThreadGUID = @ThreadGUID
  END

END

GO


IF OBJECT_ID('[sqlver].[sputilStrToTable]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilStrToTable]
END
GO

CREATE PROCEDURE [sqlver].[sputilStrToTable]
@Buf nvarchar(MAX),
@ColumnCount int = NULL,
@EOL nchar(1) = NULL,  @EOF nchar(1) = NULL,  @UseFirstRowColumns bit = 0,
@StripQuotes bit = 1
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  IF @ColumnCount > ISNULL((SELECT MAX(Number) FROM sqlver.tblNumbers), 0) BEGIN
    RAISERROR('Error in sqlver.sputilStrToTable:  @ColumnCount (%i) exceeds maximum number in sqlver.tblNumbers.', 16, 1, @ColumnCount)
    RETURN 2001
  END


  IF @EOL IS NULL BEGIN
    SET @EOL = CHAR(13)
  END

  IF @EOF IS NULL BEGIN
    SET @EOF = ','
  END


  IF @ColumnCount IS NULL BEGIN
    SELECT @ColumnCount = COUNT(*)
    FROM
     (
      SELECT TOP 1
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowSeq,
        value AS OneRow
      FROM
        STRING_SPLIT(@Buf, @EOL)
      ) r
      CROSS APPLY STRING_SPLIT(r.OneRow, @EOF) c
  END


  DECLARE @ColXref nvarchar(MAX)

  IF @UseFirstRowColumns = 1 BEGIN
    SET @ColXref = 'RowSeq'
    SELECT @ColXref = ISNULL(@ColXref + ', ', '') + '[' + CAST(splt.ColSeq AS varchar(100)) + '] AS [' +
      CASE
        WHEN @StripQuotes = 1 AND LEN(splt.Value) >= 2 THEN
          REPLACE(
            REPLACE(LEFT(LTRIM(splt.Value), 1), '"', '') +
            SUBSTRING(RTRIM(LTRIM(splt.Value)), 2, LEN(RTRIM(LTRIM(splt.Value))) - 2) +
            REPLACE(RIGHT(RTRIM(splt.Value), 1), '"', ''), '""', '"')
        ELSE splt.Value
      END 
      + ']'
    FROM
    (
    SELECT 
      r.RowSeq,
      ROW_NUMBER() OVER (PARTITION BY r.RowSeq ORDER BY (SELECT NULL)) AS ColSeq,
      c.Value
    FROM
     (
      SELECT TOP 1
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowSeq,
        value AS OneRow
      FROM
        STRING_SPLIT(@Buf, @EOL)
      ) r
      CROSS APPLY STRING_SPLIT(r.OneRow, @EOF) c
    ) splt
    WHERE
      (@ColumnCount IS NULL OR
      splt.ColSeq <= @ColumnCount)
  END


  DECLARE @ColList nvarchar(MAX)
  SELECT @ColList = ISNULL(@ColList + ',', '') + '[' + CAST(n.Number AS varchar(100)) + ']'
  FROM
    sqlver.tblNumbers n
  WHERE
    n.Number <= @ColumnCount

  DECLARE @SQL nvarchar(MAX)

  SET @SQL = 
  'SELECT
    *
  FROM
    (
    SELECT 
      r.RowSeq,
      ROW_NUMBER() OVER (PARTITION BY r.RowSeq ORDER BY (SELECT NULL)) AS ColSeq,
      CASE
        WHEN @StripQuotes = 1 AND LEN(c.Value) >= 2 THEN
          REPLACE(
            REPLACE(LEFT(LTRIM(c.Value), 1), ''"'', '''') +
            SUBSTRING(RTRIM(LTRIM(c.Value)), 2, LEN(RTRIM(LTRIM(c.Value))) - 2) +
            REPLACE(RIGHT(RTRIM(c.Value), 1), ''"'', ''''), ''""'', ''"'')
        ELSE c.Value
      END AS Value
    FROM
     (
      SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowSeq,
        CASE
          WHEN 1=1 AND LEN(value) > 1 THEN
            REPLACE(LEFT(value, 1), CHAR(10), '''') + SUBSTRING(value, 2, LEN(value) - 1)
          ELSE value
        END AS OneRow
      FROM
        STRING_SPLIT(@Buf, @EOL)
      ) r
      CROSS APPLY STRING_SPLIT(r.OneRow, @EOF) c
    ) splt

    PIVOT (
      MAX(Value) FOR ColSeq IN (' + @ColList + ')) pvt'

  IF @ColXref IS NOT NULL BEGIN
    SET @SQL = 'SELECT ' + @ColXref + ' FROM (' + @SQL + ') x WHERE x.RowSeq > 1'
  END

  PRINT @SQL

  EXEC sp_executesql
    @SQL,
    N'@Buf nvarchar(MAX), @EOL nchar(1), @EOF nchar(1), @StripQuotes bit',
    @Buf = @Buf,
    @EOL = @EOL,
    @EOF = @EOF,
    @StripQuotes = @StripQuotes

END

GO


IF OBJECT_ID('[sqlver].[spgetSecureValue]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSecureValue]
END
GO

CREATE PROCEDURE [sqlver].[spgetSecureValue]
@KeyName sysname,
@PlainValue nvarchar(4000) = NULL OUTPUT,
@PlainValueBin varbinary(8000) = NULL OUTPUT,
@CryptKey nvarchar(1024) = NULL,
@SuppressResultset bit = 1
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @CryptKey IS NULL BEGIN
    SELECT
      --@CryptKey = ENCRYPTBYPASSPHRASE('sqlver', sv.SecureValue)
      @CryptKey =sv.SecureValue
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.id = '0'

  END

  DECLARE @SVID int

  SELECT @SVID = sv.id
  FROM
    sqlver.tblSecureValues sv
  WHERE
    sv.KeyName = @KeyName


  IF @SVID IS NULL BEGIN
    SET @PlainValueBin = NULL
  END
  ELSE BEGIN
    SELECT
      @PlainValueBin = DECRYPTBYPASSPHRASE(@CryptKey, sv.SecureValue)
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.ID = @SVID
  END

  SET @PlainValue = CAST(@PlainValueBin AS nvarchar(4000))

  IF ISNULL(@SuppressResultset , 0) = 0 BEGIN
    SELECT
      sv.KeyName,
      @PlainValue AS PlainValue,
      @PlainValueBin AS PlainValueBin,
      sv.DateUpdated
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.ID = @SVID
  END

END

GO


IF OBJECT_ID('[sqlver].[spsysBackupFull]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBackupFull]
END
GO

CREATE PROCEDURE [sqlver].[spsysBackupFull]
@PerformCheck bit = 1,  --Performs DBCC CHECKDB
@PerformMaint bit = 1,  --Rebuilds all indexes and statistics
@PerformBU bit = 1,     --Performs actual full backup
@BUPath nvarchar(1024) = 'D:\Backup\Full\',  --Path on the server to store the backup
@BUFileNameSuffix nvarchar(80) = NULL, --suffix to append to the generated backup filename
@FullFileName nvarchar(512) = NULL OUTPUT  --Returns the actual filename

WITH EXECUTE AS CALLER
--$!SQLVer Oct 24 2024  5:31AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Debug bit
  SET @Debug = 0
  
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
      

      SET @BUFileName = @DBName + '_' + CONVERT(varchar(100), GETDATE(), 112)     
      SET @FullFileName = @BUPath + @DBName + '\' + @BUFileName  + ISNULL(@BUFileNameSuffix, '') + '.bak'  
      
   
      SET @SQL = 'BACKUP DATABASE [' + @DBName + '] TO  DISK = N''' + @FullFileName + '''' +
        ' WITH NOFORMAT, NOINIT,  NAME = N''' + @BUFileName + ''', SKIP, REWIND, NOUNLOAD, COMPRESSION,  STATS = 10'      
      
      IF @Debug = 1 BEGIN
        SET @Msg = 'sqlver.spsysBackupFull: ' + @SQL               
        EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID
        PRINT @Msg
      END         
   
      EXEC(@SQL)  

      PRINT 'Database ' + @DBName + ' backed up to ' + @FullFileName
      
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


IF OBJECT_ID('[sqlver].[spgetSQLTempDBSessions]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLTempDBSessions]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLTempDBSessions]
--$!SQLVer Jan 11 2022 10:46AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF NOT EXISTS(SELECT schema_id FROM sys.schemas WHERE name = 'opsstream') BEGIN
    SELECT
      x.SPID,  
      DB_NAME(spr.dbid) AS DBName,
      spr.loginame,
      j.name AS JobName,
      x.TempDBKB,
      spr.open_tran,
      spr.status,
      spr.cpu,
      spr.physical_io,
      spr.login_time,
      spr.last_batch,
      spr.hostname,
      spr.lastwaittype,
      ISNULL(OBJECT_NAME(txt.objectid), txt.text) AS Command
    FROM
      (
      SELECT
        tu.session_id AS SPID,
        (tu.user_objects_alloc_page_count - tu.user_objects_dealloc_page_count + tu.internal_objects_alloc_page_count - tu.internal_objects_dealloc_page_count) * 8192 /1024 AS TempDBKB
      FROM
        tempdb.sys.dm_db_session_space_usage (NOLOCK) tu
      ) x      
      LEFT JOIN sys.sysprocesses (NOLOCK) spr ON
        x.SPID = spr.spid
      LEFT JOIN msdb.dbo.sysjobactivity (NOLOCK) ja ON
        x.SPID = ja.session_id AND
        ja.run_requested_date IS NOT NULL AND
        ja.stop_execution_date IS NULL
      LEFT JOIN msdb.dbo.sysjobs_view (NOLOCK) j ON
        ja.job_id = j.job_id
      LEFT JOIN sys.dm_exec_connections (nolock) c ON
        c.session_id = x.spid
      OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) txt
    ORDER BY
      x.TempDBKB DESC
  END

  ELSE BEGIN
    SELECT
      x.SPID,  
      DB_NAME(spr.dbid) AS DBName,
      spr.loginame,
      u.UserName AS OpsStreamUser,
      j.name AS JobName,
      ses1.DateStarted,
      ses1.DateLast,
      x.TempDBKB,
      spr.open_tran,
      spr.status,
      spr.cpu,
      spr.physical_io,
      spr.login_time,
      spr.last_batch,
      spr.hostname,
      spr.lastwaittype,
      ISNULL(OBJECT_NAME(txt.objectid), txt.text) AS Command
    FROM
      (
      SELECT
        tu.session_id AS SPID,
        (tu.user_objects_alloc_page_count - tu.user_objects_dealloc_page_count + tu.internal_objects_alloc_page_count - tu.internal_objects_dealloc_page_count) * 8192 /1024 AS TempDBKB
      FROM
        tempdb.sys.dm_db_session_space_usage (NOLOCK) tu
      ) x
      LEFT JOIN (
        SELECT
          ses.SPID,
          ses.UserID,
          ses.DateStarted,
          ses.DateLast,
          ROW_NUMBER() OVER (PARTITION BY ses.spid ORDER BY ses.SysSessionID DESC) AS Seq
        FROM
          opsstream.tblSysUserSessions (NOLOCK) ses
        ) ses1 ON
          ses1.seq = 1 AND
          x.SPID = ses1.SPID
      LEFT JOIN opsstream.tblUsers (NOLOCK) u ON
        ses1.UserID = u.UserID
      LEFT JOIN sys.sysprocesses (NOLOCK) spr ON
        x.SPID = spr.spid
      LEFT JOIN msdb.dbo.sysjobactivity (NOLOCK) ja ON
        x.SPID = ja.session_id AND
        ja.run_requested_date IS NOT NULL AND
        ja.stop_execution_date IS NULL
      LEFT JOIN msdb.dbo.sysjobs_view (NOLOCK) j ON
        ja.job_id = j.job_id
      LEFT JOIN sys.dm_exec_connections (nolock) c ON
        c.session_id = x.spid
      OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) txt
    ORDER BY
      x.TempDBKB DESC
  END

END

GO


IF OBJECT_ID('[sqlver].[spShowRTLog]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spShowRTLog]
END
GO

CREATE PROCEDURE [sqlver].[spShowRTLog]
@MsgLike varchar(MAX) = NULL,
@ThreadGUID uniqueidentifier = NULL,
@ID int = NULL
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
  ;
  WITH
  cte AS
  (
    SELECT TOP 5000
    ROW_NUMBER() OVER (ORDER BY rt.SysRTLogID DESC) Seq,
    rt.*
  FROM 
    sqlver.tblSysRTLog rt
  WHERE
    (@MsgLike IS NULL OR rt.Msg LIKE @MsgLike + '%')
  )

  SELECT 
    c.*,
    DATEDIFF_BIG(millisecond, c2.DateLogged, c.DateLogged) AS MSElapsed
  FROM
    cte c
    LEFT JOIN cte c2 ON
      c.Seq = c2.Seq - 1
  ORDER BY
    c.Seq


  IF @ID IS NOT NULL BEGIN
    DECLARE @Buf nvarchar(Max)
      SELECT @Buf = Msg FROM sqlver.tblSysRTLog WHERE SysRTLogID = @ID
    EXEC sqlver.sputilPrintString @Buf
  END
     
END

GO


IF OBJECT_ID('[sqlver].[spSQLRAMUsed]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spSQLRAMUsed]
END
GO

CREATE PROCEDURE sqlver.spSQLRAMUsed
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --from https://www.mssqltips.com/sqlservertip/2393/determine-sql-server-memory-use-by-database-and-object/
  --by Aaron Bertrand   Updated: 2011-05-19

  -- Note: querying sys.dm_os_buffer_descriptors
  -- requires the VIEW_SERVER_STATE permission.

  DECLARE @total_buffer INT;

  SELECT @total_buffer = pc.cntr_value
  FROM
    sys.dm_os_performance_counters pc
  WHERE
    RTRIM(pc.[object_name]) LIKE '%Buffer Manager' AND
    pc.counter_name = 'Database Pages';

  ;WITH src AS
  (
  SELECT 
    bd.database_id,
    COUNT_BIG(*) AS db_buffer_pages
  FROM
    sys.dm_os_buffer_descriptors bd
  WHERE
    bd.database_id BETWEEN 5 AND 32766
  GROUP BY
    bd.database_id
  )
  SELECT
    CASE csv.[database_id]
      WHEN 32767 THEN 'Resource DB' 
      ELSE DB_NAME(csv.[database_id])
    END AS [db_name],
    csv.db_buffer_pages,
    csv.db_buffer_pages / 128 AS db_buffer_MB,
    CONVERT(DECIMAL(6,3), csv.db_buffer_pages * 100.0 / @total_buffer) AS db_buffer_percent
  FROM src csv
  ORDER BY
    db_buffer_MB DESC; 

END

GO


IF OBJECT_ID('[sqlver].[spgetSSRSDatasets]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSSRSDatasets]
END
GO

CREATE PROCEDURE [sqlver].[spgetSSRSDatasets]
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spsysReprocessObjects]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysReprocessObjects]
END
GO

CREATE PROCEDURE [sqlver].[spsysReprocessObjects]
@TargetStr nvarchar(MAX)

WITH EXECUTE AS CALLER
--$!SQLVer Jul 13 2021  8:06AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @tvFind TABLE (
    SchemaName sysname,
    ObjectName sysname,
    Context nvarchar(MAX)
  )
  INSERT INTO @tvFind
  EXEC sqlver.spUtilFindInCode @TargetStr

  DECLARE @SQL nvarchar(MAX)
  DECLARE @ThisSchemaName sysname
  DECLARE @ThisObjectName sysname
  DECLARE @P int
  DECLARE @P2 int

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    fnd.SchemaName,
    fnd.ObjectName
  FROM
    @tvFind fnd

  OPEN curThis
  FETCH curThis INTO @ThisSchemaName, @ThisObjectName

  WHILE @@FETCH_STATUS = 0 BEGIN
    PRINT 'sqlver.spsysReprocessObjects: ' + @ThisSchemaName + '.' + @ThisObjectName

    EXEC sqlver.spsysSchemaProcessObject @SchemaName = @ThisSchemaName, @ObjectName = @ThisObjectName
    
    /*
    --SET @SQL = OBJECT_DEFINITION(OBJECT_ID(@ThisObject))

    --Switch ALTER to CREATE for hash
    --Find the first ALTER that is in uncommented SQL code

    SET @P = sqlver.udfFindInSQL('CREATE', @SQL, 0)
    SET @P2 = sqlver.udfFindInSQL('ALTER', @SQL, 0)

    IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
      SET @SQL = STUFF(@SQL, @P, LEN('CREATE'), 'ALTER')
    END

    EXEC(@SQL)
    */


    FETCH curThis INTO @ThisSchemaName, @ThisObjectName
  END

  CLOSE curThis
  DEALLOCATE curThis

END

GO


IF OBJECT_ID('[sqlver].[spVersion]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spVersion]
END
GO

CREATE PROCEDURE [sqlver].[spVersion]
@ObjectName nvarchar(512) = NULL,
@MaxVersions int = NULL,
@ChangedSince datetime = NULL,
@SchemaLogId int = NULL,
@SortByName bit = 0
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[sputilFindProc]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilFindProc]
END
GO

CREATE PROCEDURE sqlver.sputilFindProc
@TargetProc sysname = NULL,
@ExecProc sysname = NULL,
@ExcludeRTLog bit = 1
--$!SQLVer Sep  3 2022  5:49AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SELECT
    procs.object_id,
    DB_ID() AS DatabaseID,
    OBJECT_SCHEMA_NAME(procs.object_id) AS SchemaName,
    procs.name AS ProcName,
    OBJECT_SCHEMA_NAME(procs.object_id) + '.' + procs.name AS FQName,
    tmp.ExecProc,
    tmp.StartPos,
    tmp.ResultContext
  FROM
    sys.procedures procs
    CROSS APPLY sqlver.udftFindExec(procs.object_id) tmp
  WHERE
    (@TargetProc IS NULL OR PATINDEX('%' + @TargetProc + '%', procs.name) > 0) AND
    (@ExecProc IS NULL OR PATINDEX('%' + @ExecProc + '%', tmp.ExecProc) > 0) AND
    (ISNULL(@ExcludeRTLog, 0) = 0 OR NOT tmp.ExecProc IN ('opsstream.spinsSysRTMessages', 'sqlver.spinsSysRTLog'))
END

GO


IF OBJECT_ID('[sqlver].[sputilFindInCode]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilFindInCode]
END
GO

CREATE PROCEDURE [sqlver].[sputilFindInCode]
@TargString nvarchar(254),
@SchTarg sysname = NULL,
@ObjTarg sysname = NULL,
@TargString2 nvarchar(254) = NULL
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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
      CASE WHEN PATINDEX('%' + @TargString + '%', sysmod.definition) - @PreLen < 1 
        THEN 1 
        ELSE PATINDEX('%' + @TargString + '%', sysmod.definition) - @PreLen
      END,
       
      CASE WHEN PATINDEX('%' + @TargString + '%', sysmod.definition) + LEN(@TargString + 'x') - 1 + @PreLen + @PostLen > LEN(sysmod.definition + 'x') - 1
        THEN LEN(sysmod.definition + 'x') - 1 - PATINDEX('%' + @TargString + '%', sysmod.definition) + 1
        ELSE LEN(@TargString + 'x') - 1 + @PreLen + @PostLen
      END) AS Context
  INTO #Results
  FROM
    sys.objects so
    JOIN sys.schemas sch ON so.schema_id = sch.schema_id
    JOIN sys.sql_modules  sysmod ON so.object_id = sysmod.object_id
  WHERE 
    ((@SchTarg IS NULL) OR (sch.name = @SchTarg)) AND
    ((@ObjTarg IS NULL) OR (PATINDEX('%' + @ObjTarg + '%', so.name) > 0)) AND
    (PATINDEX('%' + @TargString + '%', sysmod.definition) > 0) AND
    ((@TargString2 IS NULL) OR (PATINDEX('%' + @TargString2 + '%', sysmod.definition) > 0))
    
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
      CASE WHEN PATINDEX('%' + @TargString + '%', sysjs.command) - @PreLen < 1 
        THEN 1 
        ELSE PATINDEX('%' + @TargString + '%', sysjs.command) - @PreLen
      END,
       
      CASE WHEN PATINDEX('%' + @TargString + '%', sysjs.command) + LEN(@TargString + 'x') - 1 + @PreLen + @PostLen > LEN(sysjs.command + 'x') - 1
        THEN LEN(sysjs.command + 'x') - 1 - PATINDEX('%' + @TargString + '%', sysjs.command) + 1
        ELSE LEN(@TargString + 'x') - 1 + @PreLen + @PostLen
      END) collate database_default AS Context     
  FROM
    msdb.dbo.sysjobs sysj
    JOIN msdb.dbo.sysjobsteps sysjs ON
      sysj.job_id = sysjs.job_id      
  WHERE 
    (PATINDEX('%' + @TargString + '%', sysjs.command) > 0)
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


IF OBJECT_ID('[sqlver].[spsysRewriteProcWithUniqueTemps]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysRewriteProcWithUniqueTemps]
END
GO

CREATE PROCEDURE [sqlver].[spsysRewriteProcWithUniqueTemps]
@Object nvarchar(512),
@TableName sysname = NULL
--$!SQLVer Sep  6 2022 10:35AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Object) = 1 BEGIN
    SET @ObjID = CAST(@Object AS int)
  END
  ELSE IF LEN(@Object) < 254 BEGIN
    SET @ObjID = OBJECT_ID(@Object)
  END

  DECLARE @FQName nvarchar(512)
  SET @FQName = OBJECT_SCHEMA_NAME(@ObjID) + '.' + OBJECT_NAME(@ObjID)
  
  IF OBJECT_ID('tempdb..#TempUsages') IS NOT NULL BEGIN
    DROP TABLE #TempUsages
  END

  CREATE TABLE #TempUsages (
    id int IDENTITY PRIMARY KEY,
    database_id int,
    object_id int,
    SchemaName sysname,
    ProcName sysname,
    FQName nvarchar(512),
    TempTable sysname,
    StartPos int,
    ResultContext nvarchar(254)
  )

  INSERT INTO #TempUsages (
    object_id,
    database_id,
    SchemaName,
    ProcName,
    FQName,
    TempTable,
    StartPos,
    ResultContext
  )
  SELECT
    procs.object_id,
    DB_ID(),
    OBJECT_SCHEMA_NAME(procs.object_id),
    procs.name,
    OBJECT_SCHEMA_NAME(procs.object_id) + '.' + procs.name AS FQName,
    tmp.TempTable,
    tmp.StartPos,
    tmp.ResultContext
  FROM
    sys.procedures procs
    CROSS APPLY sqlver.udftFindTempTables(procs.object_id) tmp
    LEFT JOIN #TempUsages tu ON
      procs.object_id = tu.object_id
  WHERE
    procs.object_id = @ObjID AND
    tu.id IS NULL
 
  IF OBJECT_ID('sqlver.tblTempTables') IS NULL BEGIN
  CREATE TABLE sqlver.tblTempTables (
    TempTableID int IDENTITY PRIMARY KEY,
    TableName sysname,
    FoundInProc_ObjectID int,
    FoundInProc_FQName nvarchar(512),
    FirstStartPos int
  )
  END


  DELETE
  FROM
    sqlver.tblTempTables
  WHERE
    FoundInProc_FQName = @FQName

  INSERT INTO sqlver.tblTempTables (
    TableName,
    FoundInProc_ObjectID,
    FoundInProc_FQName,
    FirstStartPos
  )

  SELECT
    tu.TempTable,
    tu.object_id,
    tu.FQName,
    MIN(tu.StartPos)
  FROM
    #TempUsages tu
    LEFT JOIN sqlver.tblTempTables tt ON
      tu.object_id = tt.FoundInProc_ObjectID AND
      tu.TempTable = tt.TableName
  WHERE
    tu.object_id = @ObjID AND
    (@TableName IS NULL OR tu.TempTable = @TableName)  AND
    tu.TempTable NOT LIKE '#ix%' AND
    tt.TempTableID IS NULL
  GROUP BY
    tu.TempTable,
    tu.object_id,
    tu.FQName

  DECLARE @SQL nvarchar(MAX)

/*
  IF (SELECT db.compatibility_level FROM sys.databases db WHERE db.database_id = DB_ID()) >= 140 BEGIN
   --Modern database / can use STRING_AGG()
    SELECT @SQL =

      STRING_AGG(
        CONCAT(x.PreSQL, x.SQL),
        ''
      )
      WITHIN GROUP (
        ORDER BY
          x.StartPos
      )

    FROM
      (

      SELECT
        tt.FoundInProc_FQName,
        tt.TableName,
        tt.TempTableID,
        tu.StartPos,

        CASE 
          WHEN LAG(tu.StartPos) OVER (ORDER BY tu.id) IS NULL
            THEN LEFT(OBJECT_DEFINITION(tt.FoundInProc_ObjectID), tu.StartPos -1)
          ELSE ''
        END AS PreSQL,

        STUFF(

          SUBSTRING(
            OBJECT_DEFINITION(tt.FoundInProc_ObjectID),
            tu.StartPos,

            ISNULL(
              LEAD(
                tu.StartPos
              ) OVER(ORDER BY tu.StartPos) - 1 - tu.StartPos + 1, 

            LEN(OBJECT_DEFINITION(tt.FoundInProc_ObjectID)) --default (i.e. for last row)
            )
          ),
  
          1,
          LEN(tt.TableName),
          '#' + '___' + CAST(DB_ID() AS varchar(100)) + '_' + CAST(tt.TempTableID AS varchar(100)) + tt.TableName

        ) AS SQL

      FROM
        sqlver.tblTempTables tt
        JOIN #TempUsages tu ON
          tt.FoundInProc_ObjectID = tu.object_id AND
          tt.TableName = tu.TempTable
      WHERE
        tt.FoundInProc_ObjectID = @ObjID AND
        tt.TableName NOT LIKE '#[_][_][_]%' AND --indicates the name has already been rewritten
        tt.TableName NOT LIKE '#[_][_]%'--explicitly indicates that rewriting is not allowed

      ) x
  END
  ELSE BEGIN
    --older database / no STRING_AGG(), so use FOR XML PATH
*/
    SELECT @SQL = REPLACE(
    
        (
        SELECT
          CONVERT(xml, CONCAT(x.PreSQL, x.SQL))
        FROM
          (

          SELECT
            tt.FoundInProc_FQName,
            tt.TableName,
            tt.TempTableID,
            tu.StartPos,

            CASE 
              WHEN LAG(tu.StartPos) OVER (ORDER BY tu.id) IS NULL
                THEN LEFT(OBJECT_DEFINITION(tt.FoundInProc_ObjectID), tu.StartPos -1)
              ELSE ''
            END AS PreSQL,

            STUFF(

              SUBSTRING(
                OBJECT_DEFINITION(tt.FoundInProc_ObjectID),
                tu.StartPos,

                ISNULL(
                  LEAD(
                    tu.StartPos
                  ) OVER(ORDER BY tu.StartPos) - 1 - tu.StartPos + 1, 

                LEN(OBJECT_DEFINITION(tt.FoundInProc_ObjectID)) --default (i.e. for last row)
                )
              ),
  
              1,
              LEN(tt.TableName),
              '#' + '___' + CAST(DB_ID() AS varchar(100)) + '_' + CAST(tt.TempTableID AS varchar(100)) + tt.TableName

            ) AS SQL

          FROM
            sqlver.tblTempTables tt
            JOIN #TempUsages tu ON
              tt.FoundInProc_ObjectID = tu.object_id AND
              tt.TableName = tu.TempTable
          WHERE
            tt.FoundInProc_ObjectID = @ObjID AND
            tt.TableName NOT LIKE '#[_][_][_]%' AND --indicates the name has already been rewritten
            tt.TableName NOT LIKE '#[_][_]%'--explicitly indicates that rewriting is not allowed
          ) x
        ORDER BY
          x.StartPos
        FOR XML PATH('')
        )
        , NCHAR(10), NCHAR(13) + NCHAR(10))

  --END


    --Switch CREATE to ALTER
    DECLARE @P int
    DECLARE @P2 int

    SET @P = sqlver.udfFindInSQL('CREATE', @SQL, 0)
    SET @P2 = sqlver.udfFindInSQL('ALTER', @SQL, 0)

    IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
      SET @SQL = STUFF(@SQL, @P, LEN('CREATE'), 'ALTER')
    END

    IF OBJECT_DEFINITION(@ObjID) <> @SQL BEGIN
      EXEC (@SQL)
      PRINT 'sqlver.spsysRewriteProcWithUniqueTemps: Updated definition of ' + OBJECT_SCHEMA_NAME(@ObjID) + '.' + OBJECT_NAME(@ObjID)
    END
    ELSE BEGIN
      PRINT 'sqlver.spsysRewriteProcWithUniqueTemps: No changes made to '  + OBJECT_SCHEMA_NAME(@ObjID) + '.' + OBJECT_NAME(@ObjID)
    END

END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLRAssemblyCache]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLRAssemblyCache]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLRAssemblyCache]
@TargetPath nvarchar(1024) = 'C:\SQLVer\AssemblyCache\',
@AssemblyPath nvarchar(1024) = 'C:\SQLVer\AssemblyLibrary\'
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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
    (1, 'C:\Windows\Microsoft.NET\Framework\v4.0.30319\'),
    --(1, 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\'),
    --(2, 'C:\Windows\assembly\GAC_MSIL\'),
    --(3, 'C:\Windows\assembly\GAC_64\'),
    (4, @AssemblyPath)    
    
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
    @FileName = 'CopySystemDLLs.bat',
    @ErrorMsg = NULL
   
   
    --Temporarily enable xp_cmdshell support so we can build the source code
    DECLARE @OrigSupport_XPCmdShell bit
    SELECT @OrigSupport_XPCmdShell = CONVERT(bit, value_in_use) FROM sys.configurations WHERE name = 'xp_cmdshell'
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


IF OBJECT_ID('[sqlver].[spsysBuildCLRAssemblyInfo]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLRAssemblyInfo]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLRAssemblyInfo]
@PerformDropAll bit = 0
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spStoreSecureValue]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spStoreSecureValue]
END
GO

CREATE PROCEDURE [sqlver].[spStoreSecureValue]
@KeyName sysname,
@PlainValue nvarchar(4000) = NULL,
@PlainValueBin varbinary(8000) = NULL,
@CryptKey nvarchar(1024) = NULL
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  IF @PlainValueBin IS NULL BEGIN
    SET @PlainValueBin = CAST(@PlainValue AS varbinary(MAX))
  END

  IF @CryptKey IS NULL BEGIN

    SELECT
      @CryptKey = CAST(sv.SecureValue AS nvarchar(1024))
    FROM
      sqlver.tblSecureValues sv
    WHERE
      sv.id = 0

    IF @CryptKey IS NULL BEGIN
      SET @CryptKey = sqlver.udfGUIDToStr(NEWID()) + sqlver.udfGUIDToStr(NEWID())

      SET IDENTITY_INSERT sqlver.tblSecureValues ON

      INSERT INTO sqlver.tblSecureValues (
        id,
        KeyName,
        DateUpdated,
        SecureValue
      )
      VALUES (
        0,
        'DefaultKey',
        GETDATE(),
        CAST(@CryptKey AS varbinary(1024))
      )

      SET IDENTITY_INSERT sqlver.tblSecureValues OFF
    END

    --SET @CryptKey = ENCRYPTBYPASSPHRASE('sqlver', @CryptKey)
  END

  DECLARE @SVID int

  SELECT @SVID = sv.id
  FROM
    sqlver.tblSecureValues sv
  WHERE
    sv.KeyName = @KeyName

  IF @SVID IS NOT NULL BEGIN
    UPDATE sv
    SET
      DateUpdated = GETDATE(),
      SecureValue = ENCRYPTBYPASSPHRASE(@CryptKey, @PlainValueBin)
    FROM
     sqlver.tblSecureValues sv
    WHERE
      sv.id = @SVID
  END
  ELSE BEGIN
    INSERT INTO sqlver.tblSecureValues (
      KeyName,
      DateUpdated,
      SecureValue
    )
    VALUES (
      @KeyName,
      GETDATE(),
      ENCRYPTBYPASSPHRASE( @CryptKey, @PlainValueBin)
    )
  END

END

GO


IF OBJECT_ID('[sqlver].[spsysDropAllCLRAssemblies]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysDropAllCLRAssemblies]
END
GO

CREATE PROCEDURE [sqlver].[spsysDropAllCLRAssemblies]
@ReallyDropAll bit = 0
--$!SQLVer Jan 19 2024 12:20PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @CRLF nvarchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)

  IF @ReallyDropAll = 1 BEGIN
    --Drop existing objects
    IF OBJECT_ID('sqlver.udfMergeWordToPDF_CLR') IS NOT NULL DROP FUNCTION sqlver.udfMergeWordToPDF_CLR
    DROP ASSEMBLY [WordMergeCLR]
    DROP ASSEMBLY [GemBox.Document]    

    IF OBJECT_ID('sqlver.udfURLEncode_CLR') IS NOT NULL DROP FUNCTION sqlver.udfURLEncode_CLR
    IF OBJECT_ID('sqlver.udfURLDecode_CLR') IS NOT NULL DROP FUNCTION sqlver.udfURLDecode_CLR
    IF OBJECT_ID('sqlver.sputilGetHTTP_CLR') IS NOT NULL DROP PROCEDURE sqlver.sputilGetHTTP_CLR
    DROP ASSEMBLY [GetHTTPCLR_SQLCLR]

    IF OBJECT_ID('sqlver.sputilSendMail_CLR') IS NOT NULL DROP PROCEDURE sqlver.sputilSendMail_CLR
    IF OBJECT_ID('sqlver.udfGetMIMEType_CLR') IS NOT NULL DROP FUNCTION sqlver.udfGetMIMEType_CLR
    IF OBJECT_ID('sqlver.udfBase64Encode_CLR') IS NOT NULL DROP FUNCTION sqlver.udfBase64Encode_CLR
    DROP ASSEMBLY [SendMail_SQLCLR]

    IF OBJECT_ID('sqlver.sputilFTPUpload_CLR') IS NOT NULL DROP PROCEDURE sqlver.sputilFTPUpload_CLR
    IF OBJECT_ID('sqlver.sputilFTPDownload_CLR') IS NOT NULL DROP PROCEDURE sqlver.sputilFTPDownload_CLR
    DROP ASSEMBLY [FTPCLR]


    DECLARE @SQL nvarchar(MAX)


    WHILE EXISTS (SELECT name FROM sys.assemblies ass WHERE ass.name <> 'Microsoft.SqlServer.Types') BEGIN
      SET @SQL = NULL
       
      BEGIN TRY
        
        SELECT @SQL = ISNULL(@SQL + @CRLF, '') + N' BEGIN TRY DROP ASSEMBLY [' + ass.name + '] END TRY BEGIN CATCH PRINT ''Could not drop assembly ' + ass.name + '  Will retry.'' END CATCH'   
        FROM sys.assemblies ass        
        WHERE
         ass.name <> 'Microsoft.SqlServer.Types'

        PRINT @SQL
        EXEC (@SQL)     
      END TRY
      BEGIN CATCH
        PRINT ERROR_MESSAGE()
         
      END CATCH
    END
  END
  ELSE BEGIN
      
    PRINT 'WARNING:  sqlver.spsysDropAllCLRAssemblies will drop all non-Micoroft CLR assemblies from the database.' + @CRLF + @CRLF +
          'This may include assemblies that were not created by SQLVer.' + @CRLF + @CRLF +
          'If you are sure this is what you want to do, re-run this procedure with:' + @CRLF + @CRLF +
          '    EXEC sqlver.spsysDropAllCLRAssemblies @ReallyDropAll = 1'
        
  END
END

GO


IF OBJECT_ID('[sqlver].[spgetSQLSpaceUsed]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLSpaceUsed]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsed]
--$!SQLVer Dec 19 2020  6:03AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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
  SELECT 'EXEC sp_spaceused ' +  @Q + QUOTENAME(sch.name) + '.' + QUOTENAME(so.name) + @Q, sch.name AS SchemaName
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


IF OBJECT_ID('[sqlver].[spSysCreateSynonyms]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spSysCreateSynonyms]
END
GO

CREATE PROCEDURE [sqlver].[spSysCreateSynonyms]
--$!SQLVer Jan 11 2022 10:56AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  DECLARE @SQL nvarchar(MAX)

  DECLARE @NeedToRenableTrigger bit

  IF EXISTS(SELECT object_id FROM sys.triggers WHERE name = 'dtgSQLVerLogSchemaChanges' AND parent_class = 0 AND is_disabled = 0) BEGIN
    SET @SQL = 'DISABLE TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE'
    EXEC (@SQL)
    SET @NeedToRenableTrigger = 1
  END

  IF OBJECT_ID('tempdb..#Synonyms') IS NOT NULL BEGIN
    DROP TABLE #Synonyms
  END

  CREATE TABLE #Synonyms (
    OrigSchema sysname,
    OrigName sysname,
    OrigType sysname,
    NewSchema sysname,
    NewName sysname,
    NewType sysname,
    StillExists bit
  )

  --Special objects where the name in OpsStream is different than in SQLVer
  INSERT INTO #Synonyms (
    OrigSchema,
    OrigName,
    OrigType,
    NewSchema,
    NewName,
    NewType
  )
  VALUES
    ('opsstream', 'GUIDToStr', 'SN', 'sqlver', 'udfGUIDToStr', 'FN'),
    ('opsstream', 'strToGUID', 'SN', 'sqlver', 'udfStrToGUID', 'FN'),
    --('opsstream', 'ParseJSON', 'SN', 'sqlver', 'udftParseJSON', 'FN'),
    ('opsstream', 'udfGetParsedValues', 'SN', 'sqlver', 'udftGetParsedValues', 'TF'),
    ('opsstream', 'parseValue', 'SN', 'sqlver', 'udfParseValue', 'FN'),
    ('opsstream', 'parseValueReplace', 'SN', 'sqlver', 'udfParseValueReplace', 'FN'),
    ('opsstream', 'parseVarRemove', 'SN', 'sqlver', 'udfParseVarRemove', 'FN'),
    ('opsstream', 'parseVarValue', 'SN', 'sqlver', 'udfParseVarValue', 'FN'),
    ('opsstream', 'spinsSysRTMessage', 'SN', 'sqlver', 'spinsSysRTLog', 'P '),
    ('opsstream', 'spinsSysRTMessages', 'SN', 'sqlver', 'spinsSysRTLog', 'P '),
    ('opsstream', 'spsysBackupWeekly', 'SN', 'sqlver', 'spsysBackupFull', 'P '),
    ('sqlver', 'udftMilesFromZip', 'SN', 'geonames', 'udftMilesFromZip', 'FN')



    --All objects where the name in OpsStream is the same as in SQLVer
    INSERT INTO #Synonyms (
      OrigSchema,
      OrigName,
      OrigType,
      NewSchema,
      NewName,
      NewType,
      StillExists
    )
    SELECT
      sch2.name AS SchName,
      obj2.name AS ObjName,
      obj2.type AS ObjType,
      sch.name AS Sch2Name,
      obj.name AS Obj2Name,
      obj.type AS Obj2Type,
      1 AS StillExists
    FROM
      sys.objects obj
      JOIN sys.schemas sch ON
        obj.schema_id = sch.schema_id
      JOIN sys.objects obj2 ON
        obj.name = obj2.name
      JOIN sys.schemas sch2 ON
        obj2.schema_id = sch2.schema_id
    WHERE
      sch.name = 'sqlver' AND
      sch2.name = 'opsstream' AND

      obj.type IN (
                'P', --SQL_STORED_PROCEDURE
                'PC', --CLR_STORED_PROCEDURE
                'IF', --SQL_INLINE_TABLE_VALUED_FUNCTION
                'FN', --SQL_SCALAR_FUNCTION
                'TF', --SQL_TABLE_VALUED_FUNCTION
                'FS', --CLR_SCALAR_FUNCTION
                'FT' --CLR_TABLE_VALUED_FUNCTION
                --'SN' --SYNONYM
                --'V' --VIEW
                --'U' --USER_TABLE
              )



  --Create synonyms for all remaining objects
  INSERT INTO #Synonyms (
    OrigSchema,
    OrigName,
    OrigType,
    NewSchema,
    NewName,
    NewType
  )
  SELECT
    sch2.name AS SchName,
    obj.name AS ObjName,
    obj.type AS ObjType,
    sch.name AS Sch2Name,
    obj.name AS Obj2Name,
    obj.type AS Obj2Type
  FROM
    sys.objects obj
    JOIN sys.schemas sch ON
      obj.schema_id = sch.schema_id
    JOIN sys.schemas sch2 ON
      sch2.name = 'opsstream'
    LEFT JOIN sys.objects obj2 ON
      obj2.schema_id = sch2.schema_id and
      obj.name = obj2.name
    LEFT JOIN #synonyms tmp ON
      sch.name = tmp.NewSchema AND
      obj.name = tmp.NewName
  WHERE
    tmp.NewName IS NULL AND
    sch.name = 'sqlver' AND
    obj2.object_id IS NULL AND
    obj.type IN (
              'P', --SQL_STORED_PROCEDURE
              'PC', --CLR_STORED_PROCEDURE
              'IF', --SQL_INLINE_TABLE_VALUED_FUNCTION
              'FN', --SQL_SCALAR_FUNCTION
              'TF', --SQL_TABLE_VALUED_FUNCTION
              'FS', --CLR_SCALAR_FUNCTION
              'FT' --CLR_TABLE_VALUED_FUNCTION
              --'SN' --SYNONYM
              --'V' --VIEW
              --'U' --USER_TABLE
            )


  UPDATE tmp
  SET
    StillExists = 1,
    OrigType = obj.type
  FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
    JOIN #Synonyms tmp ON
      sch.name = tmp.OrigSchema AND
      obj.name = tmp.OrigName


  --Drop objects in schema opsstream that have corresponding objects in sqlver
  SET @SQL = NULL

  SELECT
    @SQL = ISNULL(@SQL, '') + ISNULL(CHAR(13) + CHAR(10) +
      'DROP ' + 
      CASE tmp.OrigType
        WHEN 'IF' THEN 'FUNCTION' --SQL_INLINE_TABLE_VALUED_FUNCTION
        WHEN 'FN' THEN 'FUNCTION' --SQL_SCALAR_FUNCTION
        WHEN 'TF' THEN 'FUNCTION' --SQL_TABLE_VALUED_FUNCTION
        WHEN 'FS' THEN 'FUNCTION' --CLR_SCALAR_FUNCTION
        WHEN 'FT' THEN 'FUNCTION'--CLR_TABLE_VALUED_FUNCTION
        WHEN 'P' THEN 'PROCEDURE'
        WHEN 'SN' THEN 'SYNONYM'
        ELSE tmp.OrigType
      END +
      ' [' + tmp.OrigSchema + '].[' + tmp.OrigName + ']'
      , '')
  FROM 
    #Synonyms tmp   
  WHERE
    tmp.StillExists = 1


  PRINT @SQL

  EXEC(@SQL)

            
  --Create Synonyms
  SET @SQL = NULL

  SELECT
    @SQL = ISNULL(@SQL, '') + ISNULL(CHAR(13) + CHAR(10) +
      'CREATE SYNONYM' +  ' [' + tmp.OrigSchema + '].[' + tmp.OrigName + '] FOR' + ' [' + tmp.NewSchema + '].[' + tmp.NewName + ']'
      , '')    
  FROM 
    #Synonyms tmp

  PRINT @SQL

  EXEC(@SQL)

  --Grant permissions
  SET @SQL = NULL

  SELECT
    @SQL = ISNULL(@SQL, '') + ISNULL(CHAR(13) + CHAR(10) +     

    'GRANT ' +
      CASE tmp.NewType
        WHEN 'IF' THEN 'SELECT' --SQL_INLINE_TABLE_VALUED_FUNCTION
        WHEN 'FN' THEN 'EXEC' --SQL_SCALAR_FUNCTION
        WHEN 'TF' THEN 'SELECT' --SQL_TABLE_VALUED_FUNCTION
        WHEN 'FS' THEN 'EXEC' --CLR_SCALAR_FUNCTION
        WHEN 'FT' THEN 'SELECT'--CLR_TABLE_VALUED_FUNCTION
        WHEN 'P' THEN 'EXEC'
      END +
      ' ON [' + tmp.OrigSchema + '].[' + tmp.OrigName + ']' +
      ' TO opsstream_sys' + CHAR(13) + CHAR(10) +
    
    'GRANT ' +
      CASE tmp.NewType
        WHEN 'IF' THEN 'SELECT' --SQL_INLINE_TABLE_VALUED_FUNCTION
        WHEN 'FN' THEN 'EXEC' --SQL_SCALAR_FUNCTION
        WHEN 'TF' THEN 'SELECT' --SQL_TABLE_VALUED_FUNCTION
        WHEN 'FS' THEN 'EXEC' --CLR_SCALAR_FUNCTION
        WHEN 'FT' THEN 'SELECT'--CLR_TABLE_VALUED_FUNCTION
        WHEN 'P' THEN 'EXEC'
      END +
      ' ON [' + tmp.NewSchema + '].[' + tmp.NewName + ']' +
      ' TO opsstream_sys'
      , '')
        
      +
      CASE
        WHEN tmp.NewName = 'spinsSysRTLog' THEN
            CHAR(13) + CHAR(10) +
          'GRANT EXEC ON [' + tmp.OrigSchema + '].[' + tmp.OrigName + ']' + ' TO sqlverLogger' + CHAR(13) + CHAR(10) + 
          'GRANT EXEC ON [' + tmp.NewSchema + '].[' + tmp.NewName + ']' + ' TO sqlverLogger'
        ELSE ''
      END

  FROM 
    #Synonyms tmp

  PRINT @SQL

  EXEC(@SQL)

  --Additional synonyms for convenience

  SET @SQL = NULL
  SELECT @SQL = ISNULL(@SQL + CHAR(13) + CHAR(10), '') + 
    'DROP SYNONYM [' + sch.name + '].[' + syn.name + ']'
  FROM
    sys.synonyms syn
    JOIN sys.schemas sch ON
      syn.schema_id = sch.schema_id
  WHERE
    (
      (sch.name = 'sqlver' AND syn.name = 'find') OR
      (sch.name = 'sqlver' AND syn.name = 'RTLog') OR
      (sch.name = 'sqlver' AND syn.name = 'ver') OR 
      (sch.name = 'dbo' AND syn.name = 'find')    
    )
    
  PRINT @SQL

  EXEC(@SQL)

  SET @SQL = 
    'IF OBJECT_ID(''sqlver.find'') IS NOT NULL DROP SYNONYM sqlver.find
      CREATE SYNONYM sqlver.find FOR sqlver.sputilFindInCode

      IF OBJECT_ID(''sqlver.RTLog'') IS NOT NULL DROP SYNONYM sqlver.RTLog
      CREATE SYNONYM sqlver.RTLog FOR sqlver.spinsSysRTLog

      IF OBJECT_ID(''sqlver.ver'') IS NOT NULL DROP SYNONYM sqlver.ver
      CREATE SYNONYM sqlver.ver FOR sqlver.spVersion

      IF OBJECT_ID(''dbo.find'') IS NOT NULL DROP SYNONYM dbo.find
      CREATE SYNONYM dbo.find FOR sqlver.sputilFindInCode

      IF OBJECT_ID(''dbo.ver'') IS NOT NULL DROP SYNONYM dbo.ver
      CREATE SYNONYM dbo.ver FOR sqlver.spVersion'
  
  PRINT @SQL

  EXEC(@SQL)

  IF @NeedToRenableTrigger = 1 AND EXISTS(SELECT object_id FROM sys.triggers WHERE name = 'dtgSQLVerLogSchemaChanges' AND parent_class = 0) BEGIN
    SET @SQL = 'ENABLE TRIGGER [dtgSQLVerLogSchemaChanges] ON DATABASE'
    EXEC (@SQL)
  END

END

GO


IF OBJECT_ID('[sqlver].[sputilSendMail]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilSendMail]
END
GO

CREATE PROCEDURE [sqlver].[sputilSendMail]
@From nvarchar(4000) = NULL, 
@FromFriendly nvarchar(4000) = NULL,      
@To nvarchar(MAX),  --note:  you can specify friendly name like this 'Steve Friday <something@changeme.com> and can use a comma or semicolon separated list'
@Subject nvarchar(MAX), 
@CC nvarchar(4000) = NULL,
@BCC nvarchar(4000) = NULL,

@TextBody nvarchar(MAX), --use this if sending text
@HTMLBody nvarchar(MAX),  --use this if sending HTML

@AttachFilename nvarchar(4000) = NULL, --If @AttachData is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
@AttachData varbinary(MAX), --Binary data to include as an attachment,
@AttachFilename2 nvarchar(4000) = NULL, --If @AttachData2 is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
@AttachData2 varbinary(MAX), --Binary data to include as an attachment
@SystemLocator varchar(255) = NULL --optional, to append this to the SysLog entry
--$!SQLVer Feb 10 2025  4:26PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Msg nvarchar(MAX)
  
  DECLARE @ThreadGUID uniqueidentifier
  SET @ThreadGUID = NEWID()

  SET @Msg = 'sqlver.sputilSendMail: Sending to ' + CONCAT_WS(', ', @To, @CC, @BCC) + ' Subject: ' + ISNULL(@Subject, 'NULL')
  EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID

  DECLARE @SMTPServerAddress varchar(1000) = sqlver.udfSecureValue('SMTPServer', NULL)
  DECLARE @SMTPServerPort varchar(10) = sqlver.udfSecureValue('SMTPPort', NULL)
  DECLARE @SMTPUseSSL bit = sqlver.udfSecureValue('SMTPUseSSL', NULL)
  DECLARE @SMTPUser varchar(255) = sqlver.udfSecureValue('SMTPUser', NULL)
  DECLARE @SMTPPassword varchar(255) = sqlver.udfSecureValue('SMTPPassword', NULL)


  IF NULLIF(RTRIM(@SMTPServerAddress), '') IS NULL BEGIN
    SET @Msg = 'Error in sqlver.sputilSendMail:  SMTP settings not present in SecureValues.  You must set a value for SMTPServer, SMTPPort, SMTPUseSSL, SMTPUser and SMTPPassword: EXEC sqlver.udfSecureValue(''SMTPServer'', ''myserver.mydomain.com'') , etc.'
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END

  SET @From = COALESCE(@From, sqlver.udfSecureValue('SMTPDefaultFrom', NULL), 'sys@opsstream.com')

   --trim trailing commas
   SET @To = RTRIM(@To)
   WHILE LEFT(REVERSE(@To), 1) = ',' BEGIN
     SET @To = RTRIM(SUBSTRING(@To, 1, LEN(@To) - 1))
   END
   
   SET @CC = RTRIM(@CC)
   WHILE LEFT(REVERSE(@CC), 1) = ',' BEGIN
     SET @CC = RTRIM(SUBSTRING(@CC, 1, LEN(@CC) - 1))
   END
   
   SET @BCC = RTRIM(@BCC)
   WHILE LEFT(REVERSE(@BCC), 1) = ',' BEGIN
     SET @BCC = RTRIM(SUBSTRING(@BCC, 1, LEN(@BCC) - 1))
  END      

  DECLARE @LogMsg nvarchar(MAX)

  BEGIN TRY
    EXEC sqlver.sputilSendMail_CLR
      @From = @From,
      @FromFriendly = @FromFriendly,
      @To = @To,
      @Subject = @Subject,
      @CC = @CC,
      @BCC = @BCC,
      @TextBody = @TextBody,
      @HTMLBody = @HTMLBody,

      @ServerAddress = @SMTPServerAddress,
      @ServerPort = @SMTPServerPort,
      @EnableSSL = @SMTPUseSSL,
      @User = @SMTPUser,
      @Password = @SMTPPassword,

      @AttachFileName = @AttachFilename,
      @AttachData = @AttachData,
      @AttachFilename2 = @AttachFilename2,
      @AttachData2 = @AttachData2

    SET @LogMsg = 'sqlver.sputilSendMail: Sent to ' + ISNULL(@To, 'NULL')

  END TRY
  BEGIN CATCH
    SET @LogMsg = 'sqlver.sputilSendMail: Error' + ISNULL(':' + ERROR_MESSAGE(), '')
  END CATCH


  IF OBJECT_ID('opsstream.spinsSysLog') IS NOT NULL BEGIN
    BEGIN TRY
      DECLARE @SysEventTypeCode varchar(40)
      SET @SysEventTypeCode = 'emailSend'
    
      DECLARE @AdditionalInfo varchar(2048)
      SET @AdditionalInfo = LEFT(COALESCE(@TextBody, @HTMLBody), 254)

      DECLARE @SQL nvarchar(MAX)

      SET @SQL = 'EXEC opsstream.spinsSysLog
          @SysEventTypeCode=@SysEventTypeCode,
          @Description=@Description,
          @SystemLocator=@SystemLocator,
          @AdditionalInfo=@AdditionalInfo'

      EXEC sp_executesql
        @stmt = @SQL,
        @params = N'@SysEventTypeCode varchar(40), @Description varchar(255), @SystemLocator varchar(255), @AdditionalInfo varchar(2048)',
        @SysEventTypeCode=@SysEventTypeCode,
        @Description=@LogMsg,
        @SystemLocator=@SystemLocator,
        @AdditionalInfo=@AdditionalInfo
    END TRY
    BEGIN CATCH
      PRINT 'sqlver.sputilSendMail could not log event using opsstream.spinsSysEventLog: ' + ERROR_MESSAGE()
    END CATCH

  END

END

GO


IF OBJECT_ID('[sqlver].[sputilReadFromFile]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilReadFromFile]
END
GO

CREATE PROCEDURE sqlver.sputilReadFromFile @Filename nvarchar(512), @Buf varbinary(MAX) OUTPUT
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @SQL nvarchar(MAX)

  SET @SQL = 'SELECT @Buf = ef.BulkColumn FROM OPENROWSET (BULK ''' + @Filename + ''', SINGLE_BLOB) ef'
  EXEC sp_executesql @statement = @SQL, @params = N'@Filename nvarchar(512), @Buf varbinary(MAX) OUTPUT', @Filename = @Filename, @Buf = @Buf OUTPUT

END

GO


IF OBJECT_ID('[sqlver].[spgetSQLProgress]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLProgress]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLProgress]
@FilterForCommand sysname = NULL
--$!SQLVer Dec 18 2020 12:54AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SELECT
    er.session_id, er.command, er.percent_complete
  FROM
    sys.dm_exec_requests er
  WHERE
    (@FilterForCommand IS NULL AND NULLIF(er.percent_complete, 0) IS NOT NULL) OR
    (er.command LIKE @FilterForCommand + '%')
    --er.command like 'DBCC%'
END

GO


IF OBJECT_ID('[sqlver].[spgetDBsWithSQLVer]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetDBsWithSQLVer]
END
GO

CREATE PROCEDURE [sqlver].[spgetDBsWithSQLVer]
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spgetAllDBsBackupStatus]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetAllDBsBackupStatus]
END
GO

CREATE PROCEDURE [sqlver].[spgetAllDBsBackupStatus]
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spsysRewriteProcClearUniqueTemps]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysRewriteProcClearUniqueTemps]
END
GO

CREATE PROCEDURE [sqlver].[spsysRewriteProcClearUniqueTemps]
@Object nvarchar(MAX)
--$!SQLVer Sep  6 2022 10:34AM by sa

--©Copyright 2006-2022 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  DECLARE @ObjID int

  IF sqlver.udfIsInt(@Object) = 1 BEGIN
    SET @ObjID = CAST(@Object AS int)
  END
  ELSE IF LEN(@Object) < 254 BEGIN
    SET @ObjID = OBJECT_ID(@Object)
  END

  DECLARE @FQName nvarchar(512)
  SET @FQName = OBJECT_SCHEMA_NAME(@ObjID) + '.' + OBJECT_NAME(@ObjID)

  IF @ObjID IS NOT NULL BEGIN
    SET @Object = OBJECT_DEFINITION(@ObjID)
  END

  DECLARE @P int
  DECLARE @P2 int

  --Switch CREATE to ALTER to execute DDL
  SET @P = sqlver.udfFindInSQL('CREATE', @Object, 0)
  SET @P2 = sqlver.udfFindInSQL('ALTER', @Object, 0)
  IF @P > 0 AND (NULLIF(@P2, 0) IS NULL OR @P2 > @P) BEGIN
    SET @Object= LEFT(@Object, @P - 1) + 'ALTER' + RIGHT(@Object, LEN(@Object) - LEN('CREATE'))
  END

  SET @Object = sqlver.udfStripTempTablePrefixes(@Object)

  EXEC (@Object)

  DELETE
  FROM
    sqlver.tblTempTables
  WHERE
    FoundInProc_FQName = @FQName

END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_GetHTTP]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_GetHTTP]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_GetHTTP]
--$!SQLVer Mar  8 2025 10:03PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'

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
      @AllowOldTLS bit = 0,
        --If set to 1, Tls11 (768) is used. For older protocols (i.e. Tls or Ssl3)
        --you must use @UseProtocol explicitly in addition to setting @AllowOldTLS.
        --If @AllowOldTLS is not set, we SecurityProtocolType.Tls12 to force a modern TLS version
      @SSLProtocol nvarchar(512) = NULL,
        --Only applies if @AllowOldTLS = 1
        --Will use LS 1.1 if not set (if @AllowOldTLS is set)   
        --see https://docs.microsoft.com/en-us/dotnet/api/system.net.securityprotocoltype        
        --Ssl3
        --SystemDefault
        --Tls
        --Tls11
        --Tls12
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

CREATE PROCEDURE [sqlver].[sputilGetHTTP]
  @URL nvarchar(MAX),
    --URL to retrieve data from
  @HTTPMethod nvarchar(40) = ''GET'',
    --can be either GET or POST
  @ContentType nvarchar(254)= ''text/html'' OUTPUT,
    --set to ''application/x-www-form-urlencoded'' for POST, etc.  
    --If provided in the response headers, the will be set to the Content-Type value in the response
  --@Cookies nvarchar(MAX) OUTPUT,
    --string containing name=value,name=value list of cookies and values
  --@DataToSend nvarchar(MAX), 
    --data to post, if @HTTPMethod = ''POST''
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
  @ResponseBinary varbinary(MAX) OUTPUT,
    --Full binary data returned by remote HTTP server
        
  @AutoFollowRedir bit = 1,
    --If response indicates a redirect, re-initate an HTTP request to that @RedirURL
        
  @Filename nvarchar(MAX) = NULL OUTPUT,
    --If provided in the response headers, the filename from the Content-Disposition value
  @LastModified nvarchar(MAX) = NULL OUTPUT,
    --If provided in the response headers, the Last-Modified value
  @LastModifiedDate datetime = NULL OUTPUT,
    --If provided in the response headers, the Last-Modified value cast as a datetime.
    --Does not perform any timezone offset calculations (i.e. usually GMT)    

  @ReturnHeaders bit = 0,
    --If set, any response headers are returned in a resultset

  @URLRoot nvarchar(MAX) = NULL
    --absolute URL to prepend to @RedirURL if needed  
        
  --@ErrorMsg nvarchar(MAX) OUTPUT
    --NULL unless an error message was encountered
AS 
BEGIN
  SET NOCOUNT ON

  /*
  Simplified procedure to initiate an HTTP request.
      
  Does not support @Cookies, @DataToSend, @DataToSendBin, or @Headers
  If these are needed, call sqlver.sputilGetHTTP_CLR directly.
      
  (SQL does not allow us to assign default values to long paramaters such as varchar(MAX))
  */      

  DECLARE @Headers nvarchar(MAX)
  DECLARE @Header varchar(MAX)
  DECLARE @Cookies nvarchar(MAX)
  DECLARE @AllowOldTLS bit
 
  SET @AllowOldTLS = 0
        
  DECLARE @ErrorMessage nvarchar(MAX)
      
  DECLARE @tvPV TABLE(Id int, Value nvarchar(MAX))

    
       
  DECLARE @Done bit
  SET @Done = 0
        
  WHILE @Done = 0 BEGIN
      
    SET @RedirURL = NULL
    SET @Headers = NULL
    DELETE FROM @tvPV
                   
    EXEC sqlver.sputilGetHTTP_CLR
      @URL = @URL,
      @HTTPMethod = @HTTPMethod,
      @ContentType = @ContentType,
            
      @Cookies = @Cookies OUTPUT,
      @DataToSend = NULL,
      @DataToSendBin = NULL,
      @Headers = @Headers OUTPUT,
            
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

    IF @HTTPStatus = 500 BEGIN
      PRINT ''sqlver.sputilGetHTTP: HTTPStatus = 500''
      PRINT CAST(@ResponseBinary AS varchar(MAX))
    END  
        
    INSERT INTO @tvPV (Id, Value)
    SELECT
      [Index],
      Value
    FROM
      sqlver.udftGetParsedValues(@Headers, CHAR(10))
          
          
    SELECT 
      @Header = sqlver.udfRTRIMSuper(pv.Value)
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE ''Content-Disposition:%''
        
    SET @Filename = sqlver.udfParseValue(sqlver.udfParseValue(@Header, 2, '';''), 2, ''='')
        
        
    SELECT 
      @LastModified = REPLACE(sqlver.udfRTRIMSuper(pv.Value), ''Last-Modified:'', '''')
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE ''Last-Modified:%''

        
    SET @LastModified = LTRIM(sqlver.udfParseValue(@LastModified, 2, '',''))
    SET @LastModified = LEFT(@LastModified, LEN(@LastModified) - 4)
    SET @LastModifiedDate = CAST(@LastModified AS datetime)
        
        
    SELECT 
      @ContentType = REPLACE(sqlver.udfRTRIMSuper(pv.Value), ''Content-Type:'', '''')
    FROM
      @tvPV pv
    WHERE
      pv.Value LIKE ''Content-Type:%''                       
        
    IF @AutoFollowRedir = 0 OR @RedirURL IS NULL BEGIN
      SET @Done = 1
    END
    ELSE BEGIN
    
      IF @RedirURL LIKE ''/%'' BEGIN
        SET @URLRoot = REPLACE(@URL, ''//'', ''@@'')
        SET @URLRoot = LEFT(@URLRoot, CHARINDEX(''/'', @URLRoot) - 1)
        SET @URLRoot = REPLACE(@URLRoot, ''@@'', ''//'')
      END
      ELSE IF @RedirURL LIKE ''http%'' BEGIN
        SET @URLRoot = ''''
      END
      ELSE BEGIN
        SET @URLRoot = LEFT(@URL, LEN(@URL) - CHARINDEX(''/'', REVERSE(@URL) + 1))
      END
    
      SET @URL = ISNULL(@URLRoot, '''') +  @RedirURL  

    END           
          
  END         
      
  IF @ReturnHeaders = 1 BEGIN
    SELECT * FROM sqlver.udftGetParsedValues(@Headers, char(10))    
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

          // Replace + characters with spaces
          paramBuf = paramBuf.Replace("+", " ");

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

      SqlBoolean AllowOldTLS,
      SqlString SSLProtocol,

      out SqlInt32 HTTPStatus,
      out SqlString HTTPStatusText,
      out SqlString RedirURL,
      out SqlBinary ResponseBinary,
      out SqlString ErrorMsg

    )
    {
        if (AllowOldTLS.IsTrue) {           
          if (SSLProtocol.IsNull) {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11;
            SqlContext.Pipe.Send("AllowOldTLS is set but SSLProtocol is not provided.  Using Tls11");
          }
          else {
            SqlContext.Pipe.Send("AllowOldTLS is set and SSLProtocol requested " + Convert.ToString(SSLProtocol.Value));
            SecurityProtocolType thisProtocol;
            SecurityProtocolType.TryParse(Convert.ToString(SSLProtocol.Value), out thisProtocol);
            ServicePointManager.SecurityProtocol = thisProtocol;
            //SqlContext.Pipe.Send("Using protocol " + thisProtocol.ToString());            
          }      
        }
        else {
          // force TLS 1.2
          ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
          //SqlContext.Pipe.Send("Using protocol " + SecurityProtocolType.Tls12.ToString());      
        }

        // see:  https://docs.microsoft.com/en-us/dotnet/api/system.net.securityprotocoltype
        //  Ssl3          48
        //  SystemDefault 0
        //  Tls           192
        //  Tls11         768
        //  Tls12         3072
        
        string paramURL = Convert.ToString(URL);
        string paramHTTPMethod = Convert.ToString(HTTPMethod);
        string paramContentType = Convert.ToString(ContentType);
        string paramDataToSend = Convert.ToString(DataToSend);
        string paramHeaders = Convert.ToString(Headers);
        string paramUser = Convert.ToString(User);
        string paramPassword = Convert.ToString(Password);
        string paramUserAgent = Convert.ToString(UserAgent);

        string paramCookies = "";
        bool useCookies = false;

        if (!Cookies.IsNull)
        {
          useCookies = true;
          paramCookies = Convert.ToString(Cookies);
        }

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

            CookieContainer thisCookieContainer = new CookieContainer();
            if (useCookies)
            {
              //assign cookies that were passed in

              if (paramCookies.Length > 0) {
                  //assign cookies that were passed in
                  thisCookieContainer.SetCookies(new Uri(paramURL), paramCookies);
              }

              request.CookieContainer = thisCookieContainer;
            }

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
                if (paramUser.Contains(@"\")) {
                    string thisDomain = paramUser.Substring(0, paramUser.IndexOf(@"\"));
                    paramUser = paramUser.Substring(paramUser.IndexOf(@"\") + 1);
                    request.Credentials = new System.Net.NetworkCredential(paramUser, paramPassword, thisDomain);
                } else {
                    request.Credentials = new System.Net.NetworkCredential(paramUser, paramPassword);
                }

                //request.Credentials = new System.Net.NetworkCredential(paramUser, paramPassword);
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
                //SqlContext.Pipe.Send("***" + Convert.ToString(entry.Key.ToLower()) + " " + Convert.ToString(entry.Value));
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


            if ((paramHTTPMethod.ToUpper() == "POST" || paramHTTPMethod.ToUpper() == "PUT") && (!DataToSend.IsNull || !DataToSendBin.IsNull))
            {
                paramErrorMsg = "DEBUG1";
                //convert string paramDataToSend to byte array
                byte[] binSendData;

                if (!DataToSendBin.IsNull)        
                {
                    binSendData = DataToSendBin.Buffer;
                }
                else {
                    binSendData = System.Text.Encoding.Default.GetBytes(paramDataToSend);
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

            if (useCookies)
            {
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
            }


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

            paramErrorMsg = null;

        }

        catch (WebException ex)
        {
            SqlContext.Pipe.Send(ex.Message.ToString());

            try {
              response = (HttpWebResponse)ex.Response;
              responseStatusCode = Convert.ToInt32(response.StatusCode);
              responseStatusDescription = response.StatusDescription;

              //get error response data
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
            }
            catch (Exception ex2) {
              SqlContext.Pipe.Send(ex2.Message.ToString() + " (while trying to read HTTP error response)");
            }

            try {
              response.Close();
              responseStream.Dispose();
            }
            catch (Exception ex3) {
              SqlContext.Pipe.Send(ex3.Message.ToString() + " (while trying to close response and destroy responseStream)");
            }

            //string strData = System.Text.Encoding.Default.GetString(binData);                                    

            paramErrorMsg = ex.Message.ToString();

        }


        catch (NotSupportedException ex)
        {
            paramErrorMsg += "The request cache validator indicated that the response for this request can be served from the cache; however, this request includes data to be sent to the server. Requests that send data must not use the cache. This exception can occur if you are using a custom cache validator that is incorrectly implemented.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (ProtocolViolationException ex)
        {
            paramErrorMsg += "Method is GET or HEAD, and either ContentLength is greater or equal to zero or SendChunked is true. -or- KeepAlive is true, AllowWriteStreamBuffering is false, ContentLength is -1, SendChunked is false, and Method is POST or PUT.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (InvalidOperationException ex)
        {
            paramErrorMsg += "The stream is already in use by a previous call to BeginGetResponse. -or- TransferEncoding is set to a value and SendChunked is false.";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        catch (UriFormatException ex)
        {
            paramErrorMsg += "Invalid URI: The Uri string is too long. (" + paramURL + ")(" + paramErrorMsg + ")";
            SqlContext.Pipe.Send(ex.Message.ToString());
        }


        //Assign values to output parameters

/*
        if (paramErrorMsg.StartsWith("DEBUG"))
        {
            ErrorMsg = SqlString.Null;
        }
        else
        {
*/
            ErrorMsg = paramErrorMsg;
//        }

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

        if (useCookies)
        {
          Cookies = paramCookies;
        }
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


IF OBJECT_ID('[sqlver].[spsysBuildCLRAssembly]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLRAssembly]
END
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
--$!SQLVer Nov  7 2020  5:09AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  DECLARE @Debug bit
  --Set @Debug = 1 to enable verbose PRINT output
  SET @Debug = 0

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
  --SET @PathToSN = '"C:\SQLVer\MSTools\sn.exe"'
  --SET @PathToSN = '"C:\SQLVer\Tools\netfx\x64\sn.exe"'
  SET @PathToSN = '"C:\SQLVer\Tools\netfx\sn.exe"'  
 
  DECLARE @DBName sysname
  SET @DBName = DB_NAME()
  
  
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)  


  DECLARE @SQL varchar(MAX)

 
  --Enable CLR supoprt
  IF @Debug = 1 PRINT '***Enabling CLR support'  
  DECLARE @OrigSupport_CLR bit
  SELECT @OrigSupport_CLR = CONVERT(bit, value_in_use) FROM sys.configurations WHERE name = 'clr enabled'
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
  SELECT @OrigSupport_COM = CONVERT(bit, value_in_use) FROM sys.configurations WHERE name = 'Ole Automation Procedures'
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
      @FileName = @FileName,
      @ErrorMsg = NULL


    --Temporarily enable xp_cmdshell support so we can build the source code
    DECLARE @OrigSupport_XPCmdShell bit
    SELECT @OrigSupport_XPCmdShell = CONVERT(bit, value_in_use) FROM sys.configurations WHERE name = 'xp_cmdshell'
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
    'if exist ' + @ThisPath + REPLACE(@FileName, '.cs', '.dll') + ' del /f /q ' + @ThisPath + REPLACE(@FileName, '.cs', '.dll') + @CRLF +
    'if exist ' + @ThisPath + REPLACE(@FileName, '.cs', '.snk') + ' del /f /q ' + @ThisPath + REPLACE(@FileName, '.cs', '.snk') + @CRLF +
    @PathToSN + ' -k ' + @FilePath +  REPLACE(@FileName, '.cs', '.snk') + @CRLF +
     '"C:\SQLVer\Tools\csc\csc" /t:library' +  
     @References + 
     ' /out:' + @FilePath + REPLACE(@FileName, '.cs', '.dll') + 
     ' /keyfile:' + @FilePath +  REPLACE(@FileName, '.cs', '.snk') + 
     ' ' + @FilePath + @FileName   

    IF @Debug = 1 PRINT 'EXEC sqlver.sputilWriteStringToFile... (' + @FilePath + 'tmp.bat)'
    EXEC sqlver.sputilWriteStringToFile 
      @FileData = @Command,
      @FilePath = @FilePath,
      @FileName = 'tmp.bat',
      @ErrorMsg = NULL
    
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

  DECLARE @ThisWrapperSQL nvarchar(MAX)

  IF @CreateWrapperSQL IS NOT NULL BEGIN
    IF @Debug = 1 BEGIN
      PRINT '***Creating wrapper SQL objects (functions, stored procs, etc.)'
      PRINT @CreateWrapperSQL
    END
      
    DECLARE curCreateWrap CURSOR LOCAL STATIC FOR
    SELECT sqlver.udfLTRIMSuper(sqlver.udfRTRIMSuper(pv.Value)) AS CreateWrap
    FROM sqlver.udftGetParsedValues(@CreateWrapperSQL, '~') pv
      
    OPEN curCreateWrap
    FETCH curCreateWrap INTO @ThisWrapperSQL
    WHILE @@FETCH_STATUS = 0 BEGIN
      EXEC(@ThisWrapperSQL)
      FETCH curCreateWrap INTO @ThisWrapperSQL
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


IF OBJECT_ID('[sqlver].[spsysBuildCLR_SendMail]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_SendMail]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_SendMail]
--------------------------------------------------------------------------------------------
/*
Procedure to demonstrate use of sqlver.spsysBuildCLRAssembly to build and register a CLR
assembly in SQL without the use of Visual Studio.

This procedure allows you to send SMTP mail.

By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
--$!SQLVer Aug 28 2023 11:45AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System, 'C:\Windows\Microsoft.NET\Framework64\v2.0.50727\System.dll')
  --INSERT INTO #References (AssemblyName, FQFileName) VALUES ('itextsharp', @FilePath + 'itextsharp.dll')  

  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '
    IF OBJECT_ID(''sqlver.udfGetMIMEType_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfGetMIMEType_CLR;
    END    
    
    IF OBJECT_ID(''sqlver.udfBase64Encode_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfBase64Encode_CLR;
    END
    
    IF OBJECT_ID(''sqlver.sputilSendMail_CLR'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilSendMail_CLR;
    END

    IF OBJECT_ID(''sqlver.sputilSendMail'') IS NOT NULL BEGIN
      DROP PROCEDURE sqlver.sputilSendMail;
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
      @AttachData varbinary(MAX), --Binary data to include as an attachment

      @AttachFilename2 nvarchar(4000) = NULL, --If @AttachData2 is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
      @AttachData2 varbinary(MAX) --Binary data to include as an attachment                    
    AS
      --NOTE: We would like to have some of these parameters such as @AttachData default to NULL,
      --but then we cannot use varchar(MAX) or varbinary(MAX).  It is for this reason that we  also
      --are using nvarchar(4000) on some parameters:  these can be changed to nvarchar(MAX) to support
      --longer values, but then we cannot use default values.
      EXTERNAL NAME [SendMail_SQLCLR].[Procedures].[SendMail]  
~
CREATE PROCEDURE [sqlver].[sputilSendMail]
@From nvarchar(4000) = NULL, 
@FromFriendly nvarchar(4000) = NULL,      
@To nvarchar(MAX),  --note:  you can specify friendly name like this ''Steve Friday <something@changeme.com> and can use a comma or semicolon separated list''
@Subject nvarchar(MAX), 
@CC nvarchar(4000) = NULL,
@BCC nvarchar(4000) = NULL,

@TextBody nvarchar(MAX), --use this if sending text
@HTMLBody nvarchar(MAX),  --use this if sending HTML

@AttachFilename nvarchar(4000) = NULL, --If @AttachData is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
@AttachData varbinary(MAX), --Binary data to include as an attachment,
@AttachFilename2 nvarchar(4000) = NULL, --If @AttachData2 is provided, this is used only to set the descriptive name on the attachment.  Else it is used to load the attachment.
@AttachData2 varbinary(MAX), --Binary data to include as an attachment
@SystemLocator varchar(255) = NULL --optional, to append this to the SysLog entry
--$!SQLVer Sep 13 2022 11:58AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @Msg nvarchar(MAX)
  
  DECLARE @ThreadGUID uniqueidentifier
  SET @ThreadGUID = NEWID()

  SET @Msg = ''sqlver.sputilSendMail: Sending to '' + CONCAT_WS('', '', @To, @CC, @BCC) + '' Subject: '' + ISNULL(@Subject, ''NULL'')
  EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID

  DECLARE @SMTPServerAddress varchar(1000) = sqlver.udfSecureValue(''SMTPServer'', NULL)
  DECLARE @SMTPServerPort varchar(10) = sqlver.udfSecureValue(''SMTPPort'', NULL)
  DECLARE @SMTPUseSSL bit = sqlver.udfSecureValue(''SMTPUseSSL'', NULL)
  DECLARE @SMTPUser varchar(255) = sqlver.udfSecureValue(''SMTPUser'', NULL)
  DECLARE @SMTPPassword varchar(255) = sqlver.udfSecureValue(''SMTPPassword'', NULL)


  IF NULLIF(RTRIM(@SMTPServerAddress), '''') IS NULL BEGIN
    SET @Msg = ''Error in sqlver.sputilSendMail:  SMTP settings not present in SecureValues.  You must set a value for SMTPServer, SMTPPort, SMTPUseSSL, SMTPUser and SMTPPassword: EXEC sqlver.udfSecureValue(''''SMTPServer'''', ''''myserver.mydomain.com'''') , etc.''
    EXEC sqlver.spinsSysRTLog @Msg = @Msg, @ThreadGUID = @ThreadGUID, @PersistAfterRollback = 1
    RAISERROR(@Msg, 16, 1)
    RETURN 1001
  END

  SET @From = COALESCE(@From, sqlver.udfSecureValue(''SMTPDefaultFrom'', NULL), ''sys@opsstream.com'')

   --trim trailing commas
   SET @To = RTRIM(@To)
   WHILE LEFT(REVERSE(@To), 1) = '','' BEGIN
     SET @To = RTRIM(SUBSTRING(@To, 1, LEN(@To) - 1))
   END
   
   SET @CC = RTRIM(@CC)
   WHILE LEFT(REVERSE(@CC), 1) = '','' BEGIN
     SET @CC = RTRIM(SUBSTRING(@CC, 1, LEN(@CC) - 1))
   END
   
   SET @BCC = RTRIM(@BCC)
   WHILE LEFT(REVERSE(@BCC), 1) = '','' BEGIN
     SET @BCC = RTRIM(SUBSTRING(@BCC, 1, LEN(@BCC) - 1))
  END      

  DECLARE @LogMsg nvarchar(MAX)

  BEGIN TRY
    EXEC sqlver.sputilSendMail_CLR
      @From = @From,
      @FromFriendly = @FromFriendly,
      @To = @To,
      @Subject = @Subject,
      @CC = @CC,
      @BCC = @BCC,
      @TextBody = @TextBody,
      @HTMLBody = @HTMLBody,

      @ServerAddress = @SMTPServerAddress,
      @ServerPort = @SMTPServerPort,
      @EnableSSL = @SMTPUseSSL,
      @User = @SMTPUser,
      @Password = @SMTPPassword,

      @AttachFileName = @AttachFilename,
      @AttachData = @AttachData,
      @AttachFilename2 = @AttachFilename2,
      @AttachData2 = @AttachData2

    SET @LogMsg = ''sqlver.sputilSendMail: Sent to '' + ISNULL(@To, ''NULL'')

  END TRY
  BEGIN CATCH
    SET @LogMsg = ''sqlver.sputilSendMail: Error'' + ISNULL('':'' + ERROR_MESSAGE(), '''')
  END CATCH


  IF OBJECT_ID(''opsstream.spinsSysLog'') IS NOT NULL BEGIN
    BEGIN TRY
      DECLARE @SysEventTypeCode varchar(40)
      SET @SysEventTypeCode = ''emailSend''
    
      DECLARE @AdditionalInfo varchar(2048)
      SET @AdditionalInfo = LEFT(COALESCE(@TextBody, @HTMLBody), 254)

      DECLARE @SQL nvarchar(MAX)

      SET @SQL = ''EXEC opsstream.spinsSysLog
          @SysEventTypeCode=@SysEventTypeCode,
          @Description=@Description,
          @SystemLocator=@SystemLocator,
          @AdditionalInfo=@AdditionalInfo''

      EXEC sp_executesql
        @stmt = @SQL,
        @params = N''@SysEventTypeCode varchar(40), @Description varchar(255), @SystemLocator varchar(255), @AdditionalInfo varchar(2048)'',
        @SysEventTypeCode=@SysEventTypeCode,
        @Description=@LogMsg,
        @SystemLocator=@SystemLocator,
        @AdditionalInfo=@AdditionalInfo
    END TRY
    BEGIN CATCH
      PRINT ''sqlver.sputilSendMail could not log event using opsstream.spinsSysEventLog: '' + ERROR_MESSAGE()
    END CATCH

  END

END
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
            { ".webmanifest","application/manifest+json" },
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

        if (dictMime.ContainsKey(thisExtension))
        {
            result = dictMime[thisExtension];
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
        SqlBytes AttachData,

        SqlString AttachFilename2,
        SqlBytes AttachData2
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

        string paramAttachFilename2 = AttachFilename2.IsNull ? null : Convert.ToString(AttachFilename2);
        //we don''t need to convert AttachData2;


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
                    //Note:  AttachmentFileName specifies file to read from
                    Attachment thisAttach = new Attachment(attachDataStream, paramAttachFilename, Convert.ToString(Functions.GetMIMETypeFromFilename(paramAttachFilename)));
                    thisMailMessage.Attachments.Add(thisAttach);
                }
            }


            if (!AttachData2.IsNull && AttachData2 != null)
            {
                MemoryStream attachDataStream2 = new MemoryStream(AttachData2.Buffer);
                Attachment thisAttach2 = null;
                
                if (paramAttachFilename2 != null && paramAttachFilename2.Trim() != "")
                {
                  //note:  AttachmentFileName2 is used as name for the binary data passed in                
                  thisAttach2 = new Attachment(attachDataStream2, paramAttachFilename2, Convert.ToString(Functions.GetMIMETypeFromFilename(paramAttachFilename2)));
                }
                else
                {
                  thisAttach2 = new Attachment(attachDataStream2, paramAttachFilename2);                              
                }
                thisMailMessage.Attachments.Add(thisAttach2);
            }
            else
            {
                //no AttachData2 provided
                if (paramAttachFilename2 != null && paramAttachFilename2.Trim() != "")
                {
                    FileStream attachDataStream2 = new FileStream(paramAttachFilename2, FileMode.Open);
                    //Note:  AttachmentFileName2 specifies file to read from
                    Attachment thisAttach2 = new Attachment(attachDataStream2, paramAttachFilename2, Convert.ToString(Functions.GetMIMETypeFromFilename(paramAttachFilename2)));
                    thisMailMessage.Attachments.Add(thisAttach2);
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


IF OBJECT_ID('[sqlver].[sputilExecInOtherConnection]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilExecInOtherConnection]
END
GO

CREATE PROCEDURE [sqlver].[sputilExecInOtherConnection]
@SQLCommand nvarchar(MAX),
@Server sysname = NULL, --'localhost,1433'
@Provider sysname = 'SQLNCLI11',
@Database sysname = NULL,
@Username sysname = 'sqlverLogger',
@Password sysname = 'sqlverLoggerPW'
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  /*
  This procedure is designed to allow a caller to provide a message that will be written to an error log table,
  and allow the caller to call it within a transaction.  The provided message will be persisted to the
  error log table even if the transaction is rolled back.
  
  To accomplish this, this procedure utilizes ADO to establish a second database connection (outside
  the transaction context) back into the database to execute the SQL in @SQL.
  */

  IF @Server IS NULL BEGIN
	  SET @Server = CONVERT(sysname, SERVERPROPERTY('servername'))
  END
  IF @Database IS NULL BEGIN
	  SET @Database = DB_NAME()
  END

  DECLARE @ConnStr varchar(MAX)
    --connection string for ADO to use to access the database
  SET @ConnStr = 'Provider=' + @Provider + '; Server=' + @Server + '; Database=' + @Database + '; Uid=' + @Username + '; Pwd=' + @Password + ';'
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


IF OBJECT_ID('[sqlver].[sputilWordTablePDF]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilWordTablePDF]
END
GO

CREATE PROCEDURE [sqlver].[sputilWordTablePDF]
@DocTemplate varbinary(MAX), 
  @FieldsXML xml,
  @MergedPDFDoc varbinary(MAX) OUTPUT,
  @ErrorMessage nvarchar(MAX) = NULL OUTPUT,
@HTTPStatus int = NULL OUTPUT
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
   
  DECLARE @URL varchar(1024)
  SET @URL = 'http://localhost:24800/DoCLR/' 

  --Note:  handy for testing posts:
  --SET @URL = 'http://posttestserver.com/post.php'


  DECLARE @MethodToCall varchar(MAX)
  SET @MethodToCall = 'WordTablePDF'
  
  --Filename that is echoed back in the HTTP response when the PDF
  --document is returned
  DECLARE @Filename varchar(254)
  SET @Filename = 'WordTableTemplate.docx'
  --------------------------------------
       
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)


  DECLARE @MultipartBoundary varchar(100)
  SET @MultipartBoundary = LOWER(LEFT(REPLACE(CAST(NEWID() AS varchar(100)), '-', ''), 16))
  --Alternate ways of generating a boundary value:
    --SET @MultipartBoundary = CAST(DATEDIFF(s, '19700101', GETDATE()) AS varchar(100))
    --SET @MultipartBoundary = sqlver.udfRandomString(16)
    
  SET @MultipartBoundary = sqlver.udfLPad(@MultipartBoundary, '-', 40)

  DECLARE @Headers varchar(MAX)
  DECLARE @ContentType varchar(254)
  DECLARE @DataToSendBin varbinary(MAX)
  DECLARE @DataToSend varchar(MAX)

  DECLARE @RedirURL varchar(1024) 

  --Set @ContentType.  This is passed into sqlver.spsysBuildCLR_GetHTTP
  --(i.e. does not need to be added to a header or concatenated into the data)
  SET @ContentType = 'multipart/form-data; boundary=' + @MultipartBoundary

  SET @Headers = 
    'Content-Length: {{$LENGTH}}' + @CRLF

  --Field "methodToCall"                    
  SET @DataToSend =
    '--' + @MultipartBoundary + @CRLF +  --Boundary + CRLF
    'Content-Disposition: form-data; name="methodToCall"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF + --Extra CRLF is REQUIRED!!!
    @MethodToCall +
    @CRLF + --Closing CRLF is REQUIRED!!

    --Field "fieldsXML" 
    '--' + @MultipartBoundary + @CRLF +  --Boundary + CRLF
    'Content-Disposition: form-data; name="fieldsXML"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF + --Extra CRLF is REQUIRED!!!
    CAST(@FieldsXML AS varchar(MAX)) +
/*
    @CRLF + --Closing CRLF is REQUIRED!!


    --Field "templateDOCX" to hold the binary template file
    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="templateWordDoc"; filename="' +  @Filename + '"' + @CRLF +
    'Content-Type: application/octet-stream' + @CRLF +
    'Content-Transfer-Encoding: binary' + @CRLF +
*/
     @CRLF --Extra CRLF is REQUIRED!!!

  --Add binary data payload
  SET @DataToSendBin = CAST(@DataToSend AS varbinary(MAX)) + 
    @DocTemplate

  --Final footer
  SET @DataToSendBin = @DataToSendBin +
    CAST(@CRLF +'--' + @MultipartBoundary + '--' + @CRLF AS varbinary(MAX)) --Extra CRLF is REQUIRED!!!

  DECLARE @DataLen int
  SET @DataLen = DATALENGTH(@DataToSendBin)

  SET @DataToSend = NULL
  
  SET @Headers = REPLACE(@Headers, '{{$LENGTH}}', CAST(ISNULL(@DataLen, 0) AS varchar(100)))
  
  SET @HTTPStatus = NULL
  SET @RedirURL = NULL 
  SET @ErrorMessage = NULL

  --Initiate HTTP POST
  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @HTTPMethod = 'POST',  
    @ContentType = @ContentType,
    @Cookies = NULL,
    @DataToSend = @DataToSend,
    @DataToSendBin = @DataToSendBin,
    @Headers = @Headers,
    @UserAgent = 'SQLVerCLR',
    @HTTPStatus = @HTTPStatus OUTPUT,
    @RedirURL = @RedirURL OUTPUT,  
    @ResponseBinary = @MergedPDFDoc OUTPUT,
    @ErrorMsg = @ErrorMessage OUTPUT

   
END

GO


IF OBJECT_ID('[sqlver].[spsysBuildCLR_FTP]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_FTP]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_FTP]
@FilePath varchar(1024) = 'C:\SQLVer\AssemblyLibrary\',
@FileName varchar(1024) = 'FTP_SQLCLR.dll',
@BuildFromSource bit = 1
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  DECLARE @AssemblyName sysname
  SET @AssemblyName = 'FTP_SQLCLR'
  
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
    EXTERNAL NAME [FTP_SQLCLR].[Functions].[ftpUpload]    

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
    EXTERNAL NAME [FTP_SQLCLR].[Functions].[ftpDownload]        
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
  [assembly: AssemblyTitle("FTP_SQLCLR")]
  [assembly: AssemblyDescription("Allow FTP upload and download via SQL CLR Functions.  Generated automatically by sqlver.spsysBuildCLR_FTP")]
  [assembly: AssemblyConfiguration("")]
  [assembly: AssemblyCompany("David Rueter")]
  [assembly: AssemblyProduct("FTP_SQLCLR")]
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


IF OBJECT_ID('[sqlver].[spsysBuildCLR_WordMerge]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysBuildCLR_WordMerge]
END
GO

CREATE PROCEDURE [sqlver].[spsysBuildCLR_WordMerge]
--------------------------------------------------------------------------------------------
/*
Procedure to demonstrate use of sqlver.spsysBuildCLRAssembly to build and register a CLR
assembly in SQL without the use of Visual Studio.

This is just a sample:  you can use this as a template to create your own procedures
to register your own CLR assemblies.

By David Rueter (drueter@assyst.com), 5/1/2013
*/
---------------------------------------------------------------------------------------------
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
  
  PRINT '***CAN NO LONGER USE WordMerge_SQLCLR in SQLCLR***'
  PRINT 'This has been deprecated, due to incompatibility of '
  PRINT 'the .NET 4.0 version of System.Image.dll which now'
  PRINT 'contains native code, and hence cannot be loaded into'
  PRINT 'SQLCLR.'
  PRINT ''
  PRINT 'Consider uisng the SQLVerCLR web server to host this'
  PRINT 'assembly''s functionality.'
  RAISERROR('Assembly WordMerge_SQLCLR is not supported and cannot proceed.', 16, 1)
  RETURN 1002


  DECLARE @FilePath varchar(1024)
  SET @FilePath = 'C:\SQLVer\AssemblyLibrary\'

  CREATE TABLE #References (RefSequence int IDENTITY PRIMARY KEY, AssemblyName sysname, FQFileName varchar(1024), AddToCompilerRefs bit, IdentifierRoot varchar(128))

/*
  Note:  In general we do NOT want to use ALTER DATABASE xxx SET TRUSTWORTHY ON
  Instead, we prefer to create asymmetric keys for UNSAFE assemblies.  You will
  need to GRANT UNSAFE ASSEMBLY TO {the user that owns the database}
  
  However there is a strange problem with System.Windows.Forms that results in an
  error while trying to CREATE ASYMMETRIC KEY, probably related to:
  https://msdn.microsoft.com/en-us/library/system.windows.forms(v=vs.90).aspx  
  
  Consequently, you will likely have to ALTER DATABASE xxx SET TRUSTWORTHY ON
  prior to running this procedure (and in order for the resulting assembly to run).
      
*/

  --DECLARE @SQL varchar(MAX)
  --SET @SQL = 'ALTER DATABASE ' + DB_NAME() + ' SET TRUSTWORTHY ON'
  --EXEC(@SQL)

/*
Required dependencies.  However, if copies of this DLL all exist in @FilePath + '\AssemblyLibrary', SQL will load them automatically.

  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\Windows\assembly\GAC_MSIL\System.Drawing\2.0.0.0__b03f5f7f11d50a3a\System.Drawing.dll')
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('Accessibility', 'C:\Windows\assembly\GAC_MSIL\Accessibility\2.0.0.0__b03f5f7f11d50a3a\Accessibility.dll')
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('WindowsBase', 'C:\Windows\assembly\GAC_MSIL\WindowsBase\3.0.0.0__31bf3856ad364e35\WindowsBase.dll')
  INSERT INTO #References (AssemblyName, FQFileName, IdentifierRoot) VALUES ('System.Runtime.Serialization.Formatters.Soap', 'C:\Windows\assembly\GAC_MSIL\System.Runtime.Serialization.Formatters.Soap\2.0.0.0__b03f5f7f11d50a3a\System.Runtime.Serialization.Formatters.Soap.dll', 'C:\Windows\assembly\GAC_MSIL\System.Runtime.Serialization.Formatters.Soap\2.0.0.0__b03f5f7f11d50a3a\dll')
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Windows.Forms', 'C:\Windows\assembly\GAC_MSIL\System.Windows.Forms\2.0.0.0__b77a5c561934e089\System.Windows.Forms.dll')   
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationCFFRasterizer', 'C:\Windows\assembly\GAC_MSIL\PresentationCFFRasterizer\3.0.0.0__31bf3856ad364e35\PresentationCFFRasterizer.dll')      
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationCore', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\PresentationCore.dll')    
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationFramework', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\PresentationFramework.dll') 
  --Note:  must have a copy of PresentationUI.dll in the same directory due to circular references.  See: http://www.adamtuliper.com/2009/12/adding-presentationhost-and.html
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationUI', 'C:\Windows\assembly\GAC_MSIL\PresentationFramework\3.0.0.0__31bf3856ad364e35\PresentationUI.dll') 
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('ReachFramework',  'C:\Windows\assembly\GAC_MSIL\ReachFramework\3.0.0.0__31bf3856ad364e35\ReachFramework.dll')   
*/
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\SQLVer\Temp\AssemblyCache\System.Drawing.dll')  
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('ReachFramework',  'C:\SQLVer\Temp\AssemblyCache\ReachFramework.dll')   
  INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationCore', 'C:\SQLVer\Temp\AssemblyCache\PresentationCore.dll')    

  
/*
Alternate paths for the above DLLs      

  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Drawing', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Drawing.dll') 
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('Accessibility', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\Accessibility.dll')
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('WindowsBase', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\WindowsBase.dll')   
  ----INSERT INTO #References (AssemblyName, FQFileName, IdentifierRoot) VALUES ('System.Runtime.Serialization.Formatters.Soap', 'C:\Windows\assembly\GAC_MSIL\System.Runtime.Serialization.Formatters.Soap\2.0.0.0__b03f5f7f11d50a3a\System.Runtime.Serialization.Formatters.Soap.dll', 'C:\Windows\assembly\GAC_MSIL\System.Runtime.Serialization.Formatters.Soap\2.0.0.0__b03f5f7f11d50a3a\dll')
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('System.Windows.Forms', 'C:\WINDOWS\Microsoft.NET\Framework\v2.0.50727\System.Windows.Forms.dll')   
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationCFFRasterizer', 'C:\Windows\Microsoft.NET\Framework\v3.0\WPF\PresentationCFFRasterizer.dll')    
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationFramework', 'C:\Windows\assembly\GAC_MSIL\PresentationFramework\3.0.0.0__31bf3856ad364e35\PresentationFramework.dll')
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationFramework', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\PresentationFramework.dll')
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationUI', 'C:\Windows\Microsoft.NET\Framework\v3.0\WPF\PresentationUI.dll')   
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('PresentationUI', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\PresentationUI.dll')     
  ----INSERT INTO #References (AssemblyName, FQFileName) VALUES ('ReachFramework', 'C:\Program Files\Reference Assemblies\Microsoft\Framework\v3.0\reachframework.dll') 
*/  
  
  INSERT INTO #References (AssemblyName, FQFileName, AddToCompilerRefs) VALUES ('GemBox.Document', @FilePath + 'GemBox.Document.dll', 1)  
--  CREATE ASSEMBLY [GemBox.Document] FROM 'C:\SQLVer\Temp\AssemblyLibrary\GemBox.Document.dll'
           
  DECLARE @DropWrapperSQL varchar(MAX)
  SET @DropWrapperSQL = '  
    IF OBJECT_ID(''sqlver.udfMergeWordToPDF_CLR'') IS NOT NULL BEGIN
      DROP FUNCTION sqlver.udfMergeWordToPDF_CLR;
    END
  '

  DECLARE @CreateWrapperSQL varchar(MAX)
  SET @CreateWrapperSQL = '
    CREATE FUNCTION [sqlver].[udfMergeWordToPDF_CLR](
    @TemplateDOCX varbinary(MAX),
    @FieldsXML xml
    )
    RETURNS [varbinary](max) WITH EXECUTE AS CALLER
    AS 
    EXTERNAL NAME [WordMerge_SQLCLR].[Functions].[MergeWordToPDF]
  '  

      
  --C# code.
  --Paste CLR source in below.  Replace all occurrences a single quote with two single quotes.  
  DECLARE @SourceCode nvarchar(MAX)
  SET @SourceCode = '
//------start of CLR Source------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using GemBox.Document;
//using GemBox.Document.Tables;

using System.Text.RegularExpressions;
using System.IO;

using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

using System.Xml;
using System.Xml.Linq;
using System.Security;

//from AssemblyInfo.cs
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Data.Sql;



// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("WordMerge_SQLCLR")]
[assembly: AssemblyDescription("Perform mail merge on an MS Word template in a SQL CLR Function.  Generated automatically by sqlver.spsysRebuildCLR_GemWordCLR")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("OpsStream")]
[assembly: AssemblyProduct("WordMerge_SQLCLR")]
[assembly: AssemblyCopyright("Copyright ©  2015")]
[assembly: AssemblyTrademark("OpsStream")]
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
    public static SqlBytes MergeWordToPDF(
        SqlBytes templateDOCX,
        SqlXml fieldsXML
        )
    {
        // Put your code here
        ///////////////////////////////////////////////
        if (templateDOCX.IsNull)
        {
            throw new Exception("Error in CLR function MergeToPDF: Parameter templateDOCX must contain a valid MS Word DOCX document.");
        }

        if (fieldsXML.IsNull)
        {
            throw new Exception("Error in CLR function RenderPDF: Parameter fieldsXML must contain a valid XML document.");
        }


        // If using Professional version, put your serial key below.
        //ComponentInfo.SetLicense("FREE-LIMITED-KEY");
        ComponentInfo.SetLicense("DTFX-JTAH-6R4P-ZAGV");
        ComponentInfo.FreeLimitReached += (this_sender, this_exception) => this_exception.FreeLimitReachedAction = FreeLimitReachedAction.ContinueAsTrial;


        using (MemoryStream outputPDFStream = new MemoryStream())
        {

            //Load the DOCX template template
            MemoryStream thisTemplateDOCX = new MemoryStream(templateDOCX.Buffer);

            var doc = DocumentModel.Load(thisTemplateDOCX, GemBox.Document.LoadOptions.DocxDefault);
            

            //see:  http://www.gemboxsoftware.com/Document/help/html/Mail_Merge.htm#DataSources

            XmlReader mergedataXMLReader = fieldsXML.CreateReader();

            // Initialize mail merge data source.
            var myData = new Dictionary<string, object>();

            mergedataXMLReader.MoveToContent();

            while (mergedataXMLReader.ReadToFollowing("row"))
            {
                //create a new single-page PDF document by overlaying text over template    

                XmlReader fieldsXMLReader = mergedataXMLReader.ReadSubtree();

                while (fieldsXMLReader.ReadToFollowing("field"))
                {
                    string thisValue;
                    string thisName;

                    XmlReader thisSubtreeXMLReader = fieldsXMLReader.ReadSubtree();
                    thisSubtreeXMLReader.MoveToContent();
                    //Note:  We should always be on an element, however if we do not call
                    //.MoveToContent the XNode.ReadFrom throws an error below:
                    //"The XmlReader state should be Interactive."  Calling
                    //.MoveToContent avoids this error.

                    XElement thisFieldNode = (XElement)XNode.ReadFrom(thisSubtreeXMLReader);

                    //Note:  XNode.ReadFrom advances the reader, which is a pain when we
                    //are trying to loop through XML reader (such as our loop
                    //      while (fieldsXMLReader.ReadToFollowing("Field"))
                    //above.  This leads to only every-other Field element being processed.
                    //Consequently, we use the thisSubtreeXMLReader, so that XNode.ReadFrom
                    //does not mess up our position in the fieldsXMLReader.


                    thisName = thisFieldNode.Attribute("name").Value;
                    thisValue = thisFieldNode.Value;

                    myData.Add(thisName, thisValue);

                    thisSubtreeXMLReader.Close();
                }

                fieldsXMLReader.Close();
            }
            mergedataXMLReader.Close();


            // Execute mail merge.
            doc.MailMerge.Execute(myData);


            doc.Save(outputPDFStream, GemBox.Document.SaveOptions.PdfDefault);
            return (new SqlBytes(outputPDFStream.ToArray()));

        }
        ///////////////////////////////////////////////

    }
};
//------end of CLR Source------
'

    

  EXEC sqlver.spsysBuildCLRAssembly
    @AssemblyName = 'WordMerge_SQLCLR',
    @FileName = 'WordMerge_SQLCLR.cs',
    @FilePath = @FilePath, 
    @DropWrapperSQL = @DropWrapperSQL,
    @CreateWrapperSQL = @CreateWrapperSQL,
    @SourceCode = @SourceCode

/*
  --If you do NOT have copies of all the dependency DLLs in @FilePath, you must use this very hokey cleanup:
  --The PresentationFramework assembly we created above has the wrong signature.  Alter it to an assembly with the right signature.
  --Extensive trial-and-error testing on 12/30/2015 did not reveal a better way of handling this problem.
  
  ALTER ASSEMBLY PresentationFramework FROM 'C:\Windows\assembly\GAC_MSIL\PresentationFramework\3.0.0.0__31bf3856ad364e35\PresentationFramework.dll'
*/ 

  --SET @SQL = 'ALTER DATABASE ' + DB_NAME() + ' SET TRUSTWORTHY OFF'
  --EXEC(@SQL)
 
END

GO


IF OBJECT_ID('[sqlver].[sputilWordMergePDF]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilWordMergePDF]
END
GO

CREATE PROCEDURE [sqlver].[sputilWordMergePDF]
@DocTemplate varbinary(MAX), 
  @FieldsXML xml,
  @MergedPDFDoc varbinary(MAX) OUTPUT,
  @ErrorMessage nvarchar(MAX) = NULL OUTPUT,
@HTTPStatus int = NULL OUTPUT
--$!SQLVer Nov  7 2020  5:09AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
   
  DECLARE @URL varchar(1024)
  SET @URL = 'http://localhost:24800/DoCLR/' 

  --Note:  handy for testing posts:
  --SET @URL = 'http://posttestserver.com/post.php'


  DECLARE @MethodToCall varchar(MAX)
  SET @MethodToCall = 'WordMergePDF'
  
  --Filename that is echoed back in the HTTP response when the PDF
  --document is returned
  DECLARE @Filename varchar(254)
  SET @Filename = 'WordMergeTemplate.docx'
  --------------------------------------
       
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)


  DECLARE @MultipartBoundary varchar(100)
  SET @MultipartBoundary = LOWER(LEFT(REPLACE(CAST(NEWID() AS varchar(100)), '-', ''), 16))
  --Alternate ways of generating a boundary value:
    --SET @MultipartBoundary = CAST(DATEDIFF(s, '19700101', GETDATE()) AS varchar(100))
    --SET @MultipartBoundary = sqlver.udfRandomString(16)
    
  SET @MultipartBoundary = sqlver.udfLPad(@MultipartBoundary, '-', 40)

  DECLARE @Headers varchar(MAX)
  DECLARE @ContentType varchar(254)
  DECLARE @DataToSendBin varbinary(MAX)
  DECLARE @DataToSend varchar(MAX)

  DECLARE @RedirURL varchar(1024) 

  --Set @ContentType.  This is passed into sqlver.spsysBuildCLR_GetHTTP
  --(i.e. does not need to be added to a header or concatenated into the data)
  SET @ContentType = 'multipart/form-data; boundary=' + @MultipartBoundary

  SET @Headers = 
    'Content-Length: {{$LENGTH}}' + @CRLF

  --Field "methodToCall"                    
  SET @DataToSend =
    '--' + @MultipartBoundary + @CRLF +  --Boundary + CRLF
    'Content-Disposition: form-data; name="methodToCall"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF + --Extra CRLF is REQUIRED!!!
    @MethodToCall +
    @CRLF + --Closing CRLF is REQUIRED!!

    --Field "fieldsXML" 
    '--' + @MultipartBoundary + @CRLF +  --Boundary + CRLF
    'Content-Disposition: form-data; name="fieldsXML"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF + --Extra CRLF is REQUIRED!!!
    CAST(@FieldsXML AS varchar(MAX)) +
    @CRLF + --Closing CRLF is REQUIRED!!


    --Field "templateDOCX" to hold the binary template file
    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="templateWordDoc"; filename="' +  @Filename + '"' + @CRLF +
    'Content-Type: application/octet-stream' + @CRLF +
    'Content-Transfer-Encoding: binary' + @CRLF +
     @CRLF --Extra CRLF is REQUIRED!!!

  --Add binary data payload
  SET @DataToSendBin = CAST(@DataToSend AS varbinary(MAX)) + 
    @DocTemplate

  --Final footer
  SET @DataToSendBin = @DataToSendBin +
    CAST(@CRLF +'--' + @MultipartBoundary + '--' + @CRLF AS varbinary(MAX)) --Extra CRLF is REQUIRED!!!

  DECLARE @DataLen int
  SET @DataLen = DATALENGTH(@DataToSendBin)

  SET @DataToSend = NULL
  
  SET @Headers = REPLACE(@Headers, '{{$LENGTH}}', CAST(ISNULL(@DataLen, 0) AS varchar(100)))
  
  SET @HTTPStatus = NULL
  SET @RedirURL = NULL 
  SET @ErrorMessage = NULL

  --Initiate HTTP POST
  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @HTTPMethod = 'POST',  
    @ContentType = @ContentType,
    @Cookies = NULL,
    @DataToSend = @DataToSend,
    @DataToSendBin = @DataToSendBin,
    @Headers = @Headers,
    @UserAgent = 'SQLVerCLR',
    @HTTPStatus = @HTTPStatus OUTPUT,
    @RedirURL = @RedirURL OUTPUT,  
    @ResponseBinary = @MergedPDFDoc OUTPUT,
    @ErrorMsg = @ErrorMessage OUTPUT

   
END

GO


IF OBJECT_ID('[sqlver].[spgetSQLBusyProcesses]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLBusyProcesses]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLBusyProcesses]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SELECT
    DB_NAME(sysproc.dbid) AS [Database],
    sysproc.hostname,
    sysproc.[program_name],
    --CASE WHEN PATINDEX('%SQLAgent%', sysproc.[program_name]) > 0 THEN CONVERT(varbinary(128), sqlver.udfParseValue(sysproc.program_name, 6, ' '), 1) END AS JobID,
    job.name AS SQLAgentJob,
    job.date_modified AS SQLAgentJobModified,
    CASE WHEN PATINDEX('%SQLAgent%', sysproc.[program_name]) > 0 THEN REPLACE(sqlver.udfParseValue(sysproc.program_name, 8, ' ') + ' ' + sqlver.udfParseValue(sysproc.program_name, 9, ' '), ')', '') END AS SQLAgentJobStep,
    sysproc.physical_io,
    sysproc.cpu,
    sysproc.status,
    
    sysproc.cmd,

    sysproc.open_tran,

    sysproc.blocked,
    sysproc.waittime,
    sysproc.lastwaittype,
    sysproc.waitresource,

    sysproc.loginame,
    sysproc.spid,
    sysproc.last_batch--,
    
    --COALESCE(OBJECT_SCHEMA_NAME(tx.objectid, tx.dbid) + '.' + OBJECT_NAME(tx.objectid), tx.text) AS CommandText    
  FROM
    sys.sysprocesses (nolock) sysproc
    LEFT JOIN msdb..sysjobs job ON    
      CASE WHEN PATINDEX('%SQLAgent%', sysproc.[program_name]) > 0 THEN CONVERT(varbinary(128), sqlver.udfParseValue(sysproc.program_name, 6, ' '), 1) END = job.job_id
    --OUTER APPLY sys.dm_exec_sql_text(sysproc.sql_handle) tx          
  WHERE
    sysproc.spid <> @@SPID AND
    sysproc.status <> 'sleeping'
  ORDER BY
    sysproc.open_tran DESC,
    sysproc.physical_io DESC
END

GO


IF OBJECT_ID('[sqlver].[sputilGetColumnInfo]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetColumnInfo]
END
GO

CREATE PROCEDURE [sqlver].[sputilGetColumnInfo]
@ObjectName sysname = NULL,
@ObjectSchema sysname = NULL,
@SQL varchar(MAX) = NULL

WITH EXECUTE AS CALLER
--$!SQLVer Dec  9 2020  3:43PM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON
   
  IF @ObjectName IS NOT NULL BEGIN
    SELECT
      col.ORDINAL_POSITION,
      col.COLUMN_NAME,
      col.DATA_TYPE,
      col.CHARACTER_MAXIMUM_LENGTH,
      col.NUMERIC_PRECISION,
      col.NUMERIC_SCALE,
      col.IS_NULLABLE    
    FROM
      INFORMATION_SCHEMA.COLUMNS col
    WHERE
      col.TABLE_SCHEMA = ISNULL(@ObjectSchema, 'dbo') AND
      col.TABLE_NAME = @ObjectName
    ORDER BY 
      col.ORDINAL_POSITION
  END
  ELSE IF @SQL IS NOT NULL BEGIN
    DECLARE @IsSet sql_variant
    SELECT @IsSet = value_in_use FROM sys.configurations WHERE name = 'Ad Hoc Distributed Queries'
    SET @IsSet = ISNULL(@IsSet, 0)

    IF @IsSet = 0 BEGIN
      EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
      RECONFIGURE;
    END

    DECLARE @DataSource varchar(512)
    SET @DataSource = 
      'server=' + CAST(SERVERPROPERTY('SERVERNAME') AS varchar(512)) + ';' +
      'Database=' + DB_NAME() + ';' +
      'trusted_connection=yes'

    DECLARE @Provider varchar(128)
    SET @Provider = 'SQLNCLI'

    DECLARE @CRLF nvarchar(5)
    SET @CRLF = CHAR(13) + CHAR(10)

    DECLARE @Quote nvarchar(1)
    SET @Quote = CHAR(39)

    DECLARE @SQLInt nvarchar(MAX)

    SET @SQLInt = 
    'SELECT * INTO #Test FROM OPENROWSET(' + 
      QUOTENAME(ISNULL(@Provider, '{provider}'), @Quote) + ', ' + 
      QUOTENAME(ISNULL(@DataSource, '{datasource}'), @Quote) + ', ' + 
      @Quote + 
      REPLACE(@SQL, @Quote, @Quote + @Quote) + ';' + @CRLF +   
      @Quote + '); '  + @CRLF +
      
    '
    DECLARE @ObjectID int
    SET @ObjectID = OBJECT_ID(''tempdb..#test'')

    DECLARE @ObjectName sysname
    SELECT @ObjectName = name FROM tempdb.sys.objects WHERE object_id = @ObjectID

    SELECT 
      ORDINAL_POSITION,
      COLUMN_NAME,
      DATA_TYPE,
      CHARACTER_MAXIMUM_LENGTH,
      NUMERIC_PRECISION,
      NUMERIC_SCALE,
      IS_NULLABLE
    FROM tempdb.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @ObjectName
    ORDER BY ORDINAL_POSITION

    DROP TABLE #Test'

    EXEC(@SQLInt)

    IF @IsSet = 0 BEGIN
      EXEC sp_configure 'Ad Hoc Distributed Queries', 0;
      RECONFIGURE;
    END
  END
  ELSE BEGIN
    RAISERROR('Error in sputilGetColumnInfo: Nothing passed in to either @ObjectName or @SQL parameter.', 16, 1)
  END  
  
END

GO


IF OBJECT_ID('[sqlver].[sputilWriteBinaryToFile]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilWriteBinaryToFile]
END
GO

CREATE PROCEDURE [sqlver].[sputilWriteBinaryToFile]

@FileData varbinary(MAX),
@FilePath varchar(2048),
@Filename varchar(255),

@ErrorMsg varchar(MAX) = NULL OUTPUT,
  @LastResultCode int = NULL OUTPUT,
  
@SilenceErrors bit = 0

WITH EXECUTE AS CALLER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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
    RETURN 2001
  END
END

GO


IF OBJECT_ID('[sqlver].[spWhoIsHogging]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spWhoIsHogging]
END
GO

CREATE PROCEDURE [sqlver].[spWhoIsHogging]
@LockType varchar(100) = 'X'
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[sputilWriteStringToFile]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilWriteStringToFile]
END
GO

CREATE PROCEDURE [sqlver].[sputilWriteStringToFile]
@FileData varchar(MAX),
@FilePath varchar(2048),
@Filename varchar(255),

@ErrorMsg nvarchar(MAX) OUTPUT,
@LastResultCode int = NULL OUTPUT,
@SilenceErrors bit = 0

WITH EXECUTE AS CALLER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  --From article by Phil Factor
  --http://www.simple-talk.com/sql/t-sql-programming/reading-and-writing-files-in-sql-server-using-t-sql/

  SET NOCOUNT ON

  DECLARE @objFileSystem int
  DECLARE @objTextStream int
  DECLARE @objErrorObject int

  DECLARE @ErrSource varchar(512)
  DECLARE @OAErrMsg varchar(512)
  DECLARE @ErrLocation varchar(512)
  DECLARE @Helpfile varchar(255)
  DECLARE @HelpID int

  SET @ErrorMsg = NULL

	DECLARE @Command varchar(1000)
  DECLARE @FileAndPath varchar(80)

  IF RIGHT(@FilePath, 1) <> '\' BEGIN
    SET @FilePath = @FilePath + '\'
  END
    
  SET @FileAndPath = @FilePath + @Filename

  SET @ErrLocation = NULL
  SET @objErrorObject = NULL

  --Create FileSystemObject
  EXECUTE @LastResultCode = sp_OACreate 'Scripting.FileSystemObject' , @objFileSystem OUTPUT

  IF @LastResultCode = 0 BEGIN
    --Success.  Perform next operation.
    EXEC @LastResultCode = sp_OAMethod @objFileSystem, 'CreateTextFile', @objTextStream OUTPUT, @FileAndPath, 2, False
  END
  ELSE BEGIN
    --Encountered error on previous operation
    SET @ErrLocation = COALESCE(@ErrLocation, 'creating FileSystemObject')
    SET @objErrorObject = COALESCE(@objErrorObject, @objFileSystem)
  END

  IF @LastResultCode = 0 BEGIN
    --Success.  Perform next operation.
    EXEC @LastResultCode = sp_OAMethod @objTextStream, 'Write', NULL, @FileData
  END
  ELSE BEGIN
    --Encountered error on previous operation
    SET @ErrLocation = COALESCE(@ErrLocation, 'creating the file "' + @FileAndPath + '"')
    SET @objErrorObject = COALESCE(@objErrorObject, @objFileSystem)
  END

  IF @LastResultCode = 0 BEGIN
    --Success.  Perform next operation.
    EXEC @LastResultCode = sp_OAMethod  @objTextStream, 'Close'
  END
  ELSE BEGIN
    --Encountered error on previous operation
    SET @ErrLocation = COALESCE(@ErrLocation, 'writing to the file "' + @FileAndPath + '"')
    SET @objErrorObject = COALESCE(@objErrorObject, @objTextStream)
  END

  IF @LastResultCode > 0 BEGIN
    --Encountered error on previous operation
    SET @ErrLocation = COALESCE(@ErrLocation, 'closing the file "' + @FileAndPath + '"')
    SET @objErrorObject = COALESCE(@objErrorObject, @objTextStream)
  END

  IF @LastResultCode <> 0 BEGIN
 	
    BEGIN TRY
	    EXEC sp_OAGetErrorInfo @objErrorObject, @ErrSource OUTPUT, @OAErrMsg OUTPUT, @Helpfile OUTPUT, @HelpID OUTPUT

	    SET @ErrorMsg='Error ' +
        ISNULL('while ' + @ErrLocation + ' ', '') +
			  COALESCE(@OAErrMsg,'') +
        ISNULL(' (' + @ErrSource + ')', '')
    END TRY
    BEGIN CATCH
      SET @ErrorMsg = 'Error while calling sp_OAGetErrorInfo: ' + ERROR_MESSAGE() +
        ISNULL(' Previous error: ' + @ErrLocation + ': ' + @ErrorMsg, '')
    END CATCH

	END
  	
  BEGIN TRY
    EXECUTE sp_OADestroy @objTextStream
  END TRY
  BEGIN CATCH
    SET @ErrorMsg = 'Error while calling sp_OAGetErrorInfo: ' + ERROR_MESSAGE() +
      ISNULL(' Previous error: ' + @ErrLocation + ': ' + @ErrorMsg, '')      
  END CATCH


  IF @ErrorMsg IS NOT NULL BEGIN
    SET @ErrorMsg = 
      'Error in sqlver.sputilWriteStringToFile: ' + CHAR(13) + CHAR(10) + 
      @ErrorMsg + CHAR(13) + CHAR(10) + 
      'FYI, this procedure requires use of COM (OLE Automation) objects.  To enable support, execute the following:' + CHAR(13) + CHAR(10) + 
      '  EXEC master.dbo.sp_configure ''show advanced options'', 1;' + CHAR(13) + CHAR(10) + 
      '  RECONFIGURE;' + CHAR(13) + CHAR(10) + 
      '  EXEC master.dbo.sp_configure ''Ole Automation Procedures'', 1;' + CHAR(13) + CHAR(10) + 
      '  RECONFIGURE;'  + CHAR(13) + CHAR(10) +    
    'User = ' + USER_NAME()
  END

  IF (@ErrorMsg IS NOT NULL) AND (ISNULL(@SilenceErrors, 0) = 0) BEGIN
    RAISERROR (@ErrorMsg, 16, 1)
    RETURN 2001
  END

END

GO


IF OBJECT_ID('[sqlver].[sputilGetFileList]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetFileList]
END
GO

CREATE PROCEDURE [sqlver].[sputilGetFileList]
  @StartingPath nvarchar(4000),
  @MaxDepth int = NULL,
  @IncludeFolders bit = 0,
  @ExcludeFileList nvarchar(4000) = NULL,
  @FileList nvarchar(MAX) = NULL OUTPUT,
  @SuppressResultset bit = 0
--$!SQLVer Jul 22 2022 10:36PM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  /*
  Usage:  This procedure must do an INSERT ... EXEC xp_dirtree
  SQL does not allow nested INSERT EXEC calls, so it is a little tricky for the caller
  to obtain a resultset that can be inserted in the caller's table.

  If called with @SuppressResultset = 1, no rows are returned, but instead the
  results are concatenated into a delimited string that can be returned to the
  caller.

  The caller can then parse the delimited string, and insert into a table.
  
  For example:


  DECLARE @tvFileList TABLE (
    Seq int,
    FileName nvarchar(MAX),
    RelativePath nvarchar(MAX),
    FQFileName nvarchar(MAX),

    FolderDepth int,
    FileID int,
    ParentFileID int,
    IsFolder bit
  )

  DECLARE @Buf nvarchar(MAX)
  EXEC sqlver.sputilGetFileList @StartingPath = N'C:\SQLVer\Temp\tmp7BDE566B88DC443FA4C92821989E94DE\jqwidgets\jQWidgets-master\jqwidgets' , @SuppressResultset = 1,  @FileList = @Buf OUTPUT

  INSERT INTO @tvFileList
  SELECT 
    sqlver.udfParseValue(pv.Value, 1, '|') AS Seq,
    sqlver.udfParseValue(pv.Value, 2, '|') AS FileName,
    sqlver.udfParseValue(pv.Value, 3, '|') AS RelativePath, 
    sqlver.udfParseValue(pv.Value, 4, '|') AS FQFileName,

    sqlver.udfParseValue(pv.Value, 5, '|') AS FolderDepth,
    sqlver.udfParseValue(pv.Value, 6, '|') AS FileID,
    sqlver.udfParseValue(pv.Value, 7, '|') AS ParentFileID,
    sqlver.udfParseValue(pv.Value, 8, '|') AS IsFolder
  FROM
    sqlver.udftGetParsedValues(@Buf, CHAR(10)) pv


  SELECT
    *
  FROM
    @tvFileList fl
  ORDER BY
    fl.Seq

  */

  DECLARE @Recurse bit
  
  IF @MaxDepth > 1 OR @MaxDepth IS NULL BEGIN
    SET @Recurse = 1
  END

  SET @FileList = NULL

  --trim trailing slash
  SET @StartingPath = RTRIM(@StartingPath)
  IF RIGHT(@StartingPath, 1) IN ('\', '/') BEGIN
    SET @StartingPath = LEFT(@StartingPath, LEN(@StartingPath) - 1)
  END

  DECLARE @tvOutput TABLE ([subdirectory] nvarchar(1024), [depth] int, [file] int)
  INSERT INTO @tvOutput
  EXEC master.sys.xp_dirtree @initialFolder=@StartingPath, @maxFolderDepth=@MaxDepth, @includFiles = 1;


  DECLARE @tvFileList TABLE (
    Seq int,
    FileName nvarchar(MAX),
    RelativePath nvarchar(MAX),
    FQFileName nvarchar(MAX),

    FolderDepth int,
    FileID int,
    ParentFileID int,
    IsFolder bit,
    RootPath nvarchar(MAX)
  )


  DECLARE @tvDirTree table (
    FileID INT IDENTITY(1,1),
    FileName nvarchar(4000),
    FolderDepth INT,
    IsFile BIT,
    ParentFileID  int
  )

  -- top level directory
  INSERT @tvDirTree(
    FileName,
    FolderDepth,
    IsFile
  )
  VALUES (
    @StartingPath,
    0,
    0);

  -- all the rest under top level
  INSERT @tvDirTree(
    FileName,
    FolderDepth,
    IsFile
  )
  EXEC master.sys.xp_dirtree @initialFolder=@StartingPath, @maxFolderDepth=@MaxDepth, @includFiles = 1;

  -- set ParentFileID
  UPDATE @tvDirTree
  SET
    ParentFileID = (
      SELECT MAX(d2.FileID) FROM @tvDirTree d2 WHERE d2.FolderDepth = d.FolderDepth - 1 AND d2.FileID < d.FileID
    )
  FROM
  @tvDirTree d

  ;

  WITH dirs AS (
    SELECT
      FileID,
      FileName,
      FolderDepth,
      IsFile,
      ParentFileID,
      Filename AS RootPath,
      CAST(NULL AS nvarchar(4000)) AS RelativePath,
      FileName AS FQFileName
    FROM
      @tvDirTree
    WHERE
      ParentFileID IS NULL

    UNION ALL
    SELECT
      d.FileID,
      d.FileName,
      d.FolderDepth,
      d.IsFile,
      dirs.FileID,
      dirs.RootPath,
      ISNULL(dirs.RelativePath, '') + CASE WHEN dirs.FolderDepth > 0 THEN '\' + dirs.FileName ELSE '' END,
      dirs.RootPath + ISNULL(dirs.RelativePath, '') + CASE WHEN dirs.FolderDepth > 0 THEN '\' + dirs.FileName ELSE '' END + '\' + d.FileName
    FROM
      @tvDirTree AS d
      INNER JOIN dirs ON
        d.ParentFileID = dirs.FileID
  )

  INSERT INTO @tvFileList (
    Seq,
    FileName,
    RelativePath,
    FQFileName,

    FolderDepth,
    FileID,
    ParentFileID,
    IsFolder
    --RootPath
  )
  SELECT
    ROW_NUMBER() OVER (ORDER BY cte.RelativePath, cte.IsFile, cte.FileName),
    cte.FileName,
    cte.RelativePath,
    cte.FQFileName,

    cte.FolderDepth,
    cte.FileID,
    cte.ParentFileID,
    CASE WHEN cte.IsFile = 0 THEN 1 ELSE 0 END AS IsFolder
    --cte.RootPath
  FROM 
    dirs cte
  WHERE
    cte.FolderDepth > 0 AND
    (@IncludeFolders = 1 OR cte.IsFile = 1) AND
    (@Recurse = 1 OR cte.FolderDepth = 1) AND
    cte.Filename NOT IN (SELECT value FROM sqlver.udftGetParsedValues(@ExcludeFileList, '|'))


  IF ISNULL(@SuppressResultset, 0) = 0 BEGIN
    SELECT
      Seq,
      FileName,
      RelativePath,
      FQFileName,

      FolderDepth,
      FileID,
      ParentFileID,
      IsFolder
      --RootPath
    FROM
      @tvFileList fl
    ORDER BY
      fl.Seq
  END
  ELSE BEGIN
    SELECT
      @FileList = ISNULL(@FileList + CHAR(13) + CHAR(10), '') + 
      CAST(fl.Seq AS varchar(100)) + '|' + 
      fl.FileName + '|' +
      fl.RelativePath + '|' +
      fl.FQFileName + '|' + 

      CAST(fl.FolderDepth AS nvarchar(100)) + '|' +
      CAST(fl.FileID AS nvarchar(100)) + '|' +
      CAST(fl.ParentFileID AS nvarchar(100)) + '|' +
      CAST(fl.IsFolder AS nvarchar(100)) + '|' 
      --fl.RootPath 
    FROM
      @tvFileList fl
  END

END

GO


IF OBJECT_ID('[sqlver].[spgetSQLFilegroupsOutOfSpaceAllDBs]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLFilegroupsOutOfSpaceAllDBs]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLFilegroupsOutOfSpaceAllDBs]
@ListDrives bit = 0,
@ListAllFiles bit = 0,
@MinGigsFree int = 10
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spgetMissingIndexes]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetMissingIndexes]
END
GO

CREATE PROCEDURE [sqlver].[spgetMissingIndexes]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spsysCreateSubDir]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysCreateSubDir]
END
GO

CREATE PROCEDURE [sqlver].[spsysCreateSubDir]
@NewPath nvarchar(1024)

WITH EXECUTE AS CALLER
--$!SQLVer Nov  7 2020  5:10AM by sa
--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  EXECUTE master.dbo.xp_create_subdir @NewPath 
END

GO


IF OBJECT_ID('[sqlver].[spgetSQLSpaceUsedDB]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLSpaceUsedDB]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsedDB]
@objname nvarchar(776) = NULL,		@updateusage varchar(5) = false
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spsysSchemaUpdateLogComments]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spsysSchemaUpdateLogComments]
END
GO

CREATE PROCEDURE sqlver.spsysSchemaUpdateLogComments
@SchemaLogID int = NULL,
@DatabaseName sysname = NULL,
@SchemaName sysname = NULL,
@ObjectName sysname = NULL,
@Comments nvarchar(MAX)
--$!SQLVer Dec 14 2021  8:29AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF @SchemaLogID IS NOT NULL BEGIN
    SELECT
      @DatabaseName = schl.DatabaseName,
      @SchemaName = schl.SchemaName,
      @ObjectName = schl.ObjectName
    FROM
      sqlver.tblSchemaLog schl
    WHERE
      schl.SchemaLogId = @SchemaLogID
  END

  IF @DatabaseName IS NULL BEGIN
    SET @DatabaseName = DB_NAME()
  END

  IF @SchemaName IS NULL BEGIN
    SET @SchemaName = 'opsstream'
  END

  UPDATE schl
  SET
    Comments = @Comments
  FROM
    sqlver.tblSchemaLog schl
  LEFT JOIN (
    SELECT
      schl2.DatabaseName,
      schl2.SchemaName,
      schl2.ObjectName,
      MAX(schl2.SchemaLogId) AS SchemaLogID
    FROM
      sqlver.tblSchemaLog schl2
    WHERE
      schl2.DatabaseName = @DatabaseName AND
      schl2.SchemaName = @SchemaName AND
      schl2.ObjectName = @ObjectName
    GROUP BY
      schl2.DatabaseName,
      schl2.SchemaName,
      schl2.ObjectName
    ) x ON
      schl.SchemaLogID = x.SchemaLogID
  WHERE
    (
     (@SchemaLogID IS NULL AND x.SchemaLogID IS NOT NULL) OR
     (@SchemaLogID IS NOT NULL AND @SchemaLogID  = schl.SchemaLogID)
    )
     
END

GO


IF OBJECT_ID('[sqlver].[spgetSQLSpaceUsedAllDBs]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLSpaceUsedAllDBs]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLSpaceUsedAllDBs]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spgetUnusedIndexes]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetUnusedIndexes]
END
GO

CREATE PROCEDURE [sqlver].[spgetUnusedIndexes]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[spShowSlowQueries]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spShowSlowQueries]
END
GO

CREATE PROCEDURE [sqlver].[spShowSlowQueries]
@ClearStatistics bit = 0
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[sputilRenameDefaultsAll]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilRenameDefaultsAll]
END
GO

CREATE PROCEDURE sqlver.sputilRenameDefaultsAll
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  SET NOCOUNT ON

  /* Renames all default constraints to dfTableName__ColumnName */
  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT
    'ALTER TABLE [' + sch.name + '].[' + tab.name + '] DROP CONSTRAINT' + '[' + dc.name + ']' AS SQLDrop,
    'ALTER TABLE [' + sch.name + '].[' + tab.name + '] ADD CONSTRAINT [' + 'df' + ISNULL(NULLIF(LEFT(tab.name, 3), 'tbl'), '') + SUBSTRING(tab.name, 4, LEN(tab.name)) + '__' + col.name + ']' + ' DEFAULT ' + dc.definition + ' FOR [' + col.name + ']' AS SQLAdd
  FROM
    sys.default_constraints dc
    JOIN sys.tables tab ON
      dc.parent_object_id = tab.object_id
    JOIN sys.columns col ON
      tab.object_id = col.object_id AND
      dc.parent_column_id = col.column_id
    LEFT JOIN sys.schemas sch ON
      tab.schema_id = sch.schema_id

  DECLARE @SQLDrop nvarchar(MAX)
  DECLARE @SQLAdd nvarchar(MAX)

  OPEN curThis
  FETCH curThis INTO @SQLDrop, @SQLAdd

  WHILE @@FETCH_STATUS = 0 BEGIN
    BEGIN TRY
      EXEC (@SQLDrop)
      EXEC (@SQLAdd)
    END TRY
    BEGIN CATCH
      PRINT ERROR_MESSAGE() + ' on' + ISNULL(@SQLDrop, NULL)
    END CATCH

    FETCH curThis INTO @SQLDrop, @SQLAdd
  END
  CLOSE curThis
  DEALLOCATE curThis

END

GO


IF OBJECT_ID('[sqlver].[sputilRecreateTable]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilRecreateTable]
END
GO

CREATE PROCEDURE [sqlver].[sputilRecreateTable]
@SchemaName sysname,
@ObjectName sysname,
@TableDef nvarchar(MAX) = NULL
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

  PRINT '***Dropping and re-creating ' + ISNULL(@SchemaName, 'NULL') + '.' + ISNULL(@ObjectName, 'NULL')

  IF @TableDef IS NULL BEGIN
    SET @TableDef = sqlver.udfScriptTable(@SchemaName, @ObjectName)
  END

  DECLARE @SQL nvarchar(MAX)


  PRINT ''
  PRINT 'Original table definition: '
  EXEC sqlver.sputilPrintString @SQL

  DECLARE @ColList nvarchar(MAX)


  SELECT
     @ColList = ISNULL(@ColList + ',', '') + '[' + col.name + ']' + CHAR(13) + CHAR(10)
  FROM
    sys.schemas sch
    JOIN sys.objects obj ON
      sch.schema_id = obj.schema_id
    JOIN sys.columns col ON
      obj.object_id = col.object_id
  WHERE
    sch.name = @SchemaName AND
    obj.name = @ObjectName AND
    col.is_computed = 0
  ORDER BY
    col.column_id

  PRINT ''
  PRINT '@ColList ='
  EXEC sqlver.sputilPrintString @ColList

  --Backup data
  PRINT ''
  PRINT 'Backing up data:'

  SET @SQL = 'SELECT * INTO dbo.[bak' + @ObjectName + '] FROM [' + @SchemaName + '].[' + @ObjectName + ']'
  EXEC sqlver.sputilPrintString @SQL
  EXEC (@SQL)
  
  --Find foreign keys
  PRINT ''
  PRINT 'Finding foreign keys:'
  SELECT
    ROW_NUMBER() OVER (ORDER BY sch_par.name, obj_par.name, obj_fk.name) AS Seq,
    sch_par.name AS SchemaName,
    obj_par.name AS ObjectName,

    --sch_fk.name + '.' + obj_fk.name + '.' + col_fk.name AS fk,
    --sch_par.name + '.' + obj_par.name + '.' + col_par.name AS par,
    --sch_ref.name + '.' + obj_ref.name + '.' + col_ref.name AS ref,

    'ALTER TABLE [' + sch_par.name + '].[' + obj_par.name + '] DROP CONSTRAINT ' +   
    '[' + obj_fk.name + ']' AS SQLDrop,

    'ALTER TABLE [' + sch_par.name + '].[' + obj_par.name + '] ADD CONSTRAINT ' + 
    '[' + obj_fk.name + ']' + ' FOREIGN KEY ([' + col_fk.name + ']) REFERENCES ' + 
    '[' + sch_ref.name + '].[' + obj_ref.name  + ']([' + col_ref.name + '])' AS SQLAdd
  INTO #FKSQL
  FROM 
    sys.foreign_key_columns fk

    join sys.objects obj_ref ON
      fk.referenced_object_id = obj_ref.object_id
    JOIN sys.schemas sch_ref ON
      obj_ref.schema_id = sch_ref.schema_id


    JOIN sys.objects obj_par ON
      fk.parent_object_id = obj_par.object_id
    JOIN sys.schemas sch_par ON
      obj_par.schema_id = sch_par.schema_id

    JOIN sys.objects obj_fk ON
      fk.constraint_object_id = obj_fk.object_id
    JOIN sys.schemas sch_fk ON
      obj_fk.schema_id = sch_fk.schema_id
   
    JOIN sys.foreign_key_columns fkc ON
      obj_fk.object_id = fkc.constraint_object_id

    JOIN sys.columns col_fk ON
      obj_par.object_id = col_fk.object_id AND
      fkc.parent_column_id = col_fk.column_id

    JOIN sys.columns col_par ON
      obj_par.object_id = col_par.object_id AND
      fkc.parent_column_id = col_par.column_id

    JOIN sys.columns col_ref ON
      obj_ref.object_id = col_ref.object_id AND
      fkc.referenced_column_id = col_ref.column_id

  WHERE
    sch_ref.name = @SchemaName AND
    obj_ref.name = @ObjectName


  --Drop foreign keys
  SET @SQL = NULL
  SELECT 
    @SQL = ISNULL(@SQL, '') + tmp.SQLDrop
  FROM
    #FKSQL tmp
  ORDER BY
    tmp.Seq

  IF @SQL IS NOT NULL BEGIN
    PRINT ''
    PRINT 'Dropping foreign keys that reference this table:'    
    EXEC sqlver.sputilPrintString @SQL
    EXEC(@SQL)
  END


  --drop table
  PRINT ''
  PRINT 'Dropping table:'
  SET @SQL =
    'IF OBJECT_ID(''dbo.[bak' + @ObjectName + ']'') IS NOT NULL BEGIN' + CHAR(13) + CHAR(10) +
    'DROP TABLE [' + @SchemaName + '].[' + @ObjectName + ']' + CHAR(13) + CHAR(10) +
    'END'
  EXEC sqlver.sputilPrintString @SQL
  EXEC (@SQL)

  PRINT ''
  PRINT 'Creating table:'
  EXEC sqlver.sputilPrintString @TableDef
  EXEC(@TableDef)


  --insert backed-up data
  PRINT ''
  PRINT 'Restoring data:'
  SET @SQL = 'SET IDENTITY_INSERT [' + @SchemaName + '].[' + @ObjectName + '] ON' + CHAR(13) + CHAR(10) +

    'INSERT INTO [' + @SchemaName + '].[' + @ObjectName + '] (' + @ColList + ') ' + CHAR(13) + CHAR(10) +
    'SELECT ' + @ColList + ' FROM dbo.[bak' + @ObjectName + ']' + CHAR(13) + CHAR(10) +

    'SET IDENTITY_INSERT [' + @SchemaName + '].[' + @ObjectName + '] OFF'
  EXEC sqlver.sputilPrintString @SQL
  EXEC(@SQL)

  --add foreign keys
  SET @SQL = NULL
  SELECT 
    @SQL = ISNULL(@SQL, '') + tmp.SQLAdd
  FROM
    #FKSQL tmp
  WHERE
    (tmp.SchemaName <> @SchemaName OR tmp.ObjectName <> @ObjectName)
  ORDER BY
    tmp.Seq

  IF @SQL IS NOT NULL BEGIN
    PRINT ''
    PRINT 'Adding foreign keys:'
    EXEC sqlver.sputilPrintString @SQL
    EXEC(@SQL)
  END


  --drop table
  PRINT ''
  PRINT 'Dropping backup table:'
  SET @SQL =
    'IF OBJECT_ID(''[' + @SchemaName + '].[' + @ObjectName + ']'') IS NOT NULL BEGIN' + CHAR(13) + CHAR(10) +
    'DROP TABLE dbo.[bak' + @ObjectName + ']' + CHAR(13) + CHAR(10) +
    'END'  EXEC sqlver.sputilPrintString @SQL
  EXEC (@SQL)

END

GO


IF OBJECT_ID('[sqlver].[sputilGetRowCounts]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetRowCounts]
END
GO

CREATE PROCEDURE [sqlver].[sputilGetRowCounts]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
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


IF OBJECT_ID('[sqlver].[sputilGetMaxIdentities]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilGetMaxIdentities]
END
GO

CREATE PROCEDURE sqlver.sputilGetMaxIdentities
@InhibitResultset bit = 0
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  IF OBJECT_ID('tempdb..#MaxInts') IS NULL BEGIN
    CREATE TABLE #MaxInts (
      ColName sysname,
      CurMin bigint,
      CurMax bigint
    )
  END

  DECLARE @SQL nvarchar(MAX)
  DECLARE @ColName sysname

  DECLARE curThis CURSOR LOCAL STATIC FOR
  SELECT  
      QUOTENAME(DB_NAME()) + '.' + QUOTENAME(sch.name) + '.' + QUOTENAME(tab.name) + '.' + QUOTENAME(col.name) AS ColName,
      'USE ' + QUOTENAME(DB_NAME()) + ';' + CHAR(13) + CHAR(13) + 
      'SELECT ''' + QUOTENAME(DB_NAME()) + '.' + QUOTENAME(sch.name) + '.' + QUOTENAME(tab.name) + '.' + QUOTENAME(col.name) + ''' AS ColName , ' + 
      '(SELECT MIN(' + QUOTENAME(col.name) + ') FROM ' + QUOTENAME(sch.name) + '.' + QUOTENAME(tab.name) + 'WITH (NOLOCK)), ' + 
      '(SELECT MAX(' + QUOTENAME(col.name) + ') FROM ' + QUOTENAME(sch.name) + '.' + QUOTENAME(tab.name) + 'WITH (NOLOCK))'
  FROM
    sys.tables tab
    JOIN sys.columns col ON
      tab.object_id = col.object_id
    JOIN sys.types typ ON
      col.system_type_id = typ.system_type_id
    JOIN sys.schemas sch ON
      tab.schema_id = sch.schema_id
  WHERE
    typ.name = 'int' AND
    col.is_identity = 1
  ORDER BY
    sch.name,
    tab.name,
    col.name

  OPEN curThis
  FETCH curThis INTO @ColName, @SQL

  WHILE @@FETCH_STATUS = 0 BEGIN

    PRINT @ColName

    INSERT INTO #MaxInts (ColName, CurMin, CurMax)
    EXEC(@SQL)
    
    FETCH curThis INTO @ColName, @SQL
  END

  CLOSE curThis
  DEALLOCATE curThis

  IF ISNULL(@InhibitResultset, 0) = 0 BEGIN
    SELECT
      mi.*,
      CAST(mi.CurMax / 2147483647.0 * 100 AS Decimal(5, 2)) as PctOfMaxInt
    FROM
      #MaxInts mi
    ORDER BY
      mi.CurMax DESC
  END

END

GO


IF OBJECT_ID('[sqlver].[sputilRunPDFReport]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[sputilRunPDFReport]
END
GO

CREATE PROCEDURE [sqlver].[sputilRunPDFReport]
@ReportName nvarchar(80),
@URL nvarchar(1024) = 'http://localhost:24800/DoCLR',
@HeaderFieldsXML xml = NULL,
@BodyFieldsXML xml = NULL,
@PreferredWidth int = 0, @Flow varchar(255) = '', @ColumnsOut int = 3, @PDFBuf varbinary(MAX) OUTPUT
--$!SQLVer Aug  3 2021  9:26AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN

DECLARE @XML xml

SELECT @XML =
  (SELECT 

    (SELECT

      --ReportDef
      (
      SELECT
        @ReportName AS ReportName,
        @PreferredWidth AS PreferredWidth,
        @Flow AS Flow,
        @ColumnsOut AS ColumnsOut,
        @HeaderFieldsXML,
        @BodyFieldsXML 
      FOR XML PATH('ReportDef'), TYPE
      ),
      
      --RowData
      (
      SELECT *
      FROM
        #RowData
      --ORDER BY Seq
      FOR XML PATH, ROOT('RowData'), TYPE
      )

    FOR XML PATH(''), ROOT('Report'), TYPE
    )
  FOR XML PATH(''), ROOT('Reports'), TYPE
  )

SELECT 'Debug', @XML

  DECLARE @Buf varbinary(MAX)

  DECLARE @MultipartBoundary varchar(100)
  SET @MultipartBoundary = LOWER(LEFT(REPLACE(CAST(NEWID() AS varchar(100)), '-', ''), 16))   
  SET @MultipartBoundary = sqlver.udfLPad(@MultipartBoundary, '-', 40)
  

  DECLARE @FileData varbinary(MAX)
  DECLARE @Filename varchar(254)

  SET @FileData = CAST(@XML AS varbinary(MAX))
  SET @Filename = 'Report.xml'

   
  DECLARE @CRLF varchar(5)
  SET @CRLF = CHAR(13) + CHAR(10)

  DECLARE @Headers varchar(MAX)
  DECLARE @ContentType varchar(254)
  DECLARE @DataToSendBin varbinary(MAX)
  DECLARE @DataToSend varchar(MAX)
  
  SET @ContentType = 'multipart/form-data; boundary=' + @MultipartBoundary

  SET @Headers = 
    'Content-Length: {{$LENGTH}}' + @CRLF
                  
  SET @DataToSend =
    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="methodToCall"'+ @CRLF +
    'Content-Type: text/plain' + @CRLF +
    @CRLF +
    'WordMergeTablePDF' +
    @CRLF +

    '--' + @MultipartBoundary + @CRLF +
    'Content-Disposition: form-data; name="fieldsXML"; filename="' +  @Filename + '"' + @CRLF +
    'Content-Type: application/octet-stream' + @CRLF +
    'Content-Transfer-Encoding: binary' + @CRLF +
     @CRLF


  SET @DataToSendBin =
    CAST(@DataToSend AS varbinary(MAX)) +
    @FileData +
    CAST(@CRLF +'--' + @MultipartBoundary + '--' + @CRLF AS varbinary(MAX))


  DECLARE @DataLen int
  SET @DataLen = DATALENGTH(@DataToSendBin)

  SET @DataToSend = NULL

  SET @Headers = REPLACE(@Headers, '{{$LENGTH}}', CAST(ISNULL(@DataLen, 0) AS varchar(100)))


  DECLARE @HTTPStatus int
  DECLARE @RedirURL nvarchar(1024)
  DECLARE @RXBuf varbinary(MAX)
  DECLARE @ErrorMsg nvarchar(MAX)

  EXEC sqlver.sputilGetHTTP_CLR
    @URL = @URL,
    @HTTPMethod = 'POST',  
    @ContentType = @ContentType,
    @Cookies = NULL,
    @DataToSend = NULL,
    @DataToSendBin = @DataToSendBin,
    @Headers = @Headers,
    @User = NULL,
    @Password = NULL,
    @UserAgent = 'OpsStream SQL',
    @AllowOldTLS = 0,
    @SSLProtocol = NULL,
    @HTTPStatus = @HTTPStatus OUTPUT,
    @HTTPStatusText = NULL,
    @RedirURL = @RedirURL OUTPUT,  
    @ResponseBinary = @PDFBuf OUTPUT,
    @ErrorMsg = @ErrorMsg OUTPUT  

END

GO


IF OBJECT_ID('[sqlver].[spgetSQLTempDBInfo]') IS NOT NULL BEGIN
  DROP PROCEDURE [sqlver].[spgetSQLTempDBInfo]
END
GO

CREATE PROCEDURE [sqlver].[spgetSQLTempDBInfo]
--$!SQLVer Nov  7 2020  5:10AM by sa

--©Copyright 2006-2018 by David Rueter (drueter@assyst.com)
 --See:  https://github.com/davidrueter/sqlver)
--Note: Comments after $!SQLVer and before AS are subject to automatic removal
AS
BEGIN
  ;
  WITH task_space_usage AS (
    -- SUM alloc/delloc pages
    SELECT
      session_id,
      request_id,
      SUM(internal_objects_alloc_page_count) AS alloc_pages,
      SUM(internal_objects_dealloc_page_count) AS dealloc_pages
    FROM
      sys.dm_db_task_space_usage WITH (NOLOCK)
    WHERE session_id <> @@SPID
    GROUP BY session_id, request_id
  )
  SELECT * FROM
  (
  SELECT
    TSU.session_id,
    TSU.alloc_pages * 1.0 / 128 AS [internal_object_MB_space],
    TSU.dealloc_pages * 1.0 / 128 AS [internal_object_dealloc_MB_space],
    EST.text,
    -- Extract statement from sql text
    ISNULL(
      NULLIF(
        SUBSTRING(
          EST.text, 
          ERQ.statement_start_offset / 2, 
          CASE WHEN ERQ.statement_end_offset < ERQ.statement_start_offset THEN 0 ELSE( ERQ.statement_end_offset - ERQ.statement_start_offset ) / 2 END
        ), ''
      ), EST.text
    ) AS [statement text],
    EQP.query_plan
  FROM
    task_space_usage AS TSU
    INNER JOIN sys.dm_exec_requests ERQ WITH (NOLOCK) ON
      TSU.session_id = ERQ.session_id AND
      TSU.request_id = ERQ.request_id
    OUTER APPLY sys.dm_exec_sql_text(ERQ.sql_handle) AS EST
    OUTER APPLY sys.dm_exec_query_plan(ERQ.plan_handle) AS EQP
  WHERE
    (EST.text IS NOT NULL OR EQP.query_plan IS NOT NULL)
  ) x
  ORDER BY
    internal_object_dealloc_MB_space DESC
  
END

GO


IF OBJECT_ID('[sqlver].[vwMasterSchemaManifest]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[vwMasterSchemaManifest]
END
GO

CREATE SYNONYM [sqlver].[vwMasterSchemaManifest] FOR [MASTER.OPSSTREAM.COM,24849].[osMaster].[sqlver].[vwSchemaManifest]

GO


IF OBJECT_ID('[sqlver].[spMasterSchemaObjectDefinition]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[spMasterSchemaObjectDefinition]
END
GO

CREATE SYNONYM [sqlver].[spMasterSchemaObjectDefinition] FOR [MASTER.OPSSTREAM.COM,24849].[osMaster].[sqlver].[spsysSchemaObjectDefinition]

GO


IF OBJECT_ID('[sqlver].[spMasterExecuteSQL]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[spMasterExecuteSQL]
END
GO

CREATE SYNONYM [sqlver].[spMasterExecuteSQL] FOR [MASTER.OPSSTREAM.COM,24849].[osMaster].[dbo].[sp_executesql]

GO


IF OBJECT_ID('[dbo].[rtlog]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[rtlog]
END
GO

CREATE SYNONYM [dbo].[rtlog] FOR [sqlver].[spinsSysRTLog]

GO


IF OBJECT_ID('[dbo].[rt]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[rt]
END
GO

CREATE SYNONYM [dbo].[rt] FOR [sqlver].[spShowRTLog]

GO


IF OBJECT_ID('[sqlver].[vwMasterSchemaLog]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[vwMasterSchemaLog]
END
GO

CREATE SYNONYM [sqlver].[vwMasterSchemaLog] FOR [MASTER.OPSSTREAM.COM,24849].[osMaster].[sqlver].[vwSchemaLog]

GO


IF OBJECT_ID('[dbo].[wc]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[wc]
END
GO

CREATE SYNONYM [dbo].[wc] FOR [sqlver].[spgetWhatChanged]

GO


IF OBJECT_ID('[sqlver].[spMasterSchemaObjectVersionsXML]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[spMasterSchemaObjectVersionsXML]
END
GO

CREATE SYNONYM [sqlver].[spMasterSchemaObjectVersionsXML] FOR [MASTER.OPSSTREAM.COM,24849].[osMaster].[sqlver].[spsysSchemaObjectVersionsXML]

GO


IF OBJECT_ID('[dbo].[prog]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[prog]
END
GO

CREATE SYNONYM [dbo].[prog] FOR [sqlver].[spgetSQLProgress]

GO


IF OBJECT_ID('[dbo].[chat]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[chat]
END
GO

CREATE SYNONYM [dbo].[chat] FOR [sqlver].[spactOpenAI_Chat]

GO


IF OBJECT_ID('[dbo].[find]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[find]
END
GO

CREATE SYNONYM [dbo].[find] FOR [sqlver].[sputilFindInCode]

GO


IF OBJECT_ID('[dbo].[tds]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[tds]
END
GO

CREATE SYNONYM [dbo].[tds] FOR [sqlver].[spGetSQLTempDBSessions]

GO


IF OBJECT_ID('[dbo].[sysp]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[sysp]
END
GO

CREATE SYNONYM [dbo].[sysp] FOR [sqlver].[spgetSQLProcesses]

GO


IF OBJECT_ID('[dbo].[lastMod]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[lastMod]
END
GO

CREATE SYNONYM [dbo].[lastMod] FOR [sqlver].[spgetLastModified]

GO


IF OBJECT_ID('[dbo].[ver]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[ver]
END
GO

CREATE SYNONYM [dbo].[ver] FOR [sqlver].[spVersion]

GO


IF OBJECT_ID('[sqlver].[ver]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[ver]
END
GO

CREATE SYNONYM [sqlver].[ver] FOR [sqlver].[spVersion]

GO


IF OBJECT_ID('[sqlver].[find]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[find]
END
GO

CREATE SYNONYM [sqlver].[find] FOR [sqlver].[sputilFindInCode]

GO


IF OBJECT_ID('[dbo].[verupd]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[verupd]
END
GO

CREATE SYNONYM [dbo].[verupd] FOR [sqlver].[spsysSchemaVersionUpdateFromMaster]

GO


IF OBJECT_ID('[dbo].[col]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[col]
END
GO

CREATE SYNONYM [dbo].[col] FOR [sqlver].[sputilGetColumnBlock]

GO


IF OBJECT_ID('[sqlver].[RTLog]') IS NOT NULL BEGIN
  DROP SYNONYM [sqlver].[RTLog]
END
GO

CREATE SYNONYM [sqlver].[RTLog] FOR [sqlver].[spinsSysRTLog]

GO


IF OBJECT_ID('[dbo].[diffs]') IS NOT NULL BEGIN
  DROP SYNONYM [dbo].[diffs]
END
GO

CREATE SYNONYM [dbo].[diffs] FOR [sqlver].[spsysSchemaShowDiffs]

GO





PRINT 'Done processing all database objects.  SQLVer is now ready for normal use.'
PRINT ''

PRINT ''
PRINT 'If you like, you can now build one or more of these CLR assemblies:'
PRINT ''
PRINT '
/*
EXEC sqlver.spsysBuildCLR_DiffMatch
EXEC sqlver.spsysBuildCLR_GetHTTP
EXEC sqlver.spsysBuildCLR_SendMail
EXEC sqlver.spsysBuildCLR_FTP
EXEC sqlver.spsysBuildCLR_SQLVerUtil
*/
'
/*
WARNING:  You must search-and-replace the string printed here to replace:
    ~-~{CR}{LF}
with an empty string.

For example, using T-SQL:

    REPLACE(@Buf, CHAR(126) + CHAR(45) + CHAR(126) + CHAR(13) + CHAR(10), '')

Or using SSMS, open Find and Replace (i.e. with CTRL-H), click the .* icon (to enable regular expressions), and search for:

    \x7e\x2d\x7e\x0d\x0a



(This is due to a limitation of the T-SQL PRINT statement that does not provide a way to print long strings or to suppress CR LF.)
*/
