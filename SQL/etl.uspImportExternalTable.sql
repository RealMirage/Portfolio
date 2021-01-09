/*
	The below procedure is for usage with Azure Synapse (Formerly SQL DW).
	Purpose: Imports external data into DW table and audits results. This would be used as a first step in a staging process.
	
	Usage example: EXEC etl.uspImportExternalTable @ExternaTableName = 'lake_stg_customers'
												  ,@StagingTableName = 'stg_customers'
												  ,@SchemaName = 'sales'
												  
	General convention kept is @PascalCase for Procedure parameters, @camelCase for internal variables.

*/
CREATE PROCEDURE etl.uspImportExternalTable @ExternalTableName VARCHAR(100), 
										    @StagingTableName VARCHAR(100), 
										    @SchemaName VARCHAR(100)
AS
BEGIN
	DECLARE @sqlString NVARCHAR(MAX)
		   ,@startDateTime DATETIME
		   ,@endDateTime DATETIME
		   ,@tmpStagingTable NVARCHAR(150)
		   

	SET @tmpStagingTable = 'tmp_' + @StagingTableName
	SET @startDateTime = GETDATE()
		 
	--Check to see if previous temporary table still exists, if so then drop it.
	SET @sqlString = '
						IF OBJECT_ID(''[' + @SchemaName + '].[' + @tmpStagingTable + ']'') IS NOT NULL
							BEGIN
								DROP TABLE [' + @SchemaName + '].[' + @tmpStagingTable + ']
							END
						
						CREATE TABLE [' + @SchemaName + '].[' + @StagingTableName + ']
						WITH 
						(
							DISTRIBUTION = ROUND_ROBIN,
							CLUSTERED COLUMNSTORE INDEX						
						)
						AS
						SELECT *
						FROM [' + @SchemaName + '].[' + @ExternalTableName + ']
					  '
	--Print to console for easier debugging
	PRINT @sqlString					
	EXEC (@sqlString)
	
	SET @endDateTime = GETDATE()
	
	--Get record count from temporary table and audit results.
	SET @sqlString = '
						DECLARE @auditRecordCount BIGINT = (SELECT COUNT(*) FROM [' + @SchemaName + '].[' + @tmpStagingTable + '])
						
						INSERT INTO etl_logs.external_table_import
						(start_dt, end_dt, records, source_object, target_object)
						SELECT ''' + CONVERT(VARCHAR, @startDateTime, 121) + ''' AS start_dt
							  ,''' + CONVERT(VARCHAR, @endDateTime, 121) + ''' AS end_dt
							  ,@auditRecordCount AS records
							  ,''' + '[' + @SchemaName + '].[' + @ExternalTableName + '] + '' AS source_object
							  ,''' + '[' + @SchemaName + '].[' + @StagingTableName + '] + '' AS target_object
					  '
	PRINT @sqlString
	EXEC (@sqlString)
	
	--Replace staging table with temporary table.
	SET @sqlString = '
						IF OBJECT_ID(''[' + @SchemaName + '].[' + @StagingTableName + ']'') IS NOT NULL
							BEGIN
								DROP TABLE [' + @SchemaName + '].[' + @StagingTableName + ']
							END
						
						RENAME OBJECT [' + @SchemaName + '].[' + @tmpStagingTable + '] TO [' + @StagingTableName + ']
					  '
	PRINT @sqlString
	EXEC (@sqlString)
END