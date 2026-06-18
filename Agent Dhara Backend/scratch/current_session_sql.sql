-- ============================================================
-- SQL QUALITY ASSESSMENT BADGE
-- Score: 100/100
-- Grade: A
-- No issues detected. Fully production ready!
-- ============================================================
-- ETL SQL — Agent Dhara — plan_id=plan_1781601408
-- dialect=tsql — review before executing against production.

-- ============================================================
-- Create configuration, watermark and logging tables if not exists
-- ============================================================
IF OBJECT_ID('dbo.etl_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME NULL,
        status VARCHAR(20) NOT NULL,
        error_message VARCHAR(MAX) NULL
    );
END;
GO

IF OBJECT_ID('dbo.etl_default_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_default_values (
        column_name VARCHAR(256) PRIMARY KEY,
        default_value VARCHAR(256) NOT NULL,
        data_type VARCHAR(50) NOT NULL
    );
END;
GO

IF OBJECT_ID('dbo.etl_invalid_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_invalid_values (
        column_name VARCHAR(256),
        invalid_value VARCHAR(256),
        PRIMARY KEY (column_name, invalid_value)
    );
END;
GO

IF OBJECT_ID('dbo.etl_rejects', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_rejects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        row_data VARCHAR(MAX) NOT NULL,
        error_reason VARCHAR(MAX) NOT NULL,
        rejected_at DATETIME DEFAULT GETDATE()
    );
END;
GO

IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_watermark (
        process_name VARCHAR(256) PRIMARY KEY,
        last_run_time DATETIME NOT NULL
    );
END;
GO

-- Seed ETL default configuration values
IF NOT EXISTS (SELECT 1 FROM dbo.etl_default_values WHERE column_name = 'Accounts_Clean.AccountType')
    INSERT INTO dbo.etl_default_values (column_name, default_value, data_type) VALUES ('Accounts_Clean.AccountType', N'', 'NVARCHAR(MAX)');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_default_values WHERE column_name = 'Accounts_Clean.Balance')
    INSERT INTO dbo.etl_default_values (column_name, default_value, data_type) VALUES ('Accounts_Clean.Balance', N'0', 'NVARCHAR(MAX)');
GO

-- ============================================================
-- Initialize Clean Tables if not exists
-- ============================================================
IF OBJECT_ID('dbo.Accounts_Clean', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Accounts_Clean] (
    [AccountID] BIGINT NOT NULL,
    [CustomerID] BIGINT NULL,
    [Balance] NVARCHAR(MAX) NULL,
    [AccountType] NVARCHAR(MAX) NULL,
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT NULL,
    CONSTRAINT [PK_Accounts_Clean] PRIMARY KEY ([AccountID])
    );
END;
GO

IF OBJECT_ID('dbo.Citizens_Clean', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Citizens_Clean] (
    [CitizenID] BIGINT NOT NULL,
    [Mobile] NVARCHAR(MAX) NULL,
    [RegistrationDate] DATE NULL,
    [FullName] NVARCHAR(255) NULL,
    [Email] NVARCHAR(255) NULL,
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT NULL,
    CONSTRAINT [PK_Citizens_Clean] PRIMARY KEY ([CitizenID])
    );
    CREATE NONCLUSTERED INDEX idx_Citizens_Clean_RegistrationDate ON [dbo].[Citizens_Clean]([RegistrationDate]);
END;
GO

-- === dataset: dbo.Accounts === 
IF OBJECT_ID('dbo.etl_clean_Accounts', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_Accounts;
GO
CREATE PROCEDURE dbo.etl_clean_Accounts
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_Accounts';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_Accounts', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Accounts_Clean', 'U') IS NULL
        BEGIN
            CREATE TABLE [dbo].[Accounts_Clean] (
                [AccountID] BIGINT NOT NULL,
        [CustomerID] BIGINT NULL,
        [Balance] NVARCHAR(MAX) NULL,
        [AccountType] NVARCHAR(MAX) NULL,
        etl_created_at DATETIME DEFAULT GETDATE(),
        etl_updated_at DATETIME DEFAULT GETDATE(),
        etl_batch_id INT NULL,
        CONSTRAINT [PK_Accounts_Clean] PRIMARY KEY ([AccountID])
            );
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Accounts_Staging') IS NOT NULL DROP TABLE #Accounts_Staging;
        CREATE TABLE #Accounts_Staging ([AccountID] NVARCHAR(MAX) NULL, [CustomerID] NVARCHAR(MAX) NULL, [Balance] NVARCHAR(MAX) NULL, [AccountType] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
            INSERT INTO #Accounts_Staging ([AccountID], [CustomerID], [Balance], [AccountType], etl_batch_id)
            SELECT [AccountID], [CustomerID], [Balance], [AccountType], @run_id FROM [dbo].[Accounts];

        -- Single-Pass expression updates on #Accounts_Staging
        UPDATE #Accounts_Staging
        SET [AccountType] = LOWER(LTRIM(RTRIM(CAST([AccountType] AS NVARCHAR(MAX)))))
        WHERE 1=1;

        -- Grouped config and null updates on #Accounts_Staging
        UPDATE c
        SET c.[AccountType] = COALESCE(c.[AccountType], TRY_CAST(dv_AccountType.default_value AS NVARCHAR(MAX))),
            c.[Balance] = COALESCE(c.[Balance], TRY_CAST(dv_Balance.default_value AS NVARCHAR(MAX)))
        FROM #Accounts_Staging c
        LEFT JOIN dbo.etl_default_values dv_AccountType ON dv_AccountType.column_name = 'Accounts_Clean.AccountType'
        LEFT JOIN dbo.etl_default_values dv_Balance ON dv_Balance.column_name = 'Accounts_Clean.Balance'
        WHERE c.[AccountType] IS NULL OR c.[Balance] IS NULL;

        -- Normalize empty strings and placeholders to NULL before validation
        UPDATE #Accounts_Staging
        SET [AccountID] = CASE WHEN LOWER(LTRIM(RTRIM([AccountID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [AccountID] END,
    [CustomerID] = CASE WHEN LOWER(LTRIM(RTRIM([CustomerID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [CustomerID] END,
    [Balance] = CASE WHEN LOWER(LTRIM(RTRIM([Balance]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Balance] END,
    [AccountType] = CASE WHEN LOWER(LTRIM(RTRIM([AccountType]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [AccountType] END;

        -- Quarantine rows where primary key [AccountID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Accounts', 'dbo.Accounts_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [AccountID] is NULL'
        FROM #Accounts_Staging r
        WHERE r.[AccountID] IS NULL;

        DELETE FROM #Accounts_Staging WHERE [AccountID] IS NULL;

        -- Quarantine rows where ID column [AccountID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Accounts', 'dbo.Accounts_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'AccountID is not numeric'
        FROM #Accounts_Staging r
        WHERE r.[AccountID] IS NOT NULL AND TRY_CAST(r.[AccountID] AS BIGINT) IS NULL;

        DELETE FROM #Accounts_Staging WHERE [AccountID] IS NOT NULL AND TRY_CAST([AccountID] AS BIGINT) IS NULL;

        -- Quarantine rows where ID column [CustomerID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Accounts', 'dbo.Accounts_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'CustomerID is not numeric'
        FROM #Accounts_Staging r
        WHERE r.[CustomerID] IS NOT NULL AND TRY_CAST(r.[CustomerID] AS BIGINT) IS NULL;

        DELETE FROM #Accounts_Staging WHERE [CustomerID] IS NOT NULL AND TRY_CAST([CustomerID] AS BIGINT) IS NULL;

        -- Deduplicate staging table by partition key(s)
        ;WITH _staging_dedup AS (
            SELECT ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([AccountID] AS NVARCHAR(400))))) ORDER BY (SELECT NULL)) AS _rn
            FROM #Accounts_Staging
        )
        DELETE FROM _staging_dedup WHERE _rn > 1;

        -- Copy fully transformed data from Staging to target Clean table (SCD Type 1)
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Accounts_Clean];
            INSERT INTO [dbo].[Accounts_Clean] ([AccountID], [AccountType], [Balance], [CustomerID], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([AccountID] AS BIGINT), [AccountType], [Balance], TRY_CAST([CustomerID] AS BIGINT), etl_batch_id, GETDATE(), GETDATE() FROM #Accounts_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Accounts_Clean] WHERE [AccountID] IN (SELECT [AccountID] FROM #Accounts_Staging);
            INSERT INTO [dbo].[Accounts_Clean] ([AccountID], [AccountType], [Balance], [CustomerID], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([AccountID] AS BIGINT), [AccountType], [Balance], TRY_CAST([CustomerID] AS BIGINT), etl_batch_id, GETDATE(), GETDATE() FROM #Accounts_Staging;
        END;

        DECLARE @staging_rows INT = (SELECT COUNT(*) FROM #Accounts_Staging);
        DECLARE @target_rows INT = (SELECT COUNT(*) FROM [dbo].[Accounts_Clean]);
        IF @staging_rows > 0 AND @target_rows = 0
        BEGIN
            RAISERROR('Post-load assertion failed: Target table is empty but staging had rows.', 16, 1);
        END;

        IF OBJECT_ID('tempdb..#Accounts_Staging') IS NOT NULL DROP TABLE #Accounts_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_clean_Accounts' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- === dataset: dbo.Citizens === 
IF OBJECT_ID('dbo.etl_clean_Citizens', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_Citizens;
GO
CREATE PROCEDURE dbo.etl_clean_Citizens
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_Citizens';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_Citizens', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Citizens_Clean', 'U') IS NULL
        BEGIN
            CREATE TABLE [dbo].[Citizens_Clean] (
                [CitizenID] BIGINT NOT NULL,
        [Mobile] NVARCHAR(MAX) NULL,
        [RegistrationDate] DATE NULL,
        [FullName] NVARCHAR(255) NULL,
        [Email] NVARCHAR(255) NULL,
        etl_created_at DATETIME DEFAULT GETDATE(),
        etl_updated_at DATETIME DEFAULT GETDATE(),
        etl_batch_id INT NULL,
        CONSTRAINT [PK_Citizens_Clean] PRIMARY KEY ([CitizenID])
            );
            CREATE NONCLUSTERED INDEX idx_Citizens_Clean_RegistrationDate ON [dbo].[Citizens_Clean]([RegistrationDate]);
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Citizens_Staging') IS NOT NULL DROP TABLE #Citizens_Staging;
        CREATE TABLE #Citizens_Staging ([CitizenID] NVARCHAR(MAX) NULL, [Mobile] NVARCHAR(MAX) NULL, [RegistrationDate] NVARCHAR(MAX) NULL, [FullName] NVARCHAR(MAX) NULL, [Email] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            INSERT INTO #Citizens_Staging ([CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], etl_batch_id)
            SELECT [CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], @run_id FROM [dbo].[Citizens];
        END
        ELSE
        BEGIN
            INSERT INTO #Citizens_Staging ([CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], etl_batch_id)
            SELECT [CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], @run_id FROM [dbo].[Citizens] WHERE COALESCE(TRY_CONVERT(datetime, [RegistrationDate], 120), TRY_CONVERT(datetime, [RegistrationDate], 103), TRY_CONVERT(datetime, [RegistrationDate], 101), TRY_CONVERT(datetime, [RegistrationDate], 111)) > @last_run;
        END

        -- Single-Pass expression updates on #Citizens_Staging
        UPDATE #Citizens_Staging
        SET [Email] = LOWER(LTRIM(RTRIM(LTRIM(RTRIM(CAST([Email] AS NVARCHAR(MAX))))))),
            [FullName] = LOWER(LTRIM(RTRIM(CAST([FullName] AS NVARCHAR(MAX))))),
            [Mobile] = REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CAST([Mobile] AS NVARCHAR(MAX)))), N'-', N''), N' ', N''), N'(', N''), N')', N''),
            [RegistrationDate] = LTRIM(RTRIM(CAST([RegistrationDate] AS NVARCHAR(MAX))))
        WHERE 1=1;

        -- Normalize empty strings and placeholders to NULL before validation
        UPDATE #Citizens_Staging
        SET [CitizenID] = CASE WHEN LOWER(LTRIM(RTRIM([CitizenID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [CitizenID] END,
    [Mobile] = CASE WHEN LOWER(LTRIM(RTRIM([Mobile]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Mobile] END,
    [RegistrationDate] = CASE WHEN LOWER(LTRIM(RTRIM([RegistrationDate]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [RegistrationDate] END,
    [FullName] = CASE WHEN LOWER(LTRIM(RTRIM([FullName]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [FullName] END,
    [Email] = CASE WHEN LOWER(LTRIM(RTRIM([Email]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Email] END;

        -- Quarantine rows where primary key [CitizenID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Citizens', 'dbo.Citizens_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [CitizenID] is NULL'
        FROM #Citizens_Staging r
        WHERE r.[CitizenID] IS NULL;

        DELETE FROM #Citizens_Staging WHERE [CitizenID] IS NULL;

        -- Quarantine rows where ID column [CitizenID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Citizens', 'dbo.Citizens_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'CitizenID is not numeric'
        FROM #Citizens_Staging r
        WHERE r.[CitizenID] IS NOT NULL AND TRY_CAST(r.[CitizenID] AS BIGINT) IS NULL;

        DELETE FROM #Citizens_Staging WHERE [CitizenID] IS NOT NULL AND TRY_CAST([CitizenID] AS BIGINT) IS NULL;

        -- Nullify invalid email format for optional column [Email]
        UPDATE #Citizens_Staging SET [Email] = NULL WHERE [Email] IS NOT NULL AND (NOT (CAST([Email] AS NVARCHAR(MAX)) LIKE '%_@_%._%') OR CAST([Email] AS NVARCHAR(MAX)) LIKE '%@%@%');

        -- Log unparseable dates from #Citizens_Staging.[RegistrationDate] to dbo.etl_rejects (audit only; row is kept)
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Citizens', 'dbo.Citizens_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [RegistrationDate] value ' + ISNULL(CAST(r.[RegistrationDate] AS NVARCHAR(MAX)), 'NULL') + ' is not a valid date (set to NULL)'
        FROM #Citizens_Staging r
        WHERE r.[RegistrationDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, r.[RegistrationDate], 120),
            TRY_CONVERT(date, r.[RegistrationDate], 103),
            TRY_CONVERT(date, r.[RegistrationDate], 101),
            TRY_CONVERT(date, r.[RegistrationDate], 111)
        ) IS NULL;

        -- Nullify unparseable date values in #Citizens_Staging.[RegistrationDate] (keep row, set bad date to NULL)
        UPDATE #Citizens_Staging SET [RegistrationDate] = NULL
        WHERE [RegistrationDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, [RegistrationDate], 120),
            TRY_CONVERT(date, [RegistrationDate], 103),
            TRY_CONVERT(date, [RegistrationDate], 101),
            TRY_CONVERT(date, [RegistrationDate], 111)
        ) IS NULL;

        -- Nullify invalid phone format for optional column [Mobile]
        UPDATE #Citizens_Staging SET [Mobile] = NULL WHERE [Mobile] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST([Mobile] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST([Mobile] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        -- Post-validation date/type parsing on #Citizens_Staging
        UPDATE #Citizens_Staging
        SET [RegistrationDate] = COALESCE(TRY_CONVERT(date, [RegistrationDate], 120), TRY_CONVERT(date, [RegistrationDate], 103), TRY_CONVERT(date, [RegistrationDate], 101), TRY_CONVERT(date, [RegistrationDate], 111))
        WHERE 1=1;

        -- Copy fully transformed data from Staging to target Clean table (SCD Type 1)
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Citizens_Clean];
            INSERT INTO [dbo].[Citizens_Clean] ([CitizenID], [Email], [FullName], [Mobile], [RegistrationDate], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CitizenID] AS BIGINT), TRY_CAST([Email] AS NVARCHAR(255)), TRY_CAST([FullName] AS NVARCHAR(255)), [Mobile], TRY_CAST([RegistrationDate] AS DATE), etl_batch_id, GETDATE(), GETDATE() FROM #Citizens_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Citizens_Clean] WHERE [CitizenID] IN (SELECT [CitizenID] FROM #Citizens_Staging);
            INSERT INTO [dbo].[Citizens_Clean] ([CitizenID], [Email], [FullName], [Mobile], [RegistrationDate], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CitizenID] AS BIGINT), TRY_CAST([Email] AS NVARCHAR(255)), TRY_CAST([FullName] AS NVARCHAR(255)), [Mobile], TRY_CAST([RegistrationDate] AS DATE), etl_batch_id, GETDATE(), GETDATE() FROM #Citizens_Staging;
        END;

        DECLARE @staging_rows INT = (SELECT COUNT(*) FROM #Citizens_Staging);
        DECLARE @target_rows INT = (SELECT COUNT(*) FROM [dbo].[Citizens_Clean]);
        IF @staging_rows > 0 AND @target_rows = 0
        BEGIN
            RAISERROR('Post-load assertion failed: Target table is empty but staging had rows.', 16, 1);
        END;

        DECLARE @max_watermark DATETIME = COALESCE((SELECT MAX(TRY_CAST([RegistrationDate] AS DATETIME)) FROM #Citizens_Staging), @last_run, GETDATE());

        IF OBJECT_ID('tempdb..#Citizens_Staging') IS NOT NULL DROP TABLE #Citizens_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_clean_Citizens' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = @max_watermark
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, @max_watermark);
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- Master Orchestrator Stored Procedure
-- ============================================================
IF OBJECT_ID('dbo.etl_main', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_main;
GO
CREATE PROCEDURE dbo.etl_main
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_main', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC dbo.etl_clean_Accounts @load_type = @load_type, @last_run = @last_run;
        EXEC dbo.etl_clean_Citizens @load_type = @load_type, @last_run = @last_run;
        -- Update master process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_main' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE())
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE()));
        END

        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- Auto-Execute: Run ETL pipeline to populate Clean tables
-- ============================================================
PRINT 'Starting ETL pipeline execution...';
EXEC dbo.etl_main @load_type = 'FULL';
PRINT 'ETL pipeline execution complete.';
GO

GO

-- ============================================================
-- SQL QUALITY ASSESSMENT BADGE
-- Score: 85/100
-- Grade: B
-- Issues Detected (1):
--   - Email column detected but missing format check constraint (e.g. Email LIKE '%_@_%._%')
-- ============================================================
-- ETL SQL — Agent Dhara — plan_id=plan_1781601408
-- dialect=tsql — review before executing against production.

-- ============================================================
-- Create configuration, watermark and logging tables if not exists
-- ============================================================
IF OBJECT_ID('dbo.etl_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME NULL,
        status VARCHAR(20) NOT NULL,
        error_message VARCHAR(MAX) NULL
    );
END;
GO


IF OBJECT_ID('dbo.etl_invalid_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_invalid_values (
        column_name VARCHAR(256),
        invalid_value VARCHAR(256),
        PRIMARY KEY (column_name, invalid_value)
    );
END;
GO

IF OBJECT_ID('dbo.etl_rejects', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_rejects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        row_data VARCHAR(MAX) NOT NULL,
        error_reason VARCHAR(MAX) NOT NULL,
        rejected_at DATETIME DEFAULT GETDATE()
    );
END;
GO

IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_watermark (
        process_name VARCHAR(256) PRIMARY KEY,
        last_run_time DATETIME NOT NULL
    );
END;
GO



-- ============================================================
-- Initialize Clean Tables if not exists
-- ============================================================
IF OBJECT_ID('dbo.Accounts_Clean', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Accounts_Clean] (
    [AccountID] BIGINT NOT NULL,
    [CustomerID] BIGINT NULL,
    [Balance] NVARCHAR(MAX) NULL,
    [AccountType] NVARCHAR(MAX) NULL,
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT NULL,
    CONSTRAINT [PK_Accounts_Clean] PRIMARY KEY ([AccountID])
    );
END;
GO

IF OBJECT_ID('dbo.Citizens_Clean', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Citizens_Clean] (
    [CitizenID] BIGINT NOT NULL,
    [Mobile] NVARCHAR(MAX) NULL,
    [RegistrationDate] DATE NULL,
    [FullName] NVARCHAR(255) NULL,
    [Email] NVARCHAR(255) NULL,
    etl_created_at DATETIME DEFAULT GETDATE(),
    etl_updated_at DATETIME DEFAULT GETDATE(),
    etl_batch_id INT NULL,
    CONSTRAINT [PK_Citizens_Clean] PRIMARY KEY ([CitizenID])
    );
    CREATE NONCLUSTERED INDEX idx_Citizens_Clean_RegistrationDate ON [dbo].[Citizens_Clean]([RegistrationDate]);
END;
GO

-- === dataset: dbo.Accounts === 
IF OBJECT_ID('dbo.etl_transform_Accounts', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_transform_Accounts;
GO
CREATE PROCEDURE dbo.etl_transform_Accounts
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_transform_Accounts';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_transform_Accounts', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Accounts_Clean', 'U') IS NULL
        BEGIN
            CREATE TABLE [dbo].[Accounts_Transformed] (
                [AccountID] BIGINT NOT NULL,
        [CustomerID] BIGINT NULL,
        [Balance] NVARCHAR(MAX) NULL,
        [AccountType] NVARCHAR(MAX) NULL,
        etl_created_at DATETIME DEFAULT GETDATE(),
        etl_updated_at DATETIME DEFAULT GETDATE(),
        etl_batch_id INT NULL,
        CONSTRAINT [PK_Accounts_Clean] PRIMARY KEY ([AccountID])
            );
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Accounts_Transform_Staging') IS NOT NULL DROP TABLE #Accounts_Transform_Staging;
        CREATE TABLE #Accounts_Transform_Staging ([AccountID] NVARCHAR(MAX) NULL, [CustomerID] NVARCHAR(MAX) NULL, [Balance] NVARCHAR(MAX) NULL, [AccountType] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
            INSERT INTO #Accounts_Transform_Staging ([AccountID], [CustomerID], [Balance], [AccountType], etl_batch_id)
            SELECT [AccountID], [CustomerID], [Balance], [AccountType], @run_id FROM [dbo].[Accounts_Clean];

        -- Normalize empty strings and placeholders to NULL before validation
        UPDATE #Accounts_Transform_Staging
        SET [AccountID] = CASE WHEN LOWER(LTRIM(RTRIM([AccountID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [AccountID] END,
    [CustomerID] = CASE WHEN LOWER(LTRIM(RTRIM([CustomerID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [CustomerID] END,
    [Balance] = CASE WHEN LOWER(LTRIM(RTRIM([Balance]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Balance] END,
    [AccountType] = CASE WHEN LOWER(LTRIM(RTRIM([AccountType]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [AccountType] END;

        -- Quarantine rows where ID column [AccountID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_transform_Accounts', 'dbo.Accounts_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'AccountID is not numeric'
        FROM #Accounts_Transform_Staging r
        WHERE r.[AccountID] IS NOT NULL AND TRY_CAST(r.[AccountID] AS BIGINT) IS NULL;

        DELETE FROM #Accounts_Transform_Staging WHERE [AccountID] IS NOT NULL AND TRY_CAST([AccountID] AS BIGINT) IS NULL;

        -- Quarantine rows where ID column [CustomerID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_transform_Accounts', 'dbo.Accounts_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'CustomerID is not numeric'
        FROM #Accounts_Transform_Staging r
        WHERE r.[CustomerID] IS NOT NULL AND TRY_CAST(r.[CustomerID] AS BIGINT) IS NULL;

        DELETE FROM #Accounts_Transform_Staging WHERE [CustomerID] IS NOT NULL AND TRY_CAST([CustomerID] AS BIGINT) IS NULL;

        -- Copy fully transformed data from Staging to target Clean table (SCD Type 1)
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Accounts_Transformed];
            INSERT INTO [dbo].[Accounts_Transformed] ([AccountID], [AccountType], [Balance], [CustomerID], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([AccountID] AS BIGINT), [AccountType], [Balance], TRY_CAST([CustomerID] AS BIGINT), etl_batch_id, GETDATE(), GETDATE() FROM #Accounts_Transform_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Accounts_Transformed] WHERE [AccountID] IN (SELECT [AccountID] FROM #Accounts_Transform_Staging);
            INSERT INTO [dbo].[Accounts_Transformed] ([AccountID], [AccountType], [Balance], [CustomerID], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([AccountID] AS BIGINT), [AccountType], [Balance], TRY_CAST([CustomerID] AS BIGINT), etl_batch_id, GETDATE(), GETDATE() FROM #Accounts_Transform_Staging;
        END;

        DECLARE @staging_rows INT = (SELECT COUNT(*) FROM #Accounts_Transform_Staging);
        DECLARE @target_rows INT = (SELECT COUNT(*) FROM [dbo].[Accounts_Transformed]);
        IF @staging_rows > 0 AND @target_rows = 0
        BEGIN
            RAISERROR('Post-load assertion failed: Target table is empty but staging had rows.', 16, 1);
        END;

        IF OBJECT_ID('tempdb..#Accounts_Transform_Staging') IS NOT NULL DROP TABLE #Accounts_Transform_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_transform_Accounts' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- === dataset: dbo.Citizens === 
IF OBJECT_ID('dbo.etl_transform_Citizens', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_transform_Citizens;
GO
CREATE PROCEDURE dbo.etl_transform_Citizens
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_transform_Citizens';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_transform_Citizens', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Citizens_Clean', 'U') IS NULL
        BEGIN
            CREATE TABLE [dbo].[Citizens_Transformed] (
                [CitizenID] BIGINT NOT NULL,
        [Mobile] NVARCHAR(MAX) NULL,
        [RegistrationDate] DATE NULL,
        [FullName] NVARCHAR(255) NULL,
        [Email] NVARCHAR(255) NULL,
        etl_created_at DATETIME DEFAULT GETDATE(),
        etl_updated_at DATETIME DEFAULT GETDATE(),
        etl_batch_id INT NULL,
        CONSTRAINT [PK_Citizens_Clean] PRIMARY KEY ([CitizenID])
            );
            CREATE NONCLUSTERED INDEX idx_Citizens_Clean_RegistrationDate ON [dbo].[Citizens_Transformed]([RegistrationDate]);
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Citizens_Transform_Staging') IS NOT NULL DROP TABLE #Citizens_Transform_Staging;
        CREATE TABLE #Citizens_Transform_Staging ([CitizenID] NVARCHAR(MAX) NULL, [Mobile] NVARCHAR(MAX) NULL, [RegistrationDate] NVARCHAR(MAX) NULL, [FullName] NVARCHAR(MAX) NULL, [Email] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            INSERT INTO #Citizens_Transform_Staging ([CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], etl_batch_id)
            SELECT [CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], @run_id FROM [dbo].[Citizens_Clean];
        END
        ELSE
        BEGIN
            INSERT INTO #Citizens_Transform_Staging ([CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], etl_batch_id)
            SELECT [CitizenID], [Mobile], [RegistrationDate], [FullName], [Email], @run_id FROM [dbo].[Citizens_Clean] WHERE COALESCE(TRY_CONVERT(datetime, [RegistrationDate], 120), TRY_CONVERT(datetime, [RegistrationDate], 103), TRY_CONVERT(datetime, [RegistrationDate], 101), TRY_CONVERT(datetime, [RegistrationDate], 111)) > @last_run;
        END

        -- Normalize empty strings and placeholders to NULL before validation
        UPDATE #Citizens_Transform_Staging
        SET [CitizenID] = CASE WHEN LOWER(LTRIM(RTRIM([CitizenID]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [CitizenID] END,
    [Mobile] = CASE WHEN LOWER(LTRIM(RTRIM([Mobile]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Mobile] END,
    [RegistrationDate] = CASE WHEN LOWER(LTRIM(RTRIM([RegistrationDate]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [RegistrationDate] END,
    [FullName] = CASE WHEN LOWER(LTRIM(RTRIM([FullName]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [FullName] END,
    [Email] = CASE WHEN LOWER(LTRIM(RTRIM([Email]))) IN ('', 'n/a', 'na', 'null', 'unknown') THEN NULL ELSE [Email] END;

        -- Quarantine rows where ID column [CitizenID] is not numeric to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_transform_Citizens', 'dbo.Citizens_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'CitizenID is not numeric'
        FROM #Citizens_Transform_Staging r
        WHERE r.[CitizenID] IS NOT NULL AND TRY_CAST(r.[CitizenID] AS BIGINT) IS NULL;

        DELETE FROM #Citizens_Transform_Staging WHERE [CitizenID] IS NOT NULL AND TRY_CAST([CitizenID] AS BIGINT) IS NULL;

        -- Copy fully transformed data from Staging to target Clean table (SCD Type 1)
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Citizens_Transformed];
            INSERT INTO [dbo].[Citizens_Transformed] ([CitizenID], [Email], [FullName], [Mobile], [RegistrationDate], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CitizenID] AS BIGINT), TRY_CAST([Email] AS NVARCHAR(255)), TRY_CAST([FullName] AS NVARCHAR(255)), [Mobile], TRY_CAST([RegistrationDate] AS DATE), etl_batch_id, GETDATE(), GETDATE() FROM #Citizens_Transform_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Citizens_Transformed] WHERE [CitizenID] IN (SELECT [CitizenID] FROM #Citizens_Transform_Staging);
            INSERT INTO [dbo].[Citizens_Transformed] ([CitizenID], [Email], [FullName], [Mobile], [RegistrationDate], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CitizenID] AS BIGINT), TRY_CAST([Email] AS NVARCHAR(255)), TRY_CAST([FullName] AS NVARCHAR(255)), [Mobile], TRY_CAST([RegistrationDate] AS DATE), etl_batch_id, GETDATE(), GETDATE() FROM #Citizens_Transform_Staging;
        END;

        DECLARE @staging_rows INT = (SELECT COUNT(*) FROM #Citizens_Transform_Staging);
        DECLARE @target_rows INT = (SELECT COUNT(*) FROM [dbo].[Citizens_Transformed]);
        IF @staging_rows > 0 AND @target_rows = 0
        BEGIN
            RAISERROR('Post-load assertion failed: Target table is empty but staging had rows.', 16, 1);
        END;

        DECLARE @max_watermark DATETIME = COALESCE((SELECT MAX(TRY_CAST([RegistrationDate] AS DATETIME)) FROM #Citizens_Transform_Staging), @last_run, GETDATE());

        IF OBJECT_ID('tempdb..#Citizens_Transform_Staging') IS NOT NULL DROP TABLE #Citizens_Transform_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_transform_Citizens' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = @max_watermark
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, @max_watermark);
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- Master Orchestrator Stored Procedure
-- ============================================================
IF OBJECT_ID('dbo.etl_main', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_main;
GO
CREATE PROCEDURE dbo.etl_main
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_main', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC dbo.etl_transform_Accounts @load_type = @load_type, @last_run = @last_run;
        EXEC dbo.etl_transform_Citizens @load_type = @load_type, @last_run = @last_run;
        -- Update master process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark WITH (HOLDLOCK) AS target
            USING (SELECT 'etl_main' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE())
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%' OR process_name LIKE 'etl_transform_%'), GETDATE()));
        END

        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- Auto-Execute: Run ETL pipeline to populate Clean tables
-- ============================================================
PRINT 'Starting ETL pipeline execution...';
EXEC dbo.etl_main @load_type = 'FULL';
PRINT 'ETL pipeline execution complete.';
GO


-- ============================================================
-- Phase 2: Joined Views over Clean Tables
-- ============================================================
-- ── Staging / load order (connector manifest) ──
-- dbo.Accounts_Clean: -- Source table/view: dbo.Accounts_Clean
-- dbo.Accounts_Clean: SELECT * FROM dbo.Accounts_Clean;
-- dbo.Citizens_Clean: -- Source table/view: dbo.Citizens_Clean
-- dbo.Citizens_Clean: SELECT * FROM dbo.Citizens_Clean;
