/*
    EKK: EntityType ownership and test source-system instances.
    Target: SQL Server database EKK.
    Safe to execute repeatedly: existing codes are not duplicated or overwritten.

    IMPORTANT:
    - This script does not populate ekk.EntityType. The approved canonical EntityType.Code list
      (including the 39 documentation categories) is not present in the repository/database.
    - Consequently, EntityTypeOwner remains empty until the approved type codes are loaded and
      mapped. RegisterEntity will fail closed with ENTITY_TYPE_OWNER_NOT_CONFIGURED.
    - Test instance codes are intentionally NULL until the actual PDM/ERP test instance
      identifiers are confirmed. Set the two variables below before creating instances.
    - RЦ ownership is intentionally not seeded: the prior decision left MES vs ERP (ТОиР)
      unresolved; do not infer an owner.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @PdmTestInstanceCode nvarchar(128) = NULL; -- set to the agreed PDM test instance code
DECLARE @ErpTestInstanceCode nvarchar(128) = NULL; -- set to the agreed ERP test instance code

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'ekk.EntityTypeOwner', N'U') IS NULL
    BEGIN
        CREATE TABLE ekk.EntityTypeOwner
        (
            EntityTypeKey bigint NOT NULL,
            SourceSystemKey bigint NOT NULL,
            CreatedAt datetime2(7) NOT NULL
                CONSTRAINT DF_EntityTypeOwner_CreatedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_EntityTypeOwner PRIMARY KEY CLUSTERED (EntityTypeKey),
            CONSTRAINT FK_EntityTypeOwner_EntityType
                FOREIGN KEY (EntityTypeKey) REFERENCES ekk.EntityType(EntityTypeKey),
            CONSTRAINT FK_EntityTypeOwner_SourceSystem
                FOREIGN KEY (SourceSystemKey) REFERENCES ekk.SourceSystem(SourceSystemKey)
        );
    END;

    IF NOT EXISTS (SELECT 1 FROM ekk.SourceSystem WHERE Code = N'PDM')
        INSERT ekk.SourceSystem (SourceSystemId, Code, Name, Description, IsActive, CreatedAt, UpdatedAt)
        VALUES (NEWID(), N'PDM', N'Product Data Management', N'Владелец инженерных типов Entity.', 1, SYSUTCDATETIME(), SYSUTCDATETIME());

    IF NOT EXISTS (SELECT 1 FROM ekk.SourceSystem WHERE Code = N'ERP')
        INSERT ekk.SourceSystem (SourceSystemId, Code, Name, Description, IsActive, CreatedAt, UpdatedAt)
        VALUES (NEWID(), N'ERP', N'Enterprise Resource Planning', N'Владелец организационных, кадровых, коммерческих и административных типов Entity.', 1, SYSUTCDATETIME(), SYSUTCDATETIME());

    IF NOT EXISTS (SELECT 1 FROM ekk.SourceSystem WHERE Code = N'MES')
        INSERT ekk.SourceSystem (SourceSystemId, Code, Name, Description, IsActive, CreatedAt, UpdatedAt)
        VALUES (NEWID(), N'MES', N'Manufacturing Execution System', N'Владелец производственных типов Entity, закреплённых за MES.', 1, SYSUTCDATETIME(), SYSUTCDATETIME());

    IF NULLIF(LTRIM(RTRIM(@PdmTestInstanceCode)), N'') IS NOT NULL AND NOT EXISTS
    (
        SELECT 1
        FROM ekk.SourceSystemInstance AS i
        INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey = i.SourceSystemKey
        WHERE s.Code = N'PDM' AND i.Code = @PdmTestInstanceCode
    )
    INSERT ekk.SourceSystemInstance
        (SourceSystemInstanceId, SourceSystemKey, Code, Name, Description, EnvironmentCode, IsActive, CreatedAt, UpdatedAt)
    SELECT NEWID(), s.SourceSystemKey, @PdmTestInstanceCode, N'PDM test instance',
        N'Тестовый экземпляр PDM для интеграционного тестирования ЕКК.', N'Test', 1, SYSUTCDATETIME(), SYSUTCDATETIME()
    FROM ekk.SourceSystem AS s
    WHERE s.Code = N'PDM';

    IF NULLIF(LTRIM(RTRIM(@ErpTestInstanceCode)), N'') IS NOT NULL AND NOT EXISTS
    (
        SELECT 1
        FROM ekk.SourceSystemInstance AS i
        INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey = i.SourceSystemKey
        WHERE s.Code = N'ERP' AND i.Code = @ErpTestInstanceCode
    )
    INSERT ekk.SourceSystemInstance
        (SourceSystemInstanceId, SourceSystemKey, Code, Name, Description, EnvironmentCode, IsActive, CreatedAt, UpdatedAt)
    SELECT NEWID(), s.SourceSystemKey, @ErpTestInstanceCode, N'ERP test instance',
        N'Тестовый экземпляр ERP для интеграционного тестирования ЕКК.', N'Test', 1, SYSUTCDATETIME(), SYSUTCDATETIME()
    FROM ekk.SourceSystem AS s
    WHERE s.Code = N'ERP';

    /*
        Owner assignments are intentionally data-driven. Once the approved EntityType.Code
        catalogue is available, add rows to @EntityTypeOwnerSeed below.
        Example syntax only (do not use placeholder codes):
        INSERT @EntityTypeOwnerSeed (EntityTypeCode, SourceSystemCode)
        VALUES (N'<approved EntityType.Code>', N'PDM');
    */
    DECLARE @EntityTypeOwnerSeed TABLE
    (
        EntityTypeCode nvarchar(128) NOT NULL PRIMARY KEY,
        SourceSystemCode nvarchar(128) NOT NULL
    );

    IF EXISTS
    (
        SELECT 1
        FROM @EntityTypeOwnerSeed AS seed
        LEFT JOIN ekk.EntityType AS et ON et.Code = seed.EntityTypeCode
        LEFT JOIN ekk.SourceSystem AS ss ON ss.Code = seed.SourceSystemCode
        WHERE et.EntityTypeKey IS NULL OR ss.SourceSystemKey IS NULL
    )
        THROW 51002, 'EntityType owner seed contains an unknown EntityType.Code or SourceSystem.Code.', 1;

    INSERT ekk.EntityTypeOwner (EntityTypeKey, SourceSystemKey)
    SELECT et.EntityTypeKey, ss.SourceSystemKey
    FROM @EntityTypeOwnerSeed AS seed
    INNER JOIN ekk.EntityType AS et ON et.Code = seed.EntityTypeCode
    INNER JOIN ekk.SourceSystem AS ss ON ss.Code = seed.SourceSystemCode
    WHERE NOT EXISTS
    (
        SELECT 1 FROM ekk.EntityTypeOwner AS existing
        WHERE existing.EntityTypeKey = et.EntityTypeKey
    );

    IF EXISTS
    (
        SELECT 1
        FROM @EntityTypeOwnerSeed AS seed
        INNER JOIN ekk.EntityType AS et ON et.Code = seed.EntityTypeCode
        INNER JOIN ekk.SourceSystem AS ss ON ss.Code = seed.SourceSystemCode
        INNER JOIN ekk.EntityTypeOwner AS existing ON existing.EntityTypeKey = et.EntityTypeKey
        WHERE existing.SourceSystemKey <> ss.SourceSystemKey
    )
        THROW 51003, 'An EntityType already has a different owner. Ownership changes require an explicit reviewed migration.', 1;

    IF EXISTS
    (
        SELECT 1
        FROM ekk.SourceSystemInstance AS i
        INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey = i.SourceSystemKey
        WHERE (s.Code = N'PDM' AND @PdmTestInstanceCode IS NOT NULL AND i.Code = @PdmTestInstanceCode AND i.IsActive = 0)
           OR (s.Code = N'ERP' AND @ErpTestInstanceCode IS NOT NULL AND i.Code = @ErpTestInstanceCode AND i.IsActive = 0)
    )
        THROW 51001, 'A configured test instance exists but is inactive. Resolve it explicitly; this script does not reactivate records.', 1;

    COMMIT TRANSACTION;

    SELECT s.Code AS SystemCode, s.Name AS SystemName, i.Code AS InstanceCode,
           i.SourceSystemInstanceKey, i.EnvironmentCode, i.IsActive
    FROM ekk.SourceSystem AS s
    LEFT JOIN ekk.SourceSystemInstance AS i ON i.SourceSystemKey = s.SourceSystemKey
    WHERE s.Code IN (N'PDM', N'ERP', N'MES')
    ORDER BY s.Code, i.Code;

    SELECT COUNT_BIG(*) AS EntityTypeCount FROM ekk.EntityType;
    SELECT COUNT_BIG(*) AS EntityTypeOwnerCount FROM ekk.EntityTypeOwner;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
