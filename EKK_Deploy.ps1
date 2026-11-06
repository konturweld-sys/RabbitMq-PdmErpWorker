#requires -Version 5.1
[CmdletBinding()]
param(
    [string]$InstallPath = "C:\EKK"
)

$ErrorActionPreference = "Stop"

function ConvertFrom-SecureStringToPlainText {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Value)

    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Test-EkkSqlSchema {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][long]$InstanceKey,
        [Parameter(Mandatory = $true)][string]$SystemCode,
        [Parameter(Mandatory = $true)][string]$InstanceCode
    )

    $connection = [System.Data.SqlClient.SqlConnection]::new($ConnectionString)
    try {
        $connection.Open()

        $databaseCommand = $connection.CreateCommand()
        $databaseCommand.CommandText = "SELECT DB_NAME();"
        $databaseName = [string]$databaseCommand.ExecuteScalar()
        if ($databaseName -ne "EKK") {
            throw "SQL connection points to database '$databaseName', expected 'EKK'."
        }

        $schemaCommand = $connection.CreateCommand()
        $schemaCommand.CommandText = @"
DECLARE @RequiredColumns TABLE
(
    TableName sysname NOT NULL,
    ColumnName sysname NOT NULL
);

INSERT INTO @RequiredColumns (TableName, ColumnName)
VALUES
    (N'Entity', N'EntityKey'),
    (N'Entity', N'EntityId'),
    (N'Entity', N'EntityTypeKey'),
    (N'Entity', N'CreatedBySourceSystemInstanceKey'),
    (N'Entity', N'RegistrationStatus'),
    (N'Entity', N'LastEventSequence'),
    (N'EntityType', N'EntityTypeKey'),
    (N'EntityType', N'Code'),
    (N'EntityType', N'IsActive'),
    (N'EntityTypeOwner', N'EntityTypeKey'),
    (N'EntityTypeOwner', N'SourceSystemKey'),
    (N'SourceSystem', N'SourceSystemKey'),
    (N'SourceSystem', N'Code'),
    (N'SourceSystem', N'IsActive'),
    (N'SourceSystemInstance', N'SourceSystemInstanceKey'),
    (N'SourceSystemInstance', N'SourceSystemKey'),
    (N'SourceSystemInstance', N'Code'),
    (N'SourceSystemInstance', N'IsActive'),
    (N'SourceObject', N'SourceObjectId'),
    (N'SourceObject', N'EntityKey'),
    (N'SourceObject', N'SourceSystemInstanceKey'),
    (N'SourceObject', N'ExternalObjectType'),
    (N'SourceObject', N'ExternalObjectId'),
    (N'SourceObject', N'ExternalObjectCode'),
    (N'SourceObject', N'ExternalObjectName'),
    (N'RequestIdempotency', N'SourceSystemInstanceKey'),
    (N'RequestIdempotency', N'IdempotencyKey'),
    (N'RequestIdempotency', N'OperationCode'),
    (N'RequestIdempotency', N'RequestHash'),
    (N'RequestIdempotency', N'ResponseCode'),
    (N'RequestIdempotency', N'ResponsePayload'),
    (N'RequestIdempotency', N'EntityId'),
    (N'RequestIdempotency', N'CorrelationId'),
    (N'RequestIdempotency', N'CompletedAt'),
    (N'EntityRegistrationHistory', N'RegistrationHistoryKey'),
    (N'EntityRegistrationHistory', N'EntityKey'),
    (N'EntityRegistrationHistory', N'RegistrationStatus'),
    (N'EntityRegistrationHistory', N'ChangedAt'),
    (N'EntityRegistrationHistory', N'ErrorCode'),
    (N'EntityRegistrationHistory', N'ErrorMessage'),
    (N'OutboxMessage', N'OutboxMessageKey'),
    (N'OutboxMessage', N'MessageId'),
    (N'OutboxMessage', N'EventType'),
    (N'OutboxMessage', N'SchemaVersion'),
    (N'OutboxMessage', N'EntityId'),
    (N'OutboxMessage', N'EntitySequence'),
    (N'OutboxMessage', N'RoutingKey'),
    (N'OutboxMessage', N'Payload'),
    (N'OutboxMessage', N'CreatedAt'),
    (N'OutboxMessage', N'PublishedAt'),
    (N'OutboxMessage', N'Status'),
    (N'OutboxMessage', N'AttemptCount'),
    (N'OutboxMessage', N'NextAttemptAt'),
    (N'OutboxMessage', N'LastError');

SELECT COUNT_BIG(1)
FROM @RequiredColumns AS required
WHERE NOT EXISTS
(
    SELECT 1
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
    INNER JOIN sys.columns AS c ON c.object_id = t.object_id
    WHERE s.name = N'ekk'
      AND t.name = required.TableName
      AND c.name = required.ColumnName
);
"@
        $missingColumns = [long]$schemaCommand.ExecuteScalar()
        if ($missingColumns -ne 0) {
            throw "EKK schema is incompatible: $missingColumns required table columns are missing."
        }

        $instanceCommand = $connection.CreateCommand()
        $instanceCommand.CommandText = @"
SELECT COUNT_BIG(1)
FROM ekk.SourceSystemInstance AS i
INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey = i.SourceSystemKey
WHERE i.SourceSystemInstanceKey = @InstanceKey
  AND i.Code = @InstanceCode
  AND i.IsActive = 1
  AND s.Code = @SystemCode
  AND s.IsActive = 1;
"@
        [void]$instanceCommand.Parameters.Add("@InstanceKey", [System.Data.SqlDbType]::BigInt)
        $instanceCommand.Parameters["@InstanceKey"].Value = $InstanceKey
        [void]$instanceCommand.Parameters.Add("@InstanceCode", [System.Data.SqlDbType]::NVarChar, 128)
        $instanceCommand.Parameters["@InstanceCode"].Value = $InstanceCode
        [void]$instanceCommand.Parameters.Add("@SystemCode", [System.Data.SqlDbType]::NVarChar, 128)
        $instanceCommand.Parameters["@SystemCode"].Value = $SystemCode

        if ([long]$instanceCommand.ExecuteScalar() -ne 1) {
            throw "Configured source system instance is missing, inactive, or does not match SystemCode/InstanceCode."
        }

        $ownerCommand = $connection.CreateCommand()
        $ownerCommand.CommandText = @"
SELECT COUNT_BIG(1)
FROM ekk.EntityType AS et
INNER JOIN ekk.EntityTypeOwner AS owner ON owner.EntityTypeKey = et.EntityTypeKey
INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey = owner.SourceSystemKey
WHERE et.Code = N'PART'
  AND et.IsActive = 1
  AND s.Code = @SystemCode
  AND s.IsActive = 1;
"@
        [void]$ownerCommand.Parameters.Add("@SystemCode", [System.Data.SqlDbType]::NVarChar, 128)
        $ownerCommand.Parameters["@SystemCode"].Value = $SystemCode

        if ([long]$ownerCommand.ExecuteScalar() -ne 1) {
            throw "Active PART EntityType is not configured as owned by source system '$SystemCode'."
        }

        Write-Host "SQL preflight passed: database EKK, required schema, active $SystemCode/$InstanceCode instance, PART owner."
    }
    finally {
        $connection.Dispose()
    }
}

$sourceExe = Join-Path $PSScriptRoot "Ekk.Host.exe"
if (-not (Test-Path -LiteralPath $sourceExe -PathType Leaf)) {
    $candidate = Get-ChildItem -LiteralPath $PSScriptRoot -Filter "Ekk.Host.exe" -File -Recurse |
        Select-Object -First 1
    if ($null -eq $candidate) {
        throw "Ekk.Host.exe was not found. Extract the complete EKK-win-x64 artifact first."
    }
    $sourceExe = $candidate.FullName
}

$sourceDirectory = Split-Path -Parent $sourceExe
if ([string]::IsNullOrWhiteSpace($InstallPath)) {
    throw "InstallPath is required."
}

New-Item -ItemType Directory -Path $InstallPath -Force | Out-Null
Copy-Item -Path (Join-Path $sourceDirectory "*") -Destination $InstallPath -Recurse -Force

$exePath = Join-Path $InstallPath "Ekk.Host.exe"
if (-not (Test-Path -LiteralPath $exePath -PathType Leaf)) {
    throw "Ekk.Host.exe was not found after copying to $InstallPath."
}

$appSettingsPath = Join-Path $InstallPath "appsettings.json"
if (-not (Test-Path -LiteralPath $appSettingsPath -PathType Leaf)) {
    throw "appsettings.json was not found in $InstallPath."
}
$appSettings = Get-Content -LiteralPath $appSettingsPath -Raw | ConvertFrom-Json
$rabbitHost = [string]$appSettings.RabbitMq.HostName
$rabbitPort = [int]$appSettings.RabbitMq.Port

Write-Host ""
Write-Host "Configuring the EKK PDM_TEST instance."
Write-Host "Default SQL Server: 192.168.50.173, database EKK."
Write-Host ("RabbitMQ endpoint from appsettings.json: {0}:{1}." -f $rabbitHost, $rabbitPort)
Write-Host ""

$sqlSecure = Read-Host "SQL Server connection string (hidden input)" -AsSecureString
if ($sqlSecure.Length -eq 0) {
    throw "SQL Server connection string is required."
}
$sqlConnectionString = ConvertFrom-SecureStringToPlainText -Value $sqlSecure

$rabbitUser = Read-Host "RabbitMQ user"
if ([string]::IsNullOrWhiteSpace($rabbitUser)) {
    throw "RabbitMQ user is required."
}
$rabbitPasswordSecure = Read-Host "RabbitMQ password" -AsSecureString
if ($rabbitPasswordSecure.Length -eq 0) {
    throw "RabbitMQ password is required."
}
$rabbitPassword = ConvertFrom-SecureStringToPlainText -Value $rabbitPasswordSecure

$instanceKeyText = Read-Host "SourceSystemInstanceKey [1]"
if ([string]::IsNullOrWhiteSpace($instanceKeyText)) { $instanceKeyText = "1" }
$instanceKey = 0L
if (-not [long]::TryParse($instanceKeyText, [ref]$instanceKey) -or $instanceKey -le 0) {
    throw "SourceSystemInstanceKey must be a positive integer."
}

$systemCode = Read-Host "SystemCode [PDM]"
if ([string]::IsNullOrWhiteSpace($systemCode)) { $systemCode = "PDM" }

$instanceCode = Read-Host "InstanceCode [PDM_TEST]"
if ([string]::IsNullOrWhiteSpace($instanceCode)) { $instanceCode = "PDM_TEST" }

Test-EkkSqlSchema -ConnectionString $sqlConnectionString -InstanceKey $instanceKey -SystemCode $systemCode -InstanceCode $instanceCode

$tcpTest = Test-NetConnection -ComputerName $rabbitHost -Port $rabbitPort -WarningAction SilentlyContinue
if (-not $tcpTest.TcpTestSucceeded) {
    throw ("RabbitMQ TCP endpoint {0}:{1} is not reachable." -f $rabbitHost, $rabbitPort)
}
Write-Host ("RabbitMQ TCP preflight passed: {0}:{1}." -f $rabbitHost, $rabbitPort)

$env:Sql__ConnectionString = $sqlConnectionString
$env:RabbitMq__UserName = $rabbitUser
$env:RabbitMq__Password = $rabbitPassword
$env:Ekk__SourceSystemInstanceKey = [string]$instanceKey
$env:Ekk__SystemCode = $systemCode
$env:Ekk__InstanceCode = $instanceCode

try {
    Write-Host ""
    Write-Host "Starting EKK from $InstallPath"
    Write-Host "Stop with Ctrl+C."
    Write-Host ""

    Push-Location $InstallPath
    try {
        & $exePath
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($null -ne $exitCode -and $exitCode -ne 0) {
        throw "EKK exited with code $exitCode."
    }
}
finally {
    Remove-Item Env:\Sql__ConnectionString -ErrorAction SilentlyContinue
    Remove-Item Env:\RabbitMq__UserName -ErrorAction SilentlyContinue
    Remove-Item Env:\RabbitMq__Password -ErrorAction SilentlyContinue
    Remove-Item Env:\Ekk__SourceSystemInstanceKey -ErrorAction SilentlyContinue
    Remove-Item Env:\Ekk__SystemCode -ErrorAction SilentlyContinue
    Remove-Item Env:\Ekk__InstanceCode -ErrorAction SilentlyContinue
    $sqlConnectionString = $null
    $rabbitPassword = $null
}
