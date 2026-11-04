using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
namespace Ekk.Host;

public sealed record RegisterEntityRequest(string EntityTypeCode, Guid IdempotencyKey, Guid? CorrelationId);
public sealed record RegisterSourceObjectRequest(Guid EntityId, string ExternalObjectType, string ExternalObjectId,
    string? ExternalObjectCode, string? ExternalObjectName, Guid IdempotencyKey, Guid? CorrelationId);
public sealed record RegistrationResult(bool Success, string ResponseCode, Guid? EntityId, Guid? SourceObjectId,
    string? RegistrationStatus, OperationError? Error);

public sealed class EntityRegistrationService
{
    private const int SchemaVersion = 1;
    private readonly SqlOptions _sql;
    private readonly EkkOptions _ekk;
    public EntityRegistrationService(IOptions<SqlOptions> sql, IOptions<EkkOptions> ekk)
    { _sql = sql.Value; _ekk = ekk.Value; }

    public Task<RegistrationResult> RegisterEntityAsync(RegisterEntityRequest request, CancellationToken ct) =>
        ExecuteIdempotentAsync("RegisterEntity", request.IdempotencyKey, request.CorrelationId,
            Hash("RegisterEntity", request.EntityTypeCode), async (c, t) =>
            {
                if (string.IsNullOrWhiteSpace(request.EntityTypeCode) || request.EntityTypeCode.Length > 128)
                    return Failure("INVALID_PAYLOAD", "EntityTypeCode обязателен (не более 128 символов).");
                const string typeSql = """
                    SELECT et.EntityTypeKey, owner.SourceSystemKey, instance.SourceSystemKey
                    FROM ekk.EntityType AS et WITH (UPDLOCK,HOLDLOCK)
                    LEFT JOIN ekk.EntityTypeOwner AS owner WITH (UPDLOCK,HOLDLOCK)
                        ON owner.EntityTypeKey = et.EntityTypeKey
                    INNER JOIN ekk.SourceSystemInstance AS instance
                        ON instance.SourceSystemInstanceKey = @InstanceKey
                       AND instance.IsActive = 1
                    WHERE et.Code = @Code AND et.IsActive = 1;
                    """;
                await using var typeCmd = new SqlCommand(typeSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
                typeCmd.Parameters.Add("@Code", SqlDbType.NVarChar, 128).Value = request.EntityTypeCode;
                typeCmd.Parameters.Add("@InstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
                long? entityTypeKey = null;
                long? ownerSystemKey = null;
                long? instanceSystemKey = null;
                await using (var reader = await typeCmd.ExecuteReaderAsync(ct))
                {
                    if (await reader.ReadAsync(ct))
                    {
                        entityTypeKey = reader.GetInt64(0);
                        ownerSystemKey = reader.IsDBNull(1) ? null : reader.GetInt64(1);
                        instanceSystemKey = reader.GetInt64(2);
                    }
                }
                if (entityTypeKey is null)
                    return Failure("ENTITY_TYPE_NOT_FOUND", "Активный EntityType с указанным Code не найден либо экземпляр-инициатор неактивен.");
                if (ownerSystemKey is null)
                    return Failure("ENTITY_TYPE_OWNER_NOT_CONFIGURED", "Для EntityType не назначена система-владелец.");
                if (ownerSystemKey != instanceSystemKey)
                    return Failure("FORBIDDEN_ENTITY_TYPE_OWNER", "Настроенный экземпляр системы не является владельцем указанного EntityType.");
                var entityId = Guid.CreateVersion7();
                const string insertSql = "INSERT INTO ekk.Entity (EntityId,EntityTypeKey,CreatedBySourceSystemInstanceKey,RegistrationStatus) OUTPUT INSERTED.EntityKey VALUES (@EntityId,@EntityTypeKey,@SourceSystemInstanceKey,'RegistrationPending');";
                await using var insert = new SqlCommand(insertSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
                insert.Parameters.Add("@EntityId", SqlDbType.UniqueIdentifier).Value = entityId;
                insert.Parameters.Add("@EntityTypeKey", SqlDbType.BigInt).Value = entityTypeKey.Value;
                insert.Parameters.Add("@SourceSystemInstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
                var entityKey = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
                await InsertOutboxAsync(c, t, entityKey, entityId, "EntityCreated", "entity.created",
                    new { EntityId = entityId, EntityTypeCode = request.EntityTypeCode, RegistrationStatus = "RegistrationPending" },
                    request.CorrelationId, ct);
                return new RegistrationResult(true, "CREATED", entityId, null, "RegistrationPending", null);
            }, ct);

    public Task<RegistrationResult> RegisterSourceObjectAsync(RegisterSourceObjectRequest request, CancellationToken ct) =>
        ExecuteIdempotentAsync("RegisterSourceObject", request.IdempotencyKey, request.CorrelationId,
            Hash("RegisterSourceObject", request.EntityId, request.ExternalObjectType, request.ExternalObjectId,
                request.ExternalObjectCode ?? "", request.ExternalObjectName ?? ""),
            async (c, t) =>
            {
                if (request.EntityId == Guid.Empty || string.IsNullOrWhiteSpace(request.ExternalObjectType) ||
                    request.ExternalObjectType.Length > 256 || string.IsNullOrWhiteSpace(request.ExternalObjectId) ||
                    request.ExternalObjectId.Length > 512 || request.ExternalObjectCode?.Length > 256 ||
                    request.ExternalObjectName?.Length > 800)
                    return Failure("INVALID_PAYLOAD", "Поля локального объекта не прошли проверку.");
                const string entitySql = "SELECT EntityKey,CreatedBySourceSystemInstanceKey FROM ekk.Entity WITH (UPDLOCK,HOLDLOCK) WHERE EntityId=@EntityId;";
                await using var entityCmd = new SqlCommand(entitySql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
                entityCmd.Parameters.Add("@EntityId", SqlDbType.UniqueIdentifier).Value = request.EntityId;
                long entityKey; long ownerKey;
                await using (var reader = await entityCmd.ExecuteReaderAsync(ct))
                {
                    if (!await reader.ReadAsync(ct)) return Failure("ENTITY_NOT_FOUND", "Entity не найдена.");
                    entityKey = reader.GetInt64(0); ownerKey = reader.GetInt64(1);
                }
                if (ownerKey != _ekk.SourceSystemInstanceKey)
                    return Failure("FORBIDDEN", "Только экземпляр-владелец может регистрировать локальный объект.");
                const string duplicateSql = "SELECT SourceObjectId,EntityKey FROM ekk.SourceObject WITH (UPDLOCK,HOLDLOCK) WHERE SourceSystemInstanceKey=@InstanceKey AND ExternalObjectType=@ObjectType AND ExternalObjectId=@ObjectId;";
                await using var duplicateCmd = new SqlCommand(duplicateSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
                duplicateCmd.Parameters.Add("@InstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
                duplicateCmd.Parameters.Add("@ObjectType", SqlDbType.NVarChar, 256).Value = request.ExternalObjectType;
                duplicateCmd.Parameters.Add("@ObjectId", SqlDbType.NVarChar, 512).Value = request.ExternalObjectId;
                await using (var reader = await duplicateCmd.ExecuteReaderAsync(ct))
                {
                    if (await reader.ReadAsync(ct))
                    {
                        var existingId = reader.GetGuid(0);
                        if (reader.GetInt64(1) != entityKey)
                            return Failure("SOURCE_OBJECT_CONFLICT", "Локальная идентичность уже связана с другой Entity.");
                        return new RegistrationResult(true, "ALREADY_REGISTERED", request.EntityId, existingId, null, null);
                    }
                }
                var sourceObjectId = Guid.CreateVersion7();
                const string sourceSql = "INSERT INTO ekk.SourceObject (SourceObjectId,EntityKey,SourceSystemInstanceKey,ExternalObjectType,ExternalObjectId,ExternalObjectCode,ExternalObjectName) VALUES (@SourceObjectId,@EntityKey,@InstanceKey,@ObjectType,@ObjectId,@ObjectCode,@ObjectName);";
                await using var sourceCmd = new SqlCommand(sourceSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
                sourceCmd.Parameters.Add("@SourceObjectId", SqlDbType.UniqueIdentifier).Value = sourceObjectId;
                sourceCmd.Parameters.Add("@EntityKey", SqlDbType.BigInt).Value = entityKey;
                sourceCmd.Parameters.Add("@InstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
                sourceCmd.Parameters.Add("@ObjectType", SqlDbType.NVarChar, 256).Value = request.ExternalObjectType;
                sourceCmd.Parameters.Add("@ObjectId", SqlDbType.NVarChar, 512).Value = request.ExternalObjectId;
                sourceCmd.Parameters.Add("@ObjectCode", SqlDbType.NVarChar, 256).Value = (object?)request.ExternalObjectCode ?? DBNull.Value;
                sourceCmd.Parameters.Add("@ObjectName", SqlDbType.NVarChar, 800).Value = (object?)request.ExternalObjectName ?? DBNull.Value;
                await sourceCmd.ExecuteNonQueryAsync(ct);
                await InsertOutboxAsync(c, t, entityKey, request.EntityId, "SourceObjectRegistered",
                    "entity.source-object-registered", new { EntityId = request.EntityId, SourceObjectId = sourceObjectId,
                        SourceSystemInstanceKey = _ekk.SourceSystemInstanceKey, ExternalObjectType = request.ExternalObjectType,
                        ExternalObjectId = request.ExternalObjectId, ExternalObjectCode = request.ExternalObjectCode,
                        ExternalObjectName = request.ExternalObjectName }, request.CorrelationId, ct);
                return new RegistrationResult(true, "REGISTERED", request.EntityId, sourceObjectId, null, null);
            }, ct);

    private async Task<RegistrationResult> ExecuteIdempotentAsync(string operation, Guid key, Guid? correlationId,
        byte[] requestHash, Func<SqlConnection, SqlTransaction, Task<RegistrationResult>> action, CancellationToken ct)
    {
        if (key == Guid.Empty || key.ToString("N")[12] != '7')
            return Failure("INVALID_IDEMPOTENCY_KEY", "IdempotencyKey должен быть UUIDv7.");
        if (_ekk.SourceSystemInstanceKey <= 0) throw new InvalidOperationException("Ekk:SourceSystemInstanceKey must be configured.");
        await using var connection = new SqlConnection(_sql.ConnectionString);
        await connection.OpenAsync(ct);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.Serializable, ct);
        try
        {
            const string findSql = "SELECT RequestHash,ResponsePayload FROM ekk.RequestIdempotency WITH (UPDLOCK,HOLDLOCK) WHERE SourceSystemInstanceKey=@InstanceKey AND IdempotencyKey=@IdempotencyKey;";
            await using var find = new SqlCommand(findSql, connection, transaction) { CommandTimeout = _sql.CommandTimeoutSeconds };
            find.Parameters.Add("@InstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
            find.Parameters.Add("@IdempotencyKey", SqlDbType.UniqueIdentifier).Value = key;
            byte[]? storedHash = null; RegistrationResult? replay = null;
            await using (var reader = await find.ExecuteReaderAsync(ct))
                if (await reader.ReadAsync(ct)) { storedHash = (byte[])reader["RequestHash"]; replay = JsonSerializer.Deserialize<RegistrationResult>((string)reader["ResponsePayload"]); }
            if (storedHash is not null)
            {
                await transaction.RollbackAsync(ct);
                if (!CryptographicOperations.FixedTimeEquals(storedHash, requestHash))
                    return Failure("IDEMPOTENCY_KEY_REUSE", "IdempotencyKey уже использован с другим содержимым.");
                return replay ?? Failure("IDEMPOTENCY_RESPONSE_INVALID", "Сохранённый ответ идемпотентности повреждён.");
            }
            var response = await action(connection, transaction);
            const string saveSql = "INSERT INTO ekk.RequestIdempotency (SourceSystemInstanceKey,IdempotencyKey,OperationCode,RequestHash,ResponseCode,ResponsePayload,EntityId,CorrelationId,CompletedAt) VALUES (@InstanceKey,@IdempotencyKey,@OperationCode,@RequestHash,@ResponseCode,@ResponsePayload,@EntityId,@CorrelationId,SYSUTCDATETIME());";
            await using var save = new SqlCommand(saveSql, connection, transaction) { CommandTimeout = _sql.CommandTimeoutSeconds };
            save.Parameters.Add("@InstanceKey", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
            save.Parameters.Add("@IdempotencyKey", SqlDbType.UniqueIdentifier).Value = key;
            save.Parameters.Add("@OperationCode", SqlDbType.NVarChar, 128).Value = operation;
            save.Parameters.Add("@RequestHash", SqlDbType.Binary, 32).Value = requestHash;
            save.Parameters.Add("@ResponseCode", SqlDbType.NVarChar, 128).Value = response.ResponseCode;
            save.Parameters.Add("@ResponsePayload", SqlDbType.NVarChar, -1).Value = JsonSerializer.Serialize(response);
            save.Parameters.Add("@EntityId", SqlDbType.UniqueIdentifier).Value = (object?)response.EntityId ?? DBNull.Value;
            save.Parameters.Add("@CorrelationId", SqlDbType.UniqueIdentifier).Value = (object?)correlationId ?? Guid.Empty;
            await save.ExecuteNonQueryAsync(ct);
            await transaction.CommitAsync(ct);
            return response;
        }
        catch { try { await transaction.RollbackAsync(CancellationToken.None); } catch { } throw; }
    }

    private async Task InsertOutboxAsync(SqlConnection c, SqlTransaction t, long entityKey, Guid entityId,
        string eventType, string routingKey, object payload, Guid? correlationId, CancellationToken ct)
    {
        const string sequenceSql = "UPDATE ekk.Entity SET LastEventSequence=LastEventSequence+1 OUTPUT INSERTED.LastEventSequence WHERE EntityKey=@EntityKey;";
        await using var sequenceCmd = new SqlCommand(sequenceSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        sequenceCmd.Parameters.Add("@EntityKey", SqlDbType.BigInt).Value = entityKey;
        var value = await sequenceCmd.ExecuteScalarAsync(ct);
        if (value is null || value == DBNull.Value) throw new InvalidOperationException("Entity sequence could not be incremented.");
        var messageId = Guid.CreateVersion7();
        var envelope = new { MessageId = messageId, CorrelationId = correlationId, MessageType = eventType,
            SchemaVersion, CreatedAt = DateTime.UtcNow, Payload = payload };
        const string insertSql = "INSERT INTO ekk.OutboxMessage (MessageId,EventType,SchemaVersion,EntityId,EntitySequence,RoutingKey,Payload) VALUES (@MessageId,@EventType,@SchemaVersion,@EntityId,@Sequence,@RoutingKey,@Payload);";
        await using var insert = new SqlCommand(insertSql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        insert.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId;
        insert.Parameters.Add("@EventType", SqlDbType.NVarChar, 256).Value = eventType;
        insert.Parameters.Add("@SchemaVersion", SqlDbType.Int).Value = SchemaVersion;
        insert.Parameters.Add("@EntityId", SqlDbType.UniqueIdentifier).Value = entityId;
        insert.Parameters.Add("@Sequence", SqlDbType.BigInt).Value = Convert.ToInt64(value);
        insert.Parameters.Add("@RoutingKey", SqlDbType.NVarChar, 512).Value = routingKey;
        insert.Parameters.Add("@Payload", SqlDbType.NVarChar, -1).Value = JsonSerializer.Serialize(envelope);
        await insert.ExecuteNonQueryAsync(ct);
    }
    private static byte[] Hash(params object[] values) => SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n", values.Select(x => x?.ToString() ?? ""))));
    private static RegistrationResult Failure(string code, string message) => new(false, code, null, null, null, new OperationError(code, message, false, false));
}
