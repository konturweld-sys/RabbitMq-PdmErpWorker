using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ekk.Host;

public sealed record CompletionRequest(
    Guid EntityId, string Result, Guid IdempotencyKey,
    string? ErrorCode, string? ErrorMessage, Guid? CorrelationId);

public sealed record CompletionPayload(Guid EntityId, string RegistrationStatus, DateTime ChangedAt);
public sealed record OperationError(string Code, string Message, bool Retryable, bool AdminActionRequired);
public sealed record OperationResponse(bool Success, string ResponseCode, CompletionPayload? ResponsePayload, OperationError? Error);

public sealed class RegistrationCompletionService
{
    private const string OperationCode = "CompleteEntityRegistration";
    private readonly SqlOptions _sql;
    private readonly EkkOptions _ekk;
    private readonly ILogger<RegistrationCompletionService> _logger;

    public RegistrationCompletionService(IOptions<SqlOptions> sql, IOptions<EkkOptions> ekk, ILogger<RegistrationCompletionService> logger)
    {
        _sql = sql.Value;
        _ekk = ekk.Value;
        _logger = logger;
    }

    public async Task<OperationResponse> CompleteAsync(CompletionRequest request, CancellationToken cancellationToken)
    {
        var validation = Validate(request);
        if (validation is not null) return Failure(validation, "Некорректные данные завершения регистрации.");

        var requestHash = CalculateRequestHash(request);
        await using var connection = new SqlConnection(_sql.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(
            System.Data.IsolationLevel.Serializable, cancellationToken);

        try
        {
            var duplicate = await FindIdempotencyAsync(connection, transaction, request.IdempotencyKey, cancellationToken);
            if (duplicate is not null)
            {
                await transaction.RollbackAsync(cancellationToken);
                if (!CryptographicOperations.FixedTimeEquals(duplicate.RequestHash, requestHash))
                    return Failure("IDEMPOTENCY_KEY_REUSE", "IdempotencyKey уже использован с другим содержимым.");
                return JsonSerializer.Deserialize<OperationResponse>(duplicate.ResponsePayload)
                    ?? Failure("INTERNAL_ERROR", "Не удалось прочитать сохранённый ответ.", false, true);
            }

            var entity = await LockEntityAsync(connection, transaction, request.EntityId, cancellationToken);
            if (entity is null)
                return await SaveBusinessFailureAsync(connection, transaction, request, requestHash, null,
                    "ENTITY_NOT_FOUND", "Entity не найдена.", cancellationToken);

            if (entity.CreatedBySourceSystemInstanceKey != _ekk.SourceSystemInstanceKey)
                return await SaveBusinessFailureAsync(connection, transaction, request, requestHash, request.EntityId,
                    "FORBIDDEN", "Отправитель не является системой-владельцем Entity.", cancellationToken);

            OperationResponse response;
            if (entity.RegistrationStatus == "RegistrationPending")
            {
                var changedAt = DateTime.UtcNow;
                await UpdateEntityAsync(connection, transaction, request, cancellationToken);
                await InsertHistoryAsync(connection, transaction, entity.EntityKey, request, changedAt, cancellationToken);
                response = Success("COMPLETED", request, changedAt);
            }
            else if (entity.RegistrationStatus == request.Result)
            {
                var previous = await ReadLatestHistoryAsync(connection, transaction, entity.EntityKey, cancellationToken);
                if (previous is null ||
                    !string.Equals(previous.ErrorCode, request.ErrorCode, StringComparison.Ordinal) ||
                    !string.Equals(previous.ErrorMessage, request.ErrorMessage, StringComparison.Ordinal))
                    return await SaveBusinessFailureAsync(connection, transaction, request, requestHash, request.EntityId,
                        "REGISTRATION_STATE_CONFLICT", "Конечный статус уже установлен с другим результатом.", cancellationToken);

                response = Success("ALREADY_COMPLETED", request, previous.ChangedAt);
            }
            else
            {
                return await SaveBusinessFailureAsync(connection, transaction, request, requestHash, request.EntityId,
                    "REGISTRATION_STATE_CONFLICT", "Невозможно изменить уже установленный конечный статус.", cancellationToken);
            }

            var responseJson = JsonSerializer.Serialize(response);
            await SaveIdempotencyAsync(connection, transaction, request, requestHash,
                response.ResponseCode, responseJson, request.EntityId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return response;
        }
        catch (SqlException ex)
        {
            await SafeRollbackAsync(transaction);
            _logger.LogError(ex, "SQL failure in {OperationCode} for EntityId {EntityId}; request must be retried",
                OperationCode, request.EntityId);
            throw;
        }
        catch (Exception ex)
        {
            await SafeRollbackAsync(transaction);
            _logger.LogError(ex, "Unexpected failure in {OperationCode} for EntityId {EntityId}", OperationCode, request.EntityId);
            return Failure("INTERNAL_ERROR", "Внутренняя ошибка ЕКК.", false, true);
        }
    }

    private async Task<OperationResponse> SaveBusinessFailureAsync(
        SqlConnection c, SqlTransaction t, CompletionRequest r, byte[] hash, Guid? entityId,
        string code, string message, CancellationToken ct)
    {
        var response = Failure(code, message);
        await SaveIdempotencyAsync(c, t, r, hash, code, JsonSerializer.Serialize(response), entityId, ct);
        await t.CommitAsync(ct);
        return response;
    }

    private static string? Validate(CompletionRequest r)
    {
        if (r.EntityId == Guid.Empty || r.IdempotencyKey == Guid.Empty || r.IdempotencyKey.ToString("N")[12] != '7')
            return "INVALID_PAYLOAD";
        if (r.Result is not ("Registered" or "RegistrationError")) return "INVALID_PAYLOAD";
        if (r.Result == "Registered" && (r.ErrorCode is not null || r.ErrorMessage is not null))
            return "INVALID_PAYLOAD";
        if (r.Result == "RegistrationError" && (string.IsNullOrWhiteSpace(r.ErrorCode) || r.ErrorCode.Length > 64))
            return "INVALID_PAYLOAD";
        if (r.ErrorMessage?.Length > 2000) return "INVALID_PAYLOAD";
        return null;
    }

    private static byte[] CalculateRequestHash(CompletionRequest r)
    {
        var canonical = string.Join("\n", OperationCode, r.EntityId.ToString("D").ToLowerInvariant(),
            r.Result, r.ErrorCode ?? "", r.ErrorMessage ?? "");
        return SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
    }

    private async Task<IdempotencyRecord?> FindIdempotencyAsync(SqlConnection c, SqlTransaction t, Guid key, CancellationToken ct)
    {
        const string sql = """
            SELECT RequestHash, ResponsePayload
            FROM ekk.RequestIdempotency WITH (UPDLOCK, HOLDLOCK)
            WHERE SourceSystemInstanceKey = @SourceSystemInstanceKey AND IdempotencyKey = @IdempotencyKey;
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@SourceSystemInstanceKey", _ekk.SourceSystemInstanceKey);
        cmd.Parameters.AddWithValue("@IdempotencyKey", key);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new((byte[])reader["RequestHash"], (string)reader["ResponsePayload"]) : null;
    }

    private async Task<EntityRecord?> LockEntityAsync(SqlConnection c, SqlTransaction t, Guid entityId, CancellationToken ct)
    {
        const string sql = """
            SELECT EntityKey, CreatedBySourceSystemInstanceKey, RegistrationStatus
            FROM ekk.Entity WITH (UPDLOCK, HOLDLOCK) WHERE EntityId = @EntityId;
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@EntityId", entityId);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new(reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2)) : null;
    }

    private async Task UpdateEntityAsync(SqlConnection c, SqlTransaction t, CompletionRequest r, CancellationToken ct)
    {
        const string sql = """
            UPDATE ekk.Entity SET RegistrationStatus = @RegistrationStatus
            WHERE EntityId = @EntityId AND RegistrationStatus = 'RegistrationPending';
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@RegistrationStatus", r.Result);
        cmd.Parameters.AddWithValue("@EntityId", r.EntityId);
        if (await cmd.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Entity registration state changed concurrently.");
    }

    private async Task InsertHistoryAsync(SqlConnection c, SqlTransaction t, long entityKey, CompletionRequest r, DateTime changedAt, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO ekk.EntityRegistrationHistory (EntityKey, RegistrationStatus, ChangedAt, ErrorCode, ErrorMessage)
            VALUES (@EntityKey, @RegistrationStatus, @ChangedAt, @ErrorCode, @ErrorMessage);
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@EntityKey", entityKey);
        cmd.Parameters.AddWithValue("@RegistrationStatus", r.Result);
        cmd.Parameters.AddWithValue("@ChangedAt", changedAt);
        cmd.Parameters.Add("@ErrorCode", System.Data.SqlDbType.NVarChar, 64).Value = (object?)r.ErrorCode ?? DBNull.Value;
        cmd.Parameters.Add("@ErrorMessage", System.Data.SqlDbType.NVarChar, 2000).Value = (object?)r.ErrorMessage ?? DBNull.Value;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<HistoryRecord?> ReadLatestHistoryAsync(SqlConnection c, SqlTransaction t, long entityKey, CancellationToken ct)
    {
        const string sql = """
            SELECT TOP (1) ChangedAt, ErrorCode, ErrorMessage
            FROM ekk.EntityRegistrationHistory
            WHERE EntityKey = @EntityKey ORDER BY ChangedAt DESC, RegistrationHistoryKey DESC;
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@EntityKey", entityKey);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new(reader.GetDateTime(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2)) : null;
    }

    private async Task SaveIdempotencyAsync(SqlConnection c, SqlTransaction t, CompletionRequest r,
        byte[] hash, string responseCode, string responsePayload, Guid? entityId, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO ekk.RequestIdempotency
                (SourceSystemInstanceKey, IdempotencyKey, OperationCode, RequestHash,
                 ResponseCode, ResponsePayload, EntityId, CorrelationId, CompletedAt)
            VALUES
                (@SourceSystemInstanceKey, @IdempotencyKey, @OperationCode, @RequestHash,
                 @ResponseCode, @ResponsePayload, @EntityId, @CorrelationId, SYSUTCDATETIME());
            """;
        await using var cmd = new SqlCommand(sql, c, t) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.AddWithValue("@SourceSystemInstanceKey", _ekk.SourceSystemInstanceKey);
        cmd.Parameters.AddWithValue("@IdempotencyKey", r.IdempotencyKey);
        cmd.Parameters.AddWithValue("@OperationCode", OperationCode);
        cmd.Parameters.Add("@RequestHash", System.Data.SqlDbType.Binary, 32).Value = hash;
        cmd.Parameters.AddWithValue("@ResponseCode", responseCode);
        cmd.Parameters.Add("@ResponsePayload", System.Data.SqlDbType.NVarChar, -1).Value = responsePayload;
        cmd.Parameters.Add("@EntityId", System.Data.SqlDbType.UniqueIdentifier).Value = (object?)entityId ?? DBNull.Value;
        cmd.Parameters.AddWithValue("@CorrelationId", (object?)r.CorrelationId ?? Guid.Empty);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static OperationResponse Success(string code, CompletionRequest r, DateTime changedAt) =>
        new(true, code, new(r.EntityId, r.Result, changedAt), null);
    private static OperationResponse Failure(string code, string message, bool retryable = false, bool admin = false) =>
        new(false, code, null, new(code, message, retryable, admin));
    private static async Task SafeRollbackAsync(SqlTransaction t) { try { await t.RollbackAsync(CancellationToken.None); } catch { } }

    private sealed record EntityRecord(long EntityKey, long CreatedBySourceSystemInstanceKey, string RegistrationStatus);
    private sealed record IdempotencyRecord(byte[] RequestHash, string ResponsePayload);
    private sealed record HistoryRecord(DateTime ChangedAt, string? ErrorCode, string? ErrorMessage);
}
