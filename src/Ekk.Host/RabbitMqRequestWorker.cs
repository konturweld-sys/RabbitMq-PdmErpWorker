using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
namespace Ekk.Host;

public sealed class RabbitMqRequestWorker : BackgroundService
{
    private readonly SqlOptions _sql; private readonly RabbitOptions _rabbit; private readonly EkkOptions _ekk;
    private readonly RegistrationCompletionService _completion; private readonly EntityRegistrationService _registration;
    private readonly ILogger<RabbitMqRequestWorker> _logger;
    public RabbitMqRequestWorker(IOptions<SqlOptions> sql, IOptions<RabbitOptions> rabbit, IOptions<EkkOptions> ekk,
        RegistrationCompletionService completion, EntityRegistrationService registration, ILogger<RabbitMqRequestWorker> logger)
    { _sql = sql.Value; _rabbit = rabbit.Value; _ekk = ekk.Value; _completion = completion; _registration = registration; _logger = logger; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ValidateConfiguration();
        await ValidateSourceSystemInstanceAsync(stoppingToken);
        var factory = new ConnectionFactory { HostName = _rabbit.HostName, Port = _rabbit.Port, VirtualHost = _rabbit.VirtualHost,
            UserName = _rabbit.UserName, Password = _rabbit.Password, AutomaticRecoveryEnabled = true, TopologyRecoveryEnabled = true,
            ClientProvidedName = $"EKK-Requests/{_ekk.SystemCode}/{_ekk.InstanceCode}" };
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var connection = factory.CreateConnection(); using var channel = connection.CreateModel();
                channel.BasicQos(0, 1, false);
                channel.ConfirmSelect();
                _logger.LogInformation("Request worker started; queue={Queue}", _rabbit.RequestQueue);
                while (!stoppingToken.IsCancellationRequested && connection.IsOpen && channel.IsOpen)
                {
                    var delivery = channel.BasicGet(_rabbit.RequestQueue, false);
                    if (delivery is null) { await Task.Delay(_rabbit.PollIntervalMilliseconds, stoppingToken); continue; }
                    try
                    {
                        var response = await ProcessAsync(delivery.Body.ToArray(), stoppingToken);
                        PublishResponse(channel, delivery, response);
                        channel.BasicAck(delivery.DeliveryTag, false);
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogWarning(ex, "Malformed request JSON.");
                        PublishResponse(channel, delivery, Failure("INVALID_MESSAGE", "Некорректный JSON или обязательные поля отсутствуют."));
                        channel.BasicAck(delivery.DeliveryTag, false);
                    }
                    catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogError(ex, "Request processing or response publishing failed; requeue.");
                        channel.BasicNack(delivery.DeliveryTag, false, true);
                        await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _rabbit.RetryDelaySeconds)), stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { _logger.LogError(ex, "RabbitMQ request loop failed; reconnecting."); try { await Task.Delay(5000, stoppingToken); } catch (OperationCanceledException) { break; } }
        }
    }

    private async Task<OperationResponse> ProcessAsync(byte[] body, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(body); var root = doc.RootElement;
        if (!root.TryGetProperty("MessageId", out var mid) || mid.ValueKind != JsonValueKind.String || !Guid.TryParse(mid.GetString(), out _) ||
            !root.TryGetProperty("SchemaVersion", out var sv) || !sv.TryGetInt32(out var version))
            return Failure("INVALID_MESSAGE", "Отсутствует или некорректен MessageId либо SchemaVersion.");
        if (version != 1) return Failure("UNSUPPORTED_SCHEMA_VERSION", "Версия контракта не поддерживается.");
        if (!root.TryGetProperty("MessageType", out var mt) || mt.ValueKind != JsonValueKind.String)
            return Failure("INVALID_MESSAGE", "Отсутствует MessageType.");
        Guid? correlationId = null;
        if (root.TryGetProperty("CorrelationId", out var corr) && corr.ValueKind != JsonValueKind.Null)
        {
            if (corr.ValueKind != JsonValueKind.String || !Guid.TryParse(corr.GetString(), out var parsed))
                return Failure("INVALID_MESSAGE", "Некорректный CorrelationId.");
            correlationId = parsed;
        }
        if (!root.TryGetProperty("Payload", out var p) || p.ValueKind != JsonValueKind.Object)
            return Failure("INVALID_MESSAGE", "Отсутствует объект Payload.");
        switch (mt.GetString())
        {
            case "CompleteEntityRegistration":
                return await _completion.CompleteAsync(new CompletionRequest(p.GetProperty("EntityId").GetGuid(),
                    p.GetProperty("Result").GetString() ?? "", p.GetProperty("IdempotencyKey").GetGuid(),
                    OptionalString(p, "ErrorCode"), OptionalString(p, "ErrorMessage"), correlationId), ct);
            case "RegisterEntity":
                return ToOperationResponse(await _registration.RegisterEntityAsync(new RegisterEntityRequest(
                    p.GetProperty("EntityTypeCode").GetString() ?? "", p.GetProperty("IdempotencyKey").GetGuid(), correlationId), ct));
            case "RegisterSourceObject":
                return ToOperationResponse(await _registration.RegisterSourceObjectAsync(new RegisterSourceObjectRequest(
                    p.GetProperty("EntityId").GetGuid(), p.GetProperty("ExternalObjectType").GetString() ?? "",
                    p.GetProperty("ExternalObjectId").GetString() ?? "", OptionalString(p, "ExternalObjectCode"),
                    OptionalString(p, "ExternalObjectName"), p.GetProperty("IdempotencyKey").GetGuid(), correlationId), ct));
            default: return Failure("UNKNOWN_OPERATION", "Неизвестный тип операции.");
        }
    }

    private void PublishResponse(IModel channel, BasicGetResult delivery, OperationResponse response)
    {
        var props = channel.CreateBasicProperties(); props.Persistent = true; props.ContentType = "application/json";
        props.ContentEncoding = "utf-8"; props.CorrelationId = delivery.BasicProperties.CorrelationId;
        props.MessageId = Guid.CreateVersion7().ToString("D");
        var requestType = "Request";
        try
        {
            using var requestDocument = JsonDocument.Parse(delivery.Body.ToArray());
            if (requestDocument.RootElement.TryGetProperty("MessageType", out var type) &&
                type.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(type.GetString()))
                requestType = type.GetString()!;
        }
        catch (JsonException) { }
        var route = $"response.{_ekk.SystemCode}.{_ekk.InstanceCode}.{requestType}";
        string? returnedId = null;
        EventHandler<BasicReturnEventArgs> onReturn = (_, e) => returnedId = e.BasicProperties?.MessageId;
        channel.BasicReturn += onReturn;
        try
        {
            channel.BasicPublish(_rabbit.ResponseExchange, route, true, props, Encoding.UTF8.GetBytes(JsonSerializer.Serialize(response)));
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            if (returnedId is not null)
                throw new InvalidOperationException($"RabbitMQ returned unroutable RPC response {returnedId}.");
        }
        finally { channel.BasicReturn -= onReturn; }
    }
    private static string? OptionalString(JsonElement p, string n) => p.TryGetProperty(n, out var v) && v.ValueKind != JsonValueKind.Null ? v.GetString() : null;
    private static OperationResponse ToOperationResponse(RegistrationResult r) => new(r.Success, r.ResponseCode,
        r.EntityId is Guid id ? new CompletionPayload(id, r.RegistrationStatus ?? "", DateTime.UtcNow) : null, r.Error);
    private static OperationResponse Failure(string code, string message) => new(false, code, null, new OperationError(code, message, false, false));
    private void ValidateConfiguration()
    {
        if (string.IsNullOrWhiteSpace(_sql.ConnectionString)) throw new InvalidOperationException("Sql:ConnectionString must be configured.");
        if (_sql.CommandTimeoutSeconds <= 0) throw new InvalidOperationException("Sql:CommandTimeoutSeconds must be positive.");
        if (_ekk.SourceSystemInstanceKey <= 0 || string.IsNullOrWhiteSpace(_ekk.SystemCode) || string.IsNullOrWhiteSpace(_ekk.InstanceCode))
            throw new InvalidOperationException("Ekk source instance key and codes must be configured.");
        if (string.IsNullOrWhiteSpace(_rabbit.UserName) || string.IsNullOrWhiteSpace(_rabbit.Password))
            throw new InvalidOperationException("RabbitMQ credentials must be configured.");
    }

    private async Task ValidateSourceSystemInstanceAsync(CancellationToken ct)
    {
        const string sql = "SELECT COUNT_BIG(1) FROM ekk.SourceSystemInstance AS i INNER JOIN ekk.SourceSystem AS s ON s.SourceSystemKey=i.SourceSystemKey WHERE i.SourceSystemInstanceKey=@Key AND i.Code=@InstanceCode AND s.Code=@SystemCode AND i.IsActive=1 AND s.IsActive=1;";
        await using var connection = new SqlConnection(_sql.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = _sql.CommandTimeoutSeconds };
        command.Parameters.Add("@Key", SqlDbType.BigInt).Value = _ekk.SourceSystemInstanceKey;
        command.Parameters.Add("@InstanceCode", SqlDbType.NVarChar, 128).Value = _ekk.InstanceCode;
        command.Parameters.Add("@SystemCode", SqlDbType.NVarChar, 128).Value = _ekk.SystemCode;
        var count = System.Convert.ToInt64(await command.ExecuteScalarAsync(ct));
        if (count != 1)
            throw new InvalidOperationException("Configured EKK source system instance is missing, inactive, or does not match SystemCode/InstanceCode.");
        _logger.LogInformation("EKK source instance validated: {SystemCode}/{InstanceCode}, key={InstanceKey}", _ekk.SystemCode, _ekk.InstanceCode, _ekk.SourceSystemInstanceKey);
    }
}
