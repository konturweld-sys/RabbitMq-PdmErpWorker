using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Ekk.Host;

public sealed class RabbitMqRequestWorker : BackgroundService
{
    private readonly RabbitOptions _rabbit;
    private readonly EkkOptions _ekk;
    private readonly RegistrationCompletionService _completion;
    private readonly ILogger<RabbitMqRequestWorker> _logger;
    private IConnection? _connection;
    private IModel? _channel;

    public RabbitMqRequestWorker(IOptions<RabbitOptions> rabbit, IOptions<EkkOptions> ekk,
        RegistrationCompletionService completion, ILogger<RabbitMqRequestWorker> logger)
    {
        _rabbit = rabbit.Value;
        _ekk = ekk.Value;
        _completion = completion;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _rabbit.HostName, Port = _rabbit.Port, VirtualHost = _rabbit.VirtualHost,
            UserName = _rabbit.UserName, Password = _rabbit.Password,
            AutomaticRecoveryEnabled = true, TopologyRecoveryEnabled = true,
            ClientProvidedName = $"EKK/{_ekk.SystemCode}/{_ekk.InstanceCode}"
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();
                _channel.BasicQos(0, _rabbit.PrefetchCount, false);
                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (_, ea) => HandleMessage(ea, stoppingToken);
                _channel.BasicConsume(_rabbit.RequestQueue, false, consumer);

                _logger.LogInformation("EKK consumer started. Queue={Queue}", _rabbit.RequestQueue);
                while (!stoppingToken.IsCancellationRequested && _connection.IsOpen)
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ failure. Reconnecting.");
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
            finally
            {
                try { _channel?.Close(); } catch { }
                try { _connection?.Close(); } catch { }
                _channel?.Dispose();
                _connection?.Dispose();
                _channel = null;
                _connection = null;
            }
        }
    }

    private void HandleMessage(BasicDeliverEventArgs ea, CancellationToken stoppingToken)
    {
        try
        {
            using var document = JsonDocument.Parse(Encoding.UTF8.GetString(ea.Body.ToArray()));
            var root = document.RootElement;

            if (!root.TryGetProperty("MessageId", out var messageId) ||
                !Guid.TryParse(messageId.GetString(), out _) ||
                !root.TryGetProperty("SchemaVersion", out var schemaVersion) ||
                !schemaVersion.TryGetInt32(out var version))
            {
                PublishAndAck(ea, Failure("INVALID_MESSAGE", "Отсутствует MessageId или SchemaVersion."));
                return;
            }
            if (version != 1)
            {
                PublishAndAck(ea, Failure("UNSUPPORTED_SCHEMA_VERSION", "Версия контракта не поддерживается."));
                return;
            }
            if (!root.TryGetProperty("MessageType", out var messageType) ||
                messageType.GetString() != "CompleteEntityRegistration")
            {
                PublishAndAck(ea, Failure("INVALID_MESSAGE", "Неизвестный тип операции."));
                return;
            }

            Guid? correlationId = null;
            if (root.TryGetProperty("CorrelationId", out var correlation) && correlation.ValueKind != JsonValueKind.Null)
            {
                if (!Guid.TryParse(correlation.GetString(), out var parsed))
                {
                    PublishAndAck(ea, Failure("INVALID_MESSAGE", "Некорректный CorrelationId."));
                    return;
                }
                correlationId = parsed;
            }

            var payload = root.GetProperty("Payload");
            var request = new CompletionRequest(
                payload.GetProperty("EntityId").GetGuid(),
                payload.GetProperty("Result").GetString() ?? "",
                payload.GetProperty("IdempotencyKey").GetGuid(),
                payload.TryGetProperty("ErrorCode", out var errorCode) && errorCode.ValueKind != JsonValueKind.Null ? errorCode.GetString() : null,
                payload.TryGetProperty("ErrorMessage", out var errorMessage) && errorMessage.ValueKind != JsonValueKind.Null ? errorMessage.GetString() : null,
                correlationId);

            var response = _completion.CompleteAsync(request, stoppingToken).GetAwaiter().GetResult();
            PublishAndAck(ea, response, correlationId);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Invalid JSON request.");
            PublishAndAck(ea, Failure("INVALID_MESSAGE", "Некорректный JSON или отсутствуют обязательные поля."));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Request processing failed; message will be requeued.");
            try { _channel?.BasicNack(ea.DeliveryTag, false, true); } catch { }
        }
    }

    private static OperationResponse Failure(string code, string message) =>
        new(false, code, null, new(code, message, false, false));

    private void PublishAndAck(BasicDeliverEventArgs ea, OperationResponse response, Guid? correlationId = null)
    {
        var channel = _channel ?? throw new InvalidOperationException("RabbitMQ channel is unavailable.");
        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.ContentType = "application/json";
        properties.ContentEncoding = "utf-8";
        properties.CorrelationId = correlationId?.ToString("D") ?? ea.BasicProperties.CorrelationId;
        var routingKey = $"response.{_ekk.SystemCode}.{_ekk.InstanceCode}.CompleteEntityRegistration";
        channel.BasicPublish(_rabbit.ResponseExchange, routingKey, properties,
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(response)));
        channel.BasicAck(ea.DeliveryTag, false);
    }
}
