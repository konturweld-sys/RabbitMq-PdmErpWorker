using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

internal static class Program
{
    public static async Task<int> Main()
    {
        try
        {
            var settings = Settings.Load();
            Console.WriteLine("TARGET=ERP_TEST");
            Console.WriteLine("ERP_ENDPOINT=" + settings.ErpEndpoint);
            Console.WriteLine("RABBITMQ=" + settings.RabbitHost + ":" + settings.RabbitPort);
            Console.WriteLine("STAGE=CONNECT_PDM_READ_ONLY");

            var snapshotText = ExportFromPdm(settings.PdmConnectionString);
            using var snapshot = JsonDocument.Parse(snapshotText);
            var messages = BuildMessages(snapshot.RootElement);

            Console.WriteLine("PDM_VIEWS=" + CountArray(snapshot.RootElement, "nomenclatureViews"));
            Console.WriteLine("PDM_ITEMS=" + CountArray(snapshot.RootElement, "items"));
            Console.WriteLine("MESSAGES_TO_PUBLISH=" + messages.Count);

            if (messages.Count == 0)
            {
                Console.WriteLine("RESULT=NO_DATA");
                return 0;
            }

            await TransferBatchAsync(settings, messages);
            Console.WriteLine("RESULT=SUCCESS");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("RESULT=FAILED");
            Console.Error.WriteLine(ex.ToString());
            return 1;
        }
    }

    private static string ExportFromPdm(string connectionString)
    {
        if (!OperatingSystem.IsWindows())
            throw new PlatformNotSupportedException("COM-подключение к 1С PDM поддерживается только в Windows.");

        var connectorType = Type.GetTypeFromProgID("V83.COMConnector");
        if (connectorType is null)
            throw new InvalidOperationException("Не найден V83.COMConnector. Проверьте регистрацию COM платформы PDM.");

        object? connector = null;
        object? connection = null;
        object? module = null;

        try
        {
            connector = Activator.CreateInstance(connectorType)
                ?? throw new InvalidOperationException("Не удалось создать V83.COMConnector.");

            dynamic dynamicConnector = connector;
            connection = dynamicConnector.Connect(connectionString);
            dynamic dynamicConnection = connection;
            module = dynamicConnection.CommonModule("_кадИнтеграцияВызов");
            dynamic dynamicModule = module;
            string json = dynamicModule.ЭкспортироватьКаталогиДляERP();

            if (string.IsNullOrWhiteSpace(json))
                throw new InvalidOperationException("PDM вернула пустой JSON.");
            return json;
        }
        finally
        {
            ReleaseCom(module);
            ReleaseCom(connection);
            ReleaseCom(connector);
        }
    }

    private static void ReleaseCom(object? value)
    {
        if (value is not null && Marshal.IsComObject(value))
            Marshal.FinalReleaseComObject(value);
    }

    private static List<TransferMessage> BuildMessages(JsonElement root)
    {
        var result = new List<TransferMessage>();
        var sourceSystem = GetString(root, "sourceSystem");
        if (string.IsNullOrWhiteSpace(sourceSystem))
            throw new InvalidDataException("PDM export does not contain sourceSystem.");

        foreach (var view in GetArray(root, "nomenclatureViews"))
        {
            var sourceRef = RequiredString(view, "sourceRef");
            var name = RequiredString(view, "itemName");
            var type = GetString(view, "nomenclatureType") ?? "";
            var unit = GetString(view, "storageUnit") ?? "";
            var usesCharacteristics = GetBool(view, "usesCharacteristics");
            var eventType = "nomenclatureView.created";
            var messageId = StableMessageId(eventType, sourceSystem, sourceRef);

            var payload = new Dictionary<string, object?>
            {
                ["schemaVersion"] = 1,
                ["messageId"] = messageId,
                ["eventType"] = eventType,
                ["sourceSystem"] = sourceSystem,
                ["sourceRef"] = sourceRef,
                ["itemName"] = name,
                ["nomenclatureType"] = type,
                ["storageUnit"] = unit,
                ["usesCharacteristics"] = usesCharacteristics
            };

            result.Add(new TransferMessage(messageId, eventType, sourceRef,
                JsonSerializer.Serialize(payload)));
        }

        foreach (var item in GetArray(root, "items"))
        {
            var sourceRef = RequiredString(item, "sourceRef");
            var name = RequiredString(item, "itemName");
            var kindName = GetString(item, "nomenclatureView") ?? "";
            var unit = GetString(item, "storageUnit") ?? "";
            var usesCharacteristics = GetBool(item, "usesCharacteristics");
            var eventType = "nomenclature.created";
            var messageId = StableMessageId(eventType, sourceSystem, sourceRef);

            var payload = new Dictionary<string, object?>
            {
                ["schemaVersion"] = 1,
                ["messageId"] = messageId,
                ["eventType"] = eventType,
                ["sourceSystem"] = sourceSystem,
                ["sourceRef"] = sourceRef,
                ["itemName"] = name,
                ["nomenclatureView"] = kindName,
                ["nomenclatureViewSourceRef"] = GetString(item, "nomenclatureViewSourceRef") ?? "",
                ["nomenclatureType"] = GetString(item, "nomenclatureType") ?? "",
                ["storageUnit"] = unit,
                ["usesCharacteristics"] = usesCharacteristics
            };

            result.Add(new TransferMessage(messageId, eventType, sourceRef,
                JsonSerializer.Serialize(payload)));
        }

        return result;
    }

    private static string StableMessageId(string eventType, string sourceSystem, string sourceRef)
    {
        var input = eventType + "|" + sourceSystem + "|" + sourceRef;
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        var guidBytes = bytes[..16];
        guidBytes[7] = (byte)((guidBytes[7] & 0x0F) | 0x50);
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);
        return new Guid(guidBytes).ToString("D").ToLowerInvariant();
    }

    private static async Task TransferBatchAsync(Settings settings, List<TransferMessage> messages)
    {
        var factory = new ConnectionFactory
        {
            HostName = settings.RabbitHost,
            Port = settings.RabbitPort,
            UserName = settings.RabbitUser,
            Password = settings.RabbitPassword,
            VirtualHost = settings.RabbitVhost,
            AutomaticRecoveryEnabled = true,
            RequestedHeartbeat = TimeSpan.FromSeconds(30)
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(settings.DeadLetterExchange, ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare(settings.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(settings.DeadLetterQueue, settings.DeadLetterExchange, settings.QueueName);

        var queueArgs = new Dictionary<string, object>
        {
            ["x-dead-letter-exchange"] = settings.DeadLetterExchange,
            ["x-dead-letter-routing-key"] = settings.QueueName
        };
        channel.QueueDeclare(settings.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
        channel.ConfirmSelect();

        var expectedIds = messages.Select(m => m.MessageId).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var observedIds = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        var failures = new ConcurrentBag<string>();

        Console.WriteLine("STAGE=PUBLISH");
        foreach (var message in messages)
        {
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            properties.MessageId = message.MessageId;
            properties.Type = message.EventType;
            properties.Headers = new Dictionary<string, object>
            {
                ["sourceRef"] = Encoding.UTF8.GetBytes(message.SourceRef),
                ["eventType"] = Encoding.UTF8.GetBytes(message.EventType)
            };

            channel.BasicPublish("", settings.QueueName, mandatory: true,
                basicProperties: properties, body: Encoding.UTF8.GetBytes(message.Json));
        }

        channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(15));
        Console.WriteLine("PUBLISH_CONFIRMED=true");

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes(settings.ErpUser + ":" + settings.ErpPassword));
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Console.WriteLine("STAGE=CONSUME_AND_POST_TO_ERP");

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (_, ea) =>
        {
            var messageId = ea.BasicProperties.MessageId ?? "";
            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                using var request = new HttpRequestMessage(HttpMethod.Post, settings.ErpEndpoint)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json")
                };

                using var response = http.Send(request);
                var responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                var receiverSuccess = false;

                if (response.IsSuccessStatusCode)
                {
                    try
                    {
                        using var responseJson = JsonDocument.Parse(responseBody);
                        receiverSuccess = responseJson.RootElement.TryGetProperty("success", out var success)
                            && success.ValueKind == JsonValueKind.True;
                    }
                    catch (JsonException)
                    {
                        receiverSuccess = false;
                    }
                }

                if (!response.IsSuccessStatusCode || !receiverSuccess)
                {
                    failures.Add(messageId + ": HTTP " + (int)response.StatusCode + "; " + responseBody);
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                    Console.Error.WriteLine("ERP_REJECTED messageId=" + messageId + " status=" + (int)response.StatusCode);
                }
                else
                {
                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                    Console.WriteLine("ERP_ACCEPTED messageId=" + messageId + " eventType=" + ea.BasicProperties.Type);
                }
            }
            catch (Exception ex)
            {
                failures.Add(messageId + ": " + ex.Message);
                try { channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false); } catch { }
                Console.Error.WriteLine("MESSAGE_FAILED messageId=" + messageId + ": " + ex.Message);
            }
            finally
            {
                if (expectedIds.Contains(messageId))
                {
                    observedIds.TryAdd(messageId, true);
                    if (observedIds.Count >= expectedIds.Count)
                        done.TrySetResult(true);
                }
            }
        };

        channel.BasicQos(0, prefetchCount: 1, global: false);
        var consumerTag = channel.BasicConsume(settings.QueueName, autoAck: false, consumer);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        try
        {
            await done.Task.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException("Тайм-аут ожидания ERP для " + observedIds.Count + " из " + expectedIds.Count + " сообщений. Проверьте очередь и DLQ.");
        }
        finally
        {
            if (channel.IsOpen)
                channel.BasicCancel(consumerTag);
        }

        Console.WriteLine("MESSAGES_CONFIRMED_BY_ERP=" + observedIds.Count);
        Console.WriteLine("MESSAGES_FAILED=" + failures.Count);
        if (!failures.IsEmpty)
            throw new InvalidOperationException("Часть сообщений не принята ERP. Ошибки: " + string.Join(" || ", failures));
    }

    private static int CountArray(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Array ? value.GetArrayLength() : 0;

    private static IEnumerable<JsonElement> GetArray(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.Array)
            throw new InvalidDataException("PDM export does not contain array '" + name + "'.");
        return value.EnumerateArray().ToArray();
    }

    private static string RequiredString(JsonElement element, string name)
    {
        var value = GetString(element, name);
        return string.IsNullOrWhiteSpace(value)
            ? throw new InvalidDataException("PDM export field '" + name + "' is empty.")
            : value;
    }

    private static string? GetString(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static bool GetBool(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.True;

    private sealed record TransferMessage(string MessageId, string EventType, string SourceRef, string Json);

    private sealed record Settings(
        string PdmConnectionString,
        string ErpEndpoint,
        string ErpUser,
        string ErpPassword,
        string RabbitHost,
        int RabbitPort,
        string RabbitUser,
        string RabbitPassword,
        string RabbitVhost,
        string QueueName,
        string DeadLetterExchange,
        string DeadLetterQueue)
    {
        public static Settings Load()
        {
            string Required(string name) =>
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))
                    ? throw new InvalidOperationException("Не задана переменная окружения " + name + ".")
                    : Environment.GetEnvironmentVariable(name)!;

            if (!string.Equals(Required("ERP_TARGET"), "TEST", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Защита от записи в рабочую базу: задайте ERP_TARGET=TEST только для проверенной тестовой базы.");

            var endpoint = Required("ERP_ENDPOINT");
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
                throw new InvalidOperationException("ERP_ENDPOINT должен быть абсолютным HTTP(S)-адресом HTTP-сервиса 1С.");

            return new Settings(
                Required("PDM_CONNECTION_STRING"),
                endpoint,
                Required("ERP_USER"),
                Required("ERP_PASSWORD"),
                Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "192.168.50.173",
                int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var port) ? port : 5672,
                Required("RABBITMQ_USER"),
                Required("RABBITMQ_PASSWORD"),
                Environment.GetEnvironmentVariable("RABBITMQ_VHOST") ?? "/",
                Environment.GetEnvironmentVariable("RABBITMQ_QUEUE") ?? "pdm-erp-test-transfer",
                Environment.GetEnvironmentVariable("RABBITMQ_DLX") ?? "pdm-erp-test-dlx",
                Environment.GetEnvironmentVariable("RABBITMQ_DLQ") ?? "pdm-erp-test-dead");
        }
    }
}
