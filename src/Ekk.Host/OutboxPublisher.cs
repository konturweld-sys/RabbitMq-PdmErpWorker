using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
namespace Ekk.Host;

public sealed class OutboxPublisher : BackgroundService
{
    private readonly SqlOptions _sql; private readonly RabbitOptions _rabbit; private readonly EkkOptions _ekk;
    private readonly ILogger<OutboxPublisher> _logger;
    public OutboxPublisher(IOptions<SqlOptions> sql, IOptions<RabbitOptions> rabbit, IOptions<EkkOptions> ekk, ILogger<OutboxPublisher> logger)
    { _sql = sql.Value; _rabbit = rabbit.Value; _ekk = ekk.Value; _logger = logger; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await PublishBatchAsync(stoppingToken); await Task.Delay(_ekk.OutboxPollIntervalMilliseconds, stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { _logger.LogError(ex, "Outbox cycle failed; unconfirmed messages remain Pending."); try { await Task.Delay(5000, stoppingToken); } catch (OperationCanceledException) { break; } }
        }
    }

    private async Task PublishBatchAsync(CancellationToken ct)
    {
        var messages = await ReadPendingAsync(ct); if (messages.Count == 0) return;
        var factory = new ConnectionFactory { HostName = _rabbit.HostName, Port = _rabbit.Port, VirtualHost = _rabbit.VirtualHost,
            UserName = _rabbit.UserName, Password = _rabbit.Password, AutomaticRecoveryEnabled = true, TopologyRecoveryEnabled = true,
            ClientProvidedName = $"EKK-Outbox/{_ekk.SystemCode}/{_ekk.InstanceCode}" };
        using var connection = factory.CreateConnection(); using var channel = connection.CreateModel(); channel.ConfirmSelect();
        foreach (var message in messages)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                string? returnedId = null;
                EventHandler<BasicReturnEventArgs> onReturn = (_, e) => { if (e.BasicProperties?.MessageId == message.MessageId.ToString("D")) returnedId = e.BasicProperties.MessageId; };
                channel.BasicReturn += onReturn;
                try
                {
                    var p = channel.CreateBasicProperties(); p.Persistent = true; p.ContentType = "application/json";
                    p.ContentEncoding = "utf-8"; p.MessageId = message.MessageId.ToString("D"); p.Type = message.EventType;
                    channel.BasicPublish(_rabbit.EventExchange, message.RoutingKey, true, p, Encoding.UTF8.GetBytes(message.Payload));
                    channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                    if (returnedId is not null) throw new InvalidOperationException($"Unroutable RabbitMQ message: {returnedId}");
                }
                finally { channel.BasicReturn -= onReturn; }
                await MarkPublishedAsync(message.Key, ct);
            }
            catch (Exception ex) { await RecordFailureAsync(message.Key, message.AttemptCount + 1, ex, ct); _logger.LogError(ex, "Outbox publish failed for {MessageId}", message.MessageId); break; }
        }
    }

    private async Task<List<OutboxRecord>> ReadPendingAsync(CancellationToken ct)
    {
        var rows = new List<OutboxRecord>();
        const string sql = @"SELECT TOP (@BatchSize) OutboxMessageKey,MessageId,EventType,EntityId,EntitySequence,RoutingKey,Payload,AttemptCount
FROM ekk.OutboxMessage WITH (READPAST)
WHERE Status='Pending' AND (NextAttemptAt IS NULL OR NextAttemptAt<=SYSUTCDATETIME())
AND NOT EXISTS (SELECT 1 FROM ekk.OutboxMessage earlier
    WHERE earlier.EntityId=ekk.OutboxMessage.EntityId
      AND earlier.EntitySequence<ekk.OutboxMessage.EntitySequence
      AND earlier.Status='Pending')
ORDER BY CreatedAt,OutboxMessageKey;";
        await using var c = new SqlConnection(_sql.ConnectionString); await c.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, c) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.Add("@BatchSize", SqlDbType.Int).Value = Math.Clamp(_ekk.OutboxBatchSize, 1, 500);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) rows.Add(new(r.GetInt64(0), r.GetGuid(1), r.GetString(2), r.GetGuid(3), r.GetInt64(4), r.GetString(5), r.GetString(6), r.GetInt32(7)));
        return rows;
    }

    private async Task MarkPublishedAsync(long key, CancellationToken ct)
    {
        const string sql = "UPDATE ekk.OutboxMessage SET Status='Published',PublishedAt=SYSUTCDATETIME(),NextAttemptAt=NULL,LastError=NULL WHERE OutboxMessageKey=@Key AND Status='Pending';";
        await using var c = new SqlConnection(_sql.ConnectionString); await c.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, c) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.Add("@Key", SqlDbType.BigInt).Value = key; await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task RecordFailureAsync(long key, int attempts, Exception ex, CancellationToken ct)
    {
        var seconds = Math.Min(Math.Max(_ekk.OutboxMaxBackoffSeconds, 1), 2 * Math.Pow(2, Math.Min(Math.Max(attempts - 1, 0), 20)));
        var detail = ex.ToString(); if (detail.Length > 4000) detail = detail[..4000];
        const string sql = "UPDATE ekk.OutboxMessage SET AttemptCount=@Attempts,NextAttemptAt=DATEADD(SECOND,@Delay,SYSUTCDATETIME()),LastError=@Error WHERE OutboxMessageKey=@Key AND Status='Pending';";
        await using var c = new SqlConnection(_sql.ConnectionString); await c.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, c) { CommandTimeout = _sql.CommandTimeoutSeconds };
        cmd.Parameters.Add("@Attempts", SqlDbType.Int).Value = attempts; cmd.Parameters.Add("@Delay", SqlDbType.Int).Value = (int)seconds;
        cmd.Parameters.Add("@Error", SqlDbType.NVarChar, 4000).Value = detail; cmd.Parameters.Add("@Key", SqlDbType.BigInt).Value = key;
        await cmd.ExecuteNonQueryAsync(ct);
    }
    private sealed record OutboxRecord(long Key, Guid MessageId, string EventType, Guid EntityId, long EntitySequence, string RoutingKey, string Payload, int AttemptCount);
}
