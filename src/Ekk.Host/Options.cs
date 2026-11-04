namespace Ekk.Host;

public sealed class SqlOptions
{
    public string ConnectionString { get; set; } = "";
    public int CommandTimeoutSeconds { get; set; } = 30;
}

public sealed class RabbitOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string VirtualHost { get; set; } = "/";
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public string RequestQueue { get; set; } = "ekk.requests";
    public string ResponseExchange { get; set; } = "ekk.responses";
    public ushort PrefetchCount { get; set; } = 10;
}

public sealed class EkkOptions
{
    public long SourceSystemInstanceKey { get; set; }
    public string SystemCode { get; set; } = "";
    public string InstanceCode { get; set; } = "";
}
