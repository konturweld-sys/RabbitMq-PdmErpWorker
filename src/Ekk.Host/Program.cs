using Ekk.Host;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<SqlOptions>(builder.Configuration.GetSection("Sql"));
builder.Services.Configure<RabbitOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.Configure<EkkOptions>(builder.Configuration.GetSection("Ekk"));
builder.Services.AddSingleton<RegistrationCompletionService>();
builder.Services.AddHostedService<RabbitMqRequestWorker>();

await builder.Build().RunAsync();
