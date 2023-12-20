using AzureBatchQueue;
using AzureBatchQueue.Utils;
using Microsoft.Extensions.Logging;
using SendReceiveBatch;
using Serilog;
using Serilog.Extensions.Logging;

const string queueName = "sample-send-receive-queue";

var serilogLogger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

var microsoftLogger = new SerilogLoggerFactory(serilogLogger).CreateLogger<BatchQueue<string>>();

var sender = Task.Run(async () =>
{
    var sender = new Sender(queueName, microsoftLogger);
    await sender.Init();
    await sender.SendMessages();
});

var receiver = Task.Run(async () =>
{
    var receiver = new Receiver(queueName, microsoftLogger);
    await receiver.Init();
    await receiver.Receive();
});

await Task.WhenAll(sender, receiver);
