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

Console.WriteLine("Compress batches using GZip? Write Yes/No and press Enter.");
var response = Console.ReadLine();
var serializer = GetSerializer<string>(response?.ToLower() == "yes");
Console.WriteLine($"Will use {serializer.GetType()} serializer.");

var sender = Task.Run(async () =>
{
    var sender = new Sender(queueName, microsoftLogger, serializer);
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

static IMessageBatchSerializer<T> GetSerializer<T>(bool compressed) => compressed ? new GZipCompressedSerializer<T>() : new JsonSerializer<T>();
