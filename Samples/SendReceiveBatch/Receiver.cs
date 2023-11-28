using System.Threading.Channels;
using AzureBatchQueue;
using Microsoft.Extensions.Logging;

namespace SendReceiveBatch;

public class Receiver
{
    BatchQueue<string> batchQueue;
    public string[] DoNotComplete = { "four", "six" };
    bool canceled = false;

    public Receiver(string queueName, ILogger<BatchQueue<string>>? logger)
    {
        batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, flushPeriod: TimeSpan.FromSeconds(5), logger: logger);
    }

    public async Task Init()
    {
        await batchQueue.Init();
        Log("Receiver is ready.");
    }

    public async Task Receive()
    {
        Log("Start receiving.");
        var pending = Channel.CreateUnbounded<QueueMessage<string>>();

        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(00));
                var message = await pending.Reader.ReadAsync();
                message.Complete();
                Log($"Item {{ {message.Item} }} completed");
            }
        });

        await foreach (var batchItem in batchQueue.Receive())
        {
            Log($"Item {{ {batchItem.Item} }} received");
            await pending.Writer.WriteAsync(batchItem);
        }

        Log("Finish receiving items.");
    }

    static void Sleep(TimeSpan sleep)
    {
        Log($"Sleep for {sleep.Seconds} seconds.");
        Thread.Sleep(sleep);
    }

    static void Log(string message)
    {
        Console.BackgroundColor = ConsoleColor.White;
        Console.ForegroundColor = ConsoleColor.DarkBlue;
        Console.WriteLine(message);
        Console.ResetColor();
    }

    public void Cancel() => canceled = true;
}
