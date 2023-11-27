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
        var messages = new List<QueueMessage<string>>();

        await foreach (var batchItem in batchQueue.Receive())
        {
            if (DoNotComplete.Contains(batchItem.Item))
            {
                Log($"Will not complete {batchItem.Item}");
                continue;
            }

            messages.Add(batchItem);
        }

        await Task.Delay(TimeSpan.FromMilliseconds(300));

        foreach (var msg in messages)
        {
            Log($"Completed {msg.Item}");
            msg.Complete();
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
