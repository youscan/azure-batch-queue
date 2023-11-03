using AzureBatchQueue;
using Microsoft.Extensions.Logging;

namespace SendReceiveBatch;

public class Receiver
{
    private BatchQueue<string> batchQueue;
    public string[] DoNotComplete = { "four", "six" };
    private bool canceled = false;

    public Receiver(string queueName, ILogger<BatchQueue<string>>? logger)
    {
        batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, flushPeriod: TimeSpan.FromSeconds(5), logger);
    }

    public async Task Init()
    {
        await batchQueue.CreateIfNotExists();
        Log("Receiver is ready.");
    }

    public async Task Receive()
    {
        Log("Start receiving.");

        while (!canceled)
        {
            var batchItems = await batchQueue.ReceiveBatch();

            Log($"Received batch with {batchItems.Length} items.");

            foreach (var batchItem in batchItems)
            {
                if (DoNotComplete.Contains(batchItem.Item))
                {
                    Log($"Will not complete {batchItem.Item}");
                    continue;
                }

                Log($"Completed {batchItem.Item}");
                await batchItem.Complete();
            }

            Sleep(TimeSpan.FromSeconds(2));
        }

        Log("Finish receiving items.");
    }

    static void Sleep(TimeSpan sleep)
    {
        Log($"Sleep for {sleep.Seconds} seconds.");
        Thread.Sleep(sleep);
    }

    private static void Log(string message)
    {
        Console.BackgroundColor = ConsoleColor.White;
        Console.ForegroundColor = ConsoleColor.DarkBlue;
        Console.WriteLine(message);
        Console.ResetColor();
    }

    public void Cancel() => canceled = true;
}