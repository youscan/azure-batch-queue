using System.Text.Json;
using AzureBatchQueue;
using Microsoft.Extensions.Logging;

namespace SendReceiveBatch;

public class Sender
{
    BatchQueue<string> batchQueue;

    public Sender(string queueName, ILogger logger)
    {
        batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, logger: logger);
    }

    public async Task Init()
    {
        await batchQueue.Delete();
        await batchQueue.Init();
        Log("Sender is ready.");
    }

    public async Task SendMessages()
    {
        var words = new[] { "One", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };

        await batchQueue.Send(words);

        Log($"Sent a batch {JsonSerializer.Serialize(words)}.");
    }

    static void Log(string message)
    {
        Console.BackgroundColor = ConsoleColor.White;
        Console.ForegroundColor = ConsoleColor.Black;
        Console.WriteLine(message);
        Console.ResetColor();
    }
}
