using AzureBatchQueue;
using Microsoft.Extensions.Logging;

namespace SendReceiveBatch;

public class Sender
{
    private BatchQueue<string> batchQueue;
    private const int maxBatchSizeInBytes = 30;

    public Sender(string queueName, ILogger<BatchQueue<string>> logger)
    {
        batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, flushPeriod: TimeSpan.FromSeconds(5), logger);
    }

    public async Task Init()
    {
        await batchQueue.CreateIfNotExists();
        Log("Sender is ready.");
    }

    public async Task SendMessages()
    {
        var words = new[] { "One", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };

        var batches = CreateBatches(words);

        foreach (var batch in batches)
        {
            await batchQueue.SendBatch(batch);

            Log($"Sent a batch {batch.Serialize()}.");
            Sleep(TimeSpan.FromSeconds(5));
        }
    }

    static void Sleep(TimeSpan sleep)
    {
        Log($"Sleep for {sleep.Seconds} seconds.");
        Thread.Sleep(sleep);
    }

    private static IEnumerable<MessageBatch<string>> CreateBatches(IReadOnlyList<string>? words)
    {
        if (words == null || words.Count == 0)
            return ArraySegment<MessageBatch<string>>.Empty;

        var batches = new List<MessageBatch<string>> { new(false, maxBatchSizeInBytes) };

        for (var i = 0; i < words.Count;)
        {
            var currentBatchHasSpace = batches.Last().TryAdd(words[i]);

            if (currentBatchHasSpace)
            {
                i++;
                continue;
            }

            if (batches.Last().Items().Count == 0)
                throw new Exception($"Word {words[i]} is too large to fit in the batch");

            // add new batch
            batches.Add(new MessageBatch<string>(false, maxBatchSizeInBytes));
        }

        return batches;
    }

    private static void Log(string message)
    {
        Console.BackgroundColor = ConsoleColor.White;
        Console.ForegroundColor = ConsoleColor.Black;
        Console.WriteLine(message);
        Console.ResetColor();
    }
}
