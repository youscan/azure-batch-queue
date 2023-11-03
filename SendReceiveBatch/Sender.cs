using AzureBatchQueue;
using AzureBatchQueue.Utils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace SendReceiveBatch;

public class Sender
{
    private BatchQueue<string> batchQueue;
    private const int maxBatchSizeInBytes = 30;
    private IMessageBatchSerializer<string> serializer;

    public Sender(string queueName, ILogger<BatchQueue<string>> logger, IMessageBatchSerializer<string> serializer)
    {
        this.serializer = serializer;
        batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, flushPeriod: TimeSpan.FromSeconds(5), logger);
    }

    public async Task Init()
    {
        await batchQueue.CreateIfNotExists();
        await batchQueue.ClearMessages();
        Log("Sender is ready.");
    }

    public async Task SendMessages()
    {
        var words = new[] { "One", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };

        var batches = CreateBatches(words);

        foreach (var batch in batches)
        {
            await batchQueue.SendBatch(batch);

            Log($"Sent a batch {JsonConvert.SerializeObject(batch.Items())}.");
            Sleep(TimeSpan.FromSeconds(5));
        }
    }

    static void Sleep(TimeSpan sleep)
    {
        Log($"Sleep for {sleep.Seconds} seconds.");
        Thread.Sleep(sleep);
    }

    private IEnumerable<MessageBatch<string>> CreateBatches(IReadOnlyList<string>? words)
    {
        if (words == null || words.Count == 0)
            return ArraySegment<MessageBatch<string>>.Empty;

        var batches = new List<MessageBatch<string>> { new(serializer, maxBatchSizeInBytes) };

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
            batches.Add(new MessageBatch<string>(serializer, maxBatchSizeInBytes));
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
