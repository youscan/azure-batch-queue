using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using AzureBatchQueue.Utils;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    private readonly QueueClient queue;
    private readonly TimeSpan flushPeriod;
    private readonly TimeSpan visibilityTimeout;

    public BatchQueue(string connectionString, string queueName, TimeSpan flushPeriod)
    {
        queue = new QueueClient(connectionString, queueName);
        this.flushPeriod = flushPeriod;
        visibilityTimeout = this.flushPeriod.Add(TimeSpan.FromSeconds(5));
    }

    public async Task SendBatch(IEnumerable<T> items)
    {
        try
        {
            await queue.SendMessageAsync(MessageBatch<T>.Compress(items));
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "RequestBodyTooLarge")
        {
            // log overflow

            throw new MessageTooLargeException("The request body is too large and exceeds the maximum permissible limit.", ex);
        }
    }

    public async Task<BatchItem<T>[]> ReceiveBatch()
    {
        var msg = await queue.ReceiveMessageAsync(visibilityTimeout);

        if (msg.Value == null)
            return Array.Empty<BatchItem<T>>();

        var items = DeserializeCompressed(msg.Value);
        var batchOptions = new MessageBatchOptions(msg.Value.MessageId, msg.Value.PopReceipt, flushPeriod);

        var batch = new MessageBatch<T>(queue, items, batchOptions);

        return batch.Items();
    }

    private static IEnumerable<T> Deserialize(QueueMessage value) => value.Body.ToObjectFromJson<T[]>();
    private static IEnumerable<T>? DeserializeCompressed(QueueMessage value)
    {
        var decompressed = StringCompression.Decompress(value.Body.ToString());

        return JsonConvert.DeserializeObject<T[]>(decompressed);
    }

    public async Task Create() => await queue.CreateAsync();
    public async Task Delete() => await queue.DeleteAsync();
}
