using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    private readonly QueueClient queue;
    private readonly TimeSpan flushPeriod;

    public BatchQueue(QueueClient queue, TimeSpan flushPeriod)
    {
        this.queue = queue;
        this.flushPeriod = flushPeriod;
    }

    public async Task SendBatch(IEnumerable<T> items)
    {
        await queue.SendMessageAsync(MessageBatch<T>.Serialize(items));
    }

    public async Task<BatchItem<T>[]> ReceiveBatch()
    {
        var msg = await queue.ReceiveMessageAsync();
        var items = Deserialize<T>(msg.Value);
        var batchOptions = new MessageBatchOptions(msg.Value.MessageId, msg.Value.PopReceipt, flushPeriod);

        var batch = new MessageBatch<T>(queue, items, batchOptions);

        return batch.Items();
    }

    private static IEnumerable<T> Deserialize<T>(QueueMessage value) => value.Body.ToObjectFromJson<T[]>();
}