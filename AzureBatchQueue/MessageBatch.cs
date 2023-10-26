using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class MessageBatch<T>
{
    private readonly QueueClient queue;
    private readonly MessageBatchOptions options;
    private readonly Timer timer;
    private readonly HashSet<BatchItem<T>> BatchItems;

    public MessageBatch(QueueClient queue, IEnumerable<T> items, MessageBatchOptions options)
    {
        this.queue = queue;
        this.options = options;

        BatchItems = items.Select(x => new BatchItem<T>(Guid.NewGuid(), this, x)).ToHashSet();
        timer = new Timer(async _ => await Flush(), null, options.FlushPeriod, Timeout.InfiniteTimeSpan);
    }

    private async Task Flush()
    {
        if (!BatchItems.Any())
        {
            await queue.DeleteMessageAsync(options.MessageId, options.PopReceipt);
        }
        else
        {
            await queue.UpdateMessageAsync(options.MessageId, options.PopReceipt, Serialize(BatchItems.Select(x => x.Item)));
        }
    }

    private static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
    public string Serialize() => Serialize(BatchItems.Select(x => x.Item));

    public void Complete(Guid id) => BatchItems.RemoveWhere(x => x.Id == id);

    public BatchItem<T>[] Items() => BatchItems.ToArray();
}

public record MessageBatchOptions(string MessageId, string PopReceipt, TimeSpan FlushPeriod);

public class BatchQueue<T>
{
    private readonly QueueClient queue;
    private readonly TimeSpan flushPeriod;

    public BatchQueue(QueueClient queue, TimeSpan flushPeriod)
    {
        this.queue = queue;
        this.flushPeriod = flushPeriod;
    }

    public async Task SendBatch(MessageBatch<T> batch)
    {
        await queue.SendMessageAsync(batch.Serialize());
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

public class BatchItem<T>
{
    public BatchItem(Guid id, MessageBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
    }

    public Guid Id { get; }
    private MessageBatch<T> Batch { get; }
    public T Item;

    public void Complete()
    {
        Batch.Complete(Id);
    }
}
