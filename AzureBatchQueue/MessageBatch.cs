using Azure.Storage.Queues;
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

    public static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
    public string Serialize() => Serialize(BatchItems.Select(x => x.Item));

    public async Task Complete(Guid id)
    {
        BatchItems.RemoveWhere(x => x.Id == id);
        if (!BatchItems.Any())
        {
            await queue.DeleteMessageAsync(options.MessageId, options.PopReceipt);
        }
    }

    public BatchItem<T>[] Items() => BatchItems.ToArray();
}

public record MessageBatchOptions(string MessageId, string PopReceipt, TimeSpan FlushPeriod);
