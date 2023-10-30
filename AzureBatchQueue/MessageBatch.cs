using Azure.Storage.Queues;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class MessageBatch<T>
{
    private readonly QueueClient queue;
    private readonly MessageBatchOptions options;
    private readonly HashSet<BatchItem<T>> BatchItems;
    private readonly Timer timer;

    public MessageBatch(QueueClient queue, IEnumerable<T> items, MessageBatchOptions options)
    {
        this.queue = queue;
        this.options = options;

        BatchItems = items.Select(x => new BatchItem<T>(Guid.NewGuid(), this, x)).ToHashSet();
        timer = new Timer(async _ => await Flush(), null, options.FlushPeriod, Timeout.InfiniteTimeSpan);
    }

    private async Task Flush()
    {
        try
        {
            if (!BatchItems.Any())
                await queue.DeleteMessageAsync(options.MessageId, options.PopReceipt);
            else
                await queue.UpdateMessageAsync(options.MessageId, options.PopReceipt, Serialize());
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "MessageNotFound")
        {
            // log missing queue message
        }
        finally
        {
            await DisposeTimer();
        }
    }

    public static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
    public string Serialize() => Serialize(BatchItems.Select(x => x.Item));

    public async Task Complete(Guid id)
    {
        BatchItems.RemoveWhere(x => x.Id == id);
        if (!BatchItems.Any())
        {
            await DisposeTimer();
            await queue.DeleteMessageAsync(options.MessageId, options.PopReceipt);
        }
    }

    public async Task DisposeTimer() => await timer.DisposeAsync();

    public BatchItem<T>[] Items() => BatchItems.ToArray();
}

public record MessageBatchOptions(string MessageId, string PopReceipt, TimeSpan FlushPeriod);
