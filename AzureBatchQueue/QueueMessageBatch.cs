using Azure.Storage.Queues;

namespace AzureBatchQueue;

/// <summary>
/// Internal class that only BatchQueue works with. It has a timer to flush all completed items on flushPeriod time.
/// </summary>
/// <typeparam name="T"></typeparam>
public class QueueMessageBatch<T>
{
    private readonly QueueClient queue;
    private readonly MessageBatchOptions options;
    private readonly HashSet<BatchItem<T>> BatchItems;

    private readonly Timer timer;
    private bool completed = false;

    public QueueMessageBatch(QueueClient queue, IEnumerable<T> items, MessageBatchOptions options)
    {
        this.queue = queue;
        this.options = options;

        BatchItems = items.Select(x => new BatchItem<T>(Guid.NewGuid(), this, x)).ToHashSet();
        timer = new Timer(async _ => await Flush());
    }

    private async Task Flush()
    {
        completed = true;
        await timer.DisposeAsync();

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
    }

    private string Serialize() => SerializedMessageBatch<T>.Serialize(BatchItems.Select(x => x.Item), options.Compressed);

    public Task Complete(Guid id)
    {
        BatchItems.RemoveWhere(x => x.Id == id);
        if (BatchItems.Any()) return Task.CompletedTask;

        if (completed)
            throw new MessageBatchCompletedException($"MessageBatch {options.MessageId} is already completed;");

        try
        {
            timer.Change(TimeSpan.FromMilliseconds(1), Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
            throw new MessageBatchCompletedException($"MessageBatch {options.MessageId} is already completed;");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Triggers the timer with flushPeriod from batch options.
    /// </summary>
    /// <returns></returns>
    public BatchItem<T>[] Unpack()
    {
        timer.Change(options.FlushPeriod, Timeout.InfiniteTimeSpan);
        return BatchItems.ToArray();
    }
}

public record MessageBatchOptions(string MessageId, string PopReceipt, TimeSpan FlushPeriod, bool Compressed);
