using Microsoft.Extensions.Logging;

namespace AzureBatchQueue;

public class TimerBatch<T>
{
    readonly BatchQueue<T> batchQueue;
    readonly QueueMessage<T[]> msg;
    readonly TimeSpan flushPeriod;
    readonly int maxDequeueCount;
    readonly ILogger<BatchQueue<T>> logger;
    readonly HashSet<BatchItem<T>> items;

    readonly Timer timer;

    public TimerBatch(BatchQueue<T> batchQueue, QueueMessage<T[]> msg, TimeSpan flushPeriod, int maxDequeueCount,
        ILogger<BatchQueue<T>> logger)
    {
        this.batchQueue = batchQueue;
        this.msg = msg;
        this.flushPeriod = flushPeriod;
        this.maxDequeueCount = maxDequeueCount;
        this.logger = logger;
        items = msg.Item.Select((x, idx) => new BatchItem<T>($"{msg.MessageId.Id}_{idx}", this, x)).ToHashSet();
        timer = new Timer(async _ => await Flush());
    }

    async Task Flush()
    {
        try
        {
            await timer.DisposeAsync();

            await DoFlush();
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "QueueNotFound")
        {
            logger.LogWarning(ex, "Queue {queueName} was not found when flushing {messageId} with {itemsCount} items left.",
                batchQueue.Name, msg.MessageId, items.Count);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "MessageNotFound")
        {
            logger.LogError(ex, "Accessing already flushed message with {messageId}.", msg.MessageId);
        }
        catch (ObjectDisposedException)
        {
            logger.LogWarning("Timer in messageBatch {messageId} has already been disposed.", msg.MessageId);
        }

        async Task DoFlush()
        {
            if (!items.Any())
            {
                await Delete();
                return;
            }

            if (msg.DequeueCount >= maxDequeueCount)
                await Quarantine();
            else
                await Update();

            async Task Update() => await batchQueue.UpdateMessage(Message());
            async Task Delete() => await batchQueue.DeleteMessage(msg.MessageId);
            async Task Quarantine() => await batchQueue.QuarantineMessage(Message());
        }
    }

    QueueMessage<T[]> Message()
    {
        var notCompletedItems = items.Select(x => x.Item).ToArray();
        return msg with { Item = notCompletedItems };
    }

    public void Complete(string itemId)
    {
        items.RemoveWhere(x => x.Id == itemId);

        if (!items.Any())
            TriggerFlush();
    }

    void TriggerFlush()
    {
        try
        {
            timer.Change(TimeSpan.FromMilliseconds(1), Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
            logger.LogWarning("Timer in messageBatch {messageId} has already been disposed.", msg.MessageId);
        }
    }

    public IEnumerable<BatchItem<T>> Unpack()
    {
        timer.Change(flushPeriod, Timeout.InfiniteTimeSpan);
        return items.ToArray();
    }
}
