using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace AzureBatchQueue;

public class TimerBatch<T>
{
    readonly BatchQueue<T> batchQueue;
    readonly QueueMessage<T[]> msg;
    readonly TimeSpan flushPeriod;
    readonly int maxDequeueCount;
    readonly ILogger<BatchQueue<T>> logger;
    readonly ConcurrentDictionary<string, BatchItem<T>> items;

    readonly Timer timer;
    bool completed;

    public TimerBatch(BatchQueue<T> batchQueue, QueueMessage<T[]> msg, TimeSpan flushPeriod, int maxDequeueCount,
        ILogger<BatchQueue<T>> logger)
    {
        this.batchQueue = batchQueue;
        this.msg = msg;
        this.flushPeriod = flushPeriod;
        this.maxDequeueCount = maxDequeueCount;
        this.logger = logger;
        items = new ConcurrentDictionary<string, BatchItem<T>>(
            msg.Item.Select((x, idx) => new BatchItem<T>($"{msg.MessageId.Id}_{idx}", this, x)).ToDictionary(item => item.Id));
        timer = new Timer(async _ => await Flush());
        completed = false;
    }

    async Task Flush()
    {
        try
        {
            await timer.DisposeAsync();
            completed = true;

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
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception when flushing message {messageId}.", msg.MessageId);
            throw;
        }

        async Task DoFlush()
        {
            if (items.IsEmpty)
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
        var notCompletedItems = items.Values.Select(x => x.Item).ToArray();
        return msg with { Item = notCompletedItems };
    }

    public bool Complete(string itemId)
    {
        if (completed)
            throw new BatchCompletedException($"Failed to complete item {itemId} on an already finalized batch {msg.MessageId}");

        var res = items.TryRemove(itemId, out _);
        if (!res)
            return false;

        if (items.IsEmpty)
            timer.Change(TimeSpan.FromMilliseconds(1), Timeout.InfiniteTimeSpan);

        return true;
    }

    public IEnumerable<BatchItem<T>> Unpack()
    {
        timer.Change(flushPeriod, Timeout.InfiniteTimeSpan);
        return items.Values.ToArray();
    }
}

public class BatchCompletedException : Exception
{
    public BatchCompletedException(string s) : base(s) { }
}