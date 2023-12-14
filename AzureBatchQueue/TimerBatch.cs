using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace AzureBatchQueue;

internal class TimerBatch<T>
{
    readonly BatchQueue<T> batchQueue;
    readonly QueueMessage<T[]> msg;
    readonly int maxDequeueCount;
    readonly ILogger logger;
    readonly ConcurrentDictionary<string, BatchItem<T>> items;

    readonly Timer timer;
    BatchCompletedResult? completedResult;

    public TimerBatch(BatchQueue<T> batchQueue, QueueMessage<T[]> msg, int maxDequeueCount, ILogger logger)
    {
        this.batchQueue = batchQueue;
        this.msg = msg;
        FlushPeriod = CalculateFlushPeriod(this.msg.Metadata.VisibilityTime.Subtract(DateTimeOffset.UtcNow));
        this.maxDequeueCount = maxDequeueCount;
        this.logger = logger;
        items = new ConcurrentDictionary<string, BatchItem<T>>(
            msg.Item.Select((x, idx) => new BatchItem<T>($"{msg.MessageId.Id}_{idx}", this, x)).ToDictionary(item => item.Id));
        timer = new Timer(async _ => await Flush());
    }

    public MessageId MessageId => msg.MessageId;
    public QueueMessageMetadata Metadata => msg.Metadata;
    public TimeSpan FlushPeriod { get; }

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
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception when flushing message {messageId}.", msg.MessageId);
            throw;
        }

        async Task DoFlush()
        {
            if (items.IsEmpty)
            {
                completedResult = BatchCompletedResult.FullyProcessed;
                await Delete();
                return;
            }

            completedResult = BatchCompletedResult.TriggeredByFlush;

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
        if (completedResult != null)
            throw new BatchCompletedException("Failed to complete item on an already finalized batch.",
                new BatchItemMetadata(itemId, msg.MessageId, msg.Metadata.VisibilityTime, FlushPeriod, msg.Metadata.InsertedOn), completedResult.Value);

        var res = items.TryRemove(itemId, out _);
        if (!res)
            return false;

        if (items.IsEmpty)
            timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);

        return true;
    }

    public IEnumerable<BatchItem<T>> Unpack()
    {
        timer.Change(FlushPeriod, Timeout.InfiniteTimeSpan);
        return items.Values.ToArray();
    }

    /// <summary>
    /// Subtract a small value from visibilityTimeout, to make sure that Timer has enough time for a flush.
    /// </summary>
    /// <param name="visibilityTimeout"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    static TimeSpan CalculateFlushPeriod(TimeSpan visibilityTimeout)
    {
        if (visibilityTimeout > TimeSpan.FromSeconds(10))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(2));

        if (visibilityTimeout > TimeSpan.FromSeconds(5))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(1));

        if (visibilityTimeout >= TimeSpan.FromSeconds(1))
            return visibilityTimeout.Subtract(TimeSpan.FromMilliseconds(100));

        if (visibilityTimeout >= TimeSpan.FromMilliseconds(300))
            return TimeSpan.FromMilliseconds(200);

        if (visibilityTimeout >= TimeSpan.FromMilliseconds(30))
            return TimeSpan.FromMilliseconds(5);

        throw new ArgumentOutOfRangeException(nameof(visibilityTimeout));
    }
}

public class BatchCompletedException : Exception
{
    public BatchItemMetadata BatchItemMetadata { get; }
    public BatchCompletedResult BatchCompletedResult { get; }

    public BatchCompletedException(string msg, BatchItemMetadata batchItemMetadata, BatchCompletedResult completedResult) : base(msg)
    {
        BatchItemMetadata = batchItemMetadata;
        BatchCompletedResult = completedResult;
    }
}

public enum BatchCompletedResult
{
    FullyProcessed,
    TriggeredByFlush
}
