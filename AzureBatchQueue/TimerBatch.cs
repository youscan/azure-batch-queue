using System.Collections.Concurrent;
using AzureBatchQueue.Exceptions;
using Microsoft.Extensions.Logging;

namespace AzureBatchQueue;

internal class TimerBatch<T>
{
    readonly BatchQueue<T> batchQueue;
    readonly QueueMessage<T[]> msg;
    readonly int maxDequeueCount;
    readonly ILogger logger;
    readonly BatchItemsCollection<T> items;

    readonly Timer timer;
    BatchCompletedResult? completedResult;

    bool flushTriggered;
    readonly object locker = new();

    public TimerBatch(BatchQueue<T> batchQueue, QueueMessage<T[]> msg, int maxDequeueCount, ILogger logger)
    {
        this.batchQueue = batchQueue;
        this.msg = msg;
        this.logger = logger;
        this.maxDequeueCount = maxDequeueCount;

        FlushPeriod = CalculateFlushPeriod(this.msg.Metadata.VisibilityTime.Subtract(DateTimeOffset.UtcNow));

        var batchItems = msg.Item.Select((x, idx) => new BatchItem<T>(new BatchItemId(msg.MessageId.Id, idx), this, x)).ToArray();
        items = new BatchItemsCollection<T>(batchItems);

        timer = new Timer(async _ => await Flush());
    }

    public MessageId MessageId => msg.MessageId;
    public QueueMessageMetadata Metadata => msg.Metadata;
    public TimeSpan FlushPeriod { get; }

    async Task Flush()
    {
        // check if value is already set before acquiring a lock
        if (flushTriggered)
            return;

        lock (locker)
        {
            if (flushTriggered)
                return;

            flushTriggered = true;
            timer.Dispose();
        }

        try
        {
            await DoFlush();
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "QueueNotFound")
        {
            logger.LogWarning(ex, "Queue {queueName} was not found when flushing {messageId} with {itemsCount} items left.",
                batchQueue.Name, msg.MessageId, items.NotProcessedCount());
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
            if (completedResult != null)
                return;

            if (items.RemainingCount() == 0)
            {
                completedResult = BatchCompletedResult.FullyProcessed;
                await Delete();
                return;
            }

            completedResult = items.FailedCount() > 0 ? BatchCompletedResult.PartialFailure : BatchCompletedResult.TriggeredByFlush;

            if (msg.Metadata.DequeueCount >= maxDequeueCount)
                await Quarantine();
            else
                await Update();

            async Task Update()
            {
                var remaining = Remaining();
                await batchQueue.UpdateMessage(msg.MessageId, remaining);

                logger.LogWarning("Message {MsgId} was not fully processed within a timeout ({FlushPeriod}) sec in queue {QueueName}." +
                                  " {RemainingCount} items were not completed ({NotProcessed} not processed on time and {FailedCount} failed) from {TotalCount} total",
                    msg.MessageId,
                    FlushPeriod.TotalSeconds,
                    batchQueue.Name,
                    remaining.Length,
                    NotProcessedCount(),
                    FailedCount(),
                    items.Items().Length);
            }

            async Task Delete()
            {
                await batchQueue.DeleteMessage(msg.MessageId);

                logger.LogDebug("Message {msgId} was fully processed with {totalCount} items", msg.MessageId, items.Items().Length);
            }

            async Task Quarantine()
            {
                var remaining = Remaining();
                await batchQueue.QuarantineData(msg.MessageId, remaining);

                logger.LogInformation("Message {MsgId} was quarantined after {DequeueCount} unsuccessful attempts in queue {QueueName}." +
                                      " {RemainingCount} items were not completed ({NotProcessed} not processed on time and {FailedCount} failed) from {TotalCount} total",
                    msg.MessageId,
                    msg.Metadata.DequeueCount,
                    batchQueue.Name,
                    remaining.Length,
                    NotProcessedCount(),
                    FailedCount(),
                    items.Items().Length);
            }
        }
    }

    T[] Remaining() => items.Remaining().Select(x => x.Item).ToArray();
    int NotProcessedCount() => items.NotProcessedCount();
    int FailedCount() => items.FailedCount();

    public void Complete(BatchItemId itemId)
    {
        ThrowIfCompleted(itemId);

        var remaining = items.Complete(itemId);

        FlushIfEmpty(remaining);
    }

    public void Fail(BatchItemId itemId)
    {
        ThrowIfCompleted(itemId);

        var remaining = items.Fail(itemId);

        FlushIfEmpty(remaining);
    }

    void ThrowIfCompleted(BatchItemId itemId)
    {
        if (completedResult != null)
        {
            throw new BatchCompletedException("Failed to complete item on an already finalized batch.",
                new BatchItemMetadata(itemId.ToString(), msg.MessageId, msg.Metadata.VisibilityTime, FlushPeriod, msg.Metadata.InsertedOn), completedResult.Value);
        }
    }

    void FlushIfEmpty(int remaining)
    {
        if (remaining > 0 || flushTriggered)
            return;

        lock (locker)
        {
            if (!flushTriggered)
                timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }
    }

    public BatchItem<T>[] Unpack()
    {
        timer.Change(FlushPeriod, Timeout.InfiniteTimeSpan);
        return items.Items()!;
    }

    /// <summary>
    /// Subtract a small value from visibilityTimeout, to make sure that Timer has enough time for a flush.
    /// </summary>
    /// <param name="visibilityTimeout"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    TimeSpan CalculateFlushPeriod(TimeSpan visibilityTimeout)
    {
        if (visibilityTimeout > TimeSpan.FromSeconds(10))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(2));

        if (visibilityTimeout > TimeSpan.FromSeconds(5))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(1));

        if (visibilityTimeout >= TimeSpan.FromSeconds(1))
            return visibilityTimeout.Subtract(TimeSpan.FromMilliseconds(100));

        logger.LogError("VisibilityTimeout {VisibilityTimeout} cannot be less that 1 sec." +
                        " UtcNow: {UtcNow}, MessageVisibleTime: {MessageVisibleTime}, InsertedOn: {InsertedOn}",
            visibilityTimeout, DateTimeOffset.UtcNow.ToString("O"), Metadata.VisibilityTime.ToString("O"), Metadata.InsertedOn.ToString("O"));

        throw new ArgumentOutOfRangeException(nameof(visibilityTimeout));
    }
}

internal class BatchItemsCollection<T>
{
    readonly BatchItem<T>?[] items;
    readonly ConcurrentBag<BatchItem<T>> failedItems;
    int notProcessedCount;

    public BatchItemsCollection(BatchItem<T>[] items)
    {
        this.items = items;
        failedItems = new ConcurrentBag<BatchItem<T>>();
        notProcessedCount = this.items.Length;
    }

    public int Complete(BatchItemId id)
    {
        // Use atomic exchange to ensure thread-safe null setting
        // This guarantees only one thread can successfully set the item to null
        var item = Interlocked.Exchange(ref items[id.Idx], null);

        if (item == null)
            throw new ItemNotFoundException(id.ToString());

        // Atomic decrement - no lock needed
        return Interlocked.Decrement(ref notProcessedCount);
    }

    public int Fail(BatchItemId id)
    {
        // Use atomic exchange to ensure thread-safe null setting
        var item = Interlocked.Exchange(ref items[id.Idx], null);

        if (item == null)
            throw new ItemNotFoundException(id.ToString());

        // ConcurrentBag.Add is thread-safe
        failedItems.Add(item);

        // Atomic decrement - no lock needed
        return Interlocked.Decrement(ref notProcessedCount);
    }

    public int NotProcessedCount() => Volatile.Read(ref notProcessedCount);

    public int FailedCount() => failedItems.Count; // ConcurrentBag.Count is thread-safe

    public int RemainingCount() => NotProcessedCount() + FailedCount();

    public IEnumerable<BatchItem<T>> Remaining()
    {
        // Create a consistent snapshot of the collection state
        // ConcurrentBag enumeration creates a snapshot, so it's thread-safe
        var failed = failedItems.ToArray();

        // Since items only transition from non-null to null (never back),
        // this enumeration is safe without locks
        var notProcessed = items.Where(x => x != null)!;

        return failed.Concat(notProcessed).ToArray();
    }

    public BatchItem<T>?[] Items() => items;
}

public enum BatchCompletedResult
{
    FullyProcessed,
    TriggeredByFlush,
    PartialFailure
}
