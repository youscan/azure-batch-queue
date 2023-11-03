using Azure.Storage.Queues;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
    private readonly ILogger<BatchQueue<T>> logger;

    public QueueMessageBatch(QueueClient queue, IEnumerable<T> items, MessageBatchOptions options, ILogger<BatchQueue<T>>? logger = null)
    {
        this.queue = queue;
        this.options = options;
        this.logger = logger ?? NullLogger<BatchQueue<T>>.Instance;

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
            {
                await queue.DeleteMessageAsync(options.MessageId, options.PopReceipt);
                logger.LogDebug("Deleted queue message batch {Id}.", options.MessageId);
            }
            else
            {
                await queue.UpdateMessageAsync(options.MessageId, options.PopReceipt, Serialize());
                logger.LogDebug("Updated queue message batch {Id} with {BatchItemsCount} items left.", options.MessageId,
                    BatchItems.Count);
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "MessageNotFound")
        {
            logger.LogError(ex, "Accessing already flushed message with {messageId}.", options.MessageId);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "QueueNotFound")
        {
            logger.LogError(ex, "Queue {queueName} not found when trying to delete message {messageId}.", queue.Name, options.MessageId);
        }
    }

    private string Serialize() => new MessageBatch<T>(BatchItems.Select(x => x.Item).ToList(), options.SerializerType).Serialize();

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

public record MessageBatchOptions(string MessageId, string PopReceipt, TimeSpan FlushPeriod, SerializerType SerializerType);
