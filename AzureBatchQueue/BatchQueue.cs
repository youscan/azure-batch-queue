using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    ILogger logger;

    readonly MessageQueue<T[]> queue;
    readonly int maxDequeueCount;

    readonly TimeSpan defaultReceiveVisibilityTimeout = TimeSpan.FromSeconds(30);

    public BatchQueue(
        string connectionString,
        string queueName,
        int maxDequeueCount = 5,
        IMessageQueueSerializer<T[]>? serializer = null,
        ILogger? logger = null)
    {
        queue = new MessageQueue<T[]>(connectionString, queueName, serializer: serializer, logger: logger);
        this.maxDequeueCount = maxDequeueCount;

        this.logger = logger ?? NullLogger.Instance;
    }

    public string Name => queue.Name;

    public async Task Send(T[] items) => await queue.Send(items);

    public async Task<BatchItem<T>[]> Receive(int maxMessages = 32, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var validatedTimeout = ValidVisibilityTimeout(visibilityTimeout);
        return await ReceiveInternal(queue.Receive(maxMessages, validatedTimeout, ct: ct), validatedTimeout);
    }

    public async Task<BatchItem<T>[]> ReceiveFromQuarantine(int maxMessages = 32, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var validatedTimeout = ValidVisibilityTimeout(visibilityTimeout);
        return await ReceiveInternal(queue.ReceiveFromQuarantine(maxMessages, validatedTimeout, ct: ct), validatedTimeout);
    }

    async Task<BatchItem<T>[]> ReceiveInternal(Task<QueueMessage<T[]>[]> receive, TimeSpan visibilityTimeout)
    {
        var flushPeriod = CalculateFlushPeriod(visibilityTimeout);

        var arrayOfBatches = await receive;
        var timerBatches = arrayOfBatches.Select(batch => new TimerBatch<T>(this, batch, flushPeriod, maxDequeueCount, logger)).ToList();

        return timerBatches.SelectMany(x => x.Unpack()).ToArray();
    }

    TimeSpan ValidVisibilityTimeout(TimeSpan? visibilityTimeout)
    {
        if (visibilityTimeout == null)
            return defaultReceiveVisibilityTimeout;

        if (visibilityTimeout < TimeSpan.FromSeconds(1))
            throw new ArgumentOutOfRangeException("VisibilityTimeout");

        return visibilityTimeout.Value;
    }

    // don't be too harsh on me, this is just to test it on high load
    static TimeSpan CalculateFlushPeriod(TimeSpan visibilityTimeout)
    {
        if (visibilityTimeout > TimeSpan.FromSeconds(10))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(2));

        if (visibilityTimeout > TimeSpan.FromSeconds(5))
            return visibilityTimeout.Subtract(TimeSpan.FromSeconds(1));

        if (visibilityTimeout >= TimeSpan.FromSeconds(1))
            return visibilityTimeout.Subtract(TimeSpan.FromMilliseconds(500));

        throw new ArgumentOutOfRangeException("VisibilityTimeout");
    }

    public async Task Init() => await queue.Init();
    public async Task Delete() => await queue.Delete();

    public async Task DeleteMessage(MessageId msgId, CancellationToken ct = default) => await queue.DeleteMessage(msgId, ct);
    public async Task UpdateMessage(QueueMessage<T[]> message, CancellationToken ct = default) => await queue.UpdateMessage(message, ct: ct);

    public async Task QuarantineMessage(QueueMessage<T[]> message)
    {
        try
        {
            await queue.QuarantineMessage(message);

            logger.LogInformation("Message {msgId} was quarantined after {dequeueCount} unsuccessful attempts.", message.MessageId, message.DequeueCount);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to quarantine message {messageId}.", message.MessageId);
        }
    }

    public BatchQueue<T> WithLogger(ILogger<T> queueLogger)
    {
        logger = queueLogger;
        return this;
    }
}
