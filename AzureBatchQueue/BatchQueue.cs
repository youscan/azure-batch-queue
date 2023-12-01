using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    readonly ILogger<BatchQueue<T>> logger;

    readonly MessageQueue<T[]> queue;
    readonly MessageQueue<T[]> quarantineQueue;
    readonly int maxDequeueCount;

    readonly TimeSpan defaultReceiveVisibilityTimeout = TimeSpan.FromSeconds(30);

    public BatchQueue(
        string connectionString,
        string queueName,
        int maxDequeueCount = 5,
        IMessageQueueSerializer<T[]>? serializer = null,
        ILogger<BatchQueue<T>>? logger = null)
    {
        queue = new MessageQueue<T[]>(connectionString, queueName, serializer);
        quarantineQueue = new MessageQueue<T[]>(connectionString, $"{queueName}-quarantine", serializer);
        this.maxDequeueCount = maxDequeueCount;

        this.logger = logger ?? NullLogger<BatchQueue<T>>.Instance;
    }

    public string Name => queue.Name;

    public async Task Send(T[] items) => await queue.Send(items);

    public async Task<BatchItem<T>[]> Receive(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default) => await ReceiveInternal(queue, maxMessages, ValidVisibilityTimeout(visibilityTimeout), ct);

    public async Task<BatchItem<T>[]> ReceiveFromQuarantine(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default) => await ReceiveInternal(quarantineQueue, maxMessages, ValidVisibilityTimeout(visibilityTimeout), ct);

    async Task<BatchItem<T>[]> ReceiveInternal(MessageQueue<T[]> msgQueue, int? maxMessages, TimeSpan visibilityTimeout, CancellationToken ct)
    {
        var flushPeriod = CalculateFlushPeriod(visibilityTimeout);

        var arrayOfBatches = await msgQueue.Receive(maxMessages, visibilityTimeout, ct: ct);
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

    public async Task Init() => await Task.WhenAll(queue.Init(), quarantineQueue.Init());
    public async Task Delete() => await Task.WhenAll(queue.Delete(), quarantineQueue.Delete());

    public async Task DeleteMessage(MessageId msgId) => await queue.DeleteMessage(msgId);
    public async Task UpdateMessage(QueueMessage<T[]> message) => await queue.UpdateMessage(message);

    public async Task QuarantineMessage(QueueMessage<T[]> message)
    {
        try
        {
            await quarantineQueue.Send(message.Item);
            await queue.DeleteMessage(message.MessageId);

            logger.LogInformation("Message {msgId} was quarantined after {dequeueCount} unsuccessful attempts.", message.MessageId, message.DequeueCount);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to quarantine message {messageId}.", message.MessageId);
        }
    }
}
