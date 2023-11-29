using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    readonly ILogger<BatchQueue<T>> logger;

    readonly TimeSpan flushPeriod;
    readonly TimeSpan receiveVisibilityTimeout;
    readonly MessageQueue<T[]> queue;
    readonly MessageQueue<T[]> quarantineQueue;
    readonly int maxDequeueCount;

    public BatchQueue(
        string connectionString,
        string queueName,
        TimeSpan flushPeriod,
        int maxDequeueCount = 5,
        IMessageQueueSerializer<T[]>? serializer = null,
        ILogger<BatchQueue<T>>? logger = null)
    {
        queue = new MessageQueue<T[]>(connectionString, queueName, serializer);
        quarantineQueue = new MessageQueue<T[]>(connectionString, $"{queueName}-quarantine", serializer);
        this.maxDequeueCount = maxDequeueCount;

        this.flushPeriod = flushPeriod;
        receiveVisibilityTimeout = flushPeriod.Add(TimeSpan.FromSeconds(5));
        this.logger = logger ?? NullLogger<BatchQueue<T>>.Instance;
    }

    public string Name => queue.Name;

    public async Task Send(T[] items) => await queue.Send(items);

    public async Task<BatchItem<T>[]> Receive(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default) => await ReceiveInternal(queue, maxMessages, visibilityTimeout, ct);

    public async Task<BatchItem<T>[]> ReceiveFromQuarantine(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default) => await ReceiveInternal(quarantineQueue, maxMessages, visibilityTimeout, ct);

    async Task<BatchItem<T>[]> ReceiveInternal(MessageQueue<T[]> msgQueue, int? maxMessages, TimeSpan? visibilityTimeout, CancellationToken ct)
    {
        var arrayOfBatches = await msgQueue.Receive(maxMessages, visibilityTimeout ?? receiveVisibilityTimeout, ct: ct);

        var timerBatches = arrayOfBatches
            .Select(batch => new TimerBatch<T>(this, batch, flushPeriod, maxDequeueCount, logger)).ToList();

        return timerBatches.SelectMany(x => x.Unpack()).ToArray();
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
