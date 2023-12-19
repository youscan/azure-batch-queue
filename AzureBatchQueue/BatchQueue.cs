using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    ILogger logger;

    readonly MessageQueue<T[]> queue;
    readonly int maxDequeueCount;

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

    public async Task<BatchItem<T>[]> Receive(int maxMessages = 32, TimeSpan? visibilityTimeout = null, CancellationToken ct = default)
    {
        var arrayOfBatches = await queue.Receive(maxMessages, visibilityTimeout, ct: ct);
        var timerBatches = arrayOfBatches.Select(batch => new TimerBatch<T>(this, batch, maxDequeueCount, logger)).ToList();
        return timerBatches.SelectMany(x => x.Unpack()).ToArray();
    }

    public async Task<T[]> GetItemsFromQuarantine(int maxMessages = 32, CancellationToken ct = default)
    {
        var arrayOfBatches = await queue.ReceiveFromQuarantine(maxMessages, ct: ct);
        return arrayOfBatches.SelectMany(x => x.Item).ToArray();
    }

    public async Task Dequarantine() => await queue.Dequarantine();

    public async Task Init() => await queue.Init();
    public async Task Delete() => await queue.Delete();
    public async Task ClearMessages() => await queue.ClearMessages();

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

    public BatchQueue<T> WithLogger(ILogger queueLogger)
    {
        logger = queueLogger;
        return this;
    }
}
