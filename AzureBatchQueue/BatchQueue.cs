using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    private readonly ILogger<BatchQueue<T>> logger;

    private readonly QueueClient queue;
    private readonly QueueClient quarantineQueue;
    private readonly bool initQuarantineQueue;

    private readonly TimeSpan flushPeriod;
    private readonly int maxDequeueCount;
    private readonly TimeSpan visibilityTimeout;

    public BatchQueue(
        string connectionString,
        string queueName,
        TimeSpan flushPeriod,
        int maxDequeueCount = 5,
        ILogger<BatchQueue<T>>? logger = null)
    {
        queue = new QueueClient(connectionString, queueName);
        quarantineQueue = new QueueClient(connectionString, $"quarantine-{queueName}");

        this.flushPeriod = flushPeriod;
        this.maxDequeueCount = maxDequeueCount;
        this.logger = logger ?? NullLogger<BatchQueue<T>>.Instance;
        visibilityTimeout = this.flushPeriod.Add(TimeSpan.FromSeconds(5));
    }

    public async Task SendBatch(MessageBatch<T> batch)
    {
        try
        {
            await queue.SendMessageAsync(batch.Serialize());
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "RequestBodyTooLarge")
        {
            logger.LogError(ex, "The request body is too large and exceeds the maximum permissible limit.");

            throw new MessageTooLargeException("The request body is too large and exceeds the maximum permissible limit.", ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected exception while sending a batch.");
            throw;
        }
    }

    public async Task<BatchItem<T>[]> ReceiveMany(int maxBatches = 32)
    {
        if (maxBatches is < 1 or > 32)
            throw new ArgumentException($"MaxMessages is outside of the permissible range. Actual value is {maxBatches}, when minimumAllowed is 1 and maximumAllowed is 32.");

        var messages = await queue.ReceiveMessagesAsync(maxBatches, visibilityTimeout);

        if (messages.Value == null || !messages.HasValue || !messages.Value.Any())
            return Array.Empty<BatchItem<T>>();

        var items = new List<BatchItem<T>>();

        foreach (var msg in messages.Value!)
        {
            if (msg.DequeueCount > maxDequeueCount)
            {
                await QuarantineMessage(msg, "MaxDequeueCount");
                continue;
            }

            var batch = QueueMessageBatch(msg);
            items.AddRange(batch.Unpack());
        }

        return items.ToArray();
    }

    public async Task<BatchItem<T>[]> ReceiveBatch()
    {
        var msg = await queue.ReceiveMessageAsync(visibilityTimeout);
        var queueMessage = msg.Value;

        if (queueMessage?.Body == null)
            return Array.Empty<BatchItem<T>>();

        if (queueMessage.DequeueCount > maxDequeueCount)
        {
            await QuarantineMessage(queueMessage, "MaxDequeueCount");
            return Array.Empty<BatchItem<T>>();
        }

        var batch = QueueMessageBatch(queueMessage);

        return batch.Unpack();
    }

    private QueueMessageBatch<T> QueueMessageBatch(QueueMessage queueMessage)
    {
        var messageBatch = MessageBatch<T>.Deserialize(queueMessage.Body.ToString());

        var batchOptions = new MessageBatchOptions(queueMessage.MessageId, queueMessage.PopReceipt, flushPeriod,
            messageBatch.SerializerType);
        return new QueueMessageBatch<T>(queue, messageBatch.Items(), batchOptions, logger);
    }

    private async Task QuarantineMessage(QueueMessage queueMessage, string reason)
    {
        try
        {
            await quarantineQueue.SendMessageAsync(queueMessage.Body);
            await queue.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt);

            logger.LogInformation("Message {MessageId} inserted at {InsertionTime} was quarantined. Reason {Reason}.",
                queueMessage.MessageId, queueMessage.InsertedOn, reason);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to quarantine message {messageId}.", queueMessage.MessageId);
        }
    }

    /// <summary>
    /// Creates a queue under the specified account and queue name provided in the ctor of the class.
    /// Also, if initQuarantineQueue parameter passed in ctor is set to true, will create a new queue with name quarantine-{batchQueueName}.
    /// If the queue already exists it is not changed.
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    public Task Init(CancellationToken ct = default)
    {
        var tasks = new Task[]
        {
            queue.CreateIfNotExistsAsync(null, ct),
            quarantineQueue.CreateIfNotExistsAsync(null, ct)
        };

        return Task.WhenAll(tasks);
    }

    public Task Delete() => Task.WhenAll(queue.DeleteAsync(), quarantineQueue.DeleteAsync());
    public async Task ClearMessages() => await queue.ClearMessagesAsync();

    /// <summary>
    /// /// The approximate number of messages in the quarantine queue. This number is not lower than the actual number of messages in the queue, but could be higher.
    /// </summary>
    /// <returns></returns>
    public async Task<int> QuarantineApproximateMessagesCount()
    {
        var response = await quarantineQueue.GetPropertiesAsync();

        return response.HasValue ? response.Value.ApproximateMessagesCount : 0;
    }
    /// <summary>
    /// /// The approximate number of messages in the quarantine queue. This number is not lower than the actual number of messages in the queue, but could be higher.
    /// </summary>
    /// <returns></returns>
    public async Task<int> ApproximateMessagesCount()
    {
        var response = await queue.GetPropertiesAsync();

        return response.HasValue ? response.Value.ApproximateMessagesCount : 0;
    }

    public async Task Dequarantine()
    {
        var count = 0;

        do
        {
            var messages = await quarantineQueue.ReceiveMessagesAsync();

            if (!messages.HasValue || !messages.Value.Any())
                break;

            foreach (var message in messages.Value)
            {
                await queue.SendMessageAsync(message.Body);
                await quarantineQueue.DeleteMessageAsync(message.MessageId, message.PopReceipt);

                if (count % 100 == 0)
                    logger.LogInformation("Dequarantined {messages} msgs.", count);
            }
        } while (true);

        logger.LogInformation("Dequarantined {messages} msgs", count);
    }
}
