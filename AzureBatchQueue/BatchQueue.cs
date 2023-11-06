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

    public async Task<BatchItem<T>[]> ReceiveBatch()
    {
        var msg = await queue.ReceiveMessageAsync(visibilityTimeout);

        if (msg.Value?.Body == null)
            return Array.Empty<BatchItem<T>>();

        if (msg.Value.DequeueCount > maxDequeueCount)
        {
            await QuarantineMessage(msg.Value, "MaxDequeueCount");
            return Array.Empty<BatchItem<T>>();
        }

        var messageBatch = MessageBatch<T>.Deserialize(msg.Value.Body.ToString());

        var batchOptions = new MessageBatchOptions(msg.Value.MessageId, msg.Value.PopReceipt, flushPeriod, messageBatch.SerializerType);
        var batch = new QueueMessageBatch<T>(queue, messageBatch.Items(), batchOptions, logger);

        return batch.Unpack();
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
}
