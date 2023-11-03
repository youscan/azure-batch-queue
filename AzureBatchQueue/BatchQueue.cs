using Azure.Storage.Queues;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    private readonly ILogger<BatchQueue<T>> logger;

    private readonly QueueClient queue;
    private readonly TimeSpan flushPeriod;
    private readonly TimeSpan visibilityTimeout;

    public BatchQueue(string connectionString, string queueName, TimeSpan flushPeriod, ILogger<BatchQueue<T>>? logger = null)
    {
        queue = new QueueClient(connectionString, queueName);
        this.flushPeriod = flushPeriod;
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

        var serializedBatch = MessageBatch<T>.Deserialize(msg.Value.Body.ToString());
        var batchOptions = new MessageBatchOptions(msg.Value.MessageId, msg.Value.PopReceipt, flushPeriod, serializedBatch.Compressed);

        var batch = new QueueMessageBatch<T>(queue, serializedBatch.Items(), batchOptions, logger);

        return batch.Unpack();
    }

    public async Task CreateIfNotExists(CancellationToken ct = default) => await queue.CreateIfNotExistsAsync(null, ct);
    public async Task Delete() => await queue.DeleteAsync();
}
