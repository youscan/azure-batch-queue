using Azure.Storage.Queues;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    private readonly QueueClient queue;
    private readonly TimeSpan flushPeriod;
    private readonly TimeSpan visibilityTimeout;

    public BatchQueue(string connectionString, string queueName, TimeSpan flushPeriod)
    {
        queue = new QueueClient(connectionString, queueName);
        this.flushPeriod = flushPeriod;
        visibilityTimeout = this.flushPeriod.Add(TimeSpan.FromSeconds(5));
    }

    public async Task SendBatch(IEnumerable<T> items, bool compress = true)
    {
        try
        {
            await queue.SendMessageAsync(SerializedMessageBatch<T>.Serialize(items, compress));
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "RequestBodyTooLarge")
        {
            // log overflow

            throw new MessageTooLargeException("The request body is too large and exceeds the maximum permissible limit.", ex);
        }
    }

    public async Task<BatchItem<T>[]> ReceiveBatch()
    {
        var msg = await queue.ReceiveMessageAsync(visibilityTimeout);

        if (msg.Value?.Body == null)
            return Array.Empty<BatchItem<T>>();

        var serializedBatch = SerializedMessageBatch<T>.Deserialize(msg.Value.Body.ToString());
        var batchOptions = new MessageBatchOptions(msg.Value.MessageId, msg.Value.PopReceipt, flushPeriod, serializedBatch.Compressed);

        var batch = new MessageBatch<T>(queue, serializedBatch.Items(), batchOptions);

        return batch.Unpack();
    }

    public async Task Create() => await queue.CreateAsync();
    public async Task Delete() => await queue.DeleteAsync();
}
