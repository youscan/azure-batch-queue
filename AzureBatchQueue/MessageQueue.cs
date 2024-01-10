using System.Text.Json.Serialization;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace AzureBatchQueue;

public class MessageQueue<T>
{
    readonly int maxDequeueCount;
    ILogger logger;
    readonly IMessageQueueSerializer<T> serializer;
    const int MaxMessageSize = 48 * 1024; // 48 KB
    const int MaxMessagesReceive = 32;

    readonly QueueClient queue;
    readonly QueueClient quarantineQueue;
    readonly BlobContainerClient container;

    public MessageQueue(string connectionString, string queueName,
        int maxDequeueCount = 5,
        IMessageQueueSerializer<T>? serializer = null,
        ILogger? logger = null)
    {
        this.maxDequeueCount = maxDequeueCount;
        queue = new QueueClient(connectionString, queueName, new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
        quarantineQueue = new QueueClient(connectionString, $"{queueName}-quarantine", new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
        container = new BlobContainerClient(connectionString, $"overflow-{queueName}");

        this.serializer = serializer ?? new JsonSerializer<T>();
        this.logger = logger ?? NullLogger.Instance;
    }

    public string Name => queue.Name;

    public async Task Send(T item, TimeSpan? visibilityTimeout = null, CancellationToken ct = default)
    {
        var payload = await Payload(item, ct: ct);

        await queue.SendMessageAsync(new BinaryData(payload.Data), visibilityTimeout ?? TimeSpan.Zero, null, ct);
    }

    async Task<Payload> Payload(T item, CancellationToken ct = default)
    {
        using var payload = new MemoryStream();
        serializer.Serialize(payload, item);

        var data = payload.GetBuffer().AsMemory(0, (int)payload.Position);

        if (payload.Length <= MaxMessageSize)
            return new Payload(data);

        var blobRef = BlobRef.Create();
        await container.UploadBlobAsync(blobRef.BlobName, new BinaryData(data), ct);

        var blobRefMsg = JsonSerializer.SerializeToUtf8Bytes(blobRef);
        return new Payload(blobRefMsg, blobRef.BlobName);
    }

    async Task<Payload> UpdatedPayload(QueueMessage<T> queueMessage, CancellationToken ct = default)
    {
        using var payload = new MemoryStream();
        serializer.Serialize(payload, queueMessage.Item);

        var data = payload.GetBuffer().AsMemory(0, (int)payload.Position);

        if (payload.Length <= MaxMessageSize)
            return new Payload(data);

        // Updated payload is still not small enough for a QueueMessage, will update the blob contents
        var blobName = queueMessage.MessageId.BlobName;
        if (blobName == null)
        {
            throw new Exception("Error when serializing updated QueueMessage payload. Payload size cannot increase.");
        }

        await container.GetBlobClient(blobName).UploadAsync(new BinaryData(data), true, ct);
        var blobRefMsg = JsonSerializer.SerializeToUtf8Bytes(BlobRef.Get(blobName));
        return new Payload(blobRefMsg, blobName);
    }

    public async Task DeleteMessage(MessageId id, CancellationToken ct = default)
    {
        await queue.DeleteMessageAsync(id.Id, id.PopReceipt, ct);
        if (id.BlobName is not null)
            await container.DeleteBlobIfExistsAsync(id.BlobName, cancellationToken: ct);
    }

    public async Task UpdateMessage(QueueMessage<T> msg, TimeSpan? visibilityTimeout = null, CancellationToken ct = default)
    {
        var payload = await UpdatedPayload(msg, ct);

        await queue.UpdateMessageAsync(msg.MessageId.Id, msg.MessageId.PopReceipt, new BinaryData(payload.Data), visibilityTimeout ?? TimeSpan.Zero, ct);

        if (msg.MessageId.BlobName != null && payload.BlobName == null)
            await container.DeleteBlobAsync(msg.MessageId.BlobName, cancellationToken: ct);
    }

    public async Task<QueueMessage<T>[]> Receive(int maxMessages = MaxMessagesReceive, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var r = await queue.ReceiveMessagesAsync(maxMessages, visibilityTimeout, cancellationToken: ct);

        if (!r.HasValue)
            return Array.Empty<QueueMessage<T>>();

        var quarantineMessages = r.Value.Where(x => x.DequeueCount > maxDequeueCount).ToList();
        var validMessages = r.Value.Except(quarantineMessages);

        var quarantine = Task.WhenAll(quarantineMessages.Select(x => QuarantineMessage(x, ct)));
        var response = Task.WhenAll(validMessages.Select(x => ToQueueMessage(x, ct)));

        await Task.WhenAll(quarantine, response);
        return response.Result;
    }

    public async Task<QueueMessage<T>[]> ReceiveFromQuarantine(int maxMessages = MaxMessagesReceive, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var r = await quarantineQueue.ReceiveMessagesAsync(maxMessages, visibilityTimeout, cancellationToken: ct);

        if (!r.HasValue)
            return Array.Empty<QueueMessage<T>>();

        return await Task.WhenAll(r.Value.Select(x => ToQueueMessage(x, ct)));
    }

    async Task QuarantineMessage(QueueMessage queueMessage, CancellationToken ct = default)
    {
        try
        {
            await quarantineQueue.SendMessageAsync(queueMessage.Body, cancellationToken: ct);
            await queue.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt, ct);

            logger.LogInformation("QueueMessage {msgId} with {popReceipt} was quarantined after {dequeueCount} unsuccessful attempts.",
                queueMessage.MessageId, queueMessage.PopReceipt, queueMessage.DequeueCount);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to quarantine queue message with {messageId} and {popReceipt}.",
                queueMessage.MessageId, queueMessage.PopReceipt);
        }
    }

    public async Task Dequarantine(CancellationToken ct = default)
    {
        do
        {
            var response = await quarantineQueue.ReceiveMessagesAsync(MaxMessagesReceive, cancellationToken: ct);

            if (!response.HasValue || response.Value.Length == 0)
                return;

            foreach (var msg in response.Value)
            {
                await queue.SendMessageAsync(msg.Body, TimeSpan.Zero, cancellationToken: ct);
                await quarantineQueue.DeleteMessageAsync(msg.MessageId, msg.PopReceipt, ct);
            }
        } while (!ct.IsCancellationRequested);
    }

    public async Task QuarantineMessage(QueueMessage<T> msg, CancellationToken ct = default)
    {
        try
        {
            var payload = await UpdatedPayload(msg, ct);

            await quarantineQueue.SendMessageAsync(new BinaryData(payload.Data), cancellationToken: ct);
            await queue.DeleteMessageAsync(msg.MessageId.Id, msg.MessageId.PopReceipt, ct);

            if (msg.MessageId.BlobName != null && payload.BlobName == null)
                await container.DeleteBlobAsync(msg.MessageId.BlobName, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to quarantine queue message with {messageId} and {popReceipt}.", msg.MessageId.Id, msg.MessageId.PopReceipt);
        }
    }

    async Task<QueueMessage<T>> ToQueueMessage(QueueMessage m, CancellationToken ct)
    {
        var payload = m.Body.ToMemory();
        if (IsBlobRef(m.Body, out var blobRef))
        {
            try
            {
                var blobData = await container.GetBlobClient(blobRef!.BlobName).DownloadContentAsync(ct);
                payload = blobData.Value.Content.ToMemory();
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                logger.LogError(ex, "Blob with name {blobRef!.BlobName} is not found for {messageId}", blobRef!.BlobName, m.MessageId);
                throw;
            }
        }

        var item = serializer.Deserialize(payload);
        var metadata = new QueueMessageMetadata(m.NextVisibleOn!.Value, m.InsertedOn!.Value);
        var messageId = new MessageId(m.MessageId, m.PopReceipt, blobRef?.BlobName);
        var msg = new QueueMessage<T>(item, messageId, metadata, m.DequeueCount);
        return msg;
    }

    static bool IsBlobRef(BinaryData data, out BlobRef? o)
    {
        try
        {
            o = data.ToObjectFromJson<BlobRef>();
            return o.BlobName != null;
        }
        catch
        {
            o = null;
            return false;
        }
    }

    record BlobRef([property: JsonPropertyName("__MSQ_QUEUE_BLOBNAME__")] string BlobName)
    {
        public static BlobRef Get(string blobName) => new(blobName);
        public static BlobRef Create() => new(FileName());
        static string FileName() => $"{DateTime.UtcNow:yyyy-MM-dd}/{DateTime.UtcNow:s}{Guid.NewGuid():N}.json.gzip";
    }

    public async Task Init() => await Task.WhenAll(queue.CreateIfNotExistsAsync(), quarantineQueue.CreateIfNotExistsAsync(), container.CreateIfNotExistsAsync());

    public async Task Delete(bool deleteBlobs = false)
    {
        var tasks = new List<Task> { queue.DeleteAsync(), quarantineQueue.DeleteAsync() };

        if (deleteBlobs)
            tasks.Add(container.DeleteAsync());

        await Task.WhenAll(tasks);
    }

    public async Task ClearMessages() => await queue.ClearMessagesAsync();

    public MessageQueue<T> WithLogger(ILogger queueLogger)
    {
        logger = queueLogger;
        return this;
    }
}

public record QueueMessage<T>(T Item, MessageId MessageId, QueueMessageMetadata Metadata, long DequeueCount = 0);
public record QueueMessageMetadata(DateTimeOffset VisibilityTime, DateTimeOffset InsertedOn);
public record MessageId(string Id, string PopReceipt, string? BlobName);
public record Payload(ReadOnlyMemory<byte> Data, string? BlobName = null);
