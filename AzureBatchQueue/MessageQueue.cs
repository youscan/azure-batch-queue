using System.Text.Json.Serialization;
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

    public async Task Send(T item, TimeSpan? visibilityTimeout = null, bool doubleCheckSerialization = false, CancellationToken ct = default)
    {
        var (data, offloaded) = await SerializeAndOffloadIfBig(item, ct: ct);
        await queue.SendMessageAsync(new BinaryData(data), visibilityTimeout ?? TimeSpan.Zero, null, ct);

        // Don't ask. Checking our sanity
        if (!offloaded && doubleCheckSerialization)
            serializer.Deserialize(data.Span);
    }

    public async Task DeleteMessage(MessageId id, CancellationToken ct = default)
    {
        await queue.DeleteMessageAsync(id.Id, id.PopReceipt, ct);
        if (id.BlobName is not null)
            await container.DeleteBlobIfExistsAsync(id.BlobName, cancellationToken: ct);
    }

    public async Task UpdateMessage(MessageId id, T? item, TimeSpan visibilityTimeout = default, CancellationToken ct = default)
    {
        if (item == null)
        {
            await queue.UpdateMessageAsync(id.Id, id.PopReceipt, (BinaryData?)null, visibilityTimeout, ct);
            return;
        }

        var (data, offloaded) = await SerializeAndOffloadIfBig(item, id.BlobName, ct);
        await queue.UpdateMessageAsync(id.Id, id.PopReceipt, new BinaryData(data), visibilityTimeout, ct);
        if (!offloaded && id.BlobName != null)
            await container.DeleteBlobAsync(id.BlobName, cancellationToken: ct);
    }

    async Task<Payload> SerializeAndOffloadIfBig(T item, string? blobName = null, CancellationToken ct = default)
    {
        using var stream = MemoryStreamManager.RecyclableMemory.GetStream("QueueMessage");
        serializer.Serialize(stream, item);

        if (stream.Length <= MaxMessageSize)
            return new Payload(stream.GetBuffer().AsMemory(0, (int)stream.Length), false);

        var blobRef = blobName != null ? BlobRef.Get(blobName) : BlobRef.Create();
        stream.Position = 0;
        await container.GetBlobClient(blobRef.BlobName).UploadAsync(stream, overwrite: true, ct);

        return new Payload(JsonSerializer.SerializeToUtf8Bytes(blobRef), true);
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
        return response.Result.Where(x => x != null).ToArray()!;
    }

    public async Task<QueueMessage<T>[]> ReceiveFromQuarantine(int maxMessages = MaxMessagesReceive, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var r = await quarantineQueue.ReceiveMessagesAsync(maxMessages, visibilityTimeout, cancellationToken: ct);

        if (!r.HasValue)
            return Array.Empty<QueueMessage<T>>();

        var queueMessages = await Task.WhenAll(r.Value.Select(x => ToQueueMessage(x, ct, fromQuarantine: true)));

        return queueMessages.Where(x => x != null).ToArray()!;
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
            throw;
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

    public async Task QuarantineData(T item, CancellationToken ct)
    {
        var payload = await SerializeAndOffloadIfBig(item, ct: ct);

        await quarantineQueue.SendMessageAsync(new BinaryData(payload.Data), cancellationToken: ct);
    }

    async Task<QueueMessage<T>?> ToQueueMessage(QueueMessage m, CancellationToken ct, bool fromQuarantine = false)
    {
        var payload = m.Body;
        if (IsBlobRef(m.Body, out var blobRef))
        {
            try
            {
                var blobData = await container.GetBlobClient(blobRef!.BlobName).DownloadContentAsync(ct);
                payload = blobData.Value.Content;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Exception when loading blob {BlobName} for {MessageId}", blobRef!.BlobName, m.MessageId);
                throw;
            }
        }

        T? item;
        try
        {
            item = serializer.Deserialize(payload)!;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Exception when deserializing a message {MessageId}", m.MessageId);

            if (!fromQuarantine)
                await QuarantineMessage(m, ct);

            return null;
        }

        var metadata = new QueueMessageMetadata(m.NextVisibleOn!.Value, m.InsertedOn!.Value, m.DequeueCount);
        var messageId = new MessageId(m.MessageId, m.PopReceipt, blobRef?.BlobName);
        var msg = new QueueMessage<T>(item, messageId, metadata);
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

public record QueueMessage<T>(T Item, MessageId MessageId, QueueMessageMetadata Metadata);
public record QueueMessageMetadata(DateTimeOffset VisibilityTime, DateTimeOffset InsertedOn, long DequeueCount = 0);
public record MessageId(string Id, string PopReceipt, string? BlobName);
public record Payload(ReadOnlyMemory<byte> Data, bool Offloaded);
