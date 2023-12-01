using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace AzureBatchQueue;

public class MessageQueue<T>
{
    readonly int maxDequeueCount;
    readonly IMessageQueueSerializer<T> serializer;
    const int MaxMessageSize = 48 * 1024; // 48 KB

    readonly QueueClient queue;
    readonly QueueClient quarantineQueue;
    readonly BlobContainerClient container;

    public MessageQueue(string connectionString, string queueName, int maxDequeueCount = 5, IMessageQueueSerializer<T>? serializer = null)
    {
        this.maxDequeueCount = maxDequeueCount;
        queue = new QueueClient(connectionString, queueName, new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
        quarantineQueue = new QueueClient(connectionString, $"{queueName}-quarantine", new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
        container = new BlobContainerClient(connectionString, $"overflow-{queueName}");

        this.serializer = serializer ?? new JsonSerializer<T>();
    }

    public string Name => queue.Name;

    public async Task Send(T item, TimeSpan? visibilityTimeout = null, CancellationToken ct = default)
    {
        var payload = await Payload(item, ct);

        await queue.SendMessageAsync(new BinaryData(payload), visibilityTimeout, null, ct);
    }

    async Task<byte[]> Payload(T item, CancellationToken ct)
    {
        var payload = serializer.Serialize(item);
        if (payload.Length > MaxMessageSize)
        {
            var blobRef = BlobRef.Create();
            await container.UploadBlobAsync(blobRef.BlobName, new BinaryData(payload), ct);
            payload = JsonSerializer.SerializeToUtf8Bytes(blobRef);
        }

        return payload;
    }

    public async Task DeleteMessage(MessageId id, CancellationToken ct = default)
    {
        await queue.DeleteMessageAsync(id.Id, id.PopReceipt, ct);
        if (id.BlobName is not null)
            await container.DeleteBlobIfExistsAsync(id.BlobName, cancellationToken: ct);
    }

    public async Task UpdateMessage(QueueMessage<T> message, TimeSpan? visibilityTimeout = null)
    {
        var payload = serializer.Serialize(message.Item);

        await queue.UpdateMessageAsync(message.MessageId.Id, message.MessageId.PopReceipt, new BinaryData(payload), visibilityTimeout ?? TimeSpan.Zero);
    }

    public async Task<QueueMessage<T>[]> Receive(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
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

    public async Task<QueueMessage<T>[]> ReceiveFromQuarantine(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
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
        }
        catch (Exception e)
        {
            // log Error
        }
    }

    public async Task QuarantineMessage(QueueMessage<T> msg, CancellationToken ct = default)
    {
        try
        {
            var payload = msg.MessageId.BlobName != null
                ? JsonSerializer.SerializeToUtf8Bytes(new BlobRef(msg.MessageId.BlobName))
                : serializer.Serialize(msg.Item);

            await quarantineQueue.SendMessageAsync(new BinaryData(payload), cancellationToken: ct);
            await queue.DeleteMessageAsync(msg.MessageId.Id, msg.MessageId.PopReceipt, ct);
        }
        catch (Exception e)
        {
            // log Error
        }
    }

    async Task<QueueMessage<T>> ToQueueMessage(QueueMessage m, CancellationToken ct)
    {
        var payload = m.Body.ToMemory();
        if (IsBlobRef(m.Body, out var blobRef))
        {
            var blobData = await container.GetBlobClient(blobRef!.BlobName).DownloadContentAsync(ct);
            payload = blobData.Value.Content.ToMemory();
        }

        var item = serializer.Deserialize(payload);
        var msg = new QueueMessage<T>(item, new MessageId(m.MessageId, m.PopReceipt, blobRef?.BlobName), m.DequeueCount);
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
}

public record QueueMessage<T>(T Item, MessageId MessageId, long DequeueCount = 0);
public record MessageId(string Id, string PopReceipt, string? BlobName);
