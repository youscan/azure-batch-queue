using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace AzureBatchQueue;

public class MessageQueue<T>
{
    readonly IMessageQueueSerializer<T> serializer;
    const int MaxMessageSize = 48 * 1024; // 48 KB

    readonly QueueClient queue;
    readonly BlobContainerClient container;

    public MessageQueue(string connectionString, string queueName, IMessageQueueSerializer<T>? serializer = null)
    {
        queue = new QueueClient(connectionString, queueName, new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
        container = new BlobContainerClient(connectionString, $"overflow-{queueName}");

        this.serializer = serializer ?? new JsonSerializer<T>();
    }

    public string Name => queue.Name;

    public async Task Send(T item, CancellationToken ct = default)
    {
        var payload = await Payload(item, ct);

        await queue.SendMessageAsync(new BinaryData(payload), null, null, ct);
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
        return await Task.WhenAll(r.Value.Select(x => ToQueueMessage(x, ct)));
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
        public static BlobRef Create() => new(Guid.NewGuid().ToString("N"));
    }

    public async Task Init() => await Task.WhenAll(queue.CreateIfNotExistsAsync(), container.CreateIfNotExistsAsync());

    public async Task Delete()
    {
        await queue.DeleteAsync();
        await container.DeleteIfExistsAsync();
    }
}

public record QueueMessage<T>(T Item, MessageId MessageId, long DequeueCount = 0);
public record MessageId(string Id, string PopReceipt, string? BlobName);
