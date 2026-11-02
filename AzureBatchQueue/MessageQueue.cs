using System.Text.Json.Serialization;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
        var payload = await SerializeAndOffloadIfBig(item, ct: ct);
        await SendPayload(queue, payload.Data, payload.BlobName, ct, visibilityTimeout ?? TimeSpan.Zero);
    }

    public async Task DeleteMessage(MessageId id, CancellationToken ct = default)
    {
        await DeleteQueueMessage(queue, id, ct);
        if (id.BlobName is not null)
            await DeleteBlob(id, ct);
    }

    public async Task UpdateMessage(MessageId id, T? item, TimeSpan visibilityTimeout = default, CancellationToken ct = default)
    {
        logger.UpdatingMessage(id.Id, queue.Name, id.BlobName);
        if (item == null)
        {
            await queue.UpdateMessageAsync(id.Id, id.PopReceipt, (BinaryData?)null, visibilityTimeout, ct);
            logger.MessageUpdated(id.Id, queue.Name, id.BlobName, id.BlobName);
            return;
        }

        var payload = await SerializeAndOffloadIfBig(item, id.BlobName, ct);
        await queue.UpdateMessageAsync(id.Id, id.PopReceipt, payload.Data, visibilityTimeout, ct);
        logger.MessageUpdated(id.Id, queue.Name, payload.BlobName, id.BlobName);
        if (!payload.Offloaded && id.BlobName != null)
            await DeleteBlob(id, ct);
    }

    async Task<Payload> SerializeAndOffloadIfBig(T item, string? blobName = null, CancellationToken ct = default)
    {
        using var stream = MemoryStreamManager.RecyclableMemory.GetStream("QueueMessage");
        serializer.Serialize(stream, item);

        stream.Position = 0;

        if (stream.Length <= MaxMessageSize)
            return Payload.FitsInMessage(stream);

        var blobRef = blobName != null ? BlobRef.Get(blobName) : BlobRef.Create();
        logger.UploadingBlob(blobRef.BlobName, queue.Name, stream.Length);
        var uploaded = await container.GetBlobClient(blobRef.BlobName).UploadAsync(stream, overwrite: true, ct);
        logger.BlobUploaded(blobRef.BlobName, queue.Name, uploaded.Value.ETag.ToString(), uploaded.GetRawResponse().Headers.RequestId);
        return Payload.OffloadedToBlob(blobRef);
    }

    public async Task<QueueMessage<T>[]> Receive(int maxMessages = MaxMessagesReceive, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var r = await queue.ReceiveMessagesAsync(maxMessages, visibilityTimeout, cancellationToken: ct);

        if (!r.HasValue)
            return Array.Empty<QueueMessage<T>>();

        LogReceivedMessages(queue, r.Value);
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

        LogReceivedMessages(quarantineQueue, r.Value);
        var queueMessages = await Task.WhenAll(r.Value.Select(x => ToQueueMessage(x, ct, fromQuarantine: true)));

        return queueMessages.Where(x => x != null).ToArray()!;
    }

    async Task QuarantineMessage(QueueMessage queueMessage, CancellationToken ct = default)
    {
        BlobRef? blobRef = null;
        try
        {
            BinaryData messageBody;
            string? quarantineBlobName = null;

            if (IsBlobRef(queueMessage.Body, out blobRef) && blobRef != null)
            {
                var newBlobRef = await TryCopyBlobForQuarantine(blobRef, queueMessage, ct);
                if (newBlobRef == null)
                    return;

                messageBody = Payload.OffloadedToBlob(newBlobRef).Data;
                quarantineBlobName = newBlobRef.BlobName;
            }
            else
            {
                messageBody = queueMessage.Body;
            }

            await SendPayload(quarantineQueue, messageBody, quarantineBlobName, ct);

            var messageId = new MessageId(queueMessage.MessageId, queueMessage.PopReceipt, blobRef?.BlobName);
            await DeleteMessage(messageId, ct);

            logger.MessageQuarantined(queueMessage.MessageId, queueMessage.PopReceipt, queueMessage.DequeueCount);
        }
        catch (Exception ex)
        {
            logger.QuarantineFailed(queueMessage.MessageId, queueMessage.PopReceipt, blobRef?.BlobName, ex);
        }
    }

    // Why not just move the message from main queue to quarantine queue?
    // Because on message quarantine, we can put the message into quarantine queue, and then the service stops and we don't delete it from the main queue.
    // In this case, later we will insert a duplicate message to quarantine, with the same ref to blob.
    // When one message is processed, we remove the blob. The other message will have a BlobNotFound error.
    // So it's better to do some more heavy-lifting and copy blob content on quarantine.
    async Task<BlobRef?> TryCopyBlobForQuarantine(BlobRef sourceBlobRef, QueueMessage message, CancellationToken ct)
    {
        var sourceBlobClient = container.GetBlobClient(sourceBlobRef.BlobName);
        var newBlobRef = BlobRef.Create();
        var destinationBlobClient = container.GetBlobClient(newBlobRef.BlobName);

        BinaryData content;
        try
        {
            var downloaded = await sourceBlobClient.DownloadContentAsync(ct);
            content = downloaded.Value.Content;
            logger.BlobDownloaded(sourceBlobRef.BlobName, message.MessageId, queue.Name,
                downloaded.Value.Details.ETag.ToString(), downloaded.GetRawResponse().Headers.RequestId);
        }
        catch (RequestFailedException ex) when (ex is { Status: 404, ErrorCode: "BlobNotFound" })
        {
            await DeleteMissingBlobMessage(message, sourceBlobRef, ex, ct);
            return null;
        }
        logger.UploadingBlob(newBlobRef.BlobName, queue.Name, content.ToMemory().Length);
        var uploaded = await destinationBlobClient.UploadAsync(content, ct);
        logger.BlobUploaded(newBlobRef.BlobName, queue.Name, uploaded.Value.ETag.ToString(), uploaded.GetRawResponse().Headers.RequestId);

        logger.BlobCopiedForQuarantine(sourceBlobRef.BlobName, newBlobRef.BlobName, message.MessageId);

        return newBlobRef;
    }

    public async Task Dequarantine(CancellationToken ct = default)
    {
        do
        {
            var response = await quarantineQueue.ReceiveMessagesAsync(MaxMessagesReceive, cancellationToken: ct);

            if (!response.HasValue || response.Value.Length == 0)
                return;

            LogReceivedMessages(quarantineQueue, response.Value);
            foreach (var msg in response.Value)
            {
                IsBlobRef(msg.Body, out var blobRef);
                await SendPayload(queue, msg.Body, blobRef?.BlobName, ct, TimeSpan.Zero);
                await DeleteQueueMessage(quarantineQueue, new MessageId(msg.MessageId, msg.PopReceipt, blobRef?.BlobName), ct);
            }
        } while (!ct.IsCancellationRequested);
    }

    public async Task QuarantineData(T item, CancellationToken ct)
    {
        var payload = await SerializeAndOffloadIfBig(item, ct: ct);
        await SendPayload(quarantineQueue, payload.Data, payload.BlobName, ct);
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
                logger.BlobDownloaded(blobRef.BlobName, m.MessageId, fromQuarantine ? quarantineQueue.Name : queue.Name,
                    blobData.Value.Details.ETag.ToString(), blobData.GetRawResponse().Headers.RequestId);
            }
            catch (RequestFailedException ex) when (!fromQuarantine && ex is { Status: 404, ErrorCode: "BlobNotFound" })
            {
                await DeleteMissingBlobMessage(m, blobRef!, ex, ct);
                return null;
            }
            catch (Exception ex)
            {
                logger.BlobDownloadFailed(blobRef!.BlobName, m.MessageId, ex);
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
            logger.MessageDeserializationFailed(m.MessageId, ex);

            if (!fromQuarantine)
                await QuarantineMessage(m, ct);

            return null;
        }

        var metadata = new QueueMessageMetadata(m.NextVisibleOn!.Value, m.InsertedOn!.Value, m.DequeueCount);
        var messageId = new MessageId(m.MessageId, m.PopReceipt, blobRef?.BlobName);
        var msg = new QueueMessage<T>(item, messageId, metadata);
        return msg;
    }

    async Task DeleteMissingBlobMessage(QueueMessage message, BlobRef blobRef, RequestFailedException error, CancellationToken ct)
    {
        await DeleteQueueMessage(queue, new MessageId(message.MessageId, message.PopReceipt, blobRef.BlobName), ct);

        logger.MissingBlobMessageDeleted(message.MessageId, queue.Name, blobRef.BlobName, error);
    }

    async Task SendPayload(QueueClient target, BinaryData data, string? blobName, CancellationToken ct, TimeSpan? visibilityTimeout = null)
    {
        var operationId = Guid.NewGuid();
        logger.SendingMessage(target.Name, blobName, operationId);
        try
        {
            var sent = await target.SendMessageAsync(data, visibilityTimeout, cancellationToken: ct);
            logger.MessageSent(sent.Value.MessageId, target.Name, blobName, operationId, sent.GetRawResponse().Headers.RequestId);
        }
        catch (RequestFailedException ex)
        {
            logger.MessageSendFailed(ex, target.Name, blobName, operationId, ex.Status, ex.ErrorCode, ex.GetRawResponse()?.Headers.RequestId);
            throw;
        }
    }

    async Task DeleteQueueMessage(QueueClient source, MessageId id, CancellationToken ct)
    {
        logger.DeletingMessage(id.Id, source.Name, id.BlobName);
        await source.DeleteMessageAsync(id.Id, id.PopReceipt, ct);
        logger.MessageDeleted(id.Id, source.Name, id.BlobName);
    }

    async Task DeleteBlob(MessageId id, CancellationToken ct)
    {
        logger.DeletingBlob(id.BlobName!, id.Id, queue.Name);
        var deleted = await container.DeleteBlobIfExistsAsync(id.BlobName!, cancellationToken: ct);
        logger.BlobDeleted(id.BlobName!, id.Id, queue.Name, deleted.Value, deleted.GetRawResponse()?.Headers.RequestId);
    }

    void LogReceivedMessages(QueueClient source, QueueMessage[] messages)
    {
        if (!logger.IsEnabled(LogLevel.Information))
            return;

        foreach (var message in messages)
        {
            IsBlobRef(message.Body, out var blobRef);
            logger.MessageReceived(message.MessageId, source.Name, blobRef?.BlobName, message.DequeueCount);
        }
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

    public async Task Init() => await Task.WhenAll(queue.CreateIfNotExistsAsync(), quarantineQueue.CreateIfNotExistsAsync(), container.CreateIfNotExistsAsync());

    public async Task Delete(bool deleteBlobs = false)
    {
        logger.DeletingQueueStorage(queue.Name, deleteBlobs);
        var tasks = new List<Task> { queue.DeleteAsync(), quarantineQueue.DeleteAsync() };

        if (deleteBlobs)
            tasks.Add(container.DeleteAsync());

        await Task.WhenAll(tasks);
        logger.QueueStorageDeleted(queue.Name, deleteBlobs);
    }

    public async Task ClearMessages() => await queue.ClearMessagesAsync();

    public MessageQueue<T> WithLogger(ILogger queueLogger)
    {
        logger = queueLogger;
        return this;
    }

    record BlobRef([property: JsonPropertyName("__MSQ_QUEUE_BLOBNAME__")] string BlobName)
    {
        public static BlobRef Get(string blobName) => new(blobName);
        public static BlobRef Create() => new(FileName());
        static string FileName() => $"{DateTime.UtcNow:yyyy-MM-dd}/{DateTime.UtcNow:s}{Guid.NewGuid():N}.json.gzip";
    }

    record Payload(BinaryData Data, bool Offloaded, string? BlobName = null)
    {
        public static Payload FitsInMessage(MemoryStream stream) =>
            new(BinaryData.FromStream(stream), false);

        public static Payload OffloadedToBlob(BlobRef blobRef) =>
            new(BinaryData.FromObjectAsJson(blobRef), true, blobRef.BlobName);
    }
}

public record QueueMessage<T>(T Item, MessageId MessageId, QueueMessageMetadata Metadata);
public record QueueMessageMetadata(DateTimeOffset VisibilityTime, DateTimeOffset InsertedOn, long DequeueCount = 0);
public record MessageId(string Id, string PopReceipt, string? BlobName);
