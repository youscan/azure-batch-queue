using Microsoft.Extensions.Logging;

namespace AzureBatchQueue;

internal static partial class QueueLog
{
    [LoggerMessage(1001, LogLevel.Warning, "Deleted message {MessageId} from queue {QueueName} because blob {BlobName} was not found.")]
    public static partial void MissingBlobMessageDeleted(this ILogger logger, string messageId, string queueName, string blobName, Exception exception);

    [LoggerMessage(1002, LogLevel.Information, "QueueMessage {msgId} with {popReceipt} was quarantined after {dequeueCount} unsuccessful attempts.")]
    public static partial void MessageQuarantined(this ILogger logger, string msgId, string popReceipt, long dequeueCount);

    [LoggerMessage(1003, LogLevel.Error, "Failed to quarantine queue message with {messageId}, {popReceipt}, and {blobName}.")]
    public static partial void QuarantineFailed(this ILogger logger, string messageId, string popReceipt, string? blobName, Exception exception);

    [LoggerMessage(1004, LogLevel.Information, "Copied blob {SourceBlob} to {DestBlob} for quarantine message {MsgId}")]
    public static partial void BlobCopiedForQuarantine(this ILogger logger, string sourceBlob, string destBlob, string msgId);

    [LoggerMessage(1005, LogLevel.Error, "Exception when loading blob {BlobName} for {MessageId}")]
    public static partial void BlobDownloadFailed(this ILogger logger, string blobName, string messageId, Exception exception);

    [LoggerMessage(1006, LogLevel.Error, "Exception when deserializing a message {MessageId}")]
    public static partial void MessageDeserializationFailed(this ILogger logger, string messageId, Exception exception);

    [LoggerMessage(1007, LogLevel.Warning, "Queue {queueName} was not found when flushing {messageId} with {itemsCount} items left.")]
    public static partial void QueueNotFoundOnFlush(this ILogger logger, Exception exception, string queueName, MessageId messageId, int itemsCount);

    [LoggerMessage(1008, LogLevel.Error, "Accessing already flushed message with {messageId}.")]
    public static partial void FlushedMessageAccessed(this ILogger logger, Exception exception, MessageId messageId);

    [LoggerMessage(1009, LogLevel.Error, "Unhandled exception when flushing message {messageId}.")]
    public static partial void FlushFailed(this ILogger logger, Exception exception, MessageId messageId);

    [LoggerMessage(1010, LogLevel.Warning, "Message {MsgId} was not fully processed within a timeout ({FlushPeriod}) sec in queue {QueueName}." +
        " {RemainingCount} items were not completed ({NotProcessed} not processed on time and {FailedCount} failed) from {TotalCount} total")]
    public static partial void BatchIncomplete(this ILogger logger, MessageId msgId, double flushPeriod, string queueName,
        int remainingCount, int notProcessed, int failedCount, int totalCount);

    [LoggerMessage(1011, LogLevel.Debug, "Message {msgId} was fully processed with {totalCount} items")]
    public static partial void BatchProcessed(this ILogger logger, MessageId msgId, int totalCount);

    [LoggerMessage(1012, LogLevel.Information, "Message {MsgId} was quarantined after {DequeueCount} unsuccessful attempts in queue {QueueName}." +
        " {RemainingCount} items were not completed ({NotProcessed} not processed on time and {FailedCount} failed) from {TotalCount} total")]
    public static partial void BatchQuarantined(this ILogger logger, MessageId msgId, long dequeueCount, string queueName,
        int remainingCount, int notProcessed, int failedCount, int totalCount);

    [LoggerMessage(1013, LogLevel.Error, "VisibilityTimeout {VisibilityTimeout} cannot be less that 1 sec." +
        " UtcNow: {UtcNow}, MessageVisibleTime: {MessageVisibleTime}, InsertedOn: {InsertedOn}")]
    public static partial void InvalidVisibilityTimeout(this ILogger logger, TimeSpan visibilityTimeout, string utcNow,
        string messageVisibleTime, string insertedOn);

    [LoggerMessage(1101, LogLevel.Information, "Uploading blob {BlobName} for queue {QueueName} with {ContentLength} bytes")]
    public static partial void UploadingBlob(this ILogger logger, string blobName, string queueName, long contentLength);

    [LoggerMessage(1102, LogLevel.Information, "Uploaded blob {BlobName} for queue {QueueName}; ETag {ETag}, Azure request {RequestId}")]
    public static partial void BlobUploaded(this ILogger logger, string blobName, string queueName, string eTag, string? requestId);

    [LoggerMessage(1103, LogLevel.Information, "Sending message to queue {QueueName} referencing blob {BlobName}; operation {OperationId}")]
    public static partial void SendingMessage(this ILogger logger, string queueName, string? blobName, Guid operationId);

    [LoggerMessage(1104, LogLevel.Information, "Sent message {MessageId} to queue {QueueName} referencing blob {BlobName}; operation {OperationId}, Azure request {RequestId}")]
    public static partial void MessageSent(this ILogger logger, string messageId, string queueName, string? blobName, Guid operationId, string? requestId);

    [LoggerMessage(1105, LogLevel.Information, "Received message {MessageId} from queue {QueueName} referencing blob {BlobName}; dequeue count {DequeueCount}")]
    public static partial void MessageReceived(this ILogger logger, string messageId, string queueName, string? blobName, long dequeueCount);

    [LoggerMessage(1106, LogLevel.Information, "Deleting message {MessageId} from queue {QueueName} referencing blob {BlobName}")]
    public static partial void DeletingMessage(this ILogger logger, string messageId, string queueName, string? blobName);

    [LoggerMessage(1107, LogLevel.Information, "Deleted message {MessageId} from queue {QueueName} referencing blob {BlobName}")]
    public static partial void MessageDeleted(this ILogger logger, string messageId, string queueName, string? blobName);

    [LoggerMessage(1108, LogLevel.Information, "Deleting blob {BlobName} for message {MessageId} in queue {QueueName}")]
    public static partial void DeletingBlob(this ILogger logger, string blobName, string messageId, string queueName);

    [LoggerMessage(1109, LogLevel.Information, "Deleted blob {BlobName} for message {MessageId} in queue {QueueName}; deleted {Deleted}, Azure request {RequestId}")]
    public static partial void BlobDeleted(this ILogger logger, string blobName, string messageId, string queueName, bool deleted, string? requestId);

    [LoggerMessage(1110, LogLevel.Information, "Updating message {MessageId} in queue {QueueName} referencing blob {BlobName}")]
    public static partial void UpdatingMessage(this ILogger logger, string messageId, string queueName, string? blobName);

    [LoggerMessage(1111, LogLevel.Information, "Updated message {MessageId} in queue {QueueName} referencing blob {BlobName}; previous blob {PreviousBlobName}")]
    public static partial void MessageUpdated(this ILogger logger, string messageId, string queueName, string? blobName, string? previousBlobName);

    [LoggerMessage(1112, LogLevel.Error, "Failed sending message to queue {QueueName} referencing blob {BlobName}; operation {OperationId}, status {Status}, code {ErrorCode}, Azure request {RequestId}")]
    public static partial void MessageSendFailed(this ILogger logger, Exception exception, string queueName, string? blobName,
        Guid operationId, int status, string? errorCode, string? requestId);

    [LoggerMessage(1113, LogLevel.Information, "Deleting storage for queue {QueueName}; delete blob container {DeleteBlobs}")]
    public static partial void DeletingQueueStorage(this ILogger logger, string queueName, bool deleteBlobs);

    [LoggerMessage(1114, LogLevel.Information, "Deleted storage for queue {QueueName}; delete blob container {DeleteBlobs}")]
    public static partial void QueueStorageDeleted(this ILogger logger, string queueName, bool deleteBlobs);

    [LoggerMessage(1115, LogLevel.Information, "Downloaded blob {BlobName} for message {MessageId} from queue {QueueName}; ETag {ETag}, Azure request {RequestId}")]
    public static partial void BlobDownloaded(this ILogger logger, string blobName, string messageId, string queueName, string eTag, string? requestId);
}
