using Microsoft.Extensions.Logging;

namespace AzureBatchQueue.JsonNet;

public static class BatchQueueCompressed<T>
{
    public static BatchQueue<T> New(string connectionString, string queueName, int maxDequeueCount = 5, ILogger? logger = null) =>
        new(connectionString, queueName, maxDequeueCount, new BatchQueueCompressedSerializer<T[]>(), logger);
}
