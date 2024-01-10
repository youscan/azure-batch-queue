namespace AzureBatchQueue.Exceptions;

public class BatchCompletedException : Exception
{
    public BatchItemMetadata BatchItemMetadata { get; }
    public BatchCompletedResult BatchCompletedResult { get; }

    public BatchCompletedException(string msg, BatchItemMetadata batchItemMetadata, BatchCompletedResult completedResult) : base(msg)
    {
        BatchItemMetadata = batchItemMetadata;
        BatchCompletedResult = completedResult;
    }
}
