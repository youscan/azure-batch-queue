namespace AzureBatchQueue.Exceptions;

public class ItemNotFoundException : Exception
{
    public string BatchItemId { get; }

    public ItemNotFoundException(string msg, string batchItemId) : base(msg)
    {
        BatchItemId = batchItemId;
    }

    public ItemNotFoundException(string batchItemId) : this("BatchItem was not found in batch. Likely it was already completed.", batchItemId) { }
}
