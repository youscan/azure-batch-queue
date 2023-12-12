namespace AzureBatchQueue;

public class BatchItem<T>
{
    internal BatchItem(string id, TimerBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
    }

    public string Id { get; }
    public string BatchId => Batch.MessageId;
    public T Item { get; }
    TimerBatch<T> Batch { get; }

    public bool Complete() => Batch.Complete(Id);
}
