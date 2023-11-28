namespace AzureBatchQueue;

public class BatchItem<T>
{
    public BatchItem(string id, TimerBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
    }

    public string Id { get; }
    public T Item { get; }
    TimerBatch<T> Batch { get; }

    public void Complete() => Batch.Complete(Id);
}
