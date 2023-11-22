namespace AzureBatchQueue;

public class BatchItem<T>
{
    public BatchItem(string id, QueueMessageBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
    }

    public string Id { get; }
    QueueMessageBatch<T> Batch { get; }
    public T Item;

    public async Task Complete()
    {
        await Batch.Complete(Id);
    }
}
