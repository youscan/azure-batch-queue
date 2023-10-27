namespace AzureBatchQueue;

public class BatchItem<T>
{
    public BatchItem(Guid id, MessageBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
    }

    public Guid Id { get; }
    private MessageBatch<T> Batch { get; }
    public T Item;

    public async Task Complete()
    {
        await Batch.Complete(Id);
    }
}