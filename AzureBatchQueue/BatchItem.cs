namespace AzureBatchQueue;

public class BatchItem<T>
{
    internal BatchItem(string id, TimerBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
        Metadata = new BatchItemMetadata(id, Batch.MessageId, Batch.Metadata.VisibilityTime, Batch.FlushPeriod, Batch.Metadata.InsertedOn);
    }

    public string Id { get; }
    public T Item { get; }
    TimerBatch<T> Batch { get; }

    public BatchItemMetadata Metadata { get; }

    public BatchItemCompleteResult Complete() => Batch.Complete(Id);
}

public record BatchItemMetadata(string BatchItemId, MessageId BatchId, DateTimeOffset VisibilityTime, TimeSpan FlushPeriod, DateTimeOffset InsertedOn);
