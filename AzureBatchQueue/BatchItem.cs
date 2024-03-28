namespace AzureBatchQueue;

public class BatchItem<T>
{
    internal BatchItem(BatchItemId id, TimerBatch<T> batch, T item)
    {
        Id = id;
        Batch = batch;
        Item = item;
        Metadata = new BatchItemMetadata(id.ToString(), Batch.MessageId, Batch.Metadata.VisibilityTime, Batch.FlushPeriod, Batch.Metadata.InsertedOn);
    }

    public BatchItemId Id { get; }
    public T Item { get; }
    TimerBatch<T> Batch { get; }

    public BatchItemMetadata Metadata { get; }

    public void Complete() => Batch.Complete(Id);
    public void Fail() => Batch.Fail(Id);
    public void Delay(TimeSpan delay) => Batch.Delay(Id, delay);
}

public record BatchItemId(string BatchId, int Idx)
{
    public override string ToString() => $"{BatchId}_{Idx}";
};

public record BatchItemMetadata(string BatchItemId, MessageId BatchId, DateTimeOffset VisibilityTime, TimeSpan FlushPeriod, DateTimeOffset InsertedOn);
