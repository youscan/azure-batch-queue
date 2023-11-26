namespace AzureBatchQueue;

public class BatchQueueV2<T>
{
    readonly TimeSpan flushPeriod;
    readonly MessageQueue<T[]> queue;

    public BatchQueueV2(string connectionString, string queueName, TimeSpan flushPeriod)
    {
        queue = new MessageQueue<T[]>(connectionString, queueName);

        this.flushPeriod = flushPeriod;
    }

    public async Task Send(T[] items) => await queue.Send(items);

    public async Task<BatchItemV2<T>[]> Receive(int? maxMessages = null, CancellationToken ct = default)
    {
        var arrayOfBatches = await queue.Receive(maxMessages, ct);

        var timerBatches = arrayOfBatches.Select(batch => new TimerBatch<T>(this, batch, flushPeriod)).ToList();

        return timerBatches.SelectMany(x => x.Unpack()).ToArray();
    }

    public async Task Init() => await queue.Init();
    public async Task Delete() => await queue.Delete();

    public async Task DeleteMessage(MessageId msgId) => await queue.DeleteMessage(msgId);
    public async Task UpdateMessage(QueueMessage<T[]> message) => await queue.UpdateMessage(message);
}

public class BatchItemV2<T>
{
    public BatchItemV2(string id, TimerBatch<T> batch, T item)
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

public class TimerBatch<T>
{
    readonly BatchQueueV2<T> batchQueue;
    readonly QueueMessage<T[]> msg;
    readonly TimeSpan flushPeriod;
    readonly HashSet<BatchItemV2<T>> items;
    readonly Timer timer;

    public TimerBatch(BatchQueueV2<T> batchQueue, QueueMessage<T[]> msg, TimeSpan flushPeriod)
    {
        this.batchQueue = batchQueue;
        this.msg = msg;
        this.flushPeriod = flushPeriod;
        items = msg.Item.Select((x, idx) => new BatchItemV2<T>($"{msg.MessageId.Id}_{idx}", this, x)).ToHashSet();
        timer = new Timer(async _ => await Flush());
    }

    async Task Flush()
    {
        await timer.DisposeAsync();

        if (!items.Any())
        {
            await batchQueue.DeleteMessage(msg.MessageId);
        }
        else
        {
            await batchQueue.UpdateMessage(Message());
        }
    }

    QueueMessage<T[]> Message()
    {
        var notCompletedItems = items.Select(x => x.Item).ToArray();
        return new QueueMessage<T[]>(notCompletedItems, msg.MessageId);
    }

    public void Complete(string itemId)
    {
        items.RemoveWhere(x => x.Id == itemId);
    }

    public IEnumerable<BatchItemV2<T>> Unpack()
    {
        timer.Change(flushPeriod, Timeout.InfiniteTimeSpan);
        return items.ToArray();
    }
}
