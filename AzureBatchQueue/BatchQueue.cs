using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AzureBatchQueue;

public class BatchQueue<T>
{
    readonly ILogger<BatchQueue<T>> logger;

    readonly TimeSpan flushPeriod;
    readonly TimeSpan receiveVisibilityTimeout;
    readonly MessageQueue<T[]> queue;

    public BatchQueue(string connectionString, string queueName, TimeSpan flushPeriod, ILogger<BatchQueue<T>>? logger = null)
    {
        queue = new MessageQueue<T[]>(connectionString, queueName);

        this.flushPeriod = flushPeriod;
        receiveVisibilityTimeout = flushPeriod.Add(TimeSpan.FromSeconds(5));
        this.logger = logger ?? NullLogger<BatchQueue<T>>.Instance;
    }

    public string Name => queue.Name;

    public async Task Send(T[] items) => await queue.Send(items);

    public async Task<BatchItem<T>[]> Receive(int? maxMessages = null, TimeSpan? visibilityTimeout = null,
        CancellationToken ct = default)
    {
        var arrayOfBatches = await queue.Receive(maxMessages, visibilityTimeout ?? receiveVisibilityTimeout, ct: ct);

        var timerBatches = arrayOfBatches.Select(batch => new TimerBatch<T>(this, batch, flushPeriod, logger)).ToList();

        return timerBatches.SelectMany(x => x.Unpack()).ToArray();
    }

    public async Task Init() => await queue.Init();
    public async Task Delete() => await queue.Delete();

    public async Task DeleteMessage(MessageId msgId) => await queue.DeleteMessage(msgId);
    public async Task UpdateMessage(QueueMessage<T[]> message) => await queue.UpdateMessage(message);
}
