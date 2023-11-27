using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchQueueV2Tests
{
    [Test]
    public async Task When_sending_batch()
    {
        using var queueTest = await Queue<string>(TimeSpan.FromMilliseconds(200));

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
    }

    [Test]
    public async Task When_batch_timer_flushes_after_period()
    {
        var flushPeriod = TimeSpan.FromMilliseconds(500);
        using var queueTest = await Queue<string>(flushPeriod);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
        foreach (var item in response)
        {
            // complete all except 'orange'
            if (item.Item == "orange")
                continue;

            item.Complete();
        }

        // wait for timer to flush
        await Task.Delay(flushPeriod);

        var updatedResponse = await queueTest.BatchQueue.Receive();
        updatedResponse.Single().Item.Should().Be("orange");
    }

    [Test]
    public async Task When_batch_flushes_after_all_complete()
    {
        var longFlushPeriod = TimeSpan.FromMinutes(10);
        using var queueTest = await Queue<string>(longFlushPeriod);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(1);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        foreach (var item in response)
            item.Complete();

        // wait for message to become available for read if it's not completed
        await Task.Delay(visibilityTimeout);

        var updatedResponse = await queueTest.BatchQueue.Receive();
        updatedResponse.Should().BeEmpty();
    }

    static async Task<BatchQueueTest<T>> Queue<T>(TimeSpan flushPeriod)
    {
        var queue = new BatchQueueTest<T>();
        await queue.Init(flushPeriod);
        return queue;
    }

    class BatchQueueTest<T> : IDisposable
    {
        public async Task Init(TimeSpan flushPeriod)
        {
            BatchQueue = new BatchQueueV2<T>("UseDevelopmentStorage=true", "batch-test", flushPeriod);
            await BatchQueue.Init();
        }

        public BatchQueueV2<T> BatchQueue { get; private set; }
        public void Dispose() => BatchQueue.Delete().GetAwaiter().GetResult();
    }
}
