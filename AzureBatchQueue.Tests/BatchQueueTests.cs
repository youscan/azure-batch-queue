using Azure.Storage.Queues.Models;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchQueueTests
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
        await Task.Delay(flushPeriod * 2);

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

    [Test]
    public async Task When_many_threads_complete_in_parallel()
    {
        var shortFlushPeriod = TimeSpan.FromSeconds(1);
        using var queueTest = await Queue<string>(shortFlushPeriod);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        Assert.DoesNotThrow(() => Parallel.ForEach(response, batchItem => batchItem.Complete()));
    }

    [TestCase(10)]
    [TestCase(100)]
    [TestCase(500)]
    public async Task When_many_clients_complete_in_parallel(int parallelCount)
    {
        var shortFlushPeriod = TimeSpan.FromSeconds(1);
        using var queueTest = await Queue<string>(shortFlushPeriod);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        var tasks = new Task[parallelCount];

        for (var i = 0; i < parallelCount; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                foreach (var item in response)
                    item.Complete();
            });
        }

        Assert.DoesNotThrowAsync(async () => await Task.WhenAll(tasks));
    }

    [Test]
    public async Task When_quarantine_whole_batch()
    {
        const int maxDequeueCount = 1;
        var flushPeriod = TimeSpan.FromMilliseconds(20);
        using var queueTest = await Queue<string>(flushPeriod, maxDequeueCount);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        // do not complete any items from the batch

        // wait for message to be quarantined
        await Task.Delay(flushPeriod * 3);

        var responseFromQuarantine = await queueTest.BatchQueue.ReceiveFromQuarantine();
        responseFromQuarantine.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
    }

    [Test]
    public async Task When_quarantine_only_not_completed_items_from_batch()
    {
        const int maxDequeueCount = 1;
        var flushPeriod = TimeSpan.FromMilliseconds(200);
        using var queueTest = await Queue<string>(flushPeriod, maxDequeueCount);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        const string failedItem = "orange";
        foreach (var item in response)
        {
            if (item.Item == failedItem)
                continue;

            item.Complete();
        }

        // wait for message to be quarantined
        await Task.Delay(flushPeriod * 3);

        var responseFromQuarantine = await queueTest.BatchQueue.ReceiveFromQuarantine();
        responseFromQuarantine.Select(x => x.Item).Should().BeEquivalentTo(failedItem);
    }

    [Test]
    public async Task When_completing_same_item_twice()
    {
        using var queueTest = await Queue<string>(TimeSpan.FromMilliseconds(200));

        var messageBatch = new[] { "orange" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();

        var completeFirst = response.Single().Complete();
        var completeSecond = response.Single().Complete();

        completeFirst.Should().BeTrue();
        completeSecond.Should().BeFalse();
    }

    [Test]
    public async Task When_completing_item_after_batch_was_flushed()
    {
        var flushPeriod = TimeSpan.FromMilliseconds(200);
        using var queueTest = await Queue<string>(flushPeriod);

        var messageBatch = new[] { "orange" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();

        // wait for batch to be flushed
        await Task.Delay(flushPeriod * 2);

        Assert.Throws<BatchCompletedException>(() => response.Single().Complete());
    }

    static async Task<BatchQueueTest<T>> Queue<T>(TimeSpan flushPeriod, int maxDequeueCount = 5)
    {
        var queue = new BatchQueueTest<T>();
        await queue.Init(flushPeriod, maxDequeueCount);
        return queue;
    }

    class BatchQueueTest<T> : IDisposable
    {
        public async Task Init(TimeSpan flushPeriod, int maxDequeueCount)
        {
            BatchQueue = new BatchQueue<T>("UseDevelopmentStorage=true", "batch-test", flushPeriod, maxDequeueCount);
            await BatchQueue.Init();
        }

        public BatchQueue<T> BatchQueue { get; private set; }
        public void Dispose() => BatchQueue.Delete().GetAwaiter().GetResult();
    }
}
