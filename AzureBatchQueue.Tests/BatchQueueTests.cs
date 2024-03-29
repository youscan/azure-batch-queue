using AzureBatchQueue.Exceptions;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchQueueTests
{
    [Test]
    public async Task When_sending_batch()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
    }

    [Test]
    public async Task When_batch_timer_flushes_after_period()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(3);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
        foreach (var item in response)
        {
            // complete all except 'orange'
            if (item.Item == "orange")
                continue;

            item.Complete();
        }

        // wait for timer to flush
        await Task.Delay(visibilityTimeout);

        var updatedResponse = await queueTest.BatchQueue.Receive();
        updatedResponse.Single().Item.Should().Be("orange");
    }

    [Test]
    public async Task When_batch_flushes_after_all_complete()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var longVisibilityTimeout = TimeSpan.FromSeconds(10);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: longVisibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        foreach (var item in response)
            item.Complete();

        // wait for message to flush
        await Task.Delay(TimeSpan.FromMilliseconds(10));

        // try to complete batch item again, but catch an exception that everything was already processed
        Assert.Throws<BatchCompletedException>(() => response.First().Complete())!
            .BatchCompletedResult.Should().Be(BatchCompletedResult.FullyProcessed);
    }

    [Test]
    public async Task When_batch_updates_after_all_fail()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var longVisibilityTimeout = TimeSpan.FromSeconds(10);
        var firstReceive = await queueTest.BatchQueue.Receive(visibilityTimeout: longVisibilityTimeout);
        firstReceive.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        foreach (var item in firstReceive)
            item.Fail();

        // wait for message to flush
        await Task.Delay(TimeSpan.FromMilliseconds(10));

        // try to complete batch item again, but catch an exception that everything was already processed
        Assert.Throws<BatchCompletedException>(() => firstReceive.First().Complete())!
            .BatchCompletedResult.Should().Be(BatchCompletedResult.PartialFailure);

        // wait for message to be visible
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        // message was updated with failed items
        var secondReceive = await queueTest.BatchQueue.Receive(visibilityTimeout: longVisibilityTimeout);
        secondReceive.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);
    }

    [Test]
    public async Task When_batch_flushes_after_all_complete_or_fail()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var longVisibilityTimeout = TimeSpan.FromSeconds(10);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: longVisibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        var failedItems = new[] { "orange", "apple" };
        foreach (var item in response)
        {
            if (failedItems.Contains(item.Item))
                item.Fail();
            else
                item.Complete();
        }

        // wait for message to flush
        await Task.Delay(TimeSpan.FromMilliseconds(10));

        // try to complete batch item again, but catch an exception that everything was already processed
        Assert.Throws<BatchCompletedException>(() => response.First().Complete())!
            .BatchCompletedResult.Should().Be(BatchCompletedResult.PartialFailure);

        // wait for message to be visible
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        // message was updated with failed items
        var secondReceive = await queueTest.BatchQueue.Receive(visibilityTimeout: longVisibilityTimeout);
        secondReceive.Select(x => x.Item).Should().BeEquivalentTo(failedItems);
    }

    [Test]
    public async Task When_many_threads_complete_in_parallel()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        Assert.DoesNotThrow(() => Parallel.ForEach(response, batchItem => batchItem.Complete()));
    }

    [Test]
    public async Task When_quarantine_whole_batch()
    {
        const int maxDequeueCount = 1;
        using var queueTest = await Queue<string>(maxDequeueCount);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(3);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        // do not complete any items from the batch

        // wait for message to be quarantined
        await Task.Delay(visibilityTimeout);

        var responseFromQuarantine = await queueTest.BatchQueue.GetItemsFromQuarantine();
        responseFromQuarantine.Should().BeEquivalentTo(messageBatch);
    }

    [Test]
    public async Task When_quarantine_only_not_completed_items_from_batch()
    {
        const int maxDequeueCount = 1;
        using var queueTest = await Queue<string>(maxDequeueCount);

        var messageBatch = new[] { "orange", "banana", "apple", "pear", "strawberry" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(3);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);
        response.Select(x => x.Item).Should().BeEquivalentTo(messageBatch);

        const string notCompletedItem = "orange";
        const string failedItem = "pear";
        foreach (var item in response)
        {
            if (item.Item == notCompletedItem)
                continue;

            if (item.Item == failedItem)
                item.Fail();
            else
                item.Complete();
        }

        // wait for message to be quarantined
        await Task.Delay(visibilityTimeout);

        var responseFromQuarantine = await queueTest.BatchQueue.GetItemsFromQuarantine();
        responseFromQuarantine.Should().BeEquivalentTo(notCompletedItem, failedItem);
    }

    [Test]
    public async Task When_completing_same_item_twice_throws_exception()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange", "apple" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();

        Assert.DoesNotThrow(() => response.First().Complete());
        Assert.Throws<ItemNotFoundException>(() => response.First().Complete());
    }

    [Test]
    public async Task When_completing_same_item_after_pause()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange" };
        await queueTest.BatchQueue.Send(messageBatch);

        var response = await queueTest.BatchQueue.Receive();

        Assert.DoesNotThrow(() => response.First().Complete());

        // wait for batch to be triggered
        await Task.Delay(TimeSpan.FromMilliseconds(10));

        Assert.Throws<BatchCompletedException>(() => response.Single().Complete())!
            .BatchCompletedResult.Should().Be(BatchCompletedResult.FullyProcessed);
    }

    [Test]
    public async Task When_timer_flushes_after_disposed_queue_test()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(2);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);

        queueTest.Dispose();

        response.Single().Complete();
    }

    [Test]
    public async Task When_completing_item_after_batch_was_flushed()
    {
        using var queueTest = await Queue<string>();

        var messageBatch = new[] { "orange" };
        await queueTest.BatchQueue.Send(messageBatch);

        var visibilityTimeout = TimeSpan.FromSeconds(3);
        var response = await queueTest.BatchQueue.Receive(visibilityTimeout: visibilityTimeout);

        // wait for batch to be flushed
        await Task.Delay(visibilityTimeout.Add(TimeSpan.FromMilliseconds(500)));

        Assert.Throws<BatchCompletedException>(() => response.Single().Complete())!
            .BatchCompletedResult.Should().Be(BatchCompletedResult.TriggeredByFlush);
    }

    static async Task<BatchQueueTest<T>> Queue<T>(int maxDequeueCount = 5)
    {
        var queue = new BatchQueueTest<T>();
        await queue.Init(maxDequeueCount);
        return queue;
    }

    class BatchQueueTest<T> : IDisposable
    {
        public async Task Init(int maxDequeueCount)
        {
            var loggerFactory =
                LoggerFactory.Create(builder =>
                    builder.AddSimpleConsole(options =>
                    {
                        options.IncludeScopes = true;
                        options.SingleLine = true;
                        options.TimestampFormat = "HH:mm:ss ";
                    }));

            var logger = loggerFactory.CreateLogger<BatchQueueTest<T>>();

            var random = new Random().NextInt64(0, int.MaxValue);
            BatchQueue = new BatchQueue<T>("UseDevelopmentStorage=true", $"batch-test-{random}", maxDequeueCount, logger: logger);
            await BatchQueue.Init();
        }

        public BatchQueue<T> BatchQueue { get; private set; }
        public void Dispose() => BatchQueue.ClearMessages().GetAwaiter().GetResult();
    }
}
