using Azure.Storage.Queues;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class MessageBatchTests
{
    private record TestItem(string Name, int Age);
    TimeSpan flushPeriod = TimeSpan.FromSeconds(3);

    private QueueClient queue;
    private BatchQueue<TestItem> batchQueue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        queue = new QueueClient("UseDevelopmentStorage=true", "hello-world-queue");
        batchQueue = new BatchQueue<TestItem>(queue, flushPeriod: flushPeriod);
    }

    [SetUp]
    public void SetUp()
    {
        queue.CreateAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        await queue.DeleteAsync();
    }

    [Test]
    public async Task SendBatch()
    {
        await batchQueue.SendBatch(TestItems());

        (await queue.PeekMessagesAsync()).Value.Length.Should().Be(1);
    }

    [Test]
    public async Task ReceiveBatch()
    {
        await batchQueue.SendBatch(TestItems());

        var batchItems = await batchQueue.ReceiveBatch();
        batchItems.Select(x => x.Id).All(id => id != Guid.Empty).Should().BeTrue();
    }

    [Test]
    public async Task Complete()
    {
        await batchQueue.SendBatch(TestItems());

        var batchItems = await batchQueue.ReceiveBatch();
        foreach (var batchItem in batchItems) await batchItem.Complete();

        (await queue.PeekMessagesAsync()).Value.Length.Should().Be(0);
    }

    [Test]
    public async Task FlushOnTimeout()
    {
        await batchQueue.SendBatch(TestItems());

        var batchItems = await batchQueue.ReceiveBatch();
        batchItems.Length.Should().Be(2);
        await batchItems.First().Complete();

        // wait longer than flush period for the whole batch
        await Task.Delay(flushPeriod.Add(TimeSpan.FromSeconds(2)));

        var remainingItems = await batchQueue.ReceiveBatch();
        remainingItems.Length.Should().Be(1);
    }

    [Test]
    public async Task LeaseBatchWhileProcessing()
    {
        await batchQueue.SendBatch(TestItems());

        var batchItems = await batchQueue.ReceiveBatch();
        var batchItems2 = await batchQueue.ReceiveBatch();

        batchItems.Length.Should().Be(2);
        batchItems2.Length.Should().Be(0);

        // do not complete any messages, and wait for the batch to return to the queue
        await Task.Delay(flushPeriod.Add(TimeSpan.FromMilliseconds(100)));

        var batchItems3 = await batchQueue.ReceiveBatch();
        batchItems3.Length.Should().Be(2);
    }

    private static IEnumerable<TestItem> TestItems() => new [] { new TestItem("Dimka", 33), new TestItem("Yaroslav", 26) };
}
