using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchQueueTests
{
    private record TestItem(string Name, int Age);
    TimeSpan flushPeriod = TimeSpan.FromSeconds(2);

    private BatchQueue<TestItem> batchQueue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        batchQueue = new BatchQueue<TestItem>("UseDevelopmentStorage=true", "hello-world-queue", flushPeriod: flushPeriod);
    }

    [SetUp]
    public async Task SetUp() => await batchQueue.Create();

    [TearDown]
    public async Task TearDown() => await batchQueue.Delete();

    [Test]
    public async Task SendBatch()
    {
        await batchQueue.SendBatch(TestItems());

        (await batchQueue.ReceiveBatch()).Length.Should().Be(2);
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

        (await batchQueue.ReceiveBatch()).Length.Should().Be(0);
    }

    [Test]
    public async Task CompleteAfterCompletion()
    {
        await batchQueue.SendBatch(TestItems());
        var batchItems = await batchQueue.ReceiveBatch();

        // complete whole batch
        foreach (var batchItem in batchItems)
            await batchItem.Complete();

        // complete already flushed message
         Assert.ThrowsAsync<MessageBatchCompletedException>(async () => await batchItems.First().Complete());
    }

    [Test]
    public async Task FlushOnTimeout()
    {
        await batchQueue.SendBatch(TestItems());

        var batchItems = await batchQueue.ReceiveBatch();
        batchItems.Length.Should().Be(2);
        await batchItems.First().Complete();

        // wait longer than flush period for the whole batch
        await Task.Delay(flushPeriod.Add(TimeSpan.FromMilliseconds(100)));

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

    [Test]
    public async Task HandleCompressedAndUncompressedMessages()
    {
        await batchQueue.SendBatch(TestItems(), compress: true);
        await batchQueue.SendBatch(TestItems(), compress: false);

        var batchItems = await batchQueue.ReceiveBatch();
        var batchItems2 = await batchQueue.ReceiveBatch();

        batchItems.Length.Should().Be(2);
        batchItems2.Length.Should().Be(2);

        await batchItems.First().Complete();
        await batchItems2.First().Complete();

        // wait longer than flush period for the whole batch
        await Task.Delay(flushPeriod.Add(TimeSpan.FromMilliseconds(100)));

        var batchItems1Updated = await batchQueue.ReceiveBatch();
        var batchItems2Updated = await batchQueue.ReceiveBatch();
        batchItems1Updated.Length.Should().Be(1);
        batchItems2Updated.Length.Should().Be(1);

    }

    private static IEnumerable<TestItem> TestItems() => new [] { new TestItem("Dimka", 33), new TestItem("Yaroslav", 26) };
}
