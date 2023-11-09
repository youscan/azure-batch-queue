using AzureBatchQueue.Utils;
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
        batchQueue = new BatchQueue<TestItem>("UseDevelopmentStorage=true", RandomQueueName(), flushPeriod: flushPeriod);
    }

    [SetUp]
    public async Task SetUp() => await batchQueue.Init();

    [TearDown]
    public async Task TearDown() => await batchQueue.Delete();

    [Test]
    public async Task SendBatch()
    {
        await batchQueue.SendBatch(Batch(new TestItem("Dimka", 33), new TestItem("Yaroslav", 26)));

        (await batchQueue.ReceiveBatch()).Length.Should().Be(2);
    }

    [Test]
    public async Task ReceiveBatch()
    {
        var items = new[] { new TestItem("Dimka", 33), new TestItem("Yaroslav", 26) };
        await batchQueue.SendBatch(Batch(items));

        var batchItems = await batchQueue.ReceiveBatch();
        batchItems.Length.Should().Be(items.Length);
    }

    [Test]
    public async Task ReceiveMany()
    {
        var items = new[] { new TestItem("Dimka", 33), new TestItem("Yaroslav", 26) };
        const int amountOfBatchesToSend = 3;

        for (var i = 0; i < amountOfBatchesToSend; i++)
        {
            await batchQueue.SendBatch(Batch(items));
        }

        var batchItems = await batchQueue.ReceiveMany();

        batchItems.Length.Should().Be(amountOfBatchesToSend * items.Length);
    }

    [Test]
    public void ThrowsWhenAskingToReceiveTooManyMessagesAtOnce()
    {
        Assert.ThrowsAsync<ArgumentException>(async () => await batchQueue.ReceiveMany(100));
    }

    [Test]
    public async Task Complete()
    {
        await batchQueue.SendBatch(Batch(new TestItem("Dimka", 33), new TestItem("Yaroslav", 26)));

        var batchItems = await batchQueue.ReceiveBatch();
        foreach (var batchItem in batchItems) await batchItem.Complete();

        (await batchQueue.ReceiveBatch()).Length.Should().Be(0);
    }

    [Test]
    public async Task CompleteAfterCompletion()
    {
        await batchQueue.SendBatch(Batch(new TestItem("Dimka", 33), new TestItem("Yaroslav", 26)));
        var batchItems = await batchQueue.ReceiveBatch();

        // complete whole batch
        foreach (var batchItem in batchItems)
            await batchItem.Complete();

        // wait while batch completes
        await Task.Delay(TimeSpan.FromMilliseconds(20));

        // complete already flushed message
         Assert.ThrowsAsync<MessageBatchCompletedException>(async () => await batchItems.First().Complete());
    }

    [Test]
    public async Task FlushOnTimeout()
    {
        await batchQueue.SendBatch(Batch(new TestItem("Dimka", 33), new TestItem("Yaroslav", 26)));

        var batchItems = await batchQueue.ReceiveBatch();
        batchItems.Length.Should().Be(2);
        await batchItems.First().Complete();

        await WaitTillFlush();

        var remainingItems = await batchQueue.ReceiveBatch();
        remainingItems.Length.Should().Be(1);
    }

    [Test]
    public async Task LeaseBatchWhileProcessing()
    {
        await batchQueue.SendBatch(Batch(new TestItem("Dimka", 33), new TestItem("Yaroslav", 26)));

        var batchItems = await batchQueue.ReceiveBatch();
        var batchItems2 = await batchQueue.ReceiveBatch();

        batchItems.Length.Should().Be(2);
        batchItems2.Length.Should().Be(0);

        // do not complete any messages, and wait for the batch to return to the queue
        await WaitTillFlush();

        var batchItems3 = await batchQueue.ReceiveBatch();
        batchItems3.Length.Should().Be(2);
    }

    [Test]
    public async Task HandleCompressedAndUncompressedMessages()
    {
        var items = new[] { new TestItem("Dimka", 33), new TestItem("Yaroslav", 26) };
        await batchQueue.SendBatch(CompressedBatch(items));
        await batchQueue.SendBatch(Batch(items));

        var batchItems = await batchQueue.ReceiveBatch();
        var batchItems2 = await batchQueue.ReceiveBatch();

        batchItems.Length.Should().Be(2);
        batchItems2.Length.Should().Be(2);

        await batchItems.First().Complete();
        await batchItems2.First().Complete();

        await WaitTillFlush();

        var batchItems1Updated = await batchQueue.ReceiveBatch();
        var batchItems2Updated = await batchQueue.ReceiveBatch();
        batchItems1Updated.Length.Should().Be(1);
        batchItems2Updated.Length.Should().Be(1);
    }

    private static MessageBatch<TestItem> CompressedBatch(params TestItem[] items) => Batch(new GZipCompressedSerializer<TestItem>(), items);
    private static MessageBatch<TestItem> Batch(params TestItem[] items) => Batch(new JsonSerializer<TestItem>(), items);

    private static MessageBatch<TestItem> Batch(IMessageBatchSerializer<TestItem>? serializer, TestItem[] items)
    {
        serializer ??= new JsonSerializer<TestItem>();
        var batch = new MessageBatch<TestItem>(serializer);

        foreach (var item in items)
        {
            if (!batch.TryAdd(item))
                return batch;
        }

        return batch;
    }

    private async Task WaitTillFlush() => await Task.Delay(flushPeriod.Add(TimeSpan.FromMilliseconds(100)));

    private static string RandomQueueName() => $"queue-name-{new Random(1000).Next()}";
}
