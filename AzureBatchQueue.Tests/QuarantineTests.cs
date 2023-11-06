using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

public class QuarantineTests
{
    private BatchQueue<TestItem> batchQueue;
    TimeSpan flushPeriod = TimeSpan.FromMilliseconds(100);
    private const int maxDequeueCount = 2;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        batchQueue = new BatchQueue<TestItem>("UseDevelopmentStorage=true", RandomQueueName(),
            flushPeriod: flushPeriod, maxDequeueCount: maxDequeueCount);
    }

    [SetUp]
    public async Task SetUp() => await batchQueue.Init();

    [TearDown]
    public async Task TearDown() => await batchQueue.Delete();

    [Test]
    public async Task QuarantineMessageAfterMaxDequeueCount()
    {
        await batchQueue.SendBatch(Batch());

        (await batchQueue.ApproximateMessagesCount()).Should().Be(1);

        await ReceiveBatch(maxDequeueCount);

        // message was quarantined
        var emptyBatch = await batchQueue.ReceiveBatch();
        emptyBatch.Length.Should().Be(0);

        (await batchQueue.ApproximateMessagesCount()).Should().Be(0);
        (await batchQueue.QuarantineApproximateMessagesCount()).Should().Be(1);
    }

    private async Task ReceiveBatch(int dequeueCount)
    {
        for (var i = 0; i < dequeueCount; i++)
        {
            var batch = await batchQueue.ReceiveBatch();
            batch.Length.Should().Be(1);

            // wait till message returns to the queue
            await Task.Delay(flushPeriod.Add(TimeSpan.FromMilliseconds(100)));
        }
    }

    private static MessageBatch<TestItem> Batch() => new(new List<TestItem> { new("Ivan", 12) }, SerializerType.Json);

    private record TestItem(string Name, int Age);
    private static string RandomQueueName() => $"queue-name-{new Random(1000).Next()}";
}
