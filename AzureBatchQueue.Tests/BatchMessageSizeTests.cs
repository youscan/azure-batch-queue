using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchMessageSizeTests
{
    TimeSpan flushPeriod = TimeSpan.FromSeconds(2);
    private const int MaxAllowedMessageSizeInBytes = 49_119; // ~ 48 KB

    private BatchQueue<byte[]> batchQueue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        batchQueue = new BatchQueue<byte[]>("UseDevelopmentStorage=true", "large-messages-queue", flushPeriod: flushPeriod);
    }

    [SetUp]
    public async Task SetUp() => await batchQueue.Create();

    [TearDown]
    public async Task TearDown() => await batchQueue.Delete();

    [Test]
    public async Task SendMessageWithMaxAllowedSize()
    {
        var message = BatchOfSize(MaxAllowedMessageSizeInBytes, compress: false);

        await batchQueue.SendBatch(message);
    }

    [Test]
    public Task ThrowsExceptionWhenMessageIsTooLarge()
    {
        var message = BatchOfSize(MaxAllowedMessageSizeInBytes + 1, compress: false);

        Assert.ThrowsAsync<MessageTooLargeException>(async () => await batchQueue.SendBatch(message));

        return Task.CompletedTask;
    }

    private static MessageBatch<byte[]> BatchOfSize(int bytes, bool compress)
    {
        var batch = new MessageBatch<byte[]>(compress);
        batch.TryAdd(new byte[bytes]);

        return batch;
    }
}
