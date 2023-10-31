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
        var message = MessageOfSize(MaxAllowedMessageSizeInBytes);

        await batchQueue.SendBatch(message, compress: false);
    }

    [Test]
    public Task ThrowsExceptionWhenMessageIsTooLarge()
    {
        var message = MessageOfSize(MaxAllowedMessageSizeInBytes + 1);

        Assert.ThrowsAsync<MessageTooLargeException>(async () => await batchQueue.SendBatch(message, compress: false));

        return Task.CompletedTask;
    }

    private static IEnumerable<byte[]> MessageOfSize(int bytes) => new[] { new byte[bytes] };
}
