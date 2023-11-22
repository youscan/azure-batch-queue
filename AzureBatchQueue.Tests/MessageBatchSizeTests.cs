using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class BatchMessageSizeTests
{
    TimeSpan flushPeriod = TimeSpan.FromSeconds(2);
    const int AzureQueueMaxAllowedSize = 49_126; // ~ 48 KB

    BatchQueue<byte[]> batchQueue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        batchQueue = new BatchQueue<byte[]>("UseDevelopmentStorage=true", "large-messages-queue", flushPeriod: flushPeriod);
    }

    [SetUp]
    public async Task SetUp() => await batchQueue.Init();

    [TearDown]
    public async Task TearDown() => await batchQueue.Delete();

    [Test]
    public Task ThrowsExceptionWhenMessageIsTooLargeForAzure()
    {
        var message = BatchOfSize(AzureQueueMaxAllowedSize, compress: false);
        message.Items().Count.Should().BeGreaterThan(0);

        Assert.ThrowsAsync<MessageTooLargeException>(async () => await batchQueue.SendBatch(message));

        return Task.CompletedTask;
    }

    static MessageBatch<byte[]> BatchOfSize(int bytes, bool compress)
    {
        var serializerType = compress ? SerializerType.GZipCompressed : SerializerType.Json;

        var items = new List<byte[]> { new byte[bytes] };
        var batch = new MessageBatch<byte[]>(items, serializerType, AzureQueueMaxAllowedSize);

        return batch;
    }
}
