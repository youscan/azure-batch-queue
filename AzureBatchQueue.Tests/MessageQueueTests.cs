using AzureBatchQueue.JsonNet;
using AzureBatchQueue.Tests.Serializers;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class MessageQueueTests
{
    [TestCase(typeof(BatchQueueCompressedSerializer<string>))]
    [TestCase(typeof(GZipJsonSerializer<string>))]
    public async Task When_sending_large_message(Type serializerType)
    {
        using var queueTest = await Queue(serializer: GetSerializer<string>(serializerType));

        var largeMessage = new string('*', 65 * 1024);
        await queueTest.Queue.Send(largeMessage);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(largeMessage);
        message.MessageId.BlobName.Should().NotBeEmpty();
    }

    record TestItem(string Name, int Age);
    [Test]
    public async Task When_sending_small_message()
    {
        using var queueTest = await Queue<TestItem>();

        var msg = new TestItem("Dimka", 33);
        await queueTest.Queue.Send(msg);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(msg);
        message.MessageId.BlobName.Should().BeNull();
    }

    [Test]
    public async Task When_updating_message()
    {
        using var queueTest = await Queue<TestItem>();

        var item = new TestItem("Dimka", 33);
        await queueTest.Queue.Send(item);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(item);

        var updatedItem = new TestItem("Yaroslav", 26);
        await queueTest.Queue.UpdateMessage(message.MessageId, updatedItem);

        message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(updatedItem);
    }

    [Test]
    public async Task When_updating_large_message_with_large_message()
    {
        using var queueTest = await Queue<string>();

        var largeItem = new string('*', 65 * 1024);
        await queueTest.Queue.Send(largeItem);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(largeItem);
        var blobName = message.MessageId.BlobName;
        blobName.Should().NotBeEmpty();

        var updatedItem = new string('-', 65 * 1024);
        await queueTest.Queue.UpdateMessage(message.MessageId, updatedItem);

        message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(updatedItem);
        message.MessageId.BlobName.Should().Be(blobName);
    }

    [Test]
    public async Task When_updating_large_message_with_small_message()
    {
        using var queueTest = await Queue<string>();

        var largeItem = new string('*', 65 * 1024);
        await queueTest.Queue.Send(largeItem);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(largeItem);
        var blobName = message.MessageId.BlobName;
        blobName.Should().NotBeEmpty();

        var updatedItem = new string('+', 1 * 1024);
        await queueTest.Queue.UpdateMessage(message.MessageId, updatedItem);

        message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(updatedItem);
        message.MessageId.BlobName.Should().BeNull();
    }

    record SimilarToInternalBlobReference(string BlobName, string Body);
    [Test]
    public async Task When_sending_message_similar_to_internal_blob_reference()
    {
        using var queueTest = await Queue<SimilarToInternalBlobReference>();

        var msg = new SimilarToInternalBlobReference("blob-name", "body");
        await queueTest.Queue.Send(msg);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(msg);
        message.MessageId.BlobName.Should().NotBeEmpty();
    }

    [TestCase(typeof(BatchQueueCompressedSerializer<TestItem>))]
    [TestCase(typeof(GZipJsonSerializer<TestItem>))]
    public async Task When_using_custom_serializer(Type serializerType)
    {
        using var queueTest = await Queue(serializer: GetSerializer<TestItem>(serializerType));

        var msg = new TestItem("Dimka", 33);
        await queueTest.Queue.Send(msg);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(msg);
        message.MessageId.BlobName.Should().BeNull();
    }

    [Test]
    public async Task When_quarantine_small_message()
    {
        using var queueTest = await Queue<TestItem>(maxDequeueCount: 0); // quarantine after first read

        var msg = new TestItem("Yaro", 26);
        await queueTest.Queue.Send(msg);

        (await queueTest.Queue.Receive()).Length.Should().Be(0);

        var msgFromQuarantine = (await queueTest.Queue.ReceiveFromQuarantine()).Single();
        msgFromQuarantine.Item.Should().Be(msg);
        msgFromQuarantine.MessageId.BlobName.Should().BeNull();
    }

    [Test]
    public async Task When_quarantine_large_message()
    {
        using var queueTest = await Queue<string>(maxDequeueCount: 0); // quarantine after first read

        var largeMessage = new string('*', 65 * 1024);
        await queueTest.Queue.Send(largeMessage);

        (await queueTest.Queue.Receive()).Length.Should().Be(0);

        var msgFromQuarantine = (await queueTest.Queue.ReceiveFromQuarantine()).Single();
        msgFromQuarantine.Item.Should().Be(largeMessage);
        msgFromQuarantine.MessageId.BlobName.Should().NotBeEmpty();
    }

    [Test]
    public async Task When_exception_on_deserialize()
    {
        var jsonQueue = await Queue(serializer: JsonSerializer<string>.New());
        var message = new string("test");
        await jsonQueue.Queue.Send(message);

        using var gZipQueue = await Queue(serializer: GZipJsonSerializer<string>.New());

        Assert.DoesNotThrowAsync(async () => await gZipQueue.Queue.Receive());
    }

    static async Task<QueueTest<T>> Queue<T>(int maxDequeueCount = 5, IMessageQueueSerializer<T>? serializer = null)
    {
        var queue = new QueueTest<T>(maxDequeueCount, serializer);
        await queue.Init();
        return queue;
    }

    class QueueTest<T> : IDisposable
    {
        public QueueTest(int maxDequeueCount = 5, IMessageQueueSerializer<T>? serializer = null)
        {
            Queue = new MessageQueue<T>("UseDevelopmentStorage=true", "test", maxDequeueCount: maxDequeueCount, serializer: serializer);
        }

        public async Task Init() => await Queue.Init();
        public MessageQueue<T> Queue { get; }
        public void Dispose()
        {
            Queue.Delete().GetAwaiter().GetResult();
        }
    }

    static IMessageQueueSerializer<T> GetSerializer<T>(Type serializerType)
    {
        if (serializerType == typeof(JsonSerializer<T>))
            return JsonSerializer<T>.New();

        if (serializerType == typeof(GZipJsonSerializer<T>))
            return GZipJsonSerializer<T>.New();

        if (serializerType == typeof(BatchQueueCompressedSerializer<T>))
            return new BatchQueueCompressedSerializer<T>();

        throw new Exception($"Unsupported serializer type {serializerType}");
    }
}
