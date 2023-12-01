using System.Text.Json;
using AzureBatchQueue.Utils;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class MessageQueueTests
{
    [Test]
    public async Task When_sending_large_message()
    {
        using var queueTest = await Queue<string>();

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
        var updated = new QueueMessage<TestItem>(updatedItem, message.MessageId);
        await queueTest.Queue.UpdateMessage(updated);

        message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(updatedItem);
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

    [Test]
    public async Task When_using_custom_serializer()
    {
        using var queueTest = await Queue(GZipCompressedSerializer<TestItem>.New());

        var msg = new TestItem("Dimka", 33);
        await queueTest.Queue.Send(msg);

        var message = (await queueTest.Queue.Receive()).Single();
        message.Item.Should().Be(msg);
        message.MessageId.BlobName.Should().BeNull();
    }

    static async Task<QueueTest<T>> Queue<T>(IMessageQueueSerializer<T>? serializer = null)
    {
        var queue = new QueueTest<T>(serializer);
        await queue.Init();
        return queue;
    }

    class QueueTest<T> : IDisposable
    {
        public QueueTest(IMessageQueueSerializer<T>? serializer = null)
        {
            Queue = new MessageQueue<T>("UseDevelopmentStorage=true", "test", serializer);
        }

        public async Task Init() => await Queue.Init();
        public MessageQueue<T> Queue { get; }
        public void Dispose()
        {
            Queue.Delete().GetAwaiter().GetResult();
        }
    }
}
