using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using AzureBatchQueue.JsonNet;
using AzureBatchQueue.Tests.Serializers;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class MessageQueueTests
{
    [TestCase(typeof(BatchQueueCompressedSerializer<string>))]
    [TestCase(typeof(GZipJsonSerializer<string>))]
    [TestCase(typeof(NewtonsoftJsonSerializer<string>))]
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
    [TestCase(typeof(NewtonsoftJsonSerializer<TestItem>))]
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
        var logger = new RecordingLogger();
        queueTest.Queue.WithLogger(logger);

        var largeMessage = new string('*', 65 * 1024);
        await queueTest.Queue.Send(largeMessage);

        (await queueTest.Queue.Receive()).Length.Should().Be(0);

        var msgFromQuarantine = (await queueTest.Queue.ReceiveFromQuarantine()).Single();
        msgFromQuarantine.Item.Should().Be(largeMessage);
        msgFromQuarantine.MessageId.BlobName.Should().NotBeEmpty();
        var copy = logger.Entries.Single(x => x.Name == "BlobCopiedForQuarantine");
        copy.Fields["DestBlob"].Should().Be(msgFromQuarantine.MessageId.BlobName);
        copy.Fields["SourceBlob"].Should().Be(logger.Entries.Single(x => x.Name == "BlobDeleted").Fields["BlobName"]);
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

    [Test]
    public async Task When_receiving_a_missing_blob_returns_healthy_messages()
    {
        using var queueTest = await Queue<string>();
        await queueTest.Queue.Send(new string('*', 65 * 1024));
        await queueTest.DeleteMessageBlob();
        var largeMessage = new string('h', 65 * 1024);
        await queueTest.Queue.Send("healthy-inline");
        await queueTest.Queue.Send(largeMessage);

        var messages = await queueTest.Queue.Receive(3);

        messages.Select(x => x.Item).Should().BeEquivalentTo(new[] { "healthy-inline", largeMessage });
        (await queueTest.MessageCount()).Should().Be(2);
        (await queueTest.MessageCount(fromQuarantine: true)).Should().Be(0);
    }

    [Test]
    public async Task When_quarantining_a_missing_blob_keeps_healthy_messages()
    {
        using var queueTest = await Queue<string>(maxDequeueCount: 0);
        await queueTest.Queue.Send(new string('*', 65 * 1024));
        await queueTest.DeleteMessageBlob();
        var largeMessage = new string('h', 65 * 1024);
        await queueTest.Queue.Send("healthy-inline");
        await queueTest.Queue.Send(largeMessage);

        (await queueTest.Queue.Receive(3)).Should().BeEmpty();

        (await queueTest.MessageCount()).Should().Be(0);
        (await queueTest.MessageCount(fromQuarantine: true)).Should().Be(2);
        var quarantined = await queueTest.Queue.ReceiveFromQuarantine(2);
        quarantined.Select(x => x.Item).Should().BeEquivalentTo(new[] { "healthy-inline", largeMessage });
    }

    [TestCase(0)]
    [TestCase(5)]
    public async Task When_container_is_missing_keeps_message_for_retry(int maxDequeueCount)
    {
        using var queueTest = await Queue<string>(maxDequeueCount);
        await queueTest.Queue.Send(new string('*', 65 * 1024));
        await queueTest.Blobs.DeleteAsync();

        if (maxDequeueCount == 0)
            (await queueTest.Queue.Receive()).Should().BeEmpty();
        else
        {
            var error = Assert.ThrowsAsync<RequestFailedException>(async () => await queueTest.Queue.Receive());
            error!.ErrorCode.Should().Be("ContainerNotFound");
        }

        (await queueTest.MessageCount()).Should().Be(1);
    }

    [Test]
    public async Task When_receiving_a_missing_blob_from_quarantine_keeps_message()
    {
        using var queueTest = await Queue<string>();
        await queueTest.Queue.QuarantineData(new string('*', 65 * 1024), CancellationToken.None);
        await queueTest.DeleteMessageBlob(fromQuarantine: true);

        var error = Assert.ThrowsAsync<RequestFailedException>(async () => await queueTest.Queue.ReceiveFromQuarantine());

        error!.ErrorCode.Should().Be("BlobNotFound");
        (await queueTest.MessageCount(fromQuarantine: true)).Should().Be(1);
        (await queueTest.MessageCount()).Should().Be(0);
    }

    [Test]
    public async Task When_processing_a_blob_logs_its_message_and_lifecycle()
    {
        using var queueTest = await Queue<string>();
        var logger = new RecordingLogger();
        queueTest.Queue.WithLogger(logger);

        await queueTest.Queue.Send(new string('*', 65 * 1024));
        var message = (await queueTest.Queue.Receive()).Single();
        await queueTest.Queue.DeleteMessage(message.MessageId);

        var events = logger.Entries.Where(x => Equals(x.Fields.GetValueOrDefault("BlobName"), message.MessageId.BlobName)).ToArray();
        events.Select(x => x.Name).Should().ContainInOrder("UploadingBlob", "BlobUploaded", "SendingMessage", "MessageSent",
            "MessageReceived", "DeletingMessage", "MessageDeleted", "DeletingBlob", "BlobDeleted");
        events.Single(x => x.Name == "MessageSent").Fields["MessageId"].Should().Be(message.MessageId.Id);
        events.Single(x => x.Name == "BlobDeleted").Fields["Deleted"].Should().Be(true);
    }

    [TestCase(1024)]
    [TestCase(65 * 1024)]
    public async Task When_updating_a_blob_logs_the_payload_reference(int size)
    {
        using var queueTest = await Queue<string>();
        var logger = new RecordingLogger();
        queueTest.Queue.WithLogger(logger);
        await queueTest.Queue.Send(new string('*', 65 * 1024));
        var original = (await queueTest.Queue.Receive()).Single();

        await queueTest.Queue.UpdateMessage(original.MessageId, new string('+', size));
        var updated = (await queueTest.Queue.Receive()).Single();

        var change = logger.Entries.Single(x => x.Name == "MessageUpdated");
        change.Fields["MessageId"].Should().Be(original.MessageId.Id);
        change.Fields["BlobName"].Should().Be(updated.MessageId.BlobName);
        logger.Entries.Count(x => x.Name == "BlobDeleted").Should().Be(size == 1024 ? 1 : 0);
    }

    [Test]
    public async Task When_sending_fails_logs_the_blob_and_operation()
    {
        using var queueTest = await Queue<string>();
        var logger = new RecordingLogger();
        queueTest.Queue.WithLogger(logger);
        await queueTest.RawQueue.DeleteAsync();
        try
        {
            var error = Assert.ThrowsAsync<RequestFailedException>(async () => await queueTest.Queue.Send(new string('*', 65 * 1024)));

            var failed = logger.Entries.Single(x => x.Name == "MessageSendFailed");
            failed.Fields["OperationId"].Should().Be(logger.Entries.Single(x => x.Name == "SendingMessage").Fields["OperationId"]);
            failed.Fields["BlobName"].Should().Be(logger.Entries.Single(x => x.Name == "BlobUploaded").Fields["BlobName"]);
            failed.Exception.Should().BeSameAs(error);
        }
        finally
        {
            await queueTest.Init();
        }
    }

    [Test]
    public async Task When_messages_share_a_blob_logs_both_ids_and_who_deletes_it()
    {
        using var queueTest = await Queue<string>();
        var logger = new RecordingLogger();
        queueTest.Queue.WithLogger(logger);
        await queueTest.Queue.Send(new string('*', 65 * 1024));
        var original = (await queueTest.RawQueue.PeekMessagesAsync(1)).Value.Single();
        await queueTest.RawQueue.SendMessageAsync(original.Body);

        var messages = await queueTest.Queue.Receive(2);
        await queueTest.Queue.DeleteMessage(messages[0].MessageId);

        var received = logger.Entries.Where(x => x.Name == "MessageReceived").ToArray();
        received.Select(x => x.Fields["MessageId"]).Should().BeEquivalentTo(messages.Select(x => x.MessageId.Id));
        received.Select(x => x.Fields["BlobName"]).Should().OnlyContain(x => Equals(x, messages[0].MessageId.BlobName));
        logger.Entries.Single(x => x.Name == "BlobDeleted").Fields["MessageId"].Should().Be(messages[0].MessageId.Id);

        await queueTest.Queue.UpdateMessage(messages[1].MessageId, null);
        (await queueTest.Queue.Receive()).Should().BeEmpty();
        logger.Entries.Single(x => x.Name == "MissingBlobMessageDeleted").Fields["MessageId"].Should().Be(messages[1].MessageId.Id);
    }

    sealed record LogEntry(string Name, Dictionary<string, object?> Fields, Exception? Exception);

    sealed class RecordingLogger : ILogger
    {
        public System.Collections.Concurrent.ConcurrentQueue<LogEntry> Entries { get; } = new();
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var fields = ((IEnumerable<KeyValuePair<string, object?>>)state!).ToDictionary(x => x.Key, x => x.Value);
            Entries.Enqueue(new LogEntry(eventId.Name ?? "", fields, exception));
        }
    }

    static async Task<QueueTest<T>> Queue<T>(int maxDequeueCount = 5, IMessageQueueSerializer<T>? serializer = null)
    {
        var queue = new QueueTest<T>(maxDequeueCount, serializer);
        await queue.Init();
        return queue;
    }

    class QueueTest<T> : IDisposable
    {
        readonly QueueClient quarantineQueue;

        public QueueTest(int maxDequeueCount = 5, IMessageQueueSerializer<T>? serializer = null)
        {
            var connection = Environment.GetEnvironmentVariable("AZURE_BATCH_QUEUE_TEST_CONNECTION") ?? "UseDevelopmentStorage=true";
            Queue = new MessageQueue<T>(connection, "test", maxDequeueCount: maxDequeueCount, serializer: serializer);
            var options = new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 };
            RawQueue = new QueueClient(connection, "test", options);
            quarantineQueue = new QueueClient(connection, "test-quarantine", options);
            Blobs = new BlobContainerClient(connection, "overflow-test");
        }

        public async Task Init() => await Queue.Init();
        public MessageQueue<T> Queue { get; }
        public QueueClient RawQueue { get; }
        public BlobContainerClient Blobs { get; }

        public async Task DeleteMessageBlob(bool fromQuarantine = false)
        {
            var queue = fromQuarantine ? quarantineQueue : RawQueue;
            var message = (await queue.PeekMessagesAsync(1)).Value.Single();
            using var envelope = JsonDocument.Parse(message.Body.ToString());
            await Blobs.DeleteBlobAsync(envelope.RootElement.GetProperty("__MSQ_QUEUE_BLOBNAME__").GetString()!);
        }

        public async Task<int> MessageCount(bool fromQuarantine = false)
        {
            var queue = fromQuarantine ? quarantineQueue : RawQueue;
            return (await queue.GetPropertiesAsync()).Value.ApproximateMessagesCount;
        }

        public void Dispose()
        {
            Task.WhenAll(Queue.Delete(), Blobs.DeleteIfExistsAsync()).GetAwaiter().GetResult();
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

        if (serializerType == typeof(NewtonsoftJsonSerializer<T>))
            return new NewtonsoftJsonSerializer<T>();

        throw new Exception($"Unsupported serializer type {serializerType}");
    }
}
