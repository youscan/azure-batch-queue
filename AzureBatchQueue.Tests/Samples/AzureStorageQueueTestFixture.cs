using Azure.Storage.Queues;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests.Samples;

[TestFixture]
public class AzureStorageQueueTestFixture
{
    private QueueClient queue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        queue = new QueueClient("UseDevelopmentStorage=true", "hello-world-queue");
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
    public async Task SendMessage()
    {
        await queue.SendMessageAsync("HelloWorld");

        // Verify we uploaded one message
        (await queue.PeekMessagesAsync()).Value.Length.Should().Be(1);
    }

    [Test]
    public async Task ReceiveMessages()
    {
        await queue.SendMessageAsync("1");
        await queue.SendMessageAsync("2");
        await queue.SendMessageAsync("3");
        
        var messages = (await queue.ReceiveMessagesAsync()).Value;

        var messageBody = 1;
        foreach (var message in messages)
        {
            //Process the message, verify the order
            message.Body.ToString().Should().Be($"{messageBody++}");
            
            // Let the service know we're finished with the message and
            // it can be safely deleted.
            await queue.DeleteMessageAsync(message.MessageId, message.PopReceipt);
        }
    }
    
    [Test]
    public async Task ReceiveOnlyRequestedAmount()
    {
        await queue.SendMessageAsync("1");
        await queue.SendMessageAsync("2");
        await queue.SendMessageAsync("3");
        
        var messages = (await queue.ReceiveMessagesAsync(2)).Value;

        messages.Length.Should().Be(2);
    }

    [Test]
    public async Task UpdateMessageBody()
    {
        // send original
        await queue.SendMessageAsync("original");
        var original = (await queue.ReceiveMessagesAsync()).Value.Single();

        // update
        await queue.UpdateMessageAsync(original.MessageId, original.PopReceipt, "updated");
        
        // assert body was updated
        var updated = (await queue.ReceiveMessagesAsync()).Value.Single();
        updated.Body.ToString().Should().Be("updated");
    }
}