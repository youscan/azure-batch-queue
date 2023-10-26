using Azure.Storage.Queues;
using NUnit.Framework;

namespace AzureBatchQueue.Tests.Samples;

[TestFixture]
public class HelloWorld
{
    private string connectionString;
    private string queueName;
    private QueueClient queue;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;";
        queueName = "hello-world-queue";
        
        queue = new QueueClient(connectionString, queueName);
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
    public async Task HelloWorld_SendMessage()
    {
        await queue.SendMessageAsync("HelloWorld");

        // Verify we uploaded one message
        Assert.AreEqual(1, (await queue.PeekMessagesAsync()).Value.Length);
    }
}