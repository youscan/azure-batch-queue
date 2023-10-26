using AzureBatchQueue;


const string queueName = "dequarantine-test";
var batchQueue = new BatchQueue<string>("UseDevelopmentStorage=true", queueName, 1);
await batchQueue.Init();

var items = new[] { "one", "two", "three", "four", "five" };
Console.WriteLine($"Sending {items.Length} items.");
await batchQueue.Send(items);

var visibilityTimeout = TimeSpan.FromSeconds(2);

var result = await batchQueue.Receive(visibilityTimeout: visibilityTimeout);

Console.WriteLine($"Received {result.Length} items.");

Console.WriteLine($"Will not do anything, will wait {visibilityTimeout.Seconds} seconds for messages to quarantine.");
await Task.Delay(visibilityTimeout);

result = await batchQueue.Receive(visibilityTimeout: visibilityTimeout);

Console.WriteLine($"Received {result.Length} items. Mentions are in the quarantine queue");

Console.WriteLine("Deuarantine messages now.");

await batchQueue.Dequarantine();

result = await batchQueue.Receive(visibilityTimeout: visibilityTimeout);

Console.WriteLine($"Received {result.Length} items. ");

await batchQueue.Delete();

