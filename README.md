# Azure Batch Queue
Azure Batch Queue is an open-source library designed to optimize Azure Queue operations by implementing batching for queue messages. It reduces costs by grouping multiple items into a single message, minimizing the number of operations required. The library is simple to use and requires minimum changes if you already have Azure Queues.

[![CI](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml/badge.svg)](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml) [![NuGet](https://img.shields.io/nuget/v/AzureBatchQueue.svg?style=flat)](https://www.nuget.org/packages/AzureBatchQueue/)

## Get Started
Install the latest version from [NuGet](https://www.nuget.org/packages/AzureBatchQueue):
```
dotnet add package AzureBatchQueue
```

If you prefer Newtonsoft.Json serialization instead of default System.Text.Json, install the following package:
```
dotnet add package AzureBatchQueue.JsonNet
```

## Purpose
Processing one message involves three billed operations: write, read, and complete.

> Batching multiple items together and sending them in one message reduces the number of operations (and costs) proportionally.

So now, when you send a queue message with 50 items, you pay just for 3 operations instead of 150 operations (50 writes, reads, completes).

## Features
- The semantics are close to the original `Azure.Storage.Queues` implementation, and is intended to be a drop-in replacement as much as possible.
- Ensures at-least-one delivery of every record, even in the event of system failures;
- Minimizes duplicate processing;
- Handles large messages by offloading content to Azure Blob and sending only a reference through the queue;
- Effectively manages messages that fail processing, sending them to a separate quarantine queue for inspection and resolution.

## How It Works

### Batching
Items are grouped into batches and sent as a single message. The producer sends as many items as necessary, and if the resulting message is bigger than 48 KB (Azure max allowed for Base64 encoding), BatchQueue will offload the message to Azure Blob.

Consumers process each item independently, unaware of the batch. The simplified сonsumer looks like this in the code. Notice again that there is no knowledge about the batch.

```csharp
var items = await batchQueue.Receive();

foreach (var item in items)
{
    await sendToPipeline(item);
}
```

### Loss Prevention
Each item is completed or failed independently, ensuring that no item is lost even if the entire batch fails. We maintain an in-memory structure to track the status change of every item, and the batch is considered processed only when all its items have been completed or failed.

![image](https://github.com/youscan/azure-batch-queue/assets/88326445/171258d5-a16c-433e-b784-f618f3798a61)

Once all items in a batch are processed successfully, we delete the original message from the queue. However, if any items fail processing, we return the updated message with only the failed items back to the queue for reprocessing. Even during a system crash and failure to commit processed items, we will not lose anything because a message with all records will return to the queue after the visibility timeout.

Items can also become stuck due to external service issues. With retries, processing can be delayed, impacting overall system performance. To address this, we set a timer before the message's visibility timeout expires. This ensures that only failed and unprocessed items return to the queue, minimizing the need to reprocess successfully completed items.

![image](https://github.com/youscan/azure-batch-queue/assets/88326445/6dd491e4-3840-4aab-ab71-cc3be4ff4e12)


### Support large messages

Azure queue messages have a size limit of 64KB for XML and 48KB for Base64. If the item exceeds this limit, we offload the content to Azure Blob storage and send only a reference to the data through the queue. This approach allows us to deliver large items or batches with hundreds of items in a single message without encountering size limitations.

![image](https://github.com/youscan/azure-batch-queue/assets/88326445/a06aae28-f09a-43b3-bc5e-9cb8f5d21600)


### Handle poison messages effectively

Message processing can fail due to various reasons, such as external service downtime, non-backward compatible contract changes, or corrupted message data. In such cases, the message is returned to the queue for reprocessing. To prevent poison messages from degrading pipeline throughput with endless retries, failed messages are moved to a separate quarantine queue after a configured number of failed attempts. Engineers can then inspect and resolve the issues before reprocessing the messages.

![image](https://github.com/youscan/azure-batch-queue/assets/88326445/3080814d-b8a8-40c7-bb3e-100a6e429110)


## How to run samples and tests locally
### Prepare the Queue storage
All tests and samples use the Azurite emulator for local Azure Storage development.
#### Run Azurite
Start azurite in a Docker container. Run from AzureBatchQueue folder this command:
```
make start-storage
```

### Run tests and samples from the command line or your IDE
After Azurite is ready, you can start playing with tests and samples.
To run tests from the command line, go to the AzureBatchQueue solution folder and run:
```
dotnet test
```



