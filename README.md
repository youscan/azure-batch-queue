# azure-batch-queue
Send batches of items in a single QueueMessage via Azure Storage Queue

[![CI](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml/badge.svg)](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml) [![NuGet](https://img.shields.io/nuget/v/AzureBatchQueue.svg?style=flat)](https://www.nuget.org/packages/AzureBatchQueue/)


## How to run samples and tests locally
### Prepare the Queue storage
All tests and samples use the Azurite emulator for local Azure Storage development.
#### Run Azurite
1. Install [azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?).
Run from the AzureBatchQueue solution folder:
```
make azurite-pull
```
2. Launch Azurite by issuing the following command from the AzureBatchQueue solution folder:
```
make azurite-up
```

### Run tests and samples from the command line or your IDE
After Azurite is ready, you can start playing with tests and samples.
To run tests from the command line, go to the AzureBatchQueue solution folder and run:
```
dotnet test
```



