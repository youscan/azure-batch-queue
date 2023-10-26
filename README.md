# azure-batch-queue
Send batches of items in a single QueueMessage via Azure Storage Queue

[![CI](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml/badge.svg)](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml) [![NuGet](https://img.shields.io/nuget/v/AzureBatchQueue.svg?style=flat)](https://www.nuget.org/packages/AzureBatchQueue/)


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



