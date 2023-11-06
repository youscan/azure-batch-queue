# azure-batch-queue
Send batches of items in a single QueueMessage via Azure Storage Queue

[![CI](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml/badge.svg)](https://github.com/youscan/azure-batch-queue/actions/workflows/workflow.yml) [![NuGet](https://img.shields.io/nuget/v/AzureBatchQueue.svg?style=flat)](https://www.nuget.org/packages/AzureBatchQueue/)


## How to run samples and tests locally
### Run Azurite
1. Install [azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?).
Run from the AzureBatchQueue solution folder:
```
make azurite-pull
```
2. Launch Azurite by issuing the following command from the AzureBatchQueue solution folder:
```
make azurite-up
```

### Create account
By default Azurite uses this account name and key:
```
Account name: devstoreaccount1
Account key: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```
so you can these ones in your connection string, or create [custom ones](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=npm#custom-storage-accounts-and-keys).


