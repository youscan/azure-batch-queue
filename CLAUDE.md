# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Azure Batch Queue is a .NET 8 library that optimizes Azure Queue operations by batching multiple items into single messages, reducing costs and operations. The library wraps Azure Storage Queues and provides automatic message batching, large message offloading to Blob storage, and poison message handling with quarantine queues.

## Build & Test Commands

### Local Development Setup
```bash
# Start Azurite emulator (required for tests and samples)
make start-storage

# Or manually with Docker:
docker run -p 10000:10000 -p 10001:10001 mcr.microsoft.com/azure-storage/azurite
```

### Build and Test
```bash
# Restore dependencies
dotnet restore

# Build the solution
dotnet build --configuration Release --no-restore

# Run all tests (requires Azurite running)
dotnet test

# Run tests with detailed output
dotnet test --no-restore --no-build --configuration Release --logger console AzureBatchQueue.Tests

# Run a specific test
dotnet test --filter "FullyQualifiedName~BatchQueueTests.When_sending_batch"
```

### Package Creation
```bash
# Create NuGet packages
make pack

# Or manually:
dotnet pack --configuration Release --include-symbols --include-source -p:SymbolPackageFormat=snupkg -p:PackageVersion=1.0.0 -o out/ AzureBatchQueue/
```

## Architecture

### Core Components

**BatchQueue<T>** (`BatchQueue.cs`)
- High-level API for batched queue operations
- Wraps `MessageQueue<T[]>` to provide item-level semantics
- Returns `BatchItem<T>[]` which allows individual item completion/failure
- Handles quarantine operations and queue initialization

**MessageQueue<T>** (`MessageQueue.cs`)
- Low-level queue abstraction over Azure Storage Queues
- Implements message serialization with automatic blob offloading for large messages (>48KB)
- Creates separate quarantine queues (`{queueName}-quarantine`) and blob containers (`overflow-{queueName}`)
- Moves messages to quarantine after exceeding `maxDequeueCount` (default: 5)

**TimerBatch<T>** (`TimerBatch.cs`)
- Internal component managing batch lifecycle
- Tracks completion/failure state of individual items within a batch
- Uses timer to flush batch state before visibility timeout expires (85% of timeout)
- Returns only failed/unprocessed items to queue on flush, deletes successful batches

**BatchItem<T>** (`BatchItem.cs`)
- Represents a single item within a batch
- Provides `Complete()` and `Fail()` methods to mark item status
- Maintains metadata including batch ID, visibility time, and flush period
- Individual item state is tracked in `TimerBatch` but accessed through this API

### Key Design Patterns

**At-Least-Once Delivery**
- Each item in a batch is tracked independently via `BatchItemsCollection<T>`
- Batch message is only deleted when all items are completed
- Failed/unprocessed items are returned to queue as updated message
- Visibility timeout timer ensures partial progress is saved even if process crashes

**Blob Offloading**
- Messages exceeding 48KB are automatically offloaded to blob storage
- Only a `BlobRef` (containing blob name) is sent through queue
- Message includes `BlobRef` metadata in payload envelope
- Blob cleanup happens on message deletion or when message shrinks below threshold

**Serialization**
- Default: `System.Text.Json` via `JsonSerializer<T>`
- Optional: Newtonsoft.Json via `AzureBatchQueue.JsonNet` package
- Custom serializers must implement `IMessageQueueSerializer<T>`
- Uses `Microsoft.IO.RecyclableMemoryStream` for efficient memory management

## Project Structure

```
AzureBatchQueue/              # Main library (targets net8.0)
├── BatchQueue.cs             # Primary public API
├── MessageQueue.cs           # Low-level queue operations
├── BatchItem.cs              # Individual item wrapper
├── TimerBatch.cs             # Batch lifecycle management
├── Serializer.cs             # Serialization interfaces
└── MemoryStreamManager.cs    # RecyclableMemoryStream pool

AzureBatchQueue.JsonNet/      # Newtonsoft.Json serialization support
AzureBatchQueue.Tests/        # NUnit tests (requires Azurite)
Samples/                      # Example applications
├── SendReceiveBatch/         # Basic send/receive demo
└── Dequarantine/             # Quarantine handling demo
```

## Testing

- Tests use NUnit and FluentAssertions
- All tests require Azurite emulator running locally
- Tests use connection string for local Azurite storage
- Tests create and cleanup temporary queues/containers
- GitHub Actions workflow runs tests against Azurite service container

## CI/CD

The `.github/workflows/workflow.yml` workflow:
- Runs on push and pull requests
- Starts Azurite service container for tests
- Builds with .NET 8
- Creates NuGet packages on version tags (format: `v1.0.0`)
- Publishes to NuGet.org and creates GitHub releases
