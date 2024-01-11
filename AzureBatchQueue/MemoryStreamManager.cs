using Microsoft.IO;

namespace AzureBatchQueue;

public static class MemoryStreamManager
{
    static readonly RecyclableMemoryStreamManager.Options options = new()
    {
        BlockSize = 128 * 1024,
        LargeBufferMultiple = 1024 * 1024,
        MaximumBufferSize = 128 * 1024 * 1024,
        MaximumLargePoolFreeBytes = 128 * 1024 * 1024,
        MaximumSmallPoolFreeBytes = 1024 * 1024
    };

    public static readonly RecyclableMemoryStreamManager RecyclableMemory = new(options);
}
