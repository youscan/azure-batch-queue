using System.IO.Compression;
using System.Text.Json;
using Microsoft.IO;

namespace AzureBatchQueue.Tests.Serializers;

public class GZipJsonSerializer<T> : IMessageQueueSerializer<T>
{
    public static GZipJsonSerializer<T> New() => new();
    public static readonly RecyclableMemoryStreamManager RecyclableMemory = new();

    public void Serialize(Stream stream, T item)
    {
        using var gzipStream = new GZipStream(stream, CompressionMode.Compress, true);
        JsonSerializer.Serialize(gzipStream, item);

        gzipStream.Flush();
    }

    public T? Deserialize(ReadOnlySpan<byte> bytes)
    {
        using var stream = RecyclableMemory.GetStream(bytes.ToArray());
        using var decompressorStream = new GZipStream(stream, CompressionMode.Decompress, true);
        return JsonSerializer.Deserialize<T>(decompressorStream);
    }
}
