using System.IO.Compression;
using System.Text.Json;

namespace AzureBatchQueue.Tests.Serializers;

public class GZipJsonSerializer<T> : IMessageQueueSerializer<T>
{
    public static GZipJsonSerializer<T> New() => new();

    public void Serialize(Stream stream, T item)
    {
        using var gzipStream = new GZipStream(stream, CompressionMode.Compress, true);
        JsonSerializer.Serialize(gzipStream, item);

        gzipStream.Flush();
    }

    public T? Deserialize(ReadOnlySpan<byte> bytes)
    {
        using var stream = new MemoryStream(bytes.ToArray());
        using var decompressorStream = new GZipStream(stream, CompressionMode.Decompress);
        return JsonSerializer.Deserialize<T>(decompressorStream);
    }
}
