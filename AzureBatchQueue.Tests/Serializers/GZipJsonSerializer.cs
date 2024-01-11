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

    public T? Deserialize(ReadOnlyMemory<byte> bytes)
    {
        var json = StringCompression.Decompress(bytes.ToArray());
        return JsonSerializer.Deserialize<T>(json);
    }
}
