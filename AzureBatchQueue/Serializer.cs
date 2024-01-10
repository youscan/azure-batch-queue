using System.IO.Compression;
using System.Text.Json;
using AzureBatchQueue.Utils;

namespace AzureBatchQueue;

public interface IMessageQueueSerializer<T>
{
    void Serialize(Stream stream, T item);
    T? Deserialize(ReadOnlyMemory<byte> bytes);
}

public class JsonSerializer<T> : IMessageQueueSerializer<T>
{
    public void Serialize(Stream stream, T item) => JsonSerializer.Serialize(stream, item);

    public T? Deserialize(ReadOnlyMemory<byte> bytes) => JsonSerializer.Deserialize<T>(bytes.Span);
}

public class GZipCompressedSerializer<T> : IMessageQueueSerializer<T>
{
    public static GZipCompressedSerializer<T> New() => new();

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
