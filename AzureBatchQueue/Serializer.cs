using System.Text.Json;
using AzureBatchQueue.Utils;

namespace AzureBatchQueue;

public interface IMessageQueueSerializer<T>
{
    byte[] Serialize(T item);
    T? Deserialize(ReadOnlyMemory<byte> bytes);
}

public class JsonSerializer<T> : IMessageQueueSerializer<T>
{
    public byte[] Serialize(T item) => JsonSerializer.SerializeToUtf8Bytes(item);
    public T? Deserialize(ReadOnlyMemory<byte> bytes) => JsonSerializer.Deserialize<T>(bytes.Span);
}

public class GZipCompressedSerializer<T> : IMessageQueueSerializer<T>
{
    public static GZipCompressedSerializer<T> New() => new();

    public byte[] Serialize(T item)
    {
        var json = JsonSerializer.Serialize(item);
        return StringCompression.Compress(json);
    }

    public T? Deserialize(ReadOnlyMemory<byte> bytes)
    {
        var json = StringCompression.Decompress(bytes.ToArray());
        return JsonSerializer.Deserialize<T>(json);
    }
}
