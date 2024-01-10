using System.Text.Json;

namespace AzureBatchQueue;

public interface IMessageQueueSerializer<T>
{
    void Serialize(Stream stream, T item);
    T? Deserialize(ReadOnlyMemory<byte> bytes);
}

public class JsonSerializer<T> : IMessageQueueSerializer<T>
{
    public static JsonSerializer<T> New() => new();

    public void Serialize(Stream stream, T item) => JsonSerializer.Serialize(stream, item);

    public T? Deserialize(ReadOnlyMemory<byte> bytes) => JsonSerializer.Deserialize<T>(bytes.Span);
}
