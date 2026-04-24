using System.Text;

using Newtonsoft.Json;

namespace AzureBatchQueue.JsonNet;

public class NewtonsoftJsonSerializer<T> : IMessageQueueSerializer<T>
{
    static readonly JsonSerializer Serializer = JsonSerializer.Create(new JsonSerializerSettings
    {
        TypeNameHandling = TypeNameHandling.Auto,
        DefaultValueHandling = DefaultValueHandling.Include,
        NullValueHandling = NullValueHandling.Ignore,
    });

    public T? Deserialize(ReadOnlySpan<byte> bytes)
    {
        using var stream = new MemoryStream(bytes.ToArray());
        using var jsonReader = new JsonTextReader(new StreamReader(stream, Encoding.UTF8));
        return Serializer.Deserialize<T>(jsonReader);
    }

    public void Serialize(Stream stream, T item)
    {
        using var streamWriter = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true);
        using JsonWriter jsonWriter = new JsonTextWriter(streamWriter);
        Serializer.Serialize(jsonWriter, item);
    }
}
