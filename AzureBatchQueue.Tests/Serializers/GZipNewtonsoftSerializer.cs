using System.IO.Compression;
using System.Text;
using Microsoft.IO;
using Newtonsoft.Json;

namespace AzureBatchQueue.Tests.Serializers;

public class GZipNewtonsoftSerializer<T> : IMessageQueueSerializer<T>
{
    public static readonly RecyclableMemoryStreamManager RecyclableMemory = new();

    static readonly JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings
    {
        TypeNameHandling = TypeNameHandling.Auto,
        DefaultValueHandling = DefaultValueHandling.Include,
        NullValueHandling = NullValueHandling.Ignore,
    });

    public static GZipNewtonsoftSerializer<T> New() => new();

    public void Serialize(Stream stream, T item)
    {
        using var gzipStream = new GZipStream(stream, CompressionMode.Compress, true);
        using JsonWriter jsonWriter = new JsonTextWriter(new StreamWriter(gzipStream, Encoding.UTF8));
        serializer.Serialize(jsonWriter, item);

        jsonWriter.Flush();
    }

    public T? Deserialize(ReadOnlySpan<byte> bytes)
    {
        using var stream = RecyclableMemory.GetStream(bytes);
        using var gZipStream = new GZipStream(stream, CompressionMode.Decompress, true);
        using var jsonReader = new JsonTextReader(new StreamReader(gZipStream, Encoding.UTF8));
        return serializer.Deserialize<T>(jsonReader);
    }
}
