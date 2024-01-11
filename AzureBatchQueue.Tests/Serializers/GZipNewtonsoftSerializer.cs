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

    public T? Deserialize(ReadOnlyMemory<byte> bytes)
    {
        using var compressedStream = RecyclableMemory.GetStream(bytes.ToArray());

        using var decompressorStream = new GZipStream(compressedStream, CompressionMode.Decompress);
        using var decompressedStream = new MemoryStream();
        decompressorStream.CopyTo(decompressedStream);

        decompressedStream.Position = 0;
        using var jsonReader = new JsonTextReader(new StreamReader(decompressedStream, Encoding.UTF8));
        return serializer.Deserialize<T>(jsonReader);
    }
}
