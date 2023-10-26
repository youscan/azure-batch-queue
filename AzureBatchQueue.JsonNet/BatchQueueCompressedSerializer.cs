using System.IO.Compression;
using System.Text;

using Microsoft.IO;

using Newtonsoft.Json;

namespace AzureBatchQueue.JsonNet;

public class BatchQueueCompressedSerializer<T> : IMessageQueueSerializer<T>
{
    static readonly RecyclableMemoryStreamManager.Options Options = new()
    {
        BlockSize = 128 * 1024,
        LargeBufferMultiple = 1024 * 1024,
        MaximumBufferSize = 128 * 1024 * 1024,
        MaximumLargePoolFreeBytes = 128 * 1024 * 1024,
        MaximumSmallPoolFreeBytes = 1024 * 1024
    };

    static readonly RecyclableMemoryStreamManager RecyclableMemory = new(Options);

    static readonly JsonSerializer Serializer = JsonSerializer.Create(new JsonSerializerSettings
    {
        TypeNameHandling = TypeNameHandling.Auto,
        DefaultValueHandling = DefaultValueHandling.Include,
        NullValueHandling = NullValueHandling.Ignore,
    });

    public T? Deserialize(ReadOnlySpan<byte> bytes)
    {
        using var stream = RecyclableMemory.GetStream(bytes);
        using var gZipStream = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true);
        using var jsonReader = new JsonTextReader(new StreamReader(gZipStream, Encoding.UTF8));
        return Serializer.Deserialize<T>(jsonReader);
    }

    public void Serialize(Stream stream, T item)
    {
        using var gzipStream = new GZipStream(stream, CompressionMode.Compress, true);
        using var streamWriter = new StreamWriter(gzipStream, Encoding.UTF8);
        using JsonWriter jsonWriter = new JsonTextWriter(streamWriter);
        Serializer.Serialize(jsonWriter, item);
    }
}
