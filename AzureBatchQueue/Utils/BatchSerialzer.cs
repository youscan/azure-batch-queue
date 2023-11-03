using System.Text;
using Newtonsoft.Json;

namespace AzureBatchQueue.Utils;

public interface IMessageBatchSerializer<T>
{
    public string Serialize(MessageBatch<T> batch);

    public IEnumerable<T>? Deserialize(string json);

    public int GetSize(IEnumerable<T> items);
}

/// <summary>
/// Serializes and deserializes items using Newtonsoft.Json
/// </summary>
/// <typeparam name="T"></typeparam>
public class JsonSerializer<T> : IMessageBatchSerializer<T>
{
    public string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
    public string Serialize(MessageBatch<T> batch)
    {
        var data = new SerializedMessageBatch() { Compressed = batch.Compressed, Body = Serialize(batch.Items()) };

        return JsonConvert.SerializeObject(data);
    }

    public IEnumerable<T>? Deserialize(string json) => JsonConvert.DeserializeObject<List<T>>(json);
    public int GetSize(IEnumerable<T> items)
    {
        var json = Serialize(items);

        return Encoding.UTF8.GetBytes(json).Length;
    }

    public bool IsCompressed() => false;
}

/// <summary>
/// Compresses json using GZip compression
/// </summary>
/// <typeparam name="T"></typeparam>
public class GZipCompressedSerializer<T> : IMessageBatchSerializer<T>
{
    public string Serialize(IEnumerable<T> items)
    {
        var json = JsonConvert.SerializeObject(items);

        return StringCompression.CompressToBase64(json);
    }

    public string Serialize(MessageBatch<T> batch)
    {
        var json = JsonConvert.SerializeObject(batch.Items());

        var data = new SerializedMessageBatch
        {
            Compressed = batch.Compressed,
            Body = StringCompression.CompressToBase64(json)
        };

        return JsonConvert.SerializeObject(data);
    }

    public IEnumerable<T>? Deserialize(string json)
    {
        var decompressed = StringCompression.Decompress(json);

        return JsonConvert.DeserializeObject<List<T>>(decompressed);
    }

    public int GetSize(IEnumerable<T> items)
    {
        var json = JsonConvert.SerializeObject(items);

        return StringCompression.Compress(json).Length;
    }

    public bool IsCompressed() => true;
}
