using System.Runtime.Serialization;
using AzureBatchQueue.Utils;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class SerializedMessageBatch<T>
{
    public bool Compressed { get; init; }
    public string Body { get; init; }

    [IgnoreDataMember]
    private IEnumerable<T>? items;
    public IEnumerable<T>? Items() => items ??= JsonConvert.DeserializeObject<IEnumerable<T>>(Body);

    public static string Serialize(IEnumerable<T> items, bool compress)
    {
        var jsonItems = Serialize(items);

        var data = new SerializedMessageBatch<T>
        {
            Compressed = compress,
            Body = compress ? StringCompression.Compress(jsonItems) : jsonItems
        };

        var json = JsonConvert.SerializeObject(data);
        return json;
    }

    public static SerializedMessageBatch<T> Deserialize(string json)
    {
        var data = JsonConvert.DeserializeObject<SerializedMessageBatch<T>>(json);

        var body = data.Compressed ? StringCompression.Decompress(data.Body) : data.Body;

        return new SerializedMessageBatch<T> { Compressed = data.Compressed, Body = body };
    }

    private static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
}
