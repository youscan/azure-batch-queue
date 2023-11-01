using System.Runtime.Serialization;
using System.Text;
using AzureBatchQueue.Utils;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class MessageBatch<T>
{
    public bool Compressed { get; init; }
    public string Body { get; init; }

    [IgnoreDataMember]
    private readonly List<T> items = new();
    public List<T> Items() => items;

    [IgnoreDataMember]
    private int MaxSizeInBytes { get; init; }
    public const int AzureQueueMaxSizeInBytes = 49_000; // ~ 48 KB

    public MessageBatch() { }
    public MessageBatch(bool compressed, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
    {
        Compressed = compressed;
        MaxSizeInBytes = maxSizeInBytes;
    }

    /// <summary>
    /// Use this ctor if you are sure that all items will fit into the batch
    /// </summary>
    /// <param name="items"></param>
    /// <param name="compressed"></param>
    /// <param name="maxSizeInBytes"></param>
    public MessageBatch(List<T> items, bool compressed, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
    {
        Compressed = compressed;
        this.items = items;
        MaxSizeInBytes = maxSizeInBytes;
    }

    public bool TryAdd(T item)
    {
        items.Add(item);

        var jsonSize = GetBatchSize();

        if (jsonSize <= MaxSizeInBytes)
            return true;

        items.RemoveAt(items.Count - 1);
        return false;
    }

    private int GetBatchSize()
    {
        var serializedItems = Serialize(items);

        return Compressed
            ? StringCompression.Compress(serializedItems).Length
            : Encoding.UTF8.GetBytes(serializedItems).Length;
    }

    public string Serialize()
    {
        var jsonItems = Serialize(items);

        var data = new MessageBatch<T>(Compressed)
        {
            Body = Compressed ? StringCompression.CompressToBase64(jsonItems) : jsonItems
        };

        var json = JsonConvert.SerializeObject(data);
        return json;
    }

    public static MessageBatch<T> Deserialize(string json)
    {
        var data = JsonConvert.DeserializeObject<MessageBatch<T>>(json);

        if (data == null)
            throw new SerializationException(
                $"Could not deserialize json into object of type {nameof(MessageBatch<T>)}");

        var body = data.Compressed ? StringCompression.Decompress(data.Body) : data.Body;
        var items = JsonConvert.DeserializeObject<List<T>>(body);

        if (items == null)
            throw new SerializationException(
                $"Could not deserialize items of type {typeof(T)} in MessageBatch");

        return new MessageBatch<T>(items, data.Compressed);
    }

    private static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
}
