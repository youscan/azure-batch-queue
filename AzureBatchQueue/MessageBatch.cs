using System.Runtime.Serialization;
using AzureBatchQueue.Utils;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class MessageBatch<T>
{
    public bool Compressed { get; init; }
    private int MaxSizeInBytes { get; init; }
    public const int AzureQueueMaxSizeInBytes = 49_000; // ~ 48 KB

    private readonly List<T> items = new();
    public List<T> Items() => items;

    private readonly IMessageBatchSerializer<T> serializer;

    private MessageBatch(IMessageBatchSerializer<T> serializer) => this.serializer = serializer;

    public MessageBatch(bool compressed, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
        : this(compressed ? new JsonSerializer<T>() : new GZipCompressedSerializer<T>())
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
    public MessageBatch(List<T> items, bool compressed, int maxSizeInBytes = AzureQueueMaxSizeInBytes) : this(compressed, maxSizeInBytes) => this.items = items;

    public bool TryAdd(T item)
    {
        items.Add(item);

        var jsonSize = GetBatchSize();

        if (jsonSize <= MaxSizeInBytes)
            return true;

        items.RemoveAt(items.Count - 1);
        return false;
    }

    private int GetBatchSize() => serializer.GetSize(items);
    public string Serialize() => serializer.Serialize(this);

    public static MessageBatch<T> Deserialize(string json)
    {
        var data = JsonConvert.DeserializeObject<SerializedMessageBatch>(json);

        if (data == null)
            throw new SerializationException(
                $"Could not deserialize json into object of type {nameof(SerializedMessageBatch)}");

        IMessageBatchSerializer<T> serializer = data.Compressed ? new JsonSerializer<T>() : new GZipCompressedSerializer<T>();
        var items = serializer.Deserialize(data.Body);

        if (items == null)
            throw new SerializationException(
                $"Could not deserialize items of type {typeof(T)} in MessageBatch");

        return new MessageBatch<T>(items.ToList(), data.Compressed);
    }

    private static string Serialize(IEnumerable<T> items) => JsonConvert.SerializeObject(items);
}

[Serializable]
internal class SerializedMessageBatch
{
    public bool Compressed { get; init; }
    public string Body { get; init; }
}
