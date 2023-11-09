using System.Runtime.Serialization;
using AzureBatchQueue.Utils;
using Newtonsoft.Json;

namespace AzureBatchQueue;

public class MessageBatch<T>
{
    private readonly List<T> items = new();
    public List<T> Items() => items;

    private int MaxSizeInBytes { get; init; }
    public const int AzureQueueMaxSizeInBytes = 49_000; // ~ 48 KB
    public SerializerType SerializerType { get; init; }
    private readonly IMessageBatchSerializer<T> serializer;
    private int? batchSize;

    public MessageBatch(IMessageBatchSerializer<T> serializer, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
    {
        this.serializer = serializer;
        MaxSizeInBytes = maxSizeInBytes;
        SerializerType = serializer.GetSerializerType();
    }

    public MessageBatch(SerializerType serializerType, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
    {
        serializer = GetSerializer(serializerType);
        SerializerType = serializerType;
        MaxSizeInBytes = maxSizeInBytes;
    }

    /// <summary>
    /// Use this ctor if you are sure that all items will fit into the batch
    /// </summary>
    /// <param name="items"></param>
    /// <param name="serializerType"></param>
    /// <param name="maxSizeInBytes"></param>
    public MessageBatch(List<T> items, SerializerType serializerType, int maxSizeInBytes = AzureQueueMaxSizeInBytes)
        : this(serializerType, maxSizeInBytes) => this.items = items;

    public bool TryAdd(T item)
    {
        items.Add(item);

        var jsonSize = CalculateItemsSize();

        if (jsonSize <= MaxSizeInBytes)
        {
            batchSize = jsonSize;
            return true;
        }

        items.RemoveAt(items.Count - 1);
        return false;
    }

    private int CalculateItemsSize() => serializer.GetSize(items);
    public string Serialize() => serializer.Serialize(this);
    public int GetBatchSizeInBytes() => batchSize ?? CalculateItemsSize();

    public static MessageBatch<T> Deserialize(string json)
    {
        var data = JsonConvert.DeserializeObject<SerializedMessageBatch>(json);

        if (data == null)
            throw new SerializationException(
                $"Could not deserialize json into object of type {nameof(SerializedMessageBatch)}");

        var serializer = GetSerializer(data.SerializerType);
        var items = serializer.Deserialize(data.Body);

        if (items == null)
            throw new SerializationException(
                $"Could not deserialize items of type {typeof(T)} in MessageBatch");

        return new MessageBatch<T>(items.ToList(), data.SerializerType);
    }

    private static IMessageBatchSerializer<T> GetSerializer(SerializerType serializerType) =>
        serializerType switch
        {
            SerializerType.Json => new JsonSerializer<T>(),
            SerializerType.GZipCompressed => new GZipCompressedSerializer<T>(),
            _ => throw new ArgumentOutOfRangeException(nameof(serializerType), serializerType, null)
        };
}

[Serializable]
internal class SerializedMessageBatch
{
    public string Body { get; init; }
    public SerializerType SerializerType { get; init; }
}

[Serializable]
public enum SerializerType
{
    Json = 0,
    GZipCompressed = 1
}

