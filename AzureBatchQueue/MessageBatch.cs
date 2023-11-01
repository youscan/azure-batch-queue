namespace AzureBatchQueue;

public class MessageBatch<T>
{
    private readonly List<T> items = new();
    public List<T> Items() => items;

    public bool TryAdd(T item)
    {
        items.Add(item);
        return true;
    }
}
