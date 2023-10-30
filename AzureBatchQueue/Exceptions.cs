namespace AzureBatchQueue;

public class MessageBatchCompletedException : Exception
{
    public MessageBatchCompletedException(string message) : base(message) { }
}
