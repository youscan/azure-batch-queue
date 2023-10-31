namespace AzureBatchQueue;

public class MessageBatchCompletedException : Exception
{
    public MessageBatchCompletedException(string message) : base(message) { }
}

public class MessageTooLargeException : Exception
{
    public MessageTooLargeException(string message) : base(message) { }
    public MessageTooLargeException(string message, Exception innerEx) : base(message, innerEx) { }
}
