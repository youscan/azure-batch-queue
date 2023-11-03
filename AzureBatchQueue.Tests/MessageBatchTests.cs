using AzureBatchQueue.Utils;
using FluentAssertions;
using NUnit.Framework;

namespace AzureBatchQueue.Tests;

[TestFixture]
public class MessageBatchTests
{
    [Test]
    public void FillBatchUntilMaxSize()
    {
        const long item = 123;
        const int jsonSizeOfOneItem = 5;
        var batch = new MessageBatch<long>(GetSerializer<long>(compressed: false), jsonSizeOfOneItem + 1);

        var firstAdd = batch.TryAdd(item);
        var secondAdd = batch.TryAdd(item);

        firstAdd.Should().BeTrue();
        secondAdd.Should().BeFalse();
        batch.Items().Should().BeEquivalentTo(new[] { item });
    }

    [Test]
    public void CompressedBatchAddsOverhead()
    {
        const long item = 123;
        const int jsonSizeOfOneItem = 5;
        var compressedBatch = new MessageBatch<long>(GetSerializer<long>(compressed: true), jsonSizeOfOneItem);
        var batch = new MessageBatch<long>(GetSerializer<long>(compressed: false), jsonSizeOfOneItem);

        var compressedAdd = compressedBatch.TryAdd(item);
        var add = batch.TryAdd(item);

        compressedAdd.Should().BeFalse();
        add.Should().BeTrue();
    }

    [Test]
    public void CompressWhenManySimilarItems()
    {
        var compressedBatch = new MessageBatch<TestItem>(GetSerializer<TestItem>(compressed: true), 100);
        var batch = new MessageBatch<TestItem>(GetSerializer<TestItem>(compressed: false), 100);

        bool compressedAdd;
        bool add;

        do
        {
            var item = new TestItem("Dimka", 33, "Ukraine");
            compressedAdd = compressedBatch.TryAdd(item);
            add = batch.TryAdd(item);
        } while (compressedAdd || add);

        compressedBatch.Items().Count.Should().BeGreaterThan(batch.Items().Count);
    }

    private record TestItem(string Name, int Age, string Country);

    private IMessageBatchSerializer<T> GetSerializer<T>(bool compressed) => compressed ? new GZipCompressedSerializer<T>() : new JsonSerializer<T>();
}
