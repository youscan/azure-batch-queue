using System.IO.Compression;

namespace AzureBatchQueue.Tests.Serializers;

public static class StringCompression
{
    public static byte[] Decompress(byte[] bytes)
    {
        using var compressedStream = new MemoryStream(bytes);
        using var decompressorStream = new GZipStream(compressedStream, CompressionMode.Decompress);
        using var decompressedStream = new MemoryStream();
        decompressorStream.CopyTo(decompressedStream);

        var decompressedBytes = decompressedStream.ToArray();

        return decompressedBytes;
    }
}
