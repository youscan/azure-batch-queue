using System.IO.Compression;

namespace AzureBatchQueue.Utils;

public static class StringCompression
{
    public static byte[] Decompress(byte[] bytes)
    {
        var compressedStream = new MemoryStream(bytes);

        using var decompressorStream = new GZipStream(compressedStream, CompressionMode.Decompress);
        using var decompressedStream = new MemoryStream();
        decompressorStream.CopyTo(decompressedStream);

        var decompressedBytes = decompressedStream.ToArray();

        return decompressedBytes;
    }
}
