using System.IO.Compression;
using System.Text;

namespace AzureBatchQueue.Utils;

public static class StringCompression
{
    public static byte[] Compress(byte[] bytes) => CompressInternal(bytes);
    public static byte[] Compress(string json) => CompressInternal(Encoding.UTF8.GetBytes(json));

    static byte[] CompressInternal(byte[] dataBytes)
    {
        using var memoryStream = new MemoryStream();
        using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
        {
            gzipStream.Write(dataBytes, 0, dataBytes.Length);
        }

        return memoryStream.ToArray();
    }

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
