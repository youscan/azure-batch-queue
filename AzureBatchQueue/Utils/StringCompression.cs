using System.IO.Compression;
using System.Text;

namespace AzureBatchQueue.Utils;

public static class StringCompression
{
    public static byte[] Compress(string json)
    {
        var dataBytes = Encoding.UTF8.GetBytes(json);

        using var memoryStream = new MemoryStream();
        using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
        {
            gzipStream.Write(dataBytes, 0, dataBytes.Length);
        }

        return memoryStream.ToArray();
    }

    public static string Decompress(byte[] bytes)
    {
        byte[] decompressedBytes;
        var compressedStream = new MemoryStream(bytes);

        using (var decompressorStream = new GZipStream(compressedStream, CompressionMode.Decompress))
        {
            using (var decompressedStream = new MemoryStream())
            {
                decompressorStream.CopyTo(decompressedStream);

                decompressedBytes = decompressedStream.ToArray();
            }
        }

        return Encoding.UTF8.GetString(decompressedBytes);
    }
}
