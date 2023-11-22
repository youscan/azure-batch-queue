using System.IO.Compression;
using System.Text;

namespace AzureBatchQueue.Utils;

public static class StringCompression
{
    /// <summary>
    /// Compresses a string and returns a Gzip compressed, Base64 encoded string.
    /// </summary>
    /// <param name="uncompressedString">String to compress</param>
    public static string CompressToBase64(string jsonData)
    {
        var compressedData = Compress(jsonData);
        var base64String = Convert.ToBase64String(compressedData);

        return base64String;
    }

    public static byte[] Compress(string jsonData)
    {
        var dataBytes = Encoding.UTF8.GetBytes(jsonData);

        using var memoryStream = new MemoryStream();
        using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
        {
            gzipStream.Write(dataBytes, 0, dataBytes.Length);
        }

        return memoryStream.ToArray();
    }

    public static string Decompress(byte[] bytes) => DecompressInternal(bytes);

    /// <summary>
    /// Decompresses a Gzip compressed, Base64 encoded string and returns an uncompressed string.
    /// </summary>
    /// <param name="base64String">String to decompress.</param>
    public static string Decompress(string base64String)
    {
        var fromBase64String = Convert.FromBase64String(base64String);

        return DecompressInternal(fromBase64String);
    }

    static string DecompressInternal(byte[] fromBase64String)
    {
        byte[] decompressedBytes;
        var compressedStream = new MemoryStream(fromBase64String);

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
