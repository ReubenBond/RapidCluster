namespace RapidCluster.Tests.Unit;

/// <summary>
/// Helper class for creating deterministic GUIDs from names
/// </summary>
internal static class GuidUtility
{
    public static readonly Guid DnsNamespace = new("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    public static Guid Create(Guid namespaceId, string name)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var namespaceBytes = namespaceId.ToByteArray();

        SwapByteOrder(namespaceBytes);

#pragma warning disable CA5350
        var hash = System.Security.Cryptography.SHA1.HashData([.. namespaceBytes, .. nameBytes]);
#pragma warning restore CA5350

        var newGuid = new byte[16];
        Array.Copy(hash, newGuid, 16);

        newGuid[6] = (byte)((newGuid[6] & 0x0F) | 0x50);
        newGuid[8] = (byte)((newGuid[8] & 0x3F) | 0x80);

        SwapByteOrder(newGuid);
        return new Guid(newGuid);
    }

    private static void SwapByteOrder(byte[] guid)
    {
        SwapBytes(guid, 0, 3);
        SwapBytes(guid, 1, 2);
        SwapBytes(guid, 4, 5);
        SwapBytes(guid, 6, 7);
    }

    private static void SwapBytes(byte[] guid, int left, int right) => (guid[left], guid[right]) = (guid[right], guid[left]);

    /// <summary>
    /// Returns a deterministic hash code for a string that is stable across process invocations.
    /// Uses XxHash32 which is fast and has excellent distribution.
    /// Unlike string.GetHashCode(), this produces the same value every time for the same input.
    /// </summary>
    public static int GetDeterministicHashCode(string str)
    {
        ArgumentNullException.ThrowIfNull(str);

        var bytes = System.Text.Encoding.UTF8.GetBytes(str);
        return unchecked((int)System.IO.Hashing.XxHash32.HashToUInt32(bytes));
    }
}

