using System.Net;
using System.Net.Sockets;

namespace PgWorker.NodeTools;

// Диапазоны контура: loopback, RFC1918, link-local и IPv6 ULA.
public static class NetworkAddress
{
    public static bool IsLoopbackOrPrivate(IPAddress address)
    {
        if (IPAddress.IsLoopback(address))
            return true;
        if (address.AddressFamily != AddressFamily.InterNetwork)
            return IsUla(address);
        var bytes = address.GetAddressBytes();
        return bytes[0] == 10
            || (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31)
            || (bytes[0] == 192 && bytes[1] == 168)
            || (bytes[0] == 169 && bytes[1] == 254);
    }

    private static bool IsUla(IPAddress address)
    {
        var bytes = address.GetAddressBytes();
        return bytes.Length == 16 && bytes[0] == 0xfd;
    }
}
