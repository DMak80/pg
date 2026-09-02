using System.Net;
using System.Net.Sockets;

namespace KafkaWorker.App;

// Резолв/коннект etcd-клиента против Docker embedded DNS (t09; arch/16 §7):
// 1) PooledConnectionLifetime — пул пере-резолвит DNS (застарелые адреса после
//    пересоздания etcd-контейнера; прецедент DockerEngineFactory);
// 2) последовательный IPv4-first резолв — параллельные A/AAAA-запросы .NET
//    против Docker embedded DNS (127.0.0.11) флейпят «Name or service not known».
public static class EtcdConnectCallback
{
    public static SocketsHttpHandler CreateHandler() => new()
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        ConnectCallback = ConnectAsync,
    };

    public static async ValueTask<Stream> ConnectAsync(
        SocketsHttpConnectionContext context, CancellationToken ct)
    {
        var host = context.DnsEndPoint.Host;
        var port = context.DnsEndPoint.Port;

        // IP-литерал — без DNS.
        if (IPAddress.TryParse(host, out var literal))
            return await ConnectToAddressesAsync([literal], port, ct);

        var addresses = await Dns.GetHostAddressesAsync(host, ct);
        return await ConnectToAddressesAsync(OrderIpv4First(addresses), port, ct);
    }

    // IPv4 раньше IPv6: сортировка, не фильтр (IPv6-only окружения не теряются).
    internal static IPAddress[] OrderIpv4First(IPAddress[] addresses)
        => [.. addresses.OrderBy(a => a.AddressFamily == AddressFamily.InterNetwork ? 0 : 1)];

    // Последовательные попытки: первый успех — Stream; все упали — бросок последнего
    // исключения (EtcdGateway обернёт в Result.Failed — проба отдаст структуру).
    internal static async Task<Stream> ConnectToAddressesAsync(
        IPAddress[] addresses, int port, CancellationToken ct)
    {
        Exception? last = null;
        foreach (var address in addresses)
        {
            var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await socket.ConnectAsync(address, port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch (Exception ex)
            {
                last = ex;
                socket.Dispose();
            }
        }

        throw last ?? new SocketException((int)SocketError.HostNotFound);
    }
}
