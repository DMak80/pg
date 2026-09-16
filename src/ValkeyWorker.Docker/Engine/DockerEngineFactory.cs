using System.Net.Sockets;

namespace ValkeyWorker.Docker.Engine;

// Фабрика движков: endpoint "unix:///var/run/docker.sock" | "tcp://host:2375".
// API-версия закреплена v1.44 (прецедент kfw; docker >= 23).
public class DockerEngineFactory
{
    // Транспортный handler: unix → ConnectCallback с UnixDomainSocketEndPoint.
    internal HttpMessageHandler CreateHandler(string endpoint)
    {
        var sockets = new SocketsHttpHandler
        {
            // docker-прокси держит соединения — не рвём их агрессивно
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        if (endpoint.StartsWith("unix://", StringComparison.Ordinal))
        {
            var socketPath = endpoint["unix://".Length..];
            sockets.ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new UnixDomainSocketEndPoint(socketPath), ct);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            };
        }

        return sockets;
    }

    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).
    public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
    {
        var baseAddress = endpoint.StartsWith("unix://", StringComparison.Ordinal)
            ? "http://localhost" // фиктивный хост: соединение уходит в unix-сокет через ConnectCallback
            : endpoint;
        var httpClient = new HttpClient(CreateHandler(endpoint)) { BaseAddress = new Uri(baseAddress) };
        return new DockerEngine(httpClient, hostAlias);
    }
}
