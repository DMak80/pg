using System.Net;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using PgWorker.NodeTools;

namespace PgWorker.NodeTools.Commands;

// Стендовый Patroni-эмулятор (HAProxy health-check): опрашивает локальный PG
// и регистрирует адрес ноды в etcd под lease. Все параметры — CLI.
public static class RoleCheckCommand
{
    private const int LeaseTtlSec = 5;
    private const int KeepaliveSec = 1;

    public static async Task<int> RunAsync(IReadOnlyList<string> args)
    {
        var cli = new CommandLine(args);
        var nodeName = cli.Value("node-name")
            ?? throw new ArgumentException("--node-name is required");
        var endpoints = EndpointGuard.ValidateEtcdEndpoints(
            cli.List("etcd-endpoints", "http://etcd:2379"));
        var pgHost = cli.Value("pg-host") ?? "127.0.0.1";
        var pgPort = cli.Int("pg-port", 5432);
        var pgUser = cli.Value("pg-user") ?? "postgres";
        var pgDatabase = cli.Value("pg-database") ?? "postgres";
        // Пароль — из --pg-password-file (не в argv/process list); CLI-параметр
        // оставлен для стенда без пароля (trust) — латентного риска нет.
        var pgPassword = cli.Value("pg-password-file") is { } pwFile
            ? File.ReadAllText(pwFile).Trim()
            : cli.Value("pg-password");
        var listenPort = cli.Int("listen-port", 8008);

        var registration = Task.Run(
            () => RegisterLoopAsync(endpoints, nodeName, default), CancellationToken.None);
        await ServeAsync(listenPort, nodeName, pgHost, pgPort, pgUser, pgDatabase, pgPassword);
        await registration;
        return 0;
    }

    private static async Task ServeAsync(
        int listenPort, string nodeName, string pgHost, int pgPort,
        string pgUser, string pgDatabase, string? pgPassword)
    {
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://+:{listenPort}/");
        listener.Start();

        while (true)
        {
            var context = await listener.GetContextAsync();
            _ = Task.Run(async () =>
            {
                try
                {
                    await HandleAsync(context, nodeName, pgHost, pgPort, pgUser, pgDatabase, pgPassword);
                }
                catch
                {
                    // сокет клиента мог уйти; health-check повторится.
                }
                finally
                {
                    context.Response.Close();
                }
            });
        }
    }

    private static async Task HandleAsync(
        HttpListenerContext context, string nodeName, string pgHost, int pgPort,
        string pgUser, string pgDatabase, string? pgPassword)
    {
        var path = context.Request.Url?.AbsolutePath ?? "/";
        if (path == "/whoami")
        {
            context.Response.StatusCode = string.IsNullOrEmpty(nodeName) ? 404 : 200;
            await context.Response.OutputStream.WriteAsync(Encoding.UTF8.GetBytes(nodeName + "\n"));
            return;
        }

        bool? inRecovery;
        try
        {
            inRecovery = await IsInRecoveryAsync(pgHost, pgPort, pgUser, pgDatabase, pgPassword);
        }
        catch
        {
            context.Response.StatusCode = 503;
            await context.Response.OutputStream.WriteAsync("pg unreachable\n"u8.ToArray());
            return;
        }

        bool? ok;
        if (path is "/" or "/read-only")
            ok = true;
        else if (path == "/primary")
            ok = inRecovery == false;
        else if (path == "/replica")
            ok = inRecovery == true;
        else
        {
            context.Response.StatusCode = 404;
            return;
        }

        context.Response.StatusCode = ok == true ? 200 : 503;
        await context.Response.OutputStream.WriteAsync("OK\n"u8.ToArray());
    }

    private static async Task<bool> IsInRecoveryAsync(
        string host, int port, string user, string database, string? password)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Port = port,
            Username = user,
            Password = password,
            Database = database,
            Timeout = 5,
        };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_is_in_recovery()";
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    private static async Task RegisterLoopAsync(
        IReadOnlyList<string> endpoints, string nodeName, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(nodeName))
            return;

        var key = "/cluster/nodes/" + nodeName;
        var gateway = await EtcdHelper.CreateAsync();
        long? lease = null;
        var first = true;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (lease is null)
                    lease = await EtcdHelper.LeaseGrantAsync(gateway, endpoints, LeaseTtlSec, ct);
                var ip = await NodeIpAsync(endpoints[0]);
                await EtcdHelper.PutAsync(gateway, endpoints, key, ip, lease.Value, ct);
                if (first)
                {
                    Console.WriteLine($"registered {key} (lease ttl={LeaseTtlSec}s)");
                    first = false;
                }
            }
            catch (Exception e)
            {
                lease = null;
                Console.Error.WriteLine($"registration failed — retry: {e.Message}");
            }

            try
            {
                if (lease is { } current && !await EtcdHelper.KeepaliveAsync(gateway, endpoints, current, ct))
                    lease = null;
            }
            catch (Exception e)
            {
                lease = null;
                Console.Error.WriteLine($"registration keepalive failed — retry: {e.Message}");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(KeepaliveSec), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private static async Task<string> NodeIpAsync(string endpoint)
    {
        var uri = EndpointGuard.ValidateHttpUrl(endpoint, "etcd endpoint");
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(uri.Host, uri.Port == -1 ? 2379 : uri.Port);
        return (socket.LocalEndPoint as IPEndPoint)?.Address.ToString()
            ?? throw new InvalidOperationException("local endpoint is not IPv4");
    }
}
