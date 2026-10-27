using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using PgWorker.NodeTools;

namespace PgWorker.NodeTools.Commands;

// Синкер топологии стенда: etcd → HAProxy runtime API. Адрес применяется
// только после /whoami-подтверждения идентичности ноды; недоступность etcd
// сохраняет последние адреса (P9), исчезновение ключа — graceful-увод в 127.0.0.1.
public static class HasyncCommand
{
    private const string DeadAddress = "127.0.0.1";

    public static async Task<int> RunAsync(IReadOnlyList<string> args)
    {
        var cli = new CommandLine(args);
        var endpoints = EndpointGuard.ValidateEtcdEndpoints(
            cli.List("etcd-endpoints", "http://etcd:2379"));
        var backend = cli.Value("backend")
            ?? throw new ArgumentException("--backend is required");
        var servers = cli.List("servers", "")
            ?? throw new ArgumentException("--servers is required");
        if (servers.Count == 0)
            throw new ArgumentException("--servers must contain at least one node");

        var socketPath = cli.Value("socket") ?? "/run/haproxy/admin.sock";
        // Unix-сокет HAProxy — только под /run (CWE-22, реальное расположение
        // /run/haproxy/admin.sock).
        EnvironmentGuard.EnsureFileUnderRoot(socketPath, "/run", "socket path");
        var whoamiPort = cli.Int("whoami-port", 8008);
        var serverPort = cli.Value("server-port") ?? "5432";
        var interval = cli.Double("interval", 2);
        var missingGrace = cli.Int("missing-grace", 3);
        var resyncCycles = cli.Int("resync-cycles", 30);
        ValidateRuntimeArgs(backend, servers, serverPort);

        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        var missing = new Dictionary<string, int>(StringComparer.Ordinal);
        var gateway = await EtcdHelper.CreateAsync();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };

        var cycle = 0;
        while (true)
        {
            if (cycle % resyncCycles == 0)
                state.Clear();
            cycle++;

            foreach (var server in servers)
            {
                string? address;
                try
                {
                    address = await EtcdHelper.GetAsync(gateway, endpoints, "/cluster/nodes/" + server, CancellationToken.None);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{server}: etcd unreachable — keeping the last address ({e.Message})");
                    missing[server] = 0;
                    continue;
                }

                if (address is null)
                {
                    missing[server] = missing.GetValueOrDefault(server) + 1;
                    if (missing[server] >= missingGrace && state.GetValueOrDefault(server) != DeadAddress &&
                        ApplyAddress(state, backend, server, DeadAddress, serverPort, socketPath,
                            " (registration vanished — node is dead)"))
                        missing[server] = missingGrace;
                    continue;
                }

                missing[server] = 0;
                if (address == state.GetValueOrDefault(server))
                    continue;

                // per-server изоляция отказов (P9): transient-сбой whoami не
                // рвёт цикл (python-семантика) — адрес просто не применяется,
                // ретрай следующим циклом.
                string whoami;
                try
                {
                    whoami = await WhoamiAsync(http, address, whoamiPort);
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        $"{server}: {address} does not answer /whoami ({e.Message}) — not applying the address");
                    continue;
                }

                if (whoami != server)
                {
                    Console.WriteLine(
                        $"{server}: {address} answers with name '{whoami}' — that is not {server}, not applying the address");
                    continue;
                }

                try
                {
                    if (!ApplyAddress(state, backend, server, address, serverPort, socketPath))
                        continue;
                }
                catch (Exception e)
                {
                    // HAProxy runtime недоступен (перезапуск): состояние runtime
                    // сброшено — форс-ресинк всех адресов (python-семантика).
                    Console.WriteLine(
                        $"{server}: HAProxy runtime unreachable ({e.Message}) — clearing state for resync");
                    state.Clear();
                    break;
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(interval), CancellationToken.None);
        }
    }

    // CLI-значения попадают в runtime-команду HAProxy: строгая форма —
    // инъекция в `set server <backend>/<server> …` исключена.
    private static void ValidateRuntimeArgs(string backend, IReadOnlyList<string> servers, string serverPort)
    {
        if (!System.Text.RegularExpressions.Regex.IsMatch(backend, "^[A-Za-z0-9_-]+$"))
            throw new ArgumentException("--backend must match [A-Za-z0-9_-]+");
        foreach (var server in servers)
            if (!System.Text.RegularExpressions.Regex.IsMatch(server, "^[A-Za-z0-9_-]+$"))
                throw new ArgumentException($"--servers entry '{server}' must match [A-Za-z0-9_-]+");
        if (!int.TryParse(serverPort, out var port) || port is < 1 or > 65535)
            throw new ArgumentException("--server-port must be a TCP port number");
    }

    private static async Task<string> WhoamiAsync(HttpClient http, string address, int port)
    {
        // Значение из etcd обязано быть IP-литералом (CWE-918): DNS не допускаем.
        if (!IPAddress.TryParse(address, out var parsed) || !NetworkAddress.IsLoopbackOrPrivate(parsed))
            throw new ArgumentException($"blocked non-private node ip: {address}");

        using var response = await http.GetAsync(new UriBuilder("http", address, port, "/whoami").Uri);
        return (await response.Content.ReadAsStringAsync(CancellationToken.None)).Trim();
    }

    private static bool ApplyAddress(
        Dictionary<string, string> state, string backend, string server,
        string address, string port, string socketPath, string reason = "")
    {
        var reply = HaproxyRuntime.Call(
            $"set server {backend}/{server} addr {address} port {port}", socketPath);
        var ok = string.IsNullOrWhiteSpace(reply)
            || reply.Contains("no need to change", StringComparison.OrdinalIgnoreCase)
            || reply.TrimStart().StartsWith("IP changed", StringComparison.Ordinal);
        if (!ok)
        {
            Console.WriteLine($"{server}: runtime replied: {reply.Trim()}");
            return false;
        }

        state[server] = address;
        if (string.IsNullOrWhiteSpace(reply))
            Console.WriteLine($"{server} → {address}:{port}{reason}");
        return true;
    }
}
