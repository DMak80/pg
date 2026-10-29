using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using PgWorker.NodeTools;
using Shared.Tls;

namespace PgWorker.NodeTools.Commands;

// P11: мастер-ключ ноды под lease. Команда поднимает демон через повторный
// вызов самого бинаря и сразу возвращается — Patroni-callback не блокируется.
public static class MasterLeaseCommand
{
    private const int LeaseTtlSec = 5;
    private const int KeepaliveSec = 1;
    private const string DefaultEnvFile = "/home/postgres/pgw-node.env";
    private const string DefaultPidFile = "/tmp/master-lease.pid";
    private const string PatroniHost = "127.0.0.1";

    private static readonly HashSet<string> MasterRoles = ["master", "leader", "primary"];
    private static readonly HashSet<string> ReplicaRoles = ["replica", "standby_leader"];

    public static async Task<int> RunAsync(IReadOnlyList<string> args)
    {
        var cli = new CommandLine(args);
        var envFile = cli.Value("env-file") ?? DefaultEnvFile;
        var pidFile = cli.Value("pid-file") ?? DefaultPidFile;
        var positional = cli.Positionals;
        var role = positional.FirstOrDefault()?.ToLowerInvariant();
        var daemon = cli.Has("daemon");

        // CLI-пути — только в границах разрешённых корней (CWE-22): env-файл —
        // прод /home/postgres (NodeConfigBuilders), тесты/ручные прогоны /tmp;
        // pid-файл — /tmp.
        EnvironmentGuard.EnsureFileUnderRoots(envFile, ["/home/postgres", "/tmp"], "env file");
        EnvironmentGuard.EnsureFileUnderRoot(pidFile, "/tmp", "pid file");

        if (daemon)
        {
            using var _ = new EnvFileSource(envFile);
            await LeaseLoopAsync(
                RequiredEnvList("PGW_ETCD"),
                RequiredEnv("PGW_MASTER_KEY"),
                RequiredEnv("PGW_NODE_HOST"),
                Environment.GetEnvironmentVariable("PGW_DOORMAN_PORT") ?? "6432",
                Environment.GetEnvironmentVariable("PGW_NODE_CA"),
                CancellationToken.None);
            return 0;
        }

        var etcd = new EnvFileSource(envFile);
        var key = RequiredEnv("PGW_MASTER_KEY");
        var nodeHost = RequiredEnv("PGW_NODE_HOST");
        var doormanPort = Environment.GetEnvironmentVariable("PGW_DOORMAN_PORT") ?? "6432";
        var caFile = Environment.GetEnvironmentVariable("PGW_NODE_CA");

        var isMaster = role switch
        {
            null or "" => await ProbePrimaryAsync(caFile),
            { } value when MasterRoles.Contains(value) => true,
            { } value when ReplicaRoles.Contains(value) => false,
            _ => await ProbePrimaryAsync(caFile),
        };
        etcd.Dispose();

        // Сериализация callback'ов (on_start/on_role_change могут перекрыться):
        // lock-файл держится на весь stop+start+pid-запись — двойного демона и
        // перезаписи PID не бывает.
        using var processLock = AcquireProcessLock(pidFile + ".lock");
        StopRunningDaemon(pidFile);
        if (!isMaster)
        {
            Console.WriteLine("master-lease: role is not master — daemon stopped");
            return 0;
        }

        var self = Environment.ProcessPath
            ?? throw new InvalidOperationException("pgwnodetool executable path is unavailable");
        var start = new ProcessStartInfo(self)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
        };
        start.ArgumentList.Add("master-lease");
        start.ArgumentList.Add("--env-file");
        start.ArgumentList.Add(envFile);
        start.ArgumentList.Add("--pid-file");
        start.ArgumentList.Add(pidFile);
        start.ArgumentList.Add("--daemon");

        using var child = Process.Start(start)
            ?? throw new InvalidOperationException("failed to start master-lease daemon");
        WritePidFileAtomic(pidFile, child.Id);
        Console.WriteLine($"master-lease: {key} = {nodeHost}:{doormanPort} (ttl={LeaseTtlSec}s)");
        return 0;
    }

    private static string RequiredEnv(string name)
        => Environment.GetEnvironmentVariable(name)
            ?? throw new InvalidOperationException($"required env {name} is missing or empty");

    private static IReadOnlyList<string> RequiredEnvList(string name)
    {
        var value = RequiredEnv(name);
        var endpoints = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return endpoints.Length == 0
            ? throw new InvalidOperationException($"required env {name} is empty")
            : EndpointGuard.ValidateEtcdEndpoints(endpoints);
    }

    private static async Task<bool> ProbePrimaryAsync(string? caFile)
    {
        if (string.IsNullOrWhiteSpace(caFile))
        {
            Console.Error.WriteLine("master-lease: PGW_NODE_CA is not set — https /primary unavailable");
            return false;
        }

        try
        {
            using var ca = X509CertificateLoader.LoadCertificateFromFile(caFile);
            using var handler = new SocketsHttpHandler
            {
                SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    {
                        if (certificate is not X509Certificate2 server || !TlsChain.ValidateChain(server, ca))
                            return false;
                        return ServerCertificateHasLoopbackSan(server);
                    },
                },
            };
            using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(3) };
            using var response = await client.GetAsync(
                $"https://{PatroniHost}:8008/primary", CancellationToken.None);
            return response.StatusCode == HttpStatusCode.OK;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"master-lease: probe failed — {e.GetType().Name}: {e.Message}");
            return false;
        }
    }

    // Пин ТОЧНО 127.0.0.1 (цель probe): соседние loopback-адреса не считаются.
    private static bool ServerCertificateHasLoopbackSan(X509Certificate2 certificate)
    {
        foreach (var extension in certificate.Extensions.OfType<X509SubjectAlternativeNameExtension>())
        {
            foreach (var name in extension.EnumerateIPAddresses())
                if (name.Equals(IPAddress.Loopback))
                    return true;
        }

        return false;
    }

    // Эксклюзивная блокировка stop+start (FileShare.None — POSIX-адекватный
    // mutex между callback-процессами того же контейнера).
    private static FileStream AcquireProcessLock(string path)
        => new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    // Атомарная запись PID: temp + rename — читатель никогда не видит полуфайл.
    private static void WritePidFileAtomic(string pidFile, int pid)
    {
        var temp = pidFile + ".tmp";
        File.WriteAllText(temp, pid.ToString(CultureInfo.InvariantCulture));
        File.Move(temp, pidFile, overwrite: true);
    }

    private static void StopRunningDaemon(string pidFile)
    {
        try
        {
            var pid = int.Parse(File.ReadAllText(pidFile).Trim(), CultureInfo.InvariantCulture);
            using var process = Process.GetProcessById(pid);
            // Идентификация перед kill: переиспользованный PID чужого процесса
            // (не pgwnodetool) не убивается.
            if (!process.HasExited
                && process.ProcessName.Contains("pgwnodetool", StringComparison.OrdinalIgnoreCase))
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit(2_000);
            }
        }
        catch
        {
            // демона нет, PID-файл повреждён или предыдущий процесс уже умер.
        }
    }

    private static async Task LeaseLoopAsync(
        IReadOnlyList<string> endpoints, string key, string nodeHost,
        string doormanPort, string? caFile, CancellationToken ct)
    {
        var gateway = await EtcdHelper.CreateAsync();
        long? lease = null;
        var first = true;
        while (true)
        {
            try
            {
                if (lease is null)
                    lease = await EtcdHelper.LeaseGrantAsync(gateway, endpoints, LeaseTtlSec, ct);
                await EtcdHelper.PutAsync(gateway, endpoints, key, $"{nodeHost}:{doormanPort}", lease.Value, ct);
                if (first)
                {
                    Console.WriteLine($"master-lease: {key} = {nodeHost}:{doormanPort} (ttl={LeaseTtlSec}s)");
                    first = false;
                }
            }
            catch (Exception e)
            {
                lease = null;
                Console.Error.WriteLine($"master-lease: put failed — retry: {e.Message}");
            }

            try
            {
                if (lease is { } current && !await EtcdHelper.KeepaliveAsync(gateway, endpoints, current, ct))
                    lease = null;
            }
            catch (Exception e)
            {
                lease = null;
                Console.Error.WriteLine($"master-lease: keepalive failed — retry: {e.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(KeepaliveSec), ct);
        }
    }
}
