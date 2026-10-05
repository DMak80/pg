using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

/// <summary>
/// СОБСТВЕННЫЙ postgres одного Fact (e2e-isolation, docs/e2e-isolation.md):
/// guid-имя pgw-pg-{guid}, динамический хост-порт, физический слот приёмника,
/// TLS (self-signed, генерируется кодом фикстуры): серты кладутся resource
/// mapping'ом в /tmp, после старта docker-exec раскладывает их в
/// /var/lib/postgresql/tls с правами postgres, ALTER SYSTEM + reload — ssl=on
/// для приёмника (SSL Mode=Require, Trust Server Certificate). Teardown при
/// ЛЮБОМ исходе + АССЕРТ ЧИСТОТЫ: pgw-pg-{guid} отсутствует в docker ps -a.
/// </summary>
public sealed class OwnPostgres : IAsyncDisposable
{
    public const string Password = "pgw-test-su";
    public const string Slot = "pgw_bkp_it";

    private const string Image = "postgres:17-alpine";

    private readonly IContainer _container;
    private readonly string _certsDir;

    private OwnPostgres(string slug, string runId, IContainer container, string certsDir)
    {
        Slug = slug;
        RunId = runId;
        _container = container;
        _certsDir = certsDir;
    }

    public string Slug { get; }

    /// <summary>Идентификатор прогона (полный guid): имя контейнера pgw-pg-{guid}.</summary>
    public string RunId { get; }

    private string ContainerName => $"pgw-pg-{RunId}";

    /// <summary>Динамический хост-порт (никаких литералов).</summary>
    public int HostPort { get; private set; }

    /// <summary>DSN суперпользователя для тестовых SQL-зондов (host-клиент).</summary>
    public string AdminDsn =>
        $"Host=localhost;Port={HostPort};Username=postgres;Password={Password};Database=postgres";

    /// <summary>Опции приёмника на это окружение: слот фикстуры, Cluster/Shard —
    /// guid-значения, S3 — host-эндпоинт своего MinIO.</summary>
    public PgWorker.WalReceiver.WalReceiverOptions ReceiverOptions(
        string s3Endpoint, string cluster, string shard, string accessKey, string secretKey)
        => new(
            PgHost: "localhost",
            PgPort: HostPort,
            PgUser: "postgres",
            PgPassword: Password,
            PgDbname: "postgres",
            Slot: Slot,
            Cluster: cluster,
            Shard: shard,
            S3Endpoint: s3Endpoint,
            S3Region: null,
            S3Bucket: "pgw-backups-test",
            S3AccessKey: accessKey,
            S3SecretKey: secretKey,
            S3PathStyle: true);

    /// <summary>Подъём: postgres:17-alpine (образ в images.txt), pg_isready ≤ 45 c,
    /// затем TLS-раскладка + reload + физический слот immediately_reserve.
    /// initdbArgs — нестандартные аргументы initdb (напр. «--wal-segsize=32»
    /// для permanent-теста несовместимого segment size).</summary>
    public static async Task<OwnPostgres> StartAsync(
        string slug, CancellationToken ct = default, string? initdbArgs = null)
    {
        var runId = Guid.NewGuid().ToString("N");
        var certsDir = Path.Combine(Path.GetTempPath(), $"pgw-pg-tls-{runId}");
        Directory.CreateDirectory(certsDir);
        WriteSelfSignedPems(certsDir);

        var builder = new ContainerBuilder(Image)
            .WithName($"pgw-pg-{runId}")
            .WithEnvironment("POSTGRES_PASSWORD", Password)
            .WithPortBinding(5432, assignRandomHostPort: true)
            .WithResourceMapping(new FileInfo(Path.Combine(certsDir, "server.crt")), "/tmp/pgw-tls")
            .WithResourceMapping(new FileInfo(Path.Combine(certsDir, "server.key")), "/tmp/pgw-tls")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted(
                "pg_isready -U postgres",
                w => w.WithTimeout(TimeSpan.FromSeconds(45)))); // ≤ 100 c: падаем быстро
        if (initdbArgs is not null)
            builder = builder.WithEnvironment("POSTGRES_INITDB_ARGS", initdbArgs);
        var container = builder.Build();
        await container.StartAsync(ct);
        var fx = new OwnPostgres(slug, runId, container, certsDir)
        {
            HostPort = container.GetMappedPublicPort(5432),
        };
        await fx.ConfigureTlsAsync(ct);
        await fx.AllowReplicationHbaAsync(ct);
        await fx.CreateSlotAsync(ct);
        return fx;
    }

    // Self-signed PEM (RSA-2048, CN=localhost): приёмник Trust Server Certificate —
    // цепочка доверия не нужна, важно само шифрование (SSL Mode=Require).
    private static void WriteSelfSignedPems(string dir)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        File.WriteAllText(Path.Combine(dir, "server.crt"), cert.ExportCertificatePem());
        File.WriteAllText(Path.Combine(dir, "server.key"), rsa.ExportPkcs8PrivateKeyPem());
    }

    // Раскладка сертов с правами postgres (0600 ключ — требование PG) + ALTER SYSTEM
    // ssl=on + reload (PG 10+ позволяет включить SSL по reload).
    private async Task ConfigureTlsAsync(CancellationToken ct)
    {
        var install = await _container.ExecAsync(new[]
        {
            "sh", "-c",
            "mkdir -p /var/lib/postgresql/tls && " +
            "install -m 600 -o postgres -g postgres /tmp/pgw-tls/server.key /var/lib/postgresql/tls/server.key && " +
            "install -m 644 -o postgres -g postgres /tmp/pgw-tls/server.crt /var/lib/postgresql/tls/server.crt",
        }, ct);
        install.ExitCode.Should().Be(0, $"раскладка TLS: {install.Stderr}");

        await ExecAsync("ALTER SYSTEM SET ssl = 'on'", ct);
        await ExecAsync("ALTER SYSTEM SET ssl_cert_file = '/var/lib/postgresql/tls/server.crt'", ct);
        await ExecAsync("ALTER SYSTEM SET ssl_key_file = '/var/lib/postgresql/tls/server.key'", ct);
        await ExecAsync("SELECT pg_reload_conf()", ct);

        // Проверка TLS: подключение с SSL Mode=Require проходит (доказательство ssl=on).
        await using var conn = new Npgsql.NpgsqlConnection(
            $"{AdminDsn};SSL Mode=Require;Trust Server Certificate=true");
        await conn.OpenAsync(ct);
    }

    // Физический слот приёмника: immediately_reserve — restart_lsn зафиксирован сразу.
    private async Task CreateSlotAsync(CancellationToken ct)
        => await ExecAsync(
            $"SELECT pg_create_physical_replication_slot('{Slot}', true)", ct);

    /// <summary>pg_hba.conf по умолчанию образа НЕ содержит replication-строку
    /// (диагностика: 28000 no pg_hba.conf entry for replication connection) —
    /// дописываем разрешение replication-соединений с паролем и reload.</summary>
    private async Task AllowReplicationHbaAsync(CancellationToken ct)
    {
        var append = await _container.ExecAsync(new[]
        {
            "sh", "-c",
            "echo 'host replication all 0.0.0.0/0 scram-sha-256' >> \"$PGDATA/pg_hba.conf\"",
        }, ct);
        append.ExitCode.Should().Be(0, $"pg_hba replication: {append.Stderr}");
        await ExecAsync("SELECT pg_reload_conf()", ct);
    }

    private async Task ExecAsync(string sql, CancellationToken ct)
    {
        await using var conn = new Npgsql.NpgsqlConnection(AdminDsn);
        await conn.OpenAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>SQL-зонд restart_lsn слота (AC1): LSN «X/Y» → ulong.</summary>
    public async Task<ulong> ReadRestartLsnAsync(CancellationToken ct)
    {
        await using var conn = new Npgsql.NpgsqlConnection(AdminDsn);
        await conn.OpenAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = $"SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = '{Slot}'";
        var raw = (string?)(await command.ExecuteScalarAsync(ct));
        raw.Should().NotBeNull($"слот {Slot} обязан существовать");
        var parts = raw!.Split('/');
        return (ulong.Parse(parts[0], System.Globalization.NumberStyles.HexNumber) << 32)
               | ulong.Parse(parts[1], System.Globalization.NumberStyles.HexNumber);
    }

    /// <summary>Teardown при любом исходе: стоп/rm СВОЕГО контейнера, temp-серты →
    /// АССЕРТ ЧИСТОТЫ: pgw-pg-{guid} в docker ps -a отсутствует.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _container.DisposeAsync();
        }
        catch
        {
            // падение dispose не маскируем — ассерт чистоты ниже отчитается
        }

        try
        {
            Directory.Delete(_certsDir, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
        }

        var left = await E2eFixture.RunProcessAsync(
            "docker",
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={ContainerName}"],
            CancellationToken.None);
        left.Should().BeEmpty(
            $"{Slug}: teardown окружения неполный — остался контейнер {ContainerName}");
    }
}
