using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Изолированное e2e-окружение контура «Вариант A» (docs/e2e-isolation.md).
/// RYUK ОТКЛЮЧЁН: владелец всех объектов контура — сам прогон (фаза 1/teardown,
/// stop-режим упавшего сценария обязан ОСТАВИТЬ etcd/сеть для разбора), ryuk же
/// сносит testcontainers-объекты (etcd, сеть окружения) при смерти host-процесса
/// — ломает и телеметрию failed-прогона, и ассерт чистоты.
/// ФАЗА 1 (хост, StartAsync): собственная docker-сеть pgw-en-{guid}, etcd-контур
/// (1 или 3 узла), опционально ownS3-нода — ВСЁ без публикации портов на хост:
/// участники общаются внутри сети по DNS-именам и статичным контейнерным
/// портам. ФАЗА 2 (E2eContour): тестовый раннер — контейнер в той же сети;
/// раннер получает окружение через AttachAsync (env контура) и стартует
/// SUT-воркеров (контейнеры pgworker:e2e с docker.sock, внутрисетевые адреса).
/// Смерть etcd-контейнера уносит ключи: per-environment etcd снимает проблему
/// чистки ключей. DisposeAsync (фаза 1) при ЛЮБОМ исходе: телеметрия ДО
/// удалений → успешный прогон: rm контейнеров/томов/сети + ассерт чистоты;
/// упавший (exit≠0 раннера): контейнеры ОСТАНАВЛИВАЮТСЯ, не удаляются — разбор
/// по живым объектам (README-cleanup.txt). Чужие pgw-* объекты параллельных
/// прогонов и dev-стенда не трогаются (own-only по guid/тегу).
/// Статические ассеты (publish-выводы app/тестов, образы) собираются один раз
/// на процесс — не мутабельный рантайм-стейт.
/// </summary>
public sealed class E2eEnvironment : IAsyncDisposable
{
    static E2eEnvironment()
    {
        // Владелец объектов контура — сам прогон: ryuk отключён глобально для
        // E2E-процесса (иначе сносит etcd/сеть при смерти host-процесса).
        Environment.SetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED", "true");
    }

    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";

    /// <summary>Образ SUT-воркера (контур «Вариант A»): publish PgWorker.App НА
    /// ХОСТЕ → образ publish-COPY (docker/PgWorker.E2E.Dockerfile); локальная
    /// сборка, в registry не кладётся.</summary>
    public const string WorkerImage = "pgworker:e2e";

    /// <summary>Образ тестового раннера (фаза 2): publish тестовой сборки НА
    /// ХОСТЕ → docker/E2eRunner.Dockerfile (runtime + docker CLI + артефакты).</summary>
    public const string RunnerImage = "pgw-e2e-runner:e2e";

    // Нода пула — ownS3-образ (E2E: локальная сборка publish-COPY, канон
    // E2E-образов; в registry не кладётся — только внешние образы там).
    public const string OwnS3Image = "owns3:e2e";

    // mc запинен существующим docker-тегом (ближайший к MC_VERSION образа
    // pgworker-backup, arch/19 §10; у docker-тегов и архивных тегов dl.min.io
    // метки времени расходятся).
    internal const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";

    // Образ узла (задача 25): без DOORMAN_URL — узел без пулера, PgWorker
    // запускается с EnableDoorman=false (Д4, R1).
    public const string NodeImage = "pgworker-node:e2e";

    public const string JobImage = "pgworker-backup:e2e";

    /// <summary>Образ wal-агента-приёмника (t27, arch/19 §3): собирается рядом с
    /// джобовым (та же ветка withMinio) — pgworker-wal:e2e.</summary>
    public const string WalImage = "pgworker-wal:e2e";

    /// <summary>Образ file_sd-генератора (t15, arch/18 §5.4): собирается ЛЕНИВО —
    /// из сценария E2ePatroniFileSd (EnsureSdImageAsync), чтобы остальные E2E-серии
    /// (включая кейс-маркер Scale_AddEmptyShard) не платили этой сборкой.</summary>
    public const string SdImage = "sdgenerator:e2e";

    // Креды ownS3-ноды E2E: root-пара (mc/сид/чтение теста) и read-write ключ
    // ноды — ему соответствуют env-профили кредов воркера (arch/19 §7).
    public const string S3RootUser = "owns3-e2e-root";
    public const string S3RootPassword = "owns3-e2e-root-pwd";
    public const string S3WriterKey = "pgw-e2e-writer";
    public const string S3WriterSecret = "pgw-e2e-writer-pwd";
    private const string BucketName = "pgworker-backups";

    // t36: пул S3 E2E — имя env-профиля кредов (= config.secret_profile пула,
    // сид SeedS3PoolAsync) и bucket per-install (единый на всех нодах пула,
    // arch/19 §5; ownS3-ноды окружения поднимается с ним в StartAsync).
    public const string PoolProfile = "backups-main";
    public const string PoolBucket = BucketName;

    /// <summary>Inner-режим: тело факта исполняется в раннер-контейнере
    /// (фаза 2) — окружение не создаётся, а подключается (AttachAsync).</summary>
    public static bool IsInnerRunner =>
        Environment.GetEnvironmentVariable("PGW_E2E_INNER") == "1";

    private readonly List<HostInstance> _hosts = [];

    /// <summary>Живые воркеры окружения (диагностика сценариев).</summary>
    public IReadOnlyList<HostInstance> Hosts => _hosts;

    private readonly HttpClient _gatewayHttp = new();
    private readonly IReadOnlyList<IContainer>? _etcdNodes;      // только фаза 1 (хост)
    private readonly IReadOnlyList<string> _etcdNames;
    private readonly INetwork? _net;                             // только фаза 1 (хост)
    private readonly IContainer? _minio;                         // только фаза 1

    // Политика телеметрии E2E (docs/e2e-launch.md): упавший сценарий (exit≠0
    // раннера / MarkFailed) — teardown ОСТАНАВЛИВАЕТ контейнеры, не удаляя:
    // логи «что произошло» доступны до зачистки. Перезапуск теста для
    // выяснения причин запрещён.
    private bool _failed;

    // Идентификатор прогона: полный guid — в именах сети/etcd/ownS3/воркеров/
    // раннера; тег (8 hex) — в имени кластера сценария. Имена ВСЕХ движковых
    // контейнеров/томов содержат имя кластера, поэтому всё окружение
    // опознаётся по своему идентификатору: чистка и ассерт чистоты — СТРОГО
    // по нему, чужие pgw-* параллельных прогонов не трогаем.
    private readonly string _runId;

    private readonly IReadOnlyList<string> _etcdAliases;
    private readonly string _s3Alias;
    private readonly string _caPem;
    private readonly string _caKeyPem;

    /// <summary>Тег прогона для имени кластера сценария (8 hex гуида): regex
    /// имени кластера [a-z][a-z0-9_]{0,62} допускает суффикс из hex-цифр.</summary>
    public string ClusterTag => _runId[..8];

    /// <summary>PEM per-run CA контура (mTLS API + REST-TLS нод): клиентские
    /// серты сценариев для mTLS-вызовов API воркера выпускаются из неё.</summary>
    public string CaPem => _caPem;

    public string CaKeyPem => _caKeyPem;

    /// <summary>https-клиент Patroni-проб сценария: REST :8008 нод — TLS;
    /// верификация цепочки к per-run CA контура (hostname не сверяется —
    /// сценарии ходят и по внутренним именам, и по SAN нод).</summary>
    public HttpClient PatroniHttp { get; }

    /// <summary>Мутация Patroni REST сценарием (t22): unsafe-эндпоинт — basic-auth
    /// per-cluster (patroni:&lt;rest_password из etcd&gt;); отправляется клиентом
    /// PatroniHttp (https + доверие CA контура).</summary>
    public static HttpRequestMessage PatroniPatch(string url, string restPassword, string json)
    {
        var request = new HttpRequestMessage(HttpMethod.Patch, url)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"patroni:{restPassword}")));
        return request;
    }

    /// <summary>Каталог телеметрии прогона (docs/e2e-launch.md): docker-логи
    /// контейнеров (снимаются ПЕРЕД любым удалением), фазовый журнал, дампы.
    /// Фаза 1 — хост-каталог (volume /artifacts раннера смонтирован сюда:
    /// записи видны с обеих сторон); фаза 2 — /artifacts.</summary>
    public string ArtifactsDir { get; }

    /// <summary>Пометка упавшего сценария: teardown остановит контейнеры, но не
    /// удалит их/тома/сети — разбор по живым объектам, зачистка вручную
    /// командами из artifacts/README-cleanup.txt. Вызывается в catch сценария.</summary>
    public void MarkFailed() => _failed = true;

    /// <summary>etcd окружения: внутрисетевой URL первого узла (алиас сети).</summary>
    public string EtcdEndpoint { get; }

    /// <summary>Все клиентские URL контура окружения (внутрисетевые, по алиасам);
    /// при haEtcd=false — один (прежняя семантика).</summary>
    public IReadOnlyList<string> EtcdEndpoints { get; }

    /// <summary>Имя docker-сети окружения (уникально на прогон) — удаляется в
    /// teardown'е, отсутствие проверяется ассертом.</summary>
    public string NetName { get; }

    public EtcdGateway Gateway { get; }

    /// <summary>S3-endpoint окружения: внутрисетевой URL ownS3 (алиас сети).
    /// Пустая строка, если окружение поднято без ownS3.</summary>
    public string S3Endpoint { get; }

    /// <summary>Имя ПЕРВОЙ etcd-ноды окружения (pgw-ee1-{runId}) для docker cp/exec
    /// etcdctl в restore-verify сценариях (t08). Ноды именуются pgw-ee{i+1}-{runId}
    /// (t09ha: 1-3 узла) — имя pgw-ee-{runId} не существует никогда.</summary>
    public string EtcdContainerName => $"pgw-ee1-{_runId}";

    // t36: идентификаторы контейнеров нод пула (сценарии AC4/AC6: stop/start).
    public string RunId => _runId;

    /// <summary>Имя контейнера ownS3-ноды окружения (инстанс node1 пула;
    /// детерминировано guid'ом — доступно и фазе 1, и раннеру).</summary>
    public string S3NodeContainerName => $"pgw-eo-{_runId}";

    /// <summary>Имя контейнера ДОПОЛНИТЕЛЬНОЙ ownS3-ноды пула (StartExtraS3NodeAsync).</summary>
    public string ExtraS3NodeContainerName(string alias) => $"pgw-eo-{alias}-{_runId}";

    // Свой ли объект по имени (контейнер/том/сеть).
    private bool OwnName(string name)
        => name.Contains(_runId, StringComparison.Ordinal)
           || name.Contains(ClusterTag, StringComparison.Ordinal);

    // ===== Порт-план нод: per-контур окно =====

    // База окна и ширина (arch/14 §2.4 п.2): контуры процесса получают
    // непересекающиеся окна счётчиком (физический bind-резерв упразднён —
    // тестовая инфраструктура не публикует порты; фактические коллизии троек
    // с чужими публикациями аллокатор воркера обходит штатной проверкой
    // занятости). Диапазон не пересекается с дефолтами dev-стенда (AGENTS.md).
    private const int PortWindowBase = 15100;
    private const int PortWindowSize = 200;

    private static int _windowCursor;
    private readonly int _portFrom;
    private readonly int _portTo;

    /// <summary>Окно PortRange контура (публикации троек продуктовых нод):
    /// одинаковый диапазон всем воркерам контура — их аллокаторы согласованы
    /// общим etcd (portalloc ∪ busy-проверка docker, arch/14 §2.4).</summary>
    public (int From, int To) RemainingPortRange => (_portFrom, _portTo);

    // Секреты установки (Д7) — общие обоим процессам контура.
    private IReadOnlyDictionary<string, string> SecretEnv => new Dictionary<string, string>
    {
        ["PGW_PG_SUPERUSER_PASSWORD"] = E2eFixture.SuPassword,
        ["PGW_PG_STANDBY_PASSWORD"] = E2eFixture.StandbyPassword,
        ["PGW_BUCKET_ADMIN_PASSWORD"] = E2eFixture.BucketAdminPassword,
        ["PGW_BUCKET_MOVER_PASSWORD"] = E2eFixture.MoverPassword,
    };

    private E2eEnvironment(
        string slug, string runId, string netName, string artifactsDir,
        IReadOnlyList<string> etcdAliases, string s3Alias,
        string caPem, string caKeyPem, int portFrom, int portTo,
        INetwork? net, IReadOnlyList<IContainer>? etcdNodes, IReadOnlyList<string> etcdNames,
        IContainer? minio)
    {
        Slug = slug;
        _runId = runId;
        NetName = netName;
        ArtifactsDir = artifactsDir;
        _etcdAliases = etcdAliases;
        _s3Alias = s3Alias;
        _caPem = caPem;
        _caKeyPem = caKeyPem;
        _portFrom = portFrom;
        _portTo = portTo;
        _net = net;
        _etcdNodes = etcdNodes;
        _etcdNames = etcdNames;
        _minio = minio;
        Gateway = new EtcdGateway(_gatewayHttp);
        EtcdEndpoint = $"http://{etcdAliases[0]}:2379";
        EtcdEndpoints = etcdAliases.Select(a => $"http://{a}:2379").ToList();
        S3Endpoint = s3Alias.Length == 0 ? "" : $"http://{s3Alias}:9000";
        PatroniHttp = CreatePatroniHttpsClientOnly();
        HealthHttp = CreateMtlsHttpClient();
    }

    public string Slug { get; }

    /// <summary>Хост-путь каталога снапшотов воркеров (bind-источник: воркеры
    /// пишут etcd-снапшоты в /snapshots → каталог артефактов хоста; раннер
    /// маунтит его на /snaps). Только фаза 1.</summary>
    public string SnapshotsHostDir => IsInnerRunner
        ? GetRequiredEnv("PGW_E2E_HOST_SNAPS_DIR")
        : Path.Combine(ArtifactsDir, "snapshots");

    public static string HostSnapsDirStatic => GetRequiredEnv("PGW_E2E_HOST_SNAPS_DIR");

    public static string HostArtifactsDirStatic => GetRequiredEnv("PGW_E2E_HOST_ARTIFACTS");

    /// <summary>https-клиент mTLS-проб (/healthz воркера, policy-API): клиентский
    /// серт per-run CA контура, доверие той же CA. Вызывающий Dispose-ит.</summary>
    public HttpClient CreateMtlsApiClient() => CreateMtlsHttpClient();

    private HttpClient CreateMtlsHttpClient()
    {
        var (clientPem, clientKeyPem) = E2eTestPki.Issue(
            _caPem, _caKeyPem, "e2e-healthcheck", ["e2e-healthcheck"], ip: null);
        var pemPair = X509Certificate2.CreateFromPem(clientPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pemPair.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                // TLS 1.2: macOS SslStream не шлёт клиентские серты в TLS 1.3
                // (runtime#37961) — рабочий контур это уже доказал.
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    var cert = certificate as X509Certificate2
                        ?? (certificate is null ? null : new X509Certificate2(certificate));
                    if (cert is null)
                        return false;
                    var caCert = X509Certificate2.CreateFromPem(_caPem);
                    return Shared.Tls.TlsChain.ValidateChain(cert, caCert);
                },
            },
        })
        {
            Timeout = TimeSpan.FromSeconds(3),
        };
    }

    private HttpClient HealthHttp { get; }

    private HttpClient CreatePatroniHttpsClientOnly()
        => new(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    var cert = certificate as X509Certificate2
                        ?? (certificate is null ? null : new X509Certificate2(certificate));
                    if (cert is null)
                        return false;
                    var caCert = X509Certificate2.CreateFromPem(_caPem);
                    return Shared.Tls.TlsChain.ValidateChain(cert, caCert);
                },
            },
        })
        {
            Timeout = TimeSpan.FromSeconds(3),
        };

    /// <summary>Клиент Patroni-проб (совместимость: ранее статический по классу —
    /// CA стал per-run, клиент — свойство окружения).</summary>
    public HttpClient CreatePatroniHttpsClient() => PatroniHttp;

    /// <summary>НОВЫЙ клиент Patroni-проб (изоляция от общего: диагностика
    /// ObjectDisposedException общего клиента в Scale §8-3).</summary>
    public HttpClient NewPatroniHttpClient() => CreatePatroniHttpsClientOnly();

    // ===== ФАЗА 1 (хост): подъём окружения =====

    /// <summary>Точка входа сценария. Хост (не раннер): оркестрация двухфазного
    /// контура — окружение поднимается, ТЕКУЩИЙ факт прогоняется в
    /// раннер-контейнере, исход — результат раннера; делегирование всегда
    /// завершается исключением (Assert.Skip при успехе раннера — тело факта на
    /// хосте не исполняется, см. E2eContour). Раннер (PGW_E2E_INNER=1):
    /// возвращается подключение к контуру (AttachFromEnv) — тело сценария
    /// исполняется против внутрисетевых адресов.</summary>
    public static async Task<E2eEnvironment> StartAsync(
        string slug, bool withS3Node = false, bool haEtcd = false, CancellationToken ct = default)
    {
        if (!IsInnerRunner)
        {
            await E2eContour.OrchestrateCurrentTestAsync(slug, withS3Node, haEtcd, ct);
            throw new InvalidOperationException("оркестратор обязан завершиться исключением");
        }

        // Раннер, per-факт окружение: ПОЛНЫЙ контур (etcd/ownS3/mc — новые
        // guid-контейнеры) в СЕТИ КЛАССА (PGW_E2E_NET, живёт на класс);
        // teardown факта удаляет всё, кроме сети.
        return await StartOnceAsync(slug, withS3Node, haEtcd, ct, createNet: false);
    }

    /// <summary>Статические ассеты процесса (publish-выводы + образы): вызывается
    /// оркестратором на хосте; в раннере сборок нет (образ уже собран).</summary>
    public static Task EnsureStaticAssetsAsync(CancellationToken ct) => EnsureStaticAsync(ct);

    /// <summary>Периметр класса: сеть + host-пути + CA. Живёт весь класс;
    /// etcd/ownS3/mc/воркеры — per-факт (внутри раннера).</summary>
    public sealed record ClassNetwork(string NetName, string ArtifactsDir, string SnapshotsDir,
        string CaPem, string CaKeyPem);

    /// <summary>Создание периметра класса (фаза 1, хост): docker-сеть класса +
    /// host-каталоги артефактов/снапшотов + per-класс CA.</summary>
    public static async Task<ClassNetwork> CreateClassNetworkAsync(
        string slug, CancellationToken ct)
    {
        await EnsureStaticAsync(ct);
        var runId = Guid.NewGuid().ToString("N");
        var netName = $"pgw-en-{runId}";
        var net = new NetworkBuilder().WithName(netName).Build();
        await net.CreateAsync(ct);
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(artifactsDir);
        Directory.CreateDirectory(Path.Combine(artifactsDir, "snapshots"));
        var (caPem, caKeyPem) = E2eTestPki.GenerateCa("e2e");
        File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.pem"), caPem);
        File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.key"), caKeyPem);
        Console.WriteLine($"[PHASE] e2e-class {slug}: сеть класса {netName} создана (CA: rest-ca.pem в артефактах)");
        return new ClassNetwork(netName, artifactsDir, Path.Combine(artifactsDir, "snapshots"), caPem, caKeyPem);
    }

    /// <summary>Удаление сети класса (конец класса, хост).</summary>
    public static async Task DeleteClassNetworkAsync(string netName, CancellationToken ct)
        => await E2eFixture.RunProcessAsync("docker", ["network", "rm", netName], ct);

    private static async Task<E2eEnvironment> StartOnceAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct, bool createNet)
    {
        await EnsureStaticAsync(ct);

        // Ретрай старта (только хост-ветка, createNet): чужой глобальный prune —
        // только что созданная сеть без контейнеров может быть снесена до старта
        // etcd. Per-факт подъём (раннер) — сеть уже есть, ретрай не нужен.
        for (var w = 0; w < 5; w++)
        {
            try
            {
                return await StartOnceCoreAsync(slug, withS3Node, haEtcd, ct, createNet);
            }
            catch (Exception e) when (createNet && IsRetryableStartRace(e))
            {
                Console.Error.WriteLine(
                    $"e2e[{slug}]: старт сорван внешней гонкой — пересоздаём окружение (попытка {w + 1}/5): {e.Message.Split('\n')[0]}");
                await Task.Delay(2000, ct);
            }
        }

        throw new ApplicationException(
            $"e2e[{slug}]: не удалось поднять окружение за 5 попыток (перечислен бюджет ретраев старта)");
    }

    // Ретрай-условия старта: чужой prune сети (network … not found).
    private static bool IsRetryableStartRace(Exception e)
        => e.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
           && e.Message.Contains("not found", StringComparison.OrdinalIgnoreCase);

    private static async Task<E2eEnvironment> StartOnceCoreAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct, bool createNet)
    {
        // Сеть окружения: хост-ветка создаёт свою (per-класс, живёт на класс);
        // раннер-ветка (per-факт) подключается к СЕТИ КЛАССА (PGW_E2E_NET).
        var runId = Guid.NewGuid().ToString("N");
        var netName = createNet ? $"pgw-en-{runId}" : GetRequiredEnv("PGW_E2E_NET");
        // Дескриптор сети: inner — существующая сеть класса (WithNetwork по
        // имени: testcontainers 4.x подключает контейнер к существующей сети,
        // CreateAsync не вызывается при старте контейнера); host — создаёт новую.
        var net = new NetworkBuilder().WithName(netName).Build();
        var etcdNodes = new List<IContainer>();
        var etcdNames = new List<string>();
        var etcdAliases = Enumerable.Range(1, haEtcd ? 3 : 1).Select(i => $"e2e-etcd{i}").ToList();
        IContainer? minio = null;
        var artifactsDir = createNet
            ? Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}")
            : Path.Combine(GetRequiredEnv("PGW_E2E_HOST_ARTIFACTS"), runId);
        var (caPem, caKeyPem) = createNet
            ? E2eTestPki.GenerateCa("e2e")
            : (GetRequiredEnv("PGW_E2E_CA_PEM"), GetRequiredEnv("PGW_E2E_CA_KEY_PEM"));
        var windowIndex = Interlocked.Increment(ref _windowCursor) - 1;
        var portFrom = PortWindowBase + (int)Math.Min(windowIndex, 100) * PortWindowSize;
        var portTo = portFrom + PortWindowSize;

        // Дроссель параллельности: слот КОНТУРА занят ДО подъёма (per-факт
        // контур живёт в сети класса; дроссель считает классы-контуры — слот
        // захвачен оркестратором класса). Успех — Release в DisposeAsync класса;
        // провал подъёма факта — Release здесь НЕ делаем (etcd/ownS3 факта
        // стопятся teardown'ом факта; Release на DisposeAsync класса).
        try
        {
            if (createNet)
                await net.CreateAsync(ct);
            Directory.CreateDirectory(artifactsDir);
            Directory.CreateDirectory(Path.Combine(artifactsDir, "snapshots"));

            var initialCluster = string.Join(
                ",", etcdAliases.Select(a => $"{a}=http://{a}:2380"));
            for (var i = 0; i < etcdAliases.Count; i++)
            {
                var alias = etcdAliases[i];
                var name = $"pgw-ee{i + 1}-{runId}";
                etcdNames.Add(name);
                var etcdArgs = new List<string>
                {
                    "run", "-d", "--name", name,
                    "--network", netName, "--network-alias", alias,
                    EtcdImage,
                    "etcd", $"--name={alias}", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379", "--listen-peer-urls=http://0.0.0.0:2380",
                    $"--initial-advertise-peer-urls=http://{alias}:2380",
                    $"--advertise-client-urls=http://{alias}:2379",
                    $"--initial-cluster={initialCluster}", $"--initial-cluster-token=e2e-{runId}",
                    "--initial-cluster-state=new", "--heartbeat-interval=250", "--election-timeout=2000",
                };
                await E2eFixture.RunDockerAsync([.. etcdArgs], ct);
            }

            Console.WriteLine($"[PHASE] e2e-env {slug}: etcd-ноды подняты ({etcdNames.Count}), ожидание здоровья…");
            Console.WriteLine($"[PHASE] e2e-env {slug}: etcd-ноды подняты ({etcdNames.Count}), ожидание здоровья…");

            // Готовность контура (бюджет ≤ 100 c — быстрое падение фикстуры,
            // AGENTS.md): etcdctl внутри узла №1 — endpoint health по всем
            // узлам; HA — плюс избранный лидер (endpoint status, leader:true).
            var probeAttempts = 0;
            var ready = await E2eFixture.WaitForAsync(async () =>
            {
                probeAttempts++;
                try
                {
                    var endpointsArg = string.Join(",", EtcdAliases(etcdAliases));
                    var health = await E2eFixture.RunDockerAsync(
                        ["exec", etcdNames[0], "etcdctl", $"--endpoints={endpointsArg}", "endpoint", "health"], ct);
                    if (!health.Contains("is healthy", StringComparison.Ordinal))
                        return false;
                    if (etcdAliases.Count > 1)
                    {
                        var status = await E2eFixture.RunDockerAsync(
                            ["exec", etcdNames[0], "/bin/sh", "-c",
                             $"etcdctl --endpoints={endpointsArg} endpoint status | grep -c 'true'"], ct);
                        if (!status.Trim().StartsWith("1"))
                            return false; // кворум не собран / лидер не избран
                    }

                    return true;
                }
                catch (Exception)
                {
                    return false; // ещё поднимается / кворум не собран
                }
            }, TimeSpan.FromSeconds(100), ct);
            ready.Should().BeTrue($"etcd-контур ({(haEtcd ? 3 : 1)} узла) обязан собраться за 100 c "
                + $"(проб сделано {probeAttempts})");

            if (withS3Node)
            {
                // ownS3 в сети окружения (mc ходит по алиасу; джобы/воркер — по
                // внутрисетевому endpoint). РАННЕР: docker CLI.
                minio = null;
                await E2eFixture.RunDockerAsync(
                [
                    "run", "-d", "--name", $"pgw-eo-{runId}",
                    "--network", netName, "--network-alias", "e2e-owns3",
                    "-e", $"OWNS3_ROOT_USER={S3RootUser}",
                    "-e", $"OWNS3_ROOT_PASSWORD={S3RootPassword}",
                    // Ключ 0 — read-write для подсистемы бэкапов (профиль воркера).
                    "-e", $"OWNS3_ACCESS_KEYS__0__ACCESSKEY={S3WriterKey}",
                    "-e", $"OWNS3_ACCESS_KEYS__0__SECRETKEY={S3WriterSecret}",
                    "-e", "OWNS3_ACCESS_KEYS__0__POLICY=read-write",
                    OwnS3Image,
                ], ct);
                await EnsureOwnS3ImageAsync(ct);

                // mc — ИНСТРУМЕНТАЛЬНЫЙ контейнер ФАКТА (pgw-em-{runId}): docker
                // exec для всех mc-вызовов; удаляется teardown'ом факта.
                await E2eFixture.RunProcessAsync("docker",
                [
                    "run", "-d", "--name", $"pgw-em-{runId}",
                    "--network", netName,
                    "--entrypoint", "/bin/sh", McImage, "-c", "sleep infinity",
                ], ct, timeout: TimeSpan.FromMinutes(5));

                Console.WriteLine($"[PHASE] e2e-env {slug}: ownS3-нода поднята, mc-контейнер факта, бакет/образы…");
                await EnsureOwnS3BucketAsync(runId, "e2e-owns3", ct);
            }

            var env = new E2eEnvironment(slug, runId, netName, artifactsDir, etcdAliases,
                withS3Node ? "e2e-owns3" : "", caPem, caKeyPem, portFrom, portTo,
                net, etcdNodes, etcdNames, minio);
            // REST-TLS CA контура: артефакт для разбора падений и ручных
            // curl-проверок контура (+ отпечаток — сверка с env воркера).
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.pem"), caPem);
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.sha256"),
                Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
                    Encoding.UTF8.GetBytes(caPem))));
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.key"), caKeyPem);

            Console.WriteLine(
                $"[PHASE] e2e-env {slug}: окружение поднято (сеть {netName}, окно {portFrom}–{portTo})");
            return env;
        }
        catch
        {

            // частично поднятое окружение не оставляем (лучшими усилиями):
            // контейнер, чей StartAsync упал, чистит сам testcontainers; живые
            // узлы etcd/ownS3 — здесь; сеть — здесь.
            if (minio is not null)
                try
                {
                    await minio.DisposeAsync();
                }
                catch
                {
                    // guid-имя, чужие прогоны не заденем; добьёт ассерт/ryuk
                }

            foreach (var node in etcdNodes)
                try
                {
                    await node.DisposeAsync();
                }
                catch
                {
                    // аналогично
                }

            if (net is not null)
                try
                {
                    await net.DeleteAsync();
                }
                catch
                {
                    // аналогично
                }

            throw;
        }
    }

    private static string GetRequiredEnv(string name)
        => Environment.GetEnvironmentVariable(name)
           ?? throw new ApplicationException($"{name} не задан — запуск вне контура (env раннера отсутствует)");

    // Список etcd-endpoints для etcdctl (внутрисетевые URL узлов).
    private static string EtcdAliases(IReadOnlyList<string> aliases)
        => string.Join(",", aliases.Select(a => $"http://{a}:2379"));

    // Бакет ownS3 (mc-контейнер КОНТУРА: docker exec — переиспользуемый
    // pgw-em-{runId}, канон «инструментальные контейнеры — per-контур»).
    // Retry-цикл внутри разового exec: extra-ноды стартуют асинхронно —
    // бакет ждёт готовности S3 (до 60 с), не падая на первом refused.
    public static Task EnsureOwnS3BucketAsync(string runId, string alias, CancellationToken ct)
        => E2eFixture.RunProcessAsync("docker",
        [
            "exec", $"pgw-em-{runId}", "/bin/sh", "-c",
            $"for i in $(seq 1 30); do mc alias set t http://{alias}:9000 {S3RootUser} {S3RootPassword} >/dev/null 2>&1"
            + $" && mc mb --ignore-existing t/{BucketName} 2>/dev/null && exit 0; sleep 2; done; exit 1",
        ], ct, timeout: TimeSpan.FromMinutes(3));

    // ===== ФАЗА 2 (раннер): подключение к контуру =====

    /// <summary>Подключение к окружению из раннер-контейнера: параметры контура
    /// переданы env (фаза 1 → фаза 2). Контейнеры окружения из раннера не
    /// управляются testcontainers (только docker CLI — stop/start/exec).</summary>
    public static E2eEnvironment AttachFromEnv()
    {
        var runId = Environment.GetEnvironmentVariable("PGW_E2E_RUN_ID")
            ?? throw new ApplicationException("PGW_E2E_RUN_ID не задан — раннер вне контура (фазы 1 нет)");
        var netName = Environment.GetEnvironmentVariable("PGW_E2E_NET")
            ?? throw new ApplicationException("PGW_E2E_NET не задан");
        var slug = Environment.GetEnvironmentVariable("PGW_E2E_SLUG") ?? "e2e";
        var aliases = (Environment.GetEnvironmentVariable("PGW_E2E_ETCD_ALIASES") ?? "e2e-etcd1")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var s3Alias = Environment.GetEnvironmentVariable("PGW_E2E_S3_ALIAS") ?? "";
        var caPem = Environment.GetEnvironmentVariable("PGW_E2E_CA_PEM")
            ?? throw new ApplicationException("PGW_E2E_CA_PEM не задан");
        var caKeyPem = Environment.GetEnvironmentVariable("PGW_E2E_CA_KEY_PEM")
            ?? throw new ApplicationException("PGW_E2E_CA_KEY_PEM не задан");
        var portFrom = int.Parse(
            Environment.GetEnvironmentVariable("PGW_E2E_PORT_FROM")
            ?? throw new ApplicationException("PGW_E2E_PORT_FROM не задан"), CultureInfo.InvariantCulture);
        var portTo = int.Parse(
            Environment.GetEnvironmentVariable("PGW_E2E_PORT_TO")
            ?? throw new ApplicationException("PGW_E2E_PORT_TO не задан"), CultureInfo.InvariantCulture);
        var artifactsDir = Environment.GetEnvironmentVariable("PGW_E2E_ARTIFACTS") ?? "/artifacts";

        return new E2eEnvironment(slug, runId, netName, artifactsDir, aliases, s3Alias,
            caPem, caKeyPem, portFrom, portTo, net: null, etcdNodes: null, etcdNames: [], minio: null);
    }

    /// <summary>env-комплект раннера для этого окружения (фаза 1 → фаза 2).
    /// Только фаза 1.</summary>
    internal IReadOnlyDictionary<string, string> RunnerEnv()
    {
        throw new NotSupportedException("RunnerEnv переехал в BuildRunnerEnv (per-класс периметр)");
    }

    internal static IReadOnlyDictionary<string, string> BuildRunnerEnv(
        string netName, string hostArtifacts, string hostSnaps, string caPem, string caKeyPem)
    {
        // Раннер получает ТОЛЬКО периметр класса: сеть, CA (per-факт окружение
        // выпускает серты из него) и host-пути bind-источников (bind-пути
        // резолвит демон хоста, а не раннер). etcd/ownS3/mc — per-факт внутри.
        var env = new Dictionary<string, string>
        {
            ["PGW_TEST_DOCKER"] = "1",
            // Inner-режим раннера: StartAsync факта поднимает per-факт контур.
            ["PGW_E2E_INNER"] = "1",
            ["PGW_E2E_NET"] = netName,
            ["PGW_E2E_CA_PEM"] = caPem,
            ["PGW_E2E_CA_KEY_PEM"] = caKeyPem,
            ["PGW_E2E_HOST_ARTIFACTS"] = hostArtifacts,
            ["PGW_E2E_HOST_SNAPS_DIR"] = hostSnaps,
        };
        return env;
    }

    // ===== SUT-воркер — контейнер в сети контура (фаза 2) =====

    /// <summary>
    /// Старт инстанса PgWorker.App — КОНТЕЙНЕРА pgworker:e2e в сети контура
    /// (docs/e2e-isolation.md §3): docker.sock смонтирован (воркер провижинит
    /// ноды в контур-сеть), env — внутрисетевые адреса, mTLS-серт с SAN=алиас
    /// (выпускается из per-run CA). Смотрит ТОЛЬКО в etcd этого окружения и
    /// обслуживает только его Active-кластеры. extraEnv — дополнительные
    /// env-пары поверх базовых (t02: Backups-комплект).
    /// </summary>
    public async Task<HostInstance> StartHostAsync(
        string name, int snapshotIntervalMin = 360,
        IReadOnlyDictionary<string, string>? extraEnv = null, CancellationToken ct = default)
    {
        var alias = $"pgw-ew-{name}";
        var containerName = $"pgw-ew-{name}-{_runId}";

        // mTLS-сервер воркера: SAN = DNS-алиас в сети контура (перекрывать
        // воркеру PGW_API_TLS_CLIENT_CA нельзя — пробы фикстуры перестанут
        // проходить хендшейк).
        var (serverCertPem, serverKeyPem) = E2eTestPki.Issue(
            _caPem, _caKeyPem, "pgworker", [alias], ip: null);

        var env = new Dictionary<string, string>(SecretEnv)
        {
            // Конфигурация (env-оверрайды appsettings): контур-режим (arch/14
            // §2.1) — ноды/джобы/агенты в сети контура, pgw-net-<C> не создаётся.
            ["PgWorker__Docker__Mode"] = "Plain",
            // Имя docker-хоста = advertised-имя для записей etcd (portalloc,
            // moves-подписки): Docker Desktop резолвит host.docker.internal
            // из любого контейнера.
            ["PgWorker__Docker__Hosts__0__Name"] = "host.docker.internal",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            ["PgWorker__Docker__ContourNetwork"] = NetName,
            ["PgWorker__Docker__PortRange__From"] = _portFrom.ToString(CultureInfo.InvariantCulture),
            ["PgWorker__Docker__PortRange__To"] = _portTo.ToString(CultureInfo.InvariantCulture),
            ["PgWorker__Docker__Images__Node"] = NodeImage,
            ["PgWorker__Docker__EnableDoorman"] = "false",

            // Ускоренные циклы/пороги для e2e (критерии ждут секунды, не минуты).
            ["PgWorker__Loops__ScanIntervalSec"] = "1",
            ["PgWorker__Loops__KeepaliveSec"] = "1",
            ["PgWorker__Loops__ErrorDelayMs"] = "500",
            ["PgWorker__Loops__SnapshotIntervalMin"] = snapshotIntervalMin.ToString(CultureInfo.InvariantCulture),
            ["PgWorker__Thresholds__NodeDeadSec"] = "6",
            ["PgWorker__Thresholds__ShardDeadSec"] = "5",
            ["PgWorker__Thresholds__PatroniBootSec"] = "300",

            // Переезды (t01): spilo-18 → FailoverSlots=true (штатный путь PG17+,
            // R1/Д11); AdvertisedPublisherHost: подписки ходят ИЗ контейнеров
            // приёмников — издатель виден по host-публикации тройки (канон).
            ["PgWorker__Moves__FailoverSlots"] = "true",
            ["PgWorker__Moves__FreezeWaitSec"] = "1",
            ["PgWorker__Moves__PollIntervalSec"] = "1",
            ["PgWorker__Moves__AbortMinAgeSec"] = "3",
            ["PgWorker__Moves__AdvertisedPublisherHost"] = "host.docker.internal",
            ["PgWorker__Thresholds__CutoverTimeoutSec"] = "60",
            ["PgWorker__Thresholds__ConnFailBudgetSec"] = "15",
            ["PgWorker__Parallelism__MaxClusters"] = "2",
            // Снапшоты воркера: /snapshots в контейнере (bind на каталог
            // артефактов хоста; в раннере — /snaps/<name>).
            ["PgWorker__Snapshots__Dir"] = "/snapshots",
            ["PgWorker__Snapshots__RetentionFiles"] = "10",

            // mTLS HTTP API (t03, arch/14 §1.1): внутрисетевой порт, SAN=алиас.
            ["PGW_API_TLS_CERT"] = serverCertPem,
            ["PGW_API_TLS_KEY"] = serverKeyPem,
            ["PGW_API_TLS_CLIENT_CA"] = _caPem,
            // REST-TLS нод (t22): per-run CA контура — все E2E-контуры создают
            // ноды в TLS-режиме (дефолт поставки, http-режима в тестах нет).
            ["PGW_REST_TLS_CA"] = _caPem,
            ["PGW_REST_TLS_CA_KEY"] = _caKeyPem,
            ["PgWorker__Api__Tls__AllowInsecureHttp"] = "false",
            ["PgWorker__Api__AdvertiseUrl"] = $"https://{alias}:8443",
            ["ASPNETCORE_URLS"] = $"https://0.0.0.0:8443",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        // HA-контур (t09, arch/04 §8): воркер получает СПИСОК всех узлов
        // (failover перебирает); Advertised — те же внутрисетевые URL
        // (Patroni-ноды и master-lease живут в той же сети контура).
        for (var i = 0; i < EtcdEndpoints.Count; i++)
        {
            env[$"PgWorker__Etcd__Endpoints__{i}"] = EtcdEndpoints[i];
            env[$"PgWorker__Etcd__AdvertisedEndpoints__{i}"] = EtcdEndpoints[i];
        }

        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            env[key] = value;

        // Каталог снапшотов этого воркера на хосте (bind-источник); раннер
        // видит его как /snaps/<name>.
        var hostSnapsDir = Environment.GetEnvironmentVariable("PGW_E2E_HOST_SNAPS_DIR");
        if (hostSnapsDir is null)
            throw new ApplicationException(
                "PGW_E2E_HOST_SNAPS_DIR не задан — старт воркера вне раннер-контейнера");
        Directory.CreateDirectory(Path.Combine("/snaps", name));

        return await StartWorkerWithEnvAsync(name, alias, containerName, env, ct);
    }

    // docker run воркера: env парами -e K=V (обход ограничения списка аргументов).
    private async Task<HostInstance> StartWorkerWithEnvAsync(
        string name, string alias, string containerName,
        IReadOnlyDictionary<string, string> env, CancellationToken ct)
    {
        var hostSnapsDir = Environment.GetEnvironmentVariable("PGW_E2E_HOST_SNAPS_DIR")!;
        var args = new List<string>
        {
            "run", "-d", "--name", containerName,
            "--network", NetName,
            "--network-alias", alias,
            "--restart", "no",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-v", $"{hostSnapsDir}/{name}:/snapshots",
        };
        foreach (var (key, value) in env)
            args.AddRange(["-e", $"{key}={value}"]);
        args.Add(WorkerImage);

        await E2eFixture.RunDockerAsync([.. args], ct);
        var instance = new HostInstance(this, name, alias, containerName);
        _hosts.Add(instance);

        // Готовность: mTLS /healthz по внутрисетевому имени (SAN=алиас —
        // хендшейк честный). При неуспехе — хвост логов в исключении.
        var ready = await E2eFixture.WaitForAsync(async () =>
        {
            if (!await instance.IsRunningAsync(ct))
                return false;
            try
            {
                using var response = await HealthHttp.GetAsync(
                    $"{instance.ApiUrl}/healthz", CancellationToken.None);
                return response.StatusCode != System.Net.HttpStatusCode.NotFound;
            }
            catch (HttpRequestException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                // 3-с таймаут клиента: mTLS-хендшейк при буте под нагрузкой —
                // проба повторится в следующем такте WaitForAsync.
                return false;
            }
        }, TimeSpan.FromSeconds(60), ct);
        if (!ready)
        {
            // Телеметрия ДО любых действий: логи воркера — в артефакты И хвост
            // в исключение; воркер ОСТАЁТСЯ (stop-семантика failed-прогона:
            // rm здесь потерял бы логи «что произошло»).
            string logs;
            try
            {
                logs = await E2eFixture.RunProcessAsync("docker", ["logs", containerName],
                    CancellationToken.None, TimeSpan.FromSeconds(30));
                await File.WriteAllTextAsync(
                    Path.Combine(ArtifactsDir, $"container-{containerName}.log"), logs);
            }
            catch (Exception e)
            {
                logs = $"<logs unavailable: {e.Message}>";
            }

            try
            {
                await E2eFixture.RunDockerAsync(["stop", containerName], ct);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"e2e[{Slug}]: stop воркера {name}: {e.Message}");
            }

            throw new ApplicationException(
                $"воркер {name} не поднялся за 60 с; хвост логов:\n{OutputTail(logs, 60)}");
        }

        return instance;
    }

    // Последние строки лога — для диагностики в исключении.
    private static string OutputTail(string text, int lines)
    {
        var all = text.Split(
            ['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join("\n", all.TakeLast(lines));
    }

    /// <summary>
    /// Пароль app-роли кластера из etcd (spec §3.1): e2e-сценарии читают секрет
    /// тем же путём, что и приложение — /clusters/&lt;C&gt;/app_password.
    /// </summary>
    public async Task<string> GetAppPasswordAsync(string cluster, CancellationToken ct = default)
    {
        var result = await Gateway.GetAsync(EtcdEndpoint, $"/clusters/{cluster}/app_password", ct);
        result.IsSuccess.Should().BeTrue("app-секрет обязан появиться после provisioning");
        return result.Value!.Value;
    }

    public Task<string> RunDockerAsync(string[] args, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", args, ct);

    /// <summary>Стоп/старт узла контура по индексу (0-based) — сценарии отказа;
    /// docker stop/start по имени: контейнер и data-dir переживают, член
    /// возвращается в кворум.</summary>
    public Task StopEtcdNodeAsync(int index, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", ["stop", _etcdNames[index]], ct);

    public Task StartEtcdNodeAsync(int index, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker", ["start", _etcdNames[index]], ct);

    /// <summary>etcdctl в узле №1 (сценарии: member list / endpoint health).</summary>
    public Task<string> EtcdctlAsync(params string[] args)
        => E2eFixture.RunProcessAsync("docker",
            ["exec", _etcdNames[0], "etcdctl", "--endpoints=http://localhost:2379", .. args]);

    /// <summary>Проба /healthz воркера (живость API на интервале отказа узла):
    /// true = маршрут отвечает (не 404), транспорт досягаем.</summary>
    public async Task<bool> HealthzOkAsync(HostInstance host)
    {
        try
        {
            using var response = await HealthHttp.GetAsync($"{host.ApiUrl}/healthz", CancellationToken.None);
            return response.StatusCode != System.Net.HttpStatusCode.NotFound;
        }
        catch (HttpRequestException)
        {
            return false;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    // ===== t36: пул S3 (инстансы пула — ownS3-ноды окружения) =====

    /// <summary>Сид S3-пула (t36, AC10): /s3/clusters/&lt;pool&gt;/{config,
    /// nodes/&lt;I&gt;/{state,placement}}, /s3worker/portalloc/&lt;pool&gt;,
    /// endpoints — прямо в etcd окружения (PgWorker — только читатель /s3/).
    /// Инстанс пула — ownS3-контейнер; имена нод node1..N; secret_profile —
    /// PoolProfile. endpoints: null → одна нода (ownS3-ноды окружения); иначе —
    /// список "host:port" (внутрисетевые адреса: вторые ownS3 сценариев AC2/AC4/AC6).</summary>
    public async Task SeedS3PoolAsync(string pool, CancellationToken ct, IReadOnlyList<string>? endpoints = null)
    {
        S3Endpoint.Should().NotBeEmpty("пул сидается только в окружении с ownS3 (withS3Node: true)");
        var list = (endpoints is { Count: > 0 } ? endpoints : [S3Endpoint])
            .Select(e => e.Replace("http://", "", StringComparison.Ordinal).TrimEnd('/'))
            .ToArray();
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/config",
            $"{{\"platform\":\"owns3\",\"nodes\":{list.Length},\"secret_profile\":\"{PoolProfile}\",\"created_unix\":{now}}}",
            null, ct);
        var alloc = new List<string>(list.Length);
        for (var i = 0; i < list.Length; i++)
        {
            var node = $"node{i + 1}";
            await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/nodes/{node}/state", "RUNNING", null, ct);
            await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/nodes/{node}/placement",
                "{\"docker_host\":\"local\"}", null, ct);
            var sep = list[i].LastIndexOf(':');
            sep.Should().BePositive($"endpoint '{list[i]}' обязан быть host:port (внутрисетевой адрес ownS3)");
            alloc.Add($"\"{node}\":{{\"host\":\"{list[i][..sep]}\",\"s3\":{list[i][(sep + 1)..]}}}");
        }

        await Gateway.PutAsync(EtcdEndpoint, $"/s3worker/portalloc/{pool}",
            "{" + string.Join(",", alloc) + "}", null, ct);
        await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/endpoints",
            string.Join(",", list), null, ct);
    }

    /// <summary>env-комплект пула для воркера сценария: env-профиль кредов ownS3
    /// окружения (t36, arch/19 §7 — store читает сырой env, не конфиг). Bucket
    /// per-install сценарий задаёт ключом PgWorker__Backups__S3__Bucket.</summary>
    public IReadOnlyDictionary<string, string> BackupPoolEnv()
        => new Dictionary<string, string>
        {
            [$"PGW_BACKUP_S3_PROFILES__{PoolProfile}__ACCESS_KEY"] = S3WriterKey,
            [$"PGW_BACKUP_S3_PROFILES__{PoolProfile}__SECRET_KEY"] = S3WriterSecret,
        };

    /// <summary>Секция s3 policy однонодового пула (node1 в обоих списках записи;
    /// формат spec §5 / arch/19 §4). Многонодовые списки сценарии пишут сами.</summary>
    public static string PoolS3(string pool)
        => $"\"s3\":{{\"pool\":\"{pool}\",\"write_master\":[\"node1\"],\"write_replica\":[\"node1\"]}}";

    /// <summary>Дополнительная нода ownS3-пула (сценарии AC2/AC4/AC6): контейнер
    /// в сети окружения БЕЗ публикации (docker CLI: testcontainers-объектов в
    /// раннере нет), алиас внутрисетевой; bucket per-install на ноде; teardown
    /// чистит own-only (имя содержит guid прогона). Возврат — внутрисетевой
    /// endpoint http://{alias}:9000.</summary>
    public async Task<string> StartExtraS3NodeAsync(string alias, CancellationToken ct)
    {
        var containerName = $"pgw-eo-{alias}-{_runId}";
        await E2eFixture.RunDockerAsync(
        [
            "run", "-d", "--name", containerName,
            "--network", NetName,
            "--network-alias", alias,
            "-e", $"OWNS3_ROOT_USER={S3RootUser}",
            "-e", $"OWNS3_ROOT_PASSWORD={S3RootPassword}",
            "-e", $"OWNS3_ACCESS_KEYS__0__ACCESSKEY={S3WriterKey}",
            "-e", $"OWNS3_ACCESS_KEYS__0__SECRETKEY={S3WriterSecret}",
            "-e", "OWNS3_ACCESS_KEYS__0__POLICY=read-write",
            OwnS3Image,
        ], ct);

        await EnsureOwnS3BucketAsync(_runId, alias, ct);
        return $"http://{alias}:9000";
    }

    /// <summary>mc-команда (сценарии пула: ls/mirror) — docker exec в
    /// инструментальный mc-контейнер КОНТУРА (pgw-em-{runId}): скрипт сам
    /// ставит alias "t" и ведёт им; per-вызовные docker run копили безымянные
    /// контейнеры вне own-only чистки (утечка, исправлено).</summary>
    public Task<string> RunMcAsync(string script, CancellationToken ct = default)
        => RunMcAsync(script, TimeSpan.FromMinutes(2), ct);

    /// <summary>Сценарный mc с явным бюджетом: mirror полного бакета под
    /// параллельной нагрузкой превышает дефолтные 2 минуты (AC6).</summary>
    public Task<string> RunMcAsync(string script, TimeSpan timeout, CancellationToken ct = default)
        => E2eFixture.RunProcessAsync("docker",
        [
            "exec", $"pgw-em-{_runId}", "/bin/sh", "-c", script,
        ], ct, timeout);

    // ===== Teardown (фаза 1, хост) =====

    /// <summary>
    /// Полный teardown при ЛЮБОМ исходе. Политика телеметрии (docs/e2e-launch.md):
    /// 0) ДО любых удалений — docker logs+inspect всех своих контейнеров
    /// (etcd, ownS3, воркеры, раннер, движковые pgw-*) в ArtifactsDir.
    /// 1) Упавший сценарий: контейнеры ОСТАНАВЛИВАЮТСЯ, тома/сети остаются до
    /// ручной зачистки (README-cleanup.txt). 2) Успешный: rm контейнеров →
    /// rm томов → rm сети окружения → страховка own-only per-cluster сетей →
    /// 3) АССЕРТ ЧИСТОТЫ: не осталось ни одного объекта своего идентификатора.
    /// Чужие pgw-* не трогаются.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // TEARDOWN ФАКТА (per-факт контур в сети класса, docs/e2e-isolation.md):
        // воркеры → телеметрия ДО удалений → MarkFailed: stop-режим (контейнеры
        // стоят для разбора, README-cleanup) → успех: rm своих контейнеров
        // (etcd/ownS3/mc/воркеры/ноды/джобы — всё с факт-guid) и томов →
        // чистка ключей etcd факта → ассерт чистоты факта. СЕТЬ КЛАССА и её
        // периметр — не наши (владелец — оркестратор класса).
        var problems = new List<string>();

        // 0) Телеметрия прежде удалений — снимается при ЛЮБОМ исходе: полные
        // логи обязаны быть и на зелёном прогоне (отчёт по фазам > 60 с).
        await CollectDiagnosticsAsync(_failed ? "failed" : "teardown");

        // 1) Воркеры факта (идемпотентно).
        foreach (var host in _hosts)
            try
            {
                await host.DisposeAsync();
            }
            catch (Exception e)
            {
                problems.Add($"воркер {host.Name}: {e.Message}");
            }

        // 2) Упавший сценарий: стоп, не удаление (данные для отчёта живы).
        if (_failed)
        {
            await FailedTearDownAsyncInner(problems);
            await WriteCleanupReadmeAsync();
            return;
        }

        // 3) ownS3/mc факта и etcd-контур (docker rm по факт-guid; ключи etcd
        // умирают вместе с контейнером — данных в volume нет).
        await RemoveOwnDockerAsync("pgw-eo-", problems);
        await RemoveOwnDockerAsync("pgw-em-", problems);
        await RemoveOwnDockerAsync("pgw-ee", problems);

        _gatewayHttp.Dispose();
        HealthHttp.Dispose();
        PatroniHttp.Dispose();

        // 4) Контейнеры факта: pgw-ew-*/pgw-er-*/движковые pgw-* с факт-guid.
        var containers = await OwnContainersAsync();
        var rmErrors = await Task.WhenAll(containers.Select(async id =>
        {
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["rm", "-f", id]);
                return (string?)null;
            }
            catch (ApplicationException e) when (e.Message.Contains("already in progress", StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine($"e2e[{Slug}]: контейнер {id} уже удаляется демоном — пропускаем");
                return (string?)null;
            }
            catch (Exception e)
            {
                return $"rm контейнера {id}: {e.Message}";
            }
        }));
        problems.AddRange(rmErrors.Where(e => e is not null).Select(e => e!));

        // 5) Тома факта (pgw-<C>-*-data и пр.): docker rm возвращает управление
        // до освобождения ссылки демоном — ретраи, бюджет ≤ 30 с.
        foreach (var volume in await OwnVolumesAsync())
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await E2eFixture.RunProcessAsync("docker", ["volume", "rm", "-f", volume]);
                    break;
                }
                catch (ApplicationException) when (attempt < 14)
                {
                    await Task.Delay(2000, TestContext.Current.CancellationToken);
                }
            }
        }

        // 6) Per-cluster сети СВОИХ кластеров — страховка own-only (контур-
        // режим их не создаёт; останутся только от не-контурных артефактов).
        foreach (var net in (await E2eFixture.RunProcessAsync(
                     "docker", ["network", "ls", "--format", "{{.Name}}"]))
                 .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                 .Where(OwnName))
        {
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["network", "rm", net]);
            }
            catch (Exception e)
            {
                problems.Add($"сеть кластера {net}: {e.Message}");
            }
        }

        // 7) АССЕРТ ЧИСТОТЫ факта: ни одного контейнера/тома/сети факта.
        var leftContainers = await OwnContainersAsync();
        if (leftContainers.Count > 0)
            problems.Add($"остались контейнеры факта: {string.Join(' ', leftContainers)}");
        var leftVolumes = await OwnVolumesAsync();
        if (leftVolumes.Count > 0)
            problems.Add($"остались тома факта: {string.Join(' ', leftVolumes)}");

        if (problems.Count > 0)
            throw new ApplicationException(
                $"{Slug}: teardown факта неполный:\n- " + string.Join("\n- ", problems));
    }

    /// <summary>docker rm -f СВОИХ контейнеров по префиксу имени (own-only:
    /// префикс + факт-guid).</summary>
    private async Task RemoveOwnDockerAsync(string prefix, List<string> problems)
    {
        foreach (var id in await OwnContainersAsync(prefix))
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["rm", "-f", id]);
            }
            catch (ApplicationException e) when (e.Message.Contains("already in progress", StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine($"e2e[{Slug}]: контейнер {id} уже удаляется демоном — пропускаем");
            }
            catch (Exception e)
            {
                problems.Add($"rm контейнера {id}: {e.Message}");
            }
    }

    // Контейнеры СВОИ по префиксу (pgw-ee/pgw-eo/pgw-em с факт-guid).
    private async Task<List<string>> OwnContainersAsync(string prefix)
        => (await OwnContainersCoreAsync(name => name.StartsWith(prefix, StringComparison.Ordinal)));

    // Контейнеры СВОЕГО окружения (id по имени с идентификатором прогона).
    private async Task<List<string>> OwnContainersAsync()
        => await OwnContainersCoreAsync(_ => OwnName(_));

    private async Task<List<string>> OwnContainersCoreAsync(Func<string, bool> nameFilter)
        => (await E2eFixture.RunProcessAsync("docker", ["ps", "-a", "--format", "{{.ID}} {{.Names}}"]))
            .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(row =>
            {
                var name = row.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                return nameFilter(name) && OwnName(name);
            })
            .Select(row => row.Split(' ', 2)[0])
            .ToList();

    // Тома СВОЕГО факта.
    private async Task<List<string>> OwnVolumesAsync()
        => (await E2eFixture.RunProcessAsync("docker", ["volume", "ls", "--format", "{{.Name}}"]))
            .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(OwnName)
            .ToList();

    /// <summary>Остановить ВСЕ контейнеры СВОИ (failed-режим: логи/объекты
    /// остаются для разбора; README-cleanup.txt — зачистка вручную).</summary>
    private async Task FailedTearDownAsyncInner(List<string> problems)
    {
        foreach (var id in await OwnContainersAsync())
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["stop", id]);
            }
            catch (Exception e)
            {
                problems.Add($"stop контейнера {id}: {e.Message}");
            }

        await WriteCleanupReadmeAsync();
    }

    /// <summary>Команды зачистки упавшего факта (README-cleanup.txt в артефактах:
    /// own-only по идентификатору факта, docker rm/volume/network).</summary>
    private async Task WriteCleanupReadmeAsync()
    {
        var cleanup = """
            Упавший E2E-сценарий — окружение ФАКТА ОСТАНОВЛЕНО, но НЕ удалено (разбор по месту).
            Логи всех контейнеров сняты в этот каталог (container-*.log, runner.log).
            Перезапуск теста ради логов запрещён (docs/e2e-launch.md).
            Зачистить, когда данные для отчёта больше не нужны:
              docker rm -f $(docker ps -aq --filter name=__RUN__)
              docker rm -f $(docker ps -aq --filter name=__TAG__)
              docker volume rm -f $(docker volume ls -q --filter name=__TAG__)
            """;
        await File.WriteAllTextAsync(
            Path.Combine(ArtifactsDir, "README-cleanup.txt"),
            cleanup.Replace("__RUN__", _runId).Replace("__TAG__", ClusterTag));
    }

    // ===== Телеметрия (docs/e2e-launch.md) =====

    /// <summary>Сбор состояния окружения в ArtifactsDir: docker logs+inspect
    /// каждого СВОЕГО контейнера (etcd, ownS3, воркеры, раннер, движковые
    /// pgw-*). Вызывается при каждом teardown'е (логи живут дольше
    /// контейнеров) и на медленных фазах &gt; 60 с. Ошибки сбора не роняют
    /// teardown — логи «лучшими усилиями». Только фаза 1 (хост).</summary>
    public async Task CollectDiagnosticsAsync(string reason)
    {
        try
        {
            Directory.CreateDirectory(ArtifactsDir);
            var stamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
            await File.WriteAllTextAsync(
                Path.Combine(ArtifactsDir, $"{stamp}-{reason}.txt"),
                $"reason={reason}\nslug={Slug}\nrun={_runId}\nutc={DateTime.UtcNow:O}\n"
                + $"failed={_failed}\n");

            // Логи/inspect — ПАРАЛЛЕЛЬНО (teardown-бюджет: 10-20 контейнеров ×
            // последовательные docker logs = минуты; WhenAll — секунды).
            var own = (await E2eFixture.RunProcessAsync(
                    "docker", ["ps", "-a", "--format", "{{.ID}}\t{{.Names}}"]))
                .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(row => row.Split('\t', 2))
                .Where(parts => parts.Length == 2 && OwnName(parts[1]))
                .Select(parts => (Id: parts[0], Name: parts[1]))
                .ToList();
            await Task.WhenAll(own.Select(async item =>
            {
                var (id, name) = item;
                try
                {
                    var logs = await E2eFixture.RunProcessAsync("docker", ["logs", "--timestamps", id],
                        CancellationToken.None, TimeSpan.FromSeconds(60));
                    await File.WriteAllTextAsync(Path.Combine(ArtifactsDir, $"container-{name}.log"), logs);
                }
                catch (Exception e)
                {
                    await File.WriteAllTextAsync(
                        Path.Combine(ArtifactsDir, $"container-{name}.log"),
                        $"logs unavailable: {e.Message}\n");
                }

                try
                {
                    var inspect = await E2eFixture.RunProcessAsync("docker", ["inspect", id],
                        CancellationToken.None, TimeSpan.FromSeconds(60));
                    await File.WriteAllTextAsync(Path.Combine(ArtifactsDir, $"container-{name}.json"), inspect);
                }
                catch
                {
                    // inspect вторичен к логам — отсутствие не критично
                }
            }));

            Console.Error.WriteLine($"e2e[{Slug}]: телеметрия «{reason}» → {ArtifactsDir}");
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"e2e[{Slug}]: сбор телеметрии «{reason}» не удался: {e.Message}");
        }
    }

    // Teardown упавшего сценария: контейнеры (вкл. etcd/ownS3/воркеров/раннер)
    // останавливаются, тома/сети остаются — разбор по живым объектам и снятым
    // логам; зачистка вручную по README-cleanup.txt (own-only, по
    // идентификатору прогона).
    private async Task FailedTearDownAsync(List<string> problems)
    {
        foreach (var id in await OwnContainersAsync())
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["stop", id]);
            }
            catch (Exception e)
            {
                problems.Add($"stop контейнера {id}: {e.Message}");
            }

        foreach (var container in (_etcdNodes ?? []).Concat<IContainer?>([_minio]))
            try
            {
                if (container is not null)
                    await container.StopAsync();
            }
            catch (Exception e)
            {
                problems.Add($"stop инфраструктуры: {e.Message}");
            }

        _gatewayHttp.Dispose();
        HealthHttp.Dispose();
        PatroniHttp.Dispose();

        var cleanup = """
            Упавший E2E-сценарий — окружение ОСТАНОВЛЕНО, но НЕ удалено (разбор по месту).
            Логи всех контейнеров сняты в этот каталог (container-*.log, runner.log).
            Перезапуск теста ради логов запрещён (docs/e2e-launch.md).
            Зачистить, когда данные для отчёта больше не нужны:
              docker rm -f $(docker ps -aq --filter name=__RUN__)
              docker rm -f $(docker ps -aq --filter name=__TAG__)
              docker volume rm -f $(docker volume ls -q --filter name=__TAG__)
              docker network rm __NET__
              docker network rm $(docker network ls -q --filter name=__TAG__)
            """;
        await File.WriteAllTextAsync(
            Path.Combine(ArtifactsDir, "README-cleanup.txt"),
            cleanup.Replace("__RUN__", _runId).Replace("__TAG__", ClusterTag).Replace("__NET__", NetName));

        if (problems.Count > 0)
            throw new ApplicationException(
                $"{Slug}: stop-teardown упавшего сценария неполный:\n- " + string.Join("\n- ", problems));

        Console.Error.WriteLine(
            $"e2e[{Slug}]: FAILED — окружение остановлено и оставлено для разбора: {ArtifactsDir}");
    }

    // ===== Статические ассеты процесса: publish-выводы, образы =====

    private static readonly SemaphoreSlim StaticGate = new(1, 1);

    private static bool _staticReady;
    private static string _root = "";
    private static bool _owns3ImageReady;
    private static bool _jobImageReady;
    private static bool _walImageReady;
    private static bool _sdImageReady;

    private static string AppPublishDir => Path.Combine(_root, "artifacts", "e2e", "app");
    private static string TestsPublishDir => Path.Combine(_root, "artifacts", "e2e", "tests");

    /// <summary>Подготовка статических ассетов процесса (канон AGENTS.md: сборка
    /// НА ХОСТЕ, в образы — только готовые артефакты; sdk-стадий и COPY
    /// исходников в E2E-образах нет): publish PgWorker.App → pgworker:e2e;
    /// publish тестов → pgw-e2e-runner:e2e; docker build pgworker-node:e2e.
    /// PGW_TEST_E2E_NOBUILD=1 — пропуск publish-шагов (бисект/отладка бинаря).</summary>
    private static async Task EnsureStaticAsync(CancellationToken ct)
    {
        // Раннер: статические ассеты собраны фазой 1 (publish + docker build —
        // задача хоста; FindRoot/SDK в контейнере невозможны).
        if (IsInnerRunner)
            return;
        if (_staticReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_staticReady)
                return;

            _root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var noBuild = Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1";

            // publish PgWorker.App → образ SUT-воркера.
            if (noBuild)
                E2eFixture.EnsurePublishedDir(AppPublishDir,
                    "требуется: dotnet publish src/PgWorker.App -c Release -o artifacts/e2e/app");
            else
                await E2eFixture.PublishProjectAsync(
                    Path.Combine(_root, "src", "PgWorker.App", "PgWorker.App.csproj"),
                    AppPublishDir, "app", ct);

            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {WorkerImage}: docker build (контекст artifacts/e2e/app)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/PgWorker.E2E.Dockerfile", "-t", WorkerImage, AppPublishDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-worker-e2e.log");
            E2eFixture.StaticPhase($"e2e-image {WorkerImage}: готов за {sw.Elapsed.TotalSeconds:F0} с");

            // publish тестовой сборки → образ раннера (фаза 2).
            if (noBuild)
                E2eFixture.EnsurePublishedDir(TestsPublishDir,
                    "требуется: dotnet publish src/tests/PgWorker.IntegrationTests -c Release -o artifacts/e2e/tests");
            else
                await E2eFixture.PublishProjectAsync(
                    Path.Combine(_root, "src", "tests", "PgWorker.IntegrationTests", "PgWorker.IntegrationTests.csproj"),
                    TestsPublishDir, "tests", ct);
            // xunit.runner.json (канон параллелизма) обязан быть рядом с dll.
            var runnerJson = Path.Combine(TestsPublishDir, "xunit.runner.json");
            if (!File.Exists(runnerJson))
                File.Copy(
                    Path.Combine(_root, "src", "tests", "PgWorker.IntegrationTests", "xunit.runner.json"),
                    runnerJson);

            sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {RunnerImage}: docker build (контекст artifacts/e2e/tests)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/E2eRunner.Dockerfile", "-t", RunnerImage, TestsPublishDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-runner-e2e.log");
            E2eFixture.StaticPhase($"e2e-image {RunnerImage}: готов за {sw.Elapsed.TotalSeconds:F0} с");

            // Образ узла (задача 25): docker-cache делает повторные сборки
            // инкрементальными. ПЕРЕДАЧА КОНТЕКСТА (COPY src) — самая «тихая»
            // фаза: метки до/после обязательны (канон e2e-launch).
            sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase("e2e-image pgworker-node:e2e: docker build (контекст корня)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-q", "-f", $"{_root}/docker/node/Dockerfile", "-t", NodeImage, _root,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-node-e2e.log");
            E2eFixture.StaticPhase($"e2e-image {NodeImage}: готов за {sw.Elapsed.TotalSeconds:F0} с");

            _staticReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // Образ ownS3-ноды (t36): .NET-сервис — publish НА ХОСТЕ (инкрементально,
    // секунды), в образ — только готовый вывод (docker/OwnS3.Dockerfile,
    // runtime-слой; канон AGENTS.md). Локальная сборка, в registry не кладётся.
    private static async Task EnsureOwnS3ImageAsync(CancellationToken ct)
    {
        // Раннер: образ гарантированно собран фазой 1 (host-сборка — не задача
        // раннера, SDK в контейнере нет).
        if (IsInnerRunner)
            return;
        if (_owns3ImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_owns3ImageReady)
                return;
            var outDir = Path.Combine(_root, "artifacts", "e2e", "owns3");
            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {OwnS3Image}: dotnet publish OwnS3.App (host)…");
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/OwnS3.App/OwnS3.App.csproj",
                "-c", "Release", "-o", outDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-e2e-static-process-owns3-publish.log");
            E2eFixture.StaticPhase($"e2e-image {OwnS3Image}: publish готов за {sw.Elapsed.TotalSeconds:F0} с — docker build (контекст artifacts/e2e/owns3)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/OwnS3.Dockerfile", "-t", OwnS3Image, outDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-owns3-build.log");
            E2eFixture.StaticPhase($"e2e-image {OwnS3Image}: готов за {sw.Elapsed.TotalSeconds:F0} с (publish+build)");
            _owns3ImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    private static async Task EnsureJobImageAsync(CancellationToken ct)
    {
        // Раннер: образ гарантированно собран фазой 1 (host-сборка — не задача
        // раннера, SDK в контейнере нет).
        if (IsInnerRunner)
            return;
        if (_jobImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_jobImageReady)
                return;
            // Канон AGENTS.md «E2E-образы собираются на хосте»: джоб — sh-скрипт,
            // .NET-publish не нужен; узкий контекст docker/ (без «тихой» передачи
            // корня репозитория), вывод — с [PHASE]-меткой и таймингом.
            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {JobImage}: docker build (контекст docker/)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/PgWorker.Backup.E2E.Dockerfile", "-t", JobImage,
                $"{_root}/docker",
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-backup-e2e.log");
            E2eFixture.StaticPhase($"e2e-image {JobImage}: готов за {sw.Elapsed.TotalSeconds:F0} с");
            _jobImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // Образ wal-агента-приёмника (t27, arch/19 §3): приёмник — .NET: publish НА
    // ХОСТЕ (инкрементально, секунды), в образ — только готовый вывод
    // (docker/PgWorker.Wal.E2E.Dockerfile, runtime-слой; канон AGENTS.md).
    private static async Task EnsureWalImageAsync(CancellationToken ct)
    {
        // Раннер: образ гарантированно собран фазой 1 (host-сборка — не задача
        // раннера, SDK в контейнере нет).
        if (IsInnerRunner)
            return;
        if (_walImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_walImageReady)
                return;
            var outDir = Path.Combine(_root, "artifacts", "e2e", "wal");
            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {WalImage}: dotnet publish WalReceiver (host)…");
            // NoMsBuildReuseEnv + бюджет: build-server/ноды держат stdout-пайп
            // после завершения publish (e2e-факт: 11 мин тишины) — без реюза
            // процесс умирает вместе с пайпом; зависание = fail-fast за бюджет.
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj",
                "-c", "Release", "-o", outDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-e2e-static-process-wal-e2e-publish.log");
            E2eFixture.StaticPhase($"e2e-image {WalImage}: publish готов за {sw.Elapsed.TotalSeconds:F0} с — docker build (контекст artifacts/e2e/wal)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/PgWorker.Wal.E2E.Dockerfile", "-t", WalImage, outDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-wal-e2e-build.log");
            E2eFixture.StaticPhase($"e2e-image {WalImage}: готов за {sw.Elapsed.TotalSeconds:F0} с (publish+build)");
            _walImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // Образ file_sd-генератора (t15, arch/18 §5.4): .NET-сервис — publish НА
    // ХОСТЕ (инкрементально, секунды), в образ — только готовый вывод
    // (docker/Metrics.SdGenerator.Dockerfile, runtime-слой; канон AGENTS.md).
    // Вызов — лениво из сценария E2ePatroniFileSd: чужие серии и кейс-маркер
    // гейта эту сборку не платят.
    public static async Task EnsureSdImageAsync(CancellationToken ct)
    {
        if (_sdImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_sdImageReady)
                return;
            var outDir = Path.Combine(_root, "artifacts", "e2e", "sdgenerator");
            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {SdImage}: dotnet publish SdGenerator (host)…");
            // NoMsBuildReuseEnv + бюджет — канон EnsureWalImageAsync (пайп stdout).
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/Metrics.SdGenerator/Metrics.SdGenerator.csproj",
                "-c", "Release", "-o", outDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-e2e-static-process-sd-e2e-publish.log");
            E2eFixture.StaticPhase($"e2e-image {SdImage}: publish готов за {sw.Elapsed.TotalSeconds:F0} с — docker build (контекст artifacts/e2e/sdgenerator)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/Metrics.SdGenerator.Dockerfile", "-t", SdImage, outDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-e2e-static-process-sd-e2e-build.log");
            E2eFixture.StaticPhase($"e2e-image {SdImage}: готов за {sw.Elapsed.TotalSeconds:F0} с (publish+build)");
            _sdImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }
}
