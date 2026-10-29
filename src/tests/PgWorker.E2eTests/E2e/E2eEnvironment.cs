using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Shared.Etcd.Client;
using Shared.TestInfra;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// Изолированное e2e-окружение контура «Вариант A» (docs/e2e-isolation.md).
/// RYUK ОТКЛЮЧЁН: владелец всех объектов контура — сам прогон (фаза 1/teardown,
/// stop-режим упавшего сценария обязан ОСТАВИТЬ etcd/сеть для разбора), ryuk же
/// сносит testcontainers-объекты (etcd, сеть окружения) при смерти host-процесса
/// — ломает и телеметрию failed-прогона, и ассерт чистоты.
/// Подъём (StartAsync, docs/e2e-isolation.md §1): собственная docker-сеть
/// pgw-en-{guid}, etcd-контур (1 или 3 узла), опционально ownS3-нода; etcd/ownS3
/// и API SUT-воркера публикуют ДИНАМИЧЕСКИЕ хост-порты (тест ходит по
/// localhost:{порт}, контейнеры — по host.docker.internal:{порт}). Смерть
/// etcd-контейнера уносит ключи: per-environment etcd снимает проблему чистки
/// ключей. DisposeAsync при ЛЮБОМ исходе: телеметрия ДО удалений → успешный
/// прогон: rm контейнеров/томов/сетей + ассерт чистоты; упавший (MarkFailed):
/// контейнеры ОСТАНАВЛИВАЮТСЯ, не удаляются — разбор по живым объектам
/// (README-cleanup.txt). Чужие pgw-* объекты параллельных
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

    // ===== Теги pg-собираемых e2e-образов: PER-BRANCH (docs/e2e-isolation.md §9) =====

    // Суффикс — ТОЛЬКО имя ветки (одна ветка — один набор образов; git-инвариант
    // «одна ветка — один checkout»); git недоступен / HEAD detached — fallback
    // {хэш8} пути worktree (Shared.TestInfra.E2eImageTags). Хэш пути из суффикса
    // Резолв всегда локальный (тестовые процессы той же ветки дают тот же тег).
    internal static readonly string ImageTagSuffix =
        E2eImageTags.SuffixFor(E2eFixture.FindRoot(AppContext.BaseDirectory));

    /// <summary>Образ SUT-воркера (контур «Вариант A»): publish PgWorker.App НА
    /// ХОСТЕ → образ publish-COPY (docker/PgWorker.E2E.Dockerfile); локальная
    /// сборка, в registry не кладётся.</summary>
    public static string WorkerImage => $"pgworker:e2e-{ImageTagSuffix}";

    // Нода пула — ВНЕШНИЙ образ owns3:dev (репозиторий ../OwnS3, сборка —
    // deploy/build-image.sh там; в registry не кладётся; pg его не собирает).
    public const string OwnS3Image = "owns3:dev";

    // mc запинен существующим docker-тегом (у docker-тегов и архивных тегов
    // dl.min.io метки времени расходятся).
    internal const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";

    // Образ узла (задача 25): без DOORMAN_URL — узел без пулера, PgWorker
    // запускается с EnableDoorman=false (Д4, R1).
    public static string NodeImage => $"pgworker-node:e2e-{ImageTagSuffix}";

    /// <summary>Образ file_sd-генератора (t15, arch/18 §5.4): собирается ЛЕНИВО —
    /// из сценария E2ePatroniFileSd (EnsureSdImageAsync), чтобы остальные E2E-серии
    /// (включая кейс-маркер Scale_AddEmptyShard) не платили этой сборкой.</summary>
    public static string SdImage => $"sdgenerator:e2e-{ImageTagSuffix}";

    // Креды ownS3-ноды E2E: root-пара (mc/сид/чтение теста) и read-write ключ
    // ноды — им соответствует ключ «backups» per-cluster secrets-ключа пула
    // (arch/19 §7: PgWorker читает /s3/clusters/<C>/secrets, env-профилей нет).
    // Секреты генерируются в рантайме (криптостойкий RNG) — не зашиваются в
    // исходник; инициализация раз на процесс, переиспользуются всеми сериями.
    public const string S3RootUser = "owns3-e2e-root";
    public static readonly string S3RootPassword = $"owns3-e2e-root-pwd-{ShortToken()}-{ShortToken()}";
    public const string S3WriterKey = "pgw-e2e-writer";
    public static readonly string S3WriterSecret = $"pgw-e2e-writer-pwd-{ShortToken()}-{ShortToken()}";

    private static string ShortToken()
    {
        // Криптостойкий генератор: 20 символов букв/цифр (формат ownS3-кредов).
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(20);
        return new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }
    private const string BucketName = "pgworker-backups";

    // Пул S3 E2E — bucket per-install (единый на всех нодах пула, arch/19 §5;
    // ownS3-ноды окружения поднимаются с ним в StartAsync); креды пула сеет
    // SeedS3PoolAsync в secrets-ключ (канон arch/23 §2).
    public const string PoolBucket = BucketName;

    private readonly List<HostInstance> _hosts = [];

    /// <summary>Живые воркеры окружения (диагностика сценариев).</summary>
    public IReadOnlyList<HostInstance> Hosts => _hosts;

    private readonly HttpClient _gatewayHttp = new();
    private readonly IReadOnlyList<IContainer>? _etcdNodes;      // только фаза 1 (хост)
    private readonly IReadOnlyList<string> _etcdNames;
    private readonly INetwork? _net;                             // только фаза 1 (хост)
    private readonly IContainer? _s3Node;                         // только фаза 1

    // Фактический динамический хост-порт ownS3-ноды (0 — ноды нет); тестовые
    // S3-клиенты ходят http://localhost:{_s3HostPort}, внутрисетевые потребители
    // (SeedS3PoolAsync: env воркера/portalloc) — host.docker.internal:{порт}.
    private readonly int _s3HostPort;

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

    /// <summary>Ожидание завершения провижининга кластера с fail-fast
    /// «застрял»: config без state + dsn + ноды RUNNING (+ buckets/status пуст,
    /// если передан range). Если хоть одна нода RUNNING дольше 60 с, а
    /// config.state не снят — воркер не завершает заявку (ретраи): телеметрия
    /// + ранний фейл вместо молчаливого выживания бюджета (прогон 2026-10-16
    /// AC3: 310 с ожидания при живом лидере патрони). Строгая семантика серий:
    /// shards = ["shard1","shard2"] по умолчанию; RestTls-подобные — ["shard1"].</summary>
    public static async Task<bool> WaitProvisionedAsync(
        E2eEnvironment fx,
        Func<string, Task<Shared.Etcd.Client.Kv?>> getOrNull,
        string cluster, CancellationToken ct,
        string[]? shards = null,
        Func<string, Task<System.Collections.Generic.IReadOnlyList<Shared.Etcd.Client.Kv>>>? range = null,
        TimeSpan? budget = null)
    {
        shards ??= ["shard1", "shard2"];
        var runningSince = (DateTimeOffset?)null;
        return await E2eFixture.WaitForAsync(async () =>
        {
            var config = await getOrNull($"/clusters/{cluster}/config");
            if (config is null
                || System.Text.Json.JsonSerializer
                    .Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(config.Value)!
                    .ContainsKey("state"))
            {
                var anyRunning = false;
                foreach (var shard in shards)
                {
                    foreach (var suffix in new[] { "a", "b" })
                    {
                        var st = await getOrNull(
                            $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state");
                        if (st is { Value: "RUNNING" })
                            anyRunning = true;
                    }
                }

                if (anyRunning)
                {
                    runningSince ??= DateTimeOffset.UtcNow;
                    if (DateTimeOffset.UtcNow - runningSince > TimeSpan.FromSeconds(60))
                    {
                        await fx.CollectDiagnosticsAsync("provision-stuck-fail-fast");
                        throw new ApplicationException(
                            $"provisioning is stuck: nodes {cluster} ({string.Join(',', shards)}) RUNNING "
                            + ">60 s, config.state not lifted (the worker does not complete the request — see "
                            + "container-pgw-ew-*.log in the fail-fast telemetry)");
                    }
                }
                else
                {
                    runningSince = null;
                }

                return false;
            }

            foreach (var shard in shards)
            {
                if (await getOrNull($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                    return false;
                foreach (var suffix in new[] { "a", "b" })
                {
                    var ready = await getOrNull(
                        $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state");
                    if (ready?.Value != "RUNNING")
                        return false;
                }
            }

            return range is null || (await range($"/clusters/{cluster}/buckets/status/")).Count == 0;
        }, budget ?? TimeSpan.FromSeconds(360), ct);
    }

    /// <summary>Fail-fast-декоратор условия провижининга для перевода серий:
    /// оборачивает ЛОКАЛЬНОЕ строгое условие серии (ProvisionedAsync) — если
    /// оно не выполнено, а ноды RUNNING дольше 60 с при неснятом config.state
    /// (воркер не завершает заявку, ретраи) — телеметрия + ранний фейл вместо
    /// молчаливого выживания бюджета (прогон 2026-10-16 AC3: 310 с при живом
    /// лидере). Пример: E2ePhase.WaitAsync(Fx, "…-provisioning",
    ///   E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster,
    ///       () => ProvisionedAsync(cluster)), …).</summary>
    public static Func<Task<bool>> ProvisionStuckFailFast(
        E2eEnvironment fx,
        Func<string, Task<Shared.Etcd.Client.Kv?>> getOrNull,
        string cluster,
        Func<Task<bool>> baseCondition,
        string[]? shards = null)
    {
        shards ??= ["shard1", "shard2"];
        var runningSince = (DateTimeOffset?)null;
        return async () =>
        {
            if (await baseCondition())
                return true;

            var config = await getOrNull($"/clusters/{cluster}/config");
            if (config is null
                || !System.Text.Json.JsonSerializer
                    .Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(config.Value)!
                    .ContainsKey("state"))
            {
                runningSince = null;
                return false;
            }

            var anyRunning = false;
            foreach (var shard in shards)
            foreach (var suffix in new[] { "a", "b" })
            {
                if ((await getOrNull(
                        $"/clusters/{cluster}/shards/{shard}/nodes/{shard}{suffix}/state"))
                    is { Value: "RUNNING" })
                    anyRunning = true;
            }

            if (!anyRunning)
            {
                runningSince = null;
                return false;
            }

            runningSince ??= DateTimeOffset.UtcNow;
            if (DateTimeOffset.UtcNow - runningSince > TimeSpan.FromSeconds(60))
            {
                await fx.CollectDiagnosticsAsync("provision-stuck-fail-fast");
                throw new ApplicationException(
                    $"provisioning is stuck: nodes {cluster} ({string.Join(',', shards)}) RUNNING "
                    + ">60 s, config.state not lifted (the worker does not complete the request — see "
                    + "container-pgw-ew-*.log in the fail-fast telemetry)");
            }

            return false;
        };
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

    /// <summary>Факт одного приёмника WAL из статус-ключа agent/&lt;N&gt; (t25,
    /// arch/19 §4): сценарии сверяют доставку по ключам etcd и объектам S3,
    /// а не по docker ps wal-контейнеров (их больше нет). State/Role — строковые
    /// значения ключа ("streaming"/"waiting_role"/...; "master"/"sync"/"none").</summary>
    public sealed record WalAgentFact(
        string Node, string? State, string? Role, bool PoolExhausted,
        string? JumpFrom, string? JumpTo, uint? JumpTli, string? Error);

    /// <summary>Чтение фактов приёмников шарда (t25): Range
    /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/agent/ — толерантный парсинг
    /// arch/19 §4 (битые/чужие значения пропускаются); отсутствующие ключи
    /// (lease истёк) в списке не появляются — «ключ жив = приёмник жив».</summary>
    public async Task<IReadOnlyList<WalAgentFact>> ReadWalAgentFactsAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var range = await Gateway.RangeAsync(
            EtcdEndpoint, $"/pgworker/backups/{cluster}/{shard}/agent/", ct);
        if (!range.IsSuccess)
            throw new ApplicationException($"range agent facts {cluster}/{shard}: {range.Error!.Message}");
        var facts = new List<WalAgentFact>();
        foreach (var kv in range.Value)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != System.Text.Json.JsonValueKind.Object
                    || !root.TryGetProperty("node", out var node)
                    || !root.TryGetProperty("state", out var state)
                    || node.ValueKind != System.Text.Json.JsonValueKind.String
                    || state.ValueKind != System.Text.Json.JsonValueKind.String)
                    continue;
                static string? Str(in System.Text.Json.JsonElement root, string name)
                    => root.TryGetProperty(name, out var e) && e.ValueKind == System.Text.Json.JsonValueKind.String
                        ? e.GetString() : null;
                var jumpFrom = (string?)null;
                var jumpTo = (string?)null;
                uint? jumpTli = null;
                if (root.TryGetProperty("tli_jump", out var jump)
                    && jump.ValueKind == System.Text.Json.JsonValueKind.Object)
                {
                    jumpFrom = Str(jump, "from");
                    jumpTo = Str(jump, "to");
                    if (jump.TryGetProperty("tli", out var tli) && tli.TryGetUInt32(out var tliValue))
                        jumpTli = tliValue;
                }
                facts.Add(new WalAgentFact(
                    node.GetString()!, state.GetString(), Str(root, "role"),
                    root.TryGetProperty("pool_exhausted", out var pe) && pe.ValueKind == System.Text.Json.JsonValueKind.True,
                    jumpFrom, jumpTo, jumpTli, Str(root, "error")));
            }
            catch (System.Text.Json.JsonException)
            {
                // битое/чужое значение — не факт (толерантность arch/19 §4)
            }
        }
        return facts;
    }

    /// <summary>Имена контейнеров нод шарда из portalloc (t25: приёмник —
    /// supervisord-программа контейнера ноды; docker exec supervisorctl).
    /// Именование — pgw-&lt;C&gt;-&lt;X&gt;-&lt;N&gt; (канон provisioning).</summary>
    public async Task<IReadOnlyList<string>> ShardNodeContainerNamesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var kv = await Gateway.GetAsync(EtcdEndpoint, $"/pgworker/portalloc/{cluster}", ct);
        if (kv.Value is not { } entry)
            throw new ApplicationException($"portalloc {cluster} is missing");
        var entries = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(entry.Value)!
            .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
            .Select(p => p.Key.Split('/')[1])
            .ToList();
        return [.. entries.Select(node => $"pgw-{cluster}-{shard}-{node}")];
    }

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
        IContainer? s3Node, IReadOnlyList<int> etcdHostPorts, int s3HostPort)
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
        _s3Node = s3Node;
        _s3HostPort = s3HostPort;
        Gateway = new EtcdGateway(_gatewayHttp);
        // Динамические публикации (docs/e2e-isolation.md §2): тестовый Gateway
        // ходит по localhost:{фактический порт}; контейнеры-потребители (воркер,
        // Patroni-ноды) — по host.docker.internal:{фактический порт} (env).
        EtcdEndpoint = $"http://localhost:{etcdHostPorts[0]}";
        EtcdEndpoints = etcdHostPorts.Select(p => $"http://host.docker.internal:{p}").ToList();
        S3Endpoint = s3HostPort == 0 ? "" : $"http://localhost:{s3HostPort}";
        PatroniHttp = CreatePatroniHttpsClientOnly();
        HealthHttp = CreateMtlsHttpClient();
    }

    public string Slug { get; }

    /// <summary>Хост-путь каталога снапшотов воркеров (bind-источник: воркеры
    /// пишут etcd-снапшоты в /snapshots → каталог артефактов хоста); сценарии
    /// читают snapshot-файлы напрямую.</summary>
    public string SnapshotsHostDir => Path.Combine(ArtifactsDir, "snapshots");

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

    /// <summary>mTLS-клиент пробы готовности для воркера с env-парой
    /// (PGW_API_TLS_CERT перекрывает сервер): клиентский серт — от CA контура
    /// (CLIENT_CA воркером не перекрывается), серверное доверие — по списку
    /// sha256-hex thumbprint'ов через запятую (этапы жизненного цикла серта
    /// поднимают API то на managed-ключе, то на env-фоллбеке — все self-signed,
    /// цепи нет).</summary>
    private HttpClient CreateThumbprintHealthClient(string thumbprintsCsv)
    {
        var expected = thumbprintsCsv
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(t => t.ToLowerInvariant())
            .ToHashSet();
        var (clientPem, clientKeyPem) = E2eTestPki.Issue(
            _caPem, _caKeyPem, "e2e-healthcheck", ["e2e-healthcheck"], ip: null);
        var pemPair = X509Certificate2.CreateFromPem(clientPem, clientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pemPair.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    certificate is X509Certificate2 c2
                    && expected.Contains(Convert.ToHexString(
                            System.Security.Cryptography.SHA256.HashData(c2.RawData))
                        .ToLowerInvariant()),
            },
        })
        {
            Timeout = TimeSpan.FromSeconds(3),
        };
    }

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

    // ===== Подъём окружения (хост): класс-фикстура = своё окружение =====

    /// <summary>Точка входа сценария (docs/e2e-isolation.md §1): тест исполняется
    /// на хосте — класс-фикстура поднимает ПОЛНЫЙ контур класса (Guard-слот
    /// захвачен вызывающим ДО этого вызова; сеть/etcd/ownS3/CA/окно портов),
    /// тело факта — обычный код хост-процесса. Раннер-контейнер и двухфазный
    /// запуск упразднены.</summary>
    public static Task<E2eEnvironment> StartAsync(
        string slug, bool withS3Node = false, bool haEtcd = false, CancellationToken ct = default)
        => StartOnceAsync(slug, withS3Node, haEtcd, ct);

    /// <summary>Удаление сети окружения (конец класса). Идемпотентно:
    /// «no such network» — успех; активные endpoints контейнеров держат
    /// сеть — принудительный disconnect -f каждого endpoint (ha-etcd-ноды
    /// после kill-сценария не отцепляются сами) + ретраи до 60 с.</summary>
    public static async Task DeleteClassNetworkAsync(string netName, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await ForceDisconnectEndpointsAsync(netName, ct);
                await E2eFixture.RunProcessAsync("docker", ["network", "rm", netName], ct);
                return;
            }
            catch (ApplicationException e) when (attempt < 30
                && e.Message.Contains("no such object", StringComparison.OrdinalIgnoreCase))
            {
                return; // уже удалена — успех
            }
            catch (ApplicationException) when (attempt < 30)
            {
                // сеть держат endpoints — ретрай
                await Task.Delay(2000, ct);
            }
        }
    }

    // Принудительный отцеп всех endpoints сети: docker network rm падает
    // «network has active endpoints», если контейнер умер без отцепа
    // (kill-сценарии ha-etcd). Лучшими усилиями — ошибки не роняют teardown.
    private static async Task ForceDisconnectEndpointsAsync(string netName, CancellationToken ct)
    {
        try
        {
            var inspect = await E2eFixture.RunProcessAsync(
                "docker", ["network", "inspect", netName, "--format", "{{range .Containers}}{{.Name}} {{end}}"], ct);
            foreach (var name in inspect.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                try
                {
                    await E2eFixture.RunProcessAsync(
                        "docker", ["network", "disconnect", "-f", netName, name], ct);
                }
                catch (ApplicationException)
                {
                    // контейнер уже ушёл — endpoint отцепится удалением сети
                }
            }
        }
        catch (ApplicationException)
        {
            // сети нет — отцеплять нечего
        }
    }

    private static async Task<E2eEnvironment> StartOnceAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct)
    {
        await EnsureStaticAsync(ct);

        // Ретрай старта (docs/e2e-isolation.md §6): гонка сети (чужой глобальный
        // prune — только что созданная сеть без контейнеров может быть снесена
        // до старта etcd) И занятость зондируемого хост-порта динамической
        // публикации («already allocated», «address already in use»).
        for (var w = 0; w < 5; w++)
        {
            try
            {
                return await StartOnceCoreAsync(slug, withS3Node, haEtcd, ct);
            }
            catch (Exception e) when (IsRetryableStartRace(e))
            {
                Console.Error.WriteLine(
                    $"e2e[{slug}]: start interrupted by an external race — recreating the environment (attempt {w + 1}/5): {e.Message.Split('\n')[0]}");
                await Task.Delay(2000, ct);
            }
        }

        throw new ApplicationException(
            $"e2e[{slug}]: failed to raise the environment in 5 attempts (the start-retry budget is exhausted)");
    }

    // Ретрай-условия старта: чужой prune сети (network … not found) И занятость
    // зондируемого хост-порта (docker «…port is already allocated», proxy bind
    // «address already in use»). Прочие причины не ретраятся.
    private static bool IsRetryableStartRace(Exception e)
        => e.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
           && e.Message.Contains("not found", StringComparison.OrdinalIgnoreCase)
           || e.Message.Contains("already allocated", StringComparison.OrdinalIgnoreCase)
           || e.Message.Contains("address already in use", StringComparison.OrdinalIgnoreCase);

    private static async Task<E2eEnvironment> StartOnceCoreAsync(
        string slug, bool withS3Node, bool haEtcd, CancellationToken ct)
    {
        // Сеть окружения: создаётся здесь и живёт до DisposeAsync окружения.
        var runId = Guid.NewGuid().ToString("N");
        var netName = $"pgw-en-{runId}";
        var net = new NetworkBuilder().WithName(netName).Build();
        var etcdNodes = new List<IContainer>();
        var etcdNames = new List<string>();
        var etcdAliases = Enumerable.Range(1, haEtcd ? 3 : 1).Select(i => $"e2e-etcd{i}").ToList();
        var etcdHostPorts = new List<int>();
        IContainer? s3Node = null;
        // Артефакты окружения — temp хоста (телеметрия teardown'а и dumps).
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        // per-run CA окружения генерируется здесь.
        var (caPem, caKeyPem) = E2eTestPki.GenerateCa("e2e");
        // Окно портов — локальный счётчик процесса (параллельные контуры
        // одного прогона получают непересекающиеся окна).
        var portFrom = PortWindowBase
            + (int)Math.Min(Interlocked.Increment(ref _windowCursor) - 1, 100) * PortWindowSize;
        var portTo = portFrom + PortWindowSize;
        var envS3HostPort = 0;

        // Дроссель параллельности: слот КОНТУРА захвачен класс-фикстурой ДО
        // этого вызова (docs/e2e-isolation.md §1/§9); Release — в DisposeAsync
        // фикстуры / при провале подъёма.
        try
        {
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
                    // Динамическая публикация клиента (docs/e2e-isolation.md §2):
                    // docker выделяет свободный хост-порт; контейнеры-потребители
                    // ходят через host.docker.internal:{фактический порт} (env),
                    // тест — через localhost:{фактический порт}. add-host — шлюз
                    // к опубликованному порту из сети окружения.
                    "--add-host", "host.docker.internal:host-gateway",
                    "-p", "127.0.0.1::2379",
                    EtcdImage,
                    "etcd", $"--name={alias}", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379", "--listen-peer-urls=http://0.0.0.0:2380",
                    $"--initial-advertise-peer-urls=http://{alias}:2380",
                    // advertise-client-urls задаётся ПРИ ЗАПУСКЕ, когда фактический
                    // динамический порт ещё не выделен (перевыделение при повторном
                    // run невозможно без гонки) — поэтому внутрисетевой URL, как и
                    // peer-URL. Клиенты advertise НЕ потребляют: воркер/Patroni-
                    // ноды получают СПИСОК endpoints env'ом (host.docker.internal:{p}),
                    // тестовый Gateway — localhost:{p}; member-list-discovery в
                    // контуре не используется.
                    $"--advertise-client-urls=http://{alias}:2379",
                    $"--initial-cluster={initialCluster}", $"--initial-cluster-token=e2e-{runId}",
                    "--initial-cluster-state=new", "--heartbeat-interval=250", "--election-timeout=2000",
                };
                await E2eFixture.RunDockerAsync([.. etcdArgs], ct);
                etcdHostPorts.Add(await DockerHostPortAsync(name, 2379, ct));
            }

            Console.WriteLine($"[PHASE] e2e-env {slug}: etcd nodes raised ({etcdNames.Count}), waiting for health…");

            // Готовность контура (бюджет ≤ 100 c — быстрое падение фикстуры,
            // AGENTS.md): etcdctl внутри узла №1 — endpoint health по всем
            // узлам; HA — плюс избранный лидер (endpoint status -w json,
            // разбор в EtcdQuorumElected: в etcd-образе НЕТ /bin/sh — пайп
            // «| grep» внутри контейнера всегда exec 127, кластер при этом
            // мог быть полностью здоров).
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
                            ["exec", etcdNames[0], "etcdctl", $"--endpoints={endpointsArg}",
                             "endpoint", "status", "-w", "json"], ct);
                        if (!EtcdQuorumElected(status, etcdAliases.Count))
                            return false; // кворум не собран / лидер не избран
                    }

                    return true;
                }
                catch (Exception)
                {
                    return false; // ещё поднимается / кворум не собран
                }
            }, TimeSpan.FromSeconds(100), ct);
            ready.Should().BeTrue($"the etcd contour ({(haEtcd ? 3 : 1)} node(s)) must assemble within 100 s "
                + $"(probes made: {probeAttempts})");

            if (withS3Node)
            {
                // ownS3 в сети окружения (mc ходит по алиасу; джобы/воркер — по
                // host.docker.internal:{динамический порт}). Динамическая
                // публикация API ноды на хост — docs/e2e-isolation.md §2.
                s3Node = null;
                await E2eFixture.RunDockerAsync(
                [
                    "run", "-d", "--name", $"pgw-eo-{runId}",
                    "--network", netName, "--network-alias", "e2e-owns3",
                    "-p", "127.0.0.1::9000",
                    "-e", $"OWNS3_ROOT_USER={S3RootUser}",
                    "-e", $"OWNS3_ROOT_PASSWORD={S3RootPassword}",
                    // Ключ 0 — read-write для подсистемы бэкапов (профиль воркера).
                    "-e", $"OWNS3_ACCESS_KEYS__0__ACCESSKEY={S3WriterKey}",
                    "-e", $"OWNS3_ACCESS_KEYS__0__SECRETKEY={S3WriterSecret}",
                    "-e", "OWNS3_ACCESS_KEYS__0__POLICY=read-write",
                    OwnS3Image,
                ], ct);
                await EnsureOwnS3ImageAsync(ct);
                var s3HostPort = await DockerHostPortAsync($"pgw-eo-{runId}", 9000, ct);

                // mc — ИНСТРУМЕНТАЛЬНЫЙ контейнер ФАКТА (pgw-em-{runId}): docker
                // exec для всех mc-вызовов; удаляется teardown'ом факта.
                await E2eFixture.RunProcessAsync("docker",
                [
                    "run", "-d", "--name", $"pgw-em-{runId}",
                    "--network", netName,
                    "--entrypoint", "/bin/sh", McImage, "-c", "sleep infinity",
                ], ct, timeout: TimeSpan.FromMinutes(5));

                Console.WriteLine($"[PHASE] e2e-env {slug}: ownS3 node raised (host port {s3HostPort}), fact mc container, bucket/images…");
                await EnsureOwnS3BucketAsync(runId, "e2e-owns3", ct);
                envS3HostPort = s3HostPort;
            }

            var env = new E2eEnvironment(slug, runId, netName, artifactsDir, etcdAliases,
                withS3Node ? "e2e-owns3" : "", caPem, caKeyPem, portFrom, portTo,
                net, etcdNodes, etcdNames, s3Node, etcdHostPorts, envS3HostPort);
            // REST-TLS CA контура: артефакт для разбора падений и ручных
            // curl-проверок контура (+ отпечаток — сверка с env воркера).
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.pem"), caPem);
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.sha256"),
                Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
                    Encoding.UTF8.GetBytes(caPem))));
            File.WriteAllText(Path.Combine(artifactsDir, "rest-ca.key"), caKeyPem);

            Console.WriteLine(
                $"[PHASE] e2e-env {slug}: environment raised (network {netName}, window {portFrom}–{portTo})");
            return env;
        }
        catch
        {

            // частично поднятое окружение не оставляем (лучшими усилиями):
            // контейнер, чей StartAsync упал, чистит сам testcontainers; живые
            // узлы etcd/ownS3 — здесь; сеть — здесь.
            if (s3Node is not null)
                try
                {
                    await s3Node.DisposeAsync();
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

            // Сеть окружения — создана здесь (StartOnceCoreAsync): сносим
            // частично поднятое окружение целиком.
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

    /// <summary>Фактический динамический хост-порт публикации контейнера
    /// (docs/e2e-isolation.md §2): `docker port {name} {containerPort}/tcp` →
    /// строка вида «127.0.0.1:54321» → парс порта.</summary>
    private static async Task<int> DockerHostPortAsync(string containerName, int containerPort, CancellationToken ct)
    {
        var output = await E2eFixture.RunDockerAsync(["port", containerName, $"{containerPort}/tcp"], ct);
        var line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault(l => l.StartsWith("127.0.0.1:", StringComparison.Ordinal))
            ?? throw new ApplicationException(
                $"no 127.0.0.1 publication for {containerName}:{containerPort}: {output.Trim()}");
        return int.Parse(line["127.0.0.1:".Length..], CultureInfo.InvariantCulture);
    }

    /// <summary>Фактический динамический хост-порт публикации контейнера

    // Список etcd-endpoints для etcdctl (внутрисетевые URL узлов).
    private static string EtcdAliases(IReadOnlyList<string> aliases)
        => string.Join(",", aliases.Select(a => $"http://{a}:2379"));

    /// <summary>Кворум HA-контура по выводу «endpoint status -w json»: все
    /// узлы ответили и называют ОДНОГО ненулевого лидера. Идентификатор
    /// лидера — uint64 и НЕ влезает в long (member_id &gt; long.MaxValue),
    /// поэтому сравниваем сырые JSON-токены, а не числа.</summary>
    internal static bool EtcdQuorumElected(string statusJson, int nodes)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(statusJson);
        var root = doc.RootElement;
        if (root.ValueKind != System.Text.Json.JsonValueKind.Array
            || root.GetArrayLength() != nodes)
            return false;
        string? leader = null;
        foreach (var endpoint in root.EnumerateArray())
        {
            if (!endpoint.TryGetProperty("Status", out var status)
                || !status.TryGetProperty("leader", out var elected))
                return false;
            var id = elected.GetRawText();
            if (id == "0")
                return false; // лидер ещё не избран
            leader ??= id;
            if (leader != id)
                return false; // узлы расходятся в лидере — кворум не сошёлся
        }
        return true;
    }

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
    // ===== SUT-воркер — контейнер в сети окружения =====

    /// <summary>
    /// Старт инстанса PgWorker.App — КОНТЕЙНЕРА pgworker:e2e в сети окружения
    /// (docs/e2e-isolation.md §3): docker.sock смонтирован (воркер провижинит
    /// ноды в контур-сеть), etcd — по host.docker.internal:{динамические порты},
    /// mTLS-серт с SAN [алиас, "localhost"] (выпускается из per-run CA; второй
    /// элемент покрывает хост-адрес тестовых проб https://localhost:{порт}).
    /// Смотрит ТОЛЬКО в etcd этого окружения и обслуживает только его
    /// Active-кластеры. extraEnv — дополнительные env-пары поверх базовых
    /// (t02: Backups-комплект).
    /// </summary>
    public async Task<HostInstance> StartHostAsync(
        string name, int snapshotIntervalMin = 360,
        IReadOnlyDictionary<string, string>? extraEnv = null, CancellationToken ct = default,
        string? readinessThumbprint = null)
    {
        var alias = $"pgw-ew-{name}";
        var containerName = $"pgw-ew-{name}-{_runId}";

        // mTLS-сервер воркера: SAN = DNS-алиас в сети окружения + "localhost"
        // (второй элемент — хост-адрес динамической публикации API: тестовые
        // mTLS-пробы ходят https://localhost:{фактический порт}).
        var (serverCertPem, serverKeyPem) = E2eTestPki.Issue(
            _caPem, _caKeyPem, "pgworker", [alias, "localhost"], ip: null);

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

        // Каталог снапшотов этого воркера на хосте (bind-источник); сценарии
        // читают snapshot-файлы напрямую (HostInstance.SnapshotsDir).
        var hostSnapsDir = Path.Combine(ArtifactsDir, "snapshots");
        Directory.CreateDirectory(Path.Combine(hostSnapsDir, name));

        var apiHostPort = await StartWorkerWithEnvAsync(
            name, alias, containerName, env, ct, readinessThumbprint);
        return new HostInstance(this, name, alias, containerName, apiHostPort);
    }

    // docker run воркера: env парами -e K=V (обход ограничения списка аргументов).
    // API воркера публикуется динамическим маппингом (-p 127.0.0.1::8443) —
    // возврат: фактический хост-порт (тестовые пробы ходят localhost:{порт}).
    // readinessThumbprint: sha256-hex серверного серта env-пары (PGW_API_TLS_CERT
    // перекрывает сервер фикстуры — проба /healthz доверяет CA контура и НЕ
    // прошла бы хендшейк; по thumbprint — честное доверие именно env-серту).
    private async Task<int> StartWorkerWithEnvAsync(
        string name, string alias, string containerName,
        IReadOnlyDictionary<string, string> env, CancellationToken ct,
        string? readinessThumbprint = null)
    {
        var hostSnapsDir = Path.Combine(ArtifactsDir, "snapshots");
        var args = new List<string>
        {
            "run", "-d", "--name", containerName,
            "--network", NetName,
            "--network-alias", alias,
            "--restart", "no",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-v", $"{hostSnapsDir}/{name}:/snapshots",
            // Динамическая публикация API (docs/e2e-isolation.md §2): тест —
            // https://localhost:{фактический порт}; внутрисетевые потребители —
            // advertise https://{alias}:8443.
            "-p", "127.0.0.1::8443",
        };
        foreach (var (key, value) in env)
            args.AddRange(["-e", $"{key}={value}"]);
        args.Add(WorkerImage);

        await E2eFixture.RunDockerAsync([.. args], ct);
        var apiHostPort = await DockerHostPortAsync(containerName, 8443, ct);
        var instance = new HostInstance(this, name, alias, containerName, apiHostPort);
        _hosts.Add(instance);

        var readinessClient = readinessThumbprint is null
            ? HealthHttp
            : CreateThumbprintHealthClient(readinessThumbprint);

        // Готовность: mTLS /healthz по внутрисетевому имени (SAN=алиас —
        // хендшейк честный). При неуспехе — хвост логов в исключении.
        var ready = await E2eFixture.WaitForAsync(async () =>
        {
            if (!await instance.IsRunningAsync(ct))
                return false;
            try
            {
                using var response = await readinessClient.GetAsync(
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
                logs = await E2eFixture.RunDockerLogsAsync([containerName],
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
                Console.Error.WriteLine($"e2e[{Slug}]: worker {name} stop: {e.Message}");
            }

            throw new ApplicationException(
                $"worker {name} did not come up within 60 s; log tail:\n{OutputTail(logs, 60)}");
        }

        return apiHostPort;
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
        result.IsSuccess.Should().BeTrue("the app secret must appear after provisioning");
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

    /// <summary>Сид S3-пула: /s3/clusters/&lt;pool&gt;/{config,
    /// nodes/&lt;I&gt;/{state,placement}, secrets, endpoints},
    /// /s3worker/portalloc/&lt;pool&gt; — прямо в etcd окружения (PgWorker —
    /// только читатель /s3/). Инстанс пула — ownS3-контейнер; имена нод
    /// node1..N; secrets-ключ — канон arch/23 §2 (root-пара окружения + ключ
    /// «backups» = writer-комплект ноды). endpoints: null → одна нода
    /// (ownS3-ноды окружения); иначе — список "host:port" (внутрисетевые
    /// адреса: вторые ownS3 сценариев AC2/AC4/AC6).</summary>
    public async Task SeedS3PoolAsync(string pool, CancellationToken ct, IReadOnlyList<string>? endpoints = null)
    {
        S3Endpoint.Should().NotBeEmpty("the pool is seeded only in an environment with ownS3 (withS3Node: true)");
        // Внутрисетевая форма endpoint'ов (потребители — s3worker-ноды и джобы
        // в сети окружения): host.docker.internal:{динамический порт}.
        var list = (endpoints is { Count: > 0 } ? endpoints
                : [$"host.docker.internal:{_s3HostPort}"])
            .Select(e => e.Replace("http://", "", StringComparison.Ordinal).TrimEnd('/'))
            .ToArray();
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/config",
            $"{{\"platform\":\"owns3\",\"nodes\":{list.Length},\"created_unix\":{now}}}",
            null, ct);
        await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/secrets",
            $"{{\"root_user\":\"{S3RootUser}\",\"root_password\":\"{S3RootPassword}\",\"access_keys\":"
            + $"[{{\"name\":\"backups\",\"access_key\":\"{S3WriterKey}\",\"secret_key\":\"{S3WriterSecret}\",\"policy\":\"read-write\"}},"
            + $"{{\"name\":\"panel\",\"access_key\":\"pgw-e2e-panel\",\"secret_key\":\"pgw-e2e-panel-pwd\",\"policy\":\"read-only\"}}]}}",
            null, ct);
        var alloc = new List<string>(list.Length);
        for (var i = 0; i < list.Length; i++)
        {
            var node = $"node{i + 1}";
            await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/nodes/{node}/state", "RUNNING", null, ct);
            await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/nodes/{node}/placement",
                "{\"docker_host\":\"local\"}", null, ct);
            var sep = list[i].LastIndexOf(':');
            sep.Should().BePositive($"endpoint '{list[i]}' must be host:port (ownS3 intra-network address)");
            alloc.Add($"\"{node}\":{{\"host\":\"{list[i][..sep]}\",\"s3\":{list[i][(sep + 1)..]}}}");
        }

        await Gateway.PutAsync(EtcdEndpoint, $"/s3worker/portalloc/{pool}",
            "{" + string.Join(",", alloc) + "}", null, ct);
        await Gateway.PutAsync(EtcdEndpoint, $"/s3/clusters/{pool}/endpoints",
            string.Join(",", list), null, ct);
    }

    /// <summary>Секция s3 policy однонодового пула (node1 в обоих списках записи;
    /// формат spec §5 / arch/19 §4). Многонодовые списки сценарии пишут сами.</summary>
    public static string PoolS3(string pool)
        => $"\"s3\":{{\"pool\":\"{pool}\",\"write_master\":[\"node1\"],\"write_replica\":[\"node1\"]}}";

    /// <summary>Дополнительная нода ownS3-пула (сценарии AC2/AC4/AC6): контейнер
    /// в сети окружения с ДИНАМИЧЕСКОЙ публикацией API (docs/e2e-isolation.md
    /// §2), алиас внутрисетевой; bucket per-install на ноде; teardown чистит
    /// own-only (имя содержит guid прогона). Возврат —
    /// http://host.docker.internal:{динамический порт} (env/portalloc).</summary>
    public async Task<string> StartExtraS3NodeAsync(string alias, CancellationToken ct)
    {
        var containerName = $"pgw-eo-{alias}-{_runId}";
        await E2eFixture.RunDockerAsync(
        [
            "run", "-d", "--name", containerName,
            "--network", NetName,
            "--network-alias", alias,
            "-p", "127.0.0.1::9000",
            "-e", $"OWNS3_ROOT_USER={S3RootUser}",
            "-e", $"OWNS3_ROOT_PASSWORD={S3RootPassword}",
            "-e", $"OWNS3_ACCESS_KEYS__0__ACCESSKEY={S3WriterKey}",
            "-e", $"OWNS3_ACCESS_KEYS__0__SECRETKEY={S3WriterSecret}",
            "-e", "OWNS3_ACCESS_KEYS__0__POLICY=read-write",
            OwnS3Image,
        ], ct);
        var hostPort = await DockerHostPortAsync(containerName, 9000, ct);

        await EnsureOwnS3BucketAsync(_runId, alias, ct);
        return $"http://host.docker.internal:{hostPort}";
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
    /// (etcd, ownS3, воркеры, движковые pgw-*) в ArtifactsDir.
    /// 1) Упавший сценарий: контейнеры ОСТАНАВЛИВАЮТСЯ, тома/сети остаются до
    /// ручной зачистки (README-cleanup.txt). 2) Успешный: rm контейнеров →
    /// rm томов → rm per-cluster сетей → rm сети окружения →
    /// 3) АССЕРТ ЧИСТОТЫ: не осталось ни одного объекта своего идентификатора.
    /// Чужие pgw-* не трогаются.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // TEARDOWN ОКРУЖЕНИЯ КЛАССА (docs/e2e-isolation.md): воркеры →
        // телеметрия ДО удалений → MarkFailed: stop-режим (контейнеры стоят
        // для разбора, README-cleanup) → успех: rm своих контейнеров
        // (etcd/ownS3/mc/воркеры/ноды/джобы — всё с runId-guid) и томов →
        // чистка ключей etcd → rm сети окружения → ассерт чистоты.
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
                problems.Add($"worker {host.Name}: {e.Message}");
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
                Console.Error.WriteLine($"e2e[{Slug}]: container {id} is already being removed by the daemon — skipping");
                return (string?)null;
            }
            catch (Exception e)
            {
                return $"container {id} rm: {e.Message}";
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
                problems.Add($"cluster network {net}: {e.Message}");
            }
        }

        // 6а) Сеть окружения (pgw-en-{runId}): живёт до DisposeAsync окружения —
        // удаляется здесь, после контейнеров/томов/per-cluster сетей (сила
        // disconnect: kill-сценарии ha-etcd оставляют мёртвые endpoints).
        await DeleteClassNetworkAsync(NetName, CancellationToken.None);

        // 7) АССЕРТ ЧИСТОТЫ факта: ни одного контейнера/тома/сети факта.
        var leftContainers = await OwnContainersAsync();
        if (leftContainers.Count > 0)
            problems.Add($"fact containers left: {string.Join(' ', leftContainers)}");
        var leftVolumes = await OwnVolumesAsync();
        if (leftVolumes.Count > 0)
            problems.Add($"fact volumes left: {string.Join(' ', leftVolumes)}");

        if (problems.Count > 0)
            throw new ApplicationException(
                $"{Slug}: fact teardown is incomplete:\n- " + string.Join("\n- ", problems));
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
                Console.Error.WriteLine($"e2e[{Slug}]: container {id} is already being removed by the daemon — skipping");
            }
            catch (Exception e)
            {
                problems.Add($"container {id} rm: {e.Message}");
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
                problems.Add($"container {id} stop: {e.Message}");
            }

        await WriteCleanupReadmeAsync();
    }

    /// <summary>Команды зачистки упавшего факта (README-cleanup.txt в артефактах:
    /// own-only по идентификатору факта, docker rm/volume/network).</summary>
    private async Task WriteCleanupReadmeAsync()
    {
        var cleanup = """
            Failed E2E scenario — the environment is STOPPED but NOT removed (investigate in place).
            Logs of all containers are saved into this directory (container-*.log).
            Re-running the test just to grab logs is forbidden (docs/e2e-launch.md).
            Clean up when the report data is no longer needed:
              docker rm -f $(docker ps -aq --filter name=__RUN__)
              docker rm -f $(docker ps -aq --filter name=__TAG__)
              docker volume rm -f $(docker volume ls -q --filter name=__TAG__)
              docker network rm __NET__
            """;
        await File.WriteAllTextAsync(
            Path.Combine(ArtifactsDir, "README-cleanup.txt"),
            cleanup.Replace("__RUN__", _runId).Replace("__TAG__", ClusterTag)
                .Replace("__NET__", NetName));
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

            // [TIMING]-метки накопленные (авто-WaitForAsync + ручные TimedAsync):
            // докачка в timing.log факта — stderr testhost глотает, файл survives.
            var timing = E2eFixture.DrainTiming();
            if (timing.Count > 0)
                await File.AppendAllLinesAsync(
                    Path.Combine(ArtifactsDir, "timing.log"), timing);

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
                    var logs = await E2eFixture.RunDockerLogsAsync(["--timestamps", id]);
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

            // Джоб-файлы нод (t13-телеметрия): restore-лог постгреса и
            // recovery-конфиги PGDATA снимаются docker cp ДО сноса/стопа
            // контейнеров — restore-pg.log живёт только в volume джоб-ноды,
            // docker-логи его не содержат, после демонтажа не восстанавливается.
            // Отсутствие файла (не джоб-нода / операция не стартовала) — не
            // ошибка: копия «лучшими усилиями».
            foreach (var (id, name) in own)
            {
                foreach (var (path, suffix) in new[]
                {
                    ("/tmp/restore-pg.log", "restore-pg.log"),
                    ("/home/postgres/pgdata/pgroot/data/postgresql.auto.conf", "auto.conf"),
                    ("/home/postgres/pgdata/pgroot/data/backup_label", "backup_label"),
                })
                {
                    try
                    {
                        var dest = Path.Combine(ArtifactsDir, $"job-{name}-{suffix}");
                        await E2eFixture.RunProcessAsync("docker", ["cp", $"{id}:{path}", dest],
                            CancellationToken.None, TimeSpan.FromSeconds(60));
                    }
                    catch
                    {
                        // файла нет (не джоб-нода) — нормально
                    }
                }
            }

            Console.Error.WriteLine($"e2e[{Slug}]: telemetry \"{reason}\" -> {ArtifactsDir}");
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"e2e[{Slug}]: telemetry collection \"{reason}\" failed: {e.Message}");
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
                problems.Add($"container {id} stop: {e.Message}");
            }

        foreach (var container in (_etcdNodes ?? []).Concat<IContainer?>([_s3Node]))
            try
            {
                if (container is not null)
                    await container.StopAsync();
            }
            catch (Exception e)
            {
                problems.Add($"infrastructure stop: {e.Message}");
            }

        _gatewayHttp.Dispose();
        HealthHttp.Dispose();
        PatroniHttp.Dispose();

        var cleanup = """
            Failed E2E scenario — the environment is STOPPED but NOT removed (investigate in place).
            Logs of all containers are saved into this directory (container-*.log, runner.log).
            Re-running the test just to grab logs is forbidden (docs/e2e-launch.md).
            Clean up when the report data is no longer needed:
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
                $"{Slug}: stop-teardown of the failed scenario is incomplete:\n- " + string.Join("\n- ", problems));

        Console.Error.WriteLine(
            $"e2e[{Slug}]: FAILED — the environment is stopped and kept for investigation: {ArtifactsDir}");
    }

    // ===== Статические ассеты процесса: publish-выводы, образы =====

    private static readonly SemaphoreSlim StaticGate = new(1, 1);

    private static bool _staticReady;
    private static string _root = "";
    private static bool _owns3ImageReady;
    private static bool _sdImageReady;

    private static string AppPublishDir => Path.Combine(_root, "artifacts", "e2e", "app");

    // Фаза подготовки (PgWorker.E2ePrepare): единственный режим, в котором
    // тесты СТРОЯТ статику. Запускается ОДНИМ процессом ДО тестовых прогонов.
    private const string PrepareModeEnv = "PGW_E2E_PREPARE";

    private static bool PreparingPhase => Environment.GetEnvironmentVariable(PrepareModeEnv) == "1";

    private const string PrepareHint =
        "run the preparation as ONE process BEFORE the tests: "
        + "dotnet run --project src/tests/PgWorker.E2ePrepare -c Release";

    /// <summary>Фаза подготовки статических ассетов — ОТДЕЛЬНЫЙ запуск ДО
    /// тестов (PgWorker.E2ePrepare): publish-выводы и docker-образы собирает
    /// один процесс; тестовые процессы статику НЕ строят — параллельные
    /// dotnet publish/docker build в общие каталоги ломают друг друга.
    /// Запись статики взаимоисключается локом подготовки
    /// artifacts/e2e/.prepare.lock (НЕ глобальный лок запуска серий —
    /// тестируемые процессы лок не берут). По успеху пишет штамп
    /// artifacts/e2e/.prepared — тесты сверяют его со свежестью исходников
    /// (устаревший бинарь маскирует регрессии).</summary>
    public static async Task PrepareStaticAssetsAsync(CancellationToken ct)
    {
        Environment.SetEnvironmentVariable(PrepareModeEnv, "1");
        FileStream? prepareLock = null;
        try
        {
            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var lockPath = Path.Combine(root, "artifacts", "e2e", ".prepare.lock");
            try
            {
                prepareLock = await E2ePrepareLock.AcquireAsync(lockPath, ct);
            }
            catch (IOException e)
            {
                string holder;
                try { holder = File.ReadAllText(lockPath).Trim(); }
                catch { holder = "<unreadable>"; }
                throw new ApplicationException(
                    "another preparation holds the lock for over "
                    + $"{(int)E2ePrepareLock.TotalBudget.TotalMinutes} min (holder: {holder})", e);
            }

            await EnsureStaticAsync(ct);
            await EnsureOwnS3ImageAsync(ct);
            await EnsureSdImageAsync(ct);
            WriteStampAtomic(
                Path.Combine(root, "artifacts", "e2e", ".prepared"),
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            E2eFixture.StaticPhase("e2e-prepare: readiness stamp written — tests may run");
            await E2eTagCleanup.CleanupAsync(
                ["pgworker", "pgworker-node", "sdgenerator"],
                $"e2e-{ImageTagSuffix}",
                (file, args, token) => E2eFixture.RunProcessAsync(file, args, token),
                ct);
        }
        finally
        {
            prepareLock?.Dispose();
            Environment.SetEnvironmentVariable(PrepareModeEnv, null);
        }
    }

    /// <summary>Атомарная запись штампа готовности: JSON {"stampMs"} через
    /// временный файл (уникальный НА ВЫЗОВ — конкурентные записи не разбирают
    /// tmp друг у друга) + File.Move(overwrite: true) в тот же каталог —
    /// конкурентная подготовка не оставляет полузаписанный штамп.</summary>
    internal static void WriteStampAtomic(string stampPath, long stampMs)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(stampPath)!);
        var tmp = $"{stampPath}.{Guid.NewGuid():N}.tmp";
        File.WriteAllText(tmp, $"{{\"stampMs\":{stampMs.ToString(CultureInfo.InvariantCulture)}}}");
        File.Move(tmp, stampPath, overwrite: true);
    }

    // Готовность статики для теста: штамп фазы подготовки существует и новее
    // всех исходников; ключевые образы — в docker. Иначе — fail-fast с командой
    // подготовки (не молчаливый прогон на устаревшем бинаре).
    private static async Task VerifyStaticReadyOrThrowAsync(CancellationToken ct)
    {
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var stampPath = Path.Combine(root, "artifacts", "e2e", ".prepared");
        if (!File.Exists(stampPath))
            throw new ApplicationException($"E2E statics are not prepared. {PrepareHint}");
        long stampMs;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(stampPath));
            stampMs = doc.RootElement.GetProperty("stampMs").GetInt64();
        }
        catch (Exception e) when (e is System.Text.Json.JsonException
            or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new ApplicationException($"the E2E preparation stamp is corrupted ({stampPath}). {PrepareHint}", e);
        }

        var stampUtc = DateTimeOffset.FromUnixTimeMilliseconds(stampMs).UtcDateTime;
        var newest = NewestSourceInputUtc(root);
        if (newest > stampUtc)
            throw new ApplicationException(
                $"sources are newer than the preparation (edit {newest:yyyy-MM-dd HH:mm:ss} UTC > stamp "
                + $"{stampUtc:yyyy-MM-dd HH:mm:ss} UTC) — a stale binary masks regressions. {PrepareHint}");

        _root = root;
        foreach (var image in new[] { WorkerImage, NodeImage })
            if (!await DockerImageExistsAsync(image, ct))
                throw new ApplicationException($"image {image} is missing in docker. {PrepareHint}");
    }

    // Самый свежий вход сборки: исходники src/ (.cs/.csproj/.props/.targets/
    // .json, без bin/obj), всё docker/ и корневые props/slnx.
    private static DateTime NewestSourceInputUtc(string root)
    {
        DateTime newest = DateTime.MinValue;
        var srcRoot = Path.Combine(root, "src");
        var tracked = new HashSet<string> { ".cs", ".csproj", ".props", ".targets", ".json" };
        foreach (var file in Directory.EnumerateFiles(srcRoot, "*", SearchOption.AllDirectories))
        {
            var segments = Path.GetRelativePath(srcRoot, file)
                .Split(Path.DirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj"))
                continue;
            if (!tracked.Contains(Path.GetExtension(file)))
                continue;
            var written = File.GetLastWriteTimeUtc(file);
            if (written > newest)
                newest = written;
        }

        foreach (var file in Directory.EnumerateFiles(Path.Combine(root, "docker"), "*", SearchOption.AllDirectories))
        {
            var written = File.GetLastWriteTimeUtc(file);
            if (written > newest)
                newest = written;
        }

        foreach (var file in Directory.EnumerateFiles(root))
        {
            var extension = Path.GetExtension(file);
            if (!tracked.Contains(extension) && extension != ".slnx")
                continue;
            var written = File.GetLastWriteTimeUtc(file);
            if (written > newest)
                newest = written;
        }

        return newest;
    }

    private static async Task<bool> DockerImageExistsAsync(string tag, CancellationToken ct)
    {
        try
        {
            await E2eFixture.RunProcessAsync("docker", ["image", "inspect", tag], ct);
            return true;
        }
        catch (ApplicationException)
        {
            return false;
        }
    }

    /// <summary>Подготовка статических ассетов процесса (канон AGENTS.md: сборка
    /// НА ХОСТЕ, в образы — только готовые артефакты; sdk-стадий и COPY
    /// исходников в E2E-образах нет): publish PgWorker.App → pgworker:e2e;
    /// docker build pgworker-node:e2e.
    /// СБОРКА — только фаза подготовки (PrepareStaticAssetsAsync); тест
    /// проверяет готовность (VerifyStaticReadyOrThrowAsync).
    /// PGW_TEST_E2E_NOBUILD=1 — бисект-режим: publish-шаги пропускаются,
    /// образы строятся из существующих каталогов артефактов.</summary>
    private static async Task EnsureStaticAsync(CancellationToken ct)
    {
        if (_staticReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_staticReady)
                return;

            _root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var noBuild = Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1";

            // Тест НЕ строит статику: сборка — отдельная фаза ДО тестов,
            // одним процессом. Тест лишь проверяет готовность.
            if (!PreparingPhase && !noBuild)
            {
                await VerifyStaticReadyOrThrowAsync(ct);
                _staticReady = true;
                return;
            }

            // publish PgWorker.App → образ SUT-воркера.
            if (noBuild)
                E2eFixture.EnsurePublishedDir(AppPublishDir,
                    "required: dotnet publish src/PgWorker.App -c Release -o artifacts/e2e/app");
            else
                await E2eFixture.PublishProjectAsync(
                    Path.Combine(_root, "src", "PgWorker.App", "PgWorker.App.csproj"),
                    AppPublishDir, "app", ct);

            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {WorkerImage}: docker build (context artifacts/e2e/app)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/PgWorker.E2E.Dockerfile", "-t", WorkerImage, AppPublishDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: E2eFixture.StaticProcessLogPath("worker-e2e"));
            E2eFixture.StaticPhase($"e2e-image {WorkerImage}: ready in {sw.Elapsed.TotalSeconds:F0} s");

            // Образ узла (задача 25): двухшаговая сборка (t25, arch/19 §3) —
            // (1) publish приёмника WAL и NodeTools self-contained НА ХОСТЕ
            // (RID хоста, без trimming) → artifacts/e2e/node-wal|node-tools;
            // (2) docker build ноды с publish-аргументами (в образ — только
            // publish-вывод, канон «publish на хосте, в контейнер — артефакты»).
            var nodeRid = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture
                == System.Runtime.InteropServices.Architecture.Arm64 ? "linux-arm64" : "linux-x64";
            var walDir = Path.Combine(_root, "artifacts", "e2e", "node-wal");
            var toolsDir = Path.Combine(_root, "artifacts", "e2e", "node-tools");
            sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {NodeImage}: dotnet publish WalReceiver self-contained {nodeRid} (host)…");
            // NoMsBuildReuseEnv + бюджет: build-server/ноды держат stdout-пайп
            // после завершения publish (e2e-факт: 11 мин тишины) — без реюза
            // процесс умирает вместе с пайпом; зависание = fail-fast за бюджет.
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj",
                "-c", "Release", "-r", nodeRid, "--self-contained", "-o", walDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: E2eFixture.StaticProcessLogPath("node-wal-publish"));
            E2eFixture.StaticPhase($"e2e-image {NodeImage}: dotnet publish NodeTools self-contained {nodeRid} (host)…");
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/PgWorker.NodeTools/PgWorker.NodeTools.csproj",
                "-c", "Release", "-r", nodeRid, "--self-contained", "-o", toolsDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: E2eFixture.StaticProcessLogPath("node-tools-publish"));
            E2eFixture.StaticPhase($"e2e-image {NodeImage}: publish ready in {sw.Elapsed.TotalSeconds:F0} s — docker build (context root)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-q", "-f", $"{_root}/docker/node/Dockerfile",
                "--build-arg", "WAL_PUBLISH_DIR=artifacts/e2e/node-wal",
                "--build-arg", "NODE_TOOLS_PUBLISH_DIR=artifacts/e2e/node-tools", "-t", NodeImage, _root,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: E2eFixture.StaticProcessLogPath("node-e2e"));
            E2eFixture.StaticPhase($"e2e-image {NodeImage}: ready in {sw.Elapsed.TotalSeconds:F0} s (walreceiver and pgwnodetool inside)");

            _staticReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }

    // Образ ownS3 — ВНЕШНИЙ статический ассет (репозиторий ../OwnS3, сборка —
    // deploy/build-image.sh там): pg его не собирает; freshness контролирует
    // разработчик пересборкой в ../OwnS3 (устаревший образ маскирует
    // регрессии; урок t09).
    private static async Task EnsureOwnS3ImageAsync(CancellationToken ct)
    {
        // owns3:dev — внешний ассет, его наличие обеспечивает ../OwnS3.
        if (_owns3ImageReady)
            return;
        await StaticGate.WaitAsync(ct);
        try
        {
            if (_owns3ImageReady)
                return;
            if (!await DockerImageExistsAsync(OwnS3Image, ct))
                throw new ApplicationException(
                    "image owns3:dev is missing in docker — build it in ../OwnS3: bash ../OwnS3/deploy/build-image.sh");
            _owns3ImageReady = true;
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
            // Тест не строит образ: сборка — фаза подготовки (один процесс).
            if (!PreparingPhase && Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") != "1")
            {
                if (!await DockerImageExistsAsync(SdImage, ct))
                    throw new ApplicationException($"image {SdImage} is missing in docker. {PrepareHint}");
                _sdImageReady = true;
                return;
            }
            var outDir = Path.Combine(_root, "artifacts", "e2e", "sdgenerator");
            var sw = E2eFixture.StaticSw();
            E2eFixture.StaticPhase($"e2e-image {SdImage}: dotnet publish SdGenerator (host)…");
            // NoMsBuildReuseEnv + бюджет — канон publish-фаз (пайп stdout).
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{_root}/src/Metrics.SdGenerator/Metrics.SdGenerator.csproj",
                "-c", "Release", "-o", outDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: E2eFixture.StaticProcessLogPath("sd-e2e-publish"));
            E2eFixture.StaticPhase($"e2e-image {SdImage}: publish ready in {sw.Elapsed.TotalSeconds:F0} s — docker build (context artifacts/e2e/sdgenerator)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-f", $"{_root}/docker/Metrics.SdGenerator.Dockerfile", "-t", SdImage, outDir,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: E2eFixture.StaticProcessLogPath("sd-e2e-build"));
            E2eFixture.StaticPhase($"e2e-image {SdImage}: ready in {sw.Elapsed.TotalSeconds:F0} s (publish+build)");
            _sdImageReady = true;
        }
        finally
        {
            StaticGate.Release();
        }
    }
}
