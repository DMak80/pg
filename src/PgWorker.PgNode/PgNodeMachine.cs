using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.PgNode;

/// <summary>Результат команды доставки (эндпоинт маппит: Started/Nop → 200,
/// Invalid → 400, EtcdError → 503).</summary>
public enum HandshakeResult { Started, Nop, Invalid, EtcdError }

/// <summary>Результат команды применения (200 c applied_revision / NotReady →
/// 409 / EtcdError → 503).</summary>
public sealed record ApplyResult(bool Ok, long? AppliedRevision, bool NotReady, bool EtcdError);

/// <summary>Статусная машина агента PgNode (arch/14 §3.3/§4, spec §3.6):
/// публикация not_initialized после чтения bootstrap-ключа; хендшейк —
/// валидация аргументов + атомарный txn-переход в initializing; фазы — старт
/// компонентов ≤3 попытки (параллельно) и ожидание SELECT 1 без бюджета
/// (is_tool_start — только рендер, components=prepared); применение на ready —
/// перерендер + точечные рестарты. Секреты — только в памяти процесса.</summary>
public sealed class PgNodeMachine : IDisposable
{
    private readonly PgNodeIdentity _identity;
    private readonly IEtcdGateway _etcd;
    private readonly string[] _endpoints;
    private readonly ISupervisorCommands _supervisor;
    private readonly IPatroniApplyCommands _patroniApply;
    private readonly string _doormanTemplate;
    private readonly TimeProvider _clock;

    // Ключи etcd: bootstrap (пишет воркер, читает агент) и статусный (пишет
    // только агент lease-сессией, arch/14 §3.2/§3.3).
    private readonly string _bootstrapKey;
    private readonly string _statusKey;

    // Хендшейк и применение сериализованы одним семафором с немедленным
    // отказом: параллельная команда — Handshake→Nop / Apply→NotReady
    // (спека §3.6-2/§3.6-4: повторный пинок безопасен).
    private readonly SemaphoreSlim _commandGate = new(1, 1);

    // Фоновый цикл чтения bootstrap-ключа (ретрай 5 c до первого успеха,
    // далее — освежение серта/данных для применения).
    private readonly CancellationTokenSource _loopCts = new();

    // Кеш bootstrap-данных (только после валидации IsValidForStart) + ревизия
    // ключа; серт листенера :8081 — volatile-публикация для Kestrel-селектора.
    private readonly object _dataGate = new();
    private NodeBootstrapData? _bootstrap;
    private ulong _bootstrapModRevision;
    private volatile X509Certificate2? _serverCertificate;

    // MR-#22: замена серверного серта освобождает предыдущий экземпляр
    // (rest-ротация перерендеривает ключ → новая пара P/512-объектов;
    // без Dispose — утечка неуправляемых ключей на каждом apply).
    private void SetServerCertificate(string certPem, string keyPem)
    {
        var previous = Interlocked.Exchange(ref _serverCertificate,
            X509Certificate2.CreateFromPem(certPem, keyPem));
        previous?.Dispose();
    }

    // Машина состояний в памяти процесса: секреты и применённый материал
    // переживают только жизнь процесса (рестарт контейнера — lease гасит
    // статус, хендшейк заново, spec §3.6-5). Применённый материал — ПО
    // МАТЕРИАЛУ (arch/14 §5 I, spec §3.6-4): YAML и серт применяются
    // раздельно (reload против рестарта) — и трекаются раздельно.
    private string _state = PgNodeStatus.NotInitialized;
    private long? _appliedRevision;
    private string? _appliedSpiloYaml;
    private string? _appliedCertMaterial;
    private string? _appliedDoormanConfig;
    private string? _suPassword;
    private string? _standbyPassword;
    private bool _isToolStart;

    // Писатель статусного ключа (прод — NodeStatusSession; тесты — фейк).
    private readonly Func<INodeStatusWriter>? _statusWriterFactory;
    private INodeStatusWriter? _status;

    // Юнит-seam: фейк пробы SELECT 1 (тесты машины; прод — ComponentProbes).
    internal Func<string, Task<bool>>? ProbeOverride;

    // Юнит-seam: корень рендера (тесты ошибки рендера; прод — /run/pgw).
    internal string? RenderRootDirOverride;

    // Юнит-seam: причина последнего отказа чтения bootstrap-ключа (диагностика).
    internal string? LastBootstrapReject;

    // Юнит-seam: смена SuPassword для кейса apply «изменился только doorman-
    // материал» (пара доставляется хендшейком — в тестах подставляется).
    internal void SetSuPasswordForTests(string suPassword) => _suPassword = suPassword;

    // Юнит-seam: фоновая задача активной фазы initializing (детерминизм тестов).
    internal Task? PhaseTask => _phaseTask;
    private Task? _phaseTask;

    // Фоновая задача применения (apply: рестарты компонентов под токеном
    // процесса); живая задача — признак «применение в ходу» (повторный
    // apply — ok без двойного рестарта).
    private volatile Task? _applyTask;

    // Анти-дребезг повторного применения ТОЙ ЖЕ ревизии после неудавшегося
    // рестарта: рестарт spilo на реплике с восстановлением PG длиннее
    // supervisord-startsecs даёт цикл stop/start, каждый виток которого убивает
    // поднимающийся PG (факт E2E: ротация rest-пары, нода пересоздавалась
    // надзором). Коулдаун даёт компонентам доездить — окно ротации честно
    // дозревает тиками (applied_revision не фиксирован, R3 повторяется по
    // канону arch/14 §5 I; значение — интервал анти-дребезга, не бюджет фаз).
    // Мутируемый internal static — юнит-seam (прецедент FetchWalCommand.
    // TransportAttempts): 0 выключает коулдаун (канон-тест немедленного
    // повтора MR-#12).
    internal static int ApplyRetryCooldownSec = 45;
    private long? _failedApplyRevision;
    private long _failedApplyUnix;

    // Юнит-seam: фоновая задача применения (тесты ждут её перед ассертами
    // рестартов — детерминизм без sleep-поллинга).
    internal Task? ApplyTask => _applyTask;

    /// <summary>Серверный серт mTLS-листенера :8081 — из bootstrap-ключа;
    /// null до чтения ключа (handshake отклоняется: канала нет).</summary>
    public X509Certificate2? ServerCertificate => _serverCertificate;

    /// <summary>Ревизия применённого bootstrap-ключа (null до применения).</summary>
    public long? AppliedRevision => _appliedRevision;

    /// <summary>Текущее состояние машины (для диагностики/тестов).</summary>
    public string State => _state;

    public PgNodeMachine(PgNodeIdentity identity, IEtcdGateway etcd, string[] endpoints,
        ISupervisorCommands supervisor, string doormanTemplate, TimeProvider? clock = null,
        IPatroniApplyCommands? patroniApply = null)
        : this(identity, etcd, endpoints, supervisor, doormanTemplate,
            statusWriter: null, clock, patroniApply)
    {
    }

    // Internal-конструктор с фейком писателя статуса (юнит-тесты машины:
    // сессия lease не гоняется — переходы эмулируются словарём etcd-фейка).
    internal PgNodeMachine(PgNodeIdentity identity, IEtcdGateway etcd, string[] endpoints,
        ISupervisorCommands supervisor, string doormanTemplate,
        Func<INodeStatusWriter>? statusWriter, TimeProvider? clock = null,
        IPatroniApplyCommands? patroniApply = null)
    {
        _identity = identity;
        _etcd = etcd;
        _endpoints = endpoints;
        _supervisor = supervisor;
        _doormanTemplate = doormanTemplate;
        _statusWriterFactory = statusWriter;
        _clock = clock ?? TimeProvider.System;
        _patroniApply = patroniApply ?? new PatroniApplyCommands(supervisor);
        _bootstrapKey = $"/clusters/{identity.Cluster}/shards/{identity.Shard}/nodes/{identity.Node}/bootstrap";
        _statusKey = $"/pgworker/nodes/{identity.Cluster}/{identity.Shard}/{identity.Node}/status";
    }

    /// <summary>Фоновый цикл: чтение bootstrap-ключа (ретрай 5 c) → валидация →
    /// серт листенера → публикация not_initialized (lease-сессия живёт до конца
    /// процесса). Повторные чтения — освежение серта/данных для применения.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _loopCts.Token);
        while (!linked.IsCancellationRequested)
        {
            await PollBootstrapOnceAsync(linked.Token);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), _clock, linked.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    // Одна итерация чтения bootstrap-ключа (+первичная публикация
    // not_initialized) — юнит-seam для детерминизма тестов (без ретрай-цикла).
    // Переходный флаг публикации ключа (образец PatroniRoleProbe._lastCaState):
    // диагностика — ТОЛЬКО при смене состояния (штатный poll каждые 5 c молчит,
    // иначе десятки тысяч строк/сутки — прецедент предупреждает прямо).
    private bool _bootstrapLogged;

    internal async Task PollBootstrapOnceAsync(CancellationToken ct)
    {
        var bootstrap = await ReadBootstrapAsync(ct);
        if (bootstrap is null)
        {
            if (_bootstrapLogged)
                Console.Error.WriteLine("pgnode: bootstrap key not ready yet (retry in 5 s)");
            _bootstrapLogged = false;
        }
        if (bootstrap is { Data: { } data, ModRevision: { } mod })
        {
            if (!_bootstrapLogged)
                Console.Error.WriteLine(
                    $"pgnode: bootstrap key read (mod={mod}) — publishing not_initialized");
            _bootstrapLogged = true;
            lock (_dataGate)
            {
                _bootstrap = data;
                _bootstrapModRevision = mod;
            }
            // Серт листенера :8081 из bootstrap-ключа (volatile-публикация).
            SetServerCertificate(data.CertPem, data.KeyPem);

            if (_status is null)
            {
                // Первое успешное чтение: статус-сессия + публикация
                // not_initialized (put на своём lease затирает протухший
                // статус прошлого процесса, spec §3.6-5). Неудача — ретрай
                // циклом (etcd-сбой недоступности put'а).
                var writer = _statusWriterFactory is { } factory
                    ? factory()
                    : new NodeStatusSession(_etcd, _endpoints, _statusKey);
                if (await writer.StartAsync(NotInitializedStatus(), ct))
                {
                    Console.Error.WriteLine("pgnode: not_initialized published (status session up)");
                    _status = writer;
                }
                else
                {
                    Console.Error.WriteLine("pgnode: status session start failed — retry");
                }
            }
        }
    }

    /// <summary>Команда доставки (POST /api/bootstrap): валидация аргументов
    /// (400) → статус not_initialized? → атомарный txn в initializing; не
    /// not_initialized — Nop («пинок состоялся», 200 без действий).</summary>
    public async Task<HandshakeResult> HandshakeAsync(
        string suPassword, string standbyPassword, bool isToolStart, CancellationToken ct)
    {
        // Валидация аргументов ДО статуса: неполные пароли/невалидный ключ —
        // Invalid (400), статус остаётся not_initialized (воркер ретраит тиком).
        NodeBootstrapData? bootstrap;
        ulong modRevision;
        lock (_dataGate)
        {
            bootstrap = _bootstrap;
            modRevision = _bootstrapModRevision;
        }
        if (string.IsNullOrEmpty(suPassword) || string.IsNullOrEmpty(standbyPassword)
            || bootstrap is not { } valid || !valid.IsValidForStart())
        {
            Console.Error.WriteLine(
                $"pgnode: handshake invalid (bootstrap cached: {bootstrap is not null})");
            return HandshakeResult.Invalid;
        }

        if (!await _commandGate.WaitAsync(0, ct))
            return HandshakeResult.Nop; // фазы в ходу — повторная команда ок-пинок
        try
        {
            // MR-#11: гвард по ТЕКУЩЕМУ состоянию машины — повторный handshake
            // при initializing/ready → Nop без второго запуска фаз (etcd-ключ
            // не читается — txn-переход не повторяется).
            if (_state != PgNodeStatus.NotInitialized)
            {
                Console.Error.WriteLine("pgnode: handshake nop (machine is not not_initialized)");
                return HandshakeResult.Nop;
            }

            var status = _status;
            if (status is null)
                return HandshakeResult.EtcdError; // сессии нет — etcd-сторона не готова

            // Прочитать текущее значение статусного ключа: compare по VALUE
            // («state==not_initialized») — устойчив к keepalive-put'ам сессии
            // того же значения (mod_revision меняется — значение нет, §8.16).
            var current = await EtcdEndpointFailover.CallAsync(
                _endpoints, (endpoint, token) => _etcd.GetAsync(endpoint, _statusKey, token), ct);
            if (!current.IsSuccess)
                return HandshakeResult.EtcdError;
            var currentJson = current.Value?.Value;
            if (currentJson is null)
            {
                Console.Error.WriteLine("pgnode: handshake: status key is absent yet");
                return HandshakeResult.EtcdError; // ключа нет — сессия ещё не опубликовала
            }
            if (!IsNotInitialized(currentJson))
            {
                Console.Error.WriteLine("pgnode: handshake nop (state is not not_initialized)");
                return HandshakeResult.Nop; // «пинок состоялся» — без действий
            }

            // Сохранить пароли ТОЛЬКО при выигранном переходе (секреты — память).
            var initializing = new PgNodeStatus(PgNodeStatus.Initializing, null,
                new Dictionary<string, string>(), UpdatedUnix: NowUnix()).ToJson();
            var transitioned = await status.TryTransitionAsync(initializing, currentJson, ct);
            if (!transitioned)
            {
                Console.Error.WriteLine("pgnode: handshake txn lost (parallel writer)");
                return HandshakeResult.Nop; // параллельный писатель изменил значение
            }
            Console.Error.WriteLine("pgnode: handshake accepted — starting initializing phases");

            _suPassword = suPassword;
            _standbyPassword = standbyPassword;
            _isToolStart = isToolStart;
            _state = PgNodeStatus.Initializing;
            _phaseTask = Task.Run(() => RunInitializingAsync(valid, modRevision, _loopCts.Token),
                _loopCts.Token);
            return HandshakeResult.Started;
        }
        finally
        {
            _commandGate.Release();
        }
    }

    /// <summary>Команда применения (POST /api/bootstrap/apply): только ready;
    /// перечитать ключ, ревизия изменилась — перерендер (синхронно) + точечные
    /// рестарты (ФОНОВО, под токеном процесса — не запроса). Прогресс воркер
    /// видит по статусному ключу (applied_revision, arch/14 §5 I): рестарт
    /// spilo занимает десятки секунд — дольше HttpClient-таймаута канала
    /// (10 c, spec §2.7), синхронное применение рвало каждую команду по
    /// таймауту и зацикливало окно ротации на одной ноде.</summary>
    public async Task<ApplyResult> ApplyAsync(CancellationToken ct)
    {
        if (_state != PgNodeStatus.Ready)
            return new ApplyResult(Ok: false, AppliedRevision: null, NotReady: true, EtcdError: false);
        // Применение уже в ходу (фоновые рестарты) — команда принята: рендер
        // применён, рестарты/публикация завершатся фоново; повтор — без
        // двойного рестарта (spec §3.6-4: повторный пинок — ok без действий).
        if (_applyTask is { IsCompleted: false })
            return new ApplyResult(Ok: true, AppliedRevision: _appliedRevision,
                NotReady: false, EtcdError: false);
        if (!await _commandGate.WaitAsync(0, ct))
            return new ApplyResult(Ok: true, AppliedRevision: _appliedRevision,
                NotReady: false, EtcdError: false); // гонка с рукопожатием — пинок
        try
        {
            var fresh = await ReadBootstrapAsync(ct);
            if (fresh is null || fresh.Data is not { } data || fresh.ModRevision is not { } mod)
                return new ApplyResult(Ok: false, AppliedRevision: null, NotReady: false, EtcdError: true);

            // Обновить кеш/серт (замена серта триггерит оба рестарта — серт
            // входит в оба материала, spec §6.4).
            lock (_dataGate)
            {
                _bootstrap = data;
                _bootstrapModRevision = mod;
            }
            SetServerCertificate(data.CertPem, data.KeyPem);

            if (mod == (ulong)(_appliedRevision ?? -1))
                return new ApplyResult(Ok: true, AppliedRevision: _appliedRevision,
                    NotReady: false, EtcdError: false); // ревизия без изменений — no-op

            // Повтор применения той же ревизии сразу после неудавшегося рестарта —
            // ok-пинок без рестарта (коулдаун анти-дребезга, см. поле).
            if (_failedApplyRevision == (long)mod
                && DateTimeOffset.UtcNow.ToUnixTimeSeconds() - _failedApplyUnix < ApplyRetryCooldownSec)
                return new ApplyResult(Ok: true, AppliedRevision: _appliedRevision,
                    NotReady: false, EtcdError: false);

            // Рендер tmpfs заново (перерендер идемпотентен, 0600).
            var doormanConfig = RenderedDoormanConfig(data);
            try
            {
                BootstrapRenderer.Render(data, _suPassword ?? "", _standbyPassword ?? "",
                    _doormanTemplate, RenderRootDirOverride);
            }
            catch (Exception e)
            {
                // Ошибка рендера на ready-ноде — start_failed с диагностикой
                // (значения секретов в текст не включаются).
                await PublishAsync(PgNodeStatus.StartFailed, mod,
                    Components(pg: "stopped", doorman: "stopped"), FirstLine(e.Message), ct);
                _state = PgNodeStatus.StartFailed;
                return new ApplyResult(Ok: false, AppliedRevision: null, NotReady: false, EtcdError: false);
            }

            // Применение ПО МАТЕРИАЛУ (arch/14 §5 I, spec §3.6-4, решение 29):
            // rest-пара (YAML) и серт трекаются и применяются раздельно —
            // пароль reload'ом, серт рестартом программы (только реплика).
            var restConfigChanged = _appliedSpiloYaml != data.SpiloConfiguration;
            var certChanged = _appliedCertMaterial != CertMaterial(data);
            var doormanChanged = _appliedDoormanConfig != DoormanMaterialOf(doormanConfig, data);

            // Применение — фоновая задача ПРОЦЕССА (RequestAborted запроса умирает
            // по таймауту клиента и рвал применение посреди); машина остаётся ready,
            // факт применения — публикация статусного ключа (arch/14 §5 I).
            _applyTask = Task.Run(
                () => ApplyMaterialsAsync(data, mod, doormanConfig,
                    restConfigChanged, certChanged, doormanChanged),
                _loopCts.Token);
            return new ApplyResult(Ok: true, AppliedRevision: (long)mod,
                NotReady: false, EtcdError: false);
        }
        finally
        {
            _commandGate.Release();
        }
    }

    // Фоновая часть применения ПО МАТЕРИАЛУ (arch/14 §5 I, spec §3.6-4,
    // решение 29; план П11.9): порядок — сначала пулер (рестарты пулера
    // допустимы всегда), затем spilo-материал. rest-ПАРОЛЬ — reload Patroni
    // (PG не останавливается; рестарт Patroni-процесса/контейнера для пароля
    // запрещён); REST-СЕРТ — рестарт Patroni-программы ТОЛЬКО на реплике
    // (рестарт покрывает оба материала — reload не выполняется); на мастере
    // серт агентом НЕ применяется (мастера пересоздаёт воркер — R3-СЕРТ вне
    // тикета): applied_revision НЕ фиксируется — конвергенция повторит
    // применение после пересоздания; пароль при этом применяется reload'ом.
    // Сбой любой команды — error-факт, материалы НЕ фиксируются, ready со
    // СТАРОЙ ревизией, коулдаун (MR-#12 «полуприменённое не маскируется»).
    private async Task ApplyMaterialsAsync(
        NodeBootstrapData data, ulong mod, string doormanConfig,
        bool restConfigChanged, bool certChanged, bool doormanChanged)
    {
        var ct = _loopCts.Token;
        string? error = null;
        var certDeferredToWorker = false; // серт мастера — воркеру (R3-СЕРТ)
        try
        {
            if (doormanChanged)
                await _supervisor.RestartAsync("pg_doorman", ct);

            if (certChanged)
            {
                var role = await ProbeRoleAsync(ct);
                if (role == WalReceiverRole.Master)
                {
                    // Мастер сертом агентом не применяется: пересоздание
                    // контейнера воркером (failover на обновлённую реплику).
                    // Пароль ниже применяется reload'ом — он от роли не зависит.
                    certDeferredToWorker = true;
                }
                else
                {
                    // Реплика/переходное окно: рестарт Patroni-программы —
                    // покрывает И серт, И rest-пароль (reload не выполняется).
                    await _patroniApply.RestartProgramAsync(ct);
                }
            }

            if (restConfigChanged && !(certChanged && !certDeferredToWorker))
            {
                // Пароль — reload'ом (если серт уже применён рестартом
                // программы, reload избыточен — рестарт покрыл оба материала).
                await _patroniApply.ApplyRestPairAsync(
                    data.SpiloConfiguration,
                    RestPasswordOf(_appliedSpiloYaml) ?? RestPasswordOf(data.SpiloConfiguration) ?? "",
                    RestPasswordOf(data.SpiloConfiguration) ?? "",
                    ct);
            }
        }
        catch (OperationCanceledException)
        {
            return; // процесс гасится — публикация недостижима (lease умрёт сам)
        }
        catch (Exception e)
        {
            error = FirstLine(e.Message);
            // Факт неудавшегося применения — в анти-дребезг (повтор той же
            // ревизии отложен до истечения коулдауна).
            _failedApplyRevision = (long)mod;
            _failedApplyUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        var pgComponent = restConfigChanged || (certChanged && !certDeferredToWorker)
            ? "starting"
            : "ready";

        if (error is null && !certDeferredToWorker)
        {
            // Успех: применённые материалы зафиксированы, applied — новая ревизия.
            _appliedSpiloYaml = data.SpiloConfiguration;
            _appliedCertMaterial = CertMaterial(data);
            _appliedDoormanConfig = DoormanMaterialOf(doormanConfig, data);
            _appliedRevision = (long)mod;
            _failedApplyRevision = null;
            await PublishAsync(PgNodeStatus.Ready, mod,
                Components(pg: pgComponent,
                    doorman: doormanChanged ? "starting" : "ready"),
                null, ct);
            return;
        }

        // Публикация со СТАРОЙ ревизией при сбое команды: «полуприменённое»
        // не маскируется (MR-#12) — ротатор/доставка честно повторяют Apply,
        // факт — в error. Случай «серт мастера отложен воркеру (R3-СЕРТ)» —
        // НЕ ошибка, а ожидаемое состояние (пересоздание мастера — оркестрация
        // воркера вне тикета): ready со СТАРОЙ ревизией БЕЗ error; применённые
        // материалы не фиксируются — конвергенция повторит применение после
        // пересоздания (пароль при этом уже применён reload'ом и повтор
        // идемпотентен по состояниям S1/S2).
        await PublishAsync(PgNodeStatus.Ready,
            _appliedRevision is { } appliedOld ? (ulong)appliedOld : null,
            Components(pg: pgComponent,
                doorman: doormanChanged ? "starting" : "ready"),
            error, ct);
    }

    // Роль ноды для серт-ветки: прод — PatroniRoleProbe (CA из env
    // SSL_RESTAPI_CA); юнит-seam — RoleProbeOverride.
    internal Func<CancellationToken, Task<WalReceiverRole>>? RoleProbeOverride;

    private Task<WalReceiverRole> ProbeRoleAsync(CancellationToken ct)
        => RoleProbeOverride is { } probe
            ? probe(ct)
            : PatroniRoleProbe.ProbeAsync(
                Environment.GetEnvironmentVariable("SSL_RESTAPI_CA") ?? "", ct);

    // restapi-пароль из применяемого/применённого YAML (restapi.authentication.
    // password; кавычки значений срезаются). null — секция не найдена
    // (распарсить не удалось — reload-пара недоступна, команда доложит).
    internal static string? RestPasswordOf(string? yaml)
    {
        if (string.IsNullOrEmpty(yaml))
            return null;
        var inRest = false;
        var inAuth = false;
        foreach (var raw in yaml.Split('\n'))
        {
            if (string.IsNullOrWhiteSpace(raw))
                continue;
            var indent = raw.Length - raw.TrimStart().Length;
            var line = raw.Trim();
            if (indent == 0)
            {
                inRest = line.StartsWith("restapi:", StringComparison.Ordinal);
                inAuth = false;
                continue;
            }
            if (!inRest)
                continue;
            if (line.StartsWith("authentication:", StringComparison.Ordinal))
            {
                inAuth = true;
                continue;
            }
            if (inAuth && line.StartsWith("password:", StringComparison.Ordinal))
                return line["password:".Length..].Trim().Trim('"', '\'');
        }
        return null;
    }

    // ── Фазы initializing (spec §3.6-3) ──

    // Фазы — фон под токеном процесса: tool-путь (только рендер → ready
    // prepared) либо штатный (рендер → параллельный старт ≤3 попытки/компонент
    // → пробы SELECT 1 без бюджета → ready; первый failed → start_failed).
    private async Task RunInitializingAsync(NodeBootstrapData bootstrap, ulong modRevision, CancellationToken ct)
    {
        // Фаза (а), шаг 1 — рендер tmpfs (ошибка → start_failed, компоненты
        // не запускаются).
        Console.Error.WriteLine($"pgnode: initializing: rendering tmpfs (mod={modRevision})");
        var doormanConfig = RenderedDoormanConfig(bootstrap);
        try
        {
            BootstrapRenderer.Render(bootstrap, _suPassword ?? "", _standbyPassword ?? "",
                _doormanTemplate, RenderRootDirOverride);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"pgnode: render failed: {FirstLine(e.Message)}");
            await PublishAsync(PgNodeStatus.StartFailed, modRevision,
                Components(pg: "stopped", doorman: "stopped"), FirstLine(e.Message), ct);
            _state = PgNodeStatus.StartFailed;
            return;
        }

        Console.Error.WriteLine("pgnode: tmpfs rendered — starting components");
        if (_isToolStart)
        {
            // tool-контур: терминальный успех — ПОДГОТОВЛЕННОСТЬ ПАРАМЕТРОВ
            // (spec §3.6-3а is_tool_start): компоненты не запускаются, фаза
            // (б) select 1 не выполняется (зона tool-команды).
            _appliedSpiloYaml = bootstrap.SpiloConfiguration;
            _appliedCertMaterial = CertMaterial(bootstrap);
            _appliedDoormanConfig = DoormanMaterialOf(doormanConfig, bootstrap);
            _appliedRevision = (long)modRevision;
            await PublishAsync(PgNodeStatus.Ready, modRevision,
                Components(pg: "prepared", doorman: "prepared"), null, ct);
            _state = PgNodeStatus.Ready;
            return;
        }

        // Публикация стартующих компонентов (факт фазы (а)).
        await PublishAsync(PgNodeStatus.Initializing, null,
            Components(pg: "starting", doorman: "starting"), null, ct);

        // Старт компонентов ПАРАЛЛЕЛЬНО, надзор старта ≤3 попытки/компонент;
        // сбой одного НЕ блокирует другой (spec §3.6-3а, §8.18).
        var pgTask = StartComponentAsync("spilo", ct);
        var doormanTask = StartComponentAsync("pg_doorman", ct);
        var pgOk = await pgTask;
        var doormanOk = await doormanTask;
        var components = Components(
            pg: pgOk ? "starting" : "failed",
            doorman: doormanOk ? "starting" : "failed");
        if (!pgOk || !doormanOk)
        {
            // Исчерпание попыток фазы (а) — start_failed (машина терминальна,
            // второй компонент не отменяется — spec §8.18).
            _appliedSpiloYaml = bootstrap.SpiloConfiguration;
            _appliedCertMaterial = CertMaterial(bootstrap);
            _appliedDoormanConfig = DoormanMaterialOf(doormanConfig, bootstrap);
            var failed = !pgOk ? "spilo" : "pg_doorman";
            await PublishAsync(PgNodeStatus.StartFailed, modRevision, components,
                $"{failed}: start failed after 3 attempts", ct);
            _state = PgNodeStatus.StartFailed;
            return;
        }

        // Фаза (б): ожидание готовности SELECT 1 — БЕЗ бюджета (живой процесс
        // может инициализироваться долго, восстановление из бэкапа).
        _appliedSpiloYaml = bootstrap.SpiloConfiguration;
        _appliedCertMaterial = CertMaterial(bootstrap);
        _appliedDoormanConfig = DoormanMaterialOf(doormanConfig, bootstrap);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), _clock, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            Func<string, Task<bool>> probe = ProbeOverride
                ?? (cs => ComponentProbes.ProbeAsync(cs, ct));
            var pgReady = await probe(
                ComponentProbes.PgConnectionString(_suPassword ?? ""));

            // app-креды/dbname — ленивое чтение per-cluster ключей (кеш на
            // процесс); ключей нет → doorman-проба не выполняется (компонент
            // остаётся starting), повтор в следующем цикле (канон приёмника).
            var appCreds = await ReadAppCredentialsAsync(ct);
            var doormanReady = false;
            if (appCreds is { } app)
                doormanReady = await probe(
                    ComponentProbes.DoormanConnectionString(app.User, app.Password, app.DbName));

            if (!pgReady || !doormanReady)
            {
                // Ещё инициализируется (без бюджета) — повтор в следующем цикле.
                await PublishAsync(PgNodeStatus.Initializing, null,
                    Components(pg: pgReady ? "ready" : "starting",
                        doorman: doormanReady ? "ready" : "starting"),
                    null, ct);
                continue;
            }

            // Готовы оба (SELECT 1 ok) — ready (applied_revision = mod_revision
            // применённого ключа).
            _appliedRevision = (long)modRevision;
            await PublishAsync(PgNodeStatus.Ready, modRevision,
                Components(pg: "ready", doorman: "ready"), null, ct);
            _state = PgNodeStatus.Ready;
            return;
        }
    }

    // Одна попытка-серия старта компонента: до 3 вызовов StartAsync (исключение
    // провал старта ненулевым кодом supervisorctl — попытка №+1; исчерпаны —
    // false). Семантика supervisord: start блокирует до startsecs — один вызов
    // = одна попытка (spec §3.6-3а, §8.18/§8.21).
    private async Task<bool> StartComponentAsync(string program, CancellationToken ct)
    {
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await _supervisor.StartAsync(program, ct);
                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                // Провал попытки — перезапуск (до 3); значения в текст не идут.
                Console.Error.WriteLine(
                    $"pgnode: start {program} attempt {attempt} failed: {FirstLine(e.Message)}");
            }
        }
        return false;
    }

    // ── Чтение etcd (EtcdEndpointFailover) ──

    private sealed record BootstrapSnapshot(NodeBootstrapData? Data, ulong? ModRevision);

    private async Task<BootstrapSnapshot?> ReadBootstrapAsync(CancellationToken ct)
    {
        var read = await EtcdEndpointFailover.CallAsync(
            _endpoints, (endpoint, token) => _etcd.GetAsync(endpoint, _bootstrapKey, token), ct);
        if (!read.IsSuccess)
        {
            LastBootstrapReject = "read-failed";
            return null; // etcd-сбой — ретрай циклом
        }
        var kv = read.Value;
        if (kv is null)
        {
            LastBootstrapReject = "kv-null";
            return null; // ключа ещё нет (воркер не доставил) — ретрай циклом
        }
        var data = NodeBootstrapData.Parse(kv.Value);
        if (data is null)
        {
            LastBootstrapReject = "parse-null";
            return null; // битый/чужой ключ — ретрай циклом (воркер перезапишет)
        }
        if (!data.IsValidForStart())
        {
            LastBootstrapReject = "invalid: " + NodeBootstrapData.LastValidityError
                + " cert-head=[" + data.CertPem[..Math.Min(30, data.CertPem.Length)].Replace("\n", "|") + "]"
                + " cert-tail=[" + data.CertPem[^Math.Min(20, data.CertPem.Length)..].Replace("\n", "|") + "]";
            return null; // серт/YAML не проходят валидацию — ретрай циклом
        }
        LastBootstrapReject = null;
        return new BootstrapSnapshot(data, kv.ModRevision);
    }

    private sealed record AppProbeCreds(string User, string Password, string DbName);

    // Ленивое чтение per-cluster app-кредов + dbname (кеш на процесс; null —
    // ключей нет/ошибка: doorman-проба пропускается до следующего цикла).
    private AppProbeCreds? _appCreds;
    private async Task<AppProbeCreds?> ReadAppCredentialsAsync(CancellationToken ct)
    {
        if (_appCreds is { } cached)
            return cached;
        var user = await GetAsync($"/clusters/{_identity.Cluster}/app_user", ct);
        if (user?.Value is not { Length: > 0 } u)
            return null;
        var password = await GetAsync($"/clusters/{_identity.Cluster}/app_password", ct);
        if (password?.Value is not { Length: > 0 } p)
            return null;
        var config = await GetAsync($"/clusters/{_identity.Cluster}/config", ct);
        string dbName = "";
        if (config?.Value is { Length: > 0 } raw)
        {
            try
            {
                using var doc = JsonDocument.Parse(raw);
                dbName = doc.RootElement.TryGetProperty("dbname", out var name)
                    && name.ValueKind == JsonValueKind.String
                    ? name.GetString() ?? "" : "";
            }
            catch (JsonException)
            {
                dbName = ""; // битый config — doorman-проба по пустой БД не идёт
            }
        }
        _appCreds = new AppProbeCreds(u, p, dbName);
        return _appCreds;
    }

    private async Task<Kv?> GetAsync(string key, CancellationToken ct)
    {
        var read = await EtcdEndpointFailover.CallAsync(
            _endpoints, (endpoint, token) => _etcd.GetAsync(endpoint, key, token), ct);
        return read.IsSuccess ? read.Value : null;
    }

    // ── Публикация статуса/материалы ──

    private async Task PublishAsync(string state, ulong? modRevision,
        IReadOnlyDictionary<string, string> components, string? error, CancellationToken ct)
    {
        var status = _status;
        if (status is null)
            return;
        long? applied = modRevision is { } rev ? (long)rev : null;
        status.Update(new PgNodeStatus(state, applied, components, NowUnix(), error));
        await status.FlushAsync(ct);
    }

    private PgNodeStatus NotInitializedStatus()
        => new(PgNodeStatus.NotInitialized, null, new Dictionary<string, string>(), NowUnix());

    private static IReadOnlyDictionary<string, string> Components(string pg, string doorman)
        => new Dictionary<string, string> { ["pg"] = pg, ["doorman"] = doorman };

    private string RenderedDoormanConfig(NodeBootstrapData bootstrap)
        => _doormanTemplate.Replace(
            BootstrapRenderer.SuPasswordPlaceholder, _suPassword ?? "", StringComparison.Ordinal);

    // Материал REST-серта (spec §3.6-4, П11.9): применяемость ПО МАТЕРИАЛУ —
    // серт трекается отдельно от YAML (пароль reload'ом, серт рестартом).
    private static string CertMaterial(NodeBootstrapData bootstrap)
        => bootstrap.CertPem + "\n" + bootstrap.KeyPem;

    private static string DoormanMaterialOf(string doormanConfig, NodeBootstrapData bootstrap)
        => doormanConfig + "\n" + bootstrap.CertPem + "\n" + bootstrap.KeyPem;

    private static bool IsNotInitialized(string statusJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(statusJson);
            return doc.RootElement.TryGetProperty("state", out var state)
                && state.ValueKind == JsonValueKind.String
                && state.GetString() == PgNodeStatus.NotInitialized;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string FirstLine(string text)
        => text.Split('\n')[0];

    private long NowUnix()
        => _clock.GetUtcNow().ToUnixTimeSeconds();

    public void Dispose()
    {
        _loopCts.Cancel();
        _loopCts.Dispose();
        _commandGate.Dispose();
        _status?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
