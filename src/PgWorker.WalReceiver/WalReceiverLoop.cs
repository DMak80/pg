using PgWorker.Backups;
using PgWorker.Etcd.S3;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver;

/// <summary>Главный цикл приёмника WAL (t25, arch/19 §3, spec §3.1) — hosted-сервис
/// WebApplication-хоста. Старт: идентичность из env (`WalReceiverIdentity`) —
/// невалидный набор → лог в stdout контейнера + exit 1 (supervisord с
/// `exitcodes=0` не перезапускает). Каждая итерация (~5 c при ожидании):
/// стоп-гейты etcd (disabled → `waiting_disabled`; wal-ключ STOPPED/WAL_CHAIN_ERROR →
/// `paused`; etcd недоступен → работа по последним известным гейтам, стрим не
/// рвём — слот держит WAL) → роль REST-пробой (`none` → `waiting_role`) →
/// резолв кредов/целей (`WalReceiverTargets`: waiting_password /
/// waiting_policy) → CA S3 → `PoolBackupS3` (write-список по роли, read — все
/// ноды) → стрим-сессия `WalReceiverCore.RunSessionAsync`. Статус-ключ
/// `/pgworker/backups/&lt;C&gt;/&lt;X&gt;/agent/&lt;N&gt;` — lease-сессия
/// (`WalReceiverStatusSession`, TTL 15 c / keepalive 5 c): пишется со
/// `starting`, при каждой смене состояния/фактов и не реже 5 c — вне hot-path
/// стрима. SIGTERM: graceful — отмена хоста закрывает сессию, lease revoke,
/// exit 0. Метрики — `WalReceiverMetrics` (Meter `PgWorker.WalReceiver`).</summary>
public sealed class WalReceiverLoop : BackgroundService
{
    private static readonly TimeSpan IdlePause = TimeSpan.FromSeconds(5);

    // Инъекции (прод — из env/DI-дефолта; тесты — фейки).
    private readonly Func<string, string?> _env;
    private readonly IEtcdGateway _etcd;
    private readonly string[] _endpoints;
    private readonly IS3PoolStore _pools;
    private readonly Func<CancellationToken, Task<WalReceiverRole>> _roleProbe;
    private readonly Func<WalReceiverOptions, IXLogReplicationSource> _sourceFactory;
    private readonly Func<WalReceiverOptions, ISlotPositionReader> _slotReaderFactory;
    private readonly Func<WalReceiverOptions, IBackupS3>? _s3Factory;
    private readonly TimeSpan _retryDelay;
    private readonly TimeSpan? _statusKeepalive;
    private readonly TimeSpan? _roleProbePeriod;
    private readonly string _pgWalDir;

    private WalReceiverIdentity? _identity;
    private WalReceiverMetrics? _metrics;
    private WalReceiverStatusSession? _status;

    /// <summary>Текущие живые факты (spec §7 №8/arch/19 §4: персистентны до
    /// события-снятия): единая IMMUTABLE-структура — публикации MERGE (атомарная
    /// замена ссылки), НЕ затирают другие факты значениями по умолчанию
    /// (tli_jump живёт до следующего прыжка; pool_exhausted снимается первым
    /// успешным put/исходом сессии; history_missing_tli — до следующего
    /// TLI-перехода с добытым файлом).</summary>
    private sealed record LiveFacts(
        WalReceiverTliJump? Jump, bool PoolExhausted,
        string? LastDelivered, long? LastDeliveredUnix, uint? HistoryMissingTli);

    private LiveFacts _facts = new(null, false, null, null, null);

    /// <summary>Прод: env процесса, etcd-gateway + пулы — из конфигурации цикла.</summary>
    public WalReceiverLoop()
        : this(
            Environment.GetEnvironmentVariable,
            new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) }),
            Environment.GetEnvironmentVariable("PGW_ETCD")?
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                ?? [],
            new S3PoolStore(
                new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) }),
                Environment.GetEnvironmentVariable("PGW_ETCD")?
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    ?? []),
            ct => PatroniRoleProbe.ProbeAsync(
                Environment.GetEnvironmentVariable("SSL_RESTAPI_CA") ?? "", ct),
            o => new NpgsqlXLogSource(o),
            o => new NpgsqlSlotPositionReader(o))
    {
    }

    /// <summary>Тестовый: все внешние зависимости — фейки, ретрай — быстрый.
    /// s3Factory — тестовый IBackupS3 (прод — null: PoolBackupS3 из списков пула).</summary>
    internal WalReceiverLoop(
        Func<string, string?> env,
        IEtcdGateway etcd,
        string[] endpoints,
        IS3PoolStore pools,
        Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
        Func<WalReceiverOptions, IXLogReplicationSource> sourceFactory,
        Func<WalReceiverOptions, ISlotPositionReader> slotReaderFactory,
        TimeSpan? retryDelay = null,
        Func<WalReceiverOptions, IBackupS3>? s3Factory = null,
        TimeSpan? statusKeepalive = null,
        string? pgWalDir = null,
        TimeSpan? roleProbePeriod = null)
    {
        _env = env;
        _etcd = etcd;
        _endpoints = endpoints;
        _pools = pools;
        _roleProbe = roleProbe;
        _sourceFactory = sourceFactory;
        _slotReaderFactory = slotReaderFactory;
        _retryDelay = retryDelay ?? IdlePause;
        _s3Factory = s3Factory;
        _statusKeepalive = statusKeepalive;
        _roleProbePeriod = roleProbePeriod;
        _pgWalDir = pgWalDir ?? WalReceiverCore.DefaultPgWalDir;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // 1. Идентичность: невалидный env — старт-отказ процесса (arch/19 §3):
        //    лог в stdout контейнера (диагностика) + ненулевой exit; supervisord
        //    с exitcodes=0 перезапускает только падения — лупинг env-дефекта
        //    исключён.
        var identity = WalReceiverIdentity.Parse(_env, out var errors);
        if (identity is null)
        {
            Console.Out.WriteLine(
                $"walreceiver: invalid identity env: {string.Join("; ", errors)} — exit 1");
            Environment.Exit(1);
        }

        _identity = identity;
        var agentKey = $"/pgworker/backups/{identity.Cluster}/{identity.Shard}/agent/{identity.Node}";

        // 2. Метрики приёмника (Meter с именем MeterName — регистрация в Program).
        using var meter = new System.Diagnostics.Metrics.Meter(WalReceiverMetrics.MeterName);
        var metrics = new WalReceiverMetrics(meter, identity.Cluster, identity.Shard, identity.Node);
        _metrics = metrics;

        // 3. Lease-сессия статус-ключа: создаётся сразу, StartAsync — в цикле
        //    (etcd может быть недоступен при старте — ретрай следующим тиком).
        var status = new WalReceiverStatusSession(_etcd, _endpoints, agentKey, _statusKeepalive);
        _status = status;

        // Последние известные гейты (etcd-моргание: стрим не рвётся — работаем
        // по последним известным до следующего успешного чтения).
        var lastGates = new WalReceiverGateState(Disabled: false, Paused: false, ChainError: false, Unknown: false);
        var currentRole = WalReceiverRole.None;
        var state = WalReceiverState.Starting;
        // Бэкофф ретраев failed_permanent-сессий (сек; 0 — сброшен, см. исходы).
        var permanentBackoffSec = 0.0;

        // CA https S3 (per-install, env ноды): материализация в temp — один раз
        // (как в старом Program; файл живёт до конца процесса).
        var s3CaBundlePath = (string?)null;
        if (identity.S3CaPem is { Length: > 0 } caPem)
        {
            s3CaBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
            // 0600 (UnixCreateMode; платформенный guard CA1416): в /tmp пишет
            // только владелец-процесс. Прод — Linux-контейнер ноды.
            var caOptions = OperatingSystem.IsWindows()
                ? new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None }
                : new FileStreamOptions
                {
                    Mode = FileMode.Create,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
                };
            await using (var caStream = new FileStream(s3CaBundlePath, caOptions))
            using (var writer = new StreamWriter(caStream))
            {
                await writer.WriteAsync(caPem + "\n");
            }
        }

        // Первичная попытка статус-сессии (вне цикла состояний — ключ starting
        // появляется как только etcd доступен).
        while (!ct.IsCancellationRequested
               && !await status.StartAsync(
                   new WalReceiverAgentStatus(WalReceiverRole.None, WalReceiverState.Starting,
                       null, null, false, null, null, null, NowUnix()), ct))
        {
            await SafeDelay(_retryDelay, ct);
        }

        // 4. Главный цикл состояний (spec §3.1): гейты → роль → креды/цели →
        //    S3-клиент → сессия стрима; исходы сессии — в состояние статус-ключа.
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // 4.1. Стоп-гейты: Unknown — работаем по последним известным
                //      (перепроверка через 5 c; etcd-моргание стрим не рвёт).
                var gates = await WalReceiverGates.ReadAsync(
                    _etcd, _endpoints, identity.Cluster, identity.Shard, ct);
                if (!gates.Unknown)
                    lastGates = gates;
                if (lastGates.Disabled)
                {
                    state = WalReceiverState.WaitingDisabled;
                    metrics.SetStreaming(false);
                    metrics.SetRole(null);
                    await PublishStateAsync(status, metrics, state, currentRole, ct);
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }
                if (lastGates.Paused)
                {
                    state = WalReceiverState.Paused;
                    metrics.SetStreaming(false);
                    await PublishStateAsync(status, metrics, state, currentRole, ct);
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }

                // 4.2. Роль источника (REST-проба своей ноды): none — ожидание.
                currentRole = await _roleProbe(ct);
                // Роль подтверждена свежей пробой ВНЕ сессии: подозрение
                // моргания из ПРОШЛОЙ сессии не переносится на новую — иначе
                // первая проба с иной ролью рвала бы новую сессию «по первой»
                // вместо «по второй последовательной» (граница сессий).
                _roleChangeSuspicion = null;
                metrics.SetRole(RoleName(currentRole));
                if (currentRole == WalReceiverRole.None)
                {
                    state = WalReceiverState.WaitingRole;
                    metrics.SetStreaming(false);
                    await PublishStateAsync(status, metrics, state, currentRole, ct);
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }

                // 4.3. Креды/цели (живой резолв из etcd на каждом цикле).
                var targets = await new WalReceiverTargets(_etcd, _endpoints, _pools)
                    .ResolveAsync(identity.Cluster, identity.Shard, ct);
                if (targets.WaitingPassword)
                {
                    state = WalReceiverState.WaitingPassword;
                    metrics.SetStreaming(false);
                    await PublishStateAsync(status, metrics, state, currentRole, ct);
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }
                if (targets.WaitingPolicy)
                {
                    state = WalReceiverState.WaitingPolicy;
                    metrics.SetStreaming(false);
                    await PublishStateAsync(
                        status, metrics, state, currentRole, ct, error: targets.Error);
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }

                // Bucket per-install: отсутствие (инконсистентность инсталляции —
                // валидация старта воркера Enabled⇒Bucket требует env) — ожидание
                // с диагностикой, не старт-отказ живого процесса.
                var pool = targets.Pool!;
                if (identity.S3Bucket is not { Length: > 0 } bucket)
                {
                    state = WalReceiverState.WaitingPolicy;
                    metrics.SetStreaming(false);
                    await PublishStateAsync(
                        status, metrics, state, currentRole, ct,
                        error: "env PGW_BACKUP_S3_BUCKET is not set — backups disabled on this install");
                    await SafeDelay(_retryDelay, ct);
                    continue;
                }

                // 4.4. S3-пул-клиент: write-список по роли, read — все ноды пула;
                // per-ЦЕЛЬНЫЙ таймаут — PutAttemptTimeout (фейловер доезжает).
                var writeTargets = currentRole == WalReceiverRole.Master
                    ? pool.WriteMaster
                    : pool.WriteReplica;
                var options = new WalReceiverOptions(
                    "127.0.0.1", 5432, "backup_exec", targets.Password!,
                    "postgres", targets.Slot!, identity.Cluster, identity.Shard,
                    writeTargets, pool.ReadAll, bucket,
                    pool.AccessKey, pool.SecretKey);
                IBackupS3 s3 = _s3Factory is { } factory
                    ? factory(options)
                    : new PoolBackupS3(
                        options.S3Endpoints, options.S3ReadEndpoints,
                        options.S3AccessKey, options.S3SecretKey,
                        options.S3Bucket, s3CaBundlePath,
                        requestTimeout: WalReceiverCore.PutAttemptTimeout);
                if (s3CaBundlePath is not null && !_s3CaHttpWarned
                    && writeTargets.Concat(pool.ReadAll)
                        .Any(u => !u.StartsWith("https://", StringComparison.OrdinalIgnoreCase)))
                {
                    _s3CaHttpWarned = true;
                    await Console.Error.WriteLineAsync(
                        "walreceiver: PGW_BACKUP_S3_CA_PEM is set but S3 pool endpoints are http — CA/TLS is not applied");
                }
                await using var s3Disposable = s3 as IAsyncDisposable;
                var source = _sourceFactory(options);
                await using var sourceDisposable = source;
                var slotReader = _slotReaderFactory(options);

                // 4.5. Стрим-сессия: состояние streaming + статус-ключ; проба
                //      роли из стрима (5 c) — смена роли чисто завершает сессию.
                state = WalReceiverState.Streaming;
                metrics.SetStreaming(true);
                metrics.SetRole(RoleName(currentRole));
                await PublishStateAsync(status, metrics, state, currentRole, ct);

                // Ре-доклад инвентаря .history (t25, spec §3.1 п.6): раз за цикл
                // подключения (каждая итерация, дошедшая до сессии) — pg_wal своей
                // ноды → отсутствующие в S3 put'ятся; вне hot-path (отказ —
                // diagnostic-log, доставит следующий цикл); старту сессии не мешает.
                await WalReceiverCore.RedeliverHistoryInventoryAsync(s3,
                    identity.Cluster, identity.Shard, _pgWalDir,
                    msg => Console.Error.WriteLine(msg), ct,
                    delivered: count => metrics.HistoryDelivered(count));

                var roleAtStart = currentRole;
                var confirmedBase = 0UL;
                var events = new WalSessionEvents
                {
                    PutSuccess = metrics.PutSuccess,
                    PutError = metrics.PutError,
                    PutRetry = metrics.PutRetry,
                    Reconnect = metrics.Reconnect,
                    SegmentDelivered = metrics.SegmentDelivered,
                    StreamProgress = lsn => metrics.SetBacklog(
                        Math.Max(0, (long)lsn - (long)confirmedBase)),
                    // Подтверждение слота после успешного put двигает базу
                    // backlog-гейджа (serverWalEnd − confirmedBase, arch/18 §2.8).
                    Confirmed = lsn => confirmedBase = lsn,
                    HistoryDelivered = count => metrics.HistoryDelivered(count),
                    RoleChangedProbe = token => ProbeRoleChangedAsync(token, roleAtStart),
                    // Стоп-гейт из стрима (t25/arch/19 §3): живой стрим замирает
                    // по ЛЮБОМУ гейту — /pgworker/backups-disabled ИЛИ wal-ключ
                    // шарда STOPPED/WAL_CHAIN_ERROR — ≤ 5 c, не ждёт обрыва транспорта;
                    // etcd-моргание проба глотает (false).
                    StopGateProbe = token => WalReceiverGates.IsStopGatedAsync(
                        _etcd, _endpoints, identity.Cluster, identity.Shard, token),
                    // Живые факты из стрима (put вне hot-path): pool_exhausted
                    // ставится/снимается ПЕРВЫМ успешным put при живой сессии,
                    // tli_jump случается в начале сессии, history_missing — при
                    // TLI-переходе без файла; все публикации обязаны попадать в
                    // статус-ключ сразу, не дожидаясь исхода сессии, и MERGE-ить
                    // текущую структуру фактов (не затирать другие).
                    PoolExhaustedChanged = exhausted => _ = PublishFactsQuietlyAsync(
                        status, currentRole, state, exhausted: exhausted, ct: ct),
                    TliJump = jump => _ = PublishFactsQuietlyAsync(
                        status, currentRole, state, jump: jump, ct: ct),
                    HistoryMissing = tli => _ = PublishFactsQuietlyAsync(
                        status, currentRole, state, historyMissing: tli, ct: ct),
                    Diagnostic = message => Console.Error.WriteLine(message),
                };

                var outcome = await WalReceiverCore.RunSessionAsync(
                    options, source, slotReader, s3, events, ct, _retryDelay,
                    roleProbePeriod: _roleProbePeriod);

                // Reconnect — короткая пауза до новой попытки: состояние/серию
                // не трогаем (стрим фактически продолжается; публикация
                // state=streaming поверх серии streaming=0 — рассинхрон; факты
                // доедут исходом следующей попытки). Гейт-исход перепроверяется
                // в начале итерации — waiting_disabled/paused опубликуется там.
                if (outcome.Kind != WalSessionKind.Reconnect)
                    metrics.SetStreaming(false);
                if (outcome.LastStreamLsn > 0)
                    metrics.SetBacklog((long)Math.Max(0, (long)outcome.LastStreamLsn - (long)outcome.ConfirmedLsn));

                // Факты исхода — в статус-ключ (tli_jump живёт до следующего
                // прыжка; pool_exhausted — флаг исчерпания write-списка).
                if (outcome.Kind != WalSessionKind.Reconnect)
                {
                    state = outcome.Kind switch
                    {
                        WalSessionKind.FailedPermanent => WalReceiverState.FailedPermanent,
                        WalSessionKind.WaitingSlot => WalReceiverState.WaitingSlot,
                        WalSessionKind.RoleChanged => WalReceiverState.WaitingRole,
                        _ => WalReceiverState.Streaming, // недостижимо: Reconnect отфильтрован выше
                    };
                    await PublishStateAsync(
                        status, metrics, state, currentRole, ct,
                        error: outcome.Error,
                        tliJump: outcome.TliJump,
                        historyMissingTli: outcome.HistoryMissingTli,
                        poolExhausted: outcome.PoolExhausted,
                        lastDelivered: outcome.LastDeliveredSegment,
                        lastDeliveredUnix: outcome.ConfirmedLsn > 0 ? NowUnix() : null);
                }

                if (outcome.Kind == WalSessionKind.RoleChanged)
                {
                    metrics.Reconnect();
                    currentRole = WalReceiverRole.None; // перепроверка роли без паузы
                    permanentBackoffSec = 0;
                    continue;
                }
                if (outcome.Kind == WalSessionKind.Reconnect)
                    metrics.Reconnect();
                if (outcome.Kind == WalSessionKind.FailedPermanent)
                {
                    // Permanent-причина несъёмна (segment-size mismatch/разрыв
                    // LSN): полный цикл попыток каждые 5 с бессмысленен — бэкофф
                    // ×2 до 60 c + diagnostic на каждую попытку (не молчание).
                    permanentBackoffSec = permanentBackoffSec <= 0
                        ? _retryDelay.TotalSeconds
                        : Math.Min(60, permanentBackoffSec * 2);
                    await Console.Error.WriteLineAsync(
                        $"walreceiver: failed_permanent (retry in {permanentBackoffSec:0} s): {outcome.Error}");
                    await SafeDelay(TimeSpan.FromSeconds(permanentBackoffSec), ct);
                }
                else
                {
                    permanentBackoffSec = 0;
                    // WaitingSlot/Reconnect — пауза и новая итерация (retry-полл
                    // гейтов продолжается, попытки сессии периодичны).
                    await SafeDelay(_retryDelay, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break; // SIGTERM: graceful — lease revoke в finally ниже
            }
            catch (Exception e)
            {
                // Дефект цикла (не сессии) — не убивает процесс: диагностика + ретрай.
                await Console.Error.WriteLineAsync(
                    $"walreceiver loop: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                await SafeDelay(_retryDelay, ct);
            }
        }

        // SIGTERM-путь: revoke lease (ключ гаснет сразу, не по TTL); temp-файл
        // CA S3 — публичный материал, но хвостов в temp не оставляем (finally —
        // при любом исходе DisposeAsync).
        try
        {
            await status.DisposeAsync();
        }
        finally
        {
            if (s3CaBundlePath is { } caPath)
            {
                try
                {
                    File.Delete(caPath);
                }
                catch
                {
                    // удаление temp — лучшие усилия (ФС может быть недоступна)
                }
            }
        }
    }

    // Сериализация публикаций (Update+Flush) — пара «факт→put» атомарна:
    // параллельные fire-and-forget публикации и публикация исхода сессии не
    // перекрываются (RMW LiveFacts без CAS вёл бы к потере факта, п.7 ревью).
    private readonly SemaphoreSlim _publishGate = new(1, 1);

    // Одноразовая диагностика «CA задан, но пул http» (п.9 ревью): резолвер
    // пула строит http-URI (канон arch/23) — CA-fabрика S3 при этом нечему
    // применять; оператор должен видеть расхождение конфига и схемы.
    private bool _s3CaHttpWarned;

    // Подозрение смены роли из живого стрима (debounce, #12б): null — последняя
    // проба подтвердила роль старта; значение — первая проба с иной ролью
    // (рвём сессию только при ВТОРОЙ последовательной с той же).
    private WalReceiverRole? _roleChangeSuspicion;

    // Проба роли из стрима: true — роль изменилась (сессию чисто завершить).
    // REST-моргание Patroni (одно переходное окно) живую сессию не рвёт —
    // разрыв только при второй последовательной пробе с иной ролью.
    private async Task<bool> ProbeRoleChangedAsync(CancellationToken token, WalReceiverRole roleAtStart)
    {
        var current = await _roleProbe(token);
        if (current == roleAtStart)
        {
            _roleChangeSuspicion = null;
            return false;
        }

        if (_roleChangeSuspicion == current)
            return true; // вторая последовательная проба с той же иной ролью
        _roleChangeSuspicion = current;
        return false;
    }

    // Живая публикация факта из стрима (pool_exhausted/tli_jump/history_missing):
    // MERGE в текущую структуру фактов + немедленный flush, fire-and-forget —
    // put вне hot-path: отказ/отмена стрим не рвут (при отказе flush
    // keepalive-цикл удержит lease, повторит следующая публикация). Явно
    // переданный факт обновляет структуру; неявный (null) — сохраняет прежнее
    // значение (spec §7 №8: факты персистентны до события-снятия).
    private async Task PublishFactsQuietlyAsync(
        WalReceiverStatusSession status, WalReceiverRole role, WalReceiverState state,
        WalReceiverTliJump? jump = null, bool? exhausted = null,
        uint? historyMissing = null, CancellationToken ct = default)
    {
        try
        {
            var next = PublishFacts(jump, exhausted, lastDelivered: null,
                lastDeliveredUnix: null, historyMissing: historyMissing);
            await _publishGate.WaitAsync(ct);
            try
            {
                status.Update(new WalReceiverAgentStatus(
                    role, state, next.LastDelivered, next.LastDeliveredUnix,
                    next.PoolExhausted, next.Jump, next.HistoryMissingTli, null, NowUnix()));
                await status.FlushAsync(ct);
            }
            finally
            {
                _publishGate.Release();
            }
        }
        catch (Exception)
        {
            // Публикация факта — наблюдаемость, не управляющий путь: сессия
            // и keepalive продолжаются независимо.
        }
    }

    // Put статус-ключа с текущим состоянием/фактами (немедленно — смена
    // состояния/фактов); отказ — diagnostic-log (put вне hot-path: стрим и
    // сессия не рвутся). Явно переданный факт обновляет структуру; неявный
    // (null) — сохраняет прежнее значение (spec §7 №8: факты персистентны до
    // события-снятия).
    private async Task PublishStateAsync(
        WalReceiverStatusSession status, WalReceiverMetrics metrics, WalReceiverState state,
        WalReceiverRole role, CancellationToken ct,
        string? error = null,
        WalReceiverTliJump? tliJump = null,
        uint? historyMissingTli = null,
        bool? poolExhausted = null,
        string? lastDelivered = null,
        long? lastDeliveredUnix = null)
    {
        var next = PublishFacts(tliJump, poolExhausted, lastDelivered, lastDeliveredUnix,
            historyMissingTli);
        await _publishGate.WaitAsync(ct);
        try
        {
            status.Update(new WalReceiverAgentStatus(
                role, state, next.LastDelivered, next.LastDeliveredUnix,
                next.PoolExhausted, next.Jump, next.HistoryMissingTli, error, NowUnix()));
            await status.FlushAsync(ct);
        }
        finally
        {
            _publishGate.Release();
        }
    }

    // MERGE фактов: не-null аргументы обновляют текущую структуру (атомарная
    // замена IMMUTABLE-снимка), не-null приёмника-факты — замещают (tli_jump
    // живёт до СЛЕДУЮЩЕГО прыжка; pool_exhausted — до первого успешного put,
    // исход сессии несёт фактическое состояние флага ядра; history_missing_tli —
    // до следующего TLI-перехода).
    private LiveFacts PublishFacts(
        WalReceiverTliJump? jump, bool? poolExhausted,
        string? lastDelivered, long? lastDeliveredUnix, uint? historyMissing = null)
    {
        var current = Volatile.Read(ref _facts);
        var next = current with
        {
            Jump = jump ?? current.Jump,
            PoolExhausted = poolExhausted ?? current.PoolExhausted,
            LastDelivered = lastDelivered ?? current.LastDelivered,
            LastDeliveredUnix = lastDeliveredUnix ?? current.LastDeliveredUnix,
            HistoryMissingTli = historyMissing ?? current.HistoryMissingTli,
        };
        Volatile.Write(ref _facts, next);
        return next;
    }

    private static string RoleName(WalReceiverRole role) => role switch
    {
        WalReceiverRole.Master => "master",
        WalReceiverRole.Sync => "sync",
        _ => "none",
    };

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    // Задержка, устойчивая к отмене (SIGTERM не бросает из цикла состояний).
    private static async Task SafeDelay(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, ct);
        }
        catch (OperationCanceledException)
        {
            // Отмена хоста — внешний цикл завершится по ct.
        }
    }
}
