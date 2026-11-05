using System.Security.Cryptography;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.PgNode;

/// <summary>Исход одной стрим-сессии приёмника (arch/19 §3): одна сессия =
/// «открытие → резолв хвоста → START_REPLICATION → приём до обрыва/смены роли».
/// Reconnect — обрыв источника/transient (переподключение с повторным резолвом
/// хвоста); WaitingSlot — слота нет на пустом префиксе S3 (transient-ожидание,
/// ensure делает воркер); FailedPermanent — несъёмное условие (segment-size
/// mismatch ИЛИ разрыв LSN протокола) с текстом; RoleChanged — цикл оборвал
/// сессию сам (проба роли в стриме). Факты сессии: TliJump (58P01-прыжок,
/// живёт до следующего прыжка), HistoryMissingTli (`.history` не добыта ни из
/// рантайма, ни из ФС ноды), PoolExhausted (флаг исчерпания write-списка),
/// LastDeliveredSegment/ConfirmedLsn/LastStreamLsn (прогресс).</summary>
public sealed record WalSessionOutcome(
    WalSessionKind Kind,
    string? Error = null,
    WalReceiverTliJump? TliJump = null,
    uint? HistoryMissingTli = null,
    bool PoolExhausted = false,
    string? LastDeliveredSegment = null,
    ulong ConfirmedLsn = 0,
    ulong LastStreamLsn = 0);

public enum WalSessionKind { Reconnect, WaitingSlot, FailedPermanent, RoleChanged }

/// <summary>Флаг исчерпания write-списка пула внутри сессии (ref в async
/// недопустим — состояние вынесено в объект; переходы отдаются колбэком
/// PoolExhaustedChanged, факт живёт в исходе сессии).</summary>
public sealed class PoolExhaustionFlag
{
    public bool IsSet;
}

/// <summary>Колбэки сессии (метрики приёмника + внешние пробы): все —
/// опциональные, сессия работает и без них (юниты ядра). RoleChangedProbe —
/// периодическая (5 c) проба роли ИЗ стрима: true — роль сменилась, сессия
/// завершается чисто (RoleChanged). HistoryFilePath — переопределение пути
/// `.history` на ФС ноды (прод-путь — pg_wal PGDATA; юниты — tmpdir).</summary>
public sealed class WalSessionEvents
{
    /// <summary>Успешный put: снимает pool_exhausted (первый после исчерпания).</summary>
    public Action? PutSuccess;

    /// <summary>Неудачная попытка put (вкл. ретраи) — counter walreceiver_put_errors_total.</summary>
    public Action? PutError;

    /// <summary>Повторная попытка put (перебор целей пула) — counter walreceiver_put_retries_total.</summary>
    public Action? PutRetry;

    /// <summary>Доставлен закрытый сегмент (байты) — counters delivered.</summary>
    public Action<long>? SegmentDelivered;

    /// <summary>Переподключение к источнику — counter walreceiver_reconnects_total.</summary>
    public Action? Reconnect;

    /// <summary>Прогресс позиции потока — serverWalEnd сервера (arch/18 §2.8:
    /// backlog = позиция записи сервера − подтверждённый хвост).</summary>
    public Action<ulong>? StreamProgress;

    /// <summary>Подтверждение слота принято сервером (write=flush=applied=endLsn):
    /// база backlog-гейджа цикла (confirmedBase) обновляется по факту.</summary>
    public Action<ulong>? Confirmed;

    /// <summary>Доставлена `.history` (байты) — counter bytes_delivered
    /// (arch/18 §2.8: счётчик байт включает history-объекты).</summary>
    public Action<long>? HistoryDelivered;

    /// <summary>Факт отсутствия `.history` TLI (ни из рантайма, ни из ФС ноды) —
    /// живая публикация history_missing_tli в статус-ключ (arch/19 §4):
    /// факт обязан быть наблюдаем до завершения сессии.</summary>
    public Action<uint>? HistoryMissing;

    /// <summary>Смена флага исчерпания write-списка (true — ставится, false — снят).
    /// Живая публикация: сессия при исчерпании не завершается (put-ретрай-цикл),
    /// факт обязан попасть в статус-ключ сразу — иначе воркер не увидит
    /// pool_exhausted до конца сессии.</summary>
    public Action<bool>? PoolExhaustedChanged;

    /// <summary>Факт tli-прыжка (58P01 fallback, arch/19 §3): живая публикация в
    /// статус-ключ — сессия после прыжка продолжает стрим, исход-факт доехал бы
    /// только при её завершении (прыжок живёт в статусе до следующего).</summary>
    public Action<WalReceiverTliJump>? TliJump;

    /// <summary>Проба роли из стрима (5 c): true — роль сменилась, сессия прерывается.</summary>
    public Func<CancellationToken, Task<bool>>? RoleChangedProbe;

    /// <summary>Проба ПОЛНОГО стоп-гейта из стрима (5 c, /arch/19 §3): true —
    /// глобальный ключ /pgworker/backups-disabled жив ИЛИ wal-ключ шарда в
    /// STOPPED/WAL_CHAIN_ERROR — сессия чисто завершается (Reconnect): цикл
    /// перепроверит гейты и уйдёт в waiting_disabled/paused; живой стрим
    /// обязан замирать по стоп-гейтам, а не ждать обрыва транспорта.</summary>
    public Func<CancellationToken, Task<bool>>? StopGateProbe;

    /// <summary>Путь `.history` на ФС ноды (null → pg_wal PGDATA-дефолт).</summary>
    public Func<uint, string?>? HistoryFilePath;

    /// <summary>Диагностика в stderr контейнера (не протокол — только наблюдаемость).</summary>
    public Action<string>? Diagnostic;
}

/// <summary>Ядро WAL-приёмника как управляемая сессия (→ wal-агент, arch/19 §3):
/// подключение со сверкой wal_segment_size, резолв старта от хвоста S3 (пустой
/// префикс — от restart_lsn слота), приём XLogData → сборка сегментов → put в
/// S3 → подтверждение слота ТОЛЬКО после успешного put (инвариант RPO:
/// write=flush=applied = конец последнего доставленного сегмента, никогда
/// дальше). Backpressure: при невозможности put чтение потока не двигается —
/// сервер ждёт подтверждения, слот удерживает WAL. Выходных кодов процесса
/// больше нет : permanent-условия — исход FailedPermanent (состояние
/// статус-ключа), ожидания — Reconnect/WaitingSlot (цикл ретраит), смену роли
/// детектирует проба (RoleChanged).</summary>
public sealed class WalReceiverCore
{
    private static readonly TimeSpan DefaultRetry = TimeSpan.FromSeconds(5);

    // R3 (разбор AC4): пер-попытка таймаут put — остановленная/перегруженная
    // нода пула «молча принимает» TCP и не отвечает — ПЕР-ЦЕЛЬНЫЙ бюджет: это
    // значение уходит requestTimeout'ом в КАЖДУЮ цель PoolBackupS3 (цикл),
    // чтобы висящая первая цель НЕ съедала бюджет перебора и фейловер доезжал
    // до следующей ноды списка (прецедент AC4: put к ноде под mesh-нагрузкой
    // висел до отмены попытки — вторая цель не получала ни одной попытки).
    internal static readonly TimeSpan PutAttemptTimeout = TimeSpan.FromSeconds(15);

    // Порог мгновенного сигнала пампу (arch/19 §3: «и/или по порогу байт» —
    // 4 МиБ, 1/4 сегмента; константа приёмника, не конфиг): при бурной записи
    // выкладка не ждёт тика PartialIntervalSec.
    internal static readonly ulong PartialThresholdBytes = 4L * 1024 * 1024;

    /// <summary>Сбрасываемый одноразовый сигнал (пампу): Pulse будит ожидающего,
    /// следующий WaitAsync ждёт нового сигнала; ожидание отменяемо.</summary>
    private sealed class SignalGate
    {
        private TaskCompletionSource _tcs = New();

        private static TaskCompletionSource New()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Pulse()
            => Interlocked.Exchange(ref _tcs, New()).TrySetResult();

        public Task WaitAsync(CancellationToken ct)
            => _tcs.Task.WaitAsync(ct);
    }

    // Таймаут per-попытка для операций крупного объёма (стрим/файловый GET
    // restore/verify и multipart-полный): бюджет от класса размера, не от
    // сегмента. PutAttemptTimeout выбран под WAL-сегменты 16–24 МиБ; стрим
    // tar-объекта (до сотен ГиБ) получает 300 с на попытку — многогигабайтный
    // объект доезжает и при скромном канале (~10 МиБ/с — 3 ГиБ), а «молча
    // принявшая TCP» нода (прецедент R3 выше) не съедает бюджет перебора —
    // фейловер доезжает до следующей цели; заведомо ниже воркеровских
    // бюджетов операции (6/24 ч, arch/19 §6).
    internal static readonly TimeSpan FileAttemptTimeout = TimeSpan.FromSeconds(300);

    // Период пробы роли из стрима (arch/19 §3: смена ролей подхватывается
    // приёмником без участия воркера).
    private static readonly TimeSpan RoleProbePeriod = TimeSpan.FromSeconds(5);

    // pg_wal PGDATA ноды (arch/14 §2.1): fallback-чтение `.history` приёмником
    // из ФС своей ноды (приёмник живёт в контейнере ноды, user=postgres).
    public const string DefaultPgWalDir = "/home/postgres/pgdata/pgroot/data/pg_wal";

    /// <summary>Одна сессия стрима (описание исходов — WalSessionOutcome).
    /// lastDeliveredSegment — собственный доставленный прогресс прошлой
    /// сессии (статус-ключ, arch/19 §3 «Возобновление»): стартовая позиция НЕ
    /// выше его конца — соло-хвост чужой ноды (mesh-лаг/полный набор) не
    /// переводит сессию через свой недоставленный сегмент; перечитка у
    /// источника и повторный put идемпотентны.</summary>
    public static async Task<WalSessionOutcome> RunSessionAsync(
        WalReceiverOptions o,
        IXLogReplicationSource source,
        ISlotPositionReader slots,
        IBackupS3 s3,
        WalSessionEvents events,
        CancellationToken ct,
        TimeSpan? retryDelay = null,
        TimeSpan? roleProbePeriod = null,
        string? lastDeliveredSegment = null)
    {
        var retry = retryDelay ?? DefaultRetry;
        // Период проб роли/стоп-гейта из стрима (прод — 5 c; тесты ужимают —
        // тайминговые тесты не держат реальные 5-с окна).
        var probePeriod = roleProbePeriod ?? RoleProbePeriod;
        string? lastDelivered = null;
        var confirmedLsn = 0UL;
        var lastStreamLsn = 0UL;
        WalReceiverTliJump? tliJump = null;
        uint? historyMissingTli = null;
        var exhaustion = new PoolExhaustionFlag();

        // Проба роли из стрима: фоновая задача (5 c); true → roleCts.Cancel —
        // обрыв Stream трактуется сессией как чистая смена роли (RoleChanged).
        var roleTriggered = false;
        // Проба ПОЛНОГО стоп-гейта из стрима (5 c): true → тот же Cancel —
        // исход Reconnect: цикл перепроверит гейты (waiting_disabled/paused) немедленно.
        var gateTriggered = false;
        using var roleCts = new CancellationTokenSource();
        Task? roleProbeTask = null;
        if (events.StopGateProbe is { } gateProbe)
        {
            _ = Task.Run(async () =>
            {
                while (!roleCts.IsCancellationRequested && !ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(probePeriod, roleCts.Token);
                        if (await gateProbe(roleCts.Token))
                        {
                            gateTriggered = true;
                            roleCts.Cancel();
                            return;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch
                    {
                        // Проба не удалась (etcd-моргание) — гейт не трогаем:
                        // стрим не рвём, перепроверка следующим периодом.
                    }
                }
            }, CancellationToken.None);
        }
        if (events.RoleChangedProbe is { } probe)
        {
            roleProbeTask = Task.Run(async () =>
            {
                while (!roleCts.IsCancellationRequested && !ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(probePeriod, roleCts.Token);
                        if (await probe(roleCts.Token))
                        {
                            roleTriggered = true;
                            roleCts.Cancel();
                            return;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch
                    {
                        // Проба не удалась (транспорт/ФС) — роли нет: стрим не рвём.
                    }
                }
            }, CancellationToken.None);
        }

        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct, roleCts.Token);
        var sct = sessionCts.Token;

        try
        {
            while (true)
            {
                // 1. Подключение + сверка wal_segment_size: расхождение — permanent
                //    (математика имён сегментов не должна молча врать), прочие сбои
                //    — transient (переподключение).
                var opened = await source.OpenAsync(sct);
                if (!opened.IsSuccess)
                {
                    if (opened.Error!.Message.Contains("wal_segment_size"))
                        return Outcome(WalSessionKind.FailedPermanent, opened.Error.Message);

                    // Диагностика transient-цикла подключения: причина обязана быть
                    // видимой в логе контейнера без перезапуска (R3 «не молчание»).
                    events.Reconnect?.Invoke();
                    Diagnostic(events, $"open_failed: {opened.Error.Message.Split('\n')[0]}");
                    await Task.Delay(retry, sct);
                    continue;
                }

                // 2. Стартовая позиция: хвост S3 (позиция + имя хвостового сегмента —
                //    вход tli-процедуры при 58P01); транспортный сбой резолва — ретрай;
                //    пустой префикс — restart_lsn слота; слота нет — transient-ожидание
                //    (ensure делает воркер, arch/19 §3 — НЕ permanent).
                WalTail? walTail = null;
                while (walTail is null)
                {
                    var resolved = await S3TailResolver.ResolveTailAsync(s3, o.Cluster, o.Shard, sct);
                    if (resolved.IsSuccess)
                    {
                        walTail = resolved.Value; // позиция или null (пустой префикс)
                        break;
                    }

                    Diagnostic(events, $"tail resolve failed: {resolved.Error!.Message.Split('\n')[0]}");
                    await Task.Delay(retry, sct);
                }

                ulong startAt;
                string? tailSegment;
                if (walTail is { } tailInfo)
                {
                    startAt = tailInfo.Position;
                    tailSegment = tailInfo.SegmentName;

                    // Старт НЕ выше собственного доставленного прогресса
                    // (arch/19 §3 «Возобновление»): соло-хвост резолвится с
                    // первой доступной read-ноды и может содержать сегменты,
                    // доставленные ДРУГИМ источником двойной архивации и не
                    // лежащие на write-цели ЭТОЙ ноды. Через свой
                    // недоставленный сегмент сессия не стартует: от конца
                    // собственного last_delivered — перечитка у источника и
                    // повторный put идемпотентны (одно имя — то же содержимое).
                    if (lastDeliveredSegment is { } deliveredName
                        && WalFileName.TryParse(deliveredName) is { } deliveredFile)
                    {
                        var deliveredEnd = S3TailResolver.EndLsn(deliveredFile);
                        if (startAt > deliveredEnd)
                        {
                            Diagnostic(events,
                                $"tail clamp: S3 tail {tailInfo.SegmentName} exceeds own delivered " +
                                $"{deliveredName} — starting from own progress (idempotent re-delivery)");
                            startAt = deliveredEnd;
                            tailSegment = deliveredName;
                        }
                    }
                }
                else
                {
                    var restart = await slots.ReadRestartLsnAsync(o.Slot, sct);
                    if (!restart.IsSuccess)
                    {
                        // Слота нет/lost (SQLSTATE 42704) — waiting_slot: воркер
                        // ensure'ит/recreate'ит (arch/19 §3). Иной отказ (PG
                        // недоступен/SQL-сбой) — transient: диагностика + ретрай,
                        // состояние не маскируем waiting_slot.
                        if (restart.Error is WalSlotMissing)
                            return Outcome(WalSessionKind.WaitingSlot);
                        Diagnostic(events, $"slot probe failed: {restart.Error!.Message.Split('\n')[0]}");
                        await Task.Delay(retry, sct);
                        continue;
                    }

                    startAt = restart.Value;
                    tailSegment = null;
                }

                // Стартовая позиция выше текущей позиции записи сервера —
                // хвост ОТКАТАННОЙ ветки после restore (откат вернул LSN назад;
                // сегменты прежней ветки выше точки форка остались в wal/):
                // сервер физически не отдаст точку (flush позади), и retry-цикл
                // стоял бы до его догона. Берём хвост ЖИВОЙ ветки
                // (CheckWithRestart); нет сегментов живой ветки или он тоже
                // впереди — прежний retry (сервер догонит). arch/pgnode/03 §3.
                if (tailSegment is not null)
                {
                    var identity = await source.IdentifySystemAsync(sct);
                    if (identity.IsSuccess && startAt > identity.Value.XLogPos)
                    {
                        var live = await S3TailResolver.ResolveLiveTailAsync(
                            s3, o.Cluster, o.Shard, sct);
                        if (live.IsSuccess && live.Value is { } liveTail
                            && liveTail.Position <= identity.Value.XLogPos)
                        {
                            Diagnostic(events,
                                $"tail {tailSegment} is beyond server position " +
                                $"{identity.Value.XLogPos:x} — starting from live branch tail " +
                                $"{liveTail.SegmentName}");
                            startAt = liveTail.Position;
                            tailSegment = liveTail.SegmentName;
                        }
                    }
                }

                // 3. START_REPLICATION с явной стартовой позицией; отказ 58P01 — tli-процедура
                //    (досыпка старого таймлайна / прыжок на текущий) вместо retry. Процедура
                //    стартовала — приём продолжается от её позиции; не стартовала — обычный
                //    transient-цикл. tailSegment == null (пустой префикс — старт от
                //    restart_lsn слота) — процедура неприменима (вход не определён): retry-цикл.
                var started = await source.StartReplicationAsync(o.Slot, startAt, null, sct);
                if (!started.IsSuccess)
                {
                    // Lost-слот при непустом хвосте (SQLSTATE 42704): waiting_slot,
                    // а не generic-retry в streaming (arch/19 §3 — lost-слот тоже
                    // waiting_slot; ensure/recreate делает воркер).
                    if (started.Error is WalSlotMissing)
                        return Outcome(WalSessionKind.WaitingSlot);
                    var (procedureResult, jump, historyMissing) =
                        started.Error is WalStartRefused && tailSegment is not null
                            ? await TliProcedureAsync(o, source, s3, events, startAt, tailSegment, retry, exhaustion, sct)
                            : (Result<ulong?>.Success(null), (WalReceiverTliJump?)null, (uint?)null);
                    if (jump is { } j)
                    {
                        tliJump = j;
                        events.TliJump?.Invoke(j); // живая публикация факта прыжка
                    }
                    if (historyMissing is { } missingTli)
                        historyMissingTli = missingTli;
                    if (procedureResult is { IsSuccess: true, Value: { } procedurePos })
                    {
                        startAt = procedurePos;
                    }
                    else
                    {
                        // Конфликт содержимого (.history чужой инкарнации) —
                        // permanent: retry не разрешится (arch/19 §3).
                        if (WalArchiveConflict.Is(procedureResult))
                            return Outcome(WalSessionKind.FailedPermanent,
                                procedureResult.Error!.Message);
                        Diagnostic(events, $"start_failed: {started.Error!.Message.Split('\n')[0]}");
                        await Task.Delay(retry, sct);
                        continue;
                    }
                }

                // 4. Приём: сборка сегментов → put (ретрай, backpressure — enumerator не
                //    двигается) → подтверждение ТОЛЬКО после успешного put.
                var assembler = new SegmentAssembler(startAt);
                uint prevTli = 0;

                // Дельта-памп (t15, spec §4.2, arch/pgnode/03 §2): фоновая задача
                // итерации — выкладка валидного префикса незакрытого сегмента
                // кусками с номером жизни: первая выкладка сессии — база-компак
                // [0..end), дальше — только дельты прироста; компакция раз в
                // PartialCompactIntervalSec при наличии инкремента. Подтверждение
                // слота НЕ двигает (инвариант RPO — только финалы). Триггеры:
                // таймер PartialIntervalSec ИЛИ сигнал порога (4 МиБ).
                var lastPartialEndLsn = 0UL; // Volatile-гейт прироста (вкл. покрытие соседа)
                var partialGate = new SignalGate();
                using var partialCts = CancellationTokenSource.CreateLinkedTokenSource(sct);
                var partialPumpToken = partialCts.Token;
                var partialPump = Task.Run(async () =>
                {
                    var interval = TimeSpan.FromSeconds(Math.Max(1, o.PartialIntervalSec));
                    // Компакция: фактическое значение приходит из конфигурации
                    // (клэмп ≥60 — на входе конфигурации/env, spec §4.2); здесь —
                    // только защита от неположительного периода.
                    var compactInterval = TimeSpan.FromSeconds(Math.Max(1, o.PartialCompactIntervalSec));
                    string? pumpedSegment = null;  // сегмент сессии (смена → новое покрытие)
                    ulong coverage = 0;            // конец своего выложенного покрытия
                    var deltasSinceCompact = 0;    // дельт после последней компакции
                    var compactAt = DateTimeOffset.MaxValue;
                    while (!partialPumpToken.IsCancellationRequested)
                    {
                        try
                        {
                            var delay = Task.Delay(interval, partialPumpToken);
                            var signal = partialGate.WaitAsync(partialPumpToken);
                            await Task.WhenAny(delay, signal);
                        }
                        catch (OperationCanceledException)
                        {
                            break; // итерация/сессия закрываются
                        }

                        var snap = assembler.PartialSnapshot();
                        if (snap is not { } shot)
                            continue; // незакрытого сегмента/полной записи нет (§3, развилка 22)

                        if (shot.Name != pumpedSegment)
                        {
                            // Новый сегмент сессии: покрытие с нуля, первая выкладка — база.
                            pumpedSegment = shot.Name;
                            coverage = 0;
                            deltasSinceCompact = 0;
                            compactAt = DateTimeOffset.UtcNow + compactInterval;
                        }

                        if (shot.EndLsn <= coverage)
                            continue; // нулевой прирост — куски не пере-выкладываются

                        // Компакция (spec решение №10): проверяется ДО выкладки дельты;
                        // при нулевом приросте база НЕ перезаливается, таймер сдвигается.
                        if (DateTimeOffset.UtcNow >= compactAt && deltasSinceCompact > 0)
                        {
                            var fresh = assembler.PartialSnapshot();
                            if (fresh is { } s2 && s2.EndLsn > coverage)
                            {
                                var compactPut = await PutDeltaAsync(
                                    s3, events, o, s2.Name, s2.Prefix, 0, s2.EndLsn,
                                    retry, partialPumpToken, exhaustion);
                                if (!compactPut.IsSuccess)
                                    return; // permanent-веток нет, вечный ретрай не завершается
                                coverage = s2.EndLsn;
                                Volatile.Write(ref lastPartialEndLsn, s2.EndLsn);
                                deltasSinceCompact = 0;
                            }
                            compactAt = DateTimeOffset.UtcNow + compactInterval;
                            if (shot.EndLsn <= coverage)
                                continue; // прирост этого тика покрыт компакцией
                        }

                        // Листинг узкого префикса БЕЗ ТОЧКИ (wal/<seg>): возвращает и
                        // лежащий финал wal/<seg>, и всё кусочное семейство; отказ —
                        // skip тика (стрим не рвём, хвост доедет следующим тиком/финалом).
                        var listed = await s3.ListAsync(
                            o.Cluster, o.Shard, $"wal/{shot.Name}", ct: partialPumpToken);
                        if (!listed.IsSuccess)
                        {
                            Diagnostic(events,
                                $"delta list failed: {listed.Error!.Message.Split('\n')[0]}");
                            continue;
                        }

                        // Лежащий финал сегмента — куски не выкладываются (поглощение и
                        // чистка удалят семейство); проверка работает из-за префикса без точки.
                        if (listed.Value.Any(obj =>
                                string.Equals(obj.Name, shot.Name, StringComparison.Ordinal)))
                            continue;

                        // Симметрия TailOf: покрытие кусков ТЕКУЩЕГО номера жизни;
                        // чужие номера и безномерные legacy игнорируются по построению.
                        var lying = WalDeltaName.CoverageOf(
                            listed.Value.Select(x => x.Name), o.ChainLife);
                        if (lying >= shot.EndLsn)
                        {
                            // Сосед двойной архивации длиннее — ждём прироста за его
                            // покрытие. Покрытие ЗАПОМИНАЕМ: гейт тика выше сравнивает
                            // shot.EndLsn с coverage, и без этого отстающий приёмник
                            // каждый такт делал бы листинг семейства (k имён) вхолостую.
                            // Байты префикса мастера и sync-реплики идентичны
                            // (arch/pgnode/03 §2) — брать чужое покрытие как своё можно.
                            coverage = Math.Max(coverage, lying);
                            Volatile.Write(ref lastPartialEndLsn, lying);
                            continue;
                        }
                        coverage = Math.Max(coverage, lying);

                        var putDelta = await PutDeltaAsync(
                            s3, events, o, shot.Name, shot.Prefix, coverage, shot.EndLsn,
                            retry, partialPumpToken, exhaustion);
                        if (!putDelta.IsSuccess)
                            return; // permanent-веток нет, вечный ретрай не завершается

                        var wasBase = coverage == 0
                            || coverage == WalDeltaName.SegmentStartOf(
                                WalFileName.TryParse(shot.Name)!.Value);
                        if (!wasBase)
                            deltasSinceCompact++; // инкремент для компакции (не база)
                        coverage = shot.EndLsn;
                        Volatile.Write(ref lastPartialEndLsn, shot.EndLsn);
                        events.PutSuccess?.Invoke();
                    }
                }, CancellationToken.None);

                try
                {
                    try
                    {
                        await foreach (var chunk in source.Stream.WithCancellation(sct))
                        {
                            IReadOnlyList<ClosedSegment> closedSegments;
                            try
                            {
                                closedSegments = assembler.Append(chunk);
                            }
                            catch (ApplicationException e)
                            {
                                // Разрыв LSN — протокол физической репликации дыр не даёт:
                                // это баг транспорта, не transient — невосстановимо.
                                return Outcome(WalSessionKind.FailedPermanent, e.Message);
                            }

                            // Позиция потока для наблюдаемости — serverWalEnd
                            // (arch/18 §2.8), не позиция принятых байт: backlog =
                            // позиция записи сервера − подтверждённый хвост.
                            lastStreamLsn = Math.Max(lastStreamLsn, assembler.ServerWalEnd);
                            events.StreamProgress?.Invoke(assembler.ServerWalEnd);

                            // Порог-сигнал пампу (4 МиБ от последней выкладки; LSN-прирост ≈
                            // байты — скан записей в hot-path не нужен, выкладка идёт по
                            // валидному префиксу).
                            if (assembler.LastAppendedLsn - Volatile.Read(ref lastPartialEndLsn)
                                >= PartialThresholdBytes)
                                partialGate.Pulse();

                            foreach (var segment in closedSegments)
                            {
                                try
                                {
                                    // 5. TLI-переход (смена TLI относительно доставленного; первый
                                    //    сегмент цепочки — инициализация, истории для tli=1 не
                                    //    существует): .history загружается обязательно (PITR через
                                    //    смену timeline); рантайм без TIMELINE_HISTORY — чтение
                                    //    файла ФС ноды (fallback); нет и файла — факт
                                    //    history_missing в исходе сессии.
                                    if (prevTli != 0 && segment.Tli != prevTli)
                                    {
                                        var history = await ReadHistoryWithFallbackAsync(
                                            source, events, segment.Tli, sct);
                                        if (history.Value is { } content)
                                        {
                                            var historyPut = await PutWithRetryAsync(
                                                s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Tli:x8}.history",
                                                content,
                                                Convert.ToHexString(SHA256.HashData(content)), retry, sct,
                                                exhaustion);
                                            if (!historyPut.IsSuccess)
                                                return Outcome(WalSessionKind.FailedPermanent,
                                                    historyPut.Error!.Message);
                                            events.HistoryDelivered?.Invoke(content.Length);
                                        }
                                        else
                                        {
                                            historyMissingTli = segment.Tli;
                                            events.HistoryMissing?.Invoke(segment.Tli);
                                            Diagnostic(events,
                                                $"history_missing tli={segment.Tli} (runtime + filesystem)");
                                        }
                                    }

                                    prevTli = segment.Tli;

                                    // 6. Доставка сегмента: sha256-hex — серверная сверка целостности
                                    //    (один расчёт: маркер и put используют один хэш).
                                    var sha256 = Convert.ToHexString(SHA256.HashData(segment.Data));
                                    var put = await PutWithRetryAsync(
                                        s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Name}",
                                        segment.Data, sha256, retry, sct, exhaustion);
                                    if (!put.IsSuccess)
                                        return Outcome(WalSessionKind.FailedPermanent,
                                            put.Error!.Message);
                                    events.SegmentDelivered?.Invoke(segment.Data.Length);

                                    // 7. ЕДИНСТВЕННОЕ место подтверждения слота — после успешного put.
                                    var confirmed = await source.SetConfirmedAsync(segment.EndLsn, sct);
                                    if (confirmed.IsSuccess)
                                    {
                                        lastDelivered = segment.Name;
                                        confirmedLsn = segment.EndLsn;
                                        events.Confirmed?.Invoke(segment.EndLsn);
                                    }
                                    else
                                    {
                                        // Подтверждение не прошло (транспорт): слот не двинулся,
                                        // следующий сегмент повторит статус с той же LSN;
                                        // диагностика — застой не молчит. Это НЕ put-ошибка
                                        // (словарь arch/18 §2.8) — walreceiver_put_errors_total
                                        // не растёт.
                                        Diagnostic(events,
                                            $"confirm failed: {confirmed.Error!.Message.Split('\n')[0]}");
                                    }

                                    // Поглощение версий финалом (arch/19 §3): best-effort list узкого
                                    // префикса + batch-delete; отказ — diagnostic (докладывает sweeper).
                                    var absorbed = await s3.ListAsync(
                                        o.Cluster, o.Shard, $"wal/{WalPartialName.SegmentPrefixOf(segment.Name)}",
                                        ct: sct);
                                    if (absorbed.IsSuccess && absorbed.Value.Count > 0)
                                    {
                                        var absorbedKeys = absorbed.Value
                                            .Select(w => $"{o.Cluster}/{o.Shard}/wal/{w.Name}").ToList();
                                        var absorbDelete = await s3.DeleteKeysAsync(absorbedKeys, sct);
                                        if (!absorbDelete.IsSuccess)
                                            Diagnostic(events,
                                                $"partial absorb delete failed: {absorbDelete.Error!.Message.Split('\n')[0]}");
                                    }
                                }
                                finally
                                {
                                    // Сегментный буфер (пул LOH, hot-path) возвращается
                                    // после обработки сегмента: put/подтверждение завершены
                                    // (или сессия прервана отменой) — байты никому не нужны.
                                    SegmentBufferPool.Return(segment.Data);
                                }
                            }
                        }
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        // Обрыв источника (исключение из Stream) — transient:
                        // переподключение заново (резолв хвоста, START_REPLICATION от хвоста S3).
                        events.Reconnect?.Invoke();
                        Diagnostic(events, $"stream_broken: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                    }
                }
                finally
                {
                    // Памп живёт ровно одну итерацию сборщика: гасим и дожидаемся,
                    // иначе задача переживёт assembler/буфер сегмента.
                    await partialCts.CancelAsync();
                    try
                    {
                        await partialPump;
                    }
                    catch
                    {
                        // Отменён — штатный финал итерации.
                    }
                }

                if (gateTriggered)
                    return Outcome(WalSessionKind.Reconnect); // цикл: гейты → waiting_disabled
                if (roleTriggered)
                    return Outcome(WalSessionKind.RoleChanged);

                // Поток кончился/оборвался (сервер закрыл, транспортный сбой) — transient.
                await Task.Delay(retry, sct);
            }
        }
        catch (OperationCanceledException)
        {
            // Отмена: внешняя (ct — SIGTERM супервиза) или проба роли. Чистая
            // смена роли — различимый исход; SIGTERM — наружу (graceful-завершение
            // делает цикл: недоставленное удерживается слотом, перезаказывается
            // при следующем старте).
            if (gateTriggered)
                return Outcome(WalSessionKind.Reconnect);
            if (roleTriggered)
                return Outcome(WalSessionKind.RoleChanged);
            throw;
        }
        finally
        {
            if (roleCts.IsCancellationRequested is false)
                await roleCts.CancelAsync();
            if (roleProbeTask is { } task)
            {
                try
                {
                    await task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None);
                }
                catch
                {
                    // Пробник уже мёртв/не успел за бюджетом — сессия всё равно завершена.
                }
            }
        }

        WalSessionOutcome Outcome(WalSessionKind kind, string? error = null) => new(
            kind, error, tliJump, historyMissingTli, exhaustion.IsSet,
            lastDelivered, confirmedLsn, lastStreamLsn);
    }

    private static void Diagnostic(WalSessionEvents events, string message)
    {
        if (events.Diagnostic is { } sink)
            sink(message);
        else
            Console.Error.WriteLine(message);
    }

    /// <summary>`.history` нового TLI: сначала рантайм репликации
    /// (TIMELINE_HISTORY); рантайм не отдал — чтение файла ФС ноды
    /// pg_wal/<tli:x8>.history (приёмник живёт в контейнере с PGDATA); нет и
    /// файла — Success(null) (факт history_missing у вызывающего).</summary>
    private static async Task<Result<byte[]?>> ReadHistoryWithFallbackAsync(
        IXLogReplicationSource source, WalSessionEvents events, uint tli, CancellationToken ct)
    {
        var runtime = await source.ReadTimelineHistoryAsync(tli, ct);
        if (runtime is { IsSuccess: true } && runtime.Value is not null)
            return runtime;

        // Fallback: файл ФС ноды (user=postgres — доступ к pg_wal PGDATA).
        var path = events.HistoryFilePath?.Invoke(tli) ?? $"{DefaultPgWalDir}/{tli:x8}.history";
        try
        {
            if (File.Exists(path))
                return Result<byte[]?>.Success(await File.ReadAllBytesAsync(path, ct));
        }
        catch (Exception e)
        {
            Diagnostic(events, $"history file read failed tli={tli}: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }

        return Result<byte[]?>.Success(null);
    }

    /// <summary>Tli-процедура (58P01, arch/19 §3): досыпка старого таймлайна от
    /// хвоста S3 до точки переключения (CopyDone → переподключение от выросшего
    /// хвоста — зазора нет, RPO=0) или прыжок-fallback на текущий таймлайн от
    /// границы сегмента xlogpos (сегменты (tail .. старт] в S3 не появятся —
    /// осознанный RPO-зазор, факт tli_jump наружу — цикл кладёт его в статус-ключ,
    /// живёт до следующего прыжка). Возврат: (стартовая LSN — приём продолжается
    /// штатным контуром; null — процедура не стартовала (transient: обычный
    /// retry-цикл); факт прыжка; tli истории без источника). КАЖДЫЙ старт
    /// процедуры выполняется на СВЕЖЕМ replication-соединении: отказ 58P01 гасит
    /// прежнее (транспорт), поэтому перед досыпкой и перед прыжком — OpenAsync.
    /// Идемпотентна: гонки повторного промоута конвергентны — каждая итерация
    /// начинается IDENTIFY_SYSTEM, счётчиков попыток нет; старт с историческим
    /// TIMELINE безопасен (сервер отдаёт его до конца ветки). Флаг исчерпания —
    /// СЕССИОННЫЙ (общий с put-циклом сегментов): снятие факта «первым
    /// успешным put» обязано срабатывать и на put-ах процедуры, иначе
    /// pool_exhausted залипает в статус-ключе при живых успешных put-ах.</summary>
    private static async Task<(Result<ulong?> Start, WalReceiverTliJump? Jump, uint? HistoryMissing)>
        TliProcedureAsync(
            WalReceiverOptions o, IXLogReplicationSource source, IBackupS3 s3, WalSessionEvents events,
            ulong tail, string tailSegment, TimeSpan retry, PoolExhaustionFlag exhaustion,
            CancellationToken ct)
    {
        uint? historyMissing = null;
        if (WalFileName.TryParse(tailSegment) is not { } tailFile)
            return (Result<ulong?>.Success(null), null, null); // имя не парсится — retry (защитно)

        // 1. IDENTIFY_SYSTEM: сбой — transient (retry-цикл).
        var identity = await source.IdentifySystemAsync(ct);
        if (!identity.IsSuccess)
        {
            Diagnostic(events, $"tli_procedure: identify failed: {identity.Error!.Message.Split('\n')[0]}");
            return (Result<ulong?>.Success(null), null, null);
        }
        var (serverTli, xlogPos) = identity.Value;

        // 2. .history нового TLI — ДО досыпки (канон §3: загружается обязательно;
        //    переход в потоке не наблюдается — prevTli сессии инициализируется
        //    заново первым сегментом). TLI 1 истории не имеет — не запрашивается.
        if (serverTli > tailFile.Tli)
        {
            var history = await ReadHistoryWithFallbackAsync(source, events, serverTli, ct);
            if (history.Value is { } content)
            {
                var historyPut = await PutWithRetryAsync(
                    s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{serverTli:x8}.history",
                    content,
                    Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(content)),
                    retry, ct, exhaustion);
                if (!historyPut.IsSuccess)
                    return (Result<ulong?>.Failed(historyPut.Error!), null, historyMissing);
                events.HistoryDelivered?.Invoke(content.Length);
            }
            else
            {
                historyMissing = serverTli;
                events.HistoryMissing?.Invoke(serverTli);
                Diagnostic(events, $"history_missing tli={serverTli} (runtime + filesystem)");
            }
        }

        // 3. Досыпка (только при T сервера > TLI хвоста): стрим старого таймлайна
        //    от хвоста до точки переключения — байты удерживаются слотом на ноде;
        //    повторный 58P01 (удержание потеряно) — прыжок ниже.
        if (serverTli > tailFile.Tli)
        {
            Diagnostic(events,
                $"tli_procedure: backfill segment {tailSegment} (tli {tailFile.Tli}) up to tli {serverTli} switch point");
            var openedBackfill = await source.OpenAsync(ct);
            if (!openedBackfill.IsSuccess)
            {
                // Отказ 58P01 погасил стрим-соединение — процедуре нужно свежее;
                // неудача открытия — transient (retry-цикл).
                Diagnostic(events,
                    $"tli_procedure: open failed: {openedBackfill.Error!.Message.Split('\n')[0]}");
                return (Result<ulong?>.Success(null), null, historyMissing);
            }

            var backfill = await source.StartReplicationAsync(o.Slot, tail, tailFile.Tli, ct);
            if (backfill.IsSuccess)
                return (Result<ulong?>.Success(tail), null, historyMissing);
            if (backfill.Error is not WalStartRefused)
            {
                Diagnostic(events,
                    $"tli_procedure: backfill start failed: {backfill.Error!.Message.Split('\n')[0]}");
                return (Result<ulong?>.Success(null), null, historyMissing); // transient
            }

            Diagnostic(events, "tli_procedure: backfill refused (58P01) — old-tli segments recycled, jumping");
        }

        // 4. Прыжок-fallback (и единственный путь при T == TLI хвоста — зазор
        //    внутри одного таймлайна): старт от ГРАНИЦЫ сегмента, содержащего
        //    xlogpos — текущий записываемый сегмент существует всегда, сборщик
        //    доставит его целиком (старт ровно на границе — головного отброса нет).
        //    Отказ досыпки погасил соединение — прыжок на свежем.
        var segmentStart = xlogPos / (ulong)WalFileName.SegmentBytes
                         * (ulong)WalFileName.SegmentBytes;
        var segId = (uint)(segmentStart / (ulong)WalFileName.SegmentBytes);
        var toSegment = new WalFileName(
            serverTli, segId / WalFileName.SegsPerLog, segId % WalFileName.SegsPerLog).Name;
        var jump = new WalReceiverTliJump(tailSegment, toSegment, serverTli);
        Diagnostic(events,
            $"tli_procedure: jump {tailSegment} (tli {tailFile.Tli}) -> {toSegment} (tli {serverTli})");
        var openedJump = await source.OpenAsync(ct);
        if (!openedJump.IsSuccess)
        {
            Diagnostic(events,
                $"tli_procedure: open failed: {openedJump.Error!.Message.Split('\n')[0]}");
            return (Result<ulong?>.Success(null), jump, historyMissing); // transient, факт прыжка уже случился
        }

        var jumpStart = await source.StartReplicationAsync(o.Slot, segmentStart, serverTli, ct);
        if (jumpStart.IsSuccess)
            return (Result<ulong?>.Success(segmentStart), jump, historyMissing);

        Diagnostic(events,
            $"tli_procedure: jump start failed: {jumpStart.Error!.Message.Split('\n')[0]}");
        return (Result<ulong?>.Success(null), jump, historyMissing); // включая повторный 58P01 — новая итерация процедуры
    }

    /// <summary>Put с ретраем (transient-отказ S3): на время ретраев чтение потока
    /// остановлено — backpressure, ничего не теряется, слот удерживает WAL.
    /// Хэш передаёт вызывающий: сегмент уже хэширован — повторный SHA256 16 МиБ
    /// на каждый put не считается. Исчерпание списка пула (маркер
    /// S3PoolMarkers.Exhausted в ошибке) — флаг poolExhausted=true (факт
    /// статус-ключа через PoolExhaustedChanged; снимается первым успешным put);
    /// ретрай-цикл — стрим стоит, слот держит. R3 «быстрый отказ, не молчание»:
    /// пер-ЦЕЛЬНЫЙ таймаут (PutAttemptTimeout — requestTimeout каждой цели в
    /// PoolBackupS3) + бюджет ВЫЗОВА (цели записи + 1) × таймаут цели: висящая
    /// первая цель не отменяет перебор — фейловер доезжает до живой ноды.</summary>
    // Выкладка куска незакрытого сегмента (t15, spec §4.2): байты — срез снимка
    // префикса [startLsn..endLsn), md5 — в имени куска и в Content-MD5 (один
    // расчёт); start==0 — база-компак, иначе дельта. Границы — только по целым
    // записям (снимок даёт валидный префикс сканером сборщика).
    private static async Task<Result> PutDeltaAsync(
        IBackupS3 s3, WalSessionEvents events, WalReceiverOptions o,
        string segmentName, byte[] prefix, ulong startLsn, ulong endLsn,
        TimeSpan retry, CancellationToken ct, PoolExhaustionFlag exhaustion)
    {
        if (WalFileName.TryParse(segmentName) is not { } segment)
            return Result.Failed(new ApplicationException($"delta: bad segment name {segmentName}"));
        var segmentStart = WalDeltaName.SegmentStartOf(segment);
        // coverage == 0 — «покрытия нет»: базовый кусок пишется от НАЧАЛА
        // сегмента (абсолютный LSN; spec решение №2 — Size == end − start).
        var start = startLsn == 0 ? segmentStart : startLsn;
        var from = (int)(start - segmentStart);
        var to = (int)(endLsn - segmentStart);
        if (from < 0 || to > prefix.Length || from >= to)
            return Result.Failed(new ApplicationException(
                $"delta: range [{startLsn:x}-{endLsn:x}) outside the snapshot of {segmentName}"));

        var bytes = prefix[from..to];
        var md5Hex = Convert.ToHexStringLower(MD5.HashData(bytes));
        var key = $"{o.Cluster}/{o.Shard}/wal/" +
                  WalDeltaName.Key(segment, start, endLsn, o.ChainLife, o.IsMasterSource, md5Hex);
        return await PutDeltaWithRetryAsync(s3, events, o, key, bytes, md5Hex, retry, ct, exhaustion);
    }

    // Put дельта-куска с Content-MD5 (t15, spec §4.2): петля бюджета/ретраев та же,
    // что у PutWithRetryAsync, но гварда чужой цепочки НЕТ — куски не гвардятся
    // (пересечения разрешает сборка байтовой сверкой, spec решение №12).
    private static async Task<Result> PutDeltaWithRetryAsync(
        IBackupS3 s3, WalSessionEvents events, WalReceiverOptions o,
        string key, byte[] data, string md5Hex, TimeSpan retry, CancellationToken ct,
        PoolExhaustionFlag exhaustion)
    {
        var callBudget = TimeSpan.FromTicks(PutAttemptTimeout.Ticks * (o.S3Endpoints.Count + 1));
        var attempt = 0;
        while (true)
        {
            attempt++;
            Result put;
            using (var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                attemptCts.CancelAfter(callBudget);
                put = await s3.PutObjectMd5Async(key, data, md5Hex, attemptCts.Token);
            }

            if (put.IsSuccess)
            {
                events.PutSuccess?.Invoke();
                if (exhaustion.IsSet)
                {
                    // Первый успешный put снимает флаг исчерпания (arch/19 §3).
                    exhaustion.IsSet = false;
                    events.PoolExhaustedChanged?.Invoke(false);
                }
                return put;
            }

            events.PutError?.Invoke();
            if (attempt > 1)
                events.PutRetry?.Invoke();
            if (put.Error!.Message.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal) && !exhaustion.IsSet)
            {
                exhaustion.IsSet = true;
                events.PoolExhaustedChanged?.Invoke(true);
            }
            await Task.Delay(retry, ct);
        }
    }

    private static async Task<Result> PutWithRetryAsync(
        IBackupS3 s3, WalSessionEvents events, WalReceiverOptions o,
        string key, byte[] data, string sha256, TimeSpan retry, CancellationToken ct,
        PoolExhaustionFlag exhaustion, bool useGuard = true)
    {
        var callBudget = TimeSpan.FromTicks(PutAttemptTimeout.Ticks * (o.S3Endpoints.Count + 1));
        var attempt = 0;
        while (true)
        {
            attempt++;
            Result put;
            using (var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                attemptCts.CancelAfter(callBudget);
                // Гвард чужой цепочки (arch/19 §3) — ТОЛЬКО на финальных объектах
                // wal/<segment>: .partial кладётся обычным put-ом (правило
                // «длиннее выигрывает» исключает конкурирующие байты — конфликт
                // невозможен по построению).
                put = useGuard && s3 is IWalSegmentGuard guard
                    ? await guard.PutWalSegmentAsync(key, data, sha256, attemptCts.Token)
                    : await s3.PutObjectAsync(key, data, sha256, attemptCts.Token);
            }

            if (put.IsSuccess)
            {
                events.PutSuccess?.Invoke();
                if (exhaustion.IsSet)
                {
                    // Первый успешный put снимает флаг исчерпания (arch/19 §3).
                    exhaustion.IsSet = false;
                    events.PoolExhaustedChanged?.Invoke(false);
                }
                return put;
            }

            events.PutError?.Invoke();
            if (WalArchiveConflict.Is(put))
                return put; // permanent: сессия завершится failed_permanent
            if (attempt > 1)
                events.PutRetry?.Invoke();
            if (put.Error!.Message.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal) && !exhaustion.IsSet)
            {
                exhaustion.IsSet = true;
                events.PoolExhaustedChanged?.Invoke(true);
            }
            await Task.Delay(retry, ct);
        }
    }

    /// <summary>Ре-доклад инвентаря СЕГМЕНТОВ (arch/19 §3): закрытые сегменты
    /// pg_wal своей ноды, продолжающие непрерывную часть S3-цепочки, кладутся в
    /// wal/ отсутствующие — в возрастающем порядке позиций, до первой дыры или
    /// отказа put. Покрывает исход «WAL физически есть на ноде, но приёмник его
    /// не стримил»: промоут/restore открывает новый TLI вне окна сессии, хвост
    /// S3 остаётся на прежнем таймлайне — без ре-доклада сегменты между хвостом
    /// архива и первым сегментом нового TLI не попадают в S3 никогда (валидатор
    /// restore видел бы реальный разрыв). Граница прохода — конец СОБСТВЕННОГО
    /// доставленного прогресса (deliveredSegment, arch/19 §3), при его
    /// отсутствии — хвост S3: локально закрытые сегменты между своим
    /// прогрессом и хвостом чужой ноды ДОНОСЯТСЯ (для приёмника «числится на
    /// чужой ноде» не значит «доставлено» — дубль того же имени идемпотентен);
    /// позиция, уже заархивированная ДРУГИМ именем (tli-ветвление, mixedness
    /// spec §3.3), проход останавливает — вторая копия позиции недопустима.
    /// При нескольких TLI на одну позицию грузится НОВЕЙШИЙ (живая ветка
    /// ноды). Открытый (текущий) сегмент не грузится (байты дописываются —
    /// объект был бы неполным), как и файл не штатного размера сегмента.
    /// Идемпотентно; отказ/обрыв — diagnostic-log, следующий проход повторит
    /// (цикл подключения или период живой сессии).</summary>
    public static async Task RedeliverWalInventoryAsync(
        IBackupS3 s3, string cluster, string shard, string pgWalDir,
        uint currentTli, ulong currentLsn,
        Action<string>? diagnostic, CancellationToken ct,
        Action<long>? delivered = null,
        ulong? deliveredPosition = null,
        Action? failed = null)
    {
        // 1. Список wal/ (соло-листинг первой доступной read-ноды) — имена
        //    позиций для mixedness-защиты шага 3. Граница прохода — позиция
        //    СОБСТВЕННОГО доставленного прогресса, при её отсутствии — конец
        //    непрерывной части цепочки S3 (прежняя семантика).
        var listing = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listing.IsSuccess)
        {
            diagnostic?.Invoke($"wal inventory: list wal/ failed: {listing.Error!.Message.Split('\n')[0]}");
            failed?.Invoke(); // пул недостижим — сигнал фактам (idle-детект, arch/19 §3)
            return; // list не прошёл — ре-доклад отложен до следующего прохода
        }
        // Позиция бывает занята ДВУМЯ именами после restore: сегменты откатанной
        // ветки остались в wal/, живая ветка пишет ту же позицию своим TLI
        // (TLI-ветвление). ToDictionary по позиции бросал duplicate key и убивал
        // проход ре-доклада на весь цикл; на одну позицию остаётся НОВЕЙШИЙ TLI
        // (живая ветка — тот же выбор, что у локального инвентаря шага 2).
        var archivedNames = new Dictionary<long, string>();
        foreach (var segment in listing.Value
                     .Select(o => WalFileName.TryParse(o.Name))
                     .OfType<WalFileName>())
        {
            var pos = (long)(segment.Log * WalFileName.SegsPerLog + segment.Seg);
            if (archivedNames.TryGetValue(pos, out var archived)
                && WalFileName.TryParse(archived) is { } archivedFile
                && archivedFile.Tli >= segment.Tli)
                continue;
            archivedNames[pos] = segment.Name;
        }
        long boundaryPos;
        if (deliveredPosition is { } ownPos)
            boundaryPos = (long)ownPos; // свой доставленный сегмент — проход выше него
        else
        {
            var tailName = archivedNames.Count == 0 ? null : ChainTailOf(archivedNames);
            if (tailName is null
                || WalFileName.TryParse(tailName) is not { } parsedTail)
                return; // пустой префикс/незакреплённая цепочка — продолжать нечего
            boundaryPos = (long)(parsedTail.Log * WalFileName.SegsPerLog + parsedTail.Seg);
        }

        // 2. Локальный инвентарь: PG пишет имена ВЕРХНИМ hex, канон ключа
        //    wal/ — строчный (Name). Закрытый = позиция строго ниже текущего
        //    сегмента записи (его отдаёт IDENTIFY_SYSTEM вызывающего).
        var currentPos = (long)(currentLsn / (ulong)WalFileName.SegmentBytes);
        var local = new Dictionary<long, (WalFileName Segment, string Path)>();
        try
        {
            foreach (var path in Directory.EnumerateFiles(pgWalDir))
            {
                var name = Path.GetFileName(path);
                if (WalFileName.IsPartial(name) || WalFileName.TryParse(name) is not { } segment)
                    continue; // .history/.partial/мусор — не сегменты
                var pos = (long)(segment.Log * WalFileName.SegsPerLog + segment.Seg);
                if (pos >= currentPos)
                    continue; // открытый/будущий сегмент — байты ещё дописываются
                var length = new FileInfo(path).Length;
                if (length != WalFileName.SegmentBytes)
                {
                    // Не штатный размер — усечённый/чужой файл; грузить нельзя
                    // (объект под именем целого сегмента испортил бы архив).
                    diagnostic?.Invoke(
                        $"wal inventory: local {name} has {length} bytes " +
                        $"(expected {WalFileName.SegmentBytes}) — skipped");
                    continue;
                }
                // Несколько TLI на одну позицию — живая ветка ноды (новейший TLI).
                if (local.TryGetValue(pos, out var prev) && prev.Segment.Tli >= segment.Tli)
                    continue;
                local[pos] = (segment, path);
            }
        }
        catch (Exception e)
        {
            diagnostic?.Invoke(
                $"wal inventory: enumerate {pgWalDir} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            return; // ФС ноды недоступна — не ошибка приёмника
        }

        // 3. Заливка строго по возрастанию позиций от границы+1: первый
        //    отсутствующий локально сегмент останавливает проход (дыра в
        //    локальной цепи не заполняется сегментами выше неё — цепочка в
        //    архиве не должна перепрыгивать позиции). Позиция, уже лежащая в
        //    архиве ДРУГИМ именем (tli-ветвление), тоже останавливает проход:
        //    дубль позиции под вторым именем недопустим (mixedness); то же
        //    имя — идемпотентный повтор put (донос на свою write-цель).
        var nextPos = boundaryPos + 1;
        var uploaded = 0;
        while (local.TryGetValue(nextPos, out var candidate))
        {
            if (archivedNames.TryGetValue(nextPos, out var archivedName)
                && archivedName != candidate.Segment.Name)
            {
                diagnostic?.Invoke(
                    $"wal inventory: position of {candidate.Segment.Name} is already archived as " +
                    $"{archivedName} (tli branch point) — pass stopped, no second copy of the position");
                return;
            }

            byte[] data;
            try
            {
                data = await File.ReadAllBytesAsync(candidate.Path, ct);
            }
            catch (Exception e)
            {
                diagnostic?.Invoke(
                    $"wal inventory: read {candidate.Segment.Name} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                return;
            }

            var put = s3 is IWalSegmentGuard guard
                ? await guard.PutWalSegmentAsync(
                    $"{cluster}/{shard}/wal/{candidate.Segment.Name}", data,
                    Convert.ToHexString(SHA256.HashData(data)), ct)
                : await s3.PutObjectAsync(
                    $"{cluster}/{shard}/wal/{candidate.Segment.Name}", data,
                    Convert.ToHexString(SHA256.HashData(data)), ct);
            if (!put.IsSuccess)
            {
                diagnostic?.Invoke(
                    $"wal inventory: put {candidate.Segment.Name} failed: {put.Error!.Message.Split('\n')[0]}");
                // Конфликт содержимого — чужая инкарнация: проход не продолжается
                // (иначе сегменты выше легли бы миксом поверх чужой цепочки).
                if (WalArchiveConflict.Is(put))
                    return;
                failed?.Invoke(); // пул недостижим — сигнал фактам (idle-детект, arch/19 §3)
                return; // следующий проход повторит с той же позиции
            }

            delivered?.Invoke(data.Length);
            uploaded++;
            nextPos++;
        }

        if (uploaded > 0)
            diagnostic?.Invoke(
                $"wal inventory: redelivered {uploaded} local closed segment(s) above the pass boundary pos {boundaryPos}");
    }

    /// <summary>Имя хвостового сегмента непрерывной части цепочки по списку
    /// имён (минимальный сегмент → WalChain.Check — непрерывность без
    /// перезапуска от нового TLI). Отличие от S3TailResolver осознанно:
    /// стартовая позиция стрима ОБЯЗАНА быть на живой ветке (CheckWithRestart —
    /// иначе старт окажется впереди flush-позиции сервера, arch/pgnode/03 §3),
    /// а здесь граница прохода остаётся на ПРЕЖНЕЙ непрерывной части: сегменты
    /// откатанной ветки дальше не доносятся (их позиции заняты, ветка мертва —
    /// проход остановит mixedness-гейт), но и перепрыгнуть на новую ветку
    /// нельзя — иначе закрытые локальные сегменты между своим прогрессом и
    /// новым TLI были бы потеряны. При отсутствии собственного доставленного
    /// прогресса проход начинается от этой границы (шаг 3 догонит живую ветку
    /// по возрастанию позиций).</summary>
    private static string? ChainTailOf(IReadOnlyDictionary<long, string> namesByPos)
    {
        var startName = namesByPos[namesByPos.Keys.Min()];
        if (WalFileName.TryParse(startName) is not { } start)
            return null;
        return WalChain.Check(start, namesByPos.Values).LastSegment?.Name;
    }

    /// <summary>Ре-доклад инвентаря.history (arch/19 §3): перечисляет
    /// .history-файлы pg_wal своей ноды и кладёт в S3 wal/ отсутствующие —
    /// страховка TLI-переходов вне окна стрима (промоут, пока приёмник не
    /// стримил: после промоута файл лежит в pg_wal нового мастера, после
    /// restore — в pgdata восстановленной ноды). Вне hot-path: один list
    /// (перебор целей до успешного) + точечные одиночные put с SHA256;
    /// идемпотентно (put только отсутствующих по list); любой отказ —
    /// diagnostic-log, доставит следующий цикл подключения; фактов в
    /// статус-ключ не пишет. delivered — опц. колбэк байт доставленных
    /// history-объектов (counter bytes_delivered, arch/18 §2.8).</summary>
    public static async Task RedeliverHistoryInventoryAsync(
        IBackupS3 s3, string cluster, string shard, string pgWalDir,
        Action<string>? diagnostic, CancellationToken ct,
        Action<long>? delivered = null,
        Action? failed = null)
    {
        List<string> local;
        try
        {
            // PG пишет .history как "<tli:x8>.history" (8 hex-цифр).
            local = Directory.EnumerateFiles(pgWalDir, "*.history")
                .Select(Path.GetFileName)
                .OfType<string>()
                .Where(n => n.Length == 16
                    && uint.TryParse(n.AsSpan(0, 8),
                        System.Globalization.NumberStyles.HexNumber, null, out _))
                .ToList();
        }
        catch (Exception e)
        {
            // pg_wal недоступен — инвентаря нет, не ошибка приёмника.
            diagnostic?.Invoke(
                $"history inventory: enumerate {pgWalDir} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            return;
        }

        if (local.Count == 0)
            return;

        var listing = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listing.IsSuccess)
        {
            // list не прошёл — ре-доклад отложен до следующего цикла.
            diagnostic?.Invoke(
                $"history inventory: list wal/ failed: {listing.Error!.Message.Split('\n')[0]}");
            failed?.Invoke(); // пул недостижим — сигнал фактам (idle-детект, arch/19 §3)
            return;
        }

        var remote = listing.Value.Select(o => o.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var name in local)
        {
            if (remote.Contains(name))
                continue; // уже в архиве — дубль-put не нужен (идемпотентность)

            try
            {
                var content = await File.ReadAllBytesAsync(Path.Combine(pgWalDir, name), ct);
                var put = s3 is IWalSegmentGuard guard
                    ? await guard.PutWalSegmentAsync(
                        $"{cluster}/{shard}/wal/{name}", content,
                        Convert.ToHexString(SHA256.HashData(content)), ct)
                    : await s3.PutObjectAsync(
                        $"{cluster}/{shard}/wal/{name}", content,
                        Convert.ToHexString(SHA256.HashData(content)), ct);
                if (put.IsSuccess)
                    delivered?.Invoke(content.Length);
                else
                {
                    diagnostic?.Invoke(
                        $"history inventory: put {name} failed: {put.Error!.Message.Split('\n')[0]}");
                    // Конфликт — чужая инкарнация: остановить проход (не миксовать).
                    if (WalArchiveConflict.Is(put))
                        return;
                    failed?.Invoke(); // пул недостижим — сигнал фактам (idle-детект, arch/19 §3)
                }
            }
            catch (Exception e)
            {
                diagnostic?.Invoke(
                    $"history inventory: put {name} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            }
        }
    }
}
