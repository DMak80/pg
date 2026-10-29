using System.Security.Cryptography;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

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

    // Файловые операции НАБОРА полного (PutFileAsync/GetObjectToFileAsync —
    // файлы произвольного размера, до единиц ГиБ): per-попытка бюджет от класса
    // размера, не от сегмента. PutAttemptTimeout выбран под WAL-сегменты
    // 16–24 МиБ — файл набора >~1 ГиБ при канале <70 МиБ/с не укладывается
    // никогда (бесконечный transient-цикл put / недокачанный набор). 300 с:
    // многогигабайтный файл доезжает и при скромном канале (~10 МиБ/с — 3 ГиБ),
    // а «молча принявшая TCP» нода (прецедент R3 выше) по-прежнему не съедает
    // бюджет перебора — фейловер доезжает до следующей цели; заведомо ниже
    // воркеровских бюджетов операции (6/24 ч, arch/19 §6).
    internal static readonly TimeSpan FileAttemptTimeout = TimeSpan.FromSeconds(300);

    // Период пробы роли из стрима (arch/19 §3: смена ролей подхватывается
    // приёмником без участия воркера).
    private static readonly TimeSpan RoleProbePeriod = TimeSpan.FromSeconds(5);

    // pg_wal PGDATA ноды (arch/14 §2.1): fallback-чтение `.history` приёмником
    // из ФС своей ноды (приёмник живёт в контейнере ноды, user=postgres).
    public const string DefaultPgWalDir = "/home/postgres/pgdata/pgroot/data/pg_wal";

    /// <summary>Одна сессия стрима (описание исходов — WalSessionOutcome).</summary>
    public static async Task<WalSessionOutcome> RunSessionAsync(
        WalReceiverOptions o,
        IXLogReplicationSource source,
        ISlotPositionReader slots,
        IBackupS3 s3,
        WalSessionEvents events,
        CancellationToken ct,
        TimeSpan? retryDelay = null,
        TimeSpan? roleProbePeriod = null)
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
                        Diagnostic(events, $"start_failed: {started.Error!.Message.Split('\n')[0]}");
                        await Task.Delay(retry, sct);
                        continue;
                    }
                }

                // 4. Приём: сборка сегментов → put (ретрай, backpressure — enumerator не
                //    двигается) → подтверждение ТОЛЬКО после успешного put.
                var assembler = new SegmentAssembler(startAt);
                uint prevTli = 0;
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
                                        await PutWithRetryAsync(
                                            s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Tli:x8}.history",
                                            content,
                                            Convert.ToHexString(SHA256.HashData(content)), retry, sct,
                                            exhaustion);
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
                                await PutWithRetryAsync(
                                    s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Name}",
                                    segment.Data, sha256, retry, sct, exhaustion);
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
                await PutWithRetryAsync(
                    s3, events, o, $"{o.Cluster}/{o.Shard}/wal/{serverTli:x8}.history",
                    content,
                    Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(content)),
                    retry, ct, exhaustion);
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
    private static async Task PutWithRetryAsync(
        IBackupS3 s3, WalSessionEvents events, WalReceiverOptions o,
        string key, byte[] data, string sha256, TimeSpan retry, CancellationToken ct,
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
                put = await s3.PutObjectAsync(key, data, sha256, attemptCts.Token);
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
                return;
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
        Action<long>? delivered = null)
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
                var put = await s3.PutObjectAsync(
                    $"{cluster}/{shard}/wal/{name}", content,
                    Convert.ToHexString(SHA256.HashData(content)), ct);
                if (put.IsSuccess)
                    delivered?.Invoke(content.Length);
                else
                    diagnostic?.Invoke(
                        $"history inventory: put {name} failed: {put.Error!.Message.Split('\n')[0]}");
            }
            catch (Exception e)
            {
                diagnostic?.Invoke(
                    $"history inventory: put {name} failed: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
            }
        }
    }
}
