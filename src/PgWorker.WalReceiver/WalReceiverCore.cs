using System.Security.Cryptography;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Главный цикл WAL-приёмника (t27, arch/19 §3, spec §3.1): подключение с
/// сверкой wal_segment_size, резолв старта от хвоста S3 (пустой префикс — от
/// restart_lsn слота), приём XLogData → сборка сегментов → put в S3 →
/// подтверждение слота ТОЛЬКО после успешного put (инвариант RPO: write=flush=
/// applied = конец последнего доставленного сегмента, никогда дальше).
/// Backpressure: при невозможности put чтение потока не двигается — сервер ждёт
/// подтверждения, слот удерживает WAL. Обрывы источника — transient
/// (переподключение с повторным резолвом хвоста); permanent — коды выхода:
/// 3 = segment-size mismatch, 4 = слот отсутствует, 6 = разрыв LSN протокола,
/// 5 (в Program) = невалидный env; 0 = чистая отмена.</summary>
public sealed class WalReceiverCore
{
    private static readonly TimeSpan DefaultRetry = TimeSpan.FromSeconds(5);

    // R3 (t36-разбор AC4): пер-попытка таймаут put — остановленная/перегруженная
    // нода пула «молча принимает» TCP и не отвечает — ПЕР-ЦЕЛЬНЫЙ бюджет: это
    // значение уходит requestTimeout'ом в КАЖДУЮ цель PoolBackupS3 (Program),
    // чтобы висящая первая цель НЕ съедала бюджет перебора и фейловер доезжал
    // до следующей ноды списка (прецедент AC4: put к ноде под mesh-нагрузкой
    // висел до отмены попытки — вторая цель не получала ни одной попытки).
    internal static readonly TimeSpan PutAttemptTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan StreamMarkerPeriod = TimeSpan.FromSeconds(5);

    public static async Task<int> RunAsync(
        WalReceiverOptions o,
        IXLogReplicationSource source,
        ISlotPositionReader slots,
        IBackupS3 s3,
        TextWriter stdout,
        CancellationToken ct,
        TimeSpan? retryDelay = null)
    {
        var retry = retryDelay ?? DefaultRetry;
        string? lastDelivered = null;
        var lastStreamMarker = DateTime.MinValue;

        async Task Emit(string marker)
        {
            await stdout.WriteLineAsync(marker);
        }

        try
        {
            await Emit(WalReceiverMarkers.Starting());
            while (true)
            {
                // 1. Подключение + сверка wal_segment_size: расхождение — permanent (математика
                //    имён сегментов не должна молча врать), прочие сбои — transient.
                var opened = await source.OpenAsync(ct);
                if (!opened.IsSuccess)
                {
                    if (opened.Error!.Message.Contains("wal_segment_size"))
                    {
                        await Emit(WalReceiverMarkers.Result(false, opened.Error.Message, lastDelivered));
                        return 3;
                    }

                    // Диагностика transient-цикла подключения: причина обязана быть
                    // видимой в логе контейнера без перезапуска (R3 «не молчание»);
                    // stdout — протокол воркера, ошибка идёт в stderr.
                    await Console.Error.WriteLineAsync(
                        $"open_failed: {opened.Error.Message.Split('\n')[0]}");
                    await Task.Delay(retry, ct);
                    continue;
                }

                // 2. Стартовая позиция: хвост S3 (позиция + имя хвостового сегмента —
                //    вход tli-процедуры при 58P01); транспортный сбой резолва — ретрай;
                //    пустой префикс — restart_lsn слота; слота нет — permanent (код 4).
                WalTail? walTail = null;
                while (walTail is null)
                {
                    var resolved = await S3TailResolver.ResolveTailAsync(s3, o.Cluster, o.Shard, ct);
                    if (resolved.IsSuccess && resolved.Value is { } position)
                    {
                        walTail = position;
                        break;
                    }

                    if (resolved.IsSuccess)
                        break; // пустой префикс — старт от слота

                    await Task.Delay(retry, ct);
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
                    var restart = await slots.ReadRestartLsnAsync(o.Slot, ct);
                    if (!restart.IsSuccess)
                    {
                        await Emit(WalReceiverMarkers.Result(false, restart.Error!.Message, lastDelivered));
                        return 4;
                    }

                    startAt = restart.Value;
                    tailSegment = null;
                }

                // 3. START_REPLICATION с явной стартовой позицией; отказ 58P01 — tli-процедура
                //    (досыпка старого таймлайна / прыжок на текущий) вместо retry. Процедура
                //    стартовала — приём продолжается от её позиции; не стартовала — обычный
                //    transient-цикл. tailSegment == null (пустой префикс — старт от
                //    restart_lsn слота) — процедура неприменима (вход не определён): retry-цикл.
                //    Диагностика любой неуспешной ветки старта — start_failed в stderr (R3
                //    «не молчание»: текст WalStartRefused несёт исходную ошибку сервера).
                var started = await source.StartReplicationAsync(o.Slot, startAt, null, ct);
                if (!started.IsSuccess)
                {
                    var resumed = started.Error is WalStartRefused && tailSegment is not null
                        ? await TliProcedureAsync(o, source, s3, stdout, startAt, tailSegment, retry, ct)
                        : Result<ulong?>.Success(null);
                    if (resumed is { IsSuccess: true, Value: { } procedureStart })
                    {
                        startAt = procedureStart;
                    }
                    else
                    {
                        await Console.Error.WriteLineAsync(
                            $"start_failed: {started.Error!.Message.Split('\n')[0]}");
                        await Task.Delay(retry, ct);
                        continue;
                    }
                }

                // 4. Приём: сборка сегментов → put (ретрай, backpressure — enumerator не
                //    двигается) → подтверждение ТОЛЬКО после успешного put.
                var assembler = new SegmentAssembler(startAt);
                uint prevTli = 0;
                try
                {
                    await foreach (var chunk in source.Stream.WithCancellation(ct))
                    {
                        IReadOnlyList<ClosedSegment> closedSegments;
                        try
                        {
                            closedSegments = assembler.Append(chunk);
                        }
                        catch (ApplicationException e)
                        {
                            // Разрыв LSN — протокол физической репликации дыр не даёт:
                            // это баг транспорта, не transient — невосстановимо (код 6).
                            await Emit(WalReceiverMarkers.Result(false, e.Message, lastDelivered));
                            return 6;
                        }

                        foreach (var segment in closedSegments)
                        {
                            // 5. TLI-переход (смена TLI относительно доставленного; первый
                            //    сегмент цепочки — инициализация, истории для tli=1 не
                            //    существует): .history загружается обязательно (PITR через
                            //    смену timeline); рантайм без TIMELINE_HISTORY — маркер
                            //    history_missing, fallback — контроль воркера.
                            if (prevTli != 0 && segment.Tli != prevTli)
                            {
                                var history = await source.ReadTimelineHistoryAsync(segment.Tli, ct);
                                if (history is { IsSuccess: true } && history.Value is not null)
                                {
                                    await PutWithRetryAsync(
                                        s3, stdout, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Tli:x8}.history",
                                        history.Value,
                                        Convert.ToHexString(SHA256.HashData(history.Value)), retry, ct);
                                }
                                else
                                {
                                    await Emit(WalReceiverMarkers.HistoryMissing(segment.Tli));
                                }
                            }

                            prevTli = segment.Tli;

                            // 6. Доставка сегмента: sha256-hex — серверная сверка целостности
                            //    (один расчёт: маркер и put используют один хэш).
                            await Emit(WalReceiverMarkers.Delivering(segment.Name));
                            var sha256 = Convert.ToHexString(SHA256.HashData(segment.Data));
                            await PutWithRetryAsync(
                                s3, stdout, o, $"{o.Cluster}/{o.Shard}/wal/{segment.Name}",
                                segment.Data, sha256, retry, ct);

                            // 7. ЕДИНСТВЕННОЕ место подтверждения слота — после успешного put.
                            var confirmed = await source.SetConfirmedAsync(segment.EndLsn, ct);
                            if (confirmed.IsSuccess)
                            {
                                lastDelivered = segment.Name;
                                await Emit(WalReceiverMarkers.Heartbeat(segment.EndLsn));
                            }
                        }

                        // Прогресс-маркер — не чаще 1/5 c.
                        if (DateTime.UtcNow - lastStreamMarker >= StreamMarkerPeriod)
                        {
                            lastStreamMarker = DateTime.UtcNow;
                            await Emit(WalReceiverMarkers.Streaming(assembler.LastAppendedLsn));
                        }
                    }
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    // Обрыв источника (исключение из Stream) — поглощается ниже.
                    // Диагностика причины обрыва — в stderr (тихий обрыв делал цикл
                    // ненаблюдаемым: только «starting» без причины, R3 «не молчание»).
                    await Console.Error.WriteLineAsync(
                        $"stream_broken: {e.GetType().Name}: {e.Message.Split('\n')[0]}");
                }

                // Поток кончился/оборвался (сервер закрыл, транспортный сбой) — transient:
                // переподключение заново (резолв хвоста, START_REPLICATION от хвоста S3).
                await Emit(WalReceiverMarkers.Starting());
                await Task.Delay(retry, ct);
            }
        }
        catch (OperationCanceledException)
        {
            // Чистая отмена (SIGTERM супервиза, стоп воркера): недоставленное
            // удерживается слотом — перезаказывается у источника при следующем старте.
            await Emit(WalReceiverMarkers.Result(true, null, lastDelivered));
            return 0;
        }
    }

    /// <summary>Tli-процедура (58P01, arch/19 §3): досыпка старого таймлайна от
    /// хвоста S3 до точки переключения (CopyDone → переподключение от выросшего
    /// хвоста — зазора нет, RPO=0) или прыжок-fallback на текущий таймлайн от
    /// границы сегмента xlogpos (сегменты (tail .. старт] в S3 не появятся —
    /// осознанный RPO-зазор, маркер tli_jump). Возвращает Success(стартовая LSN) —
    /// старт выполнен, приём продолжается штатным контуром (assembler → put →
    /// confirm; подтверждение слота ТОЛЬКО после успешного put — инвариант RPO не
    /// меняется); Success(null) — процедура не стартовала (transient: обычный
    /// retry-цикл). КАЖДЫЙ старт процедуры выполняется на СВЕЖЕМ replication-
    /// соединении: отказ 58P01 гасит прежнее (транспорт), поэтому перед досыпкой
    /// и перед прыжком — OpenAsync. Идемпотентна: гонки повторного промоута
    /// конвергентны — каждая итерация начинается IDENTIFY_SYSTEM, счётчиков
    /// попыток нет; старт с историческим TIMELINE безопасен (сервер отдаёт его
    /// до конца ветки).</summary>
    private static async Task<Result<ulong?>> TliProcedureAsync(
        WalReceiverOptions o, IXLogReplicationSource source, IBackupS3 s3, TextWriter stdout,
        ulong tail, string tailSegment, TimeSpan retry, CancellationToken ct)
    {
        if (WalFileName.TryParse(tailSegment) is not { } tailFile)
            return Result<ulong?>.Success(null); // имя не парсится — retry (защитно)

        // 1. IDENTIFY_SYSTEM: сбой — transient (retry-цикл).
        var identity = await source.IdentifySystemAsync(ct);
        if (!identity.IsSuccess)
        {
            await Console.Error.WriteLineAsync(
                $"tli_procedure: identify failed: {identity.Error!.Message.Split('\n')[0]}");
            return Result<ulong?>.Success(null);
        }
        var (serverTli, xlogPos) = identity.Value;

        // 2. .history нового TLI — ДО досыпки (канон §3: загружается обязательно;
        //    переход в потоке не наблюдается — prevTli сессии инициализируется
        //    заново первым сегментом). TLI 1 истории не имеет — не запрашивается.
        if (serverTli > tailFile.Tli)
        {
            var history = await source.ReadTimelineHistoryAsync(serverTli, ct);
            if (history is { IsSuccess: true } && history.Value is not null)
            {
                await PutWithRetryAsync(
                    s3, stdout, o, $"{o.Cluster}/{o.Shard}/wal/{serverTli:x8}.history",
                    history.Value,
                    Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(history.Value)),
                    retry, ct);
            }
            else
            {
                await stdout.WriteLineAsync(WalReceiverMarkers.HistoryMissing(serverTli));
            }
        }

        // 3. Досыпка (только при T сервера > TLI хвоста): стрим старого таймлайна
        //    от хвоста до точки переключения — байты удерживаются слотом на ноде;
        //    повторный 58P01 (удержание потеряно) — прыжок ниже.
        if (serverTli > tailFile.Tli)
        {
            await stdout.WriteLineAsync(WalReceiverMarkers.TliBackfill(tailSegment, serverTli));
            await Console.Error.WriteLineAsync(
                $"tli_procedure: backfill segment {tailSegment} (tli {tailFile.Tli}) up to tli {serverTli} switch point");
            var openedBackfill = await source.OpenAsync(ct);
            if (!openedBackfill.IsSuccess)
            {
                // Отказ 58P01 погасил стрим-соединение — процедуре нужно свежее;
                // неудача открытия — transient (retry-цикл).
                await Console.Error.WriteLineAsync(
                    $"tli_procedure: open failed: {openedBackfill.Error!.Message.Split('\n')[0]}");
                return Result<ulong?>.Success(null);
            }

            var backfill = await source.StartReplicationAsync(o.Slot, tail, tailFile.Tli, ct);
            if (backfill.IsSuccess)
                return Result<ulong?>.Success(tail);
            if (backfill.Error is not WalStartRefused)
            {
                await Console.Error.WriteLineAsync(
                    $"tli_procedure: backfill start failed: {backfill.Error!.Message.Split('\n')[0]}");
                return Result<ulong?>.Success(null); // transient
            }

            await Console.Error.WriteLineAsync(
                $"tli_procedure: backfill refused (58P01) — old-tli segments recycled, jumping");
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
        await stdout.WriteLineAsync(
            WalReceiverMarkers.TliJump(tailSegment, tailFile.Tli, serverTli, toSegment));
        await Console.Error.WriteLineAsync(
            $"tli_procedure: jump {tailSegment} (tli {tailFile.Tli}) -> {toSegment} (tli {serverTli})");
        var openedJump = await source.OpenAsync(ct);
        if (!openedJump.IsSuccess)
        {
            await Console.Error.WriteLineAsync(
                $"tli_procedure: open failed: {openedJump.Error!.Message.Split('\n')[0]}");
            return Result<ulong?>.Success(null); // transient
        }

        var jump = await source.StartReplicationAsync(o.Slot, segmentStart, serverTli, ct);
        if (jump.IsSuccess)
            return Result<ulong?>.Success(segmentStart);

        await Console.Error.WriteLineAsync(
            $"tli_procedure: jump start failed: {jump.Error!.Message.Split('\n')[0]}");
        return Result<ulong?>.Success(null); // включая повторный 58P01 — новая итерация процедуры
    }

    /// <summary>Put с ретраем (transient-отказ S3): на время ретраев чтение потока
    /// остановлено — backpressure, ничего не теряется, слот удерживает WAL.
    /// Хэш передаёт вызывающий: сегмент уже хэширован для маркера Delivering —
    /// повторный SHA256 16 МиБ на каждый put не считается. t36: исчерпание
    /// списка пула (маркер s3-pool-exhausted в ошибке) — маркер
    /// в stdout перед ретраем (контроль воркера читает хвост лога агента —
    /// алерт backup-pool-unreachable; ретрай-цикл — стрим стоит, слот держит).
    /// R3 «быстрый отказ, не молчание»: пер-ЦЕЛЬНЫЙ таймаут (PutAttemptTimeout —
    /// requestTimeout каждой цели в PoolBackupS3) + бюджет ВЫЗОВА
    /// (цели записи + 1) × таймаут цели: висящая первая цель не отменяет
    /// перебор — фейловер доезжает до живой ноды (прецедент AC4).</summary>
    private static async Task PutWithRetryAsync(
        IBackupS3 s3, TextWriter stdout, WalReceiverOptions o,
        string key, byte[] data, string sha256, TimeSpan retry, CancellationToken ct)
    {
        var callBudget = TimeSpan.FromTicks(PutAttemptTimeout.Ticks * (o.S3Endpoints.Count + 1));
        while (true)
        {
            Result put;
            using (var attempt = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                attempt.CancelAfter(callBudget);
                put = await s3.PutObjectAsync(key, data, sha256, attempt.Token);
            }

            if (put.IsSuccess)
                return;
            if (put.Error!.Message.Contains(S3PoolMarkers.Exhausted, StringComparison.Ordinal))
                await stdout.WriteLineAsync(WalReceiverMarkers.PoolExhausted());
            await Task.Delay(retry, ct);
        }
    }
}
