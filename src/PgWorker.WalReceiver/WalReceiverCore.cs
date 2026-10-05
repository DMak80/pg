using System.Security.Cryptography;
using PgWorker.Backups;

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

                    await Task.Delay(retry, ct);
                    continue;
                }

                // 2. Стартовая позиция: хвост S3; транспортный сбой резолва — ретрай;
                //    пустой префикс — restart_lsn слота; слота нет — permanent (код 4).
                ulong? tail = null;
                while (tail is null)
                {
                    var resolved = await S3TailResolver.ResolveTailAsync(s3, o.Cluster, o.Shard, ct);
                    if (resolved.IsSuccess && resolved.Value is { } position)
                    {
                        tail = position;
                        break;
                    }

                    if (resolved.IsSuccess)
                        break; // пустой префикс — старт от слота

                    await Task.Delay(retry, ct);
                }

                if (tail is null)
                {
                    var restart = await slots.ReadRestartLsnAsync(o.Slot, ct);
                    if (!restart.IsSuccess)
                    {
                        await Emit(WalReceiverMarkers.Result(false, restart.Error!.Message, lastDelivered));
                        return 4;
                    }

                    tail = restart.Value;
                }

                // 3. START_REPLICATION с явной стартовой позицией.
                var started = await source.StartReplicationAsync(o.Slot, tail.Value, ct);
                if (!started.IsSuccess)
                {
                    await Task.Delay(retry, ct);
                    continue;
                }

                // 4. Приём: сборка сегментов → put (ретрай, backpressure — enumerator не
                //    двигается) → подтверждение ТОЛЬКО после успешного put.
                var assembler = new SegmentAssembler(tail.Value);
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
                                        s3, stdout, $"{o.Cluster}/{o.Shard}/wal/{segment.Tli:x8}.history",
                                        history.Value, retry, ct);
                                }
                                else
                                {
                                    await Emit(WalReceiverMarkers.HistoryMissing(segment.Tli));
                                }
                            }

                            prevTli = segment.Tli;

                            // 6. Доставка сегмента: sha256-hex — серверная сверка целостности.
                            await Emit(WalReceiverMarkers.Delivering(segment.Name));
                            var sha256 = Convert.ToHexString(SHA256.HashData(segment.Data));
                            await PutWithRetryAsync(
                                s3, stdout, $"{o.Cluster}/{o.Shard}/wal/{segment.Name}",
                                segment.Data, retry, ct);

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

    /// <summary>Put с ретраем (transient-отказ S3): на время ретраев чтение потока
    /// остановлено — backpressure, ничего не теряется, слот удерживает WAL.</summary>
    private static async Task PutWithRetryAsync(
        IBackupS3 s3, TextWriter stdout, string key, byte[] data, TimeSpan retry, CancellationToken ct)
    {
        var sha256 = Convert.ToHexString(SHA256.HashData(data));
        while (true)
        {
            var put = await s3.PutObjectAsync(key, data, sha256, ct);
            if (put.IsSuccess)
                return;
            await Task.Delay(retry, ct);
        }
    }
}
