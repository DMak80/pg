using Microsoft.Extensions.Logging;
using OwnS3.Replication.Cursors;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication.Notifier;

// Исполнитель per-target (фон, глава 07 §8): цикл — ReadBatch(watermark+1, окно) →
// пусто: ждать новых (поллинг 500 мс в проде) → POST notifications → 200:
// watermark = acceptedToSeqNo, Advance (персист дебаунсом курсоров §5), сброс бэкоффа;
// 429/ошибка: бэкофф эксп 1 с → BackoffMaxSec (успех — сброс), повтор от watermark —
// at-least-once (дедуп приёмника гасит повторы). Новый сосед (курсор −1) → первая
// отправка от LastSeqNo (конец журнала, §2). Счётчики/лаги — ReplicationMetrics (§10).
public sealed class NotificationDispatcher
{
    // Прод-пауза поллинга новых событий и база бэкоффа (канон §8); internal-параметры — для тестов.
    internal static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(500);
    internal static readonly TimeSpan BackoffBase = TimeSpan.FromSeconds(1);

    private readonly TargetRecord _target;
    private readonly OutboxJournal _journal;
    private readonly TargetCursors _cursors;
    private readonly IReplicationTransport _transport;
    private readonly ReplicationOptions _options;
    private readonly ReplicationMetrics _metrics;
    private readonly string _originHostId;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<NotificationDispatcher> _logger;
    private readonly TimeSpan _pollDelay;
    private readonly TimeSpan _backoffBase;
    private volatile bool _online;

    public NotificationDispatcher(TargetRecord target, OutboxJournal journal, TargetCursors cursors,
        IReplicationTransport transport, ReplicationOptions options, ReplicationMetrics metrics,
        string originHostId, TimeProvider timeProvider, ILogger<NotificationDispatcher> logger)
        : this(target, journal, cursors, transport, options, metrics, originHostId, timeProvider, logger,
              PollDelay, BackoffBase)
    {
    }

    // internal-ctor: тесты задают малые паузы поллинга/бэкоффа (детерминизм без ожиданий).
    internal NotificationDispatcher(TargetRecord target, OutboxJournal journal, TargetCursors cursors,
        IReplicationTransport transport, ReplicationOptions options, ReplicationMetrics metrics,
        string originHostId, TimeProvider timeProvider, ILogger<NotificationDispatcher> logger,
        TimeSpan pollDelay, TimeSpan backoffBase)
    {
        _target = target;
        _journal = journal;
        _cursors = cursors;
        _transport = transport;
        _options = options;
        _metrics = metrics;
        _originHostId = originHostId;
        _timeProvider = timeProvider;
        _logger = logger;
        _pollDelay = pollDelay;
        _backoffBase = backoffBase;
    }

    public TargetRecord Target => _target;

    // Метрика link_online: успешная отправка — 1, отказ/бэкофф — 0.
    public bool Online
    {
        get => _online;
        private set
        {
            _online = value;
            _metrics.SetLinkOnline(_target.Name, value);
        }
    }

    // Пока не cancel (graceful: отмена — спокойный возврат, без исключения).
    public async Task RunAsync(CancellationToken ct)
    {
        // Новый сосед (курсор −1) → первая отправка с текущего конца журнала (§2/§8)
        var watermark = _cursors.GetAccepted(_target.Name);
        if (watermark < 0)
        {
            watermark = _journal.LastSeqNo;
            _logger.LogInformation("Новый сосед {Target}: рассылка с конца журнала (seqNo={LastSeqNo})",
                _target.Name, watermark);
        }

        var backoff = _backoffBase;
        try
        {
            await RunLoopAsync(watermark, backoff, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // graceful stop — штатный выход
        }
    }

    private async Task RunLoopAsync(long watermark, TimeSpan backoff, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Метрики окна/лага (глава 07 §10): queue_depth = LastSeqNo − watermark;
            // delivery_lag = возраст старейшего неподтверждённого события
            var lastSeq = _journal.LastSeqNo;
            _metrics.SetQueueDepth(_target.Name, Math.Max(0, lastSeq - watermark));
            _metrics.SetDeliveryLagSeconds(_target.Name, DeliveryLagSeconds(watermark));

            var batch = _journal.ReadBatch(watermark, _options.InflightWindow);
            if (batch.Count == 0)
            {
                await Task.Delay(_pollDelay, ct);
                continue;
            }

            var send = await _transport.SendNotificationsAsync(_target,
                new NotificationBatchRequest(_originHostId, batch), ct);
            if (send is { IsSuccess: true, Value: var ack })
            {
                // ACK приёма: watermark двигается (только вперёд), персист дебаунсом (§5)
                watermark = Math.Max(watermark, ack);
                _cursors.Advance(_target.Name, ack);
                Online = true;
                _metrics.Notified(_target.Name, "accepted");
                backoff = _backoffBase; // успех — сброс бэкоффа
                continue;
            }

            // 429/ошибка: приёмник не принял — бэкофф, повтор от watermark (at-least-once)
            Online = false;
            _metrics.Notified(_target.Name, "backoff");
            _metrics.Retried(_target.Name);
            _logger.LogInformation("Сосед {Target} не принял уведомления ({Error}) — бэкофф {Backoff}",
                _target.Name, send.Error!.Message, backoff);
            await Task.Delay(backoff, ct);
            backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2,
                TimeSpan.FromSeconds(_options.BackoffMaxSec).Ticks));
        }
    }

    // Возраст старейшего события после watermark (сек; 0 — хвост подтверждён).
    // PeekFirstAfter — дешёвый lookup (сегменты до watermark пропускаются целиком).
    private double DeliveryLagSeconds(long watermark)
    {
        var oldest = _journal.PeekFirstAfter(watermark);
        if (oldest is null)
            return 0;
        var ageMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds() - oldest.ModTimeUnixMs;
        return Math.Max(0, ageMs) / 1000.0;
    }
}
