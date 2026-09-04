using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AdminPanel.Probes.Kafka;

/// <summary>
/// Кэш нативных kafka-клиентов пробы (t11): один AdminClient/Consumer на
/// (bootstrap, user, password) вместо «клиент на каждый вызов» — churn
/// rd_kafka-инстансов и LongRunning poll-потоков на недоступных брокерах
/// съедал ~99% ядра панели (инцидент 2026-09-02). Пересоздание — только при
/// смене endpoints/кредов (другой ключ) или Invalidate после фейла; Dispose
/// заменяемого клиента — в фоне, не в горячем пути пробы (AdminClient.Dispose
/// синхронно ждёт poll-поток и блокировал threadpool).
/// </summary>
public sealed class KafkaClientCache(ILogger<KafkaClientCache>? logger = null) : IDisposable
{
    // Профиль librdkafka для проб (t11): дефолтные 100 мс retry/reconnect-
    // backoff при мгновенном connection-refusal превращаются в reconnect-шторм
    // (FAIL/ERROR-лог каждую секунду, poll-потоки жгут ядро) — затыкаем до ≥1 c.
    private const int BackoffMs = 1000;
    private const int BackoffMaxMs = 10000;

    // Техническая группа консьюмера оффсетов (group.id обязателен для
    // QueryWatermarkOffsets): оффсеты не коммитятся и не читаются.
    private const string WatermarkGroup = "adminpanel-probe";

    private readonly object _gate = new();
    private readonly Dictionary<(string Bootstrap, string User, string Password, string? CaPem), Entry> _entries = [];

    // Сколько нативных клиентов создано за жизнь кэша — метрика churn'а
    // (интеграционный тест t11 строит на ней границы).
    public int CreatedClients { get; private set; }

    public IAdminClient GetAdmin(string bootstrap, string user, string password, string? caPem)
    {
        lock (_gate)
        {
            var entry = GetOrCreateEntry(bootstrap, user, password, caPem);
            if (entry.Admin is not null)
                return entry.Admin;

            CreatedClients++;
            return entry.Admin = new AdminClientBuilder(BaseAdminConfig(bootstrap, user, password, caPem))
                // Лог librdkafka — на Debug панели: в инциденте FAIL/ERROR-строки
                // «5/5 brokers are down» сыпались в stdout каждую секунду.
                .SetLogHandler((_, message) => logger?.LogDebug("rdkafka: {Message}", message.Message))
                .Build();
        }
    }

    public IConsumer<Ignore, Ignore> GetConsumer(string bootstrap, string user, string password, string? caPem)
    {
        lock (_gate)
        {
            var entry = GetOrCreateEntry(bootstrap, user, password, caPem);
            if (entry.Consumer is not null)
                return entry.Consumer;

            CreatedClients++;
            return entry.Consumer = new ConsumerBuilder<Ignore, Ignore>(
                    new ConsumerConfig(BaseAdminConfig(bootstrap, user, password, caPem))
                    {
                        GroupId = WatermarkGroup,
                    })
                .SetErrorHandler((_, _) => { }) // ошибки партиции — выше по исключению вызова
                .SetLogHandler((_, message) => logger?.LogDebug("rdkafka: {Message}", message.Message))
                .Build();
        }
    }

    // Фейл пробы — клиент не переиспользуется: следующая попытка получит свежий
    // (вызов не в горячем пути — окно повтора держит backoff KafkaProbeLoop).
    public void Invalidate(string bootstrap, string user, string password, string? caPem)
    {
        Entry? removed;
        lock (_gate)
        {
            var key = (bootstrap, user, password, caPem);
            if (!_entries.Remove(key, out removed))
                return;
        }

        DisposeInBackground(removed);
    }

    public void Dispose()
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        // Выключение/тест — детерминированно: клиенты с backoff-настройками
        // не штурмуют, poll-потоки выходят из Wait быстро.
        foreach (var entry in removed)
            DisposeEntry(entry);
    }

    private Entry GetOrCreateEntry(string bootstrap, string user, string password, string? caPem)
    {
        var key = (bootstrap, user, password, caPem);
        if (!_entries.TryGetValue(key, out var entry))
            _entries[key] = entry = new Entry();
        return entry;
    }

    // Замена живого (возможно, штормящего) клиента — только фон: Dispose ждёт
    // poll-поток и не должен блокировать тик пробы/threadpool.
    private void DisposeInBackground(Entry entry)
        => Task.Run(() => DisposeEntry(entry));

    private static void DisposeEntry(Entry entry)
    {
        try
        {
            entry.Admin?.Dispose();
            entry.Consumer?.Dispose();
        }
        catch
        {
            // Нативный клиент уже мёртв — замена всё равно создаст свежий.
        }
    }

    private static AdminClientConfig BaseAdminConfig(string bootstrap, string user, string password, string? caPem)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl, // t03: дискавери-канон arch/15 §5
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
            RetryBackoffMs = BackoffMs,
            ReconnectBackoffMs = BackoffMs,
            ReconnectBackoffMaxMs = BackoffMaxMs,
        };
        if (caPem is not null)
            config.Set("ssl.ca.pem", caPem); // доверие per-cluster CA (librdkafka >= 1.5)
        return config;
    }

    private sealed class Entry
    {
        public IAdminClient? Admin;
        public IConsumer<Ignore, Ignore>? Consumer;
    }
}
