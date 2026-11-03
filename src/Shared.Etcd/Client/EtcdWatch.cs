using Shared.Core;

namespace Shared.Etcd.Client;

// Тип watch-события etcd (proto: PUT=0, DELETE=1; grpc-gateway пишет имя).
public enum EtcdWatchEventType
{
    Put,
    Delete,
}

// Декодированное watch-событие: gateway снял base64 (value — null при DELETE),
// modRevision — long (в protojson int64 приходит decimal-строкой).
public sealed record EtcdWatchEvent(string Key, string? Value, long ModRevision, EtcdWatchEventType Type);

// Исход watch-потока (обрабатывает вызывающий цикл — arch/17 S10).
public enum EtcdWatchStopReason
{
    // Отмена потребителем (StopAsync/ct) — штатное завершение подписки.
    Cancelled,

    // Поток закрыт/разорван (сервер, транспорт): переподключение к следующему
    // endpoint с последнего применённого revision.
    Closed,

    // compact_revision>0 или canceled=true: история недоступна — повторная
    // первичная загрузка (кэш пересбрасывается целиком).
    Compacted,
}

// Итог попытки watch: CompactRevision значим только для Compacted.
public sealed record EtcdWatchStop(EtcdWatchStopReason Reason, long CompactRevision = 0);

// Watch-возможность gateway — отдельный интерфейс (НЕ член IEtcdGateway):
// watch — долгоживущий поток, per-attempt бюджет failover его разорвал бы,
// поэтому endpoint выбирает ротацией сам вызывающий цикл (arch/17 S7/S10).
public interface IEtcdWatchApi
{
    // Подписка на события префикса с startRevision (последний применённый + 1):
    // каждое событие — колбэку потребителя, по порядку revision. Возвращает
    // исход потока; разрывы соединения — штатный случай, не ошибка.
    Task<Result<EtcdWatchStop>> WatchAsync(
        string endpoint,
        string prefix,
        long startRevision,
        Func<EtcdWatchEvent, Task> onEvent,
        CancellationToken ct);
}
