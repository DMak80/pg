using KafkaWorker.Core;

namespace KafkaWorker.App.HealthChecks;

/// <summary>
/// Грань наблюдаемости фонового сервиса (паттерн Puzzle): цикл отдаёт
/// Inited (запущен), Working (жив), StatusError (последняя ошибка тика).
/// </summary>
public interface IHealthCheckService
{
    bool Inited { get; }

    bool Working { get; }

    Result StatusError { get; }
}
