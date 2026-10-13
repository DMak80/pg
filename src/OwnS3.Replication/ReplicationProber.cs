using OwnS3.Replication.Transport;

namespace OwnS3.Replication;

// Активная проба репликационных линков (глава 07 §2): админ-trigger probe —
// параллельный ping всех (или одного) соседей снапшота targets исходящим
// транспортом (креды/маршрут notifier'а) МИМО outbox: журнал, курсоры,
// fetch-очереди, счётчики и link_online probe не трогает. Отдельный класс —
// тестируем без хоста (по прецеденту ReplicationMaintenance).
public sealed class ReplicationProber(ReplicationTargets targets,
    IReplicationTransport transport, ReplicationOptions options)
{
    // Отчёт probe: active — ≥1 сосед в составе. Failed(UnknownReplicationTargetException)
    // — targetName не из состава: маркер 400 для хендлера (глава 07 §2).
    public async Task<Result<ProbeReport>> ProbeAsync(string? targetName, CancellationToken ct)
    {
        var snapshot = targets.Snapshot;
        if (targetName is not null && snapshot.All(t => t.Name != targetName))
            return Result<ProbeReport>.Failed(new UnknownReplicationTargetException(
                $"цель probe не входит в состав targets: {targetName}"));
        IReadOnlyList<TargetRecord> selected = targetName is null
            ? snapshot
            : snapshot.Where(t => t.Name == targetName).ToList();
        var outcomes = await Task.WhenAll(selected.Select(t => PingQuietAsync(t, ct)));
        return Result<ProbeReport>.Success(new ProbeReport(selected.Count > 0, outcomes));
    }

    // Ветка ping одного соседа: обрыв/таймаут пер-цели — исход false без rttMs
    // (probe отвечает за все цели, глава 07 §2); внешняя отмена ct — проброс наверх.
    private async Task<ProbeOutcome> PingQuietAsync(TargetRecord target, CancellationToken ct)
    {
        var ping = await transport.PingAsync(target,
            TimeSpan.FromMilliseconds(options.ProbeTimeoutMs), ct);
        return ping is { IsSuccess: true, Value: var p }
            ? new ProbeOutcome(target.Name, p.Reachable, p.RttMs)
            : new ProbeOutcome(target.Name, false, null);
    }
}

// Отчёт probe (глава 07 §2): active = «есть соседи», targets — per-target исходы.
public sealed record ProbeReport(bool Active, IReadOnlyList<ProbeOutcome> Targets);

// Исход ping одного соседа: reachable строго по 200; rttMs — при любом
// HTTP-ответе (null при таймауте/обрыве).
public sealed record ProbeOutcome(string Name, bool Reachable, long? RttMs);
