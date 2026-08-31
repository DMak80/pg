using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос ленты алертов с фильтрами (arch/03 §1; severity уже провалидирован эндпоинтом).
public sealed record AlertsQuery(AlertSeverity? Severity, string? Kind) : IQuery<IReadOnlyList<AlertDto>>;

// Ответ: один алерт (arch/03 §2 + §4.1; severity — строчная строка, spec §3.11;
// hint/remedy/remedyText — объяснение и движитель, task etcd-via-worker-api).
public sealed record AlertDto(
    string Id,
    string Severity,
    string Kind,
    string Target,
    string Message,
    IReadOnlyDictionary<string, string>? Details,
    long? SinceUnix,
    string? Hint = null,
    string? Remedy = null,
    string? RemedyText = null);

// Core → DTO + фильтры: чистые функции (spec §6.2).
public static class AlertsMapper
{
    public static IReadOnlyList<AlertDto> Map(IReadOnlyList<Alert> alerts)
        => [.. alerts.Select(ToDto)];

    public static AlertDto ToDto(Alert alert)
        => new(
            alert.Id,
            SeverityName(alert.Severity),
            alert.Kind,
            alert.Target,
            alert.Message,
            alert.Details,
            alert.SinceUnix,
            alert.Hint,
            RemedyName(alert.Remedy),
            alert.RemedyText);

    // Фильтры до маппинга: severity и kind — точные совпадения (spec §3.13).
    public static IReadOnlyList<Alert> ApplyFilters(
        IReadOnlyList<Alert> alerts, AlertSeverity? severity, string? kind)
        => [.. alerts
            .Where(a => severity is null || a.Severity == severity)
            .Where(a => kind is null || a.Kind == kind)];

    private static string SeverityName(AlertSeverity severity)
        => severity switch
        {
            AlertSeverity.Critical => "critical",
            AlertSeverity.Warning => "warning",
            _ => "info",
        };

    // Движитель — строка camel-дефисом (канон arch/03 §4.1): worker-auto |
    // operator-api | operator-runbook; null-толерантность для старых алертов.
    private static string? RemedyName(AlertRemedy? remedy)
        => remedy switch
        {
            AlertRemedy.WorkerAuto => "worker-auto",
            AlertRemedy.OperatorApi => "operator-api",
            AlertRemedy.OperatorRunbook => "operator-runbook",
            _ => null,
        };
}

// Хендлер: store → отказ «снапшота нет» или фильтры+маппер (spec §3.12).
// GET /api/alerts объединяет алерты pg- и kafka-движков (kind различает kafka-*,
// arch/03 §7.1); до первого kafka-тика — только pg-лента.
[InjectAsScoped]
public sealed class AlertsQueryHandler(ISnapshotStore store, IKafkaSnapshotReader kafkaStore)
    : IQueryHandler<AlertsQuery, IReadOnlyList<AlertDto>>
{
    public ValueTask<Result<IReadOnlyList<AlertDto>>> Handle(AlertsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<AlertDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<AlertDto>>.Success(AlertsMapper.Map(AlertsMapper.ApplyFilters(
                Merge(snapshot.Alerts, kafkaStore.Current?.Alerts),
                query.Severity, query.Kind))));
    }

    // Merge: единая сортировка severity → kind → target (механика движков).
    private static IReadOnlyList<Alert> Merge(IReadOnlyList<Alert> pg, IReadOnlyList<Alert>? kafka)
        => kafka is null
            ? pg
            : [.. pg.Concat(kafka)
                .OrderBy(a => a.Severity, Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x)))
                .ThenBy(a => a.Kind, StringComparer.Ordinal)
                .ThenBy(a => a.Target, StringComparer.Ordinal)];
}
