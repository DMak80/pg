using AdminPanel.Core;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запрос домена EtcdWorker (adminpanel/02 §2.3.5): декларация состава +
// managed-узлы + статус DR-выгрузки для страницы «etcd» (arch/03 §2).
public sealed record EtcdDomainQuery : IQuery<EtcdDomainDto>;

// Ответ GET /api/etcd/domain: declaredNodes null — ключа /etcd/config нет
// («только наблюдение», external-контур).
public sealed record EtcdDomainDto(
    int? DeclaredNodes,
    long? CreatedUnix,
    IReadOnlyList<EtcdManagedNodeDto> Nodes,
    EtcdSnapshotsDto? Snapshots);

public sealed record EtcdManagedNodeDto(int K, string? State, string? PlacedOn);

// EtcdDomain + статус слепков → DTO: чистая функция.
public static class EtcdDomainMapper
{
    public static EtcdDomainDto Map(EtcdDomainInfo? domain, EtcdSnapshotExportInfo? snapshots) => new(
        domain?.DeclaredNodes,
        domain?.CreatedUnix,
        [.. (domain?.Nodes ?? []).Select(n => new EtcdManagedNodeDto(n.K, n.State, n.PlacedOn))],
        BackupStorageMappers.MapEtcdSnapshots(snapshots));
}

// Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
[InjectAsScoped]
public sealed class EtcdDomainQueryHandler(ISnapshotStore store)
    : IQueryHandler<EtcdDomainQuery, EtcdDomainDto>
{
    public ValueTask<Result<EtcdDomainDto>> Handle(EtcdDomainQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<EtcdDomainDto>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<EtcdDomainDto>.Success(
                EtcdDomainMapper.Map(snapshot.EtcdDomain, snapshot.EtcdSnapshots)));
    }
}
