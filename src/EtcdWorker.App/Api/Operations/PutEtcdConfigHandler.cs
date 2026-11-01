using System.Text.Json.Serialization;
using EtcdWorker.Core;
using EtcdWorker.Etcd;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.App.Api.Operations;

// Тело PUT /api/etcd/config (arch/25 §1.1): {"nodes":N} — замещение декларации.
public sealed record PutEtcdConfigRequest([property: JsonPropertyName("nodes")] int Nodes);

// 409: итоговый состав ниже безопасного (external + N < 3).
public sealed class EtcdQuorumGuardException(int nodes, int external)
    : Exception($"unsafe membership: {external} external + {nodes} managed < 3 members (quorum guard, arch/25 §1.1)");

// 400: N вне 0..7.
public sealed class EtcdConfigValidationException(string message) : Exception(message);

/// <summary>PUT /api/etcd/config (arch/25 §1.1/§5): валидация 0..7 → гвард
/// кворума (external + N ≥ 3) → RMW /etcd/config по mod_revision (нет ключа →
/// txn version==0; проигрыш гонки → Failed-транзиент, вызывающий отдаёт 503).
/// created_unix — исходная метка появления декларации (сохраняется при замене).
/// Формат/ключи домена — EtcdDomainKeys (EtcdWorker.Etcd); managed/external —
/// EtcdContourMembers (единый peer-URL-детект, EtcdWorker.Core).</summary>
public sealed class PutEtcdConfigHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    TimeProvider clock)
{
    public async Task<Result> HandleAsync(PutEtcdConfigRequest request, CancellationToken ct)
    {
        if (request.Nodes is < 0 or > 7)
            return Result.Failed(new EtcdConfigValidationException(
                $"nodes must be an integer in 0..7, got {request.Nodes}"));

        // Гвард кворума: итог external + managed ≥ 3 (external — члены без
        // managed-паттерна peer-URL; единый детект с процессами состава).
        var members = await MemberListWithFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (_, external) = EtcdContourMembers.Build(members.Value);
        if (external.Count + request.Nodes < 3)
            return Result.Failed(new EtcdQuorumGuardException(request.Nodes, external.Count));

        var current = await GetWithFailoverAsync(EtcdDomainKeys.ConfigKey, ct);
        if (!current.IsSuccess)
            return Result.Failed(current.Error!);

        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        TxnRequest txn;
        if (current.Value is { } kv)
        {
            // created_unix исходной декларации сохраняется (битый JSON → now).
            var createdUnix = EtcdConfigJson.Parse(kv.Value)?.CreatedUnix ?? now;
            txn = TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(EtcdDomainKeys.ConfigKey, (long)kv.ModRevision)],
                [new TxnOp.Put(EtcdDomainKeys.ConfigKey, EtcdConfigJson.Serialize(request.Nodes, createdUnix), null)]);
        }
        else
        {
            txn = TxnRequest.Of(
                [TxnCompare.NotExists(EtcdDomainKeys.ConfigKey)],
                [new TxnOp.Put(EtcdDomainKeys.ConfigKey, EtcdConfigJson.Serialize(request.Nodes, now), null)]);
        }

        var written = await TxnWithFailoverAsync(txn, ct);
        if (!written.IsSuccess)
            return Result.Failed(written.Error!);
        return written.Value.Succeeded
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{EtcdDomainKeys.ConfigKey} changed since read (mod_revision mismatch) — retried by client"));
    }

    private async Task<Result<IReadOnlyList<EtcdMember>>> MemberListWithFailoverAsync(CancellationToken ct)
    {
        Result<IReadOnlyList<EtcdMember>> last = default;
        foreach (var endpoint in endpoints)
        {
            var list = await gateway.MemberListAsync(endpoint, ct);
            if (list.IsSuccess)
                return list;
            last = list;
        }
        return Result<IReadOnlyList<EtcdMember>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, key, ct);
            if (got.IsSuccess)
                return got;
            last = got;
        }
        return Result<Kv?>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }
        return Result<TxnResult>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
