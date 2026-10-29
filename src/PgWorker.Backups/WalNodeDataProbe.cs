using PgWorker.Provisioning.Probes;

namespace PgWorker.Backups;

/// <summary>Проба данных ноды (arch/14 Д3) — адаптер GET /api/node/pgdata
/// приёмника (канал arch/25 №22) под шов провижининга INodeDataProbe:
/// резолв адреса приёмника — по имени docker-контейнера ноды
/// (WalReceiverAddress, arch/19 §2).</summary>
public sealed class WalNodeDataProbe(IWalReceiverApiClient receiver) : INodeDataProbe
{
    public async Task<NodeDataProbeReply> ProbeAsync(
        string cluster, string shard, string node, CancellationToken ct)
    {
        var reply = await receiver.GetPgDataAsync(
            WalReceiverAddress.ForNode(cluster, shard, node), ct);
        return reply.IsSuccess
            ? new NodeDataProbeReply(reply.StatusCode, reply.Body?.PgVersion, null)
            : new NodeDataProbeReply(reply.StatusCode, null,
                reply.Error ?? $"receiver status {(int)reply.StatusCode}");
    }
}
