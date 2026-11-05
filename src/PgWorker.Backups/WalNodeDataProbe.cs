using PgWorker.Provisioning.Probes;

namespace PgWorker.Backups;

/// <summary>Проба данных ноды (arch/pgworker/README.md Д3) — адаптер GET /api/node/pgdata
/// приёмника (канал arch/communications/02-channel-registry.md №22) под шов провижининга INodeDataProbe:
/// резолв адреса приёмника — по имени docker-контейнера ноды
/// (WalReceiverAddress, arch/backups/01-full-backup.md).</summary>
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
