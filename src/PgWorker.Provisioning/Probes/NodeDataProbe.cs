using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;

namespace PgWorker.Provisioning.Probes;

/// <summary>Исход пробы данных ноды: PgVersion — «present»/«absent» (факт
/// PG_VERSION); StatusCode 0 — транспортный отказ/таймаут, 5xx — отказ
/// приёмника; оба — Unknown для Д3 (утрата не доказана).</summary>
public sealed record NodeDataProbeReply(int StatusCode, string? PgVersion, string? Error)
{
    public bool IsSuccess => StatusCode is >= 200 and < 300;
}

/// <summary>Проба данных ноды (Д3, spec §3.7, arch/14 R11): факт PG_VERSION
/// файловой системой контейнера ноды — GET /api/node/pgdata её приёмника
/// (канал arch/25 №22). Исполнитель — процесс приёмника ноды; шов для
/// Provisioning — реализация живёт у владельца клиента приёмника.</summary>
public interface INodeDataProbe
{
    Task<NodeDataProbeReply> ProbeAsync(string cluster, string shard, string node, CancellationToken ct);
}

/// <summary>Маппинг ответа пробы в трёхуровневый вердикт Д3 (Present/Absent/
/// Unknown): не-200 и транспортный отказ — Unknown (утрата не доказана).</summary>
public static class NodeDataProbeExtensions
{
    public static DataPresence ToPresence(this NodeDataProbeReply reply)
        => reply.IsSuccess
            ? reply.PgVersion switch
            {
                "present" => DataPresence.Present,
                "absent" => DataPresence.Absent,
                _ => DataPresence.Unknown,
            }
            : DataPresence.Unknown;
}
