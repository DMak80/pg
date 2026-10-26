using Shared.Docker;

namespace PgWorker.Backups.Tool;

/// <summary>Спека tool-контейнера бэкапов (t26, arch/19 §3.5/§3.6/§5, spec
/// t26 §3.3 Р2): единая для verify/drill/restore — образ НОД (NodeImage) в
/// tool-режиме (env PGW_NODE_TOOL=1; больше НИКАКОЙ identity-env — весь
/// S3-контекст приходит POST-командой, секреты не в env, Р4). Старт БЕЗ
/// команд (Cmd null — entrypoint образа: tool-ветка, шаг 8); API :8080 —
/// прод: публикация динамического host-порта (зонд FreePortProbe), контур
/// (ContourNetwork задан): сеть контура БЕЗ публикации (адрес — alias имени).
/// Лимиты Agent {Cpu,Mem} (arch/19 §6); сеть для S3 в проде — default bridge
/// + extra_hosts (как у джобов), в контуре — сеть поставки. Рестарт no.
/// Точки монтирования — вызывающая сторона: verify — свой staging-volume в
/// /staging; restore — data-volume первой ноды в /home/postgres/pgdata;
/// drill — свой volume (одно имя с контейнером — D1 жив) туда же.</summary>
public static class ToolJobSpec
{
    public static ContainerSpec Build(
        BackupsRuntimeOptions opts,
        string cluster,
        string containerName,
        string? volumeName,
        string volumeDest,
        int? hostPort) // null = контур-режим: сеть контура, без публикации
    {
        var ports = hostPort is { } port
            ? (IReadOnlyList<PortMap>)new PortMap[] { new(8080, port) }
            : [];
        return new ContainerSpec(
            Image: opts.NodeImage,
            Ports: ports,
            Hostname: containerName,
            Env: new Dictionary<string, string> { ["PGW_NODE_TOOL"] = "1" },
            VolumeName: volumeName,
            VolumeDest: volumeDest,
            Network: opts.ContourNetwork,
            ExtraHosts: opts.ContourNetwork is null
                ? new[] { "host.docker.internal:host-gateway", "local:host-gateway" }
                : null,
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            RestartPolicy: "no",
            LabelKey: "pgworker",
            Label: cluster);
    }
}
