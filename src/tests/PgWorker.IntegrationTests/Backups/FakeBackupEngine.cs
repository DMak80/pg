using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;

namespace PgWorker.IntegrationTests.Backups;

// Публичная копия юнит-двойника BackupProcessTests.FakeBackupEngine (t05 Task 9):
// in-memory docker-движок для супервиза джобов RestoreProcess против реального etcd.
public sealed class FakeBackupEngine : IDockerEngine
{

        // ── union-члены t07 (kfw/vwk-методы): pg-доменом не используются — стабы ──
        public Task<Result> DeleteNetworkAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct) => Task.FromResult(Result<bool>.Success(false));
        public Task<Result> EnsureVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> DeleteVolumeAsync(string name, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct) => Task.FromResult(Result.Success());
        public Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct) => Task.FromResult(Result<byte[]?>.Success(null));
        public Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct) => Task.FromResult(Result<NodeLimits?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyDictionary<string, string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct) => Task.FromResult(Result<IReadOnlyList<string>?>.Success(null));
        public Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct) => Task.FromResult(Result<DockerNodeEndpoint?>.Success(null));
    public sealed record ContainerRec(string Id, string State, int ExitCode, string Logs);

    public readonly Dictionary<string, ContainerRec> Containers = [];
    public readonly List<(string Name, ContainerSpec Spec)> Created = [];
    public readonly List<string> Started = [];
    public readonly List<string> Removed = [];
    public readonly List<string> RemovedVolumes = [];

    // transport-отказы docker (S-ветки): список/логи/инспект — статус не меняем.
    public bool ListFails { get; set; }
    public bool LogsFails { get; set; }
    public bool InspectFails { get; set; }

    public Task<Result> PingAsync(CancellationToken ct) => Task.FromResult(Result.Success());

    public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
        string namePrefix, bool all, CancellationToken ct)
    {
        if (ListFails)
            return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Failed(
                new ApplicationException("docker: list failed")));
        // Names — БЕЗ ведущего "/" (реальный движок триммит, матчинг по имени)
        var list = Containers
            .Where(p => p.Key.Contains(namePrefix, StringComparison.Ordinal))
            .Select(p => new DockerContainer(p.Value.Id, [p.Key], p.Value.State, "img"))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
            (IReadOnlyList<DockerContainer>)list));
    }

    public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
    {
        if (InspectFails)
            return Task.FromResult(Result<DockerContainerInspect>.Failed(
                new ApplicationException("docker: inspect failed")));
        var found = Containers.FirstOrDefault(p => p.Value.Id == id);
        return Task.FromResult(found.Key is null
            ? Result<DockerContainerInspect>.Failed(new KeyNotFoundException(id))
            : Result<DockerContainerInspect>.Success(new DockerContainerInspect(
                id, found.Key, [], [], [],
                found.Value.State == "running", found.Value.ExitCode)));
    }

    public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
    {
        if (LogsFails)
            return Task.FromResult(Result<string>.Failed(new ApplicationException("docker: logs failed")));
        return Task.FromResult(Containers.TryGetValue(idOrName, out var rec)
            ? Result<string>.Success(rec.Logs)
            : Result<string>.Failed(new KeyNotFoundException(idOrName)));
    }

    public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
    {
        Created.Add((name, spec));
        Containers[name] = new ContainerRec(Guid.NewGuid().ToString("N"), "created", -1, "");
        return Task.FromResult(Result.Success());
    }

    public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
    {
        Started.Add(idOrName);
        if (Containers.TryGetValue(idOrName, out var rec))
            Containers[idOrName] = rec with { State = "running" };
        return Task.FromResult(Result.Success());
    }

    public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
    {
        Removed.Add(idOrName);
        Containers.Remove(idOrName);
        return Task.FromResult(Result.Success());
    }

    public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
    {
        RemovedVolumes.Add(name);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
        => Task.FromResult(Result<string>.Success(string.Empty));

    public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<DockerSwarmNode>>.Success([]));

    public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
        => Task.FromResult(Result.Success());

    public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));

    public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<DockerTask>>.Success([]));

    public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlySet<(string Host, int Port)>>.Success(
            (IReadOnlySet<(string Host, int Port)>)new HashSet<(string, int)>()));

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
