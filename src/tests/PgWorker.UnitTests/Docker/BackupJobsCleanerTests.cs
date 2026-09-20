using FluentAssertions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Чистка джобов бэкапов D1 (t02/t04): контейнеры по префиксам full/verify +
// volume (full → volume из имени без "full-"; verify → volume == имя контейнера).
public class BackupJobsCleanerTests
{
    // Локальный фейк IDockerEngine (по образцу FakeBackupEngine): только нужные
    // чистке методы; остальное — NotSupportedException.
    internal sealed class FakeCleanerEngine : IDockerEngine
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
        public List<(string Name, string State)> Containers { get; init; } = [];
        public List<string> RemovedContainers { get; } = [];
        public List<string> RemovedVolumes { get; } = [];

        public Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
            string namePrefix, bool all, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<DockerContainer>>.Success(
                (IReadOnlyList<DockerContainer>)Containers
                    .Where(c => c.Name.Contains(namePrefix, StringComparison.Ordinal))
                    .Select(c => new DockerContainer(Guid.NewGuid().ToString("N"), [c.Name], c.State, "img"))
                    .ToList()));

        public Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        {
            RemovedContainers.Add(idOrName);
            return Task.FromResult(Result.Success());
        }

        public Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
        {
            RemovedVolumes.Add(name);
            return Task.FromResult(Result.Success());
        }

        private static NotSupportedException NotSupported() => new("не используется в тестах чистки");

        public Task<Result> PingAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct) => throw NotSupported();
        public Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct) => throw NotSupported();
        public Task<Result> StartContainerAsync(string idOrName, CancellationToken ct) => throw NotSupported();
        public Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct) => throw NotSupported();
        public Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct) => throw NotSupported();
        public Task<Result> EnsureNetworkAsync(string name, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct) => throw NotSupported();
        public Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct) => throw NotSupported();
        public Task<Result> RemoveServiceAsync(string name, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct) => throw NotSupported();
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct) => throw NotSupported();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // AAA: чистка D1 убирает и full-, и verify-джобы кластера с их volumes (t04)
    [Fact]
    public async Task Remove_чищает_full_и_verify_джобы_сVolumes()
    {
        // Arrange — движок с full-джобом (t02) и verify-джобом (t04) кластера c1
        var engine = new FakeCleanerEngine
        {
            Containers =
            [
                ("pgw-backup-full-c1-shard1-20260911120000Z", "exited"),
                ("pgw-backup-verify-c1-shard1-20260911120000Z", "exited"),
                ("pgw-backup-verify-c2-shard1-20260911130000Z", "running"), // чужой кластер
            ],
        };

        // Act
        var removed = await BackupJobsCleaner.RemoveAsync([engine], "c1", CancellationToken.None);

        // Assert — контейнер c2 не тронут; volume выводится из имени:
        // full → pgw-backup-<C>-<X>-<id>; verify → имя контейнера (volume = имя)
        removed.IsSuccess.Should().BeTrue();
        engine.RemovedContainers.Should().BeEquivalentTo(
            ["pgw-backup-full-c1-shard1-20260911120000Z", "pgw-backup-verify-c1-shard1-20260911120000Z"]);
        engine.RemovedVolumes.Should().BeEquivalentTo(
            ["pgw-backup-c1-shard1-20260911120000Z", "pgw-backup-verify-c1-shard1-20260911120000Z"]);
    }
}
