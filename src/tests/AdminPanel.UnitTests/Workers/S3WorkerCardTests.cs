using System.Net;
using System.Text;
using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using AdminPanel.Api.Operations;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// s3worker в общем каркасе воркеров (arch/adminpanel/02 §2.3.4/§9.9): грань
// «Воркеры» — четвёртая карточка, гварды сертов/рестарта принимают s3worker,
// LiveHosts резолвит advertise-хосты из s3-снапшота.
public class S3WorkerCardTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    // ===== Двойники сторов (шлюз/хендлер читают только Current) =====

    private sealed class SettablePgStore : ISnapshotStore
    {
        public EtcdSnapshot? Current { get; set; }

        public void Replace(EtcdSnapshot snapshot) => Current = snapshot;
    }

    private sealed class StubKafkaReader : IKafkaSnapshotReader
    {
        public AdminPanel.Core.Kafka.KafkaSnapshot? Current { get; init; }
    }

    private sealed class StubValkeyReader : IValkeySnapshotReader
    {
        public ValkeySnapshot? Current { get; init; }
    }

    private sealed class StubS3Reader : IS3SnapshotReader
    {
        public S3Snapshot? Current { get; init; }
    }

    // Стаб шлюза: рестарт-прокси возвращает пустой per-instance итог.
    private sealed class StubGateway : IWorkerApiGateway
    {
        public Task<WorkerApiResult> SendAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
            => Task.FromResult(new WorkerApiResult(200, null));

        public Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
            => Task.FromResult<IReadOnlyList<WorkerApiInstanceResult>>([]);
    }

    // Dead-шлюз etcd для cert-сервиса (гвард срабатывает ДО обращения).
    private sealed class DeadGateway : IEtcdGateway
    {
        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => Task.FromResult(Result<TxnResult>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<byte[]>.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")));
    }

    private sealed class StubSecrets : IKafkaSecretsStore
    {
        public IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; set; } =
            new Dictionary<string, KafkaClusterSecrets>();

        public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => Current = secrets;
    }

    private static WorkerCertService CertService()
        => new(
            new DeadGateway(),
            Options.Create(new EtcdOptions { Endpoints = ["http://dead:1"] }),
            new StubSecrets(),
            Options.Create(new WorkerApiOptions()),
            TimeProvider.System);

    private static S3Snapshot S3SnapshotWith(
        params WorkerEndpoint[] endpoints) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [], [], [.. endpoints], [], [], [], 0);

    // Arrange: снапшот s3 с endpoint+cert. Act: GetWorkersQueryHandler.Handle.
    // Assert: карточка s3worker с инстансом и TargetCert; ApplyStatus по thumbprint.
    [Fact]
    public async Task WorkersView_ContainsS3WorkerCard()
    {
        var cert = new WorkerApiCert("sha256hex", "CN=s3w", "CN=ca", ["s3w"],
            Now, Now.AddYears(1), 1760150000, "admin");
        var s3 = new StubS3Reader
        {
            Current = S3SnapshotWith(new WorkerEndpoint("i1", "https://s3w:8080", 5)
            {
                CertThumbprint = "sha256hex",
            }) with
            {
                WorkerApiCert = cert,
            },
        };
        var handler = new GetWorkersQueryHandler(
            new SettablePgStore { Current = TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] } },
            new StubKafkaReader(), new StubValkeyReader(), s3);

        // Act
        var result = await handler.Handle(new GetWorkersQuery(), CancellationToken.None);

        // Assert: четвёртая карточка с инстансом и целевым сертом; applied по thumbprint.
        result.IsSuccess.Should().BeTrue();
        var card = result.Value.Workers.Single(w => w.Worker == "s3worker");
        card.Instances.Should().ContainSingle().Which.Instance.Should().Be("i1");
        card.Instances.Single().ApplyStatus.Should().Be("applied");
        card.TargetCert.Should().NotBeNull();
        card.TargetCert!.Thumbprint.Should().Be("sha256hex");
        // порядок pg→kafka→valkey→s3.
        result.Value.Workers.Select(w => w.Worker).Should()
            .Equal("pgworker", "kafkaworker", "valkeyworker", "s3worker");
    }

    // Arrange: worker="s3worker". Act: WorkerCertService.GenerateAndPutAsync гвард /
    // RestartWorkerCommandHandler.Handle. Assert: не WorkerNotFoundException; мусорное
    // имя по-прежнему 404 (WorkerNotFoundException).
    [Fact]
    public async Task S3Worker_AcceptedByGuards_TrashStill404()
    {
        // Act: generate-гвард пропускает s3worker (падает дальше на etcd — dead-шлюз).
        var gen = await CertService().GenerateAndPutAsync("s3worker", [], "admin", CancellationToken.None);

        // Assert: ошибка транспорта, НЕ гварда.
        gen.Error.Should().NotBeOfType<WorkerNotFoundException>();

        // Act: рестарт-гвард.
        var restart = await new RestartWorkerCommandHandler(new StubGateway())
            .Handle(new RestartWorkerCommand("s3worker", "admin"), CancellationToken.None);

        // Assert: s3worker принят (успех с пустым per-instance итогом).
        restart.IsSuccess.Should().BeTrue();

        // Act: мусорное имя — по-прежнему 404.
        var trash = await new RestartWorkerCommandHandler(new StubGateway())
            .Handle(new RestartWorkerCommand("who-is-this", "admin"), CancellationToken.None);

        // Assert
        trash.Error.Should().BeOfType<WorkerNotFoundException>();
    }

    // Arrange: LiveHosts("s3worker", null, null, null, s3Snapshot с URL https://s3w:8080).
    // Act: GenerateWorkerApiCertCommandHandler.LiveHosts. Assert: ["s3w"].
    [Fact]
    public void LiveHosts_S3Worker_ResolvesAdvertiseHosts()
    {
        // Arrange: s3-снапшот с одним живым ключом advertise-URL.
        var s3 = new StubS3Reader
        {
            Current = S3SnapshotWith(new WorkerEndpoint("i1", "https://s3w:8080", 5)),
        };

        // Act
        var hosts = GenerateWorkerApiCertCommandHandler.LiveHosts(
            "s3worker", null, null, null, s3.Current);

        // Assert
        hosts.Should().Equal("s3w");
    }
}
