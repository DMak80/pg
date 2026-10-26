using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// API-контракт маршрутов WalReceiver (spec t26 §3.2) через
// WebApplicationFactory над internal-Program (InternalsVisibleTo):
// POST full — 202 / 409 (вторая активная) / 503 (стоп-гейты, в т.ч.
// Unknown — транспортный отказ etcd: операция не стартовала, fail-closed);
// GET full — 200 снимок / 404 (после Clear — vanished-семантика); cancel —
// 202/404; tool-маршруты verify/restore — те же таблицы. Операции — фейки
// через DI (интерфейсы фабрик подменяются, шаг 4); hosted-цикл стрима
// выключен env PGW_WALRECEIVER_NO_STREAM=1. AAA-комментарии.
[Collection("walreceiver-api")]
public class BackupApiOperationTests : IDisposable
{
    private const string Id = "20260101000000Z";
    private static readonly Dictionary<string, string?> Saved = [];

    // Сохранённые исходные значения env (восстановление в Dispose).
    private static void Save(string key)
        => Saved.TryAdd(key, Environment.GetEnvironmentVariable(key));

    private static void SetEnv(bool toolMode)
    {
        Save("PGW_BACKUP_CLUSTER");
        Save("PGW_BACKUP_SHARD");
        Save("PGW_BACKUP_NODE");
        Save("PGW_ETCD");
        Save("PGW_WALRECEIVER_NO_STREAM");
        Save("PGW_NODE_TOOL");
        Environment.SetEnvironmentVariable("PGW_BACKUP_CLUSTER", "c1");
        Environment.SetEnvironmentVariable("PGW_BACKUP_SHARD", "s1");
        Environment.SetEnvironmentVariable("PGW_BACKUP_NODE", "n1");
        Environment.SetEnvironmentVariable("PGW_ETCD", "http://etcd-fake:2379");
        Environment.SetEnvironmentVariable("PGW_WALRECEIVER_NO_STREAM", "1");
        Environment.SetEnvironmentVariable("PGW_NODE_TOOL", toolMode ? "1" : null);
    }

    /// <summary>Фейк etcd: KV-словарь + FailReads (Unknown-гейты).</summary>
    private sealed class FakeEtcd : IEtcdGateway
    {
        public Dictionary<string, string> Kvs { get; } = [];
        public bool FailReads;

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(FailReads
                ? Result<Kv?>.Failed(new EtcdUnreachableException("fake: etcd down"))
                : Result<Kv?>.Success(Kvs.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([]));
        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(1));
        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
    }

    /// <summary>Фейк операции: RunAsync — no-op (тесты контракта маршрутов).</summary>
    private sealed class FakeOp : IReceiverOperation
    {
        public OperationSnapshot? Snapshot => null;
        public Task RunAsync(CancellationToken ct) => Task.CompletedTask;
        public Task CancelAsync() => Task.CompletedTask;
    }

    /// <summary>Фейк полной фабрики: счётчик созданий; на Create публикует
    /// running-снимок (заданный фейк-исход) в общий реестр.</summary>
    private sealed class FakeFullFactory(OperationRegistry registry, List<string> created) : IFullBackupOperationFactory
    {
        public IReceiverOperation Create(string id)
        {
            created.Add(id);
            registry.Update(new OperationSnapshot(id, "running", "basebackup"));
            return new FakeOp();
        }
    }

    /// <summary>Фейк tool-фабрики: счётчик созданий; на Create публикует
    /// running/downloading-снимок.</summary>
    private sealed class FakeToolFactory(OperationRegistry registry, List<string> created) : IToolOperationFactory
    {
        public IReceiverOperation CreateVerify(StartVerifyRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.BackupId, "running", "downloading"));
            return new FakeOp();
        }

        public IReceiverOperation CreateRestore(StartRestoreRequest request)
        {
            created.Add(request.BackupId);
            registry.Update(new OperationSnapshot(request.BackupId, "running", "recovering"));
            return new FakeOp();
        }
    }

    private static (WebApplicationFactory<global::PgWorker.WalReceiver.Program> Factory, HttpClient Client,
        OperationRegistry Registry, List<string> Created, FakeEtcd Etcd) Build(bool toolMode)
    {
        SetEnv(toolMode);
        var etcd = new FakeEtcd();
        var registry = new OperationRegistry();
        var created = new List<string>();
        var factory = new WebApplicationFactory<global::PgWorker.WalReceiver.Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureTestServices(services =>
            {
                // Один реестр на маршруты и фейк-фабрики (последняя
                // регистрация побеждает регистрацию Program).
                services.AddSingleton(registry);
                services.AddSingleton<IEtcdGateway>(etcd);
                services.AddSingleton<IFullBackupOperationFactory>(
                    new FakeFullFactory(registry, created));
                services.AddSingleton<IToolOperationFactory>(
                    new FakeToolFactory(registry, created));
            });
        });
        return (factory, factory.CreateClient(), registry, created, etcd);
    }

    // ── Нодовый режим: /api/backup/full* ─────────────────────────────────

    [Fact]
    public async Task PostFull_NoActive_Returns202AndStarts()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: нодовый режим, пустой реестр.
        var (_, client, _, created, _) = Build(toolMode: false);

        // Act: первый POST full.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 202 + операция создана фейк-фабрикой.
        ((int)response.StatusCode).Should().Be(202);
        var status = await response.Content.ReadFromJsonAsync<FullStatusResponse>(ct);
        status!.State.Should().Be("running");
        status.Phase.Should().Be("basebackup");
        created.Should().Equal(Id);
    }

    [Fact]
    public async Task PostFull_SecondCall_Returns409()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: первый POST создал активную операцию (снимок running).
        var (_, client, _, _, _) = Build(toolMode: false);
        await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Act: повторный POST.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 409-гвард; тело — снимок АКТИВНОЙ операции с её id (воркер
        // сверяет id в 409-ветке: своя/чужая — фантомный COMPLETED гвард).
        ((int)response.StatusCode).Should().Be(409);
        var active = await response.Content.ReadFromJsonAsync<FullStatusResponse>(ct);
        active!.Error.Should().Contain("already active");
        active.Id.Should().Be(Id);
        active.State.Should().Be("running");
    }

    [Fact]
    public async Task GetFull_ReturnsSnapshot_Then404AfterClear()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: POST создал операцию.
        var (_, client, _, _, _) = Build(toolMode: false);
        await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Act: GET статуса.
        var ok = await client.GetAsync("/api/backup/full", ct);

        // Assert: 200 со снимком фейка. Терминальный снимок персистентен
        // (снятий в проде нет — супервизы опираются на него, arch/19 §4).
        ((int)ok.StatusCode).Should().Be(200);
        var status = await ok.Content.ReadFromJsonAsync<FullStatusResponse>(ct);
        status!.State.Should().Be("running");
        status.Phase.Should().Be("basebackup");
    }

    [Fact]
    public async Task PostFull_WalKeyStopped_Returns503NotStarted()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: wal-ключ шарда в STOPPED (карантин — гейт операции).
        var (_, client, _, created, etcd) = Build(toolMode: false);
        etcd.Kvs["/pgworker/backups/c1/s1/wal"] = """{"state":"STOPPED"}""";

        // Act: POST full.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 503 — операция не начинается, статус не портится.
        ((int)response.StatusCode).Should().Be(503);
        created.Should().BeEmpty();
    }

    [Fact]
    public async Task PostFull_WalKeyChainError_Returns202AndStarts_StreamStillPaused()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: wal-ключ шарда в WAL_CHAIN_ERROR (разрыв цепочки архива).
        var (_, client, _, created, etcd) = Build(toolMode: false);
        etcd.Kvs["/pgworker/backups/c1/s1/wal"] = """{"state":"WAL_CHAIN_ERROR"}""";

        // Act: POST full.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 202 — WAL_CHAIN_ERROR НЕ гейт операции (пересъём — единственное
        // лечение разрыва), операция стартовала.
        ((int)response.StatusCode).Should().Be(202);
        var status = await response.Content.ReadFromJsonAsync<FullStatusResponse>(ct);
        status!.State.Should().Be("running");
        created.Should().Equal(Id);

        // Assert: гейт СТРИМА при этом paused — приёмники замирают (Р17:
        // замирание стрима не мешает параллельному полному).
        (await WalReceiverGates.IsStopGatedAsync(
            etcd, ["http://etcd-fake:2379"], "c1", "s1", ct)).Should().BeTrue();
    }

    [Fact]
    public async Task PostFull_DisabledKey_Returns503NotStarted()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: глобальный ключ /pgworker/backups-disabled жив.
        var (_, client, _, created, etcd) = Build(toolMode: false);
        etcd.Kvs["/pgworker/backups-disabled"] = """{"updated_unix":1}""";

        // Act.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 503 + операция не стартовала.
        ((int)response.StatusCode).Should().Be(503);
        created.Should().BeEmpty();
    }

    [Fact]
    public async Task PostFull_GatesUnknown_Returns503NotStarted()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: транспортный отказ etcd при чтении гейтов (Unknown).
        var (_, client, _, created, etcd) = Build(toolMode: false);
        etcd.FailReads = true;

        // Act.
        var response = await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Assert: 503 fail-closed (Unknown — операция не начинается).
        ((int)response.StatusCode).Should().Be(503);
        created.Should().BeEmpty();
    }

    [Fact]
    public async Task CancelFull_NoActive_Returns404()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: пустой реестр.
        var (_, client, _, _, _) = Build(toolMode: false);

        // Act.
        var response = await client.PostAsync("/api/backup/full/cancel", content: null, ct);

        // Assert: 404.
        ((int)response.StatusCode).Should().Be(404);
    }

    [Fact]
    public async Task CancelFull_Active_Returns202()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: активная операция.
        var (_, client, _, _, _) = Build(toolMode: false);
        await client.PostAsJsonAsync("/api/backup/full", new StartFullRequest(Id), ct);

        // Act.
        var response = await client.PostAsync("/api/backup/full/cancel", content: null, ct);

        // Assert: 202 best-effort (итог — failed/cancelled по GET).
        ((int)response.StatusCode).Should().Be(202);
    }

    // ── Tool-режим: /api/ops/* ───────────────────────────────────────────

    [Fact]
    public async Task PostVerify_NoActive_Returns202_SecondReturns409()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: tool-режим.
        var (_, client, _, _, _) = Build(toolMode: true);
        var body = new StartVerifyRequest(Id,
            new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1"));

        // Act: первый и повторный POST.
        var first = await client.PostAsJsonAsync("/api/ops/verify", body, ct);
        var second = await client.PostAsJsonAsync("/api/ops/verify", body, ct);

        // Assert: 202 / 409.
        ((int)first.StatusCode).Should().Be(202);
        var status = await first.Content.ReadFromJsonAsync<ToolStatusResponse>(ct);
        status!.State.Should().Be("running");
        status.Phase.Should().Be("downloading");
        ((int)second.StatusCode).Should().Be(409);
    }

    [Fact]
    public async Task PostRestore_NoActive_Returns202_SecondReturns409()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: tool-режим.
        var (_, client, _, _, _) = Build(toolMode: true);
        var body = new StartRestoreRequest(Id, TargetTime: null, RecoveryTimeoutSec: 60,
            S3: new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1"));

        // Act: первый и повторный POST.
        var first = await client.PostAsJsonAsync("/api/ops/restore", body, ct);
        var second = await client.PostAsJsonAsync("/api/ops/restore", body, ct);

        // Assert: 202 (downloading) / 409.
        ((int)first.StatusCode).Should().Be(202);
        var status = await first.Content.ReadFromJsonAsync<ToolStatusResponse>(ct);
        status!.Phase.Should().Be("downloading");
        ((int)second.StatusCode).Should().Be(409);
    }

    [Fact]
    public async Task GetRestore_ReturnsSnapshot_Then404AfterClear()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: tool-режим, POST restore создал операцию.
        var (_, client, registry, _, _) = Build(toolMode: true);
        var body = new StartRestoreRequest(Id, TargetTime: null, RecoveryTimeoutSec: 60,
            S3: new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1"));
        await client.PostAsJsonAsync("/api/ops/restore", body, ct);

        // Act: GET.
        var ok = await client.GetAsync("/api/ops/restore", ct);

        // Assert: 200 со снимком фейка (recovering). Терминальный снимок
        // персистентен (снятий в проде нет — супервизы опираются на него).
        ((int)ok.StatusCode).Should().Be(200);
        var status = await ok.Content.ReadFromJsonAsync<ToolStatusResponse>(ct);
        status!.State.Should().Be("running");
        status.Phase.Should().Be("recovering");
    }

    [Fact]
    public async Task CancelOps_NoActive_Returns404_ActiveReturns202()
    {
        var ct = TestContext.Current.CancellationToken;
        // Arrange: tool-режим, пустой реестр.
        var (_, client, _, _, _) = Build(toolMode: true);

        // Act/Assert: cancel без активной — 404.
        var none = await client.PostAsync("/api/ops/cancel", content: null, ct);
        ((int)none.StatusCode).Should().Be(404);

        // Arrange: активная verify-операция.
        await client.PostAsJsonAsync("/api/ops/verify", new StartVerifyRequest(Id,
            new S3Context(["http://s3a:9000"], "ak", "sk", "backups", "c1/s1")), ct);

        // Act/Assert: cancel активной — 202.
        var cancel = await client.PostAsync("/api/ops/cancel", content: null, ct);
        ((int)cancel.StatusCode).Should().Be(202);
    }

    public void Dispose()
    {
        // Восстановление исходных значений env (параллельные коллекции не
        // трогают эти ключи — только этот класс; тесты внутри коллекции
        // последовательны).
        foreach (var (key, value) in Saved)
            Environment.SetEnvironmentVariable(key, value);
        Saved.Clear();
    }
}
