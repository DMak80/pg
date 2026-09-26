using FluentAssertions;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// CaRotator (t07, arch/21 §5 K): диспетчеризация K0 (окно/заявка/ждущие),
// фазы P (staging put-if-absent) и D (bundle OLD+NEW, compare по OLD).
// ВАЖНО: против реализации Task 2 цикл останавливается после D, поэтому
// кейсы «окна» ассертят ПРОМЕЖУТОЧНОЕ состояние (staging/bundle) — Task 3
// Step 3.1 обновит их под финал полного цикла (один RunAsync = P→D→R→C→K4).
public class CaRotatorTests
{
    private static readonly ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions Options =
        new(17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");

    private static readonly FixedTimeProvider Clock = new();

    private const string Image = "valkey/valkey:9.1.2";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public WorkJournal Journal = null!;
        public ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner Tls = null!;
        public List<string> Snapshots = [];
        public ValkeyWorker.Provisioning.Processes.CaRotator Rotator = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
            rig.Tls = new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(rig.Driver, Image, Clock);
            rig.Rotator = new ValkeyWorker.Provisioning.Processes.CaRotator(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims, rig.Journal,
                rig.Tls, rig.Valkey, Options,
                async _ =>
                {
                    rig.Snapshots.Add("shot");
                    return Result.Success();
                },
                Clock);
            return rig;
        }

        // Канонический TLS-кластер (миграция T уже отработала): креды, CA-ключи,
        // endpoints, portalloc, контейнер с TLS-args, клэйм наш.
        public string SeedTls(string cluster, int port = 17001)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            var (caPem, caKeyPem) = ValkeyWorker.Core.Valkey.ValkeyPki.GenerateCa(cluster);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_pem", caPem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_key", caKeyPem);
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--tls-port", "6379", "--port", "0"], Image, "id-tls");
            return caPem;
        }

        // МИНИМАЛЬНЫЙ кластер «не поднят»: только клэйм + config (без endpoints,
        // кредов, ca-ключей) — ветка waiting-cluster достижима (spec §5 K0.5).
        public void SeedBare(string cluster)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
        }

        // Ручное открытие окна (посев staging ca_next_*): K0.3 срабатывает по
        // ФАКТУ наличия ключей — guard'ы K0.4–K0.6 пропускаются.
        public (string Pem, string Key) SeedWindow(string cluster)
        {
            var (pem, key) = ValkeyWorker.Core.Valkey.ValkeyPki.GenerateCa(cluster + "-new");
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_pem", pem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_key", key);
            return (pem, key);
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }

        public string? Get(string key)
            => Etcd.GetAsync("http://etcd:2379", key, TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult().Value?.Value;

        public void Put(string key, string value)
            => Etcd.PutAsync("http://etcd:2379", key, value, null, TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();

        public void SeedTicket(string cluster)
            => Put($"/valkeyworker/ca_rotations/{cluster}",
                $$"""{"requested_unix":1756500000,"requested_by":"it"}""");
    }

    [Fact]
    public async Task NoTicketAndNoTail_NotNeeded_ZeroMutations()
    {
        // Arrange — канонический кластер, заявки/журнала ротации нет
        var rig = Rig.Create();
        var caPem = rig.SeedTls("c1");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c1"), TestContext.Current.CancellationToken);

        // Assert — NotNeeded; ca_pem не тронут, staging нет, journal пуст
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.NotNeeded);
        rig.Get("/valkey/clusters/c1/ca_pem").Should().Be(caPem);
        rig.Get("/valkey/clusters/c1/ca_next_key").Should().BeNull();
        rig.Get("/valkeyworker/work/c1").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_ClusterNotUp_WaitingCluster_TicketKept()
    {
        // Arrange — МИНИМАЛЬНЫЙ посев (клэйм + config + заявка; БЕЗ endpoints/
        // кредов/ca-ключей — SeedTls их сеет и waiting-cluster недостижим)
        var rig = Rig.Create();
        rig.SeedBare("c2");
        rig.SeedTicket("c2");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c2"), TestContext.Current.CancellationToken);

        // Assert — Waiting, journal waiting-cluster, заявка жива, мутаций нет
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c2").Should().Contain("rotate-ca").And.Contain("waiting-cluster");
        rig.Get("/valkeyworker/ca_rotations/c2").Should().NotBeNull();
        rig.Get("/valkey/clusters/c2/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_PasswordRotationAlive_WaitingPasswordRotation()
    {
        // Arrange — канонический кластер + живая заявка ротации креда;
        // окно НЕ открыто (ca_next_* нет) — K0.5 даёт Waiting
        var rig = Rig.Create();
        rig.SeedTls("c3");
        rig.SeedTicket("c3");
        rig.Put("/valkeyworker/rotations/c3",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c3"), TestContext.Current.CancellationToken);

        // Assert — Waiting (ждущие исходы вентиль НЕ блокируют), окно НЕ открыто
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c3").Should().Contain("waiting-password-rotation");
        rig.Get("/valkey/clusters/c3/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_PasswordStateReplaying_WaitingPasswordRotation()
    {
        // Arrange — заявки rotations нет, но стейт доигрывания жив (e1-added)
        var rig = Rig.Create();
        rig.SeedTls("c4");
        rig.SeedTicket("c4");
        rig.Put("/valkeyworker/work/c4/rotation",
            """{"phase":"e1-added","request":{"role":"app","requested_unix":1756500000,"requested_by":"it"}}""");

        // Act / Assert — Waiting по стейту (spec §5 K0.5)
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c4"), TestContext.Current.CancellationToken);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c4").Should().Contain("waiting-password-rotation");
    }

    [Fact]
    public async Task Ticket_ToRemove_AbortedStateChanged_Waiting()
    {
        // Arrange — заявка жива, config со state=TO_REMOVE (посев SeedTls +
        // перезапись config: state-гейт К0.6 срабатывает после ждущих)
        var rig = Rig.Create();
        rig.SeedTls("c5");
        rig.SeedTicket("c5");
        rig.Put("/valkey/clusters/c5/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c5"), TestContext.Current.CancellationToken);

        // Assert — aborted-state-changed, Waiting; демонтаж B почистит всё
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c5").Should().Contain("aborted-state-changed");
        rig.Get("/valkey/clusters/c5/ca_next_key").Should().BeNull("окно не открывалось");
    }

    [Fact]
    public async Task Ticket_FullWindowInOneTick_CommitsNewCa()
    {
        // Arrange — канонический кластер + заявка; один RunAsync проводит
        // ВЕСЬ цикл P→D→R→C→K4 (nodes=1, FakeValkeyConnection отвечает).
        // Было Ticket_OpensWindow_StagingThenBundle (промежуточные ассерты
        // staging/bundle — устарели после появления R/C: коммит сворачивает
        // bundle и удаляет staging).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c6");
        rig.SeedTicket("c6");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c6"), TestContext.Current.CancellationToken);

        // Assert — финал: ca_pem/ca_key = NEW (сгенерированы фазой P — сам
        // факт смены доказывает, что P и D исполнились), staging/заявка
        // удалены, journal done, снапшот «после» снят
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var newPem = rig.Get("/valkey/clusters/c6/ca_pem")!;
        newPem.Should().NotBe(oldPem).And.NotContain(oldPem, "bundle свёрнут после коммита");
        rig.Get("/valkey/clusters/c6/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/c6/ca_next_pem").Should().BeNull();
        rig.Get("/valkeyworker/ca_rotations/c6").Should().BeNull();
        rig.Get("/valkeyworker/work/c6").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot", "K4 снял снапшот «после»");
    }

    [Fact]
    public async Task OpenWindow_SkipsGuards_ReplaysToCommitDespitePasswordTicket()
    {
        // Arrange — окно ОТКРЫТО ПОСЕВОМ ca_next_* (иначе K0.5 дал бы Waiting
        // ещё до открытия) ПРИ живой пароль-заявке: K0.3 срабатывает по факту
        // staging — guard'ы K0.4–K0.6 НЕ выполняются (spec §5 K0.3)
        var rig = Rig.Create();
        rig.SeedTls("c7");
        rig.SeedTicket("c7");
        rig.SeedWindow("c7"); // ca_next_pem/ca_next_key — окно открыто
        rig.Put("/valkeyworker/rotations/c7",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act — тик доигрывает окно до коммита ПРИ живой пароль-заявке
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c7"), TestContext.Current.CancellationToken);

        // Assert — не Waiting; цикл ЗАВЕРШЁН despite живую пароль-заявку;
        // пароль-заявка не тронута (E не идёт — его исполнит ветка после K)
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Get("/valkeyworker/work/c7").Should().Contain("done");
        rig.Get("/valkey/clusters/c7/ca_next_key").Should().BeNull("коммит прошёл");
        rig.Get("/valkeyworker/rotations/c7").Should().NotBeNull("заявка креда не тронута");
    }

    [Fact]
    public async Task ReplayTick_CommitsFromForeignStaging()
    {
        // Arrange — staging уже лежит (например, после краха между P и D):
        // re-read переиспользует ЧУЖУЮ генерацию
        var rig = Rig.Create();
        rig.SeedTls("c8");
        rig.SeedTicket("c8");
        var (foreignPem, foreignKey) = rig.SeedWindow("c8");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c8"), TestContext.Current.CancellationToken);

        // Assert — коммит ОТ ЧУЖОЙ staging: ca_pem/ca_key = foreign*,
        // staging удалён — переиспользование доказано самим коммитом
        rig.Get("/valkey/clusters/c8/ca_pem").Should().Be(foreignPem);
        rig.Get("/valkey/clusters/c8/ca_key").Should().Be(foreignKey);
        rig.Get("/valkey/clusters/c8/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/c8/ca_next_pem").Should().BeNull();
    }

    [Fact]
    public async Task BundleAlreadyContainsNext_PutSkipped_WindowHeldByCTxnFault()
    {
        // Arrange — D отработан (bundle в ca_pem). Проверяемый факт — пропуск
        // D-put (ModRevision ca_pem не меняется). Чтобы фаза C не перезаписала
        // ca_pem (это сломало бы факт), окно ОСТАНАВЛИВАЕТСЯ инжектом отказа
        // ТОЛЬКО коммит-txn фазы C (1 compare + 5 success-операций; D-txn —
        // 1+1, P-txn — 2+2; journal-записи идут через PutAsync, не txn —
        // WorkJournal.cs:88-89, txn-эвристика корректна). Свойство запроса —
        // TxnRequest.Success (НЕ Successes; IEtcdGateway.cs:93-96).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c9");
        rig.SeedTicket("c9");
        var (nextPem, nextKey) = rig.SeedWindow("c9");
        rig.Put("/valkey/clusters/c9/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("c9", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken); // R: факт-детект true
        var revisionBefore = rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision;
        rig.Etcd.TxnFault = req => req.Success.Count >= 5
            ? Result<Shared.Etcd.Client.TxnResult>.Failed(new ApplicationException("инжект: отказ C-txn"))
            : null;

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c9"), TestContext.Current.CancellationToken);

        // Assert — D-put пропущен (ревизия ca_pem не тунула: меняли только
        // фаза D — пропущена — и фаза C — сорвана инжектом); окно ЖИВО
        // (staging на месте, bundle в ca_pem), Failed с last_error
        rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision.Should().Be(revisionBefore,
            "put bundle пропущен (Contains-детект), коммит сорван инжектом");
        outcome.IsSuccess.Should().BeFalse("C-txn отказ — Failed");
        rig.Get("/valkey/clusters/c9/ca_next_key").Should().NotBeNull("окно живо");
        rig.Get("/valkey/clusters/c9/ca_pem").Should().Be(oldPem + "\n" + nextPem, "bundle неизменен");
        rig.Get("/valkeyworker/work/c9").Should().Contain("last_error");
    }

    [Fact]
    public async Task BundleCompareLost_Failed_WithLastError()
    {
        // Arrange — ca_pem изменён внешне после снапшота: compare сорвётся
        var rig = Rig.Create();
        rig.SeedTls("c10");
        rig.SeedTicket("c10");
        var snap = rig.Snapshot("c10");
        rig.Put("/valkey/clusters/c10/ca_pem", rig.Get("/valkey/clusters/c10/ca_pem")! + "\nextern");

        // Act
        var outcome = await rig.Rotator.RunAsync(snap, TestContext.Current.CancellationToken);

        // Assert — Failed «ретрай тиком» С last_error в journal (spec §5:
        // отказ между фазами — Failed c last_error); окно при этом открыто
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("ретрай");
        rig.Get("/valkey/clusters/c10/ca_next_key").Should().NotBeNull();
        rig.Get("/valkeyworker/work/c10").Should().Contain("last_error",
            "отказ фазы D фиксируется в journal (FailAsync)");
    }

    [Fact]
    public async Task FullCycle_CommitsNewCa_RemovesStagingAndTicket()
    {
        // Arrange — канонический кластер + заявка; FakeValkeyConnection отвечает
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r1");
        rig.SeedTicket("r1");

        // Act — ОДИН тик проводит весь цикл (nodes=1: «rolling» = одно
        // пересоздание; EnsureNodeTls кладёт серт NEW в volume)
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r1"), TestContext.Current.CancellationToken);

        // Assert — коммит: ca_pem/ca_key = NEW, staging/заявка удалены, done
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var newPem = rig.Get("/valkey/clusters/r1/ca_pem")!;
        newPem.Should().NotBe(oldPem).And.NotContain(oldPem, "bundle свёрнут после коммита");
        rig.Get("/valkey/clusters/r1/ca_key").Should().NotBe(oldPem);
        rig.Get("/valkey/clusters/r1/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/r1/ca_next_pem").Should().BeNull();
        rig.Get("/valkeyworker/ca_rotations/r1").Should().BeNull();
        rig.Get("/valkeyworker/work/r1").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot", "K4 снял снапшот «после»");
    }

    [Fact]
    public async Task PhaseR_FactDetect_ValidNewTar_SkipsRecreate()
    {
        // Arrange — staging есть, bundle есть, серт NEW УЖЕ в volume (рестарт
        // воркера посреди R): факт-детект обязан пропустить пересоздание
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r2");
        rig.SeedTicket("r2");
        var (nextPem, nextKey) = rig.SeedWindow("r2");
        rig.Put("/valkey/clusters/r2/ca_pem", oldPem + "\n" + nextPem);
        var ensured = await rig.Tls.EnsureNodeTlsAsync("r2", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        rig.Driver.Removed.Clear();
        var containerBefore = rig.Driver.Containers[$"vwk-r2-node1"].Id;

        // Act — тик доигрывает: R пропущен (факт), сразу C
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r2"), TestContext.Current.CancellationToken);

        // Assert — контейнер не тронут, RemoveNode не звался, коммит прошёл
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Driver.Removed.Should().BeEmpty("факт-детект: пересоздание не нужно");
        rig.Driver.Containers[$"vwk-r2-node1"].Id.Should().Be(containerBefore);
        rig.Get("/valkey/clusters/r2/ca_pem").Should().Be(nextPem);
    }

    [Fact]
    public async Task PhaseR_RecreatesNode_FromNewCa_ThenBoots()
    {
        // Arrange — staging+bundle есть, серт в volume — OLD (факт-детект
        // false); декларация ресурсов node1 — для ассерта лимитов (spec §7.1:
        // «порт/лимиты из portalloc/декларации»)
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r3", port: 17005); // порт portalloc = 17005
        rig.Put("/valkey/clusters/r3/nodes/node1/resources",
            """{"cpu":"2","mem":"1Gi","disk":"2Gi"}"""); // формат UpdateResourcesHandler
        rig.SeedTicket("r3");
        var (nextPem, nextKey) = rig.SeedWindow("r3");
        rig.Put("/valkey/clusters/r3/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r3", "node1", "h1", "localhost",
            oldPem, rig.Get("/valkey/clusters/r3/ca_key")!, TestContext.Current.CancellationToken);

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r3"), TestContext.Current.CancellationToken);

        // Assert — пересоздание: RemoveNode+EnsureNode звались с портом из
        // portalloc (ClientHostPort=17005) и лимитами из декларации
        // (CpuCores=2, MemoryBytes=1Gi); state RUNNING; PING — якорь NEW.
        // Removed содержит ПОЛНОЕ имя контейнера («vwk-r3-node1»), не «node1»
        // (FakeDriver.RemoveNodeAsync → PlainClusterDriver.NodeName).
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Driver.Removed.Should().Contain("vwk-r3-node1");
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "node1"
            && s.Host == "h1"
            && s.ClientHostPort == 17005
            && s.CpuCores == 2m
            && s.MemoryBytes == 1024L * 1024 * 1024
            && s.TlsVolume == ValkeyWorker.Docker.Drivers.PlainClusterDriver.TlsVolumeName("r3"));
        rig.Get("/valkey/clusters/r3/nodes/node1/state").Should().Be("RUNNING");
        rig.Valkey.LastCaPem.Should().Be(nextPem, "AwaitBoot — якорь NEW (одноблочный парсер)");
    }

    [Fact]
    public async Task PhaseR_ToRemove_Aborts()
    {
        // Arrange — окно открыто, config сменился на TO_REMOVE перед R
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r4");
        rig.SeedTicket("r4");
        var (nextPem, nextKey) = rig.SeedWindow("r4");
        rig.Put("/valkey/clusters/r4/ca_pem", oldPem + "\n" + nextPem);
        rig.Put("/valkey/clusters/r4/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r4"), TestContext.Current.CancellationToken);

        // Assert — abort, нода не тронута, коммита нет
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Driver.Removed.Should().BeEmpty();
        rig.Get("/valkeyworker/work/r4").Should().Contain("aborted-state-changed");
        rig.Get("/valkey/clusters/r4/ca_pem").Should().Contain(oldPem, "коммита не было");
    }

    [Fact]
    public async Task PhaseC_CompareLost_Failed()
    {
        // Arrange — staging-ключ подменён «параллельной ротацией» до compare
        // txn фазы C. Эвристика: в ЭТОМ кейсе P/D пропущены предпосевом
        // (staging+bundle лежат), journal идёт PutAsync — единственный txn
        // с Put — коммит-txn фазы C (2 Put: ca_pem + ca_key).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r5");
        rig.SeedTicket("r5");
        var (nextPem, nextKey) = rig.SeedWindow("r5");
        rig.Put("/valkey/clusters/r5/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r5", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);
        rig.Etcd.OnTxnBeforeCompare = req =>
        {
            if (req.Success.Count(o => o is TxnOp.Put) >= 2)
                rig.Etcd.Store["/valkey/clusters/r5/ca_next_key"] =
                    rig.Etcd.Store["/valkey/clusters/r5/ca_next_key"] with { Value = "foreign" };
        };

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r5"), TestContext.Current.CancellationToken);

        // Assert — срыв compare: Failed «ретрай тиком» + last_error,
        // ca_pem остался bundle
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("ретрай");
        rig.Get("/valkey/clusters/r5/ca_pem").Should().Contain(oldPem);
        rig.Get("/valkeyworker/work/r5").Should().Contain("last_error");
    }

    [Fact]
    public async Task TicketRemovedManually_CommitStillSafe()
    {
        // Arrange — заявку сняли руками (del вне txn): del в txn — no-op
        // (spec §5 C). Окно открыто вручную staging'ом (заявки нет).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r6");
        var (nextPem, nextKey) = rig.SeedWindow("r6");
        rig.Put("/valkey/clusters/r6/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r6", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);

        // Act — заявки нет, но окно открыто (staging): доигрывание до C
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r6"), TestContext.Current.CancellationToken);

        // Assert — коммит дошёл: del отсутствующей заявки — no-op
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Get("/valkey/clusters/r6/ca_pem").Should().Be(nextPem);
        rig.Get("/valkey/clusters/r6/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task OrderInvariant_RNotBeforeD()
    {
        // Arrange — порядок etcd-операций одного RunAsync: put ca_pem (bundle,
        // фаза D) строго РАНЬШЕ put state PROVISIONING (фаза R) — инвариант
        // «NEW-серт на ноде ТОЛЬКО после bundle в ca_pem»
        var rig = Rig.Create();
        rig.SeedTls("r7");
        rig.SeedTicket("r7");
        var ops = new List<string>();
        rig.Etcd.OnPut = key => ops.Add($"put:{key}");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("r7"), TestContext.Current.CancellationToken);

        // Assert
        var bundlePut = ops.IndexOf("put:/valkey/clusters/r7/ca_pem");
        var statePut = ops.IndexOf("put:/valkey/clusters/r7/nodes/node1/state");
        bundlePut.Should().BeGreaterThanOrEqualTo(0, "bundle записан фазой D");
        statePut.Should().BeGreaterThan(bundlePut, "R (state) строго после D (bundle)");
    }

    [Fact]
    public async Task AfterCommitTail_FinishesWithoutStagingTouch()
    {
        // Arrange — journal committed, заявка снята (краш между C и K4)
        var rig = Rig.Create();
        rig.SeedTls("r8");
        var (nextPem, _) = rig.SeedWindow("r8");
        rig.Put("/valkey/clusters/r8/ca_pem", nextPem);
        rig.Put("/valkeyworker/work/r8", """{"op":"rotate-ca","phase":"committed","instance":"x","updated_unix":1756500000}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r8"), TestContext.Current.CancellationToken);

        // Assert — K4: done + снапшот; никаких мутаций ключей кластера
        rig.Get("/valkeyworker/work/r8").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot");
        rig.Driver.Removed.Should().BeEmpty();
        rig.Valkey.LastCaPem.Should().BeNull("PING в хвосте не выполняется");
    }
}
