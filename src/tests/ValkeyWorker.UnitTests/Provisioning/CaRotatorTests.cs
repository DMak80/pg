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
    public async Task Ticket_OpensWindow_StagingThenBundle()
    {
        // Arrange — канонический кластер + заявка
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c6");
        rig.SeedTicket("c6");

        // Act — тик 1. Против реализации Task 2 цикл останавливается после D:
        // эти ассерты — ПРОМЕЖУТОЧНЫЕ (staging жив, ca_pem = bundle). После
        // реализации Task 3 (R/C/K4) ОДИН RunAsync доводит цикл до коммита —
        // кейс ОБНОВЛЯЕТСЯ в Task 3 Step 3.1 на финальные ассерты (коммит
        // сворачивает bundle и удаляет staging — промежуточные устареют).
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c6"), TestContext.Current.CancellationToken);

        // Assert — окно открыто: staging есть, bundle записан, journal phase-d
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var nextPem = rig.Get("/valkey/clusters/c6/ca_next_pem")!;
        nextPem.Should().NotBeNullOrEmpty("фаза P создала staging");
        rig.Get("/valkey/clusters/c6/ca_next_key").Should().NotBeNull();
        var caPem = rig.Get("/valkey/clusters/c6/ca_pem")!;
        caPem.Should().StartWith(oldPem).And.EndWith(nextPem,
            "bundle = OLD + \"\\n\" + NEW (фаза D)");
        rig.Get("/valkeyworker/work/c6").Should().Contain("phase-d");
    }

    [Fact]
    public async Task OpenWindow_SkipsGuards_ReplaysFromFact()
    {
        // Arrange — окно ОТКРЫТО ПОСЕВОМ ca_next_* (иначе K0.5 даст Waiting
        // ещё до открытия), ПРИ живой пароль-заявке: K0.3 срабатывает по
        // факту staging — guard'ы K0.4–K0.6 НЕ выполняются (spec §5 K0.3)
        var rig = Rig.Create();
        rig.SeedTls("c7");
        rig.SeedTicket("c7");
        rig.SeedWindow("c7"); // ca_next_pem/ca_next_key — окно открыто
        rig.Put("/valkeyworker/rotations/c7",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act — доигрывание идёт ПРИ живой пароль-заявке
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c7"), TestContext.Current.CancellationToken);

        // Assert — не Waiting: окно открыто, доигрывание пошло (Task 3 Step 3.1
        // усилит ассерты финалом: done + коммит при живой пароль-заявке)
        outcome.Value.Should().NotBe(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkey/clusters/c7/ca_next_key").Should().NotBeNull();
    }

    [Fact]
    public async Task ReplayTick_ReusesExistingStaging_NoSecondGeneration()
    {
        // Arrange — staging уже лежит (например, после краха между P и D)
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c8");
        rig.SeedTicket("c8");
        var (foreignPem, foreignKey) = rig.SeedWindow("c8");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c8"), TestContext.Current.CancellationToken);

        // Assert — чужая staging переиспользована (re-read), своя не сгенерирована
        // (Task 3 Step 3.1 перепишет под финал: коммит от ЧУЖОЙ staging)
        rig.Get("/valkey/clusters/c8/ca_next_pem").Should().Be(foreignPem);
        rig.Get("/valkey/clusters/c8/ca_pem").Should().EndWith(foreignPem);
    }

    [Fact]
    public async Task BundleAlreadyContainsNext_PutSkipped()
    {
        // Arrange — D отработан (bundle в ca_pem), R/C нет. Кейс проверяет
        // пропуск put (string.Contains-детект) по неизменности ModRevision
        // ca_pem. Чтобы после Task 3 фаза C не перезаписала ca_pem (это
        // изменило бы ревизию и сломало проверяемый факт), окно ОСТАНАВЛИВАЕТСЯ
        // инжектом TxnFault ТОЛЬКО на коммит-txn фазы C (5 success-операций;
        // инжект задействуется в Task 3 Step 3.1 — против Task 2 C-txn не
        // существует и инжект бездействует).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c9");
        rig.SeedTicket("c9");
        var (nextPem, nextKey) = rig.SeedWindow("c9");
        rig.Put("/valkey/clusters/c9/ca_pem", oldPem + "\n" + nextPem);
        var revisionBefore = rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision;

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c9"), TestContext.Current.CancellationToken);

        // Assert — put пропущен: ModRevision ca_pem не тунул (пере-put
        // изменил бы ревизию записи в Store FakeEtcd)
        rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision.Should().Be(revisionBefore);
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
}
