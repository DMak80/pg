using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Etcd.Client;
using ValkeyWorker.App;
using ValkeyWorker.App.Loops;
using ValkeyWorker.UnitTests.App;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Цикл процессов (t06): Active-ветка — миграция TLS ПЕРВЫМ шагом, InProgress
// ⇒ надзор/converge/ротация в этом тике не идут (журнал work/<C> остаётся
// op=migrate-tls done — не перезаписан надзором).
public class ValkeyClusterProcessesTests
{
    private static readonly FixedTimeProvider Clock = new();

    private const string Image = "valkey/valkey:9.1.2";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public ValkeyClusterProcesses Processes = null!;

        public Rig()
        {
            Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System);
            var journal = new WorkJournal("/valkeyworker", Etcd, ["http://etcd:2379"]);
            var options = new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");
            var tlsProvisioner = new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(Driver, Image, Clock);
            Processes = new ValkeyClusterProcesses(
                Etcd, new FixedOptionsMonitor(new ValkeyWorkerOptions
                {
                    Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
                    Parallelism = new ParallelismOptions { MaxClusters = 1 },
                }),
                Claims, journal,
                new ValkeyWorker.Provisioning.Processes.ProvisioningProcess(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                    new PortAllocLock("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System, "inst-1"),
                    new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                        Etcd, ["http://etcd:2379"],
                        NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                    new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(Etcd, ["http://etcd:2379"]),
                    tlsProvisioner, Valkey, options),
                new ValkeyWorker.Provisioning.Processes.DeprovisioningProcess(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal),
                new ValkeyWorker.Provisioning.Processes.NodeSupervisor(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal, Valkey, options,
                    new ValkeyWorker.Provisioning.Processes.PortAllocHealer(
                        Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                        new PortAllocLock("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System, "inst-1"),
                        new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                            Etcd, ["http://etcd:2379"],
                            NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
                        options),
                    tlsProvisioner, Clock),
                new ValkeyWorker.Provisioning.Processes.ConfigConverger(
                    Valkey, Etcd, ["http://etcd:2379"], journal),
                new ValkeyWorker.Provisioning.Processes.PasswordRotator(
                    Etcd, ["http://etcd:2379"], Claims, journal, Valkey,
                    ValkeyWorker.Core.Model.ValkeyPasswordGenerator.Generate),
                new ValkeyWorker.Provisioning.Processes.TlsMigrator(
                    Etcd, ["http://etcd:2379"], Driver, Claims, journal,
                    new ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer(Etcd, ["http://etcd:2379"]),
                    tlsProvisioner, Valkey, options, snapshot: null, Clock),
                NullLogger<ValkeyClusterProcesses>.Instance);
        }

        // Премиграционный Active-кластер: без ca-ключей, plain-контейнер.
        public void SeedPlain(string cluster, int port = 17001)
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
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--appendonly", "no"], "valkey/valkey:9.1.2", "id-plain");
        }
    }

    [Fact]
    public async Task ActiveTick_МиграцияПервой_НадзорОтложен()
    {
        // Arrange: премиграционный Active-кластер (plain-контейнер, без CA).
        var rig = new Rig();
        rig.SeedPlain("tlsfirst");

        // Act: один тик цикла.
        var claims = await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert: миграция доведена до done в ЭТОМ тике; журнал НЕ перезаписан
        // надзором (supervising) — надзор в этом тике не шёл (InProgress-выход).
        claims.Should().BeGreaterThanOrEqualTo(0);
        var journal = rig.Etcd.Store["/valkeyworker/work/tlsfirst"].Value;
        journal.Should().Contain("\"migrate-tls\"").And.Contain("\"done\"");
        journal.Should().NotContain("supervis");
        // Контейнер пересоздан с TLS-args (миграция T2).
        rig.Driver.Ensured.Should().ContainSingle();
        rig.Driver.Ensured[0].Args.Should().Contain("--tls-port");
    }
}
