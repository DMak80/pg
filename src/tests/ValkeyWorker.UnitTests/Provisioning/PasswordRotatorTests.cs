using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// PasswordRotator E1–E3 (arch/21 §5 E): окно двух паролей, доигрывание по
// journal-фазе, изоляция ролей, битая заявка.
public class PasswordRotatorTests
{
    private const string OldAdmin = "AdminOldPassword0123456789abcdef12";
    private const string OldApp = "AppOldPassword0123456789abcdef12345";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeValkeyConnection Valkey = new();
        public ClaimStore Claims = null!;
        public ValkeyWorker.Provisioning.Processes.PasswordRotator Rotator = null!;

        public Rig()
        {
            Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], Etcd, TimeProvider.System);
            Rotator = new ValkeyWorker.Provisioning.Processes.PasswordRotator(
                Etcd, ["http://etcd:2379"], Claims,
                new WorkJournal("/valkeyworker", Etcd, ["http://etcd:2379"]),
                Valkey, () => "NewPassword0123456789abcdefgh12345");
        }

        public void SeedActive(string cluster)
        {
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", "h1:17001");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", OldApp);
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", OldAdmin);
            // Нода: admin-проба воркера, app с OLD-паролем.
            Valkey.AddUser("admin", OldAdmin);
            Valkey.AddUser("app", OldApp, "~*", "+@read", "+@write");
        }

        public void SeedRotation(string cluster, string role) =>
            Etcd.Seed($"/valkeyworker/rotations/{cluster}",
                $$"""{"role":"{{role}}","requested_unix":1756500000,"requested_by":"panel"}""");

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task ПолныйЦикл_ОкноДвухПаролейЗатемОдинNew()
    {
        // Arrange: заявка ротации app.
        const string cluster = "rot";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: etcd содержит NEW (E2), заявка удалена, E3 удалил OLD —
        // в фейке проверяем через AUTH: OLD отвергнут, NEW работает.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/rot/app_password"].Value.Should().Be("NewPassword0123456789abcdefgh12345");
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/rot");
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Valkey.Users["app"].Passwords.Should().Contain("NewPassword0123456789abcdefgh12345");
        // журнал: полный цикл фаз
        rig.Etcd.Store[$"/valkeyworker/work/rot"].Value.Should().Contain("done");
    }

    [Fact]
    public async Task ОтказПослеЕ1_ПовторТикаДоигрываетСЕ2()
    {
        // Arrange: journal-фаза e1-added (тик упал после E1).
        const string cluster = "resume";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        await new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]).WritePhaseAsync(
            "resume", "rotate", "e1-added", "inst-1", null, TestContext.Current.CancellationToken);
        // OLD+NEW оба валидны (E1 уже применён прошлым тиком).
        rig.Valkey.Users["app"].Passwords.Add("NewPassword0123456789abcdefgh12345");

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: доиграно с E2 (etcd NEW, заявка del) и E3 (OLD удалён).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/resume/app_password"].Value.Should().Be("NewPassword0123456789abcdefgh12345");
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/resume");
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
    }

    [Fact]
    public async Task РотацияApp_НеТрогаетAdmin()
    {
        // Arrange
        const string cluster = "iso";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: admin-кред в etcd и на ноде прежний.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/iso/admin_password"].Value.Should().Be(OldAdmin);
        rig.Valkey.Users["admin"].Passwords.Should().Contain(OldAdmin);
    }

    [Fact]
    public async Task БитаяЗаявка_DelСJournal()
    {
        // Arrange: role вне канона.
        const string cluster = "junk";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "root");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: заявка удалена, journal invalid-request, пароли нетронуты.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/junk");
        rig.Etcd.Store[$"/valkeyworker/work/junk"].Value.Should().Contain("invalid-request");
        rig.Etcd.Store[$"/valkey/clusters/junk/app_password"].Value.Should().Be(OldApp);
    }
}
