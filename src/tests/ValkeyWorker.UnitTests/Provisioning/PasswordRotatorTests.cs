using System.Text.RegularExpressions;
using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// PasswordRotator E1–E3 (arch/21 §5 E): окно двух паролей, доигрывание по
// стейту work/<C>/rotation (Тот ЖЕ NEW после отказа; E3 без заявки), изоляция
// ролей, битая заявка. Генератор — реальный (ValkeyPasswordGenerator): NEW
// читается из etcd/стейта, фиксированная строка не маскирует регенерацию.
public class PasswordRotatorTests
{
    private const string OldAdmin = "AdminOldPassword0123456789abcdef12";
    private const string OldApp = "AppOldPassword0123456789abcdef12345";

    private static readonly Regex CanonPassword = new("^[A-Za-z0-9]{32}$", RegexOptions.Compiled);

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
                Valkey);
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

        // Assert: etcd содержит NEW (E2, канон 32 симв [A-Za-z0-9]), заявка и
        // стейт доигрывания удалены, E3 удалил OLD — на ноде ровно NEW.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var newInEtcd = rig.Etcd.Store[$"/valkey/clusters/rot/app_password"].Value;
        CanonPassword.IsMatch(newInEtcd).Should().BeTrue("NEW — канон 32 симв [A-Za-z0-9]");
        newInEtcd.Should().NotBe(OldApp);
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/rot");
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/work/rot/rotation");
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Valkey.Users["app"].Passwords.Should().Contain(newInEtcd);
        // журнал: полный цикл фаз
        rig.Etcd.Store[$"/valkeyworker/work/rot"].Value.Should().Contain("done");
    }

    [Fact]
    public async Task ОтказПослеЕ1_ДоигрываниеСТемЖеNew()
    {
        // Arrange: E2-txn падает в первом тике (E1 уже добавил NEW на ноду).
        const string cluster = "resume";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        // Отказ только E2-txn (compare value==OLD); прочие txn — штатно (null).
        rig.Etcd.TxnFault = req => req.Compare.Any(c => c.Target == TxnTarget.Value
            && c.Key == $"/valkey/clusters/{cluster}/app_password")
            ? Result<TxnResult>.Failed(new ApplicationException("etcd txn failed"))
            : null;

        var first = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        first.IsSuccess.Should().BeFalse("E2 упал — отказ после E1");

        // Стейт e1-added несёт NEW; OLD ещё в etcd.
        var state = rig.Etcd.Store["/valkeyworker/work/resume/rotation"].Value;
        state.Should().Contain("e1-added");
        var stagedNew = Regex.Match(state, @"""new"":""([A-Za-z0-9]{32})""").Groups[1].Value;
        stagedNew.Should().NotBeEmpty();
        rig.Etcd.Store[$"/valkey/clusters/resume/app_password"].Value.Should().Be(OldApp);

        // Act: второй тик (отказ ушёл) — доигрывание со стейта.
        rig.Etcd.TxnFault = null;
        var second = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: закоммичен ТЕМ ЖЕ NEW из стейта (не свежая генерация — иначе
        // на ноде остался бы валидный «осиротевший» пароль), E3 снял OLD.
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/resume/app_password"].Value.Should().Be(stagedNew);
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/resume");
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/work/resume/rotation");
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Valkey.Users["app"].Passwords.Should().Contain(stagedNew);
    }

    [Fact]
    public async Task ОтказМеждуЕ2иЕ3_ДоигрываниеЕ3БезЗаявки()
    {
        // Arrange: E3 (второй вызов SETUSER тика) падает — заявка уже удалена
        // в E2, стейт e2-committed — единственный след недоигранной ротации.
        const string cluster = "e2e3";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Valkey.SetUserFailFromIndex = 1;

        var first = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        first.IsSuccess.Should().BeFalse("E3 упал — краш между E2 и E3");
        rig.Etcd.Store[$"/valkey/clusters/e2e3/app_password"].Value.Should().NotBe(OldApp, "E2 закоммитил NEW");
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/rotations/e2e3", "заявка снята в E2");
        rig.Etcd.Store["/valkeyworker/work/e2e3/rotation"].Value.Should().Contain("e2-committed");
        rig.Valkey.Users["app"].Passwords.Should().Contain(OldApp, "OLD ещё не снят (E3 упал)");

        // Act: второй тик — заявки НЕТ, но стейт e2-committed доигрывает E3.
        rig.Valkey.SetUserFailFromIndex = null;
        var second = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: OLD снят с ноды (без стейта он остался бы валидным навсегда).
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Etcd.Store.Should().NotContainKey("/valkeyworker/work/e2e3/rotation");
        rig.Etcd.Store[$"/valkeyworker/work/e2e3"].Value.Should().Contain("done");
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
