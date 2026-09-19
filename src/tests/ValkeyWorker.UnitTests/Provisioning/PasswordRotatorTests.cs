using System.Text.RegularExpressions;
using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// PasswordRotator E1–E3 (arch/21 §5 E): окно двух паролей, доигрывание по
// стейту work/<C>/rotation (Тот ЖЕ NEW после отказа; E3 без заявки; срыв
// compare E2 — пароль уже NEW — доигрывается), условный del заявки (compare
// payload из стейта — чужая/снятая не трогается), изоляция ролей, битая
// заявка. Генератор — реальный (ValkeyPasswordGenerator): NEW читается из
// etcd/стейта, фиксированная строка не маскирует регенерацию.
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
            var (caPem, caKeyPem) = ValkeyPki.GenerateCa(cluster);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_pem", caPem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_key", caKeyPem);
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
    public async Task ОтказPutСтейтаПослеЕ1_ДоигрываниеСТемЖеNew()
    {
        // Arrange: стейт пишется ДО E1 (e1-pending); отказ put стейта ПОСЛЕ
        // успешного E1 — падает второй put ключа стейта (промоция e1-added).
        const string cluster = "putfail";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var statePuts = 0;
        rig.Etcd.PutFault = key => key == stateKey && ++statePuts >= 2
            ? Result.Failed(new ApplicationException("etcd put failed"))
            : null;

        var first = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);
        first.IsSuccess.Should().BeFalse("put стейта после E1 упал");

        // Стейт e1-pending записан до E1, NEW1 уже добавлен на ноду.
        var state = rig.Etcd.Store[stateKey].Value;
        state.Should().Contain("e1-pending");
        var stagedNew = Regex.Match(state, @"""new"":""([A-Za-z0-9]{32})""").Groups[1].Value;
        stagedNew.Should().NotBeEmpty();
        rig.Valkey.Users["app"].Passwords.Should().Contain(stagedNew, "E1 успел до отказа put");
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(OldApp);

        // Act: второй тик (отказ ушёл) — доигрывание с ТЕМ ЖЕ NEW из стейта
        // (не свежая генерация — иначе NEW1 остался бы «осиротевшим»).
        rig.Etcd.PutFault = null;
        var second = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: закоммичен ТЕМ ЖЕ NEW1; на ноде ровно один NEW (OLD снят E3,
        // никакого NEW2), заявка и стейт доигрывания исчерпаны.
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew);
        rig.Valkey.Users["app"].Passwords.Should().HaveCount(1,
            "E1 повторён тем же NEW (идемпотентно), без осиротевших паролей");
        rig.Valkey.Users["app"].Passwords.Should().Contain(stagedNew);
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/rotations/{cluster}");
        rig.Etcd.Store.Should().NotContainKey(stateKey);
    }

    [Theory]
    [InlineData("e1-added")]
    [InlineData("e2-committed")]
    public async Task ДоигрываниеБезEndpoints_FailedБезNre(string phase)
    {
        // Arrange: живой стейт ротации, снапшот тика без endpoints (пришёл
        // неполный Active-факт) — guard → Result.Failed, а не NRE.
        const string cluster = "noep";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        var rig = new Rig();
        rig.SeedActive(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Etcd.Seed(stateKey,
            $$"""{"phase":"{{phase}}","role":"app","old":"{{OldApp}}","new":"NewPassword0123456789abcdef0123456789abcd","requested_by":"panel"}""");
        var before = rig.Etcd.Store[stateKey].Value;
        var snap = rig.Snapshot(cluster) with { Endpoints = null };

        // Act
        var result = await rig.Rotator.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: штатный Failed без исключения; стейт и кред не тронуты —
        // доигрывание повторится следующим тиком со свежим снапшотом.
        result.IsSuccess.Should().BeFalse("без endpoints доигрывание невозможно");
        result.Error.Should().BeOfType<ApplicationException>();
        rig.Etcd.Store[stateKey].Value.Should().Be(before);
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(OldApp, "до E2 дело не дошло");
        rig.Valkey.SetUserCalls.Should().BeEmpty("ни E1, ни E3 не выполняются без endpoints");
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
    public async Task КрашМеждуЕ2ТхнИСтейтом_ВторойТикПромотитИДоиграет()
    {
        // Arrange: отказ ТРЕТЬЕЙ записи ключа стейта (промоция e2-committed) —
        // txn E2 уже закоммичена (etcd NEW, заявка снята), стейт остался e1-added.
        const string cluster = "e2crash";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.SeedRotation(cluster, "app");
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var statePuts = 0;
        rig.Etcd.PutFault = key => key == stateKey && ++statePuts >= 3
            ? Result.Failed(new ApplicationException("etcd put failed"))
            : null;

        var first = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: окно «E2 закоммичен, стейт не записан» — etcd NEW, стейт
        // e1-added (без исправления второй тик вечный Failed «изменился»).
        first.IsSuccess.Should().BeFalse("запись e2-committed упала после коммита E2");
        var state = rig.Etcd.Store[stateKey].Value;
        state.Should().Contain("e1-added");
        var stagedNew = Regex.Match(state, @"""new"":""([A-Za-z0-9]{32})""").Groups[1].Value;
        stagedNew.Should().NotBeEmpty();
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew,
            "txn E2 закоммичена до отказа put");
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/rotations/{cluster}",
            "условный del выполнен до отказа записи стейта");

        // Act: второй тик — compare E2 не проходит (значение уже NEW),
        // перечитывание подтверждает свой коммит → промоция стейта, E3.
        rig.Etcd.PutFault = null;
        var second = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: доиграно до финала — OLD снят с ноды, стейт исчерпан.
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew);
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Valkey.Users["app"].Passwords.Should().Contain(stagedNew);
        rig.Etcd.Store.Should().NotContainKey(stateKey);
        rig.Etcd.Store[$"/valkeyworker/work/{cluster}"].Value.Should().Contain("done");
    }

    [Fact]
    public async Task СидЕ1ДобавленПарольУжеNew_ТикДоигрываетДоФинала()
    {
        // Arrange: сид-кейс окна из прошлого тика — стейт e1-added, app_password
        // в etcd уже == state.New, на ноде OLD+NEW; заявки нет.
        const string cluster = "seednew";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        const string stagedNew = "StagedNewPassword0123456789abcde";
        var rig = new Rig();
        rig.SeedActive(cluster);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Etcd.Seed($"/valkey/clusters/{cluster}/app_password", stagedNew);
        rig.Etcd.Seed(stateKey,
            $$"""{"phase":"e1-added","role":"app","old":"{{OldApp}}","new":"{{stagedNew}}","requested_by":"panel"}""");
        rig.Valkey.Users["app"].Passwords.Add(stagedNew);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: срыв compare E2 доигран — стейт промоутен, OLD снят с ноды.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew);
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Valkey.Users["app"].Passwords.Should().Contain(stagedNew);
        rig.Etcd.Store.Should().NotContainKey(stateKey);
        rig.Etcd.Store[$"/valkeyworker/work/{cluster}"].Value.Should().Contain("done");
    }

    [Fact]
    public async Task ЧужаяЗаявкаДругойРоли_Е2НеУдаляет_ОбрабатываетсяСледующимТиком()
    {
        // Arrange: живой стейт доигрывания app (payload исходной заявки в
        // стейте); панель сняла заявку и поставила ЧУЖУЮ (role=admin).
        const string cluster = "foreign";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        const string foreignPayload = """{"role":"admin","requested_unix":1756500001,"requested_by":"panel"}""";
        const string stagedNew = "StagedNewPassword0123456789abcde";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Etcd.Seed($"/valkeyworker/rotations/{cluster}", foreignPayload);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Etcd.Seed(stateKey,
            $$"""{"phase":"e1-added","role":"app","old":"{{OldApp}}","new":"{{stagedNew}}","requested_by":"panel","request":"{\"role\":\"app\",\"requested_unix\":1756500000,\"requested_by\":\"panel\"}"}""");
        rig.Valkey.Users["app"].Passwords.Add(stagedNew);

        // Act: тик доигрывает E2/E3 своей ротации.
        var first = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: чужая заявка НЕ удалена (compare payload не совпал), своя
        // ротация доиграна.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        rig.Etcd.Store[$"/valkeyworker/rotations/{cluster}"].Value.Should().Be(foreignPayload,
            "чужая заявка не тронута условным del");
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew);
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Etcd.Store.Should().NotContainKey(stateKey);

        // Act: следующий тик обрабатывает чужую заявку (ротация admin).
        var second = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: admin-ротация исполнена полностью — заявка снята, admin-кред
        // сменён, app-кред не тронут.
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/rotations/{cluster}");
        var adminNew = rig.Etcd.Store[$"/valkey/clusters/{cluster}/admin_password"].Value;
        CanonPassword.IsMatch(adminNew).Should().BeTrue("admin_password — NEW канона 32 симв");
        adminNew.Should().NotBe(OldAdmin);
        rig.Valkey.Users["admin"].Passwords.Should().NotContain(OldAdmin);
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew,
            "ротация admin не трогает app-кред");
    }

    [Fact]
    public async Task СвояЗаявкаПейлоадСовпал_УдаляетсяКакРаньше()
    {
        // Arrange: стейт e1-added с payload исходной заявки; заявка в etcd —
        // ТА ЖЕ (байт в байт).
        const string cluster = "ownreq";
        var stateKey = $"/valkeyworker/work/{cluster}/rotation";
        const string ownPayload = """{"role":"app","requested_unix":1756500000,"requested_by":"panel"}""";
        const string stagedNew = "StagedNewPassword0123456789abcde";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Etcd.Seed($"/valkeyworker/rotations/{cluster}", ownPayload);
        await rig.Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        rig.Etcd.Seed(stateKey,
            $$"""{"phase":"e1-added","role":"app","old":"{{OldApp}}","new":"{{stagedNew}}","requested_by":"panel","request":"{\"role\":\"app\",\"requested_unix\":1756500000,\"requested_by\":\"panel\"}"}""");
        rig.Valkey.Users["app"].Passwords.Add(stagedNew);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: своя заявка удалена в E2 (как раньше), ротация доиграна.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().NotContainKey($"/valkeyworker/rotations/{cluster}");
        rig.Etcd.Store[$"/valkey/clusters/{cluster}/app_password"].Value.Should().Be(stagedNew);
        rig.Valkey.Users["app"].Passwords.Should().NotContain(OldApp);
        rig.Etcd.Store.Should().NotContainKey(stateKey);
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
