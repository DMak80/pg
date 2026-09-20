using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Ensure per-cluster кредов + CA (arch/21 §4, t06): txn put-if-absent
// отсутствующих из шести ключей, проигрыш → re-read, частичный набор —
// дозаполнение только отсутствующих; CA из одной генерации (пара).
public class ClusterSecretEnsurerTests
{
    private static ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer NewEnsurer(Fakes.FakeEtcd etcd)
        => new(etcd, ["http://etcd:2379"]);

    private static bool IsPassword(string value)
        => value.Length == 32 && value.All(char.IsAsciiLetterOrDigit);

    [Fact]
    public async Task ПервоеEnsure_ШестьКлючейОднойTxn()
    {
        // Arrange: пустой etcd.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: креды валидны; ca_pem/ca_key созданы ТОЙ ЖЕ txn (6 compare
        // NotExists + 6 put); PEM-пара валидна (t06).
        result.IsSuccess.Should().BeTrue();
        IsPassword(result.Value.AdminPassword).Should().BeTrue();
        IsPassword(result.Value.AppPassword).Should().BeTrue();
        ValkeyPki.TryParseCertificate(result.Value.CaPem, out _).Should().BeTrue();
        ValkeyPki.TryParseRsaKey(result.Value.CaKey, out _).Should().BeTrue();
        etcd.Store["/valkey/clusters/demo/admin_user"].Value.Should().Be("admin");
        etcd.Store["/valkey/clusters/demo/app_user"].Value.Should().Be("app");
        etcd.Store["/valkey/clusters/demo/ca_pem"].Value.Should().Be(result.Value.CaPem);
        etcd.Store["/valkey/clusters/demo/ca_key"].Value.Should().Be(result.Value.CaKey);
        var txn = etcd.Txns.Should().ContainSingle().Subject;
        txn.Compare.Should().HaveCount(6);
        txn.Compare.Should().OnlyContain(c => c.Target == TxnTarget.Version && c.Num == 0);
    }

    [Fact]
    public async Task ПовторныйEnsure_БезЗаписи()
    {
        // Arrange: первый ensure создал полный набор.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = NewEnsurer(etcd);
        await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);
        var firstCaPem = etcd.Store["/valkey/clusters/demo/ca_pem"].Value;

        // Act: повторный ensure (re-run/следующий тик).
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: второй txn нет, CA не перегенерирован (put-if-absent).
        result.IsSuccess.Should().BeTrue();
        etcd.Txns.Should().ContainSingle();
        etcd.Store["/valkey/clusters/demo/ca_pem"].Value.Should().Be(firstCaPem);
    }

    [Fact]
    public async Task КонкурентныйEnsure_ЧужиеПаролиВозвращеныReReadом()
    {
        // Arrange: полный набор уже есть (другой инстанс выиграл гонку).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkey/clusters/demo/admin_user", "admin");
        etcd.Seed("/valkey/clusters/demo/admin_password", "FOREIGNADMINPASSWORD0123456789x");
        etcd.Seed("/valkey/clusters/demo/app_user", "app");
        etcd.Seed("/valkey/clusters/demo/app_password", "FOREIGNAPPPASSWORD012345678901x");
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("demo");
        etcd.Seed("/valkey/clusters/demo/ca_pem", caPem);
        etcd.Seed("/valkey/clusters/demo/ca_key", caKeyPem);
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: re-read вернул существующие, перезаписи нет.
        result.IsSuccess.Should().BeTrue();
        result.Value.AdminPassword.Should().Be("FOREIGNADMINPASSWORD0123456789x");
        result.Value.AppPassword.Should().Be("FOREIGNAPPPASSWORD012345678901x");
        result.Value.CaPem.Should().Be(caPem);
        result.Value.CaKey.Should().Be(caKeyPem);
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task ЧастичныеКреды_ДозаполняютсяТолькоОтсутствующие()
    {
        // Arrange: есть app-креды и CA, admin отсутствует.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkey/clusters/demo/app_user", "app");
        etcd.Seed("/valkey/clusters/demo/app_password", "EXISTINGAPPPASSWORD01234567890x");
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("demo");
        etcd.Seed("/valkey/clusters/demo/ca_pem", caPem);
        etcd.Seed("/valkey/clusters/demo/ca_key", caKeyPem);
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: app и CA не тронуты, admin добран txn-ом на 2 ключа.
        result.IsSuccess.Should().BeTrue();
        result.Value.AppPassword.Should().Be("EXISTINGAPPPASSWORD01234567890x");
        result.Value.CaPem.Should().Be(caPem);
        IsPassword(result.Value.AdminPassword).Should().BeTrue();
        var txn = etcd.Txns.Should().ContainSingle().Subject;
        txn.Compare.Should().HaveCount(2);
        txn.Success.Select(op => op.Should().BeOfType<TxnOp.Put>().Subject.Key)
            .Should().OnlyContain(k => k.Contains("admin_"));
    }

    [Fact]
    public async Task ЧастичныйCa_ДобираетсяИзОднойГенерации()
    {
        // Arrange: есть ca_pem, ca_key потерян (spec §5: частичный CA).
        var etcd = new Fakes.FakeEtcd();
        var (caPem, _) = ValkeyPki.GenerateCa("demo");
        etcd.Seed("/valkey/clusters/demo/ca_pem", caPem);
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: добран только ca_key (остальные 4 кред-ключа тоже отсутствуют:
        // 5 compare всего); ca_pem не переписан.
        result.IsSuccess.Should().BeTrue();
        result.Value.CaPem.Should().Be(caPem);
        ValkeyPki.TryParseRsaKey(result.Value.CaKey, out _).Should().BeTrue();
        var txn = etcd.Txns.Should().ContainSingle().Subject;
        txn.Compare.Should().HaveCount(5);
        txn.Success.Select(op => op.Should().BeOfType<TxnOp.Put>().Subject.Key)
            .Should().OnlyContain(k => !k.EndsWith("ca_pem"));
    }
}
