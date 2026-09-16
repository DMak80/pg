using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Ensure per-cluster кредов (arch/21 §4): txn put-if-absent отсутствующих,
// проигрыш → re-read, частичные креды — дозаполнение только отсутствующих.
public class ClusterSecretEnsurerTests
{
    private static ValkeyWorker.Provisioning.Processes.ClusterSecretEnsurer NewEnsurer(Fakes.FakeEtcd etcd)
        => new(etcd, ["http://etcd:2379"]);

    private static bool IsPassword(string value)
        => value.Length == 32 && value.All(char.IsAsciiLetterOrDigit);

    [Fact]
    public async Task ПервоеEnsure_ОбеПарыСгенерированыЧерезPutIfAbsent()
    {
        // Arrange: пустой etcd.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: обе пары 32 симв [A-Za-z0-9]; txn — 4 compare NotExists + 4 put.
        result.IsSuccess.Should().BeTrue();
        IsPassword(result.Value.AdminPassword).Should().BeTrue();
        IsPassword(result.Value.AppPassword).Should().BeTrue();
        etcd.Store["/valkey/clusters/demo/admin_user"].Value.Should().Be("admin");
        etcd.Store["/valkey/clusters/demo/app_user"].Value.Should().Be("app");
        var txn = etcd.Txns.Should().ContainSingle().Subject;
        txn.Compare.Should().HaveCount(4);
        txn.Compare.Should().OnlyContain(c => c.Target == TxnTarget.Version && c.Num == 0);
    }

    [Fact]
    public async Task КонкурентныйEnsure_ЧужиеПаролиВозвращеныReReadом()
    {
        // Arrange: пароли уже есть (другой инстанс выиграл гонку).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkey/clusters/demo/admin_user", "admin");
        etcd.Seed("/valkey/clusters/demo/admin_password", "FOREIGNADMINPASSWORD0123456789x");
        etcd.Seed("/valkey/clusters/demo/app_user", "app");
        etcd.Seed("/valkey/clusters/demo/app_password", "FOREIGNAPPPASSWORD012345678901x");
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: re-read вернул существующие, перезаписи нет.
        result.IsSuccess.Should().BeTrue();
        result.Value.AdminPassword.Should().Be("FOREIGNADMINPASSWORD0123456789x");
        result.Value.AppPassword.Should().Be("FOREIGNAPPPASSWORD012345678901x");
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task ЧастичныеКреды_ДозаполняютсяТолькоОтсутствующие()
    {
        // Arrange: есть app-креды, admin отсутствует.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkey/clusters/demo/app_user", "app");
        etcd.Seed("/valkey/clusters/demo/app_password", "EXISTINGAPPPASSWORD01234567890x");
        var ensurer = NewEnsurer(etcd);

        // Act
        var result = await ensurer.EnsureAsync("demo", TestContext.Current.CancellationToken);

        // Assert: app не тронут, admin добран txn-ом на 2 ключа.
        result.IsSuccess.Should().BeTrue();
        result.Value.AppPassword.Should().Be("EXISTINGAPPPASSWORD01234567890x");
        IsPassword(result.Value.AdminPassword).Should().BeTrue();
        var txn = etcd.Txns.Should().ContainSingle().Subject;
        txn.Compare.Should().HaveCount(2);
        txn.Success.Select(op => op.Should().BeOfType<TxnOp.Put>().Subject.Key)
            .Should().OnlyContain(k => k.Contains("admin_"));
    }
}
