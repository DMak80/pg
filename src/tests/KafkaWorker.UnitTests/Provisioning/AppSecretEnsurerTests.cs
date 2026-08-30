using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// Тесты ensure per-cluster SASL-секрета (arch/16 §4, порт P1.5 PgWorker):
// absent → txn put-if-absent обоих; txn проигран → re-read существующих;
// генерация 32 симв [A-Za-z0-9].

public class AppSecretEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    [Fact]
    public async Task Ensure_BothAbsent_PutsBothTxn()
    {
        // Arrange: ключей нет.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = new AppSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var result = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: оба ключа появились; user="app"; пароль — 32 симв [A-Za-z0-9].
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/kafka/clusters/events/app_user"].Value.Should().Be("app");
        var password = etcd.Store["/kafka/clusters/events/app_password"].Value;
        password.Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
        result.Value.User.Should().Be("app");
        result.Value.Password.Should().Be(password);
    }

    [Fact]
    public async Task Ensure_BothExist_UsesExisting()
    {
        // Arrange: ключи уже есть (re-run/ротация).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "Existing0123456789AbCdEf012");
        var ensurer = new AppSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var result = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: существующие значения возвращены, не перегенерированы.
        result.IsSuccess.Should().BeTrue();
        result.Value.Password.Should().Be("Existing0123456789AbCdEf012");
        etcd.Store["/kafka/clusters/events/app_password"].Value
            .Should().Be("Existing0123456789AbCdEf012");
    }

    [Fact]
    public async Task Ensure_PartialExists_FillsOnlyMissing()
    {
        // Arrange: только app_user (частичная заявка/мусор).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        var ensurer = new AppSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var result = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: пользователь сохранён, пароль сгенерирован; txn-put только пароля.
        result.IsSuccess.Should().BeTrue();
        result.Value.User.Should().Be("app");
        etcd.Store["/kafka/clusters/events/app_password"].Value
            .Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
    }

    [Fact]
    public void Generator_Alphabet_Length32()
    {
        // Arrange: генератор KafkaPasswordGenerator (по образцу AppSecretGenerator).
        // Act: генерация.
        var password = KafkaPasswordGenerator.Generate();

        // Assert: 32 симв [A-Za-z0-9], два вызова дают разные значения.
        password.Should().HaveLength(KafkaPasswordGenerator.Length)
            .And.MatchRegex("^[A-Za-z0-9]{32}$");
        KafkaPasswordGenerator.Generate().Should().NotBe(password);
    }
}
