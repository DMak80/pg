using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// Тесты ensure per-cluster секретов (arch/16 §4, t03 Ф2): CA + креды admin/app
// одной txn put-if-absent; существующие ключи не переписываются; проигранная
// txn → re-read; генерация паролей 32 симв [A-Za-z0-9].

public class ClusterSecretEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    [Fact]
    public async Task Ensure_EmptyCluster_CreatesAllSixKeysInOneTxn()
    {
        // Arrange: пустой префикс кластера.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = new ClusterSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var ensured = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: admin_user="admin", пароли 32 симв, CA — валидный PEM-ключ+серт.
        ensured.IsSuccess.Should().BeTrue();
        var s = ensured.Value;
        s.AdminUser.Should().Be("admin");
        s.AdminPassword.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        s.AppUser.Should().Be("app");
        s.AppPassword.Should().MatchRegex("^[A-Za-z0-9]{32}$");
        s.CaPem.Should().StartWith("-----BEGIN CERTIFICATE-----");
        s.CaKey.Should().StartWith("-----BEGIN PRIVATE KEY-----");
        ClusterPki.TryParseCertificate(s.CaPem, out _).Should().BeTrue();
        ClusterPki.TryParseRsaKey(s.CaKey, out _).Should().BeTrue();
        etcd.Store[$"/kafka/clusters/events/admin_user"].Value.Should().Be("admin");
        etcd.Store[$"/kafka/clusters/events/ca_pem"].Value.Should().Be(s.CaPem);
        etcd.Store[$"/kafka/clusters/events/ca_key"].Value.Should().Be(s.CaKey);
        etcd.Store[$"/kafka/clusters/events/app_user"].Value.Should().Be("app");
    }

    [Fact]
    public async Task Ensure_PartialKeys_ExistingNotOverwritten_CaRegeneratedOnlyIfAbsent()
    {
        // Arrange: панель/прошлый ensure записал app_user=app, app_password=Secret…;
        // admin/CA отсутствуют.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "Existing0123456789AAAAAAAAA");
        var ensurer = new ClusterSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var ensured = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: существующие не переписаны, отсутствующие добраны той же txn-механикой.
        ensured.IsSuccess.Should().BeTrue();
        ensured.Value.AppUser.Should().Be("app");
        ensured.Value.AppPassword.Should().Be("Existing0123456789AAAAAAAAA");
        etcd.Store["/kafka/clusters/events/app_password"].Value
            .Should().Be("Existing0123456789AAAAAAAAA");
        etcd.Store["/kafka/clusters/events/ca_pem"].Value.Should().NotBeNull();
        ensured.Value.CaPem.Should().StartWith("-----BEGIN CERTIFICATE-----");
    }

    [Fact]
    public async Task Ensure_BothAppKeysExist_UsesExisting()
    {
        // Arrange: ключи уже есть (re-run/ротация) — только app-контур.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "Existing0123456789AbCdEf012");
        var ensurer = new ClusterSecretEnsurer(etcd, [Ep]);

        // Act: ensure.
        var result = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: существующие значения возвращены, не перегенерированы.
        result.IsSuccess.Should().BeTrue();
        result.Value.AppPassword.Should().Be("Existing0123456789AbCdEf012");
        etcd.Store["/kafka/clusters/events/app_password"].Value
            .Should().Be("Existing0123456789AbCdEf012");
    }

    [Fact]
    public async Task Ensure_ReRun_Idempotent_CaNotRegenerated()
    {
        // Arrange: первый ensure создал полный набор.
        var etcd = new Fakes.FakeEtcd();
        var ensurer = new ClusterSecretEnsurer(etcd, [Ep]);
        var first = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Act: повторный ensure.
        var second = await ensurer.EnsureAsync("events", CancellationToken.None);

        // Assert: идемпотентность — все шесть значений не изменились.
        second.IsSuccess.Should().BeTrue();
        second.Value.CaPem.Should().Be(first.Value.CaPem);
        second.Value.CaKey.Should().Be(first.Value.CaKey);
        second.Value.AdminPassword.Should().Be(first.Value.AdminPassword);
        second.Value.AppPassword.Should().Be(first.Value.AppPassword);
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
