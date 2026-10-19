using System.Text.RegularExpressions;
using FluentAssertions;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// ClusterSecretsStore — креды кластера per-cluster в etcd (spec §3.1, arch/23 §6):
// генерация txn put-if-absent (повтор — те же значения), чтение с проверкой
// полноты, спец-ключ etcd-backup из env-пары S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}
// (полупара — диагностика БЕЗ значений), ротация — RMW-замена целиком.
public class ClusterSecretsStoreTests
{
    private const string Cluster = "c1";
    private const string RootUser = "s3w-c1-root";
    private const string BackupAkEnv = "S3W_ETCD_BACKUP_ACCESSKEY";
    private const string BackupSkEnv = "S3W_ETCD_BACKUP_SECRETKEY";

    private static string SecretsKey(string cluster) => $"/s3/clusters/{cluster}/secrets";

    // Детерминированные 32-симв значения [A-Za-z0-9] для фиксур канонического JSON.
    private static readonly string RootPassword32 = "rp" + new string('0', 30);
    private static readonly string Ak32 = "ak" + new string('1', 30);
    private static readonly string Sk32 = "sk" + new string('2', 30);
    private static readonly string PanelAk32 = "pa" + new string('3', 30);
    private static readonly string PanelSk32 = "ps" + new string('4', 30);

    private static (ClusterSecretsStore Store, Fakes.FakeEtcd Etcd) Rig(Func<string?, string?>? getenv = null)
    {
        var etcd = new Fakes.FakeEtcd();
        return (new ClusterSecretsStore(etcd, ["http://etcd:2379"], getenv), etcd);
    }

    // getenv из словаря: имя переменной → значение (отсутствующие — null).
    private static Func<string?, string?> Getenv(Dictionary<string, string?> env)
        => name => name is null ? null : env.GetValueOrDefault(name);

    private static ClusterAccessKey AccessKeyOf(ClusterSecrets secrets, string name)
        => secrets.AccessKeys.Single(k => k.Name == name);

    // Сгенерированное значение: ровно 32 символа [A-Za-z0-9].
    private static bool SecretsShape(string value)
        => Regex.IsMatch(value, "^[A-Za-z0-9]{32}$");

    // Канонический JSON spec §3.1 (snake_case) для негативных фиксур TryParse.
    private static string AccessKeyJson(string name, string accessKey, string secretKey, string policy)
        => $$"""{"name":"{{name}}","access_key":"{{accessKey}}","secret_key":"{{secretKey}}","policy":"{{policy}}"}""";

    private static string CanonicalJson(string rootPassword, string accessKeysJson)
        => $$"""{"root_user":"{{RootUser}}","root_password":"{{rootPassword}}","access_keys":[{{accessKeysJson}}]}""";

    [Fact]
    public void Serialize_TryParse_RoundTrip()
    {
        // Arrange: модель с ключами backups(rw)+panel(ro), фиксированные литералы.
        var model = new ClusterSecrets(
            RootUser,
            "root-password-fixed",
            [
                new ClusterAccessKey("backups", "ak-backups-fixed", "sk-backups-fixed", "read-write"),
                new ClusterAccessKey("panel", "ak-panel-fixed", "sk-panel-fixed", "read-only"),
            ]);

        // Act: сериализация и обратный разбор.
        var parsed = ClusterSecretsStore.TryParse(ClusterSecretsStore.Serialize(model));

        // Assert: эквивалентная модель (по значениям полей).
        parsed.Should().NotBeNull();
        parsed!.RootUser.Should().Be(model.RootUser);
        parsed.RootPassword.Should().Be(model.RootPassword);
        parsed.AccessKeys.Should().HaveCount(2);
        var backups = AccessKeyOf(parsed, "backups");
        backups.AccessKey.Should().Be("ak-backups-fixed");
        backups.SecretKey.Should().Be("sk-backups-fixed");
        backups.Policy.Should().Be("read-write");
        var panel = AccessKeyOf(parsed, "panel");
        panel.AccessKey.Should().Be("ak-panel-fixed");
        panel.SecretKey.Should().Be("sk-panel-fixed");
        panel.Policy.Should().Be("read-only");
    }

    [Fact]
    public void TryParse_BrokenOrIncompleteJson_Null()
    {
        // Arrange: битый JSON и три неполных варианта канонического формата.
        var fullKeys = AccessKeyJson("backups", Ak32, Sk32, "read-write")
            + "," + AccessKeyJson("panel", PanelAk32, PanelSk32, "read-only");
        var broken = "not json";
        var noPanel = CanonicalJson(RootPassword32, AccessKeyJson("backups", Ak32, Sk32, "read-write"));
        var emptyRootPassword = CanonicalJson("", fullKeys);
        var badPolicy = CanonicalJson(RootPassword32,
            AccessKeyJson("backups", Ak32, Sk32, "admin") + "," + AccessKeyJson("panel", PanelAk32, PanelSk32, "read-only"));

        // Act
        var parsedBroken = ClusterSecretsStore.TryParse(broken);
        var parsedNoPanel = ClusterSecretsStore.TryParse(noPanel);
        var parsedEmptyRoot = ClusterSecretsStore.TryParse(emptyRootPassword);
        var parsedBadPolicy = ClusterSecretsStore.TryParse(badPolicy);

        // Assert: ни один вариант не парсится.
        parsedBroken.Should().BeNull();
        parsedNoPanel.Should().BeNull();
        parsedEmptyRoot.Should().BeNull();
        parsedBadPolicy.Should().BeNull();
    }

    [Fact]
    public async Task Ensure_EmptyEtcd_GeneratesPersistsAndIdempotent()
    {
        // Arrange: свежий etcd.
        var (store, etcd) = Rig();

        // Act: два ensure подряд.
        var first = await store.EnsureAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);
        var second = await store.EnsureAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);

        // Assert: сгенерировано и записано в etcd; состав и форматы — по spec §3.1.
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        etcd.Store.Keys.Should().Contain(SecretsKey(Cluster));
        var secrets = first.Value;
        secrets.RootUser.Should().Be(RootUser);
        SecretsShape(secrets.RootPassword).Should().BeTrue();
        SecretsShape(AccessKeyOf(secrets, "backups").AccessKey).Should().BeTrue();
        SecretsShape(AccessKeyOf(secrets, "backups").SecretKey).Should().BeTrue();
        SecretsShape(AccessKeyOf(secrets, "panel").AccessKey).Should().BeTrue();
        SecretsShape(AccessKeyOf(secrets, "panel").SecretKey).Should().BeTrue();
        AccessKeyOf(secrets, "backups").Policy.Should().Be("read-write");
        AccessKeyOf(secrets, "panel").Policy.Should().Be("read-only");

        // Assert: идемпотентность — повтор возвращает РАВНЫЕ значения.
        second.IsSuccess.Should().BeTrue(second.Error?.Message);
        second.Value.RootUser.Should().Be(secrets.RootUser);
        second.Value.RootPassword.Should().Be(secrets.RootPassword);
        AccessKeyOf(second.Value, "backups").AccessKey.Should().Be(AccessKeyOf(secrets, "backups").AccessKey);
        AccessKeyOf(second.Value, "backups").SecretKey.Should().Be(AccessKeyOf(secrets, "backups").SecretKey);
        AccessKeyOf(second.Value, "panel").AccessKey.Should().Be(AccessKeyOf(secrets, "panel").AccessKey);
        AccessKeyOf(second.Value, "panel").SecretKey.Should().Be(AccessKeyOf(secrets, "panel").SecretKey);
    }

    [Fact]
    public async Task Ensure_PrewrittenValidKey_ReturnsExisting()
    {
        // Arrange: в etcd предзаписан валидный secrets-ключ.
        var (store, etcd) = Rig();
        var prewritten = new ClusterSecrets(
            RootUser,
            "pred-root-password",
            [
                new ClusterAccessKey("backups", "pred-backups-ak", "pred-backups-sk", "read-write"),
                new ClusterAccessKey("panel", "pred-panel-ak", "pred-panel-sk", "read-only"),
            ]);
        var prewrittenJson = ClusterSecretsStore.Serialize(prewritten);
        etcd.Seed(SecretsKey(Cluster), prewrittenJson);

        // Act
        var ensured = await store.EnsureAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);

        // Assert: возвращены предзаписанные значения; ключ в etcd не перегенерирован.
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        ensured.Value.RootUser.Should().Be(RootUser);
        ensured.Value.RootPassword.Should().Be("pred-root-password");
        AccessKeyOf(ensured.Value, "backups").AccessKey.Should().Be("pred-backups-ak");
        AccessKeyOf(ensured.Value, "backups").SecretKey.Should().Be("pred-backups-sk");
        AccessKeyOf(ensured.Value, "panel").AccessKey.Should().Be("pred-panel-ak");
        AccessKeyOf(ensured.Value, "panel").SecretKey.Should().Be("pred-panel-sk");
        etcd.Store[SecretsKey(Cluster)].Value.Should().Be(prewrittenJson);
        etcd.Store[SecretsKey(Cluster)].Version.Should().Be(1);
    }

    [Fact]
    public async Task Ensure_EtcdBackup_EnvPairGuardAndSubstitution()
    {
        // Arrange: getenv без переменных пары.
        var (store, _) = Rig(_ => null);

        // Act: etcd-backup кластер без спец-ключа.
        var missing = await store.EnsureAsync(Cluster, etcdBackup: true, TestContext.Current.CancellationToken);

        // Assert: Failed с диагностикой по имени переменной; значений в сообщении нет.
        missing.IsSuccess.Should().BeFalse();
        missing.Error!.Message.Should().Contain("S3W_ETCD_BACKUP");
        Regex.IsMatch(missing.Error!.Message, "[A-Za-z0-9]{32}").Should().BeFalse();

        // Arrange: полная env-пара.
        var (withEnv, etcdEnv) = Rig(Getenv(new Dictionary<string, string?>
        {
            [BackupAkEnv] = "env-backups-ak-value",
            [BackupSkEnv] = "env-backups-sk-value",
        }));

        // Act
        var ensured = await withEnv.EnsureAsync(Cluster, etcdBackup: true, TestContext.Current.CancellationToken);

        // Assert: ключ backups — значения из env; panel/root сгенерированы.
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        ensured.Value.RootUser.Should().Be(RootUser);
        AccessKeyOf(ensured.Value, "backups").AccessKey.Should().Be("env-backups-ak-value");
        AccessKeyOf(ensured.Value, "backups").SecretKey.Should().Be("env-backups-sk-value");
        AccessKeyOf(ensured.Value, "backups").Policy.Should().Be("read-write");
        AccessKeyOf(ensured.Value, "panel").AccessKey.Should().NotBe("env-backups-ak-value");
        SecretsShape(AccessKeyOf(ensured.Value, "panel").AccessKey).Should().BeTrue();
        SecretsShape(ensured.Value.RootPassword).Should().BeTrue();
        etcdEnv.Store.Keys.Should().Contain(SecretsKey(Cluster));
    }

    [Fact]
    public async Task TryRead_MissingBrokenValid()
    {
        // Arrange: пустой etcd.
        var (store, etcd) = Rig();

        // Act: ключа нет.
        var missing = await store.TryReadAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: Success(null) — не ошибка.
        missing.IsSuccess.Should().BeTrue();
        missing.Value.Should().BeNull();

        // Arrange: под ключом битый JSON.
        etcd.Seed(SecretsKey(Cluster), "not json");

        // Act: чтение битого.
        var broken = await store.TryReadAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: Failed.
        broken.IsSuccess.Should().BeFalse();

        // Arrange: под ключом валидный JSON.
        var model = new ClusterSecrets(
            RootUser,
            "read-root-password",
            [
                new ClusterAccessKey("backups", "read-backups-ak", "read-backups-sk", "read-write"),
                new ClusterAccessKey("panel", "read-panel-ak", "read-panel-sk", "read-only"),
            ]);
        etcd.Seed(SecretsKey(Cluster), ClusterSecretsStore.Serialize(model));

        // Act: чтение валидного.
        var valid = await store.TryReadAsync(Cluster, TestContext.Current.CancellationToken);

        // Assert: Success(модель) по значениям полей.
        valid.IsSuccess.Should().BeTrue(valid.Error?.Message);
        valid.Value!.RootUser.Should().Be(model.RootUser);
        valid.Value.RootPassword.Should().Be(model.RootPassword);
        AccessKeyOf(valid.Value, "backups").AccessKey.Should().Be("read-backups-ak");
        AccessKeyOf(valid.Value, "backups").SecretKey.Should().Be("read-backups-sk");
        AccessKeyOf(valid.Value, "backups").Policy.Should().Be("read-write");
        AccessKeyOf(valid.Value, "panel").AccessKey.Should().Be("read-panel-ak");
        AccessKeyOf(valid.Value, "panel").SecretKey.Should().Be("read-panel-sk");
        AccessKeyOf(valid.Value, "panel").Policy.Should().Be("read-only");
    }

    [Fact]
    public async Task Rotate_Regenerates_KeepsRootUser()
    {
        // Arrange: secrets выпущены.
        var (store, etcd) = Rig();
        var before = await store.EnsureAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);

        // Act
        var rotated = await store.RotateAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);

        // Assert: root_user прежний; пароль и значения ключей НОВЫЕ; в etcd новое значение.
        rotated.IsSuccess.Should().BeTrue(rotated.Error?.Message);
        rotated.Value.RootUser.Should().Be(before.Value.RootUser);
        rotated.Value.RootPassword.Should().NotBe(before.Value.RootPassword);
        AccessKeyOf(rotated.Value, "backups").AccessKey.Should().NotBe(AccessKeyOf(before.Value, "backups").AccessKey);
        AccessKeyOf(rotated.Value, "backups").SecretKey.Should().NotBe(AccessKeyOf(before.Value, "backups").SecretKey);
        AccessKeyOf(rotated.Value, "panel").AccessKey.Should().NotBe(AccessKeyOf(before.Value, "panel").AccessKey);
        AccessKeyOf(rotated.Value, "panel").SecretKey.Should().NotBe(AccessKeyOf(before.Value, "panel").SecretKey);
        SecretsShape(rotated.Value.RootPassword).Should().BeTrue();
        var inEtcd = ClusterSecretsStore.TryParse(etcd.Store[SecretsKey(Cluster)].Value);
        inEtcd!.RootPassword.Should().Be(rotated.Value.RootPassword);
    }

    [Fact]
    public async Task Rotate_KeyChangedUnderneath_Failed()
    {
        // Arrange: secrets выпущены; перед compare txn тест перезаписывает ключ
        // (гонка: ModRevisionEqual ломается).
        var (store, etcd) = Rig();
        await store.EnsureAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);
        etcd.OnTxnBeforeCompare = _ => etcd.Seed(SecretsKey(Cluster), "clobbered");

        // Act
        var rotated = await store.RotateAsync(Cluster, etcdBackup: false, TestContext.Current.CancellationToken);

        // Assert: RMW проигран — Failed с диагностикой.
        rotated.IsSuccess.Should().BeFalse();
        rotated.Error!.Message.Should().Contain("changed underneath us");
    }

    [Fact]
    public async Task Rotate_EtcdBackup_ResubstitutesEnvPair()
    {
        // Arrange: secrets etcd-backup кластера с полной env-парой.
        var (store, _) = Rig(Getenv(new Dictionary<string, string?>
        {
            [BackupAkEnv] = "env-backups-ak-value",
            [BackupSkEnv] = "env-backups-sk-value",
        }));
        var before = await store.EnsureAsync(Cluster, etcdBackup: true, TestContext.Current.CancellationToken);

        // Act
        var rotated = await store.RotateAsync(Cluster, etcdBackup: true, TestContext.Current.CancellationToken);

        // Assert: спец-ключ снова из env (пере-подстановка); остальное новое.
        rotated.IsSuccess.Should().BeTrue(rotated.Error?.Message);
        AccessKeyOf(rotated.Value, "backups").AccessKey.Should().Be("env-backups-ak-value");
        AccessKeyOf(rotated.Value, "backups").SecretKey.Should().Be("env-backups-sk-value");
        rotated.Value.RootPassword.Should().NotBe(before.Value.RootPassword);
        AccessKeyOf(rotated.Value, "panel").AccessKey.Should().NotBe(AccessKeyOf(before.Value, "panel").AccessKey);
        AccessKeyOf(rotated.Value, "panel").SecretKey.Should().NotBe(AccessKeyOf(before.Value, "panel").SecretKey);
    }

    [Fact]
    public void ValidateEtcdBackupEnvPairs_HalfPairDiagnosed()
    {
        // Arrange: три getenv-профиля — обе отсутствуют, обе заданы, задана одна.
        var bothAbsent = Getenv([]);
        var bothSet = Getenv(new Dictionary<string, string?> { [BackupAkEnv] = "ak", [BackupSkEnv] = "sk" });
        var halfPair = Getenv(new Dictionary<string, string?> { [BackupAkEnv] = "ak" });

        // Act
        var absent = ClusterSecretsStore.ValidateEtcdBackupEnvPairs(bothAbsent);
        var complete = ClusterSecretsStore.ValidateEtcdBackupEnvPairs(bothSet);
        var half = ClusterSecretsStore.ValidateEtcdBackupEnvPairs(halfPair);

        // Assert: диагностика только для полупары.
        absent.Should().BeEmpty();
        complete.Should().BeEmpty();
        half.Should().Equal("S3W_ETCD_BACKUP");
    }

    [Fact]
    public void IsEtcdBackupEnvPairComplete_HalfPairIncomplete()
    {
        // Arrange: три getenv-профиля — обе отсутствуют, задана одна, обе непустые.
        var bothAbsent = Getenv([]);
        var halfPair = Getenv(new Dictionary<string, string?> { [BackupSkEnv] = "sk" });
        var bothSet = Getenv(new Dictionary<string, string?> { [BackupAkEnv] = "ak", [BackupSkEnv] = "sk" });

        // Act
        var absent = ClusterSecretsStore.IsEtcdBackupEnvPairComplete(bothAbsent);
        var half = ClusterSecretsStore.IsEtcdBackupEnvPairComplete(halfPair);
        var complete = ClusterSecretsStore.IsEtcdBackupEnvPairComplete(bothSet);

        // Assert
        absent.Should().BeFalse();
        half.Should().BeFalse();
        complete.Should().BeTrue();
    }
}
