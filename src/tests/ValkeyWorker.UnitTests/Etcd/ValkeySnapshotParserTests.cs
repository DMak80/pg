using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Etcd.Parsing;
using Xunit;

namespace ValkeyWorker.UnitTests.Etcd;

// Парсер /valkey/clusters/ (arch/20 §2.1 канон + §5 толерантность): чистая
// функция Kv[] → модель; битые значения — parseError, не исключение.
public class ValkeySnapshotParserTests
{
    private static Kv Kv(string key, string value) => new(key, value, 1);

    private static ValkeyClusterSnapshot ParseOne(params Kv[] kvs)
    {
        var parsed = ValkeySnapshotParser.Parse(kvs);
        parsed.IsSuccess.Should().BeTrue();
        return parsed.Value.Clusters.Should().ContainSingle().Subject;
    }

    // ── Приёмочные: канонические примеры arch/20 §2.1 (дословно) ──

    [Fact]
    public void Config_NotInitialized_ЗаявкаПарситсяДословно()
    {
        // Arrange: заявка создания (arch/20 §2.1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}"""));

        // Act/Assert: все поля config читаются.
        var config = snap.Config!;
        config.Nodes.Should().Be(1);
        config.MaxmemoryBytes.Should().Be(536870912);
        config.MaxmemoryPolicy.Should().Be("allkeys-lru");
        config.CreatedUnix.Should().Be(1756500000);
        config.State.Should().Be("NOT_INITIALIZED");
    }

    [Fact]
    public void Config_ActiveБезState_StateNull()
    {
        // Arrange: Active-конфиг (state снят воркером, arch/20 §2.1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}"""));

        // Act/Assert: отсутствие state = Active.
        snap.Config!.State.Should().BeNull();
        snap.ParseErrors.Should().BeEmpty();
    }

    [Fact]
    public void Config_ToRemove_Читается()
    {
        // Arrange: заявка удаления (arch/20 §2.1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}"""));

        // Act/Assert
        snap.Config!.State.Should().Be("TO_REMOVE");
    }

    [Fact]
    public void Endpoints_СтрокаЧитаетсяКакДанныеКлюча()
    {
        // Arrange: дискавери-ключ (arch/20 §2.1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/endpoints", "host.docker.internal:17001"));

        // Act/Assert: значение-строка целиком (парсер ничего не переиначивает).
        snap.Endpoints.Should().Be("host.docker.internal:17001");
    }

    [Fact]
    public void Креды_Читаются()
    {
        // Arrange: ensure-нутые креды (arch/20 §2.1: "app"/"admin", пароли 32 симв).
        var snap = ParseOne(
            Kv("/valkey/clusters/demo/app_user", "app"),
            Kv("/valkey/clusters/demo/app_password", "AppPassword0123456789abcdef12345678"),
            Kv("/valkey/clusters/demo/admin_user", "admin"),
            Kv("/valkey/clusters/demo/admin_password", "AdminPassword0123456789abcdef12345"));

        // Act/Assert
        snap.AppUser.Should().Be("app");
        snap.AppPassword.Should().Be("AppPassword0123456789abcdef12345678");
        snap.AdminUser.Should().Be("admin");
        snap.AdminPassword.Should().Be("AdminPassword0123456789abcdef12345");
    }

    [Fact]
    public void CaPem_CaKey_Читаются()
    {
        // Arrange: ключи CA в префиксе кластера (валидный PEM от ValkeyPki, t06).
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("demo");
        var snap = ParseOne(
            Kv("/valkey/clusters/demo/config",
                """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}"""),
            Kv("/valkey/clusters/demo/ca_pem", caPem),
            Kv("/valkey/clusters/demo/ca_key", caKeyPem));

        // Act/Assert: PEM читается как есть.
        snap.CaPem.Should().Be(caPem);
        snap.CaKey.Should().Be(caKeyPem);
    }

    [Fact]
    public void БитыйCaPem_ParseErrorКластерЖив()
    {
        // Arrange: битый PEM в обоих CA-ключах (arch/20 §5) — не роняет парсер.
        var snap = ParseOne(
            Kv("/valkey/clusters/demo/config",
                """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}"""),
            Kv("/valkey/clusters/demo/ca_pem", "garbage"),
            Kv("/valkey/clusters/demo/ca_key", "-----BEGIN PRIVATE KEY-----\nZ2FyYmFnZQ==\n-----END PRIVATE KEY-----\n"));

        // Act/Assert: по ошибке на каждый битый ключ, поля null — кластер жив.
        snap.ParseErrors.Should().HaveCount(2);
        snap.ParseErrors.Should().Contain(e => e.Contains("/valkey/clusters/demo/ca_pem"));
        snap.ParseErrors.Should().Contain(e => e.Contains("/valkey/clusters/demo/ca_key"));
        snap.CaPem.Should().BeNull();
        snap.CaKey.Should().BeNull();
    }

    [Fact]
    public void NodeState_NotInitialized_Читается()
    {
        // Arrange: заявка ноды (arch/20 §2.1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/nodes/node1/state", "NOT_INITIALIZED"));

        // Act/Assert
        snap.Nodes.Should().ContainKey("node1");
        snap.Nodes["node1"].State.Should().Be("NOT_INITIALIZED");
    }

    [Fact]
    public void NodeResources_КанонPg93_ЧитаетсяRaw()
    {
        // Arrange: заявка ресурсов (arch/20 §2.1, форматы pg §9.3).
        var snap = ParseOne(Kv("/valkey/clusters/demo/nodes/node1/resources",
            """{"cpu":"1","mem":"1Gi","disk":"10Gi"}"""));

        // Act/Assert: raw-строки хранятся как есть (парсинг в лимиты — ProcessCommon).
        var resources = snap.Nodes["node1"].Resources!;
        resources.Cpu.Should().Be("1");
        resources.Mem.Should().Be("1Gi");
        resources.Disk.Should().Be("10Gi");
    }

    // ── Толерантность: все 6 строк таблицы arch/20 §5 ──

    [Fact]
    public void БитыйJsonConfig_ParseErrorБезИсключения()
    {
        // Arrange: ключ config с битым JSON.
        var snap = ParseOne(Kv("/valkey/clusters/demo/config", "{not json"));

        // Act/Assert: parseError-запись, Config=null (классификатор — Skip).
        snap.Config.Should().BeNull();
        snap.ParseErrors.Should().Contain(e => e.Contains("/valkey/clusters/demo/config"));
    }

    [Fact]
    public void НеизвестныйКлюч_СчётчикUnknownKeys()
    {
        // Arrange: ключ вне канона внутри префикса кластера.
        var snap = ParseOne(Kv("/valkey/clusters/demo/something_new", "x"));

        // Act/Assert: парсер не падает, ключ учтён.
        snap.UnknownKeys.Should().Contain(k => k.Contains("something_new"));
    }

    [Fact]
    public void НезнакомоеState_ActiveВеткаСRawСтрокой()
    {
        // Arrange: state вне канона (система развивается, arch/20 §5).
        var snap = ParseOne(Kv("/valkey/clusters/demo/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"DRAFT"}"""));

        // Act/Assert: raw-строка проходит без ошибки (ветку выберет классификатор).
        snap.Config!.State.Should().Be("DRAFT");
        snap.ParseErrors.Should().BeEmpty();
    }

    [Fact]
    public void ПустойEndpoints_ТрактуетсяКакОтсутствующий()
    {
        // Arrange/Act: пустой и пробельный варианты.
        var empty = ParseOne(Kv("/valkey/clusters/demo/endpoints", ""));
        var blank = ParseOne(Kv("/valkey/clusters/demo/endpoints", "   "));

        // Assert
        empty.Endpoints.Should().BeNull();
        blank.Endpoints.Should().BeNull();
    }

    [Fact]
    public void НеполныеКреды_PasswordNullНезависимоОтЮзера()
    {
        // Arrange: app_user без пароля.
        var snap = ParseOne(Kv("/valkey/clusters/demo/app_user", "app"));

        // Act/Assert: AppPassword null (потребитель проверяет набор).
        snap.AppUser.Should().Be("app");
        snap.AppPassword.Should().BeNull();
    }

    [Fact]
    public void Config_Nodes2_ПарсерПропускаетКакЕсть()
    {
        // Arrange: nodes=2 (валидация — API; парсер толерантен, arch/20 §5).
        var snap = ParseOne(Kv("/valkey/clusters/demo/config",
            """{"nodes":2,"maxmemory_bytes":1,"maxmemory_policy":"noeviction","created_unix":1}"""));

        // Act/Assert: значение проходит без parseError.
        snap.Config!.Nodes.Should().Be(2);
        snap.ParseErrors.Should().BeEmpty();
    }

    [Fact]
    public void ЧужойПрефикс_Игнорируется()
    {
        // Arrange: ключи других доменов в подаче.
        var parsed = ValkeySnapshotParser.Parse([
            Kv("/kafka/clusters/other/config", "{}"),
            Kv("/valkeyworker/leader", "x"),
        ]);

        // Act/Assert: ни одного кластера, без ошибок.
        parsed.Value.Clusters.Should().BeEmpty();
    }

    [Fact]
    public void БитыйJsonResources_ParseErrorРесурсыNull()
    {
        // Arrange: resources с битым JSON (arch/20 §5 строка 1).
        var snap = ParseOne(Kv("/valkey/clusters/demo/nodes/node1/resources", "nonsense"));

        // Act/Assert: parseError, Resources=null (надзор не получит лимит).
        snap.Nodes["node1"].Resources.Should().BeNull();
        snap.ParseErrors.Should().Contain(e => e.Contains("node1/resources"));
    }

    [Fact]
    public void СписокКластеров_СортированПоИмени()
    {
        // Arrange: два кластера в неупорядоченной подаче.
        var parsed = ValkeySnapshotParser.Parse([
            Kv("/valkey/clusters/zeta/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"noeviction","created_unix":1}"""),
            Kv("/valkey/clusters/alpha/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"noeviction","created_unix":1}"""),
        ]);

        // Act/Assert: стабильный порядок (детерминизм тика).
        parsed.Value.Clusters.Select(c => c.Cluster).Should().Equal("alpha", "zeta");
    }
}
