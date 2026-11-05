using FluentAssertions;
using PgWorker.App.Api.Operations;
using Xunit;

namespace PgWorker.UnitTests.Api;

// policy-поле garbage_min_age_sec (u16, arch/02 §9.12 / arch/backups/05-etcd-contract.md): тело с
// garbage_min_age_sec=600 → policy-put содержит поле; < 600 → валидация
// (BackupsPolicyValidationException, поле garbage_min_age_sec); без поля →
// поле в payload отсутствует (замещение целиком снимает пер-кластерный гейт —
// кластер живёт на дефолте конфига супервизора 600 с). AAA.
public class BackupsPolicyHandlerGarbageTests
{
    private const string Ep = "http://etcd";

    private static BackupsPolicyHandler Handler(FakeEtcdGateway gw) => new(gw, [Ep]);

    private static async Task<FakeEtcdGateway> SeedClusterAsync(string name)
    {
        var gw = new FakeEtcdGateway();
        // Arrange — config-ключ кластера (гвард 404)
        await gw.PutAsync(Ep, $"/clusters/{name}/config",
            $$"""{"buckets":4,"dbname":"{{name}}","created_unix":1756000000}""", null, CancellationToken.None);
        return gw;
    }

    [Fact]
    public async Task Garbage600_PutContainsField()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        var body = """
            {"retention":{"days":7,"weeks":4,"months":6},
             "full_max_age_sec":86400,"verify":{"on_create":true},
             "drill":{"interval_days":1},"garbage_min_age_sec":600}
            """;

        // Act
        var result = await Handler(gw).HandleAsync("c", body, TestContext.Current.CancellationToken);

        // Assert — ключ policy записан и содержит garbage_min_age_sec
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        gw.Store["/pgworker/backups/c/policy"].Should().Contain("\"garbage_min_age_sec\":600");
    }

    [Fact]
    public async Task GarbageBelow600_ValidationByField()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        var body = """
            {"retention":{"days":7,"weeks":4,"months":6},
             "full_max_age_sec":86400,"verify":{"on_create":true},
             "garbage_min_age_sec":599}
            """;

        // Act
        var result = await Handler(gw).HandleAsync("c", body, TestContext.Current.CancellationToken);

        // Assert — 400 по полю garbage_min_age_sec; policy не записан
        result.Error.Should().BeOfType<BackupsPolicyValidationException>()
            .Which.Errors.Should().Contain(e => e.Field == "garbage_min_age_sec");
        gw.Store.Should().NotContainKey("/pgworker/backups/c/policy");
    }

    [Fact]
    public async Task NoGarbageField_PayloadOmitsIt()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        var body = """
            {"retention":{"days":1,"weeks":1,"months":1},
             "full_max_age_sec":86400,"verify":{"on_create":true}}
            """;

        // Act
        var result = await Handler(gw).HandleAsync("c", body, TestContext.Current.CancellationToken);

        // Assert — поле в payload отсутствует (дефолт конфига 600 с)
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        gw.Store["/pgworker/backups/c/policy"].Should().NotContain("garbage_min_age_sec");
    }
}
