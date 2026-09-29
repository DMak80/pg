using System.Text.Json;
using PgWorker.App.Api.Operations;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.Api;

// t06 (spec §3.3/§6.5): мутация config — чистая функция RewriteStrict (поле
// заменено/добавлено, прочие перенесены) + детерминированный юнит гонки RMW
// (проигрыш compare mod_revision → ClusterConcurrentWriteException → 503).
public class UpdateClusterConfigHandlerTests
{
    // AAA (t06): пересборка config — поле заменено/добавлено, прочие сохранены.
    [Theory]
    [InlineData("""{"buckets":10,"dbname":"demo","created_unix":123,"synchronous_mode_strict":true}""", false)]
    [InlineData("""{"buckets":10,"dbname":"demo","created_unix":123}""", true)]
    public void RewriteStrict_ReplacesOrAddsField_OthersUntouched(string raw, bool strict)
    {
        // Arrange / Act
        var result = UpdateClusterConfigHandler.RewriteStrict(raw, strict);

        // Assert
        using var doc = JsonDocument.Parse(result);
        doc.RootElement.GetProperty("buckets").GetInt32().Should().Be(10);
        doc.RootElement.GetProperty("dbname").GetString().Should().Be("demo");
        doc.RootElement.GetProperty("created_unix").GetInt64().Should().Be(123);
        doc.RootElement.GetProperty("synchronous_mode_strict").GetBoolean().Should().Be(strict);
    }

    // AAA (t06, spec §6.5 — форма: юнит, см. примечание в Задаче 15 шаг 2):
    // конкурентная запись между read и txn — compare проигран, хендлер возвращает
    // ClusterConcurrentWriteException (маппинг ApiModule — 503, retry клиентом).
    [Fact]
    public async Task HandleAsync_ConcurrentConfigWrite_LosesCompare()
    {
        // Arrange — FakeEtcd: config strict=true, шард без ограничений; хук пишет
        // конкурирующий config ДО compare хендлерского txn (mod_revision растёт).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/config", """{"buckets":2,"dbname":"shop","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/shards/shard1/replicas", "2");
        etcd.OnTxnBeforeCompare = _ =>
            etcd.PutAsync("http://fake", "/clusters/shop/config",
                """{"buckets":2,"dbname":"shop","created_unix":1755900000,"synchronous_mode_strict":true}""",
                null, CancellationToken.None).GetAwaiter().GetResult();
        var handler = new UpdateClusterConfigHandler(etcd, ["http://fake"]);

        // Act
        var result = await handler.HandleAsync("shop",
            new UpdateClusterConfigRequest(SynchronousModeStrict: false), CancellationToken.None);

        // Assert — txn проигран: ошибка гонки, значение НЕ перезаписано хендлером.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ClusterConcurrentWriteException>();
        etcd.Store["/clusters/shop/config"].Value
            .Should().Contain("\"synchronous_mode_strict\":true", "конкурентная запись победила");
    }
}
