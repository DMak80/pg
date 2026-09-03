using PgWorker.App.Api.Operations;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Api;

// ClusterGuardData.Status.UpdatedUnix (t07, спека §5.3): возраст статус-ключа
// для пред-проверки свежести abort; отсутствие поля → null (проверка пропускается).
public class ClusterGuardDataTests
{
    private const string Ep = "http://etcd";

    [Fact]
    public async Task ReadAsync_StatusWithUpdatedUnix_Parsed()
    {
        // Arrange
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, "/clusters/c/config", """{"buckets":2,"dbname":"c"}""", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/routing/bucket_0", "s1", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"SYNCING","owner":"s1","target":"s2","updated_unix":1756000100}""", null, CancellationToken.None);

        // Act
        var data = await ClusterGuardData.ReadAsync(gw, [Ep], "c", CancellationToken.None);

        // Assert
        data.IsSuccess.Should().BeTrue();
        var status = data.Value.Status[0];
        status.State.Should().Be("SYNCING");
        status.Owner.Should().Be("s1");
        status.Target.Should().Be("s2");
        status.UpdatedUnix.Should().Be(1756000100);
    }

    [Fact]
    public async Task ReadAsync_StatusWithoutUpdatedUnix_Null()
    {
        // Arrange — старый формат ключа без updated_unix (толерантность §5.3)
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, "/clusters/c/config", """{"buckets":2,"dbname":"c"}""", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_1",
            """{"state":"FROZEN","owner":"s1","target":"s2"}""", null, CancellationToken.None);

        // Act
        var data = await ClusterGuardData.ReadAsync(gw, [Ep], "c", CancellationToken.None);

        // Assert
        data.Value.Status[1].UpdatedUnix.Should().BeNull();
    }
}
