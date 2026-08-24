using PgWorker.Etcd.Client;
using PgWorker.Moves;
using PgWorker.UnitTests.Provisioning;
using FakeEtcd = PgWorker.UnitTests.Provisioning.Fakes.FakeEtcd;

namespace PgWorker.UnitTests.Moves;

public class MoveRequestsStoreTests
{
    private static MoveRequestsStore StoreOf(FakeEtcd etcd) => new(etcd, ["http://x"]);

    // AAA: range по префиксу кластера возвращает только его заявки
    [Fact]
    public async Task ListAsync_ReturnsOnlyClusterRequests()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":20}""");
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_2"), """{"op":"abort","force":true,"requested_unix":10}""");
        etcd.Seed(MoveNames.MoveKey("other", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":5}""");

        // Act
        var list = await StoreOf(etcd).ListAsync("shop", CancellationToken.None);

        // Assert
        list.Value.Requests.Should().HaveCount(2, "чужой кластер не попадает в выборку");
        list.Value.ParseErrors.Should().BeEmpty("все ключи валидны");
    }

    // AAA: старейшая заявка — по requested_unix (Д2: одна активная заявка на кластер)
    [Fact]
    public async Task OldestAsync_PicksMinRequestedUnix()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"move","to":"shard2","requested_unix":20}""");
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_2"), """{"op":"abort","requested_unix":10}""");

        // Act
        var oldest = await StoreOf(etcd).OldestAsync("shop", CancellationToken.None);

        // Assert
        oldest.Value.Request!.Value.Bucket.Should().Be("bucket_2");
    }

    // AAA: удаление заявки по завершении (успех/перманентный отказ, spec §4.1)
    [Fact]
    public async Task DeleteAsync_RemovesKey()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"abort","requested_unix":1}""");

        // Act
        var deleted = await StoreOf(etcd).DeleteAsync("shop", "bucket_1", CancellationToken.None);

        // Assert
        deleted.IsSuccess.Should().BeTrue();
        etcd.Store.ContainsKey(MoveNames.MoveKey("shop", "bucket_1")).Should().BeFalse();
    }

    // AAA: битая заявка не роняет список — исключается из выборки, причина
    //      возвращается рядом (её залогирует процесс, ревью №2)
    [Fact]
    public async Task ListAsync_SkipsBrokenJson()
    {
        // Arrange
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_9"), "not-json");

        // Act
        var list = await StoreOf(etcd).ListAsync("shop", CancellationToken.None);

        // Assert
        list.IsSuccess.Should().BeTrue("битая заявка — не ошибка тика, её увидит оператор в логе");
        list.Value.Requests.Should().BeEmpty();
        list.Value.ParseErrors.Should().ContainSingle().Which.Should().Contain("/pgworker/moves/shop/bucket_9",
            "ошибка называет ключ по имени");
    }

    // AAA: старейшая выбирается и при битых соседях — ошибки едут рядом (ревью №2)
    [Fact]
    public async Task OldestAsync_WithBrokenKeys_StillPicksOldestAndReportsErrors()
    {
        // Arrange — валидная заявка + битый ключ того же кластера
        var etcd = new FakeEtcd();
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_1"), """{"op":"abort","requested_unix":10}""");
        etcd.Seed(MoveNames.MoveKey("shop", "bucket_9"), "not-json");

        // Act
        var oldest = await StoreOf(etcd).OldestAsync("shop", CancellationToken.None);

        // Assert
        oldest.Value.Request!.Value.Bucket.Should().Be("bucket_1",
            "битая заявка не мешает выбору старейшей валидной");
        oldest.Value.ParseErrors.Should().ContainSingle().Which.Should().Contain("bucket_9",
            "ошибка битого ключа не теряется");
    }
}
