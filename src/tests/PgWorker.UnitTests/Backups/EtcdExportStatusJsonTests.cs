using FluentAssertions;
using PgWorker.Backups.EtcdExport;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Статус-ключ /pgworker/etcd-snapshots (t08, arch/14 §3.3): сериализация OK/FAILED
// (FAILED сохраняет поля последнего успеха), толерантный парсинг, IsBehind-детект.
public class EtcdExportStatusJsonTests
{
    // AAA: OK-ключ — полный формат канона §3.1; last_uploaded_unix — метка
    // ПОКРЫТОГО слепка, не время put-запроса (§3.1)
    [Fact]
    public void Ok_полный_формат()
    {
        // Act
        var json = EtcdSnapshotStatusJson.Ok(
            coveredTakenUnix: 1759330000, lastObject: "etcd/snapshot-20261001-120000.db",
            lastSha256: "abc123", sizeBytes: 2048, intervalMin: 360);

        // Assert — snake_case-поля, error отсутствует
        json.Should().Contain("\"state\":\"OK\"")
            .And.Contain("\"enabled\":true")
            .And.Contain("\"last_uploaded_unix\":1759330000")
            .And.Contain("\"last_object\":\"etcd/snapshot-20261001-120000.db\"")
            .And.Contain("\"last_sha256\":\"abc123\"")
            .And.Contain("\"size_bytes\":2048")
            .And.Contain("\"interval_min\":360")
            .And.NotContain("\"error\"");
    }

    // AAA: FAILED сохраняет поля последнего успеха (оператор видит, насколько отстал)
    [Fact]
    public void Failed_сохраняет_поля_последнего_успеха()
    {
        // Arrange
        var lastOk = EtcdSnapshotStatusJson.Parse(EtcdSnapshotStatusJson.Ok(
            1759330000, "etcd/snapshot-20261001-120000.db", "abc123", 2048, 360))!;

        // Act
        var json = EtcdSnapshotStatusJson.Failed(lastOk, "S3 put: connection refused", 360);

        // Assert
        json.Should().Contain("\"state\":\"FAILED\"")
            .And.Contain("\"error\":\"S3 put: connection refused\"")
            .And.Contain("\"last_uploaded_unix\":1759330000")
            .And.Contain("\"last_sha256\":\"abc123\"");
    }

    // AAA: Failed без прошлого успеха — поля факта отсутствуют, enabled/interval/error есть
    [Fact]
    public void Failed_без_прошлого_успеха_минимальный_формат()
    {
        // Act
        var json = EtcdSnapshotStatusJson.Failed(null, "boom", 360);

        // Assert
        json.Should().Contain("\"state\":\"FAILED\"")
            .And.Contain("\"error\":\"boom\"")
            .And.NotContain("\"last_uploaded_unix\"")
            .And.NotContain("\"last_sha256\"");
    }

    // AAA: парсинг толерантен к отсутствующим полям; битый JSON → null
    [Fact]
    public void Parse_толерантный_и_битый()
    {
        // Act/Assert — валидный минимальный ключ
        var parsed = EtcdSnapshotStatusJson.Parse("""{"enabled":true,"state":"OK","interval_min":60}""");
        var status = parsed!; // Parse возвращает nullable-ссылку (не Nullable<T>): .Value нет
        status.Enabled.Should().BeTrue();
        status.State.Should().Be("OK");
        status.LastUploadedUnix.Should().BeNull();
        // Act/Assert — битый JSON → null (панель/парсер молчат)
        EtcdSnapshotStatusJson.Parse("{не json").Should().BeNull();
    }

    // AAA: IsBehind — FAILED/ключа нет/локальный новее подтверждённого покрытия
    // (uploaded = метка ПОКРЫТОГО слепка — инвариант §3.5 п.1: каждый успешный
    // проход sink'а продвигает поле, отставание закрывает)
    [Theory]
    [InlineData("FAILED", 1759330100, 1759330000, true)]   // FAILED — всегда отстаёт
    [InlineData("OK", 1759330100, 1759330000, true)]       // локальный слепок снят позже покрытия
    [InlineData("OK", 1759330000, 1759330100, false)]      // покрытие свежее локального — здорово
    public void IsBehind_детект(string state, long localTaken, long uploaded, bool expected)
    {
        // Arrange
        EtcdSnapshotStatus? status = new(true, state, uploaded, null, "abc", 1, 360, null);

        // Act
        var behind = EtcdSnapshotStatus.IsBehind(status, localTaken);

        // Assert
        behind.Should().Be(expected);
    }

    // AAA: ключа нет вовсе (включённая опция) — отстаёт; статус выключен — нет
    [Fact]
    public void IsBehind_нет_ключа_или_выключен()
    {
        // Act/Assert
        EtcdSnapshotStatus.IsBehind(null, 1759330100).Should().BeTrue("ключа нет при включённой опции — выгрузка отстаёт");
        EtcdSnapshotStatus.IsBehind(new EtcdSnapshotStatus(false, "FAILED", null, null, null, null, 360, null), 1759330100)
            .Should().BeFalse("enabled=false — доводка не нужна");
    }

    // AAA: таймстемп имени слепка (yyyyMMdd-HHmmss, UTC — формат SnapshotJob)
    [Fact]
    public void TakenUnixFromName_из_имени_файла()
    {
        // Act/Assert
        EtcdSnapshotStatus.TakenUnixFromName("snapshot-20261001-120000.db")
            .Should().Be(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());
        EtcdSnapshotStatus.TakenUnixFromName("иное.db").Should().BeNull();
    }
}
