using System.Text.Json;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.UnitTests.PgNode;

// Разбор backup_manifest (spec t01 §3.5): Files[] с Path/Size/Checksum;
// битый JSON и чужой алгоритм checksum — исключение (сверка была бы ложной).
public class BackupManifestTests
{
    // AAA: эталонный фрагмент манифеста PG (2 файла) → словарь Path→файл.
    [Fact]
    public void ParseFiles_TwoFiles_ReturnsMap()
    {
        // Arrange — формат PG18: Checksum-Algorithm в каждой записи.
        const string json = """
            {
              "PostgreSQL-Backup-Manifest-Version": 2,
              "Files": [
                {"Path":"backup_label","Size":214,"Checksum-Algorithm":"SHA256","Checksum":"DFC32A90"},
                {"Path":"global/pg_control","Size":8192,"Checksum-Algorithm":"SHA256","Checksum":"00ff"}
              ]
            }
            """;

        // Act
        var files = BackupManifest.ParseFiles(json);

        // Assert
        files.Should().HaveCount(2);
        files["backup_label"].Size.Should().Be(214);
        files["backup_label"].Sha256Hex.Should().Be("dfc32a90");
        files["global/pg_control"].Size.Should().Be(8192);
    }

    // AAA: symlink-запись манифеста PG (LinkTarget, без Checksum) не роняет
    // разбор — по содержимому не сверяется и в карту не входит (spec §6.15).
    [Fact]
    public void ParseFiles_SymlinkEntryWithoutChecksum_Skipped()
    {
        // Arrange — обычный файл + symlink-запись формата PG.
        const string json = """
            {
              "PostgreSQL-Backup-Manifest-Version": 2,
              "Files": [
                {"Path":"backup_label","Size":214,"Checksum-Algorithm":"SHA256","Checksum":"DFC32A90"},
                {"Path":"pg_wal/archive_status","Size":0,"Last-Modified":"2026-11-05 10:00:00 GMT","LinkTarget":"../archive_status"}
              ]
            }
            """;

        // Act
        var files = BackupManifest.ParseFiles(json);

        // Assert — symlink пропущен, сверяемый файл на месте.
        files.Should().ContainKey("backup_label");
        files.Should().HaveCount(1, "symlink-записи не сверяются по содержимому");
        files.Should().NotContainKey("pg_wal/archive_status");
    }

    // AAA: битый JSON — JsonException (verify трактует как порчу набора).
    [Fact]
    public void ParseFiles_BrokenJson_Throws()
    {
        var act = () => BackupManifest.ParseFiles("{ not json");
        act.Should().Throw<JsonException>();
    }

    // AAA: алгоритм, отличный от SHA256 (CRC32), — JsonException.
    [Fact]
    public void ParseFiles_UnsupportedAlgorithm_Throws()
    {
        // Arrange
        const string json = """
            {"Files":[{"Path":"x","Size":1,"Checksum-Algorithm":"CRC32","Checksum":"deadbeef"}]}
            """;

        // Act
        var act = () => BackupManifest.ParseFiles(json);

        // Assert
        var thrown = act.Should().Throw<JsonException>();
        thrown.Which.Message.Should().Contain("CRC32");
    }

    // AAA: нет массива Files — JsonException.
    [Fact]
    public void ParseFiles_NoFilesArray_Throws()
    {
        var act = () => BackupManifest.ParseFiles("""{"PostgreSQL-Backup-Manifest-Version":2}""");
        act.Should().Throw<JsonException>();
    }
}
