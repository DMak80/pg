using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Алерты экспорта etcd-снапшотов (t08, adminpanel/03 §4): FAILED+enabled →
// critical с текстом error; тишина > 2×interval_min → warning; enabled=false/
// ключа нет — молчат; живой каденс выгрузки — под порогом (семантика покрытия).
public class EtcdSnapshotExportRuleTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly EtcdSnapshotExportFailedRule _failedRule = new();
    private readonly EtcdSnapshotExportStaleRule _staleRule = new();

    private static IReadOnlyList<Alert> Evaluate(IAlertRule rule, EtcdSnapshot snapshot)
        => [.. rule.Evaluate(snapshot, new AlertContext(null, Now, 3))];

    private static EtcdSnapshot Snapshot(EtcdSnapshotExportInfo? export) =>
        TestSnapshots.Healthy(Now) with { EtcdSnapshots = export };

    // AAA 1: FAILED + enabled → critical etcd-snapshot-export-failed, Description содержит error
    [Fact]
    public void Failed_включено_critical_алерт_с_ошибкой()
    {
        // Arrange — снапшот с FAILED-статусом выгрузки
        var snapshot = Snapshot(new EtcdSnapshotExportInfo(true, "FAILED", 1759330000, null, "abc", 1, 360, "S3 недоступен"));

        // Act
        var alerts = Evaluate(_failedRule, snapshot);

        // Assert
        alerts.Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-failed"
            && a.Severity == AlertSeverity.Critical
            && a.Message.Contains("S3 недоступен"));
    }

    // AAA 2: FAILED + enabled=false → молчат оба (выключено)
    [Fact]
    public void Failed_выключено_молчит()
    {
        // Arrange — enabled=false (ключ воркера при выключенной опции)
        var snapshot = Snapshot(new EtcdSnapshotExportInfo(false, "FAILED", null, null, null, null, 360, "err"));

        // Act/Assert
        Evaluate(_failedRule, snapshot).Should().BeEmpty();
        Evaluate(_staleRule, snapshot).Should().BeEmpty();
    }

    // AAA 3: EtcdSnapshots=null (ключа нет/битый) → оба правила молчат
    [Fact]
    public void Нет_ключа_молчат_оба()
    {
        // Arrange
        var snapshot = Snapshot(null);

        // Act/Assert
        Evaluate(_failedRule, snapshot).Should().BeEmpty();
        Evaluate(_staleRule, snapshot).Should().BeEmpty();
    }

    // AAA 4: OK + enabled, last_uploaded старше 2×interval_min (interval_min=360) → warning stale
    [Fact]
    public void Устаревшая_выгрузка_warning_stale()
    {
        // Arrange — выгрузка 2 суток назад, порог 2×360 мин = 12 ч
        // 1790683200 = Now − 48 ч (порог 2×360 мин = 12 ч)
        var snapshot = Snapshot(new EtcdSnapshotExportInfo(true, "OK", 1790683200, "etcd/snapshot-x.db", "abc", 1, 360, null));

        // Act/Assert
        var alerts = Evaluate(_staleRule, snapshot);
        alerts.Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-stale"
            && a.Severity == AlertSeverity.Warning);
        Evaluate(_failedRule, snapshot).Should().BeEmpty();
    }

    // AAA 5: OK + enabled, свежая выгрузка → молчат; живой каденс (AC2): каждый
    // слепок (плановый и внеочередной) продвигает last_uploaded_unix —
    // исправный контур под порогом, stale молчит (семантика «покрытия» §3.1/§3.7)
    [Fact]
    public void Свежая_выгрузка_молчит()
    {
        // Arrange — выгрузка 1 ч назад (порог 12 ч)
        // 1790852400 = Now − 1 ч
        var snapshot = Snapshot(new EtcdSnapshotExportInfo(true, "OK", 1790852400, "etcd/snapshot-x.db", "abc", 1, 360, null));

        // Act/Assert
        Evaluate(_staleRule, snapshot).Should().BeEmpty();
        Evaluate(_failedRule, snapshot).Should().BeEmpty();
    }

    // AAA 6: OK + enabled, last_uploaded отсутствует → warning stale («выгрузка молчит»)
    [Fact]
    public void Без_выгрузок_warning_stale()
    {
        // Arrange — включено, но ни одной успешной выгрузки
        var snapshot = Snapshot(new EtcdSnapshotExportInfo(true, "OK", null, null, null, null, 360, null));

        // Act/Assert
        var alerts = Evaluate(_staleRule, snapshot);
        alerts.Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-stale"
            && a.Severity == AlertSeverity.Warning);
    }

    // AAA 7: interval_min отсутствует → панельный дефолт 360 (порог 720 мин)
    [Fact]
    public void Без_интервала_дефолт_360()
    {
        // Arrange — выгрузка 11 ч назад: под порогом дефолта 2×360 мин
        // 1790816400 = Now − 11 ч
        var fresh = Snapshot(new EtcdSnapshotExportInfo(true, "OK", 1790816400, null, null, null, null, null));
        // и 13 ч назад — сверх порога
        // 1790809200 = Now − 13 ч
        var stale = Snapshot(new EtcdSnapshotExportInfo(true, "OK", 1790809200, null, null, null, null, null));

        // Act/Assert — 11 ч < 12 ч → молчит; 13 ч > 12 ч → stale
        Evaluate(_staleRule, fresh).Should().BeEmpty("порог по дефолту 360 мин = 12 ч");
        Evaluate(_staleRule, stale).Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-stale");
    }
}
