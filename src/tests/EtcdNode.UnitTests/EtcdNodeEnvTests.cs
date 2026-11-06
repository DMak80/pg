using EtcdNode.App;
using FluentAssertions;
using Xunit;

namespace EtcdNode.UnitTests;

// Ручное чтение env конфигурации агента (arch/etcdnode/01-agent.md §3):
// отсутствующая/битая переменная — дефолт Int-fallback; невалидный
// собранный набор — fail-fast в Load. Env — глобальное состояние процесса:
// выставляем и снимаем в finally (единственный читатель ETCDNODE_* в сборке).
public class EtcdNodeEnvTests
{
    private static readonly string[] Keys =
    [
        "ETCDNODE_NODE_NAME",
        "ETCDNODE_SNAPSHOTS__INTERVAL_MIN",
        "ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC",
        "ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC",
        "ETCDNODE_SNAPSHOTS__TIMEOUT_SEC",
        "ETCDNODE_SNAPSHOTS__RETENTION_FILES",
        "ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS",
        "ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN",
    ];

    private static void ClearEnv()
    {
        foreach (var key in Keys)
            Environment.SetEnvironmentVariable(key, null);
    }

    // AAA: без env — все дефолты, имя-плейсхолдер, PEM-путь проходит насквозь
    [Fact]
    public void Load_NoEnv_Defaults()
    {
        // Arrange: чистый env
        var saved = Keys.ToDictionary(k => k, k => Environment.GetEnvironmentVariable(k));
        ClearEnv();
        try
        {
            // Act
            var o = EtcdNodeEnv.Load("/tmp/ca.pem");

            // Assert — дефолты бюджетов (spec §5) и сквозной PEM-путь
            o.NodeName.Should().Be("etcdw-node-?");
            o.S3CaPemPath.Should().Be("/tmp/ca.pem");
            o.Snapshots.IntervalMin.Should().Be(360);
            o.Snapshots.MinIntervalSec.Should().Be(300);
            o.Snapshots.RetryIntervalSec.Should().Be(300);
            o.Snapshots.TimeoutSec.Should().Be(30);
            o.Snapshots.RetentionFiles.Should().Be(10);
            o.Snapshots.RetentionObjects.Should().Be(28);
            o.Snapshots.MaintenanceIntervalMin.Should().Be(60);
        }
        finally
        {
            foreach (var (key, value) in saved)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    // AAA: валидный env — все поля читаются
    [Fact]
    public void Load_ValidEnv_AllFields()
    {
        // Arrange
        var saved = Keys.ToDictionary(k => k, k => Environment.GetEnvironmentVariable(k));
        ClearEnv();
        try
        {
            Environment.SetEnvironmentVariable("ETCDNODE_NODE_NAME", "etcdw-node-7");
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", "15");
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", "1");
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "5");

            // Act
            var o = EtcdNodeEnv.Load();

            // Assert — прочитанные значения поверх дефолтов остальных
            o.NodeName.Should().Be("etcdw-node-7");
            o.Snapshots.IntervalMin.Should().Be(15);
            o.Snapshots.MinIntervalSec.Should().Be(1);
            o.Snapshots.RetentionObjects.Should().Be(5);
            o.Snapshots.TimeoutSec.Should().Be(30);
        }
        finally
        {
            foreach (var (key, value) in saved)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    // AAA: невалидный собранный набор — fail-fast (IntervalMin=0)
    [Fact]
    public void Load_InvalidInterval_FailFast()
    {
        // Arrange
        var saved = Keys.ToDictionary(k => k, k => Environment.GetEnvironmentVariable(k));
        ClearEnv();
        try
        {
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", "0");

            // Act + Assert: сообщение перечисляет фактические значения
            var act = () => EtcdNodeEnv.Load();
            act.Should().Throw<InvalidOperationException>()
                .WithMessage("*intervalMin=0*");
        }
        finally
        {
            foreach (var (key, value) in saved)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    // AAA: нулевая ретенция (< 1) — fail-fast
    [Fact]
    public void Load_ZeroRetention_FailFast()
    {
        // Arrange
        var saved = Keys.ToDictionary(k => k, k => Environment.GetEnvironmentVariable(k));
        ClearEnv();
        try
        {
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", "0");

            // Act + Assert
            var act = () => EtcdNodeEnv.Load();
            act.Should().Throw<InvalidOperationException>()
                .WithMessage("*retentionObjects=0*");
        }
        finally
        {
            foreach (var (key, value) in saved)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    // AAA: битое числовое значение — НЕ fail-fast, а дефолт Int-fallback
    // (невалидный НАБОР ловит Load; опечатка одного поля — дефолт бюджета)
    [Fact]
    public void Load_BitNumericValue_FallsBackToDefault()
    {
        // Arrange
        var saved = Keys.ToDictionary(k => k, k => Environment.GetEnvironmentVariable(k));
        ClearEnv();
        try
        {
            Environment.SetEnvironmentVariable("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", "abc");

            // Act
            var o = EtcdNodeEnv.Load();

            // Assert — дефолт 360
            o.Snapshots.IntervalMin.Should().Be(360);
        }
        finally
        {
            foreach (var (key, value) in saved)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    // AAA: Int-хелпер — валидное/отсутствующее/битое значение
    [Theory]
    [InlineData("42", 7, 42)]
    [InlineData(null, 7, 7)]
    [InlineData("oops", 7, 7)]
    public void Int_Parsing(string? raw, int fallback, int expected)
    {
        // Arrange
        var saved = Environment.GetEnvironmentVariable("ETCDNODE_TEST_INT");
        Environment.SetEnvironmentVariable("ETCDNODE_TEST_INT", raw);
        try
        {
            // Act + Assert
            EtcdNodeEnv.Int("ETCDNODE_TEST_INT", fallback).Should().Be(expected);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ETCDNODE_TEST_INT", saved);
        }
    }
}
