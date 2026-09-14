using System.Text.Json;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Templates;

// DcsConfigConvergence (t11, spec §4.2): минимальный патч-документ для PATCH
// /config — тайминги Patroni (PatroniTimings, единый канон t09) +
// postgresql.parameters от PgParametersCanon.Desired. Поглощает
// PatroniTimings.DivergencePatch (кейсы Regression_T09_* мигрированы, смысл и
// имена сохранены). Конвергентно → null («не второй регулярный писатель»).
public class DcsConfigConvergenceTests
{
    // Desired-набор эталонного входа (oltp/8GiB/4cpu/60/ssd/mid_ram, exclude пуст).
    private static IReadOnlyList<(string Name, string RawValue)> Desired() =>
        PgParametersCanon.Desired(PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)), null);

    // ---------- Миграция кейсов t09 (desired == null → патч только таймингов) ----------

    // AAA (t09): дефолтный конфиг Patroni — патч несёт ВСЕ канонические поля.
    [Fact]
    public void Regression_T09_Divergence_DefaultConfig_PatchCarriesCanonical()
    {
        // Arrange — динамический конфиг на Patroni-дефолтах.
        const string config = """{"ttl":30,"loop_wait":10,"retry_timeout":10,"postgresql":{"use_pg_rewind":true}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert: все канонические тайминги в патче; postgresql не тронут
        // (desired == null — параметры вне игры).
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
        patch.Should().NotContain("postgresql");
    }

    // AAA (t09): конфиг, молча скорректированный Patroni 4.1 — патч минимальный.
    [Fact]
    public void Regression_T09_Divergence_PatroniAdjustedConfig_MinimalPatch()
    {
        // Arrange — фактический /config из диагностики t09.
        const string config = """{"ttl":20,"loop_wait":2,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert
        patch.Should().Be("""{"loop_wait":1}""");
    }

    // AAA (t09): канонический конфиг — null, мутаций нет.
    [Fact]
    public void Regression_T09_Divergence_CanonicalConfig_NoPatch()
    {
        // Arrange
        const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert
        patch.Should().BeNull();
    }

    // AAA (t09): пустой/битый/чужой ответ — полный патч (тайминги + весь desired).
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-json{")]
    [InlineData("""{"foreign":"document"}""")]
    public void Regression_T09_Divergence_GarbageConfig_PatchAllCanonical(string? config)
    {
        // Arrange — непонятный конфиг приводится к канону.

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, Desired());

        // Assert: тайминги + все параметры желаемого набора в патче.
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
        patch.Should().Contain("\"postgresql\":{\"parameters\":{")
            .And.Contain("\"max_connections\":\"60\"")
            .And.Contain("\"wal_level\":\"logical\"");
    }

    // ---------- Новые кейсы t11 ----------

    // AAA: расходящееся значение параметра → обновление в патче; счётчик Updated.
    [Fact]
    public void Divergence_ParameterValueDiffers_UpdatedInPatch()
    {
        // Arrange — живой конфиг каноничен по таймингам; shared_buffers от
        // прежней заявки (1GB вместо 2GB), лишних ключей нет.
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("shared_buffers", "2GB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"60","shared_buffers":"1GB"}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: только расходящийся параметр; updated=1; патч без таймингов.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"shared_buffers":"2GB"}}}""");
        divergence.Updated.Should().Be(1);
        divergence.Added.Should().Be(0);
        divergence.Removed.Should().Be(0);
        divergence.PostmasterTouched.Should().BeTrue("shared_buffers — postmaster");
    }

    // AAA: параметр отсутствует в живом конфиге → добавление; счётчик Added.
    [Fact]
    public void Divergence_ParameterMissingInLive_AddedInPatch()
    {
        // Arrange — живой конфиг без блока parameters вовсе.
        var desired = new List<(string Name, string RawValue)> { ("max_connections", "60") };
        const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: desired добавлен; postmaster-имя затронуто.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"max_connections":"60"}}}""");
        divergence.Added.Should().Be(1);
        divergence.Updated.Should().Be(0);
        divergence.Removed.Should().Be(0);
        divergence.PostmasterTouched.Should().BeTrue();
    }

    // AAA: лишний ключ в живом конфиге (нет в desired) → удаление null-значением;
    // удаления — в КОНЦЕ патча; счётчик Removed.
    [Fact]
    public void Divergence_ExtraLiveParameter_RemovedWithNullAtEnd()
    {
        // Arrange — операторский ключ в DCS + один расходящийся desired-ключ.
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("shared_buffers", "2GB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"60","shared_buffers":"1GB","my_custom":"42"}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, desired);

        // Assert: обновление раньше, удаление null — последним ключом.
        divergence.Patch.Should().Be(
            """{"postgresql":{"parameters":{"shared_buffers":"2GB","my_custom":null}}}""");
        divergence.Updated.Should().Be(1);
        divergence.Removed.Should().Be(1);
        divergence.Added.Should().Be(0);
    }

    // AAA: число в живом конфиге нормализуется (GetRawText): 2048 ≡ "2048" —
    // конвергентно, в патч не попадает.
    [Fact]
    public void Divergence_NumberInLive_NormalizedConvergent()
    {
        // Arrange — shared_buffers числом; desired несёт строку "2048".
        var desired = new List<(string Name, string RawValue)> { ("shared_buffers", "2048") };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"shared_buffers":2048}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert: нормализация числа к строке — совпадение, патч пуст.
        patch.Should().BeNull();
    }

    // AAA: bool в живом конфиге нормализуется к "true"/"false" (НЕ семантически:
    // "on" ≠ "true" — посторонний формат конвергируется первым патчем; spec §6).
    [Fact]
    public void Divergence_BoolInLive_NotSemanticOn()
    {
        // Arrange — hot_standby: true (JSON-bool), канон несёт "on".
        var desired = new List<(string Name, string RawValue)> { ("hot_standby", "on") };
        const string config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"hot_standby":true}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert: "true" ≠ "on" → расхождение, в патч каноническим значением.
        patch.Should().Be("""{"postgresql":{"parameters":{"hot_standby":"on"}}}""");
    }

    // AAA: тайминги и параметры — в ОДНОМ документе; порядок: тайминги, затем
    // параметры в порядке desired, удаления в конце.
    [Fact]
    public void Divergence_TimingsAndParametersSingleDocument()
    {
        // Arrange — расходится loop_wait + два параметра (один удаляется).
        var desired = new List<(string Name, string RawValue)>
        {
            ("max_connections", "60"), ("work_mem", "16MB"),
        };
        const string config =
            """{"ttl":20,"loop_wait":5,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{"max_connections":"20","work_mem":"16MB","old_key":"1"}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, desired);

        // Assert
        patch.Should().Be(
            """{"loop_wait":1,"postgresql":{"parameters":{"max_connections":"60","old_key":null}}}""");
    }

    // AAA: desired == null → патч ТОЛЬКО таймингов (нет/неполна заявка — домен
    // конвергенции параметров не расширяется).
    [Fact]
    public void Divergence_DesiredNull_TimingsOnly()
    {
        // Arrange — живой конфиг с чужими параметрами, desired нет.
        const string config =
            """{"ttl":30,"loop_wait":10,"retry_timeout":10,"postgresql":{"parameters":{"max_connections":"20"}}}""";

        // Act
        var patch = DcsConfigConvergence.DivergencePatch(config, null);

        // Assert: только тайминги; параметры не тронуты.
        patch.Should().NotContain("postgresql");
    }

    // AAA: конвергентный живой конфиг (тайминги + весь desired как строки) → null.
    [Fact]
    public void Divergence_FullyConvergent_NoPatch()
    {
        // Arrange — конфиг собран из desired + канонические тайминги.
        var parameters = string.Join(",", Desired()
            .Select(p => $"{JsonSerializer.Serialize(p.Name)}:{JsonSerializer.Serialize(p.RawValue)}"));
        // (конкатенация вместо $$"""-интерполяции: серия фигурных скобок ломает raw-string)
        var config =
            """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true,"postgresql":{"parameters":{""" + parameters + """}}}""";

        // Act
        var divergence = DcsConfigConvergence.Analyze(config, Desired());

        // Assert: расхождений нет, postmaster не тронут.
        divergence.Patch.Should().BeNull();
        divergence.PostmasterTouched.Should().BeFalse();
    }

    // AAA (self-check SingleCanonicalSource, t11 spec §4.4): JSON, собранный из
    // SPILO_CONFIGURATION текущего билдера, конвергентен с desired (null-патч)
    // — bootstrap и конвергенция из одного источника, молодой кластер не патчится.
    [Fact]
    public void Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource()
    {
        // Arrange: топология шарда из одной ноды; tuning эталонного входа.
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Act: env → вырезаем YAML-блок parameters → JSON живого конфига.
        var spilo = SpiloEnvBuilder.Build(
            topology, new EtcdEndpoints(["http://e1:2379"]),
            new InstallSecrets("su", "sb", "adm", "mov"), tuning, null)["SPILO_CONFIGURATION"];
        var lines = spilo.Split('\n')
            .SkipWhile(l => !l.TrimEnd().EndsWith("parameters:", StringComparison.Ordinal))
            .Skip(1)
            .TakeWhile(l => l.StartsWith("        ", StringComparison.Ordinal))
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith('#'))
            .Select(l =>
            {
                var parts = l.Split(':', 2);
                return (Name: parts[0].Trim(), Value: parts[1].Trim().Trim('"')); // YAML-цитирование снимается
            })
            .ToList();
        var parameters = string.Join(",", lines.Select(p =>
            $"{JsonSerializer.Serialize(p.Name)}:{JsonSerializer.Serialize(p.Value)}"));
        // (конкатенация вместо $$"""-интерполяции: серия фигурных скобок ломает raw-string)
        var config =
            $$"""{"ttl":{{PatroniTimings.Ttl}},"loop_wait":{{PatroniTimings.LoopWait}},"retry_timeout":{{PatroniTimings.RetryTimeout}},"synchronous_mode":true,"postgresql":{"parameters":{""" + parameters + """}}}""";

        // Assert: bootstrap-YAML и desired дают нулевой патч (один источник);
        // состав блока совпадает с desired поимённо.
        var desired = PgParametersCanon.Desired(tuning, null);
        lines.Select(p => p.Name).Should().Equal(desired.Select(p => p.Name));
        DcsConfigConvergence.DivergencePatch(config, desired).Should()
            .BeNull("bootstrap и конвергенция строят параметры из одного набора");
    }
}
