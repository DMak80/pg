using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/backups/policy (t06, spec Ф4): валидация + put
// policy-ключа формата канона; парсер t01 читает его без parseErrors.
[Collection(NonE2eCollection.Name)]
public class BackupsPolicyApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // AAA: валидное тело → 200, ключ в etcd равен телу канона, парсер t01
    // читает policy без parseErrors (AC7).
    [Fact]
    public async Task Валидное_тело_200_и_ключ_в_etcd()
    {
        // Arrange — активный кластер + валидная политика
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bp", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bp/backups/policy", new
        {
            retention = new { days = 3, weeks = 2, months = 1 },
            full_max_age_sec = 7200,
            verify = new { on_create = false },
        }, ct);

        // Assert — 200 и ключ формата канона
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bp/policy", ct);
        stored.Value.Should().NotBeNull();
        stored.Value!.Value.Should().Contain("\"days\":3")
            .And.Contain("\"weeks\":2")
            .And.Contain("\"months\":1")
            .And.Contain("\"full_max_age_sec\":7200")
            .And.Contain("\"on_create\":false");

        // Assert — парсер t01 читает без parseErrors, гранулы 3/2/1
        var range = await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/pgworker/backups/bp/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        var policy = parsed.Value.Should().ContainSingle(c => c.Cluster == "bp").Subject.Policy;
        policy.Should().NotBeNull();
        policy!.RetentionDays.Should().Be(3);
        policy.RetentionWeeks.Should().Be(2);
        policy.RetentionMonths.Should().Be(1);
        policy.FullMaxAgeSec.Should().Be(7200);
        policy.VerifyOnCreate.Should().BeFalse();
    }

    // AAA: пустое тело — отсутствующие поля замещаются дефолтами канона.
    [Fact]
    public async Task Опущенные_поля_дефолты()
    {
        // Arrange — активный кластер, тело {}
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/bpd/backups/policy",
            new StringContent("{}", System.Text.Encoding.UTF8, "application/json"), ct);

        // Assert — 200, ключ с дефолтами 7/4/6/86400/true
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd/policy", ct);
        stored.Value!.Value.Should().Contain("\"days\":7")
            .And.Contain("\"weeks\":4")
            .And.Contain("\"months\":6")
            .And.Contain("\"full_max_age_sec\":86400")
            .And.Contain("\"on_create\":true");
    }

    // AAA: нарушения диапазонов — 400 с перечнем по всем полям.
    [Fact]
    public async Task Невалидные_диапазоны_400_с_перечнем()
    {
        // Arrange — days=400, weeks=99, full_max_age_sec=1 (все три мимо)
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpv", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpv/backups/policy", new
        {
            retention = new { days = 400, weeks = 99 },
            full_max_age_sec = 1,
        }, ct);

        // Assert — 400 и errors по трём полям
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        var errors = problem.GetProperty("errors");
        errors.TryGetProperty("retention.days", out _).Should().BeTrue();
        errors.TryGetProperty("retention.weeks", out _).Should().BeTrue();
        errors.TryGetProperty("full_max_age_sec", out _).Should().BeTrue();
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpv/policy", ct);
        stored.Value.Should().BeNull("невалидная политика не пишется");
    }

    // AAA: мусорное тело — 400.
    [Fact]
    public async Task Мусорный_JSON_400()
    {
        // Arrange — активный кластер
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpj", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/bpj/backups/policy",
            new StringContent("not-json", System.Text.Encoding.UTF8, "application/json"), ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Validation failed");
    }

    // AAA: несуществующий кластер — 404, ключ не пишется.
    [Fact]
    public async Task Несуществующий_кластер_404()
    {
        // Arrange — кластер nocluster не сеялся
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/nocluster/backups/policy",
            new StringContent("{}", System.Text.Encoding.UTF8, "application/json"), ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/nocluster/policy", ct);
        stored.Value.Should().BeNull();
    }

    // ---- reliability t02: drill.interval_days ----

    // AAA: тело с drill.interval_days=3 → 200, policy-ключ содержит
    // "drill":{"interval_days":3} рядом с прежними секциями.
    [Fact]
    public async Task Policy_WithDrillInterval_AcceptedAndStored()
    {
        // Arrange — активный кластер + валидная политика с drill.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd1", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpd1/backups/policy", new
        {
            retention = new { days = 7, weeks = 4, months = 6 },
            full_max_age_sec = 86400,
            verify = new { on_create = true },
            drill = new { interval_days = 3 },
        }, ct);

        // Assert — 200 и секция drill в ключе; парсер читает интервал.
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd1/policy", ct);
        stored.Value!.Value.Should().Contain("\"drill\":{\"interval_days\":3}");
        var range = await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/pgworker/backups/bpd1/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        parsed.Value.Single().Policy!.DrillIntervalDays.Should().Be(3);
    }

    // AAA: drill в теле отсутствует → в записанной policy секции drill НЕТ
    // (кластер на глобальном дефолте; замещение целиком сохраняется).
    [Fact]
    public async Task Policy_WithoutDrill_DrillSectionOmitted()
    {
        // Arrange — активный кластер; тело без drill (ранее ключ с drill был).
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd2", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/pgworker/backups/bpd2/policy",
            """{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"drill":{"interval_days":5}}""",
            null, ct);

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpd2/backups/policy", new
        {
            retention = new { days = 7, weeks = 4, months = 6 },
            full_max_age_sec = 86400,
        }, ct);

        // Assert — секция drill ушла из ключа (замещение целиком).
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd2/policy", ct);
        stored.Value!.Value.Should().NotContain("drill");
    }

    // AAA: interval_days=-1 / 3651 → 400 с "drill.interval_days" в перечне.
    [Theory]
    [InlineData(-1)]
    [InlineData(3651)]
    public async Task Policy_DrillOutOfRange_Rejected400(int intervalDays)
    {
        // Arrange — активный кластер.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd3", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpd3/backups/policy", new
        {
            drill = new { interval_days = intervalDays },
        }, ct);

        // Assert — 400 и поле в перечне; ключ не пишется.
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("errors").TryGetProperty("drill.interval_days", out _).Should().BeTrue();
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd3/policy", ct);
        stored.Value.Should().BeNull();
    }

    // AAA: interval_days=0 — валиден (выключение дрилов кластера).
    [Fact]
    public async Task Policy_DrillZero_Valid()
    {
        // Arrange — активный кластер.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd4", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpd4/backups/policy", new
        {
            drill = new { interval_days = 0 },
        }, ct);

        // Assert — 200 и drill.interval_days=0 в ключе.
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd4/policy", ct);
        stored.Value!.Value.Should().Contain("\"drill\":{\"interval_days\":0}");
    }

    // AAA: мисматч остального тела по-прежнему 400 (замещение целиком не
    // ослаблено появлением drill-секции).
    [Fact]
    public async Task Policy_BadRetentionWithDrill_Rejected400()
    {
        // Arrange — валидный drill + невалидная retention в одном теле.
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd5", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/bpd5/backups/policy", new
        {
            retention = new { days = 400 },
            drill = new { interval_days = 2 },
        }, ct);

        // Assert — 400 по retention.days; drill-ключа в etcd нет.
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd5/policy", ct);
        stored.Value.Should().BeNull();
    }
}
