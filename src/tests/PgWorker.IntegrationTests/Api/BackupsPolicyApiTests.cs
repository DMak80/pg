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
    public async Task ValidBody200AndKeyInEtcd()
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
    public async Task OmittedFieldsDefaults()
    {
        // Arrange — активный кластер, тело {}
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bpd", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsync("/api/clusters/bpd/backups/policy",
            new StringContent("{}", System.Text.Encoding.UTF8, "application/json"), ct);

        // Assert — 200, ключ с дефолтами 1/1/1/86400/true (t18: целевая схема,
        // хендлер зеркалит дефолт канона arch/19 §9)
        resp.StatusCode.Should().Be(HttpStatusCode.OK);
        var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bpd/policy", ct);
        stored.Value!.Value.Should().Contain("\"days\":1")
            .And.Contain("\"weeks\":1")
            .And.Contain("\"months\":1")
            .And.Contain("\"full_max_age_sec\":86400")
            .And.Contain("\"on_create\":true");
    }

    // AAA: нарушения диапазонов — 400 с перечнем по всем полям.
    [Fact]
    public async Task InvalidRanges400WithList()
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
        stored.Value.Should().BeNull("an invalid policy is not written");
    }

    // AAA: мусорное тело — 400.
    [Fact]
    public async Task GarbageJson400()
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
    public async Task NonexistentCluster404()
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

// ---- t36: секция s3 policy (arch/14 §1.1) ----

// Сид пула /s3/clusters/backups/: config + заявка двух нод (placement/state)
// + portalloc ТОЛЬКО на node1 (кейс «заявлена, не провижинена — валидна»).
private async Task SeedS3PoolAsync(string ctTag = "")
{
    var ct = TestContext.Current.CancellationToken;
    var g = Etcd.Gateway; var ep = Etcd.Endpoint;
    await g.PutAsync(ep, "/s3/clusters/backups/config",
        """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""", null, ct);
    await g.PutAsync(ep, "/s3/clusters/backups/nodes/node1/placement",
        """{"docker_host":"h1"}""", null, ct);
    await g.PutAsync(ep, "/s3/clusters/backups/nodes/node1/state", "\"RUNNING\"", null, ct);
    await g.PutAsync(ep, "/s3/clusters/backups/nodes/node2/placement",
        """{"docker_host":"h2"}""", null, ct);
    await g.PutAsync(ep, "/s3/clusters/backups/endpoints", "h1:18001", null, ct);
    await g.PutAsync(ep, "/s3worker/portalloc/backups",
        """{"node1":{"host":"h1","s3":18001}}""", null, ct);
}

// AAA: политика с валидной секцией s3 → 200; ключ каноничен (парсер t01
// читает pool/списки); инстанс с placement БЕЗ portalloc — ВАЛИДЕН (201/200,
// без 400: адрес появится при provisioning).
[Fact]
public async Task PolicyWithS3ValidInstanceWithoutPortallocIsValid()
{
    // Arrange — кластер + пул (node2 без portalloc-записи).
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bps3", buckets: 4, shards: 2);
    await SeedS3PoolAsync();
    var ct = TestContext.Current.CancellationToken;

    // Act — список write_master включает node2 (заявлена, не провижинена).
    var resp = await Client.PostAsJsonAsync("/api/clusters/bps3/backups/policy", new
    {
        retention = new { days = 1, weeks = 1, months = 1 },
        full_max_age_sec = 86400,
        verify = new { on_create = true },
        s3 = new { pool = "backups", write_master = new[] { "node1", "node2" }, write_replica = new[] { "node1" } },
    }, ct);

    // Assert — 200 и ключ несёт s3; парсер читает секцию.
    resp.StatusCode.Should().Be(HttpStatusCode.OK);
    var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bps3/policy", ct);
    stored.Value!.Value.Should().Contain("\"pool\":\"backups\"")
        .And.Contain("node2");
    var range = await Etcd.Gateway.RangeAsync(Etcd.Endpoint, "/pgworker/backups/bps3/", ct);
    var parsed = BackupsParser.Parse(range.Value, out var errors);
    errors.Should().BeEmpty();
    parsed.Value.Single(c => c.Cluster == "bps3").Policy!.S3!.WriteMaster
        .Should().Equal("node1", "node2");
}

// AAA: повторный PUT без s3 — секция исчезла (выключение бэкапов кластера).
[Fact]
public async Task RepeatedPutWithoutS3SectionRemoved()
{
    // Arrange — policy с s3 уже записана.
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bps3r", buckets: 4, shards: 2);
    await SeedS3PoolAsync();
    var ct = TestContext.Current.CancellationToken;
    await Client.PostAsJsonAsync("/api/clusters/bps3r/backups/policy", new
    {
        retention = new { days = 1, weeks = 1, months = 1 },
        full_max_age_sec = 86400,
        verify = new { on_create = true },
        s3 = new { pool = "backups", write_master = new[] { "node1" }, write_replica = new[] { "node1" } },
    }, ct);

    // Act — тот же PUT без секции s3.
    var resp = await Client.PostAsJsonAsync("/api/clusters/bps3r/backups/policy", new
    {
        retention = new { days = 1, weeks = 1, months = 1 },
        full_max_age_sec = 86400,
        verify = new { on_create = true },
    }, ct);

    // Assert — 200, s3 в ключе нет.
    resp.StatusCode.Should().Be(HttpStatusCode.OK);
    var stored = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/backups/bps3r/policy", ct);
    stored.Value!.Value.Should().NotContain("\"s3\"");
}

// AAA: 400-набор — несуществующий пул / нода не в пуле / дубликат / пустой
// список / pool пуст при заданных списках — ошибки ПО ПОЛЯМ.
[Fact]
public async Task InvalidS3_400PerFields()
{
    // Arrange — кластер + пул.
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bps3v", buckets: 4, shards: 2);
    await SeedS3PoolAsync();
    var ct = TestContext.Current.CancellationToken;

    // Act/Assert — шесть граней валидации.
    var cases = new (string Body, string Field, string Fragment)[]
    {
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"pool":"ghost","write_master":["node1"],"write_replica":["node1"]}}
        """, "s3.pool", "not found"),
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"pool":"backups","write_master":["ghost1"],"write_replica":["node1"]}}
        """, "s3.write_master", "is not in pool"),
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"pool":"backups","write_master":["node1","node1"],"write_replica":["node1"]}}
        """, "s3.write_master", "duplicate"),
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"pool":"backups","write_master":[],"write_replica":["node1"]}}
        """, "s3.write_master", "list is empty"),
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"write_master":["node1"],"write_replica":["node1"]}}
        """, "s3.pool", "required"),
        ("""
        {"retention":{"days":1},"full_max_age_sec":86400,"verify":{"on_create":true},
         "s3":{"pool":"backups","write_master":["node1"],"write_replica":[]}}
        """, "s3.write_replica", "list is empty"),
    };
    foreach (var (body, field, fragment) in cases)
    {
        var resp = await Client.PostAsync("/api/clusters/bps3v/backups/policy",
            new StringContent(body, System.Text.Encoding.UTF8, "application/json"), ct);
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest, body);
        var text = await resp.Content.ReadAsStringAsync(ct);
        text.Should().Contain(field).And.Contain(fragment, body);
    }
}

// AAA: пул в TO_REMOVE → 400 по полю s3.pool.
[Fact]
public async Task PoolToRemove400()
{
    // Arrange — пул с заявкой удаления.
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "bps3t", buckets: 4, shards: 2);
    await SeedS3PoolAsync();
    var ct = TestContext.Current.CancellationToken;
    await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/s3/clusters/backups/config",
        """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1,"state":"TO_REMOVE"}""", null, ct);

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/bps3t/backups/policy", new
    {
        retention = new { days = 1, weeks = 1, months = 1 },
        full_max_age_sec = 86400,
        verify = new { on_create = true },
        s3 = new { pool = "backups", write_master = new[] { "node1" }, write_replica = new[] { "node1" } },
    }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    (await resp.Content.ReadAsStringAsync(ct)).Should().Contain("TO_REMOVE");
}
}
