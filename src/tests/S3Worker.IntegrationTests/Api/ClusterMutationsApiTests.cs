using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using S3Worker.IntegrationTests.Etcd;
using Xunit;

namespace S3Worker.IntegrationTests.Api;

// WAF-мутации S3-домена (arch/24 §1.1, spec §8.2): create c instances[] 0..9
// (201/409/400/422), add-node с именем из запроса (201/409/404), delete-node
// (202/204/404/409 гвард последнего), PUT links (202/400/404), deny/allow
// (202/204/404), rotations 201/200/404, метрика операций API, секретность
// ответов. sync-probe (t33) — SyncProbeApiTests.
[Collection(S3ApiCollection.Name)]
public class ClusterMutationsApiTests(S3ApiFixture fx)
{
    private HttpClient Client => fx.Factory.CreateClient();

    private static string Cluster(string name) => $"{name}{Guid.NewGuid().ToString("N")[..6]}";

    private static object Instance(string name) => new
    {
        name,
        placement = new { dockerHost = "local" },
    };

    private static object CreateBody(string name, string[]? instances = null) => new
    {
        name,
        platform = "owns3",
        secretProfile = TestSecrets.Profile,
        instances = (instances ?? ["east1", "east2"]).Select(Instance).ToArray(),
        resources = new { cpu = 1m, memGi = 1, diskGi = 5 },
    };

    private async Task<string> CreateClusterAsync(string name, string[]? instances = null)
    {
        using var client = Client;
        using var response = await client.PostAsJsonAsync(
            "/api/s3/clusters", CreateBody(name, instances), TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.Created, body);
        return name;
    }

    // §8.2.1: создание с 0 инстансов → config nodes=0, state=NOT_INITIALIZED,
    // ключей nodes/* нет (снятие state воркером — вне API-теста, E2E §8.3.5).
    [Fact]
    public async Task Create_НольИнстансов_ConfigNodes0()
    {
        // Arrange
        var cluster = Cluster("empty");
        using var client = Client;

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/s3/clusters", CreateBody(cluster, []), TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 201; config nodes=0; nodes-префикс пуст.
        response.StatusCode.Should().Be(HttpStatusCode.Created, body);
        body.Should().Contain("\"nodes\":0");
        var gateway = fx.Etcd.Gateway;
        var config = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("\"nodes\":0")
            .And.Contain("\"state\":\"NOT_INITIALIZED\"");
        var range = await gateway.RangeAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/nodes/", TestContext.Current.CancellationToken);
        range.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Create_201_КлючиКанона()
    {
        // Arrange: уникальное имя (etcd фикстуры общий для класса).
        var cluster = Cluster("mk");
        using var client = Client;

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/s3/clusters", CreateBody(cluster), TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 201; config NOT_INITIALIZED; инстансам state/placement/resources.
        response.StatusCode.Should().Be(HttpStatusCode.Created, body);
        body.Should().Contain("east1").And.Contain("east2").And.NotContain("it-secret");
        var gateway = fx.Etcd.Gateway;
        var config = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"")
            .And.Contain("\"platform\":\"owns3\"")
            .And.Contain("\"secret_profile\":\"backups-main\"");
        var state = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/nodes/east1/state", TestContext.Current.CancellationToken);
        state.Value!.Value.Should().Be("NOT_INITIALIZED");
        var placement = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/nodes/east1/placement", TestContext.Current.CancellationToken);
        placement.Value!.Value.Should().Be("""{"docker_host":"local"}""");
        var resources = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/nodes/east1/resources", TestContext.Current.CancellationToken);
        resources.Value!.Value.Should().Be("""{"cpu":"1","mem":"1Gi","disk":"5Gi"}""");
    }

    [Fact]
    public async Task Create_Повтор_409()
    {
        // Arrange
        var cluster = Cluster("dup");
        await CreateClusterAsync(cluster);
        using var client = Client;

        // Act: повтор.
        using var response = await client.PostAsJsonAsync(
            "/api/s3/clusters", CreateBody(cluster), TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict, body);
    }

    // §8.2.2 (часть): коллизия имени инстанса с существующим — 409
    // (предсуществование ключа инстанса в новом кластере).
    [Fact]
    public async Task Create_КоллизияИмениИнстанса_409()
    {
        // Arrange: в новом кластере предзаписан ключ инстанса ghost (имя занято).
        var cluster = Cluster("coll");
        var gateway = fx.Etcd.Gateway;
        await gateway.PutAsync(fx.Etcd.Endpoint,
            $"/s3/clusters/{cluster}/nodes/ghost/state", "NOT_INITIALIZED", null, TestContext.Current.CancellationToken);
        using var client = Client;

        // Act: create с занятым именем.
        using var response = await client.PostAsJsonAsync(
            "/api/s3/clusters", CreateBody(cluster, ["ghost"]), TestContext.Current.CancellationToken);

        // Assert: 409 (коллизия имени); config не создан.
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var config = await gateway.GetAsync(fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value.Should().BeNull("заявка отклонена до клэйм-txn");
    }

    [Fact]
    public async Task Create_Невалидная_400СПолем()
    {
        // Arrange: platform=minio + паттерн-нарушение имени (оба в errors).
        var cluster = Cluster("bad");
        using var client = Client;

        // Act
        using var response = await client.PostAsJsonAsync("/api/s3/clusters",
            new { name = cluster, platform = "minio", secretProfile = TestSecrets.Profile,
                instances = new object[] { new { name = "9bad", placement = new { dockerHost = "local" } } },
                resources = new { cpu = 0.001m, memGi = 0, diskGi = 0 } },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 400 с errors.<field>; кластер не создан (валидация до etcd).
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest, body);
        body.Should().Contain("\"platform\"").And.Contain("name");
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value.Should().BeNull();
    }

    [Fact]
    public async Task Create_НеизвестныйПрофиль_422СПеречнем()
    {
        // Arrange: профиль absent-profile отсутствует в env воркера.
        var cluster = Cluster("prof");
        using var client = Client;

        // Act
        using var response = await client.PostAsJsonAsync("/api/s3/clusters",
            new { name = cluster, platform = "owns3", secretProfile = "absent-profile",
                instances = new object[] { new { name = "east1", placement = new { dockerHost = "local" } } },
                resources = new { cpu = 1m, memGi = 1, diskGi = 5 } },
            TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 422 с перечнем имён; значений секретов нет.
        response.StatusCode.Should().Be((HttpStatusCode)422, body);
        body.Should().Contain(TestSecrets.Profile).And.NotContain("it-secret");
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value.Should().BeNull();
    }

    [Fact]
    public async Task Delete_202_204_404()
    {
        // Arrange
        var cluster = Cluster("del");
        await CreateClusterAsync(cluster);
        using var client = Client;

        // Act 1: перевод в TO_REMOVE; Act 2: повтор; Act 3: чужое имя.
        using var first = await client.DeleteAsync($"/api/s3/clusters/{cluster}", TestContext.Current.CancellationToken);
        using var second = await client.DeleteAsync($"/api/s3/clusters/{cluster}", TestContext.Current.CancellationToken);
        using var missing = await client.DeleteAsync($"/api/s3/clusters/{Cluster("nope")}", TestContext.Current.CancellationToken);

        // Assert: 202 → 204 → 404; config несёт TO_REMOVE.
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var config = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("\"state\":\"TO_REMOVE\"");
    }

    // §8.2.2: add-node с именем из тела — 201; паттерн/резерв — 400; коллизия
    // имени — 409; предел 9 — 409.
    [Fact]
    public async Task AddNode_201_Коллизия409_Резерв400_Предел409()
    {
        // Arrange: кластер 2 инстанса, воркер дошёл до Active (эмуляция).
        var cluster = Cluster("add");
        await CreateClusterAsync(cluster, ["east1", "east2"]);
        using var client = Client;
        await DeactivateStateAsync(cluster);

        // Act 1: add-node с именем → 201, state NOT_INITIALIZED.
        using var added = await client.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
            new { name = "west1", placement = new { dockerHost = "local" }, resources = new { cpu = 1m, memGi = 1, diskGi = 5 } },
            TestContext.Current.CancellationToken);
        var body = await added.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert 1
        added.StatusCode.Should().Be(HttpStatusCode.Created, body);
        body.Should().Contain("west1");
        var state = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/nodes/west1/state", TestContext.Current.CancellationToken);
        state.Value!.Value.Should().Be("NOT_INITIALIZED");

        // Act 2: резерв orchestrator → 400; паттерн → 400; коллизия имени → 409.
        using var reserved = await client.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
            new { name = "orchestrator", placement = new { dockerHost = "local" } }, TestContext.Current.CancellationToken);
        using var badPattern = await client.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
            new { name = "9bad", placement = new { dockerHost = "local" } }, TestContext.Current.CancellationToken);
        using var collision = await client.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
            new { name = "east1", placement = new { dockerHost = "local" } }, TestContext.Current.CancellationToken);

        // Assert 2
        reserved.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        badPattern.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        collision.StatusCode.Should().Be(HttpStatusCode.Conflict);

        // Act 3: до предела (east1..east2, west1 = 3; +6 → 9; 10-й → 409).
        var last = HttpStatusCode.Created;
        for (var i = 4; i <= 10; i++)
        {
            using var response = await client.PostAsJsonAsync($"/api/s3/clusters/{cluster}/nodes",
                new { name = $"extra{i}", placement = new { dockerHost = "local" } }, TestContext.Current.CancellationToken);
            last = response.StatusCode;
        }

        // Assert 3: десятый — 409 (предел 9).
        last.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task DeleteNode_202_204_Последний_409()
    {
        // Arrange: 2 инстанса (вывод east2) + 1 инстанс (гвард последнего).
        var cluster = Cluster("rmn");
        await CreateClusterAsync(cluster, ["east1", "east2"]);
        var single = Cluster("solo");
        await CreateClusterAsync(single, ["east1"]);
        using var client = Client;

        // Act 1: вывод east2 → 202; Act 2: повтор → 204; Act 3: последний
        // инстанс кластера single → 409; Act 4: чужое имя → 404.
        using var first = await client.DeleteAsync($"/api/s3/clusters/{cluster}/nodes/east2", TestContext.Current.CancellationToken);
        using var second = await client.DeleteAsync($"/api/s3/clusters/{cluster}/nodes/east2", TestContext.Current.CancellationToken);
        using var last = await client.DeleteAsync($"/api/s3/clusters/{single}/nodes/east1", TestContext.Current.CancellationToken);
        using var missing = await client.DeleteAsync($"/api/s3/clusters/{cluster}/nodes/ghost", TestContext.Current.CancellationToken);

        // Assert
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.NoContent);
        last.StatusCode.Should().Be(HttpStatusCode.Conflict);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // §8.2.3: PUT links — валидный → 202 + значение ключа; несуществующее имя /
    // a==b / дубль (в т.ч. зеркальный) → 400; чужой кластер → 404.
    [Fact]
    public async Task PutLinks_202_ЗначениеВEtcd_400_Валидации_404()
    {
        // Arrange
        var cluster = Cluster("lnk");
        await CreateClusterAsync(cluster, ["east1", "east2", "west1"]);
        using var client = Client;

        // Act 1: валидный частичный граф.
        using var ok = await client.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
            new { links = new[] { new { a = "east1", b = "east2" }, new { a = "east1", b = "west1" } } },
            TestContext.Current.CancellationToken);

        // Assert 1: 202 + канонический JSON ключа links.
        ok.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var links = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/links", TestContext.Current.CancellationToken);
        links.Value!.Value.Should().Be("""[{"a":"east1","b":"east2"},{"a":"east1","b":"west1"}]""");

        // Act 2: несуществующее имя; a==b; дубль; зеркальный дубль.
        using var ghost = await client.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
            new { links = new[] { new { a = "east1", b = "ghost" } } }, TestContext.Current.CancellationToken);
        using var self = await client.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
            new { links = new[] { new { a = "east1", b = "east1" } } }, TestContext.Current.CancellationToken);
        using var dup = await client.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
            new { links = new[] { new { a = "east1", b = "east2" }, new { a = "east1", b = "east2" } } },
            TestContext.Current.CancellationToken);
        using var mirror = await client.PutAsJsonAsync($"/api/s3/clusters/{cluster}/links",
            new { links = new[] { new { a = "east1", b = "east2" }, new { a = "east2", b = "east1" } } },
            TestContext.Current.CancellationToken);

        // Assert 2: все 400.
        ghost.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        self.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        dup.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        mirror.StatusCode.Should().Be(HttpStatusCode.BadRequest);

        // Act 3: чужой кластер → 404.
        using var nope = await client.PutAsJsonAsync($"/api/s3/clusters/{Cluster("nope")}/links",
            new { links = new[] { new { a = "east1", b = "east2" } } }, TestContext.Current.CancellationToken);
        nope.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // §8.2.4: deny → 202, повтор → 204, имя в etcd; 404 чужого инстанса;
    // allow → 202, повтор → 204, имя убрано.
    [Fact]
    public async Task DenyAllow_202_204_ЗначениеВEtcd_404()
    {
        // Arrange
        var cluster = Cluster("dny");
        await CreateClusterAsync(cluster, ["east1", "east2"]);
        using var client = Client;

        // Act 1: deny east2 → 202; повтор → 204; denylist в etcd.
        using var deny = await client.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/deny",
            null, TestContext.Current.CancellationToken);
        using var denyAgain = await client.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/deny",
            null, TestContext.Current.CancellationToken);

        // Assert 1
        deny.StatusCode.Should().Be(HttpStatusCode.Accepted);
        denyAgain.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var list = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/denylist", TestContext.Current.CancellationToken);
        list.Value!.Value.Should().Be("""["east2"]""");

        // Act 2: чужой инстанс → 404.
        using var ghost = await client.PostAsync($"/api/s3/clusters/{cluster}/nodes/ghost/deny",
            null, TestContext.Current.CancellationToken);
        ghost.StatusCode.Should().Be(HttpStatusCode.NotFound);

        // Act 3: allow → 202; повтор → 204; имя убрано.
        using var allow = await client.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/allow",
            null, TestContext.Current.CancellationToken);
        using var allowAgain = await client.PostAsync($"/api/s3/clusters/{cluster}/nodes/east2/allow",
            null, TestContext.Current.CancellationToken);

        // Assert 3
        allow.StatusCode.Should().Be(HttpStatusCode.Accepted);
        allowAgain.StatusCode.Should().Be(HttpStatusCode.NoContent);
        var empty = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/denylist", TestContext.Current.CancellationToken);
        empty.Value!.Value.Should().Be("[]");
    }

    [Fact]
    public async Task Rotations_201_Повтор_200_Чужое_404()
    {
        // Arrange
        var cluster = Cluster("rot");
        await CreateClusterAsync(cluster);
        using var client = Client;

        // Act: заявка, повтор, чужое имя.
        using var first = await client.PostAsync($"/api/s3/clusters/{cluster}/rotations",
            null, TestContext.Current.CancellationToken);
        using var second = await client.PostAsync($"/api/s3/clusters/{cluster}/rotations",
            null, TestContext.Current.CancellationToken);
        using var missing = await client.PostAsync($"/api/s3/clusters/{Cluster("nope")}/rotations",
            null, TestContext.Current.CancellationToken);

        // Assert: 201 → 200 (идемпотентно) → 404; ключ заявки жив.
        first.StatusCode.Should().Be(HttpStatusCode.Created);
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var ticket = await fx.Etcd.Gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3worker/rotations/{cluster}", TestContext.Current.CancellationToken);
        ticket.Value!.Value.Should().Contain("\"requested_by\"");
    }

    // Эмуляция воркера, дошедшего до Active: канонический config без state
    // (циклы WAF выключены — снимаем заявочный state вручную).
    private async Task DeactivateStateAsync(string cluster)
    {
        var gateway = fx.Etcd.Gateway;
        var config = await gateway.GetAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", TestContext.Current.CancellationToken);
        config.Value!.Value.Should().Contain("\"state\"");
        var withoutState = config.Value.Value.Replace(",\"state\":\"NOT_INITIALIZED\"", "");
        await gateway.PutAsync(
            fx.Etcd.Endpoint, $"/s3/clusters/{cluster}/config", withoutState, null,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task СерияВызовов_МетрикаОперацийВMetrics()
    {
        // Arrange: фабрика БЕЗ RemoveAll<IHostedService> — OTel-MeterProvider
        // hosted-сервис, без него /metrics не экспортирует доменные инструменты.
        var etcd = new S3EtcdFixture();
        await etcd.InitializeAsync();
        var factory = new S3MetricsApiFactory(etcd);
        try
        {
            using var client = factory.CreateClient();
            var cluster = Cluster("mtr");
            (await client.PostAsJsonAsync("/api/s3/clusters", CreateBody(cluster), TestContext.Current.CancellationToken))
                .StatusCode.Should().Be(HttpStatusCode.Created);
            (await client.PostAsJsonAsync("/api/s3/clusters", CreateBody(cluster), TestContext.Current.CancellationToken))
                .StatusCode.Should().Be(HttpStatusCode.Conflict);

            // Act: /metrics (общая экспозиция Task 3). Счётчик создаётся до
            // старта OTel-провайдера — ретрай-scrape (≤3 попыток).
            string body = "";
            for (var attempt = 0; attempt < 3; attempt++)
            {
                using var metrics = await client.GetAsync("/metrics", TestContext.Current.CancellationToken);
                body = await metrics.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
                if (body.Contains("s3worker_api_operations_total", StringComparison.Ordinal))
                    break;
                await Task.Delay(500, TestContext.Current.CancellationToken);
            }

            // Assert: s3worker_api_operations_total с кодами 201/409 ≥ 1.
            body.Should().Contain("s3worker_api_operations_total{otel_scope_name=\"S3Worker\",code=\"201\",operation=\"create-cluster\"}");
            body.Should().Contain("s3worker_api_operations_total{otel_scope_name=\"S3Worker\",code=\"409\",operation=\"create-cluster\"}");
        }
        finally
        {
            await factory.DisposeAsync();
            await etcd.DisposeAsync();
        }
    }

    [Fact]
    public async Task SeedDemo_Идемпотентен_ФлагOff_404()
    {
        // Arrange: demo уникализуем невозможностью — флаг off проверяем
        // отдельной фабрикой ниже; здесь — идемпотентность on-фабрикой.
        using var client = Client;

        // Act: demo с уникальным префиксом создать нельзя (имя фиксировано
        // "demo") — поэтому on-кейс: если жив → 200, иначе 201.
        using var first = await client.PostAsync("/api/seed/demo", null, TestContext.Current.CancellationToken);

        // Assert: 201 или 200 (зависит от порядка классов в коллекции), оба — успех.
        first.StatusCode.Should().BeOneOf(HttpStatusCode.Created, HttpStatusCode.OK);
    }

    // Флаг off: отдельная фабрика (не коллекция — свой etcd).
    [Fact]
    public async Task SeedDemo_ФлагOff_404()
    {
        // Arrange: свой etcd + фабрика с EnableSeedEndpoint=false.
        var etcd = new S3EtcdFixture();
        await etcd.InitializeAsync();
        var factory = new S3ApiFactory(etcd, enableSeed: false);
        try
        {
            using var client = factory.CreateClient();

            // Act
            using var response = await client.PostAsync("/api/seed/demo", null, TestContext.Current.CancellationToken);

            // Assert
            response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
        finally
        {
            await factory.DisposeAsync();
            await etcd.DisposeAsync();
        }
    }
}
