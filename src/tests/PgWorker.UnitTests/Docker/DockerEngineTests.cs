using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Тонкий клиент Docker Engine API (задача 14): формат запросов, идемпотентность 404/409, BusyPorts.
public class DockerEngineTests
{
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url, string Body)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var body = request.Content is null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(ct);
            Requests.Add((request.Method.Method, request.RequestUri!.PathAndQuery, body));
            return responder(request);
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode code = HttpStatusCode.OK) => new()
    {
        StatusCode = code,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static DockerEngine NewEngine(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("http://docker") }, "h1");

    [Fact]
    public void Factory_UnixEndpoint_HasConnectCallback_TcpHasNot()
    {
        // Arrange — фабрика строит handler под транспорт endpoint'а
        var factory = new DockerEngineFactory();

        // Act
        var unixHandler = factory.CreateHandler("unix:///var/run/docker.sock");
        var tcpHandler = factory.CreateHandler("tcp://10.0.1.11:2375");

        // Assert: unix → SocketsHttpHandler с ConnectCallback (UnixDomainSocketEndPoint);
        // tcp — обычный handler без callback
        var unix = unixHandler.Should().BeOfType<SocketsHttpHandler>().Subject;
        unix.ConnectCallback.Should().NotBeNull();
        var tcp = tcpHandler.Should().BeOfType<SocketsHttpHandler>().Subject;
        tcp.ConnectCallback.Should().BeNull();
    }

    [Fact]
    public async Task Ping_SendsVersionedGet()
    {
        // Arrange
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("OK") });
        var engine = NewEngine(handler);

        // Act
        var result = await engine.PingAsync(CancellationToken.None);

        // Assert: v1.44 закреплена в пути (решение фазы plan №2)
        result.IsSuccess.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Method.Should().Be("GET");
        request.Url.Should().Be("/v1.44/_ping");
    }

    [Fact]
    public async Task ListContainers_ParsesNamesWithLeadingSlash()
    {
        // Arrange — реальный фрагмент ответа Engine API /containers/json
        var handler = new FakeHandler(_ => Json(
            """
            [{"Id":"abc123","Names":["/pgw-shop-shard1-shard1a"],"Image":"pgworker-node:dev","State":"running",
              "Ports":[{"PrivatePort":5432,"PublicPort":15432,"Type":"tcp"}]}]
            """));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.ListContainersAsync("pgw-", all: true, CancellationToken.None);

        // Assert: ведущий "/" снят, фильтр имени в query
        result.IsSuccess.Should().BeTrue();
        var container = result.Value.Should().ContainSingle().Subject;
        container.Id.Should().Be("abc123");
        container.Names.Should().Equal("pgw-shop-shard1-shard1a");
        container.State.Should().Be("running");
        container.Image.Should().Be("pgworker-node:dev");
        handler.Requests.Single().Url.Should().Contain("/v1.44/containers/json");
        handler.Requests.Single().Url.Should().Contain("all=1");
        handler.Requests.Single().Url.Should().Contain("name");
    }

    [Fact]
    public async Task CreateContainer_SendsEnvPortsVolumeInBody()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"Id":"abc","Warnings":[]}""", HttpStatusCode.Created));
        var engine = NewEngine(handler);
        var spec = new ContainerSpec(
            "pgworker-node:dev",
            new Dictionary<string, string> { ["SCOPE"] = "shop-shard1", ["ETCD_HOSTS"] = "http://etcd:2379" },
            "pgw-shop-shard1-shard1a-data",
            "/home/postgres/pgroot",
            [new PortMap(5432, 15432), new PortMap(8008, 18008)],
            "shard1a",
            CpuCores: 2,
            MemoryBytes: 2147483648,
            Label: "shop");

        // Act
        var result = await engine.CreateContainerAsync(spec, "pgw-shop-shard1-shard1a", CancellationToken.None);

        // Assert: тело содержит env-массив, PortBindings с HostPort, volume-bind, ресурсы
        result.IsSuccess.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Method.Should().Be("POST");
        request.Url.Should().Be("/v1.44/containers/create?name=pgw-shop-shard1-shard1a");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("Image").GetString().Should().Be("pgworker-node:dev");
        body.GetProperty("Env").EnumerateArray().Select(e => e.GetString()).Should().Contain("SCOPE=shop-shard1");
        body.GetProperty("Hostname").GetString().Should().Be("shard1a");
        var hostConfig = body.GetProperty("HostConfig");
        hostConfig.GetProperty("Binds")[0].GetString().Should().Be("pgw-shop-shard1-shard1a-data:/home/postgres/pgroot");
        hostConfig.GetProperty("PortBindings").GetProperty("5432/tcp")[0].GetProperty("HostPort").GetString().Should().Be("15432");
        hostConfig.GetProperty("PortBindings").GetProperty("8008/tcp")[0].GetProperty("HostPort").GetString().Should().Be("18008");
        hostConfig.GetProperty("RestartPolicy").GetProperty("Name").GetString().Should().Be("unless-stopped");
        hostConfig.GetProperty("Resources").GetProperty("NanoCPUs").GetInt64().Should().Be(2_000_000_000);
        hostConfig.GetProperty("Resources").GetProperty("MemoryBytes").GetInt64().Should().Be(2147483648);
    }

    [Fact]
    public async Task CreateContainer_409AlreadyExists_ReturnsSuccess()
    {
        // Arrange — контейнер с таким именем уже есть (идемпотентный re-run)
        var handler = new FakeHandler(_ => Json(
            """{"message":"Conflict. The container name \"/pgw-shop-shard1-shard1a\" is already in use"}""",
            HttpStatusCode.Conflict));
        var engine = NewEngine(handler);
        var spec = new ContainerSpec("alpine", new Dictionary<string, string>(), "v", "/d", [], "h", null, null, null);

        // Act
        var result = await engine.CreateContainerAsync(spec, "pgw-shop-shard1-shard1a", CancellationToken.None);

        // Assert: 409 «already in use» — не ошибка
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task RemoveContainer_404_ReturnsSuccess()
    {
        // Arrange — контейнера уже нет (повторное удаление после сбоя)
        var handler = new FakeHandler(_ => Json("""{"message":"No such container"}""", HttpStatusCode.NotFound));
        var engine = NewEngine(handler);

        // Act
        var rm = await engine.RemoveContainerAsync("pgw-shop", force: true, CancellationToken.None);
        var volume = await engine.RemoveVolumeAsync("pgw-shop-data", CancellationToken.None);

        // Assert: 404 = успех для обоих; force=1 в query
        rm.IsSuccess.Should().BeTrue();
        volume.IsSuccess.Should().BeTrue();
        handler.Requests[0].Url.Should().Be("/v1.44/containers/pgw-shop?force=1&v=1");
        handler.Requests[1].Url.Should().Be("/v1.44/volumes/pgw-shop-data");
    }

    [Fact]
    public async Task StopContainer_SendsTimeoutQuery()
    {
        // Arrange
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.NoContent));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.StopContainerAsync("pgw-shop", 30, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        handler.Requests.Single().Url.Should().Be("/v1.44/containers/pgw-shop/stop?t=30");
    }

    [Fact]
    public async Task BusyPorts_CollectsUniquePairsFromContainers()
    {
        // Arrange — два контейнера, порты повторяются (например, без host-порта)
        var handler = new FakeHandler(req =>
        {
            var path = req.RequestUri!.PathAndQuery;
            if (path.StartsWith("/v1.44/containers/json"))
            {
                return Json(
                    """
                    [{"Id":"a","Names":["/x"],"Image":"i","State":"running",
                      "Ports":[{"PrivatePort":5432,"PublicPort":15432,"Type":"tcp"},
                               {"PrivatePort":8008,"PublicPort":18008,"Type":"tcp"}]},
                     {"Id":"b","Names":["/y"],"Image":"i","State":"exited",
                      "Ports":[{"PrivatePort":5432,"PublicPort":15432,"Type":"tcp"},
                               {"PrivatePort":5432,"Type":"tcp"}]}]
                    """);
            }

            // не swarm-менеджер: tasks/services/nodes недоступны — пустой результат
            return Json("""{"message":"not a swarm manager"}""", HttpStatusCode.ServiceUnavailable);
        });
        var engine = NewEngine(handler);

        // Act
        var result = await engine.BusyPortsAsync(CancellationToken.None);

        // Assert: уникальные (host, port) этого docker-хоста; порт без publish игнорируется
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(new (string, int)[] { ("h1", 15432), ("h1", 18008) });
    }

    [Fact]
    public async Task ListServices_ParsesNamesAndAppliesPrefixFilter()
    {
        // Arrange — фрагмент ответа GET /services (swarm): имя живёт в Spec.Name;
        // docker name-фильтр подстрочный → клиент дублирует строгий StartsWith
        var handler = new FakeHandler(_ => Json(
            """
            [{"ID":"svc-1","Spec":{"Name":"pgw-shop-shard1-shard1a"},"Endpoint":{"Ports":[]}},
             {"ID":"svc-2","Spec":{"Name":"pgw-shop2-shard1-shard1a"},"Endpoint":{"Ports":[]}}]
            """));
        var engine = NewEngine(handler);

        // Act
        var result = await engine.ListServicesAsync("pgw-shop-", CancellationToken.None);

        // Assert: только имена нужного префикса (rework №4), фильтр в query
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal("pgw-shop-shard1-shard1a");
        handler.Requests.Single().Url.Should().Contain("/v1.44/services");
        handler.Requests.Single().Url.Should().Contain("filters=");
    }

    [Fact]
    public async Task CreateService_SendsConstraintAndHostPublish()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"ID":"svc-1"}""", HttpStatusCode.Created));
        var engine = NewEngine(handler);
        var template = new ContainerSpec(
            "pgworker-node:dev", new Dictionary<string, string> { ["SCOPE"] = "shop-shard1" },
            "pgw-shop-shard1-shard1a-data", "/home/postgres/pgroot",
            [new PortMap(5432, 15432)], "shard1a", null, null, "shop");

        // Act
        var result = await engine.CreateServiceAsync(
            new ServiceSpec("pgw-shop-shard1-shard1a", template, "node-abc"), CancellationToken.None);

        // Assert: constraint node.id==, publish mode=host, volume-mount
        result.IsSuccess.Should().BeTrue();
        var body = JsonDocument.Parse(handler.Requests.Single().Body).RootElement;
        body.GetProperty("Name").GetString().Should().Be("pgw-shop-shard1-shard1a");
        var task = body.GetProperty("TaskTemplate");
        task.GetProperty("Placement").GetProperty("Constraints")[0].GetString().Should().Be("node.id==node-abc");
        var containerSpec = task.GetProperty("ContainerSpec");
        containerSpec.GetProperty("Image").GetString().Should().Be("pgworker-node:dev");
        containerSpec.GetProperty("Env")[0].GetString().Should().Be("SCOPE=shop-shard1");
        containerSpec.GetProperty("Mounts")[0].GetProperty("Type").GetString().Should().Be("volume");
        var port = body.GetProperty("Endpoint").GetProperty("Ports")[0];
        port.GetProperty("TargetPort").GetInt32().Should().Be(5432);
        port.GetProperty("PublishedPort").GetInt32().Should().Be(15432);
        port.GetProperty("PublishMode").GetString().Should().Be("host");
    }
}
