using System.Net;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// PullImageAsync — поллинг идемпотентных итераций (arch/14 §6, образец
// StopContainerAsync): пер-попыточный клиентский таймаут итерации меньше
// длительности загрузки — клиентский обрыв НЕ неудача (демон продолжает,
// слои кэшированы), повтор POST /images/create в бюджете порога сноса;
// не-2xx (образ не найден) — DockerHttpException с путём немедленно.
public class DockerEnginePullTests
{
    // Async-фейк (образец DockerEngineStopTests): задержки ОТМЕНЯЕМЫЕ —
    // HttpClient.Timeout «мягкий», реальный handler отмену уважает.
    private sealed class SlowHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.Method.Method, request.RequestUri!.PathAndQuery));
            return await responder(request, ct);
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode code = HttpStatusCode.OK) => new()
    {
        StatusCode = code,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static DockerEngine Engine(SlowHandler handler, TimeSpan clientTimeout)
        => new(new HttpClient(handler)
        {
            BaseAddress = new Uri("http://docker"),
            Timeout = clientTimeout,
        }, "h1");

    // AAA: клиентский таймаут первого POST /images/create (демон ещё тянет) →
    //      повторный идемпотентный POST отвечает 200 — образ на хосте
    [Fact]
    public async Task Pull_ClientTimeoutThenSuccess_Success()
    {
        // Arrange — первый pull молчит 5 с (клиентский таймаут 0.5 с), второй — 200
        var pullCalls = 0;
        var handler = new SlowHandler(async (req, ct) =>
        {
            if (req.RequestUri!.PathAndQuery.Contains("/images/create")
                && Interlocked.Increment(ref pullCalls) == 1)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                return Json("{}"); // не будет прочитан — клиент оборвёт запрос
            }

            return Json("{}");
        });

        // Act
        await Engine(handler, TimeSpan.FromMilliseconds(500))
            .PullImageAsync("192.168.0.1:5000/alpine:3.20", CancellationToken.None);

        // Assert: итерация 1 оборвана таймаутом, итерация 2 довела до факта
        pullCalls.Should().Be(2, "the timed-out pull is retried idempotently");
        handler.Requests.Should().OnlyContain(r => r.Url.Contains("/images/create"));
    }

    // AAA: бюджет исчерпан без факта — ApplicationException с путём
    //      /images/create (диагностика pull-fallback §7.2 — по пути отличают
    //      pull-фейл от голого 404 /containers/create); тестовое окно бюджета —
    //      internal-перегрузка (прецедент DockerEngineFactory.Create)
    [Fact]
    public async Task Pull_BudgetExhausted_ThrowsWithPath()
    {
        // Arrange — каждый POST молчит 5 с при клиентском таймауте 0.5 с;
        // бюджет 1.5 с → три итерации, отказ не тянет порог сноса
        var handler = new SlowHandler(async (_, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
            return Json("{}");
        });

        // Act
        var act = async () => await Engine(handler, TimeSpan.FromMilliseconds(500))
            .PullImageAsync("reg:5000/some:tag", CancellationToken.None, TimeSpan.FromSeconds(1.5));

        // Assert — не вечный цикл; путь в тексте, /containers/create не упоминается
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var ex = (await act.Should().ThrowAsync<ApplicationException>()).Which;
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5), "the polling budget bounds the loop");
        ex.Message.Should().Contain("/images/create");
        ex.Message.Should().Contain("reg:5000/some:tag");
        ex.Message.Should().NotContain("/containers/create");
    }

    // AAA: не-2xx (образ не найден/registry недоступен) — DockerHttpException
    //      с путём немедленно, без ретраев (повтор бессмыслен — ответ демона
    //      об образе окончателен; транзиентность обеспечивает тик процесса)
    [Fact]
    public async Task Pull_DaemonError_ThrowsDockerHttpImmediately()
    {
        // Arrange — pull отвечает 500 (pull access denied / no such repo)
        var handler = new SlowHandler((_, _) => Task.FromResult(Json(
            """{"message":"pull access denied"}""", HttpStatusCode.InternalServerError)));

        // Act
        var act = async () => await Engine(handler, TimeSpan.FromSeconds(2))
            .PullImageAsync("reg:5000/no-such:missing", CancellationToken.None);

        // Assert — один вызов, путь и тело в сообщении
        var ex = (await act.Should().ThrowAsync<DockerHttpException>()).Which;
        handler.Requests.Should().ContainSingle();
        ex.Message.Should().Contain("/images/create");
        ex.Body.Should().Contain("pull access denied");
    }

    // AAA: канонный бюджет цикла — порог сноса (arch/14 §6); защита от
    //      невнимательной правки (образец DockerEngineFactoryCallTimeoutTests)
    [Fact]
    public void PullBudget_CanonValue_30Seconds()
    {
        // Arrange / Act / Assert: порог сноса — единый для долгих фаз движка.
        DockerEngine.PullBudget.Should().Be(TimeSpan.FromSeconds(30));
    }
}
