using OwnS3.IntegrationTests.E2e;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

// Негативные TLS-кейсы (t42, AC2/AC3): грань валидирует цепочку (клиент без
// доверия — TLS-отказ); HTTP на HTTPS-порт — разрыв, не тихий downgrade;
// fail-fast Program-ветки — docker-кейс (контейнер с одним OWNS3_TLS_CERT
// умирает с exit ≠ 0 и Critical-диагностикой; вне WAF — см. план Task 1).
[Collection(OwnS3E2eCollection.Name)]
public class TlsNegativeScenarios
{
    // AAA (1): клиент БЕЗ доверия CA получает TLS-отказ — грань валидирует цепочку
    [Fact]
    public async Task КлиентБезДоверия_Отказ()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3TlsE2eEnvironment.RunScenarioAsync("t3", async fx =>
        {
            using var http = new HttpClient(); // системный магазин — тестовой CA там нет
            var act = () => http.GetAsync($"{fx.S3BaseUrl}/healthz", ct);
            await act.Should().ThrowAsync<HttpRequestException>(); // TLS-рукопожатие сорвано
        }, ct);
    }

    // AAA (2): обычный HTTP-запрос на HTTPS-порт — отказ, не тихий downgrade
    [Fact]
    public async Task HttpНаHttpsПорт_Отказ()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3TlsE2eEnvironment.RunScenarioAsync("t4", async fx =>
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            var httpUrl = fx.S3BaseUrl.Replace("https://", "http://", StringComparison.Ordinal);
            var act = () => http.GetAsync($"{httpUrl}/healthz", ct);
            await act.Should().ThrowAsync<Exception>(); // разрыв соединения TLS-листенером
        }, ct);
    }

    // AAA (3): fail-fast Program-ветки (AC3, вне WAF — см. план Task 1): контейнер
    // с одним OWNS3_TLS_CERT (нет ключа) умирает ненулевым exit-кодом, в логах —
    // Critical-диагностика с OWNS3_TLS; живой сервис не поднимается «молча на HTTP»
    [Fact]
    public async Task HalfPair_КонтейнерУмирает_СДиагностикой()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3TlsE2eEnvironment.RunScenarioAsync("t5", async fx =>
        {
            var name = $"os3-tlsfail-{fx.RunId}";
            try
            {
                // Act: отдельный контейнер (не сервис окружения) — образ уже собран окружением
                await fx.RunDockerAsync(
                [
                    "run", "--detach", "--name", name,
                    "-e", $"OWNS3_ROOT_USER={OwnS3E2eEnvironment.RootUser}",
                    "-e", $"OWNS3_ROOT_PASSWORD={OwnS3E2eEnvironment.RootPassword}",
                    "-e", "OWNS3_TLS_CERT=garbage-without-key", // один из пары
                    OwnS3E2eImage.Tag,
                ], ct);

                // поллинг exited ≤ 30 с (шаг 1 с) — fail-fast мгновенный
                string state = "";
                for (var i = 0; i < 30; i++)
                {
                    state = await fx.RunDockerAsync(
                        ["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name], ct);
                    if (state.StartsWith("exited", StringComparison.Ordinal))
                        break;
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                }

                // Assert: exited с ненулевым кодом; в логах — Critical-диагностика
                state.Should().StartWith("exited ", "невалидный материал — отказ старта");
                var exitCode = state.Split(' ').ElementAt(1);
                exitCode.Should().NotBe("0", "fail-fast = exit ≠ 0");
                var logs = await fx.RunDockerAsync(["logs", name], ct);
                logs.Should().Contain("OWNS3_TLS").And.Contain("TLS", "диагностика указывает на TLS-материал");
            }
            finally
            {
                // Чистка — ЛУЧШЕЕ УСИЛИЕ (образец TryDockerAsync эталона): RunDockerAsync
                // бросает исключение при ненулевом exit docker (вкл. «No such container»,
                // если упал сам docker run) — исключение из finally маскировало бы
                // первопричину сценария; контейнер own-only по runId.
                try { await fx.RunDockerAsync(["rm", "-f", name], ct); }
                catch { /* уже нет / docker недоступен — чистка лучшим усилием */ }
            }
        }, ct);
    }
}
