using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 3 (spec §4.5): mc — второй реальный клиент (оператор; SigV4 с
// собственной канонизацией). Ephemeral docker run в сети окружения
// (прецедент E2eBackupJobsScenarios); образ запинен. Формат вывода mc
// версионно-зависим — сверки по маркерам/счётчикам, без хрупких колонок.
// Образ mc урезан (нет grep/sed/cmp/awk): ls/stat — case-матчингом sh,
// побайтовая сверка — sha256sum; wc/tr/cut/head/cat доступны.
[Collection(OwnS3E2eCollection.Name)]
public sealed class McScenarios
{
    // mc-скрипт: alias → операции; вывод маркеров; exit-код mc = код скрипта.
    private static Task<string> McAsync(OwnS3E2eEnvironment fx, string script, CancellationToken ct)
        => fx.RunDockerAsync(
        [
            "run", "--rm", "--network", fx.NetName, "--entrypoint", "/bin/sh",
            OwnS3E2eEnvironment.McImage,
            "-c", $"mc alias set t {fx.S3InternalUrl} '{OwnS3E2eEnvironment.RootUser}' " +
                  $"'{OwnS3E2eEnvironment.RootPassword}' >/dev/null && {script}",
        ], ct);

    [Fact]
    public async Task Mc_Bucket_Object_Lifecycle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: alias → mb → cp (128 КиБ /dev/urandom внутри mc-контейнера) →
        // ls видит → cat|sha256 == источник → stat → скачивание (sha256) → rm →
        // ls пуст → rb — полный жизненный цикл оператора
        await OwnS3E2eEnvironment.RunScenarioAsync("c1", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}mc";

            // Act: один sh-сценарий; маркер несёт обе sha256
            var output = await McAsync(fx, $"""
                mc mb t/{bucket} \
                  && head -c 131072 /dev/urandom > /tmp/os3e2e.bin \
                  && SRC=$(sha256sum /tmp/os3e2e.bin | cut -d' ' -f1) \
                  && mc cp /tmp/os3e2e.bin t/{bucket}/obj.bin >/dev/null \
                  && case "$(mc ls t/{bucket})" in *obj.bin*) true;; *) false;; esac \
                  && GOT=$(mc cat t/{bucket}/obj.bin | sha256sum | cut -d' ' -f1) \
                  && case "$(mc stat t/{bucket}/obj.bin)" in *Size*) true;; *) false;; esac \
                  && case "$(mc stat t/{bucket}/obj.bin)" in *ETag*) true;; *) false;; esac \
                  && mc cp t/{bucket}/obj.bin /tmp/os3e2e-out.bin >/dev/null \
                  && [ "$(sha256sum /tmp/os3e2e-out.bin | cut -d' ' -f1)" = "$SRC" ] \
                  && mc rm t/{bucket}/obj.bin >/dev/null \
                  && [ -z "$(mc ls t/{bucket} 2>/dev/null)" ] \
                  && mc rb t/{bucket} >/dev/null \
                  && echo "OS3E2E_SHA $SRC $GOT"
                """, ct);

            // Assert: roundtrip байтов сходится (sha256 источник == cat)
            output.Should().MatchRegex(@"OS3E2E_SHA [0-9a-f]{64} [0-9a-f]{64}",
                "mc cat обязан вернуть те же байты, что cp; вывод: {0}", output);
            var shas = output.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Where(t => t.Length == 64 && t.All(char.IsAsciiHexDigit)).ToList();
            shas.Should().HaveCountGreaterThanOrEqualTo(2);
            shas[0].Should().Be(shas[1], "sha256(mc cat) == sha256(источник)");
        }, ct);
    }

    [Fact]
    public async Task Mc_Recursive_Listing()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: набор ключей с префиксами → ls --recursive перечисляет все;
        // ls <bucket>/prefix фильтрует (путь листинга цепочек бэкапов)
        await OwnS3E2eEnvironment.RunScenarioAsync("c2", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}ml";

            // Act
            var output = await McAsync(fx, $"""
                mc mb t/{bucket} \
                  && mc cp /etc/hostname t/{bucket}/demo/s1/full/base.tar >/dev/null \
                  && mc cp /etc/hostname t/{bucket}/demo/s1/wal/00001 >/dev/null \
                  && mc cp /etc/hostname t/{bucket}/demo/s2/full/base.tar >/dev/null \
                  && echo "OS3E2E_ALL $(mc ls --recursive t/{bucket} | wc -l | tr -d ' ')" \
                  && echo "OS3E2E_PREFIX $(mc ls t/{bucket}/demo/s1 | wc -l | tr -d ' ')"
                """, ct);

            // Assert: 3 объекта рекурсивно; префикс demo/s1 — 2 записи
            output.Should().Contain("OS3E2E_ALL 3",
                "рекурсивный листинг видит все ключи; вывод: {0}", output);
            output.Should().Contain("OS3E2E_PREFIX 2",
                "листинг по префиксу фильтрует; вывод: {0}", output);
        }, ct);
    }

    [Fact]
    public async Task Mc_Alias_Region_Protocol()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: mc alias set с --api S3v4 — сквозная проверка заголовочной
        // SigV4-подписи вторым независимым клиентом (собственная канонизация)
        await OwnS3E2eEnvironment.RunScenarioAsync("c3", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}mv";

            // Act: alias с явным протоколом + полный мини-цикл
            var output = await fx.RunDockerAsync(
            [
                "run", "--rm", "--network", fx.NetName, "--entrypoint", "/bin/sh",
                OwnS3E2eEnvironment.McImage,
                "-c", $"mc alias set --api S3v4 t {fx.S3InternalUrl} " +
                      $"'{OwnS3E2eEnvironment.RootUser}' '{OwnS3E2eEnvironment.RootPassword}' >/dev/null " +
                      $"&& mc mb t/{bucket} >/dev/null " +
                      $"&& mc cp /etc/hostname t/{bucket}/probe >/dev/null " +
                      $"&& mc cat t/{bucket}/probe | wc -c | tr -d ' '",
            ], ct);

            // Assert: копия читается (непустой счётчик байтов)
            int.TryParse(output.Trim(), out var bytes).Should().BeTrue("вывод mc: {0}", output);
            bytes.Should().BeGreaterThan(0, "mc (--api S3v4) проходит подпись ownS3 end-to-end");
        }, ct);
    }
}
