using OwnS3.IntegrationTests.E2e;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

// mc TLS-сценарий (t42, AC2): клиент mc (Go) доверяет per-install CA через
// SSL_CERT_FILE — полный цикл mb→cp→cat→rm→rb по https. mb/rb — root-креды
// alias'а (матрица 05 §3: бакетные операции — admin/root).
[Collection(OwnS3E2eCollection.Name)]
public class McTlsScenarios
{
    // docker run с CA: ro bind-mount ca.pem в /tmp + SSL_CERT_FILE; скрипт —
    // alias + полный цикл (образец McScenarios.McAsync, но с доверием CA).
    private static Task<string> McTlsAsync(OwnS3TlsE2eEnvironment fx, string script, CancellationToken ct)
        => fx.RunDockerAsync(
        [
            "run", "--rm", "--network", fx.NetName,
            "-v", $"{fx.CaPemPath}:/tmp/s3-ca.pem:ro",
            "-e", "SSL_CERT_FILE=/tmp/s3-ca.pem",
            "--entrypoint", "/bin/sh", OwnS3E2eEnvironment.McImage,
            "-c", $"mc alias set t {fx.S3InternalUrl} '{OwnS3E2eEnvironment.RootUser}' " +
                  $"'{OwnS3E2eEnvironment.RootPassword}' >/dev/null && {script}",
        ], ct);

    // AAA: mc доверяет CA через SSL_CERT_FILE (Go crypto/x509) — mb→cp→cat→rm→rb
    // по https; roundtrip побайтово сходится (sha256 источника == полученного).
    [Fact]
    public async Task Mc_SslCertFile_ПолныйЦикл()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3TlsE2eEnvironment.RunScenarioAsync("t2", async fx =>
        {
            var bucket = $"os3e2e{fx.RunTag}tmc";
            var output = await McTlsAsync(fx, $"""
                mc mb t/{bucket} \
                  && head -c 65536 /dev/urandom > /tmp/b.bin \
                  && SRC=$(sha256sum /tmp/b.bin | cut -d ' ' -f1) \
                  && mc cp /tmp/b.bin t/{bucket}/o.bin >/dev/null \
                  && GOT=$(mc cat t/{bucket}/o.bin | sha256sum | cut -d ' ' -f1) \
                  && mc rm t/{bucket}/o.bin >/dev/null \
                  && mc rb t/{bucket} >/dev/null \
                  && echo "OS3E2E_SHA $SRC $GOT"
                """, ct);
            var shas = output.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Where(t => t.Length == 64 && t.All(char.IsAsciiHexDigit)).ToList();
            shas.Should().HaveCountGreaterThanOrEqualTo(2);
            shas[0].Should().Be(shas[1], "roundtrip mc по https сходится побайтово");
        }, ct);
    }
}
