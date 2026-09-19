using System.Security.Cryptography.X509Certificates;
using Shared.Core;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;
using ValkeyWorker.Docker.Engine;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// Ensure TLS-материала ноды (t06, arch/21 §2/V3): named volume
/// vwk-&lt;C&gt;-tls с node.crt/node.key/ca.pem. Валидность = ca.pem совпадает с
/// текущим CA кластера, node.crt подписан этим CA, SAN покрывает advertised-
/// хост, NotAfter в будущем → переиспользование; иначе — перевыпуск
/// (IssueNodeCertificate) и запись tar поверх. PING по TLS в процессах —
/// финальный критерий (spec §5). Вызывается ДО EnsureNodeAsync (файлы сертов
/// обязаны быть в volume к старту контейнера).
/// </summary>
public sealed class NodeTlsProvisioner(
    IClusterDriver driver,
    TimeProvider? clock = null)
{
    public const string MountPath = "/tls";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    public async Task<Result> EnsureNodeTlsAsync(
        string cluster, string node, string host, string advertisedHost,
        string caPem, string caKeyPem, CancellationToken ct)
    {
        var volume = await driver.EnsureTlsVolumeAsync(cluster, host, ct);
        if (!volume.IsSuccess)
            return volume;

        var existing = await driver.GetTlsArchiveAsync(cluster, host, ct);
        if (!existing.IsSuccess)
            return existing;
        if (existing.Value is { } tar && IsValidTar(tar, advertisedHost, caPem, _clock))
            return Result.Success(); // валидный серт уже в volume — переиспользование

        var (certPem, keyPem) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, node, advertisedHost);
        var entries = new[]
        {
            new TarArchive.Entry("node.crt", 0b1_1010_0100, System.Text.Encoding.UTF8.GetBytes(certPem)), // 0o644
            new TarArchive.Entry("node.key", 0b1_1000_0000, System.Text.Encoding.UTF8.GetBytes(keyPem)),  // 0o600
            new TarArchive.Entry("ca.pem", 0b1_1010_0100, System.Text.Encoding.UTF8.GetBytes(caPem)),
        };
        return await driver.PutTlsArchiveAsync(cluster, host, TarArchive.Build(entries), ct);
    }

    // Валидность факта (идемпотентность по факту, spec §2.4): ca.pem == текущему
    // CA, серт подписан им, SAN покрывает advertised, срок жив.
    internal static bool IsValidTar(byte[] tar, string advertisedHost, string caPem, TimeProvider clock)
    {
        try
        {
            var files = TarArchive.Read(tar);
            if (!files.TryGetValue("ca.pem", out var caBytes)
                || !files.TryGetValue("node.crt", out var certBytes)
                || !files.TryGetValue("node.key", out _))
                return false;
            if (System.Text.Encoding.UTF8.GetString(caBytes).Trim() != caPem.Trim())
                return false; // чужой/старый CA — перевыпуск
            if (!ValkeyPki.TryParseCertificate(PemOf(certBytes), out var cert) || cert is null)
                return false;
            using (cert)
            {
                if (!Shared.Tls.TlsChain.ValidateChain(cert, ParseCa(caPem)))
                    return false;
                if (cert.NotAfter < clock.GetUtcNow())
                    return false;
                var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();
                if (san is null)
                    return false;
                if (System.Net.IPAddress.TryParse(advertisedHost, out var ip))
                    return san.EnumerateIPAddresses().Contains(ip);
                return san.EnumerateDnsNames()
                    .Contains(advertisedHost, StringComparer.OrdinalIgnoreCase);
            }
        }
        catch (Exception e) when (e is ArgumentException or FormatException or ApplicationException)
        {
            return false; // битый tar/PEM — перевыпуск
        }
    }

    private static string PemOf(byte[] bytes) => System.Text.Encoding.UTF8.GetString(bytes);

    private static X509Certificate2 ParseCa(string caPem)
        => ValkeyPki.TryParseCertificate(caPem, out var ca) && ca is not null
            ? ca
            : throw new ArgumentException("ca_pem: невалидный PEM");
}
