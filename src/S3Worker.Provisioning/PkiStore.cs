using System.Security.Cryptography;
using System.Text.Json;
using OwnS3.Protocol.PeerAuth;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning;

/// <summary>
/// PKI репликации per-cluster в etcd (arch/23 §2/§6, spec §3.2): ленивый выпуск
/// CA кластера, сертификата оркестратора и сертификатов инстансов — txn
/// put-if-absent (проигрыш → re-read, первый записавший — истина, arch/17 S5).
/// Сертификаты бессрочные (expiresDays ≤ 0). Чтение — с проверкой целостности
/// (ключ ↔ сертификат, Verify против CA); битый/несогласованный материал —
/// Failed с диагностикой, без исключений наружу. Криптомеханика — библиотека
/// peer-PKI ownS3 (переиспользование; копирование запрещено — spec §3.2).
/// </summary>
public sealed class PkiStore(IEtcdGateway gateway, string[] endpoints, TimeProvider clock)
{
    // Формат pki/ca (arch/23 §2.1 — смешанный регистр полей канона, ручная
    // сериализация): {"name":"<C>","publicKeyPem":…,"private_key_pem":…}.
    internal static string SerializeCa(ClusterCaMaterial material)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("name", material.Ca.Name);
            writer.WriteString("publicKeyPem", material.Ca.PublicKeyPem);
            writer.WriteString("private_key_pem", material.CaPrivateKeyPem);
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    // Формат pki/orchestrator и nodes/<I>/pki (arch/23 §2.1): сертификат —
    // экранированная JSON-строка (PeerCertificate JSON), ключ — snake_case.
    internal static string SerializeIssued(IssuedPkiMaterial material)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("certificate", material.CertificateJson);
            writer.WriteString("private_key_pem", material.PrivateKeyPem);
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    /// <summary>
    /// Ленивый выпуск пары CA+оркестратор (всегда согласованная: сертификат
    /// подписан тем же CA): оба есть → идемпотентный возврат; обоих нет →
    /// CreateCa + IssueServer + txn put-if-absent ОБЕИХ, проигрыш → re-read;
    /// ca есть / orchestrator нет → добор СУЩЕСТВУЮЩИМ CA (put-if-absent);
    /// orchestrator есть / CA нет → Failed (приватный ключ CA невосстановим —
    /// перевыпуск разорвал бы trust-домен: инстансы держат прежний
    /// OWNS3_REPLICATION_TRUST_CA). Битости материалов — тот же Failed-путь.
    /// </summary>
    public async Task<Result<(ClusterCaMaterial Ca, IssuedPkiMaterial Orchestrator)>> EnsureClusterPkiAsync(
        string cluster, CancellationToken ct)
    {
        var ca = await TryReadCaAsync(cluster, ct);
        if (!ca.IsSuccess)
            return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Failed(ca.Error!);
        var orch = await TryReadOrchestratorPkiAsync(cluster, ct);
        if (!orch.IsSuccess)
            return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Failed(orch.Error!);

        var existingCa = ca.Value;
        var existingOrch = orch.Value;

        // Согласованная пара — идемпотентный возврат (повторный вызов — тот же материал).
        if (existingCa is not null && existingOrch is not null)
            return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Success((existingCa, existingOrch));

        // CA есть, оркестратора нет → добор существующим CA (идемпотентный).
        if (existingCa is not null)
        {
            var issued = await IssueIfAbsentAsync(
                cluster, "orchestrator", S3ProcessCommon.OrchestratorPkiKey(cluster), existingCa, ct);
            return issued.IsSuccess
                ? Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Success((existingCa, issued.Value))
                : Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Failed(issued.Error!);
        }

        // Оркестратор есть, CA нет → восстановить нечем (см. сводку).
        if (existingOrch is not null)
            return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Failed(new ApplicationException(
                $"pki {cluster}: приватный ключ CA невосстановим, перевыпуск CA разорвал бы " +
                "trust-домен (сертификат оркестратора подписан неизвестным CA; " +
                "инстансы держат прежний OWNS3_REPLICATION_TRUST_CA)"));

        // Обоих нет: выпуск пары + txn put-if-absent обеих; проигрыш → re-read
        // (первый записавший — истина) и повторный проход по тем же веткам.
        var at = clock.GetUtcNow();
        var (caJson, caPem) = PeerCertificateAuthority.CreateCa(cluster);
        // CreateCa отдаёт trust-anchor JSON — для IssueServer нужен PEM публичного ключа.
        string caPublicKeyPem;
        using (var caDoc = JsonDocument.Parse(caJson))
            caPublicKeyPem = caDoc.RootElement.GetProperty("publicKeyPem").GetString()
                ?? throw new InvalidOperationException("CreateCa: пустой publicKeyPem");
        var caAnchor = new PeerTrustAnchor(cluster, caPublicKeyPem);
        var (orchCert, orchKeyPem) = PeerCertificateAuthority.IssueServer(
            "orchestrator", caAnchor, caPem, at, expiresDays: 0);
        var caMaterial = new ClusterCaMaterial(caAnchor, caPem);
        var orchMaterial = new IssuedPkiMaterial(orchCert, orchCert.ToJson(), orchKeyPem);

        var caKey = S3ProcessCommon.ClusterPkiCaKey(cluster);
        var orchKey = S3ProcessCommon.OrchestratorPkiKey(cluster);
        var txn = await TxnWithFailoverAsync(
            TxnRequest.Of(
                [TxnCompare.NotExists(caKey), TxnCompare.NotExists(orchKey)],
                [new TxnOp.Put(caKey, SerializeCa(caMaterial), null),
                 new TxnOp.Put(orchKey, SerializeIssued(orchMaterial), null)]), ct);
        if (!txn.IsSuccess)
            return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return await EnsureClusterPkiAsync(cluster, ct); // re-read — см. ветки выше

        return Result<(ClusterCaMaterial, IssuedPkiMaterial)>.Success((caMaterial, orchMaterial));
    }

    /// <summary>Ленивый выпуск материала инстанса (CA передан вызывающим —
    /// обязан существовать): txn put-if-absent, проигрыш → re-read. Бессрочный
    /// сертификат (expiresDays ≤ 0) — ключи переживают provision/переезд (§3.2).</summary>
    public async Task<Result<IssuedPkiMaterial>> EnsureNodePkiAsync(
        string cluster, string node, ClusterCaMaterial ca, CancellationToken ct)
        => await IssueIfAbsentAsync(cluster, node, S3ProcessCommon.NodePkiKey(cluster, node), ca, ct);

    /// <summary>Чтение CA кластера: null — ключа нет; битый/несогласованный
    /// материал (ключ не соответствует публичному, PEM не читается) — Failed.</summary>
    public async Task<Result<ClusterCaMaterial?>> TryReadCaAsync(string cluster, CancellationToken ct)
    {
        var read = await S3ProcessCommon.GetWithFailoverAsync(
            gateway, endpoints, S3ProcessCommon.ClusterPkiCaKey(cluster), ct);
        if (!read.IsSuccess)
            return Result<ClusterCaMaterial?>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<ClusterCaMaterial?>.Success(null);

        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            var name = root.GetProperty("name").GetString()
                ?? throw new InvalidOperationException("поле name пусто");
            var publicKeyPem = root.GetProperty("publicKeyPem").GetString()
                ?? throw new InvalidOperationException("поле publicKeyPem пусто");
            var privateKeyPem = root.GetProperty("private_key_pem").GetString()
                ?? throw new InvalidOperationException("поле private_key_pem пусто");

            // Целостность: приватный ключ соответствует публичному (по образцу
            // PeerIdentityLoader — нормализация \r\n); из него можно выпускать.
            using var key = ECDsa.Create();
            key.ImportFromPem(privateKeyPem);
            var exported = key.ExportSubjectPublicKeyInfoPem();
            if (!string.Equals(exported.Replace("\r\n", "\n"),
                    publicKeyPem.Replace("\r\n", "\n"), StringComparison.Ordinal))
                throw new InvalidOperationException(
                    "приватный ключ CA не соответствует publicKeyPem (материал несогласован)");

            return Result<ClusterCaMaterial?>.Success(
                new ClusterCaMaterial(new PeerTrustAnchor(name, publicKeyPem), privateKeyPem));
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException
            or InvalidOperationException or FormatException or CryptographicException)
        {
            return Result<ClusterCaMaterial?>.Failed(new ApplicationException(
                $"pki {S3ProcessCommon.ClusterPkiCaKey(cluster)}: битый/несогласованный материал CA — {ex.Message}"));
        }
    }

    /// <summary>Чтение материала пира (инстанса/оркестратора): null — ключа нет;
    /// битый JSON, несоответствие ключа сертификату, провал Verify против CA
    /// своего кластера, истёкший сертификат — Failed.</summary>
    public Task<Result<IssuedPkiMaterial?>> TryReadNodePkiAsync(
        string cluster, string node, CancellationToken ct)
        => TryReadIssuedAsync(S3ProcessCommon.NodePkiKey(cluster, node), cluster, ct);

    /// <summary>Чтение сертификата оркестратора (ключ pki/orchestrator —
    /// НЕ nodes/orchestrator/pki; см. arch/23 §2).</summary>
    public Task<Result<IssuedPkiMaterial?>> TryReadOrchestratorPkiAsync(
        string cluster, CancellationToken ct)
        => TryReadIssuedAsync(S3ProcessCommon.OrchestratorPkiKey(cluster), cluster, ct);

    private async Task<Result<IssuedPkiMaterial?>> TryReadIssuedAsync(string key, string cluster, CancellationToken ct)
    {
        var read = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!read.IsSuccess)
            return Result<IssuedPkiMaterial?>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<IssuedPkiMaterial?>.Success(null);

        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            var certificateJson = root.GetProperty("certificate").GetString()
                ?? throw new InvalidOperationException("поле certificate пусто");
            var privateKeyPem = root.GetProperty("private_key_pem").GetString()
                ?? throw new InvalidOperationException("поле private_key_pem пусто");

            var cert = PeerCertificate.FromJson(certificateJson);
            if (cert.IsExpired(clock.GetUtcNow()))
                throw new InvalidOperationException($"сертификат {cert.Name} истёк");

            // Целостность: приватный ключ соответствует публичному ключу сертификата.
            using var ecdsa = ECDsa.Create();
            ecdsa.ImportFromPem(privateKeyPem);
            var exported = ecdsa.ExportSubjectPublicKeyInfoPem();
            if (!string.Equals(exported.Replace("\r\n", "\n"),
                    cert.PublicKeyPem.Replace("\r\n", "\n"), StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"приватный ключ не соответствует сертификату {cert.Name}");

            // Верификация против CA СВОЕГО кластера (изоляция trust-доменов).
            // CA отсутствует — материал не виним (EnsureClusterPki различит ветку
            // «orchestrator есть / CA нет»); читающие живой кластер всегда имеют CA.
            var ca = await TryReadCaAsync(cluster, ct);
            if (!ca.IsSuccess)
                return Result<IssuedPkiMaterial?>.Failed(ca.Error!); // etcd/битость CA
            if (ca.Value is { } caMaterial
                && !PeerCertificateAuthority.Verify(cert, caMaterial.Ca, clock.GetUtcNow()))
                throw new InvalidOperationException(
                    $"сертификат {cert.Name} не проходит верификацию CA кластера {cluster}");

            return Result<IssuedPkiMaterial?>.Success(new IssuedPkiMaterial(cert, certificateJson, privateKeyPem));
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException
            or InvalidOperationException or FormatException or CryptographicException)
        {
            return Result<IssuedPkiMaterial?>.Failed(new ApplicationException(
                $"pki {key}: битый/несогласованный материал — {ex.Message}"));
        }
    }

    // Общий путь «выпуск if-absent»: ключа нет → IssueServer + txn put-if-absent;
    // проигрыш → re-read чтением (первый записавший — истина); битость прочитанного — Failed.
    private async Task<Result<IssuedPkiMaterial>> IssueIfAbsentAsync(
        string cluster, string name, string key, ClusterCaMaterial ca, CancellationToken ct)
    {
        var read = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!read.IsSuccess)
            return Result<IssuedPkiMaterial>.Failed(read.Error!);

        var (cert, keyPem) = PeerCertificateAuthority.IssueServer(
            name, ca.Ca, ca.CaPrivateKeyPem, clock.GetUtcNow(), expiresDays: 0);
        var material = new IssuedPkiMaterial(cert, cert.ToJson(), keyPem);

        var txn = await TxnWithFailoverAsync(
            TxnRequest.Of([TxnCompare.NotExists(key)],
                [new TxnOp.Put(key, SerializeIssued(material), null)]), ct);
        if (!txn.IsSuccess)
            return Result<IssuedPkiMaterial>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
        {
            // Выиграл конкурент — его материал истина; верифицируем чтением.
            var reread = await TryReadIssuedAsync(key, cluster, ct);
            return reread.IsSuccess && reread.Value is { } winner
                ? Result<IssuedPkiMaterial>.Success(winner)
                : Result<IssuedPkiMaterial>.Failed(reread.IsSuccess
                    ? new ApplicationException($"pki {key}: проиграна txn put-if-absent, материал исчез")
                    : reread.Error!);
        }

        return Result<IssuedPkiMaterial>.Success(material);
    }

    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }
}
