using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using AdminPanel.Core;
using Shared.Etcd.Client;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd.Workers;

// 422: сертификат влияет на исходящие коммуникации (spec §4.3 правила 1–3).
public sealed class WorkerCertAffectsOutgoingException(string reason)
    : Exception($"сертификат влияет на коммуникации воркеров с их подчинёнными сервисами: {reason}");

// 400: не годен как серверный (правила 4–6).
public sealed class WorkerCertInvalidException(string reason) : Exception(reason);

// 409: generate при живом ключе (spec §4.1 п.2).
public sealed class WorkerCertAlreadyManagedException(string worker)
    : Exception($"сертификат {worker} уже управляется — замените (PUT api-cert) или удалите (DELETE api-cert)");

// 404: ключа нет (DELETE, spec §4.2).
public sealed class WorkerCertNotFoundException(string worker)
    : Exception($"ключ /workers/api_tls/{worker} не найден");

// 404: неизвестный воркер (worker ∈ pgworker|kafkaworker|valkeyworker, spec §3.3 п.4,
// t03: valkeyworker в перечне — arch/adminpanel/02 §9.9). Гвардится сервисом
// ДО KeyOf во ВСЕХ write-методах (и рестарт-хендлером панели): мусорный ключ
// /workers/api_tls/<foo> в etcd не пишется никогда.
public sealed class WorkerNotFoundException(string worker)
    : Exception($"неизвестный воркер {worker} (ожидался pgworker|kafkaworker|valkeyworker)");

// Итог записи: воркер + метаданные записанного серта.
public sealed record WorkerCertWriteResult(string Worker, WorkerApiCert Meta);

// Ядро грани «серты API воркеров» (spec §3.3 п.1, arch/adminpanel/02 §9.9):
// валидация «не затрагивает исходящие», генерация self-signed листа,
// запись/удаление ключа /workers/api_tls/<worker>. Панель — единственный
// писатель; запись напрямую в etcd (вторая категория после §9 provisioning).
[InjectAsSingleton]
public sealed class WorkerCertService(
    IEtcdGateway gateway,
    IOptions<EtcdOptions> etcdOptions,
    IKafkaSecretsStore kafkaSecrets,
    IOptions<WorkerApiOptions> workerApiOptions,
    TimeProvider clock)
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static string KeyOf(string worker) => $"/workers/api_tls/{worker}";

    // Хост advertise-URL (DNS или IP-строка) для SAN генерации.
    public static string? HostOf(string url)
        => Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Host.Length > 0 ? uri.Host : null;

    // ===== Валидация §4.3 =====
    public WorkerApiCert ValidateAndBuildMeta(string worker, string certPem, string keyPem)
    {
        X509Certificate2 cert;
        try
        {
            cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        }
        catch (Exception e)
        {
            throw new WorkerCertInvalidException(
                $"PEM-пара невалидна (синтаксис или ключ не соответствует серту): {e.Message}");
        }

        using var _ = cert;
        var now = clock.GetUtcNow();

        // Правило 5: NotBefore ≤ now < NotAfter (NotBefore/NotAfter — локальные
        // DateTime на host-рантаймах: приведение к UTC даёт offset 0).
        if (new DateTimeOffset(cert.NotBefore.ToUniversalTime(), TimeSpan.Zero) > now
            || new DateTimeOffset(cert.NotAfter.ToUniversalTime(), TimeSpan.Zero) <= now)
            throw new WorkerCertInvalidException(
                $"срок действия: NotBefore {cert.NotBefore:u} ≤ now < NotAfter {cert.NotAfter:u} — сейчас {now:u}");

        // SAN-коллекция — заодно для метаданных; проверку правила 6 (есть SAN)
        // делаем ПОСЛЕ правил 1–3: CA/EKU-кандидат обязан получать 422
        // «влияет на исходящие» даже без SAN (явность причины для оператора).
        var san = new List<string>();
        foreach (var ext in cert.Extensions.OfType<X509SubjectAlternativeNameExtension>())
        {
            san.AddRange(ext.EnumerateDnsNames());
            san.AddRange(ext.EnumerateIPAddresses().Select(ip => ip.ToString()));
        }

        // Правило 1: CA=TRUE — потенциальный trust anchor исходящих.
        if (cert.Extensions.OfType<X509BasicConstraintsExtension>()
            .Any(bc => bc.CertificateAuthority))
            throw new WorkerCertAffectsOutgoingException(
                "сертификат — CA (BasicConstraints CA=TRUE)");

        // Правило 2: EKU задан → serverAuth есть, clientAuth нет.
        var eku = cert.Extensions.OfType<X509EnhancedKeyUsageExtension>().FirstOrDefault();
        if (eku is not null)
        {
            var oids = eku.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value).ToHashSet();
            if (!oids.Contains(ServerAuthOid.Value) || oids.Contains(ClientAuthOid.Value))
                throw new WorkerCertAffectsOutgoingException(
                    "EKU не содержит serverAuth либо пригоден в исходящих (clientAuth)");
        }

        // Правило 3: sha256 не совпадает ни с одним известным материалом доверия/исходящих.
        var thumbprint = Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
        foreach (var known in KnownThumbprints())
        {
            if (known == thumbprint)
                throw new WorkerCertAffectsOutgoingException(
                    "sha256-отпечаток совпадает с известным материалом доверия/исходящих установки "
                    + "(per-cluster CA kafka-кластера или серты панели)");
        }

        // Правило 6: есть хотя бы один SAN (DNS или IP).
        if (san.Count == 0)
            throw new WorkerCertInvalidException("нет ни одного SAN (DNS или IP)");

        return new WorkerApiCert(
            thumbprint, cert.Subject, cert.Issuer, san,
            new DateTimeOffset(cert.NotBefore.ToUniversalTime(), TimeSpan.Zero),
            new DateTimeOffset(cert.NotAfter.ToUniversalTime(), TimeSpan.Zero),
            clock.GetUtcNow().ToUnixTimeSeconds(),
            UpdatedBy: null);
    }

    // ===== Генерация (spec §3.3 п.1) =====
    public (string CertPem, string KeyPem, WorkerApiCert Meta) Generate(
        string worker, IReadOnlyList<string> advertiseHosts)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={worker}-api", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        foreach (var host in advertiseHosts.Where(h => h.Length > 0).Distinct())
        {
            if (IPAddress.TryParse(host, out var ip))
                san.AddIpAddress(ip);
            else
                san.AddDnsName(host);
        }

        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        var now = clock.GetUtcNow();
        using var cert = request.CreateSelfSigned(now.AddMinutes(-5), now.AddDays(825));
        var certPem = cert.ExportCertificatePem();
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        return (certPem, keyPem, ValidateAndBuildMeta(worker, certPem, keyPem));
    }

    // ===== Запись (spec §4.1/§4.2) =====
    public async Task<Result<WorkerCertWriteResult>> GenerateAndPutAsync(
        string worker, IReadOnlyList<string> advertiseHosts, string updatedBy, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker" or "valkeyworker"))
            return Result<WorkerCertWriteResult>.Failed(new WorkerNotFoundException(worker)); // до KeyOf: мусорные ключи не пишем
        var (certPem, keyPem, meta) = Generate(worker, advertiseHosts);
        var value = SerializePayload(certPem, keyPem, updatedBy);
        var txn = await WithEtcdAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(KeyOf(worker))],
                [new TxnOp.Put(KeyOf(worker), value, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<WorkerCertWriteResult>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<WorkerCertWriteResult>.Failed(new WorkerCertAlreadyManagedException(worker));
        return Result<WorkerCertWriteResult>.Success(new WorkerCertWriteResult(worker, meta with { UpdatedBy = updatedBy }));
    }

    public async Task<Result<WorkerCertWriteResult>> PutAsync(
        string worker, string certPem, string keyPem, string updatedBy, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker" or "valkeyworker"))
            return Result<WorkerCertWriteResult>.Failed(new WorkerNotFoundException(worker)); // до KeyOf: мусорные ключи не пишем
        // Валидация §4.3 (400/422) — в Result (REST-контракт 03 §1), не исключением:
        // Error()-ветка модуля мапит тип ошибки на код ответа.
        WorkerApiCert meta;
        try
        {
            meta = ValidateAndBuildMeta(worker, certPem, keyPem);
        }
        catch (Exception e) when (e is WorkerCertInvalidException or WorkerCertAffectsOutgoingException)
        {
            return Result<WorkerCertWriteResult>.Failed(e);
        }

        var put = await WithEtcdAsync(endpoint => gateway.PutAsync(
            endpoint, KeyOf(worker), SerializePayload(certPem, keyPem, updatedBy), lease: null, ct));
        if (!put.IsSuccess)
            return Result<WorkerCertWriteResult>.Failed(put.Error!);
        return Result<WorkerCertWriteResult>.Success(new WorkerCertWriteResult(worker, meta with { UpdatedBy = updatedBy }));
    }

    public async Task<Result> DeleteAsync(string worker, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker" or "valkeyworker"))
            return Result.Failed(new WorkerNotFoundException(worker)); // даже если мусорный ключ кем-то записан — не трогаем
        // Осознанная гонка (TOCTOU) Range→Delete: между проверкой наличия ключа
        // и удалением конкурентный generate (txn version==0) может создать ключ —
        // DELETE удалит уже свежесозданный и вернёт 204 вместо 404. Общий
        // TxnRequest (t08) умеет delete-ветки, но атомарный txn
        // «compare version>0 → delete» остаётся неиспользованным осознанно:
        // расширение протокола удаления — изменение контракта arch/adminpanel/02,
        // вне t08. Окно ничтожно: панель — единственный
        // писатель, оператор один; потеря видна на следующем тике (статус
        // инстансов unmanaged), восстановление — повторный generate/PUT.
        var existing = await WithEtcdAsync(endpoint =>
            gateway.RangeAsync(endpoint, KeyOf(worker), ct));
        if (!existing.IsSuccess)
            return Result.Failed(existing.Error!);
        if (existing.Value.Count == 0)
            return Result.Failed(new WorkerCertNotFoundException(worker));
        return await WithEtcdAsync(endpoint => gateway.DeleteAsync(endpoint, KeyOf(worker), prefix: false, ct));
    }

    // Value ключа: {"cert_pem","key_pem","updated_unix","updated_by"} (§3.1).
    private string SerializePayload(string certPem, string keyPem, string updatedBy)
    {
        var meta = new { cert_pem = certPem, key_pem = keyPem,
            updated_unix = clock.GetUtcNow().ToUnixTimeSeconds(), updated_by = updatedBy };
        return JsonSerializer.Serialize(meta, PayloadJson);
    }

    private static readonly JsonSerializerOptions PayloadJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Известные материалы исходящих/доверия: per-cluster ca_pem kafka (бандл
    // OLD+NEW разворачиваем по кускам) + клиентский серт и ServerCa панели.
    // Терминология: «ClientCa» из spec §4.3 правило 3 трактуется как клиентский
    // СЕРТ панели — AdminPanel:Workers:WorkerTls:ClientCertPem[_PATH]; поля
    // ClientCa в конфиге НЕТ (WorkerTlsOptions: ClientCert*/ServerCa*).
    private IEnumerable<string> KnownThumbprints()
    {
        foreach (var secrets in kafkaSecrets.Current.Values)
            foreach (var pem in SplitBundle(secrets.CaPem))
                if (TryThumbprint(pem, out var thumb))
                    yield return thumb;

        var tls = workerApiOptions.Value.WorkerTls;
        foreach (var pem in new[]
        {
            tls.ClientCertPem ?? ReadFile(tls.ClientCertPath),
            tls.ServerCaPem ?? ReadFile(tls.ServerCaPath),
        })
            if (pem is not null && TryThumbprint(pem, out var thumb))
                yield return thumb;
    }

    // Бандл ca_pem в окне ротации — конкатенация PEM (arch/15 §2.1): каждый кусок.
    private static IEnumerable<string> SplitBundle(string pem)
        => pem.Split("-----END CERTIFICATE-----", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => p.Contains("BEGIN CERTIFICATE"))
            .Select(p => p + "\n-----END CERTIFICATE-----");

    private static bool TryThumbprint(string pem, out string thumbprint)
    {
        try
        {
            using var cert = X509Certificate2.CreateFromPem(pem);
            thumbprint = Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
            return true;
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        {
            thumbprint = "";
            return false;
        }
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();

    // Не-generic перегрузка для вызовов Task<Result>.
    private async Task<Result> WithEtcdAsync(Func<string, Task<Result>> call)
    {
        Result? last = null;
        foreach (var endpoint in etcdOptions.Value.Endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last ?? Result.Failed(new EtcdUnreachableException("AdminPanel:Etcd:Endpoints не заданы"));
    }

    // Failover по endpoint'ам панели: первый успешный ответ выигрывает.
    private async Task<Result<T>> WithEtcdAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in etcdOptions.Value.Endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last ?? Result<T>.Failed(new EtcdUnreachableException("AdminPanel:Etcd:Endpoints не заданы"));
    }
}
