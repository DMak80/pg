using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки (spec §4.6).
public sealed record PatroniMemberResult(HaMemberProbe Enrichment, ProbeResult Result);

// Проба члена HA-скопа: GET https://<host>:8008/cluster (arch/02 §6.1 — TLS
// per-install CA + basic-auth только unsafe-эндпоинтам; GET /cluster — без
// кредов, t22).
public interface IPatroniRestProbe
{
    Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct);
}

// Запись member'а отсутствует в ответе /cluster — ошибка пробы (spec §3.4).
public sealed class PatroniProbeException(string message) : Exception(message);

// Реализация: typed HttpClient "patroni" (таймаут из ProbesOptions — ModuleExtensions,
// паттерн EtcdGateway t03); адрес host:8008 прогоняется через HostMap (§3.6);
// из ответа берётся запись name == member.Name (§3.4); User-Agent — §3.22.
[InjectAsSingleton(typeof(IPatroniRestProbe))]
public sealed class PatroniRestProbe(
    HttpClient httpClient,
    IOptions<ProbesOptions> options,
    TimeProvider time) : IPatroniRestProbe
{
    public const string HttpClientName = "patroni";

    // Порт Patroni REST — стандарт :8008 (arch/02 §6.1); реальный порт ноды
    // несёт portalloc PgWorker (HostMember.Port после ServiceParser) — на стенде
    // это опубликованный 18xxx, в проде — выделенный REST-порт ноды.
    private const int RestPort = 8008;

    public async Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct)
    {
        var url = $"https://{HostMapResolver.Resolve(options.Value.HostMap, member.Host, member.Port ?? RestPort)}/cluster";
        var started = Stopwatch.GetTimestamp();
        var at = time.GetUtcNow();
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            // Идентификация панели в access-логах Patroni/эмуляторов (spec §3.22).
            request.Headers.UserAgent.TryParseAdd("AdminPanel");
            using var response = await httpClient.SendAsync(request, ct);
            response.EnsureSuccessStatusCode();
            var json = await response.Content.ReadAsStringAsync(ct);
            var latency = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            var entry = PatroniClusterParser.Parse(json).FirstOrDefault(m => m.Name == member.Name)
                ?? throw new PatroniProbeException(
                    $"member {member.Name} не найден в ответе /cluster scope {scope.Scope}");

            return new PatroniMemberResult(
                new HaMemberProbe(entry.Role, entry.State, entry.Timeline, entry.LagBytes, at, null),
                new ProbeResult($"{scope.Scope}/{member.Name}", "patroni", true, latency, null, at));
        }
        catch (Exception e)
        {
            // Любой отказ (транспорт/HTTP/JSON/отсутствие записи) — ошибка пробы этого
            // члена, не тика: DCS-часть HA остаётся (arch/01 §8, spec §3.5).
            return new PatroniMemberResult(
                new HaMemberProbe(null, null, null, null, at, e.Message),
                new ProbeResult(
                    $"{scope.Scope}/{member.Name}", "patroni", false,
                    Stopwatch.GetElapsedTime(started).TotalMilliseconds, e.Message, at));
        }
    }

    // TLS-handler пробы (t22, arch/adminpanel/02 §6.1): верификация цепочки к
    // per-install ServerCA из WorkerTls (ServerCaPem ?? ServerCaPath — env
    // WORKERS_PANEL_TLS_SERVER_CA[_PATH]; ОДИН корень доверия на установку),
    // hostname НЕ сверяется (P17-канон: SAN серта ноды — имена сетей, панель
    // ходит по отображённым адресам). Чтение CA ленивое (первый хендшейк),
    // загруженный сертификат кешируется на жизнь handler'а. Клиентских кредов
    // нет: GET /cluster вне зоны authentication Patroni.
    public static HttpMessageHandler BuildTlsHandler(AdminPanel.Etcd.Workers.WorkerTlsOptions tls)
    {
        var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) };
        X509Certificate2? ca = null;
        handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, _, _) =>
        {
            var cert2 = certificate as X509Certificate2
                ?? (certificate is null ? null : new X509Certificate2(certificate));
            if (cert2 is null)
                return false;
            ca ??= LoadCa(tls);
            return ca is not null && Shared.Tls.TlsChain.ValidateChain(cert2, ca);
        };
        return handler;
    }

    private static X509Certificate2? LoadCa(AdminPanel.Etcd.Workers.WorkerTlsOptions tls)
    {
        try
        {
            var pem = tls.ServerCaPem ?? Shared.Tls.TlsMaterial.ReadPemFile(tls.ServerCaPath);
            return pem is null ? null : Shared.Tls.TlsMaterial.LoadPem(pem);
        }
        catch (Exception)
        {
            return null; // пакет TLS не читается — панель не доверяет ничему
        }
    }
}
