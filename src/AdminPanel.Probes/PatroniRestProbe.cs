using System.Diagnostics;
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки (spec §4.6).
public sealed record PatroniMemberResult(HaMemberProbe Enrichment, ProbeResult Result);

// Проба члена HA-скопа: GET http://<host>:8008/cluster (arch/02 §6.1).
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

    // Порт Patroni REST — стандарт :8008 (arch/02 §6.1; PG-порт member'а не используется).
    private const int RestPort = 8008;

    public async Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct)
    {
        var url = $"http://{HostMapResolver.Resolve(options.Value.HostMap, member.Host, RestPort)}/cluster";
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
}
