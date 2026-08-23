using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;

namespace PgWorker.Provisioning.Probes;

/// <summary>
/// Член Patroni-кластера из GET /cluster: role — master|replica, state —
/// running|streaming|stopped|creating (P2.2 ожидание поднятия, надзор C).
/// </summary>
public sealed record PatroniMember(string Name, string Role, string State);

/// <summary>
/// Пробы Patroni REST нод шарда (arch/14 §5 A/C): GET /cluster по
/// patroni-порту ноды с таймаутом 3 с. Ошибки сети/5xx/таймаут — Result.Failed
/// (транзиент-толерантность: процессы ждут, надзор считает ноду недоступной).
/// </summary>
public sealed class ShardProbe(HttpClient http)
{
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(3);

    // GET http://host:patroniPort/cluster — состояние всех нод шарда (P2.2).
    public async Task<Result<IReadOnlyList<PatroniMember>>> GetClusterAsync(NodeAddress node, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ProbeTimeout);
            using var response = await http.GetAsync(BuildUri(node, "cluster"), timeout.Token);
            if (!response.IsSuccessStatusCode)
                return Result<IReadOnlyList<PatroniMember>>.Failed(new ApplicationException(
                    $"Patroni {node.Host}:{node.Ports.Patroni} /cluster → HTTP {(int)response.StatusCode}"));

            await using var stream = await response.Content.ReadAsStreamAsync(timeout.Token);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: timeout.Token);
            var members = doc.RootElement.GetProperty("members").EnumerateArray()
                .Select(m => new PatroniMember(
                    m.GetProperty("name").GetString() ?? string.Empty,
                    m.GetProperty("role").GetString() ?? string.Empty,
                    m.GetProperty("state").GetString() ?? string.Empty))
                .ToList();

            return Result<IReadOnlyList<PatroniMember>>.Success(members);
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException or JsonException)
        {
            return Result<IReadOnlyList<PatroniMember>>.Failed(new ApplicationException(
                $"Patroni {node.Host}:{node.Ports.Patroni} /cluster недоступен: {e.Message}", e));
        }
    }

    // Живость конкретной ноды: GET /cluster отвечает 200 (надзор C, IsAlive).
    public async Task<bool> IsAliveAsync(NodeAddress node, CancellationToken ct)
    {
        var result = await GetClusterAsync(node, ct);
        return result.IsSuccess;
    }

    // Является ли нода текущим primary шарда: GET /primary → 200 (P11-сверка
    // мастер-ключа; HAProxy использует тот же эндпоинт, arch/14 §2.1).
    public async Task<bool> IsPrimaryAsync(NodeAddress node, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ProbeTimeout);
            using var response = await http.GetAsync(BuildUri(node, "primary"), timeout.Token);
            return response.IsSuccessStatusCode;
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return false; // транспортная недоступность — не primary
        }
    }

    private static Uri BuildUri(NodeAddress node, string path)
        => new($"http://{node.Host}:{node.Ports.Patroni}/{path}");
}
