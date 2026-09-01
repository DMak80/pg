using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;

namespace PgWorker.Provisioning.Probes;

/// <summary>
/// Член Patroni-кластера из GET /cluster: role — master|replica, state —
/// running|streaming|stopped|creating (P2.2 ожидание поднятия, надзор C).
/// </summary>
public sealed record PatroniMember(string Name, string Role, string State);

/// <summary>Идентичность Patroni-ноды из GET /patroni (spec §3.7 Д1б): scope
/// глобально уникален (&lt;C&gt;-&lt;X&gt;), name — имя ноды; пары достаточно для вывода
/// «наша/чужая» (у /cluster поля scope нет — имена нод шаблонные между кластерами).</summary>
public sealed record NodeIdentity(string Name, string Scope);

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

    // Идентификация ноды (Д1б, spec §3.7): GET /patroni несёт scope+name — в живом
    // формате (Patroni 4.x, стенд) они ВО ВЛОЖЕННОМ объекте "patroni"; fallback —
    // корневые поля. Транспорт/битый JSON/не-2xx/отсутствующие поля → Success(null) —
    // «не опознана» (чужой ответ по коллизионному порту не является успехом ожидания).
    public async Task<Result<NodeIdentity?>> IdentifyAsync(NodeAddress node, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ProbeTimeout);
            using var response = await http.GetAsync(BuildUri(node, "patroni"), timeout.Token);
            if (!response.IsSuccessStatusCode)
                return Result<NodeIdentity?>.Success(null);

            await using var stream = await response.Content.ReadAsStreamAsync(timeout.Token);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: timeout.Token);
            var root = doc.RootElement;
            var holder = root.TryGetProperty("patroni", out var nested) && nested.ValueKind == JsonValueKind.Object
                ? nested
                : root; // живой формат: {"state","role","patroni":{"version","scope","name"}}; fallback — корень
            var name = holder.TryGetProperty("name", out var n) ? n.GetString() : null;
            var scope = holder.TryGetProperty("scope", out var s) ? s.GetString() : null;
            return Result<NodeIdentity?>.Success(
                name is { Length: > 0 } && scope is { Length: > 0 } ? new NodeIdentity(name, scope) : null);
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException or JsonException)
        {
            return Result<NodeIdentity?>.Success(null);
        }
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

    // Graceful-переезд лидерства (пересоздание «мягко», режим soft): POST
    // /switchover на Patroni-порт ЛИДЕРА с телом {"leader": имя} — Patroni
    // переведёт лидерство на лучшую реплику (sync-standby) без паузы записи.
    // Таймаут больше пробы: switchover выполняется в рамках запроса и занимает
    // пару loop_wait-циклов Patroni.
    public async Task<Result> SwitchoverAsync(NodeAddress leader, string leaderName, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(15));
            using var content = new StringContent(
                $$"""{"leader":"{{leaderName}}"}""", System.Text.Encoding.UTF8, "application/json");
            using var response = await http.PostAsync(BuildUri(leader, "switchover"), content, timeout.Token);
            if (!response.IsSuccessStatusCode)
                return Result.Failed(new ApplicationException(
                    $"Patroni {leader.Host}:{leader.Ports.Patroni} /switchover → HTTP {(int)response.StatusCode}"));

            return Result.Success();
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return Result.Failed(new ApplicationException(
                $"Patroni {leader.Host}:{leader.Ports.Patroni} /switchover недоступен: {e.Message}", e));
        }
    }

    private static Uri BuildUri(NodeAddress node, string path)
        => new($"http://{node.Host}:{node.Ports.Patroni}/{path}");
}
