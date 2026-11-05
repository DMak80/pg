using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Журнал фаз состава (arch/etcdworker/00-etcd-contract.md): ключ /etcdworker/work — БЕЗ
/// суффикса (домен один); формат {"op","phase","updated_unix","instance",
/// "last_error"?} — null-поля опускаются. Failover-put по endpoints.
/// Событие PhaseWritten — подписка метрик WorkerMetricsInstrumentation
/// (кластер-параметр — константа "etcd").</summary>
public sealed class EtcdWorkJournal(IEtcdGateway gateway, string[] endpoints)
{
    public const string Key = "/etcdworker/work";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public sealed record PhaseEntry(string Op, string Phase);

    public event Action<PhaseEntry>? PhaseWritten;

    public async Task<Result> WriteAsync(
        string op, string phase, string instance, string? lastError, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(
            new Payload(op, phase, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), instance, lastError), Json);
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, Key, payload, lease: null, ct);
            if (put.IsSuccess)
            {
                try { PhaseWritten?.Invoke(new PhaseEntry(op, phase)); }
                catch { /* наблюдатель не влияет на журнал */ }
                return Result.Success();
            }
            last = put;
        }

        return Result.Failed(last.Error ?? new Shared.Etcd.Client.EtcdUnreachableException("etcd endpoints are not set"));
    }

    private sealed record Payload(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("phase")] string Phase,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("last_error")] string? LastError);
}
