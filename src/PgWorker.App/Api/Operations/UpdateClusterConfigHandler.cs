using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using Shared.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Тело PUT /api/clusters/{c}/config (t06, 02 §9.10): поле обязательное —
// отсутствие/не-bool → 400 (выключение strict «по умолчанию» недопустимо).
// camelCase-биндинг Minimal API: SynchronousModeStrict → "synchronousModeStrict".
public sealed record UpdateClusterConfigRequest(bool? SynchronousModeStrict);

// Мутация synchronous_mode_strict через API воркера (t06, arch/14 §1.1):
// RMW-txn по mod_revision прочитанного config (образец — KafkaWorker §10.2).
// Гварды: кластер Active; включение strict требует replicas ≥ 2 на всех шардах.
// Успех — 204 (без тела); применение к Patroni — конвергенция DCS воркера.
public sealed class UpdateClusterConfigHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result> HandleAsync(string cluster, UpdateClusterConfigRequest request, CancellationToken ct)
    {
        // 1) Тело: поле обязательно.
        if (request.SynchronousModeStrict is not { } strict)
            return Result.Failed(new UpdateClusterConfigValidationException(
                [new ValidationError("synchronousModeStrict", "поле обязательно: boolean (true|false)")]));

        // 2) Имя каноническое + чтение config с mod_revision (напрямую у etcd).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result.Failed(new ClusterNotFoundException(cluster));
        var read = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/config", ct));
        if (!read.IsSuccess)
            return Result.Failed(read.Error!); // 503
        var kv = read.Value.FirstOrDefault(k => k.Key == $"/clusters/{cluster}/config");
        if (kv is null)
            return Result.Failed(new ClusterNotFoundException(cluster)); // 404
        string? rawState;
        bool current;
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            rawState = root.TryGetProperty("state", out var state)
                && state.ValueKind == JsonValueKind.String ? state.GetString() : null;
            current = root.TryGetProperty("synchronous_mode_strict", out var strictField)
                && strictField.ValueKind is JsonValueKind.True or JsonValueKind.False
                    ? strictField.GetBoolean() : true; // отсутствие/не-bool = true
        }
        catch (JsonException)
        {
            return Result.Failed(new InvalidClusterConfigException(cluster)); // 503
        }

        // 3) Гвард: только Active (409).
        if (rawState is not null)
            return Result.Failed(new ClusterNotActiveException(cluster, rawState));

        // 4) Валидация включения: strict=true → replicas ≥ 2 на всех шардах.
        //    Сбой чтения шардов — 503 (НЕ «валидация не прошла»).
        if (strict)
        {
            var check = await HasUndersizedShardAsync(cluster, ct);
            if (!check.IsSuccess)
                return Result.Failed(check.Error!); // 503
            if (check.Value is { } undersized)
                return Result.Failed(new UpdateClusterConfigValidationException(
                [
                    new ValidationError("syncStrict",
                        $"включение strict требует replicas ≥ 2 на всех шардах (шард {undersized} с меньшим числом реплик; без sync-standby запись блокируется)"),
                ]));
        }

        // 5) Идемпотентность: значение совпадает → 204 без записи.
        if (current == strict)
            return Result.Success();

        // 6) RMW-txn: compare mod_revision + put пересобранного config
        //    (поле обновлено, прочие поля перенесены без изменений).
        //    Kv.ModRevision — ulong, ModRevisionEqual(long) — каст обязателен.
        var updated = RewriteStrict(kv.Value, strict);
        var txn = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(kv.Key, (long)kv.ModRevision)],
                [new TxnOp.Put(kv.Key, updated, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result.Failed(new ClusterConcurrentWriteException(kv.Key)); // 503, retry
        return Result.Success();
    }

    // Шард с replicas < 2 (имя, напр. "shard3"); null — все ≥ 2; Failed — 503.
    private async Task<Result<string?>> HasUndersizedShardAsync(string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/shards/", ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        foreach (var k in range.Value)
        {
            var segments = k.Key.Split('/');
            if (segments.Length == 6 && segments[3] == "shards" && segments[5] == "replicas"
                && int.TryParse(k.Value.Trim(), out var replicas) && replicas < 2)
                return Result<string?>.Success(segments[4]);
        }

        return Result<string?>.Success(null);
    }

    // Пересборка config-JSON: synchronous_mode_strict заменён/добавлен, прочие
    // свойства перенесены как есть (сырые JsonElement — формат значений 1:1).
    internal static string RewriteStrict(string raw, bool strict)
    {
        using var doc = JsonDocument.Parse(raw);
        var properties = doc.RootElement.EnumerateObject()
            .Where(p => p.Name != "synchronous_mode_strict")
            .Select(p => JsonSerializer.Serialize(p.Name) + ":" + p.Value.GetRawText())
            .ToList();
        properties.Add($"\"synchronous_mode_strict\":{(strict ? "true" : "false")}");
        return "{" + string.Join(",", properties) + "}";
    }
}
