using System.Text;
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Удаление кластера через API воркера: перевод config.state в TO_REMOVE
// (arch/02 §9.4). Порт панельного DeleteClusterCommandHandler (он уже читал
// config напрямую у etcd); источник endpoints — свой список с failover.
// Ключи кластера не удаляются — очистка у воркера (deprovisioning)/runbook.
// Без txn: config уже существует (уникальность не участвует), конкурентные
// удаления сходятся к одному значению. Без ретраев — повтор = новый DELETE.
public sealed class DeleteClusterHandler(IEtcdGateway gateway, string[] endpoints)
{
    public const string ToRemoveState = "TO_REMOVE"; // канон config.state (arch/02 §9.4)

    public async Task<Result> HandleAsync(string name, CancellationToken ct)
    {
        // 1) Неканоническое имя (§9.3) создать не могли — сразу 404, без etcd.
        if (!CreateClusterLimits.NamePattern().IsMatch(name))
            return Result.Failed(new ClusterNotFoundException(name));

        // 2) Config напрямую: отсутствие ключа = 404.
        var configKey = $"/clusters/{name}/config";
        var read = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, configKey, ct));
        if (!read.IsSuccess)
            return Result.Failed(read.Error!);
        var config = read.Value.FirstOrDefault(kv => kv.Key == configKey);
        if (config is null)
            return Result.Failed(new ClusterNotFoundException(name));

        string rewritten;
        try
        {
            // 3) Уже TO_REMOVE — идемпотентный успех без записи (§9.4).
            if (ReadState(config.Value) == ToRemoveState)
                return Result.Success();

            // 4) Перезапись канонического набора полей с state=TO_REMOVE (§9.4).
            rewritten = WithToRemoveState(config.Value);
        }
        catch (JsonException)
        {
            return Result.Failed(new InvalidClusterConfigException(name));
        }

        var put = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, configKey, rewritten, null, ct));
        return put.IsSuccess ? Result.Success() : Result.Failed(put.Error!);
    }

    // state без проверки формата: битый JSON ловит вызывающий (JsonException).
    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var state)
            && state.ValueKind == JsonValueKind.String
            ? state.GetString()
            : null;
    }

    // Канонический config §2.1: buckets/dbname/created_unix сохраняются,
    // state заменяется на TO_REMOVE. created_unix отсутствует у старых init —
    // не добавляем. Прочие/будущие поля config не переносятся (§9.4).
    private static string WithToRemoveState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        if (!root.TryGetProperty("buckets", out var buckets) || buckets.ValueKind != JsonValueKind.Number
            || !root.TryGetProperty("dbname", out var dbname) || dbname.ValueKind != JsonValueKind.String)
            throw new JsonException("config без обязательных полей buckets/dbname");
        long? created = root.TryGetProperty("created_unix", out var createdEl)
            && createdEl.ValueKind == JsonValueKind.Number
            ? createdEl.GetInt64()
            : null;

        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("buckets", buckets.GetInt64());
            writer.WriteString("dbname", dbname.GetString());
            if (created is not null)
                writer.WriteNumber("created_unix", created.Value);
            writer.WriteString("state", ToRemoveState);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }
}
