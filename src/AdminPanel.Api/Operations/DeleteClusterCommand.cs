using System.Text;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда удаления кластера: перевод config.state в TO_REMOVE (arch/02 §9.4).
// Ключи кластера не удаляются — очистка у внешнего оркестратора/runbook.
public sealed record DeleteClusterCommand(string Name) : ICommand<ClusterDeletedDto>;

// Результат DELETE /api/clusters/{name} (arch/03 §1.2); эндпоинт отвечает 204.
public sealed record ClusterDeletedDto(string Name, string State);

// Кластера нет (config-ключ отсутствует) или имя неканоническое — 404 (arch/03 §1.2).
public sealed class ClusterNotFoundException(string name)
    : Exception($"кластер {name} не найден (config-ключ отсутствует)");

// Config-ключ есть, но не парсится/без обязательных полей — 503 (arch/03 §1.2).
public sealed class InvalidClusterConfigException(string name)
    : Exception($"config кластера {name} битый или без обязательных полей buckets/dbname");

// Читает config напрямую у etcd и перезаписывает state=TO_REMOVE (arch/02 §9.4).
// Без txn: config уже существует (уникальность не участвует), конкурентные
// удаления сходятся к одному значению. Без ретраев — повтор = новый DELETE.
[InjectAsScoped]
public sealed class DeleteClusterCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<DeleteClusterCommand, ClusterDeletedDto>
{
    public const string ToRemoveState = "TO_REMOVE"; // канон config.state (arch/02 §9.4)

    public async ValueTask<Result<ClusterDeletedDto>> Handle(DeleteClusterCommand command, CancellationToken ct)
    {
        var name = command.Name;

        // 1) Неканоническое имя (§9.3) панель создать не могла — сразу 404, без etcd.
        if (!CreateClusterLimits.NamePattern().IsMatch(name))
            return Result<ClusterDeletedDto>.Failed(new ClusterNotFoundException(name));

        // 2) Активный endpoint из снапшота — как при создании (§9.2).
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ClusterDeletedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую (снапшот отстаёт до тика): отсутствие ключа = 404.
        var configKey = $"/clusters/{name}/config";
        var read = await gateway.RangeAsync(endpoint, configKey, ct);
        if (!read.IsSuccess)
            return Result<ClusterDeletedDto>.Failed(read.Error!);
        var config = read.Value.FirstOrDefault(kv => kv.Key == configKey);
        if (config is null)
            return Result<ClusterDeletedDto>.Failed(new ClusterNotFoundException(name));

        string rewritten;
        try
        {
            // 4) Уже TO_REMOVE — идемпотентный успех без записи (§9.4).
            if (ReadState(config.Value) == ToRemoveState)
                return Result<ClusterDeletedDto>.Success(new ClusterDeletedDto(name, ToRemoveState));

            // 5) Перезапись канонического набора полей с state=TO_REMOVE (§9.4).
            rewritten = WithToRemoveState(config.Value);
        }
        catch (JsonException)
        {
            return Result<ClusterDeletedDto>.Failed(new InvalidClusterConfigException(name));
        }

        var put = await gateway.PutAsync(endpoint, configKey, rewritten, ct);
        if (!put.IsSuccess)
            return Result<ClusterDeletedDto>.Failed(put.Error!);
        return Result<ClusterDeletedDto>.Success(new ClusterDeletedDto(name, ToRemoveState));
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
