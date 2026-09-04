using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.Provisioning.Processes;

// Прогресс /kafkaworker/regens/<C> (t06, arch/15 §4): live-ключ — живёт
// только во время операции (put при старте первого пересоздания, del по
// сходимости; отсутствие ключа = операции нет — фантомы запрещены).
internal sealed record KafkaRegenProgressJson(
    [property: JsonPropertyName("brokers_total")] int BrokersTotal,
    [property: JsonPropertyName("brokers_remaining")] int BrokersRemaining,
    [property: JsonPropertyName("current_broker")] string? CurrentBroker,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("last_error")] string? LastError = null);

/// <summary>
/// NodeRegenerator (arch/16 §5 J, t06): автоконверге лимитов контейнера к
/// декларации brokers/&lt;b&gt;/resources. Триггер — ТОЛЬКО расхождение cpu/mem
/// (inspect vs декларация — NodeRegenPlanner, арифметика записи; env
/// пересобирается попутно, disk не сверяется). Один брокер за тик:
/// Remove(том жив) → Ensure(лимиты из декларации) → state=PROVISIONING;
/// возврат в RUNNING — AddBrokerProcess (F) следующих тиков; следующий
/// брокер — только когда все ноды RUNNING. Прогресс-ключ ставится/держится
/// ТОЛЬКО при живой операции (расхождения есть ИЛИ хвост: ключ жив, а
/// последний пересозданный ещё не RUNNING) — чужие недоведённые ноды
/// (add-broker F, надзор C) фантомного прогресса не создают (spec §4.1;
/// ревью Фазы 4 раунд 2, замечание 1). Guard'ы: живая ротация/reassignment —
/// передержка; ошибка инспекта — ошибка тика без действий (порт слепоты
/// надзора). Без снапшотов P12 (spec §5.4). Вызывается только держателем
/// клэйма &lt;C&gt;.
/// </summary>
public sealed class NodeRegenerator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    ProvisioningOptions options,
    BrokerCertificateCache certificates)
{
    private const string Op = "regen";

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"regen {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // J0a: живая заявка ротации (ключ фаз A–B) ИЛИ journal-фаза ротации
        // (фаза C живёт после del заявки; надзор мог перезаписать журнал —
        // тогда guard просто не сработает, пересечение идемпотентно).
        var rotation = await GetAsync($"/kafkaworker/rotations/{cluster}", ct);
        if (!rotation.IsSuccess)
            return Fail(cluster, rotation.Error!, "reading-rotation");
        var rotateJournal = await journal.ReadAsync(cluster, ct);
        if (!rotateJournal.IsSuccess)
            return Fail(cluster, rotateJournal.Error!, "reading-journal");
        if (rotation.Value is not null
            || rotateJournal.Value is { Op: "rotate" } r && r.Phase != "done")
            return await journal.WriteAsync(cluster, Op, "waiting-rotation", claims.InstanceId, null, ct);

        // J0b: живой reassignment — пересоздания не смешиваются с переездами реплик.
        var reassignment = await GetAsync($"/kafkaworker/reassignments/{cluster}", ct);
        if (!reassignment.IsSuccess)
            return Fail(cluster, reassignment.Error!, "reading-reassignment");
        if (reassignment.Value is not null)
            return await journal.WriteAsync(cluster, Op, "waiting-reassign", claims.InstanceId, null, ct);

        // J1: кандидаты — стабильные ноды с декларацией ресурсов (TO_REMOVE/
        // REMOVING/PROVISIONING/NOT_INITIALIZED/UNREACHABLE — чужие процессы).
        var candidates = snap.Brokers
            .Where(b => b.State == "RUNNING" && b.Resources is not null)
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();

        // J2: сверка лимитов (ошибка инспекта → ошибка тика; контейнера нет →
        // пропуск — надзор восстановит).
        var diverged = new List<KafkaBrokerDecl>();
        foreach (var broker in candidates)
        {
            var limits = await driver.NodeResourcesAsync(cluster, broker.Name, ct);
            if (!limits.IsSuccess)
                return Fail(cluster, limits.Error!, "inspecting-limits");
            if (limits.Value is null)
                continue;
            if (NodeRegenPlanner.NeedsRegen(broker.Resources!, limits.Value))
                diverged.Add(broker);
        }

        // Прогресс-ключ читаем ДО ветвлений: «операция жива» = расхождения
        // есть ИЛИ ключ уже стоит (хвост — последний пересозданный брокер
        // ещё не вернулся в RUNNING). Ключ без операции не создаём (§4.1).
        var progress = await GetAsync(RegenKey(cluster), ct);
        if (!progress.IsSuccess)
            return Fail(cluster, progress.Error!, "reading-progress");
        var operationLive = diverged.Count > 0 || progress.Value is not null;
        if (!operationLive)
            return Result.Success(); // операции нет и не было — no-op: чужие
                                     // недоведённые ноды прогресс не рисуют

        // Недоведённые ноды кластера (не все RUNNING) — доводит F; при живой
        // операции их счёт входит в remaining (операциональная оценка, §4.3).
        var pending = diverged.Count
            + snap.Brokers.Count(b => b.State != "RUNNING");

        // J3: сходимость — прогресс-ключ гасим (operationLive ⇒ ключ мог
        // стоять; если не стоит — просто успех без записи).
        if (pending == 0)
        {
            if (progress.Value is not null)
            {
                var deleted = await DeleteAsync(RegenKey(cluster), ct);
                if (!deleted.IsSuccess)
                    return Fail(cluster, deleted.Error!, "dropping-progress");
                return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId, null, ct);
            }

            return Result.Success();
        }

        // J4: операция жива, но в кластере есть недоведённые ноды — ждём
        // возврата (без новых пересозданий); remaining пересчитываем фактом.
        if (snap.Brokers.Any(b => b.State != "RUNNING"))
        {
            var current = snap.Brokers
                .FirstOrDefault(b => b.State != "RUNNING")?.Name;
            var written = await WriteProgressAsync(cluster, pending, current, null, ct);
            if (!written.IsSuccess)
                return written;
            return await journal.WriteAsync(cluster, Op, "waiting-return", claims.InstanceId, null, ct);
        }

        // J5: пересоздание первой расходящейся ноды (одна за тик;
        // diverged гарантированно непуст — иначе мы выше в no-op/J3/J4).
        var target = diverged[0];
        var marked = await journal.WriteAsync(
            cluster, Op, $"regenerating:{target.Name}", claims.InstanceId, null, ct);
        if (!marked.IsSuccess)
            return marked;

        if (snap.AppUser is null || snap.AppPassword is null)
            return Fail(cluster, new ApplicationException(
                $"regen {cluster}: нет app-кредов (ensure не выполнен)"), "no-creds");

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Fail(cluster, addresses.Error!, "reading-portalloc");
        if (!addresses.Value.TryGetValue(target.Name, out var addr))
            return Fail(cluster, new ApplicationException(
                $"regen {cluster}: broker {target.Name} не закреплён в portalloc"), "reading-portalloc");

        var removed = await driver.RemoveNodeAsync(cluster, target.Name, removeVolume: false, ct);
        if (!removed.IsSuccess)
            return Fail(cluster, removed.Error!, "removing-container");

        // Env пересобирается из текущей декларации (новые server-props
        // применяются тем же рестартом — детерминизм NodeEnvBuilder, R3).
        var env = BrokerEnvBuilder.Build(snap, target.Name, addr, [snap.AppPassword!], [snap.AdminPassword!], options, certificates);
        var ensured = await driver.EnsureNodeAsync(new KafkaNodeSpec(
            cluster, target.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
            target.Resources!.Cpu,
            target.Resources.MemGi * 1024L * 1024 * 1024), ct);
        if (!ensured.IsSuccess)
            return Fail(cluster, ensured.Error!, "ensuring-container");

        var state = await PutAsync(BrokerStateKey(cluster, target.Name), "PROVISIONING", ct);
        if (!state.IsSuccess)
            return Fail(cluster, state.Error!, "mark-provisioning");

        var progressWritten = await WriteProgressAsync(cluster, pending, target.Name, null, ct);
        if (!progressWritten.IsSuccess)
            return progressWritten;

        return Result.Success(); // следующий брокер — после RUNNING этого (J4)
    }

    // total — монотонный пик (PUT ресурсов посреди операции растит total),
    // remaining — текущий недоведённый счёт: UI видит «2 из 3».
    private async Task<Result> WriteProgressAsync(
        string cluster, int pending, string? currentBroker, string? lastError, CancellationToken ct)
    {
        var key = RegenKey(cluster);
        var existing = await GetAsync(key, ct);
        if (!existing.IsSuccess)
            return existing;
        var total = pending;
        if (existing.Value is { } kv)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                // ValueKind-чек: валидный JSON с не-числом в поле — тоже мусор
                // (битый ключ перезаписываем фактом, spec §5.2 Note).
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && doc.RootElement.TryGetProperty("brokers_total", out var prev)
                    && prev.ValueKind == JsonValueKind.Number
                    && prev.GetInt32() > total)
                    total = prev.GetInt32();
            }
            catch (JsonException)
            {
                // Битый прогресс — мусор (arch/15 §6): перезаписываем фактом.
            }
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        return await PutAsync(key, JsonSerializer.Serialize(new KafkaRegenProgressJson(
            total, pending, currentBroker, now, claims.InstanceId, lastError)), ct);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/kafkaworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        var addresses = new Dictionary<string, NodeAddress>();
        if (result.Value is { } kv)
        {
            using var doc = JsonDocument.Parse(kv.Value);
            foreach (var node in doc.RootElement.EnumerateObject())
                addresses[node.Name] = new NodeAddress(
                    node.Value.GetProperty("host").GetString()!,
                    node.Value.GetProperty("client").GetInt32());
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private static string RegenKey(string cluster) => $"/kafkaworker/regens/{cluster}";

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
