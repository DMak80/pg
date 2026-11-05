using System.Text.Json;
using PgWorker.Etcd.Parsing;

namespace PgWorker.Backups.Supervisor;

/// <summary>Чистые решения сверок S3↔etcd (arch/19 §4): без I/O,
/// юнит-тестируемы. Мгновенно удаляется только мусор, который восстановление
/// гарантированно не выберет: объект full/&lt;id&gt;.tar в S3 БЕЗ etcd-ключа
/// (выбора кандидатов restore не имеет); всё, что может иметь DR-ценность, —
/// реестр сирот + TTL (BackupOrphanSweeper).</summary>
public static class SupervisorPlanner
{
    /// <summary>S3-id полных (объект full/&lt;id&gt;.tar) без etcd-ключа (мусор
    /// живого шарда). Ключ с любым state (вкл. FAILED/DELETING) — владелец есть:
    /// судьбу решают гигиена FAILED и ретенция, не супервизор.
    /// Порядок входа сохранён.</summary>
    public static IReadOnlyList<string> SelectUnownedFulls(
        IReadOnlyList<string> s3FullIds, IReadOnlyList<FullBackupState> etcdFulls)
    {
        var owned = etcdFulls.Select(f => f.Id).ToHashSet(StringComparer.Ordinal);
        return s3FullIds.Where(id => !owned.Contains(id)).ToList();
    }

    /// <summary>Возрастной гейт мусора (arch/19 §4 «Гейты мусора»): самый старый
    /// объект unowned-префикса достиг порога GarbageMinAgeSec → мусор дозрел
    /// (заливка in-flight молода — снимок тика мог отстать от планировщика
    /// этого же тика). minAgeSec &lt;= 0 — гейт снят (мгновенное удаление).</summary>
    public static bool IsGarbageMatured(
        DateTimeOffset oldestObject, long minAgeSec, DateTimeOffset now)
        => minAgeSec <= 0 || now - oldestObject >= TimeSpan.FromSeconds(minAgeSec);

    /// <summary>id полного из ключа multipart-сессии шарда
    /// (`&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt;.tar`); чужой/нестандартный ключ — null
    /// (трогать нечего: сессия не нашего шарда либо гипотетический иной ключ).</summary>
    public static string? FullIdFromMultipartKey(string key, string cluster, string shard)
    {
        var prefix = $"{cluster}/{shard}/full/";
        if (!key.StartsWith(prefix, StringComparison.Ordinal)
            || !key.EndsWith(".tar", StringComparison.Ordinal))
            return null;
        var id = key[prefix.Length..^".tar".Length];
        return id.Length > 0 && !id.Contains('/', StringComparison.Ordinal) ? id : null;
    }

    /// <summary>Живой ли полный по свежему etcd-статусу (гигиена multipart,
    /// e-supervisor §1): активное состояние (PLANNED/RUNNING/UPLOADING) с
    /// возрастом операции в пределах JobFullTimeoutSec. Статус не разобран —
    /// null (неизвестно; вызывающий не абортит: безопасность важнее места).</summary>
    public static bool? IsLiveFull(string statusJson, long nowUnix, int jobFullTimeoutSec)
    {
        try
        {
            using var doc = JsonDocument.Parse(statusJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("state", out var stateElement)
                || stateElement.GetString() is not { } state)
                return null;
            if (state is not ("PLANNED" or "RUNNING" or "UPLOADING"))
                return false;
            if (!root.TryGetProperty("started_unix", out var startedElement)
                || !startedElement.TryGetInt64(out var started))
                return true; // возраст не доказан — считаем живым
            return nowUnix - started <= jobFullTimeoutSec;
        }
        catch (JsonException)
        {
            return null;
        }
        catch (InvalidOperationException)
        {
            return null; // поле-нечисло/нестрока — статус не разобран
        }
    }
}
