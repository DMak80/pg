using Shared.Core.DI;

namespace AdminPanel.Probes.S3;

// [Config]-POCO S3-грани «Хранилище бэкапов» (t08, spec §4.1, arch/01 §6):
// симметрия PgWorker:Backups:S3 (arch/19 §7). Пустой Endpoint — грань выключена
// (тик не стартует, API configured=false, алерт молчит) — отдельного Enabled-
// флага нет. Креды — только env поверх appsettings (AdminPanel__Backups__S3__*).
[Config("AdminPanel:Backups")]
public class BackupsS3Options
{
    public S3Options S3 { get; set; } = new();

    // Период инвентарь-тика (полный list-v2 bucket).
    public int IntervalSec { get; set; } = 60;

    // HTTP-таймаут list/health-вызовов.
    public int TimeoutSec { get; set; } = 5;

    // Грань включена ⟺ задан Endpoint (пустой endpoint и есть выключение).
    public bool IsConfigured => !string.IsNullOrWhiteSpace(S3.Endpoint);

    /// <summary>Fail-fast старта (образец PgWorker:Backups): Endpoint задан при
    /// пустых Bucket/AccessKey/SecretKey или IntervalSec/TimeoutSec &lt;= 0 — бросок
    /// с именем поля. Выключенная грань (пустой Endpoint) не валидируется.</summary>
    public void EnsureValid()
    {
        if (!IsConfigured)
            return;
        if (string.IsNullOrWhiteSpace(S3.Bucket))
            throw new InvalidOperationException(
                "AdminPanel:Backups:S3:Bucket пуст при заданном Endpoint");
        if (string.IsNullOrWhiteSpace(S3.AccessKey))
            throw new InvalidOperationException(
                "AdminPanel:Backups:S3:AccessKey пуст при заданном Endpoint");
        if (string.IsNullOrWhiteSpace(S3.SecretKey))
            throw new InvalidOperationException(
                "AdminPanel:Backups:S3:SecretKey пуст при заданном Endpoint");
        if (IntervalSec <= 0)
            throw new InvalidOperationException(
                "AdminPanel:Backups:IntervalSec <= 0 при заданном Endpoint");
        if (TimeoutSec <= 0)
            throw new InvalidOperationException(
                "AdminPanel:Backups:TimeoutSec <= 0 при заданном Endpoint");
    }

    public sealed class S3Options
    {
        // Пусто — грань выключена (переключатель грани, t08).
        public string Endpoint { get; set; } = "";

        // t36 (02 §2.5): запятая-список целей инвентаря (ноды пула); пусто →
        // [Endpoint] (одиночный endpoint — валидный частный случай). Перебор —
        // с первого доступного (быстрый отказ per-попытки, TimeoutSec).
        public string Endpoints { get; set; } = "";

        // Разрешённый список целей инвентаря (02 §2.5): Endpoints (запятая)
        // ?? [Endpoint]; пустые элементы и хвостовые слэши отсечены, дубли схлопнуты.
        public string[] TargetList()
        {
            var targets = Endpoints
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(e => e.TrimEnd('/'))
                .Where(e => e.Length > 0)
                .Distinct()
                .ToArray();
            if (targets.Length > 0)
                return targets;
            var endpoint = Endpoint.TrimEnd('/');
            return endpoint.Length == 0 ? [] : [endpoint];
        }

        // MinIO не требует; облако — регион (AuthenticationRegion).
        public string? Region { get; set; }

        public string Bucket { get; set; } = "";
        public string AccessKey { get; set; } = "";
        public string SecretKey { get; set; } = "";

        // ForcePathStyle: MinIO и облако одним клиентом (arch/19 §5).
        public bool PathStyle { get; set; } = true;
    }
}
