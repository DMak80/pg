using Shared.Core.DI;

namespace AdminPanel.Probes.S3;

// [Config]-POCO S3-грани «Хранилище бэкапов»: креды и цели — из снапшота
// S3-домена (первый Active-пул с endpoints и secrets, ключ panel;
// arch/adminpanel/02 §12.4); в конфиге — только Bucket
// (env AdminPanel__Backups__S3__Bucket) и интервалы.
[Config("AdminPanel:Backups")]
public class BackupsS3Options
{
    public S3Options S3 { get; set; } = new();

    // Период инвентарь-тика (полный list-v2 bucket).
    public int IntervalSec { get; set; } = 60;

    // HTTP-таймаут list/health-вызовов.
    public int TimeoutSec { get; set; } = 5;

    public sealed class S3Options
    {
        // ownS3 не требует; облако — регион (AuthenticationRegion).
        public string? Region { get; set; }

        public string Bucket { get; set; } = "";

        // ForcePathStyle: ownS3 и облако одним клиентом (arch/19 §5).
        public bool PathStyle { get; set; } = true;
    }
}
