using System.Reflection;
using Shared.Core.DI;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace AdminPanel.IntegrationTests;

// Сериализация сборок Program-хостов панели в одном тестовом процессе (t08):
// статический кеш сборок attribute-DI (ServiceCollectionExtensions._assemblies —
// процессный dedup) пропускает уже отсканированные сборки, и ВТОРОЙ хост терял
// бы все [Config]/[InjectAs*]-регистрации (см. комментарий AuthWebFactory —
// «не допускает второй хост»). Лечим причину, а не следствие: перед КАЖДЫМ
// build кеш очищается, сами build'ы строго сериализованы замком (гонка двух
// сборок хостов исключена). Обе фабрики сборки (AuthWebFactory и
// BackupsWebFactory) обязаны строить хост только через этот хелпер.
internal static class PanelHostBuilder
{
    private static readonly object Sync = new();

    private static readonly FieldInfo AssembliesField =
        typeof(ServiceCollectionExtensions).GetField(
            "_assemblies", BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly FieldInfo BehavioursField =
        typeof(ServiceCollectionExtensions).GetField(
            "_behaviours", BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>Полный build хоста под замком с очисткой кеша сканирования:
    /// после возврата фабрика готова к CreateClient/Services.</summary>
    public static void BuildExclusive(WebApplicationFactory<Program> factory)
    {
        lock (Sync)
        {
            // Кеш сборок: без очистки повторный скан пропускает все сборки.
            ((HashSet<Assembly>)AssembliesField.GetValue(null)!).Clear();
            // Поведения прошлых build'ов привязаны к УЖЕ СОБРАННЫМ (read-only)
            // коллекциям — повторная регистрация в них кидает InvalidOperationException;
            // текущий хост добавит свои поведения при исполнении Program (под замком).
            ((HashSet<DiTypeBehaviour>)BehavioursField.GetValue(null)!).Clear();
            _ = factory.Services; // форсируем полную сборку хоста под замком
        }
    }
}

// Фабрика грани «Хранилище бэкапов» (t08, план Task 9, Решение 7): РЕАЛЬНЫЙ
// SnapshotStore, SnapshotRefresher и MinioInventoryLoop из композиции Program
// (никаких TestSnapshotStore/стабов гейтвея), но hosted-сервисы сняты — тики
// двигает тест вручную (loop.RunOnceAsync → refresher.RefreshOnceAsync;
// прецедент spec §3.10/§3.17 — InspectionEtcdApiTests). Конфиг — UseSetting:
// простые листья AdminPanel:Backups:S3:* биндятся тем же классом, что env
// AdminPanel__Backups__S3__* (Решение 9 — тест Options_Bind_FromConfiguration
// закрепляет байндинг).
public sealed class BackupsWebFactory : WebApplicationFactory<Program>
{
    /// <summary>Endpoint etcd-контейнера сценария: тест-класс ставит в своём
    /// конструкторе (фикстуры уже инициализированы) — хост строится лениво
    /// при первом EnsureBuilt/LoginAsync.</summary>
    public string EtcdEndpoint { get; set; } = "";

    /// <summary>Endpoint MinIO-контейнера сценария; null — грань выключена
    /// (AC1): секция AdminPanel:Backups не задаётся вовсе.</summary>
    public string? MinioEndpoint { get; set; }

    /// <summary>Bucket фикстуры MinIO (apm-backups-{guid}).</summary>
    public string MinioBucket { get; set; } = "";

    /// <summary>Вариант AC1: хост без настроек MinIO-грани.</summary>
    public static BackupsWebFactory WithoutMinio(string etcdEndpoint) => new()
    {
        EtcdEndpoint = etcdEndpoint,
    };

    private bool _built;

    /// <summary>Строит хост под замком с очисткой кеша сканирования (см.
    /// PanelHostBuilder) — вызывать ПОСЛЕ установки Endpoint'ов сценария и
    /// ОБЯЗАТЕЛЬНО до первого обращения к Services/CreateClient: ленивый
    /// build мимо хелпера пропустил бы скан сборок и терял [Config]/[InjectAs*]
    /// регистрации (фикс полной серии t08).</summary>
    public void EnsureBuilt()
    {
        if (_built)
            return;
        PanelHostBuilder.BuildExclusive(this);
        _built = true;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        // http-стенд: без AllowHttp Secure-cookie не вернётся по http (spec t02 §10).
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");

        // EtcdOptions.Endpoints — массив: лист "0" биндится в string[].
        if (EtcdEndpoint.Length > 0)
            builder.UseSetting("AdminPanel:Etcd:Endpoints:0", EtcdEndpoint);

        if (MinioEndpoint is not null)
        {
            builder.UseSetting("AdminPanel:Backups:S3:Endpoint", MinioEndpoint);
            builder.UseSetting("AdminPanel:Backups:S3:Bucket", MinioBucket);
            builder.UseSetting("AdminPanel:Backups:S3:AccessKey", MinioContainerFixture.AccessKey);
            builder.UseSetting("AdminPanel:Backups:S3:SecretKey", MinioContainerFixture.SecretKey);
            builder.UseSetting("AdminPanel:Backups:S3:PathStyle", "true");
            builder.UseSetting("AdminPanel:Backups:IntervalSec", "60");
            builder.UseSetting("AdminPanel:Backups:TimeoutSec", "5");
        }

        builder.ConfigureTestServices(services =>
        {
            // hosted НЕ стартуют: снапшот/инвентарь — только ручные тики теста.
            // Сами сервисы разрешимы из Services: конкретные типы (SnapshotRefresher,
            // MinioInventoryLoop) зарегистрированы отдельными дескрипторами,
            // RemoveAll снимает только пересылки IHostedService.
            services.RemoveAll<IHostedService>();
        });
    }
}
