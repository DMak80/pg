using System.Reflection;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Модуль etcd-клиента: attribute-DI + именованный HttpClient "etcd" с таймаутом из настроек.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddEtcd(this IServiceCollection services)
    {
        services.AutoRegistration(Assembly);

        // Порядок важен: AddHttpClient<EtcdGateway> добавляется ПОСЛЕ AutoRegistration,
        // чтобы typed-фабрика (последняя регистрация типа) перекрыла дескриптор AutoRegistration
        // и EtcdGateway получал HttpClient из фабрики, а не из дефолтного резолва.
        // Маркер логгера — EtcdGateway (не static: прецедент CS0718).
        services
           .AddHttpClient<EtcdGateway>(EtcdGateway.HttpClientName)
           .ConfigureHttpClient((sp, client) =>
            {
                var seconds = sp.GetRequiredService<IOptions<EtcdOptions>>().Value.RequestTimeoutSeconds;
                if (seconds <= 0)
                {
                    sp.GetRequiredService<ILogger<EtcdGateway>>()
                       .LogWarning("AdminPanel:Etcd:RequestTimeoutSeconds <= 0 — использую 2 c");
                    seconds = 2;
                }

                client.Timeout = TimeSpan.FromSeconds(seconds);
            });

        return services;
    }

    // Модуль kafka-домена (план B3): hosted-service refresher'а + стор снапшота.
    // Отдельный HttpClient "kafka-etcd" не заводится осознанно: транспорт общий
    // с pg-циклом (IEtcdGateway/«etcd», таймаут один — EtcdOptions), дублирование
    // клиента не даёт изоляции; failover/sticky — внутри refresher'а.
    public static IServiceCollection AddKafka(this IServiceCollection services)
    {
        services.AddSingleton<KafkaSnapshotStore>();
        services.AddSingleton<IKafkaSnapshotStore>(sp => sp.GetRequiredService<KafkaSnapshotStore>());
        services.AddSingleton<IKafkaSnapshotReader>(sp => sp.GetRequiredService<KafkaSnapshotStore>());

        services.AddSingleton<KafkaSnapshotRefresher>();
        services.AddHostedService(sp => sp.GetRequiredService<KafkaSnapshotRefresher>());
        return services;
    }
}
