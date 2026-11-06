using EtcdNode.App;
using EtcdNode.App.Loops;
using Shared.Metrics;

// Точка входа агента EtcdNode (arch/etcdnode/README.md, 01-agent.md):
// единственный цикл AgentLoop; БЕЗ сетевых портов (наблюдаемость — ключи
// /etcdnode/ + логи). CA-PEM из env материализуется в temp-файл (AWSSDK
// путь принимает, содержимое — нет).

var s3CaPem = Environment.GetEnvironmentVariable("ETCDNODE_S3_CA_PEM");
string? s3CaPemPath = null;
if (!string.IsNullOrWhiteSpace(s3CaPem))
{
    s3CaPemPath = Path.Combine(Path.GetTempPath(), "etcdnode-s3-ca.pem");
    File.WriteAllText(s3CaPemPath, s3CaPem);
}

var options = EtcdNodeEnv.Load(s3CaPemPath); // fail-fast при битых значениях

var builder = Host.CreateApplicationBuilder(args);
// Опции — singleton-ЭКЗЕМПЛЯР (ручная сборка из env; IOptionsMonitor дал бы
// дефолты — см. контракт AgentLoop).
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddAppMetrics("EtcdNode", builder.Configuration.GetSection("EtcdNode:Metrics"));
builder.Services.AddSingleton(sp => new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5));
builder.Services.AddSingleton<Shared.Etcd.Client.IEtcdGateway>(sp =>
    new Shared.Etcd.Client.EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new Shared.Etcd.Coordination.ClaimStore(
    "/etcdnode",
    ["http://localhost:2379"],
    sp.GetRequiredService<Shared.Etcd.Client.IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    instanceId: options.NodeName));
builder.Services.AddHostedService<AgentLoop>();

await builder.Build().RunAsync();
