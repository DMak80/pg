using Microsoft.Extensions.Hosting;

// Точка входа PgWorker. Каркас host-builder'а: циклы Reconcile/Keepalive/Snapshot,
// DI и конфигурация подключаются в последующих задачах (блок G плана).

var builder = Host.CreateApplicationBuilder(args);

await builder.Build().RunAsync();
