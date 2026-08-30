// Заглушка точки входа — композиция хоста наполняется задачей A12 (arch/16 §8).
var builder = Host.CreateApplicationBuilder(args);
await builder.Build().RunAsync();
