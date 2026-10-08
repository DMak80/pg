using Microsoft.AspNetCore.Builder;
using Metrics.SdGenerator;
using Shared.Etcd.Client;
using Shared.Metrics;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<SdGeneratorOptions>(builder.Configuration.GetSection("SdGenerator"));
// Самонаблюдение (arch/18 §5.2 job sd-generator): AddAppMetrics при Enabled=true
// регистрирует OTel-провайдер и DI-Meter с именем SdGenerator; инструментарий
// (Task 5) берёт этот Meter из DI (канон KafkaWorker.App). Enabled=false —
// провайдера/Meter в DI нет, SdGeneratorMetrics создаст свой (пишем «в никуда»).
builder.Services.AddAppMetrics(SdGeneratorMetrics.MeterName,
    builder.Configuration.GetSection("SdGenerator:Metrics"));
builder.Services.AddHttpClient("etcd");
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
// TODO(t15 Task 5): SdFileWriter, SdGeneratorMetrics, SdGeneratorLoop, HostedService
var app = builder.Build();
app.MapAppMetrics();
app.Run();
