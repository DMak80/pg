namespace S3Worker.Core.Model;

// Адрес S3-ноды пула (arch/23 §2/§8): advertised host + S3-порт из
// /s3worker/portalloc/<C> (формат значения поля "s3"); единица планирования
// PortAllocIndex/endpoints (S3Planning — образец KfwPlanning).
public sealed record NodeAddress(string Host, int S3Port);
