namespace S3Worker.Core.Model;

// Адрес S3-ноды пула (arch/s3-clusters/02-keys.md §1/§8): advertised host + S3-порт из
// /s3worker/portalloc/<C> (формат значения поля "s3"); единица планирования
// PortAllocIndex/endpoints (S3Planning — образец KfwPlanning).
public sealed record NodeAddress(string Host, int S3Port);
