namespace OwnS3.App;

// Конфигурация ownS3 (arch/owns3/05 §4): секция OwnS3 / env OWNS3_*.
// DataDir — корень тома данных xl-хранения (arch/owns3/04 §1; env OWNS3_DATA_DIR).
public sealed class OwnS3Options
{
    public const string SectionName = "OwnS3";

    public ServerOptions Server { get; set; } = new();
    public string DataDir { get; set; } = "/data";
    public EcOptions Ec { get; set; } = new();
    public RootOptions Root { get; set; } = new();
    public AccessKeyOptions[] AccessKeys { get; set; } = [];
    public string? HostId { get; set; }
    public TlsOptions Tls { get; set; } = new();
    // Mesh-репликация (t35, глава 07 §11): секция OwnS3:Replication / OWNS3_REPLICATION_*.
    public OwnS3.Replication.ReplicationOptions Replication { get; set; } = new();
}

// EC-параметры (канон 05 §4/06 §1): SetSize null = авто (наибольший делитель N
// из {16,8,4}); parity — default для НОВЫХ объектов (per-object, канон 06 §1).
public sealed class EcOptions
{
    public int? SetSize { get; set; }
    public int Parity { get; set; } = 2;
}

public sealed class ServerOptions
{
    public int Port { get; set; } = 9000;
}

public sealed class RootOptions
{
    public string User { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

// Статический access key (arch/owns3/05 §2): env OWNS3_ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}.
public sealed class AccessKeyOptions
{
    public string AccessKey { get; set; } = string.Empty;
    public string SecretKey { get; set; } = string.Empty;
    public string Policy { get; set; } = "read-only";
}

// TLS-материал HTTPS-грани (arch/owns3/05 §4): PEM-значения или _PATH-файлы;
// env OWNS3_TLS_{CERT,KEY}[_PATH]. Пусто — HTTP-режим (стенд).
public sealed class TlsOptions
{
    public string? ServerCertPem { get; set; }
    public string? ServerKeyPem { get; set; }
    public string? ServerCertPath { get; set; }
    public string? ServerKeyPath { get; set; }
}
