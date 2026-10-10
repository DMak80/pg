namespace OwnS3.App;

// Конфигурация ownS3 (arch/owns3/05 §4): секция OwnS3 / env OWNS3_*.
// DataDir — корень тома данных xl-хранения (arch/owns3/04 §1; env OWNS3_DATA_DIR).
public sealed class OwnS3Options
{
    public const string SectionName = "OwnS3";

    public ServerOptions Server { get; set; } = new();
    public string DataDir { get; set; } = "/data";
    public RootOptions Root { get; set; } = new();
    public AccessKeyOptions[] AccessKeys { get; set; } = [];
    public string? HostId { get; set; }
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
