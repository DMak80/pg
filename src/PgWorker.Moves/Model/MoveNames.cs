using System.Text.RegularExpressions;

namespace PgWorker.Moves;

/// <summary>Строки state статус-ключа (формат скриптов, spec §4.2; нет ключа = ACTIVE).</summary>
public static class MoveStates
{
    public const string Syncing = "SYNCING";
    public const string Frozen = "FROZEN";
    public const string Aborting = "ABORTING";
}

/// <summary>App-роль, чей write-доступ срезается заморозкой P1 (создаёт provisioning).</summary>
public static partial class MoveNames
{
    public const string AppRole = "app";
    public const string MoverRole = "bucket_mover";

    [GeneratedRegex("^[a-z][a-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierRegex();

    public static bool ValidateIdentifier(string name) => IdentifierRegex().IsMatch(name);

    public static string Pub(string bucket) => $"pub_{bucket}";
    public static string Sub(string bucket) => $"sub_{bucket}";
    public static string PubRb(string bucket) => $"pub_{bucket}_rb";
    public static string SubRb(string bucket) => $"sub_{bucket}_rb";

    public static string RoutingKey(string cluster, string bucket) => $"/clusters/{cluster}/buckets/routing/{bucket}";
    public static string StatusKey(string cluster, string bucket) => $"/clusters/{cluster}/buckets/status/{bucket}";
    public static string MoveKey(string cluster, string bucket) => $"/pgworker/moves/{cluster}/{bucket}";
    public static string MovesPrefix(string cluster) => $"/pgworker/moves/{cluster}/";
}
