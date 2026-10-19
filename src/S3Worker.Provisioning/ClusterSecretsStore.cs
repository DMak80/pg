using System.Text.Json;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning;

/// <summary>
/// Per-cluster клиентские креды S3 в etcd (arch/23 §6): ленивая генерация
/// (txn put-if-absent, проигрыш → re-read — первый записавший — истина),
/// чтение с проверкой полноты канона, ротация (txn по mod_revision).
/// Состав канона: root-пара + ключи <c>backups</c> (read-write) и
/// <c>panel</c> (read-only); при etcdBackup пара <c>backups</c> —
/// фиксированная из env (спец-ключ per-install). Диагностики — ТОЛЬКО
/// имена переменных/ключей, никогда значения.
/// </summary>
public sealed class ClusterSecretsStore(IEtcdGateway gateway, string[] endpoints, Func<string?, string?>? getenv = null)
{
    private const string BackupsKeyName = "backups";
    private const string PanelKeyName = "panel";
    private const string ReadWritePolicy = "read-write";
    private const string ReadOnlyPolicy = "read-only";

    private const string EtcdBackupAccessKeyEnv = "S3W_ETCD_BACKUP_ACCESSKEY";
    private const string EtcdBackupSecretKeyEnv = "S3W_ETCD_BACKUP_SECRETKEY";
    private const string EtcdBackupEnvName = "S3W_ETCD_BACKUP";

    // getenv по умолчанию — процесс-окружение (инъекция — для юнит-тестов).
    private readonly Func<string?, string?> _getenv = DefaultGetenv(getenv);

    private static Func<string?, string?> DefaultGetenv(Func<string?, string?>? getenv)
        => getenv ?? (name => name is null ? null : Environment.GetEnvironmentVariable(name));

    // Формат secrets (ручная сериализация, порядок полей канона):
    // {"root_user":…,"root_password":…,"access_keys":[{"name":…,"access_key":…,
    // "secret_key":…,"policy":…},…]} — в порядке следования списка.
    internal static string Serialize(ClusterSecrets secrets)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("root_user", secrets.RootUser);
            writer.WriteString("root_password", secrets.RootPassword);
            writer.WriteStartArray("access_keys");
            foreach (var key in secrets.AccessKeys)
            {
                writer.WriteStartObject();
                writer.WriteString("name", key.Name);
                writer.WriteString("access_key", key.AccessKey);
                writer.WriteString("secret_key", key.SecretKey);
                writer.WriteString("policy", key.Policy);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    /// <summary>Разбор материала: null — битый JSON/не-объект/неполный канон
    /// (root-пара непуста; каждый элемент access_keys — объект с непустыми
    /// name/access_key/secret_key/policy; есть backups=read-write и
    /// panel=read-only). Лишние элементы сохраняются.</summary>
    internal static ClusterSecrets? TryParse(string json)
    {
        List<ClusterAccessKey> keys;
        string rootUser;
        string rootPassword;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !TryGetString(root, "root_user", out rootUser)
                || !TryGetString(root, "root_password", out rootPassword)
                || !root.TryGetProperty("access_keys", out var array)
                || array.ValueKind != JsonValueKind.Array)
                return null;

            keys = [];
            foreach (var item in array.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object
                    || !TryGetString(item, "name", out var name)
                    || !TryGetString(item, "access_key", out var accessKey)
                    || !TryGetString(item, "secret_key", out var secretKey)
                    || !TryGetString(item, "policy", out var policy))
                    return null;
                keys.Add(new ClusterAccessKey(name, accessKey, secretKey, policy));
            }
        }
        catch (JsonException)
        {
            return null;
        }

        // Полнота канона: обязательные роли с их политиками.
        var backups = keys.FirstOrDefault(k => k.Name == BackupsKeyName);
        var panel = keys.FirstOrDefault(k => k.Name == PanelKeyName);
        return backups is { Policy: ReadWritePolicy } && panel is { Policy: ReadOnlyPolicy }
            ? new ClusterSecrets(rootUser, rootPassword, keys)
            : null;
    }

    // Непустая строка-поле объекта; отсутствие/не-строка/пустая — false.
    private static bool TryGetString(JsonElement element, string name, out string value)
    {
        if (element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String)
        {
            var s = v.GetString();
            if (!string.IsNullOrEmpty(s))
            {
                value = s;
                return true;
            }
        }

        value = string.Empty;
        return false;
    }

    /// <summary>Ленивая генерация per-cluster кредов: есть → идемпотентный
    /// возврат (битость — Failed); нет → Generate + txn put-if-absent,
    /// проигрыш → один повтор чтения (первый записавший — истина).</summary>
    public async Task<Result<ClusterSecrets>> EnsureAsync(string cluster, bool etcdBackup, CancellationToken ct)
    {
        var key = S3ProcessCommon.ClusterSecretsKey(cluster);
        var read = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!read.IsSuccess)
            return Result<ClusterSecrets>.Failed(read.Error!);

        // Ключ есть: материал уже выпущен — идемпотентный возврат; битость — Failed.
        if (read.Value is { } existing)
        {
            var parsed = TryParse(existing.Value);
            return parsed is not null
                ? Result<ClusterSecrets>.Success(parsed)
                : Result<ClusterSecrets>.Failed(new ApplicationException(
                    $"secrets {key}: corrupt/incomplete material"));
        }

        var generated = Generate(cluster, etcdBackup);
        if (!generated.IsSuccess)
            return Result<ClusterSecrets>.Failed(generated.Error!);

        var txn = await TxnWithFailoverAsync(
            TxnRequest.Of([TxnCompare.NotExists(key)],
                [new TxnOp.Put(key, Serialize(generated.Value), null)]), ct);
        if (!txn.IsSuccess)
            return Result<ClusterSecrets>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
        {
            // Выиграл конкурент — его материал истина: один повтор шага чтения.
            var reread = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
            if (!reread.IsSuccess)
                return Result<ClusterSecrets>.Failed(reread.Error!);
            var winner = reread.Value is { } kv ? TryParse(kv.Value) : null;
            return winner is not null
                ? Result<ClusterSecrets>.Success(winner)
                : Result<ClusterSecrets>.Failed(new ApplicationException(
                    $"secrets {key}: lost txn put-if-absent race, material is missing/broken"));
        }

        return Result<ClusterSecrets>.Success(generated.Value);
    }

    /// <summary>Чтение кредов кластера: null — ключа нет; битый/неполный
    /// материал — Failed (без значений в диагностике).</summary>
    public async Task<Result<ClusterSecrets?>> TryReadAsync(string cluster, CancellationToken ct)
    {
        var key = S3ProcessCommon.ClusterSecretsKey(cluster);
        var read = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!read.IsSuccess)
            return Result<ClusterSecrets?>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<ClusterSecrets?>.Success(null);

        var parsed = TryParse(kv.Value);
        return parsed is not null
            ? Result<ClusterSecrets?>.Success(parsed)
            : Result<ClusterSecrets?>.Failed(new ApplicationException(
                $"secrets {key}: corrupt/incomplete material"));
    }

    /// <summary>Ротация: root-пара (user прежний) и все ключи перегенерируются;
    /// при etcdBackup пара backups — из env (полупара/отсутствие — Failed);
    /// состав заменяется каноном (backups, panel) — прежние лишние имена НЕ
    /// переносятся. Запись — txn по mod_revision (проигрыш — Failed, повтор
    /// следующим тиком).</summary>
    public async Task<Result<ClusterSecrets>> RotateAsync(string cluster, bool etcdBackup, CancellationToken ct)
    {
        var key = S3ProcessCommon.ClusterSecretsKey(cluster);
        var read = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!read.IsSuccess)
            return Result<ClusterSecrets>.Failed(read.Error!);
        if (read.Value is not { } kv)
            return Result<ClusterSecrets>.Failed(new ApplicationException($"secrets {key}: not found"));

        var current = TryParse(kv.Value);
        if (current is null)
            return Result<ClusterSecrets>.Failed(new ApplicationException(
                $"secrets {key}: corrupt/incomplete material"));

        string backupsAccessKey;
        string backupsSecretKey;
        if (etcdBackup)
        {
            var pair = EtcdBackupPair();
            if (!pair.IsSuccess)
                return Result<ClusterSecrets>.Failed(pair.Error!);
            (backupsAccessKey, backupsSecretKey) = pair.Value;
        }
        else
        {
            backupsAccessKey = ClusterSecretGenerator.Generate();
            backupsSecretKey = ClusterSecretGenerator.Generate();
        }

        var fresh = new ClusterSecrets(
            current.RootUser,
            ClusterSecretGenerator.Generate(),
            [
                new ClusterAccessKey(BackupsKeyName, backupsAccessKey, backupsSecretKey, ReadWritePolicy),
                new ClusterAccessKey(PanelKeyName,
                    ClusterSecretGenerator.Generate(), ClusterSecretGenerator.Generate(), ReadOnlyPolicy),
            ]);

        var txn = await TxnWithFailoverAsync(
            TxnRequest.Of([TxnCompare.ModRevisionEqual(key, (long)kv.ModRevision)],
                [new TxnOp.Put(key, Serialize(fresh), null)]), ct);
        if (!txn.IsSuccess)
            return Result<ClusterSecrets>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<ClusterSecrets>.Failed(new ApplicationException(
                $"secrets {key}: changed underneath us — retried next tick"));

        return Result<ClusterSecrets>.Success(fresh);
    }

    // Генерация нового материала: root-пара + канонический состав ключей
    // (backups=read-write, panel=read-only); при etcdBackup пара backups —
    // из env (полупара/отсутствие — Failed).
    private Result<ClusterSecrets> Generate(string cluster, bool etcdBackup)
    {
        string backupsAccessKey;
        string backupsSecretKey;
        if (etcdBackup)
        {
            var pair = EtcdBackupPair();
            if (!pair.IsSuccess)
                return Result<ClusterSecrets>.Failed(pair.Error!);
            (backupsAccessKey, backupsSecretKey) = pair.Value;
        }
        else
        {
            backupsAccessKey = ClusterSecretGenerator.Generate();
            backupsSecretKey = ClusterSecretGenerator.Generate();
        }

        return Result<ClusterSecrets>.Success(new ClusterSecrets(
            $"s3w-{cluster}-root",
            ClusterSecretGenerator.Generate(),
            [
                new ClusterAccessKey(BackupsKeyName, backupsAccessKey, backupsSecretKey, ReadWritePolicy),
                new ClusterAccessKey(PanelKeyName,
                    ClusterSecretGenerator.Generate(), ClusterSecretGenerator.Generate(), ReadOnlyPolicy),
            ]));
    }

    // Пара etcd-backup из env: обе переменные обязательны (null/пустая любая —
    // полупара/отсутствие → Failed); диагностика — только имена переменных.
    private Result<(string AccessKey, string SecretKey)> EtcdBackupPair()
    {
        var accessKey = _getenv(EtcdBackupAccessKeyEnv);
        var secretKey = _getenv(EtcdBackupSecretKeyEnv);
        return string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey)
            ? Result<(string, string)>.Failed(new ApplicationException(
                $"etcd-backup cluster: {EtcdBackupAccessKeyEnv}/{EtcdBackupSecretKeyEnv} env pair is incomplete"))
            : Result<(string, string)>.Success((accessKey, secretKey));
    }

    /// <summary>Полупары etcd-backup env для fail-fast СТАРТА (полупара —
    /// мусорный деплой; полное отсутствие пары — легально, спец-ключ per-install
    /// опционален). Возвращает ["S3W_ETCD_BACKUP"], если задана ровно одна
    /// переменная пары; иначе пустой список.</summary>
    public static IReadOnlyList<string> ValidateEtcdBackupEnvPairs(Func<string?, string?>? getenv = null)
    {
        var read = DefaultGetenv(getenv);
        var hasAccessKey = !string.IsNullOrEmpty(read(EtcdBackupAccessKeyEnv));
        var hasSecretKey = !string.IsNullOrEmpty(read(EtcdBackupSecretKeyEnv));
        return hasAccessKey != hasSecretKey ? [EtcdBackupEnvName] : [];
    }

    /// <summary>Гвард API СОЗДАНИЯ etcd-backup кластера: true ⟺ ОБЕ переменные
    /// S3W_ETCD_BACKUP_ACCESSKEY и S3W_ETCD_BACKUP_SECRETKEY непустые. Отсутствие
    /// пары — тоже неполная (false): создание без пары отказывает 422, а не
    /// уводит provisioning P2 в permanent-fail.</summary>
    public static bool IsEtcdBackupEnvPairComplete(Func<string?, string?>? getenv = null)
    {
        var read = DefaultGetenv(getenv);
        return !string.IsNullOrEmpty(read(EtcdBackupAccessKeyEnv))
            && !string.IsNullOrEmpty(read(EtcdBackupSecretKeyEnv));
    }

    // Failover-исполнение txn (первый успешный endpoint) — по образцу PkiStore.
    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
