using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace OwnS3.App.Access;

// Реестр учёток (arch/owns3/05 §1–2): root-пара становится ключом admin;
// статические ключи — по policy; неизвестная policy — отказ построения
// (fail-fast конфигурации). Учётки t36 живут в памяти из конфигурации.
public sealed class AccessKeyRegistry
{
    private readonly ConcurrentDictionary<string, AccessKeyRecord> _records = new(StringComparer.Ordinal);

    public AccessKeyRegistry(IOptions<OwnS3Options> options)
    {
        foreach (var key in options.Value.AccessKeys)
        {
            var record = new AccessKeyRecord(key.AccessKey, key.SecretKey, ParsePolicy(key.Policy));
            if (!_records.TryAdd(record.AccessKey, record))
                throw new ArgumentException($"Дубликат access key в конфигурации: {record.AccessKey}");
        }

        // Root — полный доступ (глава 05 §1), аутентифицируется как обычный ключ.
        var root = new AccessKeyRecord(options.Value.Root.User, options.Value.Root.Password, AccessPolicy.Admin);
        _records[root.AccessKey] = root;
    }

    public AccessKeyRecord? Find(string accessKey) =>
        _records.TryGetValue(accessKey, out var record) ? record : null;

    private static AccessPolicy ParsePolicy(string policy) => policy switch
    {
        "read-only" => AccessPolicy.ReadOnly,
        "read-write" => AccessPolicy.ReadWrite,
        "admin" => AccessPolicy.Admin,
        _ => throw new ArgumentException($"Неизвестная policy access key: '{policy}' (допустимо read-only/read-write/admin)"),
    };
}
