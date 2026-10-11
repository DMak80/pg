using OwnS3.Storage.Erasure;

namespace OwnS3.App;

// Итог режима хранения по конфигу (канон 06 §1): 1 путь — XL Single, список ≥ 2 —
// XL Erasure. Разрешается лениво по IOptions (после Build): прод-конфиг и
// in-memory-инжект WAF-тестов видны одинаково; ошибки парсинга/топологии —
// fail-fast при первом разрешении (Program — Critical + exit 1 до Kestrel).
public sealed record StorageModeInfo(bool IsErasure, IReadOnlyList<string> DataRoots, ErasureTopology? Topology)
{
    // Единственный путь single-режима (EC — null)
    public string? SingleRoot => IsErasure ? null : DataRoots[0];

    public static StorageModeInfo Resolve(OwnS3Options options)
    {
        var dirs = OwnS3EcOptions.ParseDataDirs(options.DataDir);
        if (dirs.Count == 1)
            return new StorageModeInfo(IsErasure: false, dirs, Topology: null);
        var topology = OwnS3EcOptions.BuildTopology(dirs.Count, options.Ec.SetSize, options.Ec.Parity);
        return new StorageModeInfo(IsErasure: true, dirs, topology);
    }
}
