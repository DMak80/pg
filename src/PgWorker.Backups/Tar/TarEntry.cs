namespace PgWorker.Backups.Tar;

/// <summary>Запись tar-архива (ustar + GNU longname + PAX): заголовочные
/// поля, значимые для перепаковщика/верификатора. Size — размер данных
/// записи; для PAX/longname-служебных записей читатель применяет их к
/// СЛЕДУЮЩЕЙ записи и наружу не отдаёт.</summary>
public sealed record TarEntry(string Name, char TypeFlag, long Size, int Mode, string LinkName);

/// <summary>Поддерживаемые typeflag tar-записей (spec t01 §6.15): файл,
/// каталог, symlink, служебные GNU longname/longlink и PAX. Прочие — ошибка
/// формата (hardlink'ов в PGDATA нет — fail-fast вместо эмуляции).</summary>
public static class TarTypeFlags
{
    public const char File = '0';
    public const char LegacyFile = '\0';
    public const char Dir = '5';
    public const char Symlink = '2';
    public const char GnuLongName = 'L';
    public const char GnuLongLink = 'K';
    public const char PaxHeader = 'x';
}

/// <summary>Нарушение формата tar (битый checksum, чужой typeflag, обрыв
/// данных): поток не является валидным tar — читать дальше нельзя.</summary>
public sealed class TarFormatException(string message) : Exception(message);
