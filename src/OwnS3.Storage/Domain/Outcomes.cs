namespace OwnS3.Storage;

// Терминальные варианты одного исхода (конвенция главы 01 §4): Success несёт
// терминальное знание, включая не-ошибочные варианты; Failed — только
// технический отказ. Стиль вариантных записей — по прецеденту UploadVisibility.

/// <summary>Исход поиска xl-меты: запись есть | записи нет (обе — терминальный
/// Success; nullable-маркер устранён).</summary>
public abstract record XlMetaLookup
{
    public sealed record Found(XlMetaRecord Record) : XlMetaLookup;

    public sealed record Missing() : XlMetaLookup;
}

/// <summary>Состояние ключа репликации для LWW (глава 07 §7): запись есть |
/// записи нет (обе — терминальный Success).</summary>
public abstract record ReplicaKeyLookup
{
    public sealed record Found(ReplicaKeyState State) : ReplicaKeyLookup;

    public sealed record Missing() : ReplicaKeyLookup;
}

/// <summary>Исход GetObjectAttributes (глава 02): полные атрибуты |
/// Not-Modified (Metadata несёт ETag/LastModified для заголовков 304);
/// отдельный DTO-дискриминатор не нужен.</summary>
public abstract record GetAttributesOutcome
{
    public sealed record NotModified(ObjectMetadata Metadata) : GetAttributesOutcome;

    public sealed record Full(ObjectAttributes Attributes) : GetAttributesOutcome;
}

/// <summary>Страница листинга replication-грани (глава 07 §6): маркер
/// NextMarker уходит из out-параметра в состав значения (null — конец).</summary>
public sealed record ReplicaListingPage(IReadOnlyList<ReplicationListEntry> Entries,
    string? NextMarker);
