namespace OwnS3.App.Access;

// Роли доступа (arch/owns3/05 §3). Replication — peer-креды mesh (t35, глава 07 §6):
// доступ ТОЛЬКО к replication-грани, все 21 операция закрыта (A13).
public enum AccessPolicy
{
    ReadOnly,
    ReadWrite,
    Admin,
    Replication,
}
