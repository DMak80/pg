namespace AdminPanel.Core;

/// <summary>Пул S3 (read-минимум, t36; adminpanel/02 §12): только страница
/// «Хранилище бэкапов» — список пулов/нод/адресов для модала «Пул бэкапов».
/// Active = нет state-заявки в config (семантика «отсутствие = Active»,
/// arch/23 §2). Полное управление пулами — t34, глава расширится.</summary>
public sealed record S3PoolInfo(
    string Name,
    string? SecretProfile,
    bool Active,
    IReadOnlyList<S3PoolNodeInfo> Nodes,
    string[] Endpoints);

/// <summary>Нода пула: имя инстанса (ключи nodes/&lt;I&gt;/state|placement) + состояние
/// nodes/&lt;I&gt;/state (null — ключа нет: заявлена placement'ом, не поднята).</summary>
public sealed record S3PoolNodeInfo(string Name, string? State);
