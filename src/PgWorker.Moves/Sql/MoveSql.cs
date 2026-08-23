namespace PgWorker.Moves;

/// <summary>
/// Строковые SQL-билдеры переезда бакета — перенос buckets-common.sh/move-bucket.sh
/// 1:1 (spec Д6: скрипты и PgWorker взаимозаменяемы). Каждый идентификатор
/// (схема/публикация/подписка/слот/роль) валидируется перед подстановкой —
/// защита от SQL-инъекций (паттерн DatabaseProvisioner.ValidateIdentifier).
/// </summary>
public static class MoveSql
{
    // Проверка идентификатора по конвенции скриптов (^bucket$/^shard$):
    // невалидное имя — ArgumentException, а не SQL-текст.
    private static string Ident(string name)
    {
        if (!MoveNames.ValidateIdentifier(name))
            throw new ArgumentException($"недопустимый идентификатор SQL: '{name}' (шаблон ^[a-z][a-z0-9_]*)");
        return name;
    }

    // ── Проверки существования (schema_exists/pub_exists/sub_exists/slot_exists) ──

    // Схема на шарде: t/f через to_regnamespace (schema_exists скрипта).
    public static string SchemaExists(string schema)
        => $"SELECT to_regnamespace('{Ident(schema)}') IS NOT NULL";

    // Публикация существует (pub_exists скрипта).
    public static string PubExists(string pub)
        => $"SELECT count(*) FROM pg_publication WHERE pubname = '{Ident(pub)}'";

    // Подписка существует (sub_exists скрипта).
    public static string SubExists(string sub)
        => $"SELECT count(*) FROM pg_subscription WHERE subname = '{Ident(sub)}'";

    // Слот существует (slot_exists скрипта).
    public static string SlotExists(string slot)
        => $"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{Ident(slot)}'";

    // ── Префлайт источника (cmd_move шаг 0) ──

    // wal_level источника: через pg_settings — SHOW в Npgsql-батчах ненадёжен.
    public static string WalLevel()
        => "SELECT setting FROM pg_settings WHERE name = 'wal_level'";

    // Лимит replication-слотов (префлайт «слоты не кончились»).
    public static string MaxSlots()
        => "SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'";

    // Занятые replication-слоты.
    public static string UsedSlots()
        => "SELECT count(*) FROM pg_replication_slots";

    // Лимит walsender'ов (префлайт).
    public static string MaxWalSenders()
        => "SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders'";

    // Занятые walsender'ы.
    public static string UsedWalSenders()
        => "SELECT count(*) FROM pg_stat_replication";

    // P4-префлайт: инвалидированные слоты (прошлый переезд умер от WAL-лимита).
    public static string LostSlots()
        => "SELECT count(*) FROM pg_replication_slots WHERE wal_status = 'lost'";

    // Проба mover-роли: rolsuper ИЛИ rolreplication текущего пользователя
    // (mover подписывается — нужен атрибут REPLICATION, §4 доки 11).
    public static string MoverRoleOk()
        => "SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user";

    // ── P8: sync-standby приёмника (remote_apply без него вырождается) ──

    // synchronous_standby_names приёмника: пусто → remote_apply вырожден.
    public static string SyncStandbyNames()
        => "SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'";

    // Живые sync/quorum-реплики мастера приёмника (check_sync_standby).
    public static string SyncStandbyCount()
        => "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')";

    // ── Инвентарь схемы (P5/P8) ──

    // Таблицы для LOCK-барьера заморозки и сверки строк (r/p, имя в кавычках
    // через format %I.%I — freeze_source/verify_row_counts скриптов).
    public static string TableNames(string schema)
        => "SELECT coalesce(string_agg(format('%I.%I', '" + Ident(schema) + "', c.relname), ', '), '') " +
           "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
           $"WHERE c.relkind IN ('r','p') AND n.nspname = '{Ident(schema)}'";

    // Инвентарь схемы «relkind|relname» построчно для сверки DDL (P5,
    // schema_inventory скрипта; порядок стабилен для построчного diff).
    public static string SchemaInventory(string schema)
        => $"SELECT c.relkind::text || '|' || c.relname " +
           "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
           $"WHERE n.nspname = '{Ident(schema)}' AND c.relkind IN ('r','S','v','m','p') " +
           "ORDER BY c.relname, 1";

    // Sequences схемы источника по алфавиту (sync_sequences_forward, P6).
    public static string SequenceNames(string schema)
        => $"SELECT s.relname FROM pg_class s JOIN pg_namespace ns ON ns.oid = s.relnamespace " +
           $"WHERE s.relkind = 'S' AND ns.nspname = '{Ident(schema)}' ORDER BY 1";

    // Генератор проверки пустоты схемы (шаг 0 move, ветка --resume): первый
    // скаляр возвращает текст второго SQL — суммы count(*) всех r-таблиц;
    // 0 = схема пустая (допустимый resume), иначе — остатки данных.
    public static string EmptySchemaCheckSqlGen(string schema)
        => "SELECT 'SELECT ' || coalesce(string_agg(" +
           "'(SELECT count(*) FROM ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ')', '+'), '0') " +
           "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
           $"WHERE n.nspname = '{Ident(schema)}' AND c.relkind = 'r'";

    // P8: осиротевшие tablesync-слоты (failover приёмника посреди copy
    // рестартует таблицу новым слотом) — уборка finalize.
    public static string OrphanTablesyncSlots(string sub)
        => $"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '{Ident(sub)}_sync_%'";
}
