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

    // ── Заморозка/разморозка P1/P5 (freeze_source/unfreeze_shard/grant_app_role) ──

    // Заморозка источника: три REVOKE + барьер LOCK в ОДНОМ батче (REVOKE —
    // лёгкая блокировка и писателей НЕ ждёт). BEGIN/COMMIT и lock_timeout
    // НЕ входят — их ставит исполнитель (ExecuteTransactionalAsync).
    // tables — уже собранный список из TableNames; пустой → без LOCK.
    public static string Freeze(string schema, string appRole, string? tables = null)
    {
        Ident(schema);
        Ident(appRole);
        var barrier = string.IsNullOrEmpty(tables)
            ? string.Empty
            : $"\nLOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE;";
        return
            $"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {schema} FROM {appRole};\n" +
            $"REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} FROM {appRole};\n" +
            $"REVOKE CREATE ON SCHEMA {schema} FROM {appRole};" +
            barrier;
    }

    // Разморозка: симметричные GRANT; БЕЗ GRANT CREATE — его app-роли никогда
    // не выдавалось (дефолт скриптов APP_GRANT_CREATE=0, глобальное ограничение t01).
    public static string Unfreeze(string schema, string appRole)
        => $"GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {Ident(schema)} TO {Ident(appRole)};\n" +
           $"GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {Ident(schema)} TO {Ident(appRole)};";

    // Базовые гранты app-роли на приёмнике (grant_app_role скрипта, §4 доки 11):
    // USAGE + DML + sequences; CREATE ROLE здесь нет — роль создаёт provisioning.
    public static string GrantAppOnSchema(string schema, string appRole)
        => $"GRANT USAGE ON SCHEMA {Ident(schema)} TO {Ident(appRole)};\n" +
           $"GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {Ident(schema)} TO {Ident(appRole)};\n" +
           $"GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {Ident(schema)} TO {Ident(appRole)};";

    // ── Публикации/подписки (P3/P8) ──

    // Публикация бакета на источнике (шаг 2 move).
    public static string CreatePublication(string pub, string schema)
        => $"CREATE PUBLICATION {Ident(pub)} FOR TABLES IN SCHEMA {Ident(schema)}";

    // Подписка на приёмнике: conninfo — строка libpq в SQL-литерале (одинарные
    // кавычки экранируются удвоением); failover-флаг конфигурируем (PG17+),
    // synchronous_commit=remote_apply — всегда (P8).
    // failover=true — только PG17+ (R1/Д11); при false опция ОПУСКАЕТСЯ: в PG16
    // параметра failover нет вовсе («unrecognized subscription parameter», e2e-факт
    // t01 на spilo-16), а в PG17 отсутствие опции семантически равно false.
    public static string CreateSubscription(
        string sub, string conninfo, string pub, bool copyData, bool failover)
    {
        var options = failover
            ? $"copy_data = {Bool(copyData)}, failover = true, synchronous_commit = remote_apply"
            : $"copy_data = {Bool(copyData)}, synchronous_commit = remote_apply";
        return $"CREATE SUBSCRIPTION {Ident(sub)} CONNECTION '{conninfo.Replace("'", "''")}' PUBLICATION {Ident(pub)} " +
               $"WITH ({options})";
    }

    // Fallback-цепочка среза подписки при недоступном источнике (drop_sub
    // abort-move.sh): DISABLE → SET (slot_name = NONE) → DROP.
    public static string DisableSubscription(string sub)
        => $"ALTER SUBSCRIPTION {Ident(sub)} DISABLE";

    public static string SetSlotNone(string sub)
        => $"ALTER SUBSCRIPTION {Ident(sub)} SET (slot_name = NONE)";

    public static string DropSubscription(string sub)
        => $"DROP SUBSCRIPTION {Ident(sub)}";

    public static string DropPublication(string pub)
        => $"DROP PUBLICATION {Ident(pub)}";

    // Схема на не-владельце — с данными (finalize/abort; схема владельца не трогается).
    public static string DropSchemaCascade(string schema)
        => $"DROP SCHEMA {Ident(schema)} CASCADE";

    // ── Cutover: LSN/слоты/sequences/сверки ──

    // Последний LSN записи источника (текстом для сравнения с confirmed_flush).
    public static string CurrentWalLsn()
        => "SELECT pg_current_wal_lsn()::text";

    // Слот догнал: активен и подтвердил LSN (slot_caught_up скрипта).
    public static string SlotCaughtUp(string slot, string lsn)
        => $"SELECT coalesce(bool_and(active AND confirmed_flush_lsn >= '{lsn}'::pg_lsn), false) " +
           $"FROM pg_replication_slots WHERE slot_name = '{Ident(slot)}'";

    // Отставание слота в байтах (slot_lag скрипта; 0, если слота нет).
    public static string SlotLag(string slot)
        => "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)), 0)::bigint " +
           $"FROM pg_replication_slots WHERE slot_name = '{Ident(slot)}'";

    // Слот активен (cleanup_slots: до/после terminate walsender'а).
    public static string SlotActive(string slot)
        => $"SELECT active FROM pg_replication_slots WHERE slot_name = '{Ident(slot)}'";

    // Глушилка walsender'а активного слота (cleanup_slots abort-move.sh).
    public static string TerminateSlotBackend(string slot)
        => $"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " +
           $"WHERE slot_name = '{Ident(slot)}' AND active";

    public static string DropSlot(string slot)
        => $"SELECT pg_drop_replication_slot('{Ident(slot)}')";

    // Готовность подписки «ready/total» (sub_sync скрипта: srsubstate='r').
    public static string SubSyncReady(string sub)
        => "SELECT coalesce(sum((srsubstate = 'r')::int), 0) || '/' || count(*) " +
           "FROM pg_subscription_rel " +
           $"WHERE srsubid = (SELECT oid FROM pg_subscription WHERE subname = '{Ident(sub)}')";

    // Последнее ВЫДАННОЕ на источнике (is_called учитывается на стороне SQL —
    // баш-нюанс стенда, P6): is_called → last_value, иначе last_value-1.
    public static string SequenceIssued(string schema, string seq)
        => $"SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM {Qualified(schema, seq)}";

    // Следующее, которое выдаст sequence приёмника: +1 при is_called.
    public static string SequenceNext(string schema, string seq)
        => $"SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END FROM {Qualified(schema, seq)}";

    // setval только ВПЕРЁД (P6): счётчик приёмника доводится до выданного на
    // источнике; is_called=true, чтобы следующий nextval выдал issued+1.
    public static string SetvalForward(string schema, string seq, long issued)
        => $"SELECT setval('{Qualified(schema, seq)}', {issued}, true)";

    // Count таблицы для сверки строк P8 (verify_row_counts).
    public static string RowCount(string schema, string table)
        => $"SELECT count(*) FROM {Qualified(schema, table)}";

    // Схема.имя в кавычках-идентификаторах (имя всегда в «…»: последовательности
    // и таблицы могут называться зарезервированными словами).
    private static string Qualified(string schema, string name)
        => $"{Ident(schema)}.\"{Ident(name)}\"";

    private static string Bool(bool value) => value ? "true" : "false";
}
