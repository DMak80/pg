using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class MoveSqlTests
{
    // AAA: префлайт wal_level — через pg_settings (Npgsql-надёжнее SHOW)
    [Fact]
    public void WalLevel_SelectsFromPgSettings()
    {
        // Act
        var sql = MoveSql.WalLevel();

        // Assert
        sql.Should().Be("SELECT setting FROM pg_settings WHERE name = 'wal_level'");
    }

    // AAA: sync-standby приёмника — имена непусты + живой sync/quorum (P8)
    [Fact]
    public void SyncStandbyNames_SelectsFromPgSettings()
    {
        // Act
        var sql = MoveSql.SyncStandbyNames();

        // Assert
        sql.Should().Be("SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'");
    }

    // AAA: sync-standby приёмника — имена непусты + живой sync/quorum (P8)
    [Fact]
    public void SyncStandbyCount_UsesSyncState()
    {
        // Act
        var sql = MoveSql.SyncStandbyCount();

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')");
    }

    // AAA: список таблиц для LOCK-барьера и сверки — из каталога, не хардкод (дока 11 §5 4.2)
    [Fact]
    public void TableNames_AggregatesQuoted()
    {
        // Act
        var sql = MoveSql.TableNames("bucket_42");

        // Assert
        sql.Should().Be("SELECT coalesce(string_agg(format('%I.%I', 'bucket_42', c.relname), ', '), '') " +
                        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
                        "WHERE c.relkind IN ('r','p') AND n.nspname = 'bucket_42'");
    }

    // AAA: инвентарь P5 — relkind|relname построчно, сортировка стабильна
    [Fact]
    public void SchemaInventory_KindsAndOrder()
    {
        // Act
        var sql = MoveSql.SchemaInventory("bucket_42");

        // Assert
        sql.Should().Contain("c.relkind IN ('r','S','v','m','p')");
        sql.Should().Contain("ORDER BY c.relname, 1");
    }

    // AAA: невалидное имя схемы — исключение (SQL-инъекция)
    [Theory]
    [InlineData("B;DROP TABLE x")]
    [InlineData("bucket-42")]
    public void Builders_RejectInvalidIdentifiers(string bad)
    {
        // Act
        var act = () => MoveSql.TableNames(bad);

        // Assert
        act.Should().Throw<ArgumentException>("идентификаторы обязаны проходить ^[a-z][a-z0-9_]*$");
    }

    // AAA: схема есть на шарде — to_regnamespace (schema_exists скрипта)
    [Fact]
    public void SchemaExists_UsesToRegnamespace()
    {
        // Act
        var sql = MoveSql.SchemaExists("bucket_42");

        // Assert
        sql.Should().Be("SELECT to_regnamespace('bucket_42') IS NOT NULL");
    }

    // AAA: публикация существует — count по pg_publication (pub_exists скрипта)
    [Fact]
    public void PubExists_CountsByPubname()
    {
        // Act
        var sql = MoveSql.PubExists("pub_b42");

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_publication WHERE pubname = 'pub_b42'");
    }

    // AAA: подписка существует — count по pg_subscription (sub_exists скрипта)
    [Fact]
    public void SubExists_CountsBySubname()
    {
        // Act
        var sql = MoveSql.SubExists("sub_b42");

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_subscription WHERE subname = 'sub_b42'");
    }

    // AAA: слот существует — count по pg_replication_slots (slot_exists скрипта)
    [Fact]
    public void SlotExists_CountsBySlotName()
    {
        // Act
        var sql = MoveSql.SlotExists("sub_b42");

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sub_b42'");
    }

    // AAA: лимит слотов — setting::int из pg_settings (префлайт move)
    [Fact]
    public void MaxSlots_ReadsPgSettings()
    {
        // Act
        var sql = MoveSql.MaxSlots();

        // Assert
        sql.Should().Be("SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'");
    }

    // AAA: занятые слоты — count по pg_replication_slots
    [Fact]
    public void UsedSlots_CountsReplicationSlots()
    {
        // Act
        var sql = MoveSql.UsedSlots();

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_replication_slots");
    }

    // AAA: лимит walsender'ов — setting::int из pg_settings (префлайт move)
    [Fact]
    public void MaxWalSenders_ReadsPgSettings()
    {
        // Act
        var sql = MoveSql.MaxWalSenders();

        // Assert
        sql.Should().Be("SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders'");
    }

    // AAA: занятые walsender'ы — count по pg_stat_replication
    [Fact]
    public void UsedWalSenders_CountsStatReplication()
    {
        // Act
        var sql = MoveSql.UsedWalSenders();

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_stat_replication");
    }

    // AAA: P4-префлайт — инвалидированные слоты (wal_status='lost')
    [Fact]
    public void LostSlots_FiltersWalStatus()
    {
        // Act
        var sql = MoveSql.LostSlots();

        // Assert
        sql.Should().Be("SELECT count(*) FROM pg_replication_slots WHERE wal_status = 'lost'");
    }

    // AAA: mover-роль с REPLICATION — rolsuper/rolreplication текущего юзера
    [Fact]
    public void MoverRoleOk_ChecksReplicationAttr()
    {
        // Act
        var sql = MoveSql.MoverRoleOk();

        // Assert
        sql.Should().Be("SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user");
    }

    // AAA: sequences схемы источника — relkind='S' по алфавиту (P6)
    [Fact]
    public void SequenceNames_ListsSorted()
    {
        // Act
        var sql = MoveSql.SequenceNames("bucket_42");

        // Assert
        sql.Should().Be("SELECT s.relname FROM pg_class s JOIN pg_namespace ns ON ns.oid = s.relnamespace " +
                        "WHERE s.relkind = 'S' AND ns.nspname = 'bucket_42' ORDER BY 1");
    }

    // AAA: осиротевшие tablesync-слоты — LIKE sub_<bucket>_sync_% (P8, finalize)
    [Fact]
    public void OrphanTablesyncSlots_LikesSyncPattern()
    {
        // Act
        var sql = MoveSql.OrphanTablesyncSlots("sub_b42");

        // Assert
        sql.Should().Be("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'sub_b42_sync_%'");
    }

    // AAA: генератор проверки пустоты схемы — первый SQL порождает второй
    // (сумма count всех r-таблиц; --resume допустим только для пустой схемы)
    [Fact]
    public void EmptySchemaCheckSqlGen_GeneratesSumOfCounts()
    {
        // Act
        var sql = MoveSql.EmptySchemaCheckSqlGen("bucket_42");

        // Assert
        sql.Should().Contain("quote_ident(n.nspname)");
        sql.Should().Contain("quote_ident(c.relname)");
        sql.Should().Contain("n.nspname = 'bucket_42'");
        sql.Should().Contain("c.relkind = 'r'");
    }
}
