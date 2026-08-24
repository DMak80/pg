using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class MoveSqlPart2Tests
{
    // AAA: заморозка P1/P5 — три REVOKE + барьер LOCK в одном батче (REVOKE не барьер!)
    [Fact]
    public void Freeze_ThreeRevokesAndLockBarrier()
    {
        // Act
        var sql = MoveSql.Freeze("bucket_42", "app", "bucket_42.\"t1\", bucket_42.\"t2\"");

        // Assert
        sql.Should().Contain("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app");
        sql.Should().Contain("REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 FROM app");
        sql.Should().Contain("REVOKE CREATE ON SCHEMA bucket_42 FROM app");
        sql.Should().Contain("LOCK TABLE bucket_42.\"t1\", bucket_42.\"t2\" IN ACCESS EXCLUSIVE MODE;");
    }

    // AAA: разморозка — симметричные GRANT, без CREATE (его app-роли не выдавалось)
    [Fact]
    public void Unfreeze_SymmetricGrants()
    {
        // Act
        var sql = MoveSql.Unfreeze("bucket_42", "app");

        // Assert
        sql.Should().Contain("GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app");
        sql.Should().Contain("GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app");
        sql.Should().NotContain("GRANT CREATE");
    }

    // AAA: подписка — failover=true только PG17+ (R1/Д11); false опускает опцию
    // (в PG16 параметра нет вовсе — e2e-факт t01), remote_apply всегда (P3/P8)
    [Theory]
    [InlineData(true, "copy_data = true, failover = true, synchronous_commit = remote_apply")]
    [InlineData(false, "copy_data = true, synchronous_commit = remote_apply")]
    public void CreateSubscription_Flags(bool failover, string withOptions)
    {
        // Act
        var sql = MoveSql.CreateSubscription("sub_b42", "host=h1,h2 port=1,2 dbname=shop user=bucket_mover password=p'x",
            "pub_b42", copyData: true, failover: failover);

        // Assert
        sql.Should().StartWith("CREATE SUBSCRIPTION sub_b42 CONNECTION '");
        sql.Should().Contain("password=p''x'"); // кавычка conninfo экранирована
        sql.Should().Contain($"WITH ({withOptions})");
    }

    // AAA: sequence-issued — is_called учитывается на стороне SQL (баш-нюанс стенда, P6)
    [Fact]
    public void SequenceIssued_CaseWhenOnSqlSide()
    {
        // Act
        var sql = MoveSql.SequenceIssued("bucket_42", "seq1");

        // Assert
        sql.Should().Be("SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM bucket_42.\"seq1\"");
    }

    // AAA: слот догнал — активен и подтвердил LSN
    [Fact]
    public void SlotCaughtUp_ActiveAndConfirmed()
    {
        // Act
        var sql = MoveSql.SlotCaughtUp("sub_b42", "0/A000123");

        // Assert
        sql.Should().Contain("confirmed_flush_lsn >= '0/A000123'::pg_lsn");
        sql.Should().Contain("bool_and(active");
    }

    // AAA: текущий LSN — текстом (pg_current_wal_lsn, шаг cutover)
    [Fact]
    public void CurrentWalLsn_AsText()
    {
        // Act
        var sql = MoveSql.CurrentWalLsn();

        // Assert
        sql.Should().Be("SELECT pg_current_wal_lsn()::text");
    }

    // AAA: setval только вперёд — схема/seq всегда в кавычках, is_called=true (P6)
    [Fact]
    public void SetvalForward_QuotedAndCalled()
    {
        // Act
        var sql = MoveSql.SetvalForward("bucket_42", "seq1", 100);

        // Assert
        sql.Should().Be("SELECT setval('bucket_42.\"seq1\"', 100, true)");
    }

    // AAA: готовность подписки — «ready/total» по srsubstate='r' (sub_sync скрипта)
    [Fact]
    public void SubSyncReady_ReadyOverTotal()
    {
        // Act
        var sql = MoveSql.SubSyncReady("sub_b42");

        // Assert
        sql.Should().Be("SELECT coalesce(sum((srsubstate = 'r')::int), 0) || '/' || count(*) " +
                        "FROM pg_subscription_rel " +
                        "WHERE srsubid = (SELECT oid FROM pg_subscription WHERE subname = 'sub_b42')");
    }

    // AAA: удаление схемы на не-владельце — CASCADE (finalize/abort)
    [Fact]
    public void DropSchemaCascade_ExactText()
    {
        // Act
        var sql = MoveSql.DropSchemaCascade("bucket_42");

        // Assert
        sql.Should().Be("DROP SCHEMA bucket_42 CASCADE");
    }

    // AAA: count таблицы для сверки строк (verify_row_counts, P8)
    [Fact]
    public void RowCount_QuotedTable()
    {
        // Act
        var sql = MoveSql.RowCount("bucket_42", "items");

        // Assert
        sql.Should().Be("SELECT count(*) FROM bucket_42.\"items\"");
    }

    // AAA: глушилка walsender'а активного слота (cleanup_slots abort-move.sh)
    [Fact]
    public void TerminateSlotBackend_ActiveOnly()
    {
        // Act
        var sql = MoveSql.TerminateSlotBackend("sub_b42");

        // Assert
        sql.Should().Be("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " +
                        "WHERE slot_name = 'sub_b42' AND active");
    }

    // AAA: удаление слота функцией (pg_drop_replication_slot)
    [Fact]
    public void DropSlot_ByFunctionCall()
    {
        // Act
        var sql = MoveSql.DropSlot("sub_b42");

        // Assert
        sql.Should().Be("SELECT pg_drop_replication_slot('sub_b42')");
    }

    // AAA: слот активен — до/после terminate (cleanup_slots)
    [Fact]
    public void SlotActive_ReadsFlag()
    {
        // Act
        var sql = MoveSql.SlotActive("sub_b42");

        // Assert
        sql.Should().Be("SELECT active FROM pg_replication_slots WHERE slot_name = 'sub_b42'");
    }

    // AAA: лаг слота в байтах (slot_lag скрипта, лог copy-wait)
    [Fact]
    public void SlotLag_WalLsnDiff()
    {
        // Act
        var sql = MoveSql.SlotLag("sub_b42");

        // Assert
        sql.Should().Be("SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)), 0)::bigint " +
                        "FROM pg_replication_slots WHERE slot_name = 'sub_b42'");
    }

    // AAA: следующий номер sequence приёмника (is_called→last_value+1, P6)
    [Fact]
    public void SequenceNext_CaseWhen()
    {
        // Act
        var sql = MoveSql.SequenceNext("bucket_42", "seq1");

        // Assert
        sql.Should().Be("SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END FROM bucket_42.\"seq1\"");
    }

    // AAA: disable подписки — первый шаг fallback при недоступном источнике (abort-move.sh)
    [Fact]
    public void DisableSubscription_Alters()
    {
        // Act
        var sql = MoveSql.DisableSubscription("sub_b42");

        // Assert
        sql.Should().Be("ALTER SUBSCRIPTION sub_b42 DISABLE");
    }

    // AAA: slot_name=NONE — срез слота локально, слот останется сиротой (fallback)
    [Fact]
    public void SetSlotNone_AltersSubscription()
    {
        // Act
        var sql = MoveSql.SetSlotNone("sub_b42");

        // Assert
        sql.Should().Be("ALTER SUBSCRIPTION sub_b42 SET (slot_name = NONE)");
    }

    // AAA: drop подписки/публикации — идентификатор без кавычек, как в скриптах
    [Fact]
    public void DropSubscriptionAndPublication_ExactTexts()
    {
        // Act
        var dropSub = MoveSql.DropSubscription("sub_b42");
        var dropPub = MoveSql.DropPublication("pub_b42");

        // Assert
        dropSub.Should().Be("DROP SUBSCRIPTION sub_b42");
        dropPub.Should().Be("DROP PUBLICATION pub_b42");
    }

    // AAA: базовые гранты app-роли на приёмнике — USAGE + DML + sequences (grant_app_role)
    [Fact]
    public void GrantAppOnSchema_UsageDmlSequences()
    {
        // Act
        var sql = MoveSql.GrantAppOnSchema("bucket_42", "app");

        // Assert
        sql.Should().Contain("GRANT USAGE ON SCHEMA bucket_42 TO app");
        sql.Should().Contain("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app");
        sql.Should().Contain("GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app");
        sql.Should().NotContain("GRANT CREATE");
    }

    // AAA: публикация бакета — FOR TABLES IN SCHEMA (P3, шаг 2 move)
    [Fact]
    public void CreatePublication_ForTablesInSchema()
    {
        // Act
        var sql = MoveSql.CreatePublication("pub_bucket_42", "bucket_42");

        // Assert
        sql.Should().Be("CREATE PUBLICATION pub_bucket_42 FOR TABLES IN SCHEMA bucket_42");
    }

    // AAA: freeze без таблиц — LOCK-барьер не добавляется (пустая схема)
    [Fact]
    public void Freeze_EmptyTables_NoLockBarrier()
    {
        // Act
        var sql = MoveSql.Freeze("bucket_42", "app", "");

        // Assert
        sql.Should().NotContain("LOCK TABLE");
        sql.Should().Contain("REVOKE CREATE ON SCHEMA bucket_42 FROM app");
    }

    // AAA: невалидная роль/подписка — исключение до подстановки в SQL
    [Theory]
    [InlineData("App;DROP ROLE x")]
    [InlineData("sub-42")]
    public void Part2Builders_RejectInvalidIdentifiers(string bad)
    {
        // Act
        var freeze = () => MoveSql.Freeze("bucket_42", bad, "");
        var createSub = () => MoveSql.CreateSubscription(bad, "host=h", "pub_b42", copyData: true, failover: true);

        // Assert
        freeze.Should().Throw<ArgumentException>("роль — идентификатор SQL");
        createSub.Should().Throw<ArgumentException>("имя подписки — идентификатор SQL");
    }
}
