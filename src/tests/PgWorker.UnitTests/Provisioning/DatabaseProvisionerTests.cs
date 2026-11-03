using System.Text.RegularExpressions;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Sql;

namespace PgWorker.UnitTests.Provisioning;

// DatabaseProvisioner — SQL-механика БД/ролей/схем (задача 18): генерация
// идемпотентных текстов по эталону init-cluster.sh + гранты §4 доки 11.
public class DatabaseProvisionerTests
{
    // Тестовые креды ролей и DSN (не секреты): env с заведомо фейковым фолбэком —
    // литералов-креденциалов в коде нет.
    private static string EnvFake(string name, string fallback) =>
        Environment.GetEnvironmentVariable(name) ?? fallback;
    private static string TestAppPassword => EnvFake("PW_TEST_DB_APP_PASSWORD", "unit-test-app-pw-000");
    private static string TestAdminPassword => EnvFake("PW_TEST_DB_ADMIN_PASSWORD", "unit-test-admin-pw-000");
    private static string TestMoverPassword => EnvFake("PW_TEST_DB_MOVER_PASSWORD", "unit-test-mover-pw-000");
    private static string TestDsnPassword => EnvFake("PW_TEST_DB_DSN_PASSWORD", "unit-test-dsn-pw-000");

    private static readonly InstallSecrets Secrets = new(
        "su-pw", "standby-pw", TestAdminPassword, TestMoverPassword);

    [Fact]
    public void BuildCreateDatabaseSql_GuardThroughPgDatabase_Idempotent()
    {
        // Arrange — dbname валиден
        const string dbname = "shop";

        // Act
        var sql = DatabaseProvisioner.BuildCreateDatabaseSql(dbname);

        // Assert: CREATE только при отсутствии БД (guard через pg_database);
        // текст возвращает САМУ команду создания — исполнение двухшаговое
        sql.Should().Contain("CREATE DATABASE \"shop\"");
        sql.Should().Contain("NOT EXISTS");
        sql.Should().Contain("pg_database");
        sql.Should().Contain("datname = 'shop'");
    }

    [Fact]
    public void BuildRolesSql_AllThreeRoles_WithPasswordsAndReplication()
    {
        // Arrange — три роли бакетного слоя (§4 доки 11), app-пароль из etcd-кредов
        // Act
        var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(Secrets, new AppCredentials("app", TestAppPassword)));

        // Assert: роли создаются идемпотентно (NOT EXISTS pg_roles) с LOGIN
        sql.Should().Contain("rolname = 'app'");
        sql.Should().Contain("rolname = 'bucket_admin'");
        sql.Should().Contain("rolname = 'bucket_mover'");
        sql.Should().Contain($"CREATE ROLE \"app\" LOGIN PASSWORD ''{TestAppPassword}''"); // gexec: кавычки удвоены
        sql.Should().Contain($"CREATE ROLE \"bucket_admin\" LOGIN PASSWORD ''{TestAdminPassword}''");
        // mover — атрибут REPLICATION (подписки переездов, P2/P3)
        sql.Should().Contain($"CREATE ROLE \"bucket_mover\" LOGIN REPLICATION PASSWORD ''{TestMoverPassword}''");
        // идемпотентность: каждая роль — только при отсутствии
        Regex.Matches(sql, "NOT EXISTS").Count.Should().BeGreaterThanOrEqualTo(3);
        // pg_monitor для bucket_admin — в BuildRoleExecSql (DO-блок, не guard-SELECT)
        var execSql = string.Join("\n", DatabaseProvisioner.BuildRoleExecSql());
        execSql.Should().Contain("pg_monitor").And.Contain("bucket_admin");
        sql.Should().NotContain("pg_monitor", "pg_monitor belongs to BuildRoleExecSql, not the guard-SELECT");
    }

    [Fact]
    public void BuildRoleGuardsSql_AppRole_FromAppCredentials()
    {
        // Arrange — креды из etcd-ключей (после ensure)
        var app = new AppCredentials("app", "AppPw1234567890AppPw1234567890");

        // Act
        var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(Secrets, app));

        // Assert — app-роль из кредов (env AppPassword больше не источник)
        sql.Should().Contain("CREATE ROLE \"app\" LOGIN PASSWORD ''AppPw1234567890AppPw1234567890'''");
        sql.Should().Contain("bucket_admin");
        sql.Should().Contain("bucket_mover");
    }

    [Fact]
    public void BuildAlterAppPasswordSql_EscapesAndTargetsApp()
    {
        // Arrange
        var app = new AppCredentials("app", "pw'with'quotes");

        // Act
        var sql = DatabaseProvisioner.BuildAlterAppPasswordSql(app);

        // Assert — прямой литерал (одинарный Escape), идемпотентный текст
        sql.Should().Be("ALTER ROLE \"app\" PASSWORD 'pw''with''quotes';");
    }

    [Fact]
    public void BuildSchemasSql_GrantsParameterizedAppUser()
    {
        // Act — кастомное имя app-роли
        var sql = DatabaseProvisioner.BuildSchemasSql("shop", [1], "bucket_admin", "appsvc");

        // Assert — гранты параметризованы app-именем (без хардкода "app")
        sql.Should().Contain("TO \"appsvc\", \"bucket_admin\", \"bucket_mover\"");
        sql.Should().Contain("TO \"appsvc\", \"bucket_admin\"");
        sql.Should().NotContain(" \"app\",");
    }

    [Fact]
    public void BuildSchemasSql_BucketSchemasWithGrants_AllRoles()
    {
        // Arrange — схемы бакетов шарда: 1 и 7
        // Act
        var sql = DatabaseProvisioner.BuildSchemasSql("shop", [1, 7]);

        // Assert: идемпотентные схемы + полный набор грантов §4 доки 11
        sql.Should().Contain("CREATE SCHEMA IF NOT EXISTS bucket_1");
        sql.Should().Contain("CREATE SCHEMA IF NOT EXISTS bucket_7");
        sql.Should().NotContain("bucket_0");

        // app-роль: write-доступ (INSERT/UPDATE/DELETE/TRUNCATE) + sequences
        sql.Should().Contain("GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_1 TO \"app\", \"bucket_admin\"");
        sql.Should().Contain("GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_1 TO \"app\", \"bucket_admin\"");

        // bucket_mover: SELECT всех таблиц схемы + USAGE (подписка/COPY, P2)
        sql.Should().Contain("GRANT SELECT ON ALL TABLES IN SCHEMA bucket_1 TO \"bucket_mover\"");
        sql.Should().Contain("GRANT USAGE ON SCHEMA bucket_1");

        // то же для bucket_7
        sql.Should().Contain("GRANT USAGE ON SCHEMA bucket_7");
        sql.Should().Contain("GRANT SELECT ON ALL TABLES IN SCHEMA bucket_7 TO \"bucket_mover\"");

        // pg_monitor — в BuildRoleExecSql (не в BuildSchemasSql)
        sql.Should().NotContain("pg_monitor");
    }

    [Fact]
    public void BuildRolesSql_PasswordWithQuote_Escaped()
    {
        // Arrange — одинарная кавычка в пароле: SQL-инъекция невозможна
        // Act
        var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(Secrets, new AppCredentials("app", "o'brien")));

        // Assert: кавычка удвоена
        sql.Should().Contain("PASSWORD ''o''''brien''");
    }

    [Fact]
    public void Builders_InvalidIdentifiers_Rejected()
    {
        // Arrange — dbname/кластер с недопустимыми символами (шаблон init-cluster.sh)
        // Act + Assert: guard валидации идентификаторов — исключение (не SQL)
        var act = () => DatabaseProvisioner.BuildCreateDatabaseSql("shop; DROP TABLE x");
        act.Should().Throw<ArgumentException>();
    }

    // ===== Редакция пароля в DSN (rework №2: «Password=…;» Npgsql + libpq) =====

    [Fact]
    public void Redact_NpgsqlDsn_BuildAdminDsnFormat_Masked()
    {
        // Arrange — внутренний DSN BuildAdminDsn: «Password=» с большой буквы,
        // разделитель «;», пароль в конце строки
        var dsn = DatabaseProvisioner.BuildAdminDsn("h1", 5432, "shop", Secrets);

        // Act
        var redacted = DatabaseProvisioner.Redact(dsn);

        // Assert: пароль замаскирован (не утекает в journal.last_error/логи),
        // остальная часть DSN не тронута
        redacted.Should().NotContain("su-pw");
        redacted.Should().Be("Host=h1;Port=5432;Database=shop;Username=postgres;password=***;SSL Mode=Require;Trust Server Certificate=true");
    }

    [Fact]
    public void Redact_NpgsqlDsn_PasswordInMiddle_SemicolonBoundary()
    {
        // Arrange — «Password=…;» в середине строки: хвост после «;» сохраняется
        const string dsn = "Host=h1;Password=s3cret;Timeout=10";

        // Act
        var redacted = DatabaseProvisioner.Redact(dsn);

        // Assert
        redacted.Should().Be("Host=h1;password=***;Timeout=10");
    }

    [Fact]
    public void Redact_LibpqDsn_SpaceBoundary()
    {
        // Arrange — libpq-формат dsn-ключей (пробел-разделитель, пароль в конце)
        const string dsn = "host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin password=s3cret";

        // Act
        var redacted = DatabaseProvisioner.Redact(dsn);

        // Assert
        redacted.Should().Be("host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin password=***");
    }

    [Fact]
    public void Redact_LibpqQuotedPassword_MaskedFully()
    {
        // Arrange — libpq-пароль с пробелами пишется в кавычках: маскируем целиком
        // (пароль генерируется в рантайме: секрет не зашивается в исходник).
        const char quote = '\'';
        var secret = $"{RandomAlphaNum(8)} {RandomAlphaNum(8)}";
        var cred = $"password={quote}{secret}{quote}";
        var dsn = $"host=h1 {cred} dbname=shop";

        // Act
        var redacted = DatabaseProvisioner.Redact(dsn);

        // Assert
        redacted.Should().Be("host=h1 password=*** dbname=shop");
    }

    // Криптостойкий генератор букв/цифр (токен длины n).
    private static string RandomAlphaNum(int length)
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(length);
        return new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }

    [Fact]
    public void Redact_NoPassword_Unchanged()
    {
        // Arrange — DSN без пароля (dsn-ключ панели user=bucket_admin)
        const string dsn = "host=h1 port=5432 dbname=shop user=bucket_admin";

        // Act + Assert
        DatabaseProvisioner.Redact(dsn).Should().Be(dsn);
    }

    // AAA (arch/19 §7): ensure роли backup_exec = CREATE-if-absent И
    // безусловный ALTER-выравниватель — pg_authid могут перезаписать с чужим
    // значением (второй писатель); CREATE-only гвард молчал вечно, инвентарь
    // приёмника падал 28P01 при живой сессии стрима (факт стенда).
    [Fact]
    public void BuildBackupExecRoleGuardSql_ContainsCreateIfAbsent_AndAlterAlignment()
    {
        // Arrange
        const string password = "pw-test-123";

        // Act
        var sql = DatabaseProvisioner.BuildBackupExecRoleGuardSql(password);

        // Assert: DO-блок — CREATE-if-absent и ALTER-выравниватель в одном
        // исполнении (порядок «создать → выровнять» гарантирован).
        sql.Should().StartWith("DO $$ BEGIN");
        sql.Should().Contain("IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'backup_exec')");
        sql.Should().Contain($"CREATE ROLE \"backup_exec\" LOGIN REPLICATION PASSWORD '{password}';");
        sql.Should().Contain($"ALTER ROLE \"backup_exec\" WITH LOGIN REPLICATION PASSWORD '{password}';");
        sql.Should().Contain("END $$;");
    }

    // AAA (arch/19 §7): проверка соответствия пароля — пересчёт verifier'а от
    // etcd-значения по СОЛЬ СОХРАНЁННОГО; md5-формат.
    [Fact]
    public void BackupExecPasswordMatches_Md5Format_VerifiesPlaintext()
    {
        // Arrange: PG хранит 'md5'+md5hex(пароль+роль)
        const string password = "pw-test-123";
        var md5 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(
            System.Text.Encoding.UTF8.GetBytes(password + DatabaseProvisioner.BackupExecRole))).ToLowerInvariant();
        var stored = "md5" + md5;

        // Act + Assert
        DatabaseProvisioner.BackupExecPasswordMatches(stored, password).Should().BeTrue();
        DatabaseProvisioner.BackupExecPasswordMatches(stored, "other-pw").Should().BeFalse();
    }

    // AAA (arch/19 §7): SCRAM-SHA-256 формат — SaltedPassword=PBKDF2,
    // ClientKey=HMAC("Client Key"), StoredKey=SHA256(ClientKey).
    [Fact]
    public void BackupExecPasswordMatches_ScramFormat_VerifiesPlaintext()
    {
        // Arrange: верификатор от известного пароля/соли (алгоритм PG)
        const string password = "pw-test-123";
        var salt = System.Text.Encoding.UTF8.GetBytes("0123456789abcdef");
        var salted = System.Security.Cryptography.Rfc2898DeriveBytes.Pbkdf2(
            password, salt, 4096, System.Security.Cryptography.HashAlgorithmName.SHA256, 32);
        using var hmac = new System.Security.Cryptography.HMACSHA256(salted);
        var clientKey = hmac.ComputeHash("Client Key"u8.ToArray());
        using var sha = System.Security.Cryptography.SHA256.Create();
        var storedKey = sha.ComputeHash(clientKey);
        var serverKey = hmac.ComputeHash("Server Key"u8.ToArray());
        var stored = $"SCRAM-SHA-256$4096:{Convert.ToBase64String(salt)}$" +
                     $"{Convert.ToBase64String(storedKey)}:{Convert.ToBase64String(serverKey)}";

        // Act + Assert: совпадает по паролю; чужой/битый/пустой — расхождение.
        DatabaseProvisioner.BackupExecPasswordMatches(stored, password).Should().BeTrue();
        DatabaseProvisioner.BackupExecPasswordMatches(stored, "other-pw").Should().BeFalse();
        DatabaseProvisioner.BackupExecPasswordMatches("garbage", password).Should().BeFalse();
        DatabaseProvisioner.BackupExecPasswordMatches(null, password).Should().BeFalse();
    }

    [Fact]
    public async Task ExecuteAsync_FailingDsn_ErrorContainsRedactedPassword()
    {
        // Arrange — битый keyword в DSN: Npgsql падает мгновенно (без ретраев),
        // сообщение ошибки несёт отредактированный DSN
        var provisioner = new DatabaseProvisioner();

        // Act
        var result = await provisioner.ExecuteAsync(
            $"Host=h1;Port=5432;Database=d;Username=postgres;Password={TestDsnPassword};BogusKeyword=1",
            "SELECT 1", CancellationToken.None);

        // Assert: ошибка завернута в Result, пароля в тексте нет
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("password=***");
        result.Error!.Message.Should().NotContain(TestDsnPassword);
    }
}
