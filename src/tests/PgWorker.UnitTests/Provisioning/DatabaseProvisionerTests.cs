using System.Text.RegularExpressions;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Sql;

namespace PgWorker.UnitTests.Provisioning;

// DatabaseProvisioner — SQL-механика БД/ролей/схем (задача 18): генерация
// идемпотентных текстов по эталону init-cluster.sh + гранты §4 доки 11.
public class DatabaseProvisionerTests
{
    private static readonly InstallSecrets Secrets = new(
        "su-pw", "standby-pw", "app-pw", "admin-pw", "mover-pw");

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
        // Arrange — три роли бакетного слоя (§4 доки 11), пароли из InstallSecrets
        // Act
        var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(Secrets));

        // Assert: роли создаются идемпотентно (NOT EXISTS pg_roles) с LOGIN
        sql.Should().Contain("rolname = 'app'");
        sql.Should().Contain("rolname = 'bucket_admin'");
        sql.Should().Contain("rolname = 'bucket_mover'");
        sql.Should().Contain("CREATE ROLE \"app\" LOGIN PASSWORD ''app-pw''"); // gexec: кавычки удвоены
        sql.Should().Contain("CREATE ROLE \"bucket_admin\" LOGIN PASSWORD ''admin-pw''");
        // mover — атрибут REPLICATION (подписки переездов, P2/P3)
        sql.Should().Contain("CREATE ROLE \"bucket_mover\" LOGIN REPLICATION PASSWORD ''mover-pw''");
        // идемпотентность: каждая роль — только при отсутствии
        Regex.Matches(sql, "NOT EXISTS").Count.Should().BeGreaterThanOrEqualTo(3);
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
    }

    [Fact]
    public void BuildRolesSql_PasswordWithQuote_Escaped()
    {
        // Arrange — одинарная кавычка в пароле: SQL-инъекция невозможна
        var secrets = Secrets with { AppPassword = "o'brien" };

        // Act
        var sql = string.Join("\n", DatabaseProvisioner.BuildRoleGuardsSql(secrets));

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
}
