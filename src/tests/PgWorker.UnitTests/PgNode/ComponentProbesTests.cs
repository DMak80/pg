using PgWorker.PgNode;
using Xunit;

// AAA: ComponentProbes — строки подключения проб фазы (б) (spec §3.6-3б,
// §8.17: Timeout=5 — кратный интервалу опроса; новых таймаут-констант нет).
public class ComponentProbesTests
{
    [Fact]
    public void PgConnectionString_Port5432Timeout5SuPassword()
    {
        // Arrange: доставленный SuPassword.

        // Act: строка подключения PG-пробы.
        var cs = ComponentProbes.PgConnectionString("su-secret");

        // Assert: 127.0.0.1:5432 под postgres, переданный пароль, Timeout=5.
        cs.Should().Contain("Host=127.0.0.1");
        cs.Should().Contain("Port=5432");
        cs.Should().Contain("Username=postgres");
        cs.Should().Contain("Password=su-secret");
        cs.Should().Contain("Timeout=5");
    }

    [Fact]
    public void DoormanConnectionString_Port6432SslRequireAppRole()
    {
        // Arrange: app-креды per-cluster etcd + dbname.

        // Act: строка подключения doorman-пробы (клиентский вход).
        var cs = ComponentProbes.DoormanConnectionString("app", "app-pw", "shop");

        // Assert: 127.0.0.1:6432, SslMode=Require, app-роль, Timeout=5.
        cs.Should().Contain("Host=127.0.0.1");
        cs.Should().Contain("Port=6432");
        cs.Should().Contain("Username=app");
        cs.Should().Contain("Password=app-pw");
        cs.Should().Contain("Database=shop");
        cs.Should().Contain("SslMode=Require");
        cs.Should().Contain("Timeout=5");
    }

    [Fact]
    public void TimeoutSec_IsFive()
    {
        // Arrange/Act/Assert: канонические 5 c (кратные тику, spec §8.17).
        ComponentProbes.TimeoutSec.Should().Be(5);
    }
}
