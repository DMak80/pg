using FluentAssertions;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Сборка args контейнера (arch/21 §2, spec §4.5.1): канонический порядок
// флагов valkey-server из декларации и кредов — детерминизм сверки V3.
public class NodeArgsBuilderTests
{
    // AAA: канонический набор флагов valkey-server из декларации и кредов (arch/21 §2)
    [Fact]
    public void Build_канонический_набор_флагов()
    {
        // Arrange
        long maxmemory = 536870912;
        var policy = "allkeys-lru";

        // Act
        var args = NodeArgsBuilder.Build(maxmemory, policy, "ADMINPWD32SYMBOLSxxxxxxxxxxx", "APPPWD32SYMBOLSxxxxxxxxxxxxx");

        // Assert — порядок флагов канонический (детерминизм сверки V3), хвост TLS (t06)
        args.Should().Equal(
            "valkey-server",
            "--user", "default", "off",
            "--user", "admin", "on", ">ADMINPWD32SYMBOLSxxxxxxxxxxx", "~*", "+@all",
            "--user", "app", "on", ">APPPWD32SYMBOLSxxxxxxxxxxxxx", "~*", "+@read", "+@write",
            "--maxmemory", "536870912",
            "--maxmemory-policy", "allkeys-lru",
            "--save", "",
            "--appendonly", "no",
            "--tls-port", "6379", "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no");
    }

    [Fact]
    public void Build_ПустойАргументSave_РовноОдинПустойЭлемент()
    {
        // Arrange: persistence off (arch/21 §2) — `--save ""`.
        // Act
        var args = NodeArgsBuilder.Build(1, "noeviction", "a", "b");

        // Assert: РОВНО один пустой элемент после --save (не отсутствие и не ""-литерал).
        args.Should().Contain("--save").And.Contain("");
        args.Count(a => a.Length == 0).Should().Be(1);
        args.Should().NotContain("\"\"");
    }

    [Fact]
    public void Build_РазныеПаролиИлиMaxmemory_РазныеArgs()
    {
        // Arrange: два набора входов.
        var first = NodeArgsBuilder.Build(536870912, "allkeys-lru", "pwd1pwd1pwd1pwd1pwd1pwd1pwd1", "apppwdapppwdapppwdapppwdapppwd");
        var second = NodeArgsBuilder.Build(1073741824, "allkeys-lfu", "pwd2pwd2pwd2pwd2pwd2pwd2pwd2", "apppwdapppwdapppwdapppwdapppwd");

        // Assert: args — детерминированная функция входов (пересоздание собирает актуальные).
        first.Should().NotEqual(second);
    }

    // AAA: TLS-хвост канон arch/21 §2 (t06) — TLS-порт = контейнерный 6379,
    // plain закрыт, серты из /tls, клиенты без сертов (ACL).
    [Fact]
    public void Build_IncludesCanonicalTlsFlags()
    {
        // Arrange / Act
        var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", "adm", "app").ToArray();
        // Assert
        args.Should().Contain("--tls-port");
        args[Array.IndexOf(args, "--tls-port") + 1].Should().Be("6379");
        args.Should().Contain("--port");
        args[Array.IndexOf(args, "--port") + 1].Should().Be("0");
        args.Skip(args.Length - 14).Should().Equal(new[]
        {
            "--tls-port", "6379", "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no",
        });
    }

    // ── BuildCmd (env-TLS, arch/21 §2): cmd-обёртка раскатки PEM + exec ──

    // AAA: канонический args → ["sh","-c", раскатка §2 + exec с экранированными args]
    [Fact]
    public void BuildCmd_КаноническаяОбёрткаРаскаткиИExec()
    {
        // Arrange
        var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", "ADMINPWD32SYMBOLSxxxxxxxxxxx", "APPPWD32SYMBOLSxxxxxxxxxxxxx");

        // Act
        var cmd = NodeArgsBuilder.BuildCmd(args);

        // Assert — форма обёртки канона arch/21 §2 (env → /tls, exec valkey-server);
        // каждый arg экранирован литералом '…' (контракт shell-escape §4.1)
        cmd.Should().HaveCount(3);
        cmd[0].Should().Be("sh");
        cmd[1].Should().Be("-c");
        cmd[2].Should().StartWith(
            "umask 077; mkdir -p /tls; printf %s \"$VALKEY_TLS_CERT\" > /tls/node.crt; " +
            "printf %s \"$VALKEY_TLS_KEY\" > /tls/node.key; printf %s \"$VALKEY_TLS_CA\" > /tls/ca.pem; " +
            "exec 'valkey-server'");
    }

    // AAA: детерминизм обёртки — одинаковый args → побитово равная строка
    // (свежий серт в env случаен, идемпотентность V3 держится на Cmd-сверке).
    [Fact]
    public void BuildCmd_ДетерминизмОтArgs()
    {
        // Arrange
        var first = NodeArgsBuilder.Build(536870912, "allkeys-lru", "pwd1pwd1pwd1pwd1pwd1pwd1pwd1", "apppwdapppwdapppwdapppwdapppwd");
        var second = NodeArgsBuilder.Build(536870912, "allkeys-lru", "pwd1pwd1pwd1pwd1pwd1pwd1pwd1", "apppwdapppwdapppwdapppwdapppwd");

        // Act
        var cmd1 = NodeArgsBuilder.BuildCmd(first);
        var cmd2 = NodeArgsBuilder.BuildCmd(second);

        // Assert
        cmd2[2].Should().Be(cmd1[2]);
    }

    // AAA: пустой аргумент (--save '') survives экранирование литералом ''
    [Fact]
    public void BuildCmd_ПустойАргумент_СохраняетсяКакПустойЛитерал()
    {
        // Arrange — persistence off: `--save ""`
        var args = NodeArgsBuilder.Build(1, "noeviction", "a", "b");

        // Act
        var cmd = NodeArgsBuilder.BuildCmd(args);

        // Assert — пустой arg закавычен пустой парой кавычек (не исчез, не ""-литерал)
        cmd[2].Should().Contain("'--save' ''");
    }

    // AAA: пароли [A-Za-z0-9] не искажаются экранированием (ACL-префикс > — литерал)
    [Fact]
    public void BuildCmd_ПаролиБезСпецсимволов_НеИскажаются()
    {
        // Arrange
        const string adminPwd = "ADMINPWD32SYMBOLSxxxxxxxxxxx";
        var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", adminPwd, "APPPWD32SYMBOLSxxxxxxxxxxxxx");

        // Act
        var cmd = NodeArgsBuilder.BuildCmd(args);

        // Assert — ACL-токен >пароль передан литералом (без shell-редиректа)
        cmd[2].Should().Contain($"'>{adminPwd}'");
        cmd[2].Should().NotContain($"> {adminPwd}");
    }

    // AAA: обратный парсинг — строка обёртки содержит все флаги канона в исходном порядке
    [Fact]
    public void BuildCmd_СодержитВсеФлагиКанонаВПорядке()
    {
        // Arrange
        var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", "adm", "app");

        // Act
        var cmd = NodeArgsBuilder.BuildCmd(args);

        // Assert — каждый флаг канона в строке, порядок не перемешан
        var flags = new[] { "--user", "--maxmemory", "--maxmemory-policy", "--save", "--appendonly", "--tls-port", "--port", "--tls-cert-file", "--tls-key-file", "--tls-ca-cert-file", "--tls-auth-clients", "--tls-replication" };
        var positions = flags.Select(f => cmd[2].IndexOf($" '{f}' ", StringComparison.Ordinal) >= 0
            ? cmd[2].IndexOf($"'{f}'", StringComparison.Ordinal)
            : cmd[2].LastIndexOf($"'{f}'", StringComparison.Ordinal)).ToArray();
        positions.Should().OnlyContain(p => p > 0);
        positions.Should().BeInAscendingOrder();
        cmd[2].Should().Contain("'--tls-cert-file' '/tls/node.crt'");
        cmd[2].Should().Contain("'--tls-key-file' '/tls/node.key'");
        cmd[2].Should().Contain("'--tls-ca-cert-file' '/tls/ca.pem'");
    }
}
