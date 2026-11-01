using PgWorker.PgNode;
using PgWorker.UnitTests.Provisioning;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

// AAA (П6.2): статусная машина агента — полный хендшейк → ready; ok-пинок при
// ready; 400 при пустом пароле/битом ключе; ≤3 попытки → start_failed; один
// компонент failed при живом втором; tool-путь → prepared; apply no-op /
// spilo-restart / doorman-restart / оба; apply не из ready → NotReady; apply
// при etcd-фейле → EtcdError. Сессия lease не гоняется: писатель статуса —
// фейк поверх словаря Fakes.FakeEtcd (txn по VALUE — тот же контракт, что
// у прод-сессии).
public class PgNodeMachineTests
{
    private static readonly PgNodeIdentity Identity =
        new("shop", "shard1", "shard1a", ["http://e1:2379"], null, null);

    private const string BootstrapKey = "/clusters/shop/shards/shard1/nodes/shard1a/bootstrap";
    private const string StatusKey = "/pgworker/nodes/shop/shard1/shard1a/status";

    private static string BootstrapJson(
        string spilo = "spilo-yaml", string cert = "CERT", string key = "KEY")
        => System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, string>
        {
            ["spilo_configuration"] = spilo,
            ["cert_pem"] = cert,
            ["key_pem"] = key,
        });

    // Валидная PEM-пара (IsValidForStart машины требует парсимую пару).    // Валидная PEM-пара (IsValidForStart машины требует парсимую пару).
    private static (string Cert, string Key) RealPemPair()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=test", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    // supervisorctl-фейк: журнал команд; настраиваемый провал start по
    // компоненту (первые N вызовов StartAsync(program) бросают).
    private sealed class FakeSupervisor : ISupervisorCommands
    {
        public readonly List<string> Commands = [];
        private readonly Dictionary<string, int> _startFailures = [];

        public void FailStartTimes(string program, int times) => _startFailures[program] = times;

        public Task StartAsync(string program, CancellationToken ct)
        {
            Commands.Add($"start {program}");
            if (_startFailures.TryGetValue(program, out var left) && left > 0)
            {
                _startFailures[program] = left - 1;
                throw new ApplicationException($"supervisorctl start {program}: exit 2 (fake)");
            }
            return Task.CompletedTask;
        }

        private readonly Dictionary<string, int> _restartFailures = [];

        public void FailRestartTimes(string program, int times) => _restartFailures[program] = times;

        public Task RestartAsync(string program, CancellationToken ct)
        {
            Commands.Add($"restart {program}");
            if (_restartFailures.TryGetValue(program, out var left) && left > 0)
            {
                _restartFailures[program] = left - 1;
                throw new ApplicationException($"supervisorctl restart {program}: exit 1 (fake)");
            }
            return Task.CompletedTask;
        }
    }

    // Фейк команд применения ротаций (П11.9): журнал в ОБЩИЙ список с
    // FakeSupervisor (порядок пулер→spilo-материал проверяется по нему);
    // настраиваемый провал reload (apply-ветка throws → error-факт машины).
    private sealed class FakePatroniApply(List<string> commands) : IPatroniApplyCommands
    {
        public List<string> Commands { get; } = commands;

        public int ReloadCalls;
        public readonly List<(string Yaml, string Current, string New)> ReloadArgs = [];
        public int RestartProgramCalls;
        private readonly Dictionary<int, int> _reloadFailures = [];

        public void FailReloadTimes(int times) => _reloadFailures[1] = times;

        public Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
            string restPasswordNew, CancellationToken ct)
        {
            ReloadCalls++;
            ReloadArgs.Add((spiloConfigurationYaml, restPasswordCurrent, restPasswordNew));
            Commands.Add("reload rest-pair");
            if (_reloadFailures.TryGetValue(1, out var left) && left > 0)
            {
                _reloadFailures[1] = left - 1;
                throw new ApplicationException("patroni reload: 401 (fake)");
            }
            return Task.CompletedTask;
        }

        public Task RestartProgramAsync(CancellationToken ct)
        {
            RestartProgramCalls++;
            Commands.Add("restart spilo(program)");
            return Task.CompletedTask;
        }
    }

    // Фейк-писатель статусного ключа поверх словаря etcd-фейка: StartAsync —
    // put not_initialized, TryTransitionAsync — txn по VALUE (контракт
    // прод-сессии), Update+Flush — put последнего статуса.
    private sealed class FakeStatusWriter(Fakes.FakeEtcd etcd) : INodeStatusWriter
    {
        public readonly List<string> Published = [];
        public int StartCalls;
        public Action? OnBeforeTransition; // инжекция гонки keepalive-put
        private string _lastJson = "";

        public async Task<bool> StartAsync(PgNodeStatus initial, CancellationToken ct)
        {
            StartCalls++;
            Published.Add(initial.State);
            _lastJson = initial.ToJson();
            await etcd.PutAsync("e", StatusKey, _lastJson, lease: 1, ct);
            return true;
        }

        public void Update(PgNodeStatus status)
        {
            Published.Add(status.State);
            _lastJson = status.ToJson();
        }

        public async Task<bool> FlushAsync(CancellationToken ct)
        {
            if (_lastJson.Length == 0)
                return true;
            await etcd.PutAsync("e", StatusKey, _lastJson, lease: 1, ct);
            return true;
        }

        public async Task<bool> TryTransitionAsync(string newValueJson, string expectedValueJson, CancellationToken ct)
        {
            OnBeforeTransition?.Invoke(); // гонка keepalive-put того же значения
            var txn = TxnRequest.Of(
                [TxnCompare.ValueEqual(StatusKey, expectedValueJson)],
                [new TxnOp.Put(StatusKey, newValueJson, 1)]);
            var result = await etcd.TxnAsync("e", txn, ct);
            if (result.IsSuccess && result.Value.Succeeded)
                _lastJson = newValueJson;
            return result.IsSuccess && result.Value.Succeeded;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Обвязка кейса: etcd-фейк с bootstrap-ключом, машина с фейками, загрузка
    // ключа (PollBootstrapOnce), not_initialized в статусном ключе.
    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd { get; } = new();
        public FakeSupervisor Supervisor { get; } = new();
        public FakePatroniApply PatroniApply { get; }
        public FakeStatusWriter Status { get; }
        public PgNodeMachine Machine { get; }

        // Роль ноды для серт-ветки apply (гвард П11.9: Master → reload без
        // фиксации ревизии; не-Master → рестарт программы). По умолчанию —
        // реплика (sync).
        public WalReceiverRole Role { get; set; } = WalReceiverRole.Sync;

        private Rig((string Cert, string Key) pem)
        {
            Pem = pem;
            Status = new FakeStatusWriter(Etcd);
            PatroniApply = new FakePatroniApply(Supervisor.Commands);
            Machine = new PgNodeMachine(
                Identity, Etcd, ["http://e1:2379"], Supervisor,
                doormanTemplate: "pooler ${PGW_SU_PASSWORD} template",
                statusWriter: () => Status,
                patroniApply: PatroniApply);
            Machine.RenderRootDirOverride = RenderDir;
            Machine.RoleProbeOverride = _ => Task.FromResult(Role);
            Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
                BootstrapJson(cert: pem.Cert, key: pem.Key), 7, 1);
            Etcd.Store[StatusKey] = new Fakes.FakeEtcd.Entry(
                new PgNodeStatus(PgNodeStatus.NotInitialized, null,
                    new Dictionary<string, string>(), 1).ToJson(), 3, 1);
            // app-креды/dbname (per-cluster ключи фазы (б): без них doorman-проба
            // не выполняется и машина ждёт ready БЕЗ бюджета — тест зависает).
            Etcd.Store["/clusters/shop/app_user"] = new Fakes.FakeEtcd.Entry("app", 2, 1);
            Etcd.Store["/clusters/shop/app_password"] = new Fakes.FakeEtcd.Entry("app-pw", 2, 1);
            Etcd.Store["/clusters/shop/config"] =
                new Fakes.FakeEtcd.Entry("""{"dbname":"shop"}""", 2, 1);
        }

        // Фабрика: расстановка etcd-стейта + загрузка ключа (PollBootstrapOnce)
        // без блокирующего Wait в конструкторе (xUnit1031-канон). Корень
        // рендера — temp (на хосте /run/pgw недоступен), Dispose подчищает.
        public static async Task<Rig> CreateAsync((string Cert, string Key) pem)
        {
            var rig = new Rig(pem);
            await rig.Machine.PollBootstrapOnceAsync(CancellationToken.None);
            return rig;
        }

        public (string Cert, string Key) Pem;
        public (string Cert, string Key) MachinePem => Pem;
        public readonly string RenderDir =
            Path.Combine(Path.GetTempPath(), "pgw-machine-" + Guid.NewGuid().ToString("N"));

        public ValueTask DisposeAsync()
        {
            try { Directory.Delete(RenderDir, recursive: true); } catch { /* нет — ок */ }
            return ValueTask.CompletedTask;
        }

        public void ProbeAllReady() => Machine.ProbeOverride = _ => Task.FromResult(true);

        public async Task<HandshakeResult> HandshakeAsync(
            string su = "su-secret", string standby = "standby-secret", bool tool = false)
        {
            var result = await Machine.HandshakeAsync(su, standby, tool, CancellationToken.None);
            if (Machine.PhaseTask is { } phase)
                await phase;
            return result;
        }
    }

    // (1) Полный хендшейк: not_initialized → txn initializing → рендер →
    // старт обоих компонентов (оба в журнале) → пробы → ready
    // с applied_revision == mod_revision bootstrap-ключа.
    [Fact]
    public async Task Handshake_FullFlow_ReachesReady()
    {
        // Arrange: валидный bootstrap-ключ; пробы сразу готовы.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();

        // Act: команда доставки (пара per-install паролей).
        var result = await rig.HandshakeAsync();

        // Assert: Started; статус ready с applied_revision==7; оба компонента
        // стартованы; серт листенера опубликован из ключа.
        result.Should().Be(HandshakeResult.Started);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        rig.Machine.AppliedRevision.Should().Be(7);
        rig.Supervisor.Commands.Should().Contain("start spilo");
        rig.Supervisor.Commands.Should().Contain("start pg_doorman");
        rig.Machine.ServerCertificate.Should().NotBeNull();
    }

    // (2) Повторный хендшейк при ready → Nop без новых start'ов.
    [Fact]
    public async Task Handshake_AtReady_IsNop()
    {
        // Arrange: нода доведена до ready первым хендшейком.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var commandsAfterFirst = rig.Supervisor.Commands.Count;

        // Act: повторный пинок.
        var second = await rig.Machine.HandshakeAsync("su", "sb", false, CancellationToken.None);

        // Assert: Nop, никаких новых стартов.
        second.Should().Be(HandshakeResult.Nop);
        rig.Supervisor.Commands.Should().HaveCount(commandsAfterFirst);
    }

    // (2а) Гонка keepalive: put ТОГО ЖЕ значения not_initialized между чтением
    // статуса и txn (mod_revision вырос) — txn по ValueEqual успешен, переход
    // не ломается (ложных Nop нет).
    [Fact]
    public async Task Handshake_KeepalivePutOfSameValue_DoesNotBreakTransition()
    {
        // Arrange: хук «конкурентный keepalive-put» перед txn.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        rig.Status.OnBeforeTransition = () =>
        {
            // keepalive-сессии: то же значение, mod_revision растёт.
            var current = rig.Etcd.Store[StatusKey];
            rig.Etcd.Store[StatusKey] = current with { ModRevision = current.ModRevision + 1 };
        };

        // Act: хендшейк.
        var result = await rig.HandshakeAsync();

        // Assert: Started (compare по VALUE не сорван).
        result.Should().Be(HandshakeResult.Started);
    }

    // (2б) Параллельный писатель ИЗМЕНИЛ значение (уже initializing) — txn
    // проигран → Nop без запуска фаз.
    [Fact]
    public async Task Handshake_ParallelWriterChangedValue_IsNop()
    {
        // Arrange: статус уже initializing (другой писатель).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.Etcd.Store[StatusKey] = new Fakes.FakeEtcd.Entry(
            new PgNodeStatus(PgNodeStatus.Initializing, null,
                new Dictionary<string, string>(), 1).ToJson(), 3, 1);

        // Act: команда доставки.
        var result = await rig.Machine.HandshakeAsync("su", "sb", false, CancellationToken.None);

        // Assert: Nop, стартов нет.
        result.Should().Be(HandshakeResult.Nop);
        rig.Supervisor.Commands.Should().BeEmpty();
    }

    // (3) Пустой su_password → Invalid, статус не меняется.
    [Fact]
    public async Task Handshake_EmptyPassword_Invalid()
    {
        // Arrange: валидный ключ, not_initialized.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        var before = rig.Etcd.Store[StatusKey];

        // Act
        var result = await rig.Machine.HandshakeAsync("", "sb", false, CancellationToken.None);

        // Assert: Invalid; значение статусного ключа не изменилось.
        result.Should().Be(HandshakeResult.Invalid);
        rig.Etcd.Store[StatusKey].Should().Be(before);
    }

    // (4) Битый bootstrap-ключ → Invalid (валидация аргументов хендшейка).
    [Fact]
    public async Task Handshake_InvalidBootstrapKey_Invalid()
    {
        // Arrange: мусор в bootstrap-ключе (Parse не пройдёт — машина не
        // закеширует данные).
        await using var rig = await Rig.CreateAsync(("GARBAGE", "GARBAGE-KEY"));

        // Act
        var result = await rig.Machine.HandshakeAsync("su", "sb", false, CancellationToken.None);

        // Assert
        result.Should().Be(HandshakeResult.Invalid);
    }

    // (5) start pg_doorman падает 3 раза → start_failed c error,
    // components.doorman=failed; spilo стартуется (второй не блокирован).
    [Fact]
    public async Task Handshake_DoormanStartFailsThreeTimes_StartFailed()
    {
        // Arrange: doorman старт падает 3 раза.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.Supervisor.FailStartTimes("pg_doorman", 3);

        // Act
        var result = await rig.HandshakeAsync();

        // Assert: start_failed; попыток ровно 3; spilo в журнале старта.
        result.Should().Be(HandshakeResult.Started);
        rig.Machine.State.Should().Be(PgNodeStatus.StartFailed);
        rig.Supervisor.Commands.Count(c => c == "start pg_doorman").Should().Be(3);
        rig.Supervisor.Commands.Should().Contain("start spilo");
    }

    // (6) is_tool_start=true → рендер без start, ready, components=prepared,
    // select-пробы не вызывались.
    [Fact]
    public async Task Handshake_ToolStart_PreparedNoProbes()
    {
        // Arrange: счётчик проб через оверрайд-обёртку.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        var probeCalls = 0;
        rig.Machine.ProbeOverride = _ =>
        {
            probeCalls++;
            return Task.FromResult(true);
        };

        // Act: tool-старт.
        var result = await rig.HandshakeAsync(tool: true);

        // Assert: ready, стартов нет, проб нет.
        result.Should().Be(HandshakeResult.Started);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        rig.Machine.AppliedRevision.Should().Be(7);
        rig.Supervisor.Commands.Should().BeEmpty();
        probeCalls.Should().Be(0);
    }

    // (6a) Окно ротации ETCD-FIRST: doorman-проба false (28P01 при OLD
    // pg_authid в открытом окне) — waiting-повтор БЕЗ бюджета (не
    // start_failed): пробы циклятся на реальных 5-с интервалах машины
    // (фейковый TimeProvider с корректным ITimer-контрактом — отдельная
    // задача; ускорение юнита не стоит риска флейка таймеров).
    [Fact]
    public async Task Handshake_ProbeFalseWaits_ThenTrueReachesReady()
    {
        // Arrange: первые ДВАДЦАТЬ проб false (28P01-окно держится), затем true.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        var calls = 0;
        rig.Machine.ProbeOverride = _ =>
        {
            calls++;
            return Task.FromResult(calls > 20);
        };

        // Act
        var result = await rig.HandshakeAsync();

        // Assert: waiting-повтор — НЕ start_failed при длительной false-пробе;
        // после true — ready. Поллинг состояния (фаза (б) без бюджета, clock
        // рига мгновенный — пробы циклятся быстро).
        result.Should().Be(HandshakeResult.Started);
        calls.Should().BeGreaterThan(10, "the probe loops without budget while 28P01 holds");
        for (var i = 0; i < 200 && rig.Machine.State != PgNodeStatus.Ready; i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        rig.Supervisor.Commands.Should().NotBeEmpty("components were started");
    }

    // (6c) Apply на ready-ноде оставляет машину ready (фоновое применение —
    // рестарты компонентов не меняют состояние, ETCD-FIRST окно).
    [Fact]
    public async Task Apply_OnReady_StateStaysReady()
    {
        // Arrange: нода ready; ключ перезаписан с новым YAML (spilo-материал).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-window", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: Ok; машина остаётся ready (не initializing).
        apply.Ok.Should().BeTrue();
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
    }

    // (7) Ошибка рендера (недоступный корень рендера через internal-seam) →
    // start_failed.
    [Fact]
    public async Task Handshake_RenderError_StartFailed()
    {
        // Arrange: корень рендера — ФАЙЛ по месту каталога (запись в него
        // как в каталог невозможна → рендер падает).
        var blocker = Path.Combine(Path.GetTempPath(), $"pgw-block-{Guid.NewGuid():N}");
        File.WriteAllText(blocker, "x");
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.Machine.RenderRootDirOverride = Path.Combine(blocker, "run", "pgw");

        try
        {
            // Act: хендшейк штатного пути.
            var result = await rig.HandshakeAsync();

            // Assert: машина в start_failed (рендер упал, компоненты не
            // запускались).
            result.Should().Be(HandshakeResult.Started);
            rig.Machine.State.Should().Be(PgNodeStatus.StartFailed);
            rig.Supervisor.Commands.Should().BeEmpty();
        }
        finally
        {
            File.Delete(blocker);
        }
    }

    // (8) apply при ready-ноде с тем же ключом → no-op (рестартов нет).
    [Fact]
    public async Task Apply_SameRevision_NoOp()
    {
        // Arrange: нода доведена до ready.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var commandsAfterHandshake = rig.Supervisor.Commands.Count;

        // Act: apply при неизменённом ключе.
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);

        // Assert: Ok с прежней ревизией; рестартов нет.
        apply.Ok.Should().BeTrue();
        apply.AppliedRevision.Should().Be(7);
        rig.Supervisor.Commands.Skip(commandsAfterHandshake)
            .Should().NotContain(c => c.StartsWith("restart", StringComparison.Ordinal));
    }

    // (9) apply с изменённой spilo_configuration (rest-пара) → reload Patroni
    // (ApplyRestPairAsync); рестарт Patroni-программы/контейнера для пароля
    // ЗАПРЕЩЁН (arch/14 §5 I — П11.9).
    [Fact]
    public async Task Apply_RestPairChanged_ReloadsPatroni()
    {
        // Arrange: нода ready; ключ перезаписан с новым YAML.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-v2", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!; // применение — фоновая задача: дождаться её

        // Assert: Ok с новой ревизией; rest-пара — reload'ом (фейк), рестарт
        // программы НЕ вызывался, рестартов supervisor'а нет.
        apply.Ok.Should().BeTrue();
        apply.AppliedRevision.Should().Be(8);
        rig.PatroniApply.ReloadCalls.Should().Be(1);
        rig.PatroniApply.ReloadArgs.Single().Yaml.Should().Be("spilo-yaml-v2");
        rig.PatroniApply.RestartProgramCalls.Should().Be(0);
        rig.Supervisor.Commands.Should().NotContain(c => c.StartsWith("restart", StringComparison.Ordinal));
    }

    // (10) apply с изменённым только doorman-материалом (подстановка пары в
    // шаблон при неизменном серт/YAML) → только restart pg_doorman; Patroni
    // команд (reload/рестарт программы) нет.
    [Fact]
    public async Task Apply_DoormanMaterialChanged_RestartsDoormanOnly()
    {
        // Arrange: нода ready; смена SuPassword меняет только отрендеренный
        // doorman-конфиг (seam теста: пара доставляется хендшейком); ключ
        // перезаписан тем же содержимым (mod растёт — put идемпотентен по
        // содержимому, серт/YAML не менялись).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Machine.SetSuPasswordForTests("new-su-password");
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!; // применение — фоновая задача: дождаться её

        // Assert: Ok; рестарт только pg_doorman; Patroni-команд нет.
        apply.Ok.Should().BeTrue();
        rig.Supervisor.Commands.Should().Contain("restart pg_doorman");
        rig.PatroniApply.ReloadCalls.Should().Be(0);
        rig.PatroniApply.RestartProgramCalls.Should().Be(0);
    }

    // (11) apply с изменённым cert_pem/key_pem на РЕПЛИКЕ → рестарт
    // Patroni-программы (покрывает серт и rest-пароль — reload НЕ выполняется),
    // doorman ПЕРВЫЙ (spec §3.6-4); ревизия фиксируется.
    [Fact]
    public async Task Apply_CertChanged_Replica_RestartsProgram()
    {
        // Arrange: нода ready (роль реплики — по умолчанию рига); ключ
        // перезаписан с НОВЫМ сертом.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var (cert2, key2) = RealPemPair();
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(cert: cert2, key: key2), 9, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: Ok; рестарт программы после пулера; reload НЕ выполнялся
        // (рестарт покрыл оба материала); ревизия зафиксирована.
        apply.Ok.Should().BeTrue();
        apply.AppliedRevision.Should().Be(9);
        rig.PatroniApply.RestartProgramCalls.Should().Be(1);
        rig.PatroniApply.ReloadCalls.Should().Be(0);
        var doormanIdx = rig.Supervisor.Commands.IndexOf("restart pg_doorman");
        var programIdx = rig.Supervisor.Commands.IndexOf("restart spilo(program)");
        doormanIdx.Should().BeGreaterThan(-1);
        programIdx.Should().BeGreaterThan(-1);
        doormanIdx.Should().BeLessThan(programIdx, "doorman restarts before the spilo program");
        rig.Machine.ServerCertificate.Should().NotBeNull();
    }

    // (11а) П11.9 гвард роли: cert changed на МАСТЕРЕ — серт агентом НЕ
    // применяется (пересоздание воркером, R3-СЕРТ вне тикета); пароль
    // применяется reload'ом; applied_revision НЕ фиксируется — ready со
    // СТАРОЙ ревизией и error-фактом.
    [Fact]
    public async Task Apply_CertChanged_Master_ReloadOnly_RevisionNotFixed()
    {
        // Arrange: нода ready; роль — мастер; ключ с НОВЫМ сертом (и YAML).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Role = WalReceiverRole.Master;
        var (cert2, key2) = RealPemPair();
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-v2", cert: cert2, key: key2), 9, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: рестарта программы нет; reload был; applied_revision осталась
        // старой (7); машина ready.
        rig.PatroniApply.RestartProgramCalls.Should().Be(0);
        rig.PatroniApply.ReloadCalls.Should().Be(1);
        apply.Ok.Should().BeTrue();
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        rig.Machine.AppliedRevision.Should().Be(7,
            "cert on master is deferred to worker recreation — revision is not fixed");
        var lastPublished = rig.Status.Published.Last();
        lastPublished.Should().Be(PgNodeStatus.Ready);
    }

    // (11б) комбинация «серт+пара» на РЕПЛИКЕ: рестарт программы покрывает
    // ОБА материала — reload не выполняется (П11.9 в).
    [Fact]
    public async Task Apply_RestAndCertChanged_Replica_ProgramCoversBoth()
    {
        // Arrange: нода ready; ключ с НОВЫМ YAML и НОВЫМ сертом; роль реплики.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var (cert2, key2) = RealPemPair();
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-v2", cert: cert2, key: key2), 9, 2);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: рестарт программы есть; reload нет; ревизия зафиксирована.
        apply.Ok.Should().BeTrue();
        apply.AppliedRevision.Should().Be(9);
        rig.PatroniApply.RestartProgramCalls.Should().Be(1);
        rig.PatroniApply.ReloadCalls.Should().Be(0);
    }

    // (12) apply не из ready → NotReady.
    // Т7а (MR-#11): повторный handshake при initializing → Nop РЕШАЕТСЯ по
    // текущему состоянию (txn-переход не повторяется — Txn-счётчик фейка
    // не вырос), фазы не запускаются второй раз.
    [Fact]
    public async Task Handshake_RepeatWhileInitializing_NopWithoutSecondRun()
    {
        // Arrange: полная машина через handshake → ready (риг ждёт фазы).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        var txnsAfterFirst = rig.Etcd.Txns.Count;
        var commandsAfterFirst = rig.Supervisor.Commands.Count;

        // Act: повторный handshake на ready-машине.
        var repeat = await rig.HandshakeAsync();

        // Assert: Nop решён по текущему состоянию (MR-#11: TryTransition
        // обновляет _current) — txn-счётчик не вырос (повторного txn-перехода
        // нет), фазы не запущены второй раз.
        repeat.Should().Be(HandshakeResult.Nop);
        rig.Etcd.Txns.Count.Should().Be(txnsAfterFirst);
        rig.Supervisor.Commands.Count.Should().Be(commandsAfterFirst);
    }

    // Т7б (MR-#12): Apply с падающим применением (reload) — applied НЕ
    // зафиксирован («полуприменённое» не маскируется); повторный Apply по ТОМУ
    // ЖЕ ключу с рабочим применением — выполняется, applied обновлён.
    [Fact]
    public async Task Apply_RestartFailure_AppliedNotFixed_ThenRetryApplies()
    {
        // Arrange: нода ready; ключ перезаписан с новым YAML; команда
        // применения бросает (reload реплики — «полуприменённое»).
        // Коулдаун анти-дребезга выключен — тест проверяет КАНОН немедленного
        // повтора (MR-#12); в проде повтор той же ревизии отложен на
        // ApplyRetryCooldownSec (анти-цикл stop/start спило, факт E2E).
        PgNodeMachine.ApplyRetryCooldownSec = 0;
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var appliedBefore = rig.Machine.AppliedRevision;
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-fail", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act 1: apply с падающим reload.
        rig.PatroniApply.FailReloadTimes(1);
        var apply1 = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;
        var reloadsAfterFail = rig.PatroniApply.ReloadCalls;

        // Assert 1: команда принята (Ok=true), но «полуприменённое» не
        // маскируется: applied в машине И в статус-ключе — СТАРАЯ ревизия
        // (ротатор/доставка честно повторяют Apply), материалы не
        // зафиксированы (применение повторится).
        apply1.Ok.Should().BeTrue("the command is accepted, the background apply failed");
        rig.Machine.AppliedRevision.Should().Be(appliedBefore,
            "MR-#12: the failed apply must NOT fix the applied revision");
        var statusAfterFail = rig.Etcd.Store[StatusKey];
        statusAfterFail.Value.Should().Contain(
            "\"applied_revision\":" + appliedBefore,
            "the published Ready carries the OLD revision (not the new mod)");
        statusAfterFail.Value.Should().Contain("patroni reload: 401 (fake)",
            "the failure fact is in error");

        // Act 2: повторный Apply по ТОМУ ЖЕ ключу с рабочим применением —
        // не no-op: reload ПОВТОРЯЕТСЯ (счётчик grew — материалы не были
        // зафиксированы упавшим применением).
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-fail", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);
        var apply2 = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert 2: Ok=true; reload grew; applied обновлён.
        apply2.Ok.Should().BeTrue("the retry applies the same key");
        rig.PatroniApply.ReloadCalls.Should()
            .BeGreaterThan(reloadsAfterFail, "the retry repeats the apply (not a no-op)");
        rig.Machine.AppliedRevision.Should().Be(8);
    }
    [Fact]
    public async Task Apply_NotReady_ReturnsNotReady()
    {
        // Arrange: машина без хендшейка (not_initialized).
        await using var rig = await Rig.CreateAsync(RealPemPair());

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);

        // Assert
        apply.Ok.Should().BeFalse();
        apply.NotReady.Should().BeTrue();
    }

    // (13) apply при etcd-фейле чтения ключа → EtcdError.
    [Fact]
    public async Task Apply_EtcdFailure_ReturnsEtcdError()
    {
        // Arrange: нода доведена до ready; затем GET-фейл (etcd недоступен).
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);

        // Act: включаем etcd-сбой, apply.
        rig.Etcd.GetFailure = _ => Result<Kv?>.Failed(new ApplicationException("etcd down (fake)"));
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);

        // Assert
        apply.EtcdError.Should().BeTrue();
        apply.Ok.Should().BeFalse();
    }
}
