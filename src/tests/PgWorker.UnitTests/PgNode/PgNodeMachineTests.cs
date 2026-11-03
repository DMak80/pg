using Microsoft.Extensions.Logging;
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
    // Мгновенный TimeProvider: таймеры задержек фазы (б) срабатывают
    // немедленно — юнит без реальных 5-с ожиданий (всё мокается). Callback
    // срабатывает и при СОЗДАНИИ таймера (dueTime из CreateTimer — Delay
    // не зовёт Change), и на каждый Change.
    private sealed class InstantTimeProvider : TimeProvider
    {
        public static readonly InstantTimeProvider Instance = new();

        private sealed class InstantTimer(TimerCallback callback, object? state) : ITimer
        {
            private TimerCallback? _callback = callback;

            public void Fire()
            {
                var cb = _callback;
                if (cb is not null)
                    Task.Run(() => cb(state));
            }

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                Fire();
                return true;
            }

            public TimeSpan Period { get; set; } = Timeout.InfiniteTimeSpan;
            public TimeSpan DueTime { get; set; } = TimeSpan.Zero;

            public void Dispose() => _callback = null;

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }

        public override ITimer CreateTimer(
            TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new InstantTimer(callback, state);
            if (dueTime != Timeout.InfiniteTimeSpan)
                timer.Fire();
            return timer;
        }
    }

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

        public Task ShutdownAsync(CancellationToken ct)
        {
            Commands.Add("shutdown");
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
        public readonly List<(string Yaml, string Current, string New, string Su, string Standby)> ReloadArgs = [];
        public int RestartProgramCalls;
        private readonly Dictionary<int, int> _reloadFailures = [];

        public void FailReloadTimes(int times) => _reloadFailures[1] = times;

        public Task ApplyRestPairAsync(string spiloConfigurationYaml, string restPasswordCurrent,
            string restPasswordNew, string pgSuPassword, string pgStandbyPassword, CancellationToken ct)
        {
            ReloadCalls++;
            ReloadArgs.Add((spiloConfigurationYaml, restPasswordCurrent, restPasswordNew,
                pgSuPassword, pgStandbyPassword));
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

    // Коллектор логов (ХВОСТ «молчащего stderr»): диагностика машины/применителя
    // обязана идти через ILogger хоста — тест фиксирует факт записей.
    public sealed class TestLogger : ILogger
    {
        public readonly List<(LogLevel Level, string Message)> Entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }

    // Фейк-писатель статусного ключа поверх словаря etcd-фейка: StartAsync —
    // put not_initialized, TryTransitionAsync — txn по VALUE (контракт
    // прод-сессии), Update+Flush — put последнего статуса.
    private sealed class FakeStatusWriter(Fakes.FakeEtcd etcd) : INodeStatusWriter
    {
        public readonly List<string> Published = [];
        public readonly List<string> Jsons = []; // ВСЕ публикации — для ассертов промежуточных фактов
        public int StartCalls;
        public Action? OnBeforeTransition; // инжекция гонки keepalive-put
        private string _lastJson = "";

        public async Task<bool> StartAsync(PgNodeStatus initial, CancellationToken ct)
        {
            StartCalls++;
            Published.Add(initial.State);
            _lastJson = initial.ToJson();
            Jsons.Add(_lastJson);
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
            Jsons.Add(_lastJson);
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
            {
                _lastJson = newValueJson;
                Jsons.Add(newValueJson);
            }
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
        public TestLogger Logger { get; } = new();

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
                clock: InstantTimeProvider.Instance,
                patroniApply: PatroniApply,
                logger: Logger);
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

    // (6b) Умная валидация пулера (решение пользователя): PG готов, doorman
    // fail → конфигурация doorman в лог (SU маскируется), рестарт pg_doorman
    // с обязательным перечитыванием app-пароля из etcd; перечитанный пароль
    // (etcd сменился между пробами) доезжает до пробы → ready.
    [Fact]
    public async Task Handshake_PgReadyDoormanFail_RestartsAndReReadsEtcdPassword()
    {
        // Arrange: pg-проба (:5432) всегда true; doorman-проба (:6432) true
        // только с «new-pw». Первый doorman-фейл — это первый опрос со
        // СТАРЫМ паролем: в этот момент etcd меняется на «new-pw» —
        // рестарт с инвалидацией кеша обязан перечитать и пройти.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.Machine.ProbeOverride = cs =>
        {
            if (cs.Contains("Port=6432"))
            {
                // Первый doorman-фейл — проба с ПЕРВИЧНЫМ паролем рига
                // («app-pw», кеш процесса); в этот момент etcd меняется на
                // «new-pw» — рестарт с инвалидацией кеша обязан перечитать.
                if (cs.Contains("app-pw"))
                {
                    rig.Etcd.Store["/clusters/shop/app_password"] =
                        new Fakes.FakeEtcd.Entry("new-pw", 5, 2);
                    return Task.FromResult(false);
                }
                return Task.FromResult(cs.Contains("new-pw"));
            }
            return Task.FromResult(true); // pg :5432 — готов
        };

        // Act
        var result = await rig.HandshakeAsync();

        // Assert: рестарт pg_doorman ровно один; after-restart проба с
        // перечитанным из etcd паролем дошла до ready; конфиг в логе (su
        // замаскирован, фактический su-secret в лог не попал).
        result.Should().Be(HandshakeResult.Started);
        rig.Supervisor.Commands.Count(c => c == "restart pg_doorman").Should().Be(1);
        rig.Machine.State.Should().Be(PgNodeStatus.Ready);
        rig.Logger.Entries.Should().Contain(e =>
            e.Message.Contains("restarting pg_doorman"));
        rig.Logger.Entries.Should().Contain(e =>
            e.Message.Contains("doorman configuration (su masked)")
            && e.Message.Contains("pooler ${PGW_SU_PASSWORD} template")
            && !e.Message.Contains("su-secret"));
    }

    // (6b-стоп) PG готов, doorman fail ПОСЛЕ рестарта ещё раз → конфигурация
    // в лог повторно, статус start_failed, supervisorctl shutdown (стоп ноды:
    // воркеровский надзор пересоздаёт ноду по факту не-running).
    [Fact]
    public async Task Handshake_DoormanFailsTwiceAfterPgReady_StopsNode()
    {
        // Arrange: doorman-проба (:6432) никогда не проходит, PG — всегда ok.
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.Machine.ProbeOverride = cs =>
            Task.FromResult(!cs.Contains("Port=6432"));

        // Act
        var result = await rig.HandshakeAsync();

        // Assert: рестарт ровно один (второй fail — не рестарт), shutdown
        // вызван, машина терминальна start_failed, конфиг в логе дважды.
        result.Should().Be(HandshakeResult.Started);
        rig.Supervisor.Commands.Count(c => c == "restart pg_doorman").Should().Be(1);
        rig.Supervisor.Commands.Should().Contain("shutdown");
        rig.Machine.State.Should().Be(PgNodeStatus.StartFailed);
        rig.Logger.Entries.Count(e =>
            e.Message.Contains("doorman configuration (su masked)")).Should().Be(2);
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
        // программы НЕ вызывался, рестартов supervisor'а нет; per-install
        // пароли PG (хендшейк) переданы команде — env регенерации
        // configure_spilo собирает postgresql.authentication с фактическими
        // паролями (ХВОСТ t05probe3a: без них секция пишется дефолтами).
        apply.Ok.Should().BeTrue();
        apply.AppliedRevision.Should().Be(8);
        rig.PatroniApply.ReloadCalls.Should().Be(1);
        rig.PatroniApply.ReloadArgs.Single().Yaml.Should().Be("spilo-yaml-v2");
        rig.PatroniApply.ReloadArgs.Single().Su.Should().Be("su-secret",
            "per-install su password is passed through to the regeneration env");
        rig.PatroniApply.ReloadArgs.Single().Standby.Should().Be("standby-secret",
            "per-install standby password is passed through to the regeneration env");
        rig.PatroniApply.RestartProgramCalls.Should().Be(0);
        rig.Supervisor.Commands.Should().NotContain(c => c.StartsWith("restart", StringComparison.Ordinal));
    }

    // (9b) неудачный виток применителя — диагностика через ILogger хоста
    // («молчащий stderr» красного прогона: Console.Error-строки применителя в
    // docker-лог не попали; с ILogger факт ошибки гарантированно логируется).
    [Fact]
    public async Task Apply_FailedAttempt_LogsErrorThroughLogger()
    {
        // Arrange: нода ready; ключ перезаписан; reload падает ОДИН раз
        // (первый виток — ошибка с логом, самоповтор — успех).
        PgNodeMachine.ApplyRetryCooldownSec = 0;
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-log", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act: пин — применитель: виток-ошибка (лог) + ретрай-успех.
        rig.PatroniApply.FailReloadTimes(1);
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: ошибка витка зафиксирована В ЛОГГЕРЕ (уровень Error,
        // текст факта); успешный итог тоже логируется (converged).
        apply.Ok.Should().BeTrue();
        rig.Machine.AppliedRevision.Should().Be(8, "the self-retry converged the material");
        rig.Logger.Entries.Should().Contain(e =>
                e.Level == LogLevel.Error
                && e.Message.Contains("apply attempt failed", StringComparison.Ordinal)
                && e.Message.Contains("patroni reload: 401 (fake)", StringComparison.Ordinal),
            "the failed applier attempt is logged through the host logger (docker-лог контейнера)");
        rig.Logger.Entries.Should().Contain(e =>
                e.Level == LogLevel.Information
                && e.Message.Contains("apply converged", StringComparison.Ordinal),
            "the converged outcome is logged as well");
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

    // Т7б (MR-#12 + arch/14 §5 I R3): Apply с падающим применением (reload) —
    // промежуточная публикация со СТАРОЙ ревизией и error-фактом
    // («полуприменённое» не маскируется), затем применитель САМ ретраит
    // (коулдаун-пауза 0) и доводит материал — applied обновлён БЕЗ второго
    // пина воркера (воркер пинает один раз на ревизию).
    [Fact]
    public async Task Apply_FailedReload_SelfRetryConverges_WithoutSecondPin()
    {
        // Arrange: нода ready; ключ перезаписан с новым YAML; reload падает
        // ОДИН раз (самоповтор применителя — второй вызов успешен).
        // Коулдаун-пауза 0 — тест немедленного самоповтора (MR-#12); в проде
        // пауза ApplyRetryCooldownSec (анти-цикл stop/start спило, факт E2E).
        PgNodeMachine.ApplyRetryCooldownSec = 0;
        await using var rig = await Rig.CreateAsync(RealPemPair());
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var appliedBefore = rig.Machine.AppliedRevision;
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(spilo: "spilo-yaml-fail", cert: rig.MachinePem.Cert, key: rig.MachinePem.Key), 8, 2);

        // Act: ОДИН пин — применитель сам ретраит после неудачи.
        rig.PatroniApply.FailReloadTimes(1);
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: команда принята (Ok=true); промежуточный факт неудачи
        // опубликован со СТАРОЙ ревизией и error (MR-#12); самоповтор довёл
        // материал — applied = новая ревизия; reload-вызовов ровно 2 (неудача
        // + ретрай) БЕЗ второго пина.
        apply.Ok.Should().BeTrue("the command is accepted, the background apply self-retries");
        rig.Machine.AppliedRevision.Should().Be(8,
            "the applier's own retry converges the material");
        rig.PatroniApply.ReloadCalls.Should().Be(2,
            "one failed attempt + one self-retry — no second worker pin");
        rig.Status.Jsons.Should().Contain(j =>
                j.Contains("\"applied_revision\":" + appliedBefore, StringComparison.Ordinal)
                && j.Contains("patroni reload: 401 (fake)", StringComparison.Ordinal),
            "MR-#12: the failed attempt published Ready with the OLD revision and the error fact");
    }

    // Растущий mod_revision ТОГО ЖЕ контента (переписывание ключа тем же
    // материалом) НЕ триггерит re-применение: материалы совпадают с
    // применёнными — ревизия догоняется фиксацией БЕЗ рестартов/reload
    // (arch/14 §5 I R3: переписывание тем же контентом не снимает/не будит).
    [Fact]
    public async Task Apply_SameContentRewrite_FixesRevisionWithoutRestarts()
    {
        // Arrange: ready на ревизии 7 (материал M); ключ переписан ТЕМ ЖЕ
        // материалом — mod вырос до 9.
        PgNodeMachine.ApplyRetryCooldownSec = 0;
        var pem = RealPemPair();
        await using var rig = await Rig.CreateAsync(pem);
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        rig.Machine.AppliedRevision.Should().Be(7);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(
            BootstrapJson(cert: pem.Cert, key: pem.Key), 9, 3);
        var commandsBefore = rig.Supervisor.Commands.Count;
        var reloadsBefore = rig.PatroniApply.ReloadCalls;

        // Act: пин новой ревизии того же материала.
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: Ok; applied = 9; НИ ОДНОЙ команды рестарта/reload.
        apply.Ok.Should().BeTrue();
        rig.Machine.AppliedRevision.Should().Be(9,
            "same-content rewrite is caught up by fixing the revision only");
        rig.Supervisor.Commands.Skip(commandsBefore).Should().BeEmpty("no restarts on same content");
        rig.PatroniApply.ReloadCalls.Should().Be(reloadsBefore, "no reload on same content");
    }

    // Материальный гейт анти-дребезга (arch/14 §5 I R3): материал НЕДАВНО
    // падал — пин при РАСТУЩЕЙ ревизии того же контента держится коулдауном:
    // ok-пинок, применитель не запускается, команд нет.
    [Fact]
    public async Task Apply_FailedMaterial_GrowingModRevisionKeepsCooldown()
    {
        // Arrange: ready; ключ с материалом M (rev 9); в машине зафиксирован
        // неудавшийся материал M «только что» (юнит-seam); коулдаун 45.
        PgNodeMachine.ApplyRetryCooldownSec = 45;
        var pem = RealPemPair();
        await using var rig = await Rig.CreateAsync(pem);
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var json = BootstrapJson(spilo: "spilo-yaml-v2", cert: pem.Cert, key: pem.Key);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(json, 9, 3);
        var data = NodeBootstrapData.Parse(json)!;
        rig.Machine.MarkFailedMaterialForTests(
            PgNodeMachine.MaterialFingerprint(data),
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        var commandsBefore = rig.Supervisor.Commands.Count;

        // Act: пин при растущей ревизии того же материала.
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);

        // Assert: коулдаун ДЕРЖИТСЯ — ok-пинок; применитель НЕ запущен
        // (ApplyTask null), команд нет, applied не тронут.
        apply.Ok.Should().BeTrue("the pin is accepted, the material is in cooldown");
        rig.Machine.ApplyTask.Should().BeNull("the applier must not start for a material in cooldown");
        rig.Machine.AppliedRevision.Should().Be(7, "the revision is not fixed while in cooldown");
        rig.Supervisor.Commands.Skip(commandsBefore).Should().BeEmpty("no restarts while in cooldown");
    }

    // Материальный гейт: ИСТЕКШИЙ коулдаун (тот же материал) — применитель
    // запускается, материал применяется.
    [Fact]
    public async Task Apply_FailedMaterial_CooldownExpired_Applies()
    {
        // Arrange: как в гейт-тесте, но неудача зафиксирована 46 c назад.
        PgNodeMachine.ApplyRetryCooldownSec = 45;
        var pem = RealPemPair();
        await using var rig = await Rig.CreateAsync(pem);
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var json = BootstrapJson(spilo: "spilo-yaml-v2", cert: pem.Cert, key: pem.Key);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(json, 9, 3);
        var data = NodeBootstrapData.Parse(json)!;
        rig.Machine.MarkFailedMaterialForTests(
            PgNodeMachine.MaterialFingerprint(data),
            DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 46);

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: применён — applied = 9, reload выполнен.
        apply.Ok.Should().BeTrue();
        rig.Machine.AppliedRevision.Should().Be(9);
        rig.PatroniApply.ReloadCalls.Should().Be(1);
    }

    // Материальный гейт: ДРУГОЙ материал коулдаун не держит (материал
    // изменился — применение немедленно).
    [Fact]
    public async Task Apply_DifferentMaterial_CooldownDoesNotHold()
    {
        // Arrange: неудавшийся материал — ЧУЖОЙ (старый YAML); ключ несёт M2.
        PgNodeMachine.ApplyRetryCooldownSec = 45;
        var pem = RealPemPair();
        await using var rig = await Rig.CreateAsync(pem);
        rig.ProbeAllReady();
        (await rig.HandshakeAsync()).Should().Be(HandshakeResult.Started);
        var json = BootstrapJson(spilo: "spilo-yaml-v2", cert: pem.Cert, key: pem.Key);
        rig.Etcd.Store[BootstrapKey] = new Fakes.FakeEtcd.Entry(json, 9, 3);
        var other = NodeBootstrapData.Parse(
            BootstrapJson(spilo: "spilo-yaml-old", cert: pem.Cert, key: pem.Key))!;
        rig.Machine.MarkFailedMaterialForTests(
            PgNodeMachine.MaterialFingerprint(other),
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        // Act
        var apply = await rig.Machine.ApplyAsync(CancellationToken.None);
        await rig.Machine.ApplyTask!;

        // Assert: применён немедленно — applied = 9.
        apply.Ok.Should().BeTrue();
        rig.Machine.AppliedRevision.Should().Be(9);
        rig.PatroniApply.ReloadCalls.Should().Be(1);
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

// П11.9: RestPasswordOf — парсер restapi-пароля из YAML (reload-аутентификация).
// Формат — фактический шаблон SpiloConfiguration (top-level restapi,
// authentication вложен).
public class RestPasswordOfTests
{
    // Пароль найден (шаблонный вид с кавычками).
    [Fact]
    public void Finds_Quoted_Password()
    {
        const string yaml = """
            ---
            restapi:
              connect_address: "node:8008"
              authentication:
                username: patroni
                password: "s3cret"
            postgresql:
              use_pg_rewind: true
            """;
        PgNodeMachine.RestPasswordOf(yaml).Should().Be("s3cret");
    }

    // Пароль без кавычек.
    [Fact]
    public void Finds_Unquoted_Password()
    {
        const string yaml = """
            restapi:
              authentication:
                username: patroni
                password: plain123
            """;
        PgNodeMachine.RestPasswordOf(yaml).Should().Be("plain123");
    }

    // Секции restapi/authentication нет → null.
    [Fact]
    public void No_Restapi_Section_Returns_Null()
    {
        const string yaml = """
            scope: shop
            postgresql:
              use_pg_rewind: true
            """;
        PgNodeMachine.RestPasswordOf(yaml).Should().BeNull();
    }

    // password в ЧУЖОЙ секции (postgresql.authentication) не подхватывается.
    [Fact]
    public void Password_In_Foreign_Section_Not_Picked()
    {
        const string yaml = """
            scope: shop
            postgresql:
              authentication:
                password: super-secret
            bootstrap:
              dcs:
                ttl: 30
            """;
        PgNodeMachine.RestPasswordOf(yaml).Should().BeNull();
    }

    // Отпечаток зонда зависшей doorman-пробы: формат един с [t06-diag]
    // SecretFingerprint воркера (SHA256-HEX[..10]) — сверка отпечатков пробы
    // применителя и ensure/ALTER по логам одного прогона.
    [Fact]
    public void PasswordFingerprint_Is_Stable_TenHexChars_And_Secret_Dependent()
    {
        // Arrange
        const string password = "pgw-e2e-app-pwd";

        // Act
        var fp1 = PgNodeMachine.PasswordFingerprint(password);
        var fp2 = PgNodeMachine.PasswordFingerprint(password);
        var fpOther = PgNodeMachine.PasswordFingerprint(password + "-other");

        // Assert
        fp1.Should().HaveLength(10).And.MatchRegex("^[0-9A-F]{10}$");
        fp2.Should().Be(fp1, "отпечаток детерминирован — сверка между прогонами");
        fpOther.Should().NotBe(fp1, "отпечаток зависит от пароля — различение расхождений");
    }
}
