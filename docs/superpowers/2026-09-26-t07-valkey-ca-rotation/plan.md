# t07-valkey-ca-rotation — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** ротация per-cluster CA и серверных сертов Valkey-кластера без остановки обслуживания — окно двойного доверия P→D→R→C (CaRotator воркера), заявка через API воркера и панель, docker-E2E кейс, многосертовое чтение `ca_pem` в `../Puzzle`.

**Архитектура:** порт kafka CaRotator (`src/KafkaWorker.Provisioning/Processes/CaRotator.cs`) на механику valkey (nodes=1, факт-детект через `GetTlsArchiveAsync`, эксклюзивный второй шаг Active-ветки после TlsMigrator T). Состояние — только в etcd (staging `ca_next_*`, bundle `ca_pem`, journal `work/<C>`, заявка `ca_rotations/<C>`); идемпотентность по факту, без in-memory-треков. Панель — чтение очереди + мутация-прокси + DTO/бейдж; Puzzle — `X509Certificate2Collection.ImportFromPem` вместо одноблочного `CreateFromPem`.

**Тех-стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions, Testcontainers, docker; React/Mantine (frontend), `../Puzzle` — свой солюшен `src/PuzzleServer.Api.slnx`.

**Spec:** [`docs/superpowers/2026-09-26-t07-valkey-ca-rotation/spec.md`](spec.md) — план разворачивает spec в решения кода; исполнители читают ОБА файла. Канон: `arch/20-valkey-clusters.md` §2/§2.1/§3/§4/§5, `arch/21-valkeyworker.md` §2/§3/§5 K/§9, `arch/adminpanel/02-etcd-contract.md` §11.1/§11.2 (уже обновлены в worktree, Task 1 фиксирует коммитом).

**Ревизия 4 (раунд 3 ревью plan↔spec, 3 минорных механики):** (1) кейс `PhaseR_RecreatesNode…` — ассерт `Removed.Contain("vwk-r3-node1")` (FakeDriver.RemoveNodeAsync кладёт ПОЛНОЕ имя контейнера `PlainClusterDriver.NodeName`, не короткое имя ноды; канон `ProvisioningProcessTests.cs:168`). (2) Кейс `PhaseC_CompareLost_Failed` — подмена staging-ключа через `with { Value = "foreign" }` (переписывание всей Entry: `FakeEtcd.Entry` — позиционный record с init-only-свойствами, присваивание `.Value` — CS8852). (3) Files Task 3 дополнен `Fakes.cs` (поле `LastCaPem` в FakeValkeyConnection — коммитится шагом 3.6).

**Ревизия 3 (раунд 2 ревью plan↔spec, 6 замечаний):** (1) кейсы `OpenWindow_SkipsGuards_*` (обе версии, Step 2.4 и Step 3.1) — в arrange добавлен посев окна `ca_next_*` (без него живая пароль-заявка даёт Waiting до открытия окна — K0.3 не срабатывал). (2) Вентиль-кейс `Active_CaRotationWaiting_PasswordRotationPlays` — снят ассерт `Contain("waiting-password-rotation")` (надзор C и ротатор E тем же тиком перезаписывают единый journal-ключ: supervise → rotate); заменён на `Contain("\"rotate\"")`+`Contain("done")` — факт «K не заблокировал ветку». (3) `req.Successes` → `req.Success` (свойство `TxnRequest(Compare, Success, Failure)`, `IEtcdGateway.cs:93-96`) в TxnFault/OnTxnBeforeCompare-инжектах кейсов c9/r5; эвристики подтверждены (journal-записи идут PutAsync, не txn — `WorkJournal.cs:88-89`). (4) Step 3.3 — примечание о кейсе c9 исправлено: он КРАСНЫЙ на Step 3.3 вместе с остальными (инжект не срабатывает без C-txn); гейт «ожидание FAIL» не страдает. (5) Греп-гейт Step 14.6 — паттерн `'"/valkey/'` (кавычка-префикс, не совпадает с `/api/valkey/…`). (6) Кейс `PhaseR_RecreatesNode…` — добавлены посев resources-декларации и ассерты порта/лимитов (`ClientHostPort`/`CpuCores`/`MemoryBytes` — по фактическим полям `ValkeyNodeSpec`); попутно `s.Node` → `s.NodeName` (фактическое имя поля).

## Глобальные ограничения (каждая задача наследует неявно)

- Сборка Release — 0 ошибок, 0 warnings (`TreatWarningsAsErrors=true`).
- Комментарии/документация — русские; идентификаторы — английские; тесты — AAA-комментарии.
- E2E/интеграции: ПОЛНЫЙ teardown при любом исходе, ассерт чистоты, guid-изоляция, ТОЛЬКО динамические порты (`WithPortBinding(..., assignRandomHostPort: true)` / `FreePortWindow`), никаких хардкод-портов в expects (AGENTS.md, `docs/e2e-isolation.md`).
- Телеметрия E2E: артефакты в `/tmp/pgw-e2e-artifacts-<guid>/`, `MarkFailed()` при падении, `[PHASE]`-строки для фаз > 60 с (`docs/e2e-launch.md`); перезапуск упавших тестов без согласия пользователя ЗАПРЕЩЁН.
- После КАЖДОЙ тестовой серии (юниты → интеграции → E2E, серия за серией) — зачистка контейнеров/сетей (`docker network prune -f` страховочно); следующая серия — только поверх зачищенной предыдущей.
- Бюджеты ожидания в тестах: `NodeBootSec` ≤ 100 с; sleep/поллинг агента ≤ 30 с (правило `AGENTS.base.md` §Таймауты).
- Задача трогает код воркеров ⇒ мерж-гейт: docker-E2E на свежем Release (образ `valkeyworker:e2e` пересобирается фикстурой из текущего кода — урок t09 закрыт механикой `ValkeyE2eEnvironment.StartAsync`).
- Двухрепозиторийность: правки `../Puzzle` — отдельная feature-ветка и СВОЙ коммит в СВОЁМ репозитории; в мерж-гейт pg НЕ входит; мерж Puzzle — отдельным решением пользователя (прецедент t06).
- `../Puzzle` сейчас: ветка `main`, рабочее дерево чистое (main@5ef7d61). Коммитить прямо в `main` репозитория Puzzle ЗАПРЕЩЕНО (AGENTS.base.md §6) — только feature-ветка.
- Решения пользователя (фиксированы spec §0): CaRotator — эксклюзивный второй шаг Active-ветки; скоуп панели — полный аналог kafka; тесты — юниты + интеграции + docker-E2E; Puzzle в скоупе.

## Важная механика для понимания кейсов (Task 2/3)

`FakeValkeyConnection { TrustAnyPassword = true }` отвечает на PING всегда ⇒ после реализации фаз R/C/K4 (Task 3) ОДИН `RunAsync` доводит полный цикл до `done` за один вызов (nodes=1, «rolling» = одно пересоздание). Поэтому: кейсы Task 2, ассертящие ПРОМЕЖУТОЧНОЕ состояние окна (staging есть, `ca_pem` = bundle), зелёны только против реализации Task 2 (цикл останавливается после D); Task 3 Step 3.1 ОБЯЗАТЕЛЬНО обновляет их под финальное состояние (или останавливает окно инжектом отказа) ДО реализации R/C — см. Task 3.

Диспетчеризация K0 (для конструирования кейсов): окно считается открытым по ФАКТУ (`ca_next_*` есть ИЛИ journal `rotate-ca` вне `done/waiting-*`); кейсы «окно открыто при живой пароль-заявке» ОБЯЗАНЫ сеять `ca_next_*` руками — иначе K0.5 даст `Waiting` ещё до открытия.

## Карта задач и зависимостей

```
Task 1 (arch-коммит)
  └→ Task 2 (CaRotator: K0+P+D) ─→ Task 3 (CaRotator: R+C+K4 + обновление кейсов Task 2)
        └→ Task 4 (вентиль ветки + DI)
        └→ Task 5 (RotateCaHandler + endpoint)
        └→ Task 6 (X2-чистка ca_rotations)
  └→ Task 7 (панель: чтение очереди + API-DTO) ─→ Task 8 (панель: команда+endpoint) ─→ Task 9 (фронтенд + сверка caRotation)
  └→ Task 10 (интеграции; после 2–6)
  └→ Task 11 (docker-E2E; после 4,5)
  └→ Task 12 (runbook)
  └→ Task 13 (Puzzle — независим от 2–12, свой репозиторий)
  └→ Task 14 (мерж-гейт pg: полная приёмка + сверка канона + roadmap-тег)
```

Рабочие каталоги:
- Монорепо: `/Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation` (ветка `t07-valkey-ca-rotation`), далее `$WT`.
- Puzzle: `/Users/demakaev/ZCodeProject/Puzzle`, далее `$PUZZLE`.

Команды сборки/тестов монорепо (все — из `$WT`):
- Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release`
- Юниты воркера: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~ValkeyWorker.UnitTests"`
- Юниты панели: `... --filter "FullyQualifiedName~AdminPanel.UnitTests"`
- Интеграции valkey: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"` (реальный docker; после прогона — зачистка, см. глобальные ограничения)
- E2E: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"` (фикстура сама пересобирает `valkeyworker:e2e` свежим Release)
- Фронтенд: `cd frontend && npm run typecheck` (изменения TS/TSX)

---

### Task 1: Зафиксировать arch-first базлайн коммитом

**Вход (предусловие):** worktree `$WT` на ветке `t07-valkey-ca-rotation`; незакоммичены правки канонов `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/adminpanel/02-etcd-contract.md` и untracked-каталог `docs/superpowers/2026-09-26-t07-valkey-ca-rotation/` (spec). Это состояние одобрено гейтом user-review.

**Действие:** один коммит, фиксирующий канон + spec до любого кода (arch-first, AGENTS.base.md §1).

**Выход:** чистое рабочее дерево; точка сверки «код обязан зеркалить канон».

**Проверка:** `cd $WT && git status --porcelain` → пусто; `git log --oneline -1` показывает коммит.

**Связь со spec:** §2.1 (arch-first), §10.7.

- [ ] **Step 1.1: коммит базлайна**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation
git add arch/20-valkey-clusters.md arch/21-valkeyworker.md arch/adminpanel/02-etcd-contract.md \
  docs/superpowers/2026-09-26-t07-valkey-ca-rotation
git commit -m "arch(t07): канон ротации per-cluster CA valkey — окно двойного доверия (arch/20 §2/§2.1/§3/§4/§5, arch/21 §2/§3/§5 K/§9, adminpanel/02 §11.1/§11.2) + spec"
```

- [ ] **Step 1.2: проверить**

Run: `git status --porcelain && git show --stat HEAD | head -10`
Ожидание: пустой статус; в коммите 4 записи (3 arch-файла + каталог spec).

---

### Task 2: CaRotator — диспетчеризация K0 и фазы P/D

**Files:**
- Create: `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs`

**Interfaces:**
- Consumes (существующее, НЕ менять): `ValkeyPki.GenerateCa(cluster)` → `(string CaPem, string CaKeyPem)`; `NodeTlsProvisioner.EnsureNodeTlsAsync(cluster, node, host, advertisedHost, caPem, caKeyPem, ct)`; `NodeTlsProvisioner.IsValidTar(tar, advertisedHost, caPem, clock)` (internal static, доступен из сборки Provisioning); `IClusterDriver.{RemoveNodeAsync(cluster,node,ct), EnsureNodeAsync(spec,ct), GetTlsArchiveAsync(cluster,host,image,ct)}`; `PlainClusterDriver.TlsVolumeName(cluster)`; `NodeArgsBuilder.Build(maxmemoryBytes, policy, adminPw, appPw)`; `ProcessCommon.{ConfigKey, PortAllocKey, ParsePortAlloc, WriteNodeStateAsync, ParseResources, RotationStateKey}`; `WorkJournal.{WritePhaseAsync(cluster,op,phase,instanceId,error,ct), ReadAsync(cluster,ct)}`; `ClaimStore.IsMine(cluster)`; `IValkeyConnection.PingAsync(endpoint, ct)`; `ValkeyEndpoint(host, port, user, password, caPem)`; `ValkeyProvisioningOptions` (поля `NodeImage`, `NodeBootSec`, `AdvertisedClientHost`); `ValkeyClusterSnapshot` (поля `Cluster, Config, Nodes, Endpoints, AdminPassword, AppPassword, CaPem, CaKey` — `src/ValkeyWorker.Core/Model/ValkeyDomain.cs:34-46`); `ValkeyNodeSpec(Cluster, NodeName, Host, ClientHostPort, Image, Args, CpuCores, MemoryBytes, TlsVolume)` — `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs:23-32`; фейк-набор `Fakes.{FakeEtcd, FakeDriver, FakeValkeyConnection}` + `FixedTimeProvider`.
- Produces (для задач 3–5, 10, 11): `public sealed class CaRotator(gateway, endpoints, driver, claims, journal, tlsProvisioner, valkey, options, snapshot = null, clock = null)`; `public const string Op = "rotate-ca"`; `public enum RotationOutcome { NotNeeded, Waiting, InProgress }`; `public Task<Result<RotationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)`; private-хелперы `FailAsync`/`FailStagingAsync` — last_error в journal (spec §5; P/D-отказы тоже через них).

**Вход:** Task 1 закоммичен.

**Действие:** новый файл `CaRotator.cs` — порт kafka CaRotator на механику valkey по spec §4.1/§5 (фазы K0/P/D в этой задаче; R/C/K4 добавит Task 3 тем же файлом — вместо них плейсхолдер, возвращающий `InProgress`).

**Выход:** компилирующийся CaRotator с K0-диспетчеризацией и фазами staging/bundle; зелёные юнит-кейсы no-op/ждущие/P/D (промежуточные ассерты окна валидны, пока цикл останавливается после D).

**Проверка:** точечный прогон `CaRotatorTests` зелёный.

**Связь со spec:** §3.1 (ключи/txn), §4.1, §5 (K0, P, D, «Отказ etcd/docker между фазами» — last_error), §7.1 (часть кейсов).

- [ ] **Step 2.1: написать фейлящие тесты — Rig + кейсы K0/P/D**

Файл `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs` (Rig — порт `TlsMigratorTests.Rig`):

```csharp
using FluentAssertions;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// CaRotator (t07, arch/21 §5 K): диспетчеризация K0 (окно/заявка/ждущие),
// фазы P (staging put-if-absent) и D (bundle OLD+NEW, compare по OLD).
// ВАЖНО: против реализации Task 2 цикл останавливается после D, поэтому
// кейсы «окна» ассертят ПРОМЕЖУТОЧНОЕ состояние (staging/bundle) — Task 3
// Step 3.1 обновит их под финал полного цикла (один RunAsync = P→D→R→C→K4).
public class CaRotatorTests
{
    private static readonly ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions Options =
        new(17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2");

    private static readonly FixedTimeProvider Clock = new();

    private const string Image = "valkey/valkey:9.1.2";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeDriver Driver = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ClaimStore Claims = null!;
        public WorkJournal Journal = null!;
        public ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner Tls = null!;
        public List<string> Snapshots = [];
        public ValkeyWorker.Provisioning.Processes.CaRotator Rotator = null!;

        public static Rig Create()
        {
            var rig = new Rig();
            rig.Claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Journal = new WorkJournal("/valkeyworker", rig.Etcd, ["http://etcd:2379"]);
            rig.Tls = new ValkeyWorker.Provisioning.Processes.NodeTlsProvisioner(rig.Driver, Image, Clock);
            rig.Rotator = new ValkeyWorker.Provisioning.Processes.CaRotator(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims, rig.Journal,
                rig.Tls, rig.Valkey, Options,
                async _ =>
                {
                    rig.Snapshots.Add("shot");
                    return Result.Success();
                },
                Clock);
            return rig;
        }

        // Канонический TLS-кластер (миграция T уже отработала): креды, CA-ключи,
        // endpoints, portalloc, контейнер с TLS-args, клэйм наш.
        public string SeedTls(string cluster, int port = 17001)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
            var (caPem, caKeyPem) = ValkeyWorker.Core.Valkey.ValkeyPki.GenerateCa(cluster);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_pem", caPem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_key", caKeyPem);
            Etcd.Seed($"/valkeyworker/portalloc/{cluster}", "{\"node1\":{\"host\":\"h1\",\"client\":" + port + "}}");
            Driver.Containers[$"vwk-{cluster}-node1"] =
                new Fakes.FakeDriver.ContainerFact("h1", port, 2m, 1024L * 1024 * 1024,
                    ["valkey-server", "--tls-port", "6379", "--port", "0"], Image, "id-tls");
            return caPem;
        }

        // МИНИМАЛЬНЫЙ кластер «не поднят»: только клэйм + config (без endpoints,
        // кредов, ca-ключей) — ветка waiting-cluster достижима (spec §5 K0.5).
        public void SeedBare(string cluster)
        {
            Claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
        }

        // Ручное открытие окна (посев staging ca_next_*): K0.3 срабатывает по
        // ФАКТУ наличия ключей — guard'ы K0.4–K0.6 пропускаются.
        public (string Pem, string Key) SeedWindow(string cluster)
        {
            var (pem, key) = ValkeyWorker.Core.Valkey.ValkeyPki.GenerateCa(cluster + "-new");
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_pem", pem);
            Etcd.Seed($"/valkey/clusters/{cluster}/ca_next_key", key);
            return (pem, key);
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }

        public string? Get(string key)
            => Etcd.GetAsync("http://etcd:2379", key, TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult().Value?.Value;

        public void Put(string key, string value)
            => Etcd.PutAsync("http://etcd:2379", key, value, null, TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();

        public void SeedTicket(string cluster)
            => Put($"/valkeyworker/ca_rotations/{cluster}",
                $$"""{"requested_unix":1756500000,"requested_by":"it"}""");
    }

    // ... кейсы — Steps 2.2, 2.4
}
```

- [ ] **Step 2.2: кейсы K0 (no-op / ждущие)** — добавить в класс:

```csharp
    [Fact]
    public async Task NoTicketAndNoTail_NotNeeded_ZeroMutations()
    {
        // Arrange — канонический кластер, заявки/журнала ротации нет
        var rig = Rig.Create();
        var caPem = rig.SeedTls("c1");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c1"), TestContext.Current.CancellationToken);

        // Assert — NotNeeded; ca_pem не тронут, staging нет, journal пуст
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.NotNeeded);
        rig.Get("/valkey/clusters/c1/ca_pem").Should().Be(caPem);
        rig.Get("/valkey/clusters/c1/ca_next_key").Should().BeNull();
        rig.Get("/valkeyworker/work/c1").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_ClusterNotUp_WaitingCluster_TicketKept()
    {
        // Arrange — МИНИМАЛЬНЫЙ посев (клэйм + config + заявка; БЕЗ endpoints/
        // кредов/ca-ключей — SeedTls их сеёт и waiting-cluster недостижим)
        var rig = Rig.Create();
        rig.SeedBare("c2");
        rig.SeedTicket("c2");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c2"), TestContext.Current.CancellationToken);

        // Assert — Waiting, journal waiting-cluster, заявка жива, мутаций нет
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c2").Should().Contain("rotate-ca").And.Contain("waiting-cluster");
        rig.Get("/valkeyworker/ca_rotations/c2").Should().NotBeNull();
        rig.Get("/valkey/clusters/c2/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_PasswordRotationAlive_WaitingPasswordRotation()
    {
        // Arrange — канонический кластер + живая заявка ротации креда;
        // окно НЕ открыто (ca_next_* нет) — K0.5 даёт Waiting
        var rig = Rig.Create();
        rig.SeedTls("c3");
        rig.SeedTicket("c3");
        rig.Put("/valkeyworker/rotations/c3",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c3"), TestContext.Current.CancellationToken);

        // Assert — Waiting (ждущие исходы вентиль НЕ блокируют), окно НЕ открыто
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c3").Should().Contain("waiting-password-rotation");
        rig.Get("/valkey/clusters/c3/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task Ticket_PasswordStateReplaying_WaitingPasswordRotation()
    {
        // Arrange — заявки rotations нет, но стейт доигрывания жив (e1-added)
        var rig = Rig.Create();
        rig.SeedTls("c4");
        rig.SeedTicket("c4");
        rig.Put("/valkeyworker/work/c4/rotation",
            """{"phase":"e1-added","request":{"role":"app","requested_unix":1756500000,"requested_by":"it"}}""");

        // Act / Assert — Waiting по стейту (spec §5 K0.5)
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c4"), TestContext.Current.CancellationToken);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c4").Should().Contain("waiting-password-rotation");
    }

    [Fact]
    public async Task Ticket_ToRemove_AbortedStateChanged_Waiting()
    {
        // Arrange — заявка жива, config со state=TO_REMOVE (посев SeedTls +
        // перезапись config: state-гейт К0.6 срабатывает после ждущих)
        var rig = Rig.Create();
        rig.SeedTls("c5");
        rig.SeedTicket("c5");
        rig.Put("/valkey/clusters/c5/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c5"), TestContext.Current.CancellationToken);

        // Assert — aborted-state-changed, Waiting; демонтаж B почистит всё
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkeyworker/work/c5").Should().Contain("aborted-state-changed");
        rig.Get("/valkey/clusters/c5/ca_next_key").Should().BeNull("окно не открывалось");
    }
```

- [ ] **Step 2.3: прогнать — FAIL**

Run: `cd $WT && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~CaRotatorTests"`
Ожидание: ошибка компиляции — типа `CaRotator` не существует.

- [ ] **Step 2.4: кейсы P/D + диспетчеризация окна** — добавить в класс:

```csharp
    [Fact]
    public async Task Ticket_OpensWindow_StagingThenBundle()
    {
        // Arrange — канонический кластер + заявка
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c6");
        rig.SeedTicket("c6");

        // Act — тик 1. Против реализации Task 2 цикл останавливается после D:
        // эти ассерты — ПРОМЕЖУТОЧНЫЕ (staging жив, ca_pem = bundle). После
        // реализации Task 3 (R/C/K4) ОДИН RunAsync доводит цикл до коммита —
        // кейс ОБНОВЛЯЕТСЯ в Task 3 Step 3.1 на финальные ассерты (коммит
        // сворачивает bundle и удаляет staging — промежуточные устареют).
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c6"), TestContext.Current.CancellationToken);

        // Assert — окно открыто: staging есть, bundle записан, journal phase-d
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var nextPem = rig.Get("/valkey/clusters/c6/ca_next_pem")!;
        nextPem.Should().NotBeNullOrEmpty("фаза P создала staging");
        rig.Get("/valkey/clusters/c6/ca_next_key").Should().NotBeNull();
        var caPem = rig.Get("/valkey/clusters/c6/ca_pem")!;
        caPem.Should().StartWith(oldPem).And.EndWith(nextPem,
            "bundle = OLD + \"\\n\" + NEW (фаза D)");
        rig.Get("/valkeyworker/work/c6").Should().Contain("phase-d");
    }

    [Fact]
    public async Task OpenWindow_SkipsGuards_ReplaysFromFact()
    {
        // Arrange — окно ОТКРЫТО ПОСЕВОМ ca_next_* (иначе K0.5 даст Waiting
        // ещё до открытия), ПРИ живой пароль-заявке: K0.3 срабатывает по
        // факту staging — guard'ы K0.4–K0.6 НЕ выполняются (spec §5 K0.3)
        var rig = Rig.Create();
        rig.SeedTls("c7");
        rig.SeedTicket("c7");
        rig.SeedWindow("c7"); // ca_next_pem/ca_next_key — окно открыто
        rig.Put("/valkeyworker/rotations/c7",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act — доигрывание идёт ПРИ живой пароль-заявке
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c7"), TestContext.Current.CancellationToken);

        // Assert — не Waiting: окно открыто, доигрывание пошло (Task 3 Step 3.1
        // усилит ассерты финалом: done + коммит при живой пароль-заявке)
        outcome.Value.Should().NotBe(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Get("/valkey/clusters/c7/ca_next_key").Should().NotBeNull();
    }

    [Fact]
    public async Task ReplayTick_ReusesExistingStaging_NoSecondGeneration()
    {
        // Arrange — staging уже лежит (например, после краха между P и D)
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c8");
        rig.SeedTicket("c8");
        var (foreignPem, foreignKey) = rig.SeedWindow("c8");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c8"), TestContext.Current.CancellationToken);

        // Assert — чужая staging переиспользована (re-read), своя не сгенерирована
        // (Task 3 Step 3.1 перепишет под финал: коммит от ЧУЖОЙ staging)
        rig.Get("/valkey/clusters/c8/ca_next_pem").Should().Be(foreignPem);
        rig.Get("/valkey/clusters/c8/ca_pem").Should().EndWith(foreignPem);
    }

    [Fact]
    public async Task BundleAlreadyContainsNext_PutSkipped()
    {
        // Arrange — D отработан (bundle в ca_pem), R/C нет. Кейс проверяет
        // пропуск put (string.Contains-детект) по неизменности ModRevision
        // ca_pem. Чтобы после Task 3 фаза C не перезаписала ca_pem (это
        // изменило бы ревизию и сломало проверяемый факт), окно ОСТАНАВЛИВАЕТСЯ
        // инжектом TxnFault ТОЛЬКО на коммит-txn фазы C (5 success-операций;
        // инжект задействуется в Task 3 Step 3.1 — против Task 2 C-txn не
        // существует и инжект бездействует).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c9");
        rig.SeedTicket("c9");
        var (nextPem, nextKey) = rig.SeedWindow("c9");
        rig.Put("/valkey/clusters/c9/ca_pem", oldPem + "\n" + nextPem);
        var revisionBefore = rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision;

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c9"), TestContext.Current.CancellationToken);

        // Assert — put пропущен: ModRevision ca_pem не тунул (пере-put
        // изменил бы ревизию записи в Store FakeEtcd)
        rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision.Should().Be(revisionBefore);
    }

    [Fact]
    public async Task BundleCompareLost_Failed_WithLastError()
    {
        // Arrange — ca_pem изменён внешне после снапшота: compare сорвётся
        var rig = Rig.Create();
        rig.SeedTls("c10");
        rig.SeedTicket("c10");
        var snap = rig.Snapshot("c10");
        rig.Put("/valkey/clusters/c10/ca_pem", rig.Get("/valkey/clusters/c10/ca_pem")! + "\nextern");

        // Act
        var outcome = await rig.Rotator.RunAsync(snap, TestContext.Current.CancellationToken);

        // Assert — Failed «ретрай тиком» С last_error в journal (spec §5:
        // отказ между фазами — Failed c last_error); окно при этом открыто
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("ретрай");
        rig.Get("/valkey/clusters/c10/ca_next_key").Should().NotBeNull();
        rig.Get("/valkeyworker/work/c10").Should().Contain("last_error",
            "отказ фазы D фиксируется в journal (FailAsync)");
    }
```

- [ ] **Step 2.5: реализовать CaRotator (K0 + P + D)** — файл `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs`:

```csharp
using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// CaRotator (t07, arch/21 §5 K): ротация per-cluster CA и серверного серта
/// по заявке /valkeyworker/ca_rotations/&lt;C&gt; — окно двойного доверия без
/// остановки обслуживания. Фазы: P (staging ca_next_* put-if-absent) →
/// D (ca_pem = bundle OLD+NEW — перечитавшие дискавери доверяют обоим) →
/// R (пересоздание node1 с сертом от NEW; факт-детект IsValidTar — без
/// in-memory-треков, nodes=1) → C (атомарный txn: ca_pem/ca_key ← NEW,
/// del staging, del заявки) → K4 (снапшот + done). Эксклюзивный второй шаг
/// Active-ветки: окно открыто ⇒ InProgress ⇒ надзор/конвергер/ротация
/// кредов в тике не идут (решение пользователя, spec §2.4). Ждущие исходы
/// (waiting-*) вентиль НЕ блокируют. Вызывается только держателем клэйма
/// &lt;C&gt;; состояние — только в etcd. Отказ etcd/docker между фазами —
/// Failed c last_error в journal (spec §5).
/// </summary>
public sealed class CaRotator(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    NodeTlsProvisioner tlsProvisioner,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    TimeProvider? clock = null)
{
    public const string Op = "rotate-ca";

    private const string PhaseDone = "done";
    private const string PhaseCommitted = "committed";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    /// <summary>Итог тика: NotNeeded — no-op ветки; Waiting — заявка жива,
    /// окно НЕ открыто (ветка продолжается); InProgress — окно открыто/
    /// доигрывается (вентиль блокирует C/D/E).</summary>
    public enum RotationOutcome
    {
        NotNeeded,
        Waiting,
        InProgress,
    }

    public async Task<Result<RotationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result<RotationOutcome>.Failed(new ApplicationException(
                $"rotate-ca {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // K0.1: заявка + journal.
        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result<RotationOutcome>.Failed(ticket.Error!);
        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result<RotationOutcome>.Failed(journalState.Error!);

        // K0.2: хвост после коммита (заявка снята, done не записан) —
        // идемпотентный финал K4 без мутаций staging/ноды.
        var afterCommit = journalState.Value is { Op: Op } j && j.Phase == PhaseCommitted;
        if (afterCommit && ticket.Value is null)
            return await FinishAsync(cluster, ct);

        // K0.3: окно уже открыто? — доигрывание P→D→R→C БЕЗ ждущих проверок.
        var windowOpen = await WindowOpenAsync(cluster, journalState.Value, ct);
        if (windowOpen.IsSuccess && windowOpen.Value)
        {
            // Аномалия-защита: окно открыто, а канона нет — внешняя порча;
            // Failed = ретрай тиком (самокоррекции нет).
            if (snap.CaPem is null || snap.CaKey is null)
                return Result<RotationOutcome>.Failed(new ApplicationException(
                    $"rotate-ca {cluster}: окно открыто, но ca_pem/ca_key отсутствуют — внешняя порча, ретрай тиком"));
            return await PhasesAsync(snap, ct);
        }
        if (!windowOpen.IsSuccess)
            return Result<RotationOutcome>.Failed(windowOpen.Error!);

        // K0.4: заявки нет и хвоста/окна нет — no-op ветки.
        if (ticket.Value is null)
            return Result<RotationOutcome>.Success(RotationOutcome.NotNeeded);

        // K0.5: ждущие причины (исход Waiting, БЕЗ мутаций; journal-запись фазы).
        if (snap.Endpoints is null || snap.AdminPassword is null || snap.AppPassword is null
            || snap.CaPem is null || snap.CaKey is null)
            return await WaitAsync(cluster, "waiting-cluster", ct);
        var passwordAlive = await PasswordRotationAliveAsync(cluster, journalState.Value, ct);
        if (!passwordAlive.IsSuccess)
            return Result<RotationOutcome>.Failed(passwordAlive.Error!);
        if (passwordAlive.Value)
            return await WaitAsync(cluster, "waiting-password-rotation", ct);

        // K0.6: перечитка config — TO_REMOVE: демонтаж B всё почистит.
        var removed = await ConfigRemovedAsync(cluster, ct);
        if (!removed.IsSuccess)
            return Result<RotationOutcome>.Failed(removed.Error!);
        if (removed.Value)
            return await AbortAsync(cluster);

        return await PhasesAsync(snap, ct);
    }

    // Фазы P→D→(R→C→K4 — Task 3): доигрывание по факту.
    private async Task<Result<RotationOutcome>> PhasesAsync(
        ValkeyClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // P: staging НОВОЙ CA — одна генерация на жизнь ротации.
        var staging = await EnsureStagingAsync(cluster, ct);
        if (!staging.IsSuccess)
            return Result<RotationOutcome>.Failed(staging.Error!);
        var (nextKey, nextPem) = staging.Value;

        // D: bundle OLD+NEW в точке дискавери ДО замены серта ноды.
        var caPem = snap.CaPem!;
        if (!caPem.Contains(nextPem))
        {
            var markedD = await journal.WritePhaseAsync(cluster, Op, "phase-d", claims.InstanceId, null, ct);
            if (!markedD.IsSuccess)
                return Result<RotationOutcome>.Failed(markedD.Error!);
            var bundlePut = await TxnAsync(TxnRequest.Of(
                [TxnCompare.ValueEqual(CaPemKey(cluster), caPem)],
                [new TxnOp.Put(CaPemKey(cluster), caPem + "\n" + nextPem, null)]), ct);
            if (!bundlePut.IsSuccess)
                return await FailAsync(cluster, bundlePut.Error!, "phase-d", ct);
            if (!bundlePut.Value.Succeeded)
                return await FailAsync(cluster, new ApplicationException(
                    $"rotate-ca {cluster}: ca_pem изменился с момента чтения (внешняя запись?) — ретрай тиком"), "phase-d", ct);
        }

        // R→C→K4: Task 3 (ReplayNodeCommitAsync); до его появления — окно живо.
        return await ReplayNodeCommitAsync(snap, nextPem, nextKey, ct);
    }

    // Плейсхолдер этой задачи: окно открыто и держится (реализация R/C — Task 3).
    private async Task<Result<RotationOutcome>> ReplayNodeCommitAsync(
        ValkeyClusterSnapshot snap, string nextPem, string nextKey, CancellationToken ct)
    {
        await Task.CompletedTask;
        return Result<RotationOutcome>.Success(RotationOutcome.InProgress);
    }

    // Окно открыто: staging есть ИЛИ journal rotate-ca вне {done, waiting-*}.
    private async Task<Result<bool>> WindowOpenAsync(string cluster, WorkState? journalState, CancellationToken ct)
    {
        var nextKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!nextKey.IsSuccess)
            return Result<bool>.Failed(nextKey.Error!);
        var nextPem = await GetAsync(NextPemKey(cluster), ct);
        if (!nextPem.IsSuccess)
            return Result<bool>.Failed(nextPem.Error!);
        if (nextKey.Value is not null || nextPem.Value is not null)
            return Result<bool>.Success(true);
        return Result<bool>.Success(journalState is { Op: Op } j
            && j.Phase != PhaseDone
            && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal));
    }

    // Живая ротация креда (spec §5 K0.5): заявка rotations ИЛИ стейт
    // work/<C>/rotation с фазой e1-pending|e1-added|e2-committed.
    private async Task<Result<bool>> PasswordRotationAliveAsync(string cluster, WorkState? journalState, CancellationToken ct)
    {
        if (journalState is { Op: "rotate" } r && r.Phase != PhaseDone)
            return Result<bool>.Success(true);
        var passwordTicket = await GetAsync($"/valkeyworker/rotations/{cluster}", ct);
        if (!passwordTicket.IsSuccess)
            return Result<bool>.Failed(passwordTicket.Error!);
        if (passwordTicket.Value is not null)
            return Result<bool>.Success(true);
        var state = await GetAsync(ProcessCommon.RotationStateKey(cluster), ct);
        if (!state.IsSuccess)
            return Result<bool>.Failed(state.Error!);
        if (state.Value is not { } kv)
            return Result<bool>.Success(false);
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var phase = doc.RootElement.TryGetProperty("phase", out var p)
                        && p.ValueKind == JsonValueKind.String ? p.GetString() : null;
            return Result<bool>.Success(phase is "e1-pending" or "e1-added" or "e2-committed");
        }
        catch (JsonException)
        {
            return Result<bool>.Success(false); // битый стейт — не «живая ротация»
        }
    }

    // Фаза P: чтение staging; отсутствующая — генерация + txn put-if-absent;
    // проигрыш compare — re-read (чужая staging валидна, образец kafka).
    // Отказы (journal/txn) — FailStagingAsync: last_error в journal (spec §5).
    private async Task<Result<(string Key, string Pem)>> EnsureStagingAsync(string cluster, CancellationToken ct)
    {
        var key = await GetAsync(NextKeyKey(cluster), ct);
        if (!key.IsSuccess)
            return Result<(string, string)>.Failed(key.Error!);
        var pem = await GetAsync(NextPemKey(cluster), ct);
        if (!pem.IsSuccess)
            return Result<(string, string)>.Failed(pem.Error!);
        if (key.Value is { } existingKey && pem.Value is { } existingPem)
            return Result<(string, string)>.Success((existingKey.Value, existingPem.Value));

        var markedP = await journal.WritePhaseAsync(cluster, Op, "phase-p", claims.InstanceId, null, ct);
        if (!markedP.IsSuccess)
            return Result<(string, string)>.Failed(markedP.Error!);

        var generated = ValkeyPki.GenerateCa(cluster);
        var txn = await TxnAsync(TxnRequest.Of(
            [TxnCompare.NotExists(NextKeyKey(cluster)), TxnCompare.NotExists(NextPemKey(cluster))],
            [
                new TxnOp.Put(NextKeyKey(cluster), generated.CaKeyPem, null),
                new TxnOp.Put(NextPemKey(cluster), generated.CaPem, null),
            ]), ct);
        if (!txn.IsSuccess)
            return await FailStagingAsync(cluster, txn.Error!);

        var finalKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!finalKey.IsSuccess || finalKey.Value is null)
            return await FailStagingAsync(cluster, finalKey.Error
                ?? new ApplicationException($"rotate-ca {cluster}: ca_next_key не читается после txn"));
        var finalPem = await GetAsync(NextPemKey(cluster), ct);
        if (!finalPem.IsSuccess || finalPem.Value is null)
            return await FailStagingAsync(cluster, finalPem.Error
                ?? new ApplicationException($"rotate-ca {cluster}: ca_next_pem не читается после txn"));
        return Result<(string, string)>.Success((finalKey.Value!.Value, finalPem.Value!.Value));
    }

    // FailAsync для фазы P (стадия staging): last_error в journal, затем Failed.
    private async Task<Result<(string Key, string Pem)>> FailStagingAsync(string cluster, Exception error)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, "phase-p", claims.InstanceId, error.Message, CancellationToken.None);
        return written.IsSuccess
            ? Result<(string, string)>.Failed(error)
            : Result<(string, string)>.Failed(written.Error!);
    }

    // Финал K4: снапшот «после» + journal done (идемпотентно).
    private async Task<Result<RotationOutcome>> FinishAsync(string cluster, CancellationToken ct)
    {
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result<RotationOutcome>.Failed(after.Error!);
        }
        var done = await journal.WritePhaseAsync(cluster, Op, PhaseDone, claims.InstanceId, null, ct);
        return done.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.InProgress)
            : Result<RotationOutcome>.Failed(done.Error!);
    }

    // Ждущий исход: journal-запись + Waiting (без мутаций; заявка жива).
    private async Task<Result<RotationOutcome>> WaitAsync(string cluster, string phase, CancellationToken ct)
    {
        var waiting = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return waiting.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.Waiting)
            : Result<RotationOutcome>.Failed(waiting.Error!);
    }

    // config.state=TO_REMOVE — безопасная остановка (порт TlsMigrator.AbortAsync).
    private async Task<Result<RotationOutcome>> AbortAsync(string cluster)
    {
        var aborted = await journal.WritePhaseAsync(
            cluster, Op, "aborted-state-changed", claims.InstanceId, null, CancellationToken.None);
        return aborted.IsSuccess
            ? Result<RotationOutcome>.Success(RotationOutcome.Waiting)
            : Result<RotationOutcome>.Failed(aborted.Error!);
    }

    private async Task<Result<bool>> ConfigRemovedAsync(string cluster, CancellationToken ct)
    {
        var read = await GetAsync(ProcessCommon.ConfigKey(cluster), ct);
        if (!read.IsSuccess)
            return Result<bool>.Failed(read.Error!);
        return Result<bool>.Success(read.Value is { } kv && TryReadState(kv.Value) is "TO_REMOVE");
    }

    private static string? TryReadState(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("state", out var state)
                   && state.ValueKind == JsonValueKind.String
                ? state.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null; // битый config — не TO_REMOVE (парсер уже отметил ошибку)
        }
    }

    // Отказ фазы — last_error в journal (spec §5 «Отказ etcd/docker между
    // фазами: Failed c last_error в journal»), затем Failed (ретрай тиком).
    private async Task<Result<RotationOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return written.IsSuccess
            ? Result<RotationOutcome>.Failed(error)
            : Result<RotationOutcome>.Failed(written.Error!);
    }

    private static string TicketKey(string cluster) => $"/valkeyworker/ca_rotations/{cluster}";

    private static string NextKeyKey(string cluster) => $"/valkey/clusters/{cluster}/ca_next_key";

    private static string NextPemKey(string cluster) => $"/valkey/clusters/{cluster}/ca_next_pem";

    private static string CaPemKey(string cluster) => $"/valkey/clusters/{cluster}/ca_pem";

    private static string CaKeyKey(string cluster) => $"/valkey/clusters/{cluster}/ca_key";

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

- [ ] **Step 2.6: прогнать — PASS**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~CaRotatorTests"`
Ожидание: все кейсы зелёные.

- [ ] **Step 2.7: коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation
git add src/ValkeyWorker.Provisioning/Processes/CaRotator.cs \
  src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs
git commit -m "feat(vwk): CaRotator t07 — K0-диспетчеризация (окно/ждущие/abort) + фазы P (staging put-if-absent) и D (bundle OLD+NEW, compare; отказы через FailAsync с last_error) с юнитами"
```

---

### Task 3: CaRotator — фазы R, C, K4 + обновление кейсов Task 2 под финал

**Files:**
- Modify: `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs` (заменить плейсхолдер `ReplayNodeCommitAsync` на R→C→K4; удалить строку `await Task.CompletedTask`)
- Modify: `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` (поле `LastCaPem` в `FakeValkeyConnection` — заполняется в `PingAsync` из якоря endpoint'а; используется кейсами AwaitBoot)
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs` (Шаг 3.1 — ОБЯЗАТЕЛЬНОЕ обновление кейсов Task 2; Шаг 3.2 — новые кейсы R/C/K4)

**Interfaces:**
- Consumes: `NodeTlsProvisioner.IsValidTar` (internal static, 4 аргумента: `tar, advertisedHost, caPem, clock`); `driver.GetTlsArchiveAsync(cluster, host, image, ct)`; `ProcessCommon.ParsePortAlloc/WriteNodeStateAsync/ParseResources`; `NodeArgsBuilder.Build`; `ValkeyNodeSpec(Cluster, NodeName, Host, ClientHostPort, Image, Args, CpuCores, MemoryBytes, TlsVolume:)`; `PlainClusterDriver.TlsVolumeName(cluster)`; `ValkeyEndpoint(host, port, "admin", adminPw, nextPem)`; `options.{NodeBootSec, AdvertisedClientHost, NodeImage}`; `CaRotator.FailAsync` (Task 2).
- Produces: полный `CaRotator.RunAsync` — P→D→R→C→K4 одним вызовом; используется задачами 4, 10, 11.

**Вход:** Task 2 зелёный/закоммичен.

**Действие:** (а) обновить кейсы Task 2, ассертившие промежуточное состояние окна, под ФИНАЛЬНОЕ состояние полного цикла (один `RunAsync` = весь цикл, `journal=done`); кейс `BundleAlreadyContainsNext_PutSkipped` — остановка окна инжектом `TxnFault` на коммит-txn; (б) реализация R (факт-детект + пересоздание node1 + AwaitBoot с якорем nextPem), C (атомарный коммит-txn), K4 (финал); (в) новые кейсы §7.1.

**Выход:** CaRotator полный; ВСЕ кейсы (обновлённые Task 2 + новые Task 3) зелёные.

**Проверка:** точечный прогон `CaRotatorTests` зелёный.

**Связь со spec:** §3.1 (txn C), §5 (R, C, K4, отказы с last_error), §7.1.

- [ ] **Step 3.1: обновить кейсы Task 2 под финальное состояние** (заменить тела кейсов `Ticket_OpensWindow_StagingThenBundle`, `OpenWindow_SkipsGuards_ReplaysFromFact`, `ReplayTick_ReusesExistingStaging_NoSecondGeneration`, `BundleAlreadyContainsNext_PutSkipped`):

```csharp
    [Fact]
    public async Task Ticket_FullWindowInOneTick_CommitsNewCa()
    {
        // Arrange — канонический кластер + заявка; один RunAsync проводит
        // ВЕСЬ цикл P→D→R→C→K4 (nodes=1, FakeValkeyConnection отвечает).
        // Было Ticket_OpensWindow_StagingThenBundle (промежуточные ассерты
        // staging/bundle — устарели после появления R/C: коммит сворачивает
        // bundle и удаляет staging).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c6");
        rig.SeedTicket("c6");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c6"), TestContext.Current.CancellationToken);

        // Assert — финал: ca_pem/ca_key = NEW (сгенерированы фазой P — сам
        // факт смены доказывает, что P и D исполнились), staging/заявка
        // удалены, journal done, снапшот «после» снят
        outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var newPem = rig.Get("/valkey/clusters/c6/ca_pem")!;
        newPem.Should().NotBe(oldPem).And.NotContain(oldPem, "bundle свёрнут после коммита");
        rig.Get("/valkey/clusters/c6/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/c6/ca_next_pem").Should().BeNull();
        rig.Get("/valkeyworker/ca_rotations/c6").Should().BeNull();
        rig.Get("/valkeyworker/work/c6").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot", "K4 снял снапшот «после»");
    }

    [Fact]
    public async Task OpenWindow_SkipsGuards_ReplaysToCommitDespitePasswordTicket()
    {
        // Arrange — окно ОТКРЫТО ПОСЕВОМ ca_next_* (иначе K0.5 дал бы Waiting
        // ещё до открытия) ПРИ живой пароль-заявке: K0.3 срабатывает по факту
        // staging — guard'ы K0.4–K0.6 НЕ выполняются (spec §5 K0.3)
        var rig = Rig.Create();
        rig.SeedTls("c7");
        rig.SeedTicket("c7");
        rig.SeedWindow("c7"); // ca_next_pem/ca_next_key — окно открыто
        rig.Put("/valkeyworker/rotations/c7",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act — тик доигрывает окно до коммита ПРИ живой пароль-заявке
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c7"), TestContext.Current.CancellationToken);

        // Assert — не Waiting; цикл ЗАВЕРШЁН despite живую пароль-заявку;
        // пароль-заявка не тронута (E не идёт — его исполнит ветка после K)
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Get("/valkeyworker/work/c7").Should().Contain("done");
        rig.Get("/valkey/clusters/c7/ca_next_key").Should().BeNull("коммит прошёл");
        rig.Get("/valkeyworker/rotations/c7").Should().NotBeNull("заявка креда не тронута");
    }

    [Fact]
    public async Task ReplayTick_CommitsFromForeignStaging()
    {
        // Arrange — staging уже лежит (например, после краха между P и D):
        // re-read переиспользует ЧУЖУЮ генерацию
        var rig = Rig.Create();
        rig.SeedTls("c8");
        rig.SeedTicket("c8");
        var (foreignPem, foreignKey) = rig.SeedWindow("c8");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("c8"), TestContext.Current.CancellationToken);

        // Assert — коммит ОТ ЧУЖОЙ staging: ca_pem/ca_key = foreign*,
        // staging удалён — переиспользование доказано самим коммитом
        rig.Get("/valkey/clusters/c8/ca_pem").Should().Be(foreignPem);
        rig.Get("/valkey/clusters/c8/ca_key").Should().Be(foreignKey);
        rig.Get("/valkey/clusters/c8/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/c8/ca_next_pem").Should().BeNull();
    }

    [Fact]
    public async Task BundleAlreadyContainsNext_PutSkipped_WindowHeldByCTxnFault()
    {
        // Arrange — D отработан (bundle в ca_pem). Проверяемый факт — пропуск
        // D-put (ModRevision ca_pem не меняется). Чтобы фаза C не перезаписала
        // ca_pem (это сломало бы факт), окно ОСТАНАВЛИВАЕТСЯ инжектом отказа
        // ТОЛЬКО коммит-txn фазы C (1 compare + 5 success-операций; D-txn —
        // 1+1, P-txn — 2+2; journal-записи идут через PutAsync, не txn —
        // WorkJournal.cs:88-89, txn-эвристика корректна). Свойство запроса —
        // TxnRequest.Success (НЕ Successes; IEtcdGateway.cs:93-96).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("c9");
        rig.SeedTicket("c9");
        var (nextPem, nextKey) = rig.SeedWindow("c9");
        rig.Put("/valkey/clusters/c9/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("c9", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken); // R: факт-детект true
        var revisionBefore = rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision;
        rig.Etcd.TxnFault = req => req.Success.Count >= 5
            ? Result<Shared.Etcd.Client.TxnResult>.Failed(new ApplicationException("инжект: отказ C-txn"))
            : null;

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("c9"), TestContext.Current.CancellationToken);

        // Assert — D-put пропущен (ревизия ca_pem не тунула: меняли только
        // фаза D — пропущена — и фаза C — сорвана инжектом); окно ЖИВО
        // (staging на месте, bundle в ca_pem), Failed с last_error
        rig.Etcd.Store["/valkey/clusters/c9/ca_pem"].ModRevision.Should().Be(revisionBefore,
            "put bundle пропущен (Contains-детект), коммит сорван инжектом");
        outcome.IsSuccess.Should().BeFalse("C-txn отказ — Failed");
        rig.Get("/valkey/clusters/c9/ca_next_key").Should().NotBeNull("окно живо");
        rig.Get("/valkey/clusters/c9/ca_pem").Should().Be(oldPem + "\n" + nextPem, "bundle неизменен");
        rig.Get("/valkeyworker/work/c9").Should().Contain("last_error");
    }
```

ПРИМЕЧАНИЕ: `FakeEtcd.TxnFault` — сигнатура `Func<TxnRequest, Result<TxnResult>?>`; в запросе — свойство `Success` (`TxnRequest(Compare, Success, Failure)`, `src/Shared.Etcd/Client/IEtcdGateway.cs:93-96`). `Result<...TxnResult>.Failed(...)` привести к фактической сигнатуре фейка при расхождении.

- [ ] **Step 3.2: новые кейсы R/C/K4** — добавить в `CaRotatorTests`:

```csharp
    [Fact]
    public async Task FullCycle_CommitsNewCa_RemovesStagingAndTicket()
    {
        // Arrange — канонический кластер + заявка; FakeValkeyConnection отвечает
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r1");
        rig.SeedTicket("r1");

        // Act — ОДИН тик проводит весь цикл (nodes=1: «rolling» = одно
        // пересоздание; EnsureNodeTls кладёт серт NEW в volume)
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r1"), TestContext.Current.CancellationToken);

        // Assert — коммит: ca_pem/ca_key = NEW, staging/заявка удалены, done
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        var newPem = rig.Get("/valkey/clusters/r1/ca_pem")!;
        newPem.Should().NotBe(oldPem).And.NotContain(oldPem, "bundle свёрнут после коммита");
        rig.Get("/valkey/clusters/r1/ca_key").Should().NotBe(oldPem);
        rig.Get("/valkey/clusters/r1/ca_next_key").Should().BeNull();
        rig.Get("/valkey/clusters/r1/ca_next_pem").Should().BeNull();
        rig.Get("/valkeyworker/ca_rotations/r1").Should().BeNull();
        rig.Get("/valkeyworker/work/r1").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot", "K4 снял снапшот «после»");
    }

    [Fact]
    public async Task PhaseR_FactDetect_ValidNewTar_SkipsRecreate()
    {
        // Arrange — staging есть, bundle есть, серт NEW УЖЕ в volume (рестарт
        // воркера посреди R): факт-детект обязан пропустить пересоздание
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r2");
        rig.SeedTicket("r2");
        var (nextPem, nextKey) = rig.SeedWindow("r2");
        rig.Put("/valkey/clusters/r2/ca_pem", oldPem + "\n" + nextPem);
        var ensured = await rig.Tls.EnsureNodeTlsAsync("r2", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        rig.Driver.Removed.Clear();
        var containerBefore = rig.Driver.Containers[$"vwk-r2-node1"].Id;

        // Act — тик доигрывает: R пропущен (факт), сразу C
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r2"), TestContext.Current.CancellationToken);

        // Assert — контейнер не тронут, RemoveNode не звался, коммит прошёл
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Driver.Removed.Should().BeEmpty("факт-детект: пересоздание не нужно");
        rig.Driver.Containers[$"vwk-r2-node1"].Id.Should().Be(containerBefore);
        rig.Get("/valkey/clusters/r2/ca_pem").Should().Be(nextPem);
    }

    [Fact]
    public async Task PhaseR_RecreatesNode_FromNewCa_ThenBoots()
    {
        // Arrange — staging+bundle есть, серт в volume — OLD (факт-детект
        // false); декларация ресурсов node1 — для ассерта лимитов (spec §7.1:
        // «порт/лимиты из portalloc/декларации»)
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r3", port: 17005); // порт portalloc = 17005
        rig.Put("/valkey/clusters/r3/nodes/node1/resources",
            """{"cpu":"2","mem":"1Gi","disk":"2Gi"}"""); // формат UpdateResourcesHandler
        rig.SeedTicket("r3");
        var (nextPem, nextKey) = rig.SeedWindow("r3");
        rig.Put("/valkey/clusters/r3/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r3", "node1", "h1", "localhost",
            oldPem, rig.Get("/valkey/clusters/r3/ca_key")!, TestContext.Current.CancellationToken);

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r3"), TestContext.Current.CancellationToken);

        // Assert — пересоздание: RemoveNode+EnsureNode звались с портом из
        // portalloc (ClientHostPort=17005) и лимитами из декларации
        // (CpuCores=2, MemoryBytes=1Gi); state RUNNING; PING — якорь NEW.
        // Removed содержит ПОЛНОЕ имя контейнера («vwk-r3-node1»), не «node1»
        // (FakeDriver.RemoveNodeAsync → PlainClusterDriver.NodeName).
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Driver.Removed.Should().Contain("vwk-r3-node1");
        rig.Driver.Ensured.Should().ContainSingle(s => s.NodeName == "node1"
            && s.Host == "h1"
            && s.ClientHostPort == 17005
            && s.CpuCores == 2m
            && s.MemoryBytes == 1024L * 1024 * 1024
            && s.TlsVolume == ValkeyWorker.Docker.Drivers.PlainClusterDriver.TlsVolumeName("r3"));
        rig.Get("/valkey/clusters/r3/nodes/node1/state").Should().Be("RUNNING");
        rig.Valkey.LastCaPem.Should().Be(nextPem, "AwaitBoot — якорь NEW (одноблочный парсер)");
    }

    [Fact]
    public async Task PhaseR_ToRemove_Aborts()
    {
        // Arrange — окно открыто, config сменился на TO_REMOVE перед R
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r4");
        rig.SeedTicket("r4");
        var (nextPem, nextKey) = rig.SeedWindow("r4");
        rig.Put("/valkey/clusters/r4/ca_pem", oldPem + "\n" + nextPem);
        rig.Put("/valkey/clusters/r4/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r4"), TestContext.Current.CancellationToken);

        // Assert — abort, нода не тронута, коммита нет
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.Waiting);
        rig.Driver.Removed.Should().BeEmpty();
        rig.Get("/valkeyworker/work/r4").Should().Contain("aborted-state-changed");
        rig.Get("/valkey/clusters/r4/ca_pem").Should().Contain(oldPem, "коммита не было");
    }

    [Fact]
    public async Task PhaseC_CompareLost_Failed()
    {
        // Arrange — staging-ключ подменён «параллельной ротацией» до compare
        // txn фазы C. Эвристика: в ЭТОМ кейсе P/D пропущены предпосевом
        // (staging+bundle лежат), journal идёт PutAsync — единственный txn
        // с Put — коммит-txn фазы C (2 Put: ca_pem + ca_key).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r5");
        rig.SeedTicket("r5");
        var (nextPem, nextKey) = rig.SeedWindow("r5");
        rig.Put("/valkey/clusters/r5/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r5", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);
        rig.Etcd.OnTxnBeforeCompare = req =>
        {
            if (req.Success.Count(o => o is TxnOp.Put) >= 2)
                rig.Etcd.Store["/valkey/clusters/r5/ca_next_key"] =
                    rig.Etcd.Store["/valkey/clusters/r5/ca_next_key"] with { Value = "foreign" };
        };

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r5"), TestContext.Current.CancellationToken);

        // Assert — срыв compare: Failed «ретрай тиком» + last_error,
        // ca_pem остался bundle
        outcome.IsSuccess.Should().BeFalse();
        outcome.Error!.Message.Should().Contain("ретрай");
        rig.Get("/valkey/clusters/r5/ca_pem").Should().Contain(oldPem);
        rig.Get("/valkeyworker/work/r5").Should().Contain("last_error");
    }

    [Fact]
    public async Task TicketRemovedManually_CommitStillSafe()
    {
        // Arrange — заявку сняли руками (del вне txn): del в txn — no-op
        // (spec §5 C). Окно открыто вручную staging'ом (заявки нет).
        var rig = Rig.Create();
        var oldPem = rig.SeedTls("r6");
        var (nextPem, nextKey) = rig.SeedWindow("r6");
        rig.Put("/valkey/clusters/r6/ca_pem", oldPem + "\n" + nextPem);
        await rig.Tls.EnsureNodeTlsAsync("r6", "node1", "h1", "localhost",
            nextPem, nextKey, TestContext.Current.CancellationToken);

        // Act — заявки нет, но окно открыто (staging): доигрывание до C
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r6"), TestContext.Current.CancellationToken);

        // Assert — коммит дошёл: del отсутствующей заявки — no-op
        outcome.Value.Should().Be(ValkeyWorker.Provisioning.Processes.CaRotator.RotationOutcome.InProgress);
        rig.Get("/valkey/clusters/r6/ca_pem").Should().Be(nextPem);
        rig.Get("/valkey/clusters/r6/ca_next_key").Should().BeNull();
    }

    [Fact]
    public async Task OrderInvariant_RNotBeforeD()
    {
        // Arrange — порядок etcd-операций одного RunAsync: put ca_pem (bundle,
        // фаза D) строго РАНЬШЕ put state PROVISIONING (фаза R) — инвариант
        // «NEW-серт на ноде ТОЛЬКО после bundle в ca_pem»
        var rig = Rig.Create();
        rig.SeedTls("r7");
        rig.SeedTicket("r7");
        var ops = new List<string>();
        rig.Etcd.OnPut = key => ops.Add($"put:{key}");

        // Act
        await rig.Rotator.RunAsync(rig.Snapshot("r7"), TestContext.Current.CancellationToken);

        // Assert
        var bundlePut = ops.IndexOf("put:/valkey/clusters/r7/ca_pem");
        var statePut = ops.IndexOf("put:/valkey/clusters/r7/nodes/node1/state");
        bundlePut.Should().BeGreaterThanOrEqualTo(0, "bundle записан фазой D");
        statePut.Should().BeGreaterThan(bundlePut, "R (state) строго после D (bundle)");
    }

    [Fact]
    public async Task AfterCommitTail_FinishesWithoutStagingTouch()
    {
        // Arrange — journal committed, заявка снята (краш между C и K4)
        var rig = Rig.Create();
        rig.SeedTls("r8");
        var (nextPem, _) = rig.SeedWindow("r8");
        rig.Put("/valkey/clusters/r8/ca_pem", nextPem);
        rig.Put("/valkeyworker/work/r8", """{"op":"rotate-ca","phase":"committed","instance":"x","updated_unix":1756500000}""");

        // Act
        var outcome = await rig.Rotator.RunAsync(rig.Snapshot("r8"), TestContext.Current.CancellationToken);

        // Assert — K4: done + снапшот; никаких мутаций ключей кластера
        rig.Get("/valkeyworker/work/r8").Should().Contain("done");
        rig.Snapshots.Should().Contain("shot");
        rig.Driver.Removed.Should().BeEmpty();
        rig.Valkey.LastCaPem.Should().BeNull("PING в хвосте не выполняется");
    }
```

ЗАМЕЧАНИЯ: (1) `rig.Valkey.LastCaPem` — если у `FakeValkeyConnection` нет такого свойства, ДОБАВИТЬ в `Fakes.cs` поле `public string? LastCaPem;`, заполняемое в `PingAsync` из `endpoint.CaPem` (сверить фактическое имя поля якоря у `ValkeyEndpoint`). (2) Формат journal-JSON в `AfterCommitTail`-посеве свести с фактическим JSON `WorkJournal.WritePhaseAsync` (имена полей `op/phase` — по `Shared.Etcd/Coordination/WorkJournal.cs`). (3) `SeedWindow` в `AfterCommitTail` используется частично (нужен только nextPem для посева ca_pem; ключ staging потом удалён вручную не будет — окно хвоста не требует отсутствия staging, K0.2 срабатывает раньше; если парсер снапшота заругается на ca_next_* как unknownKeys — это warning-лог, не ошибка теста).

- [ ] **Step 3.3: прогнать — FAIL** (кейсы Step 3.1 с финальными ассертами и новые кейсы Step 3.2 красные: плейсхолдер не доводит цикл до коммита). В ТОМ ЧИСЛЕ кейс `BundleAlreadyContainsNext_PutSkipped_WindowHeldByCTxnFault`: против плейсхолдера C-txn не существует, инжект не срабатывает, `RunAsync` возвращает успех — ассерты `IsSuccess.BeFalse()` и `last_error` красные. Гейт шага (ожидание FAIL) не страдает: все кейсы файла, кроме не требующих R/C (K0-группа), красные.

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~CaRotatorTests"`
Ожидание: финальные/новые кейсы FAIL (включая c9-кейс с инжектом).

- [ ] **Step 3.4: реализовать R→C→K4** — заменить плейсхолдер `ReplayNodeCommitAsync` в `CaRotator.cs`:

```csharp
    // R: пересоздание node1 с перевыпуском серта от NEW (лечение ЛЮБОГО
    // состояния ноды; преф-чека живости нет — nodes=1, persistence off,
    // кеш восполним; spec §5 R) → C: атомарный коммит → K4: финал.
    private async Task<Result<RotationOutcome>> ReplayNodeCommitAsync(
        ValkeyClusterSnapshot snap, string nextPem, string nextKey, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // R.1: факт-детект — валидный NEW-серт в TLS-volume ⇒ R завершён.
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<RotationOutcome>.Failed(addresses.Error!);
        if (!addresses.Value.TryGetValue("node1", out var address))
            return Result<RotationOutcome>.Failed(new ApplicationException(
                $"rotate-ca {cluster}: node1 не закреплён в portalloc"));
        var archive = await driver.GetTlsArchiveAsync(cluster, address.Host, options.NodeImage, ct);
        if (!archive.IsSuccess)
            return await FailAsync(cluster, archive.Error!, "phase-r", ct);
        if (archive.Value is { } tar && NodeTlsProvisioner.IsValidTar(tar,
                options.AdvertisedClientHost ?? address.Host, nextPem, _clock))
            return await CommitAsync(cluster, nextPem, nextKey, ct);

        // R.2: гонка TO_REMOVE перед пересозданием — abort.
        var removed = await ConfigRemovedAsync(cluster, ct);
        if (!removed.IsSuccess)
            return Result<RotationOutcome>.Failed(removed.Error!);
        if (removed.Value)
            return await AbortAsync(cluster);

        // R.3: серт/ca.pem volume = NEW (НЕ bundle: --tls-auth-clients no),
        // journal phase-r, RemoveNode → EnsureNode (порт/лимиты прежние —
        // порт из portalloc, лимиты из декларации resources ноды).
        var tls = await tlsProvisioner.EnsureNodeTlsAsync(
            cluster, "node1", address.Host, options.AdvertisedClientHost ?? address.Host,
            nextPem, nextKey, ct);
        if (!tls.IsSuccess)
            return await FailAsync(cluster, tls.Error!, "phase-r", ct);
        var markedR = await journal.WritePhaseAsync(cluster, Op, "phase-r", claims.InstanceId, null, ct);
        if (!markedR.IsSuccess)
            return Result<RotationOutcome>.Failed(markedR.Error!);

        var nodeSnap = snap.Nodes.GetValueOrDefault("node1");
        var limits = ProcessCommon.ParseResources(nodeSnap?.Resources);
        var args = NodeArgsBuilder.Build(
            snap.Config?.MaxmemoryBytes ?? 0, snap.Config?.MaxmemoryPolicy ?? "allkeys-lru",
            snap.AdminPassword!, snap.AppPassword!);
        var removedNode = await driver.RemoveNodeAsync(cluster, "node1", ct);
        if (!removedNode.IsSuccess)
            return await FailAsync(cluster, removedNode.Error!, "phase-r/node1", ct);
        var ensured = await driver.EnsureNodeAsync(new ValkeyNodeSpec(
            cluster, "node1", address.Host, address.ClientPort, options.NodeImage, args,
            limits?.Cpu, limits?.MemBytes,
            TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)), ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "phase-r/node1", ct);
        var provisioning = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "PROVISIONING", ct);
        if (!provisioning.IsSuccess)
            return Result<RotationOutcome>.Failed(provisioning.Error!);
        var markedRNode = await journal.WritePhaseAsync(
            cluster, Op, "phase-r/node1", claims.InstanceId, null, ct);
        if (!markedRNode.IsSuccess)
            return Result<RotationOutcome>.Failed(markedRNode.Error!);

        // R.4: AwaitBoot — PING по TLS с якорем nextPem (серт уже NEW);
        // бюджет NodeBootSec, цикл 100 мс (порт TlsMigrator.AwaitBootAsync).
        var boot = await AwaitBootAsync(address, snap.AdminPassword!, nextPem, ct);
        if (!boot.IsSuccess)
            return await FailAsync(cluster, boot.Error!, "boot-timeout", ct);
        var running = await ProcessCommon.WriteNodeStateAsync(
            gateway, endpoints, cluster, "node1", "RUNNING", ct);
        if (!running.IsSuccess)
            return Result<RotationOutcome>.Failed(running.Error!);

        return await CommitAsync(cluster, nextPem, nextKey, ct);
    }

    // C: атомарный коммит ОДНОЙ txn (compare по staging-ключу — гонка
    // параллельной ротации закрыта) → K4.
    private async Task<Result<RotationOutcome>> CommitAsync(
        string cluster, string nextPem, string nextKey, CancellationToken ct)
    {
        var markedC = await journal.WritePhaseAsync(cluster, Op, PhaseCommitted, claims.InstanceId, null, ct);
        if (!markedC.IsSuccess)
            return Result<RotationOutcome>.Failed(markedC.Error!);
        var commit = await TxnAsync(TxnRequest.Of(
            [TxnCompare.ValueEqual(NextKeyKey(cluster), nextKey)],
            [
                new TxnOp.Put(CaPemKey(cluster), nextPem, null),
                new TxnOp.Put(CaKeyKey(cluster), nextKey, null),
                new TxnOp.Delete(NextPemKey(cluster), Prefix: false),
                new TxnOp.Delete(NextKeyKey(cluster), Prefix: false),
                new TxnOp.Delete(TicketKey(cluster), Prefix: false),
            ]), ct);
        if (!commit.IsSuccess)
            return await FailAsync(cluster, commit.Error!, PhaseCommitted, ct);
        if (!commit.Value.Succeeded)
            return await FailAsync(cluster, new ApplicationException(
                $"rotate-ca {cluster}: ca_next_key изменился с момента чтения (параллельная ротация?) — ретрай тиком"), PhaseCommitted, ct);

        return await FinishAsync(cluster, ct);
    }

    // PING по TLS с якорем NEW: транзиент-толерантный цикл в бюджете NodeBootSec.
    private async Task<Result> AwaitBootAsync(
        NodeAddress address, string adminPassword, string nextPem, CancellationToken ct)
    {
        var endpoint = new ValkeyEndpoint(
            options.AdvertisedClientHost ?? address.Host, address.ClientPort,
            "admin", adminPassword, nextPem);
        var startedAt = _clock.GetUtcNow();
        var budget = TimeSpan.FromSeconds(options.NodeBootSec);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var ping = await valkey.PingAsync(endpoint, ct);
            if (ping.IsSuccess)
                return Result.Success();
            if (_clock.GetUtcNow() - startedAt > budget)
                return Result.Failed(new TimeoutException(
                    $"rotate-ca нода не отвечает по TLS (якорь NEW) {budget.TotalSeconds:F0} c " +
                    $"({ping.Error!.Message})"));
            await Task.Delay(100, ct);
        }
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(ProcessCommon.PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
            ProcessCommon.ParsePortAlloc(kv.Value));
    }
```

Сверить по факту: имя свойства серта-якоря в `ValkeyEndpoint` (5-й позиционный аргумент — как в `TlsMigrator.AwaitBootAsync`); `ValkeyNodeSpec` — поля `NodeName/Host/ClientHostPort/CpuCores/MemoryBytes` + именованный `TlsVolume` (ClusterDriver.cs:23-32); `ProcessCommon.RotationStateKey` — существует (используется `PasswordRotator`).

- [ ] **Step 3.5: прогнать — PASS (ВСЕ кейсы: обновлённые Task 2 + новые Task 3)**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~CaRotatorTests"`
Ожидание: весь файл зелёный.

- [ ] **Step 3.6: коммит**

```bash
git add src/ValkeyWorker.Provisioning/Processes/CaRotator.cs \
  src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs \
  src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs
git commit -m "feat(vwk): CaRotator t07 — фазы R (факт-детект IsValidTar + пересоздание node1 + AwaitBoot якорем NEW), C (атомарный коммит-txn), K4 (финал); кейсы окна переведены на финальное состояние полного цикла (инжект C-txn-отказа для ModRevision-кейса)"
```

---

### Task 4: Вентиль Active-ветки + DI-регистрация

**Files:**
- Modify: `src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs` (конструктор + вставка шага K)
- Modify: `src/ValkeyWorker.App/Program.cs` (DI после TlsMigrator, строки ~166–176)
- Test: `src/tests/ValkeyWorker.UnitTests/App/ValkeyClusterProcessesTests.cs` (Rig: +аргумент CaRotator; +кейсы вентиля)

**Interfaces:**
- Consumes: `CaRotator.RunAsync` → `Result<RotationOutcome>`; `TlsMigrator.MigrationOutcome`.
- Produces: Active-ветка = T → K → C/D/E; окно `InProgress` ⇒ `C/D/E` в тике не идут.

**Вход:** Task 3 закоммичен.

**Действие:** вставка эксклюзивного шага K после T (spec §4.2); DI; вентиль-тесты.

**Выход:** цикл исполняет ротацию; юниты цикла зелёные.

**Проверка:** `--filter "FullyQualifiedName~ValkeyClusterProcessesTests"` зелёный.

**Связь со spec:** §2.4 (эксклюзивность), §4.2, §7.1 (вентиль-кейс).

- [ ] **Step 4.1: фейлящие вентиль-тесты** — в `ValkeyClusterProcessesTests` (Rig создаёт реальные процессы на фейках; добавить в `Rig`-конструктор аргумент `CaRotator` — см. Step 4.3). МЕХАНИКА: journal `work/<C>` — ЕДИНЫЙ KV, перезаписывается каждым процессом (`supervise` → `rotate` — надзор и ротатор пишут свои op); поэтому ассерты «waiting-фаза K дожила до конца тика» невозможны — Waiting не блокирует ветку, и K-запись перезаписывается идущими ниже C/E. Доказательства вентиля: (InProgress) journal `rotate-ca done` без `supervise`; (Waiting) ротатор E завершил свою заявку тем же тиком:

```csharp
    [Fact]
    public async Task Active_CaRotationInProgress_SupervisorConvergerSkipped()
    {
        // Arrange — канонический TLS-кластер + заявка CA-ротации: тик
        // исполнит ротацию (окно было открыто в момент вызова K) и вернёт
        // InProgress — надзор/конвергер/ротатор в этом тике НЕ вызываются
        var rig = new Rig();
        rig.SeedTlsCanonical("gate1"); // посев канонического кластера — по
        // образцу существующих кейсов файла (клэйм+config+креды+ca-ключи+
        // endpoints+portalloc+state RUNNING+контейнер TLS-args)
        rig.Etcd.Seed("/valkeyworker/ca_rotations/gate1",
            """{"requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert — журнал работы держит op=rotate-ca done: надзор НЕ
        // перезаписал его своим op («supervise» появился бы, если бы ветка
        // дошла до C) — эксклюзивность окна доказана journal'ом
        var work = rig.Work("gate1");
        work.Should().Contain("rotate-ca", "K исполнился и держит ветку");
        work.Should().Contain("done", "цикл завершён одним тиком");
        work.Should().NotContain("supervise", "надзор в окне не идёт");
        // Коммит прошёл: заявка снята (staging после C отсутствует —
        // ассертить его наличие НЕЛЬЗЯ)
        rig.Etcd.Store.TryGetValue("/valkeyworker/ca_rotations/gate1", out _)
            .Should().BeFalse("заявка снята атомарно коммиту фазы C");
    }

    [Fact]
    public async Task Active_CaRotationWaiting_PasswordRotationPlays()
    {
        // Arrange — канонический кластер + заявки: CA (K вернёт Waiting —
        // окно не открывается) И пароль (E доиграет тем же тиком ниже по
        // ветке). Журнал: K запишет waiting-password-rotation, но затем
        // надзор C и ротатор E перезапишут work/<C> своими op — финальное
        // состояние журнала «rotate done», а НЕ waiting-фаза K.
        var rig = new Rig();
        rig.SeedTlsCanonical("gate2");
        rig.Etcd.Seed("/valkeyworker/ca_rotations/gate2",
            """{"requested_unix":1756500000,"requested_by":"it"}""");
        rig.Etcd.Seed("/valkeyworker/rotations/gate2",
            """{"role":"app","requested_unix":1756500000,"requested_by":"it"}""");

        // Act
        await rig.Processes.TickAsync(TestContext.Current.CancellationToken);

        // Assert — ветка НЕ заблокирована Waiting: ротатор E доиграл свою
        // заявку этим же тиком (заявка rotations снята, журнал — op rotate
        // done); CA-заявка жива, окно НЕ открывалось
        rig.Etcd.Store.TryGetValue("/valkeyworker/rotations/gate2", out _)
            .Should().BeFalse("E доиграл заявку пароля этим же тиком");
        var work = rig.Work("gate2");
        work.Should().Contain("\"rotate\"", "журнал завершён ротатором E (K не заблокировал ветку)");
        work.Should().Contain("done", "E доведён до конца");
        rig.Etcd.Store.TryGetValue("/valkeyworker/ca_rotations/gate2", out _)
            .Should().BeTrue("CA-заявка ждёт (окно не открывалось)");
        rig.Etcd.Store.TryGetValue("/valkey/clusters/gate2/ca_next_key", out _)
            .Should().BeFalse("окно не открывалось");
    }
```

Хелперы: `rig.Work(cluster)` — чтение `/valkeyworker/work/<C>` (добавить, если нет); `SeedTlsCanonical` — по образцу существующего посева канонического кластера в этом файле (если посев уже есть под другим именем — переиспользовать его имя, не плодить).

- [ ] **Step 4.2: прогнать — FAIL** (компиляция: `ValkeyClusterProcesses` без параметра `caRotator`).

- [ ] **Step 4.3: правка вентиля и DI**

`src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs` — конструктор (после `TlsMigrator tlsMigrator,`):

```csharp
    CaRotator caRotator,
```

В `case ValkeyClusterKind.Active:` после блока `tlsMigrator.RunAsync` (внутри того же `RunClusterOpAsync(cluster, "active", async () => { ... })`):

```csharp
                        // t07: ротация CA — ВТОРОЙ шаг Active-ветки (arch/21 §5 K):
                        // окно открыто (InProgress) ⇒ надзор/converge/ротация в
                        // этом тике не идут (узкое окно двойного доверия);
                        // Waiting/NotNeeded — ветка продолжается (ждущие исходы
                        // ничего не мутировали; E доиграет ниже по ветке).
                        var rotation = await caRotator.RunAsync(snap, ct);
                        if (!rotation.IsSuccess)
                            return rotation.Error!;
                        if (rotation.Value == CaRotator.RotationOutcome.InProgress)
                            return Result.Success();
```

`src/ValkeyWorker.App/Program.cs` — после регистрации `TlsMigrator` (строки ~166–176):

```csharp
builder.Services.AddSingleton(sp => new CaRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<NodeTlsProvisioner>(),
    sp.GetRequiredService<IValkeyConnection>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
```

`src/tests/ValkeyWorker.UnitTests/App/ValkeyClusterProcessesTests.cs` — в `Rig`-конструктор добавить тот же `CaRotator` (по образцу `TlsMigrator` в Rig: `new CaRotator(Etcd, ["http://etcd:2379"], Driver, Claims, journal, tlsProvisioner, Valkey, options, snapshot: null, Clock)`).

- [ ] **Step 4.4: прогнать — PASS**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyClusterProcessesTests"`
Ожидание: все кейсы (существующие + 2 новых) зелёные.

- [ ] **Step 4.5: коммит**

```bash
git add src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs src/ValkeyWorker.App/Program.cs \
  src/tests/ValkeyWorker.UnitTests/App/ValkeyClusterProcessesTests.cs
git commit -m "feat(vwk): вентиль Active-ветки t07 — CaRotator вторым шагом после миграции TLS (окно InProgress блокирует C/D/E; waiting не блокирует) + DI"
```

---

### Task 5: RotateCaHandler + endpoint `POST /api/valkey/clusters/{c}/ca/rotate`

**Files:**
- Create: `src/ValkeyWorker.App/Api/Operations/RotateCaHandler.cs`
- Modify: `src/ValkeyWorker.App/Api/ApiModule.cs` (MapPost рядом с `password/rotate`, строки ~139–175)
- Modify: `src/ValkeyWorker.App/Program.cs` (DI handler)
- Test: Create `src/tests/ValkeyWorker.UnitTests/Api/RotateCaHandlerTests.cs`

**Interfaces:**
- Consumes: `ValkeyApiHelpers.{ReadConfigAsync, ReadKeyAsync}`, `ValkeyEtcdFailover.CallAsync`, `ValkeyLimits.ClusterPattern()`, исключения `ValkeyExceptions.cs` (`ValkeyClusterNotFoundException`, `ValkeyClusterNotActiveException` — уже существует, `ValkeyRotationAlreadyRequestedException` — переиспользуется); `ValkeyConfigJson.State` (`ValkeyLimits.cs:72`).
- Produces: `ValkeyCaRotatedDto(Cluster, RequestedUnix, RequestedBy)`; заявка `{"requested_unix":u,"requested_by":user}` (БЕЗ role); endpoint 202/404/409/503.

**Вход:** Task 1 закоммичен (задача независима от 2–4 по коду, но по порядку идёт после).

**Действие:** порт kafka `RotateCaHandler` + маппинг endpoint.

**Выход:** заявка через API создаётся; юниты 404/409/202/payload зелёные.

**Проверка:** `--filter "FullyQualifiedName~RotateCaHandlerTests"` зелёный.

**Связь со spec:** §3.1 (заявка), §6.1, §7.2.

- [ ] **Step 5.1: фейлящие тесты** — `src/tests/ValkeyWorker.UnitTests/Api/RotateCaHandlerTests.cs` (фейк `FakeEtcd`; при недоступности `Fakes` из `Api`-неймспейса — using `ValkeyWorker.UnitTests.Provisioning` по факту проекта):

```csharp
using FluentAssertions;
using ValkeyWorker.App.Api.Operations;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Api;

// RotateCaHandler (t07): валидация имени → 404; config-гейт → 404;
// state-гейт (НЕ-Active) → 409; живая заявка → 409; happy-path → 202-DTO +
// payload заявки {"requested_unix","requested_by"} без role. AAA.
public class RotateCaHandlerTests
{
    private static readonly Fakes.FakeEtcd Etcd = new();

    public RotateCaHandlerTests()
    {
        Etcd.Seed("/valkey/clusters/live/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""");
        Etcd.Seed("/valkey/clusters/removing/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");
    }

    private static RotateCaHandler Handler()
        => new(Etcd, ["http://etcd:2379"], TimeProvider.System);

    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange / Act — имя вне канона
        var result = await Handler().HandleAsync("Bad_Name", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    [Fact]
    public async Task NoConfig_ClusterNotFound()
    {
        // Arrange / Act — имя каноническое, config-ключа нет
        var result = await Handler().HandleAsync("ghost", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    [Fact]
    public async Task NotActive_Conflict()
    {
        // Arrange / Act — state=TO_REMOVE: ротация не поднятого кластера бессмысленна
        var result = await Handler().HandleAsync("removing", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotActiveException>();
    }

    [Fact]
    public async Task LiveTicket_Conflict()
    {
        // Arrange — заявка уже жива
        Etcd.Seed("/valkeyworker/ca_rotations/live",
            """{"requested_unix":1756500000,"requested_by":"x"}""");

        // Act
        var result = await Handler().HandleAsync("live", "it", TestContext.Current.CancellationToken);

        // Assert — 409 (после исполнения ключ исчезает — POST снова валиден)
        result.Error.Should().BeOfType<ValkeyRotationAlreadyRequestedException>();
    }

    [Fact]
    public async Task HappyPath_ClaimTxnAndDto()
    {
        // Arrange / Act
        var result = await Handler().HandleAsync("live", "opsuser", TestContext.Current.CancellationToken);

        // Assert — 202-DTO + payload заявки в etcd (без role)
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("live");
        result.Value.RequestedBy.Should().Be("opsuser");
        result.Value.RequestedUnix.Should().BeGreaterThan(0);
        var raw = Etcd.GetAsync("http://etcd:2379", "/valkeyworker/ca_rotations/live",
            TestContext.Current.CancellationToken).GetAwaiter().GetResult().Value!.Value;
        raw.Should().Contain("\"requested_by\":\"opsuser\"").And.NotContain("role");
    }
}
```

- [ ] **Step 5.2: прогнать — FAIL** (тип не существует).

- [ ] **Step 5.3: реализация** — `src/ValkeyWorker.App/Api/Operations/RotateCaHandler.cs`:

```csharp
using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Ответ 202 POST /api/valkey/clusters/{c}/ca/rotate (арх-канон; дубль осознан).
public sealed record ValkeyCaRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Заявка ротации per-cluster CA/сертов через API воркера (t07, arch/21 §5 K):
// клэйм-txn /valkeyworker/ca_rotations/<C> NotExists; исполнение — CaRotator
// (фазы P/D/R/C). state-гейт: НЕ-Active (NOT_INITIALIZED/TO_REMOVE) → 409 —
// ротация не поднятого кластера бессмысленна (образец kafka, ОТЛИЧИЕ от
// ротации паролей). requested_by — заголовок X-Requested-By, fallback "api".
public sealed class RotateCaHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<ValkeyCaRotatedDto>> HandleAsync(
        string cluster, string requestedBy, CancellationToken ct)
    {
        // Имя каноническое, иначе 404.
        if (!ValkeyLimits.ClusterPattern().IsMatch(cluster))
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));

        // Кластер существует; НЕ-Active → 409.
        var config = await ValkeyApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result<ValkeyCaRotatedDto>.Failed(config.Error);
        if (config.Value is null)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result<ValkeyCaRotatedDto>.Failed(
                new ValkeyClusterNotActiveException(cluster, config.Value.State));

        // Живая заявка → 409 (после исполнения ключ исчезает — POST валиден).
        var key = $"/valkeyworker/ca_rotations/{cluster}";
        var ticket = await ValkeyApiHelpers.ReadKeyAsync(gateway, endpoints, key, ct);
        if (!ticket.IsSuccess)
            return Result<ValkeyCaRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        // Клэйм-txn: compare NotExists + put (протокол §9.8; БЕЗ role).
        var requestedUnix = time.GetUtcNow().ToUnixTimeSeconds();
        var txn = await ValkeyEtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(
                    key, JsonSerializer.Serialize(new CaRotationTicketJson(requestedUnix, requestedBy)), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<ValkeyCaRotatedDto>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<ValkeyCaRotatedDto>.Failed(new ValkeyRotationAlreadyRequestedException(cluster));

        return Result<ValkeyCaRotatedDto>.Success(
            new ValkeyCaRotatedDto(cluster, requestedUnix, requestedBy));
    }
}

// Заявка ротации CA (arch/20 §3): {"requested_unix","requested_by"} — без role.
public sealed record CaRotationTicketJson(
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string? RequestedBy);
```

`ApiModule.cs` — после блока `password/rotate` (тот же switch-набор ошибок; тело запроса пустое — биндить только заголовок):

```csharp
        // POST /api/valkey/clusters/{cluster}/ca/rotate — заявка ротации CA/сертов
        // (t07, окно двойного доверия P/D/R/C); 202/404/409/503.
        endpoints.MapPost("/api/valkey/clusters/{cluster}/ca/rotate", async (
            string cluster, HttpRequest http, RotateCaHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(cluster, RequestedBy(http), ct);
            if (result.IsSuccess)
                return Results.Accepted((string?)null, result.Value);

            return result.Error switch
            {
                ValkeyClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                ValkeyClusterNotActiveException or ValkeyRotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                EtcdWriteUnavailableException or InvalidValkeyConfigException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });
```

`Program.cs` — DI рядом с `RotatePasswordHandler`:

```csharp
builder.Services.AddSingleton(sp => new RotateCaHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
```

- [ ] **Step 5.4: прогнать — PASS; сборка всего солюшена — 0 warnings**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~RotateCaHandlerTests" && DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release`

- [ ] **Step 5.5: коммит**

```bash
git add src/ValkeyWorker.App/Api/Operations/RotateCaHandler.cs src/ValkeyWorker.App/Api/ApiModule.cs \
  src/ValkeyWorker.App/Program.cs src/tests/ValkeyWorker.UnitTests/Api/RotateCaHandlerTests.cs
git commit -m "feat(vwk): API t07 — POST /api/valkey/clusters/{c}/ca/rotate (клэйм-txn заявки, state-гейт 409, 202-DTO) с юнитами"
```

---

### Task 6: X2-чистка координации — `ca_rotations`

**Files:**
- Modify: `src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs:73-80` (перечень del-ключей)
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (+кейс)

**Interfaces:**
- Produces: X2 сносит `/valkeyworker/ca_rotations/<C>` вместе с прочей координацией (staging `ca_next_*` уже забирает `del --prefix /valkey/clusters/<C>/`).

**Вход:** Task 1 закоммичен.

**Действие:** одна строка в перечень + кейс.

**Выход:** демонтаж кластера с живой CA-заявкой не оставляет ключей.

**Проверка:** `--filter "FullyQualifiedName~DeprovisioningProcessTests"` зелёный.

**Связь со spec:** §3.2 (X2), §1 (п.5), §7.3 (последний кейс), §10.6 (греп-гейт).

- [ ] **Step 6.1: фейлящий кейс** — в `DeprovisioningProcessTests` (по образцу существующих кейсов чистки координации):

```csharp
    [Fact]
    public async Task Deprovision_WithLiveCaRotationTicket_CleansCoordination()
    {
        // Arrange — TO_REMOVE-кластер с ЖИВОЙ заявкой ca_rotations и staging
        var rig = Rig.Create(); // фактический хелпер файла; посев по образцу
        rig.SeedRemoving("x2ca"); // config state=TO_REMOVE + контейнер (образец файла)
        rig.Etcd.Seed("/valkeyworker/ca_rotations/x2ca",
            """{"requested_unix":1756500000,"requested_by":"it"}""");
        rig.Etcd.Seed("/valkey/clusters/x2ca/ca_next_key", "stg");
        rig.Etcd.Seed("/valkey/clusters/x2ca/ca_next_pem", "stg");

        // Act — тик демонтажа доводит до конца (цикл по образцу существующих)
        await rig.RunToCompletionAsync("x2ca");

        // Assert — ни заявки, ни staging, ни координации
        rig.Get("/valkeyworker/ca_rotations/x2ca").Should().BeNull();
        rig.Get("/valkey/clusters/x2ca/ca_next_key").Should().BeNull("префиксный del домена забирает");
        rig.Get("/valkeyworker/claims/x2ca").Should().BeNull();
    }
```

(Имена хелперов `Rig`/`SeedRemoving`/`RunToCompletionAsync`/`Get` — свести с фактическими в `DeprovisioningProcessTests.cs`; если посев TO_REMOVE-кластера называется иначе — использовать фактическое.)

- [ ] **Step 6.2: прогнать — FAIL** (ключ `ca_rotations` остаётся).

- [ ] **Step 6.3: правка** — в `DeprovisioningProcess.cs` перечень:

```csharp
            foreach (var key in new[]
                     {
                         $"/valkeyworker/claims/{cluster}",
                         $"/valkeyworker/work/{cluster}",
                         $"/valkeyworker/work/{cluster}/rotation",
                         $"/valkeyworker/portalloc/{cluster}",
                         $"/valkeyworker/rotations/{cluster}",
                         $"/valkeyworker/ca_rotations/{cluster}",
                     })
```

- [ ] **Step 6.4: прогнать — PASS; коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~DeprovisioningProcessTests"
git add src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs \
  src/tests/ValkeyWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs
git commit -m "feat(vwk): X2 t07 — чистка демонтажа сносит ca_rotations/<C> (staging забирает префиксный del домена)"
```

---

### Task 7: Панель — чтение очереди `ca_rotations` (Core/Etcd/парсер + API-DTO деталей)

**Files:**
- Modify: `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs` (+`ParseCaRotations`)
- Modify: `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs` (+`ValkeyCaRotationTicket`; +`CaRotations` в снапшот; +`CaRotation` в `ValkeyClusterInfo` — последним параметром со значением по умолчанию, после `HasCaPem = false`, не ломая позиционные вызовы)
- Modify: `src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs` (+`Prefixes.CaRotations`, range, parse, джойн)
- Modify: `src/AdminPanel.Api/Inspection/ValkeyQuery.cs` (+`ValkeyCaRotationDto`, +поле `CaRotation` в `ValkeyClusterDto` после `Rotation` (строка 34), маппинг в `ValkeyMappers.MapDetails` (строки 63–76))
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/Valkey/ca-rotations.json`
- Test: `src/tests/AdminPanel.UnitTests/ValkeyParserTests.cs` (+кейс)
- Test: Create `src/tests/AdminPanel.UnitTests/ValkeyModelTests.cs` (кейс маппера — по образцу `KafkaModelTests.cs`)

**Interfaces:**
- Consumes: образец `ParseRotations`/`ValkeyRotationTicket` (порт без role); `ValkeyClusterInfo` (`ValkeySnapshot.cs:23-33`, поле `Rotation = null` предпоследнее, `HasCaPem = false` последнее); `ValkeyClusterDto.Rotation`/`ValkeyRotationDto` (`ValkeyQuery.cs:25-47`); `MapDetails` (`ValkeyQuery.cs:63`).
- Produces: `ValkeyCaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy)` (Core); `ValkeyClusterInfo.CaRotation` (джойн); `ValkeySnapshot.CaRotations` (очередь); API: `ValkeyCaRotationDto(long RequestedUnix, string? RequestedBy)` + `ValkeyClusterDto.CaRotation` → JSON `caRotation` (ASP.NET camelCase) — источник для бейджа фронтенда (Task 9).

**Вход:** Task 1 закоммичен.

**Действие:** порт чтения rotations на ca_rotations + проброс тикета в API-DTO деталей кластера (без этого поля бейдж фронтенда получает `undefined` и ломает страницу — см. Task 9).

**Выход:** снапшот панели несёт живые CA-заявки; GET /api/valkey/clusters/{c} отдаёт `caRotation`; юниты зелёные.

**Проверка:** `--filter "FullyQualifiedName~ValkeyParserTests|FullyQualifiedName~ValkeyModelTests"` зелёный.

**Связь со spec:** §3.1 (ключ), §4.4, §6.2 (бейдж в деталях), §7.2 (парсер), §10.6 (снапшот НЕ выносит `ca_next_*` — парсер их не читает).

- [ ] **Step 7.1: fixture-файл** `EtcdFixtures/Valkey/ca-rotations.json` (по образцу `rotations.json`):

```json
[
  { "key": "/valkeyworker/ca_rotations/live", "value": "{\"requested_unix\":1756500123,\"requested_by\":\"seed\"}", "modRevision": 1 },
  { "key": "/valkeyworker/ca_rotations/other", "value": "{\"requested_unix\":1756500456}", "modRevision": 2 },
  { "key": "/valkeyworker/ca_rotations/broken", "value": "{oops", "modRevision": 3 },
  { "key": "/valkeyworker/ca_rotations/nofield", "value": "{}", "modRevision": 4 },
  { "key": "/valkeyworker/ca_rotations/nested/bad", "value": "{}", "modRevision": 5 }
]
```

- [ ] **Step 7.2: фейлящие кейсы** — в `ValkeyParserTests`:

```csharp
    // Arrange: ca-rotations.json. Act: ParseCaRotations. Assert: тикет без role;
    // битый JSON/пустой/вложенный ключ → parseError-толерантность (порт rotations).
    [Fact]
    public void ParseCaRotations_TicketWithoutRole_And_BrokenJson()
    {
        var result = ValkeyParser.ParseCaRotations(EtcdFixtures.LoadKv("Valkey/ca-rotations.json"));

        result.Tickets.Should().ContainSingle(t => t.Cluster == "live"
            && t.RequestedUnix == 1756500123 && t.RequestedBy == "seed");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/broken");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/nofield");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/nested/bad");
    }
```

и новый `src/tests/AdminPanel.UnitTests/ValkeyModelTests.cs` (кейс маппера):

```csharp
using AdminPanel.Api.Inspection;
using AdminPanel.Core.Valkey;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Маппер деталей valkey-кластера (t07): CaRotation Core-тикета → API-DTO
// (null-пропagation при отсутствии заявки). AAA.
public class ValkeyModelTests
{
    [Fact]
    public void MapDetails_CaRotationTicket_MapsToDto()
    {
        // Arrange — Core-модель с живой CA-заявкой (Rotation = null)
        var cluster = new ValkeyClusterInfo(
            "live", ValkeyClusterState.Active, 1, 536870912L, "allkeys-lru",
            1756500000, "localhost:17001", [],
            CaRotation: new ValkeyCaRotationTicket("live", 1756500123, "seed"));

        // Act
        var dto = ValkeyMappers.MapDetails(cluster);

        // Assert — поле проброшено в API-DTO (JSON caRotation — бейдж Task 9)
        dto.CaRotation.Should().NotBeNull();
        dto.CaRotation!.RequestedUnix.Should().Be(1756500123);
        dto.CaRotation.RequestedBy.Should().Be("seed");
    }

    [Fact]
    public void MapDetails_NoCaRotation_NullDtoField()
    {
        // Arrange — заявки нет (Rotation и CaRotation = null)
        var cluster = new ValkeyClusterInfo(
            "live", ValkeyClusterState.Active, 1, 536870912L, "allkeys-lru",
            1756500000, "localhost:17001", []);

        // Act
        var dto = ValkeyMappers.MapDetails(cluster);

        // Assert — null (не undefined): фронтенд-бейдж не рендерится
        dto.CaRotation.Should().BeNull();
    }
}
```

(Позиции параметров `ValkeyClusterInfo` — сверить с фактическим record-определением; при именованных аргументах порядок не важен.)

- [ ] **Step 7.3: прогнать — FAIL** (метода/полей нет).

- [ ] **Step 7.4: реализация** — четыре файла:
  1. `ValkeyParser.cs`: `ParseCaRotations(IReadOnlyList<Kv> kvs)` → `ValkeyCaRotationsQueue(IReadOnlyList<ValkeyCaRotationTicket> Tickets, IReadOnlyList<ParseError> Errors)` — точный порт `ParseRotations` (строки ~111–140) с изменениями: префикс-сплит на 3 сегмента `/valkeyworker/ca_rotations/<C>` (лишняя глубина → parseError «ожидается /valkeyworker/ca_rotations/<cluster>»), парсинг `requested_unix`/`requested_by` (БЕЗ role; отсутствие `requested_unix` → parseError — по фактической семантике `rotations`; `requested_by` — опционально).
  2. `ValkeySnapshot.cs`: `record ValkeyCaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);`; в корневой снапшот — `IReadOnlyList<ValkeyCaRotationTicket> CaRotations`; в `ValkeyClusterInfo` — последний параметр `ValkeyCaRotationTicket? CaRotation = null` (после `HasCaPem = false`).
  3. `ValkeySnapshotRefresher.cs`: `Prefixes.CaRotations = "/valkeyworker/ca_rotations/"`; чтение рядом с `rotationsKv` (failover-range), `ValkeyParser.ParseCaRotations`, словарь по кластеру, `CaRotation = caByCluster.GetValueOrDefault(c.Name)`, ошибки — в общий список.
  4. `ValkeyQuery.cs`: `public sealed record ValkeyCaRotationDto(long RequestedUnix, string? RequestedBy);`; в `ValkeyClusterDto` — поле `ValkeyCaRotationDto? CaRotation` после `Rotation`; в `MapDetails` (рядом с маппингом `Rotation`, строки 74–76):

```csharp
            CaRotation: cluster.CaRotation is null
                ? null
                : new ValkeyCaRotationDto(cluster.CaRotation.RequestedUnix, cluster.CaRotation.RequestedBy));
```

- [ ] **Step 7.5: прогнать — PASS; сборка 0 warnings; коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyParserTests|FullyQualifiedName~ValkeyModelTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
git add src/AdminPanel.Etcd/Parsing/ValkeyParser.cs src/AdminPanel.Core/Valkey/ValkeySnapshot.cs \
  src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs src/AdminPanel.Api/Inspection/ValkeyQuery.cs \
  src/tests/AdminPanel.UnitTests/ValkeyParserTests.cs src/tests/AdminPanel.UnitTests/ValkeyModelTests.cs \
  src/tests/AdminPanel.UnitTests/EtcdFixtures/Valkey/ca-rotations.json
git commit -m "feat(panel): чтение очереди /valkeyworker/ca_rotations/ t07 — Core-тикеты + джойн + API-DTO деталей caRotation (бейдж UI) с юнитами"
```

---

### Task 8: Панель — команда `RotateValkeyCa` + endpoint

**Files:**
- Modify: `src/AdminPanel.Api/Operations/Valkey/ValkeyCommands.cs` (+№6)
- Modify: `src/AdminPanel.Api/Operations/Valkey/ValkeyOperationsModule.cs` (+MapPost)
- Test: `src/tests/AdminPanel.UnitTests/Operations/Valkey/ValkeyOperationsTests.cs` (+кейс)

**Interfaces:**
- Consumes: `WorkerProxy.SendAsync<T>(api, "valkeyworker", HttpMethod.Post, url, body: null, requestedBy, ct)`; образец kafka `RotateCaCommandHandler` (`KafkaCommands.cs:157-172`).
- Produces: `RotateValkeyCaCommand(string Cluster, string RequestedBy) : ICommand<ValkeyCaRotatedDto>`; endpoint `POST /api/valkey/clusters/{cluster}/ca/rotate` (аудит — username сессии). `ValkeyCaRotatedDto(Cluster, RequestedUnix, RequestedBy)` — свой record API-слоя операций (не смешивать с DTO Task 7).

**Вход:** Task 7 закоммичен.

**Действие:** мутация №6 — прокси в API воркера.

**Выход:** панель создаёт заявку через воркера (коды маппятся 1:1).

**Проверка:** `--filter "FullyQualifiedName~ValkeyOperationsTests"` зелёный.

**Связь со spec:** §4.4, §6.2, §7.2 (команда), §10.6.

- [ ] **Step 8.1: фейлящий кейс** — в `ValkeyOperationsTests` (по образцу кейса `RotateValkeyPasswordCommandHandler`, строки ~162; хелпер перехвата — фактический, сверить имя):

```csharp
    [Fact]
    public async Task RotateValkeyCa_ProxiesToWorkerApi()
    {
        // Arrange — фейк IWorkerApiGateway файла: возвращает DTO и фиксирует запрос
        var (api, sent) = Fakes.WorkerApiSuccess(
            new ValkeyCaRotatedDto("live", 1756500123, "opsuser"));

        // Act
        var result = await new RotateValkeyCaCommandHandler(api).Handle(
            new RotateValkeyCaCommand("live", "opsuser"), CancellationToken.None);

        // Assert — POST в API воркера, тело пустое, оператор — в заголовке
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        sent.Method.Should().Be(HttpMethod.Post);
        sent.Url.Should().Be("/api/valkey/clusters/live/ca/rotate");
        sent.RequestedBy.Should().Be("opsuser");
    }
```

- [ ] **Step 8.2: прогнать — FAIL**.

- [ ] **Step 8.3: реализация** — `ValkeyCommands.cs` (после №5):

```csharp
// 6. Заявка ротации per-cluster CA/сертов (02 §11.2-6, t07; окно двойного
// доверия P/D/R/C исполняет CaRotator воркера; нода пересоздаётся).
public sealed record RotateValkeyCaCommand(string Cluster, string RequestedBy)
    : ICommand<ValkeyCaRotatedDto>;

public sealed record ValkeyCaRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateValkeyCaCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateValkeyCaCommand, ValkeyCaRotatedDto>
{
    public async ValueTask<Result<ValkeyCaRotatedDto>> Handle(
        RotateValkeyCaCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyCaRotatedDto>(
            api, "valkeyworker", HttpMethod.Post,
            $"/api/valkey/clusters/{command.Cluster}/ca/rotate",
            body: null, command.RequestedBy, ct);
}
```

`ValkeyOperationsModule.cs` (рядом с `password/rotate`):

```csharp
        // POST /api/valkey/clusters/{cluster}/ca/rotate — заявка ротации CA/сертов
        // (02 §11.2-6, t07): 202; оператор сессии — X-Requested-By.
        endpoints.MapPost("/api/valkey/clusters/{cluster}/ca/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateValkeyCaCommand, ValkeyCaRotatedDto>(
                new RotateValkeyCaCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });
```

- [ ] **Step 8.4: прогнать — PASS; коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyOperationsTests"
git add src/AdminPanel.Api/Operations/Valkey/ValkeyCommands.cs \
  src/AdminPanel.Api/Operations/Valkey/ValkeyOperationsModule.cs \
  src/tests/AdminPanel.UnitTests/Operations/Valkey/ValkeyOperationsTests.cs
git commit -m "feat(panel): мутация №6 RotateValkeyCa t07 — прокси POST /api/valkey/clusters/{c}/ca/rotate в API воркера"
```

---

### Task 9: Фронтенд — кнопка «Ротация CA» + бейдж заявки + сверка caRotation

**Files:**
- Create: `frontend/src/pages/valkey-cluster/RotateCaButton.tsx` (порт `kafka-cluster/RotateCaButton.tsx`)
- Modify: `frontend/src/pages/valkey-cluster/ValkeyClusterDetailsPage.tsx` (кнопка + бейдж)
- Modify: `frontend/src/api/queries.ts` (+`rotateValkeyCa`)
- Modify: `frontend/src/api/dto.ts` (+`ValkeyCaRotatedDto`, +`caRotation` в DTO кластера)

**Interfaces:**
- Consumes: API-DTO Task 7: JSON-поле `caRotation` (ASP.NET camelCase от `ValkeyClusterDto.CaRotation`) в ответе `GET /api/valkey/clusters/{cluster}`; `rotateValkeyCa` → POST панели (Task 8).
- Produces: UI-кнопка (подтверждение с предупреждением «нода будет пересоздана (кеш холодный старт), окно секунд») + бейдж живой заявки.

**Вход:** Tasks 7–8 закоммичены.

**Действие:** порт UI kafka с адаптацией текстов под механику valkey (одно пересоздание, не rolling брокеров); Шаг 9.5 — обязательная сверка «поле JSON ↔ поле TS» (без неё `c.caRotation !== null` даёт `true` на `undefined` → бейдж рендерится всегда → рантайм-ошибка `c.caRotation.requestedUnix`).

**Выход:** UI панели отправляет заявку и показывает живую; typecheck/build зелёные.

**Проверка:** `cd frontend && npm run typecheck` — 0 ошибок; `npm run build` — успех; ручная сверка поля (Шаг 9.5).

**Связь со spec:** §4.4 (UI), §6.2 (очередь в UI), §1 (п.3).

- [ ] **Step 9.1: `dto.ts`** — рядом с `ValkeyPasswordRotatedDto` (строка ~743):

```ts
// Ответ 202 POST /api/valkey/clusters/{c}/ca/rotate (t07).
export interface ValkeyCaRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// Живая заявка ротации CA из деталей кластера (t07; JSON-поле caRotation).
export interface ValkeyCaRotationDto {
  requestedUnix: number;
  requestedBy: string | null;
}
```

и в DTO деталей кластера (там, где `rotation: ValkeyRotationDto | null`, строка ~679): `caRotation: ValkeyCaRotationDto | null;`.

- [ ] **Step 9.2: `queries.ts`** — рядом с valkey-мутациями:

```ts
// POST /api/valkey/clusters/{cluster}/ca/rotate — заявка ротации CA/сертов
// (t07): окно двойного доверия P/D/R/C исполняет CaRotator воркера.
export function rotateValkeyCa(cluster: string): Promise<ValkeyCaRotatedDto> {
  return apiFetch<ValkeyCaRotatedDto>(
    `/api/valkey/clusters/${encodeURIComponent(cluster)}/ca/rotate`,
    { method: 'POST' });
}
```

- [ ] **Step 9.3: `RotateCaButton.tsx`** — порт kafka-кнопки (тот же каркас `useMutation`/Modal/Alert), отличия текстов:

```tsx
// Кнопка «Ротация CA» valkey-кластера (мутация №6, t07): заявка
// /valkeyworker/ca_rotations/<C>; исполняет CaRotator воркера окном двойного
// доверия (P/D/R/C) — клиенты, перечитавшие ca_pem из etcd, работают
// непрерывно; нода пересоздаётся с холодным стартом кеша (persistence off).
```

Тексты: заголовок модалки `Ротация CA — {cluster}`; тело: «Воркер выполнит ротацию per-cluster CA и серверного серта ноды без остановки обслуживания: (P) staging новой CA, (D) окно двойного доверия — ca_pem = bundle OLD+NEW, (R) пересоздание ноды с сертом от новой CA, (C) атомарный коммит.»; Alert «Внимание»: «Нода будет пересоздана — кеш начнётся с холодного старта (persistence off), окно — секунды.» + список: «приложения, читающие ca_pem из etcd, работают непрерывно», «после коммита клиенты с закешированным старым CA перечитывают ca_pem». Инвалидация: `queryKey: ['valkey-clusters']`.

- [ ] **Step 9.4: `ValkeyClusterDetailsPage.tsx`** — в блок бейджей (после `c.rotation !== null`, строки ~53–59):

```tsx
          {c.caRotation != null ? (
            <Tooltip label={`заявка ротации CA жива: воркер применяет окно двойного доверия (${c.caRotation.requestedBy ?? '—'})`}>
              <Badge color="violet" variant="light">
                ротация CA · {rotationAgeMinutes(c.caRotation.requestedUnix)}
              </Badge>
            </Tooltip>
          ) : null}
```

(использовать `!= null` — ловит и `undefined`: защита от рассинхрона JSON↔TS); в кнопки (`canMutate`, строки ~61–68), между `RotatePasswordButton` и `DeleteValkeyClusterButton`:

```tsx
            <RotateCaButton cluster={c.name} disabled={c.rotation !== null || c.caRotation != null} />
```

(хелпер `rotationAgeMinutes` уже есть в файле).

- [ ] **Step 9.5: сверка JSON↔TS поля `caRotation`** — обязательная: поднять панель НЕ требуется; сверить сериализацию статически: `ValkeyClusterDto.CaRotation` (Task 7) при стандартном camelCase-JSON панели даёт поле `caRotation`; проверить фактический конфиг JSON-сериализации панели (если PascalCase — привести имя поля TS к фактическому). Команда контроля: `grep -n "caRotation" frontend/src/api/dto.ts` + `grep -n "CaRotation" src/AdminPanel.Api/Inspection/ValkeyQuery.cs` — оба присутствуют; при отличии регистра JSON (проверить по соседним полям `rotation`/`requestedUnix` в существующем `ValkeyRotationDto`-фронтенде) — согласовать.

- [ ] **Step 9.6: проверить typecheck/build**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation/frontend && npm run typecheck && npm run build
```

- [ ] **Step 9.7: коммит**

```bash
git add frontend/src/pages/valkey-cluster/RotateCaButton.tsx \
  frontend/src/pages/valkey-cluster/ValkeyClusterDetailsPage.tsx \
  frontend/src/api/queries.ts frontend/src/api/dto.ts
git commit -m "feat(panel-ui): кнопка «Ротация CA» и бейдж заявки ca_rotations t07 (порт kafka RotateCaButton; null-guard бейджа)"
```

---

### Task 10: Интеграционные тесты — `CaRotationTests`

**Files:**
- Create: `src/tests/ValkeyWorker.IntegrationTests/Valkey/CaRotationTests.cs`

**Interfaces:**
- Consumes: `ValkeyClusterFixture` (`[Collection(ValkeyClusterCollection.Name)]`; `fx.{Cluster, SeedClusterAsync, NewClaimStore, NewJournal, NewSecretEnsurer, NewTlsProvisioner, Driver, Gateway, Endpoint, Options, NextPort, PutAsync, GetAsync, DelAsync, RequireSnapshotAsync, DockerHost}` — фактические имена сверить по `TlsMigrationTests`); `RespProbe.ExecuteTls(host, port, user, password, caPem, args...)`; `TarArchive.Read`.
- Produces: зелёная интеграция полного цикла + сбой-доигрывание + X2-чистка.

**Вход:** Tasks 2–6 закоммичены.

**Действие:** интеграция на реальном etcd (testcontainers) + реальном valkey-контейнере — порт `KafkaWorker.IntegrationTests/Kafka/CaRotationTests.cs` на механику `TlsMigrationTests`.

**ПРИМЕЧАНИЕ (план сильнее spec §7.3):** spec формулирует контур как «TlsTestServer + фейк docker-драйвера»; план использует СУЩЕСТВУЮЩУЮ фикстуру `ValkeyClusterFixture` — реальный testcontainers-etcd + реальный valkey-контейнер через docker-драйвер (`RespProbe.ExecuteTls` по живому контейнеру). Покрытие — superset формулировки спеки (те же ассерты фаз/txn/доверия плюс реальное пересоздание контейнера и реальный TLS-хендшейк). Спеку не трогаем (не каскадировать гейты); рассинхрона нет — spec допускает «существующие механики фикстур».

**Выход:** счёт критериев §10.3 закрыт.

**Проверка:** серия интеграций valkey зелёная (с зачисткой контейнеров/сетей ПОСЛЕ серии: `docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f`).

**Связь со spec:** §7.3 (superset — см. примечание), §1 (п.6).

⚠️ Правило серий: перед серией — убедиться в чистоте (нет остаточных `vwk-*`/сетей от прошлых прогонов); после серии — зачистка; следующая серия только поверх зачищенной (AGENTS.md).

- [ ] **Step 10.1: тест-файл** — 4 кейса (полный код кейса 1; остальные — по его каркасу):

```csharp
using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.IntegrationTests.Valkey;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Интеграция ротации CA (t07): заявка → CaRotator доводит окно двойного
// доверия на живом docker-valkey; после коммита ca_pem/ca_key = NEW,
// staging удалён, заявка снята, PING по NEW отвечает, OLD доверия нет.
// Контур — реальный etcd + реальный valkey-контейнер (см. примечание задачи).
[Collection(ValkeyClusterCollection.Name)]
public class CaRotationTests(ValkeyClusterFixture fx)
{
    private CaRotator NewRotator(ClaimStore claims)
        => new(fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewTlsProvisioner(),
            new ValkeyConnection(TimeSpan.FromSeconds(2)), fx.Options);

    [Fact]
    public async Task CaRotate_FullWindow_CommitsNewCa()
    {
        // Arrange: канонический TLS-кластер (миграция T доигрывает тиками —
        // образец TlsMigrationTests.PlainCluster_MigratesToTls), клэйм наш.
        var cluster = fx.Cluster("carot");
        var ct = TestContext.Current.CancellationToken;
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var migrator = new TlsMigrator(
            fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewSecretEnsurer(), fx.NewTlsProvisioner(),
            new ValkeyConnection(TimeSpan.FromSeconds(2)), fx.Options);
        while (true)
        {
            var snap = await fx.RequireSnapshotAsync(cluster);
            var tick = await migrator.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(tick.Error?.Message);
            if (tick.Value == TlsMigrator.MigrationOutcome.NotNeeded)
                break;
            await Task.Delay(1000, ct);
        }

        var oldCaPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var probeHost = endpoints.Split(':')[0];
        var port = int.Parse(endpoints.Split(':')[1]);
        var adminPw = (await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password"))!;

        // Baseline: PING по OLD-CA отвечает.
        RespProbe.ExecuteTls(probeHost, port, "admin", adminPw, oldCaPem, "PING").Ok
            .Should().BeTrue("стартовое доверие OLD");

        // Act: заявка (формат §9.8, без role) + тик-цикл CaRotator до del заявки.
        await fx.PutAsync($"/valkeyworker/ca_rotations/{cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"it"}""");
        var rotator = NewRotator(claims);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200); // ≤ BrokerBootSec-класс бюджета
        var done = false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tick = await rotator.RunAsync(await fx.RequireSnapshotAsync(cluster), ct);
            tick.IsSuccess.Should().BeTrue($"тик ротации не должен падать: {tick.Error?.Message}");
            if (await fx.GetAsync($"/valkeyworker/ca_rotations/{cluster}") is null)
            {
                done = true;
                break;
            }
            await Task.Delay(1000, ct);
        }
        done.Should().BeTrue($"ротация исполнена за бюджет; journal={await fx.GetAsync($"/valkeyworker/work/{cluster}")}");

        // Assert 1: ca_pem = NEW (не bundle), ca_key = NEW, staging нет.
        var newCaPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        newCaPem.Should().NotBe(oldCaPem).And.NotContain(oldCaPem, "bundle свёрнут");
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_key")).Should().NotBe(oldCaPem);
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_key")).Should().BeNull();
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_pem")).Should().BeNull();
        (await fx.GetAsync($"/valkeyworker/work/{cluster}")).Should().Contain("done");

        // Assert 2: PING по NEW-CA отвечает; OLD-CA отклоняется (ключ уничтожен).
        RespProbe.ExecuteTls(probeHost, port, "admin", adminPw, newCaPem, "PING").Ok
            .Should().BeTrue("доверие NEW после коммита");
        var oldRejected = false;
        try
        {
            oldRejected = !RespProbe.ExecuteTls(probeHost, port, "admin", adminPw, oldCaPem, "PING").Ok;
        }
        catch (ApplicationException)
        {
            oldRejected = true; // TLS-отказ на хендшейке
        }
        oldRejected.Should().BeTrue("серверный серт подписан NEW — OLD больше не якорь");
    }

    // Кейс 2 (сбой между D и R): заявка → тик до фазы D (проверить bundle) →
    // инжект отказа EnsureNode (fx.Driver обёртка с fault — по факту доступных
    // механик фикстуры; если fault-обёртки нет — снести контейнер руками
    // docker rm vwk-<C>-node1) → повторный тик → полный коммит (фаза R
    // пересоздаёт ноду — «лечение мёртвой ноды», spec §2.4).
    [Fact]
    public async Task CrashBetweenDAndR_ReplaysToCommit() { /* каркас кейса 1 */ }

    // Кейс 3 (сбой между R и C): после пересоздания (серт NEW в volume) —
    // повторный тик: факт-детект R пропускает пересоздание → коммит.
    // Доказательство «без второго пересоздания» — Id контейнера до/после
    // повторного тика не сменился (механика ContainerId из TlsMigrationTests).
    [Fact]
    public async Task CrashBetweenRAndC_FactDetectSkipsRecreate() { /* каркас кейса 1 */ }

    // Кейс 4 (X2): TO_REMOVE кластера с живой заявкой → демонтаж тиками →
    // ключей ca_rotations/staging нет.
    [Fact]
    public async Task Deprovision_WithLiveTicket_CleansAll() { /* каркас: подъём
        кейса 1 + заявка + config state=TO_REMOVE + цикл DeprovisioningProcess */ }
}
```

Кейсы 2–4 реализуются по каркасу кейса 1 (посев тождествен; различия — в точке сбоя и ассертах; AAA-комментарии).

- [ ] **Step 10.2: прогнать серию интеграций valkey (не только новый файл!)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network ls --format '{{.Name}}' | grep -c '^vwk-' || true
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"
```

Ожидание: все интеграции зелёные (новые + существующие не регрессировали).

- [ ] **Step 10.3: зачистка после серии + коммит**

```bash
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
git add src/tests/ValkeyWorker.IntegrationTests/Valkey/CaRotationTests.cs
git commit -m "test(vwk): интеграция CaRotation t07 — полный цикл окна, сбой-доигрывания D/R, X2-чистка (реальный etcd+valkey, TLS; покрытие superset spec §7.3)"
```

---

### Task 11: Docker-E2E — `CaRotation_ClusterRotatesWithoutDowntimeOfTrust`

**Files:**
- Modify: `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eLifecycleTests.cs` (+тест-метод в существующем классе)

**Interfaces:**
- Consumes: `ValkeyE2eEnvironment.StartAsync(slug)` (поднимает сеть/etcd/воркера `valkeyworker:e2e` СВЕЖИМ Release-сборкой из текущего кода); `fx.{CreateApiHttpClient, Gateway, EtcdEndpoint, ClusterTag, ContainerAliveAsync, WaitPhaseAsync, MarkFailed}`; `RespProbe.ExecuteTls`.
- Produces: E2E-кейс мерж-гейта (полный прод-контур заявки через mTLS HTTP-грань воркера).

**Вход:** Tasks 4, 5 закоммичены (вентиль + API — в образ воркера попадают при docker build).

**Действие:** новый тест-метод по spec §7.4; полная самоочистка фикстуры уже реализована (`DisposeAsync` + ассерты чистоты + телеметрия `/tmp/pgw-e2e-artifacts-<guid>/` + `MarkFailed`).

**Выход:** E2E-счёт §10.4 закрыт.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"` — все кейсы зелёные; после серии — зачистка + проверка `docker ps -aq --filter name=vwk- | wc -l` → 0.

**Связь со spec:** §7.4, §1 (п.6), §10.4; правила `docs/e2e-isolation.md`/`docs/e2e-launch.md`.

⚠️ Телеметрия: разбор любого падения — ТОЛЬКО по артефактам фикстуры; перезапуск упавшего E2E без согласия пользователя ЗАПРЕЩЁН (AGENTS.md).

- [ ] **Step 11.1: тест-метод** (вставить в `ValkeyE2eLifecycleTests` по образцу `Tls_ClusterLifecycleTlsOnly`):

```csharp
    [Fact]
    public async Task CaRotation_ClusterRotatesWithoutDowntimeOfTrust()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        var enabled = Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") == "1";
        if (!enabled)
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        // Окружение: сеть + etcd + воркер (Release-образ) + TLS-пакет.
        await using var fx = await ValkeyE2eEnvironment.StartAsync("carot");
        var cluster = $"e2e{fx.ClusterTag}";
        var ct = TestContext.Current.CancellationToken;
        try
        {
            using var api = fx.CreateApiHttpClient();

            // [PHASE] wait-worker: /healthz по mTLS готов (бюджет ≤ 30 с).
            // (цикл по образцу Tls_ClusterLifecycleTlsOnly)

            // Act 1: создание кластера через API (full-прод-контур).
            // (POST /api/valkey/clusters → 201; ожидание state=RUNNING по
            // образцу существующего кейса, бюджет 100 с)

            // Baseline: PING по OLD-CA (ca_pem из etcd) отвечает.
            var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/endpoints", ct)).Value!.Value;
            var probeHost = endpoints.Split(':')[0];
            var port = int.Parse(endpoints.Split(':')[1]);
            var oldCaPem = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_pem", ct)).Value!.Value;
            var adminPw = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/admin_password", ct)).Value!.Value;
            RespProbe.ExecuteTls(probeHost, port, "admin", adminPw, oldCaPem, "PING").Ok
                .Should().BeTrue("baseline: OLD-CA доверие");

            // Act 2: заявка ротации через HTTP-грань воркера (mTLS-клиент).
            using var posted = await api.PostAsync(
                $"/api/valkey/clusters/{cluster}/ca/rotate", content: null);
            posted.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // [PHASE] wait-rotation: journal done (цикл ≤ 100 с; каждый проход —
            // чтение /valkeyworker/work/<C>, содержит op rotate-ca + done).
            await fx.WaitPhaseAsync("wait-rotation", async () =>
                (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/valkeyworker/work/{cluster}", ct)).Value?.Value
                    is { } w && w.Contains("rotate-ca") && w.Contains("done"),
                TimeSpan.FromSeconds(100), ct);

            // Assert 1: ca_pem/ca_key = NEW (не bundle), staging нет, заявки нет.
            var newCaPem = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_pem", ct)).Value!.Value;
            newCaPem.Should().NotBe(oldCaPem).And.NotContain(oldCaPem, "bundle свёрнут");
            (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_next_key", ct)).Value.Should().BeNull();
            (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_next_pem", ct)).Value.Should().BeNull();
            (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkeyworker/ca_rotations/{cluster}", ct)).Value.Should().BeNull();

            // Assert 2: контейнер пересоздан (жив, имя то же vwk-<C>-node1).
            (await fx.ContainerAliveAsync($"vwk-{cluster}-node1")).Should().BeTrue();

            // Assert 3: PING по NEW-CA отвечает; OLD-CA отказывает.
            RespProbe.ExecuteTls(probeHost, port, "admin", adminPw, newCaPem, "PING").Ok
                .Should().BeTrue("после коммита доверие NEW");
            var oldRejected = false;
            try
            {
                oldRejected = !RespProbe.ExecuteTls(
                    probeHost, port, "admin", adminPw, oldCaPem, "PING").Ok;
            }
            catch (ApplicationException)
            {
                oldRejected = true; // TLS-отказ: серт подписан NEW
            }
            oldRejected.Should().BeTrue("OLD-ключ уничтожен перезаписью");

            // Teardown фикстуры (DisposeAsync) сносит всё; ассерты чистоты —
            // внутри фикстуры (тома/контейнеры/сети тега) при любом исходе.
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам /tmp/pgw-e2e-artifacts-*).
            fx.MarkFailed();
            throw;
        }
    }
```

Детали свести с фактическим каркасом `Tls_ClusterLifecycleTlsOnly` (создание кластера, ожидание RUNNING, чтение дискавери-ключей, `WaitPhaseAsync`-сигнатура).

- [ ] **Step 11.2: прогон E2E-серии на свежем Release**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"
```

Ожидание: все кейсы (существующие 2 + новый) зелёные; фикстура собирает образ из текущего кода.

- [ ] **Step 11.3: зачистка после серии + коммит**

```bash
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
git add src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eLifecycleTests.cs
git commit -m "test(vwk-e2e): кейс CaRotation_ClusterRotatesWithoutDowntimeOfTrust t07 — полный прод-контур заявки (mTLS API → окно → NEW-CA; самоочистка/телеметрия фикстуры)"
```

---

### Task 12: Runbook — раздел «ротация CA valkey»

**Files:**
- Modify: `docs/runbook.md` (раздел рядом с «TLS-подключения к Valkey-кластерами (t06)», строка ~158)

**Interfaces:** нет (документация).

**Вход:** Task 1 закоммичен.

**Действие:** подраздел по spec §9: ручная проверка состояния окна (ключи `ca_pem` (bundle?), `ca_next_*`, заявка), снятие зависшей заявки (`etcdctl del /valkeyworker/ca_rotations/<C>`), переиспользование осиротевшего staging (put-if-absent), примечание об окне D→C для клиентов после обеих сторон t07 (pg + Puzzle-мерж).

**Выход:** операторская процедура задокументирована.

**Проверка:** текст в файле; коммит.

**Связь со spec:** §9 (runbook), §8 (п.2).

- [ ] **Step 12.1: дописать раздел и закоммитить**

```bash
git add docs/runbook.md
git commit -m "docs(runbook): раздел «ротация CA valkey» t07 — состояние окна, снятие зависшей заявки, осиротевший staging"
```

---

### Task 13: Puzzle — многосертовое чтение `ca_pem` (СВОЙ репозиторий, отдельная ветка)

**Files:**
- Modify: `$PUZZLE/src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionOptions.cs` (ТОЛЬКО `TryBuildCertificateValidation` + `ValidateAgainstCa`)
- Test: `$PUZZLE/src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs` (+кейсы)

**Interfaces:**
- Consumes: `X509Certificate2Collection.ImportFromPem(ReadOnlySpan<char>)` (стандартный .NET — читает ВСЕ PEM CERTIFICATE-блоки); действующий macOS-паттерн PFX round-trip; `TestPki.{GenerateCa, IssueCert}`.
- Produces: доверие по всем блокам CERTIFICATE значения `ca_pem` (bundle OLD+NEW окна ротации D→C); пустая коллекция/битый PEM → `false` без построения callback (действующая семантика).

**Вход:** санкция пользователя на правки `../Puzzle` (зафиксирована spec §0/§12.1); `$PUZZLE` чистый, ветка `main@5ef7d61`.

**Действие:** feature-ветка в Puzzle; правка одного метода + тесты; СВОЙ коммит. Мерж в `main` Puzzle — отдельным решением пользователя (в мерж-гейт pg НЕ входит).

**Выход:** клиент валидирует серт против любого якоря bundle; юниты Puzzle зелёные.

**Проверка:** `cd $PUZZLE && dotnet test src/PuzzleServer.Api.slnx -c Release --filter "FullyQualifiedName~ValkeyConnectionOptionsTests"` зелёный; `git -C $PUZZLE status` — только два файла в диффе.

**Связь со spec:** §3.3, §4.5, §7.5, §10.8.

- [ ] **Step 13.1: feature-ветка в Puzzle**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git switch -c t07-valkey-ca-bundle
```

- [ ] **Step 13.2: фейлящие тесты** — добавить в `ValkeyConnectionOptionsTests`:

```csharp
    [Fact]
    public void Bundle_TwoCas_ValidatesAgainstBoth()
    {
        // Arrange — bundle окна ротации D→C: OLD + "\n" + NEW (арх/20 §2.1)
        var (oldCa, oldCaKey) = TestPki.GenerateCa();
        var (newCa, newCaKey) = TestPki.GenerateCa();
        var bundle = oldCa + "\n" + newCa;
        ValkeyConnectionOptions.TryBuildCertificateValidation(bundle, "h1:6379", out var validation)
            .Should().BeTrue();
        validation.Should().NotBeNull();

        // Act / Assert — серт от NEW валиден против bundle (якорей два)
        var newCertPem = TestPki.IssueCert(newCa, newCaKey, "h1");
        using var newCert = X509CertificateLoader.LoadCertificate(System.Text.Encoding.UTF8.GetBytes(newCertPem));
        validation!(this, newCert, null, SslPolicyErrors.None).Should().BeTrue("окно: NEW-якорь в bundle");

        // Серт от OLD тоже валиден (двойное доверие окна)
        var oldCertPem = TestPki.IssueCert(oldCa, oldCaKey, "h1");
        using var oldCert = X509CertificateLoader.LoadCertificate(System.Text.Encoding.UTF8.GetBytes(oldCertPem));
        validation(this, oldCert, null, SslPolicyErrors.None).Should().BeTrue("окно: OLD-якорь в bundle");
    }

    [Fact]
    public void Bundle_ForeignCaStillRejected()
    {
        // Arrange — bundle из двух якорей; серт посторонней CA
        var (oldCa, oldCaKey) = TestPki.GenerateCa();
        var (newCa, newCaKey) = TestPki.GenerateCa();
        var bundle = oldCa + "\n" + newCa;
        ValkeyConnectionOptions.TryBuildCertificateValidation(bundle, "h1:6379", out var validation)
            .Should().BeTrue();

        // Act / Assert — bundle НЕ расширяет доверие за пределы якорей
        var (foreignCa, foreignCaKey) = TestPki.GenerateCa();
        var foreignCertPem = TestPki.IssueCert(foreignCa, foreignCaKey, "h1");
        using var foreignCert = X509CertificateLoader.LoadCertificate(
            System.Text.Encoding.UTF8.GetBytes(foreignCertPem));
        validation!(this, foreignCert, null, SslPolicyErrors.None).Should().BeFalse();
    }

    [Fact]
    public void SinglePem_T06BehaviorPreserved()
    {
        // Arrange / Act — один PEM (вне окна): регресс t06
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var certPem = TestPki.IssueCert(caPem, caKeyPem, "h1");
        ValkeyConnectionOptions.TryBuildCertificateValidation(caPem, "h1:6379", out var validation)
            .Should().BeTrue();
        using var cert = X509CertificateLoader.LoadCertificate(System.Text.Encoding.UTF8.GetBytes(certPem));

        // Assert — валидация и SAN-сверка работают как в t06
        validation!(this, cert, null, SslPolicyErrors.None).Should().BeTrue();
        var otherSanPem = TestPki.IssueCert(caPem, caKeyPem, "other.host");
        using var otherSan = X509CertificateLoader.LoadCertificate(
            System.Text.Encoding.UTF8.GetBytes(otherSanPem));
        validation(this, otherSan, null, SslPolicyErrors.None).Should().BeFalse("SAN-сверка без изменений");
    }

    [Theory]
    [InlineData("garbage")]
    [InlineData("")]
    [InlineData("-----BEGIN CERTIFICATE-----\ntruncated")]
    public void Bundle_NoValidBlocks_NoCallback(string caPem)
    {
        // Arrange / Act / Assert — нет валидных блоков: callback не строится
        // (Ssl не сбрасывается — подключение честно упадёт на хендшейке)
        ValkeyConnectionOptions.TryBuildCertificateValidation(caPem, "h1:6379", out var validation)
            .Should().BeFalse();
        validation.Should().BeNull();
    }
```

- [ ] **Step 13.3: прогнать — FAIL** (`CreateFromPem` читает только первый блок: кейс `Bundle_TwoCas_...` падает на NEW-серте).

```bash
cd /Users/demakaev/ZCodeProject/Puzzle && dotnet test src/PuzzleServer.Api.slnx -c Release --filter "FullyQualifiedName~ValkeyConnectionOptionsTests"
```

- [ ] **Step 13.4: реализация** — заменить `TryBuildCertificateValidation` (и `ValidateAgainstCa`) в `ValkeyConnectionOptions.cs`:

```csharp
    // Построение certificate-validation callback против per-cluster CA.
    // t07: значение ca_pem в окне ротации — bundle OLD+NEW: доверие строится
    // по ВСЕМ блокам CERTIFICATE (X509Certificate2Collection.ImportFromPem);
    // вне окна блок один — поведение t06 сохранено. Битый/пустой PEM → false
    // (callback не построен): Ssl=true НЕ сбрасывается — селективно отключить
    // TLS нельзя (канон дискавери), подключение честно упадёт на хендшейке.
    internal static bool TryBuildCertificateValidation(
        string caPem, string endpoints, out RemoteCertificateValidationCallback? validation)
    {
        validation = null;
        try
        {
            // Все CERTIFICATE-блоки значения (окно двойного доверия D→C).
            var collection = new X509Certificate2Collection();
            collection.ImportFromPem(caPem);
            if (collection.Count == 0)
                return false;

            // PFX round-trip (macOS): эфемерные ключи CreateFromPem/ImportFromPem
            // для построения цепочки ненадёжны — пере-импорт через PKCS#12
            // (паттерн E2E-окружения; t07 — для каждого серта коллекции).
            var anchors = new List<X509Certificate2>();
            foreach (var pemCa in collection)
            {
                anchors.Add(X509CertificateLoader.LoadPkcs12(
                    pemCa.Export(X509ContentType.Pkcs12), null));
                pemCa.Dispose();
            }

            var hosts = EndpointHosts(endpoints);
            validation = (_, certificate, _, _) =>
                certificate is not null
                && ValidateAgainstCa(certificate, anchors)
                && hosts.Any(h => SanMatchesHost(certificate, h));
            return true;
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        {
            return false;
        }
    }

    // Цепочка CustomRootTrust против ВСЕХ якорей bundle (NoCheck — частные CA
    // без CRL/OCSP; копия Shared.Tls TlsChain: Puzzle не ссылается на pg).
    private static bool ValidateAgainstCa(X509Certificate certificate, IReadOnlyList<X509Certificate2> anchors)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        foreach (var anchor in anchors)
            chain.ChainPolicy.CustomTrustStore.Add(anchor);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(certificate as X509Certificate2 ?? new X509Certificate2(certificate));
    }
```

Проверить фактический catch-набор `ImportFromPem` на пустых/битых данных: если какой-то из Theory-кейсов бросает исключение вне набора `ArgumentException/CryptographicException/FormatException` — расширить набор ПО ФАКТУ (запустить кейсы и сверить тип; цель — пустая коллекция/битый PEM дают `false`, а не падение).

- [ ] **Step 13.5: прогнать — PASS; границы правки**

```bash
dotnet test src/PuzzleServer.Api.slnx -c Release --filter "FullyQualifiedName~ValkeyConnectionOptionsTests"
dotnet test src/PuzzleServer.Api.slnx -c Release   # соседи Puzzle не регрессировали
git status --porcelain   # ровно 2 файла
```

- [ ] **Step 13.6: коммит в ветке Puzzle (СВОЙ репозиторий; в мерж-гейт pg НЕ входит)**

```bash
git add src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionOptions.cs \
  src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs
git commit -m "feat(valkey): многосертовое чтение ca_pem t07 — ImportFromPem всей коллекции, CustomTrustStore из bundle OLD+NEW (окно ротации CA pg/t07); регресс t06 сохранён"
```

Мерж ветки `t07-valkey-ca-bundle` в `main` Puzzle — ТОЛЬКО по отдельному решению пользователя; статус Puzzle-мержа фиксируется в журнале исполнения t07 (spec §11).

---

### Task 14: Мерж-гейт pg — полная приёмка + сверка канона

**Files:**
- Modify: `arch/roadmap/valkey.md` (удаление тега `t07-valkey-ca-rotation` — ТЕМ ЖЕ коммитом мержа в `main`; из списков и из `←`-зависимостей)

**Interfaces:** нет (гейт).

**Вход:** задачи 1–12 закоммичены; Task 13 закоммичен в своей ветке (гейт pg от него не зависит).

**Действие:** полная приёмка по §10 + греп-гейты + соседние серии + сверка канона с фактом кода (spec §10.7); после user-review/code-review — мерж в `main` (по решению пользователя через гейт dev-flow) одним коммитом с удалением roadmap-тега.

**Выход:** все критерии §10 подтверждены командами; ветка готова к ревью.

**Проверка:** перечень ниже — каждый пункт командой; выводы фиксируются в журнале исполнения.

**Связь со spec:** §10 (критерии 1–8), §11 (мерж-гейт).

⚠️ Между сериями — зачистка (`docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f`); серия запускается только поверх зачищенной предыдущей.

- [ ] **Step 14.1: сборка Release — 0 ошибок/0 warnings**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```

- [ ] **Step 14.2: юниты (все затронутые)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~ValkeyWorker.UnitTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~AdminPanel.UnitTests"
```

- [ ] **Step 14.3: зачистка → интеграции valkey → зачистка**

```bash
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
```

- [ ] **Step 14.4: зачистка → E2E на свежем Release → зачистка** (код воркера затронут — обязательная часть гейта)

```bash
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eLifecycleTests"
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
docker volume ls --format '{{.Name}}' | grep -c '^vwk-' || true   # 0 — чистота томов
```

- [ ] **Step 14.5: соседние серии (затронутые сборки пересобраны)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~KafkaWorker.UnitTests|FullyQualifiedName~PgWorker.UnitTests|FullyQualifiedName~Shared"
```

- [ ] **Step 14.6: греп-гейты чистоты**

```bash
grep -n "ca_rotations" src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs   # есть в перечне X2
# Прямые чтения/записи доменных ключей "/valkey/..." в панельных командах —
# НЕТ: паттерн с кавычкой-префиксом (НЕ совпадает с прокси-URL "/api/valkey/..."):
grep -rn '"/valkey/' src/AdminPanel.Api/Operations/Valkey/                                  # пусто
grep -rn "ca_next" src/AdminPanel.Etcd/ src/AdminPanel.Core/ src/AdminPanel.Api/ frontend/src/ # пусто: снапшот НЕ выносит staging в UI/API
```

- [ ] **Step 14.7: Puzzle-статус в журнале исполнения** — зафиксировать: ветка `t07-valkey-ca-bundle` закоммичена/зелёная; мерж в `main` Puzzle — статус решения пользователя (spec §11).

- [ ] **Step 14.8: сверка канона с фактом кода (spec §10.7 — «рассинхронов после кода нет»)** — пройтись по чек-листу «канон ↔ реализация» и зафиксировать результат в журнале исполнения (несоответствие = блокер гейта, правится до мержа):
  - `arch/20` §2/§2.1: ключи `ca_next_key`/`ca_next_pem`/`ca_rotations` и bundle-форма `ca_pem` — сверить с `CaRotator` (ключи-хелперы, txn-наборы P/D/C) и `RotateCaHandler` (payload заявки без role);
  - `arch/20` §3: `ca_rotations/` в перечне координации — сверить с X2 `DeprovisioningProcess`;
  - `arch/20` §4/§5: многосертовое доверие клиента и unknownKeys-толерантность `ca_next_*` — сверить с парсерами (`ValkeySnapshotParser` не расширялся; Puzzle-ветка зелёная);
  - `arch/21` §5 K: порядок фаз/журнал-фазы (`phase-p/phase-d/phase-r/phase-r/node1/committed/done/waiting-*/aborted-state-changed`), эксклюзивный второй шаг Active-ветки — сверить с `CaRotator`/`ValkeyClusterProcesses`;
  - `arch/21` §9: endpoint и коды — сверить с `ApiModule`/`ValkeyOperationsModule` (202/404/409/503, `X-Requested-By`);
  - `adminpanel/02` §11.1/§11.2: мутация №6 и чтение очереди — сверить с `ValkeyCommands`/`ValkeyOperationsModule`/`ValkeyParser`/`ValkeyQuery` (`caRotation` в DTO).

- [ ] **Step 14.9: ревью plan↔spec и код-ревью по канону dev-flow; user-review; мерж**

После одобрений: мерж ветки в `main` ОДНИМ коммитом, удаляющим тег `t07-valkey-ca-rotation` из `arch/roadmap/valkey.md` (из списков и `←`-зависимостей; правила `arch/roadmap/README.md`), сам, без пометок «закрыта». Пуш — по явной просьбе пользователя.

---

## Самопроверка плана (выполнена при написании, ревизия 3)

1. **Покрытие spec:** §3.1 ключи/txn → Tasks 2,3,5,6; §3.2 отражение в коде → Tasks 2,3,6; §3.3/§4.5 Puzzle → Task 13; §4.1 → Tasks 2,3; §4.2 → Tasks 4,5; §4.3 → Task 6; §4.4 → Tasks 7,8,9; §5 фазы (вкл. «отказы с last_error») → Tasks 2,3; §6.1/§6.2 → Tasks 5,7,8,9 (DTO-цепочка caRotation: Core → API → JSON → TS — Step 7.4.4/9.1/9.5); §7.1 → Tasks 2,3,4 (порт/лимиты — кейс r3); §7.2 → Tasks 5,7,8; §7.3 → Task 10 (superset — примечание); §7.4 → Task 11; §7.5 → Task 13; §9 runbook → Task 12; §10 критерии 1–8 → Task 14 (критерий 7 — Step 14.8); §11 мерж-гейт → Task 14.9; roadmap-тег → Task 14.9. Пробелов нет.
2. **Достижимость веток диспетчеризации в кейсах:** кейсы «окно открыто» всегда открывают окно ФАКТОМ (посев `ca_next_*` через `SeedWindow`) — `OpenWindow_SkipsGuards_*` (обе версии) сеют staging; кейсы «waiting-*» НЕ сеят (окно закрыто); waiting-cluster — минимальный посев `SeedBare`.
3. **Journal — единый KV:** вентиль-кейс Waiting не ассертит waiting-фазу K (перезаписывается идущими ниже C/E: `supervise` → `rotate`); финальный журнал — `"rotate"`+`done`; InProgress-кейс — `rotate-ca done` без `supervise`.
4. **Компилируемость инжектов/ассертов:** `TxnRequest.Success` (не `Successes`); `ValkeyNodeSpec.NodeName/Host/ClientHostPort/CpuCores/MemoryBytes` (не `Node`/`Port`); `Removed` содержит полные имена контейнеров (`vwk-<C>-node1`); подмена Entry FakeEtcd — `with { Value = ... }` (init-only). Эвристики txn-детекции корректны (journal идёт PutAsync, не txn).
5. **Жизненный цикл кейсов Task 2/3:** промежуточные ассерты окна существуют ТОЛЬКО против реализации Task 2; Task 3 Step 3.1 переписывает все четыре кейса (финал или C-txn-инжект) ДО реализации R/C; на Step 3.3 КРАСНЫ все кейсы, кроме K0-группы (включая c9-инжект — инжект бездействует без C-txn); Step 3.5 требует весь файл зелёным.
6. **Греп-гейты различимы:** `'"/valkey/'` не совпадает с `"/api/valkey/..."`.
7. **Консистентность типов:** `RotationOutcome`, `Op`, ключи, payload, DTO-цепочка `caRotation` — едины по всему плану.
8. **Известные точки сверки с фактом** (отмечены в шагах): формат journal-JSON в посеве юнитов; хелперы Rig в DeprovisioningProcessTests/ValkeyOperationsTests/E2E-каркасе; поле `LastCaPem` в FakeValkeyConnection (добавить при отсутствии); catch-набор `ImportFromPem` в Puzzle; сигнатура `FakeEtcd.TxnFault` (+ свойство `Success`).
