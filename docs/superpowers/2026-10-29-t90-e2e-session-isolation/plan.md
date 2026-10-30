# t90: изоляция docker-E2E сессий + нулевые ложные скипы + параллельность классов — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-branch теги E2E-образов, per-process рантайм, ноль ложных скипов («запустил проект — ноль скипов»), параллельность E2E-классов (maxParallelThreads 5 + Guard до создания окружения), упразднение двухфазного раннер-контура PgWorker (класс = своё окружение на хосте).

**Architecture:** Статика (образы) — едина в ветке (`e2e-{ветка}`), раздельна между ветками; рантайм (контейнеры/сети/тома) — per-run guid. Тесты делятся на E2E-проекты (`<Имя>.E2eTests`) и интеграционные (`<Имя>.IntegrationTests`); общие тест-хелперы вынесены в новый проект `Shared.TestInfra` (решение владельца, AskUserQuestion 2026-10-29 — фиксируется в spec). PgWorker E2E — хостовая модель: класс-фикстура сама поднимает контур (Guard → сеть → etcd → ownS3 → CA → окно портов), тело факта исполняется на хосте по `localhost:{динамический порт}`.

**Tech Stack:** .NET 10, xunit.v3, Testcontainers, docker CLI.

**Spec:** `docs/superpowers/2026-10-29-t90-e2e-session-isolation/spec.md` (путь от корня worktree `/Users/demakaev/ZCodeProject/worktrees/fix-t90-e2e-session-isolation`).

## Global Constraints

- .NET 10, `TreatWarningsAsErrors=true`, `LangVersion=latest`, `Nullable=enable`; централизованные версии — `Directory.Packages.props` (новых NuGet-пакетов не добавлять).
- P/Invoke запрещён. Комментарии — по-русски, идентификаторы — по-английски. Тесты — AAA-комментарии (`// Arrange/Act/Assert`).
- Бюджеты не меняются: publish 10 мин, Kafka/Valkey build 120 с, retry старта 3 попытки, BrokerBootSec ≤ 100 с. Единственное новое ожидание — лок подготовки: шаг 5 с, бюджет 10 минут. `RunnerBudget` (60 мин) уходит вместе с раннером.
- Гейт-прогоны — двумя ОТДЕЛЬНЫМИ командами: сначала `dotnet build`, затем `dotnet test --no-build`; env-префиксы — при самой команде, не `export`.
- Порты docker-контейнеров — только динамические (`assignRandomHostPort` / `-p 127.0.0.1::<контейнерный>`); литералов хост-портов в ассертах нет.
- Локально собираемые образы в registry не кладутся. E2E-образы pg — publish на хосте, в образ — только артефакты (sdk-стадий нет).
- После каждой docker-серии — зачистка контейнеров/сетей (`docker ps -a` пуст от своего guid; `docker network prune -f` — только страховочный гейт).
- Все команды запускаются в фоне (`run_in_background: true`), вывод — в файл (`cmd > лог 2>&1`), анализ — из файла.

## Принятые решения (фиксируются в spec)

1. **Общие тест-хелперы** — новый общий проект `src/tests/Shared.TestInfra` (ВЫБОР ПОЛЬЗОВАТЕЛЯ, AskUserQuestion 2026-10-29: «Новый общий проект инфраструктуры»); оба типа тестовых проектов ссылаются на него; namespaces переносимых файлов сохраняются.
2. **OwnS3.cs остаётся** в `PgWorker.IntegrationTests/Backups/` (следует из spec §3.7 «интеграционные остаются: Backups/» и §3.11: BackupS3Tests зелёный в интеграционном прогоне; в перечне переноса §3.7 упомянут ошибочно — физически лежит в Backups/ и используется остающимися тестами).
3. **E2eProcessMergeTests переезжает без `[Collection]`-строки** — атрибут ссылается на `NonE2eCollection` из другой сборки (компиляция невозможна); в E2E-проекте классы collection-per-class (spec §3.9).
4. **FakePatroniPki.cs остаётся** в `PgWorker.IntegrationTests/E2e/` (используется только `Backups/FakeBackupDeps.cs`, который остаётся; в E2E-проекте не нужен).
5. **HostInstance (запущенный SUT-воркер, `E2eFixture.cs:493`) — тип PgWorker-проектов**: поле/конструктор ссылаются на `E2eEnvironment` (`E2eFixture.cs:495/498`) — в Shared.TestInfra не переносится (обратной ссылки Shared.TestInfra → PgWorker.* быть не может — цикл с подпунктом 2.1б): вырезается в отдельный файл `PgWorker.IntegrationTests/E2e/HostInstance.cs` (namespace сохранён) и переезжает в `PgWorker.E2eTests` вместе с `E2e/` (подпункт 3.2).
6. Мультифактные сценарные классы PgWorker делят контур класса; имена кластеров фактов различны литеральными префиксами (проверяется при миграции); при несовместимости фактов в одном контуре — класс делится (spec §3.8: «класс делится на два класса»).

## Review Focus

1. **Литерал хост-порта в ассерте** — тест обязан ходить по фактическому `docker port`-значению (`localhost:{p}`); ожидание: port-строка нигде не захардкожена. Закреплено: шаг 5 (динамические публикации), шаг 10 (grep-чеклист).
2. **Гонка двойной подготовки одного worktree** — конкурентные `PgWorker.E2ePrepare` обязаны сериализоваться локом `.prepare.lock`; критерий spec §6.8 — два РЕАЛЬНЫХ процесса: конкурентный запуск двух `dotnet run --project src/tests/PgWorker.E2ePrepare` (проверка шага 4, п.4) и same-process-ожидание в юнит-факте `PrepareLock_SecondAcquirerWaitsForHolder` (подпункт 4.6.9).
3. **Ложнозелёный прогон на устаревшем бинаре** — штамп `.prepared` (`{"stampMs"}`) против `NewestSourceInputUtc`; устаревший — пачка-фейл с подсказкой (С7), не скип и не автосборка. Закреплено: шаг 4 (штамп), шаг 5 (прогон без подготовки падает пачкой).
4. **Занятый хост-порт в окне старта окружения** — `already allocated` / `address already in use` обязаны попадать в ретрай (3 попытки, новые FreePort). Закреплено: шаги 6–8, факты `*RetryMask_*`.
5. **Остатки окружения после teardown** — сеть окружения теперь удаляется `DisposeAsync`'ом окружения (раньше её удалял контур); ассерт чистоты own-only по runId обязан видеть ноль объектов. Закреплено: шаг 5 (teardown сети), шаг 10 (`docker ps -a` пуст от guid).

---

### ШАГ 1. Каноны: docs/e2e-isolation.md, docs/e2e-launch.md, docs/runbook.md, AGENTS.md (arch-first)

**Вход:** Worktree `fix-t90-e2e-session-isolation` чист; spec одобрен; код не трогается.
**Действие (делает кодер сам):**

- [ ] `docs/e2e-isolation.md`:
  - §1 «Тест — две фазы…» — переписать на хостовую модель: единица окружения — тестовый класс (класс-фикстура, `IAsyncLifetime`); подъём контура в `InitializeAsync` (Guard-слот → сеть `pgw-en-{guid}` → etcd → ownS3 → per-run CA → окно портов 15100+); тело факта исполняется в хост-процессе; раннер-контейнер и двухфазность упразднены; факты класса последовательны и делят окружение; разным фактам нужны разные контуры — класс делится.
  - §2 «Ноль publish на хост…» — заменить на «динамические publish обязательны»: etcd/ownS3/API публикуют динамические хост-порты (`assignRandomHostPort` / `-p 127.0.0.1::`); фиксированные хост-порты запрещены; адресация теста — `localhost:{динамический порт}`, контейнеров — `host.docker.internal:{динамический порт}` и внутрисетевые имена (меняется только путь ТЕСТА).
  - §6 — старт-ретрай окружения: гонка сети И занятость зондируемого порта (`already allocated`, `address already in use` — в ретрай).
  - §9 — теги всех e2e-образов per-branch `e2e-{ветка}` (fallback `{хэш8}` пути worktree при git-недоступности/detached); publish-каталоги записи и /tmp-телеметрия статики — per-process; лок подготовки `artifacts/e2e/.prepare.lock` (5 с / 10 мин); авто-уборка тегов `e2e-*` старше 24 ч (отказы `rmi` игнорируются, текущий тег исключён).
  - §10 — дополнить: «рантайм per-process/per-run; статические образы per-branch — разные ветки не пересекаются ни по одному образу».
  - Чек-лист нового E2E-сценария (пп. 1–6, строки ~213–232) — ПЕРЕПИСАТЬ (не «дополнить»: после rewrite §1/§2 прежние пп. 1/3/6 противоречат канону): п.1 — единица окружения — тестовый класс: класс-фикстура (`IAsyncLifetime`), полный контур класса в `InitializeAsync`, тело факта — хост-процесс (никаких `E2eContour.OrchestrateClassAsync` и per-fact `await using var fx = E2eEnvironment.StartAsync(…)`); п.3 — адресация: тест ходит `localhost:{динамический порт}`, контейнеры — `host.docker.internal:{динамический порт}` и внутрисетевые имена (прежний запрет «Хост-портов, localhost-адресов и host.docker.internal в ассертах нет» УДАЛИТЬ — он прямо запрещает новую модель); п.6 — прогон: `dotnet build … -c Release` → `dotnet test … -c Release --no-build` БЕЗ `PGW_TEST_DOCKER` (env-гейта больше не существует); пп. 2 (имя кластера `{slug}{Fx.ClusterTag}`), 4 (нет `sleep`, поллинг ≤ 100 с), 5 (teardown + ассерт чистоты) — сохранить по смыслу; ДОБАВИТЬ пункт: Guard-слот (`E2eParallelismGuard.Acquire`) захватывается ДО создания первого docker-объекта окружения.
- [ ] `docs/e2e-launch.md`:
  - §0 — статика БЕЗ раннер-образа (только app/node/sd; для Kafka/Valkey/S3 — их per-branch образы, once-per-process); теги per-branch; лок подготовки (5 с / 10 мин); per-process имена `/tmp/pgw-e2e-static-{phase,process}-{ветка}-{pid}*.log`; авто-уборка 24 ч; NOBUILD — существующий per-branch тег (отсутствие — fail-fast).
  - §5 — параллелизм: класс-фикстуры + `xunit.runner.json maxParallelThreads: 5` + Guard-слот ДО создания окружения (PgWorker); Kafka/Valkey/S3 — только maxParallelThreads; отчётность: `Skipped: 0` — норма штатного прогона; fail-fast статики без подготовки.
  - Таблица «Где что лежит» — per-process имена статических журналов.
- [ ] `docs/runbook.md`: раздел «Запуск docker-E2E серий» (строки ~484+) — переписать по командам §3.7 spec (без `PGW_TEST_DOCKER`, перечисление всех проектов, «запустил проект — ноль скипов», двухфазность PgWorker удалена); строки ~17 и ~44-45 — убрать `PGW_TEST_DOCKER=1` из упоминаний запуска docker-E2E.
- [ ] `AGENTS.md` (корень pg):
  - строка ~27 (⚠️ «Skip'ы в тестах могут быть из-за оркестратора…») — заменить на: «⚠️ **Скипов в штатном прогоне тестового проекта нет.** Запуск любого тестового проекта на docker-хосте даёт отчёт только из Passed/Failed; единственный допускаемый скип — условное предусловие TLI-сценария (`_prepareSkip`: проверяемое состояние физически не возникло). Любой иной Skip — дефект.»
  - блок мерж-гейта (строки ~38-48): команды без `PGW_TEST_DOCKER`: сборка `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`; подготовка `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` (собирает e2e-образы pgworker/pgworker-{node,backup,wal}/sdgenerator и пишет штамп — без `pgw-e2e-runner`); кейс-маркер `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`.

**Выход:** каноны описывают целевое состояние; `grep -rn "PGW_TEST_DOCKER" docs/e2e-isolation.md docs/e2e-launch.md docs/runbook.md AGENTS.md` — пусто.
**Проверка:** `grep -rn "PGW_TEST_DOCKER" docs/e2e-isolation.md docs/e2e-launch.md docs/runbook.md AGENTS.md` (от корня worktree) — пусто. Каталог `docs/` ЦЕЛИКОМ не сканировать: `docs/superpowers/` хранит историю задач (spec/plan t90 и прежних задач с десятками упоминаний) — история не переписывается (spec §8); все упоминания вне superpowers — ровно в этих четырёх канон-файлах. `grep -rn "pgw-e2e-runner\|две фазы" docs/e2e-isolation.md docs/e2e-launch.md` — пусто.
**Связь со spec:** §3.10 (каноны первой фазой), §2 (arch-first), §8 (история).

### ШАГ 2. Общий проект инфраструктуры Shared.TestInfra

**Вход:** Шаг 1 выполнен; solution собирается до правок (`dotnet build src/PgWorker.slnx -c Release --nologo` зелёный).
**Действие (кодер):** создать `src/tests/Shared.TestInfra`, перенести в него общие хелперы (один экземпляр, namespaces сохранены), перевести используемые кросс-сборочно члены из `internal` в `public`, подключить к slnx и четырём интеграционным проектам.

- [ ] **2.1 (ПОДПУНКТ для слабой модели)** Создать проект и включить его в solution и потребители.
- [ ] **2.2 (ПОДПУНКТ для слабой модели)** Переместить 5 файлов хелперов в Shared.TestInfra с точным списком правок (включая судьбу `HostInstance` — см. решение 5).
- [ ] **2.3 (ПОДПУНКТ для слабой модели)** Добавить новый файл `E2eImageTags.cs` (per-branch суффикс) и `E2eTagCleanup.cs` (чистая функция отбора + docker-обёртка уборки).
- [ ] Ревью переноса (кодер): убедиться, что `E2eFixture.cs` больше не содержит `WaitProvisionedAsync`/`ProvisionStuckFailFast` (они переехали в `E2eEnvironment.cs`), что `HostInstance` вынесен в `src/tests/PgWorker.IntegrationTests/E2e/HostInstance.cs` (в Shared.TestInfra его нет), что ничего из Shared.TestInfra не ссылается на типы четырёх воркеров, и собрать solution.

**Выход:** `src/tests/Shared.TestInfra` собирается; интеграционные проекты компилируются без правок своих `using` (namespaces сохранены); `E2eImageTags`/`E2eTagCleanup` доступны следующим шагам.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` (отдельная команда, фон, вывод в файл) — 0 ошибок/предупреждений.
**Связь со spec:** §3.1, §3.2, §3.0; решение владельца «Shared.TestInfra» (AskUserQuestion 2026-10-29).

**ПОДПУНКТ 2.1.** Создать файл `src/tests/Shared.TestInfra/Shared.TestInfra.csproj` с содержимым:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <PropertyGroup>
        <TargetFramework>net10.0</TargetFramework>
        <LangVersion>latest</LangVersion>
        <Nullable>enable</Nullable>
        <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    </PropertyGroup>

</Project>
```

Затем: (а) в `/Users/demakaev/ZCodeProject/worktrees/fix-t90-e2e-session-isolation/src/PgWorker.slnx` в `Folder Name="/tests/"` после строки `<Project Path="tests/PgWorker.E2ePrepare/PgWorker.E2ePrepare.csproj" />` добавить `<Project Path="tests/Shared.TestInfra/Shared.TestInfra.csproj" />`; (б) в каждый из четырёх csproj — `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj`, `src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`, `src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj`, `src/tests/S3Worker.IntegrationTests/S3Worker.IntegrationTests.csproj` — в ItemGroup с ProjectReference первой строкой добавить `<ProjectReference Include="..\Shared.TestInfra\Shared.TestInfra.csproj"/>`.

**ПОДПУНКТ 2.2.** Переместить файлы (из корня `/Users/demakaev/ZCodeProject/worktrees/fix-t90-e2e-session-isolation`, все пути абсолютные при исполнении):

```
git mv src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs   src/tests/Shared.TestInfra/E2e/E2eFixture.cs
git mv src/tests/PgWorker.IntegrationTests/E2e/E2eTestPki.cs   src/tests/Shared.TestInfra/E2e/E2eTestPki.cs
git mv src/tests/KafkaWorker.IntegrationTests/Kafka/FreePortWindow.cs  src/tests/Shared.TestInfra/Kafka/FreePortWindow.cs
git mv src/tests/ValkeyWorker.IntegrationTests/Valkey/FreePortWindow.cs src/tests/Shared.TestInfra/Valkey/FreePortWindow.cs
git mv src/tests/S3Worker.IntegrationTests/S3/FreePortWindow.cs src/tests/Shared.TestInfra/S3/FreePortWindow.cs
```

Правки (namespaces НЕ менять):
1. `src/tests/Shared.TestInfra/E2e/E2eFixture.cs`: удалить строку `using Xunit;` (после правки 2 типы xUnit не используются). Вырезать целиком два метода — `WaitProvisionedAsync` и `ProvisionStuckFailFast` (с их xml-doc) — и вставить их в `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` внутрь класса `E2eEnvironment` как `public static` (тела дословно; они ссылаются на тип `E2eEnvironment`, который остаётся в PgWorker-проектах). Во всех файлах `src/tests/PgWorker.IntegrationTests/E2e/*.cs` заменить вызовы: `E2eFixture.WaitProvisionedAsync` → `E2eEnvironment.WaitProvisionedAsync`, `E2eFixture.ProvisionStuckFailFast` → `E2eEnvironment.ProvisionStuckFailFast` (20 файлов: 19 с `ProvisionStuckFailFast` и `E2eBackupPoolScenarios.cs:181` с `WaitProvisionedAsync`; замена строковая, тела не трогать). Перевести члены из `internal` в `public` (точные имена): `Timing`, `DrainTiming`, обе перегрузки `RunProcessAsync`, `RunProcessMergedAsync`, `RunDockerLogsAsync`, `NoMsBuildReuseEnv`, `PublishProjectAsync`, `EnsurePublishedDir`, `OutputTail`, `FreePort`, `FindRoot`. `private` члены (`RunProcessCoreAsync`, `MergeOutput`, `WriteLogAsync`, `DrainAsync`, `GenerateSecret`) не трогать.
2. **Судьба `HostInstance`** (класс держит поле `private readonly E2eEnvironment _env;` и конструктор `public HostInstance(E2eEnvironment env, string name, string alias, string containerName)` — `E2eFixture.cs:493/495/498`; `E2eEnvironment` — тип PgWorker-проектов, в Shared.TestInfra класс не скомпилируется, обратной ссылки Shared.TestInfra → PgWorker.* быть не может — цикл с подпунктом 2.1б): вырезать целиком `public sealed class HostInstance` С его xml-doc (хвост файла `src/tests/Shared.TestInfra/E2e/E2eFixture.cs` после закрывающей скобки класса `E2eFixture`) в НОВЫЙ файл `src/tests/PgWorker.IntegrationTests/E2e/HostInstance.cs` — namespace `PgWorker.IntegrationTests.E2e` (сохранён), тело класса дословно; `using` не нужны (`IAsyncDisposable` — из System, входит в ImplicitUsings `src/Directory.Build.props`; `E2eFixture`/`E2eEnvironment` — тот же namespace). Файл остаётся в PgWorker-проекте и переезжает в `PgWorker.E2eTests` вместе с `E2e/` на подпункте 3.2 (включён в его список переноса).
3. `src/tests/Shared.TestInfra/E2e/E2eTestPki.cs` — правок нет (все члены уже public).
4. Каждый из трёх `FreePortWindow.cs` — заменить `internal static class FreePortWindow` на `public static class FreePortWindow` и `internal const int Size` на `public const int Size`; остальное дословно.
5. `src/tests/PgWorker.IntegrationTests/Backups/OwnS3.cs` — строку `private const string Image = E2eEnvironment.OwnS3Image;` заменить на `private const string Image = "owns3:dev";` (комментарий над ней сохранить).

**ПОДПУНКТ 2.3.** Создать файл `src/tests/Shared.TestInfra/ImageTags/E2eImageTags.cs`:

```csharp
using System.Security.Cryptography;
using System.Text;

namespace Shared.TestInfra;

/// <summary>Суффикс per-branch тегов e2e-образов (docs/e2e-isolation.md §9):
/// санитизированное имя ветки — [a-z0-9._-]{1,32}; git недоступен или HEAD
/// detached — fallback {хэш8}: первые 8 hex SHA256 абсолютного пути корня.
/// Одна ветка — один набор образов (git-инвариант «одна ветка — один checkout»).</summary>
public static class E2eImageTags
{
    public static string SuffixFor(string repoRoot)
    {
        var branch = SanitizeBranch(ReadBranch(repoRoot));
        return branch.Length > 0 ? branch : FallbackHash(repoRoot);
    }

    /// <summary>Ветка для лог-суффиксов: санитизированное имя или пустая строка
    /// (git недоступен / detached). Отличается от SuffixFor отсутствием hash8.</summary>
    public static string BranchFor(string repoRoot) => SanitizeBranch(ReadBranch(repoRoot));

    /// <summary>Санитизация имени ветки: lower-case, недопустимые символы → '-',
    /// трим конечных '-', максимум 32 символа.</summary>
    public static string SanitizeBranch(string raw)
    {
        var chars = raw.Trim().ToLowerInvariant()
            .Select(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '-' or '_' ? c : '-')
            .ToArray();
        var branch = new string(chars).TrimEnd('-');
        return branch.Length > 32 ? branch[..32].TrimEnd('-') : branch;
    }

    public static string FallbackHash(string root)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(root))).ToLowerInvariant()[..8];

    private static string ReadBranch(string root)
    {
        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo("git")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = root,
            };
            psi.ArgumentList.Add("rev-parse");
            psi.ArgumentList.Add("--abbrev-ref");
            psi.ArgumentList.Add("HEAD");
            using var process = new System.Diagnostics.Process { StartInfo = psi };
            process.Start();
            if (!process.WaitForExit(5000) || process.ExitCode != 0)
                return "";
            var output = process.StandardOutput.ReadToEnd().Trim();
            // detached HEAD: abbrev-ref печатает буквально "HEAD" — ветки нет.
            return output.Length == 0 || output == "HEAD" ? "" : output;
        }
        catch
        {
            return "";
        }
    }
}
```

Создать файл `src/tests/Shared.TestInfra/ImageTags/E2eTagCleanup.cs`:

```csharp
using System.Globalization;

namespace Shared.TestInfra;

/// <summary>Авто-уборка per-branch e2e-тегов старше 24 ч (docs/e2e-isolation.md
/// §9). Отбор — чистая функция (юнит-тестируема без docker); удаление —
/// `docker rmi` по одному, ЛЮБОЙ отказ (в т.ч. «image is being used by running
/// container») игнорируется. NEVER-список (owns3:dev, minio/mc, etcd, *:dev и
/// любые теги без префикса e2e-) отсечён фильтром «тег начинается с e2e-».</summary>
public static class E2eTagCleanup
{
    public static readonly TimeSpan MaxAge = TimeSpan.FromHours(24);

    /// <summary>Чистая функция отбора. Вход — вывод
    /// `docker images --format "{{.Repository}}:{{.Tag}}|{{.CreatedAt}}"`;
    /// время в формате «2006-01-02 15:04:05 -0700 MST». Отбор: репозиторий ∈
    /// repos, тег начинается с "e2e-", возраст &gt; age от nowUtc, тег ≠ currentTag
    /// (страховка от перекоса часов), строка неразбираема — пропускается.</summary>
    public static IReadOnlyList<string> SelectStaleE2eTags(
        string dockerImagesOutput, IReadOnlyCollection<string> repos,
        string currentTag, TimeSpan age, DateTime nowUtc)
    {
        var stale = new List<string>();
        foreach (var raw in dockerImagesOutput.Split(
                     '\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var sep = raw.LastIndexOf('|');
            if (sep <= 0 || !TryParseDockerCreated(raw[(sep + 1)..].Trim(), out var createdUtc))
                continue;
            var image = raw[..sep];
            var colon = image.LastIndexOf(':');
            if (colon <= 0)
                continue;
            var repo = image[..colon];
            var tag = image[(colon + 1)..];
            if (!repos.Contains(repo))
                continue;
            if (!tag.StartsWith("e2e-", StringComparison.Ordinal))
                continue;
            if (tag == currentTag)
                continue;
            if (nowUtc - createdUtc > age)
                stale.Add(image);
        }
        return stale;
    }

    /// <summary>Docker-обёртка: читает список образов, удаляет отобранные
    /// теги; ошибки списка/удаления не роняют вызов (телеметрия [PHASE]).</summary>
    public static async Task CleanupAsync(
        IReadOnlyCollection<string> repos, string currentTag,
        Func<string, string[], CancellationToken, Task<string>> runProcessAsync,
        CancellationToken ct)
    {
        string output;
        try
        {
            output = (await runProcessAsync("docker",
                ["images", "--format", "{{.Repository}}:{{.Tag}}|{{.CreatedAt}}"], ct)).Trim();
        }
        catch (Exception e)
        {
            Console.WriteLine(
                $"[PHASE] e2e-static: cleanup skipped — docker images failed: {e.Message.Split('\n')[0]}");
            return;
        }

        var stale = SelectStaleE2eTags(output, repos, currentTag, MaxAge, DateTime.UtcNow);
        foreach (var image in stale)
        {
            try
            {
                await runProcessAsync("docker", ["rmi", image], ct);
            }
            catch
            {
                // отказ rmi (образ занят живым контейнером и пр.) — игнорируется
            }
        }

        if (stale.Count > 0)
            Console.WriteLine($"[PHASE] e2e-static: cleanup removed {stale.Count} stale e2e tag(s)");
    }

    private static bool TryParseDockerCreated(string raw, out DateTime utc)
    {
        utc = DateTime.MinValue;
        // Формат docker («yyyy-MM-dd HH:mm:ss +HHMM TZ»): смещение без
        // двоеточия и буквенная зона в конце; штатный DateTimeOffset.TryParse
        // этот формат НЕ разбирает — состав разбираем вручную.
        var parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 4 || parts[2].Length != 5 || (parts[2][0] is not '+' and not '-'))
            return false;
        var offset = $"{parts[2][..3]}:{parts[2][3..]}";
        if (!DateTime.TryParseExact(
                $"{parts[0]} {parts[1]} {offset}", "yyyy-MM-dd HH:mm:ss zzz",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            return false;
        utc = parsed.ToUniversalTime();
        return true;
    }
}
```

Проверка подпункта: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/Shared.TestInfra/Shared.TestInfra.csproj -c Release --nologo` — 0 ошибок (фон, вывод в файл).

### ШАГ 3. Разделение на проекты: четыре `<Имя>.E2eTests`; упразднение гейта PGW_TEST_DOCKER

**Вход:** Шаг 2 выполнен (Shared.TestInfra существует, solution собирается). Namespace переносимых файлов сохраняется (`*.IntegrationTests.E2e` и т.п.) — «как есть».
**Действие (кодер):** создать четыре E2E-проекта, перенести E2e-папки, удалить `DockerTrait` (обе копии), все `PGW_TEST_DOCKER`-гейты/броски/упоминания (код И комментарии) в `src/`, обновить `E2ePrepare`.

- [ ] **3.1 (ПОДПУНКТ)** Создать `PgWorker.E2eTests` (csproj + runner.json + slnx).
- [ ] **3.2 (ПОДПУНКТ)** Перенести 39 файлов `PgWorker.IntegrationTests/E2e/` в новый проект; снять `[Collection]`-строку с E2eProcessMergeTests.
- [ ] **3.3 (ПОДПУНКТ)** Удалить PgWorker-`DockerTrait` и все его вызовы и комментарии-упоминания в обоих PgWorker-тестовых проектах.
- [ ] **3.4 (ПОДПУНКТ)** Создать `KafkaWorker.E2eTests` и перенести его E2e-папку с удалением гейтов.
- [ ] **3.5 (ПОДПУНКТ)** Создать `ValkeyWorker.E2eTests` и перенести его E2e-папку с удалением гейтов.
- [ ] **3.6 (ПОДПУНКТ)** Создать `S3Worker.E2eTests` и перенести его E2e-папку с удалением гейтов (включая копию DockerTrait в S3Worker.IntegrationTests).
- [ ] **3.7 (ПОДПУНКТ)** Переключить `PgWorker.E2ePrepare` на ссылку нового проекта и вычистить упоминание `PGW_TEST_DOCKER` из его комментариев.
- [ ] Ревью (кодер): сборка обоих конфигурационных состояний, поиск остатков гейта, запуск интеграционной серии PgWorker.

**Выход:** `src/tests/{PgWorker,KafkaWorker,ValkeyWorker,S3Worker}.E2eTests` существуют в slnx, каждый с `xunit.runner.json` (`maxParallelThreads: 5`); упоминаний `PGW_TEST_DOCKER`/`DockerTrait` в src/ нет, КРОМЕ трёх остатков раннер-механики — `E2eContour.cs:11`/`:48` и строки `["PGW_TEST_DOCKER"] = "1",` в `BuildRunnerEnv` (`E2eEnvironment.cs:954`) — все три удаляются в шаге 5 вместе с раннер-контуром; интеграционные проекты собираются и запускаются без env.
**Проверка:** (1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 ошибок; (2) `grep -rn "PGW_TEST_DOCKER\|DockerTrait" src/` показывает только три остатка раннер-механики (E2eContour.cs:11/:48, E2eEnvironment.cs:954 внутри BuildRunnerEnv), удаляемых в шаге 5; полный `grep -rn "PGW_TEST_DOCKER" src/` → пусто проверяется в шаге 5 и в шаге 10; (3) `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER` НЕ задан: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~Docker"` — в отчёте `Skipped: 0` (docker-хост доступен; упавшие вне области задачи тесты — в `arch/roadmap/bugs.md`, не чинить здесь).
**Связь со spec:** §3.7 (разделение + упразднение env-гейта), §3.9 (runner.json 5), §6.2-6.3.

**ПОДПУНКТ 3.1.** Создать `src/tests/PgWorker.E2eTests/PgWorker.E2eTests.csproj`: взять `/Users/demakaev/ZCodeProject/worktrees/fix-t90-e2e-session-isolation/src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` и скопировать из него ЦЕЛИКОМ группы: коммент-блок + `<ItemGroup>` с `xunit.runner.json` (CopyToOutputDirectory), `<ItemGroup>` с Using (FluentAssertions, Xunit), `<ItemGroup>` с PackageReference (coverlet.collector, FluentAssertions, Microsoft.AspNetCore.Mvc.Testing, Microsoft.NET.Test.Sdk, Npgsql, Testcontainers, xunit.runner.visualstudio, xunit.v3), `<ItemGroup>` с ProjectReference (PgWorker.Core/Etcd/Docker/Provisioning/Moves/Backups/WalReceiver/App, AdminPanel.Etcd — все пути заменить с `..\..\` на `..\..\..\`, т.е. `..\..\..\PgWorker.Core\PgWorker.Core.csproj` и т.д., включая `..\..\..\AdminPanel.Etcd\AdminPanel.Etcd.csproj`), `<ItemGroup>` с Using (Shared.Core, Shared.Core.Planning, Shared.Docker, Shared.Etcd.Coordination). Шапка: `<Project Sdk="Microsoft.NET.Sdk">` + PropertyGroup `net10.0`/`LangVersion=latest`/`Nullable=enable`/`TreatWarningsAsErrors=true`. Ссылку на `PgWorker.IntegrationTests.csproj` НЕ добавлять. Дополнительно добавить `<ProjectReference Include="..\Shared.TestInfra\Shared.TestInfra.csproj"/>`. Создать `src/tests/PgWorker.E2eTests/xunit.runner.json`:

```json
{
    "$schema": "https://xunit.net/schema/current/xunit.runner.schema.json",
    "maxParallelThreads": 5
}
```

В `src/PgWorker.slnx` в `Folder Name="/tests/"` после строки `tests/PgWorker.IntegrationTests/...` добавить `<Project Path="tests/PgWorker.E2eTests/PgWorker.E2eTests.csproj" />`.

**ПОДПУНКТ 3.2.** Из корня worktree создать каталог `src/tests/PgWorker.E2eTests/E2e/` и выполнить `git mv` (39 файлов, пути относительно `src/tests/PgWorker.IntegrationTests/E2e/` → `src/tests/PgWorker.E2eTests/E2e/`): `E2eAppParamsScenarios.cs E2eAppSecretScenarios.cs E2eAutoBuildTests.cs E2eBackupFailsOnBadS3Scenarios.cs E2eBackupJobsScenarios.cs E2eBackupPoolScenarios.cs E2eBackupVerifyScenarios.cs E2eContour.cs E2eDrillScenarios.cs E2eEnvironment.cs E2eEtcdSnapshotExportScenarios.cs E2eHaEtcdScenarios.cs E2eMoveScenarios.cs E2eParallelismGuard.cs E2ePatroniFileSdScenarios.cs E2ePatroniRestTlsScenarios.cs E2ePgtuneScenarios.cs E2ePhase.cs E2eProcessMergeTests.cs E2eQuorumElectedTests.cs E2eRestoreNewClusterScenarios.cs E2eRestoreScenarios.cs E2eRetentionScenarios.cs E2eRotateScenarios.cs E2eScaleScenarios.cs E2eScenarios.cs E2eSecondInstanceScenarios.cs E2eStrictScenarios.cs E2eSupervisorOrphanDrHoldScenarios.cs E2eSupervisorOrphanRegistryScenarios.cs E2eSupervisorSelfHealScenarios.cs E2eTiming.cs E2eTliSwitchScenarios.cs E2eWalStreamMasterDownScenarios.cs E2eWalStreamPromoteScenarios.cs E2eWalStreamScenarios.cs E2eWalStreamTliScenarios.cs E2eWorkerCertScenarios.cs HostInstance.cs`. НЕ переносить (уже в Shared.TestInfra): `E2eFixture.cs`, `E2eTestPki.cs`; остаётся на месте: `FakePatroniPki.cs`. После переноса в `src/tests/PgWorker.E2eTests/E2e/E2eProcessMergeTests.cs` удалить единственную строку `[Collection(NonE2eCollection.Name)]` (атрибут и её коммент-строку «Не-E2E факт: …» оставить: удалить ТОЛЬКО строку атрибута).

**ПОДПУНКТ 3.3.** (1) Удалить файл `src/tests/PgWorker.IntegrationTests/Docker/DockerTrait.cs`. (2) Во всех `.cs` под `src/tests/PgWorker.IntegrationTests/` и `src/tests/PgWorker.E2eTests/` удалить каждую строку вида `        DockerTrait.SkipIfUnavailable();` (grep: `grep -rn "DockerTrait.SkipIfUnavailable()" src/tests/PgWorker.IntegrationTests src/tests/PgWorker.E2eTests`). (3) В файлах `src/tests/PgWorker.E2eTests/E2e/*.cs` дополнительно удалить каждую строку `using PgWorker.IntegrationTests.Docker;` (в этом проекте namespace Docker отсутствует). (4) В `src/tests/PgWorker.E2eTests/E2e/E2eTliSwitchScenarios.cs` в `TliSwitchContext.InitializeAsync` удалить гвард-блок:

```csharp
        // Гейт docker в фикстуре: без PGW_TEST_DOCKER=1 Fact'ы скипаются
        // DockerTrait'ом — контур не поднимаем (InitializeAsync выполняется
        // до первого Fact'а класса).
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            return;
```

(5) Удалить комментарии, упоминающие `PGW_TEST_DOCKER`/`DockerTrait`, в правленых файлах (точные места — строки реального кода): `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs:9` («…Гейт: PGW_TEST_DOCKER=1.»), `Docker/MasterLeaseFailoverTests.cs:18` («…Гейт PGW_TEST_DOCKER=1 (DockerTrait).»), `Docker/MasterLeaseHttpsTests.cs:17` («// Гейт PGW_TEST_DOCKER=1 (DockerTrait).»), `Docker/NodeEntrypointRestCaTests.cs:16` («…Гейт PGW_TEST_DOCKER=1.»), `Docker/RestRotationTests.cs:12` («PGW_TEST_DOCKER=1; окружение per-guid с ассертом чистоты (e2e-isolation).»), `Docker/RestTlsConvergenceTests.cs:220` («…Гейт PGW_TEST_DOCKER=1.»); строка комментария удаляется целиком, если она только про гейт, иначе обрезается до текста без упоминания гейта. (6) Убедиться, что `grep -rn "DockerTrait\|PGW_TEST_DOCKER" src/tests/PgWorker.IntegrationTests src/tests/PgWorker.E2eTests` показывает ТОЛЬКО три остатка раннер-механики, удаляемые в ШАГЕ 5 вместе с раннер-контуром (на шаге 3 НЕ трогать): `src/tests/PgWorker.E2eTests/E2e/E2eContour.cs:11` (комментарий «…DockerTrait-гейт →…»), `src/tests/PgWorker.E2eTests/E2e/E2eContour.cs:48` (комментарий «Гейт docker-серии — на хосте (раннер получает PGW_TEST_DOCKER=1).» — файл `E2eContour.cs` удаляется ЦЕЛИКОМ в шаге 5) и `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs:954` (`["PGW_TEST_DOCKER"] = "1",` внутри `BuildRunnerEnv` — удаляется вместе с `BuildRunnerEnv` в шаге 5); иных упоминаний быть не должно.

**ПОДПУНКТ 3.4.** KafkaWorker.E2eTests: (а) создать `src/tests/KafkaWorker.E2eTests/KafkaWorker.E2eTests.csproj` по образцу `src/tests/KafkaWorker.IntegrationTests/KafkaWorker.IntegrationTests.csproj`: скопировать целиком группы PackageReference, ProjectReference (пути `..\..\` → `..\..\..\`), Using; добавить `<ProjectReference Include="..\Shared.TestInfra\Shared.TestInfra.csproj"/>`; добавить `<ItemGroup><None Update="xunit.runner.json" CopyToOutputDirectory="PreserveNewest"/></ItemGroup>`; (б) `cp src/tests/KafkaWorker.IntegrationTests/xunit.runner.json src/tests/KafkaWorker.E2eTests/xunit.runner.json` (git add); (в) slnx: после строки KafkaWorker.IntegrationTests добавить `<Project Path="tests/KafkaWorker.E2eTests/KafkaWorker.E2eTests.csproj" />`; (г) `git mv src/tests/KafkaWorker.IntegrationTests/E2e src/tests/KafkaWorker.E2eTests/E2e` (4 файла: KafkaE2eEnvironment.cs, KafkaE2ePhase.cs, KafkaE2eTakeoverTests.cs, KafkaProcessMergeTests.cs); (д) во всех файлах `src/tests/KafkaWorker.E2eTests/E2e/*.cs` удалить блоки:

```csharp
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E requires PGW_TEST_DOCKER=1 (otherwise Skip in the test)");
```

и

```csharp
            Assert.Skip("PGW_TEST_DOCKER=1 not set — docker-E2E skipped");
```

вместе с охватывающим `if (...) { ... }`, если Assert.Skip обёрнут (grep: `grep -rn "PGW_TEST_DOCKER" src/tests/KafkaWorker.E2eTests`); удалить комментарии, упоминающие `PGW_TEST_DOCKER`, в этих файлах (например, строку «Гейт запуска: PGW_TEST_DOCKER=1; …» в xml-doc KafkaE2eEnvironment). Проверка: `grep -rn "PGW_TEST_DOCKER" src/tests/KafkaWorker.IntegrationTests src/tests/KafkaWorker.E2eTests` — пусто.

**ПОДПУНКТ 3.5.** ValkeyWorker.E2eTests — аналогично ПОДПУНКТУ 3.4 по образцу `src/tests/ValkeyWorker.IntegrationTests/ValkeyWorker.IntegrationTests.csproj` (группы: PackageReference coverlet.collector/FluentAssertions/Microsoft.AspNetCore.Mvc.Testing/Microsoft.NET.Test.Sdk/Testcontainers/xunit.runner.visualstudio/xunit.v3; ProjectReference ValkeyWorker.Core/Etcd/Docker/Provisioning/App с путями `..\..\..\`; Using Shared.Core, Shared.Docker, Shared.Core.Planning, Shared.Etcd.Coordination; + Shared.TestInfra; + ItemGroup None xunit.runner.json). `src/tests/ValkeyWorker.E2eTests/xunit.runner.json` — создать с содержимым из ПОДПУНКТА 3.1 (maxParallelThreads 5). `git mv src/tests/ValkeyWorker.IntegrationTests/E2e src/tests/ValkeyWorker.E2eTests/E2e` (6 файлов: ValkeyE2eEnvironment.cs, ValkeyE2eLifecycleTests.cs, ValkeyE2ePhase.cs, ValkeyE2eTakeoverTests.cs, ValkeyProcessMergeTests.cs, ValkeyShellToFileTests.cs). Удалить все PGW_TEST_DOCKER-блоки/броски `InvalidOperationException("E2E requires PGW_TEST_DOCKER=1…")`/`Assert.Skip("PGW_TEST_DOCKER=1 is not set…")` с охватами и комментарии-упоминания (grep: `grep -rn "PGW_TEST_DOCKER" src/tests/ValkeyWorker.IntegrationTests src/tests/ValkeyWorker.E2eTests` → пусто). slnx-строку добавить после ValkeyWorker.IntegrationTests.

**ПОДПУНКТ 3.6.** S3Worker.E2eTests — аналогично по образцу `src/tests/S3Worker.IntegrationTests/S3Worker.IntegrationTests.csproj` (ProjectReference S3Worker.Core/Etcd/Docker/Provisioning/App с `..\..\..\`; Using Shared.Core, Shared.Docker, Shared.Core.Planning, Shared.Etcd.Coordination; + Shared.TestInfra; + ItemGroup None xunit.runner.json). `xunit.runner.json` — как в 3.1. `git mv src/tests/S3Worker.IntegrationTests/E2e src/tests/S3Worker.E2eTests/E2e` (10 файлов: S3E2eApiTests.cs S3E2eEnvironment.cs S3E2eLifecycleTests.cs S3E2eMeshTests.cs S3E2eRotationTests.cs S3E2eScaleTests.cs S3E2eSupervisionTests.cs S3E2eSyncProbeTests.cs S3E2eTakeoverTests.cs S3ShellToFileTests.cs). Удалить: (а) все `InvalidOperationException("E2E requires PGW_TEST_DOCKER=1…")`-гварды в S3E2eEnvironment.cs (2 шт., в StartAsync и StartTwoAsync); (б) все `Assert.Skip("PGW_TEST_DOCKER=1 not set — docker E2E skipped");`-блоки во всех файлах E2e/ (с охватывающими if, если есть); (в) файл `src/tests/S3Worker.IntegrationTests/Etcd/DockerTrait.cs`; (г) строки `DockerTrait.SkipIfUnavailable();` в `src/tests/S3Worker.IntegrationTests/S3/ProvisioningTests.cs` (2 шт.) и `src/tests/S3Worker.IntegrationTests/S3/SupervisionTests.cs` (1 шт.); (д) комментарии-упоминания PGW_TEST_DOCKER во всех правленых файлах. slnx-строку добавить после S3Worker.IntegrationTests. Проверка: `grep -rn "PGW_TEST_DOCKER" src/tests/S3Worker.IntegrationTests src/tests/S3Worker.E2eTests` — пусто.

**ПОДПУНКТ 3.7.** В `src/tests/PgWorker.E2ePrepare/PgWorker.E2ePrepare.csproj` заменить ProjectReference: `..\PgWorker.IntegrationTests\PgWorker.IntegrationTests.csproj` → `..\PgWorker.E2eTests\PgWorker.E2eTests.csproj` (атрибут `ExcludeAssets="build;buildTransitive;analyzers"` и комментарий сохранить). В `src/tests/PgWorker.E2ePrepare/Program.cs` в шапке-комментарии строку «Затем:  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test … (серии,» заменить на «Затем:  DOTNET_CLI_UI_LANGUAGE=en dotnet test … (серии,» (убрать `PGW_TEST_DOCKER=1 `).

### ШАГ 4. PgWorker per-branch статика: тег, штамп JSON, лок подготовки, per-process /tmp-логи, авто-уборка, юнит-тесты

**Вход:** Шаг 3 выполнен; `E2eEnvironment.cs` и `E2eFixture.cs` (Shared.TestInfra) на местах; solution собирается.
**Действие (кодер + подпункты):**

- [ ] **4.1 (ПОДПУНКТ)** Перевод `E2eEnvironment` на per-branch суффикс (удаление хэша пути и `PGW_E2E_TAG`).
- [ ] **4.2 (ПОДПУНКТ)** Штамп `.prepared` — JSON `{"stampMs"}` + атомарная запись + парсинг при проверке.
- [ ] **4.3 (ПОДПУНКТ)** Лок подготовки `E2ePrepareLock` + интеграция в `PrepareStaticAssetsAsync`.
- [ ] **4.4 (ПОДПУНКТ)** Per-process имена /tmp-логов статических фаз.
- [ ] **4.5 (ПОДПУНКТ)** Вызов авто-уборки после штампа.
- [ ] **4.6 (ПОДПУНКТ)** Юнит-тесты `E2eStaticInfraTests` (без docker).
- [ ] Ревью (кодер): проверить, что `VerifyStaticReadyOrThrowAsync` по-прежнему сравнивает штамп с `NewestSourceInputUtc` и что `NOBUILD`-ветка не выполняет проверку штампа.

**Выход:** теги образов PgWorker — `e2e-{ветка}` (без хэша пути); штамп — JSON; конкурентные подготовки сериализуются (same-process — факт 4.6.9, ДВА реальных процесса — п.4 проверки шага); /tmp-логи — per-process; юнит-факты зелёные.
**Проверка:** (1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 ошибок; (2) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~E2eStaticInfraTests"` — все Passed (docker не нужен); (3) ручная подготовка `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` — успех; в `artifacts/e2e/.prepared` — JSON с `stampMs`; повторный запуск — тот же тег (`docker images | grep pgworker` — `e2e-{ветка}` без хэша); файл `/tmp/pgw-e2e-static-phase-{ветка}-{pid}.log` появился, старого имени `/tmp/pgw-e2e-static-phase.log` нет; (4) сериализация лока ДВУХ РЕАЛЬНЫХ процессов (критерий spec §6.8): запустить ОДНОВРЕМЕННО два `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` (фон; логи в два разных файла); оба завершились успехом — никто не упал по бюджету лока; publish-фазы в журнале второго процесса начинаются после финальных `[PHASE]`-меток первого (сериализованы локом, publish-каталоги не конкурировали); (5) после docker-операций — зачистка (`docker ps -a` пуст от своих guid).
**Связь со spec:** §3.1, §3.2, §3.3, §6.6, §6.8, §6.10-6.12.

**ПОДПУНКТ 4.1.** В `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` заменить блок от строки `// ===== Теги pg-собираемых e2e-образов: per-worktree =====` до конца метода `TryGitBranch` (включительно, т.е. константу `TagSuffixEnv`, поле `ImageTagSuffix`, метод `ComputeImageTagSuffix`, метод `TryGitBranch`) на:

```csharp
    // ===== Теги pg-собираемых e2e-образов: PER-BRANCH (docs/e2e-isolation.md §9) =====

    // Суффикс — ТОЛЬКО имя ветки (одна ветка — один набор образов; git-инвариант
    // «одна ветка — один checkout»); git недоступен / HEAD detached — fallback
    // {хэш8} пути worktree (Shared.TestInfra.E2eImageTags). Хэш пути из суффикса
    // УДАЛЁН; механизм передачи тега раннеру (PGW_E2E_TAG) упраздняется вместе
    // с раннером — резолв всегда локальный.
    internal static readonly string ImageTagSuffix =
        E2eImageTags.SuffixFor(E2eFixture.FindRoot(AppContext.BaseDirectory));
```

и добавить в начало файла `using Shared.TestInfra;` (после существующих using).

**ПОДПУНКТ 4.2.** В `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs`:
(а) В `PrepareStaticAssetsAsync` заменить запись штампа

```csharp
            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            await File.WriteAllTextAsync(
                Path.Combine(root, "artifacts", "e2e", ".prepared"),
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    .ToString(CultureInfo.InvariantCulture), ct);
```

на

```csharp
            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            WriteStampAtomic(
                Path.Combine(root, "artifacts", "e2e", ".prepared"),
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
```

(переменную `root` ниже по методу, если используется ещё раз, не дублировать — вынести вычисление root до `EnsureStaticAsync(ct)` при необходимости; семантика метода не меняется).
(б) Рядом (в конце класса или после `PrepareStaticAssetsAsync`) добавить:

```csharp
    /// <summary>Атомарная запись штампа готовности: JSON {"stampMs"} через
    /// временный файл .prepared.tmp + File.Move(overwrite: true) — конкурентная
    /// подготовка не оставляет полузаписанный штамп.</summary>
    internal static void WriteStampAtomic(string stampPath, long stampMs)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(stampPath)!);
        var tmp = stampPath + ".tmp";
        File.WriteAllText(tmp, $"{{\"stampMs\":{stampMs.ToString(CultureInfo.InvariantCulture)}}}");
        File.Move(tmp, stampPath, overwrite: true);
    }
```

(в) В `VerifyStaticReadyOrThrowAsync` заменить блок разбора штампа

```csharp
        long stampMs;
        try
        {
            stampMs = long.Parse(File.ReadAllText(stampPath).Trim(), CultureInfo.InvariantCulture);
        }
        catch (FormatException e)
        {
            throw new ApplicationException($"the E2E preparation stamp is corrupted ({stampPath}). {PrepareHint}", e);
        }
```

на

```csharp
        long stampMs;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(stampPath));
            stampMs = doc.RootElement.GetProperty("stampMs").GetInt64();
        }
        catch (Exception e) when (e is System.Text.Json.JsonException
            or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new ApplicationException($"the E2E preparation stamp is corrupted ({stampPath}). {PrepareHint}", e);
        }
```

(строку `var stampUtc = DateTimeOffset.FromUnixTimeMilliseconds(stampMs).UtcDateTime;` и всё далее не менять; проверку `foreach (var image in new[] { WorkerImage, RunnerImage, NodeImage })` НЕ менять на этом шаге — раннер уйдёт в шаге 5).

**ПОДПУНКТ 4.3.** Создать `src/tests/PgWorker.E2eTests/E2e/E2ePrepareLock.cs`:

```csharp
using System.Text;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Файловый лок подготовки статики `artifacts/e2e/.prepare.lock`:
/// взаимное исключение ЗАПИСИ в publish-каталоги одного worktree (НЕ глобальный
/// лок запуска серий; тестируемые процессы лок не берут). Занят — поллинг
/// шагом 5 с, общий бюджет 10 минут; pid/started владельца пишется В ОТКРЫТЫЙ
/// лок-файл — через тот же удерживаемый FileStream, что держит лок (отдельное
/// открытие пути при удерживаемом FileShare.None конфликтует само с собой);
/// освобождение — Dispose потока-владельца (finally).</summary>
internal static class E2ePrepareLock
{
    public static readonly TimeSpan PollStep = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan TotalBudget = TimeSpan.FromMinutes(10);

    /// <summary>Захват лока. Занят (IOException от FileShare.None; .NET
    /// enforce'ит его на Unix через flock — конфликт действует и между
    /// потоками одного процесса) — ждёт до бюджета; истёкший бюджет —
    /// исходный IOException наружу (вызывающий оборачивает в
    /// ApplicationException; при живом держателе лок-файл нечитаем —
    /// FileShare.None — и сообщение деградирует до holder: &lt;unreadable&gt;
    /// (оговорка ниже, после PrepareStaticAssetsAsync).</summary>
    public static async Task<FileStream> AcquireAsync(string lockPath, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TotalBudget;
        while (true)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);
            FileStream? stream = null;
            try
            {
                stream = new FileStream(
                    lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                stream.SetLength(0);
                var payload = Encoding.UTF8.GetBytes(
                    $"pid={Environment.ProcessId} started={DateTime.UtcNow:O}");
                await stream.WriteAsync(payload, ct);
                await stream.FlushAsync(ct);
                return stream;
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                // Хэндл освобождаем ДО retry: «утёкший» stream продолжал бы
                // держать лок до конца процесса.
                if (stream is not null)
                    await stream.DisposeAsync();
                await Task.Delay(PollStep, ct);
            }
            catch
            {
                if (stream is not null)
                    await stream.DisposeAsync();
                throw;
            }
        }
    }
}
```

В `PrepareStaticAssetsAsync` обернуть тело (от `EnsureStaticAsync` до записи штампа включительно) захватом/освобождением лока:

```csharp
    public static async Task PrepareStaticAssetsAsync(CancellationToken ct)
    {
        Environment.SetEnvironmentVariable(PrepareModeEnv, "1");
        FileStream? prepareLock = null;
        try
        {
            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var lockPath = Path.Combine(root, "artifacts", "e2e", ".prepare.lock");
            try
            {
                prepareLock = await E2ePrepareLock.AcquireAsync(lockPath, ct);
            }
            catch (IOException e)
            {
                string holder;
                try { holder = File.ReadAllText(lockPath).Trim(); }
                catch { holder = "<unreadable>"; }
                throw new ApplicationException(
                    "another preparation holds the lock for over "
                    + $"{(int)E2ePrepareLock.TotalBudget.TotalMinutes} min (holder: {holder})", e);
            }

            await EnsureStaticAsync(ct);
            await EnsureOwnS3ImageAsync(ct);
            await EnsureSdImageAsync(ct);
            WriteStampAtomic(
                Path.Combine(root, "artifacts", "e2e", ".prepared"),
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            E2eFixture.StaticPhase("e2e-prepare: readiness stamp written — tests may run");
        }
        finally
        {
            prepareLock?.Dispose();
            Environment.SetEnvironmentVariable(PrepareModeEnv, null);
        }
    }
```

**Оговорка диагностики лока (не уходить от буквы spec §3.1).** При истёкшем
бюджете держатель удерживает `FileStream` с `FileShare.None`, поэтому
`File.ReadAllText(lockPath)` в catch бросает IOException — содержимое
лок-файла при живом держателе нечитаемо, и сообщение диагностики всегда
деградирует до `holder: <unreadable>`. Это ожидаемое поведение: spec-требование
«ApplicationException с содержимым лок-файла» покрыто fallback'ом
(`<unreadable>`); сателлит-файл с идентификацией держателя НЕ вводится
(pid/started владельца уже пишутся в сам удерживаемый лок-файл).

**ПОДПУНКТ 4.4.** В `src/tests/Shared.TestInfra/E2e/E2eFixture.cs` добавить `using Shared.TestInfra;` в начало файла (обязательно: `E2eFixture` остаётся в namespace `PgWorker.IntegrationTests.E2e` — namespaces переносимых файлов сохранены решением шага 2; без этого using обращение к `E2eImageTags` даёт CS0103). Затем добавить (после поля `TimingQueue` или рядом со `StaticPhase`) и переключить вызовы:

```csharp
    // ===== Per-process журналы статических фаз (docs/e2e-launch.md §0) =====

    // Суффикс {ветка|fallback}-{pid} вычисляется ОДИН раз на процесс; соседние
    // процессы (любые ветки) пишут в разные файлы.
    public static readonly string ProcessLogSuffix = ComputeProcessLogSuffix();

    public static string StaticPhaseLogPath
        => $"/tmp/pgw-e2e-static-phase-{ProcessLogSuffix}.log";

    public static string StaticProcessLogPath(string step)
        => $"/tmp/pgw-e2e-static-process-{ProcessLogSuffix}-{step}.log";

    private static string ComputeProcessLogSuffix()
    {
        string root;
        try { root = FindRoot(AppContext.BaseDirectory); }
        catch { root = AppContext.BaseDirectory; }
        var branch = E2eImageTags.BranchFor(root);
        if (branch.Length == 0)
            branch = "fallback";
        return $"{branch}-{Environment.ProcessId}";
    }
```

Заменить: (а) в `StaticPhase` — `File.AppendAllText("/tmp/pgw-e2e-static-phase.log", …)` → `File.AppendAllText(StaticPhaseLogPath, …)`; (б) в `PublishProjectAsync` — `logFile: $"/tmp/pgw-e2e-static-process-{tag}-publish.log"` → `logFile: StaticProcessLogPath($"{tag}-publish")`. В `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` заменить литеральные logFile: `"/tmp/pgw-e2e-static-process-worker-e2e.log"` → `E2eFixture.StaticProcessLogPath("worker-e2e")`; `"/tmp/pgw-e2e-static-process-node-wal-publish.log"` → `E2eFixture.StaticProcessLogPath("node-wal-publish")`; `"/tmp/pgw-e2e-static-process-node-tools-publish.log"` → `E2eFixture.StaticProcessLogPath("node-tools-publish")`; `"/tmp/pgw-e2e-static-process-node-e2e.log"` → `E2eFixture.StaticProcessLogPath("node-e2e")`; `"/tmp/pgw-e2e-static-process-sd-e2e-publish.log"` → `E2eFixture.StaticProcessLogPath("sd-e2e-publish")`; `"/tmp/pgw-e2e-static-process-sd-e2e-build.log"` → `E2eFixture.StaticProcessLogPath("sd-e2e-build")` (лог раннера `…runner-e2e.log` не трогать на этом шаге). В `src/tests/PgWorker.E2eTests/E2e/E2eParallelismGuard.cs` — `File.AppendAllText("/tmp/pgw-e2e-static-phase.log", …)` → `File.AppendAllText(E2eFixture.StaticPhaseLogPath, …)`.

**ПОДПУНКТ 4.5.** В `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` в `PrepareStaticAssetsAsync` сразу после строки `E2eFixture.StaticPhase("e2e-prepare: readiness stamp written — tests may run");` добавить:

```csharp
            await E2eTagCleanup.CleanupAsync(
                ["pgworker", "pgworker-node", "sdgenerator"],
                $"e2e-{ImageTagSuffix}",
                (file, args, token) => E2eFixture.RunProcessAsync(file, args, token),
                ct);
```

и добавить `using Shared.TestInfra;` (если ещё нет после подпункта 4.1).

**ПОДПУНКТ 4.6.** Создать `src/tests/PgWorker.E2eTests/E2e/E2eStaticInfraTests.cs` — ОДИН класс `public sealed class E2eStaticInfraTests` с фактами (AAA-комментарии в каждом; using `Shared.TestInfra`, `FluentAssertions`, `Xunit`):
1. `SanitizeBranch_LowercaseAndIllegalCharsReplaced` — `E2eImageTags.SanitizeBranch("Fix_T90/E2E Session")` → `"fix_t90-e2e-session"` (пробел и `/` заменены на `-`, lower-case).
2. `SanitizeBranch_LongBranchTruncatedTo32` — вход из 40 символов `'a'` → строка из 32 `'a'`.
3. `FallbackHash_EightHexOfSha256` — `E2eImageTags.FallbackHash("/tmp/x")` == первые 8 hex `SHA256.HashData(Encoding.UTF8.GetBytes("/tmp/x"))` (посчитать в тесте тем же кодом и сравнить).
4. `SuffixFor_NonGitDir_FallsBackToPathHash` — временный каталог вне git-репозитория (`Path.Combine(Path.GetTempPath(), $"pgw-tagtest-{Guid.NewGuid():N}")`, удалить в finally): `SuffixFor(dir)` == `FallbackHash(dir)`.
5. `StampAtomic_WriteAndOverwrite_ParseableJson` — во временном каталоге: `E2eEnvironment.WriteStampAtomic(path, 1700000000000)` → файл парсится `JsonDocument`, `stampMs` == 1700000000000; повторная запись с 1700000000001 перезаписывает (без `.tmp`-остатка: `File.Exists(path + ".tmp")` == false).
6. `StampAtomic_ConcurrentWrites_NoCorruption` — 5 параллельных `Task.Run(WriteStampAtomic(path, i))`; по завершении всех файл парсится и `stampMs` ∈ {0..4}.
7. `CleanupSelector_SelectsOnlyStaleOwnE2eTags` — вход (строки через `\n`): `pgworker:e2e-main|2020-01-01 00:00:00 +0000 UTC` (stale), `pgworker:e2e-main|2030-01-01 23:30:00 +0000 UTC` (свежая: `nowUtc` минус 30 минут), `other:e2e-old|2020-01-01 00:00:00 +0000 UTC`, `pgworker:dev|2020-01-01 00:00:00 +0000 UTC`, `pgworker:e2e-main|не-дата`; вызов `E2eTagCleanup.SelectStaleE2eTags(output, ["pgworker"], "e2e-current", TimeSpan.FromHours(24), new DateTime(2030, 1, 2, 0, 0, 0, DateTimeKind.Utc))` → ровно один элемент `pgworker:e2e-main`. Комментарий теста: currentTag `"e2e-current"` во входе НЕ встречается — свежая строка отсекается возрастом (а не тождеством тега), stale-строка попадает в отбор (AAA).
8. `CleanupSelector_ExcludesCurrentAndNeverTags` — тот же вход плюс `pgworker:e2e-main2|2020-01-01 …`; вызов с `currentTag = "e2e-main2"` → НЕ содержит `pgworker:e2e-main2`; `pgworker:dev` отсутствует в результате всегда.
9. `PrepareLock_SecondAcquirerWaitsForHolder` — временный lockPath; задача A: `var f = await E2ePrepareLock.AcquireAsync(path, ct);` держит 3 с (`await Task.Delay(3000)`), затем `f.Dispose()`; задача B (стартует через 500 мс): замерить `Stopwatch` вокруг `AcquireAsync`; Assert: B получил лок, ожидание ≥ 2500 мс, содержимое файла начинается с `"pid="`. Токены — `CancellationTokenSource` с отменой в finally. Комментарий над фактом (AAA): same-process-ожидание детерминировано — .NET enforce'ит `FileShare.None` на Unix через flock (конфликт действует и между потоками одного процесса); кросс-процессную сериализацию ДВУХ процессов `PgWorker.E2ePrepare` (spec §6.8) проверяет пункт 4 «Проверки» шага 4.

Проверка подпункта: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~E2eStaticInfraTests"` — 9 фактов Passed (после `dotnet build`; docker не нужен).

### ШАГ 5. PgWorker E2E: хостовая модель «класс = своё окружение» (упразднение двухфазного контура)

**Вход:** Шаг 4 выполнен; кейс-маркер до правок зелёный на текущей механике; серия PgWorker E2E зелёная.
**Действие (делает кодер САМ — семантика контуров, миграция 39 файлов):**

- [ ] **Упразднение раннер-механики.** Удалить файл `src/tests/PgWorker.E2eTests/E2e/E2eContour.cs`; удалить из `E2eEnvironment.cs`: свойство `RunnerImage`, `TestsPublishDir`, блок publish тестовой сборки + копирование `xunit.runner.json` + docker build раннера в `EnsureStaticAsync`, early-return `if (IsInnerRunner)` в `EnsureStaticAsync`/`EnsureOwnS3ImageAsync`, `AttachFromEnv`, `IsInnerRunner`, `RunnerEnv`, `BuildRunnerEnv`, статические `HostSnapsDirStatic`/`HostArtifactsDirStatic`, inner-ветку `SnapshotsHostDir`, метод `GetRequiredEnv`; удалить файл `docker/E2eRunner.Dockerfile`. В списке образов `VerifyStaticReadyOrThrowAsync` оставить `new[] { WorkerImage, NodeImage }`.
- [ ] **Host-only `StartAsync`.** Точка входа: `public static Task<E2eEnvironment> StartAsync(string slug, bool withS3Node = false, bool haEtcd = false, CancellationToken ct = default)` — всегда `StartOnceAsync(slug, withS3Node, haEtcd, ct, createNet: true)`; ветки `IsInnerRunner`/оркестрации удалить; `StartOnceCoreAsync` — параметр `createNet` убрать (всегда true): артефакты — temp хоста, CA генерируется здесь, окно портов — локальный счётчик, сеть `pgw-en-{runId}` создаётся здесь и живёт до `DisposeAsync` окружения.
- [ ] **Динамические публикации + адресация.**
  - etcd-узлы: `docker run -d --name pgw-ee{i}-{runId} --network {netName} --network-alias {alias} --add-host=host.docker.internal:host-gateway -p 127.0.0.1::2379 …`; после run фактический порт: `docker port {name} 2379/tcp` → парс `127.0.0.1:{p}`; `--advertise-client-urls=http://host.docker.internal:{p}`; peers (`2380`, initial-cluster по алиасам) — без изменений.
  - Свойства: `EtcdEndpoint` = `http://localhost:{p1}` (тестовый Gateway); `EtcdEndpoints` = список `http://host.docker.internal:{pi}` (env воркера; в `StartHostAsync` пары `Endpoints__i`/`AdvertisedEndpoints__i` — из него).
  - ownS3-нода: `-p 127.0.0.1::9000` (+ `--add-host` не нужен — сервер); фактический порт через `docker port`; `S3Endpoint` = `http://localhost:{p}` (тестовые S3-клиенты); `SeedS3PoolAsync` — список endpoints/alloc в форме `host.docker.internal:{p}`; `StartExtraS3NodeAsync` — с `-p`, возвращает `http://host.docker.internal:{port}`.
  - mc-контейнер (`pgw-em-{runId}`) и `EnsureOwnS3BucketAsync` — без изменений (внутрисетевой алиас).
  - Воркер (`StartHostAsync`/`StartWorkerWithEnvAsync`): `-p 127.0.0.1::8443`; фактический порт через `docker port`; серверный серт — SAN `[alias, "localhost"]` (второй элемент покрывает хост-адрес, spec §3.8); `HostInstance` получает хост-порт: `ApiUrl` = `https://localhost:{p}`; `AdvertiseUrl` воркера остаётся `https://{alias}:8443` (внутрисетевые потребители — spec: меняется только путь ТЕСТА); `hostSnapsDir` = `Path.Combine(ArtifactsDir, "snapshots")` (env `PGW_E2E_HOST_SNAPS_DIR` удалён); `HostInstance.SnapshotsDir` = абсолютный хост-путь `Path.Combine(hostSnapsDir, Name)` (сценарии читают snapshot-файлы напрямую).
- [ ] **Teardown сети.** `E2eEnvironment.DisposeAsync` в конце (после удаления контейнеров/томов/своих per-cluster сетей, до ассерта чистоты) — удалить сеть окружения: `await DeleteClassNetworkAsync(NetName, CancellationToken.None);` (статический хелпер с ForceDisconnect/ретраями сохраняется; `CreateClassNetworkAsync` и record `ClassNetwork` удалить). В README-cleanup упавшего прогона сохранить строку `docker network rm {NetName}`.
- [ ] **Класс-фикстура.** Создать `src/tests/PgWorker.E2eTests/E2e/E2eContourFixture.cs`:

```csharp
namespace PgWorker.IntegrationTests.E2e;

/// <summary>База класс-фикстур PgWorker E2E (docs/e2e-isolation.md §1): слот
/// E2eParallelismGuard ДО первого docker-объекта, затем полный контур класса
/// (сеть/etcd/ownS3/CA/окно портов — E2eEnvironment.StartAsync); teardown
/// при любом исходе в DisposeAsync. Единица окружения — тестовый класс.</summary>
public abstract class E2eContourFixture : IAsyncLifetime
{
    protected abstract string Slug { get; }
    protected virtual bool WithS3Node => false;
    protected virtual bool HaEtcd => false;

    public E2eEnvironment Fx { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        E2eParallelismGuard.Acquire(Slug, ct);
        try
        {
            Fx = await E2eEnvironment.StartAsync(Slug, WithS3Node, HaEtcd, ct);
        }
        catch
        {
            E2eParallelismGuard.Release(Slug);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Fx is not null)
            await Fx.DisposeAsync();
        E2eParallelismGuard.Release(Slug);
    }

    /// <summary>Пометка упавшего сценария (stop-режим teardown'а).</summary>
    public void MarkFailed() => Fx?.MarkFailed();
}
```

- [ ] **Миграция сценариев (39 файлов, кроме E2eContourFixture и инфраструктуры).** Для КАЖДОГО сценарного класса механический рецепт:
  1. Создать компаньон-фикстуру в том же файле: `private sealed class {Class}Fx : E2eContourFixture { protected override string Slug => "{slug первого удалённого StartAsync}"; protected override bool WithS3Node => {литерал из удалённого вызова}; }` (для классов с `haEtcd: true` — переопределить `HaEtcd`).
  2. Класс: `public sealed class {Class}({Class}Fx Ctx) : IClassFixture<{Class}Fx>`; добавить свойство `private E2eEnvironment Fx => Ctx.Fx;` (в абстрактных базах с наследниками — фиксура у каждого concrete-класса со СВОИМ slug).
  3. Тело каждого факта: удалить строки `await using var fx = await E2eEnvironment.StartAsync(…);` и `Fx = fx;` (или эквивалент через локальный хелпер — факт без собственного подъёма делит контур класса); остальное сохранить, включая `try/catch { Fx.MarkFailed(); throw; }` и `await using` воркеров.
  4. Мультифактные классы (E2eBackupJobsScenarios, E2eBackupVerifyScenarios, E2eDrillScenarios, E2eEtcdSnapshotExportScenarios, E2eMoveScenarios, E2ePgtuneScenarios, E2eRestoreScenarios, E2eScaleScenarios, E2eStrictScenarios, E2eWalStreamScenarios, E2eWalStreamTliScenarios, E2eWorkerCertScenarios, E2eBackupPoolScenarios+8 наследников): факты делят контур класса; проверить, что имена кластеров/пулов фактов различны (литеральные префиксы вида `sshop{tag}`/`stshop{tag}`) либо факт пересеивает свои ключи в начале; при совпадающих выражениях имён — добавить факту литеральный суффикс к имени кластера; при семантической несовместимости фактов в одном контуре — разделить класс: каждому факту свой concrete-класс со своей компаньон-фикстурой и СВОИМ slug (в том же файле; правило spec §3.8 «класс делится на два класса»). Абстрактная база E2eBackupPoolScenarios: фиксура у каждого из 8 наследников со своим slug (`pool-ac1`…`pool-ac8`).
  5. `DockerTrait.SkipIfUnavailable()` в фактах уже удалён (шаг 3).
- [ ] **TliSwitch.** `TliSwitchContext` — убрать `OrchestrateClassAsync`/`_hostDelegated`/`_hostError`/`SkipIfHostDelegated`/поле `LatestRestoreSucceeded` и его присвоение (Assert 5 факта 1) и гейт `if (!Ctx.LatestRestoreSucceeded) Assert.Skip(…)` (С5); `InitializeAsync` = `E2eParallelismGuard.Acquire` + `StartAsync("tlsw", withS3Node: true)` + `PrepareAsync` (try/catch: `Fx?.MarkFailed()` + Release Guard'а + rethrow); `DisposeAsync` = `Fx.DisposeAsync()` + Release; `[TestCaseOrderer]` остаётся; `SkipIfPrepareSkipped` (С4) остаётся без изменений; факты — убрать вызовы `Ctx.SkipIfHostDelegated()`.
- [ ] **Guard.** `E2eParallelismGuard` не меняется (потолок 3, env `PGW_E2E_MAX_CONTOURS`); захват слота — теперь в `E2eContourFixture.InitializeAsync` ДО `StartAsync` (первого docker-объекта), освобождение — в `DisposeAsync`/провале подъёма.
- [ ] Сохранить без изменений: телеметрию teardown ДО удалений (`/tmp/pgw-e2e-artifacts-{guid}/`), `MarkFailed` stop-режим + `README-cleanup.txt`, own-only чистку по runId/ClusterTag, АССЕРТ ЧИСТОТЫ, wait-стратегии (≤ 100 с), `[PHASE]`/`[PHASE-TICK]`, `E2ePhase.WaitAsync`, fail-fast «provision-stuck», `E2eFixture`-хелперы, `E2eProcessMergeTests`.

**Выход:** PgWorker E2E исполняется на хосте: `E2eContour`/`pgw-e2e-runner`/`E2eRunner.Dockerfile`/`PGW_E2E_INNER`/`PGW_E2E_TAG`/`PGW_E2E_NET`/`PGW_E2E_HOST_*` отсутствуют; каждый E2E-класс — класс-фикстура с полным подъёмом/удалением контура; тест ходит по `localhost:{динамический порт}`; кейс-маркер зелёный.
**Проверка:** (1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 ошибок; (2) `grep -rn "E2eContour\|pgw-e2e-runner\|PGW_E2E_INNER\|PGW_E2E_TAG\|PGW_E2E_NET\|PGW_E2E_HOST_\|E2eRunner\|PGW_TEST_DOCKER\|DockerTrait" src/ docker/` — пусто (включая полный `grep -rn "PGW_TEST_DOCKER" src/` — spec §6.2: три остатка из шага 3 удалены вместе с раннер-механикой); (3) подготовка `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` (фон, лог в файл); (4) кейс-маркер `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` — Passed, `Skipped: 0`; (5) в журнале прогона — `[PHASE] e2e-class…`/`[PHASE] e2e-env…` с Guard-порядком (слот до сети); после прогона `docker ps -a` не содержит guid прогона; зачистка выполнена. Полный прогон серии — в шаге 10.
**Связь со spec:** §3.8, §3.9, §6.4-6.5, §6.9, §6.14, §6.16.

### ШАГ 6. KafkaWorker E2E: per-branch тег, сборка once-per-process, уборка, retry-маска портов

**Вход:** Шаги 3–5 выполнены; `KafkaWorker.E2eTests` существует; серия зелёная на фиксированном теге.
**Действие (кодер + подпункты):**

- [ ] **6.1 (ПОДПУНКТ)** Per-branch тег + `EnsureWorkerImageAsync` (once-per-process) + NOBUILD fail-fast + уборка.
- [ ] **6.2 (ПОДПУНКТ)** Retry-маска старта + юнит-тесты.
- [ ] Ревью (кодер): запуск полной серии проекта.

**Выход:** `kafkaworker:e2e-{ветка}`; один build на процесс в журнале; фиксированный `kafkaworker:e2e` не используется; `already allocated`/`address already in use` — в ретрай.
**Проверка:** build; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~RetryMask"` — Passed; полная серия `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.E2eTests -c Release --no-build` — `Failed: 0, Skipped: 0`; в журнале ровно один `[PHASE] build kafkaworker:e2e-…` на процесс; `docker images | grep kafkaworker` — тег с веткой; зачистка после серии.
**Связь со spec:** §3.4, §3.6, §6.11, §6.13.

**ПОДПУНКТ 6.1.** В `src/tests/KafkaWorker.E2eTests/E2e/KafkaE2eEnvironment.cs`:
(а) заменить `private const string WorkerImage = "kafkaworker:e2e";` на

```csharp
    // Per-branch тег (docs/e2e-isolation.md §9): одна ветка — один набор образов.
    private static readonly string WorkerImageTag =
        $"e2e-{E2eImageTags.SuffixFor(FindRepoRoot())}";
    private static readonly string WorkerImage = $"kafkaworker:{WorkerImageTag}";
```

добавить `using Shared.TestInfra;`.
(б) Заменить вызов `await BuildImageAsync(runId, artifactsDir);` в `StartAsync` на `await EnsureWorkerImageAsync(runId, artifactsDir);` и заменить метод `BuildImageAsync` целиком на:

```csharp
    private static readonly SemaphoreSlim BuildGate = new(1, 1);
    private static bool _imageReady;

    /// <summary>Сборка per-branch образа ONCE-per-process (docs/e2e-isolation.md
    /// §9): параллельные классы ждут SemaphoreSlim-гейт; повторным StartAsync
    /// образ не пересобирается. NOBUILD (бисект) — сборка скипается, требуется
    /// существующий тег (отсутствие — fail-fast с подсказкой). После успешного
    /// build — авто-уборка тегов репозитория старше 24 ч (текущий исключён).</summary>
    private static async Task EnsureWorkerImageAsync(string runId, string artifactsDir)
    {
        if (_imageReady)
            return;
        await BuildGate.WaitAsync(CancellationToken.None);
        try
        {
            if (_imageReady)
                return;
            if (Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            {
                if (!await ImageExistsAsync(WorkerImage))
                    throw new ApplicationException(
                        $"image {WorkerImage} is missing — run without PGW_TEST_E2E_NOBUILD "
                        + $"(or docker build -f docker/KafkaWorker.Dockerfile -t {WorkerImage} .)");
            }
            else
            {
                var root = FindRepoRoot();
                Console.WriteLine($"[PHASE] build {WorkerImage}: start (prod Dockerfile, budget 120 s)…");
                var sw = Stopwatch.StartNew();
                await RunProcessAsync("docker",
                    ["build", "-f", "docker/KafkaWorker.Dockerfile", "-t", WorkerImage, "."],
                    CancellationToken.None, TimeSpan.FromSeconds(120),
                    workingDirectory: root,
                    logFile: Path.Combine(artifactsDir, $"process-build-{runId[..8]}.log"));
                Console.WriteLine($"[PHASE] build {WorkerImage}: {sw.Elapsed.TotalSeconds:F0} s");
                await E2eTagCleanup.CleanupAsync(
                    ["kafkaworker"], WorkerImageTag,
                    (file, args, token) => RunProcessAsync(file, args, token, TimeSpan.FromMinutes(2)),
                    CancellationToken.None);
            }
            _imageReady = true;
        }
        finally
        {
            BuildGate.Release();
        }
    }

    private static async Task<bool> ImageExistsAsync(string image)
    {
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add("image");
        psi.ArgumentList.Add("inspect");
        psi.ArgumentList.Add(image);
        using var process = new Process { StartInfo = psi };
        process.Start();
        await process.WaitForExitAsync();
        return process.ExitCode == 0;
    }
```

**ПОДПУНКТ 6.2.** В том же файле добавить:

```csharp
    /// <summary>Ретрай-маска старта окружения: гонки сети И занятость
    /// зондируемого хост-порта — docker «…port is already allocated», proxy
    /// bind «address already in use». Прочие причины не ретраятся (3 попытки,
    /// новые FreePort при каждой — бюджет без изменений).</summary>
    internal static bool IsRetryableStartException(Exception ex)
        => ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("already allocated", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("address already in use", StringComparison.OrdinalIgnoreCase);
```

и заменить фильтр retry в `StartAsync`: `catch (Exception ex) when (attempt < 3 && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))` → `catch (Exception ex) when (attempt < 3 && IsRetryableStartException(ex))`. Создать `src/tests/KafkaWorker.E2eTests/E2e/KafkaRetryMaskTests.cs` — класс `KafkaRetryMaskTests`, факты (AAA): `AlreadyAllocated_IsRetryable` (`new ApplicationException("failed: port is already allocated")` → true), `AddressInUse_IsRetryable` (`"bind: address already in use"` → true), `NetworkRace_IsRetryable` (`"network not found"` → true), `OtherFailure_NotRetryable` (`"image missing"` → false).

### ШАГ 7. ValkeyWorker E2E: per-branch тег, once-per-process сборка, NOBUILD fail-fast, уборка, retry-маска

**Вход:** Шаг 6 выполнен (образец механики); `ValkeyWorker.E2eTests` существует.
**Действие (кодер + подпункты):**

- [ ] **7.1 (ПОДПУНКТ)** Тег/гейт/NOBUILD/уборка — по образцу шага 6 для `valkeyworker` (Dockerfile `docker/ValkeyWorker.Dockerfile`; метод Valkey `BuildImageAsync` не пишет `[PHASE]` — [PHASE]-метки НЕ добавлять, способ сборки не меняется; logFile у Valkey-сборки нет — сохранить; NOBUILD — добавить явную проверку существования тега с подсказкой `docker build -f docker/ValkeyWorker.Dockerfile -t {WorkerImage} .`; уборка `E2eTagCleanup.CleanupAsync(["valkeyworker"], WorkerImageTag, …)` после успешного build — вызов RunProcessAsync обёртки Valkey: `(file, args, token) => RunProcessAsync(file, args, token)` в сигнатуре окружения — у ValkeyRunProcessAsync сигнатура `(string file, string[] args, CancellationToken ct, TimeSpan timeout)` — адаптировать лямбдой с `TimeSpan.FromMinutes(2)`).
- [ ] **7.2 (ПОДПУНКТ)** Retry-маска `IsRetryableStartException` (тот же код, что в 6.2) в `ValkeyE2eEnvironment.cs` + замена фильтров `when` в ОБОИХ методах старта (StartAsync и StartTwoAsync — в StartTwoAsync фильтр такой же `ex.Message.Contains("network")`, если присутствует; grep `Contains("network"` покажет все) + `src/tests/ValkeyWorker.E2eTests/E2e/ValkeyRetryMaskTests.cs` с 4 фактами (те же имена, что в 6.2).
- [ ] Ревью (кодер): полная серия проекта зелёная.

**Выход/Проверка/Связь со spec:** как в шаге 6, для `valkeyworker` и проекта `src/tests/ValkeyWorker.E2eTests`; `docker images | grep valkeyworker` — тег с веткой; фиксированный `valkeyworker:e2e` не используется. Связь: §3.4, §3.6, §6.13.

### ШАГ 8. S3Worker E2E: per-branch тег, per-process publish-каталог, once-per-process сборка, уборка, retry-маска

**Вход:** Шаги 6–7 выполнены; `S3Worker.E2eTests` существует.
**Действие (кодер + подпункты):**

- [ ] **8.1 (ПОДПУНКТ)** Тег + per-process publish-каталог + once-гейт + NOBUILD + уборка в `S3E2eEnvironment.cs`.
- [ ] **8.2 (ПОДПУНКТ)** Retry-маска + тесты.
- [ ] Ревью (кодер): полная серия.

**Выход/Проверка/Связь со spec:** как в шаге 6 для `s3worker` и `src/tests/S3Worker.E2eTests`; publish-каталог — абсолютный `{root}/artifacts/e2e/s3worker-{pid}-{s6}` (никаких конкурентных `-o` в общий каталог); `s3worker:e2e-{runId}` и фоллбек `s3worker:e2e` не используются. Связь: §3.5, §3.6, §6.13.

**ПОДПУНКТ 8.1.** В `src/tests/S3Worker.E2eTests/E2e/S3E2eEnvironment.cs`:
(а) удалить константы `private const string S3WPublishDir = "artifacts/e2e/s3worker";` и `private const string WorkerImageFallback = "s3worker:e2e";`; добавить `using Shared.TestInfra;` и поля:

```csharp
    // Per-branch тег (docs/e2e-isolation.md §9); per-run тег e2e-{runId} и
    // NOBUILD-фоллбек s3worker:e2e упразднены.
    private static readonly string WorkerImageTag =
        $"e2e-{E2eImageTags.SuffixFor(FindRepoRoot())}";
    private static readonly string WorkerImage = $"s3worker:{WorkerImageTag}";

    // Per-process publish-каталог записи: абсолютный
    // {root}/artifacts/e2e/s3worker-{pid}-{s6}; s6 — 6 hex криптослучайных
    // (защита от переиспользования pid). Конкурентные процессы одной ветки
    // пишут в разные каталоги и собирают один тег идентичного контента.
    private static string NewPublishDir()
    {
        var s6 = Convert.ToHexString(
            System.Security.Cryptography.RandomNumberGenerator.GetBytes(3)).ToLowerInvariant();
        return Path.Combine(FindRepoRoot(), "artifacts", "e2e", $"s3worker-{Environment.ProcessId}-{s6}");
    }

    private static readonly SemaphoreSlim BuildGate = new(1, 1);
    private static bool _imageReady;
```

(б) заменить `BuildImagesAsync` целиком:

```csharp
    /// <summary>Сборка образа ONCE-per-process (docs/e2e-isolation.md §9):
    /// owns3:dev — внешний ассет (fail-fast с командой ../OwnS3);
    /// s3worker — publish в СВОЙ per-process каталог + docker build per-branch
    /// тега; NOBUILD — существующий тег обязателен (fail-fast с подсказкой);
    /// после успешного build — авто-уборка тегов репозитория старше 24 ч.</summary>
    private static async Task<string> BuildImagesAsync()
    {
        await EnsureImageAsync(OwnS3Image, "bash ../OwnS3/deploy/build-image.sh");
        if (!_imageReady)
        {
            await BuildGate.WaitAsync(CancellationToken.None);
            try
            {
                if (!_imageReady)
                {
                    if (Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
                    {
                        await EnsureImageAsync(WorkerImage,
                            $"dotnet publish src/S3Worker.App/S3Worker.App.csproj -c Release -o artifacts/e2e/s3worker && "
                            + $"docker build -f docker/S3Worker.Dockerfile -t {WorkerImage} artifacts/e2e/s3worker");
                    }
                    else
                    {
                        var root = FindRepoRoot();
                        var publishDir = NewPublishDir();
                        Directory.CreateDirectory(publishDir);
                        var ct = CancellationToken.None;
                        var t0 = Stopwatch.GetTimestamp();
                        Console.WriteLine($">>> [PHASE] e2e publish S3Worker.App ({DateTime.Now:HH:mm:ss})");
                        await RunHostAsync(root, "dotnet",
                            ["publish", "src/S3Worker.App/S3Worker.App.csproj", "-c", "Release", "-o", publishDir, "--nologo"], ct);
                        await RunHostAsync(root, "docker",
                            ["build", "-q", "-f", "docker/S3Worker.Dockerfile", "-t", WorkerImage, publishDir], ct);
                        Console.WriteLine($">>> [PHASE] e2e {WorkerImage} ready ({Elapsed(t0)})");
                        await E2eTagCleanup.CleanupAsync(
                            ["s3worker"], WorkerImageTag,
                            (file, args, token) => RunProcessAsync(file, args, token, TimeSpan.FromMinutes(2)),
                            ct);
                    }
                    _imageReady = true;
                }
            }
            finally
            {
                BuildGate.Release();
            }
        }
        return WorkerImage;
    }
```

(в) в `StartAsync` и `StartTwoAsync` заменить `var workerImage = await BuildImagesAsync(runId);` / `await BuildImagesAsync(runId);` на `var workerImage = await BuildImagesAsync();` (без параметров) и использования `workerImage` не менять.
**ПОДПУНКТ 8.2.** Добавить `internal static bool IsRetryableStartException(Exception ex)` (код из 6.2) и заменить ОБА retry-фильтра (`StartAsync` и `StartTwoAsync`): `when (attempt < 3 && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))` → `when (attempt < 3 && IsRetryableStartException(ex))`. Создать `src/tests/S3Worker.E2eTests/E2e/S3RetryMaskTests.cs` с 4 фактами (как в 6.2).

### ШАГ 9. Раскип BackupS3Tests (С6): скип устарел — OwnS3 уже валидирует чексумму

**Вход:** Шаги 3–8 выполнены; на docker-хосте образ `owns3:dev` актуального main (`../OwnS3`); задача t91 НИГДЕ не заводится.
**Действие:**

- [ ] **9.1 (ПОДПУНКТ)** Точная правка `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs`.
- [ ] Ревью (кодер): тест зелёный в прогоне интеграционного проекта; `grep -rn "t91-owns3-checksum-verify" arch/ src/ docs/` — пусто; `grep -rn "t91" arch/roadmap/` — пусто (пункт не заводился).

**Выход:** `PutObjectBrokenShaRejectedByServer` исполняется и зелёный (сервер возвращает 400 BadDigest при несовпадении `x-amz-checksum-sha256`; ассерт `put.IsSuccess == false`).
**Проверка:** build; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~PutObjectBrokenShaRejectedByServer"` — Passed, `Skipped: 0`; grep-чеклисты выше — пусто. (Устаревший локальный `owns3:dev` → тест честно красный; лечение — пересборка образа `bash ../OwnS3/deploy/build-image.sh`, НЕ возврат скипа.)
**Связь со spec:** §3.11, §7.21, §6.15.

**ПОДПУНКТ 9.1.** В файле `/Users/demakaev/ZCodeProject/worktrees/fix-t90-e2e-session-isolation/src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs` заменить блок (строки ~488-493):

```csharp
    // AAA (t08): несовпадающий sha256 — сервер отвергает put (BadDigest) → Failed.
    // SKIP: продуктовый пробел ownS3 ВНЕ зоны t36 — сервер не верифицирует
    // x-amz-checksum-sha256 (пут проходит, BadDigest не возвращается).
    // Возврат теста в строй — задача t91-owns3-checksum-verify
    // (arch/roadmap/owns3.md).
    [Fact(Skip = "t91-owns3-checksum-verify (arch/roadmap/owns3.md): the ownS3 server does not verify x-amz-checksum-sha256 (BadDigest) — a put with a wrong checksum passes")]
```

на

```csharp
    // AAA (t08, раскип t90): несовпадающий sha256 — сервер OwnS3 отвергает put
    // (400 BadDigest — серверная валидация x-amz-checksum-sha256 на PutObject)
    // → Failed.
    [Fact]
```

Остальное тело факта не менять.

### ШАГ 10. Гейт-прогоны и чек-листы приёмки

**Вход:** Шаги 1–9 выполнены; docker-хост свободен от чужих серий.
**Действие (делает кодер сам; каждая серия — фон, лог в файл, наблюдение по правилам AGENTS.base.md §13; после КАЖДОЙ docker-серии — зачистка и проверка `docker ps -a` пуст от guid'ов серий, `docker network ls | grep -c kfw-net` ≈ 0, при остатках — `docker network prune -f`):**

- [ ] Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` (отдельная команда; вывод — часть гейта).
- [ ] Подготовка: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`; проверить `artifacts/e2e/.prepared` (JSON `stampMs`) и `docker images` (`pgworker`/`pgworker-node`/`sdgenerator` с `e2e-{ветка}`; повторная подготовка — тот же тег; подготовка из другого worktree другой ветки — другой тег).
- [ ] Юнит-серии: `dotnet test src/tests/PgWorker.UnitTests`, `KafkaWorker.UnitTests`, `ValkeyWorker.UnitTests`, `S3Worker.UnitTests`, `Shared.Core.UnitTests`, `Shared.Etcd.UnitTests`, `Shared.Docker.UnitTests`, `Shared.Metrics.UnitTests`, `Metrics.SdGenerator.UnitTests`, `AdminPanel.UnitTests`, `PgWorker.NodeTools.UnitTests` — зелёные; юнит-факты `E2eStaticInfraTests` + три `*RetryMaskTests` — Passed.
- [ ] Интеграционные серии (docker): `PgWorker.IntegrationTests`, `KafkaWorker.IntegrationTests`, `ValkeyWorker.IntegrationTests`, `S3Worker.IntegrationTests`, `AdminPanel.IntegrationTests`, `Metrics.SdGenerator.IntegrationTests` — `Skipped: 0` в каждой.
- [ ] E2E-серии: `PgWorker.E2eTests` (после E2ePrepare), `KafkaWorker.E2eTests`, `ValkeyWorker.E2eTests`, `S3Worker.E2eTests` — `Failed: 0, Skipped: 0`; PgWorker-классы параллельны при ≤ 3 живых контурах (Guard-слот до сети — `[PHASE]`-порядок); параллельные процессы пишут в разные `/tmp/pgw-e2e-static-*-{ветка}-{pid}*.log`.
- [ ] Кейс-маркер мерж-гейта: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` — зелёный.
- [ ] NOBUILD-прогон (критерий spec §6.13): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_E2E_NOBUILD=1 dotnet test src/tests/KafkaWorker.E2eTests -c Release --no-build` — `Failed: 0, Skipped: 0`; в журнале прогона НЕТ `[PHASE] build kafkaworker:e2e-` — сборка скипнута, использован существующий per-branch тег, оставшийся от полной Kafka-серии выше; после прогона — зачистка.
- [ ] Авто-уборка по возрасту (критерий spec §6.12): `docker images --format "{{.Repository}}:{{.Tag}}|{{.CreatedAt}}"` — среди репозиториев `pgworker`, `pgworker-node`, `sdgenerator`, `kafkaworker`, `valkeyworker`, `s3worker` нет тегов `e2e-*` старше 24 ч (тег не удалён только при отказе `rmi` «image is being used by running container» — живой контейнер); NEVER-список не тронут: `owns3:dev`, `pgworker:dev`, `adminpanel:dev`, `minio/mc:*`, `quay.io/coreos/etcd:*` и прочие теги без префикса `e2e-` на месте.
- [ ] Чек-листы grep (от корня worktree): `grep -rn "PGW_TEST_DOCKER" src/` — пусто; `grep -rn "PGW_TEST_DOCKER" docs/e2e-isolation.md docs/e2e-launch.md docs/runbook.md AGENTS.md` — пусто (канон-файлы; каталог `docs/` целиком НЕ сканировать — `docs/superpowers/` хранит историю задач, spec §8); `grep -rn "Assert.skip\|Skip = \|Assert.Skip" src/tests` — только `_prepareSkip`-вызовы и его `Assert.Skip(_prepareSkip)` в E2eTliSwitchScenarios; `grep -rn "E2eContour\|pgw-e2e-runner\|PGW_E2E_INNER\|PGW_E2E_TAG\|PGW_E2E_NET\|PGW_E2E_HOST_\|E2eRunner" src/ docker/` — пусто (spec §3.8 упраздняет и `PGW_E2E_NET`/`PGW_E2E_HOST_*`; проверка шага 5 использует этот же список, дополненный `PGW_TEST_DOCKER\|DockerTrait`, — первый grep этой строки уже покрывает их по src/); `grep -rn "t91-owns3-checksum-verify" arch/ src/ docs/` — пусто; `grep -rln "kafkaworker:e2e\"\|valkeyworker:e2e\"\|s3worker:e2e\"" src/tests` — пусто (фиксированные теги ушли); `ls src/tests/PgWorker.E2eTests/xunit.runner.json src/tests/KafkaWorker.E2eTests/xunit.runner.json src/tests/ValkeyWorker.E2eTests/xunit.runner.json src/tests/S3Worker.E2eTests/xunit.runner.json` — все с `maxParallelThreads: 5`.
- [ ] Упавшие вне области задачи тесты — в `arch/roadmap/bugs.md` (пункт + папка логов), не чинить в этом проходе.
- [ ] Мерж-гейт (выполняется при закрытии задачи, Фаза 8): удалить пункт `t90-e2e-session-isolation` из `arch/roadmap/reliability.md` (строка ~114) ТЕМ ЖЕ коммитом, что закрывает задачу; в `arch/roadmap/reliability-report.md` t90 не упоминается; правки канонов (шаг 1) — частью того же мерж-коммита.

**Выход:** все критерии §6 spec выполнены; серия за серией пройдена с нулём скипов и с зачисткой.
**Проверка:** сводный отчёт по каждой серии (`Passed/Failed/Skipped` из финальных строк логов) + выводы всех grep-чеклистов.
**Связь со spec:** §4 (фаза 8), §6 (все критерии), §8 (закрытие задачи).
