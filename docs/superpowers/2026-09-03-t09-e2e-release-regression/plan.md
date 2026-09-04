# t09-e2e-release-regression — план исполнения

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** найти бисектом коммит-виновник регрессии 4 E2E-кейсов PgWorker, починить дефект (логика или тест — по контракту) и исключить саму маскировку: E2eFixture всегда гоняет свежий Release.

**Architecture:** сначала инструмент — автосборка Release в E2eFixture (она же закрывает требование «фаза Г» спеки); затем репро на HEAD, верификация good-границы (классификация исходов по содержимому лога: тело → цвет; красный перепроверяется повтором и фильтром сигнатур контура) и `git bisect` в отдельном временном worktree (main не трогаем) с раннером «build → прогон кейса-маркера» и трёхзначной классификацией (good / bad / skip); фикс по критерию контракта arch/14 — сценарий А (arch-first), Б (фикс теста) или смешанный (оба, по спеке фаза В); регресс-правило в AGENTS.md; финальная полная верификация.

**Tech Stack:** .NET 10 (`src/PgWorker.slnx`, `TreatWarningsAsErrors`), xUnit v3 (VSTest-раннер), Testcontainers, docker CLI, git bisect/worktree.

**Spec:** `docs/superpowers/2026-09-03-t09-e2e-release-regression/spec.md` — план аргументируется от спеки; исполнитель читает обе.

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — новый код не даёт warn.
- Порты docker-контейнеров в тестах динамические (`FreePort()`), никаких литералов вида `:16000`; таймауты короткие.
- Docker-E2E гонится только с `PGW_TEST_DOCKER=1` (гейт `DockerTrait.SkipIfUnavailable`).
- Бисект — во временном worktree (`/tmp/pgw-bisect*`), main и рабочая ветка не переключаются; сценарии бисекта последовательны (один docker-хост).
- PgWorker в E2E остаётся хост-процессами фикстуры (не переводим в docker).
- Язык: документация/комментарии — русский; идентификаторы — английский; тесты — с AAA-комментариями.
- Обход до мержа фикса: E2E только после явной пересборки Release (устраняется Task 1).
- Бюджеты прогонов: 1 E2E-кейс ≈ 5–15 мин; 4 кейса ≈ 30–45 мин; бисект ≈ 5–8 шагов × ~10–20 мин (раннер при red гоняет маркер дважды — «стабильно красный», spec фаза Б.4а).
- Все коммиты в задачах — `git add` по явным путям (никаких `-a`/`-A`): в worktree между задачами живут spec-журнал и прочие артефакты, не относящиеся к конкретному коммиту.

## Справочник команд (общие для задач)

- Сборка Release: `dotnet build src/PgWorker.slnx -c Release`
- Кейс-маркер (спека, фаза Б.1): `Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused` в `PgWorker.IntegrationTests.E2e.E2eScaleScenarios`
- **Префикс окружения для КАЖДОГО прогона dotnet test**: `DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C`. Проверено живым прогоном при планировании: без `DOTNET_CLI_UI_LANGUAGE=en` вывод VSTest русифицирован хостом («Пройден!   : не пройдено     0, … всего     7»), `LC_ALL=C` его НЕ переключает — все английские сигнатуры ниже работают только с этим префиксом.
- Прогон маркера: `DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter 'FullyQualifiedName~E2eScaleScenarios.Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused'`
- Прогон 4 кейсов: тот же запуск с `--filter 'FullyQualifiedName~Scale_TakeoverMidAdd|FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7|FullyQualifiedName~Move_Lifecycle_Chain'` (обратите внимание: точное имя acceptance-кейса — `Acceptance_Scenario_Ac2_To_Ac7`)
- **Формат итоговой строки VSTest** (по одной на тестовую dll, где фильтр нашёл тесты; проверено живым прогоном): `Passed!  - Failed:     0, Passed:     2, Skipped:     0, Total:     2, Duration: 17 ms - <Dll>.dll` — с ВЫРАВНИВАЮЩИМИ пробелами после `Total:`/`Failed:`/`Skipped:`. Отсюда сигнатуры классификации (grep -E, устойчивы к паддингу):
  - **ТЕЛО прогона** (тест выполнен): `Total: +[1-9]` — итоговая строка с ненулевым числом тестов; пустой фильтр даёт «No test matches the given testcase filter» без Total-строки;
  - **КРАСНЫЙ**: `Failed: +[1-9]` (в зелёном итоге счётчик `Failed:     0`; строки списка провалов «Failed <имя>» без двоеточия — не матчатся);
  - **СКИП>0**: `Skipped: +[1-9]`.
- **Цвет прогона определяется ТОЛЬКО грепами лога, не exit-кодом пайплайна**: `$?` после `dotnet test … | tee log` — статус `tee` (практически всегда 0) — красный легко принять за зелёный. Раннер Task 4 пишет вывод через `>>$log 2>&1` без пайпа — там `$?` корректен, но и он используется только для выбора ветки, цвет дублируется сигнатурой `Passed!`.
- Все E2e-классы в коллекции `E2eCollection` — кейсы одного прогона идут последовательно.
- Сигнатуры сугубо инфраструктурных сбоев (не вины кода), единый список для Task 3 и Task 4 — держать синхронно:
  `ENV_SIG='etcd в .* не поднялся за 30|volume is in use|Test host process crashed|active test run was aborted|error response from daemon|docker daemon is not running|no space left|docker build -q .* →'`
  Пояснения — в Task 4 Step 1. Категория ВАЛИДНОГО red (в ENV_SIG НЕ входит, легитимность подтверждается повтором): `ApplicationException` фикстуры «инстанс … упал при старте» И «инстанс … не поднялся за 30 с» (E2eFixture.cs:226/241 — неготовность живого PgWorker.App может быть самим дефектом).

---

### Task 1: Автосборка Release в E2eFixture (инструмент бисекта + требование «фаза Г»)

**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs` (блок `InitializeAsync`, строки ~44–49; новый метод рядом с `RunProcessAsync`)
- Test: `src/tests/PgWorker.IntegrationTests/E2e/E2eAutoBuildTests.cs` (новый)

**Interfaces:**
- Consumes: существующий `private static Task<string> RunProcessAsync(string file, string[] args, CancellationToken ct = default)` (бросает `ApplicationException` при exit≠0 с текстом ошибки — переиспользуем для `dotnet build`).
- Produces: `internal static Task EnsureAppDllAsync(string root, bool noBuild)` — используется `InitializeAsync`; env-флаг `PGW_TEST_E2E_NOBUILD=1` отключает автосборку (обход для бисекта/отладки конкретного бинаря).

- [ ] **Step 1: Красный юнит-тест детерминированной части (NOBUILD без бинаря)**

Создать `src/tests/PgWorker.IntegrationTests/E2e/E2eAutoBuildTests.cs`:

```csharp
using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// t09, spec «фаза Г»: правило пересборки Release в E2eFixture. Юнит-слой —
// детерминированная ветка PGW_TEST_E2E_NOBUILD без бинаря (fail-fast);
// автосборка проверяется живым прогоном (Task 2).
public class E2eAutoBuildTests
{
    [Fact]
    public async Task EnsureAppDll_NoBuild_WithoutDll_FailsFastWithBuildHint()
    {
        // Arrange: пустой временный каталог — бинаря нет, автосборка выключена.
        var root = Path.Combine(Path.GetTempPath(), $"pgw-e2e-nobuild-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            // Act: NOBUILD + отсутствие бинаря.
            var ex = await Assert.ThrowsAsync<ApplicationException>(
                () => E2eFixture.EnsureAppDllAsync(root, noBuild: true));

            // Assert: fail-fast с командой сборки в сообщении.
            ex.Message.Should().Contain("dotnet build src/PgWorker.slnx -c Release");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
```

- [ ] **Step 2: Прогнать тест — убедиться, что красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eAutoBuildTests"`
Expected: FAIL — компиляция: `EnsureAppDllAsync` не существует (CS0117).

- [ ] **Step 3: Реализовать EnsureAppDllAsync и врезать в InitializeAsync**

В `E2eFixture.cs` добавить метод (рядом с `RunProcessAsync`):

```csharp
/// <summary>
/// Правило пересборки Release (t09, spec «фаза Г»): автосборка при каждом
/// прогоне E2E — устаревший зелёный бинарь маскировал регрессию 29.08–02.09.
/// PGW_TEST_E2E_NOBUILD=1 — обход для бисекта/отладки конкретного бинаря.
/// </summary>
internal static async Task EnsureAppDllAsync(string root, bool noBuild)
{
    var appDll = Path.Combine(
        root, "src", "PgWorker.App", "bin", "Release", "net10.0", "PgWorker.App.dll");
    if (noBuild)
    {
        if (!File.Exists(appDll))
            throw new ApplicationException(
                $"нет {appDll} — соберите решение: dotnet build src/PgWorker.slnx -c Release"
                + " (или снимите PGW_TEST_E2E_NOBUILD для автосборки)");
        return;
    }

    // Инкрементальный msbuild: no-op при актуальном бинаре (секунды);
    // ошибка сборки → ApplicationException от RunProcessAsync (fail-fast,
    // а не молчаливый запуск старого бинаря).
    await RunProcessAsync(
        "dotnet", ["build", Path.Combine(root, "src", "PgWorker.slnx"), "-c Release"]);
    if (!File.Exists(appDll))
        throw new ApplicationException($"после автосборки нет {appDll} — проверьте сборку решения");
}
```

В `InitializeAsync` заменить блок (текущие строки 44–49: `Root = ...; AppDll = ...; if (!File.Exists(AppDll)) throw ...`) на:

```csharp
// Корень репозитория и артефакты: от каталога тестовой сборки вверх.
Root = FindRoot(AppContext.BaseDirectory);
// Автосборка Release до docker-очистки (быстрый fail); NOBUILD — лазейка t09.
await EnsureAppDllAsync(
    Root, Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1");
AppDll = Path.Combine(Root, "src", "PgWorker.App", "bin", "Release", "net10.0", "PgWorker.App.dll");
```

(Прежний `if (!File.Exists(AppDll)) throw ...` удалить — он полностью заменён методом.)

- [ ] **Step 4: Прогнать тест — зелёный; вся сборка без warn**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~E2eAutoBuildTests"`
Expected: PASS.
Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: 0 Error(s), 0 Warning(s) (`TreatWarningsAsErrors`).

- [ ] **Step 5: Быстрый live-чек fail-fast без docker-цикла**

Run (в рабочем worktree):
```bash
mv src/PgWorker.App/bin/Release/net10.0/PgWorker.App.dll /tmp/pgw-app-dll-backup.dll 2>/dev/null || true
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_E2E_NOBUILD=1 PGW_TEST_DOCKER=1 \
  dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter "FullyQualifiedName~E2eScaleScenarios.Scale_AddEmptyShard" 2>&1 | tail -20
mv /tmp/pgw-app-dll-backup.dll src/PgWorker.App/bin/Release/net10.0/PgWorker.App.dll 2>/dev/null || true
```
Expected: падение фикстуры за секунды с сообщением «нет …PgWorker.App.dll — соберите решение: dotnet build src/PgWorker.slnx -c Release»; docker-контейнеры НЕ поднимаются (автосборка/проверка идут до очистки).

- [ ] **Step 6: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs src/tests/PgWorker.IntegrationTests/E2e/E2eAutoBuildTests.cs
git commit -m "t09: автосборка Release в E2eFixture (PGW_TEST_E2E_NOBUILD — обход) — устраняет маскировку устаревшим бинарем (spec фаза Г)"
```

---

### Task 2: Репро на HEAD (spec «фаза А»)

**Files:**
- Modify: `docs/superpowers/2026-09-03-t09-e2e-release-regression/spec.md` — добавить раздел «## 8. Журнал бисекта» и первую запись.

**Interfaces:**
- Consumes: Task 1 (автосборка должна поднять бинарь сама).
- Produces: запись в журнале с исходом А1 (падают) или А2 (не падают) — определяет акценты Task 5, но НЕ отменяет бисект (спека, фаза А2).

- [ ] **Step 1: Прогон 4 кейсов на свежем Release через автосборку**

Run (в рабочем worktree):
```bash
rm -rf src/PgWorker.App/bin/Release   # заодно live-верификация автосборки Task 1
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter 'FullyQualifiedName~Scale_TakeoverMidAdd|FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7|FullyQualifiedName~Move_Lifecycle_Chain' \
  2>&1 | tee /tmp/pgw-repro-head.log
grep -Eq 'Total: +4' /tmp/pgw-repro-head.log && echo "BODY: 4 теста выполнены"
grep -Ec 'Failed: +[1-9]' /tmp/pgw-repro-head.log
```
Expected: (а) прогон стартует — автосборка Task 1 восстановила Release; (б) `BODY: 4 теста выполнены` (тело прогона полное); (в) каждый из 4 кейсов даёт вердикт Passed/Failed — счётчик `Failed: +[1-9]` показывает, сколько кейсов красные (0 = А2, 4 = А1).

- [ ] **Step 2: Зафиксировать исход в журнале spec.md**

Добавить в spec.md:

```markdown
## 8. Журнал бисекта (дополняется по фазам)

- Фаза А (HEAD на дату прогона, автосборка включена): <Passed|Failed> по каждому
  из 4 кейсов; вывод: А1 (регрессия воспроизводится) | А2 (t90/t07 закрыли —
  исторический бисект по фазе А2).
```

Ветвление: все 4 красные → А1, идти Task 3. Любая комбинация зелёных → записать какие; если все зелёные → А2, Task 3–4 всё равно выполняются (исторический бисект), Task 5 вырождается согласно спеке.

---

### Task 3: Верификация good-границы (spec «фаза Б, шаг 0»)

**Files:** — (временные worktree/логи вне git; результат — запись в журнале spec.md).

**Interfaces:**
- Consumes: список PgWorker-коммитов окна (спека, факт 4); маркер, сигнатуры тела/цвета и `ENV_SIG` из справочника.
- Produces: `GOOD` — хеш последнего зелёного (ожидаемо `55e2962`), вход Task 4.

- [ ] **Step 0: Пре-чек существования маркера на историческом коммите (без поднятия worktree)**

Run (из рабочего worktree; для любого проверяемого исторического хеша `<H>`):
```bash
git grep -n "Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused" <H> -- src/tests
```
Expected: совпадение в `src/tests/PgWorker.IntegrationTests/E2e/E2eScaleScenarios.cs` (на `55e2962` проверено при планировании: маркер и все 4 кейса существуют под текущими именами). Нет совпадений → маркер на `<H>` отсутствует/переименован → действовать по Step 2, ветка «невалидный прогон» (остановка и пересмотр методики; расширение назад не лечит).

- [ ] **Step 1: Временный worktree на кандидате good**

Run:
```bash
git worktree add --detach /tmp/pgw-bisect-good 55e2962
cd /tmp/pgw-bisect-good && dotnet build src/PgWorker.slnx -c Release
```
Expected: сборка успешна (0 errors).

- [ ] **Step 2: Прогон маркера на кандидате good — классификация: сначала «тело», потом цвет**

Классификация двухступенчатая (полная, ветки не пересекаются; первична валидность прогона). **Цвет и тело определяются ТОЛЬКО грепами лога** — exit-код пайплайна с `tee` не используется вовсе (`$?` после `… | tee log` — статус `tee`, практически всегда 0: реальный красный был бы ошибочно принят за зелёный и ложно закрепил GOOD).

Run:
```bash
cd /tmp/pgw-bisect-good && DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 \
  dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter 'FullyQualifiedName~E2eScaleScenarios.Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused' \
  2>&1 | tee /tmp/pgw-good-check.log
# Классификация по логу (сигнатуры справочника):
echo "body=$(grep -Ec 'Total: +[1-9]' /tmp/pgw-good-check.log) red=$(grep -Ec 'Failed: +[1-9]' /tmp/pgw-good-check.log) skip=$(grep -Ec 'Skipped: +[1-9]' /tmp/pgw-good-check.log)"
```

**Ступень 1 — тело прогона** (`body=0` при ЛЮБОМ exit-коде — пустой фильтр «No test matches…» либо крэш тест-хоста/SDK до итоговой строки) → **«невалидный прогон»**: это НЕ зелёный и НЕ красный. **Остановка задачи** — возврат на пересмотр методики (в т.ч. выбор альтернативного кейса-маркера, существующего на всём окне — пре-чек Step 0 в помощь, — и перезапуск Task 3–4 с ним). Закрепление такого `GOOD` и расширение назад эту ветку не лечат.

**Ступень 2 — цвет (body≥1)**:
- **«Зелёный с телом»**: `red=0` И `skip=0` → единственный исход, закрепляющий `GOOD=<H>`. Записать в журнал, шаг 3.
- **«Красный с телом»** (`red≥1`): красный на good-кандидате недостоверен до подтверждения (разовый инфраструктурный сбой в VSTest даёт тот же итог с Failed):
  1. Прогнать фильтр контура по логу: `grep -icE "<ENV_SIG из справочника>" /tmp/pgw-good-check.log` — совпадение (>0) → сбой контура (etcd-контейнер фикстуры, гонка volume и т.п.): устранить причину, GOOD-кандидат НЕ трогать, повторить Step 2 заново.
  2. Совпадения нет → повторить прогон командой Step 2 с `tee /tmp/pgw-good-retry.log` и тем же блоком классификации по retry-логу:
     - повтор — тоже «красный с телом» (`red≥1`, `body≥1`, без ENV_SIG-совпадений) → red подтверждён дважды → good-граница шире (спека, фаза Б.2): расширение назад — шагать по PgWorker-коммитам 28.08 (в порядке убывания даты: `8318995`, `9e14347`, `88ec65b`, `3cc2d0f`; ранее — список из `git log --format='%h %ad %s' --date=format:'%m-%d %H:%M' --reverse <старейший>~5..55e2962 -- src/PgWorker.Core src/PgWorker.App src/PgWorker.Provisioning`), для каждого повторять Step 0–2 (`git -C /tmp/pgw-bisect-good checkout <хеш>`), пока исход не станет «зелёным с телом»; он и есть `GOOD`;
     - повтор зелёный (`red=0`) / с ENV_SIG-совпадением / без тела (`body=0`) → «исход недостоверен»: диагностировать окружение (логи `/tmp/pgw-good-{check,retry}.log`), затем повторить Step 2; `GOOD` не закрепляется до стабильного исхода.

- [ ] **Step 3: Записать GOOD в журнал spec.md**

Дописать в раздел 8: `- Фаза Б, шаг 0: GOOD=<хеш> (маркер зелёный «с телом»: body≥1, red=0, skip=0 по логу; красные кандидаты — по факту перепроверки повтором); bad-граница 2161f33 (спека, факт 2).`

- [ ] **Step 4: Убрать временный worktree**

Run: `git worktree remove --force /tmp/pgw-bisect-good`
Expected: worktree исчез (`git worktree list` не содержит `/tmp/pgw-bisect-good`).

---

### Task 4: Бисект (spec «фаза Б»)

**Files:** — (скрипт во `/tmp`, не коммитится; результат — записи в журнале spec.md).

**Interfaces:**
- Consumes: `GOOD` из Task 3; bad = `2161f33`; маркер, сигнатуры тела/цвета и `ENV_SIG` из справочника.
- Produces: `CULPRIT` — хеш коммита-виновника (маркер стабильно красный на нём — ≥2 подтверждённых red, зелёный на родителе) + артефакты прогона (логи `/tmp/pgw-bisect-*.log`, пути `host.log` — вход Task 5).

- [ ] **Step 1: Создать раннер бисекта (трёхзначная классификация: good / bad / skip)**

Создать `/tmp/pgw-bisect-run.sh` (обязателен `chmod +x`). Классификация по spec фаза Б.3: сбой окружения — В ТОМ ЧИСЛЕ «фикстура не поднялась не по вине кода» и прогон без тела — даёт exit 125 (git bisect skip), а не bad; «красный» засчитывается только при двукратном подтверждении (spec фаза Б.4а «стабильно красный»). Сигнатуры окружения (`ENV_SIG` справочника) — намеренно УЗКИЕ. Значения сигнатур: «etcd в .* не поднялся за 30» — ТОЛЬКО etcd-контейнер фикстуры (E2eFixture.cs:117 — `InvalidOperationException("etcd в … не поднялся за 30 c")`; НЕ матчит «инстанс … не поднялся за 30 с» E2eFixture.cs:241 — неготовность живого PgWorker.App: это валидный red, как и «инстанс … упал при старте» E2eFixture.cs:226 — их легитимность подтверждает повтор); «volume is in use» — гонка уборки volume; «Test host process crashed | active test run was aborted» — крэш тест-хоста; «error response from daemon | docker daemon is not running | no space left» — docker runtime/диск; «docker build -q … →» — сбой сборки образа pgworker-node:e2e (RunProcessAsync включает командную строку в текст ApplicationException).

```bash
#!/bin/bash
# t09 bisect-раннер (spec фаза Б.3): build Release + прогон кейса-маркера.
# Выход: 0 = good (маркер выполнен и зелёный), 1 = bad (red подтверждён дважды),
# 125 = skip (docker/сборка/сбой контура фикстуры/прогон без тела/неповторяемый red).
export LC_ALL=C
export DOTNET_CLI_UI_LANGUAGE=en   # без него вывод VSTest русифицирован — сигнатуры не работают
set -uo pipefail
WT="${PGW_BISECT_WT:?укажите каталог бисект-worktree}"

run_marker() {  # $1 = лог-файл; вывод БЕЗ пайпа — $? после вызова = статус dotnet test
  DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 \
    dotnet test src/PgWorker.slnx -c Release --no-build \
    --filter 'FullyQualifiedName~E2eScaleScenarios.Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused' \
    >>"$1" 2>&1
}

env_fail() { echo "$1 — skip" | tee -a "$2"; exit 125; }

# ЕДИНЫЙ список ENV_SIG (справочник плана; идентичен фильтру Task 3 Step 2).
ENV_SIG='etcd в .* не поднялся за 30|volume is in use|Test host process crashed|active test run was aborted|error response from daemon|docker daemon is not running|no space left|docker build -q .* →'

cd "$WT" || exit 125
: > /tmp/pgw-bisect-last.log
docker info >/dev/null 2>&1 || env_fail "docker недоступен" /tmp/pgw-bisect-last.log
dotnet build src/PgWorker.slnx -c Release >>/tmp/pgw-bisect-last.log 2>&1 \
  || env_fail "сборка упала" /tmp/pgw-bisect-last.log

run_marker /tmp/pgw-bisect-last.log
code1=$?

# Прогон без тела (Total: +[1-9] нет) = фильтр не нашёл тест либо крэш до
# итоговой строки: размечать good/bad нельзя — разбираться руками.
grep -Eq 'Total: +[1-9]' /tmp/pgw-bisect-last.log \
  || env_fail "прогон без тела (пустой фильтр или крэш тест-хоста)" /tmp/pgw-bisect-last.log

if [ $code1 -eq 0 ]; then
  # Цвет дублируется сигнатурой (exit-коду в одиночку не доверяем).
  grep -q 'Passed!' /tmp/pgw-bisect-last.log \
    || env_fail "итог без Passed! — разбраться" /tmp/pgw-bisect-last.log
  # Страховка от ложно-зелёного: маркер не скипнулся.
  grep -Eq 'Skipped: +[1-9]' /tmp/pgw-bisect-last.log \
    && env_fail "тест скипнулся" /tmp/pgw-bisect-last.log
  exit 0
fi

# Красный (code1 != 0): сначала отделить сбой контура от падения ассерта.
grep -qiE "$ENV_SIG" /tmp/pgw-bisect-last.log \
  && env_fail "сбой контура фикстуры" /tmp/pgw-bisect-last.log
grep -Eq 'Failed: +[1-9]' /tmp/pgw-bisect-last.log \
  || env_fail "ненулевой exit без красного итога — разбраться" /tmp/pgw-bisect-last.log

# «Стабильно красный» (spec фаза Б.4а): единственный red перепроверяется повтором.
: > /tmp/pgw-bisect-retry.log
run_marker /tmp/pgw-bisect-retry.log
code2=$?
[ $code2 -eq 0 ] && env_fail "red не повторился — флак" /tmp/pgw-bisect-retry.log
grep -qiE "$ENV_SIG" /tmp/pgw-bisect-retry.log \
  && env_fail "повтор упал по контуру" /tmp/pgw-bisect-retry.log
grep -Eq 'Total: +[1-9]' /tmp/pgw-bisect-retry.log \
  || env_fail "повтор без тела теста" /tmp/pgw-bisect-retry.log
grep -Eq 'Failed: +[1-9]' /tmp/pgw-bisect-retry.log \
  || env_fail "повтор без красного итога" /tmp/pgw-bisect-retry.log
exit 1   # red подтверждён дважды
```

Run: `chmod +x /tmp/pgw-bisect-run.sh`
Expected: файл существует, исполняем; `bash -n /tmp/pgw-bisect-run.sh` — без синтаксических ошибок.

- [ ] **Step 2: Запустить бисект**

Run:
```bash
git worktree add --detach /tmp/pgw-bisect 2161f33
cd /tmp/pgw-bisect
git bisect start
git bisect bad 2161f33
git bisect good <GOOD>          # из Task 3
PGW_BISECT_WT=/tmp/pgw-bisect git bisect run bash /tmp/pgw-bisect-run.sh
```
Expected: `<CULPRIT> is the first bad commit` (~5–8 шагов; шаг с red занимает двойной прогон маркера — до ~30 мин; суммарно ≈ 1.5–2.5 ч). **Сразу зафиксировать хеш `CULPRIT` из вывода** (до `bisect reset`). Если бисект остановился с «skipped»-коммитами и неоднозначностью — `git bisect log`, разобраться руками (перезапуск конкретных шагов раннером), НЕ угадывать виновника. Хвосты прогонов: `/tmp/pgw-bisect-last.log`, `/tmp/pgw-bisect-retry.log` (при необходимости сохранить: `cp … /tmp/pgw-bisect-<хеш>-{last,retry}.log`).

- [ ] **Step 3: Финальная верификация пары — «стабильно красный» + все 4 кейса на CULPRIT и на родителе**

Операционализация критерия spec фаза Б.4(а) «стабильно красный»: суммарный счётчик подтверждённых red маркера на `CULPRIT` — раннер Task 4 уже дал 2 (основной + повтор); парный полный прогон ниже даёт 3-й. Критерий: **все прогоны маркера на `CULPRIT` — red (≥2 из них с повтором), на `<CULPRIT>^` — green; любой зелёный прогон маркера на `CULPRIT` = флак-сигнал** → вернуться к `git bisect log` и перезапустить сомнительные шаги раннером (не подбирать виновника вручную).

Run (в `/tmp/pgw-bisect`, bisect ещё стоит на CULPRIT):
```bash
cd /tmp/pgw-bisect && dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter 'FullyQualifiedName~Scale_TakeoverMidAdd|FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7|FullyQualifiedName~Move_Lifecycle_Chain' \
  2>&1 | tee /tmp/pgw-bisect-culprit-4cases.log
git bisect reset && git checkout <CULPRIT>^ && dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build \
  --filter 'FullyQualifiedName~Scale_TakeoverMidAdd|FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7|FullyQualifiedName~Move_Lifecycle_Chain' \
  2>&1 | tee /tmp/pgw-bisect-parent-4cases.log
echo "culprit: body=$(grep -Ec 'Total: +4' /tmp/pgw-bisect-culprit-4cases.log) red=$(grep -Ec 'Failed: +[1-9]' /tmp/pgw-bisect-culprit-4cases.log)"
echo "parent:  body=$(grep -Ec 'Total: +4' /tmp/pgw-bisect-parent-4cases.log) red=$(grep -Ec 'Failed: +[1-9]' /tmp/pgw-bisect-parent-4cases.log)"
```
Expected (критерии виновника, спека «фаза Б.4»; цвет — по логам, не по exit пайплайна):
- оба прогона «с телом»: `body=1` (итоговая строка `Total:     4`);
- родитель `<CULPRIT>^`: `red=0` — все 4 зелёные (гипотеза единого корня; если нет — записать расхождение в журнал как факт, это не блокер);
- `CULPRIT`: `red≥1` — маркер red (3-й подтверждённый red сверх двух из раннера); прочие кейсы — Passed/Failed по факту (`Failed:     N` = сколько из 4 красные), записать.

- [ ] **Step 4: Записать CULPRIT и артефакты в журнал spec.md**

Дописать в раздел 8: хеш и тему CULPRIT (`git -C /tmp/pgw-bisect show --stat --oneline <CULPRIT> | head -20`), счётчик подтверждённых red маркера (≥3: 2 из раннера + парный прогон), вердикты 4 кейсов на паре, пути сохранённых логов.

- [ ] **Step 5: Убрать временный worktree**

Run: `git -C /tmp/pgw-bisect bisect reset; git worktree remove --force /tmp/pgw-bisect`
Expected: worktree исчез; рабочая ветка Task'ов не тронута.

---

### Task 5: Диагноз → фикс (spec «фаза В», сценарий А / Б / смешанный)

**Files:**
- Modify: `arch/14-pgworker.md` — сценарий А и смешанный (arch-first, ПЕРВЫМ коммитом)
- Modify: PgWorker-код точечно по виновнику (`src/PgWorker.Core`, `src/PgWorker.Provisioning`, `src/PgWorker.App`) — сценарий А и смешанный
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/{E2eScaleScenarios,E2eScenarios,E2eMoveScenarios}.cs` — сценарий Б и смешанный (детерминизация ожиданий оставшихся красных кейсов)
- Test: регресс-тест корня (юнит/интеграционный, без полного E2E где воспроизводимо) — сценарий А и смешанный
- Modify: `docs/superpowers/2026-09-03-t09-e2e-release-regression/spec.md` — журнал

**Interfaces:**
- Consumes: `CULPRIT` + логи Task 4 (`/tmp/pgw-bisect-culprit-4cases.log`, `/tmp/pgw-bisect-<хеш>-*.log`, `host.log` инстансов из каталогов снапшотов `pgw-e2e-*` во временной папке); `git show <CULPRIT>`.
- Produces: починенный дефект; запись механизма в журнале; зелёные 4 кейса на ветке.

Внимание: код фикса не предопределяется спекой (состав определяет бисект) — ниже детерминированная процедура с гейтами; менять шаги местами нельзя.

- [ ] **Step 1: Собрать материал диагноза**

Run:
```bash
git show <CULPRIT> > /tmp/pgw-culprit.diff && wc -l /tmp/pgw-culprit.diff
grep -iE 'fail|assert|Expected|work=' /tmp/pgw-bisect-culprit-4cases.log | head -40
```
Плюс: `host.log` инстансов прогона (каталоги `pgw-e2e-*` во временной папке ОС) и WorkDump из сообщения об ошибке маркера.

- [ ] **Step 2: ГЕЙТ — сформулировать механизм дефекта словами**

≤10 строк текста: что коммит изменил → почему это роняет маркер (цепочка «изменение → состояние etcd/docker → ассерт»). Записать в журнал spec.md раздела 8. **Без формулировки фикс не начинается** (спека, фаза Б.4в).

- [ ] **Step 3: ГЕЙТ — выбрать сценарий по контракту (три исхода)**

Сравнить сформулированное наблюдаемое с `arch/14-pgworker.md` (разделы-кандидаты по подсистеме виновника: §5 A portalloc, §6 provisioning, §5 G add-shard, §5 F moves, §5 J adoption; адресация/dsn — §1–§2):
- код ведёт себя НЕ по контракту, тест детерминирован → **сценарий А** (Step 4A);
- код контракту соответствует, тест полагался на незаконтрактованное (тайминги/порядок тиков, изменившиеся бэкоффом `4ca780f` либо portalloc-клэймом t90) → **сценарий Б** (Step 4Б);
- **смешанный** (спека, фаза В, последний абзац: логика менялась И тест уязвим — для этой регрессии правдоподобен, spec факт 4: в окне и тайминги `4ca780f`, и логика порт-репланирования/adoption) → **Step 4С**: чиним оба.
Если вскрылась неточность самого контракта — уточнить arch/14 первым (в любом сценарии).
Записать выбор и обоснование в журнал.

- [ ] **Step 4A (сценарий А): arch-first, затем ТДД-фикс логики**

1. Обновить `arch/14-pgworker.md` в затронутом параграфе так, чтобы корректное поведение было сформулировано явно (язык существующих §). Commit строго по явному пути (spec-журнал Steps 2–3 в коммит НЕ входит):
   ```bash
   git add arch/14-pgworker.md
   git commit -m "t09: контракт arch/14 §<N> — <суть корректировки> (arch-first)"
   ```
2. Написать красный регресс-тест на механизм (уровень — минимальный, где механизм воспроизводим: юнит на процессе/планировщике или интеграционный на реальном etcd; структура — AAA по образцу `E2eAutoBuildTests` (Task 1 Step 1): Arrange — предусловие из шага 2, Act — исследуемая операция, Assert — контрактное ожидание; имя `Regression_T09_<Механизм>`).
3. Убедиться, что тест красный на текущем коде: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Regression_T09"` → FAIL.
4. Минимальный фикс в коде виновной подсистемы (править причину, не симптом).
5. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Regression_T09"` → PASS; `dotnet build src/PgWorker.slnx -c Release` → 0 Warning(s).

- [ ] **Step 4Б (сценарий Б): ТДД-фикс теста**

1. Красный тест уже существует — это сам падающий кейс на ветке (сценарий Б возможен только при исходе А1 фазы А; при А2 см. спеку — фикс-код не пишется). Зафиксировать красный прогон: `DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter 'FullyQualifiedName~Scale_AddEmptyShard'` → FAIL.
2. Привести ожидание теста к детерминированному контрактному (что именно ждать и в каком ключе etcd это видно — из формулировки шага 2); убрать уязвимость к таймингам (ждать состояние, а не порядок тиков).
3. Прогон кейса → PASS.

- [ ] **Step 4С (смешанный сценарий): сначала логика по контракту, затем детерминизация теста оставшихся красных кейсов**

1. Выполнить целиком Step 4A.1–4A.5 (arch-first, регресс-тест `Regression_T09_<Механизм>`, минимальный фикс логики).
2. Прогон всех 4 кейсов (команда Step 5 ниже) — зафиксировать, какие остались красными после фикса логики (часть должна позеленеть: их роняла логика; остальные роняет уязвимость теста).
3. К каждому оставшемуся красному кейсу применить шаги детерминизации из Step 4Б.2: привести ожидание к детерминированному контрактному (что именно ждать и в каком ключе etcd это видно — из формулировки шага 2 и контракта), убрать уязвимость к таймингам (ждать состояние, а не порядок тиков). Красный прогон каждого кейса до правки — его собственный «красный тест» (фикс-гейт — шаг 4).
4. Повторный прогон всех 4 кейсов → все зелёные.

- [ ] **Step 5: Прогон всех 4 кейсов на ветке (фикс-гейт)**

Run (в рабочем worktree, автосборка включена):
```bash
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter 'FullyQualifiedName~Scale_TakeoverMidAdd|FullyQualifiedName~Scale_AddEmptyShard|FullyQualifiedName~Acceptance_Scenario_Ac2_To_Ac7|FullyQualifiedName~Move_Lifecycle_Chain'
```
Expected: все 4 Passed. Если сценарий А2 (фаза А дала «не падают»): подтверждение, что текущий код дефект не содержит — этот шаг уже зелёный по определению, фикс-код не пишется (спека, фаза А2), Task сводится к журналу.

- [ ] **Step 6: Commit (явные пути, без захвата лишнего)**

Сначала сверка: `git status --short` — в коммит идут ТОЛЬКО файлы задачи (arch/14 при сценарии А/смешанном; правки кода/тестов по сценарию; spec-журнал). Затем:
```bash
git add arch/14-pgworker.md src/ docs/superpowers/2026-09-03-t09-e2e-release-regression/spec.md
git commit -m "t09: фикс регрессии E2E — <механизм одним предложением> (виновник <CULPRIT>, сценарий <А|Б|смешанный>); spec-журнал дополнен"
```
(путь `src/` сузить до фактически изменённых проектов по выводу `git status --short`).

---

### Task 6: Правило мерж-гейта в AGENTS.md (spec «фаза Д.1»)

**Files:**
- Modify: `AGENTS.md` (корень проекта pg; новый ⚠️-абзац после блока про порты docker-контейнеров)

**Interfaces:**
- Consumes: маркер из справочника; флаг `PGW_TEST_E2E_NOBUILD` из Task 1.
- Produces: процессное правило (критерий приёмки 4 спеки).

- [ ] **Step 1: Зафиксировать базовое число ⚠️-блоков ДО вставки**

Run: `before=$(grep -c '^⚠️' AGENTS.md); echo "before=$before"`
Expected: выведено целое число (запомнить/записать).

- [ ] **Step 2: Вставить правило в AGENTS.md**

После абзаца «⚠️ Порты docker-контейнеров в тестах — динамические…» вставить:

```markdown
⚠️ **E2E на свежем Release — обязателен в мерж-гейте задач, трогающих код воркеров.**
Задачи, меняющие `src/PgWorker.App`/`Core`/`Provisioning`/`Etcd` (и аналоги
KafkaWorker), в мерж-гейте прогоняют docker-E2E серию PgWorker на свежем
Release: минимум кейс-маркер
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`;
полный прогон E2eFixture — при изменении provisioning/portalloc/moves-процессов.
E2eFixture собирает Release сам (инкрементальный no-op — секунды);
`PGW_TEST_E2E_NOBUILD=1` — только для бисекта/отладки конкретного бинаря.
Урок t09: устаревший зелёный бинарь маскировал регрессию 29.08–02.09.
```

- [ ] **Step 3: Проверка (правило на месте, счётчик вырос ровно на 1)**

Run: `after=$(grep -c '^⚠️' AGENTS.md); echo "after=$after (ожидалось $((before+1)))"; grep -n "E2E на свежем Release" AGENTS.md`
Expected: `after == before+1`; правило найдено grep'ом.

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md
git commit -m "t09: AGENTS.md — правило мерж-гейта: docker-E2E на свежем Release для задач, трогающих код воркеров (spec фаза Д)"
```

---

### Task 7: Финальная верификация (spec «фаза Д.2», критерии приёмки)

**Files:**
- Modify: `docs/superpowers/2026-09-03-t09-e2e-release-regression/spec.md` (финальное дополнение журнала, если что-то осталось).

**Interfaces:**
- Consumes: всё выше (Task 1–6 влиты в ветку).
- Produces: финальные серии зелёные — основа мержа (roadmap-гейт — удаление тега t09 — делается мерж-коммитом, в план НЕ входит).

- [ ] **Step 1: Полный E2E-прогон (вся коллекция, не только 4 кейса)**

Run:
```bash
DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter 'FullyQualifiedName~PgWorker.IntegrationTests.E2e' 2>&1 | tee /tmp/pgw-final-e2e.log
grep -Eq 'Skipped: +[1-9]|Failed: +[1-9]' /tmp/pgw-final-e2e.log && echo "ЕСТЬ красные/скипы" || echo "OK: без Failed/Skipped"
```
Expected: «OK: без Failed/Skipped» (бюджет ~1–1.5 ч).

- [ ] **Step 2: Полные серии без docker и с docker**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release` → все зелёные (юнит + интеграционные без docker-гейта).
Run: `DOTNET_CLI_UI_LANGUAGE=en LC_ALL=C PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release` → все зелёные.
Expected: 0 Failed (Skipped допустимы только там, где гейт выключен намеренно).

- [ ] **Step 3: Чек-лист критериев приёмки спеки**

Пройти по spec.md «Критерии приёмки» 1–4: (1) виновник + пара прогонов + механизм в журнале раздела 8; (2) 4 кейса зелёные на свежем Release (+регресс-тест и arch/14 при сценарии А/смешанном); (3) автосборка + NOBUILD + fail-fast — Task 1; (4) правило в AGENTS.md — Task 6. Пункт 5 (roadmap-тег) — мерж-гейт, вне плана. Расхождения — исправить (включая правку задач выше) до завершения.

- [ ] **Step 4: Финальный commit (если остались правки журнала)**

```bash
git add docs/superpowers/2026-09-03-t09-e2e-release-regression/
git commit -m "t09: spec-журнал бисекта финализирован (виновник, механизм, верификация)" || true
```
