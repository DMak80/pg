# t24-e2e-suite-under-load — план реализации (Фаза 3 dev-flow)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** полная docker-E2E-серия PgWorker устойчива к параллельной нагрузке на том же docker-хосте в границах канона прогона: причина деградации (симптомы A «подвисание при тяжёлом соседе» и Б «DNS-деградация при N≥5») либо устранена, либо осознанно закрыта каноном N=3 с рельсами.

**Архитектура:** исследовательская задача с управляемым воспроизведением — синтетический DNS-зонд (`dns_probe.py` в тестовом проекте, режимы measure/storm) пишет CSV латентности/ошибок резолва в docker-логи (подбирается существующей телеметрией `CollectDiagnosticsAsync`); диагностический нагрузочный генератор (`src/tools/E2eLoadGen`, временный инструмент ветки, вне `PgWorker.slnx`) поднимает K шумовых guid-контуров (профили dns/cpu/io/full) на том же скрипте; матрица прогонов серии с гипотезами различает H1–H4; гейт решения фиксирует ветку «канон N=3» (рельса-предупреждение по фактическому числу живых контуров в `E2eEnvironment`) или «устранение» (отдельный мини-план митигаций); итог канонизируется в `docs/e2e-launch.md` и одном источнике истины csproj.

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), xUnit v3 (`XunitMaxParallelThreads`), Testcontainers 4.14.0, Docker CLI, python:3.12-alpine + alpine:3.20 (уже зеркалированы в локальный registry), etcd v3.5.21, MinIO.

**Spec:** [`docs/superpowers/2026-10-06-t24-e2e-suite-under-load/spec.md`](spec.md) — план аргументируется от спеки; исполнители читают оба документа. Рабочие каноны: `AGENTS.md`, `AGENTS.base.md` §12–13, [`docs/e2e-isolation.md`](../../../docs/e2e-isolation.md), [`docs/e2e-launch.md`](../../../docs/e2e-launch.md). Канон требований: `arch/roadmap/reliability.md`, пункт `t24`.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t24-e2e-suite-under-load`, ветка `feat-t24-e2e-suite-under-load`. Все пути ниже — от корня worktree.

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — 0 warnings в каждой сборке.
- Канон изоляции E2E нерушим (spec §7): guid-контуры, own-only чистка (`OwnName` по runId/тегу), ассерт чистоты, динамические порты, никаких широких фильтров `pgw-*` и глобальных prune из кода. Шумовые контуры генератора — те же правила (`pgw-noise-` префикс + guid).
- Таймауты ожидания агента ≤ 30 с (AGENTS.base §12): длинные прогоны — фоновыми процессами с поллингом хвоста лога (см. «Операционная модель прогонов» ниже). Полл в тестах 500 мс — канон репо. Бюджеты фикстур ≤ 100 с.
- Каждый экспериментальный прогон — с гипотезой, записанной в журнал ДО запуска (spec §3.2); перезапуск упавших прогонов для выяснения «что было» запрещён (docs/e2e-launch.md §4); падение серии — разбор по телеметрии (`/tmp/pgw-e2e-artifacts-<guid>/`), зачистка stop-остатков по `README-cleanup.txt` после разбора; спорный результат повторяется один раз и с той же гипотезой (spec §9).
- Гейт чистоты хоста перед/после КАЖДОЙ серии (AGENTS.md): `docker ps -a --format '{{.Names}}' | grep pgw-` → пусто; `docker network ls --format '{{.Name}}' | grep -E 'pgw-en-|pgw-noise-|kfw-net'` → пусто; страховка `docker network prune -f`. Осиротевшую пустую сеть движка `pgw-net` снимать `docker network rm pgw-net` (без endpoints демоном разрешается).
- Образы: только уже зеркалированные (`dev-stand/images/images.txt`: alpine:3.20, python:3.12-alpine, etcd, minio/mc); перед сериями — `dev-stand/images/pull-images.sh`. Новых внешних образов НЕ вводить; локальные образы в registry НЕ класть.
- Суммарное число живых user-сетей в экспериментах не подбирается к пулу ~30; между прогонами серии — контроль `docker network ls` (spec §7).
- Генератор нагрузки — временный инструмент ветки: постоянным тестом в main не остаётся (удаляется фазой приёмки, Task 10); DNS-зонд в E2E (env-флаг) — кандидат остаться (судьба — на гейте).
- Язык: документация/журнал — русский; идентификаторы — английские; тесты — AAA-комментарии.
- Интеграционные не-E2E тесты той же сборки: фиксируется только отсутствие ИЗМЕНЕНИЯ их поведения после чистки `AssemblyInfo.cs` (spec §7).
- Коммиты — в feature-ветке по ходу задач (execute-фаза); мерж-гейт roadmap (Task 11) — тем же мерж-коммитом.

## Операционная модель прогонов (обязательна для Tasks 4, 5, 10)

Полная серия — 37–59 мин; агент НЕ ждёт команду дольше 30 с (AGENTS.base §12). Модель каждого серийного прогона:

1. Записать гипотезу/конфигурацию в `journal.md` (строка в «Сводка прогонов»).
2. Гейт чистоты хоста (Global Constraints) — все проверки пустые.
3. Запуск серии в фон с записью полного лога:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 \
  dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e" \
  --logger "console;verbosity=detailed" \
  2>&1 | tee /tmp/pgw-t24-runNN.log
```

   Вариация N — ключом `-p:XunitMaxParallelThreads=N` (MSBuild-свойство xunit.v3; инкрементальная пересборка — секунды). Вариация соседа — генератор (Task 3) СОБРАННЫМ бинарём в фоне (бинарь собран Task 3 Step 5):

```bash
dotnet src/tools/E2eLoadGen/bin/Release/net10.0/E2eLoadGen.dll \
  -k <K> -p full -d <T+20мин> -o /tmp/pgw-noise-runNN > /tmp/pgw-noise-runNN.log 2>&1 &
```

   Длительность T+20 мин — потолок-страховка (сосед переживает затянувшуюся серию), а НЕ план остановки. Остановка соседа после завершения серии — `kill -INT $(pgrep -f 'E2eLoadGen.dll')`: Ctrl-C фоновой job НЕ доставляется (SIGINT идёт только foreground-группе процессов), а SIGKILL/SIGTERM без обработчика не гарантируют `finally` — останутся `pgw-noise-` сети/контейнеры. SIGINT же .NET маршрутизирует в `Console.CancelKeyPress` → `cts.Cancel()` → teardown в `finally`. Гейт чистоты — ТОЛЬКО после подтверждённого teardown соседа: итоговая строка `noise:` в его логе (по времени или отмена) ЛИБО пустой `docker network ls | grep pgw-noise-`. Если SIGINT не снял процесс за ~30 с — `kill $(pgrep -f 'E2eLoadGen.dll')` (SIGTERM) и страховочная ручная зачистка `docker rm -f $(docker ps -aq --filter name=pgw-noise-)` + `docker network rm $(docker network ls -q --filter name=pgw-noise-)` (префикс задачи, не общий `pgw-*`; внекодовая чистка хоста — канон AGENTS.md), факт — в журнал.
4. Поллинг: раз в ~25 с `tail -5 /tmp/pgw-t24-runNN.log` (и хвост лога генератора) — до итоговой строки `Passed!|Failed!` от vstest. Серия идёт десятки минут — норма; каждый поллинг короткий.
5. По завершении: зафиксировать в `journal.md` зелёность, время (строка `Total duration`), пиковое число живых сетей серии (поллингом `docker network ls --format '{{.Name}}' | grep -c '^pgw-en-'` во время серии — наблюдение фактического параллелизма, AC4).
6. Разбор (при фейле — по телеметрии артефактов, БЕЗ перезапуска), зачистка stop-остатков упавших сценариев (по их `README-cleanup.txt`), гейт чистоты хоста, вердикт в `journal.md`.

---

### Task 0: Журнал задачи — каркас для протоколов прогонов и гипотез

**Вход:** spec утверждён; worktree создан; журнал — обязательный артефакт задачи (spec §3.5: «каждый прогон — гипотеза, условия, измерения, вердикт»).

**Действие (Files):**
- Create: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md`

**Interfaces:**
- Consumes: —
- Produces: `journal.md` с секциями-якорями, которые заполняют все последующие задачи: `## Сводка прогонов`, `## Карта гипотез (AC2)`, `## Гейт решения (AC3)`, `## Наблюдения вне прогонов`.

**Выход:** место, куда ДО запуска пишется гипотеза каждого прогона (spec §3.2: «прогон без сформулированной гипотезы — нарушение»), и куда сводятся измерения для AC1–AC3.

- [ ] **Step 1: Создать journal.md с шаблоном**

```markdown
# Журнал задачи t24-e2e-suite-under-load

Протокол расследования (spec §3.5): каждый прогон/эксперимент — гипотеза ДО
запуска, условия, измерения, вердикт. Перезапуски упавших прогонов — только
по канону docs/e2e-launch.md §4 (полный анализ телеметрии + гипотеза).

## Сводка прогонов

| # | Дата | Конфигурация (N; профиль/K соседа) | Гипотеза (записана ДО запуска) | Исход (зелёность, время, пик pgw-en-сетей) | Артефакты |
|---|------|------------------------------------|-------------------------------|---------------------------------------------|-----------|

## Карта гипотез (AC2)

| Гипотеза | Проверяемое следствие | Измерение | Вердикт (подтверждено/исключено) | Артефакт |
|----------|-----------------------|-----------|----------------------------------|----------|
| H1 — DNS-резолвер Docker Desktop деградирует (латентность/ошибки растут с числом сетей; фейлы на этапе резолва, не TCP) | | | | |
| H2 — CPU/IO-конкуренция на хосте (корреляция фейлов с загрузкой; синтетическая CPU/IO без DNS-шума воспроизводит фейлы коннекта) | | | | |
| H3 — «подвисание» = транзиент-циклы против бюджетов фикстур (журнал прогресса `[PHASE] … elapsed` до обрыва; движение между ретраями) | | | | |
| H4 — соседний прогон как множитель (порог от суммарной нагрузки K соседей, не от N серии) | | | | |

## Гейт решения (AC3)

- Критерии: spec §6 «ГЕЙТ РЕШЕНИЯ» (устраняем и поднимаем N — причина в наших границах + митигация дёшева + матрица зелёная при N>3; канон N=3 + рельсы — ограничение Docker Desktop/хоста, или митигация дороже выгоды, или N>3 без выигрыша).
- Измерения к гейту: (заполняет Task 7 из карты гипотез и сводки прогонов)
- РЕШЕНИЕ: (заполняет Task 7: ветка А или Б, обоснование по критериям; при неоднозначности — вопрос/ответ пользователя)
- Судьба инструментов: генератор E2eLoadGen — удалить перед мержем (Task 10); DNS-зонд в E2E — (решение гейта: остаётся как опциональная телеметрия / удаляется)

## Наблюдения вне прогонов

(дискретные факты: диагностика хоста, выводы из телеметрии, команды зачистки)
```

- [ ] **Step 2: Проверка**

Run: `test -f docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md && grep -c "Карта гипотез" docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md`
Expected: `1`.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): журнал задачи — каркас протоколов прогонов и карты гипотез"
```

**Проверка задачи:** журнал существует с якорями секций.
**Связь со spec:** §3.5 (журнал задачи), §3.2 (гипотеза до эксперимента) — инфраструктура для AC1–AC3.

---

### Task 1: Один источник истины параллелизма — удаление мёртвого `CollectionBehavior`-атрибута

**Вход:** Task 0 (журнал). Фактическое поведение раннера задаёт csproj (`XunitParallelizeTestCollections=true`, `XunitMaxParallelThreads=3`), атрибут в `AssemblyInfo.cs` устарел и перекрыт (spec §1 «Конфигурационный долг»).

**Действие (Files):**
- Delete: `src/tests/PgWorker.IntegrationTests/AssemblyInfo.cs` (весь файл — кроме мёртвого атрибута с ложным комментарием содержимого нет).
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (записи до/после).

**Interfaces:**
- Consumes: —
- Produces: сборка `PgWorker.IntegrationTests` без assembly-атрибута; поведение раннера определяется ТОЛЬКО csproj (AC4).

**Выход:** устранена ловушка для следующего читателя; все дальнейшие прогоны задачи идут в финальной конфигурации параллелизма.

- [ ] **Step 1: Контрольная не-E2E серия ДО правки (базлайн поведения)**

Интеграционные не-E2E классы той же сборки (Docker/Etcd/Api — `[Collection]`-группы): прогон ДО удаления атрибута, фиксация зелёности и времени:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 \
  dotnet test src/tests/PgWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName!~E2e" --logger "console;verbosity=detailed" \
  2>&1 | tee /tmp/pgw-t24-none2e-before.log
```

(серия короче E2E — минуты; гейты чистоты хоста до/после по Global Constraints). Записать в `journal.md` → «Наблюдения вне прогонов»: `не-E2E до: <итоговая строка Passed! + Total duration>`.

- [ ] **Step 2: Удалить AssemblyInfo.cs**

```bash
git rm src/tests/PgWorker.IntegrationTests/AssemblyInfo.cs
```

- [ ] **Step 3: Сборка без warnings**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: 0 errors, 0 warnings (`TreatWarningsAsErrors=true`).

- [ ] **Step 4: Контрольная не-E2E серия ПОСЛЕ правки**

Та же команда, лог `/tmp/pgw-t24-none2e-after.log`. Сравнить с ДО в `journal.md`: зелёность та же (0 failed), время сопоставимо (±20%). КРИТЕРИЙ «поведение НЕ изменилось» (spec §5.4) — эти две записи: атрибут был мёртвым (перекрыт csproj), удаление ничего не меняет.

- [ ] **Step 5: Если серия ПОСЛЕ деградировала (fail/замедление > 20%)**

Вернуть файл (`git checkout HEAD -- src/tests/PgWorker.IntegrationTests/AssemblyInfo.cs`, не коммитить), зафиксировать факт в `journal.md`, СТОП и вопрос пользователю через контролёр флоу (`{status: NEEDS_CONTEXT}`): атрибут оказался НЕ мёртвым — трактовка spec §1 неверна, способ чистки решает пользователь. Не продолжать молча.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(t24): убран устаревший CollectionBehavior-атрибут из AssemblyInfo.cs — единственный источник истины параллелизма csproj (AC4); не-E2E серии до/после в журнале задачи"
```

**Проверка задачи:** `git ls-files src/tests/PgWorker.IntegrationTests/ | grep AssemblyInfo` → пусто; обе контрольные серии в журнале, поведение не изменилось.
**Связь со spec:** §1 (конфигурационный долг), §5.4, AC4, §7 (не-E2E — только фиксация отсутствия изменения).

---

### Task 2: Синтетический DNS-зонд — общий скрипт + опция в E2E-окружении

**Вход:** Task 0. Зонд (spec §5.2): циклический резолв набора имён с логированием исхода и латентности; встраивается опционально (env `PGW_TEST_E2E_DNS_PROBE=1`) в тестовые окружения — доказательная база «резолв умер, а не PG». Делается ДО генератора: генератор (Task 3) монтирует тот же скрипт для шторм-профиля.

**Действие (Files):**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py` (единый скрипт для E2E и генератора; живёт в тестовом проекте постоянно — переживает удаление генератора)
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eDnsProbe.cs` (контейнер-обёртка)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (опциональный старт зонда в `StartOnceAsync`, dispose в `DisposeAsync`)

**Interfaces:**
- Consumes: `E2eEnvironment.StartOnceAsync` (etcd-алиасы, `net`, `minio`), `E2eFixture.FindRoot`.
- Produces (используют Task 3 генератор и Tasks 5–6 эксперименты):
  - `dns_probe.py` — env-контракт: `DNS_PROBE_TARGETS` (имена через запятую), `DNS_PROBE_MODE=measure|storm`, `DNS_PROBE_INTERVAL` (сек, default 1, только measure). Вывод в stdout (docker logs): measure — `probe-dns,<unix_ts>,<target>,<1|0>,<latency_ms>,<error>` на попытку; storm — без пауз, агрегат каждые 5 с `storm-dns,<window_ts>,<iterations>,<fails>,<avg_ms>,<max_ms>`.
  - `internal static class E2eDnsProbe` c `internal const string Image = "python:3.12-alpine";`, `internal const string ScriptRelativePath = "src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py";`, `internal static IContainer Build(string runId, INetwork net, IReadOnlyList<string> targets, string mode, string intervalSec)`.
  - measure-цели E2E-окружения — ТРИ категории spec §5.2: алиасы сети `e2e-etcdN` (+`e2e-minio` при наличии), спец-резолвер `host.docker.internal`, внешнее имя `quay.io` (резолв через форвардеры Docker Desktop наружу — отдельный тракт от embedded DNS 127.0.0.11: различает, ЧТО деградирует, работает на H1/AC2).
  - `E2eEnvironment`: поле `private readonly IContainer? _dnsProbe;`, контейнер `pgw-dns-{runId}` (runId в имени → `OwnName` подбирает его и teardown'ом, и телеметрией `CollectDiagnosticsAsync`).

**Выход:** латентность/ошибки резолва измеримы в любом прогоне (фазы 1–2, H1/H3-доказательства); лог зонда автоматически попадает в артефакты teardown как `container-pgw-dns-{runId}.log`.

- [ ] **Step 1: dns_probe.py**

```python
# Синтетический DNS-зонд (t24, spec §5.2): циклический резолв набора имён с
# логированием исхода и латентности. Режимы:
#   measure — попытка за попыткой (probe-dns,...) — телеметрия E2E-окружений;
#   storm   — резолв без пауз, агрегат каждые 5 с (storm-dns,...) — шумовой
#             профиль нагрузочного генератора.
# Вывод в stdout (docker logs): CSV; в E2E подбирается CollectDiagnosticsAsync.
import os, socket, time

targets = [t for t in os.environ.get("DNS_PROBE_TARGETS", "").split(",") if t]
mode = os.environ.get("DNS_PROBE_MODE", "measure")
interval = float(os.environ.get("DNS_PROBE_INTERVAL", "1"))


def resolve(target):
    ts = time.time()
    try:
        socket.getaddrinfo(target, None)
        return ts, True, (time.time() - ts) * 1000.0, ""
    except Exception as e:
        return ts, False, (time.time() - ts) * 1000.0, str(e)


if mode == "storm":
    iterations = fails = 0
    total_ms = max_ms = 0.0
    window = time.time()
    while True:
        for t in targets:
            _, ok, ms, _ = resolve(t)
            iterations += 1
            total_ms += ms
            max_ms = max(max_ms, ms)
            if not ok:
                fails += 1
        if time.time() - window >= 5:
            print(f"storm-dns,{window:.0f},{iterations},{fails},"
                  f"{total_ms / max(iterations, 1):.1f},{max_ms:.1f}", flush=True)
            iterations = fails = 0
            total_ms = max_ms = 0.0
            window = time.time()
else:
    while True:
        for t in targets:
            ts, ok, ms, err = resolve(t)
            print(f"probe-dns,{ts:.3f},{t},{1 if ok else 0},{ms:.1f},{err}", flush=True)
        time.sleep(interval)
```

- [ ] **Step 2: E2eDnsProbe.cs**

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Синтетический DNS-зонд (t24, spec §5.2): контейнер-резолвер в
/// сети окружения; env-контракт — dns_probe.py. Лог — CSV в docker logs;
/// имя pgw-dns-{runId} содержит runId — телеметрия CollectDiagnosticsAsync и
/// teardown подбирают его по OwnName без специальных крючков. Wait-стратегия
/// не нужна: зонд ничего не отдаёт наружу, старт python мгновенен.</summary>
internal static class E2eDnsProbe
{
    internal const string Image = "python:3.12-alpine";

    // Путь от корня репозитория (скрипт общий для E2E и генератора нагрузки).
    internal const string ScriptRelativePath = "src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py";

    internal static IContainer Build(
        string runId, INetwork net, IReadOnlyList<string> targets, string mode, string intervalSec)
    {
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var script = File.ReadAllBytes(Path.Combine(root, ScriptRelativePath));
        return new ContainerBuilder(Image)
            .WithName($"pgw-dns-{runId}")
            .WithNetwork(net)
            .WithNetworkAliases("e2e-dns-probe")
            .WithEnvironment("DNS_PROBE_TARGETS", string.Join(",", targets))
            .WithEnvironment("DNS_PROBE_MODE", mode)
            .WithEnvironment("DNS_PROBE_INTERVAL", intervalSec)
            .WithResourceMapping(script, "/dns_probe.py")
            .WithCommand("python3", "-u", "/dns_probe.py")
            .Build();
    }
}
```

- [ ] **Step 3: Врезка в E2eEnvironment.StartOnceAsync (после блока MinIO, перед return)**

```csharp
// Синтетический DNS-зонд (t24, spec §5.2): опциональная телеметрия E2E —
// env PGW_TEST_E2E_DNS_PROBE=1. Лог (container-pgw-dns-*.log) подбирается
// CollectDiagnosticsAsync по OwnName(runId) — доказательная база «резолв
// умер, а не PG» при разборе упавших прогонов.
IContainer? dnsProbe = null;
if (Environment.GetEnvironmentVariable("PGW_TEST_E2E_DNS_PROBE") == "1")
{
    var targets = Enumerable.Range(1, etcdNodes.Count).Select(n => $"e2e-etcd{n}").ToList();
    if (minio is not null)
        targets.Add("e2e-minio");
    targets.Add("host.docker.internal");
    // Внешнее имя (третья категория spec §5.2): резолв через форвардеры
    // Docker Desktop наружу — отдельный тракт от embedded DNS 127.0.0.11;
    // quay.io уже зеркалирован в локальный registry, getaddrinfo трафика не тянет.
    targets.Add("quay.io");
    dnsProbe = E2eDnsProbe.Build(runId, net, targets, "measure", "1");
    await dnsProbe.StartAsync(ct);
}
```

и передать `dnsProbe` в приватный конструктор (новый последний параметр, поле `private readonly IContainer? _dnsProbe;`). Dispose — в `DisposeAsync` рядом с `_minio` тем же try/catch-паттерном `problems`:

```csharp
try
{
    if (_dnsProbe is not null)
        await _dnsProbe.DisposeAsync();
}
catch (Exception e)
{
    problems.Add($"dns-probe: {e.Message}");
}
```

В `FailedTearDownAsync` зонд останавливается общим циклом `docker stop` по `OwnContainersAsync` (имя содержит runId) — отдельного кода не нужно.

- [ ] **Step 4: Сборка**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: 0 errors, 0 warnings.

- [ ] **Step 5: Smoke — зонд в E2E-окружении (лёгкий 1-факт класс)**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 PGW_TEST_E2E_DNS_PROBE=1 \
  dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~E2eAppParams" --logger "console;verbosity=detailed" \
  2>&1 | tee /tmp/pgw-t24-dnsprobe-smoke.log
```

Expected: тест зелёный; в `/tmp/pgw-e2e-artifacts-<guid>/` (путь из строки `e2e[app-params]: телеметрия …`) есть `container-pgw-dns-*.log` со строками `probe-dns,…`; гейт чистоты хоста после — пусто.

- [ ] **Step 6: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py src/tests/PgWorker.IntegrationTests/E2e/E2eDnsProbe.cs src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs
git commit -m "feat(t24): синтетический DNS-зонд — dns_probe.py (measure/storm), опция PGW_TEST_E2E_DNS_PROBE в E2eEnvironment (spec §5.2)"
```

**Проверка задачи:** smoke: зонд пишет CSV в docker-логи, лог оказывается в артефактах teardown, зачистка полная.
**Связь со spec:** §5.2, §4 (H1/H3 — измерения «фейл на резолве, не на TCP»), §7 («DNS-зонд как опциональная телеметрия E2E может остаться»).

---

### Task 3: Диагностический нагрузочный генератор `src/tools/E2eLoadGen`

**Вход:** Task 2 (`dns_probe.py` существует). Генератор — временный инструмент ветки (spec §5.1): поднимает K «шумовых» изолированных контуров по канону изоляции, с профилями `dns|cpu|io|full`, управляется аргументами (K, профиль, время).

**Действие (Files):**
- Create: `src/tools/E2eLoadGen/E2eLoadGen.csproj`
- Create: `src/tools/E2eLoadGen/LoadGenOptions.cs`
- Create: `src/tools/E2eLoadGen/NoiseContour.cs`
- Create: `src/tools/E2eLoadGen/Program.cs`

Проект НЕ включать в `src/PgWorker.slnx` — диагностический инструмент, не должен собираться в soln-гейтах и попасть в main (удаляется Task 10).

**Interfaces:**
- Consumes: PackageReference `Testcontainers` (версия из `src/Directory.Packages.props`, 4.14.0 — центральное версионирование работает и вне slnx); `dns_probe.py` из Task 2 (генератор монтирует его по своему пути от корня репо).
- Produces (используют Tasks 5, 6):
  - CLI: `dotnet run --project src/tools/E2eLoadGen -- -k <контуров> -p <dns|cpu|io|full> -d <минуты> [-o <каталог>]`; дефолты `k=1, p=full, d=10, o=/tmp/pgw-noise-<guid>`; невалидные аргументы → usage в stderr, exit 2.
  - Состав контура по профилю (все контейнеры в ОДНОЙ сети `pgw-noise-{guid}` контура): `dns` = target + storm-зонд (2 контейнера); `cpu` = 2 × sha256sum-нагрузки; `io` = 1 × dd-нагрузка (bind-каталог /data); `full` = target + storm + 2×cpu + io (5 контейнеров).
- Публичные имена: `LoadGenOptions.Parse`, `NoiseContour.CreateAsync(int index, string profile, string outDir, CancellationToken ct)`, `NoiseContour.DisposeAsync` (телеметрия → dispose контейнеров → rm сети/каталога → ассерт чистоты).

**Выход:** управляемый и повторяемый соседний источник нагрузки (закрывает вторичную цель «воспроизводимость»); используется фазой 1 (симптомы А/Б, H4) и фазой 2 (изолирующие эксперименты).

- [ ] **Step 1: Каркас csproj**

`src/tools/E2eLoadGen/E2eLoadGen.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
    <!-- Диагностический нагрузочный генератор (t24, spec §5.1): временный
         инструмент ВЕТКИ задачи — в src/PgWorker.slnx НЕ включён, в main не
         поставляется (удаляется фазой приёмки). Поднимает K шумовых
         изолированных контуров (профили dns/cpu/io/full) по канону
         docs/e2e-isolation.md: guid-имена, own-only teardown, ассерт чистоты.
         Шторм-режим DNS — общий скрипт
         src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py. -->
    <PropertyGroup>
        <OutputType>Exe</OutputType>
        <TargetFramework>net10.0</TargetFramework>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
        <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
        <IsPackable>false</IsPackable>
        <RootNamespace>PgWorker.IntegrationTests.E2eLoadGen</RootNamespace>
    </PropertyGroup>
    <ItemGroup>
        <PackageReference Include="Testcontainers"/>
    </ItemGroup>
</Project>
```

(`src/Directory.Build.props` накрывает и `src/tools/` — LangVersion/Nullable/warnings-as-errors те же.)

- [ ] **Step 2: LoadGenOptions.cs — полный парсер**

```csharp
using System.Globalization;

namespace PgWorker.IntegrationTests.E2eLoadGen;

/// <summary>Аргументы генератора (t24, spec §5.1: управление — аргументы
/// запуска): -k/--count — контуров (1..8); -p/--profile — dns|cpu|io|full;
/// -d/--duration — минуты (0.05..480); -o/--out — каталог лога. Невалидное —
/// ApplicationException с usage (Program печатает в stderr, exit 2).</summary>
public sealed record LoadGenOptions(int Count, string Profile, double DurationMinutes, string OutDir)
{
    public const string ProfileDns = "dns";
    public const string ProfileCpu = "cpu";
    public const string ProfileIo = "io";
    public const string ProfileFull = "full";

    private const string Usage =
        "usage: e2eloadgen [-k count=1] [-p dns|cpu|io|full=full]"
        + " [-d minutes=10] [-o outdir=/tmp/pgw-noise-<guid>]";

    public TimeSpan Duration => TimeSpan.FromMinutes(DurationMinutes);

    public static LoadGenOptions Parse(string[] args)
    {
        var count = 1;
        var profile = ProfileFull;
        var duration = 10.0;
        var outDir = $"/tmp/pgw-noise-{Guid.NewGuid():N}";
        for (var i = 0; i < args.Length; i++)
        {
            var flag = args[i];
            string Value()
            {
                if (i + 1 >= args.Length)
                    throw new ApplicationException($"нет значения для {flag}\n{Usage}");
                return args[++i];
            }

            switch (flag)
            {
                case "-k":
                case "--count":
                    if (!int.TryParse(Value(), CultureInfo.InvariantCulture, out count) || count is < 1 or > 8)
                        throw new ApplicationException($"-k: ожидано целое 1..8\n{Usage}");
                    break;
                case "-p":
                case "--profile":
                    profile = Value();
                    if (profile is not (ProfileDns or ProfileCpu or ProfileIo or ProfileFull))
                        throw new ApplicationException($"-p: неизвестный профиль '{profile}'\n{Usage}");
                    break;
                case "-d":
                case "--duration":
                    if (!double.TryParse(Value(), CultureInfo.InvariantCulture, out duration)
                        || duration is < 0.05 or > 480)
                        throw new ApplicationException($"-d: ожиданы минуты 0.05..480\n{Usage}");
                    break;
                case "-o":
                case "--out":
                    outDir = Value();
                    break;
                default:
                    throw new ApplicationException($"неизвестный аргумент '{flag}'\n{Usage}");
            }
        }

        return new LoadGenOptions(count, profile, duration, outDir);
    }
}
```

- [ ] **Step 3: NoiseContour.cs — контур и teardown**

```csharp
using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace PgWorker.IntegrationTests.E2eLoadGen;

/// <summary>Один шумовой контур (t24, spec §5.1): сеть pgw-noise-{guid} +
/// контейнеры профиля. Канон docs/e2e-isolation.md: guid во всех именах,
/// телеметрия до удалений, own-only teardown при любом исходе, ассерт
/// чистоты своего контура.</summary>
public sealed class NoiseContour : IAsyncDisposable
{
    private const string AlpineImage = "alpine:3.20";
    private const string PythonImage = "python:3.12-alpine";

    // Общий скрипт DNS-зонда (t24 spec §5.2) — живёт в тестовом проекте.
    private const string ProbeScriptRelativePath =
        "src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py";

    private readonly string _prefix;
    private readonly INetwork _net;
    private readonly List<IContainer> _containers = [];
    private readonly DirectoryInfo _ioDir;
    private readonly string _outDir;

    private NoiseContour(string prefix, INetwork net, DirectoryInfo ioDir, string outDir)
    {
        _prefix = prefix;
        _net = net;
        _ioDir = ioDir;
        _outDir = outDir;
    }

    public static async Task<NoiseContour> CreateAsync(
        int index, string profile, string outDir, CancellationToken ct)
    {
        var prefix = $"pgw-noise-{Guid.NewGuid():N}";
        var net = new NetworkBuilder().WithName(prefix).Build();
        var ioDir = Directory.CreateDirectory(Path.Combine(outDir, $"contour{index}", "io"));
        var contour = new NoiseContour(prefix, net, ioDir, outDir);
        try
        {
            await net.CreateAsync(ct);
            contour.AddContainers(index, profile);
            foreach (var container in contour._containers)
                await container.StartAsync(ct);
            return contour;
        }
        catch
        {
            // частично поднятый контур не оставляем (лучшими усилиями)
            try
            {
                await contour.DisposeAsync();
            }
            catch
            {
                // guid-имена: чужие прогоны не заденем; добьёт ручная зачистка
            }

            throw;
        }
    }

    private void AddContainers(int index, string profile)
    {
        var root = FindRepoRoot();
        var script = File.ReadAllBytes(Path.Combine(root, ProbeScriptRelativePath));
        // Три категории целей spec §5.2: алиас сети + спец-резолвер хоста +
        // внешнее имя (форвард наружу — отдельный тракт от embedded DNS;
        // quay.io в локальном зеркале, трафика не тянет).
        var stormTargets = "noise-target,host.docker.internal,quay.io";

        IContainer Target() => new ContainerBuilder(PythonImage)
            .WithName($"{_prefix}-t{index}")
            .WithNetwork(_net)
            .WithNetworkAliases("noise-target")
            .WithCommand("sh", "-c", "while true; do echo alive; sleep 1; done")
            .Build();

        IContainer Storm() => new ContainerBuilder(PythonImage)
            .WithName($"{_prefix}-dns{index}")
            .WithNetwork(_net)
            .WithEnvironment("DNS_PROBE_TARGETS", stormTargets)
            .WithEnvironment("DNS_PROBE_MODE", "storm")
            .WithResourceMapping(script, "/dns_probe.py")
            .WithCommand("python3", "-u", "/dns_probe.py")
            .Build();

        IContainer Cpu(int j) => new ContainerBuilder(AlpineImage)
            .WithName($"{_prefix}-cpu{j}")
            .WithNetwork(_net)
            .WithCommand("sha256sum", "/dev/zero")
            .Build();

        IContainer Io() => new ContainerBuilder(AlpineImage)
            .WithName($"{_prefix}-io")
            .WithNetwork(_net)
            .WithBindMount(_ioDir.FullName, "/data")
            .WithCommand("sh", "-c",
                "while true; do dd if=/dev/zero of=/data/noise bs=1M count=256 conv=fsync 2>/dev/null; rm -f /data/noise; done")
            .Build();

        // Профили spec §5.1: dns — шторм резолвов; cpu — чистая CPU-нагрузка;
        // io — дисковая демона; full — комбинация (паттерн реального контура
        // по совокупной нагрузке на демона: сеть+контейнеры+DNS+CPU+IO).
        switch (profile)
        {
            case LoadGenOptions.ProfileDns:
                _containers.Add(Target());
                _containers.Add(Storm());
                break;
            case LoadGenOptions.ProfileCpu:
                _containers.Add(Cpu(1));
                _containers.Add(Cpu(2));
                break;
            case LoadGenOptions.ProfileIo:
                _containers.Add(Io());
                break;
            case LoadGenOptions.ProfileFull:
                _containers.Add(Target());
                _containers.Add(Storm());
                _containers.Add(Cpu(1));
                _containers.Add(Cpu(2));
                _containers.Add(Io());
                break;
            default:
                throw new ApplicationException($"неизвестный профиль {profile}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        var problems = new List<string>();
        try
        {
            Directory.CreateDirectory(_outDir);
        }
        catch
        {
            // телеметрия — «лучшими усилиями»
        }

        // Телеметрия прежде удалений: docker logs своих контейнеров (шторм —
        // агрегаты storm-dns; target — пульс) — данные для отчёта по фазам.
        foreach (var container in _containers)
        {
            try
            {
                var id = container.Id;
                if (!string.IsNullOrEmpty(id))
                {
                    var name = (await RunAsync("docker", ["inspect", "-f", "{{.Name}}", id])).Trim().TrimStart('/');
                    var logs = await RunAsync("docker", ["logs", "--timestamps", id]);
                    await File.WriteAllTextAsync(Path.Combine(_outDir, $"container-{name}.log"), logs);
                }
            }
            catch (Exception e)
            {
                problems.Add($"логи {container.Name}: {e.Message}");
            }
        }

        foreach (var container in _containers)
        {
            try
            {
                await container.DisposeAsync();
            }
            catch (Exception e)
            {
                problems.Add($"dispose {container.Name}: {e.Message}");
            }
        }

        try
        {
            await _net.DeleteAsync();
        }
        catch (Exception e)
        {
            problems.Add($"сеть {_prefix}: {e.Message}");
        }

        try
        {
            _ioDir.Delete(recursive: true);
        }
        catch (IOException)
        {
            // bind-каталог мог быть занят демоном;.guid-имя — заденем только своё
        }

        // АССЕРТ ЧИСТОТЫ контура: ни контейнеров, ни сети со своим префиксом.
        var leftContainers = (await RunAsync("docker", ["ps", "-a", "--format", "{{.Names}}"]))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Count(n => n.StartsWith(_prefix, StringComparison.Ordinal));
        if (leftContainers > 0)
            problems.Add($"остались контейнеры контура: {leftContainers}");
        var leftNet = (await RunAsync("docker", ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={_prefix}"])).Trim();
        if (leftNet.Length > 0)
            problems.Add($"осталась сеть {_prefix}");
        if (problems.Count > 0)
            throw new ApplicationException($"{_prefix}: teardown шумового контура неполный:\n- " + string.Join("\n- ", problems));
    }

    // Корень репозитория: тот же маркер, что у E2eFixture.FindRoot.
    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "node", "Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new ApplicationException("корень репозитория не найден");
    }

    // docker-CLI с бюджетом 2 мин (зависший процесс обязан умирать, а не висеть).
    private static async Task<string> RunAsync(string file, string[] args)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi) ?? throw new ApplicationException($"не удалось запустить {file}");
        using var budget = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var outTask = process.StandardOutput.ReadToEndAsync(budget.Token);
        var errTask = process.StandardError.ReadToEndAsync(budget.Token);
        try
        {
            await process.WaitForExitAsync(budget.Token);
            var output = await outTask;
            var error = await errTask;
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new ApplicationException($"{file} {string.Join(' ', args)} не завершился за 2 мин — убит");
        }
    }
}
```

- [ ] **Step 4: Program.cs**

```csharp
using System.Diagnostics;
using PgWorker.IntegrationTests.E2eLoadGen;

// Диагностический нагрузочный генератор (t24, spec §5.1): K шумовых
// изолированных контуров на время D — управляемый «тяжёлый сосед» для
// E2E-серии. Телеметрия — в out-каталоге; teardown own-only при завершении
// по времени ИЛИ Ctrl-C.
try
{
    var opts = LoadGenOptions.Parse(args);
    Console.WriteLine(
        $"noise: старт k={opts.Count} profile={opts.Profile} d={opts.DurationMinutes}m out={opts.OutDir}");
    var contours = new List<NoiseContour>();
    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
    var sw = Stopwatch.StartNew();
    try
    {
        for (var i = 1; i <= opts.Count; i++)
        {
            contours.Add(await NoiseContour.CreateAsync(i, opts.Profile, opts.OutDir, cts.Token));
            Console.WriteLine($"noise: контур {i}/{opts.Count} поднят ({sw.Elapsed:hh\\:mm\\:ss})");
        }

        await Task.Delay(opts.Duration, cts.Token);
        Console.WriteLine($"noise: время истекло ({sw.Elapsed:hh\\:mm\\:ss}) — teardown");
    }
    finally
    {
        foreach (var contour in contours)
        {
            try
            {
                await contour.DisposeAsync();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"noise: teardown контура неполный: {e.Message}");
            }
        }
    }

    Console.WriteLine($"noise: k={opts.Count} profile={opts.Profile} up={sw.Elapsed:hh\\:mm\\:ss} out={opts.OutDir}");
    return 0;
}
catch (ApplicationException e) // usage / невалидные аргументы / teardown-проблемы
{
    Console.Error.WriteLine(e.Message);
    return 2;
}
catch (OperationCanceledException)
{
    // Отмена по времени ИЛИ SIGINT/Ctrl-C (CancelKeyPress → cts.Cancel());
    // teardown контуров уже выполнен в finally — это подтверждение чистоты.
    Console.WriteLine("noise: отменён (SIGINT/Ctrl-C) — teardown выполнен в finally");
    return 0;
}
```

- [ ] **Step 5: Сборка**

Run: `dotnet build src/tools/E2eLoadGen -c Release`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Smoke-прогон профиля cpu (20 с)**

```bash
dotnet run --project src/tools/E2eLoadGen -c Release -- -k 1 -p cpu -d 0.3 -o /tmp/pgw-noise-smoke 2>&1 | tee /tmp/pgw-t24-loadgen-smoke.log
```

Во время (параллельный шелл): `docker ps --format '{{.Names}}' | grep pgw-noise-` → 2 контейнера. После: `docker ps -a --format '{{.Names}}' | grep -c pgw-noise-` → `0`; `docker network ls --format '{{.Name}}' | grep -c pgw-noise-` → `0`; в логе — итоговая строка `noise: k=1 profile=cpu …`.

- [ ] **Step 7: Smoke-профили dns, io и full по 0.3 мин тем же способом**

Ожидания: dns — 2 контейнера, в out-каталоге `container-*-dns1.log` (имя контейнера содержит `-dns`) со строками `storm-dns,…`; io — 1 контейнер, в bind-каталоге после dispose пусто; full — 5 контейнеров. Хост чист после каждого.

- [ ] **Step 8: Commit**

```bash
git add src/tools/E2eLoadGen
git commit -m "feat(t24): диагностический нагрузочный генератор E2eLoadGen — K шумовых guid-контуров, профили dns/cpu/io/full, own-only teardown (spec §5.1)"
```

**Проверка задачи:** smoke четырёх профилей зелёные, хост чист после каждого (изоляция соблюдена даже для диагностического инструмента).
**Связь со spec:** §5.1, §2 (вторичная цель «воспроизводимость»), §7 (генератор — по канону изоляции; временный).

---

### Task 4: Фаза 0 — базлайны серии (N=3 и N=1, без соседей)

**Вход:** Tasks 0–3 (журнал, чистая конфигурация параллелизма, зонд+генератор готовы). Хост чист; dev-стенд опущен (если поднят — зафиксировать в журнале как условие прогона).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (две строки «Сводки прогонов», наблюдения).

**Interfaces:**
- Consumes: «Операционная модель прогонов» (Global Constraints).
- Produces: журнал с базлайнами — эталон времени/зелёности для сравнения в фазах 1–2; фактическое число фактов серии (spec говорит 26 — сверить с фактом живого прогона); наблюдение фактического параллелизма N=3 (AC4: «поведение раннера подтверждено НАБЛЮДЕНИЕМ прогона», не чтением конфига).

**Выход:** «тихое» состояние серии зелёное и измеренное (spec фаза 0).

- [ ] **Step 1: Гипотезы в журнал ДО запуска**

`journal.md` → «Сводка прогонов», две строки: прогон №1 (N=3, K=0): гипотеза «серия в тишине зелёная, ~37–40 мин; пик живых pgw-en-сетей = 3»; прогон №2 (N=1, K=0): гипотеза «зелёная, дольше N=3 (последовательные классы), пик сетей = 1».

- [ ] **Step 2: Прогон №1 — N=3 (канон)**

По «Операционной модели»: гейт чистоты → запуск полной серии (без `-p:`) с `PGW_TEST_E2E_DNS_PROBE=1` (телеметрия резолва на каждом прогоне — бесплатно) → поллинг хвоста лога раз в ~25 с, фиксировать пик `docker network ls --format '{{.Name}}' | grep -c '^pgw-en-'` → итог в журнал (зелёность, Total duration, пик сетей, число фактов из итоговой строки) → гейт чистоты.

- [ ] **Step 3: Прогон №2 — N=1**

Та же процедура с `-p:XunitMaxParallelThreads=1`. Сравнение времени с N=3 — в журнал (базлайн «последовательного» режима из симптома А: «последовательно по классам — зелёные»).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): фаза 0 — базлайны полной серии N=3/N=1 в тишине (зелёность, время, пик контуров)"
```

**Проверка задачи:** обе серии зелёные; в журнале оба эталона; если ЛЮБАЯ серия упала — разбор по телеметрии без перезапуска (канон e2e-launch §4), причины в журнал, зачистка stop-остатков, повтор только после анализа и с гипотезой (spec §9: спорный результат повторяется один раз).
**Связь со spec:** §6 фаза 0, AC4 (наблюдение N=3), AC6-база.

---

### Task 5: Фаза 1 — матрица воспроизведения (симптомы А и Б, H4)

**Вход:** Task 4 (базлайны). Инструменты готовы (Tasks 2–3).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (строки прогонов, промежуточные вердикты).

**Interfaces:**
- Consumes: генератор (CLI из Task 3), зонд (Task 2), базлайны Task 4.
- Produces: устойчиво воспроизведённые (или опровергнутые — тоже результат, spec §9) симптомы А и Б с измерениями; данные для различения H1–H4 (Task 6); фиксация K «тяжёлого соседа».

Определение «тяжёлого соседа» (управляемая переменная): генератор `-p full -k 3 -d <время серии по базлайну + 20 мин>` — эквивалент соседнего docker-тяжёлого прогона (мерж-гейта) по совокупной нагрузке на демона (до 3 живых контуров). Если N=3 + сосед K=3 НЕ воспроизводит симптом А — K поднимается ступенчато (5, 7), каждая попытка — строкой в журнал (это эмпирика порога H4, не «подгонка»).

**Выход:** матрица прогонов (spec §6 фаза 1 п.1–5) выполнена; каждый прогон различает гипотезы одной переменной (N, либо K, либо профиль).

- [ ] **Step 1: Прогон №3 — (N=3, K=3 full) — воспроизведение симптома А**

Гипотеза в журнал ДО: «при тяжёлом соседе серия N=3 деградирует: рост времени фаз (`[PHASE]` elapsed), фейлы/подвисания на Restore/WalStream-сценариях, в DNS-зонде — рост латентности/ошибок». Запуск: сосед в фоне собранным бинарём (по «Операционной модели», `-k 3 -p full`), затем серия N=3 (с `PGW_TEST_E2E_DNS_PROBE=1`). Поллинг ОБОИХ логов. По завершении серии: журнал (зелёность, время vs базлайн N=3, суммарный пик сетей pgw-en-+pgw-noise-, агрегаты storm-лога соседа: fails/avg/max) → разбор фейлов по телеметрии при наличии → остановить соседа `kill -INT $(pgrep -f 'E2eLoadGen.dll')` и дождаться подтверждённого teardown (итоговая строка `noise:` в логе генератора либо пустой `docker network ls | grep pgw-noise-`) → гейт чистоты.

- [ ] **Step 2: Прогон №4 — (N=5, K=0) — воспроизведение симптома Б**

Гипотеза ДО: «N=5 без соседей воспроизводит DNS-деградацию: `getaddrinfo`-фейлы в логах PG-нод и/или зонде, мгновенные ~0,3 с фейлы коннекта, время серии ≥ базлайна N=3 (ускорения нет)». Серия с `-p:XunitMaxParallelThreads=5`, зонд включён. Ожидаемо 25/26 зелёные + WalStream-падение (симптом Б) — это прогон с гипотезой, не «перезапуск упавшего»: падения разбираются по телеметрии, MarkFailed-остатки зачищаются после разбора по README-cleanup.

- [ ] **Step 3: Прогон №5 — (N=2, K=3 full) — проверка H4 (порог от суммарной нагрузки)**

Гипотеза ДО: «если решает СУММАРНАЯ нагрузка (H4), N=2 + сосед K=3 (суммарно ~5 контуров) деградирует аналогично прогону №4; если решает N самой серии — N=2+сосед устойчив». Серия `-p:XunitMaxParallelThreads=2` + сосед как в Step 1. Сравнение с прогонами №3/№4 в журнале.

- [ ] **Step 4: DNS-кривая — резолв от числа сетей без PG (H1 в чистом виде)**

Без серии: 4 ступени генератором dns-профиля, каждая — изолированный вызов (гейт чистоты между ступенями). K — число ДОПОЛНИТЕЛЬНЫХ сетей сверх контура зонда (ступень 0 — только зонд+target):

```bash
for K in 0 1 3 5 7; do
  dotnet run --project src/tools/E2eLoadGen -c Release -- \
    -k $((K+1)) -p dns -d 2 -o /tmp/pgw-t24-dnscurve-k$K 2>&1 | tee /tmp/pgw-t24-dnscurve-k$K.log
done
```

(каждая сеть dns-профиля содержит свой target+storm — резолв-нагрузка и число живых сетей растут вместе: паттерн «0/1/3/5/7 живых сетей» из spec §6 фазы 1 п.5.) Метрики ступени: `grep -h storm-dns /tmp/pgw-t24-dnscurve-k$K/container-*.log | tail -12` — ряды `iterations,fails,avg_ms,max_ms`. Таблица в журнал: K → fails, avg_ms, max_ms. Гипотеза ДО: «H1 подтверждается, если fails/латентность растут монотонно с K уже без PG; H1 слабеет, если кривая плоская до ~8 сетей».

- [ ] **Step 5: Сводка фазы 1 в журнал**

Заполнить по прогонам №3–№5 и DNS-кривой колонки «карты гипотез» ПРЕДВАРИТЕЛЬНЫМИ вердиктами (окончательные — Task 6 после изолирующих экспериментов). Записать фактический K тяжёлого соседа (воспроизвёл/не воспроизвёл симптом А; если не воспроизвёл ни при 3/5/7 — риск §9 «невоспроизводимость» фиксируется как результат: канон будет опираться на симптом Б и профилактические рельсы).

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): фаза 1 — матрица воспроизведения: N3+сосед, N5, N2+сосед, DNS-кривая без PG (симптомы А/Б, H4)"
```

**Проверка задачи:** 3 серийных прогона + DNS-кривая в журнале; каждая строка — с гипотезой ДО; хост чист после каждого.
**Связь со spec:** §6 фаза 1 (п.1–5; прогон №1 матрицы = базлайн Task 4), AC1.

---

### Task 6: Фаза 2 — различение гипотез H1–H4 и карта «симптом → причина → измерение»

**Вход:** Task 5 (матрица фазы 1 с телеметрией: артефакты `/tmp/pgw-e2e-artifacts-*` упавших, `[PHASE]`-тайминги, DNS-зонды всех прогонов).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (карта гипотез — окончательные вердикты; оценка границ причины и стоимости митигации).

**Interfaces:**
- Consumes: телеметрия прогонов №1–№5 (docker-логи, host.log, зонды, `[PHASE]`), генератор.
- Produces: для H1–H4 строки «следствие → измерение → подтверждено/исключено» со ссылками на артефакты (AC2); вердикт «устранимо в наших границах или ограничение Docker Desktop/хоста»; оценка стоимости митигации — вход гейта Task 7.

**Выход:** карта причин-следствий с измерениями — аргументационная база решения гейта (spec §6 фаза 2).

- [ ] **Step 1: H1 — где именно умирает соединение**

По артефактам упавших прогонов №3/№4: сверка DNS-зонда окружения (`container-pgw-dns-*.log`) с логами PG-нод (`getaddrinfo returns an empty list`) и transient-циклами воркеров (`host-*.log`): фейлы на этапе РЕЗОЛВА (зонд фиксирует fail в те же секунды) vs TCP-коннект/хендшейк (зонд зелёный, коннект падает). Вывод в карту: H1 подтверждён/исключён + ссылки на файлы артефактов. Дополнительный аргумент — DNS-кривая Task 5 Step 4.

- [ ] **Step 2: H2 — CPU/IO без DNS-шума**

Изолирующий прогон №6: сосед из двух генераторов БЕЗ dns-профиля (`-p cpu -k 3` и `-p io -k 3` параллельно) при серии N=3. Гипотеза ДО: «если H2 — чистая CPU/IO-нагрузка воспроизводит фейлы коннекта при зелёном DNS-зондe; если фейлов нет — H2 исключается как самостоятельная причина симптомов». Во время серии поллить `docker stats --no-stream` и `uptime` — корреляция фейлов с пиком нагрузки в журнал.

- [ ] **Step 3: H3 — «подвисание» = транзиент-циклы против бюджетов**

По журналам прогонов №3/№5 (симптом А): есть ли на «подвисших» фазах движение между ретраями (лог воркера: повторяющиеся попытки с шагом ErrorDelayMs=500) и строка `[PHASE] … elapsed=…` при фейле по бюджету — то есть прогон НЕ висит, а ретраится до бюджета. Измерение: по `host-*.log` доля времени фазы в ретраях vs прогресс. Вердикт в карту.

- [ ] **Step 4: H4 — суммарная нагрузка vs N серии**

Сводка по прогонам №3 (N3+K3), №4 (N5+K0), №5 (N2+K3), №6 (N3+cpu/io-сосед): при каком (N, K, профиль) деградация появляется/исчезает. Если порог зависит от суммы контуров независимо от их принадлежности — H4 подтверждён.

- [ ] **Step 5: Вердикт границ и стоимость митигации**

В журнал: (а) причина локализована в наших границах (код тестов/конфигурация контуров/имена-vs-DNS) или в Docker Desktop/хосте (vpnkit/резолвер); (б) кандидаты митигаций (если есть) с оценкой «дёшево/дорого» (по spec §5.6 каждый кандидат потом проходит свой мини-эксперимент; здесь — только оценка для гейта); (в) фиксация: выгоды в скорости нет (N=5 измерено медленнее N=3), выгода возможна только в устойчивости.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): фаза 2 — карта гипотез H1-H4 (следствие→измерение→вердикт), границы причины, оценка митигации"
```

**Проверка задачи:** в карте H1–H4 нет пустых вердиктов; каждый вердикт ссылается на артефакт/прогон; ни один новый прогон не запущен без гипотезы в журнале.
**Связь со spec:** §6 фаза 2, AC2, вход ГЕЙТА.

---

### Task 7: ГЕЙТ РЕШЕНИЯ — ветка «устранить» vs «канон N=3 + рельсы»

**Вход:** Task 6 (карта гипотез полная).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (секция «Гейт решения»).

**Interfaces:**
- Consumes: карта Task 6, критерии spec §6 «ГЕЙТ РЕШЕНИЯ».
- Produces: зафиксированное решение (ветка А или Б) с критериями и измерениями — определяет дальнейшее (Tasks 8–9 для А; для Б — остановка и возврат к планированию).

**Выход:** AC3 закрыт: выбор ветки не молчаливый, в журнале, с опорой на измерения.

- [ ] **Step 1: Применить критерии к карте**

В «Гейт решения» журнала записать по критериям spec §6: (1) причина в наших границах? (из Task 6 Step 5-а); (2) митигация дёшева? (Step 5-б); (3) есть ли измеримый выигрыш N>3? (нет — измерено: 58м59с/26 фактов при N=5 против 37м37с/24 при N=3).

- [ ] **Step 2: Зафиксировать решение**

- **Ветка А (канон N=3 + рельсы)** — если причина = ограничение Docker Desktop/хоста, ИЛИ митигация дороже выгоды, ИЛИ N>3 не даёт выигрыша. Записать: «РЕШЕНИЕ: ветка А» + обоснование по пунктам Step 1 + судьба инструментов (генератор — удалить; зонд — остаётся, если его данные пригодились в разборе, иначе тоже удалить). → исполнять Tasks 8, 9, 10, 11.
- **Ветка Б (устранение)** — только если ВСЕ три: причина в наших границах, митигация дёшева, устойчивость при N>3 доказуема. Записать: «РЕШЕНИЕ: ветка Б» + состав митигаций из карты. → Step 3.
- Критерии неоднозначны → вернуть контролёру флоу `{status: NEEDS_CONTEXT, question: "Гейт t24: критерии spec §6 не дают однозначного ответа (детали в journal.md §Гейт решения). Какую ветку выбираем?", options: ["Ветка А — канон N=3 + рельсы", "Ветка Б — устранение (состав митигаций — из карты гипотез журнала)"]}`; ответ пользователя — дословно в журнал; далее по выбранной ветке.

- [ ] **Step 3: (Только ветка Б) Остановка и возврат к планированию**

Состав митигаций определяется результатами фазы 2 и НЕ известен на момент написания этого плана (spec §5.6: «состав определяется фазой 2. Каждый кандидат проходит через свой мини-эксперимент»). Действие: зафиксировать в журнале состав митигаций и СТОП — через контролёра флоу вынести пользователю вопрос о составлении отдельного мини-плана (mini-spec → plan) ветки Б; Tasks 8–9 этого плана пропускаются, Task 10 исполняется по mini-плану, Task 11 — общий. НЕ импровизировать митигации внутри этого плана.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): гейт решения — ветка <А|Б> зафиксирована по критериям spec §6 (AC3)"
```

**Проверка задачи:** секция «Гейт решения» заполнена полностью (критерии → измерения → решение [+ вопрос/ответ при неоднозначности]).
**Связь со spec:** §6 ГЕЙТ РЕШЕНИЯ, AC3.

---

### Task 8: (Ветка А) Рельса канона — громкое предупреждение при превышении фактического параллелизма

**Вход:** Task 7 = ветка А. Канон: ≤3 контура; жёсткий запрет НЕ вводим — диагностические прогоны с N>3 легальны, но громки (spec §6 фаза 3 ветвь А, AC7).

**Действие (Files):**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eParallelismGuard.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (вызовы guard при старте/dispose окружения)

**Interfaces:**
- Consumes: —
- Produces: `internal static class E2eParallelismGuard` c `internal const int CanonMaxLiveEnvironments = 3;`, `public static int OnEnvironmentStarted(string slug)` (инкремент живых окружений процесса; при превышении канона — ОДНО на процесс громкое предупреждение `[E2E-PARALLELISM]` в stderr + `/tmp/pgw-e2e-static-phase.log`), `public static void OnEnvironmentDisposed()`. Рельса меряет ФАКТ (число одновременно живых контуров), а не декларацию конфига — срабатывает при любом способе задания N.

**Выход:** прогон серии с фактическим N выше канона маркирует себя в журнале прогона (AC7).

- [ ] **Step 1: E2eParallelismGuard.cs**

```csharp
namespace PgWorker.IntegrationTests.E2e;

/// <summary>Рельса канона параллелизма E2E (t24, docs/e2e-launch.md §5):
/// потолок — 3 одновременно живых контура. Считает ФАКТИЧЕСКИ живые окружения
/// процесса: при превышении канона один раз на процесс пишет громкое
/// предупреждение в stderr и /tmp/pgw-e2e-static-phase.log. Жёсткий запрет
/// не вводится — диагностические прогоны с N&gt;3 легальны, но обязаны быть
/// громкими.</summary>
internal static class E2eParallelismGuard
{
    internal const int CanonMaxLiveEnvironments = 3;

    private static int _live;
    private static int _warned;

    /// <summary>Вызывается E2eEnvironment после успешного подъёма окружения.
    /// Возвращает текущее число живых окружений (диагностика стартовых строк).</summary>
    public static int OnEnvironmentStarted(string slug)
    {
        var live = Interlocked.Increment(ref _live);
        if (live > CanonMaxLiveEnvironments && Interlocked.Exchange(ref _warned, 1) == 0)
        {
            var line = $"{DateTime.UtcNow:HH:mm:ss} [E2E-PARALLELISM] e2e[{slug}]: живых E2E-контуров {live} > канона"
                + $" {CanonMaxLiveEnvironments} (XunitMaxParallelThreads / docs/e2e-launch.md «Параллелизм и нагрузка»)"
                + " — прогон ВНЕ канона: вероятна деградация docker-хоста (DNS/сети); результаты требуют этой пометки.";
            Console.Error.WriteLine(line);
            try
            {
                File.AppendAllText("/tmp/pgw-e2e-static-phase.log", line + Environment.NewLine);
            }
            catch
            {
                // файловая телеметрия — «лучшими усилиями», не роняет серию
            }
        }

        return live;
    }

    /// <summary>Вызывается E2eEnvironment.DisposeAsync при любом исходе.</summary>
    public static void OnEnvironmentDisposed()
        => Interlocked.Decrement(ref _live);
}
```

- [ ] **Step 2: Врезка вызовов в E2eEnvironment**

В `StartOnceAsync` перед `return new E2eEnvironment(...)` (успешный подъём; неудачные попытки ретрая `StartAsync` до return не доходят — инкремент только за живые окружения):

```csharp
var liveEnvironments = E2eParallelismGuard.OnEnvironmentStarted(slug);
Console.WriteLine($"[PHASE] e2e-env {slug}: окружение поднято (живых контуров процесса: {liveEnvironments})");
```

В `DisposeAsync` — первой строкой тела (декремент при любом исходе, до возможных исключений teardown):

```csharp
E2eParallelismGuard.OnEnvironmentDisposed();
```

- [ ] **Step 3: Сборка**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: 0 errors, 0 warnings.

- [ ] **Step 4: Проверка рельсы прогоном с N=5 (AC7)**

5 классов по 1–4 факта, по «Операционной модели» (ВНИМАНИЕ: пик живых контуров >3 — вероятностное перекрытие окружений по времени, для лёгких классов НЕ гарантирован — молчание рельсы без достигнутого пика не означает её поломку):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 \
  dotnet test src/PgWorker.slnx -c Release -p:XunitMaxParallelThreads=5 \
  --filter "FullyQualifiedName~E2eAppParams|FullyQualifiedName~E2eAppSecret|FullyQualifiedName~E2eRotate|FullyQualifiedName~E2ePgtune|FullyQualifiedName~E2eRetention" \
  --logger "console;verbosity=detailed" 2>&1 | tee /tmp/pgw-t24-railsmoke.log
```

Expected — двухчастный критерий (отличает «рельса сработала» от «пика не было»):
1. Пик достигнут: в логе есть стартовые строки `[PHASE] e2e-env … (живых контуров процесса: N)` хотя бы с одним `N > 3`. Если все стартовые строки ≤ 3 — пик не случился: повторить прогон с расширенным фильтром, добавив тяжёлые классы с длинным временем жизни окружений (`|FullyQualifiedName~E2eMove|FullyQualifiedName~E2eBackup` — их окружения живут минуты, перекрытие ≥4 контуров достигается надёжно); молчание рельсы при недостигнутом пике фейлом рельсы НЕ является.
2. При достигнутом пике: `grep -c "E2E-PARALLELISM" /tmp/pgw-t24-railsmoke.log` ≥ 1; предупреждение также в `/tmp/pgw-e2e-static-phase.log`.

Это диагностический прогон вне канона (легален, громок — что и проверяем); зелёность желательна, но не критерий шага; при падении сценариев — разбор по телеметрии без перезапуска + зачистка stop-остатков, факт в журнал.

- [ ] **Step 5: Контрольная проверка молчания рельсы в каноне**

Прогон тех же 5 классов с `-p:XunitMaxParallelThreads=3`: `grep -c "E2E-PARALLELISM" <лог>` → `0` (пик ≤ 3 — рельса молчит; ложных срабатываний нет).

- [ ] **Step 6: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eParallelismGuard.cs src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs
git commit -m "feat(t24): рельса канона N=3 — E2eParallelismGuard: громкое [E2E-PARALLELISM]-предупреждение при фактическом превышении живых контуров (AC7)"
```

**Проверка задачи:** Step 4 — пик живых контуров > 3 ДОСТИГНУТ (по стартовым строкам `[PHASE] e2e-env … живых контуров …`) И предупреждение `[E2E-PARALLELISM]` есть; Step 5 — молчит в каноне.
**Связь со spec:** §6 фаза 3 ветвь А, AC7.

---

### Task 9: (Ветка А) Канонизация — раздел «Параллелизм и нагрузка» в docs/e2e-launch.md + согласование csproj

**Вход:** Task 8 (рельса live); решение гейта Task 7 (N канона = 3).

**Действие (Files):**
- Modify: `docs/e2e-launch.md` (новый раздел §5 перед «Где что лежит после прогона»)
- Modify: `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` (комментарий над PropertyGroup)

**Interfaces:**
- Consumes: измерения журнала (базлайны, матрица), `E2eParallelismGuard` (Task 8).
- Produces: канон прогона серии целиком (AC5): потолок N, правило соседних прогонов, признаки деградации, действия при фейле — согласованный с csproj.

**Выход:** канон «как гонять серию» описан в каноническом документе запуска; комментарий csproj ссылается на него (число N — единственный источник истины csproj, толкование — docs).

- [ ] **Step 1: Раздел в docs/e2e-launch.md**

Перед секцией «Где что лежит после прогона» вставить (приведённые числа — измерения spec/journal; если журналом получены другие — подставить фактические из `journal.md`):

```markdown
## 5. Параллелизм и нагрузка (канон прогона серии)

Потолок параллелизма полной E2E-серии — **3 контура**
(`XunitMaxParallelThreads=3` в
`src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` —
единственный источник истины). Выше — docker-хост деградирует: DNS-резолв
внутри сетей отдаёт пустые ответы (`getaddrinfo returns an empty list` в
логах PG-нод), коннекты стрим-клиентов падают мгновенно (~0,3 с) в
transient-цикле; ускорения нет (N=5 медленнее N=3 — измерения в журнале
задачи t24 в `docs/superpowers/`).

Правила соседней нагрузки:

- суммарное число одновременно живых E2E-контуров на docker-хосте — ≤3,
  С УЧЁТОМ соседних серий, диагностических контуров и поднятого dev-станда
  (стенд = минимум один «контур» нагрузки);
- серию не запускать поверх другого тяжёлого docker-прогона (мерж-гейты,
  интеграционные серии соседних задач) — дождаться завершения;
- рельса: `E2eEnvironment` при фактическом превышении канона пишет в журнал
  прогона громкое предупреждение `[E2E-PARALLELISM]` (жёсткий запрет не
  вводится — диагностические прогоны с N>3 легальны, но обязаны быть
  громкими; пример запуска: `dotnet test … -p:XunitMaxParallelThreads=5`).

Признаки деградации (что смотреть в логах):

- `getaddrinfo returns an empty list` в docker-логах PG-нод;
- мгновенные (~0,3 с) фейлы коннекта в transient-циклах воркера
  (`host-*.log`);
- рост `[PHASE] … elapsed` у фаз, которые в тишине проходят быстро;
- рост fails/латентности в логах DNS-зонда (`container-pgw-dns-*.log`,
  включается `PGW_TEST_E2E_DNS_PROBE=1`).

Действия при фейле серии под нагрузкой:

- разбор по телеметрии teardown, БЕЗ перезапуска (§4): локализовать этап
  смерти соединения — резолв / TCP-коннект / хендшейк (DNS-зонд + docker-логи
  + host.log);
- снизить суммарную нагрузку (N, соседние прогоны, стенд) и повторять
  ТОЛЬКО после полного анализа и с сформулированной гипотезой (§4).
```

- [ ] **Step 2: Комментарий csproj — согласование с docs**

Заменить комментарий над PropertyGroup в `PgWorker.IntegrationTests.csproj` на:

```xml
    <!-- E2E-классы изолированы каноном (guid-контуры, own-only teardown) —
         коллекции гоняются параллельно. Потолок 3 контура: docker-хост
         деградирует выше (DNS/сети) — канон и правила соседней нагрузки:
         docs/e2e-launch.md §5 «Параллелизм и нагрузка» (t24). «Тихие»
         классы с общими ассетами при конфликте — в [Collection]-группу. -->
```

- [ ] **Step 3: Проверка согласованности**

Run: `grep -c "Параллелизм и нагрузка" docs/e2e-launch.md` → `1`; `grep -c "e2e-launch.md" src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` → `1`; `grep -rc "CollectionBehavior" src/tests` → `0` по всем файлам (атрибут не вернулся).

- [ ] **Step 4: Commit**

```bash
git add docs/e2e-launch.md src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj
git commit -m "docs(t24): канон прогона серии — раздел «Параллелизм и нагрузка» в e2e-launch.md (потолок N=3, соседи, признаки, действия) + согласование csproj (AC5)"
```

**Проверка задачи:** раздел существует; csproj ссылается на него; `CollectionBehavior` отсутствует в src/tests.
**Связь со spec:** §5.5, §6 фаза 3 ветвь А, AC5.

---

### Task 10: Фаза 4 — приёмка: финальные контрольные прогоны и судьба инструментов

**Вход:** Tasks 8–9 (ветка А) или mini-план ветки Б (Task 7 Step 3); все митигации/рельсы закоммичены.

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (итоговые прогоны приёмки)
- Delete: `src/tools/E2eLoadGen/` (обе ветки — временный инструмент; код доступен в git-истории ветки, коммит фиксируется в журнале)
- (Только при подтверждённом вкладе H3) Modify: `src/tests/PgWorker.IntegrationTests/E2e/*.cs` — точечные уточнения телеметрии/бюджетов медленных фаз

**Interfaces:**
- Consumes: канон Task 9, рельса Task 8, журнал.
- Produces: приёмочные прогоны AC6 (для ветки А — ДВЕ последовательные зелёные серии: устойчивость, а не удача).

**Выход:** финальная приёмка: серия(и) на свежем Release по канону зелёные, хост чист, инструменты-диагностики покинули ветку (кроме зонда, если решено оставить).

- [ ] **Step 1: (Ветка А) Две последовательные зелёные серии по канону**

По «Операционной модели», подряд, между ними — только гейт чистоты хоста (никаких правок кода): серия A1 (N=3 канон, `PGW_TEST_E2E_DNS_PROBE=1`), гейт чистоты, серия A2 (та же команда). Обе зелёные; время каждой + сравнение с базлайном Task 4 — в журнал. Любой фейл — разбор по телеметрии без перезапуска; повтор серии допустим только после анализа и с гипотезой (spec §9: один повтор).

- [ ] **Step 2: (Ветка Б) Матрица при новом N**

По mini-плану ветки Б: серия без соседей + серия с соседом при новом N — обе зелёные; канон в docs/csproj обновлён на новое N (правки `docs/e2e-launch.md` §5 и csproj-комментария — те же шаги, что Task 9, с новым числом). Итоги в журнал.

- [ ] **Step 3: Точечные уточнения H3 (только если вклад H3 подтверждён в Task 6)**

Каждое уточнение бюджета/телеметрии медленной фазы — отдельный коммит с объяснением ПО ТЕЛЕМЕТРИИ (какая фаза, почему бюджет корректен: прогресс в ретраях виден, бюджет покрывает p100 фазы из журналов). Запрет «просто увеличить таймаут» не снимается (spec §3.3, §6). Если вклад H3 не подтверждён — шаг пропускается с записью в журнал «вклад H3 не подтверждён — правки бюджетов не требуются».

- [ ] **Step 4: Удаление генератора из ветки**

```bash
git rm -r src/tools/E2eLoadGen
dotnet build src/PgWorker.slnx -c Release
```

Expected: удаление ок; сборка решения зелёная (генератор в slnx не входил). В journal.md → «Гейт решения»: пометка «генератор удалён коммитом <hash> (код — в истории ветки)». DNS-зонд: судьба по решению гейта Task 7; если «удаляется» — `git rm src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py src/tests/PgWorker.IntegrationTests/E2e/E2eDnsProbe.cs` + откат врезки зонда из `E2eEnvironment` (блок `if (…PGW_TEST_E2E_DNS_PROBE…)`, поле/параметр конструктора/dispose) + `dotnet build src/PgWorker.slnx -c Release` зелёная.

- [ ] **Step 5: Гейт чистоты хоста — финальный**

`docker ps -a --format '{{.Names}}' | grep -c pgw-` → `0`; `docker network ls --format '{{.Name}}' | grep -E 'pgw-en-|pgw-noise-|kfw-net' | wc -l` → `0`; `docker volume ls --format '{{.Name}}' | grep -c pgw-` → `0`; `docker network prune -f`. Фиксация в журнал («хост чист после приёмки»).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(t24): приёмка — финальные прогоны по канону в журнале, генератор удалён из ветки (AC6)"
```

**Проверка задачи:** AC6 закрыт прогонами (ветка А: 2 зелёные серии подряд; ветка Б: матрица зелёная при новом N); хост чист; `src/tools/E2eLoadGen` не существует.
**Связь со spec:** §6 фаза 4, AC6, §7 (генератор не остаётся в main).

---

### Task 11: Мерж-гейт трека — снятие t24 из roadmap и отчёта надёжности

**Вход:** Task 10 (приёмка закрыта). Правки готовятся здесь; попадают в main мерж-коммитом ветки (контролёр флоу) — по правилу AGENTS.md «тем же коммитом».

**Действие (Files):**
- Modify: `arch/roadmap/reliability.md` (удалить пункт `t24-e2e-suite-under-load`; проверить `← t24`-зависимости других пунктов — на момент плана их нет, но проверить grep'ом)
- Modify: `arch/roadmap/reliability-report.md` (строку `t24-e2e-suite-under-load` из таблицы «Осталось» удалить; добавить строку в «Сделано в рамках трека»)

**Interfaces:**
- Consumes: журнал (вердикт гейта, итоги приёмки) — источник формулировок.
- Produces: roadmap и отчёт синхронны с закрытой задачей (AC8).

**Выход:** тег t24 снят тем же мерж-коммитом; исторические пассажи о задаче в AGENTS/arch/docs не оставляются (история — git и `docs/superpowers/`).

- [ ] **Step 1: reliability.md — удалить пункт t24 и проверить зависимости**

Удалить блок списка, начинающийся `- **\`t24-e2e-suite-under-load\`** —` (устойчивость полной E2E-серии к параллельной нагрузке, канон `XunitMaxParallelThreads=3`). Run: `grep -c "t24" arch/roadmap/reliability.md` → `0`.

- [ ] **Step 2: reliability-report.md — перенос строки**

Из таблицы «Осталось» удалить строку `| \`t24-e2e-suite-under-load\` | устойчивость полной E2E-серии к параллельной нагрузке | P3 | N |`. В «Сделано в рамках трека» добавить строку по образцу строки t27 — вариант для ветки А (вместо фрагментов в ‹› — фактические формулировки из journal.md: вердикт фазы 2 по границам причины):

```markdown
| `t24-e2e-suite-under-load` | — (мерж-коммит t24-e2e-suite-under-load) | устойчивость полной E2E-серии к параллельной нагрузке (характеристика N): симптомы воспроизведены управляемым нагрузочным генератором, причина локализована как ‹вердикт фазы 2: ограничение Docker Desktop/хоста — DNS-резолвер деградирует при росте параллельных сетей ИЛИ иная локализация из journal.md›; принят канон прогона N=3 с рельсами — единственный источник истины csproj (устаревший CollectionBehavior-атрибут удалён), громкое предупреждение [E2E-PARALLELISM] при фактическом превышении живых контуров, раздел «Параллелизм и нагрузка» в docs/e2e-launch.md (потолок, соседи, признаки деградации, действия при фейле), опциональный DNS-зонд PGW_TEST_E2E_DNS_PROBE=1 в телеметрии окружения; приёмка — две последовательные зелёные серии по канону, хост чист |
```

Для ветки Б — та же строка с заменой середины на реализованные митигации и новый канон N (из mini-плана ветки Б и журнала). Проверить сводку по характеристике N в начале отчёта: если она упоминала неустойчивость серии — дополнить фактом закрытия (только текущее состояние, без истории).

- [ ] **Step 3: Проверка синхронности**

Run: `grep -c "t24-e2e-suite-under-load" arch/roadmap/reliability-report.md` → `1` (только строка в «Сделано»); `grep -rc "t24" arch/roadmap/` → `0` везде, кроме строки «Сделано» отчёта.

- [ ] **Step 4: Commit (уходит мерж-коммитом ветки)**

```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "roadmap(t24): пункт снят из reliability.md, отчёт — строка в «Сделано» (мерж-гейт трека, AC8)"
```

**Проверка задачи:** t24 отсутствует в списках «Осталось» обоих документов; присутствует один раз в «Сделано» отчёта.
**Связь со spec:** §6 фаза 4, §8 AC8; правило мерж-гейта roadmap из AGENTS.md.

---

## Самопроверка плана (итог self-review)

- **Покрытие spec:** §5.1 генератор → Task 3; §5.2 зонд → Task 2; §5.3 матрица → Tasks 4–5; §5.4 единый источник истины → Task 1; §5.5 раздел docs → Task 9; §5.6 митигации → Task 7 Step 3 (ветка Б, mini-план: состав определяется фазой 2, импровизация в плане запрещена); фаза 0 → Task 4; фаза 1 → Task 5; фаза 2 → Task 6; гейт → Task 7; фаза 3 ветвь А → Tasks 8–9 (H3-уточнения — Task 10 Step 3, обе ветви); фаза 4 → Task 10; мерж-гейт → Task 11. AC: AC1→Task 5, AC2→Task 6, AC3→Task 7, AC4→Tasks 1+4, AC5→Task 9, AC6→Task 10, AC7→Task 8, AC8→Task 11. Ограничения §7 — в Global Constraints; риски §9 — в Tasks 4/5/10 (повтор один раз, невоспроизводимость = результат).
- **Развилка веток:** гейт (Task 7) — единственная точка выбора; ветка Б уходит в отдельный mini-план (состав митигаций по spec §5.6 определяется фазой 2 — в плане процедура, а не выдуманный состав); при неоднозначности — возврат `NEEDS_CONTEXT` контролёру.
- **Типы/имена:** `LoadGenOptions.Parse` / `NoiseContour.CreateAsync(int index, string profile, string outDir, CancellationToken ct)` (Task 3) согласованы с использованием в Task 5; `E2eDnsProbe.Build(runId, net, targets, mode, intervalSec)` и имя `pgw-dns-{runId}` — между Steps 3 Task 2 и телеметрией/teardown окружения; `E2eParallelismGuard.OnEnvironmentStarted/OnEnvironmentDisposed` — между Steps 1–2 Task 8; скрипт `dns_probe.py` и его env-контракт (`DNS_PROBE_TARGETS/MODE/INTERVAL`) едины для E2E (measure) и генератора (storm); состав целей — ТРИ категории spec §5.2 (алиас сети, `host.docker.internal`, внешнее имя `quay.io`) в обоих потребителях (врезка Task 2 Step 3 и `stormTargets` Task 3 Step 3).
- **Операционные механики:** фоновый сосед запускается собранным бинарём и останавливается `kill -INT $(pgrep -f 'E2eLoadGen.dll')` (SIGINT → `Console.CancelKeyPress` → teardown в `finally`; Ctrl-C фоновой job не доставляется) — гейт чистоты только после подтверждённого teardown соседа; проверка AC7 — двухчастная: сначала факт пика контуров >3 по стартовым строкам guard'а, затем наличие `[E2E-PARALLELISM]` (молчание без пика — не фейл рельсы).
