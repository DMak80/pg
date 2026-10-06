# t24-e2e-suite-under-load — план реализации (Фаза 3 dev-flow)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** полная docker-E2E-серия PgWorker устойчива к параллельной нагрузке на том же docker-хосте: причина деградации устраняется (сетевые митигации обязательны — per-cluster сети `pgw-net-<C>` + advertise-канон; прочие — по гейту фазы 2), а канон параллелизма выбирается приёмкой на N=3/5/6 («максимально быстрое и стабильное») и фиксируется в runner.json, `docs/e2e-launch.md` и рельсе `E2eParallelismGuard`.

**Архитектура:** три слоя. (1) Конфигурация параллелизма — целевая схема Task 1 (выполнена): xunit.runner.json — единственный источник потолка, не-E2E — последовательная `NonE2eCollection`, атрибут и мёртвые csproj-свойства удалены. (2) Сетевой слой — Task 1d: движок создаёт per-cluster сеть нод `pgw-net-<C>` (kfw-паттерн, константа `pgw-net` уходит, ensure-инвариант «нода и её wal-агенты в одной сети кластера», демонтаж удаляет сеть), E2eEnvironment — advertise без внутрисетевого алиаса (причина NXDOMAIN-бурь Patroni, разбор 4f7b146) + own-only teardown и ассерт чистоты per-cluster сетей. (3) Расследование остатка (фазы 1–2: генератор + DNS-зонд + анализ Patroni-логов) → гейт «прочие митигации» → приёмка фазы 4: три канонические серии N=3/5/6 на целевой конфигурации, победитель по критерию «максимально быстрое И стабильное» тройно фиксируется и подтверждается второй зелёной серией.

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), xUnit v3 3.2.2 (`src/Directory.Packages.props`; конфигурация — xunit.runner.json), Testcontainers 4.14.0, Docker CLI, python:3.12-alpine + alpine:3.20 (зеркалированы), etcd v3.5.21, MinIO.

**Spec:** [`docs/superpowers/2026-10-06-t24-e2e-suite-under-load/spec.md`](spec.md) (ревизии 2026-10-06 (1)–(3): целевая схема параллелизма; сетевой слой per-unit — требование «свой контур НЕ ПО СЕРВИСАМ А ПО ТЕСТАМ»; приёмка N=3/5/6 и выбор канона). Рабочие каноны: `AGENTS.md`, `AGENTS.base.md` §12–13, `docs/e2e-isolation.md`, `docs/e2e-launch.md`; контракты сетевого слоя (arch-first, уже в worktree): `arch/14-pgworker.md` §2.1 (per-cluster сеть `pgw-net-<C>`, ensure-инвариант, демонтаж), `arch/04-deploy-etcd.md` §8 п.3 (advertise — только резолвимые URL). Эмпирика: `journal.md` (прогоны №1–№5, фокус-разбор 4f7b146).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t24-e2e-suite-under-load`, ветка `feat-t24-e2e-suite-under-load`. Все пути — от корня worktree.

**Порядок исполнения:** выполнено — Tasks 0, 1, 1b, 1c, 2, 3, 4 (частично, см. статусы); далее: **Task 1d → 5 → 6 → 7 → 10 → 8 → 9 → 11** (Task 10 выбирает канон N — рельса Task 8 и docs Task 9 исполняются ПОСЛЕ него с выбранным N).

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — 0 warnings в каждой сборке.
- Конфигурация параллелизма (spec §1.3): единственный источник потолка — `src/tests/PgWorker.IntegrationTests/xunit.runner.json` (`maxParallelThreads`; стартовое 3, финальное — приёмка фазы 4, AC10; копируется в output); `CollectionBehavior`-атрибута и `Xunit*`-свойств csproj нет; не-E2E — `[Collection(NonE2eCollection.Name)]` с `DisableParallelization=true`. Канон N — НЕ «≤3 по определению»: выбирается приёмкой из {3,5,6} по критерию «максимально быстрое и стабильное».
- Сетевой слой (spec §1.4, arch/14 §2.1, arch/04 §8 п.3, AGENTS.base §13): per-cluster сети нод `pgw-net-<C>`; константы/общесистемной сети `pgw-net` в коде не остаётся; ensure-инвариант «нода и её wal-агенты в одной сети кластера» (лечение: агент — пересоздание, нода — подключение к сети); advertise etcd — только URL, резолвимые из сетей потребителей (внутрисетевой алиас сети окружения в advertise НЕ включается); per-cluster сети входят в own-only teardown и ассерт чистоты E2E.
- Канон изоляции E2E нерушим (spec §7): guid-контуры, own-only чистка (`OwnName` по runId/тегу), ассерт чистоты, динамические порты, никаких широких фильтров `pgw-*` и глобальных prune из кода. Шумовые контуры генератора — те же правила (`pgw-noise-` + guid).
- Таймауты ожидания агента ≤ 30 с (AGENTS.base §12): длинные прогоны — фоновыми процессами с поллингом хвоста лога. Полл в тестах 500 мс — канон репо. Бюджеты фикстур ≤ 100 с.
- Каждый экспериментальный прогон — с гипотезой, записанной в журнал ДО запуска (spec §3.2); перезапуск упавших запрещён (docs/e2e-launch.md §4); падение — разбор по телеметрии; спорный результат повторяется один раз (spec §9).
- Гейт чистоты хоста перед/последователь КАЖДОЙ серии: `docker ps -a --format '{{.Names}}' | grep pgw-` → пусто; `docker network ls --format '{{.Name}}' | grep -E 'pgw-en-|pgw-net-|pgw-noise-|kfw-net'` → пусто; `docker volume ls --format '{{.Name}}' | grep pgw-` → пусто; страховка `docker network prune -f`. Чужие объекты НЕ трогать (own-only).
- Образы: только зеркалированные (`dev-stand/images/images.txt`); новых внешних не вводить; локальные в registry НЕ класть.
- Число живых user-сетей не подбирается к пулу ~30 (per-cluster сети короткоживущие, удаляются демонтажем; контроль после каждой серии).
- Генератор нагрузки — временный инструмент ветки (удаляется фазой приёмки); DNS-зонд — кандидат остаться (судьба — на гейте Task 7).
- Язык: документация/журнал — русский; идентификаторы — английские; тесты — AAA-комментарии.
- Коммиты — в feature-ветке; мерж-гейт roadmap (Task 11) — тем же мерж-коммитом.

## Операционная модель прогонов (обязательна для Tasks 4, 5, 8, 10)

Полная серия — 37–59 мин; агент НЕ ждёт команду дольше 30 с (AGENTS.base §12). Модель каждого серийного прогона:

1. Записать гипотезу/конфигурацию в `journal.md` (строка «Сводки прогонов»; для новых прогонов — следующий свободный номер; предзаписанные строки №6/№7 журнала — за матрицей Task 5).
2. Гейт чистоты хоста (Global Constraints). Для K=0-прогонов (базлайны/приёмка) — ДОПОЛНИТЕЛЬНО гейт тишины: три контрольных замера `docker ps -a --format '{{.Names}}' | grep -c pgw-` → `0` с интервалом ~30 с (чужие раннеры поднимают контуры циклами — активный проявится между замерами; чужие объекты НЕ трогать, старт отложить, известить контролёра). Условие тишины — в строку журнала.
3. Запуск серии в фон с записью полного лога:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 \
  dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e" \
  --logger "console;verbosity=detailed" \
  2>&1 | tee /tmp/pgw-t24-runNN.log
```

   **Канонический прогон** — без правок конфигурации; перед ним контроль-гейт: `grep -c '"maxParallelThreads": 3' src/tests/PgWorker.IntegrationTests/xunit.runner.json` → `1` (после фиксации выбора в Task 10 — выбранное N).

   **Прогон с иным N** (матрица, приёмка N=3/5/6, проверка рельсы) — временная правка runner.json (MSBuild-свойства xunit.v3 мёртвы):

```bash
sed -i '' 's/"maxParallelThreads": 3/"maxParallelThreads": 5/' src/tests/PgWorker.IntegrationTests/xunit.runner.json
# …прогон (сборка скопирует runner.json в output — PreserveNewest)…
# возврат ОБЯЗАТЕЛЬНО, сразу после прогона (до фиксации выбора Task 10):
git checkout -- src/tests/PgWorker.IntegrationTests/xunit.runner.json
grep '"maxParallelThreads": 3' src/tests/PgWorker.IntegrationTests/xunit.runner.json
```

   Прогон с переопределённым потолком помечается в журнале; для приёмки фазы 4 серии N=3/5/6 — «полноправные канонические прогоны» (spec §6 фаза 4), переключение N правкой runner.json с возвратом ДО фиксации выбора.

   Вариация соседа — генератор (Task 3) СОБРАННЫМ бинарём в фоне:

```bash
dotnet src/tools/E2eLoadGen/bin/Release/net10.0/E2eLoadGen.dll \
  -k <K> -p full -d <T+20мин> -o /tmp/pgw-noise-runNN > /tmp/pgw-noise-runNN.log 2>&1 &
```

   Остановка соседа — `kill -INT $(pgrep -f 'E2eLoadGen.dll')` (SIGINT → `Console.CancelKeyPress` → teardown в `finally`; Ctrl-C фоновой job не доставляется, SIGTERM не гарантирует finally). Гейт чистоты — ТОЛЬКО после подтверждённого teardown соседа (итоговая строка `noise:` в его логе ЛИБО пустой `docker network ls | grep pgw-noise-`). Если SIGINT не снял за ~30 с — SIGTERM + страховочная зачистка `pgw-noise-`-остатков (префикс задачи, не общий `pgw-*`), факт в журнал. Механика подтверждена прогоном №5 (сосед K=3 full отработал, SIGINT-teardown — 0 остатков).
4. Поллинг: раз в ~25 с `tail -5` лога серии (и соседа) — до итоговой строки `Passed!|Failed!`. При появлении ЧУЖИХ контуров в окне серии — зафиксировать в журнал (K≠0, прогон кандидат на невалидность), НЕ останавливать без решения контролёра.
5. По завершении: в `journal.md` зелёность, время (`Total duration`), пик живых сетей серии (поллинг `docker network ls --format '{{.Name}}' | grep -c '^pgw-en-'`), DNS-телеметрия (зонд + Patroni-логи: `grep -c "failed to resolve host\|getaddrinfo" container-pgw-*.log` по артефактам).
6. Разбор (при фейле — по телеметрии артефактов `/tmp/…/pgw-e2e-artifacts-*`, БЕЗ перезапуска), зачистка stop-остатков упавших сценариев (по их `README-cleanup.txt`), гейт чистоты, вердикт в `journal.md`.

---

### Task 0: Журнал задачи — каркас для протоколов прогонов и гипотез

**Статус: ВЫПОЛНЕН (Фаза 6).** `journal.md` ведётся: сводные строки прогонов №1–№5 (№6/№7 — предзаписанные гипотезы матрицы), карта гипотез, гейт, «Наблюдения вне прогонов» с полной эмпирикой (конфигурация, фокус-разбор 4f7b146, разборы всех фейлов). Якоря секций (`## Сводка прогонов`, `## Карта гипотез (AC2)`, `## Гейт решения (AC3)`, `## Наблюдения вне прогонов`) используются всеми задачами.

**Вход:** spec утверждён; журнал — обязательный артефакт (spec §3.5).
**Действие (Files):** Create `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md`.
**Interfaces:** Produces — якоря секций журнала для всех последующих задач.
**Выход:** место для гипотез ДО запуска и измерений AC1–AC3, AC10.
- [x] Step 1–3: каркас создан, проверен, закоммичен (Фаза 6).
**Проверка задачи:** якоря на месте; эмпирика фиксируется по ходу.
**Связь со spec:** §3.5, §3.2.

---

### Task 1: Приведение конфигурации параллелизма к целевой схеме — runner.json + последовательная не-E2E-коллекция

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 2fe1ac4).** Итоги: `xunit.runner.json` (`maxParallelThreads=3`) — единственный источник; атрибут и мёртвые `Xunit*`-свойства удалены; `NonE2eCollection` (`DisableParallelization=true`, фикстуры Etcd/PgApi/PgMetrics как ICollectionFixture) — 24 переноса + 17 новых `[Collection]`-пометок + правка двух t07-комментариев. Контроль «после»: 309/310 → повтор 310/310 (4,53 мин, строгие блоки; WalReceiver-фейл беззащитной конфигурации не воспроизводится); E2E-smoke 7/7 с пиком сетей 4 — параллелизм N=3 реален впервые. Полные шаги — в git-истории плана; ниже контракт, на который опираются остальные задачи.

**Вход:** эмпирика Фазы 6 (атрибут рабочий N=1 при любом запуске; csproj-свойства мёртвые; WalReceiver-фейл под конкуренцией); решение пользователя «не е2е должны быть последовательно, их в коллекцию надо закинуть».
**Действие (Files):** Delete `AssemblyInfo.cs`; Modify csproj (минус мёртвые свойства, плюс копирование runner.json); Create `xunit.runner.json`, `NonE2eCollection.cs`; Modify 24+17 тестовых файлов.
**Interfaces:** Produces — `NonE2eCollection.Name = "non-e2e-sequential"` (все не-E2E классы); runner.json — единственный источник потолка (стартовое 3).
**Выход:** целевая схема §1.3 установлена (AC4, без финального N — за ним приёмка Task 10).
**Проверка задачи:** `grep -rn "CollectionBehavior\|XunitParallelize\|EtcdCollection.Name\|PgApiCollection.Name\|PgMetricsCollection.Name" --include='*.cs' src/tests/PgWorker.IntegrationTests/` → пусто; серии журнала.
**Связь со spec:** §1.3, §5.4, §6 фаза 0, §7, AC4.

---

### Task 1b: Перевод интеграционных тестов бэкапов на postgres:18-alpine

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 341a577).** Итоги: образ зеркалирован мульти-арх; OwnPostgres.cs (константа+doc), WalSqlTests.cs, images.txt (замена строки), runbook.md; контрольная серия Backups 166/166 (3,29 мин) — зелёная на 18-alpine; хост чист.

**Вход/Действие/Interfaces/Выход:** — (в git-истории плана; требование пользователя, вне исходного scope).
**Проверка задачи:** `grep -rn "postgres:17-alpine" src dev-stand docs/runbook.md` → пусто.
**Связь со spec:** вне исходного scope; прямое требование пользователя 2026-10-06 (Фаза 6).

---

### Task 1c: Фикс тестовой дыры AgentImage — override wal-образа в E2E бэкап-хостах

**Статус: ВЫПОЛНЕН (Фаза 6, коммит baba3c0).** Итоги: гейт полноты — дыра ровно в 3 файлах (Restore 1/0, Retention 1/0, Supervisor 2/0 по `Job__Image`/`Wal__AgentImage`); 4 врезки `["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage`; точечная серия Restore/Supervisor/Retention 7/7 за 11,77 мин — все источники детерминированных фейлов прогона №1 зелёные, `404 pull access denied` — 0. Дыра закрыта.

**Вход/Действие/Interfaces/Выход:** — (в git-истории плана; требование с гейта прогона №1).
**Проверка задачи:** повторный гейт — `Wal__AgentImage` = `Job__Image` по счётчикам у всех E2e-файлов.
**Связь со spec:** вне исходного scope; прямое требование пользователя 2026-10-06 (гейт прогона №1); косвенно — валидность базлайнов фазы 0.

---

### Task 1d: Сетевые митигации — per-cluster сети движка `pgw-net-<C>` + advertise-фикс и per-cluster teardown E2eEnvironment

**Статус: НОВАЯ задача (spec ревизии 2; причина локализована разбором 4f7b146; контракты arch-first уже в worktree). Исполняется ПЕРВОЙ после снятия паузы execute.**

**Вход:** контракты обновлены spec-агентом: `arch/14-pgworker.md` §2.1 (per-cluster сеть нод `pgw-net-<C>`; ensure-инвариант «нода и её wal-агенты в одной сети кластера»; лечение: агент — пересозданием, нода с данными — подключением `docker network connect`; демонтаж кластера удаляет сеть), `arch/04-deploy-etcd.md` §8 п.3 (в advertise — ТОЛЬКО URL, резолвимые для потребителей; внутрисетевой алиас НЕ включается — Patroni ретраит весь список, недостижимый URL даёт NXDOMAIN-ретраи), `docs/e2e-isolation.md` (таблица §1: `pgw-net-{C}` создаёт движок; teardown §3.5; §8). Причина болей локализована: фокус-разбор 4f7b146 — Patroni-ноды (сеть `pgw-net`) ретраят advertise-URL `http://e2e-etcd1:2379` (алиас существует только в сети окружения) → NXDOMAIN-бури → потеря DCS-сессий → съеденные бюджеты Restore/WalStream (стабильные фейлы №3/№3п при N=3; при N=1 серия зелёная — №4). Эталон per-cluster паттерна: `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs:94-96` (`NodesNetworkPrefix = "kfw-net-"`, `NetworkName(cluster)`), `:231` (удаление сети при демонтаже). Сетевые митигации НЕ зависят от гейта фазы 2 (спец §1.4: изоляция контуров — требование канона §13).

**Действие (Files):**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (константа → префикс+NetworkName; использования; ensure-инвариант; демонтаж сети)
- Modify: `src/Shared.Docker/Engine/IDockerEngine.cs` + `DockerEngine.cs` (расширение `DockerContainerInspect` полем сетей контейнера — исполнимость сверки ensure-инварианта; член `NetworkConnectAsync` с прод-реализацией; если у kfw есть готовый аналог в Shared.Docker — переиспользовать)
- Modify: ВСЕ реализации `IDockerEngine` — стабы `NetworkConnectAsync` (`Task.FromResult(Result.Success())` по образцу стаба `DeleteNetworkAsync`; член интерфейса обязывает, иначе CS0535): `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs`, `Backups/BackupVerifyProcessTests.cs` (FakeVerifyEngine), `Backups/RestoreDrillProcessTests.cs` (FakeDrillEngine), `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (FakeBackupEngine), `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs` (FakeCleanerEngine), `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (FakeEngine — осмысленный стаб: сети в inspect + connect в Calls для Step 3(б)), `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverLimitsTests.cs` (StubEngine), `src/tests/ValkeyWorker.UnitTests/Docker/SwarmClusterDriverTlsTests.cs` (RecordingEngine) — 8 тестовых + прод `DockerEngine` = 9
- Modify: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (юниты: NetworkName, create-спека, ensure-инвариант, демонтаж; расширение FakeEngine — сети контейнера в inspect-ответе, запись connect-вызова в Calls)
- Modify: `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineTests.cs` (тестовое имя сети `"pgw-net"` → нейтральное `"test-net"` — тест проверяет маппинг Network → HostConfig.NetworkMode, а не имя; `pgw-net` уходит из репо целиком, гейт Step 6 исполним)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (advertise без внутрисетевого алиаса; teardown: минус «снести pgw-net», плюс own-only per-cluster сети; ассерт чистоты сетей)
- Modify: `src/tests/PgWorker.IntegrationTests/Etcd/OwnedEtcdFixture.cs` (унификация на ЕДИНЫЙ runId окружения — требование пользователя, Step 5)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (контрольный прогон)

**Interfaces:**
- Consumes: `IDockerEngine.EnsureNetworkAsync/DeleteNetworkAsync` (Shared.Docker, `DockerEngine.cs:238`); kfw-паттерн (образец в коде); `OwnName` (E2eEnvironment — матчит `pgw-net-<C>` по ClusterTag).
- Produces (для Tasks 4–10, AC9):
  - `PlainClusterDriver.NodesNetworkPrefix = "pgw-net-"` + `public static string NetworkName(string cluster) => $"{NodesNetworkPrefix}{cluster}";` — константа `NodesNetwork` удаляется (AC9а: в коде движка нет `pgw-net`);
  - ensure-инвариант: при create/усыновлении ноды и агента — сеть контейнера сверяется с `pgw-net-<C>`; расхождение лечится по arch/14 §2.1 (агент — пересоздание; нода — подключение к сети кластера); миграция усыновлённых из старой `pgw-net` — тем же механизмом (сверка на первом ensure после обновления);
  - демонтаж кластера удаляет `pgw-net-<C>` (ошибки удаления не роняют демонтаж — сеть могла быть снесена/не существует);
  - E2eEnvironment: `--advertise-client-urls` etcd = ТОЛЬКО `http://host.docker.internal:{порт}` (peer-urls не трогать — их потребители в той же сети окружения); teardown — own-only удаление своих per-cluster сетей + ассерт «не осталось сетей своего идентификатора» (охватывает `pgw-en-{runId}` и `pgw-net-{C}`);
  - `OwnedEtcdFixture`: единый runId окружения — один guid на сеть и контейнер (`pgw-it-net-{runId}` / `pgw-it-etcd-{runId}`), own-only чистка и ассерт чистоты по этому runId.

**Выход:** сетевая изоляция контуров по построению (per-cluster DNS-зоны) + advertise-канон; NXDOMAIN-шторм Patroni устранён по механике (AC9 а–г); все тестовые сети — per-runId на единицу, включая OwnedEtcdFixture (единый guid окружения); контрольный прогон доказывает эффект (AC9д).

- [ ] **Step 1: Движок — per-cluster имя сети**

`src/PgWorker.Docker/Drivers/ClusterDriver.cs:140` заменить (по образцу `KafkaWorker.Docker/Drivers/ClusterDriver.cs:94-96`):

```csharp
    // Сеть нод — PER-CLUSTER (arch/14 §2.1, как kfw-net-<C> у KafkaWorker):
    // единая сеть делала DNS-зону общей для всех кластеров docker-хоста.
    public const string NodesNetworkPrefix = "pgw-net-";

    public static string NetworkName(string cluster) => $"{NodesNetworkPrefix}{cluster}";
```

Использования (grep `NodesNetwork` по `src/PgWorker.*`): `ClusterDriver.cs:208` → `EnsureNetworkAsync(NetworkName(topology.Cluster), ct)`; `:314` → `EnsureNetworkAsync(NetworkName(cluster), ct)`; `:323` → `agentSpec = spec with { Network = NetworkName(cluster) }`; `:629` → `Network: NetworkName(topology.Cluster)`. Вне драйвера: `E2eEnvironment.cs:771/775` (шаг teardown — снимается в Step 4), `ClusterDriverTests.cs:822` (правится в Step 3).

- [ ] **Step 2: Ensure-инвариант + демонтаж сети (+ модель инспекта как предусловие)**

Предусловие исполнимости сверки: текущая модель `DockerContainerInspect` (`src/Shared.Docker/Engine/IDockerEngine.cs:51-56` — Id/Hostname/Aliases/Env/Ports/Running/ExitCode/StartedAtUnix) НЕ содержит сетей контейнера — «сверка сети (inspect)» берёт их неоткуда. Расширить: (1) `DockerContainerInspect` — поле `IReadOnlyList<string> Networks` (в `DockerEngine.cs` парсинг ключей `NetworkSettings.Networks` — прецедент парсинга этой же секции для aliases уже есть, `DockerEngine.cs:68`); (2) FakeEngine (`ClusterDriverTests.cs:15`) — сети контейнера в inspect-ответе + запись connect-вызова в `Calls` (основа юнитов Step 3(б)); (3) `NetworkConnectAsync` — ЧЛЕН `IDockerEngine` (по образцу `DeleteNetworkAsync`, `DockerEngine.cs:238`; готовый аналог kfw — переиспользовать): прод-реализация в `DockerEngine` + стабы `Task.FromResult(Result.Success())` во ВСЕХ тестовых реализациях интерфейса — иначе CS0535 в 8 фейках вне границ шага (перечень — Files); контроль-гейт полноты: `grep -rn ': IDockerEngine' --include='*.cs' src/` → 9 мест, каждое покрыто.

Затем — сам инвариант. В `EnsureNode` (блок после `EnsureNetworkAsync`, рядом с идемпотентностью-сверкой портов) и `EnsureWalAgent`: при найденном существующем контейнере — сверка сети по inspect (`Networks`): сеть ≠ `pgw-net-<C>` → лечение по arch/14 §2.1: агент — пересоздание (exited/данных нет); нода — подключение к сети кластера (`docker network connect pgw-net-<C> <имя>`). В путь демонтажа кластера (движок снимает контейнеры кластера; ориентир — `grep -n "Dismantle\|демонтаж\|RemoveCluster" src/PgWorker.*`, образец — `KafkaWorker.Docker/Drivers/ClusterDriver.cs:231`): `await engine.DeleteNetworkAsync(NetworkName(cluster), ct)` в конце, неудача — лог, не ошибка. Миграция усыновлённых из старой `pgw-net`: обеспечивается сверкой на первом ensure; пустая осиротевшая `pgw-net` вычищается страховкой хоста (не кодом теста).

Сопутствующее (гейт Step 6): `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineTests.cs` — тестовое имя сети `"pgw-net"` (:141 комментарий, :153 `Network: "pgw-net"`, :161 `Should().Be("pgw-net")`, :163 `p.Name == "pgw-net"`) заменить на нейтральное `"test-net"`: тест проверяет маппинг `Network` → `HostConfig.NetworkMode`, семантически не связан с константой `PlainClusterDriver`; без этого `grep '"pgw-net"' src/` непроходим, хотя константа ушла.

Актуализация комментариев в тех же файлах (упоминания `pgw-net` после ухода константы — дезинформация; без отдельного гейта — комментарии без кавычек литерал-гейт не ломают): `src/PgWorker.Backups/WalStreamProcess.cs:344` («сеть назначает драйвер (pgw-net)» → per-cluster `pgw-net-<C>`), `src/PgWorker.Provisioning/Endpoints/ShardEndpoints.cs:189`, `src/PgWorker.Docker/Drivers/ClusterDriver.cs:81`, `src/tests/PgWorker.IntegrationTests/Backups/WalStreamProcessTests.cs:111`, `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs:817` (правится вместе с ожиданием Step 3).

- [ ] **Step 3: Юниты движка (ClusterDriverTests.cs, AAA)**

По образцу существующих тестов с FakeEngine (`ClusterDriverTests.cs:822` — правится ожидание на `pgw-net-shop`):

```csharp
    [Fact]
    public void NetworkName_имя_сети_кластера_пер_кластерный_префикс()
    {
        // Arrange / Act — kfw-паттерн: префикс + имя кластера
        // Assert
        PlainClusterDriver.NetworkName("shop").Should().Be("pgw-net-shop");
        PlainClusterDriver.NodesNetworkPrefix.Should().Be("pgw-net-");
    }
```

плюс: (а) create-спека ноды и агента — `Network == "pgw-net-<C>"`; (б) ensure-инвариант: существующая нода в чужой сети → вызван connect к `pgw-net-<C>` (FakeEngine.Calls), существующий агент в чужой сети → пересоздан; (в) демонтаж кластера → `DeleteNetworkAsync("pgw-net-<C>")` вызван. Run: `dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~ClusterDriver` → зелёные, 0 warnings.

- [ ] **Step 4: E2eEnvironment — advertise-фикс + per-cluster teardown/ассерт**

(a) В команде etcd-узла (`StartOnceAsync`, флаг `--advertise-client-urls`): убрать внутрисетевой алиас, оставить только достижимый для всех потребителей URL (arch/04 §8 п.3; peer-urls НЕ трогать — их потребители в сети окружения):

```csharp
                    // Advertise — ТОЛЬКО URL, резолвимые из КАЖДОЙ сети потребителей
                    // (arch/04 §8 п.3, t24/4f7b146): Patroni-ноды живут в per-cluster
                    // сетях движка — алиас e2e-etcdN в их DNS-зоне не существует,
                    // etcd отдавал его Patroni в member-discovery → NXDOMAIN-шторм.
                    // Потребители peer-URL — другие узлы ТОЙ ЖЕ сети окружения.
                    $"--advertise-client-urls=http://host.docker.internal:{etcdPorts[i]}",
```

(b) Teardown `DisposeAsync`: удалить блок «шаг 6 — снести общесистемную pgw-net» (строки ~727–776, `docker network rm PlainClusterDriver.NodesNetwork` + catch-вывод). Вместо него — own-only per-cluster сети (после шага 5, до ассерта):

```csharp
        // 6) Per-cluster сети нод СВОИХ кластеров (pgw-net-<C>, создаёт движок;
        // демонтаж удаляет — здесь страховка own-only: упавшие сценарии оставляют).
        foreach (var net in (await E2eFixture.RunProcessAsync(
                     "docker", ["network", "ls", "--format", "{{.Name}}"]))
                 .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                 .Where(n => n != NetName && OwnName(n)))
        {
            try
            {
                await E2eFixture.RunProcessAsync("docker", ["network", "rm", net]);
            }
            catch (Exception e)
            {
                problems.Add($"сеть кластера {net}: {e.Message}");
            }
        }
```

(в) Ассерт чистоты (шаг 7): проверку `leftNet` расширить с «сеть окружения» до «ВСЕ сети своего идентификатора» (OwnName — покроет `pgw-en-{runId}` и `pgw-net-{C}`):

```csharp
        var leftNets = (await E2eFixture.RunProcessAsync(
                "docker", ["network", "ls", "--format", "{{.Name}}"]))
            .Split(['\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(OwnName)
            .ToList();
        if (leftNets.Count > 0)
            problems.Add($"остались сети окружения/кластеров: {string.Join(' ', leftNets)}");
```

Актуализация комментариев E2eEnvironment.cs теми же правками (без отдельного гейта): :263–266 — «advertise двумя URL: compose-alias (сеть окружения) + …» устаревает после (a) (остаётся один URL — переписать абзац под arch/04 §8 п.3); :654 (doc-сводка teardown «…6) rm сети движка pgw-net (попытка) → 7) АССЕРТ…») и :766 (заголовок удаляемого блока) — синхронизировать с новой последовательностью шагов (6 — per-cluster сети, ассерт — по всем своим сетям).

- [ ] **Step 5: Унификация OwnedEtcdFixture на ЕДИНЫЙ runId окружения (требование пользователя дословно: «все сети частные на один тест класс и внутри поднимается все необходимое для тестов. НИКАКИХ "своя per-fixture сеть" — все сети ДОЛЖНЫ БЫТЬ на класс вида pgw-en-{runId} (per-runId, guid)»)**

Факт (проверен): `src/tests/PgWorker.IntegrationTests/Etcd/OwnedEtcdFixture.cs:29-30` — сеть `pgw-it-net-{Guid…}` и контейнер `pgw-it-etcd-{Guid…}` на ДВУХ РАЗНЫХ guid (единого runId окружения нет; комментарий «Разные guid-префиксы сети и контейнера: substring-фильтры docker в ассерте чистоты не пересекаются» — обоснование уходит вместе с двумя guid). Инвентаризация `new NetworkBuilder` в `src/tests/`: ровно 3 места — `E2e/E2eEnvironment.cs:242` (`pgw-en-{runId}` ✓ канону), `Etcd/OwnedEtcdFixture.cs:43` (отклонение — этот шаг), `ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs:105` (`vwk-en-{runId}` ✓); других отклонений нет — гейт-проверка шага это подтверждает на момент исполнения.

Правка `OwnedEtcdFixture.cs` — один guid на все объекты окружения; прежние имена `_netName`/`_containerName` СОХРАНЯЮТСЯ (свойства-выражения от `_runId` — минимальный дифф: использования в конструкторе `:43`/`:45` компилируются без правок; в остальном коде фикстуры меняется только ассерт — замена ниже):

```csharp
    // ЕДИНЫЙ runId окружения (канон per-runId, t24: «все сети частные на один
    // тест класс»): один guid опознаёт и сеть, и контейнер — own-only чистка
    // и ассерт чистоты по этому идентификатору (docs/e2e-isolation.md §2–3).
    // Прежние имена сохранены свойствами-выражениями: конструктор (:43/:45)
    // не меняется; ассерт DisposeAsync переформулирован по _runId (ниже).
    private readonly string _runId = Guid.NewGuid().ToString("N");
    private string _netName => $"pgw-it-net-{_runId}";
    private string _containerName => $"pgw-it-etcd-{_runId}";
```

Ассерт чистоты `DisposeAsync` (замена фильтров `:92`/`:95` — фильтр по единому runId покрывает оба объекта):

```csharp
        var leftContainers = await RunDockerAsync(
            $"ps -a --filter name={_runId} --format {{{{.Names}}}}");
        leftContainers.Should().BeEmpty("teardown окружения неполный: остался контейнер");
        var leftNetworks = await RunDockerAsync(
            $"network ls --filter name={_runId} --format {{{{.Names}}}}");
        leftNetworks.Should().BeEmpty("teardown окружения неполный: осталась сеть");
```

Проверка шага (юнит-контроля нет — интеграционная фикстура): (а) гейт grep — в `OwnedEtcdFixture.cs` ровно ОДИН `Guid.NewGuid` (только `_runId`): `grep -c "Guid.NewGuid" src/tests/PgWorker.IntegrationTests/Etcd/OwnedEtcdFixture.cs` → `1`; (б) гейт инвентаризации: `grep -rn "new NetworkBuilder" --include='*.cs' src/tests/` — 3 места, все с единым runId; (в) интеграционная серия Etcd зелёная (`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~PgWorker.IntegrationTests.Etcd" --logger "console;verbosity=detailed"` — потребители OwnedEtcdFixture: RestoreProcessTests, BackupOrphanSweeperTests, OrphansApiTests — зелёные, ассерты чистоты фикстуры проходят), гейты чистоты хоста вокруг — по Global Constraints.

- [ ] **Step 6: Сборка + юниты**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~ClusterDriver`
Expected: 0 errors/0 warnings; юниты зелёные. Гейт: `grep -rn '"pgw-net"' src/` → пусто (константа ушла); `grep -rn "NodesNetwork\b" src/PgWorker.Docker/` → пусто (только `NodesNetworkPrefix`/`NetworkName`).

- [ ] **Step 7: Контрольный прогон на целевой конфигурации (AC9 а–д; новая строка журнала, следующий свободный номер)**

Гипотеза в журнал ДО: «после сетевых митигаций каноническая серия N=3 (K=0, тишина) ЗЕЛЁНАЯ 41/41 — NXDOMAIN-шторм Patroni устранён: в docker-логах PG-нод нет серийных `failed to resolve host e2e-etcd1`/`getaddrinfo … empty` (AC9г-д, включая Restore/WalStream, стабильно падавшие в №3/№3п); per-cluster сети `pgw-net-<C>` видны в поллинге и отсутствуют после (ассерт чистоты)». По «Операционной модели» (гейты тишины+чистоты, канонический запуск, `PGW_TEST_E2E_DNS_PROBE=1`); во время серии, при ≥2 живых контурах, — прямые проверки AC9(б)/(в) из per-cluster сети живого кластера (имена сетей — из `docker network ls | grep pgw-net-`):

```bash
NET=$(docker network ls --format '{{.Name}}' | grep '^pgw-net-' | head -1)
# (в) advertise-URL резолвится из сети Patroni-нод (остальные URL списка — те же):
docker run --rm --network "$NET" python:3.12-alpine python3 -c \
  "import socket; socket.getaddrinfo('host.docker.internal', 2379); print('advertise-url: OK')"
# (б) DNS-изоляция: имя ноды/алиас ЧУЖОГО контура из своей сети НЕ резолвится:
ALIEN_EN=$(docker network ls --format '{{.Name}}' | grep '^pgw-en-' | head -1 | sed 's/pgw-en-//')
docker run --rm --network "$NET" python:3.12-alpine python3 -c \
  "import socket, sys
try:
    socket.getaddrinfo('e2e-etcd1', 2379); print('FAIL: чужой алиас резолвится')
except socket.gaierror:
    print('isolation: OK (NXDOMAIN для чужой зоны)')"
```

(вторая команда осмысленна, когда жив ≥1 чужой контур серии — его сеть `pgw-en-*`; если контур один — повторить проверку в окне следующего факта). Итог прогона + проверки AC9 — в журнал.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(t24): сетевые митигации — per-cluster сети pgw-net-<C> движка (arch/14 §2.1, ensure-инвариант, демонтаж сети) + advertise без внутрисетевого алиаса (arch/04 §8 п.3) + own-only teardown/ассерт per-cluster сетей E2E + OwnedEtcdFixture на едином runId окружения (AC9, причина 4f7b146; per-runId — требование «все сети частные на один тест класс»)"
```

**Проверка задачи:** Step 6-гейты пусты; Step 5 — гейты grep (один `Guid.NewGuid`; инвентаризация NetworkBuilder) и серия Etcd зелёная; контрольный прогон зелёный 41/41 без NXDOMAIN-серий (или разобранный по телеметрии фейл с НОВОЙ причиной — тогда вердикт в журнал, STOP и вопрос контролёру); проверки AC9(б)/(в) зафиксированы.
**Связь со spec:** §1.2 (сетевой факт), §1.4 (целевое состояние), §5.7, §6 фаза 3 (обязательные сетевые митигации), §7 (канон изоляции), AC9; риски §9 (миграция усыновлённых, рост числа сетей).

---

### Task 2: Синтетический DNS-зонд — общий скрипт + опция в E2E-окружении

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 1a920bc).** `dns_probe.py` (measure/storm, три категории целей: алиас сети, `host.docker.internal`, внешнее `quay.io`), `E2eDnsProbe` (контейнер `pgw-dns-{runId}`, лог подбирается телеметрией), врезка в `E2eEnvironment` (объявление до try; catch — best-effort dispose). Smoke зелёный.

**Дополнение по эмпирике (для Task 6, НЕ переделка):** тракт зонда (сеть окружения) НЕ видит Patroni-путь (per-cluster сети) — факт №3/№3п: у зонда 0 полных фейлов при бурях у Patroni. Вывод журнала: различение гипотез дополняется анализом Patroni-логов (`getaddrinfo`/`failed to resolve host` в `container-pgw-*.log`), не только CSV зонда.

**Interfaces:** Produces — env-контракт `dns_probe.py` (`DNS_PROBE_TARGETS/MODE/INTERVAL`), `E2eDnsProbe.Build(runId, net, targets, mode, intervalSec)`, опция `PGW_TEST_E2E_DNS_PROBE=1`.
**Связь со spec:** §5.2, §4 (H1/H3), §7.

---

### Task 3: Диагностический нагрузочный генератор `src/tools/E2eLoadGen`

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 0bccc5a).** K шумовых guid-контуров, профили dns/cpu/io/full, own-only teardown с ассертом чистоты; SIGINT-механика остановки. Подтверждено прогоном №5: сосед K=3 full (15 контейнеров/3 сети) отработал 27:43, teardown по SIGINT — 0 остатков.

**Interfaces:** Produces — CLI `dotnet src/tools/E2eLoadGen/bin/Release/net10.0/E2eLoadGen.dll -k <K> -p <dns|cpu|io|full> -d <мин> [-o <dir>]`.
**Связь со spec:** §5.1, §2 (воспроизводимость), §7 (временный инструмент).

---

### Task 4: Фаза 0 (базлайны) — контрольные прогоны серии

**Статус: ИСПОЛНЕН (Фаза 6; результат — центральный вход фаз 1–2, НЕ переигрывается).** Итоги журнала:
- №3 (N=3, K=0, тишина): 39/41 за 40,07 мин; №3п (повтор по §9): 38/41 за 49,84 мин — Restore_Latest/Restore_TargetTime падают СТАБИЛЬНО (2/2), +WalStream_Promote_TliHistory; у Patroni-нод всех кластеров (вкл. зелёные) — массовые `failed to resolve host e2e-etcd1` (70–354 события). Центральный результат фазы 0: канонический N=3 НЕ даёт зелёного базлайна в тишине; провалы резолва — всеобщий фон, фатальный для жёсткобюджетных Restore/WalStream (H1/H3-вход).
- №4 (N=1): ЧАСТИЧНЫЙ, закрыт решением пользователя (34 passed/0 failed; ключевой факт: `Restore_Latest` при N=1 ЗЕЛЁНЫЙ [1м45с] против 2/2 падений при N=3 — деградация зависит от параллелизма серии).
- №5 (N=3+K=3 full): прерван приказом пользователя на ~32-м факте (30 passed/2 failed — Restore_Latest + Backup_Verify); сосед отработал чисто; осиротевшие контуры зачищены, хост 0/0/0.
- Причина №3/№5 локализована фокус-разбором 4f7b146 (advertise-алиас + единая сеть) → закрывается Task 1d.

**Вход/Действие/Interfaces/Выход:** — (полная редакция — в git-истории плана).
**Проверка задачи:** строки №1–№5 журнала с разборами; хост чист (0/0/0 подтверждён).
**Связь со spec:** §6 фаза 0, AC4 (наблюдение параллелизма N=3 — пик сетей 3–4), вход фаз 1–2.

---

### Task 5: Фаза 1 — матрица воспроизведения на целевой конфигурации

**Вход:** Task 1d (сетевые митигации внедрены; контрольный прогон — новый базлайн N=3 на целевой конфигурации); Tools 2–3 готовы; предзаписанные строки-гипотезы №6/№7 в журнале (исполняются здесь; уточнить формулировки «на целевой конфигурации» при старте).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (строки прогонов, промежуточные вердикты карты гипотез).

**Interfaces:**
- Consumes: генератор (Task 3), зонд + Patroni-лог-анализ (Task 2 + дополнение), базлайн Task 1d, «Операционная модель» (переопределение N, сосед, гейты тишины/чистоты).
- Produces: различающие гипотезы измерения на ЦЕЛЕВОЙ конфигурации (H1-остаток «не объясняется advertise-алиасом», H2, H4); данные Task 6.

Состав (каждый прогон — с гипотезой ДО; уже исполненные до-митигационные №3/№3п/№4/№5 — зафиксированные улики, НЕ переигрываются):

- [ ] **Step 1: Прогон №6 — (N=5, переопределённый потолок, K=0, тишина) — симптом Б на целевой конфигурации**

Гипотеза (строка №6 журнала, дополнить «после Task 1d»): «если после сетевых митигаций N=5 зелёный — остаточной DNS-деградации нет, симптом Б закрывался сетевой причиной; если фейлы/DNS-телеметрия ухудшаются к N=3 — H1-остаток (docker-резолвер от числа сетей/контейнеров)». Зонд вкл; Patroni-лог-подсчёт по артефактам (`grep -c "failed to resolve host\|getaddrinfo" container-pgw-*.log`).

- [ ] **Step 2: Прогон №7 — (N=2, переопределённый потолок, K=3 full) — H4 (порог от суммарной нагрузки)**

Гипотеза (строка №7, дополнить): «если решает СУММАРНАЯ нагрузка (N=2+K=3 ≈ 5 контуров), деградация ≈ №6 (N=5+K=0); если N серии — N=2+сосед устойчивее базлайна N=3». Сосед по «Операционной модели»; остановка `kill -INT` + подтверждение teardown.

- [ ] **Step 3: DNS-кривая — резолв от числа сетей без PG (H1 в чистом виде; не зависела от митигаций — выполняется как есть)**

4 ступени генератором dns-профиля (гейт чистоты между ступенями; K — дополнительные сети сверх контура зонда):

```bash
for K in 0 1 3 5 7; do
  dotnet run --project src/tools/E2eLoadGen -c Release -- \
    -k $((K+1)) -p dns -d 2 -o /tmp/pgw-t24-dnscurve-k$K 2>&1 | tee /tmp/pgw-t24-dnscurve-k$K.log
done
```

Метрики: `grep -h storm-dns /tmp/pgw-t24-dnscurve-k$K/container-*.log | tail -12` → таблица «K → fails/avg_ms/max_ms» в журнал. Гипотеза ДО: «H1-остаток подтверждается монотонным ростом fails/латентности с K; плоская кривая до ~8 сетей — H1-остаток исключается».

- [ ] **Step 4: Прогон №5-доз (только если данных H4 недостаточно)**

№5 прерван приказом (2 фейла на 32 фактах при K=3 — улика есть). Повтор N=3+K=3 на целевой конфигурации — только если Step 1–3 не различили H4, с новой гипотезой в журнал; иначе — не выполняется (матрица минимальна, spec §9).

- [ ] **Step 5: Сводка фазы 1 — предварительные вердикты карты H1–H4 + commit**

`journal.md`; commit `docs(t24): фаза 1 на целевой конфигурации — N5, N2+сосед, DNS-кривая (H1-остаток/H2/H4)`.

**Проверка задачи:** каждый прогон — с гипотезой ДО, гейты тишины/чистоты вокруг, хост чист после.
**Связь со spec:** §6 фаза 1 (матрица; N=5 — переопределённый потолок), AC1 (симптомы после митигаций: воспроизведены или опровергнуты — оба исхода результат).

---

### Task 6: Фаза 2 — различение гипотез H1–H4 и карта «симптом → причина → измерение»

**Вход:** Task 5 (матрица целевой конфигурации) + зафиксированные улики ДО-митигационных прогонов (№3/№3п/№4/№5) + фокус-разбор 4f7b146 (готовый подтверждённый экземпляр H3: NXDOMAIN-шторм → потеря DCS → съеденные бюджеты Restore/WalStream при N=3).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (карта гипотез — окончательные вердикты; доля, закрываемая сетевыми митигациями; оценка остатка).

**Interfaces:**
- Consumes: телеметрия прогонов №3–№7 (+№5/№5-доз) — docker-логи, host.log, CSV зонда, Patroni-логи (`failed to resolve host`/`getaddrinfo` — главный источник, тракт зонда отличается), `[PHASE]`-тайминги; генератор.
- Produces (вход гейта Task 7): карта H1–H4 «следствие → измерение → подтверждено/исключено» со ссылками на артефакты (AC2); явное выделение доли, закрываемой сетевыми митигациями §1.4 (AC2); вердикт об остаточных причинах — в наших границах или ограничение Docker Desktop/хоста.

- [ ] **Step 1: H1-остаток — деградация docker-резолвера, НЕ объясняёмая advertise**

По прогонам целевой конфигурации (Task 1d-контроль, №6, №7) и DNS-кривой: (а) Patroni-логи — остались ли `getaddrinfo`/`resolve`-серии ПОСЛЕ advertise-фикса (если нет — «симптом Б» исчерпан сетевой причиной; H1-остаток = исключён в этом тракте); (б) зонд — латентность/fails от числа живых сетей (кривая); (в) WHERE умирает соединение (резолв vs TCP/хендшейк — сверка времени фейла с зондом/логами). Вердикт в карту.

- [ ] **Step 2: H2 — CPU/IO без DNS-шума**

Изолирующий прогон (новая строка журнала): сосед `-p cpu -k 3` + `-p io -k 3` (БЕЗ dns-профиля) при канонической серии N=3. Гипотеза ДО: «CPU/IO-нагрузка без DNS-шума воспроизводит фейлы коннекта при чистых Patroni-логах → H2 самостоятельная причина; фейлов нет → H2 исключается». Поллинг `docker stats --no-stream`/`uptime` — корреляция.

- [ ] **Step 3: H3 — транзиент-циклы против бюджетов**

ДО-митигационные улики №3/№3п (готовые): движение между ретраями, `[PHASE] … elapsed=…` при фейлах по бюджету. Дополнить по целевой конфигурации: остались ли ЖЁСТКИЕ бюджеты, которые при N≥5 съедаются медленными (но успешными) ретраями — кандидат точечных уточнений телеметрии/бюджетов (Task 7). Вердикт в карту.

- [ ] **Step 4: H4 — суммарная нагрузка vs N серии**

Сводка: базлайн-целевой (N=3, K=0), №6 (N=5, K=0), №7 (N=2, K=3), H2-прогон (N=3, cpu/io): при каком (N, K, профиль) деградация появляется/исчезает; зависит ли порог от суммы контуров.

- [ ] **Step 5: Доля сетевых митигаций + вердикт границ + commit**

В журнал: (а) строка карты «доля болей, закрываемая сетевыми митигациями §1.4 — по сопоставлению №3/№3п (до) с контрольным прогоном Task 1d (после)»; (б) остаточные причины — в наших границах (код тестов/конфигурация) или ограничение Docker Desktop/хоста; (в) кандидаты ПРОЧИХ митигаций (сверх сетевых) с оценкой «дёшево/дорого». Commit `docs(t24): фаза 2 — карта H1-H4 с выделением доли сетевых митигаций, вердикт остатка`.

**Проверка задачи:** карта полна; каждый вердикт — со ссылкой на артефакт/прогон; ни один прогон — без гипотезы в журнале.
**Связь со spec:** §6 фаза 2, AC2 (в т.ч. «доля, закрываемая сетевыми митигациями, выделена»), вход ГЕЙТА.

---

### Task 7: ГЕЙТ РЕШЕНИЯ — «прочие митигации сверх сетевых» + H3-уточнения

**Вход:** Task 6 (карта полная; сетевые митигации уже внедрены Task 1d независимо от гейта).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (секция «Гейт решения»: только «прочие митигации»; судьба инструментов).

**Interfaces:**
- Consumes: карта Task 6, критерии spec §6 «ГЕЙТ РЕШЕНИЯ» (редакция ревизии 3).
- Produces: зафиксированное решение (+ состав прочих митигаций либо их отклонение) — вход фазы приёмки Task 10.

- [ ] **Step 0: Актуализация каркаса журнала под ревизию 3 spec**

Каркас (Task 0) писался до ревизий 2–3 и несёт устаревшие строки-подсказки (НЕ записи результатов — их не трогать): (а) секция «Гейт решения (AC3)» — критерии-подсказки «ветка А/Б», «канон N=3 + рельсы — ограничение Docker Desktop/хоста» заменить формулировками spec §6 ревизии 3: гейт решает ТОЛЬКО «прочие митигации СВЕРХ сетевых» (реализуем дешёвые / не реализуем — остаточное ограничение Docker Desktop/хоста); сетевые митигации выполнены Task 1d вне гейта; канон параллелизма выбирает приёмка Task 10 из {3,5,6} — НЕ гейт; (б) строку-гипотезу №6 «Сводки прогонов» дополнить пометкой «на целевой конфигурации (после Task 1d)». Проверка: `grep -c "ветка А\|ветка Б" journal.md` → `0`; строка №6 содержит пометку.

- [ ] **Step 1: Применить критерии**

По карте: (1) есть ли остаточные причины В НАШИХ ГРАНИЦАХ с дешёвым каноничным фиксом (код тестов / конфигурация контуров); (2) остаточное — ограничение Docker Desktop/хоста → прочие митигации не реализуем, канон выбирается приёмкой Task 10 в реально доступном диапазоне.

- [ ] **Step 2: Зафиксировать решение**

- **Реализуем прочие** — если есть дешёвые кандидаты: состав в журнал, каждый кандидат — свой мини-эксперимент (реализация → точечный прогон-проверка; состав крупный/рискованный → вопрос пользователю через контролёра, `NEEDS_CONTEXT`).
- **Не реализуем** — если остаточное — ограничение хоста или митигация дороже выгоды: фиксируется с измерениями.
- Критерии неоднозначны → `NEEDS_CONTEXT` (вопрос + варианты), ответ дословно в журнал.
- Судьба инструментов: генератор — удалить (Task 10); DNS-зонд — остаётся, если данные пригодились (по умолчанию — остаётся).

- [ ] **Step 3: H3-уточнения (если вклад H3 в остатке подтверждён картой)**

Точечные уточнения телеметрии/бюджетов медленных фаз — каждое отдельным коммитом с объяснением ПО ТЕЛЕМЕТРИИ (запрет «просто увеличить таймаут» не снимается). Нет вклада — запись в журнал «не требуется».

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): гейт — решение по прочим митигациям (сверх сетевых) зафиксировано (AC3)"
```

**Проверка задачи:** секция «Гейт решения» заполнена (критерии → измерения → решение [+ вопрос/ответ]).
**Связь со spec:** §6 ГЕЙТ РЕШЕНИЯ (сужен до прочих митигаций), AC3.

---

### Task 10: Фаза 4 — приёмка канона параллелизма N=3/5/6, выбор и тройная фиксация

**Вход:** Task 1d (сетевые митигации) + Task 7 (решение по прочим митигациям) — целевая конфигурация достигнута. Порядок исполнения: ДО Tasks 8–9 (рельса и docs получают выбранное здесь N).

**Действие (Files):**
- Modify: `src/tests/PgWorker.IntegrationTests/xunit.runner.json` (финальное `maxParallelThreads` — победитель; фиксируется БЕЗ возврата)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (три серии приёмки + выбор + подтверждение)
- Delete: `src/tools/E2eLoadGen/` (судьба по гейту Task 7 — генератор)
- (если зонд удаляется по гейту) Delete: `dns_probe.py`, `E2eDnsProbe.cs` + откат врезки `E2eEnvironment`

**Interfaces:**
- Consumes: «Операционная модель» (серии приёмки — «полноправные канонические прогоны»: N переключается правкой runner.json с возвратом до фиксации; гейт тишины/чистоты на КАЖДУЮ серию).
- Produces (AC10): выбранный канон `N* ∈ {3,5,6}` — по критерию «МАКСИМАЛЬНО БЫСТРОЕ И СТАБИЛЬНОЕ»; вход для Task 8 (порог рельсы = N*), Task 9 (docs от N*), AC4/AC5/AC7 согласуются с N*.

**Выход:** канон параллелизма выбран измерениями и зафиксирован тройно (runner.json + docs + рельса), подтверждён второй зелёной серией; хост чист; инструменты-диагностики покинули ветку (кроме зонда, если решено оставить).

- [ ] **Step 1: Три канонические серии — N=3, N=5, N=6 (целевая конфигурация)**

Каждая по «Операционной модели»: гейт тишины + чистоты → контроль runner.json (для 5/6 — переопределение с возвратом ПОСЛЕ прогона) → полная серия (`PGW_TEST_E2E_DNS_PROBE=1`) → телеметрия (время, зелёность, Patroni-лог-подсчёт `getaddrinfo`, зонд-латентность, пик сетей, чистота) → строка журнала. Между сериями — только гейты (никаких правок кода).

- [ ] **Step 2: Сравнение и выбор — «максимально быстрое И стабильное»**

Критерии (spec §6 фаза 4): время полной серии; зелёность всех сценариев; отсутствие NXDOMAIN-бурь/DNS-деградации в телеметрии; отсутствие флейков. Правила: (а) среди N с ПОЛНОСТЬЮ зелёными сериями — минимальное время; (б) если все три с фейлами — STOP: канон не достигается, возврат к Task 7 (остаточные причины) с данными, вопрос контролёру; (в) равные показатели — больший N (меньше времени при равной стабильности; при полном равенстве — меньший N, консервативнее). Выбор + измерения — в журнал («Гейт решения»/сводка).

- [ ] **Step 3: Тройная фиксация победителя N***

- `xunit.runner.json`: `"maxParallelThreads": <N*>` — фиксируется БЕЗ возврата (контроль-гейт операционной модели теперь проверяет N*);
- порог рельсы фиксируется значением N* — константу `CanonMaxLiveEnvironments = <N*>` вносит Task 8 при создании `E2eParallelismGuard` (класс на момент Task 10 не существует); здесь — только решение (запись N* в журнал «Гейт решения»/сводку как порог рельсы);
- раздел `docs/e2e-launch.md` — наполняется выбранным N* (Task 9; до его исполнения — N* уже в runner.json и журнале).

- [ ] **Step 4: Подтверждение выбранного канона**

Вторая последовательная зелёная серия на N* (канонический запуск, гейты тишины/чистоты) — «устойчивость, а не удача» (AC6). Фейл — разбор по телеметрии без перезапуска; повтор один раз после анализа (§9).

- [ ] **Step 5: Судьба инструментов + финальная чистота**

Генератор: `git rm -r src/tools/E2eLoadGen` (коммит-хэш удаления — в журнал). Зонд: по гейту Task 7 (если «удаляется» — файлы + откат врезки + сборка зелёная). Финальный гейт: `docker ps -a | grep pgw-` → 0; `docker network ls | grep -E 'pgw-en-|pgw-net-|pgw-noise-|kfw-net'` → 0; `docker volume ls | grep pgw-` → 0; `docker network prune -f`; фиксация в журнал.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(t24): приёмка канона — серии N=3/5/6, выбран N* (<N*>) по критерию «максимально быстрое и стабильное»; runner.json+рельса зафиксированы, подтверждение второй зелёной серией; генератор удалён (AC6/AC10)"
```

**Проверка задачи:** три серии в журнале с измерениями; выбор по критериям с измерениями; N* в runner.json; вторая зелёная серия на N*; хост чист; генератора нет.
**Связь со spec:** §6 фаза 4, AC6, AC10 (также AC4 — финальное N).

---

### Task 8: Рельса канона — E2eParallelismGuard с порогом = выбранный N*

**Вход:** Task 10 (канон N* выбран и зафиксирован). Код рельсы `E2eParallelismGuard` (счётчик живых окружений процесса; предупреждение `[E2E-PARALLELISM]` в stderr + `/tmp/pgw-e2e-static-phase.log`, одно на процесс; стартовые строки `[PHASE] e2e-env … (живых контуров процесса: N)`) — создаётся здесь, порог = N* из Task 10 Step 3.

**Действие (Files):**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eParallelismGuard.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (вызовы guard при старте/dispose окружения)

**Interfaces:**
- Consumes: N* (Task 10).
- Produces: `internal static class E2eParallelismGuard` c `internal const int CanonMaxLiveEnvironments = <N*>;`, `public static int OnEnvironmentStarted(string slug)`, `public static void OnEnvironmentDisposed()`. Рельса меряет ФАКТ (число одновременно живых контуров) — срабатывает при любом способе переопределения потолка.

**Выход:** прогон с фактически переопределённым потолком выше N* маркирует себя в журнале прогона (AC7).

- [ ] **Step 1: E2eParallelismGuard.cs** — класс по контракту Interfaces (предупреждение ссылается на `xunit.runner.json maxParallelThreads / docs/e2e-launch.md «Параллелизм и нагрузка»`; файловая телеметрия — «лучшими усилиями»).

- [ ] **Step 2: Врезка вызовов в E2eEnvironment** — в `StartOnceAsync` перед `return` : `var liveEnvironments = E2eParallelismGuard.OnEnvironmentStarted(slug); Console.WriteLine($"[PHASE] e2e-env {slug}: окружение поднято (живых контуров процесса: {liveEnvironments})");`; в `DisposeAsync` — первой строкой: `E2eParallelismGuard.OnEnvironmentDisposed();`.

- [ ] **Step 3: Сборка** — `dotnet build src/PgWorker.slnx -c Release` → 0/0.

- [ ] **Step 4: Проверка рельсы прогоном с потолком N*+2 (AC7; двухчастный критерий)**

5+ классов (лёгкие + тяжёлые `|E2eMove|E2eBackup` — надёжное перекрытие), потолок `N* → N*+2` по «Операционной модели» (sed → прогон → возврат → контроль): (1) пик достигнут — стартовые строки с числом живых контуров > N* (недостигнутый пик — не фейл рельсы, повторить с расширенным фильтром); (2) при достигнутом пике `grep -c "E2E-PARALLELISM" <лог>` ≥ 1 + строка в static-phase-логе. Диагностический прогон вне канона — легален, но громок; зелёность не критерий шага; падения — разбор без перезапуска.

- [ ] **Step 5: Контроль молчания в каноне** — канонический запуск (N*) тех же классов: `grep -c "E2E-PARALLELISM"` → `0`.

- [ ] **Step 6: Commit** — `feat(t24): рельса канона N* — E2eParallelismGuard: громкое [E2E-PARALLELISM]-предупреждение при фактическом превышении живых контуров (AC7)`.

**Проверка задачи:** Step 4 — пик > N* достигнут И предупреждение есть; Step 5 — молчит в каноне.
**Связь со spec:** §5.6, AC7 (порог = выбранное N).

---

### Task 9: Канонизация — раздел «Параллелизм и нагрузка» в docs/e2e-launch.md от выбранного N*

**Вход:** Task 10 (N* выбран; runner.json уже несёт N*).

**Действие (Files):**
- Modify: `docs/e2e-launch.md` (новый раздел §5 перед «Где что лежит после прогона»)
- Modify: `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` (сверка комментария над ItemGroup копирования runner.json с финальным N*)

**Interfaces:**
- Consumes: N* (Task 10), измерения журнала (серии приёмки), сетевой слой Task 1d.
- Produces: канон прогона целиком (AC5): потолок N*, правило соседей, признаки деградации, действия при фейле — согласованный с runner.json и `docs/e2e-isolation.md`.

- [ ] **Step 1: Раздел в docs/e2e-launch.md** (числа — фактические из журнала приёмки Task 10; `<N*>` заменить выбранным):

```markdown
## 5. Параллелизм и нагрузка (канон прогона серии)

Канон параллелизма полной E2E-серии — **<N*> контура** (`maxParallelThreads=<N*>`
в `src/tests/PgWorker.IntegrationTests/xunit.runner.json` — единственный
источник потолка; xunit.v3 читает конфиг из output). Канон выбран приёмкой
(серии N=3/5/6 на целевой конфигурации, критерий «максимально быстрое и
стабильное» — измерения в журнале задачи t24 в `docs/superpowers/`); выше
канона docker-хост деградирует (DNS/сети), жёсткобюджетные сценарии
Restore/WalStream падают первыми.

Сетевой слой — per-cluster (arch/14 §2.1): сети нод `pgw-net-<C>` создаются
движком и удаляются демонтажем; DNS-зоны контуров изолированы; advertise etcd
— только URL, резолвимые из сетей потребителей (arch/04 §8 п.3). Не-E2E
тесты последовательны всегда: `[Collection]-группа `NonE2eCollection`
(`DisableParallelization=true`).

Правила соседней нагрузки:
- суммарное число одновременно живых E2E-контуров на docker-хосте — ≤<N*>,
  С УЧЁТОМ соседних серий, диагностических контуров и поднятого dev-станда;
- серию не запускать поверх другого тяжёлого docker-прогона — дождаться
  завершения; K=0-базлайны — только после гейта тишины;
- рельса: `E2eEnvironment` при фактическом превышении канона пишет громкое
  `[E2E-PARALLELISM]` (диагностические прогоны с переопределённым потолком
  легальны, но громки; переопределение — временная правка `maxParallelThreads`
  в runner.json с ОБЯЗАТЕЛЬНЫМ возвратом и контролем канонического значения).

Признаки деградации (что смотреть в логах):
- серийные `getaddrinfo … empty` / `failed to resolve host` в docker-логах
  PG-нод (Patroni-клиент etcd: сверять advertise-лист и сеть контейнера —
  алиас чужой сети в advertise недопустим, arch/04 §8 п.3);
- мгновенные (~0,3 с) фейлы коннекта в transient-циклах воркера (`host-*.log`);
- рост `[PHASE] … elapsed` у фаз, которые в тишине проходят быстро;
- рост fails/латентности в логах DNS-зонда (`container-pgw-dns-*.log`,
  `PGW_TEST_E2E_DNS_PROBE=1`).

Действия при фейле серии под нагрузкой:
- разбор по телеметрии teardown БЕЗ перезапуска (§4): этап смерти соединения
  (резолв / TCP / хендшейк) по Patroni-логам + зонду + host.log;
- снизить суммарную нагрузку и повторять ТОЛЬКО после анализа и с гипотезой.
```

- [ ] **Step 2: Сверка согласованности** — комментарий csproj над копированием runner.json соответствует N*; при расхождении — согласовать.

- [ ] **Step 3: Проверка** — `grep -c "Параллелизм и нагрузка" docs/e2e-launch.md` → `1`; `grep '"maxParallelThreads"' src/tests/PgWorker.IntegrationTests/xunit.runner.json` → `<N*>`; `grep -c "e2e-launch.md" src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` → `1`; `grep -rn "CollectionBehavior\|XunitParallelize" --include='*.cs' src/tests/PgWorker.IntegrationTests/` → пусто.

- [ ] **Step 4: Commit** — `docs(t24): канон прогона серии N*=<N*> — раздел «Параллелизм и нагрузка» в e2e-launch.md + согласование runner.json/csproj (AC5)`.

**Проверка задачи:** раздел существует с N*; runner.json/docs/csproj согласованы.
**Связь со spec:** §5.5, AC5 (согласование с runner.json и docs/e2e-isolation.md).

---

### Task 11: Мерж-гейт трека — снятие t24 из roadmap и отчёта надёжности

**Вход:** Tasks 8–10 закрыты (канон N* выбран, зафиксирован, подтверждён). Правки готовятся здесь; в main — мерж-коммитом ветки.

**Действие (Files):**
- Modify: `arch/roadmap/reliability.md` (удалить пункт `t24-e2e-suite-under-load`; grep `← t24`-зависимостей — на момент плана нет)
- Modify: `arch/roadmap/reliability-report.md` (строку из «Осталось» удалить; добавить строку в «Сделано в рамках трека»)

- [ ] **Step 1: reliability.md** — удалить блок `- **\`t24-e2e-suite-under-load\`** — …` целиком. Run: `grep -c "t24" arch/roadmap/reliability.md` → `0`.

- [ ] **Step 2: reliability-report.md — строка «Сделано»** (вместо ‹› — фактические формулировки из journal.md: итоги приёмки N=3/5/6, выбранное N*, вердикт гейта по прочим митигациям):

```markdown
| `t24-e2e-suite-under-load` | — (мерж-коммит t24-e2e-suite-under-load) | устойчивость полной E2E-серии к параллельной нагрузке (характеристика N): сетевая изоляция контуров per-unit — per-cluster сети нод `pgw-net-<C>` движка (arch/14 §2.1: ensure-инвариант «нода и агенты в одной сети кластера», демонтаж чистит сеть; единая `pgw-net` с общей DNS-зоной устранена) + advertise-канон etcd (arch/04 §8 п.3: только резолвимые URL — устранён NXDOMAIN-шторм Patroni по внутрисетевому алиасу, причина стабильных фейлов Restore/WalStream при N=3, разбор 4f7b146); конфигурация параллелизма — xunit.runner.json единственный источник (мёртвые csproj-свойства и CollectionBehavior-атрибут удалены), не-E2E гарантированно последовательны NonE2eCollection; канон параллелизма выбран приёмкой из {3,5,6} по критерию «максимально быстрое и стабильное»: N*=‹N*› (‹время/зелёность серий›), зафиксирован в runner.json/docs/рельсе E2eParallelismGuard, подтверждён второй зелёной серией; ‹прочие митигации по гейту: реализованы ‹состав› / не реализованы — остаточное ограничение Docker Desktop/хоста›; воспроизводимость — нагрузочный генератор (в ветке, не поставляется) и опциональный DNS-зонд PGW_TEST_E2E_DNS_PROBE=1 |
```

- [ ] **Step 3: Проверка синхронности** — `grep -c "t24-e2e-suite-under-load" arch/roadmap/reliability-report.md` → `1` (только «Сделано»); `grep -rc "t24" arch/roadmap/` → `0` вне этой строки.

- [ ] **Step 4: Commit (уходит мерж-коммитом ветки)** — `roadmap(t24): пункт снят, отчёт — строка в «Сделано» (мерж-гейт трека, AC8)`.

**Проверка задачи:** t24 отсутствует в «Осталось» обоих документов; одна строка в «Сделано».
**Связь со spec:** §6 фаза 4, §8 AC8; правило мерж-гейта roadmap (AGENTS.md).

---

## Самопроверка плана (итог self-review)

- **Покрытие spec (ревизии 1–3):** §5.1 генератор → Task 3 (выполнен); §5.2 зонд → Task 2 (выполнен; дополнение — Patroni-лог-анализ для Task 6); §5.3 матрица → Tasks 4 (выполнена, до-митигационные улики) + 5 (целевая конфигурация); §5.4 конфигурация → Task 1 (выполнена; финальное N — Task 10); §5.5 docs → Task 9; §5.6 рельса → Task 8 (порог N*); §5.7 сетевые митигации → Task 1d (движок+E2E+юниты+контрольный прогон), прочие — Task 7; фаза 0 → Tasks 0/1/4 (исполнены; центральный результат — N=3 систематически красный в тишине, причина 4f7b146); фаза 1 → Task 5; фаза 2 → Task 6; гейт (сужен до прочих митигаций) → Task 7; фаза 3 сетевые → Task 1d, прочие+H3 → Task 7; фаза 4 приёмка N=3/5/6 + выбор + тройная фиксация + подтверждение → Task 10; мерж-гейт → Task 11. AC: AC1→Task 5, AC2→Task 6 (доля сетевых выделена), AC3→Task 7, AC4→Task 1+10 (N*), AC5→Task 9, AC6→Task 10, AC7→Task 8, AC8→Task 11, AC9→Task 1d (а–е включая контрольный прогон и прямые проверки изоляции/advertise), AC10→Task 10. Ограничения §7 и риски §9 (миграция усыновлённых → ensure-инвариант Task 1d Step 2; рост числа сетей → демонтаж+ассерт+контроль) — в Global Constraints/задачах.
- **Статусы исполнения:** Tasks 0, 1 (2fe1ac4), 1b (341a577), 1c (baba3c0), 2 (1a920bc), 3 (0bccc5a) — выполнены; Task 4 — исполнен с центральным результатом фазы 0 (№3 39/41, №3п 38/41 — стабильные Restore-фейлы; №4 закрыт пользователем; №5 прерван приказом; причина локализована 4f7b146 → Task 1d). Нумерация задач не менялась; порядок исполнения после паузы: **1d → 5 → 6 → 7 → 10 → 8 → 9 → 11**.
- **Вне scope spec (прямые требования пользователя):** Task 1b (postgres:18-alpine) и Task 1c (AgentImage) — выполнены, статус-блоки с итогами.
- **Типы/имена:** `PlainClusterDriver.NodesNetworkPrefix`/`NetworkName(cluster)` (Task 1d) — по kfw-эталону; константа `NodesNetwork` удаляется; исполнимость ensure-инварианта обеспечена расширением модели: `DockerContainerInspect.Networks` (парсинг `NetworkSettings.Networks` по прецеденту aliases) + `NetworkConnectAsync` как член `IDockerEngine` (прод-реализация + стабы во ВСЕХ 9 реализациях — гейт `grep ': IDockerEngine' src/` → 9 покрытых мест, без CS0535) + FakeEngine (сети в inspect, connect в Calls); гейт `grep '"pgw-net"' src/` пуст благодаря и удалению константы, и замене тестового литерала в DockerEngineTests.cs на `"test-net"`; упоминания `pgw-net` в комментариях (WalStreamProcess:344, ShardEndpoints:189, ClusterDriver:81, WalStreamProcessTests:111, ClusterDriverTests:817, E2eEnvironment:263–266/654/766) актуализируются теми же шагами; `NonE2eCollection.Name` — без изменений; `E2eParallelismGuard.CanonMaxLiveEnvironments = N*` вносится Task 8 при создании рельсы (Task 10 фиксирует N* в runner.json и журнале); advertise: peer-urls не трогаются (потребители в сети окружения), client-urls — только `host.docker.internal:{порт}`; OwnName-матчинг `pgw-net-<C>` по ClusterTag — основа own-only чистки и расширенного ассерта; OwnedEtcdFixture — `_runId` + свойства-выражения с прежними именами `_netName`/`_containerName` (конструктор не меняется; гейт: один `Guid.NewGuid` в файле), инвентаризация `new NetworkBuilder` в src/tests — 3 места, все per-runId (Task 1d Step 5).
- **Операционные механики:** гейт тишины для K=0-прогонов (три замера ×30 с); переопределение потолка sed'ом runner.json с возвратом (приёмка N=3/5/6 — возврат до фиксации выбора, Task 10 Step 3 фиксирует без возврата); остановка соседа `kill -INT` (подтверждена прогоном №5); двухчастная проверка рельсы (пик по стартовым строкам → предупреждение); номерация прогонов — продолжение журнала (№6/№7 — предзаписанные гипотезы Task 5, контрольный Task 1d — следующий свободный номер).
