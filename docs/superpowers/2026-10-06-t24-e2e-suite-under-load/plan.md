# t24-e2e-suite-under-load — план реализации (Фаза 3 dev-flow)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** предмет задачи (ревизия 7, решение пользователя дословно: «ПРОВЕРЯТЬ ПАРАЛЛЕЛЬНЫЙ ЗАПУСК ТЕСТОВ, А НЕ ПОД НАГРУЗКОЙ ЗАПУСК ТЕСТОВ») — устойчивость ПАРАЛЛЕЛЬНОГО запуска: полная E2E-серия стабильно зелёная при N контурах, работающих одновременно (N=3/5/6). Поведение тестов под внешней нагрузкой/шумовым соседом — ВНЕ предмета (прогоны K>0 исключены, H4 исключена, генератор не используется и демонтируется). Причина деградации устраняется: сетевые митигации обязательны, прочие — по гейту фазы 2; канон параллелизма выбирается приёмкой на N=3/5/6 («максимально быстрое и стабильное») и фиксируется в runner.json, docs и рельсе `E2eParallelismGuard`.

**Архитектура:** четыре слоя. (1) Конфигурация параллелизма — Task 1 (выполнена): xunit.runner.json — единственный источник потолка, не-E2E — последовательная `NonE2eCollection`. (2) Сетевой слой — Task 1d (выполнен): per-cluster сети `pgw-net-<C>`, advertise-канон, own-only teardown per-cluster сетей. (3) Портовый слой — Task 1e (выполнена): per-unit окна из 200 портов с bind-резервированием, в движке упразднены смещения +3000/+1500. (4) Расследование остатка ПАРАЛЛЕЛЬНОГО режима по данным прогонов с варьированием N без соседа (№3/№8/№9/№10) → гейт «прочие митигации» (предмет — параллельный запуск: PITR-бюджет, HaEtcd-тайминг, docker-build SecondInstance) → демонтаж зонда (Task 13) → приёмка фазы 4: три серии N=3/5/6, победитель «максимально быстрое И стабильное» тройно фиксируется и подтверждается второй зелёной серией.

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), xUnit v3 3.2.2 (`src/Directory.Packages.props`; конфигурация — xunit.runner.json), Testcontainers 4.14.0, Docker CLI, python:3.12-alpine + alpine:3.20 (зеркалированы), etcd v3.5.21, MinIO.

**Spec:** [`docs/superpowers/2026-10-06-t24-e2e-suite-under-load/spec.md`](spec.md) (ревизии 2026-10-06 (1)–(5): целевая схема параллелизма; сетевой слой per-unit — «свой контур НЕ ПО СЕРВИСАМ А ПО ТЕСТАМ»; приёмка N=3/5/6 и выбор канона; портовой слой — per-contour окна 200; DNS-кривая исключена). Рабочие каноны: `AGENTS.md`, `AGENTS.base.md` §12–13, `docs/e2e-isolation.md`, `docs/e2e-launch.md`; контракты сетевого слоя (arch-first, уже в worktree): `arch/14-pgworker.md` §2.1 (per-cluster сеть `pgw-net-<C>`, ensure-инвариант, демонтаж), `arch/04-deploy-etcd.md` §8 п.3 (advertise — только резолвимые URL). Эмпирика: `journal.md` (прогоны №1–№5, фокус-разбор 4f7b146).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t24-e2e-suite-under-load`, ветка `feat-t24-e2e-suite-under-load`. Все пути — от корня worktree.

**Порядок исполнения:** выполнено — Tasks 0, 1, 1b, 1c, 2, 3, 4 (частично), 1d (c02ed79), 1e (№9), 5 (№10 — 894fbb7), 6, 7, 13, 14; приёмка на разбитой структуре снята (№14 N=3 35/41/30,90 мин; №15 N=5 39/41/20,05 мин; №16 N=6 39/41/20,57 мин; resolve/port = 0 везде; PITR-фейл в каждой — обе причины классифицированы, митигация утверждена); далее: **Task 12 [ДО завершения приёмки — решение пользователя 2026-10-07 «да, сначала таск 12, потом еще раз перемерить»; прежнее «в конец плана» отменено этим же решением] → 10 [№17: полная серия N=5 на новой структуре С фиксом = перемер и подтверждающая серия; при полностью зелёной №17 → N*=5 (PITR закрыт + лучшее время 20,05 < 20,57 < 30,90), тройная фиксация; при незелёной — STOP, правило (б)] → 8 → 9 → 11**.

## Global Constraints

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — 0 warnings в каждой сборке.
- Конфигурация параллелизма (spec §1.3): единственный источник потолка — `src/tests/PgWorker.IntegrationTests/xunit.runner.json` (`maxParallelThreads`; стартовое 3, финальное — приёмка фазы 4, AC10; копируется в output); `CollectionBehavior`-атрибута и `Xunit*`-свойств csproj нет; не-E2E — `[Collection(NonE2eCollection.Name)]` с `DisableParallelization=true`. Канон N — НЕ «≤3 по определению»: выбирается приёмкой из {3,5,6} по критерию «максимально быстрое и стабильное».
- Сетевой слой (spec §1.4, arch/14 §2.1, arch/04 §8 п.3, AGENTS.base §13): per-cluster сети нод `pgw-net-<C>`; константы/общесистемной сети `pgw-net` в коде не остаётся; ensure-инвариант «нода и её wal-агенты в одной сети кластера» (лечение: агент — пересоздание, нода — подключение к сети); advertise etcd — только URL, резолвимые из сетей потребителей (внутрисетевой алиас сети окружения в advertise НЕ включается); per-cluster сети входят в own-only teardown и ассерт чистоты E2E.
- Портовый слой (spec §1.5, arch/14 §2.4 п.2, docs/e2e-isolation.md §5): per-unit окна из 200 портов с физическим резервированием bind'ом (первым портом окна публикуется etcd контура); занятость → сдвиг окна на +200, бюджет попыток конечен; все контейнеры контура — host-порты последовательно из окна; в движке НЕТ схемы смещений +3000/+1500 (последовательные слоты); литеральные PortRange в конфиге воркера E2E запрещены.
- Канон изоляции E2E нерушим (spec §7): guid-контуры, own-only чистка (`OwnName` по runId/тегу), ассерт чистоты, динамические порты, никаких широких фильтров `pgw-*` и глобальных prune из кода.
- Таймауты ожидания агента ≤ 30 с (AGENTS.base §12): длинные прогоны — фоновыми процессами с поллингом хвоста лога. Полл в тестах 500 мс — канон репо. Бюджеты фикстур ≤ 100 с.
- Каждый экспериментальный прогон — с гипотезой, записанной в журнал ДО запуска (spec §3.2); перезапуск упавших запрещён (docs/e2e-launch.md §4); падение — разбор по телеметрии; спорный результат повторяется один раз (spec §9).
- Гейт чистоты хоста перед/последователь КАЖДОЙ серии: `docker ps -a --format '{{.Names}}' | grep pgw-` → пусто; `docker network ls --format '{{.Name}}' | grep -E 'pgw-en-|pgw-net-|kfw-net'` → пусто; `docker volume ls --format '{{.Name}}' | grep pgw-` → пусто; страховка `docker network prune -f`. Чужие объекты НЕ трогать (own-only).
- Образы: только зеркалированные (`dev-stand/images/images.txt`); новых внешних не вводить; локальные в registry НЕ класть.
- Число живых user-сетей не подбирается к пулу ~30 (per-cluster сети короткоживущие, удаляются демонтажем; контроль после каждой серии).
- Генератор нагрузки — инструмент исследования, в целевом состоянии НЕ используется (ревизия 7: прогоны K>0 исключены) и демонтируется с веткой (Task 10, вместе с `dns_probe.py`); встроенный DNS-зонд демонтирован Task 13 (ревизия 6, безусловно). Шумовые прогоны исключены из нормативов: №1 (скомпрометирован чужим раннером), №5 (прерван приказом), №7 (остановлен по приказу удаления noise, кандидат на невалидность — НЕ переигрывается) — история-улики журнала.
- Язык: документация/журнал — русский; идентификаторы — английские; тесты — AAA-комментарии.
- Коммиты — в feature-ветке; мерж-гейт roadmap (Task 11) — тем же мерж-коммитом.

## Операционная модель прогонов (обязательна для Tasks 4, 5, 8, 10)

Полная серия — 37–59 мин; агент НЕ ждёт команду дольше 30 с (AGENTS.base §12). Модель каждого серийного прогона:

1. Записать гипотезу/конфигурацию в `journal.md` (строка «Сводки прогонов»; для новых прогонов — следующий свободный номер; предзаписанные строки №6/№7 журнала — за матрицей Task 5).
2. Гейт чистоты хоста (Global Constraints). Все прогоны задачи — без соседа (ревизия 7); перед каждым — гейт тишины: три контрольных замера `docker ps -a --format '{{.Names}}' | grep -c pgw-` → `0` с интервалом ~30 с (чужие раннеры поднимают контуры циклами — активный проявится между замерами; чужие объекты НЕ трогать, старт отложить, известить контролёра). Условие тишины — в строку журнала.
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

   Шумовые прогоны (сосед-генератор, K>0) — ИСКЛЮЧЕНЫ ревизией 7 (предмет задачи — «параллельный запуск тестов, а не под нагрузкой запуск тестов»): генератор не используется ни в одном прогоне и демонтируется (Task 10). История-улики прежних соседских прогонов (№1 — чужой раннер, №5 — прерван приказом, №7 — остановлен по приказу удаления noise) остаются в журнале и на решения не влияют.
4. Поллинг: раз в ~25 с `tail -5` лога серии — до итоговой строки `Passed!|Failed!`. При появлении ЧУЖИХ контуров в окне серии — зафиксировать в журнал (внешнее вмешательство, прогон кандидат на невалидность), НЕ останавливать без решения контролёра.
5. По завершении: в `journal.md` зелёность, время (`Total duration`), пик живых сетей серии (поллинг `docker network ls --format '{{.Name}}' | grep -c '^pgw-en-'`), DNS-телеметрия (Patroni-логи: `grep -c "failed to resolve host\|getaddrinfo" container-pgw-*.log` по артефактам — ревизия 6), портовая телеметрия (`grep -c "port is already allocated" container-*.log` — критерий AC11(д) для серий приёмки).
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

**Статус: ВЫПОЛНЕН (Фаза 6, коммит c02ed79).** Контрольный прогон №8 (N=3, тишина): 40/41, `resolve`-фейлов 0 — сетевые митигации эффект подтверждён (было 39/41 и 38/41 с массовыми NXDOMAIN); ЕДИНСТВЕННЫЙ фейл №8 — `Bind 0.0.0.0:15102 failed: port is already allocated` (portalloc-гонка литерального диапазона) → закрывается Task 1e. Полные шаги — в git-истории плана; контракт задачи (Files/Interfaces) сохранён ниже как опора Task 1e и ссылок.

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

### Task 1e: Портовые митигации — per-unit окна 200 портов + последовательное выделение (упразднение +3000/+1500)

**Статус: НОВАЯ задача (spec ревизия 4; решение пользователя «решаем сразу в текущей задаче»; контракты arch-first уже в worktree: arch/14 §2.4 п.2, docs/e2e-isolation.md §5). Исполняется ПЕРВОЙ после Task 1d — приказ «сначала portalloc, матрица потом».**

**Вход:** решение пользователя дословно (spec §1.5): «надо чинить. крайне ошибочное решение: на каждую ноду резервируется база + смещения: patroni +3000, doorman +1500. на каждый тест класс резервируется свои 200 портов динамически и для всех контейнеров они выделяются последовательно (никаких + 3000 и + 1500). резервирование происходит запуском на первом порту из диапазона контейнера etcd…; если порт занят — пытаемся следующие 200 портов зарезервировать. решаем сразу в текущей задаче». Улики: прогон №8 (после сетевых митигаций) — единственный фейл `Bind 0.0.0.0:15102: port is already allocated`; прогон №6 (N=5 до фикса, 77d8702) — единственный фейл portalloc-гонка `Acceptance_Ac2_To_Ac7` при 28 passed и 0 resolve-фейлов. Код-факты (сверено): E2E передаёт литеральный PortRange в ДВУХ местах — `E2eEnvironment.cs:475-476` (хост-воркеры) И `E2eSecondInstanceScenarios.cs:146-147` (`RunWorkerContainerAsync`: контейнеры w1/w2, контур `E2eEnvironment.StartAsync` :30); host-порты w1/w2 — `E2eFixture.FreePort()` (:60-61) — вне окна контура (отступление от AC11(г)); смещения в прод-коде живут ТОЛЬКО в `PgPlanning.MakeAddress` (`src/PgWorker.Core/Planning/PgPlanning.cs:15-16`: `new NodePorts(basePort, basePort + 3000, basePort + 1500)`); `PortRangeOptions` — док-комментарий смещений (`src/PgWorker.App/Options.cs:210-216`); потребители `PortAllocator.Allocate` с `PgPlanning` (ProvisioningProcess:325, AdoptionProcess:318, AddShardProcess:218) смещений не содержат — не меняются; юниты со смещениями: `src/tests/Shared.Core.UnitTests/Planning/PortAllocatorTests.cs:14,105`, `src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs:15,22,188`; kfw/Valkey — свои `MakeAddress` без смещений (не трогаются); etcd-порты контура — `E2eFixture.FreePort()` (E2eEnvironment.cs:253), MinIO — `assignRandomHostPort` (:327) — переводятся на окно.

**Действие (Files):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (окно контура: bind-резерв через etcd, последовательная выдача MinIO/API-портам, PortRange воркеру = остаток окна — замена литерала :475-476; экспонирование окна сценариям; док-комментарии)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs` (второй литеральный PortRange :146-147 → остаток окна контура; порты w1/w2 :60-61 → из счётчика окна)
- Modify: `src/PgWorker.Core/Planning/PgPlanning.cs` (MakeAddress — последовательная тройка вместо +3000/+1500; doc)
- Modify: `src/PgWorker.App/Options.cs:210-216` (док-комментарий `PortRangeOptions`: последовательные слоты per-node, диапазон параметризуется средой — прод per-install / E2E per-contour окно)
- Modify: `src/tests/Shared.Core.UnitTests/Planning/PortAllocatorTests.cs` (правка смещений + НОВЫЕ: последовательность слотов, исчерпание диапазона — фейл, не выход за To)
- Modify: `src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs` (правка смещений :15/:22/:188)
- Modify: `docs/e2e-isolation.md` §4 (актуализация канона ретрая старта — Finding-синхронизация с оконным бюджетом)
- Modify: `src/AdminPanel.Api/appsettings.Development.json:28` (комментарий `_comment2`: маппинги doorman 165xx/Patroni 18xxx устаревают)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (контрольный прогон, итоги AC11)

**Interfaces:**
- Consumes: контракты arch/14 §2.4 п.2 и docs/e2e-isolation.md §5 (обновлены spec-агентом); `PortAllocator.Allocate(plan, existing, taken, from, to, PortsOf, HostOf, MakeAddress, KeyOf)` — сигнатура НЕ меняется (смещение инкапсулировано в `MakeAddress`); внутриконтурные защиты (лок portalloc в своём etcd, занятость docker ∪ portalloc, глобальный клэйм) — сохраняются как есть.
- Produces (для Tasks 5–10, AC11):
  - окно контура: `const int PortWindowSize = 200;` + резервирующая выдача в `E2eEnvironment.StartOnceAsync`: перебор окон от базовой границы (напр. `PortWindowBase = 15100`) с шагом +200; для каждого окна etcd-узлы контура поднимаются с публикацией ПОСЛЕДОВАТЕЛЬНЫХ портов окна (bind = физическое резервирование); bind/старт-фейл → окно сдвигается, контур пересоздаётся (уже существующий ретрай-цикл `StartAsync` против foreign-prune расширяется распознаванием портового фейла); бюджет попыток конечен (40 окон — быстрый фейл с диагностикой в исключении: база окна, занявший порт);
  - последовательная выдача из окна остатку контура: MinIO (замена `assignRandomHostPort`), API-порты хост-процессов воркеров (`StartHostOnPortAsync` — вместо `FreePort()`); воркеру `PgWorker__Docker__PortRange__From/To` = окно за вычетом выданного инфраструктурой (`From = base + <выдано>`, `To = base + 200`);
  - экспонирование окна сценариям (для E2eSecondInstanceScenarios): `public int ReserveWindowPort()` — следующий порт счётчика `_windowNext` (w1/w2-контейнеры, любые контейнерные порты сценария) и `public (int From, int To) RemainingPortRange` — остаток окна для `PortRange` воркер-контейнеров (тот же контракт, что у хост-воркеров);
  - `PgPlanning.MakeAddress(host, basePort) => new(host, new NodePorts(basePort, basePort + 1, basePort + 2));` — последовательная тройка per-node (arch/14 §2.4 п.2); `PortsOf`/потребители не меняются; advertised-адреса — от фактических портов (уже канон §13 п.4).

**Выход:** межконтурные портовые коллизии исключены конструкцией (окна резервируются bind'ом до старта нод); «port is already allocated» между контурами исчезает из серий (AC11 а–г; (д) — серии приёмки Task 10).

- [ ] **Step 1: E2eEnvironment — окно контура с bind-резервом через etcd**

В `StartOnceAsync` заменить выдачу etcd-портов (`E2eFixture.FreePort()`, :253) на оконную стратегию (скелет; ретрай-цикл `StartAsync` сохраняется):

```csharp
    // Портовое окно контура (t24, spec §1.5, arch/14 §2.4 п.2): 200 портов,
    // физическое резервирование bind'ом — первым(и) портом(ами) окна
    // публикуются etcd-узлы (etcd поднимается окружением всегда — его подъём
    // «запирает» окно); занятость (bind/старт-фейл) → окно сдвигается на
    // следующий блок → пересоздание контура. Бюджет конечен: быстрый фейл
    // с диагностикой, не вечный ретрай.
    private const int PortWindowBase = 15100;
    private const int PortWindowSize = 200;
    private const int PortWindowBudget = 40;
```

Механика: `for (var w = 0; w < PortWindowBudget; w++) { var windowStart = PortWindowBase + w * PortWindowSize; /* etcd-порты = windowStart..+etcdNodes.Count; попытка StartOnceAsync с ними */ }` — bind-фейл порта (сообщение `port is already allocated` / `bind: address already in use`) распознаётся в существующем catch-ретрае `StartAsync` (расширить предикат `IsForeignPruneRace` → общий `IsRetryableStartRace`: network-not-found ИЛИ портовый bind-фейл) → следующая итерация окна; успех — база окна сохраняется в поле контура (`_portWindowStart`), следующий выдаваемый порт — счётчик `_windowNext`. Исчерпание бюджета — `ApplicationException` с диагностикой (просмотренные окна, последнее сообщение bind-фейла).

Актуализация канона `docs/e2e-isolation.md` §4 (обязательна — иначе док дезинформирует): «обёртка из 3 попыток… ретрай только на распознанную гонку (`network` + `not found`)» заменить на оконную редакцию: ретрай старта — на распознанные ретрай-условия (чужой prune сети / портовый bind-фейл — окно сдвигается на следующий блок 200), бюджет = бюджет окон (`PortWindowBudget`), ретраится подъём заново, не «продолжить с половиной окружения».

- [ ] **Step 2: Последовательная выдача остатку контура + PortRange воркеру**

(а) MinIO: `WithPortBinding(9000, assignRandomHostPort: true)` (:327) → `WithPortBinding(_windowNext++, 9000)`; (б) API-порты воркеров: `StartHostOnPortAsync` — `var port = E2eFixture.FreePort();` (:418) → `var port = _windowNext++;` (ретрай `address already in use` из :407 становится невозможным внутри окна — ретрай-блок не удалять, он безвреден); (в) литерал :475-476 → `["PgWorker__Docker__PortRange__From"] = (_portWindowStart + выдано).ToString(), ["PgWorker__Docker__PortRange__To"] = (_portWindowStart + PortWindowSize).ToString()`; (г) док-комментарии E2eEnvironment, объяснявшие литерал, — актуализировать под окно (канон e2e-isolation §5: литеральные диапазоны в конфиге воркера запрещены); (д) **E2eSecondInstanceScenarios — ВТОРОЙ литеральный PortRange (:146-147) и порты w1/w2 вне окна (:60-61)**: `RunWorkerContainerAsync` — `PortRange__From/To` из `Fx.RemainingPortRange` (остаток окна контура, тот же контракт, что у хост-воркеров); `var p1 = E2eFixture.FreePort(); var p2 = E2eFixture.FreePort();` → `var p1 = Fx.ReserveWindowPort(); var p2 = Fx.ReserveWindowPort();` — оба воркер-контейнера и их порты входят в окно контура (AC11(г)); без этого фикса w1/w2 гоняются за теми же 15100–15200, что и первое кандидатное окно Task 1e (`PortWindowBase = 15100`) — ровно класс фейлов №8/№6.

- [ ] **Step 3: Движок — упразднение смещений + юниты**

`PgPlanning.cs:15-16`:

```csharp
    /// <summary>Адрес ноды: последовательная тройка слотов диапазона
    /// (pg=base, patroni=base+1, doorman=base+2 — arch/14 §2.4 п.2, t24:
    /// схема смещений +3000/+1500 упразднена; диапазон параметризуется
    /// средой — прод per-install, E2E — per-contour окно).</summary>
    public static NodeAddress MakeAddress(string host, int basePort)
        => new(host, new NodePorts(basePort, basePort + 1, basePort + 2));
```

`Options.cs:210` — док-комментарий `PortRangeOptions`: «Диапазон портов нод [From, To): последовательные слоты per-node (pg, +1 patroni, +2 doorman — arch/14 §2.4 п.2); параметризуется средой (прод — per-install; E2E — per-contour окно)». Юниты: правка смещений в `PortAllocatorTests` (:14 `MakeAddress`-хелпер, :105 комментарий) и `PortPlanConvergenceTests` (:15/:22/:188 `pg + 3000 / pg + 1500` → `pg + 1 / pg + 2`); НОВЫЕ (AAA): последовательность — `Allocate` двух нод даёт непересекающиеся последовательные тройки внутри [From,To); исчерпание — заполненный диапазон → фейл (не выход за `To`).

Сопутствующая правка комментария (фиксация в journal): `src/AdminPanel.Api/appsettings.Development.json:28` — `_comment2` «…doorman 165xx, Patroni 18xxx…» устаревает (маппинги 165xx/18xxx становятся неиспользуемыми после упразднения смещений; функционально плотный HostMap `local__15000..15999` покрывает) — из `_comment2` убрать упоминания диапазонов смещений, оставив актуальные portalloc-маппинги стенда.

- [ ] **Step 4: Сборка + гейты**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet test src/tests/Shared.Core.UnitTests src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~PortAllocator|FullyQualifiedName~PortPlanConvergence"`
Expected: 0/0, юниты зелёные. Гейты: `grep -rn '+ 3000\|+ 1500\|patroni=+3000\|doorman=+1500\|pg=base' --include='*.cs' src/PgWorker.Core/ src/PgWorker.App/ src/tests/` → пусто (смещения ушли; kfw/Valkey не содержали); `grep -rn '"15100"\|"15200"' src/tests/PgWorker.IntegrationTests/E2e/` → **0 вхождений по всему каталогу E2e** (оба литеральных PortRange — E2eEnvironment и E2eSecondInstanceScenarios — ушли; паттерн с кавычками матчит ТОЛЬКО строковые литералы конфигурации — int-константа `PortWindowBase = 15100` им не покрывается и допустима как база перебора окон).

- [ ] **Step 5: Контрольный прогон (гейт тишины; N=3 канонический; новая строка журнала) — проверки AC11(а)–(г)**

Гипотеза ДО: «после портовых митигаций каноническая серия зелёная 41/41: межконтурных "port is already allocated" нет (AC11д-предвестник), окна параллельных контуров дискретны». По «Операционной модели»; во время серии (≥2 живых контура):
(в) `docker ps --format '{{.Names}} {{.Ports}}' | grep pgw-` — фактические published-порты разных контуров НЕ пересекаются (группируются по непересекающимся блокам по 200);
(г) порты контура возрастают последовательно от базы окна (etcd → MinIO → API → ноды: подряд, без +3000/+1500-скачков);
(а)(б) — гейты кода Step 4. Телеметрия: `grep -c "port is already allocated" container-*.log` по артефактам → 0. Итог + проверки — в журнал.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(t24): портовые митигации — per-unit окна 200 портов (bind-резерв через etcd, последовательная выдача контуру) + упразднение смещений +3000/+1500 движка (PgPlanning — последовательная тройка, arch/14 §2.4 п.2; AC11, spec §1.5)"
```

**Проверка задачи:** Step 4-гейты пусты; контрольный прогон зелёный (или разобранный по телеметрии фейл с НОВОЙ причиной — вердикт в журнал, STOP и вопрос контролёру); проверки AC11(в)/(г) зафиксированы в журнале.
**Связь со spec:** §1.5 (портовый факт и целевое состояние — решение пользователя), §5.8, §6 фаза 3 «Портовые митигации (обязательные)», §7, AC11 (а–г; (д) — серии Task 10); контракты arch/14 §2.4 п.2 + docs/e2e-isolation.md §5.

---

### Task 2: Синтетический DNS-зонд — общий скрипт + опция в E2E-окружении

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 1a920bc).** `dns_probe.py` (measure/storm, три категории целей: алиас сети, `host.docker.internal`, внешнее `quay.io`), `E2eDnsProbe` (контейнер `pgw-dns-{runId}`, лог подбирается телеметрией), врезка в `E2eEnvironment` (объявление до try; catch — best-effort dispose). Smoke зелёный.

**Дополнение по эмпирике (для Task 6, НЕ переделка):** тракт зонда (сеть окружения) НЕ видит Patroni-путь (per-cluster сети) — факт №3/№3п: у зонда 0 полных фейлов при бурях у Patroni. Вывод журнала: различение гипотез дополняется анализом Patroni-логов (`getaddrinfo`/`failed to resolve host` в `container-pgw-*.log`), не только CSV зонда.

**Interfaces:** Produces — env-контракт `dns_probe.py` (`DNS_PROBE_TARGETS/MODE/INTERVAL`), `E2eDnsProbe.Build(runId, net, targets, mode, intervalSec)`, опция `PGW_TEST_E2E_DNS_PROBE=1`.
**Связь со spec:** §5.2, §4 (H1/H3), §7.

---

### Task 3: Диагностический нагрузочный генератор `src/tools/E2eLoadGen`

**Статус: ВЫПОЛНЕН (Фаза 6, коммит 0bccc5a) и выведен из использования (ревизия 7):** прогоны с шумовым соседом (K>0) исключены из целевого состояния — генератор (включая dns-профиль и `dns_probe.py`) не используется ни в одном прогоне и демонтируется с веткой (Task 10). История использования: прогон №5 (сосед K=3 full отработал 27:43, SIGINT-teardown — 0 остатков) — улика журнала.

**Interfaces:** — (инструмент исследования; описание CLI — в git-истории плана и журнале).
**Связь со spec:** §5.1 ревизии 7 (в целевом состоянии НЕ используется, демонтируется).

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

### Task 5: Фаза 1 — матрица параллельного варьирования N (ревизия 7)

**Статус: ФАКТИЧЕСКИ ЗАКРЫТА (прогон №10/№6-пост — 894fbb7; остаётся сводка фазы 1).**

**Вход:** Task 1d (№8 — 40/41, resolve-фейлов 0) и Task 1e (№9) — целевая конфигурация. Матрица ревизии 7 — ПАРАЛЛЕЛЬНОЕ варьирование N, БЕЗ внешнего соседа: (N=3) — контроль, (N=5) — симптом Б. До-митигационные улики в журнале остаются (№6-до-фикса — 77d8702: 28+1, portalloc-гонка, resolve 0; №1/№5/№7 — шумовая история, ревизия 7 исключила K>0: прогон №7 остановлен в начале по приказу удаления noise — кандидат на невалидность, НЕ переигрывается). DNS-кривая исключена (ревизия 5), H4 исключена (ревизия 7).

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (вердикты фазы 1).

**Interfaces:**
- Consumes: прогоны №3/№3п/№8/№9/№10 (параллельное N-варьирование без соседа), Patroni-лог-анализ (Task 2-дополнение), «Операционная модель» (переопределение N, гейты тишины/чистоты).
- Produces: данные фазы 2 (Task 6): H2/H3-вердикты по параллельным прогонам; симптом Б на целевой конфигурации ОПРОВЕРГНУТ как DNS/портовый (№10: resolve-фейлов 0, port-allocated 0 при N=5).

- [x] **Step 1: Прогон №6-пост (журнальный №10) — (N=5, переопределённый потолок, тишина) — ВЫПОЛНЕН (894fbb7)**

Итог: 38/41 за 34,7 мин; resolve-фейлов 0, port-allocated 0 — симптом Б (DNS/портовая деградация при N≥5) на целевой конфигурации ОПРОВЕРГНУТ; 3 фейла H2/H3-класса параллельного режима: HaEtcd NullReference-тайминг, SecondInstance docker-build > 120 с, Restore_TargetTime PITR (повторился — устойчив при N=5).

- [ ] **Step 2: Сводка фазы 1 — предварительные вердикты карты H1–H3 + commit**

`journal.md`: H1 — закрыт сетевыми митигациями (№8/№9/№10: resolve 0); симптом Б — опровергнут на целевой конфигурации; H2/H3 — кандидаты по фейлам №10 (конкуренция CPU/IO параллельного режима и бюджеты); H4 — исключена ревизией 7 (строка-пометка). Commit `docs(t24): фаза 1 — матрица параллельного N-варьирования закрыта прогоном №10 (38/41, симптом Б опровергнут; H2/H3-кандидаты в фазу 2)`.

**Проверка задачи:** сводка в журнале (вердикты H1–H3 + пометка исключения H4); новые прогоны — только с гипотезой ДО и гейтами тишины/чистоты.
**Связь со spec:** §6 фаза 1 ревизии 7 (параллельное варьирование N без соседа), AC1 (симптом Б воспроизведён или опровергнут — опровергнут, оба исхода результат).

---

### Task 6: Фаза 2 — различение гипотез H1–H3 (H4 исключена ревизией 7) и карта «симптом → причина → измерение»

**Вход:** Task 5 закрыта (прогон №10/№6-пост — 894fbb7; сводка фазы 1) + до-митигационные улики (№3/№3п/№4) + фокус-разбор 4f7b146 (подтверждённый экземпляр H3: NXDOMAIN-шторм → потеря DCS → съеденные бюджеты) + портовая компонента закрыта Task 1e. Данные — параллельные прогоны с варьированием N без соседа: №3/№3п (N=3 до-сетевых), №8 (N=3 целевой), №9 (N=3 целевой, после 1e), №10 (N=5 целевой: 38/41, resolve 0, port 0; фейлы — HaEtcd NullReference-тайминг, SecondInstance docker-build >120 с, Restore_TargetTime PITR — устойчив при N=3/5, кандидат H3). H4 — исключена решением пользователя (ревизия 7), не проверяется.

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (карта гипотез H1–H3 + строка-пометка исключения H4; доля, закрываемая сетевыми и портовыми митигациями; оценка остатка параллельного режима).

**Interfaces:**
- Consumes: телеметрия параллельных прогонов №3/№3п/№8/№9/№10 — docker-логи, host.log, Patroni-логи (`failed to resolve host`/`getaddrinfo` — ревизия 6), `[PHASE]`-тайминги; корреляция фейлов с N (3 vs 5) и с конкуренцией внутри серии.
- Produces (вход гейта Task 7): карта H1–H3 «следствие → измерение → подтверждено/исключено» со ссылками на артефакты (AC2); доли, закрываемые сетевыми/портыми митигациями; кандидаты прочих митигаций параллельного режима (PITR-бюджет; HaEtcd-тайминг; docker-build SecondInstance) с оценкой «дёшево/дорого».

- [ ] **Step 1: H1 — вердикт по имеющимся уликам (ЗАКРЫТ ревизией 5; отдельного эксперимента нет)**

По 4f7b146 (улика `getaddrinfo empty` — Patroni-клиент etcd по advertise-алиасу, не docker-резолвер) + факт-подтверждения целевой конфигурации (№8/№9/№10 — resolve-фейлов 0 при N=3 и N=5). Итоговая строка карты: «H1 — закрыт сетевыми митигациями (пересекающиеся DNS-зоны + advertise-алиас), решение ревизии 5; артефакты — 4f7b146, №8, №9, №10».

- [ ] **Step 2: H2 — конкуренция CPU/IO параллельного режима (без синтетического эксперимента — генератор исключён ревизией 7)**

Вердикт по фейлам №10 против базлайнов №8/№9 (N=3): HaEtcd NullReference-тайминг и SecondInstance docker-build > 120 с — появляются/усиливаются ли с ростом N (3→5); сверка таймингов одинаковых фаз по `[PHASE]`-логам и host.log (замедление конкретных этапов при 5 контурах = конкуренция CPU/IO самой серии). Если фейлы N-независимы (встречаются и при N=3/№8-№9) — H2-часть ослабляется, вердикт «единичные тайминги фикстур». Строка карты с измерениями.

- [ ] **Step 3: H3 — транзиент-циклы против бюджетов**

Улики №3/№3п (до-митигационные) + целевые: Restore_TargetTime PITR — УСТОЙЧИВЫЙ H3-кандидат (№3/№9/№10 при N=3/5, паттерн «T0-часть восстановлена, WAL-хвост не применён»; секундная гранулярность recovery_target_time + расхождение часов) — в гейт Task 7 (митигация уже в плане: Task 12 — пауза 5 с; гейт подтверждает по данным). Дополнительно: какие бюджеты съедаются медленными ретраями при N=5 (по host.log упавших). Вердикт в карту.

- [ ] **Step 4: H4 — строка-пометка исключения**

В карту: «H4 — исключена решением пользователя (ревизия 7: предмет — параллельный запуск, не под нагрузкой); не проверяется». Без экспериментов.

- [ ] **Step 5: Доли митигаций + вердикт остатка + commit**

В журнал: (а) доля, закрываемая сетевыми+портыми митигациями (№3/№3п против №8/№9/№10); (б) остаток параллельного режима — кандидаты гейта Task 7 с оценкой; (в) вердикт «в наших границах или ограничение хоста». Commit `docs(t24): фаза 2 — карта H1-H3 (H4 исключена ревизией 7), доли митигаций, остаток параллельного режима в гейт`.

**Проверка задачи:** карта H1–H3 полна + пометка H4; каждый вердикт — со ссылкой на артефакт/прогон; ни один новый прогон — без гипотезы в журнале.
**Связь со spec:** §6 фаза 2 ревизии 7 (H1–H3; H4 исключена), AC2, вход ГЕЙТА.

---

### Task 7: ГЕЙТ РЕШЕНИЯ — прочие митигации параллельного режима (сверх сетевых и портовых) + H3-уточнения

**Вход:** Task 6 (карта H1–H3 полная; сетевые — Task 1d, портовые — Task 1e, обе вне гейта). Предмет гейта (ревизия 7): остаточные причины НЕзелёности параллельного запуска по фейлам №10 — (1) Restore_TargetTime PITR (устойчивый H3-кандидат №3/№9/№10; митигация уже в плане — Task 12, пауза 5 с: гейт подтверждает её достаточность по данным либо расширяет ПО ДАННЫМ, не молча); (2) HaEtcd NullReference-тайминг (конкурентный старт 3-узлового etcd-контура при N контурах — кандидат: таймаут/ретрай пробы или сериализация старта); (3) SecondInstance docker-build > 120 с (сборка образа воркера в сценарии при параллельной конкуренции — кандидаты: бюджет static-фазы, предсборка в статических ассетах). Формулировки митигаций — ПО ДАННЫМ телеметрии, без расширения рамок молча.

**Действие (Files):**
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (секция «Гейт решения»: прочие митигации параллельного режима).

**Interfaces:**
- Consumes: карта Task 6, критерии spec §6 «ГЕЙТ РЕШЕНИЯ» (редакция ревизий 3–7).
- Produces: зафиксированное решение (+ состав прочих митигаций либо их отклонение) — вход фазы приёмки Task 10.

- [ ] **Step 0: Актуализация каркаса журнала под ревизии 3–7 spec**

Каркас (Task 0) писался до ревизий 2–7 и несёт устаревшие строки-подсказки (НЕ записи результатов — их не трогать): (а) секция «Гейт решения (AC3)» — критерии-подсказки «ветка А/Б», «канон N=3 + рельсы — ограничение Docker Desktop/хоста» заменить формулировками spec §6 (редакции 3–7): гейт решает ТОЛЬКО «прочие митигации СВЕРХ сетевых и портовых» ПРЕДМЕТА «параллельный запуск» (реализуем дешёвые / не реализуем — остаточное ограничение Docker Desktop/хоста); сетевые — Task 1d, портовые — Task 1e, вне гейта; канон параллелизма выбирает приёмка Task 10 из {3,5,6} — НЕ гейт; (б) строку-гипотезу №6 «Сводки прогонов» дополнить пометкой «до портового фикса (улика); повтор — №10 (894fbb7)»; (в) строку №7 — пометкой «остановлен по приказу удаления noise (ревизия 7), кандидат на невалидность, НЕ переигрывается». Проверка: `grep -c "ветка А\|ветка Б" journal.md` → `0`; пометки на месте.

- [ ] **Step 1: Применить критерии к трём кандидатам**

По карте Task 6, для каждого из (1) PITR / (2) HaEtcd-тайминг / (3) SecondInstance build: причина в наших границах с дешёвым каноничным фиксом → «реализуем» (PITR — уже Task 12, сверить достаточность 5 с по данным; HaEtcd/build — сформулировать фикс по телеметрии); остаточное — ограничение Docker Desktop/хоста или дороже выгоды → «не реализуем» с измерениями.

- [ ] **Step 2: Зафиксировать решение**

- **Реализуем** — состав в журнал; каждый новый кандидат — свой мини-эксперимент (реализация → точечный прогон-проверка; крупный/рискованный состав → вопрос пользователю через контролёра, `NEEDS_CONTEXT`).
- **Не реализуем** — фиксируется с измерениями.
- Критерии неоднозначны → `NEEDS_CONTEXT` (вопрос + варианты), ответ дословно в журнал.
- Судьба инструментов решена вне гейта: генератор — демонтируется Task 10 (ревизия 7: не используется); DNS-зонд — демонтирован Task 13 (ревизия 6).

- [ ] **Step 3: H3-уточнения (если вклад H3 в остатке подтверждён картой)**

Точечные уточнения телеметрии/бюджетов медленных фаз параллельного режима (PITR-пауза — Task 12; прочие — каждое отдельным коммитом с объяснением ПО ТЕЛЕМЕТРИИ; запрет «просто увеличить таймаут» не снимается). Нет вклада — запись «не требуется».

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "docs(t24): гейт — решение по прочим митигациям параллельного режима (PITR/HaEtcd/build) зафиксировано (AC3, ревизия 7)"
```

**Проверка задачи:** секция «Гейт решения» заполнена (критерии → измерения → решение по каждому кандидату [+ вопрос/ответ]).
**Связь со spec:** §6 ГЕЙТ РЕШЕНИЯ (редакция 7: предмет — параллельный запуск), AC3.

---

### Task 13: Демонтаж встроенного DNS-зонда (ревизия 6)

**Статус: ВЫПОЛНЕН (демонтаж по ревизии 6 — итоги в журнале: `E2eDnsProbe.cs` удалён, врезка `E2eEnvironment` вырезана, env-опция `PGW_TEST_E2E_DNS_PROBE` демонтирована; серии приёмки — без зонда; прогоны до демонтажа с зондовыми CSV — законный артефакт Task 2, spec §6 фаза 1). Порядок (исполнен): … → 7 → 13 → 10[№14–№16] → 12[досрочная] → 10[№17] → 8 → 9 → 11.**

**Вход:** spec §5.2 ревизии 6: «**Синтетический DNS-зонд — ИСКЛЮЧЁН из целевого состояния** (решение пользователя)… подлежит ДЕМОНТАЖУ отдельной задачей плана: врезка `E2eDnsProbe.cs`, крюки в `StartOnceAsync`/catch/`DisposeAsync`, env-опция — удаляются». Обоснование spec: причина DNS-симптоматики устранена сетевыми митигациями (per-cluster сети + advertise-фикс; прогоны №8/№9 — Patroni resolve-фейлов 0), отдельная DNS-телеметрия контура не нужна; контроль DNS-здоровия оставшихся прогонов — по Patroni-логам/docker-логам кластеров (`failed to resolve`/`getaddrinfo` считаются по ним — главный источник и так). Код-якоря (сверено, диапазоны фактические): `E2eDnsProbe.cs` (класс + `ScriptRelativePath`); врезка `E2eEnvironment.cs` — поле `:75`, параметр конструктора `:121` + присвоение `:134`, объявление `IContainer? dnsProbe = null;` до try `:295`, if-блок env-опции с комментарием `:410-427`, передача в конструктор `:429`, catch-best-effort-dispose `:446-457`, фрагмент `DisposeAsync` `:754-763`. `dns_probe.py` ОСТАЁТСЯ: общий скрипт — им пользуется генератор (`NoiseContour.cs:19` монтирует по тому же пути; spec §5.2: «DNS-профиль ГЕНЕРАТОРА — НЕ трогать: это инструмент нагрузки, не телеметрия»); `OwnName`/`CollectDiagnosticsAsync` — общая телеметрия окружения (не зонд-специфичная) — НЕ трогать.

**Действие (Files):**
- Delete: `src/tests/PgWorker.IntegrationTests/E2e/E2eDnsProbe.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (вырезать врезку зонда целиком — все якоря выше)
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py` (только doc-строка: measure-режим/телеметрия E2E исключены — остаётся storm-режим генератора)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (строка к ревизии 6)

**Interfaces:**
- Consumes: завершённые прогоны матрицы Task 5 (зонд в них — законный артефакт Task 2).
- Produces: целевое состояние ревизии 6 — в тестовом контуре нет зонд-контейнера; DNS-контроль прогонов — только Patroni/docker-логи (подсчёт `failed to resolve host|getaddrinfo` по `container-pgw-*.log` — уже в операционной модели п.5).

**Выход:** демонтаж без следов в тестовой сборке; серии приёмки Task 10 идут без зонда; генератор (dns-профиль) работает по-прежнему.

- [ ] **Step 1: Вырезать врезку из E2eEnvironment.cs (зеркально Task 2; фрагменты ЦЕЛИКОМ — включая комментарии и try/catch-обёртки)**

Удалить (диапазоны фактические, сверено): объявление `IContainer? dnsProbe = null;` (:295, до try); if-блок env-опции С комментарием «Синтетический DNS-зонд…» (:410–427 — комментарий четырьмя строками выше `if` тоже удаляется, иначе осиротевший); аргумент `dnsProbe` из вызова конструктора (:429) и сам параметр/присвоение (:121/:134); поле `_dnsProbe` (:75); catch-фрагмент best-effort dispose ЦЕЛИКОМ (:446–457 — комментарий «зонд поднимается последним…» + if/try/catch с внутренним комментарием); фрагмент `DisposeAsync` ЦЕЛИКОМ (:754–763 — try/catch со строкой `problems.Add($"dns-probe: …")`, пустая обёртка не остаётся). Общие механизмы (`OwnName`-матчинг по runId, `CollectDiagnosticsAsync`, `FailedTearDownAsync`) — НЕ трогать: они не зонд-специфичны, просто исчезает контейнер `pgw-dns-{runId}`.

- [ ] **Step 2: Удалить E2eDnsProbe.cs + актуализировать dns_probe.py**

```bash
git rm src/tests/PgWorker.IntegrationTests/E2e/E2eDnsProbe.cs
```

В `dns_probe.py` doc-строку привести к актуальному назначению (скрипт генератора: storm-агрегаты `storm-dns,…`; measure-режим и «подбирается CollectDiagnosticsAsync» — исключены ревизией 6).

- [ ] **Step 3: Сборка + гейты**

Run: `dotnet build src/PgWorker.slnx -c Release && dotnet build src/tools/E2eLoadGen -c Release`
Expected: 0/0 обе сборки (скрипт генератора на месте — dns-профиль жив). Гейт (паттерн расширен: ловит осиротевшие комментарии/обёртки — «Синтетический DNS-зонд», пустой try/catch со строкой `dns-probe`): `grep -rn 'E2eDnsProbe\|pgw-dns\|PGW_TEST_E2E_DNS_PROBE\|dnsProbe\|dns-probe\|DNS-зонд' --include='*.cs' src/tests/` → пусто (`dns_probe.py` — не .cs, в паттерн не входит и остаётся за гейтом честно: единственный потребитель — генератор).

- [ ] **Step 4: Smoke-серия E2e без зонда (гейт тишины)**

1–2 лёгких класса (например `E2eAppParams`), по «Операционной модели», БЕЗ env-префикса: контуры поднимаются и вычищаются без зонда (ассерт чистоты зелёный), в артефактах нет `container-pgw-dns-*`. Итог — в журнал строкой к ревизии 6 («встроенный зонд демонтирован; smoke N/N зелёный»).

- [ ] **Step 5: Синхронизация исполнения последующих команд**

Прогоны после Task 13 (приёмка Task 10, повторы Task 12) исполняются БЕЗ префикса `PGW_TEST_E2E_DNS_PROBE=1` — переменная демонтирована; встречающийся в командах последующих задач префикс — историческая редакция до ревизии 6, при исполнении опускается.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(t24): демонтаж встроенного DNS-зонда — E2eDnsProbe, врезка E2eEnvironment, env PGW_TEST_E2E_DNS_PROBE (ревизия 6, решение пользователя); dns_probe.py остаётся генератору"
```

**Проверка задачи:** Step 3-гейты пусты; smoke-серия зелёная без зонда; генератор собирается, dns-профиль не тронут; строка журнала к ревизии 6 есть.
**Связь со spec:** §5.2 ревизии 6 (дословно: «Синтетический DNS-зонд — ИСКЛЮЧЁН из целевого состояния (ревизия 6, решение пользователя)… подлежит ДЕМОНТАЖУ отдельной задачей плана»); §6 фаза 1 («до демонтажа врезки прогоны могут продолжать использовать уже внедрённый зонд — артефакт Task 2»); §7 («встроенный DNS-зонд демонтируется отдельной задачей плана (ревизия 6, §5.2)»).

---

### Task 10: Фаза 4 — приёмка канона параллелизма N=3/5/6, выбор и тройная фиксация

**Вход:** Task 1d + Task 1e + Task 7 (целевая конфигурация) + Task 14 (разбиение гигантов — ревизия 8). РЕШЕНИЕ пользователя (2026-10-07, дословно): «если тесты изменились, то и предыдущие N уже невалидны, так что перепрогонять 3, 5 и 6» — ВСЕ точки приёмки снимаются заново на НОВОЙ структуре. Порядок исполнения: ДО Tasks 8–9 (рельса и docs получают выбранное здесь N).

**Действие (Files):**
- Modify: `src/tests/PgWorker.IntegrationTests/xunit.runner.json` (финальное `maxParallelThreads` — победитель; фиксируется БЕЗ возврата)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (три серии приёмки + выбор + подтверждение)
- Delete: `src/tools/E2eLoadGen/` (судьба по гейту Task 7 — генератор)
- Delete: `src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py` — удаляется ВМЕСТЕ с генератором (после Task 13 потребителей нет: E2eDnsProbe.cs и врезка E2eEnvironment уже демонтированы задачей 13)

**Interfaces:**
- Consumes: «Операционная модель» (серии приёмки — «полноправные канонические прогоны»: N переключается правкой runner.json с возвратом до фиксации; гейт тишины/чистоты на КАЖДУЮ серию).
- Produces (AC10): выбранный канон `N* ∈ {3,5,6}` — по критерию «МАКСИМАЛЬНО БЫСТРОЕ И СТАБИЛЬНОЕ»; вход для Task 8 (порог рельсы = N*), Task 9 (docs от N*), AC4/AC5/AC7 согласуются с N*.

**Выход:** канон параллелизма выбран измерениями и зафиксирован тройно (runner.json + docs + рельса), подтверждён второй зелёной серией; хост чист; инструменты-диагностики покинули ветку целиком (встроенный зонд — Task 13, генератор + dns_probe.py — здесь).

- [ ] **Step 1: [ВЫПОЛНЕНО] Три канонические серии N=3/5/6 на НОВОЙ структуре — сняты: №14 (N=3) 35/41 за 30,90 мин; №15 (N=5) 39/41 за 20,05 мин; №16 (N=6) 39/41 за 20,57 мин; resolve/port = 0 везде; PITR-фейл в КАЖДОЙ серии — правило (б) сработало, обе причины классифицированы по телеметрии, митигация утверждена (Task 12)**

Хронология: №11 (N=3) и №12 (N=5) — ДО-РАЗБИВОЧНЫЕ СПРАВОЧНЫЕ точки (пометка в журнале: «до-разбивочная, структура изменена ревизией 8»; в выборе канона НЕ участвуют) → **[окно Task 14 — разбиение гигантов; НЕ прерывать идущую серию №12 — Task 14 после её завершения]** → ТРИ НОВЫЕ серии на новой структуре: N=3, N=5, N=6 (решение пользователя: «если тесты изменились, то и предыдущие N уже невалидны, так что перепрогонять 3, 5 и 6»), каждая — свой следующий СВОБОДНЫЙ номер журнала (№13 занята строкой «остановлена, недействительна»; по факту журнала — №14/№15/№16). Каждая по «Операционной модели»: гейт тишины + чистоты → контроль runner.json (для 5/6 — переопределение с возвратом ПОСЛЕ прогона) → полная серия (БЕЗ env-зонда — демонтирован Task 13) → телеметрия (время, зелёность, Patroni-лог-подсчёт `getaddrinfo`/`failed to resolve` по docker-логам PG-нод — DNS-контроль канона ревизии 6, пик сетей, чистота, портовая телеметрия `grep -c "port is already allocated" container-*.log` → 0 — AC11(д)) → строка журнала. Между сериями — только гейты (никаких правок кода).

- [ ] **Step 2: Сравнение и развилка канона (точки №14–№16 — НОВОЙ структуры, факт: ни одна не полностью зелёная — во всех PITR-фейл)**

Сравнение времени: №15 (N=5) 20,05 < №16 (N=6) 20,57 < №14 (N=3) 30,90 — лучший кандидат N=5. Правило (б) сработало: во всех трёх сериях один и тот же PITR-фейл (обе причины классифицированы по телеметрии, митигация утверждена — Task 12) — не возврат к Task 7, а РАЗВИЛКА, решённая пользователем (2026-10-07, дословно): «да, сначала таск 12, потом еще раз перемерить». Выбор N* откладывается до перемера с фиксом (Step 4): при полностью зелёной №17 → N*=5 (PITR закрыт + лучшее время); при незелёной №17 — STOP, правило (б) с возвратом к разбору остатка. Запись развилки и решения — в журнал («Гейт решения»/сводка).

- [ ] **Step 3: Тройная фиксация победителя N* (после зелёной №17 — перемера с фиксом Task 12; ожидаемый N*=5 по факту времени №14–№16)**

- `xunit.runner.json`: `"maxParallelThreads": <N*>` — фиксируется БЕЗ возврата (контроль-гейт операционной модели теперь проверяет N*);
- порог рельсы фиксируется значением N* — константу `CanonMaxLiveEnvironments = <N*>` вносит Task 8 при создании `E2eParallelismGuard` (класс на момент Task 10 не существует); здесь — только решение (запись N* в журнал «Гейт решения»/сводку как порог рельсы);
- раздел `docs/e2e-launch.md` — наполняется выбранным N* (Task 9; до его исполнения — N* уже в runner.json и журнале).

- [ ] **Step 4: Серия №17 — ПЕРЕМЕР с фиксом Task 12 и подтверждающая серия (N=5, новая структура)**

После исполнения Task 12 (досрочного, по решению пользователя): полная серия N=5 на новой структуре С фиксом — следующий свободный номер журнала (№17), канонический запуск, гейты тишины/чистоты, полная телеметрия. №17 совмещает две роли: (1) ПЕРЕМЕР — точка «с фиксом» для выбора N* (при полностью зелёной → N*=5: PITR закрыт + лучшее время 20,05 < 20,57 < 30,90; тройная фиксация Step 3); (2) ПОДТВЕРЖДАЮЩАЯ серия — «устойчивость, а не удача» (AC6; единственная полностью зелёная точка новой структуры — зелёность №17 и есть подтверждение канона). Фейл — разбор по телеметрии без перезапуска; повтор один раз после анализа (§9); повторный фейл — STOP, правило (б) Step 2.

- [ ] **Step 5: Судьба инструментов + финальная чистота**

Генератор: `git rm -r src/tools/E2eLoadGen` (ревизия 7: не используется ни в одном прогоне; коммит-хэш удаления — в журнал) вместе с `dns_probe.py` (после Task 13 потребителей нет — Files); E2eDnsProbe.cs/врезка E2eEnvironment уже демонтированы задачей 13 (проверка: гейт Task 13 по-прежнему пуст). Финальный гейт: `docker ps -a | grep pgw-` → 0; `docker network ls | grep -E 'pgw-en-|pgw-net-|kfw-net'` → 0; `docker volume ls | grep pgw-` → 0; `docker network prune -f`; фиксация в журнал.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(t24): приёмка канона — серии N=3/5/6, выбран N* (<N*>) по критерию «максимально быстрое и стабильное»; runner.json+рельса зафиксированы, подтверждение второй зелёной серией; генератор удалён (AC6/AC10)"
```

**Проверка задачи:** три серии в журнале с измерениями; выбор по критериям с измерениями; N* в runner.json; вторая зелёная серия на N*; серии без «port is already allocated» между контурами (AC11(д)); хост чист; генератора нет.
**Связь со spec:** §6 фаза 4, AC6, AC10 (также AC4 — финальное N).

---

### Task 14: Структурная митигация — разбиение классов-гигантов E2E (ревизия 8)

**Статус: НОВАЯ задача (spec ревизия 8; решение пользователя дословно: «Разбивать в t24 сейчас»). МЕСТО: ПОСЛЕ завершения серии приёмки №12 (N=5 — до-разбивочная точка: решение пользователя о разбивании пришло после №12) и ДО трёх новых приёмочных серий N=3/5/6 на разбитой структуре (решение 2026-10-07 — перепрогон всех точек; каждая — следующий СВОБОДНЫЙ номер журнала, №13 уже занята строкой «остановлена, недействительна»); выбор канона и подтверждение — на разбитой структуре (AC12(г)). Окно объявлено в Task 10 Step 1.**

**Вход:** spec §5.9/фаза 3/AC12: факты внутри класса идут последовательно (collection-per-class = один поток), гиганты не дают планировщику использовать параллелизм; целевой порог — суммарное время фактов класса ≤ ~6 мин (по журнальным длительностям серий №10–№12), КРОМЕ классов из одного монолитного факта (§7 — критический путь; монолиты не переписываются: `Acceptance_Scenario_Ac2_To_Ac7`, `Backup_FailsOnBadS3` и др. одиночные остаются как есть). Код-факты (сверено): главный кандидат — `E2eBackupScenarios` (13 фактов, по журналу ~15–20 мин одним потоком) с явными семействами: `Backup_*` (FullDaily, FailsOnBadS3, Deprovision, Rotator, Verify_Ok, Verify_Corruption — 6), `WalStream_*` (UploadsSegments, AgentRecreate, DualArchiving, MasterDown, Promote — 5; Promote_TliHistory — до 6м20с один), `Drill_*` (Succeeds, Failed — 2); кандидаты по порогу (длительности журнала): `E2eRestoreScenarios` (3 факта ≈ 9–10 мин: Latest ~2–3 м + TargetTime ~3 м + NewCluster 4м14с), `E2eSupervisorScenarios` (3 факта ≈ 11,5 мин: ChainBroken 3м20с + OrphanRegistry 3м14с + OrphanDrHold 5м10с); под порогом/маломонолитные — Pgtune (4 факта, smoke-общее ~4 мин), Retention (1 факт 4м35с), EtcdSnapshotExport (3 факта — проверить по журналу), Strict/WorkerCert/Move/Scale/HaEtcd/SecondInstance/Rotate/App* — малые. Окружение и так per-fact — разбиение механически безопасно (канон §13).

**Действие (Files):**
- Modify/Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eBackupScenarios.cs` — разнос 13 фактов: `Backup_FailsOnBadS3_RetriesWithNewId` — отдельный ОДНОФАКТНЫЙ класс (монолит-исключение §7); остальные 5 `Backup_*` — группами ≤ ~6 мин по таблице Step 1 (набор всех 6 одним классом ≈ 12 мин — порог нарушен; например `E2eBackupJobsScenarios` FullDaily+Deprovision+Rotator и `E2eBackupVerifyScenarios` Verify-пара — финальный состав по таблице); `WalStream_*` (5) — `E2eWalStreamScenarios` (базовые) + `E2eWalStreamFailoverScenarios` (MasterDown+Promote — Promote один до 6м20с); `Drill_*` (2) — `E2eDrillScenarios`
- Modify/Create: `E2eRestoreScenarios` → при превышении порога — вынос `Restore_NewCluster_FromSourcePrefix` в отдельный класс (PITR-пара Latest+TargetTime остаётся; финальная нарезка — по таблице Step 1)
- Modify/Create: `E2eSupervisorScenarios` → ТРИ однофактных класса (`E2eSupervisorSelfHealScenarios` ChainBroken, `E2eSupervisorOrphanRegistryScenarios` Registry, `E2eSupervisorOrphanDrHoldScenarios` DrHold): любая пара семейства > 6 мин (Registry+DrHold ≈ 8м24с)
- Хелперы разбитых классов (SeedBucketAsync, StartBackupHostAsync и т.п.): предпочтительно КОПИРОВАНИЕ в каждый новый класс (прецедент репо `E2eRestoreScenarios.cs:385-386` — «копии образцов — файлы сценариев независимы, паттерн репо»); поле `Fx` остаётся ЭКЗЕМПЛЯРНЫМ (`private E2eEnvironment Fx = null!;` — :27), тела фактов не меняются; static-вынос — только с прокидыванием `Fx` в параметры (не по умолчанию); монолиты-факты НЕ переписываются
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (инвентаризация до/после, пометка смены структуры)

**Interfaces:**
- Consumes: журнальные длительности фактов серий №10–№12 (detailed-логи); канон §13 (окружение per-fact).
- Produces (AC12): разбитые классы — обычные collection-per-class единицы (БЕЗ `[Collection]`-атрибутов, в NonE2eCollection НЕ входят — E2E параллельны каноном); инвариант фильтров: `--filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e"` и рельса E2eParallelismGuard (счётчик живых окружений) покрывают всю совокупность фактов без изменений; все серии после разбиения — на новой структуре.

**Выход:** гиганты разбиты; критерий полноты 41 факт сохранён; ТРИ приёмочные серии N=3/5/6 (перепрогон по решению 2026-10-07), выбор канона N*, подтверждение и рельса — на разбитой структуре (AC12(г)).

- [ ] **Step 1: Инвентаризация ДО (в журнал)**

Парный подсчёт (BSD-совместимо; ловит только атрибутные факты — хелперы `public async Task StopAsync` и комментарии «Один [Fact]» не сматчиваются):

```bash
grep -rn -A1 '\[Fact\]' --include='E2e*.cs' src/tests/PgWorker.IntegrationTests/E2e/ | grep -c 'public async Task'
```

Expected: `41`. Детальный список по классам (тот же конвейер без `-c`) + суммарные длительности классов по detailed-логам серий №10–№12 — таблица в журнал (база полноты и основа нарезки порога).

- [ ] **Step 2: Разбиение по семействам (порог ≤ ~6 мин; монолиты не трогать)**

Новые классы по правилам §5.9: без `[Collection]`/`DisableParallelization`; состав — по Files (FailsOnBadS3 — однофактный монолит-класс; Backup-пятерка/WalStream/Drill/Restore-вынос/Supervisor-тройка — группами ≤ ~6 мин по таблице Step 1); хелперы — копированием (прецедент :385-386), `Fx` — экземплярное поле, тела фактов переносятся как есть (без переписывания логики сценариев).

- [ ] **Step 3: Гейты**

Сборка `dotnet build src/PgWorker.slnx -c Release` → 0/0. Полнота — парный греп Step 1 по НОВОЙ структуре (конвейер грепает все `E2e*.cs`, включая новые файлы классов): → `41`; список до/после в журнал (AC12(а)). Структура: `grep -rn '\[Collection\|DisableParallelization' <все новые файлы классов Step 2>` → пусто (AC12(в)).

- [ ] **Step 4: Smoke-прогон разбитого семейства — параллельность**

По «Операционной модели» (гейт тишины): 3–4 новых класса одного бывшего гиганта (например Backup Jobs + WalStream + Drill) фильтром `FullyQualifiedName~E2eBackupJobs|E2eWalStream|E2eDrill` — зелёные; во время прогона пик живых `pgw-en-`-сетей ≥ 2 (классы параллелятся — раньше семейство шло одним потоком). Итог в журнал.

- [ ] **Step 5: Пометка смены структуры в журнал**

Строка к ревизии 8: состав разбиения (таблица до/после), Smoke-итог; ОГОВОРКА: сравнение времени пост-разбивочных серий с до-разбивочными точками (№10–№12) — честное, с пометкой смены структуры (AC12(г); пометка ревизии 8 уже в журнале).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(t24): разбиение классов-гигантов E2E (E2eBackupScenarios 13 фактов → FailsOnBadS3-монолит/Jobs/Verify/WalStream/WalStreamFailover/Drill; Restore-вынос; Supervisor → три однофактных) — 41 факт сохранён, новые классы collection-per-class (ревизия 8, AC12)"
```

**Проверка задачи:** 41 факт до/после (журнал); новые классы без `[Collection]`; smoke параллелен (пик ≥ 2); сборка 0/0; пометка смены структуры в журнале.
**Связь со spec:** §5.9, §6 фаза 3 «Структурная митигация (обязательная)», AC12; решение пользователя дословно «Разбивать в t24 сейчас»; §7 (монолитные одиночные факты не переписываются).

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

Правила окружения docker-хоста (внешнее — не предмет тестов):
- параллельность серии — канонический потолок <N*> контура; чужие E2E-серии
  и поднятый dev-стенд на том же хосте — учитывать перед запуском серии;
- серию не запускать поверх другого тяжёлого docker-прогона — дождаться
  завершения; прогон — только после гейта тишины;
- рельса: `E2eEnvironment` при фактическом превышении канона пишет громкое
  `[E2E-PARALLELISM]` (диагностические прогоны с переопределённым потолком
  легальны, но громки; переопределение — временная правка `maxParallelThreads`
  в runner.json с ОБЯЗАТЕЛЬНЫМ возвратом и контролем канонического значения).

Признаки деградации (что смотреть в логах):
- серийные `getaddrinfo … empty` / `failed to resolve host` в docker-логах
  PG-нод (DNS-контроль канона — ревизия 6: подсчёт по этим логам;
  Patroni-клиент etcd: сверять advertise-лист и сеть контейнера — алиас
  чужой сети в advertise недопустим, arch/04 §8 п.3);
- мгновенные (~0,3 с) фейлы коннекта в transient-циклах воркера (`host-*.log`);
- рост `[PHASE] … elapsed` у фаз, которые в тишине проходят быстро.

Действия при фейле параллельной серии:
- разбор по телеметрии teardown БЕЗ перезапуска (§4): этап смерти соединения
  (резолв / TCP / хендшейк) по Patroni-логам/docker-логам PG-нод + host.log;
- отложить прогон до тишины хоста (чужие контуры) / пересмотреть N и
  повторять ТОЛЬКО после анализа и с гипотезой.
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
| `t24-e2e-suite-under-load` | — (мерж-коммит t24-e2e-suite-under-load) | устойчивость ПАРАЛЛЕЛЬНОГО запуска E2E-серии (характеристика N; предмет ревизии 7 — «параллельный запуск тестов, а не под нагрузкой запуск тестов»): сетевая изоляция контуров per-unit — per-cluster сети нод `pgw-net-<C>` движка (arch/14 §2.1: ensure-инвариант «нода и агенты в одной сети кластера», демонтаж чистит сеть; единая `pgw-net` с общей DNS-зоной устранена) + advertise-канон etcd (arch/04 §8 п.3: только резолвимые URL — устранён NXDOMAIN-шторм Patroni по внутрисетевому алиасу, причина стабильных фейлов Restore/WalStream при N=3, разбор 4f7b146) + портовые митигации: per-unit окна 200 портов с физическим резервированием bind'ом (первым портом публикуется etcd контура, занятость → сдвиг окна) и упразднение схемы смещений +3000/+1500 (последовательные слоты, arch/14 §2.4 п.2) — межконтурные портовые коллизии исключены конструкцией; конфигурация параллелизма — xunit.runner.json единственный источник (мёртвые csproj-свойства и CollectionBehavior-атрибут удалены), не-E2E гарантированно последовательны NonE2eCollection; канон параллелизма выбран приёмкой из {3,5,6} по критерию «максимально быстрое и стабильное»: N*=‹N*› (‹время/зелёность серий›), зафиксирован в runner.json/docs/рельсе E2eParallelismGuard, подтверждён второй зелёной серией; ‹прочие митигации параллельного режима по гейту: реализованы ‹состав: PITR-пауза/HaEtcd/build› / не реализованы — остаточное ограничение Docker Desktop/хоста›; инструменты исследования (шумовой генератор, встроенный DNS-зонд Task 2 — демонтирован ревизией 6) не поставляются; DNS-контроль канона — подсчёт getaddrinfo/failed to resolve по Patroni/docker-логам PG-нод |
```

- [ ] **Step 3: Проверка синхронности** — `grep -c "t24-e2e-suite-under-load" arch/roadmap/reliability-report.md` → `1` (только «Сделано»); `grep -rc "t24" arch/roadmap/` → `0` вне этой строки.

- [ ] **Step 4: Commit (уходит мерж-коммитом ветки)** — `roadmap(t24): пункт снят, отчёт — строка в «Сделано» (мерж-гейт трека, AC8)`.

**Проверка задачи:** t24 отсутствует в «Осталось» обоих документов; одна строка в «Сделано».
**Связь со spec:** §6 фаза 4, §8 AC8; правило мерж-гейта roadmap (AGENTS.md).

---

### Task 12: PITR-митигация — страховочная пауза 5 с в Restore_TargetTime (решение пользователя)

**Статус: НОВАЯ задача; исполняется ДОСРОЧНО — ДО завершения приёмки Task 10, решением пользователя (2026-10-07, развилка канона: «да, сначала таск 12, потом еще раз перемерить»; прежнее размещение «в конец плана» ОТМЕНЕНО этим же решением). После Task 12 — перемер №17 (Task 10 Step 4).**

**Вход:** решение пользователя дословно: «для PITR-гонки решение 5 сек вместо 2х, добавь в конец плана кодера сделать так и прогнать». Предмет — устойчивый H3-транзиент ПАРАЛЛЕЛЬНОГО режима (ревизия 7): `Restore_TargetTime_PitrRollback` падал в №3/№3п и повторился на целевой конфигурации (№9/№10 при N=3/5) — паттерн «восстановлена T0-часть, WAL-хвост не применён»; секундная гранулярность `recovery_target_time` + расхождение часов хост/нода съедают страховочную паузу между фиксацией `tCut` и порчей. Код-факт (сверено): `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs:213` — `await Task.Delay(2000, ct);` между `var tCut = DateTime.UtcNow;` (:212) и порчей `DROP TABLE pitr_probe` (:215); метод — `Restore_TargetTime_PitrRollback`, класс `E2eRestoreScenarios` (фильтр `FullyQualifiedName~Restore_TargetTime` однозначен).

**Действие (Files):**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs:213` (пауза 2000 → 5000 мс + краткий комментарий по канону)
- Modify: `docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md` (итог серии повторов; связь с H3-строкой карты)

**Interfaces:**
- Consumes: целевая конфигурация (Tasks 1d/1e; канон N* из Task 10); «Операционная модель» (гейт тишины для точечных прогонов).
- Produces: закрытая H3-строка карты гипотез (Task 6) для `Restore_TargetTime` — митигация 5 с подтверждена повторами; либо зафиксированный остаточный фейл (без молчаливого расширения).

**Выход:** PITR-сценарий устойчив к гранулярности `recovery_target_time`/расхождению часов (страховка 5 с); серии приёмки Task 10 больше не встречают этот транзиент.

- [ ] **Step 1: Правка паузы с комментарием**

`E2eRestoreScenarios.cs:213`:

```csharp
            var tCut = DateTime.UtcNow;
            // Страховка от гранулярности recovery_target_time (секунда) и
            // расхождения часов хост/нода: tCut обязан строго предшествовать
            // DROP-сегменту — 5 с (решение пользователя по H3-транзиенту №3/№3п;
            // 2 с оказался недостаточным при параллельном N=3/5).
            await Task.Delay(5000, ct);
```

- [ ] **Step 2: Сборка**

Run: `dotnet build src/PgWorker.slnx -c Release`
Expected: 0 errors, 0 warnings.

- [ ] **Step 3: Точечная серия повторов Restore_TargetTime (гейт тишины; 3 повтора)**

По «Операционной модели» (гейт тишины + чистоты вокруг каждого; гипотеза ДО в журнал: «пауза 5 с закрывает PITR-гонку — 3/3 зелёные»):

```bash
for i in 1 2 3; do
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 PGW_TEST_E2E_DNS_PROBE=1 \
    dotnet test src/PgWorker.slnx -c Release \
    --filter "FullyQualifiedName~Restore_TargetTime" \
    --logger "console;verbosity=detailed" 2>&1 | tee /tmp/pgw-t24-pitr5s-$i.log
done
```

Expected: 3/3 зелёные (`Test Run Successful` в каждом логе); хост чист после каждого. Итог — в `journal.md` (строка к H3-карте: «транзиент Restore_TargetTime закрыт паузой 5 с — решение пользователя, 3/3 зелёные»).

- [ ] **Step 4: Если фейл сохраняется после 5 с**

Запись в журнал с разбором по телеметрии (без перезапуска); паузу НЕ расширять молча — STOP и вопрос контролёру флоу (`NEEDS_CONTEXT`): остаточная причина иная (не гранулярность/часы) и требует собственного решения.

- [ ] **Step 5: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs docs/superpowers/2026-10-06-t24-e2e-suite-under-load/journal.md
git commit -m "test(t24): PITR-страховка Restore_TargetTime 2с → 5с (решение пользователя по H3-транзиенту №3/№3п) + подтверждение 3 повторами"
```

**Проверка задачи:** `grep -n "Task.Delay(5000" src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs` → 1 совпадение (:213); `Task.Delay(2000` в Restore_TargetTime отсутствует; 3/3 зелёные повтора в журнале (или зафиксированный остаточный фейл + вопрос контролёру).
**Связь со spec:** вне исходного scope; прямое решение пользователя по митигации H3-транзиента параллельного режима (дословно: «для PITR-гонки решение 5 сек вместо 2х…»); закрывает H3-строку карты гипотез (Task 6) для Restore_TargetTime; принцип §3.3 соблюдён — увеличение обосновано телеметрией №3/№3п/№9/№10 (прогресс до обрыва, съеденный бюджет), не «авось проедет».

---

## Самопроверка плана (итог self-review)

- **Покрытие spec (ревизии 1–8):** §5.9 разбиение гигантов → Task 14 (окно после до-разбивочных серий №11–№12, до серии N=6 на новой структуре; 41 факт сохранён, новые классы collection-per-class; AC12); §5.1 генератор → Task 3 (выполнен; выведен из использования ревизией 7 — демонтаж Task 10); §5.2 зонд → Task 2 (выполнен как артефакт; ИСКЛЮЧЁН ревизией 6 — демонтаж Task 13 до приёмки Task 10; dns_probe.py удаляется вместе с генератором); §5.3 матрица → Tasks 4 (до-митигационные улики; №6-до-фикса — улика 77d8702) + 5 (ЗАКРЫТА: прогон №10/№6-пост — 894fbb7, 38/41, симптом Б опровергнут; DNS-кривая исключена ревизией 5, H1 закрыт уликами, H4 исключена ревизией 7); §5.4 конфигурация → Task 1 (выполнена; финальное N — Task 10); §5.5 docs → Task 9; §5.6 рельса → Task 8 (порог N*); §5.7 сетевые митигации → Task 1d (выполнен, c02ed79); §5.8 портовые митигации → Task 1e (окна 200/bind-резерв через etcd/последовательное выделение; упразднение +3000/+1500); прочие — Task 7; фаза 0 → Tasks 0/1/4 (исполнены; центральный результат — причина 4f7b146); фаза 1 → Task 5 (закрыта); фаза 2 → Task 6 (H1–H3, H4 исключена ревизией 7); гейт (предмет — параллельный запуск: PITR/HaEtcd/build) → Task 7; фаза 3: сетевые → 1d, портовые → 1e, прочие+H3 → Task 7; фаза 4 приёмка — №14–№16 (N=3/5/6, новая структура; PITR-фейл в каждой — обе причины классифицированы) → досрочный Task 12 (развилка канона) → №17-перемер с фиксом = подтверждающая серия; зелёная №17 → N*=5 + тройная фиксация → Task 10 (без зонда — после Task 13); мерж-гейт → Task 11. AC: AC1→Task 5, AC2→Task 6, AC3→Task 7, AC4→Task 1+10 (N*), AC5→Task 9, AC6→Task 10, AC7→Task 8, AC8→Task 11, AC9→Task 1d, AC10→Task 10 (три точки — новой структуры, AC12(г)), AC11→Task 1e (а–г) + Task 10 (д), AC12→Task 14. Ограничения §7 и риски §9 — в Global Constraints/задачах.
- **Статусы исполнения:** Tasks 0, 1 (2fe1ac4), 1b (341a577), 1c (baba3c0), 2 (1a920bc), 3 (0bccc5a; выведен из использования ревизией 7), 4 (частично), 1d (c02ed79: №8 40/41, resolve 0), 1e (контрольный №9), 5 (фактически закрыта: №10/№6-пост — 894fbb7, 38/41 за 34,7 мин, resolve 0, port 0; 3 фейла H2/H3-класса в гейт) — выполнены; прогон №7 остановлен по приказу удаления noise — история, НЕ переигрывается. Порядок текущего остатка: **12[досрочная] → 10[№17] → 8 → 9 → 11**; хронология до неё: 6 → 7 → 13 → 10[№11–№12 — справочные] → 14 → 10[№14–№16 сняты: 35/41, 39/41, 39/41; PITR-фейл в каждой] → 12[досрочное исполнение — решение «сначала таск 12, потом еще раз перемерить»] → 10[№17 — перемер с фиксом и подтверждающая серия; зелёная №17 → N*=5, тройная фиксация] → 8 → 9 → 11** (ревизии 7–8 + решения 2026-10-07: перепрогон точек после Task 14 и досрочный Task 12 по развилке канона; прежнее «Task 12 — последняя» отменено решением о развилке).
- **Вне scope spec (прямые требования пользователя):** Task 1b (postgres:18-alpine) и Task 1c (AgentImage) — выполнены, статус-блоки с итогами; Task 12 — PITR-пауза 5 с в Restore_TargetTime (митигация устойчивого H3-транзиента параллельного режима №3/№9/№10; подтверждение 3 повторами; закрывает H3-строку карты Task 6).
- **Типы/имена:** `PlainClusterDriver.NodesNetworkPrefix`/`NetworkName(cluster)` (Task 1d) — по kfw-эталону; константа `NodesNetwork` удаляется; исполнимость ensure-инварианта обеспечена расширением модели: `DockerContainerInspect.Networks` (парсинг `NetworkSettings.Networks` по прецеденту aliases) + `NetworkConnectAsync` как член `IDockerEngine` (прод-реализация + стабы во ВСЕХ 9 реализациях — гейт `grep ': IDockerEngine' src/` → 9 покрытых мест, без CS0535) + FakeEngine (сети в inspect, connect в Calls); гейт `grep '"pgw-net"' src/` пуст благодаря и удалению константы, и замене тестового литерала в DockerEngineTests.cs на `"test-net"`; упоминания `pgw-net` в комментариях (WalStreamProcess:344, ShardEndpoints:189, ClusterDriver:81, WalStreamProcessTests:111, ClusterDriverTests:817, E2eEnvironment:263–266/654/766) актуализируются теми же шагами; `NonE2eCollection.Name` — без изменений; `E2eParallelismGuard.CanonMaxLiveEnvironments = N*` вносится Task 8 при создании рельсы (Task 10 фиксирует N* в runner.json и журнале); advertise: peer-urls не трогаются (потребители в сети окружения), client-urls — только `host.docker.internal:{порт}`; OwnName-матчинг `pgw-net-<C>` по ClusterTag — основа own-only чистки и расширенного ассерта; OwnedEtcdFixture — `_runId` + свойства-выражения с прежними именами `_netName`/`_containerName` (конструктор не меняется; гейт: один `Guid.NewGuid` в файле), инвентаризация `new NetworkBuilder` в src/tests — 3 места, все per-runId (Task 1d Step 5).
- **Операционные механики:** гейт тишины перед каждым прогоном (все прогоны — без соседа, ревизия 7; три замера ×30 с); переопределение потолка sed'ом runner.json с возвратом (приёмка N=3/5/6 — возврат до фиксации выбора, Task 10 Step 3 фиксирует без возврата); двухчастная проверка рельсы (пик по стартовым строкам → предупреждение); номерация прогонов — продолжение журнала (№7 — остановлен-история; №8/№9 — контрольные 1d/1e; №10 = №6-пост — 894fbb7).
- **Портовые имена (Task 1e):** `PortWindowBase = 15100` / `PortWindowSize = 200` / `PortWindowBudget = 40` (E2eEnvironment); литеральный PortRange в E2E — в ДВУХ местах (E2eEnvironment.cs:475-476; E2eSecondInstanceScenarios.cs:146-147 — воркер-контейнеры w1/w2), оба закрываются окном; экспонирование окна сценариям — `ReserveWindowPort()` (порты w1/w2 вместо `FreePort()`) и `RemainingPortRange` (PortRange воркер-контейнеров); гейт литералов — по всему каталогу E2e: `grep -rn '"15100"\|"15200"' src/tests/PgWorker.IntegrationTests/E2e/` → 0 (паттерн с кавычками — только строковые литералы конфигурации, int-`PortWindowBase` не покрывает и допустим); `PgPlanning.MakeAddress` — последовательная тройка `NodePorts(base, base+1, base+2)` (единственное место смещений в прод-коде; потребители `PortAllocator.Allocate` не меняются — смещение инкапсулировано в MakeAddress); юниты смещений: PortAllocatorTests:14/:105, PortPlanConvergenceTests:15/:22/:188; kfw/Valkey MakeAddress — без смещений, не трогаются; актуализация канона docs/e2e-isolation.md §4 (ретрай старта: чужой prune / портовый bind-фейл, бюджет оконный) и комментария appsettings.Development.json:28 (маппинги 165xx/18xxx устаревают).
