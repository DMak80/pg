# План: cutover — квотирование SQL для имён из каталога БД (+ часть Б: фикс 4 вне-диффовых docker-E2E-фейлов + kill-дельта spilo)

**Цель (часть А):** имена таблиц/sequences, пришедшие из каталога источника (`pg_class.relname`, любые валидные для PostgreSQL), подставляются в SQL cutover квотированием (`"имя"`, удвоение внутренних `"`; в SQL-литерале setval — ещё и удвоение `'`), а не через `Ident`-валидатор — переезд бакета с таблицей `"Orders"` и подобными именами проходит cutover без `ArgumentException` и без вечной заморозки.

**Цель (часть Б):** закрыть 4 вне-диффовых фейла полного E2E-прогона (диагнозы — `investigation-e2e-4-failures.md`, обязателен к прочтению): (1–2) `DockerEngine.StopContainerAsync` несовместим с пер-попыточным таймаутом 7.5 с + журнал эвакуатора после стопов; (3) env `OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS` не прокидывается в E2E-контур; (4) негвардированный доступ к portalloc в Kafka K3 при takeover.

**Цель (kill-дельта, фаза 4a — приказ пользователя, §6 п.21):** остановка spilo-нод (PG/Patroni) переводится на `docker kill` (SIGKILL) — семантика внезапной потери машины (жёсткая смерть ноды — штатный канал, arch/14 §5 C): exited-факт немедленный, корень гонки `CallTimeout=7.5 с` на spilo-путях устранён вовсе; поллинг-стоп сохраняется для всех прочих контейнеров (§5.9).

**Архитектура (часть А):** каталоговые имена → `Quote`; заявочные → `Ident`. Сверка P8 — построчный `TableRelnames`; freeze — агрегат. `AbortSequence` чинится общими билдерами.

**Архитектура (часть Б + kill):** arch-правки первыми (§8.8; kill-дополнения — фаза 4a, до kill-кода); поллинг `StopContainerAsync` — один дефект, одно место, все потребители; эвакуатор — журнал P7 до стопов; S3 — опция с env-гайкой; Kafka K3 — transient-гвард; kill — только spilo-пути PgWorker: новый `KillContainerAsync` в `Shared.Docker` (интерфейс + движок), проводки `PlainClusterDriver` (EnsureNode-recreate/RemoveNode/StopNode) и `NodeSupervisor` (rest-tls); NodeSupervisor-ы Kafka/S3/Valkey и их драйверы остаются на поллинг-стопе.

**Спецификация:** `docs/superpowers/2026-10-21-fix-cutover-argumentexception-sql-quoting/spec.md` + `investigation-e2e-4-failures.md` — план аргументирует от spec; исполнитель читает ОБА целиком.

**Режим исполнения:** субагент coder (сильная модель), ОДИН, без своих субагентов. Подпункты — самодостаточные инструкции; прогоны/репро/отладка/arch-тексты — сильная модель.

**Порядок исполнения:** 1–6 (ВЫПОЛНЕНЫ) → 9–14 (ВЫПОЛНЕНЫ) → **9a → 10a (kill-дельта)** → 15 → 16 → 17 → 7 → 8.

---

## Глобальные ограничения (из spec §5 и AGENTS)

- **Часть А:** `arch/` не трогается. **Часть Б + kill:** arch-правки — ПЕРВЫМИ до кода, исчерпывающий список в §8.8; никаких иных касаний `arch/`.
- .NET 10, `Nullable=enable`, **`TreatWarningsAsErrors=true`**; мёртвый `ParseTables` удалён (не закомментирован).
- Идентификаторы — английские; комментарии/docstring — русские; тесты — AAA.
- Никаких литералов хост-портов; порты/окружение E2E не меняются.
- Поведенческие инварианты cutover заморожены (§5.4). Freeze-путь, `AbortSequence.cs`, `MoveProcess.cs` — не правятся.
- Enterprise-гварды не добавляются (§5.5).
- **Docker kill — ТОЛЬКО spilo-ноды (PG/Patroni) PgWorker** (§5.9): все прочие контейнеры (брокеры Kafka, OwnS3-инстансы, джобы, воркер-контейнеры, wal/backup-агенты) останавливаются поллинг-стопом `StopContainerAsync`; kill для них не вводится.
- **Прод-дефолт OwnS3 (reconcile 3 ч) не меняется** (§5.8).
- **Docker-репро (фаза 6)** — только execute-фаза, строго последовательно, по одному контуру; перед КАЖДЫМ прогоном пред-чек §7.7 (чистота хоста от чужих `pgw-*`/`kfw-*`/`s3w-*` контуров) **и §7.7.5** (окно репро свободно от чужих docker-сессий: чужие контуры/сессии → СТОП и доклад; снесённый репро повторяется только после убеждения в отсутствии чужой активности, факт помехи и повтор — записью в лог задачи); вывод ПОЛНОСТЬЮ в файлы; зачистка после каждого; перезапуск без диагноза запрещён (§5.6–5.7).
- Все dotnet-команды — `DOTNET_CLI_UI_LANGUAGE=en`; сборка и тесты — ДВУМЯ ОТДЕЛЬНЫМИ командами; `--no-build` у тестов. Запуск/мониторинг — по AGENTS.base.md §4/§12/§13/§15. Рабочая директория — корень worktree.
- Коммиты — свободно в feature-ветке после зелёной проверки; conventional commits с русским описанием.
- Сценарии E2E по существу не меняются; бюджеты не увеличиваются (единственные санкционированные тест-правки: сид §3.3, S3-самодиагностика §8.5.3, фаза `ac6-returned-stopped` §6 п.22).

## Review Focus (входные классы, не очевидные из задач)

Часть А:
1. Имя с `"` внутри → удвоение — задачи 1/4 (выполнено).
2. Имя с `'`: идентификатор как есть / setval-литерал `''` — задача 1 (выполнено).
3. Имя с `, `/пробелами — построчный листинг — задачи 1/4 (выполнено).
4. Инъекция через имя → один закавыченный идентификатор — задача 1 (выполнено).
5. Пустая схема → `foreach` пропускается (§6.9, конструкция цикла; тест не заказан §2.3).

Часть Б:
6. Контейнер умирает дольше клиентского таймаута → поллинг до exited; 304/404; бюджет → Failed — задача 10 (выполнено, 750ae4af).
7. Возврат шарда: журнал до стопов, сбой стопа не теряет факт — задача 11 (выполнено, 8b94a1fe).
8. S3: опция не задана → переменной нет — задача 12 (выполнено, 210469d5).
9. Kafka: частичный portalloc → transient — задача 13 (выполнено, f1f27a91).
10. Чужая docker-сессия на хосте — не корень фейлов, но сносит репро-прогоны: пред-чек §7.7+§7.7.5 перед каждым репро (до реализации t90) — задачи 15–17.

Kill-дельта:
11. Kill выборочен: spilo-нода → kill, WAL/backup-агент → поллинг-стоп (§5.9) — юнит-проверки выбора в задаче 10a (обновлённые `RemoveNode`/`StopNode`/`PortDrift`/оба `RestTlsStep`-теста + не меняющийся агентный `EnsureBackupAgent`-тест).
12. Kill идемпотентен: 404 = успех (контейнера нет) — задача 10a.
13. Контракт `Task<Result>` kill-метода: любой сбой — `Result.Failed`, не исключение (образец Stop) — задача 10a.
14. Свободный от гонки порядок теста AC6: exited-ассерты только ПОСЛЕ фактической остановки (`ac6-returned-stopped`, §6 п.22) — фаза уже в `E2eScenarios.cs` (незакоммичена; коммитится задачей 10a).

---

### Задача 1: красные юнит-тесты SQL-билдеров (TDD) — ВЫПОЛНЕНО (bcb934c7)

**Статус:** выполнено. В `MoveSqlPart2Tests`/`MoveSqlTests` добавлены `RowCount_QuotesCatalogNames_WithoutValidation`, `SequenceIssuedNext_QuoteCatalogNames`, `SetvalForward_EscapesQuotesInsideLiteral`, `TableRelnames_ListsSorted`; TDD-red подтверждён (`ArgumentException`/ошибка компиляции). Тексты подпунктов — в git-истории плана. Связь: §7.1, §4 Фаза 1.

### Задача 2: реализация квотирования в `MoveSql` — ВЫПОЛНЕНО (bcb934c7)

**Статус:** выполнено. `Quote`, `Qualified` = Ident+Quote, `SetvalForward` экранирует `'`, `TableRelnames`, docstring; тесты задачи 1 зелёные, существующие expected не изменены. Связь: §3.1, §7.1.

### Задача 3: `CutoverSequence` построчная сверка + удаление `ParseTables` + фейки — ВЫПОЛНЕНО (17ab12a2)

**Статус:** выполнено. Шаг 6 на `ListAsync`/`TableRelnames`; `ParseTables` удалён; `CutoverSql`/`CutoverLayer` параметризованы; все существующие тесты Moves зелёные без ослабления. Связь: §3.2, §3.5, §7.2.3.

### Задача 4: cutover-тесты (экзотические имена, повторный заход) — ВЫПОЛНЕНО (17ab12a2)

**Статус:** выполнено. `ExoticCatalogNames_CutoverFlips`, `ExoticNames_SetvalFailThenRetry_Flips` зелёные. Связь: §7.2.1, §7.3.

### Задача 5: E2E-сид `"Orders"`/`"Orders_seq"` — ВЫПОЛНЕНО (32584b8f)

**Статус:** выполнено. `SeedBucketAsync` создаёт `"Orders"` (+3 строки) и `"Orders_seq"`. Связь: §3.3.

### Задача 6: полная сборка и юниты PgWorker — ВЫПОЛНЕНО (в составе bcb934c7/17ab12a2/32584b8f)

**Статус:** выполнено. slnx Release 0 warnings; `PgWorker.UnitTests` зелёный. Связь: §7.4.

---

### Задача 9: arch-правки части Б — ПЕРВЫМИ (§8.8, фаза 4) — ВЫПОЛНЕНО (11f87b75)

**Статус:** выполнено. arch/14 §6 (поллинг-стоп), arch/24 §5 A + §8 (env + опция), arch/16 §5 A K3 (transient-ветка), roadmap `t90-e2e-session-isolation` — закоммичены 11f87b75 до кода. Связь: §8.8 (строки 1/3/4/5/7 таблицы — Исполнено), §4 Фаза 4.

### Задача 10: фикс 1а — `DockerEngine.StopContainerAsync` поллинг — ВЫПОЛНЕНО (750ae4af)

**Статус:** выполнено. Поллинг до exited-факта (бюджет `timeoutSec + httpClient.Timeout`, кэп 30 с), `IsContainerStoppedAsync`, контракт `Result.Failed` на любой сбой (catch-all — семантика `Result.FromAsync`), лог-строки elapsed; `DockerEngineStopTests` (5 кейсов, cancellation-aware фейк) в том же коммите; крест-прогон юнитов воркеров зелёный. Связь: §8.3.1, §7.13.1, §8.3.

### Задача 11: фикс 1б — эвакуатор: журнал P7 до стопов — ВЫПОЛНЕНО (8b94a1fe)

**Статус:** выполнено. `QUARANTINED`+`returned_unix` до docker-стопов; сбой стопа → Failed с живым фактом, retry тиком; `FakeDriver.StopResultByNode`; тест `Tick_ShardReturned_JournalBeforeStops_StopFailureRetried` зелёный. Связь: §8.3.2, §7.13.2.

### Задача 12: фикс 3 — S3Worker env reconcile + E2E + самодиагностика — ВЫПОЛНЕНО (210469d5)

**Статус:** выполнено. Опция `S3Worker:OwnS3{ReplicationReconcileIntervalSec}` (0 → env не ставится), `InstanceEnvComposer` + 4 вызывающих, E2E-env 10 с, `AdminReconcileAsync`-дубль при 404, юнит `Compose_ReconcileInterval_OptionalEnv`. Связь: §8.5, §7.13.3.

### Задача 13: фикс 4 — Kafka K3 гвард + аудит — ВЫПОЛНЕНО (f1f27a91)

**Статус:** выполнено. `TryGetValue` → маркер → `waiting-portalloc/<broker>` (FinishAsync); `PortAllocHealer` гвард `merged` (строка 184); `AddBrokerProcess:259` — аудит без правки; тест `Run_PartialPortAlloc_WaitsForBrokerAddress_InProgress`. Связь: §8.6, §7.13.4.

### Задача 14: репро фейла 1 — `Acceptance_Scenario_Ac2_To_Ac7` — ЗАКРЫТА, НЕ ПЕРЕЗАПУСКАЕТСЯ

**Статус:** ЗАКРЫТА (§6 п.23, §7.8): соло-прогон на stop-семантике зелёный — ac6-quarantine `ok=True` за 4.5 с (было: таймаут 120 с), фаза `ac6-returned-stopped` `ok=True` за 19.8 с; журнал достигает `QUARANTINED` с `returned_unix`, контейнеры шарда exited. Контур зачищен. Повтор при kill-дельте НЕ обязателен (§6 п.23); kill-проводка верифицируется полным E2E задачи 7 (AC6 входит в `E2eScenarios`).

---

### Задача 9a: kill-дельта — arch-правки ПЕРВЫМИ до kill-кода (§8.8 kill-строки, §4 фаза 4a) — сильная модель

**Вход:** задачи 1–14 завершены; kill-код не тронут.

**Действие (coder):** ровно две правки канона (§8.8, строки «Kill-дельта» — «В очереди (фаза 4a, ПЕРВОЙ до kill-кода)»):

1. `arch/14-pgworker.md` §6, абзац «**Инвариант поллинга долгих фаз**» — сразу ПОСЛЕ предложения, вставленного задачей 9 и заканчивающегося «бюджет исчерпан без exited-факта — отказ.» (непосредственно ПЕРЕД продолжением «Поллинг не итерируется (висящий вызов)…»), вставить:
   «Для spilo-нод (PG/Patroni) остановка — `docker kill` (SIGKILL: грейса нет, exited-факт немедленный — поллинг не нужен; жёсткая смерть ноды — штатный канал, §5 C); поллинг-стоп — для всех прочих контейнеров (брокеры, инстансы, джобы, агенты).»
2. `arch/14-pgworker.md` §5 D, формулировка E3 (строка ~1100): фразу «но при возврате REST-доступности — остановить (docker stop),» заменить на «но при возврате REST-доступности — остановить (docker kill — spilo-нода: жёсткая смерть — штатный канал, §5 C; exited-факт немедленный),». Остальное E3 (P1-логика, «призраки») — без изменений.

**Выход:** канон фиксирует kill-семантику spilo (§6) и kill-формулировку E3 (§5 D).

**Проверка:** `git -C <worktree> status --short` — изменён ТОЛЬКО `arch/14-pgworker.md` (2 правки). Коммит:

```bash
git add arch/14-pgworker.md
git commit -m "docs(arch): kill-дельта — spilo-ноды: docker kill (exited-факт немедленный, поллинг не нужен; жёсткая смерть — штатный канал §5 C), прочие контейнеры — поллинг-стоп (§6); E3 возврата — остановка kill (§5 D)"
```

**Связь со spec:** §8.8 (kill-строки таблицы), §6 п.21, §4 Фаза 4a. Подпунктов нет (тексты канона — сильная модель).

---

### Задача 10a: kill-дельта — `KillContainerAsync` + проводки spilo-путей + юниты (§8.3a, §7.13.5)

**Вход:** задача 9a закоммичена (arch-first).

**Действие (coder):** выполнить подпункты 10a.1–10a.7 АТОМАРНО до первого прогона (правка интерфейса ломает компиляцию всех стабов — до 10a.4/10a.5 сборка невозможна), затем прогоны и коммит. Границы kill (§5.9): ТОЛЬКО spilo-контейнеры нод шардов PgWorker — все 4 spilo-вызова `StopContainerAsync` в PgWorker-прод-коде (сверено grep'ом; других нет): `PlainClusterDriver.EnsureNodeAsync` port-mismatch-ветка (ClusterDriver.cs:308 — пересоздание контейнера ноды), `RemoveNodeAsync` (ClusterDriver.cs:355 — демонтаж ноды; restore-заявка, §8.4.1), `StopNodeAsync` (ClusterDriver.cs:412 — карантин E3/возврат), `NodeSupervisor` rest-tls-ветка (NodeSupervisor.cs:645 — пересоздание). НЕ меняются: WAL/backup-агентные вызовы (ClusterDriver.cs:468/508 — не spilo), `SwarmClusterDriver.StopNodeAsync` (RemoveServiceAsync — сервисная семантика, без StopContainerAsync), NodeSupervisor-ы/драйверы Kafka/S3/Valkey.

**Выход:** `KillContainerAsync(idOrName, ct)` в `IDockerEngine` + `DockerEngine` (POST `/containers/{id}/kill`; 404 = успех; контракт `Task<Result>`); 4 spilo-проводки на kill; 11 тестовых стабов `IDockerEngine` дополнены; `DockerEngineKillTests` (3 кейса); 5 существующих тестов обновлены под kill-вызов (включая оба rest-tls-теста пересоздания); незакоммиченная правка `E2eScenarios.cs` (`ac6-returned-stopped`, §6 п.22) включена в коммит. Факт для коммита: `DockerEngineStopTests.cs` (cancellation-aware фейк) УЖЕ в 750ae4af — по `git status` незакоммичен только `E2eScenarios.cs`.

**Проверка:**
1) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests -c Release` — зелёные (новые kill-кейсы + существующие stop-кейсы).
2) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Warning(s) (сборка покрывает и Integration-стабы движка).
3) `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build` — зелёные (обновлённые ClusterDriverTests/NodeSupervisorTests).
4) Крест-прогон §8.3 (правка интерфейса): `KafkaWorker.UnitTests`, `S3Worker.UnitTests`, `ValkeyWorker.UnitTests` — отдельными командами `--no-build`, зелёные.
Коммит:

```bash
git add src/Shared.Docker/Engine/IDockerEngine.cs src/Shared.Docker/Engine/DockerEngine.cs src/PgWorker.Docker/Drivers/ClusterDriver.cs src/PgWorker.Provisioning/Processes/NodeSupervisor.cs src/tests/Shared.Docker.UnitTests/Engine/DockerEngineKillTests.cs src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs src/tests/S3Worker.UnitTests/Docker/ClusterDriverTests.cs src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverEnvTests.cs src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverLimitsTests.cs src/tests/KafkaWorker.UnitTests/Docker/PlainClusterDriverTests.cs src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs
git commit -m "feat(docker): KillContainerAsync (POST /containers/{id}/kill, 404=успех) — spilo-ноды PgWorker переводятся на docker kill (арх. §6/§5 D E3, приказ §6-21): PlainClusterDriver EnsureNode-recreate/RemoveNode/StopNode + NodeSupervisor rest-tls; exited-факт немедленный, гонка CallTimeout на spilo-путях устранена; агенты и Kafka/S3/Valkey — прежний поллинг-стоп (§5.9); + фаза ac6-returned-stopped E2E (exited ≤60 с ПОСЛЕ QUARANTINED, §6-22)"
```

**Связь со spec:** §8.3a (п.1–4), §5.9, §6 п.21–22, §7.13.5, §8.1a, §8.4.1, §4 Фаза 4a.

**Подпункты (механика, для слабой модели):**

- **10a.1.** В файле `src/Shared.Docker/Engine/IDockerEngine.cs` после строки `Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);` добавь с комментарием:

```csharp
    // Жёсткая остановка spilo-нод PgWorker (arch/14 §6/§5 D E3): SIGKILL —
    // exited-факт немедленный, поллинг не нужен; 404 = успех (идемпотентность).
    Task<Result> KillContainerAsync(string idOrName, CancellationToken ct);
```

- **10a.2.** В файле `src/Shared.Docker/Engine/DockerEngine.cs` сразу ПОСЛЕ метода `StopContainerAsync` (перед хелпером `IsContainerStoppedAsync`) добавь метод с точным кодом:

```csharp
    // Жёсткая остановка spilo-нод (arch/14 §6/§5 D E3): SIGKILL — грейса нет,
    // exited-факт немедленный, поллинг не нужен; одиночный запрос ≤ CallTimeout.
    // 404 = успех (контейнера нет — идемпотентность); контракт Task<Result>
    // прежний (Result.FromAsync): любой сбой — Failed, не исключение.
    public async Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, $"/containers/{Uri.EscapeDataString(idOrName)}/kill", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // контейнера уже нет — идемпотентность (как stop/карантин E3)
            }
        });
```

- **10a.3.** Проводки spilo-путей (замена вызова; тела веток и комментарии ниже по течению НЕ меняются; единственное исключение — комментарий в `StopNodeAsync`, указанный ниже):
  - `src/PgWorker.Docker/Drivers/ClusterDriver.cs`, три места в `PlainClusterDriver` — в каждом замени строку `var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);` на `var stopped = await engine.KillContainerAsync(name, ct);`:
    1) метод `EnsureNodeAsync`, port-mismatch-ветка пересоздания (строка ~308; после `return; // контейнер на месте с планом — идемпотентность`);
    2) метод `RemoveNodeAsync` (строка ~355; после комментария «404 на каждом шаге — успех (движок); volume pgw-…-data удаляем следом.»);
    3) метод `StopNodeAsync` (строка ~412; комментарий выброса `// карантин E3: только stop, volume/данные на месте` замени на `// карантин E3: только kill (spilo, arch/14 §5 D E3), volume/данные на месте`).
    Вызовы `StopContainerAsync` в методах `EnsureBackupAgentAsync` (~468) и `RemoveBackupAgentsAsync` (~508) — агенты, НЕ spilo — НЕ трогать; `SwarmClusterDriver.StopNodeAsync` — НЕ трогать.
  - `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`, rest-tls-ветка пересоздания (~645): строку `var stopped = await engine.StopContainerAsync(containerName, timeoutSec: 10, ct);` замени на `var stopped = await engine.KillContainerAsync(containerName, ct);` (комментарий над блоком «Пересоздание с сохранением volume: stop + rm force движка» дополни: «stop → kill (spilo, arch/14 §6)»).

- **10a.4.** Добавь член `KillContainerAsync` во ВСЕ тестовые стабы `IDockerEngine` (правка интерфейса 10a.1 иначе ломает сборку; полный список по grep `: IDockerEngine`):
  - с фиксацией вызова: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (класс `FakeEngine`, рядом с `StopContainerAsync`, строки ~117–121):

```csharp
        public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
        {
            Calls.Add(("kill", idOrName));
            return Task.FromResult(Result.Success());
        }
```

  - с фиксацией вызова: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (класс `FakeContainerEngine`, рядом с `Stopped`, ~строки 237–247): добавь поле `public readonly List<string> Killed = [];` и метод:

```csharp
    public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct)
    {
        Killed.Add(idOrName);
        return Task.FromResult(Result.Success());
    }
```

  - заглушка (без фиксации) — в остальные 9 стабов, по образцу соседних членов:

```csharp
        public Task<Result> KillContainerAsync(string idOrName, CancellationToken ct) => Task.FromResult(Result.Success());
```

    Файлы: `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverEnvTests.cs` (`StubEngine`), `src/tests/ValkeyWorker.UnitTests/Docker/ClusterDriverLimitsTests.cs` (`StubEngine`), `src/tests/S3Worker.UnitTests/Docker/ClusterDriverTests.cs` (`StubEngine`), `src/tests/KafkaWorker.UnitTests/Docker/PlainClusterDriverTests.cs` (`FakeEngine`), `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs` (`FakeCleanerEngine`), `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs` (`FakeBackupEngine`), `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs`, `src/tests/PgWorker.IntegrationTests/Backups/RestoreDrillProcessTests.cs` (`FakeDrillEngine`), `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs` (`FakeVerifyEngine`).

- **10a.5.** Создай файл `src/tests/Shared.Docker.UnitTests/Engine/DockerEngineKillTests.cs` с точным содержимым:

```csharp
using System.Net;
using System.Text;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// KillContainerAsync — жёсткая остановка spilo-нод (arch/14 §6/§5 D E3):
// SIGKILL, exited-факт немедленный — поллинга нет; 404 = успех (идемпотентность);
// транспортный отказ/ошибка демона — Result.Failed, не исключение.
public class DockerEngineKillTests
{
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Method, string Url)> Requests = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.Method.Method, request.RequestUri!.PathAndQuery));
            return Task.FromResult(responder(request));
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode code = HttpStatusCode.OK) => new()
    {
        StatusCode = code,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static DockerEngine Engine(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("http://docker") }, "h1");

    // AAA: kill успешен (204) — единственный POST /containers/{id}/kill
    [Fact]
    public async Task Kill_Success_SendsSinglePost()
    {
        // Arrange
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.NoContent));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop-shard1-shard1a", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        handler.Requests.Should().ContainSingle();
        handler.Requests.Single().Method.Should().Be("POST");
        handler.Requests.Single().Url.Should().Be("/v1.44/containers/pgw-shop-shard1-shard1a/kill");
    }

    // AAA: 404 — контейнера нет — успех (идемпотентность kill-пути spilo)
    [Fact]
    public async Task Kill_MissingContainer404_Success()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"message":"No such container"}""", HttpStatusCode.NotFound));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    // AAA: ошибка демона (500) — Result.Failed, НЕ исключение (контракт
    //      Task<Result> — вызыватели классифицируют исходы по Result)
    [Fact]
    public async Task Kill_DaemonError_FailedNotThrown()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(
            """{"message":"driver: device is busy"}""", HttpStatusCode.InternalServerError));

        // Act
        var result = await Engine(handler).KillContainerAsync("pgw-shop", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse("a daemon error is a Failed result, not a thrown exception");
        result.Error.Should().BeOfType<DockerHttpException>();
    }
}
```

- **10a.6.** Обнови 5 существующих тестов под kill-вызов (агентный тест `EnsureBackupAgent…` с «stop»-цепочкой — НЕ менять):
  - `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`:
    - `EnsureNode_PortDrift_RecreatesContainer`: ассерт `calls.Should().ContainInOrder("stop", "create", "start");` → `ContainInOrder("kill", "create", "start");`;
    - `RemoveNode_StopsRemovesContainerAndVolume`: `calls.Take(3).Should().Equal("stop", "rm-container", "rm-volume");` → `Equal("kill", "rm-container", "rm-volume");`;
    - `StopNode_StopsOnly_NoRemoveNoVolume`: `engine.Calls.Select(c => c.Call).Should().Equal("stop");` → `Equal("kill");`.
  - `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs` — оба rest-tls-теста пересоздания (путь NodeSupervisor.cs:645):
    - `RestTlsStep_LegacyContainer_RecreatedWithEnsuredPair`: `engine.Stopped.Should().ContainSingle()…` → `engine.Killed.Should().ContainSingle().Which.Should().Be("pgw-shop-shard1-shard1a");`;
    - `RestTlsStep_LegacyLeader_DcsWitnessRunning_HardTeardown` (~строки 2121–2148): замени фрагмент

старое:

```csharp
        // Assert: жёсткий снос лидера (выборы за Patroni) + пересоздание в TLS
        // с парой ensure; volume сохранён (stop+rm, не RemoveNodeAsync).
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        engine.Stopped.Should().ContainSingle().Which.Should().Be("pgw-shop-shard1-shard1a");
```

новое:

```csharp
        // Assert: жёсткий снос лидера (выборы за Patroni) + пересоздание в TLS
        // с парой ensure; volume сохранён (kill+rm, не RemoveNodeAsync).
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        engine.Killed.Should().ContainSingle().Which.Should().Be("pgw-shop-shard1-shard1a");
```

- **10a.7.** Убедись, что незакоммиченная правка `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs` (фаза `ac6-returned-stopped` — WaitPhase exited ≤60 с после QUARANTINED) присутствует и попадает в коммит 10a (санкция §6 п.22/§8.1a); иных незакоммиченных изменений вне перечня задачи 10a быть не должно (`git status`).

---

### Задача 15: репро фейла 2 — `Pool_ReadAnyNode_RestoreAfterNodeStop` + локализация пересоздателя (§7.9) — сильная модель

**Вход:** задачи 9a/10a закоммичены (демонтаж spilo идёт через kill — CallTimeout-конфликт на spilo-путях исчезает, §8.4.1); хост зачищен; сборка slnx Release зелёная.

**Действие (coder):** пред-чек §7.7 И §7.7.5 (см. преамбулу ниже); ДО старта теста запустить параллельный съём в файл (фон): `docker events --filter 'container=pgw-pac*' > /tmp/pgw-plan-logs/part-b/fail2-docker-events.log 2>&1` (фильтр по префиксу контура `pgw-pac*`, сузить до `*shard1*` по факту tag); соло-прогон `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~Pool_ReadAnyNode_RestoreAfterNodeStop"` (E2ePrepare-штамп из задачи 14 действует; если штамп снят пересборкой — повторить `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`). Анализ events + лога прогона.

**Операционное предусловие §7.7.5 (обязательное, задачи 15–17):** окно репро свободно от чужих docker-сессий. Пред-чек перед КАЖДЫМ репро: (а) `docker ps --format '{{.Names}}' | grep -E '^(pgw-|kfw-|s3w-)'` → вывод в `/tmp/pgw-plan-logs/part-b/precheck-<имя>.log`, непустой = чужой контур → СТОП и доклад (не запускать); (б) `ps aux | grep '[d]otnet test'` → чужие прогоны (не нашей сессии) в лог того же файла → СТОП и доклад. Снесённый чужой сессией репро повторяется ТОЛЬКО после повторного пред-чека, подтверждающего отсутствие чужой активности; факт помехи и повтор — записью в лог задачи (`/tmp/pgw-plan-logs/part-b/interference.log`).

**Выход:** вердикт по пересоздателю нод; verify: restore-джоба создаётся, заявка COMPLETED в бюджете 420 с.

**Проверка (критерии §7.9):** подтверждение диагноза: в events видны create `pgw-…-shard1-shard1a/b` ПОСЛЕ stop/kill-событий демонтажа в окне RUNNING-заявки, автор create — процесс воркера → закрыть гвардом надзора restore-шардов по канону arch/14 §5 C (правка кода к канону; при расхождении формулировок — arch-first; гвард — сильная модель, отдельный коммит `fix(supervisor): …` + юнит-тест гварда). Критерий опровержения: create отсутствует, а демонтаж всё равно циклится — расширять диагностику логом DockerEngine (лог-строки поллинга), перезапуск без диагноза запрещён. После прогона — съём docker events остановить, зачистка контура.

**Связь со spec:** §7.7 (включая §7.7.5), §7.9, §8.4 (п.1 — kill уже в 10a); §4 Фаза 6. Подпунктов нет.

---

### Задача 16: репро фейла 4 — `Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates` (§7.11) — сильная модель

**Вход:** задача 15 завершена, хост зачищен; задачи 9/13 закоммичены (K3-гвард).

**Действие (coder):** пред-чек §7.7 + §7.7.5; параллельные съёмы в файлы (фон): `docker events` по префиксу контура `kfw-tkw*` (`/tmp/pgw-plan-logs/part-b/fail4-docker-events.log`) и периодический (шаг ≤10 с в окне прогона) дамп etcd-ключей контура `/kafkaworker/portalloc/<C>` и `/kafkaworker/work/<C>` в `/tmp/pgw-plan-logs/part-b/fail4-etcd-dump.log` (etcdctl в контейнере etcd контура или HTTP-range gateway — фактический способ по окружению теста). Соло-прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates"` (KafkaE2eEnvironment самодостаточен, E2ePrepare PgWorker не нужен). Анализ дампов + лога.

**Выход:** подтверждение/опровержение механизма частичного portalloc на живом репро; verify фикса K3.

**Проверка (критерии §7.11):** подтверждение: дамп фиксирует частичный portalloc (без brokerN) в окне takeover; после фикта: survivor доводит provisioning до done ≤360 с, без дубликатов контейнеров (docker events: один create на брокера). Критерий опровержения: фейл воспроизводится с ПОЛНЫМ portalloc в дампе — искать другой словарь (расширять детализацию last_error; см. investigation §Фейл 4 (e)). После прогона — съёмы остановить, зачистка контура.

**Связь со spec:** §7.7 + §7.7.5, §7.11, §8.6; §4 Фаза 6. Подпунктов нет.

---

### Задача 17: репро фейла 3 — S3 `PkiProvisioning_ReplicationOverLinks` ×3 (§7.10) — сильная модель

**Вход:** задача 16 завершена, хост зачищен; задачи 9/12 закоммичены (env=10 + самодиагностика).

**Действие (coder):** пред-чек §7.7 + §7.7.5; ТРИ ПОДРЯД зелёных соло-прогона (N=3, §6 п.20): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/S3Worker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~PkiProvisioning_ReplicationOverLinks"` (S3E2eEnvironment сам собирает образы, E2ePrepare не нужен); между прогонами — зачистка контура; каждый прогон — лог в `/tmp/pgw-plan-logs/part-b/fail3-run<N>.log`. Фиксация env: `docker inspect` контейнеров `s3w-<C>-east1/east2` — `OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS=10`.

**Выход:** три подряд зелёных прогона — стартовое окно OwnS3 закрыто механизмом, не ожиданием.

**Проверка (критерии §7.10):** каждый прогон: фаза `replicated` ok за ≤10–30 с после `joined`; env=10 в inspect; тест-бюджет 150 с не исчерпан. Критерий опровержения: с env=10 с объект всё равно не доставлен и в логах OwnS3 нет ни одного запуска ReconcileRunner после стартового — разбирать маппинг env-префикса в OwnS3 (`OwnS3.App/Program.cs:55`, репозиторий `../OwnS3`), перезапуск без диагноза запрещён. После каждого прогона — зачистка контура.

**Связь со spec:** §7.7 + §7.7.5, §7.10, §8.5, §6 п.17/20; §4 Фаза 6. Подпунктов нет.

---

### Задача 7: финальная валидация обеих частей + kill-дельты (§7.5, §7.12)

**Вход:** задачи 1–6, 9–14, 9a/10a, 15–17 завершены; хост зачищен.

**Действие (coder):** финальная валидация строго по порядку; решение одно — `src/PgWorker.slnx` (мегасолюшен: PgWorker/KafkaWorker/S3Worker/ValkeyWorker/Shared.Docker и их тестовые проекты). Сборка и тесты — ДВУМЯ ОТДЕЛЬНЫМИ командами. Полный docker-E2E — ОДИН раз, здесь, после всех фиксов (§7.5), включая kill-проводку (AC6 в `E2eScenarios` верифицирует kill на живом контуре).

**Выход:** зелёные полные юниты всех затронутых сборок (крест-прогон §8.3: PgWorker/Kafka/S3/Valkey + Shared.Docker); зелёный полный E2E на свежем Release, включая все четыре ранее падавших сценария (с kill-семантикой spilo) и moves-сценарии части А (сид `"Orders"`/`"Orders_seq"`).

**Проверка (команды, по порядку):**
1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Warning(s)/0 Error(s).
2) Полные юниты (каждую — отдельной командой, `--no-build`): `PgWorker.UnitTests`, `KafkaWorker.UnitTests`, `S3Worker.UnitTests`, `ValkeyWorker.UnitTests`, `Shared.Docker.UnitTests` (все с префиксом `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/<сборка> -c Release --no-build`) — зелёные.
3) `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` — штамп E2E-статики (канонический путь — `src/tests/PgWorker.E2ePrepare`).
4) Минимум-маркер: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard` — зелёный.
5) ПОЛНЫЙ E2E-прогон ОДИН раз: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~E2e"` — зелёный: все `E2e*Scenarios` всех воркеров, включая `E2eMoveScenarios` (сид части А), `Acceptance_Scenario_Ac2_To_Ac7` (с kill и фазой `ac6-returned-stopped`), `Pool_ReadAnyNode_RestoreAfterNodeStop`, `S3E2eMeshTests.PkiProvisioning_ReplicationOverLinks`, `KafkaE2eTakeoverTests.Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates`. («Полный прогон E2eFixture» — идиома полной E2E-серии; фильтр — по namespace `~E2e`, сужение состава запрещено.) Упавший сценарий — канон телеметрии (docs/e2e-launch.md): MarkFailed — стоп без удаления, разбор по артефактам `/tmp/pgw-e2e-artifacts-*` И `$TMPDIR` и docker-логам; перезапуск без диагноза запрещён.
6) Зачистка контейнеров/сетей после серии по канону AGENTS.md.

**Связь со spec:** §7.5, §7.12, §8.3, §6 п.21–23; §4 Фазы 4a/7. Подпунктов нет.

---

### Задача 8: гигиена и финальная самопроверка (§7.6) — сильная модель

**Вход:** задача 7 зелёная, зачистка после серий сделана.

**Действие (coder):**
1. Удалить закрытый пункт #3 из ревью-документа ОСНОВНОГО чекаута `/Users/demakaev/ZCodeProject/pg/.my/review_project.md` (файл вне git и вне worktree — правится по этому абсолютному пути; правило AGENTS.base.md §16: закрыл пункт — удали, перенумерация НЕ нужна). Фактическое состояние файла (сверено): (а) в таблице «Что найдено» строки #3 УЖЕ НЕТ (таблица начинается с `| [10](#p10) |`) — таблицу НЕ трогать; (б) секция деталей #3 существует: блок от строки `<a id="p3"></a>` (заголовок `## 🟠 #3. Cutover: ArgumentException …`) до ДВОЙНОГО разделителя `---`/`---`, за которым следует `<a id="p10"></a>` (якоря `p4` в файле нет). Удалить строки от `<a id="p3"></a>` по конец секции включительно с ОДНИМ из двух завершающих `---` — так, чтобы перед `<a id="p10"></a>` остался ровно один разделитель `---`. Остальные пункты — без изменений.
2. Финальная самопроверка в worktree: `git status` — изменения строго в scope: часть А (`src/PgWorker.Moves/Sql/MoveSql.cs`, `src/PgWorker.Moves/Process/CutoverSequence.cs`, `src/tests/PgWorker.UnitTests/Moves/{MoveSqlTests,MoveSqlPart2Tests,CutoverSequenceTests,FakesMove}.cs`, `src/tests/PgWorker.IntegrationTests/E2e/E2eMoveScenarios.cs`); часть Б (`arch/14-pgworker.md` [правки задач 9 И 9a], `arch/24-s3worker.md`, `arch/16-kafkaworker.md`, `arch/roadmap/reliability.md`, `src/Shared.Docker/Engine/{IDockerEngine,DockerEngine}.cs`, `src/tests/Shared.Docker.UnitTests/Engine/{DockerEngineStopTests,DockerEngineKillTests}.cs`, `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs`, `src/tests/PgWorker.UnitTests/Provisioning/{Fakes,BucketEvacuatorTests}.cs`, `src/S3Worker.App/{Options,Program}.cs`, `src/S3Worker.Provisioning/InstanceEnvComposer.cs`, `src/S3Worker.Provisioning/Processes/{S3ProcessCommon,ProvisioningProcess,NodeSupervisor,AddNodeProcess,SecretRotator}.cs`, `src/tests/S3Worker.IntegrationTests/E2e/{S3E2eEnvironment,S3E2eMeshTests}.cs`, `src/tests/S3Worker.UnitTests/Provisioning/InstanceEnvComposerTests.cs`, `src/KafkaWorker.Provisioning/Processes/{ProvisioningProcess,PortAllocHealer}.cs`, `src/tests/KafkaWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`, `src/PgWorker.Docker/Drivers/ClusterDriver.cs`, `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`, `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`, `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`, `src/tests/PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`, `src/tests/PgWorker.IntegrationTests/Backups/{RestoreDrillProcessTests,BackupVerifyProcessTests,FakeBackupEngine}.cs`, `src/tests/S3Worker.UnitTests/Docker/ClusterDriverTests.cs`, `src/tests/ValkeyWorker.UnitTests/Docker/{ClusterDriverEnvTests,ClusterDriverLimitsTests}.cs`, `src/tests/KafkaWorker.UnitTests/Docker/PlainClusterDriverTests.cs`, `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs`, плюс условный гвард задачи 15 при подтверждении breach) + `docs/superpowers/2026-10-21-…/{spec.md,investigation-e2e-4-failures.md,plan.md}`; `git diff --stat HEAD -- arch/` — только задачи 9/9a (никаких иных касаний arch/).

**Выход:** пункт #3 удалён; дифф ветки строго в scope spec (части А, Б, kill-дельты).

**Проверка:** `grep -n "p3\|#3\b" /Users/demakaev/ZCodeProject/pg/.my/review_project.md` — пусто; `git -C <worktree> status --short` — только перечисленные файлы.

**Связь со spec:** §7.6, §5.1. Подпунктов нет.

---

## Соответствие задач требованиям spec (самопроверка плана)

| Требование spec | Задача | Статус |
|---|---|---|
| §3.1–3.5, §7.1–7.3 (часть А: билдеры/verify/фейки/тесты) | 1–4 | Исполнено (bcb934c7, 17ab12a2) |
| §3.3 E2E-сид; §7.4 сборка+юниты | 5–6 | Исполнено (32584b8f) |
| §8.8 arch: 14§6 поллинг; 24§5A+§8; 16§5A K3; roadmap t90 | 9 | Исполнено (11f87b75) |
| §8.3.1/§7.13.1 поллинг StopContainerAsync | 10 | Исполнено (750ae4af) |
| §8.3.2/§7.13.2 эвакуатор P7 | 11 | Исполнено (8b94a1fe) |
| §8.5/§7.13.3 S3 env+опция+самодиагностика | 12 | Исполнено (210469d5) |
| §8.6/§7.13.4 Kafka K3 гвард | 13 | Исполнено (f1f27a91) |
| §7.8 репро фейла 1 (ac6-quarantine) | 14 | ЗАКРЫТО (§6 п.23; повтор не нужен) |
| §8.8 kill: arch/14 §6 дополнение + §5 D E3 kill-формулировка | 9a | в очереди |
| §8.3a.1 KillContainerAsync (движок+интерфейс) | 10a (пп. 10a.1–2, 10a.5) | в очереди |
| §8.3a.2 проводки ClusterDriver (spilo→kill) | 10a (пп. 10a.3, 10a.6) | в очереди |
| §8.3a.3 проводка NodeSupervisor:645 | 10a (пп. 10a.3, 10a.6) | в очереди |
| §8.3a.4/§5.9 границы kill (агенты/Kafka/S3/Valkey — stop) | 10a (пп. 10a.3–4; агентный тест не меняется) | в очереди |
| §7.13.5 kill-юниты + kill-vs-stop по типу ноды | 10a (пп. 10a.5–6) | в очереди |
| §8.1a коммит незакоммиченных правок (ac6-returned-stopped; StopTests — уже в 750ae4af) | 10a (пп. 10a.7) | в очереди |
| §7.7.1–7.7.5 общие правила репро (чужие сессии) | 15 (преамбула серии) | в очереди |
| §7.9 репро фейла 2 + локализация + гвард | 15 | в очереди |
| §7.11 репро фейла 4 | 16 | в очереди |
| §7.10 репро фейла 3 ×3 (N=3, В6) | 17 | в очереди |
| §7.12.1 + §8.3 сборка + полные юниты (5 сборок) | 7 (п.1–2) | в очереди |
| §7.5/§7.12.2 полный E2E ОДИН раз (все 4 фейла + moves + kill) | 7 (п.3–6) | в очереди |
| §7.6 гигиена (review #3; arch только §8.8; docs) | 8 | в очереди |

Развилок при реализации нет: kill-граница предопределена §5.9 (класс контейнера: spilo = контейнер ноды шарда — все 4 spilo-вызова в PgWorker перечислены по результату grep'а; агенты 468/508 и SwarmClusterDriver — stop/remove без изменений); повторы задач 14 — запрещены §6 п.23; условные ветки задач 15/16/17 имеют объективные критерии §7.9–7.11; операционное правило §7.7.5 выписано в преамбулу задачи 15 (действует на 15–17); ассерты exited после QUARANTINED — фаза `ac6-returned-stopped` (§6 п.22), уже в `E2eScenarios.cs`; контракт kill-метода — `Result.FromAsync`-стиль (Failed на любой сбой, 404 = успех).
