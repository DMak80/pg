# t10-rs-dr-master-readiness-gate — гейт готовности мастера после restore (spec)

- **Дата**: 2026-09-15
- **Roadmap**: `arch/roadmap/pgworker.md`, тег `t10-rs-dr-master-readiness-gate` (снимается тем же коммитом мержа в `main` — мерж-гейт)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/fix-t10-rs-dr-master-readiness-gate`
- **Тип**: по исходной постановке — доработка E2E-сценария rs-dr; **расширена пользователем в фазе brainstorming**: чиним и прод-причину рестарт-окна (restore-механика воркера), не только тест. Контракт `arch/19-backups.md` §3.5 меняется (arch-first, Фаза 0).
- **Правка по итогам исполнения** (Фаза 6, коммит `2102259`, 2026-09-15): §4.2 — буквальный контракт SQL-пробы `pg_is_in_recovery()` переведён со строкового `"f"` на **boxed bool `false`** (PG18: `boolean::text` возвращает `"false"`, строковый паттерн `"f"` не сходился бы никогда — инцидент первого E2E-прогона t10; семантика гейта «пропускать только при мастере вне recovery» не изменилась).

## 1. Цель

Устранить системный флейк E2E-сценария rs-dr (`Restore_NewCluster_FromSourcePrefix`,
`E2eRestoreScenarios`) — финальный `SELECT count(*)` после restore ловит
рестарт-окно Patroni/postgres: Npgsql `57P01 «terminating connection due to
administrator command»` (`E2eRestoreScenarios.cs` ScalarAsync:693, вызов :338).
Флейк стабилен на текущей машине (падения на main-бейзлайне `cf06ce0` и на ветке
t09 идентичны), на среду не списываем — чиним причину. Три уровня:

1. **Прод-фикс корня** — restore-джоба возвращает pristine `postgresql.auto.conf`,
   несущий `archive_mode` источника → Patroni нового HA-scope ставит
   «Pending restart» → отложенный рестарт postmaster рвёт соединения. Вычищаем.
2. **Прод-гейт готовности** — restore переходит в `COMPLETED` только после
   SQL-пробы фактического постгреса лидера: планировщик полных и читатели
   (тест/панель) после `COMPLETED` работают в закрытом окне.
3. **Тестовый гейт rs-dr** (страховка) — SQL-проба мастера перед финальным
   чтением + ретрай чтения по эталону rs-latest.

## 2. Диагноз (проверен по коду worktree и артефактам)

### 2.1. Механика гонки

Сценарий rs-dr (строки ~328–340): после `RestoreWithRetryAsync(shard1)` +
`RestoreWithRetryAsync(shard2)` тест делает `MasterPgAsync(cluster,"shard1")`
(пробы Patroni `GET /primary`, первая 200-ответившая = мастер) и СРАЗУ выполняет
ОДИН `ScalarAsync("SELECT count(*) FROM dr_probe")` — без ретрая (в rs-latest
:114–127 и rs-time :239–251 ретрай-цикл `catch (NpgsqlException)` + `Delay(8000)`
×5 уже есть; в rs-dr его нет).

Проба `/primary` отвечает 200 и в переходном окне: Patroni API жив, роль
формально primary, даже когда postmaster рестартует.

### 2.2. Происхождение рестарт-окна (артефакты `pgw-e2e-artifacts-02e65f9fa8654ff4ac15d2dc6a8f4d93`)

Таймлайн (UTC, 2026-09-15):

| Время | Событие | Источник |
|---|---|---|
| 12:40:17.78 | restore-джоба завершилась (promote → `PGCTL -m fast stop`) | RestoreJobCommand.cs:136; лог ноды |
| 12:40:22.85 | shard1a: `Changed archive_mode from 'on' to 'None' (restart might be required)` | container-…shard1a.log:434 |
| 12:40:23.86 | shard1a: `…bypassing Patroni config. Setting 'Pending restart' flag` (повторяется каждый тик) | …shard1a.log:454,459 |
| 12:40:21→12:40:35 | плановый full-shard2 фейл («pg_basebackup failed», 14 с) | container-pgw-backup-full-…shard2-20260915124021Z |
| 12:41:09.24→.27 | плановый full-shard1 стартовал и упал за 28 мс («pg_basebackup failed») | container-pgw-backup-full-…shard1-20260915124109Z.log |
| 12:41:09.6 | тест: Npgsql 57P01 в ScalarAsync:693 (вызов :338) | pg-main-baseline.log:35 |

Механизм: restore-джоба перед stop возвращает pristine auto.conf
(`mv "$AUTO.orig" "$AUTO"`, RestoreJobCommand.cs:141) — он снят `pg_basebackup`-ом
с ноды-источника и несёт её `archive_mode=on`. Демонтаж restore'а вычищает
HA-scope `/service/<C>-<X>/`, Patroni нового scope навязывает свой динамический
конфиг (`archive_mode=None`): reload для `archive_mode` недостаточен →
«Pending restart» → при применении — рестарт postmaster, соединения рвутся
(57P01). Docker-лог ноды оборван teardown'ом на 12:41:09.01Z (рестарт внутрь
окна не попал — ограничение доказательной базы, отмечено); фейл full-shard1 за
28 мс в ту же секунду — независимое подтверждение, что постгрес был недоступен.

`RestoreProcess.RejoinAsync` ставит `COMPLETED` по Patroni API-пробам
(лидер `running|streaming` + реплики `running|streaming|creating replica`,
RestoreProcess.cs:572–609) — SQL-пробы постгреса нет, поэтому `COMPLETED`
приходит ДО закрытия рестарт-окна; сразу после `COMPLETED` удаляется wal-ключ
и планировщик немедленно стартует пересъём полного (BackupProcess G3,
`IsDue(!walKeyExists)`) — отсюда фейлы full-джоб в окне.

### 2.3. Идентичность на базе и ветке

Падения идентичны: ветка t09 (`pg-rerun.log`, 5 м 24 с) и main-бейзлайн
`cf06ce0` (`pg-main-baseline.log`, 30 м 54 с) — тот же 57P01 в ScalarAsync:693
из :338. Код t09 ни при чём: гонка в связке «тест без гейта × прод без гарантий
после restore», ставшая системной на текущей машине (фоновая нагрузка удлиняет
окна). В мерже t08 (14.09) rs-dr уже флейкнул («зелёный на повторе») — окно
существовало и раньше.

## 3. Решения пользователя (brainstorming 2026-09-15)

1. Конструкция тестового гейта: **SQL-проба готовности + ретрай чтения**
   (не «только ретрай», не «проба + Patroni pending_restart»).
2. Область тестовой правки: **только rs-dr** (rs-latest/rs-time не трогаем).
3. Прод-артефакт «Pending restart после restore»: **«делать сразу сейчас,
   исправлять всё»** — чиним в рамках t10, не в roadmap.
4. Размещение прод-гейта: **SQL-проба перед COMPLETED в RestoreProcess**
   (не settle-окно в планировщике, не оба).

## 4. Дизайн

### 4.1. Компонент A — вычистка archive-параметров в restore-джобе (корень)

`src/PgWorker.Backups/Restore/RestoreJobCommand.cs`, после возврата pristine
(`mv "$AUTO.orig" "$AUTO"`, :141) — вычистить из auto.conf параметры архивации,
которыми управляет Patroni:

```bash
sed -i '/^archive_mode[[:space:]=]/d;/^archive_command[[:space:]=]/d' "$AUTO"
chown 101:101 "$AUTO"
```

- Постгрес стартует с дефолтом (`archive_mode=off`), Patroni-конфиг нового
  scope совпадает → «Pending restart» не возникает → отложенный рестарт
  postmaster исчезает. WAL-архивация в системе — внешний `pg_receivewal`-агент
  t03, а не `archive_mode` постгреса: вычистка поведение бэкап-контура не ломает.
- `chown 101:101` обязателен: sed -i пересоздаёт файл под root, а блок
  chown (:111) уже прошёл; прецедент осознания — комментарий в
  RestoreJobCommandTests.cs:33 («sed -i пересоздаёт файл под root»).
- Комментарий в скрипте — по канону файла (русский, со ссылкой на инцидент t10).

Юнит-тесты (`RestoreJobCommandTests`): скрипт содержит вычистку после `mv`
(ассерт порядка IndexOf — прецедент :34–37) и `chown 101:101 "$AUTO"` после sed.

### 4.2. Компонент B — SQL-проба мастера перед COMPLETED (гейт готовности)

`src/PgWorker.Backups/Process/RestoreProcess.cs`, `RejoinAsync`: между
`allStarted` (:604–609) и постановкой нод `RUNNING`/`COMPLETED` (:614+) — проба
фактического постгреса лидера:

- DSN: `ShardEndpoints.AdminDsn(firstAddr, snap.Config.DbName, secrets)` —
  паттерн BackupProcess G2 (BackupProcess.cs:96–99); `firstAddr` уже вычислен.
- Проба: `ISqlExecutor.ExecuteScalarAsync(dsn, "SELECT pg_is_in_recovery()")`
  — гейт пропускает при `false`: Npgsql возвращает **boxed bool**, результат
  сравнивается с `false` (НЕ строка). ПРИМЕЧАНИЕ PG18 (инцидент первого
  E2E-прогона t10, фикс — коммит `2102259`): `boolean::text` в PostgreSQL 18
  возвращает `"false"`, а не `"f"` — строковый паттерн `"f"` не сходился бы
  никогда; `::text` в пробе не использовать. Проба сильнее `SELECT 1`:
  закрывает и окно crash recovery после рестарта (Patroni `/primary` там уже
  200, постгрес — ещё в recovery). Транспорт/`true` → НЕ готова.
- Не готова → ожидание по образцу `RejoinWaitAsync`: тот же бюджет
  `thresholds.PatroniBootSec` (E2E-хост 120 с), тот же `_rejoinWaitSince`-трекер
  с суффиксом SQL-ожидания в waitKey, отдельная фаза-метка журнала
  (`master-sql-wait/{shard}/{op.Id}`), warn-телеметрия каждые ~10 тиков.
  Никаких мутаций до готовности (TransientAsync — тик повторит). Бюджет
  исчерпан → permanent-FAILED «мастер не принял SQL за N с» со снимком
  Patroni-диагностики (по образцу :654–666).
- `ISqlExecutor` добавляется в конструктор RestoreProcess; wiring —
  `src/PgWorker.App/Program.cs:476` (регистрация рядом, прецедент BackupProcess).
- Инвариант планировщика не меняется: после `COMPLETED` (уже SQL-готов)
  wal-ключ удаляется и full стартует в закрытом окне — BackupProcess не трогаем.

Контракт (arch-first, Фаза 0): `arch/19-backups.md` §3.5 —
«→ все ноды RUNNING → `COMPLETED`» дополняется: перед `COMPLETED` воркер ждёт
SQL-готовности мастера восстановленного шарда (`pg_is_in_recovery()=false`),
бюджет — `PatroniBootSec`, исчерпан — permanent-FAILED. Механика джоба §3.5
дополняется: при возврате pristine auto.conf вычищаются управляющие Patroni
параметры архивации (`archive_mode`/`archive_command`).

### 4.3. Компонент C — тестовый гейт rs-dr (страховка)

`src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs`, сценарий
`Restore_NewCluster_FromSourcePrefix`, после `RestoreWithRetryAsync(shard2)`
перед финальным чтением (:335–340):

1. **Гейт готовности** через `WaitPhaseAsync("master-ready", …)` (правило
   docs/e2e-launch.md §2: новый ожидания-участок — только через WaitPhaseAsync;
   строка `[PHASE]`, авто-сбор логов при >60 с): поллинг с бюджетом 120 с —
   `MasterPgAsync` → DSN → `ScalarAsync(dsn, "SELECT 1")` в try/catch
   (`catch (NpgsqlException) → false`); эталон — E2eBackupScenarios.cs:225–236.
   `SELECT 1` достаточно: факт «соединение открыто и запрос отвечен».
2. **Финальное чтение с ретраем** — эталон rs-latest (:114–127):
   `for (1..5) { try ScalarAsync(count) break; catch (NpgsqlException) when (attempt<5) Delay(8000) }`.

Окружение/teardown сценария не меняются: тест остаётся изолированным и
самоочищающимся (гейт — только порядок чтения, новых docker-объектов нет).

### 4.4. Отвергнутые альтернативы

| Альтернатива | Почему нет |
|---|---|
| Только ретрай чтения в rs-dr | Не гейт: вслепую тратит попытки в окне; не чинит прод-причину (пользователь: «исправлять всё») |
| Гейт «нет активных джоб шарда» | Не защищает: рестарт вызывает Patroni-доводка конфига, не джобы; full-джобы бесконечны (перепланирование) — гейт не сходится |
| Гейт по Patroni `pending_restart` (GET /patroni) | Зависимость от формата Patroni API; `pending_restart=false` не гарантирует отсутствия следующего рестарта (Patroni и так переписывает конфиг на rejoin) |
| Settle-окно (N сек после restore) в планировщике BackupProcess.G3 | Гейт «по времени», окно эмпирично; меняет планировщик; SQL-проба перед COMPLETED закрывает окно для ВСЕХ потребителей фактом, а не таймером (выбор пользователя) |
| Общий хелпер на все 3 сценария файла | Пользователь: «только rs-dr» (буква задачи) |
| Прод-фикс только в roadmap | Пользователь: «делать сразу сейчас» |

## 5. Фазы

0. **arch-first**: `arch/19-backups.md` §3.5 — два дополнения (механика джоба:
   вычистка archive-параметров при возврате pristine; условия COMPLETED:
   SQL-проба мастера). До кода.
1. **Компонент A** (джоба): правка скрипта `RestoreJobCommand.cs` + юнит-ассерты
   `RestoreJobCommandTests` (содержание и порядок вычистки/chown).
2. **Компонент B** (гейт): `RestoreProcess.RejoinAsync` (проба + ожидание +
   FAILED-бюджет), конструктор + `Program.cs` wiring; интеграционные кейсы
   `RestoreProcessTests` (фейк `ISqlExecutor`): (а) не готова → статус REJOINING
   без мутаций, повтор тика; (б) готова → COMPLETED; (в) бюджет исчерпан →
   permanent-FAILED с причиной; (г) takeover: рестарт воркера не сбрасывает
   ожидание (waitKey стабильный).
3. **Компонент C** (тест rs-dr): гейт + ретрай в `E2eRestoreScenarios.cs`.
4. **Прогоны** (порядок и зачистка — по AGENTS.md: серия за серией, после
   КАЖДОЙ — зачистка контейнеров/сетей): build → юниты → интеграция →
   docker-E2E на свежем Release: маркер `Scale_AddEmptyShard` (правило
   «трогаем код воркеров») + `E2eRestoreScenarios` (все 3 сценария, rs-dr
   зелёный). Логи анализируем ОНЛАЙН (см. §7).
5. **Мерж-гейт**: снятие тега `t10-rs-dr-master-readiness-gate` из
   `arch/roadmap/pgworker.md` тем же коммитом мержа.

## 6. Ограничения и принципы

- Планировщик полных (`BackupProcess`) НЕ меняем: гейт до COMPLETED закрывает
  окно для него автоматически.
- rs-latest/rs-time НЕ трогаем (решение пользователя); их собственные
  ретраи-эталоны остаются как есть.
- Прод-правки минимальны и в духе окружающего кода: bash-строка в скрипте
  джобы (канон «единственное место restore-механики»), проба в RejoinAsync на
  существующем трекере/бюджете — новых ключей etcd, новых статусов, новых
  конфиг-параметров НЕ вводим.
- E2E-телеметрия — по docs/e2e-launch.md: гейт через WaitPhaseAsync; упавший
  сценарий — MarkFailed (stop, не delete); перезапуск упавших — только после
  полного анализа логов и согласия пользователя.
- Порты динамические, хардкодов нет и не появляется (гейт ходит по DSN из
  portalloc — как уже делает MasterPgAsync).
- Таймауты тестов короткие: бюджет гейта 120 с согласован с PatroniBootSec=120
  E2E-хоста; упавший прогон падает быстро.

## 7. Критерии приёмки

1. **rs-dr зелёный** на свежем Release (`PGW_TEST_DOCKER=1 dotnet test
   src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eRestoreScenarios`;
   E2eFixture собирает Release сам).
2. **Docker-E2E мерж-гейт**: маркер `Scale_AddEmptyShard` на Release зелёный
   (код воркера меняется) + полный прогон `E2eRestoreScenarios` (все 3).
3. Полные серии юнитов и интеграции зелёные; после каждой серии — зачистка
   контейнеров/сетей (никаких наложений серий).
4. Новые кейсы компонентов A/B зелёные (RestoreJobCommandTests,
   RestoreProcessTests).
5. По артефактам E2E-прогона (фаза исполнения, ручная проверка разбора):
   в docker-логах нод после restore-джобы отсутствуют строки
   `Setting 'Pending restart' flag` — факт устранения корня; плановые
   full-джобы после restore не фейлятся в переходном окне.
6. **Онлайн-анализ логов при прогонах E2E** (процессное требование пользователя,
   постоянно): агент следит за фоновым прогоном и разбирает логи/артефакты
   ПО МЕРЕ их появления (журнал теста, host.log, docker-логи, slow-phase
   артефакты), не дожидаясь завершения всей серии; упало — разбор до
   какого-либо перезапуска.

## 8. Риски и ограничители

- В pristine auto.conf возможны и другие рестарт-требующие расхождения с
  Patroni-конфигом (по артефактам инцидента — только `archive_mode`). Если
  после фикса в прогонах всплывёт новое «Pending restart (X)» — расширить
  вычистку той же sed-строкой по факту; отдельного обобщения сейчас не делаем
  (YAGNI, по букве инцидента).
- SQL-проба удлиняет путь до COMPLETED (обычно секунды; верхняя граница —
  PatroniBootSec). rs-time ждёт wal-reinit с бюджетом 300 с — запас есть.
- Фейк `ISqlExecutor` в RestoreProcessTests обязан покрывать оба transient-вида:
  «соединение рвётся» (исключение) и «висит в recovery» (`true`, boxed bool —
  см. §4.2 ПРИМЕЧАНИЕ PG18) — иначе кейс (а) недетерминирован.
- RestoreProcessTests сегодня не передаёт ISqlExecutor (его нет в конструкторе):
  все существующие кейсы получают тривиальный фейк «готова» — поведение старых
  кейсов не меняется (проба после allStarted, который они уже проходят).

## 9. Ключевые файлы

| Файл | Роль |
|---|---|
| `arch/19-backups.md` §3.5 | контракт restore (Фаза 0) |
| `src/PgWorker.Backups/Restore/RestoreJobCommand.cs` | скрипт джобы: вычистка (A) |
| `src/PgWorker.Backups/Process/RestoreProcess.cs` | RejoinAsync: SQL-проба (B) |
| `src/PgWorker.App/Program.cs` (:476) | wiring ISqlExecutor (B) |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eRestoreScenarios.cs` | rs-dr: гейт + ретрай (C) |
| `src/tests/PgWorker.UnitTests/Backups/RestoreJobCommandTests.cs` | юнит-ассерты A |
| `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` | интеграционные кейсы B |
| `arch/roadmap/pgworker.md` | снятие тега t10 (мерж-гейт) |
