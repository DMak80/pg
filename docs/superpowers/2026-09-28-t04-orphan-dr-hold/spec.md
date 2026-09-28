# t04-orphan-dr-hold — защита DR-источника от сиротского TTL

- **Дата**: 2026-09-28
- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), тег `t04-orphan-dr-hold` (мерж-гейт трека: тег снимается из roadmap И строка в [`reliability-report.md`](../../../arch/roadmap/reliability-report.md) обновляется тем же мерж-коммитом). Не путать с `t04-backup-verify` из карты arch/19 §8 — это задача ДРУГОЙ серии (сквозная надёжность, P1, характеристика D).
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t04-orphan-dr-hold` (ветка `feat-t04-orphan-dr-hold`); правки — только здесь.
- **Тип**: доработка существующей подсистемы сирот бэкапов (t07-супервизор) — новые правила TTL-отбора + заявочная механика (etcd + API воркера + панель) + runbook + E2E.
- **Канон (обновлён этой задачей, arch-first, до кода)**: [`arch/19-backups.md`](../../../arch/19-backups.md) §4 (таблица ключей: `orphans` c `has_valid_full`, новые `orphan-holds`/`orphan-deletes`; правила «DR-hold»; deprovisioning-абзац) и §10 (риски); панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §2.3.1 (две новые строки таблицы), §2.5 (индикация защиты), §9.10 (мутации hold/unhold/delete).
- **Образцы**: BackupOrphanSweeper/OrphanRegistry (`src/PgWorker.Backups/Supervisor/`, t07); заявка+аудполя — ротация секретов §9.8 adminpanel/02 (`requested_unix`/`requested_by`); confirm-гвард — RestoreShardHandler (`src/PgWorker.App/Api/Operations/RestoreShardHandler.cs`); панель-прокси — `DeleteClusterCommand` (`src/AdminPanel.Api/Operations/`).
- **Решения пользователя (гейты уточнений, зафиксированы)**: (1) механизм — **гибрид**: автоправило «сирота с валидным полным не удаляется автоматикой никогда» + явный hold-флаг оператора; (2) канал управления — **API воркера + кнопки панели** (панель — прокси, в etcd не пишет), явное удаление — confirm-команда; ручной etcdctl-путь — runbook; (3) тесты — **полный контур + docker-E2E кейс** (расширение `E2eSupervisorScenarios`).

## 1. Цель

Исключить потерю единственного DR-источника автоматикой сиротского TTL.
Сегодня: после deprovisioning кластера его S3-префикс `<C>/<X>/` становится
сиротой, глобальный лидер-проход `BackupOrphanSweeper` удаляет её по
`Supervisor:OrphanTtlSec` (7 сут) — DR-восстановление из этого префикса
(runbook backup-restore.md §5, source-override) возможно только «если
успеть». После удаления восстановить кластер нечем: это прямая потеря
данных (характеристика D).

Функциональные результаты:

1. **Автоправило «последнего полного»**: TTL-кандидатом становится только
   сирота БЕЗ валидного полного. Валидный полный = объект
   `full/<id>/backup_manifest` в префиксе (критерий тот же, что у
   DR-выбора restore, arch/19 §3.5/§5) — детект из уже снимаемого list
   bucket, ноль дополнительных запросов к S3. Сирота с валидным полным
   автоматикой не удаляется НИКОГДА — осознанное удаление только явной
   заявкой оператора.
2. **Hold-флаг**: оператор помечает сироту ценной (защита «до разбора»)
   даже без полного — WAL-огрызки, подозрение на ценность, ожидание
   решения. Hold исключает префикс из TTL-отбора; снятие возвращает под
   общие правила (истёкший TTL без иных защит → удаление ближайшим
   проходом).
3. **Явное удаление (заявка)**: confirm-команда оператора
   (`confirm = <C>/<X>`) — единственный путь удалить защищённую сироту;
   минует и hold, и автоправило (осознанная потеря). Исполняет sweeper
   ближайшим проходом (асинхронно, как весь TTL-механизм).
4. **API воркера**: `POST/DELETE /api/backups/orphans/{C}/{X}/hold`,
   `POST /api/backups/orphans/{C}/{X}/delete` — mTLS-грань, гварды
   400/404/409/503, идемпотентность.
5. **Панель**: бейджи защиты («полный: защита» / «hold» / «к удалению»),
   кнопки Hold/Unhold (обратимы, без подтверждения) и Delete
   (confirm-модал «введи `<C>/<X>`») в списке сирот грани «Хранилище
   бэкапов» — через прокси в API воркера; алерт `backup-orphan`
   различает защищённые/незащищённые записи.
6. **Runbook**: §5.1 docs/backup-restore.md переписан под новые правила;
   curl-команды hold/delete и etcdctl-форматы — в §2.
7. **Тесты**: юниты чистых функций, интеграции sweeper (реальный etcd +
   FakeS3, сжатое время), API-интеграции гвардов, панельные юниты,
   docker-E2E кейс «DR-источник выжил после истёкшего TTL» в
   `E2eSupervisorScenarios`.

## 2. Принципы

1. **Arch-first**: контракт в `arch/19` §4/§10 и `adminpanel/02`
   §2.3.1/§2.5/§9.10 обновлён ДО кода (выполнено этим коммитом спеки в
   worktree); код зеркалит канон. Ключевые формулировки канона:
   DR-hold-правило, форматы ключей, гварды API, приоритет кандидатов
   прохода.
2. **Данные дороже места (R4, arch/14/§19)**: автоматика не удаляет
   потенциально ценное. Плата — защищённые сироты копятся в bucket;
   накопление наблюдаемо (ключ `/pgworker/backups/storage` + квота-алерты
   t06 + реестр + алерт `backup-orphan`), чистка — явная заявка. Новых
   конфиг-опций НЕ появляется: автоправило безусловно (суть задачи),
   OrphanTtlSec сохраняет семантику (TTL незащищённых; `0` — только
   алерт, заявки работают).
3. **Single-writer по ключам**: реестр `orphans` по-прежнему пишет ТОЛЬКО
   глобальный лидер-проход; hold/deletes — ОТДЕЛЬНЫЕ per-префиксные ключи,
   которые пишет API воркера атомарными put/del одного ключа (без
   read-modify-write) — гонок между инстансами API и лидер-проходом нет.
   Дублирование состояния исключено: hold живёт только в hold-ключах
   (реестр его не копирует), факт полных — только в `has_valid_full`
   записи реестра (наблюдение sweeper'а).
4. **Паттерны arch/17**: идемпотентность каждого шага (повторные hold,
   заявки, доводки безопасны), journal-before-manipulations (DELETING
   пишется до batch-delete), transient vs permanent (сбой S3/etcd —
   Result.Failed без мутаций, следующий проход повторяет).
5. **Осознанность потери**: удаление защищённой сироты — только confirm
   (мисматч → 400), UI требует ввода префикса. Защита от «нажал не то» —
   как у restore (необратимость проговорена).
6. **Санитар ключей — sweeper**: hold/заявки, чей префикс перестал быть
   сиротой (владелец воскрес / префикс исчез), гасятся ближайшим проходом
   — «висящих» флагов не копится; заявка на воскресшую сироту НЕ
   исполняется (удаление живого исключено).

## 3. Структура и компоненты

### 3.1. Контракт etcd (канон arch/19 §4; отражение — код)

| Ключ | Значение | Писатель |
|---|---|---|
| `/pgworker/backups/orphans` | запись сироты получает `"has_valid_full":bool` — пересчёт каждым проходом из list bucket (`full/<id>/backup_manifest` в префиксе); ненаблюдаемый сирота (transient list) сохраняет прежнее значение — защита переживает моргание list. Остальной формат неизменен | лидер-проход (sweeper) |
| `/pgworker/backups/orphan-holds/<C>/<X>` | `{"set_unix":T,"set_by":"operator\|panel"}`; hold-флаг сироты | API воркера (put/del, атомарно) |
| `/pgworker/backups/orphan-deletes/<C>/<X>` | `{"requested_unix":T,"requested_by":"operator\|panel"}`; заявка явного удаления | API воркера (put; del — sweeper по исполнении/погашении) |

Правила TTL-отбора (чистая функция, см. §3.2): OBSERVED старше
`OrphanTtlSec` становится кандидатом ТОЛЬКО при `has_valid_full=false` И
отсутствии живого hold-ключа. Приоритет за проход: (1) доводка первого
DELETING (безусловно, как сегодня); (2) первая заявка `orphan-deletes`
(обходит hold и автоправило — осознанная команда); (3) TTL-кандидат. Один
удаляемый префикс за проход — прежний темп.

Санитар прохода: после merge реестра — hold-ключи и заявки, чей префикс
отсутствует в merged-реестре (воскрес/исчез/уже удалён), удаляются.
`OrphanTtlSec=0`: авто-удаление выключено (только доводка DELETING и
заявки), прежняя семантика для алерта сохраняется.

Backwards-compat: JSON записи без `has_valid_full` парсится (поле →
`false`, ближайший проход sweeper'а пересчитает и перепишет; один лишний
put на запись после апгрейда — шум, не сбой).

### 3.2. Чистые функции — `OrphanRegistry` (`src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`)

- `OrphanEntry` + `HasValidFull` (сериализация `has_valid_full`;
  Parse-толерантность к отсутствию — `false`).
- Группировка list-объектов: расширение `GroupShardPrefixes` (или
  парная функция) — тем же проходом по объектам собирает (а) суммы размеров
  по префиксам (как сегодня), (б) множество префиксов, где встретился ключ
  вида `<C>/<X>/full/<id>/backup_manifest` (5 сегментов, 3-й — `full`,
  последний — `backup_manifest`).
- `Merge`: `has_valid_full` — из свежего наблюдения для наблюдаемых,
  перенос прежнего для ненаблюдаемых (консервативно — защита не
  теряется на transient list).
- `SelectTtlCandidate(registry, ttlSec, nowUnix, heldPrefixes)`:
  DELETING-доводка → OBSERVED с истёкшим TTL И `!has_valid_full` И не в
  `heldPrefixes`; `ttl<=0` → только доводка (как сегодня).
- `SameOrphans`: сравнивает и `HasValidFull` (put только при изменении).

### 3.3. Sweeper — `BackupOrphanSweeper.SweepAsync` (`src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs`)

Шаги прохода (нумерация продолжает существующие комментарии):

1. (1–2) list bucket + группировка (теперь с детектом полных), владельцы
   из `/clusters/` — без изменений, кроме выхода группировки.
2. (3) merge реестра (+`has_valid_full`), put при изменении.
3. (3.5) **санитар**: range `/pgworker/backups/orphan-holds/` и
   `/pgworker/backups/orphan-deletes/` → ключи с префиксом вне
   merged-реестра → del (журнал-факт `orphan-key-cleaned/<prefix>`).
4. (4) **отбор**: первый DELETING (доводка) → первая заявка delete по
   порядку → `SelectTtlCandidate` с hold-множеством. Кандидат из заявки
   журналируется `orphan-delete-requested/<prefix>` до перехода в
   DELETING.
5. (5–7) доводка удаления прежняя (DELETING → batch-delete →
   list-подтверждение → del записи); при удалении по заявке — в финале
   дополнительно del ключа заявки.

Воскресение владельца: запись гасится merge'ем (как сегодня), hold и
заявка того же префикса — санитаром этого же прохода. Гварды прежние:
только лидер, `Enabled=false` → no-op (заявки копятся, исполнятся при
включении).

### 3.4. API воркера (`src/PgWorker.App/Api/ApiModule.cs` + `Operations/`)

Новые эндпоинты (mTLS-грань; `requested_by` — заголовок, как restore):

| Метод и путь | Поведение | Коды |
|---|---|---|
| `POST /api/backups/orphans/{cluster}/{shard}/hold` | put hold-ключа `{"set_unix":now,"set_by":<requested_by\|operator>}` | 204; 404 не-сирота; 409 DELETING; 503 etcd/`Enabled=false` |
| `DELETE /api/backups/orphans/{cluster}/{shard}/hold` | del hold-ключа | 204 (идемпотентен, ключа нет — тоже 204); 503 |
| `POST /api/backups/orphans/{cluster}/{shard}/delete` | body `{"confirm":"<C>/<X>"}`; put заявки `{"requested_unix":now,"requested_by":…}` | 202 `{prefix,requestedUnix,requestedBy}`; 400 мисматч/нет тела; 404 не-сирота; 409 DELETING; 503 etcd/`Enabled=false` |

Гварды читают реестр напрямую (get `/pgworker/backups/orphans`, разбор
`OrphanRegistry.Parse`): префикса нет в записях → 404; state=DELETING →
409 («доводку начатого удаления не спасти»). Имена `{cluster}/{shard}` —
в форме regex имён (иначе 404 без похода в etcd). Повторная заявка —
put поверх (не 409: заявка — состояние, не эксклюзивный клэйм; §9.10
adminpanel/02).

### 3.5. Панель

- **Снапшот** (`SnapshotBuilder`/`BackupsParser`): чтение
  `/pgworker/backups/orphan-holds/` и `/pgworker/backups/orphan-deletes/`
  → `BackupOrphansInfo` расширяется `Holds`/`DeleteRequests` (мапы
  prefix → {unix, by}); битый JSON — толерантный пропуск (образец
  остальных парсеров).
- **DTO грани** (`BackupStorageQuery`): `BackupOrphanDto` + `Held`,
  `HeldUnix?`, `HeldBy?`, `DeleteRequested` (джойн реестра, hold-ключей,
  заявок и S3-сверки — в `MergeOrphans`); `TtlLeftSec` — только у
  незащищённых (у защищённых null).
- **Алерт `backup-orphan`** (`BackupOrphanRule`): для OBSERVED+hold —
  «защищён hold-флагом (<by>) — удаление только явной командой»;
  OBSERVED+`has_valid_full` — «защищён автоправилом (есть валидный
  полный) — удаление только явной командой» (обе — Remedy
  OperatorRunbook: «удали заявкой delete, если данные не нужны»);
  незащищённые — прежний остаток TTL; DELETING — прежнее «идёт удаление»
  (включая по заявке оператора). Severity — warning для всех (сирота =
  разбор оператора, срочности нет: защита держит).
- **Команды** (`AdminPanel.Api/Operations/OrphansCommands.cs`, образец
  `DeleteClusterCommand`): `HoldOrphanCommand`/`UnholdOrphanCommand`/
  `DeleteOrphanCommand(prefix, confirm, requestedBy)` — прокси
  `WorkerProxy.SendAsync` в живой инстанс API PgWorker.
- **Frontend**: список сирот грани «Хранилище бэкапов» — бейджи «полный:
  защита» / «hold» / «к удалению», остаток TTL у незащищённых; кнопки
  Hold/Unhold (обратимы — без модала, спиннер до 204) и Delete
  (confirm-модал «введи `<C>/<X>`», предупреждение о необратимости —
  образец confirm-модалей restore/удаления кластера).

### 3.6. Runbook — `docs/backup-restore.md`

§5.1 переписать: DR-источник с валидным полным больше не «успеть до TTL» —
автоправило держит бессрочно; TTL чистит только незащищённые; hold-команды
(«защитить до разбора»), delete-заявка («осознанно удалить»), etcdctl-форматы
ключей (ручной путь без API). §2 дополнить curl-командами. §5 (сценарий DR)
— заменить фразу «успеть до истечения TTL».

## 4. Фазы

1. **Чистые функции** (§3.1–3.2): `OrphanEntry.HasValidFull` +
   сериализация/парсинг, группировка с детектом полных, merge-перенос,
   `SelectTtlCandidate` с hold-множеством; юниты `OrphanRegistryTests`
   (TDD: детект полных из ключей, перенос при ненаблюдаемости, исключение
   защищённых из кандидатов, ttl=0, backcompat-парсинг).
2. **Sweeper** (§3.3): санитар hold/заявок, приоритет кандидатов, del
   заявки в доводке; интеграции `BackupOrphanSweeperTests` (реальный etcd
   + FakeS3, сжатый TTL): полный держит после истёкшего TTL, hold держит,
   unhold → удаление следующим проходом, заявка удаляет защищённую и
   гасится, воскресение гасит hold/заявку, санитар чистит чужие ключи.
3. **API воркера** (§3.4): эндпоинты + гварды; API-интеграции
   (204/202/400/404/409/503, идемпотентность, `Enabled=false`).
4. **Панель** (§3.5): парсеры снапшота → DTO/алерт → команды-прокси →
   UI-кнопки; панельные юниты (`BackupsParserTests`, `BackupOrphanRuleTests`,
   `BackupStorageQueryTests`, мапперы) + интеграции
   (`BackupsStorageApiTests`, командные по образцам).
5. **Runbook + E2E + гейт** (§3.6): runbook; docker-E2E кейс в
   `E2eSupervisorScenarios` (реальный MinIO, OrphanTtlSec=120): сирота с
   объектом `full/<id>/backup_manifest` переживает истёкший TTL, заявка
   delete чистит; мерж-гейт — кейс-маркер
   `Scale_AddEmptyShard` на свежем Release (AGENTS.md), снятие тега
   roadmap-строки + статус в `reliability-report.md` мерж-коммитом.

## 5. Ограничения

- Реестр `orphans` пишет только лидер-проход; API не трогает реестр
  (только свои per-префиксные ключи) — single-writer сохранён.
- Новых опций конфигурации нет (валидация `Options.cs` не меняется);
  автоправило не отключается.
- Механика restore/DR-выбора полных не меняется (критерий manifest —
  разделяем, не чиним).
- Ретенция t06 и per-cluster-мусор супервизора не затрагиваются (живые
  шарды вне скоупа).
- Панель остаётся read-only к S3 и «не пишет в etcd» (прокси-паттерн).
- E2E/интеграции — по канонам AGENTS.base §11: своё окружение с guid,
  динамические порты, полный teardown, ассерт чистоты; `BrokerBootSec`
  ≤ 100 с; зачистка контейнеров/сетей после каждой серии.
- Код — .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`.

## 6. Критерии приёмки

- **AC1 (автоправило)**: сирота с объектом `full/<id>/backup_manifest` и
  истёкшим TTL НЕ удаляется — объекты живы, запись OBSERVED с
  `has_valid_full=true` (интеграция + E2E).
- **AC2 (hold)**: сирота без полных с живым hold-ключом НЕ удаляется по
  истёкшему TTL; алерт/панель показывают «hold».
- **AC3 (unhold)**: снятие hold у незащищённой просроченной сироты →
  удаление ближайшим проходом sweeper'а.
- **AC4 (заявка)**: delete-заявка (confirm-гвард) удаляет ЛЮБУЮ сироту,
  включая защищённую полным/hold, ближайшим проходом; заявка и запись
  погашены; journal-фазы `orphan-delete-requested`/`orphan-deleting`/
  `orphan-deleted` в журнале.
- **AC5 (воскресение)**: владелец префикса воскрес → запись сироты, hold и
  заявка гаснут; удаление живого префикса не происходит.
- **AC6 (гварды API)**: 400 (confirm-мисматч/нет тела), 404 (не сирота,
  мусорные имена), 409 (DELETING), 503 (etcd-сбой, `Enabled=false`);
  повторные hold/заявки идемпотентны.
- **AC7 (панель)**: DTO несёт `Held`/`DeleteRequested`/защиту;
  `TtlLeftSec` только у незащищённых; кнопки проксируют в API воркера
  (204/202 проксируются, ошибки — ProblemDetails); алерт различает четыре
  состояния (hold / автозащита / TTL-остаток / DELETING).
- **AC8 (runbook)**: §5.1/§2/§5 обновлены (curl-команды, etcdctl-форматы,
  новая судьба DR-источника); противоречий со старым текстом нет.
- **AC9 (backcompat)**: реестр, записанный старым кодом (без
  `has_valid_full`), парсится и переживает апгрейд без потери записей.
- **AC10 (мерж-гейт)**: юниты/интеграции/E2E зелёные на свежем Release
  (кейс-маркер `Scale_AddEmptyShard` + новый сирот-кейс); тег
  `t04-orphan-dr-hold` снят из `arch/roadmap/reliability.md` и статус
  строки в `reliability-report.md` обновлен тем же мерж-коммитом.
