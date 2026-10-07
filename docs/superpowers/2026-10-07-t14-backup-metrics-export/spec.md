# t14-backup-metrics-export — экспорт бэкапных статусов в Prometheus

**Фаза 1 dev-flow (spec).** Roadmap: `arch/roadmap/reliability.md` (тег
`t14-backup-metrics-export`, P3 — наблюдаемость, цель N; отчёт —
`arch/roadmap/reliability-report.md`). Связанный пункт `t17-rpo-rto-dashboard`
зависит от этой задачи (`← t14-backup-metrics-export`).

## 1. Цель

Бэкапные статусы PgWorker становятся рядами Prometheus: возраст последнего
валидного полного, WAL-лаг и тишина загрузок, исходы verify/restore/drill —
сегодня эти факты видны только панельным правилам (AlertEngine поверх
etcd-снапшота), которые невидимы при падении самой панели, и словарь метрик
`arch/18 §2.2` бэкапных серий не включает. Результат: независимый от панели
канал наблюдения восстановимости (Prometheus-алерты + Grafana-дашборд) и
источник чисел для `t17-rpo-rto-dashboard`.

Текущее несоответствие, закрываемое задачей: две бэкапные серии УЖЕ живут в
коде (`pgworker_backup_wal_lag_segments`, `pgworker_backup_verify_total`), но
отсутствуют в словаре-каноне — словарь наводится в порядок вместе с
расширением.

## 2. Принципы

- **arch-first**: контракт словаря метрик — `arch/18` (обновлён этой задачей
  ДО кода: новый §2.7, правки §5.3/§6). Контракт etcd (`arch/19 §4`) НЕ
  меняется — метрики читают те же факты, что пишутся сегодня.
- **Метрики — пассивные наблюдатели** (`arch/18`): observer-делегаты никогда
  не бросают исключений и не влияют на процессы бэкапов; ошибки
  instrumentation глотаются (паттерн `WorkerMetricsInstrumentation`).
- **Отдельного коллектора нет**: серии питаются тиками СУЩЕСТВУЮЩИХ процессов
  (`BackupProcess`, `WalStreamProcess`, `BackupVerifyProcess`,
  `RestoreProcess`, `RestoreDrillProcess`) через observer-делегаты — паттерн
  уже заложен `lagObserver`/`verifyObserver`. Никаких новых фоновых циклов и
  обращений к S3/etcd сверх уже выполняемых тиками.
- **Семантика «валидный полный» едина**: `IsValid` планировщика
  (`BackupPlanner`, arch/19 §2) = COMPLETED и `verify ≠ FAILED` — та же
  семантика у панельного правила `backup-full-stale` и у freshness-логики
  пересъёма. Метрика не вводит третьего толкования.
- **Панельные правила остаются** (etcd — источник истины статусов);
  Prometheus-группа — зеркало для канала, независимого от панели, а не замена.
- **Лейблы конечны** (`arch/18 §2`, риск M1): только `cluster`, `shard`,
  `result` из фиксированных словарей — никакие свободные строки (имена
  бэкапов, тексты ошибок) в лейблы не попадают.

## 3. Структура/компоненты

### 3.1. Словарь серий (канон — arch/18 §2.7, приведён сюда для обзора)

| Имя | Тип | Лейблы | Источник |
|---|---|---|---|
| `pgworker_backup_full_age_seconds` | gauge | cluster, shard | BackupProcess: `now − finished_unix` последнего валидного COMPLETED-полного; нет валидного — серия не эмитится |
| `pgworker_backup_full_max_age_seconds` | gauge | cluster, shard | BackupProcess: `full_max_age_sec` эффективной политики кластера (policy-ключ ?? дефолт `PgWorker:Backups:Policy`); эмитится для каждого бэкапимого шарда тика |
| `pgworker_backup_wal_lag_segments` | gauge | cluster, shard | WalStreamProcess, контрольный проход (существует, канонизируется) |
| `pgworker_backup_wal_last_uploaded_age_seconds` | gauge | cluster, shard | WalStreamProcess: `now − last_uploaded_unix` контрольного прохода |
| `pgworker_backup_verify_total` | counter | cluster, shard, result∈{ok,failed,transient} | BackupVerifyProcess (существует; лейблы cluster/shard добавляются — миграция серии) |
| `pgworker_backup_restore_total` | counter | cluster, shard, result∈{ok,failed} | RestoreProcess: фиксация терминального статуса (COMPLETED→ok, FAILED→failed) |
| `pgworker_backup_drill_total` | counter | cluster, shard, result∈{ok,failed} | RestoreDrillProcess: чистый терминальный итог (SUCCEEDED→ok, FAILED→failed), включая FAILED-валидацию без запуска джоба |

Форма возрастных серий — gauge-возраст (паттерн `worker_snapshot_age_seconds`:
значение растёт при каждом scrape, готовое число для дашборда) + pair
`max_age` для честного per-cluster порога алерта без хардкода в правиле.

### 3.2. Кодовые компоненты

- **`src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`** — новые
  марк-методы и ObservableGauge/Counter-инструменты:
  - `BackupFullAge(cluster, shards)` — ЕДИНЫЙ марк-метод на тик кластера:
    набор `(shard → age|null, maxAge)` замещает стейт кластера целиком
    (age null → age-серия шарда отсутствует, max_age пишется всегда; шард,
    не пришедший в наборе, — серии кластера больше не эмитит);
  - `BackupWalUploadedAge(cluster, shard, long? ageSec)` — null удаляет серию
    (симметрия `BackupWalLag`);
  - `BackupRestore(cluster, shard, result)` / `BackupDrill(cluster, shard, result)`;
  - миграция `BackupVerify` — counter получает лейблы `cluster`/`shard`
    (делегат их уже получает; меняется только создание инструмента).
  - Набор серий кластера перезатирается каждым тиком (паттерн `UpdateCluster`
    arch/18 §4.2): full-серии — замещением набора шардов тика (см.
    `BackupFullAge` выше), WAL-серии — null-удалением per-shard; ушедшие
    шарды/кластеры стейт не копят.
- **`src/PgWorker.Backups/Process/BackupProcess.cs`** — observer-делегат
  (nullable, по образцу `verifyObserver`): тик кластера передаёт полный набор
  `(shard → age|null, maxAge)`; wiring в `src/PgWorker.App/Program.cs`.- **`src/PgWorker.Backups/WalStreamProcess.cs`** — второй nullable-делегат
  `uploadedAgeObserver(cluster, shard, long? ageSec)` рядом с существующим
  `lagObserver`; оба живут в контрольном проходе, одинаковая null-семантика
  (wal-ключ STOPPED/BROKEN/удалён — серии исчезают).
- **`src/PgWorker.Backups/Process/RestoreProcess.cs`**,
  **`RestoreDrillProcess.cs`** — nullable-делегаты терминального исхода;
  вызов в точке записи терминального etcd-статуса (restore: COMPLETED/FAILED;
  drill: снятие `phase=cleaning` — единственная точка чистого итога — плюс
  путь «FAILED-валидация без джоба», который чистый итог пишет сразу).
- **`dev-stand/adminpanel/metrics/prometheus/rules.yml`** — новая группа
  `backups` (см. §3.3).
- **`dev-stand/adminpanel/metrics/grafana/dashboards/backups.json`** — новый
  дашборд (см. §3.4); provisioning общий, отдельной правки не требует.
- **`dev-stand/adminpanel/checks/65-metrics.sh`** — счётчик alert-рулов
  обновляется под группу `backups` (сейчас `>= 11`).

### 3.3. Prometheus-алерты (группа `backups`, severity — зеркало панельных правил)

| Alert | Expr (сокращённо) | Severity |
|---|---|---|
| `BackupFullStale` | `pgworker_backup_full_age_seconds > pgworker_backup_full_max_age_seconds` | critical |
| `BackupFullMissing` | `pgworker_backup_full_max_age_seconds unless pgworker_backup_full_age_seconds` (max_age есть, age нет — валидного полного нет вовсе) | critical |
| `BackupWalLagHigh` | `pgworker_backup_wal_lag_segments > 1024` (дефолт `Wal:LagMaxSegments`, per-install конфиг; комментарий в правиле) | warning |
| `BackupWalStale` | `pgworker_backup_wal_last_uploaded_age_seconds > 300` (дефолт `Wal:StaleSec`) | warning |
| `BackupVerifyFailed` | `increase(pgworker_backup_verify_total{result="failed"}[15m]) > 0` | critical |
| `BackupRestoreFailed` | `increase(pgworker_backup_restore_total{result="failed"}[15m]) > 0` | critical |
| `BackupDrillFailed` | `increase(pgworker_backup_drill_total{result="failed"}[15m]) > 0` | critical |

Окно `for` — по образцу существующих правил (stale-алерты `0m`, счётчики
через `increase[15m]` при интервале scrape 15 с). Пороги WAL — статические
дефолты конфига per-install (per-cluster политики WAL в etcd нет — arch/19
§4), у полных — честный per-cluster порог из серии `max_age`.

### 3.4. Grafana-дашборд `backups.json`

Панели (по образцу workers.json/pg.json, datasource-переменная Prometheus):
«Full backup age, s (by cluster/shard)» (вторым target — max_age как пороговая
линия), «WAL lag, segments», «WAL uploaded age, s», «Verify outcomes
(increase 15m, by result)», «Restore outcomes», «Drill outcomes». Разрезы —
лейблы `cluster`/`shard`.

### 3.5. Отражение в arch (сделано ДО кода, этой же задачей)

- `arch/18-metrics.md`: §2.7 «Бэкап-домен PgWorker» (таблица серий,
  семантика тиков/затирания/null, перечень алертов группы `backups`);
  §5.3 — `dashboards/backups.json`; §6 — счётчик рулов в E2E-чеке.
- `arch/19-backups.md` — не меняется: контракт etcd и механика бэкапов
  затронуты только чтением уже пишущихся фактов.

## 4. Фазы

1. **Архитектура (arch-first)** — §3.5 (выполнено в рамках spec-фазы; правки
   arch/18 уже в ветке).
2. **Instrumentation** — марк-методы/инструменты `WorkerMetricsInstrumentation`
   (§3.1–3.2), юнит-семантика (null/затирание/лейблы) — TDD.
3. **Питание серий** — observer-делегаты четырёх процессов + wiring
   `Program.cs`; юнит-тесты процессов расширяются проверками вызовов
   делегатов (исходы, null-семантика, затирание набора шардов).
4. **Стек мониторинга** — группа `backups` в rules.yml, дашборд
   `backups.json`, счётчик в `checks/65-metrics.sh`.
5. **Интеграционная фиксация словаря** — `MetricsTests`: канонические имена
   §2.7 присутствуют в `/metrics` (экспортированные имена против словаря —
   канон-тест arch/18 §6, риск M3).

## 5. Ограничения

- Вне скоупа: экспорт RPO/RTO-агрегатов и панель t17 (живёт на etcd-снапшоте,
  не на Prometheus); метрики Patroni-нод (§2.5 — scrape напрямую); WAL-пороги
  per-cluster (не существуют в политике); возраст последнего успешного дрилла
  (панельное `backup-drill-stale` достаточно; counter даёт исходы, timestamp
  возраста дрилла — YAGNI до запроса t17); алерты `backup-deleting-stuck`,
  `backup-storage-quota`, `backup-orphan`, `backup-chain-broken`,
  `wal-stream-stopped`, `backup-s3-unreachable` — остаются панельными
  (композиция нескольких ключей/глобальных реестров в одном PromQL-правиле
  не выражается без новых серий; вводить серии под них — вне roadmap-объёма).
- Миграция `pgworker_backup_verify_total` (добавление лейблов) меняет
  identity серии: допустимо, стенд мониторинга — dev-контур; канон
  фиксируется этой задачей.
- Никаких новых конфиг-опций: серии питаются существующими тиками, интервалы
  обновления серий наследуют интервалы тиков процессов (BackupProcess — тик
  ReconcileLoop; WAL — `Wal:VerifyIntervalSec`).
- Значения счётчиков не сбрасываются при рестарте воркера (OTel-процессные
  counters): `increase()` устойчив к сбросу — канон §2.6 (gauge-кумулятивы
  Valkey) здесь не нужен, counters соответствуют семантике исходов.

## 6. Критерии приёмки

1. Словарь arch/18 §2.7 содержит все 7 серий; `MetricsTests` фиксирует
   фактические экспортированные имена против словаря (все серии, включая
   лейблы verify/restore/drill).
2. Юнит-тесты: age null → серия отсутствует, max_age пишется; затирание
   набора шардов тика (ушедший шард не копится); null-семантика WAL-серий;
   счётчики по исходам; verify-серии с лейблами cluster/shard.
3. Процессы зовут делегаты в точках терминальных исходов (restore
   COMPLETED/FAILED, drill SUCCEEDED/FAILED включая провал валидации без
   джоба, verify ok/failed/transient) — юнит-проверки вызовов.
4. rules.yml: группа `backups` из 7 правил загружается (валидация
   Prometheus); `checks/65-metrics.sh` зелёный с обновлённым счётчиком
   (18 алертов: 11 существующих + 7 новых).
5. Дашборд `backups.json` загружается Grafana provisioning (чек 65) и
   показывает ряды при живом стенде с включёнными бэкапами.
6. Мерж-гейт: сборка 0 warnings; юниты + интеграция PgWorker зелёные; задача
   трогает `src/PgWorker.App` → E2E-кейс-маркер
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx
   -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` на свежем
   Release зелёный.
7. Roadmap-гейт трека reliability: мерж-коммит снимает тег
   `t14-backup-metrics-export` из `arch/roadmap/reliability.md` (вкл.
   `←`-зависимость t17) и обновляет строку t14 в
   `arch/roadmap/reliability-report.md` тем же коммитом.

## 7. Риски и решения

| Риск | Решение |
|---|---|
| Кардинальность cluster×shard растёт (M1) | лейблы конечны, шарды — реальные сущности; затирание набора тиком не даёт копиться ушедшим |
| Серии замирают при потере клэйма (никто не тикает) — панель может показать устаревший age | то же семантика, что у существующих lag-серий и коллекторов §4: свежесть scrape-грани контролирует `ServiceDown`/`WorkerLoopStalled`; при возврате клэйма тик перезатирает |
| Gauge-age между тиками замирает и занижает возраст | значение пересчитывается в колбэке ObservableGauge на каждом scrape (`now − finished_unix`) — паттерн `worker_snapshot_age_seconds` |
| Статические WAL-пороги в правиле расходятся с конфигом воркера | пороги per-install идентичны дефолтам конфига; комментарий в правиле; per-cluster WAL-политики не существует (arch/19 §4) |
| Дублирование алертов панель + Prometheus | осознанно: каналы независимы (панель может упасть — цель N трека); Prometheus-группа — зеркало подмножества правил |
| Изменение лейблов verify-серии ломает существующие запросы | серий в rules/дашбордах до t14 нет; словарь фиксируется этой задачей |
