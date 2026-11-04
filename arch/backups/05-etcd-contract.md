# Бэкапы: контракт etcd `/pgworker/backups/*`

Уровень 3. Ключи домена бэкапов, их форматы и правила. Пишет PgWorker под
клэймом `<C>` (операции бэкапов — под тем же клэймом, что остальные процессы
кластера) и приёмники WAL — каждый ТОЛЬКО свой статус-ключ
`/pgworker/backups/<C>/<X>/agent/<N>` (воркер статусы приёмников только
читает); панель читает (контекст —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1;
UI/алерты — [adminpanel/03-panels.md](../adminpanel/03-panels.md)). Процессы,
пишущие ключи, — [processes/](processes/); состояния —
[processes/enums.md](processes/enums.md).

## 1. Ключи кластера и шарда

| Ключ | Значение |
|---|---|
| `/pgworker/backups/<C>/policy` | per-cluster политика: `{"retention":{"days":1,"weeks":1,"months":1},"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":604800},"drill":{"interval_days":N}}` (`drill.interval_days` — период дрилов шардов кластера в сутках, `N=0` — дрилл кластера выключен; поле отсутствует → дефолт конфига `PgWorker:Backups:Drill:IntervalDays`); `garbage_min_age_sec` — возрастной гейт мусора супервизора: минимальный возраст объекта S3 `full/<id>.tar` (и висячих multipart-сессий), прежде чем объект без etcd-ключа сочтётся удаляемым мусором (защита заливающегося полного от устаревшего снапшота сверяющего прохода; поле отсутствует → дефолт конфига 600 с); секция `"s3":{"pool":"<C пула>","write_master":["<I>",…],"write_replica":["<I>",…]}` (§3); пишет воркер (приём через API); отсутствует → дефолт `PgWorker:Backups:Policy` |
| `/pgworker/backups/<C>/<X>/full/<id>` | статус полного: `{"state":"PLANNED\|RUNNING\|UPLOADING\|COMPLETED\|FAILED\|DELETING","node":"<n>","role":"replica\|master","started_unix","finished_unix"?,"wal_start_segment"?,"size_bytes"?,"sha256"?,"progress"?:{"bytes":<N>,"parts":<N>},"chain_life":<N>?,"error"?,"verify":{"state":"PENDING\|OK\|FAILED","checked_unix"?,"error"?}}`; набор хранится одним объектом `full/<id>.tar` ([06-s3-layout.md](06-s3-layout.md) §1); `wal_start_segment` заполняется с фазы UPLOADING (из `backup_label` tar-потока; для рано упавших FAILED может отсутствовать — до этого неизвестен); `size_bytes` — размер tar-объекта; `sha256` — сквозной хэш tar-объекта (считается при заливке; сверяется потоковым verify); `progress` — ход заливки (прочитано байт/залито частей), публикуется в фазах RUNNING/UPLOADING, в терминальных состояниях отсутствует; `chain_life` — номер жизни цепочки шарда на момент создания записи (воркер читает `wal-life` при создании `full/<id>`; отсутствие счётчика → 0): манифест полного хранит номер своей жизни — источник номера для restore-заявок (полный обязан сохранять номер жизни); `verify` — результат проверки ([pgnode/05-verify.md](../pgnode/05-verify.md)): PENDING в т.ч. «идёт», `error` — причина невалидности (checksums) |
| `/pgworker/backups/<C>/<X>/wal` | состояние WAL-потока шарда: `{"state":"ACTIVE\|DEGRADED\|STOPPED\|WAL_CHAIN_ERROR","slot":"<slot>","master_node","chain_start_segment","last_received_segment","last_uploaded_segment","last_uploaded_unix","lag_segments"?,"error"?,"agents"?":[{"node":"<n>","state":"running\|exited\|absent"}],"gap"?}`; `agents` — факты приёмников по ВСЕМ нодам шарда с приёмником (приёмник живёт в контейнере каждой ноды; состав шире нод-источников двойной архивации — наблюдаемость: async-реплика с живым приёмником видна как `running`, не-стримящие ноды — в тех же фактах): `running` = живой статус-ключ приёмника ноды (lease), `absent` = ключа нет (приёмник не жив/нода без приёмника), `exited` — legacy-значение, новыми записями не пишется (парсеры панели толерантны); поле опционально; `gap` — опциональное поле `{"from":"<seg>","to":"<seg>","tli":<N>}` — границы RPO-зазора tli-прыжка приёмника (58P01); пишет контроль по факту `tli_jump` статус-ключа мастер-приёмника, снимает покрытием — COMPLETED-полный с `wal_start_segment` ≥ `to` (сравнение имён), либо исчезновением профиля зазора из листинга — ретенция срезала нижележащие TLI; `error` при живом `gap` несёт префикс `tli-gap:`; парсеры панели читают известные поля, старые ключи без `gap` — без ошибок |
| `/pgworker/backups/<C>/<X>/wal-life` | номер жизни цепочки WAL шарда (различает последовательные жизни WAL-потока в одном S3-префиксе): `{"chain_life":<N>,"bumped_unix":<unix>,"reason":"shard-rebuilt"\|"restore"\|"foreign-chain"}`; отсутствие ключа = жизнь 0 (первая жизнь — запись не создаётся); пишет ТОЛЬКО воркер под клэймом `<C>` (единственный писатель счётчика; приёмник номер только читает — конфигурация сессии, номер кладётся в имя каждого куска и кумулятива, [pgnode/03-wal-receiver.md](../pgnode/03-wal-receiver.md) §2). Инкремент — ТОЛЬКО при разрыве цепочки, ровно одной из причин: (а) `shard-rebuilt` — AddShard на позиции с существующим счётчиком (пересоздание шарда поверх непустого бэкап-контура; счётчик переживает remove-shard — тот ключи бэкапов не удаляет, чистит только D2 deprovisioning); (б) `restore` — RestoreProcess при переходе заявки в фазу `demolished` (все ноды снесены — старая жизнь гарантированно закончилась ДО возможного старта стрима восстановленных нод; покрывает restore/DR в тот же префикс кластера и восстановление разрушенного шарда); (в) `foreign-chain` — по свежему факту «чужая цепочка» гварда приёмника (`failed_permanent` «foreign WAL chain» статус-ключа — гвард фиксирует смену жизни): инкремент только если текущий `reason` счётчика ≠ `foreign-chain` (повторные детекты той же лежащей чужой цепочки счётчик не инкрементируют — рекурсии нет; следующий foreign-chain-инкремент возможен лишь после инкремента иной причины). Инкремент идемпотентен на заявку/факт — повторные тики того же процесса/детекта не считают: AddShard/Restore маркерят bump временем (`bumped_unix` ≥ старта заявки — bump этой заявки уже сделан), foreign-chain — reason-гейтом выше. №→№+1, понижения/сброса нет, больший номер = более поздняя жизнь. Читают: приёмник (сессия), контроль цепи и wal-sweeper (фильтр листинга по текущему номеру — куски чужих жизней не рассматриваются вовсе и чистятся сразу, [processes/b-wal-control.md](processes/b-wal-control.md) §1, [processes/d-wal-sweeper.md](processes/d-wal-sweeper.md)), restore-заявки (номер выбранного полного, ниже) и панель (алерт смены жизни — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1) |
| `/pgworker/backups/<C>/<X>/agent/<N>` | статус приёмника WAL ноды `<N>`: lease TTL 15 с (ключ жив = приёмник жив), пишет САМ приёмник; `{"node":"<N>","role":"master\|sync\|none","state":"starting\|waiting_role\|waiting_password\|waiting_policy\|waiting_slot\|waiting_disabled\|streaming\|paused\|failed_permanent","last_delivered_segment"?,"last_delivered_unix"?,"pool_exhausted":bool,"tli_jump"?,"history_missing_tli"?,"error"?,"updated_unix"}`; `history_missing_tli` — TLI-число факта «`.history` перехода не добыта ни из рантайма репликации, ни из ФС ноды» (диагностика PITR-защищённости перехода, воркер ничего не докладывает); `waiting_*` — ожидание условий (роль/креды/policy/слот/стоп-ключ), `paused` — стоп-семантика приёмника (wal-ключ STOPPED/WAL_CHAIN_ERROR / `/pgworker/backups-disabled`), `failed_permanent` — несъёмное условие (segment-size mismatch, разрыв LSN протокола, конфликт содержимого `wal/<segment>` — чужая цепочка (чужая жизнь) в префиксе), причина в `error`; воркер читает для agents-фактов, маркеров `s3-pool-exhausted`/`tli_jump` и диагностики (метрики работы приёмника — Prometheus, [metrics/README.md](../metrics/02-dictionary.md) §8); панель читает ключ в снапшот (наблюдаемость «Приёмники WAL»: `WalAgentInfo`/`agents[]` — adminpanel/02 §2.3.1/§3; битый JSON — parseError-запись, толерантно) |
| `/pgworker/backups/<C>/<X>/restore/<id>` | операция восстановления шарда: `{"state":"PLANNED\|RUNNING\|REJOINING\|COMPLETED\|FAILED","backup_id","source":"<srcC>/<srcX>","target":"latest"\|"time:<RFC3339>","chain_life":<N>?,"node","requested_unix","requested_by","started_unix"?,"finished_unix"?,"phase"?,"phase_unix"?,"restored_to_lsn"?,"error"?}`; `id` — как у полных; максимум один активный (не COMPLETED/FAILED) restore на шард; `chain_life` — номер жизни ВЫБРАННОГО полного (воркер подставляет из `chain_life` full-статуса выбранного при создании заявки; DR-кандидат без etcd-статуса — поля нет): задание restore несёт номер жизни выбранного полного — fetch-wal фильтрует куски `wal/` по этому номеру (отсутствие поля — фильтр не активен, [pgnode/06-restore-drill.md](../pgnode/06-restore-drill.md) §2); переход заявки в фазу `demolished` инкрементирует `wal-life` (причина `restore`); `phase` — фазы тика запуска заявки (`demolished\|launching`) и фазы restore-операции приёмника ноды (`downloading\|recovering`); `phase_unix` — unix-время перехода в текущую фазу (пишет воркер; стенка наката restore считается от перехода в recovering — скачивание набора recovery-бюджет не съедает); по COMPLETED ключ `wal` шарда удаляется (переснятие полного планировщиком) |
| `/pgworker/backups/<C>/<X>/drill` | состояние последнего/текущего дрилла шарда: `{"state":"RUNNING\|SUCCEEDED\|FAILED","id","backup_id","started_unix","finished_unix"?,"phase"?,"restored_to_lsn"?,"error"?}`; `id` — как у полных; `phase` — `downloading\|recovering` (фазы операции приёмника); терминальный ключ без `phase` — чистый итог (scratch-каталог приёмник чистит при любом исходе); ключ перезаписывается каждым новым дриллом (история — фазы журнала `backup-drill/<X>/<id>`); ключ один на шард — «максимум один активный дрилл» следует из формата; пишет воркер (держатель клэйма `<C>`) |

## 2. Глобальные ключи

| Ключ | Значение |
|---|---|
| `/pgworker/backups-disabled` | глобальный ключ-стоп WAL-архивации: пишет/стирает PgWorker: держатель глобального лидера при `Enabled=false` — периодический put `{"updated_unix":T}` на каждом тике лидера, при `Enabled=true` — идемпотентный del в КАЖДОМ тике лидера (stale-ключ от прежнего лидера/режима не выживает takeover/reload); вдобавок любой живой инстанс при старте с `Enabled=true` снимает ключ (del); ключ есть ⇒ приёмники всех кластеров замирают; ключ ГЛОБАЛЬНЫЙ (вне per-cluster префиксов, D2-чистки не касается: появление/удаление ключа не меняет «пустоту» префикса кластера — панельные правила «подсистема не включена» не затрагиваются); смерть всех инстансов воркера при `Enabled=false` ключ оставляет (стрим стоит — конфиг так велит), при `Enabled=true` ключа нет с первого старта инстанса |
| `/pgworker/backups/storage` | занятость хранилища установки: `{"used_bytes":…,"quota_bytes"?,"used_percent"?,"state":"OK\|WARN\|CRIT","updated_unix":…}`; ключ ГЛОБАЛЬНЫЙ (вне per-cluster префиксов, D2-чистки не касается); пишет ретенционный проход — любой живой клэйм пишет одно и то же свежее значение (идемпотентно); `quota_bytes` не задан (0) → пишется только `used_bytes`, `state=OK` |
| `/pgworker/backups/orphans` | реестр осиротевших S3-префиксов (DR-hold): `{"orphans":[{"prefix":"<C>/<X>","kind":"shard\|cluster","size_bytes":N,"first_seen_unix":T,"state":"OBSERVED\|DELETING","has_valid_full":bool}],"updated_unix":T}`; ключ ГЛОБАЛЬНЫЙ; пишет ТОЛЬКО глобальный лидер-проход `/pgworker/leader`. Живость лидер-прохода наблюдаема серией `worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"}` (тик цикла по итогам прохода; алерт OrphanSweepStalled — [metrics/README.md](../metrics/02-dictionary.md) §2). `first_seen_unix` переносится из предыдущей записи (merge — TTL от первого наблюдения); `has_valid_full` пересчитывается каждым проходом из уже снятого list bucket (в префиксе есть объект `full/<id>.tar` — критерий валидного полного, тот же, что у DR-выбора; ненаблюдаемый сирота сохраняет прежнее значение — защита переживает transient list). Владелец воскрес (кластер/шард появился в `/clusters/`) → запись удаляется, идущая DELETING-доводка отменяется, hold-ключ гасится; воскресение проверяется и при merge (по снимку `/clusters/` прохода), и повторно перед удалением объектов; **DR-hold**: TTL-кандидатом становится OBSERVED старше `Supervisor:OrphanTtlSec` ТОЛЬКО при `has_valid_full=false` И без живого hold-ключа — сирота с валидным полным автоматикой НЕ удаляется никогда (последний DR-источник; осознанное удаление — только заявкой `orphan-deletes`); кандидат → DELETING (фаза orphan-journal) → batch-delete префикса → list-подтверждение → del записи; один префикс за проход (приоритет: доводка DELETING → заявка orphan-deletes → TTL-кандидат); `OrphanTtlSec=0` — авто-удаление выключено, только алерт панели (заявки orphan-deletes работают) |
| `/pgworker/backups/orphan-holds/<C>/<X>` | hold-флаг сироты: `{"set_unix":T,"set_by":"operator\|panel"}`; ключ ГЛОБАЛЬНЫЙ per-префиксный (D2-чистки не касается); ставит/снимает API воркера (`POST`/`DELETE /api/backups/orphans/{C}/{X}/hold`, [pgworker/07-api.md](../pgworker/07-api.md) §1) — атомарные put/del одного ключа, без read-modify-write (нет гонок ни между инстансами API, ни с лидер-проходом sweeper'а — реестр тот не пишет); гварды API: префикс — OBSERVED-сирота реестра (нет → 404; DELETING → 409 — доводку уже начатого удаления не спасти); повторный hold идемпотентен (put поверх); снятие hold (unhold) → сирота возвращается под общие правила (TTL уже истёк и защиты нет → удаление ближайшим проходом; unhold — гварды реестра не применяются: идемпотентен, только 503). Sweeper читает при TTL-отборе (hold → не кандидат), повторно перед удалением TTL-кандидата (hold появился после отбора → удаление откладывается до следующего прохода) и гасит ключи несирот (владелец воскрес / префикс исчез) — санитар прохода; DELETING-записи hold не защищают |
| `/pgworker/backups/orphan-deletes/<C>/<X>` | заявка явного удаления сироты: `{"requested_unix":T,"requested_by":"operator\|panel"}`; пишет API воркера по confirm-команде оператора (`POST /api/backups/orphans/{C}/{X}/delete`, body `{"confirm":"<C>/<X>"}` — мисматч → 400; сироты в реестре нет → 404; уже DELETING → 409; повторная заявка идемпотентна — put поверх); исполняет sweeper ближайшим проходом: DELETING (фаза orphan-journal) → batch-delete → del записи реестра + del заявки; заявка гасится и когда сирота к проходу исчезла/воскресла; заявка — осознанная команда оператора: минует и hold, и автоправило «последнего полного» (единственный путь удалить защищённый DR-источник) |
| `/pgworker/backups/orphan-journal` | журнал фаз лидер-прохода свипера сирот: `{"op":"backup-orphan-sweeper","phase":"orphan-…/<C>/<X>","instance":"<id>","updated_unix":T,"last_error"?}` (фазы: orphan-key-cleaned/orphan-cancelled/orphan-delete-requested/orphan-deleting/orphan-deleted — каждая с «/<C>/<X>»); ключ ГЛОБАЛЬНЫЙ; пишет ТОЛЬКО глобальный лидер `/pgworker/leader` (единственный писатель — гонок нет; plain put без read-modify-write); «последняя фаза побеждает» — ключ перезаписывается каждым проходом, чистка не нужна; панель ключ НЕ читает (состояние сирот наблюдаемо через `/pgworker/backups/orphans` + orphan-holds + orphan-deletes) |

## 3. Секция `s3` policy

`"s3":{"pool":"<C пула>","write_master":["<I>",…],"write_replica":["<I>",…]}`
— выбор пула S3 кластера (архитектура пула —
[s3-clusters/README.md](../s3-clusters/README.md)); `pool` — имя кластера
`/s3/clusters/`; `write_master`/`write_replica` — упорядоченные (по
приоритету записи) имена инстансов пула — ССЫЛКИ, не адреса: резолв
`имя → host:port` воркер выполняет чтением `endpoints`-ключа пула и
`/s3worker/portalloc/<C>` ([s3-clusters/README.md](../s3-clusters/README.md));
поля названы по роли ИСТОЧНИКА записи (мастер-потоки/репличные), не по роли
ноды. Пересечение списков и неполный охват нод пула допустимы. Отсутствие
секции — пул не выбран: подсистема бэкапов кластера НЕ пишет и НЕ читает
(алерт `backup-pool-missing` панели); никаких env-дефолтов. Приём секции —
замещение целиком (как drill); сброс — отправкой policy без `s3`.

## 4. Правила

Transient-сбой → статус с `error` + ретраи тиками;
**Failed-резолв пула** — пул удалён/`TO_REMOVE`, имя списка не заявлено в
пуле (`nodes/<I>/placement` нет), пул без читаемых secrets, нода без
portalloc-адреса — transient для процессов: ретраи тиком + journal-фаза
`waiting-backup-pool`, НЕ краш тика; в `Error` wal/full-статусов при этом
ставится тот же маркер `s3-pool-exhausted` — алерт
`backup-pool-unreachable` закрывает и случай «policy записана, но резолв не
удался» (единая алертная грань записи в пул); permanent-отказ → фиксация
причины + алерт. `DELETING` — транзитная фаза ретенции (запрет удалять
последний валидный полный — guard). `WAL_CHAIN_ERROR` — разрыв цепочки в
АРХИВЕ S3 (дыра WalChain / исчезновение слотов всех нод-источников при
живой цепочке — per-instance): НЕ состояние кластера и НЕ свидетельство о
данных нод; приёмники замирают (стоп-семантика приёмника — гейт СТРИМА;
операцию полного НЕ гейтит), лечение — пересъём полного.
`chain_start_segment` в WAL_CHAIN_ERROR-записи — граница разрыва; ratchet —
значение никогда не понижается ([02-wal-archive.md](02-wal-archive.md) §4).

## 5. Backwards-compat дрилла

Панельный парсер толерантен к отсутствию drill-ключей (подсистема не
включена / дрилов не было — правила дрилла молчат) и к policy без поля
`drill` (парсится как «дефолт интервала»); JSON статусов без новых полей
читается без ошибок.

## 6. Ретенция, сироты, deprovisioning

Правила GFS-ретенции, `DELETING`, guard последнего валидного, чистка WAL,
wal-sweeper, гейты мусора супервизора, storage/quota и алерты —
[07-retention.md](07-retention.md). Deprovisioning кластера (D2,
[pgworker/processes/b-deprovisioning.md](../pgworker/processes/b-deprovisioning.md))
чистит etcd-префикс `/pgworker/backups/<C>/` тем же `del --prefix` (идущие
операции бэкапов уходят вместе с нодами шарда — D1 сносит контейнеры нод с
приёмниками; отдельных контейнеров/джобов бэкапов нет); объекты S3 этим
путём НЕ удаляются (данные дороже места; осознанное удаление — только
ретенция по политике): префикс S3 без etcd-владельца попадает в реестр
сирот `/pgworker/backups/orphans` супервизора — алерт панели + удаление по
`Supervisor:OrphanTtlSec` (гибрид: мусор живого шарда — объект
`full/<id>.tar` без ключа — удаляется per-cluster-проходом после гейтов;
исчезнувшие владельцы —
реестр+TTL; DR-hold: TTL чистит только незащищённых — валидный полный или
hold держат префикс до явной заявки `orphan-deletes`); восстановление
удалённого кластера из S3 — runbook backup-restore (воркер не уничтожает
потенциально ценные данные автоматикой).
