# t32-etcdworker — выделенный оркестратор etcd-контура установки (spec, Фаза 1 dev-flow)

Дата: 2026-10-19 (обновлён по решениям: bucket слепков и адресация контура
«peer по именам узлов, мультихост — Swarm» + замена deploy/etcd на seed-рецепт
— Фаза 3 resume). Worktree: `feat-t32-etcdworker-2`. Источник задачи:
`arch/roadmap/reliability.md` (тег `t32-etcdworker`), формулировка
синхронизирована с решениями пользователя: per-install ключ бэкапа etcd —
зона S3Worker (механика t31 без изменений); имя bucket слепков — фиксированная
константа `etcd-backups`; адресация контура — только имена узлов (host-network/
IP-модель удалена). Канон системы —
[`arch/25-etcdworker.md`](../../../arch/25-etcdworker.md) (новый, обновлён
этой фазой arch-first вместе со связанными документами — §8).

## 1. Цель и границы

**Цель**: выделенный воркер **EtcdWorker** — хозяин etcd-контура контроль-плейна
установки:

1. **Поддержание состава etcd-узлов** — декларативное число managed-узлов
   (`/etcd/config {"nodes":N}`), кворум не проседает при любых изменениях
   (learner + promote), авто-замена мёртвого члена (решение пользователя),
   снос осиротевших data-dir при возврате. Адресация контура — ТОЛЬКО имена
   узлов в общей docker-сети: peer-URL managed-узла
   `http://etcdw-node-<k>:2380`, имя docker-хоста в peer-адресации не
   фигурирует (мультихост — Swarm; plain docker — имена резолвятся общей
   сетью; решение пользователя).
2. **DR-слепки etcd в S3** — перенос зоны `Snapshots:Export` у PgWorker:
   слепки по расписанию (360 мин) + детекция изменений (revision etcd,
   MinIntervalSec=300), выгрузка в фиксированный etcd-слепковый bucket
   `etcd-backups` (имя-константа; создаёт S3Worker на нодах etcd-backup
   S3-кластера — признак `etcd_backup=true` в `/s3/clusters/`, arch/23 §2/§5),
   статус-ключ `/etcdworker/snapshots` + алерты панели.
3. **Изъятие бэкапа etcd из PgWorker ПОЛНОСТЬЮ** (решение пользователя):
   выгрузка слепков в S3 удаляется из PgWorker, дублирующей механики не
   остаётся; EtcdWorker — единственный источник DR-бэкапов etcd в S3.
   Локальные слепки P12 у PgWorker (точки изменений «до/после», healthz
   snapshot-freshness) остаются без выгрузки.
4. **Замена операторского рецепта бутстрапа** (решение пользователя):
   `deploy/etcd` переделывается в рецепт ОДНОГО seed-узла (compose, общая
   сеть, peer-URL по имени узла), состав до целевого доводит EtcdWorker;
   вариант «3 узла на разных хостах, `network_mode: host`, peer по IP»
   удаляется из deploy/etcd и arch/04 §8 ПОЛНОСТЬЮ — старой модели
   (host-сеть, IP-адресация) в документах и рецептах воркер-инсталляции не
   остаётся.
5. **Панель**: страница «etcd» — состав (декларация/managed/external/health),
   карточка «DR-слепки», форма «Поддерживать N узлов» (полный UI — решение
   пользователя); алерты `etcd-snapshot-export-failed`/`-stale` на новом
   ключе + новый `etcd-backup-cluster-missing`; грань «Воркеры» — ряд
   etcdworker.

Влияние (reliability): **R** — самовосстанавливаемость control-plane (замена
мёртвого члена автоматикой), **D** — восстановимость контроль-плейна из S3
(каденс слепков с детекцией изменений).

**Границы (НЕ входит)**:

- Бутстрап контура «с нуля» силами воркера — невозможен по построению
  (координация воркера живёт в самом etcd): первичный контур — оператор
  (ЕДИНСТВЕННЫЙ seed-узел, новый рецепт deploy/etcd, arch/04 §8 п.5); далее
  состав доводит воркер.
- Disaster-recovery при потере кворума — runbook arch/09 §4; восстановление
  из слепка — runbook `docs/runbook.md` (обновлён); автоматизации нет.
- Механика спец-ключа бэкапа etcd у S3Worker — НЕ меняется (env
  `S3W_ETCD_BACKUP_*`, подстановка в secrets при provisioning, arch/23 §6 /
  arch/24 §4): перенос ключа etcdworker'у из roadmap-формулировки ОТМЕНЁН
  решением пользователя. Единственное касание S3Worker в t32 — минимальная
  контрактная правка: при разворачивании etcd-backup кластера создавать на
  нодах ДОПОЛНИТЕЛЬНО фиксированный etcd-слепковый bucket `etcd-backups`
  (arch/23 §5, arch/24 §5 A P3).
- Управление external-узлами (seed, стендовые as-etcd) — только наблюдение;
  перестройка dev-станда НЕ выполняется (решение пользователя: стенд
  остаётся на compose-узлах as-etcd-1/2/3, etcdworker на стенде —
  наблюдение + слепки/экспорт).
- Рецепт §1–7 arch/04 (Patroni-стенд слоёв 01–13, pg1/pg2/pg3 host-network) —
  ДРУГОЕ применение рецепта (мир 01–13, вне воркер-инсталляции): не меняется.
- Доменные Prometheus-серии etcd-домена — не вводятся (каркас `worker_*`,
  Meter `EtcdWorker`, arch/18 §2.2 достаточен).
- Мутация peer-адресов существующих членов контура — вне штатных операций;
  managed-замена пишет peer-URL только НОВОГО члена (по имени узла).

## 2. Принципы

- **Arch-first**: канон — `arch/25-etcdworker.md` (обновлён ДО кода: arch/25
  новый; arch/04 §0/§8 — переработаны под модель «имена/сеть + seed-бутстрап»;
  arch/09 §4, arch/14 §3.3/§6/§8, arch/19 §5/§7/§9, arch/23 §5, arch/24 §5 A
  P3, adminpanel/02 §2.3.1/§2.3.5/§9.9/§9.13, adminpanel/03 §1/§2/§4,
  docs/runbook.md, roadmap-формулировки). Код зеркалит arch; расхождения —
  блокирующее замечание ревью.
- **Образец воркера — S3Worker** (arch/24 + `src/S3Worker.*`): каркас
  Program/DI, ClaimStore/WorkJournal, Keepalive/Snapshot/Reconcile-циклы,
  mTLS API + `/workers/api_tls/<worker>` + api-дискавери, healthz, watchdog,
  env-биндинги, Docker-модель (Shared.Docker). Дублирование не изобретаем:
  копируем паттерны S3Worker.
- **Паттерны arch/17**: идемпотентность каждого шага (перепроверка факта),
  journal-before-manipulations, transient vs permanent, флап ≠ смерть (S7),
  takeover ≤ TTL 15 с + тик.
- **Watchdog-канон arch/14 §6**: инвариант живости (тики быстрых циклов чаще
  порога сноса — fail-fast старта), поллинг долгих фаз (docker create/start,
  health-ожидание — итерации с Mark + elapsed-логом), пульс конечного
  S3-вызова.
- **Multi-host**: всё значимое состояние — в etcd (декларация, states,
  portalloc, journal, статус слепков); локальные тома — кэш без гарантий.
  Мультихост — ТОЛЬКО Swarm (имена сервисов overlay-сети); plain — контур на
  одном docker-хосте (R8 arch/25: потеря хоста = потеря контура, переживание
  — DR-слепки; отказоустойчивый контур — Swarm с расносом по swarm-нодам).
- **Один etcd-контур на установку** (AGENTS): домен `/etcd/` per-install,
  кластеров у EtcdWorker нет — Parallelism/MaxClusters не вводится.

## 3. Структура и компоненты

### 3.1. Сборки (новые, в `src/`, включаются в `src/PgWorker.slnx` — по образцу S3Worker)

| Сборка | Содержимое |
|---|---|
| `EtcdWorker.App` | Program.cs (каркас S3Worker-образца), `EtcdWorkerOptions` (§3.6), env-биндинги `ETCDW_*`, healthz/метрики, API-хендлеры (`PUT /api/etcd/config`, `POST /api/restart`), циклы Keepalive/Snapshot/Reconcile, `EtcdWorkerLoopsVitality`, StartupChecks |
| `EtcdWorker.Core` | Домен: managed-узел (модель/state-машина значений), member-модель (парсинг member list, детект managed по peer-URL-паттерну `/etcdw-node-<k>:`), планировщик размещения (Swarm — анти-аффинити по swarm-нодам + least-loaded), S3-клиент слепков (AWSSDK, по образцу `PgWorker.Backups.BackupS3`: пер-попыточный таймаут, SHA256-проверка put, программное доверие CA из `CaBundlePath`) |
| `EtcdWorker.Etcd` | Парсеры ключей `/etcd/` (config, nodes), статус-JSON `/etcdworker/snapshots` (перенос `EtcdSnapshotStatusJson`-механики: OK/FAILED с сохранением полей успеха, IsBehind, TakenUnixFromName) |
| `EtcdWorker.Provisioning` | Процессы: `MembershipProcess`, `NodeSupervisor`, `EtcdNodeProvisioner` (создание контейнера/сервиса managed-узла в сети контура), `EtcdBackupTargetResolver` (резолв etcd-backup кластера, §3.4), `SnapshotExporter` (механика `EtcdSnapshotSink` — перенос), portalloc-механика (client-порты) |
| `EtcdWorker.Docker` | Драйверы создания/инспекции/сноса узлов (Plain — контейнер в сети `Docker:Network`; Swarm — сервис с расносом по нодам; ensure сети контура идемпотентно) |

Константа имени слепкового bucket'а `etcd-backups` — общая для S3Worker
(создание) и EtcdWorker (цель выгрузки): одна строка в `Shared.Core`
(без дублирования строк в двух воркерах).

`docker/EtcdWorker.Dockerfile` — runtime-слой с `COPY` publish-вывода
(канон E2E/поставки: исходники в образ НЕ копируются; сборка на хосте в
`artifacts/etcdworker/publish` — по образцу s3worker в 00-up.sh и deploy).
Meter — `EtcdWorker`; словарь arch/18 §2.2 без расширений.

### 3.2. Shared.Etcd — расширение gateway (member-API)

`IEtcdGateway`/`EtcdGateway` дополняются (базовый HTTP-транспорт тот же,
JSON `/v3/cluster/*`; `MemberListAsync` уже есть):

- `MemberAddAsync(endpoint, peerUrls[], isLearner, ct)` →
  `POST /v3/cluster/member/add` (тело `{"peerURLs":[...],"isLearner":bool}`;
  peer-URL managed-узла — `http://etcdw-node-<k>:2380` — имя узла, etcd
  выводит member name из hostname peer-URL; фактическая механика имени члена
  сверяется по поведению etcd 3.5 на интеграционной фикстуре — инвариант
  спеки: managed-члены опознаются по peer-URL-паттерну `/etcdw-node-<k>:`,
  НЕ по name);
- `MemberRemoveAsync(endpoint, memberId, ct)` → `POST /v3/cluster/member/remove`;
- `MemberPromoteAsync(endpoint, memberId, ct)` → `POST /v3/cluster/member/promote`.

Ответы-ошибки (not-in-sync promote, unstarted remove) — Result.Failed →
transient-ретраи тиком. Юниты — на JSON-маппинг ответов.

### 3.3. Контракт etcd (канон — arch/25 §5; таблица здесь — рабочая сводка)

| Ключ | Тип | Назначение |
|---|---|---|
| `/etcd/config` | `{"nodes":N,"created_unix":T}` | декларация числа managed-узлов; пишет API воркера (`PUT /api/etcd/config`); отсутствие = 0 (наблюдение) |
| `/etcd/nodes/<k>/state` | `NOT_INITIALIZED\|PROVISIONING\|RUNNING\|UNREACHABLE\|REMOVING` | жизненный цикл managed-узла |
| `/etcd/nodes/<k>/placed_on` | строка (Plain — единственный хост; Swarm — swarm-нода) | размещение |
| `/etcdworker/leader` | lease TTL 15 с | singleton-лидер (слепки/обслуживание/доводка состава) |
| `/etcdworker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (t90-паттерн) |
| `/etcdworker/instances/<id>` | lease TTL 15 с | живость инстансов |
| `/etcdworker/api/<id>` | lease TTL 15 с | дискавери API (панель) |
| `/etcdworker/portalloc` | `{"<k>":{"host":"h1","client":18100}}` | закрепление host-портов КЛИЕНТСКОЙ грани (peer — имена сети, без host-портов) |
| `/etcdworker/work` | `{"op","phase","updated_unix","instance","last_error"?}` | журнал фаз состава |
| `/etcdworker/snapshots` | `{"state":"OK"\|"FAILED","last_uploaded_unix"?,"last_object"?,"last_sha256"?,"size_bytes"?,"interval_min"?,"error"?}` | статус DR-выгрузки (семантика покрытия) |

Пишут префиксы `/etcd/` и `/etcdworker/` ТОЛЬКО инстансы EtcdWorker (декларацию
— API-хендлер под клэймом-txn `version==0`/RMW по mod_revision). Панель читает
`/etcd/config`, `/etcd/nodes/*`, `/etcdworker/{api,snapshots}`.

### 3.4. Процессы воркера

**Классификация тика ReconcileLoop** (лидер): config нет/N=0 — только
наблюдение; иначе — MembershipProcess → NodeSupervisor. Все операции состава —
только при здоровом кворуме (leader есть в status; иначе transient-ожидание).

**MembershipProcess (доводка к декларации)** — одна структурная операция за
тик (шаги идемпотентны, journal `/etcdworker/work`):

- Недобор (managed < N): резолв номера `<k>` нового узла (максимальный
  существующий +1; имена НЕ переиспользуются) → закрепление client-порта из
  PortRange под клэймом (занятость = docker-публикации ∪ portalloc-записи
  всех доменов) → `MemberAddAsync(isLearner=true,
  peerUrls=["http://etcdw-node-<k>:2380"])` → создание контейнера
  (`etcdw-node-<k>`, volume `etcdw-node-<k>-data`) / Swarm-сервиса В СЕТИ
  КОНТУРА `Docker:Network` (env/флаги по arch/04 §3:
  `--initial-cluster-state=existing` — член добавлен member-API до старта;
  `--listen-peer-urls=http://0.0.0.0:2380` без host-публикации (межузловой
  трафик — имена сети); `--listen-client-urls=http://0.0.0.0:2379` с
  host-публикацией client-порта из portalloc;
  `--initial-advertise-peer-urls=http://etcdw-node-<k>:2380`;
  `--advertise-client-urls=http://etcdw-node-<k>:2379,http://<advertised-host>:<client-port>`;
  heartbeat 250/election 2000/auto-compaction 1/quota 8GiB; label
  `etcdworker`) → health-ожидание (бюджет NodeBootSec, поллинг-итерации) →
  promote-ретраи тиками (`MemberPromoteAsync`; not-in-sync — transient) →
  `state=RUNNING`; провал boot-бюджета → REMOVING (member remove + снос,
  retry новым номером).
- Перебор (managed > N): ОДИН вывод за тик: кандидат — старейший по номеру,
  НЕ лидер (member ID ≠ leader из status); кворум-гвард: после remove ≥ 3
  членов; `MemberRemoveAsync` → снос контейнера/сервиса и volume → чистка
  state/placed_on/portalloc.
- **Авто-замена мёртвого managed-члена** (решение пользователя): member
  unstarted/endpoint-health молчит И supervisor не восстановил (контейнер
  отсутствует на placed_on, машина недоступна; либо volume потерян) дольше
  `NodeDeadSec=300` → member remove (гварды как выше) → недобор → путь
  добавления на живом размещении. Мёртв лидер — сначала выборы etcd, замена
  после смены лидера.
- Возврат машины после замены (NodeSupervisor): контейнер с label
  `etcdworker`, чей member (по peer-URL-паттерну `/etcdw-node-<k>:`)
  отсутствует в member list → снос контейнера и volume (`orphan-cleanup`,
  journal).

**NodeSupervisor**: снесённый контейнер managed-узла на живом окружении →
пересоздание (volume жив — member возвращается с данными; сверка
имени+client-порта, расхождение — пересоздание контейнера, volume
сохраняется). Health-проба каждого узла (`StatusAsync` по client-порту,
HealthIntervalSec): молчит дольше NodeDeadSec → `UNREACHABLE` + ретраи (без
деструктива — материал для замены). Слепая проба — никаких действий; один
repair-шаг за тик.

**SnapshotLoop (лидер `/etcdworker/leader`)**:

- Расписание: плановый слепок раз в `IntervalMin=360`; между плановыми — тик
  короткого сна (пульсирующий, чанк < окна watchdog): чтение revision кластера
  (`StatusAsync` failover) — revision вырос относительно метки последней
  выгрузки И выгрузка старше `MinIntervalSec=300` → внеочередное снятие.
- Снятие: `SnapshotJob.TakeAsync` (Shared.Etcd; локальный том
  `/snapshots`, ретенция `RetentionFiles=10`) → `SnapshotExporter.ExportAsync`
  (перенос механики `EtcdSnapshotSink`: put `etcd/snapshot-<id>.db` +
  `.meta.json`, ретенция `RetentionObjects=28` пар guard ≥1, статус-ключ
  семантики покрытия, единый бюджет `TimeoutSec=30` linked-CTS, пульс
  конечного S3-вызова, CatchUp до снятия + повторный расчёт после, короткий
  сон `RetryIntervalSec=300` при отставании/FAILED — механика t08
  переносится как есть).
- Цель выгрузки — `EtcdBackupTargetResolver`: живой резолв на каждом проходе:
  range `/s3/clusters/` (config-ключи) → единственный `etcd_backup=true` →
  endpoints-ключ + secrets (ключ `backups`) → цель S3-клиента:
  bucket = константа `etcd-backups` (имя не конфигурируется), endpoint'ы из
  endpoints-ключа, креды из secrets, CA — опция `S3:CaBundlePath` воркера.
  Отсутствие кластера/`TO_REMOVE`/битые secrets/пустые endpoints — FAILED
  статуса с маркером `etcd-backup-missing` (алерт панели; transient для
  цикла — ретраи тиком; слепки продолжают сниматься локально, CatchUp
  догонит).
- Обслуживание контура: `SnapshotJob.MaintainAsync` (compact + defrag
  последовательно, не чаще `MaintenanceIntervalMin=60`) — этим же циклом;
  у PgWorker обслуживание ДЕМОНТИРУЕТСЯ.

**Правка S3Worker (контрактная, минимальная)**: фаза P3 provisioning'а
(arch/24 §5 A) для кластера с `etcd_backup=true` создаёт на каждом инстансе
ДОПОЛНИТЕЛЬНО фиксированный etcd-слепковый bucket `etcd-backups` (тот же
идемпотентный CreateBucket root-кредом: exists → пропуск; константа — общая,
§3.1). AddNodeProcess (D) — тот же шаг для нового инстанса. Никаких других
изменений S3Worker.

**API**: `PUT /api/etcd/config` (валидация N 0..7; гвард кворума — итог
external+managed ≥ 3 → иначе 409; тело замещает config целиком; RMW по
mod_revision), `POST /api/restart` (паттерн arch/14 §1.1), `/healthz`
(`etcd-reachable`, `docker-hosts`, `loops-alive`, `snapshot-export`),
`/metrics`. mTLS: серверный серт `/workers/api_tls/etcdworker` > env-фоллбек
`ETCDW_API_TLS_{CERT,KEY,CLIENT_CA}`; дискавери `/etcdworker/api/<id>` с
`cert_thumbprint`.

### 3.5. Модель managed-узла (docker; канон arch/25 §2)

- Образ `Images{Etcd}` (внешний, `quay.io/coreos/etcd:v3.5.21` — уже
  зеркалирован, images.txt не меняется); параметры узла — arch/04 §3.
- **Адресация — только имена узлов**: peer-URL `http://etcdw-node-<k>:2380`
  (имя docker-хоста/AdvertisedHost в peer-адресации НЕ фигурирует — решение
  пользователя). Узлы контура (managed + seed/внешние) живут в ОДНОЙ общей
  сети `Docker:Network` (дефолт `etcdw-net`): Plain — bridge-сеть
  (ensure-создание идемпотентно воркером, 409=ок); Swarm — overlay-сеть
  (создаёт воркер); стенд/E2E — имя сети контура поставки/сценария.
  Peer-порт 2380 на хост НЕ публикуется. Клиентский порт 2379 публикуется
  на host-порт из PortRange (закрепление portalloc) — единственный канал
  потребителей вне сети; `--advertise-client-urls` — СПИСОК:
  `http://etcdw-node-<k>:2379` + `http://<advertised-host>:<client-port>`
  (прецедент as-etcd; в список — только URL, резолвимые потребителями,
  правило NXDOMAIN arch/04 §8 п.3). AdvertisedHost используется ТОЛЬКО в
  клиентской части advertise/member list — не в peer.
- Имена: контейнер/сервис `etcdw-node-<k>`, volume `etcdw-node-<k>-data`;
  label `etcdworker`; restart-политика `unless-stopped`.
  `--initial-cluster-state=new` — только seed бутстрапа (оператор);
  managed-узлы всегда `existing`.
- **Размещение**: Plain — контур на ОДНОМ docker-хосте (`Hosts[]` ровно один,
  fail-fast старта при большем: «мультихост — Swarm»); Swarm — managed-узлы
  сервисы, разносятся по swarm-нодам (анти-аффинити кворума при
  `nodes ≥ N`, иначе least-loaded; факт — journal/панель). `placed_on`
  фиксирован (том привязан к машине); перепривязка на месте запрещена.
- Docker-транспорт — общий `Shared.Docker` (unix/tcp+TLS/SSH), env
  `ETCDW_DOCKER_TLS_*`, `ETCDW_DOCKER_SSH_*`.

### 3.6. Конфигурация (appsettings + env — канон arch/25 §8)

```
EtcdWorker:Etcd { Endpoints[] }
EtcdWorker:Docker { Mode, Hosts[{Name,Endpoint}], SwarmManager,
                    Network="etcdw-net", PortRange{From,To}, Images{Etcd},
                    Tls, Ssh, AdvertisedHost }
EtcdWorker:Snapshots { Dir="/snapshots", RetentionFiles=10, IntervalMin=360,
                       MinIntervalSec=300, MaintenanceIntervalMin=60,
                       RetentionObjects=28, RetryIntervalSec=300,
                       TimeoutSec=30, S3 { CaBundlePath? } }
EtcdWorker:Thresholds { NodeBootSec=100, NodeDeadSec=300, HealthIntervalSec=15 }
EtcdWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5,
                   Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15 } }
EtcdWorker:Api { AdvertiseUrl, Tls{...} }
```

`Network` — общая сеть контура (Plain — ensure-bridge воркером; Swarm —
overlay воркера; стенд/E2E — сеть контура поставки/сценария). Plain — Hosts
ровно ОДИН хост (fail-fast иначе). Имя слепкового bucket'а НЕ конфигурируется
— константа `etcd-backups`. env-секреты `ETCDW_API_TLS_{CERT,KEY,CLIENT_CA}`,
`ETCDW_DOCKER_TLS_{CA,CERT,KEY}`, `ETCDW_DOCKER_SSH_KEY[_PATH]`,
`ETCDW_DOCKER_SSH_FINGERPRINT`. Fail-fast старта: endpoints, AdvertiseUrl
(https), docker-топология (Plain: 1 хост + непустая Network; Swarm:
SwarmManager), PortRange, пороги, интервалы слепков, watchdog-инвариант
живости. Per-install S3-пар в env НЕТ.

### 3.7. Демонтаж зоны в PgWorker (изъятие бэкапа etcd — решение пользователя)

- Удаляются: `PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs`,
  `EtcdSnapshotStatus.cs`, `EtcdSnapshotMeta.cs` (+ретенционная механика
  префикса `etcd/` в коде PgWorker), `EtcdSnapshotSinkFactory.cs`,
  `SnapshotExportOptions`/`SnapshotExportS3Options` из `Options.cs`,
  подключение sink в `Program.cs`, CatchUp/behind-логика в
  `SnapshotLoop.cs` (остаётся: лидерский регулярный локальный слепок +
  ретенция), `MaintainAsync`-вызов (`MaintenanceIntervalMin` удаляется из
  опций), fail-fast валидации Export-пары, env `PGW_SNAPSHOTS_S3_*` из
  deploy (§3.9). Статус-ключ `/pgworker/etcd-snapshots` PgWorker больше не
  пишет.
- Переносятся в EtcdWorker (механика 1:1): статус-JSON/IsBehind/
  TakenUnixFromName, meta-JSON, retention-отбор пар, sink-конвейер, CatchUp.
- Остаются у PgWorker: локальные P12-слепки (`Snapshots { Dir,
  RetentionFiles }`), healthz snapshot-freshness, точки изменений
  «до/после» (локальные).

### 3.8. Панель (AdminPanel)

- Модель/парсеры: `EtcdDomain` (config/nodes) в EtcdSnapshot; парсер
  `/etcdworker/snapshots` — переиспользование `EtcdSnapshotExportInfo`
  (поле `enabled` уходит из формата); чтение — SnapshotRefresher (префиксы
  `/etcd/config`, `/etcd/nodes/`, точечный `/etcdworker/snapshots`,
  `/etcdworker/api/`); отказный тик — прежние значения переживают.
- Страница «etcd» (adminpanel/03 §2): блок «Состав» (заявлено N / managed
  `/etcd/nodes/*` с placed_on и бейджами / фактическое число членов member
  list; пометки managed/external), карточка «DR-слепки» (состояние, возраст,
  sha256-префикс, размер, ошибка), форма «Поддерживать N узлов» — модал,
  `PUT /api/etcd/config` через API воркера (панельный REST-прокси,
  adminpanel/02 §9.13; двойной клик защищён, 409-текст гварда отображается).
- Алерты: `etcd-snapshot-export-failed` (critical; `/etcdworker/snapshots`,
  state=FAILED), `etcd-snapshot-export-stale` (warning; 2×interval_min,
  панельный дефолт 360), новый `etcd-backup-cluster-missing` (critical; нет
  живого кластера с признаком `etcd_backup` в `/s3/clusters/`).
- Грань «Воркеры»: ряд `etcdworker` (живые инстансы, серт/статус применения —
  `/workers/api_tls/etcdworker`); общий healthz-поллер.
- Карточка «etcd-снапшоты» грани «Хранилище бэкапов» — источник новый ключ.
- Защита серта §9.9 (валидация «не влияет на исходящие») — воркер
  etcdworker добавляется в перечень.

### 3.9. deploy / dev-stand / поставка

- **deploy/etcd — новый seed-рецепт** (решение пользователя): compose-файл
  ОДНОГО узла etcd в сети `etcdw-net` (`external: true`; инструкция:
  `docker network create etcdw-net` перед первым стартом), env-пример
  (`NODE_NAME` — имя узла в сети, peer `http://<NODE_NAME>:2380`;
  `INITIAL_CLUSTER_STATE=new`; образ из локального registry), healthcheck;
  env-интерполяция из env-файла (как прежде). Старые артефакты
  «3 хоста / NODE_IP / PEERS / network_mode: host» удаляются полностью.
  Swarm-бутстрап — канон arch/04 §8 п.5 (сервис seed в overlay; файлов в
  рамках t32 не заводится, шаги — runbook-текст).
- `deploy/docker-compose.yml`: сервисы `etcdworker`/`etcdworker-2`
  (x-etcdworker-env: `EtcdWorker__Etcd__Endpoints__0..2` —
  `ETCDW_ETCD_ENDPOINT_0..2` (стендовые дефолты
  `http://host.docker.internal:{2379,2381,2383}`),
  `EtcdWorker__Docker__Network` (дефолт `etcdw-net`),
  `EtcdWorker__Api__AdvertiseUrl`, docker-transport, TLS-API), порты ряда
  8087/8088 (`ETCDW_API_HOST_PORT{,2}`), общий том `etcdw-snapshots`;
  демонтируются строки `PgWorker__Snapshots__Export__*` и
  `PGW_SNAPSHOTS_EXPORT_ENABLED`; `deploy/.env.example`: удалить блок
  `PGW_SNAPSHOTS_S3_*`, добавить ETCDW-переменные (закомментированные
  дефолты). S3-переменных для слепков НЕ добавляется.
- `docker/EtcdWorker.Dockerfile` — runtime-слой COPY publish (без исходников).
- dev-stand: сервис etcdworker в подъёме 00-up.sh (publish в
  `artifacts/etcdworker/publish`, сборка отдельной командой), env —
  external-контур as-etcd (endpoints 2379/2381/2383), декларация
  `/etcd/config` на стенде НЕ ставится (nodes=0 — наблюдение+слепки).
  Перестройка as-etcd-узлов НЕ выполняется. Чеки: существующие не ломаются;
  новый чек состава НЕ добавляется (механика покрыта тестами — решение
  пользователя); чек/штамп etcdworker в 00-up.sh (healthz зелёный, статус
  слепков появляется при поднятых etcd-backup S3-кластере и воркере).
- images.txt — без изменений (образ etcd уже зеркалирован).

## 4. Фазы (структура для плана; порядок обязателен)

1. **Shared.Etcd member-API**: MemberAdd/Remove/Promote + юниты
   (JSON-маппинг, isLearner, ошибки).
2. **Каркас EtcdWorker**: сборки/Program/Options/env-биндинги/StartupChecks
   (вкл. Plain=1-хост/Network), ClaimStore/WorkJournal/экземпрефикс, mTLS
   API + серт-ключ + api-дискавери, healthz/metrics/watchdog,
   KeepaliveLoop; API `PUT /api/etcd/config` + `POST /api/restart`
   (+ WAF-интеграции по образцу S3Worker).
3. **S3Worker — фиксированный слепковый bucket**: константа `etcd-backups`
   (Shared.Core) + создание bucket'а в P3/AddNode для etcd-backup кластера
   (идемпотентно) + интеграционный тест (оба bucket'а на инстансах;
   повторный проход — no-op).
4. **Слепки/экспорт**: перенос статус/meta/retention/sink-механики в
   EtcdWorker (+`EtcdBackupTargetResolver` с bucket-константой), SnapshotLoop
   с расписанием + детекцией revision + maintenance; юниты + интеграции
   (OwnEtcd + S3-цель: пары в `etcd-backups`, FAILED при недоступной цели,
   CatchUp, отставание → короткий сон).
5. **Состав**: portalloc (client-порты) + размещение (Swarm-раснос;
   Plain=один хост) + ensure сети контура + EtcdNodeProvisioner (контейнер/
   сервис узла в сети), MembershipProcess (add learner → boot → promote;
   remove; авто-замена), NodeSupervisor (пересоздание/orphan-cleanup/
   health); юниты (решения доводки — чистые функции) + docker-интеграции
   (§7.2).
6. **Демонтаж PgWorker**: §3.7; правка юнитов/интеграций PgWorker
   (E2eEtcdSnapshotExportScenarios — переносятся/адаптируются к EtcdWorker),
   зелёные серии PgWorker.
7. **Панель**: §3.8 + юниты панели + WAF при необходимости.
8. **deploy/dev-stand/runbook**: замена `deploy/etcd` на seed-рецепт
   (§3.9), сервисы etcdworker в deploy-compose/.env, 00-up.sh стенда,
   финальные прогоны (мерж-гейт).

## 5. Ограничения и каноны исполнения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, центральные
  версии пакетов (`Directory.Packages.props`; AWSSDK — как у PgWorker.Backups).
- Тесты: порты docker-контейнеров ТОЛЬКО динамические
  (`assignRandomHostPort`+`GetMappedPublicPort`/зонд), никаких литералов;
  boot-бюджеты ≤ 100 с; полная самоочистка каждого сценария (контейнеры/
  тома/сети/ключи своего префикса; ассерт чистоты); сети сценария создаются
  и удаляются им самим (сеть контура интеграций состава — сеть фикстуры,
  `Docker:Network` воркера в тесте указывает на неё); телеметрия E2E — по
  docs/e2e-launch.md.
- Комментарии/документация — по-русски; идентификаторы — по-английски.
- Мерж-гейт (код воркеров PgWorker/S3Worker затронут): сборка Release
  отдельной командой → `PGW_TEST_DOCKER=1 ... --no-build --filter
  FullyQualifiedName~Scale_AddEmptyShard` минимум + E2ePrepare ДО тестов;
  свои серии: EtcdWorker.UnitTests/IntegrationTests, S3Worker-интеграции
  (bucket), PgWorker-юниты/интеграции (демонтаж), AdminPanel.UnitTests;
  зачистка контейнеров/сетей между сериями.
- Docs: только текущее/планируемое состояние; история — docs/superpowers
  и git.

## 6. Принятые решения (закрытые развилки)

### Решения пользователя (фиксируются как требования)

1. **Per-install ключ бэкапа etcd — зона S3Worker**: механика t31 не
   меняется (env `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}`; спец-ключ в
   `/s3/clusters/<C>/secrets` при provisioning). EtcdWorker — читатель
   secrets-ключа (ключ `backups`). Перенос ключа etcdworker'у НЕ
   выполняется; arch/24 §9 R7 — без изменений.
2. **Авто-замена мёртвого члена**: member unstarted + машина недоступна
   дольше 300 с (`NodeDeadSec`) → member remove + новый узел (learner +
   promote, кворум не проседает); возврат машины — снос осиротевшего
   data-dir автоматикой; принятый риск — ложная замена при долгом ребуте.
3. **Стенд — без перестройки**: as-etcd-1/2/3 остаются compose-сервисами
   (external-контур); etcdworker на стенде — наблюдение + слепки/экспорт;
   механика состава — в тестах.
4. **Каденс слепков — расписание + детекция изменений**: 360 мин +
   внеочередной при росте revision и возрасте выгрузки > MinIntervalSec=300
   (RPO ≈ 5 мин при активности).
5. **Полный UI состава**: страница «etcd» — форма «Поддерживать N узлов»
   (мутация через API воркера) + показ состава и здоровья.
6. **Бэкап etcd изымается из PgWorker полностью**: дублирующей механики
   нет; EtcdWorker — единственный источник DR-бэкапов etcd в S3; локальные
   P12 PgWorker — без выгрузки.
7. **Имя bucket слепков — фиксированная константа `etcd-backups`**:
   одинаковый на всех установках; создаёт S3Worker на нодах etcd-backup
   кластера дополнительно к per-install bucket'у (правка P3/AddNode —
   arch/23 §5, arch/24 §5 A P3); у EtcdWorker — константа (общая с
   S3Worker, Shared.Core), env-пар и конфиг-поля имени НЕТ.
8. **Адресация контура — только имена узлов** (решение Фазы 3-resume):
   peer-URL managed-узла строится ОТ ИМЕНИ УЗЛА
   (`http://etcdw-node-<k>:2380`), НЕ от docker-хоста/AdvertisedHost/
   Hosts[]; имя хоста в peer-адресации нигде не фигурирует (ошибка старой
   архитектуры — удалена). Резолвимость имён: мультихост — ТОЛЬКО Swarm
   (имена сервисов в overlay); одиночный docker — имена резолвятся внутри
   ОБЩЕЙ сети контура. Детект managed-членов по peer-URL-паттерну
   `/etcdw-node-<k>:` — сохранён (теперь согласован с конструкцией
   адреса).
9. **deploy/etcd заменяется на seed-рецепт** (решение Фазы 3-resume):
   рецепт ОДНОГО seed-узла (compose, общая сеть, peer-URL по имени),
   состав доводит EtcdWorker; вариант «3 узла, network_mode: host, peer
   по IP» удаляется из deploy/etcd и arch/04 §8 ПОЛНОСТЬЮ — старой модели
   (host-сеть, IP-адресация) в документах и рецептах не остаётся.

### Решения спеки (однозначно из задачи/канонов/кода)

10. Воркер строится по образцу S3Worker; env-префикс `ETCDW_*`; порты API
    8087/8088; Meter `EtcdWorker`; сборки в PgWorker.slnx; образ
    `EtcdWorker.Dockerfile` (runtime COPY).
11. Домен etcd-ключей: `/etcd/` (контроль-плейн) + `/etcdworker/`
    (координация+статус) — per-install; global-лидер — слепки/
    обслуживание/доводка; portalloc-клэйм t90-паттерном.
12. Managed-узлы добавляются только в ЖИВОЙ кластер (`member add` до старта
    контейнера, `initial-cluster-state=existing`); seed и внешние члены —
    наблюдение; после доводки состава seed рекомендуется вывести
    (runbook-шаг, arch/04 §8 п.4).
13. Детекция managed — peer-URL-паттерн `/etcdw-node-<k>:` в member list;
    имя члена etcd выводится из hostname peer-URL (фактическое поведение
    etcd 3.5 сверяется на фикстуре — инвариант: детект по паттерну, не по
    name).
14. Сквозная нумерация узлов (замена = следующий номер); label `etcdworker`
    для перечисления своих docker-объектов.
15. Кворум-гварды: операции при здоровом кворуме; remove — не лидера и с
    остатком ≥ 3 членов; приём декларации — гвард «итог ≥ 3» (409);
    рекомендация нечётного числа — панель/доки; лимит N 0..7.
16. Learner-механика добавления (безголосый до catch-up; promote-ретраи,
    not-in-sync transient); провал boot-бюджета → member remove + снос +
    retry новым номером.
17. **Сеть контура `Docker:Network` (дефолт `etcdw-net`)**: Plain —
    ensure-bridge воркером; Swarm — overlay воркера; стенд/E2E — имя сети
    контура поставки/сценария. Peer-порт 2380 без host-публикации
    (межузловой трафик — имена сети); клиентский порт 2379 — host-порт из
    PortRange (единственный канал потребителей вне сети); portalloc —
    только client-порты. `advertise-client-urls` — список «имя сети +
    advertised-host:client-port» (прецедент as-etcd); AdvertisedHost —
    только клиентская грань.
18. **Plain — ровно один docker-хост** (fail-fast старта при >1: «мультихост
    — Swarm»); Swarm — managed-сервисы разносятся по swarm-нодам
    (анти-аффинити кворума, least-loaded). Потеря единственного
    plain-хоста = потеря контура (осознанное следствие; переживание —
    DR-слепки, R8 arch/25).
19. maintenance (compact+defrag) — переносится к EtcdWorker (единственный
    исполнитель); из PgWorker удаляется вместе с `MaintenanceIntervalMin`.
20. Статус-ключ `/etcdworker/snapshots`: формат t08 МИНУС `enabled`
    (безусловная выгрузка; «не настроено» = FAILED `etcd-backup-missing`);
    семантика покрытия, IsBehind/CatchUp/Retry-сон — 1:1. Layout `etcd/`
    (пары db+meta, ретенция 28, guard ≥1) — без изменений; живут в
    фиксированном bucket `etcd-backups`, отдельном от per-install bucket'а
    бэкапов PG (объекты `etcd/` в `used_bytes` storage не входят).
21. Резолв цели — живой по признаку `etcd_backup=true`; отсутствие →
    FAILED-маркер + алерт `etcd-backup-cluster-missing`; env-пар S3 у
    EtcdWorker нет.
22. DR-restore — ручной runbook (обновлён под seed-модель и новый ключ);
    автолечение потери кворума — вне (arch/09 §4).
23. Доменных Prometheus-серий не вводится; наблюдаемость — панель + healthz.
24. PgWorker после t32: `Snapshots { Dir, RetentionFiles }` + локальный
    лидерский слепок; K/VW/S3W не трогаются, кроме создания S3Worker'ом
    фиксированного слепкового bucket'а (решение 7).

## 7. Критерии приёмки

### 7.1. Юниты

- Shared.Etcd: member add/remove/promote — маппинг тел/ответов/ошибок.
- EtcdWorker: статус-JSON (OK/FAILED с сохранением полей успеха, Parse
  битого), IsBehind/TakenUnixFromName, отбор ретенции пар (guard ≥1,
  парность db+meta), резолвер etcd-backup (отсутствие/TO_REMOVE/битые
  secrets/пустые endpoints → маркер; bucket-константа в цели),
  планировщик размещения (Swarm-раснос: анти-аффинити/least-loaded/
  tie-break — детерминизм), решения доводки состава (недобор/перебор/
  кандидат вывода/детект авто-замены/лидер-гвард/кворум-гварды;
  формирование env/advertise узла: peer по имени, client-список) — чистые
  функции, полный охват веток.
- S3Worker: отбор bucket'ов для инстанса (per-install всегда;
  +`etcd-backups` только при `etcd_backup=true`).
- Панель: парсеры `/etcd/config`, `/etcd/nodes/*`, `/etcdworker/snapshots`
  (толерантность, parseError), AlertEngine-правила (failed/stale/missing —
  включая молчание при отсутствии ключей).

### 7.2. Интеграции (docker, самоочистка, динамические порты)

- S3Worker: provisioning etcd-backup кластера → на каждом инстансе ОБА
  bucket'а; повторный проход — no-op; AddNode — новый инстанс получает
  оба.
- Слепки: OwnEtcd-фикстура + S3-цель: плановое снятие+выгрузка пар в
  `etcd-backups`; транзиент цели → FAILED+error, CatchUp-доводка; ретенция
  пар; maintenance (compact/defrag выполняется); детекция изменений
  (рост revision → внеочередной слепок; тишина → один на интервале — время
  теста сокращается конфигом).
- Состав (ядро приёмки): seed-фикстура (single-member etcd, peer по имени
  в сети сценария) + воркер с `Docker:Network` = сеть сценария →
  декларация nodes=2 → добавление 2 managed-узлов (learner → promote;
  финальный ассерт: 3 started-члена, peer-URL всех — имена); понижение
  nodes=1 → rolling-вывод (гварды: не лидер); авто-замена: снос
  контейнера+volume managed-узла + недоступность среды (`NodeDeadSec`
  конфигурационно мал) → замена новым номером; ветка orphan —
  docker-тест «член удалён вручную member-API → контейнер снесён»;
  отсутствие etcd-backup кластера → статус FAILED с маркером.
- API: PUT /api/etcd/config (валидации 0..7, 409 гварда, RMW-гонка),
  /api/restart — WAF-интеграции по образцу S3Worker.
- PgWorker: демонтаж — существующие suite зелёные минус перенесённые
  кейсы; PgWorker стартует без `PGW_SNAPSHOTS_*`.

### 7.3. E2E / мерж-гейт / deploy

- PgWorker docker-E2E минимум `Scale_AddEmptyShard` на свежем Release
  (демонтаж не сломал воркер; сборка отдельной командой, E2ePrepare до);
  S3Worker-интеграции (bucket) зелёные.
- Полный прогон EtcdWorker-интеграций зелёный; зачистка между сериями
  подтверждена (нет осиротевших `etcdw-*`/сетей, вкл. сеть контура
  сценариев).
- Dev-стенд: 00-up.sh поднимает etcdworker (healthz зелёный,
  external-наблюдение: декларация не ставится), статус слепков появляется
  при поднятом etcd-backup S3-кластере; существующие чеки стенда зелёные.
- deploy/etcd: новый seed-рецепт поднимает seed-узел (сеть `etcdw-net`,
  peer по имени, healthcheck); ручной прогон канона arch/04 §8 п.5
  (seed → nodes=N → вывод seed) на локальном контуре — шаг приёмки фазы 8
  (runbook-сценарий).
- Мерж-гейт трека reliability: тег `t32-etcdworker` удаляется из
  `arch/roadmap/reliability.md` И `←`-ссылки тем же коммитом; строка
  переносится в «Сделано» `reliability-report.md` с обновлением сводок
  R/D и актуализацией строк «etcd-обслуживание»/t08 на новое состояние.
- Код-ревью: сверка arch↔код по чек-листам dev-flow; противоречия
  arch/25/arch/04 §8/arch/23 §5 — блокирующие.

## 8. Риски (снимаются планом/ревью)

| Риск | Митигация |
|---|---|
| Ложная авто-замена при ребуте машины > 300 с | принятый риск (решение пользователя): порог конфигурируем, кворум не проседает, слепки независимы; осиротевший data-dir сносится при возврате |
| Member-API по HTTP-gateway отличается от etcdctl-поведения (name из peer-URL, ошибки promote) | фаза 1 плана — сверка фактического поведения etcd 3.5 на интеграционной фикстуре; инвариант — детект managed по peer-URL-паттерну |
| Plain-контур на одном docker-хосте: потеря хоста = потеря контура | осознанное следствие адресации по именам (мультихост — Swarm): переживание — DR-слепки S3 + runbook-восстановление; отказоустойчивый контур — Swarm с расносом узлов |
| Seed остаётся в контуре после доводки (деградация при его смерти) | runbook-шаг вывода seed (arch/04 §8 п.4); панель показывает состав — висящий seed виден |
| Демонтаж PgWorker ломает существующие тесты/стенд | фаза 6 — отдельный проход с полными прогонами серий PgWorker; перенос кейсов t08 механический |
| Клиенты (env endpoints) не знают адрес нового узла | failover по списку держит потребителей; полный список — env поставки (runbook-шаг); панель показывает member list |
| S3-клиент слепков дублирует BackupS3 | осознанное дублирование минимального клиента в EtcdWorker.Core (Shared без AWSSDK); сверка в ревью |
| Фиксированное имя bucket `etcd-backups` на всех установках — коллизия при внешнем общем S3 | установки изолированы своими S3-кластерами; внешний общий S3 — вне канона (пулы `/s3/clusters/` установки) |
| Слепковый bucket вне квоты/учёта `/pgworker/backups/storage` | отдельный bucket (решение 7); объём слепков мал (ретенция 28 пар); занятость — админ-грань ownS3 инстансов |
| Один seed при старте установки = SPOF окна бутстрапа | окно короткое (доводка состава — минуты); seed жив пока кворум не собран; после вывода seed контур полностью managed |

## 9. Связанные документы

- Канон: `arch/25-etcdworker.md` (новый), переработка `arch/04` §0/§8
  (модель «имена/сеть + seed-бутстрап»; host-network/IP-модель контура
  воркер-инсталляции удалена), правки `arch/09` §4, `arch/14`
  §3.3/§6/§8, `arch/19` §5/§7/§9, `arch/23` §5 (фиксированный слепковый
  bucket), `arch/24` §5 A P3 (+вводная), `arch/adminpanel/02`
  §2.3.1/§2.3.5/§9.9/§9.13, `arch/adminpanel/03` §1/§2/§4, `arch/README`,
  `docs/runbook.md` (HA-etcd контур: seed-рецепт; восстановление),
  `arch/roadmap/{reliability,reliability-report}.md` (формулировки
  синхронизированы; удаление тега — мерж-гейт t32).
- Образцы кода: `src/S3Worker.*` (каркас + P3-точка правки bucket'ов),
  `src/PgWorker.Backups/EtcdExport/*` (переносимая механика),
  `src/Shared.Etcd` (gateway/SnapshotJob).
