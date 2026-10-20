# 25. EtcdWorker: оркестратор etcd-контура установки ★

**EtcdWorker** — фоновый сервис (.NET 10), хозяин etcd-контура контроль-плейна
установки: поддерживает состав etcd-узлов (декларативное число
**managed**-контейнеров, кворум) и выгружает DR-слепки etcd в S3 (etcd-backup
кластер `/s3/clusters/`, arch/23 §2/§6). **DR-бэкапы etcd в S3 делает
ТОЛЬКО EtcdWorker** (дублирующей механики в PgWorker нет — снятая зона
`Snapshots:Export`, arch/14 §6/§8).

Граница бутстрапа: воркер координируется через сам etcd (клэймы, декларации) и
НЕ поднимает контур с полного нуля. Первичный контур — оператор: ЕДИНСТВЕННЫЙ
seed-узел (рецепт `deploy/etcd` — compose одного узла в общей сети контура,
peer-URL по имени узла, arch/04 §8 п.5); воркер подключается к ЖИВОМУ контуру
по env endpoints и далее доводит состав до декларации: **managed-узлы**
(контейнеры/сервисы, созданные воркером: детерминированные имена
`etcdw-node-<k>`, peer-URL `http://etcdw-node-<k>:2380`, label `etcdworker`)
+ **external-члены** (seed и прочие поднятые оператором/компоузом —
наблюдаются, не трогаются; после доводки состава seed рекомендуется вывести —
arch/04 §8 п.4). Дев-стенд живёт на external-контуре
(`as-etcd-1/2/3`, dev-stand/adminpanel/compose) — воркер наблюдает и ведёт
слепки, managed-механика на стенде не используется.

**Ответственность изменений etcd**: префиксы `/etcd/` и `/etcdworker/` пишет
ТОЛЬКО EtcdWorker (панель — через его HTTP API, §1.1); слепки воркер кладёт в
фиксированный etcd-слепковый bucket `etcd-backups` на нодах etcd-backup
кластера (префикс `etcd/`, arch/19 §5 — layout без изменений). Спец-ключ
бэкапа etcd — зона S3Worker (env
`S3W_ETCD_BACKUP_*`, arch/24 §4): EtcdWorker — только читатель
`/s3/clusters/<C>/secrets` (ключ `backups`), как остальные клиенты пула.

Процессы (машины тиков; паттерны arch/17):
1. **MembershipProcess** — доводка состава к декларации `/etcd/config`
   (недобор → add-learner; перебор → rolling remove; авто-замена мёртвого
   managed-члена);
2. **NodeSupervisor** — надзор managed-узлов (пересоздание снесённого
   контейнера, health-пробы, снос осиротевших data-dir на вернувшихся хостах);
3. **SnapshotLoop** (глобальный лидер) — DR-слепки по расписанию + детекция
   изменений, выгрузка в S3 (§4), CatchUp-доводка; обслуживание контура
   (compact + defrag — перенесено у PgWorker).

Свойства: два инстанса по умолчанию (deploy), координация — lease-клэймы в
etcd, takeover ≤ TTL 15 с + тик; все операции идемпотентны; значимое состояние
в etcd.

Границы (что НЕ входит): disaster-recovery при потере кворума (2/3 недоступны —
runbook arch/09 §4, автоматика невозможна без кворума); восстановление контура
из слепка (runbook docs/runbook.md); Patroni-ноды и прочие потребители
контура (их адресация — env поставки); S3-кластеры сами по себе (арх/24).

---

## 1. Роль и разделение ответственности

```
AdminPanel (UI)          EtcdWorker (оркестратор контура)      docker-хосты
──────────────           ──────────────────────────────       ────────────
мутация декларации ──►   HTTP API воркера (§1.1)               контейнеры
состава (nodes=N);       ──пишет──► /etcd/config,              etcdw-node-<k>
наблюдение состава,      /etcd/nodes/<k>/state                 (etcd),
слепков, воркера         ──member-API──► etcd (/v3/cluster/*)  volume данных,
                         ──создаёт/удаляет──► контейнеры,      client/peer
                         volumes, публикации портов            host-порты
                         ──снимает слепки──► SnapshotJob;      из portalloc
инспекция ◄──            ──выгружает──► bucket etcd-backups
(read-only всё видит;     нод etcd-backup кластера /s3/clusters/
ключ /etcdworker/api/<id>) (префикс etcd/, статуса /etcdworker/snapshots)
```

- **Панель** — декларатор и наблюдатель: etcd читает, мутации (число
  managed-узлов) — через API воркера; никогда напрямую.
- **EtcdWorker** — исполнитель: единственный, кто меняет состав кластера etcd
  (member add/remove/promote), создаёт/удаляет managed-контейнеры и их тома,
  пишет префиксы `/etcd/`, `/etcdworker/`, выгружает слепки.
- **PgWorker/KafkaWorker/ValkeyWorker/S3Worker** — клиенты контура: их
  локальные P12-слепки остаются (процессы «до/после», healthz
  snapshot-freshness), выгрузки в S3 больше НЕ делают; maintenance контура
  (compact/defrag) — только EtcdWorker.

### 1.1. HTTP API воркера (мутации панели)

Та же mTLS-грань, что `/healthz` и `/metrics` (порт `:8080`, канон
[14-pgworker.md](14-pgworker.md) §1.1; клиенты — клиентским сертом per-install
API-CA). Серверный серт — ключ `/workers/api_tls/etcdworker` (канон —
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.9; пишет
только панель); env-фоллбек `ETCDW_API_TLS_{CERT,KEY,CLIENT_CA}`. Дискавери —
lease-ключ `/etcdworker/api/<id>`; URL — из `EtcdWorker:Api:AdvertiseUrl`.

| Метод+путь | Назначение |
|---|---|
| `PUT /api/etcd/config` | декларация состава: тело `{"nodes":N}` → put `/etcd/config` (замещение целиком; 0 — managed-узлов нет, только наблюдение). Валидация: `N` — целое 0..7; гвард кворума — итоговое число членов (external + N) ≥ 3, иначе 409 «состав ниже безопасного»; понижение N запускает rolling-вывод managed-узлов (по одному за тик, §3.1) |
| `POST /api/restart` | graceful self-stop инстанса (паттерн arch/14 §1.1) |

`/healthz` — `etcd-reachable`, `docker-hosts`, `loops-alive`, `snapshot-export`
(последняя выгрузка: OK/FAILED/возраст). `/metrics` — каркас arch/18 §2.2
(Meter `EtcdWorker`, серии `worker_*`).

## 2. Модель managed-узла (docker)

- **Образ** — внешний `Images{Etcd}` (канон `quay.io/coreos/etcd:v3.5.21`,
  зеркалируется в локальный registry, ранбук); параметры узла — по arch/04 §3
  (`heartbeat-interval=250`, `election-timeout=2000`,
  `auto-compaction-retention=1`, `quota-backend-bytes=8GiB`).
- **Имена**: контейнер `etcdw-node-<k>`, volume `etcdw-node-<k>-data`;
  `<k>` — сквозной номер (замена узла получает СЛЕДУЮЩИЙ
  номер, имена не переиспользуются — data-dir старого при возврате не
  конфликтует); label `etcdworker` на контейнере (перечисление своих объектов
  по label, имена произвольны после замен). Restart-политика `unless-stopped`.
- **Адресация — ТОЛЬКО по именам узлов, peer от имени** (arch/04 §8 п.1):
  peer-URL managed-узла — `http://etcdw-node-<k>:2380`; имя docker-хоста /
  `AdvertisedHost` в peer-адресации НЕ фигурирует. Узлы контура (managed +
  seed/внешние) живут в ОДНОЙ общей сети `Docker:Network` (дефолт `etcdw-net`;
  Plain — bridge-сеть, ensure-создание идемпотентно воркером; Swarm —
  overlay-сеть, создаёт воркер; стенд/E2E — имя сети контура поставки).
  Peer-порт 2380 на хост НЕ публикуется (межузловой трафик — внутри сети);
  клиентский порт 2379 публикуется на host-порт из `PortRange` (закрепление
  `/etcdworker/portalloc`, переживает rebuild) — единственный канал
  потребителей вне сети; `--advertise-client-urls` — СПИСОК
  `http://etcdw-node-<k>:2379` + `http://<advertised-host>:<client-port>`
  (прецедент `as-etcd` стенда; правило резолвимости — arch/04 §8 п.3).
  `--initial-cluster-state=new` — только seed бутстрапа (оператор);
  managed-узлы всегда стартуют с `existing` (member добавлен
  member-API ДО старта контейнера).
- **Размещение**: мультихост — ТОЛЬКО Swarm: managed-узлы —
  сервисы, разносятся по swarm-нодам (анти-аффинити кворума — по разным
  нодам при `nodes ≥ N`, иначе least-loaded, факт — в journal и панель);
  Plain — контур на ОДНОМ docker-хосте (`Hosts[]` — ровно один хост,
  fail-fast старта при большем: «мультихост — Swarm»). `placed_on`
  фиксирован (том данных привязан к машине); перепривязка на месте не
  допускается.
- **Docker-транспорт** — общий `Shared.Docker` (unix/tcp+TLS/SSH), env
  `ETCDW_DOCKER_TLS_*`, `ETCDW_DOCKER_SSH_*`.

## 3. Поддержание состава (кворум)

Классификация тика: `/etcd/config` нет → N=0 (только наблюдение/слепки);
иначе — доводка. Все операции состава — только при ЗДОРОВОМ кворуме контура
(иначе transient-ожидание тиками; потеря кворума — runbook arch/09 §4,
панельный алерт уже есть).

### 3.1. MembershipProcess (доводка к декларации)

- **Недобор** (managed-членов < N): `member add` НОВОГО узла как **learner**
  (`POST /v3/cluster/member/add`, peer-urls `http://etcdw-node-<k>:2380` —
  по имени узла, §2) → создание контейнера/сервиса в сети контура на
  размещении §2 (client-порт под глобальным клэймом
  `/etcdworker/locks/portalloc`, t90-паттерн) → health-ожидание (`NodeBootSec`
  бюджет) → **catch-up**: тиковые попытки `member promote` (etcd сам
  отвергает промоут отстающего learner'а — отказ transient, ретрай тиком) →
  `state=RUNNING`. Learner в member list не голосует: кворум не меняется ни на
  одном шаге.
- **Перебор** (managed-членов > N): rolling-вывод — ОДИН узел за тик:
  кандидат — не лидер (гвард: member ID ≠ leader из status), старейший по
  номеру; `member remove` → снос контейнера и volume → чистка portalloc.
  Гвард кворума: после remove останется ≥ 3 членов всего (external+managed) —
  иначе отказ фазы (409 на приёме декларации уже отсёк, здесь — защита вглубину).
- **Авто-замена мёртвого managed-члена**: member unstarted/недоступен И
  supervisor не восстанавливает (контейнер отсутствует, хост недоступен —
  либо volume потерян) дольше `NodeDeadSec=300` (дольше ребута хоста) →
  `member remove` (не лидер-гвард; гейт здорового кворума цикла — лидер жив;
  ≥3-гвард перебора к замене НЕ применяется: замена сохраняет итоговый
  состав, окно с меньшим числом voting в 3-членном контуре — принятый риск
  R1, мёртвый член не голосует) → недобор → путь добавления
  (learner + promote) на живом хосте. Ложная замена при долгом ребуте —
  принятый риск (решение авто-замены): возврат старого хоста обрабатывает
  NodeSupervisor (§3.2). Если мёртв ЛИДЕР — сначала выборы etcd (сам), замена —
  после смены лидера.
- **External-члены**: в доводке не участвуют; member list парсится, члены без
  peer-URL-паттерна `/etcdw-node-<k>:` отображаются панелью как external
  (seed, стендовые `etcdN`).
- Journal — `/etcdworker/work` (journal-before-manipulations, фазы
  `add-learner/promoting/removing/replacing`); takeover продолжает фазу.
- **Транспорт member-API** — расширение `Shared.Etcd` EtcdGateway:
  `MemberAddAsync` (isLearner), `MemberRemoveAsync`, `MemberPromoteAsync`
  (HTTP JSON `/v3/cluster/member/*`, базовый транспорт тот же).

### 3.2. NodeSupervisor (надзор managed-узлов)

- Снесённый контейнер на ЖИВОМ хосте → пересоздание (volume сохраняется —
  member возвращается с теми же данными; идемпотентно по имени+портам).
- Health-проба каждого узла (`/v3/maintenance/status` по client-порту,
  `HealthIntervalSec`): молчит дольше `NodeDeadSec` → `UNREACHABLE` +
  ретраи; деструктивных действий по факту «не отвечает» нет (S7) — материал
  для авто-замены (§3.1) собирается комбинацией свидетельств.
- **Возврат хоста после авто-замены**: контейнер с label `etcdworker`, имя
  `etcdw-node-<k>`, а member `etcdw<k>` отсутствует в member list → снос
  контейнера и volume (осиротевший data-dir; etcd сам отверг бы старт
  удалённого члена) + journal-фаза `orphan-cleanup`.
- Слепая проба (хост молчит) — никаких действий; один repairs-шаг за тик.

## 4. DR-слепки: выгрузка в etcd-backup кластер

- **Цель** — единственный S3-кластер с признаком `etcd_backup=true` в
  `/s3/clusters/` (живой резолв на каждом проходе: range config'ов → признак;
  единственность гарантирует гвард создания 409, arch/24 §1.1). **Bucket —
  фиксированная константа `etcd-backups`**, одинаковое имя на всех установках
  (создаёт S3Worker на каждом инстансе кластера при provisioning ДОПОЛНИТЕЛЬНО
  к per-install bucket'у — arch/23 §5/arch/24 §5 A P3; имя не
  конфигурируется, per-install env-пар S3 у воркера нет). Резолв
  компонентов — по образцу пула бэкапов (arch/19 §4): endpoints-ключ +
  `/s3/clusters/<C>/secrets` (ключ `backups` — read-write; для etcd-backup
  кластера значение — спец-ключ из env S3Worker, arch/23 §6). Отсутствие
  кластера с признаком / `TO_REMOVE` / битые secrets — FAILED статуса с
  маркером `etcd-backup-missing` (алерт панели; transient для цикла —
  ретраи тиком), НЕ краш.
- **Снятие** — `SnapshotJob` (Shared.Etcd, снимает весь контур через
  `/v3/snapshot/save`, failover по endpoints); локальный том
  `/snapshots` (ретенция `RetentionFiles`) — кэш доводки.
- **Расписание**: плановый слепок раз в `IntervalMin=360` + **детекция
  изменений** — тик цикла (сон `ScanIntervalSec`, пульсирующий) читает
  revision кластера (`StatusAsync`): revision вырос, а последняя выгрузка
  старше `MinIntervalSec=300` → внеочередной слепок с выгрузкой. RPO
  контроль-плейна ≈ 5 мин при активности, без лишних слепков в тишине.
- **Выгрузка** — механика t08 целиком (переносится у PgWorker): пары
  `etcd/snapshot-<id>.db` + `.meta.json` (sha256/size/revision/taken/
  uploaded/instance — layout arch/19 §5 без изменений), ретенция N=28 пар
  (guard ≥1), статус-ключ с семантикой покрытия, CatchUp-доводка до снятия и
  повторный расчёт после, короткий сон `RetryIntervalSec=300` при
  отставании/FAILED, единый бюджет попытки `TimeoutSec=30`, пульс конечного
  S3-вызова (arch/14 §6). Статус-ключ — `/etcdworker/snapshots` (формат §5;
  поле `enabled` не пишется — выгрузка безусловна, «не настроено» отражает
  FAILED `etcd-backup-missing`).
- **Обслуживание контура** (compact + defrag каждой ноды последовательно, не
  чаще `MaintenanceIntervalMin=60`) — этот же лидерный цикл (перенесено у
  PgWorker; второй исполнитель исключён).
- S3-клиент — AWSSDK-клиент по образцу `BackupS3` (per-попыточный таймаут,
  SHA256-проверка put, программное доверие CA https-endpoint из
  `S3:CaBundlePath`); bucket — константа `etcd-backups` (§4), credы/endpoints —
  из резолва §4, никаких per-install env-пар S3 у воркера НЕТ.

## 5. Контракт etcd (домен `/etcd/` + координация `/etcdworker/`)

| Ключ | Формат | Пишет | Назначение |
|---|---|---|---|
| `/etcd/config` | JSON `{"nodes":N,"created_unix":T}` | API воркера (панель) | декларация числа managed-узлов; 0/отсутствие — только наблюдение (external-контур, стенд) |
| `/etcd/nodes/<k>/state` | строка `NOT_INITIALIZED\|PROVISIONING\|RUNNING\|UNREACHABLE\|REMOVING` | воркер | жизненный цикл managed-узла (семантика kafka/valkey/s3) |
| `/etcd/nodes/<k>/placed_on` | строка — имя docker-хоста (Plain — единственный; Swarm — swarm-нода) | воркер | фактическое размещение (после резолвции размещения §2) |
| `/etcdworker/leader` | lease TTL 15 с | воркер | глобальный лидер singleton-цикла (слепки/обслуживание/доводка состава) |
| `/etcdworker/locks/portalloc` | lease TTL 15 с | воркер | глобальный portalloc-клэйм (t90-паттерн) |
| `/etcdworker/instances/<id>` | lease TTL 15 с | воркер | живость инстансов |
| `/etcdworker/api/<id>` | lease TTL 15 с, JSON `{"url","instance","since_unix","cert_thumbprint"?}` | воркер | дискавери API (панель — единственный читатель мутаций) |
| `/etcdworker/portalloc` | JSON `{"<k>":{"host":"h1","client":18100}}` | воркер | закрепление host-портов клиентской грани managed-узлов (peer — имена сети, без host-портов; переживает rebuild) |
| `/etcdworker/work` | JSON `{"op","phase","updated_unix","instance","last_error"?}` | воркер | журнал фаз состава (journal-before-manipulations) |
| `/etcdworker/snapshots` | JSON `{"state":"OK"\|"FAILED","last_uploaded_unix"?,"last_object"?,"last_sha256"?,"size_bytes"?,"interval_min"?,"error"?}` | воркер (инстанс-исполнитель выгрузки) | статус DR-выгрузки (семантика покрытия: `last_uploaded_unix` — метка ПОКРЫВАЕМОГО слепка, двигается каждым успешным проходом; поле `enabled` не пишется — выгрузка безусловна, «не настроено» отражает FAILED с маркером `etcd-backup-missing`; `interval_min` — эффективный интервал для панельного stale-алерта 2×interval) |

Панель читает `/etcd/config`, `/etcd/nodes/*`, `/etcdworker/{api,snapshots}`;
координационные ключи — вне панельного контракта. Unknown-ключи — не ошибка
(толерантные парсеры, как pg/kafka/valkey/s3).

## 6. Надёжность

- Идемпотентность каждого шага (перепроверка факта: member в списке?
  контейнер есть? порт закреплён?); takeover ≤ TTL 15 с; journal фаз.
- **Что теряется при отключении машины X**: data-dir узла на ней — локальное
  хранение; переживание — кворум остальных + слепки в S3 (RPO = интервал
  выгрузки: 360 мин планово / ≈5 мин при активности); замена члена —
  автоматикой §3.1. Потеря кворума — заморозка контроль-плейна (датаплейн
  живёт сам), восстановление — runbook.
- Watchdog зависших циклов, поллинг-инвариант долгих фаз, пульс конечных
  S3-вызовов — общий канон arch/14 §6.
- Смена адресов клиентских публикаций (`AdvertisedHost`, PortRange):
  peer-urls членов (имена сети) не меняются — переиздание client-публикаций
  переживает rebuild; `advertise-client-urls` существующего члена фиксируется
  в кластере — плановая смена клиентского адреса отдельного члена — вне
  штатных операций (замена члена пишет адреса нового).

## 7. Наблюдаемость

`/healthz` — §1.1; метрики — общий словарь arch/18 §2.2 (`worker_*`, Meter
`EtcdWorker`; доменных серий не вводит — панельный канал статуса
достаточен). Диагностика: `/etcdworker/work`, `/etcd/nodes/*/state`,
`/etcdworker/snapshots`; панель — грань etcd (состав: декларация/managed/
external/health + форма N) и карточка «etcd-снапшоты» грани «Хранилище
бэкапов» (adminpanel/03).

## 8. Конфигурация (appsettings + env)

```
EtcdWorker:Etcd { Endpoints[] }                    # ЖИВОЙ контур (arch/04 §8)
EtcdWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                    Network="etcdw-net", PortRange{From,To}, Images{Etcd},
                    Tls, Ssh, AdvertisedHost }
                                                   # Network — общая сеть контура
                                                   # (Plain — ensure-bridge
                                                   #  воркером; Swarm — overlay
                                                   #  воркера; стенд/E2E — сеть
                                                   #  контура поставки);
                                                   # Plain — Hosts ровно ОДИН хост
                                                   # (fail-fast: мультихост —
                                                   #  Swarm); AdvertisedHost —
                                                   # только клиентская грань
                                                   # публикаций (§2)
EtcdWorker:Snapshots { Dir="/snapshots", RetentionFiles=10,
                       IntervalMin=360, MinIntervalSec=300,
                       MaintenanceIntervalMin=60,
                       RetentionObjects=28, RetryIntervalSec=300, TimeoutSec=30,
                       S3 { CaBundlePath? } }      # §4; bucket — константа
                                                   # etcd-backups (создаёт
                                                   # S3Worker); creds/endpoints
                                                   # — из /s3/clusters/
                                                   # (etcd_backup)
EtcdWorker:Thresholds { NodeBootSec=100 (≤100), NodeDeadSec=300,
                        HealthIntervalSec=15 }
EtcdWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5,
                   Watchdog{ Enabled=true, Multiplier=2, CheckIntervalSec=15 } }
EtcdWorker:Api { AdvertiseUrl, Tls{...} }          # mTLS, §1.1
# env-секреты ETCDW_*: ETCDW_API_TLS_{CERT,KEY,CLIENT_CA},
#   ETCDW_DOCKER_TLS_{CA,CERT,KEY}, ETCDW_DOCKER_SSH_KEY[_PATH],
#   ETCDW_DOCKER_SSH_FINGERPRINT
```

Fail-fast старта: endpoints etcd, AdvertiseUrl (https), docker-топология
(Plain — ровно один Hosts + непустое Network; Swarm — SwarmManager), PortRange,
пороги (NodeBootSec ≤ 100, NodeDeadSec > 0), интервалы слепков
(IntervalMin > 0, MinIntervalSec > 0, RetentionObjects ≥ 1, RetryIntervalSec >
0, TimeoutSec > 0). Per-install S3-пар НЕТ (цель резолвится из `/s3/`).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Ложная авто-замена при ребуте хоста дольше 300 с (member удалён, данные узла устарели) | принятый риск (решение авто-замены): порог NodeDeadSec настраиваем; кворум не проседает (learner+promote); осиротевший data-dir сносится при возврате хоста; данные контура — на кворуме+слепках |
| R2 | Операции состава разрушают кворум | гварды: все операции при здоровом кворуме; remove — не лидера и с остатком ≥ 3 членов; add — learner (безголосый) до catch-up |
| R3 | Advertise-адрес клиентской грани managed-узла не резолвится потребителями вне сети контура | `advertise-client-urls` — список «имя сети + advertised-host:client-port» (прецедент as-etcd); в список — только резолвимые URL (правило NXDOMAIN arch/04 §8 п.3); проверка — health-пробы воркера + панель |
| R4 | Клиенты (env endpoints) не знают адреса нового узла | клиенты работают по списку с failover — замена одного узла не роняет их; полный список обновляется env поставки (runbook-шаг после замен); панель показывает member list (advertise-client-urls) |
| R5 | Двойной исполнитель maintenance/экспорта (легаси PgWorker) | зона PgWorker демонтирована (arch/14 §6/§8) |
| R6 | etcd-backup кластер не заявлен/удалён — выгрузка невозможна | FAILED `etcd-backup-missing` + панельный алерт; слепки продолжают сниматься локально (том), CatchUp догонит после появления кластера |
| R7 | DR-окно ротации спец-ключа (R7 arch/24) | слепок до ротации несёт старое значение secrets — восстановление даёт устаревший ключ до свежей записи; лечение — заявка ротации (runbook arch/24 §9 R7) |
| R8 | Plain-контур живёт на ОДНОМ docker-хосте: потеря хоста = потеря всего контура (кворум не переживается) | осознанное следствие адресации по именам сети (мультихост — Swarm): переживание — DR-слепки в S3 (восстановление — runbook); отказоустойчивый контур = Swarm с расносом узлов по swarm-нодам (§2) |
| R9 | Seed-узел остаётся в контуре после доводки (деградация кворума при его незапланированной смерти) | runbook-шаг вывода seed после доводки состава (arch/04 §8 п.4); панель показывает состав (managed/external) — висящий seed виден |

## 10. Дальше

- Рецепт контура (бутстрап, параметры, кворум) — [04-deploy-etcd.md](04-deploy-etcd.md) §8.
- Хранилище слепков (layout `etcd/`) — [19-backups.md](19-backups.md) §5;
  S3-домен — [23-s3-clusters.md](23-s3-clusters.md) (§2 признак `etcd_backup`,
  §6 креды).
- Образец воркера — [24-s3worker.md](24-s3worker.md) (каркас, docker-модель,
  placement); паттерны PgWorker — [14-pgworker.md](14-pgworker.md).
- Восстановление из слепков — runbook `docs/runbook.md`; потеря кворума —
  [09-troubleshooting.md](09-troubleshooting.md) §4.

---

→ Указатель — [README.md](README.md). Панельная проекция домена —
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §2.3.5,
[adminpanel/03-panels.md](adminpanel/03-panels.md).
