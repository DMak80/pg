# EtcdWorker: эксплуатация

Уровень 3. Функция «эксплуатация»: циклы и лидерство, надёжность,
наблюдаемость, конфигурация и риски. Процессы —
[A](processes/a-membership.md)…[C](processes/c-snapshot-export.md);
перечисления — [processes/enums.md](processes/enums.md); общие принципы —
[synchronization/README.md](../synchronization/README.md).

## 1. Циклы и лидерство

Два инстанса воркера работают одновременно; исполняет работу только
глобальный лидер (`/etcdworker/leader`, lease TTL 15 с). Потеря
leader-lease прерывает тик (cancellation-гвард, S5
[synchronization](../synchronization/README.md)); цикл продолжает как не-лидер и
перезахватывает лидерство ≤ TTL + тик скана.

| Цикл | Что делает | Сон |
|---|---|---|
| **ReconcileLoop** | доводка состава под лидер-клэймом: кворум-гейт (лидер известен) → надзор-наблюдение (факты проблем) → `MembershipProcess` (недобор/перебор/замена) → repair (один шаг) → orphan-cleanup | `ScanIntervalSec` (пульсирующий) |
| **SnapshotLoop** | DR-слепки: доводка отставания → решение снятия (расписание + revision) → снятие → выгрузка → обслуживание контура | отставание/`FAILED` — `RetryIntervalSec`; здоровый контур — `ScanIntervalSec` |
| **KeepaliveLoop** | lease-клэймы и живость инстанса | `KeepaliveSec` |

Порядок внутри тика доводки фиксирован: наблюдение (собирает факты ДО
доводки) → membership (может вывести/добавить члена) → repair → orphan-cleanup
— последние два перечитывают состав и объекты на момент решения
([03-node-supervision.md](03-node-supervision.md) §2–§3).

## 2. Клэймы, идемпотентность, takeover

- **Идемпотентность**: каждый шаг перепроверяет факт (member в списке?
  learner уже voting? контейнер есть и порт совпадает? слепок выгружен?);
  повтор тика доигрывает с записанной journal-фазы и фактического состояния.
- **Takeover** ≤ TTL 15 с: лидер-клэйм, журнал, portalloc — в etcd; смерть
  лидера не оставляет «полу-руководства»: member-операции атомарны на
  стороне etcd, docker-операции идемпотентны, журнал — носитель возобновляемости
  ([00-etcd-contract.md](00-etcd-contract.md) §2).
- **Одна структурная операция за тик** (add/remove/replace) и **один
  repair-шаг за тик** — кворум не проседает серией параллельных операций.
- **Атомарность**: декларация — RMW по `mod_revision`; порт + member-add —
  одна секция под глобальным portalloc-клэймом; статус выгрузки — put поверх
  (семантика покрытия).

## 3. Надёжность

- **Что теряется при отключении машины X**: data-dir узла на ней — локальное
  хранение без гарантий durability; переживание — кворум остальных узлов +
  слепки в S3 (RPO = интервал выгрузки: 360 мин планово / ≈5 мин при
  активности); замена члена — автоматикой ([02-membership.md](02-membership.md) §4).
  Потеря кворума — заморозка контроль-плейна (датаплейн живёт сам),
  восстановление — [§8](#8-восстановление-etcd-из-s3-выгрузки),
  [pg-clusters/09-troubleshooting.md](../pg-clusters/09-troubleshooting.md) §5.
- **Том слепков** (`/snapshots`) — кэш доводки: гарантией не является,
  источник истины для восстановления — выгруженные объекты S3.
- **Watchdog зависших циклов**, поллинг-инвариант долгих фаз (серия проб
  молчащих узлов отмечает прогресс между узлами), «пульс конечного внешнего
  вызова» (S3-шаги выгрузки) — общий канон
  [pgworker/08-operability.md](../pgworker/08-operability.md) §3.
- **Слепые пробы**: слепая health-проба не даёт фактов для авто-замены; слепая
  docker-инспекция — никаких действий ([03-node-supervision.md](03-node-supervision.md) §4).
- **Смена адресов клиентских публикаций** (`AdvertisedHost`, `PortRange`):
  peer-urls членов (имена сети) не меняются — переиздание client-публикаций
  переживает rebuild; `advertise-client-urls` существующего члена
  фиксируется в кластере — плановая смена клиентского адреса отдельного
  члена вне штатных операций (замена члена пишет адреса нового).
- **Единственный исполнитель** maintenance и экспорта — лидерный SnapshotLoop
  (второго исполнителя нет; зоны PgWorker демонтированы).

## 4. Наблюдаемость

`/healthz` — секции: `etcd-reachable` (активная проба range по endpoints),
`docker-hosts` (ping каждого хоста конфигурации), `loops-alive` (возраст
последнего keepalive-тика; порог — общий хелпер `LoopStaleness`),
`snapshot-export` (последняя выгрузка: OK/FAILED/возраст — **информационная**
секция: FAILED-выгрузка не деградирует health, её видит панельный алерт;
«ключа ещё нет» — стартовое окно первого слепка). Недоступный etcd /
docker-хост / зависший цикл — `Degraded`; чек всегда отдаёт структуру
(catch-all).

**Единая правда для панели**: опрос `/healthz` живых инстансов по URL из
`/etcdworker/api/<id>` ([05-api.md](05-api.md) §3).

Метрики — общий словарь [metrics/README.md](../metrics/02-dictionary.md) §2
(`worker_*`, Meter `EtcdWorker`; доменных серий воркер не вводит —
панельный канал статуса достаточен); счётчики API по эндпоинтам и
фактическим кодам. Диагностика: `/etcdworker/work`,
`/etcd/nodes/<k>/state`, `/etcdworker/snapshots`.

## 5. Конфигурация (appsettings + env)

```
EtcdWorker:Etcd { Endpoints[] }                    # ЖИВОЙ контур (../pg-clusters/04-etcd-dcs.md)
EtcdWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                    Network="etcdw-net", PortRange{From=18100,To=18199},
                    Images{Etcd="quay.io/coreos/etcd:v3.5.21"},
                    Tls, Ssh, AdvertisedHost }
                                                   # Network — общая сеть контура
                                                   # (Plain — ensure-bridge
                                                   #  воркером; Swarm — overlay
                                                   #  воркера; стенд/E2E — сеть
                                                   #  контура поставки);
                                                   # Plain — Hosts ровно ОДИН хост
                                                   # (fail-fast: мультихост —
                                                   #  Swarm); AdvertisedHost —
                                                   #  только клиентская грань
                                                   #  публикаций (01 §3)
EtcdWorker:Snapshots { Dir="/snapshots", RetentionFiles=10,
                       IntervalMin=360, MinIntervalSec=300,
                       MaintenanceIntervalMin=60,
                       RetentionObjects=28, RetryIntervalSec=300, TimeoutSec=30,
                       S3 { CaBundlePath? } }      # 04; bucket — константа
                                                   # etcd-backups (создаёт
                                                   # S3Worker); creds/endpoints
                                                   # — из /s3/clusters/
                                                   # (etcd_backup)
EtcdWorker:Thresholds { NodeBootSec=100 (≤100), NodeDeadSec=300,
                        HealthIntervalSec=15 }
EtcdWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5,
                   Watchdog{ Enabled=true, Multiplier=2, CheckIntervalSec=15 } }
EtcdWorker:Api { AdvertiseUrl (https), Tls{...} }  # mTLS, 05
# env-секреты ETCDW_*: ETCDW_API_TLS_{CERT,KEY,CLIENT_CA},
#   ETCDW_DOCKER_TLS_{CA,CERT,KEY}, ETCDW_DOCKER_SSH_KEY[_PATH],
#   ETCDW_DOCKER_SSH_FINGERPRINT
```

Fail-fast старта: endpoints etcd; `AdvertiseUrl` непустой и `https` (или
`AllowInsecureHttp` для WAF-тестов); docker-топология (Plain — ровно один
`Hosts` + непустая `Network`; Swarm — `SwarmManager`); `PortRange`
(0 < From < To ≤ 65535); пороги (`NodeBootSec` ≤ 100, `NodeDeadSec` > 0,
`HealthIntervalSec` > 0); интервалы слепков (`IntervalMin` > 0,
`MinIntervalSec` > 0, `RetentionFiles`/`RetentionObjects` ≥ 1,
`RetryIntervalSec` > 0, `TimeoutSec` > 0, `MaintenanceIntervalMin` > 0).
Per-install S3-пар НЕТ (цель резолвится из `/s3/`).

## 6. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Ложная авто-замена при ребуте хоста дольше `NodeDeadSec` (member удалён, данные узла устарели) | принятый риск (решение авто-замены): порог настраиваем; кворум не проседает (learner + promote); осиротевший data-dir сносится при возврате хоста (03 §3); данные контура — на кворуме + слепках |
| R2 | Операции состава разрушают кворум | гварды: операции только при здоровом кворуме; вывод перебора — не лидера и с остатком ≥ 3 членов; авто-замена — не лидера (≥3-гвард не применяется: замена сохраняет итоговый состав, мёртвый член не голосует — 02 §4); add — learner (безголосый) до catch-up |
| R3 | Advertise-адрес клиентской грани managed-узла не резолвится потребителями вне сети контура | `advertise-client-urls` — список «имя сети + advertised-host:client-port» (прецедент `as-etcd`); в список — только резолвимые URL (правило NXDOMAIN [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §4); проверка — health-пробы воркера + панель |
| R4 | Клиенты (env endpoints) не знают адреса нового узла | клиенты работают по списку с failover — замена одного узла их не роняет; полный список обновляется env поставки (runbook-шаг после замены); панель показывает member list (`advertise-client-urls`) |
| R5 | Двойной исполнитель maintenance/экспорта (легаси PgWorker) | зона PgWorker демонтирована ([pgworker/08-operability.md](../pgworker/08-operability.md) §3/§5) |
| R6 | etcd-backup кластер не заявлен/удалён — выгрузка невозможна | `FAILED` `etcd-backup-missing` + панельный алерт; слепки продолжают сниматься локально (том), доводка догонит после появления кластера |
| R7 | DR-окно ротации спец-ключа: спец-ключ живёт в env S3Worker и копией в secrets-ключе etcd; слепок, снятый до ротации, несёт старое значение — восстановление даёт etcdworker'у устаревший ключ до первой свежей записи | фиксируется риск-строкой; лечение — пере-подстановка значения через заявку ротации ([s3worker/08-operability.md](../s3worker/08-operability.md) §6 R7); механик DR нет |
| R8 | Plain-контур живёт на ОДНОМ docker-хосте: потеря хоста = потеря всего контура (кворум не переживается) | осознанное следствие адресации по именам сети: переживание — DR-слепки в S3 (восстановление — [§8](#8-восстановление-etcd-из-s3-выгрузки)); отказоустойчивый контур = Swarm с разносом узлов по swarm-нодам (01 §4) |
| R9 | Seed-узел остаётся в контуре после доводки (деградация кворума при его незапланированной смерти) | runbook-шаг вывода seed после доводки состава ([§7](#7-ha-etcd-контур-подъём-стендовое-зеркало-чеки)); панель показывает состав (managed/external) — висящий seed виден |

## 7. HA-etcd контур: подъём, стендовое зеркало, чеки

Канон — [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2
(адресация по именам узлов в общей сети, бутстрап seed-узлом, состав доводит
EtcdWorker; параметры, advertised-правила потребителей, кворум-семантика,
чек-лист). Кратко:

- **Подъём (прод)**: `docker network create etcdw-net` (если нет) →
  `deploy/etcd` — рецепт ОДНОГО seed-узла: в `etcd.env` — `NODE_NAME` (имя
  узла в сети, peer `http://<NODE_NAME>:2380`), образ
  `192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`;
  `docker compose --env-file etcd.env up -d` (контур 1/1) → воркеры/панель
  получают endpoints (`*_ETCD_ENDPOINT_0..2`,
  `AdminPanel__Etcd__Endpoints__0..2`; на старте — адрес seed) → заявить
  состав `PUT /api/etcd/config {"nodes":N}` (панель «etcd» → форма
  «Поддерживать N узлов») → EtcdWorker доводит состав (learner + promote;
  мультихост — Swarm: узлы разносятся по swarm-нодам) → вывести seed
  (member remove + `docker compose down`) — контур полностью managed.
- **Стендовое зеркало**: дев-стенд поднимает 3-узловой контур внешними
  compose-узлами (`as-etcd-1/2/3`, peer по именам compose-сети, публикации
  2379/2381/2383, env `ETCD{1,2,3}_HOST_PORT`); EtcdWorker стенда —
  наблюдение (декларация не ставится).
- **Чеки**: стендовый `dev-stand/adminpanel/checks/43-etcd-ha.sh` (member list
  3 started → stop узла: запись/healthz/master-ключ живы → start → 3/3);
  прод-чек-лист — [pg-clusters/04-etcd-dcs.md](../pg-clusters/04-etcd-dcs.md) §2
  (member list — все peer-URL имена; `endpoint health --cluster`;
  панель `/api/etcd/status` = N членов/единый leader).
- **Потеря узла/кворума**: 1 узел — не-событие (кворум жив); большинство
  недоступно — контроль-плейн заморожен, датаплейн живёт сам, восстановление
  — [pg-clusters/09-troubleshooting.md](../pg-clusters/09-troubleshooting.md) §5;
  managed-узел навсегда — авто-замена EtcdWorker
  ([processes/b-node-supervisor.md](processes/b-node-supervisor.md),
  [02-membership.md](02-membership.md) §4); seed/внешний —
  `member remove` руками (peer-URL нового — имя в той же сети контура).

## 8. Восстановление etcd из S3-выгрузки

Снапшоты etcd (контроль-плейн: декларации кластеров, маршрутизация, секреты,
клэймы, portalloc) снимает и выгружает EtcdWorker ([04-snapshots.md](04-snapshots.md))
в фиксированный bucket `etcd-backups` на нодах etcd-backup S3-кластера
(признак `etcd_backup=true` в `/s3/clusters/`, [s3worker/README.md](../s3worker/README.md);
bucket создаёт S3Worker — [s3-clusters/README.md](../s3-clusters/README.md)):
префикс `etcd/`, пары `snapshot-<id>.db` + `snapshot-<id>.meta.json`
(sha256/размер/ревизия/instance), каденс — плановый интервал + детекция
изменений. Потеря docker-хоста вместе с etcd — восстановимая авария.

1. **Диагностика**: подтвердить наличие выгрузок —
   `mc ls --recursive <alias>/<bucket>/etcd/` — выбрать новейший
   `snapshot-<id>.db` и его `.meta.json`. Пусто → см. границы (п.6).
2. **Проверка целостности ДО восстановления** (обязательный шаг):
   скачать `.db`; сверить sha256 файла с `sha256` из `.meta.json` — ручная
   сверка и есть операторский барьер (ловит порчу до restore); затем
   `etcdctl snapshot status <file>` — информационный шаг (структура/размер/
   ревизия слепка; etcd 3.5.x печатает hash, но при status его НЕ сверяет —
   целостность им не доказывается); restore дополнительно верифицирует сам
   слепок (`etcdutl snapshot restore` проверяет sha256 при разворачивании,
   шаг 4). Расхождение sha256 или провал restore ⇒ взять предыдущий слепок
   (ретенция хранит N последних пар); файл с битым хешем не разворачивать
   НИКОГДА.
3. **Свежий etcd**: контейнер(ы) etcd с чистым data-dir
   (`INITIAL_CLUSTER_STATE=new`). Прод-3-ноды: сначала single-member,
   остальные — `etcdctl member add` после старта.
4. **Restore**: остановить etcd → `etcdutl snapshot restore <file>
   --data-dir=<data-dir>` (restore — офлайн-операция над слепком;
   верифицирует sha256 слепка при разворачивании — финальный барьер
   целостности) → стартовать etcd на восстановленном data-dir.
5. **Контроль**: `etcdctl endpoint health`; воркеры/панель переподключаются
   сами (poll-тик; клэймы переснимутся lease-механикой; R7
   [pgworker/08-operability.md](../pgworker/08-operability.md) §6: journal
   мог откатиться к точке слепка — процессы идемпотентны и доводят фазы);
   панель видит кластеры, healthz воркеров зелёные. Статус-ключ
   `/etcdworker/snapshots` появится первым же слепком EtcdWorker.
6. **Границы**: слепок = точка его снятия (RPO = окно от последнего
   изменения до выгрузки — детекция изменений закрывает ≈5 мин при
   активности, транзиент S3 — доводкой тика `RetryIntervalSec`); локальные
   тома снапшотов воркеров (если пережили) — альтернативный источник того
   же формата; WAL-данные PG-шардов живут в S3 бэкапов независимо
   ([backups/README.md](../backups/README.md)) — восстановление etcd их не касается. Алерты панели:
   `etcd-snapshot-export-failed` (выгрузка провалилась — проверить
   etcd-backup S3-кластер: заявлен признаком, ноды живы, secrets читаются),
   `etcd-snapshot-export-stale` (выгрузка молчит — обе инстанции
   EtcdWorker лежат или вечный transient), `etcd-backup-cluster-missing`
   (кластер с признаком не заявлен).
