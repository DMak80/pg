# 23. S3-кластеры хранилища бэкапов: контракт etcd (контроль-плейн + дискавери) ★

Канон ключей S3-домена: контроль-плейн кластеров `/s3/` (декларирует панель
AdminPanel через HTTP API воркера — исполняет S3Worker, канон —
[24-s3worker.md](24-s3worker.md)), координация воркера `/s3worker/` и
**клиентский дискавери** (`endpoints`). Назначение домена — **пул
S3-серверов хранилища бэкапов с гео-распределением** (привязка нод к
docker/swarm-хостам) и mesh-репликацией между нодами; потребитель —
подсистема бэкапов ([19-backups.md](19-backups.md)); для клиентов пул —
обычный S3 (§7). PgWorker — клиент пула (читатель `endpoints`), участником
контракта не является.

Имена: кластер `<C>` — `^[a-z][a-z0-9_]{0,62}$` (как pg/kafka/valkey; без
дефиса); инстансы `<I>` — уникальные произвольные имена того же паттерна
(задаёт панель; генерация `node<k>` НЕ используется), ≤ 9; кластер может
иметь **0 и более инстансов** (декларация без развёртывания валидна). Имя
инстанса = ключ etcd (`nodes/<I>/…`) = имя docker-объектов
(`s3w-<C>-<I>…`, arch/24 §2) = имя target/коммуникации репликации
(`TargetRecord.Name`, [owns3/07-replication.md](../OwnS3/arch/owns3/07-replication.md)
§2). Имя `orchestrator` зарезервировано (сертификат оркестратора PKI, §6).

## 1. Транспорт: HTTP JSON gateway `/v3/*`

Как панель и PgWorker ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§1, [14-pgworker.md](14-pgworker.md) §3): `HttpClient` против gRPC-gateway
etcd (JSON+base64), `POST /v3/kv/range` / `/v3/kv/put` / `/v3/kv/txn` /
`/v3/lease/*`. Один общий etcd-контур со стендом. **Poll, без watch** —
тик воркера 5 с / панели 3 с покрывают динамику.

Два корневых префикса: `/s3/` (контроль-плейн кластеров) и `/s3worker/`
(координация воркера; панель читает избирательно — §8).

## 2. Ключи кластера `/s3/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"platform":"owns3","nodes":N,"etcd_backup"?:true,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE), воркер (снимает `state` после инициализации, актуализирует `nodes` при изменении состава (D/E), txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka/valkey); `platform` — единственное значение `"owns3"` (фиксируется при создании навсегда; точка расширения контракта); `nodes` — число инстансов заявки при создании (0..9 — кластер без инстансов валиден), после D/E — поддерживается воркером равным фактическому составу (структурный предел 0..9; вывод последнего инстанса запрещён — arch/24 §5 E, 0 достигается только созданием; операционная рекомендация 2–3 — гео-минимум и бюджет R1); `etcd_backup` — признак «бэкапный кластер etcd» (bool, default false, пишется только при `true`; фиксируется при создании навсегда — меняется пересозданием кластера, как `platform`; единственный в `/s3/clusters/` — валидация API создания, 409 при коллизии; требует полной спец-ключевой env-пары воркера — §6, arch/24 §1.1) |
| `secrets` | JSON `{"root_user","root_password","access_keys":[{"name","access_key","secret_key","policy"}…]}` — формат §2.1 | воркер (лениво при provisioning, txn put-if-absent; ротация — замена целиком, txn по `mod_revision`) | клиентские креды кластера (§6): root-пара админ-грани + статические access keys (`backups` read-write, `panel` read-only; etcd-backup кластер — `backups` из спец-ключа env); читатели — воркер (env инстансов, root админ-граней), PgWorker (ключ `backups`, §7), панель (ключ `panel`), etcdworker t32 (ключ `backups`); удаляется deprovisioning'ом (X3, префикс кластера); значения — только env контейнеров и S3-клиенты процессов, никогда журнал/логи/API/UI |
| `nodes/<I>/placement` | JSON-селектор атрибутов хоста: `{"docker_host":"h1"}` (plain) \| `{"swarm_node":"n1"}` \| `{"swarm_labels":{"geo":"dc1",...}}` \| комбинация атрибутов | панель через API воркера | явная привязка инстанса; модель «хост = набор атрибутов» (§4); привязка = предикат равенства по всем указанным атрибутам; изменение привязки живого инстанса — только выводом/перевозом инстанса (перепривязка на месте не допускается: данные тома привязаны к хосту) |
| `nodes/<I>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"200Gi"}` | панель через API воркера (мутация ресурсов) | форматы — канон pg ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.3; kafka — §10.3): cpu — десятичные ядра 0.01..64, mem/disk — целые GiB, в etcd `"<n>Gi"`; cpu/mem — лимиты контейнера; `disk` — целевой объём volume данных инстанса (декларация: создание volume и операционная квота-наблюдение; жёсткого enforcement-примитива у docker-volume нет — занятость наблюдается `usage` и алертами панели) |
| `nodes/<I>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — **только панель** (заявка / one-way маркер демонтажа инстанса); остальные — воркер | жизненный цикл инстанса — семантика kafka/valkey; `UNREACHABLE` ставит надзор по health-пробам (платформенные health-грани — §5) |
| `nodes/<I>/placed_on` | строка — имя docker-хоста `"h1"` | воркер (при резолвции placement) | фактическое размещение после резолвции селектора (§4); переживает rebuild (том инстанса живёт на этом хосте) |
| `nodes/<I>/usage` | JSON `{"used_bytes":N,"total_bytes":N,"updated_unix":T}` | воркер (надзор, периодически) | занятость диска инстанса из платформенной админ-грани/метрик; кормит квота-наблюдение `resources.disk` и панельные алерты |
| `nodes/<I>/pki` | JSON `{"certificate":"<PeerCertificate JSON, owns3/07 §2>","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}` | воркер (ленивый выпуск, txn put-if-absent) | материал инстанса репликационного PKI (§6): сертификат выпущен CA кластера, имя = `<I>`; **переживает provision/переезд контейнера** (контейнер пересоздаётся с тем же сертификатом); удаляется вместе с инстансом (E) |
| `links` | JSON `[{"a":"<I1>","b":"<I2>"},…]` | панель через API воркера (замена целиком) | **граф репликации** — произвольный набор связей (пар) между инстансами; full-mesh НЕ инвариант (частичный/незеркальный состав нормален: полнота и симметричность состава соседей у разных инстансов не контролируются — настройка состава зона оркестратора, зонд t33 сверяет только настроенные линки); связь пары реализуется join-механикой двусторонне (owns3/07 §2); применяет воркер (процесс G, arch/24 §5): join недостающих пар, снос лишних target-записей; валидация API: имена существуют, `a≠b`, уникальность пар |
| `denylist` | JSON `["<I1>",…]` | панель через API воркера (deny/allow инстанса) | **отзыв ключей** кластера — источник истины в etcd; операция «инстанс в deny-list» = отзыв его ключа: join с ним невозможен (owns3/07 §2 deny-list), существующие связи с ним сносятся воркером (G); список доводится воркером до всех RUNNING-инстансов, КРОМЕ DenyList-имён (отозванному собственный список отзывов не нужен — изоляцию обеспечивают записи у соседей и запрет будущих join его сертификата; `PUT /replication/denylist`, owns3/07 §2/§6); валидация API: имена существуют |
| `pki/ca` | JSON `{"name":"<C>","publicKeyPem":"-----BEGIN PUBLIC KEY-----…","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}` | воркер (ленивый выпуск при первом инстансе/deny, txn put-if-absent) | **CA кластера** репликации — ECDSA P-256 (механика owns3/07 §2: PeerCertificateAuthority); `publicKeyPem` — trust-anchor, передаётся каждому инстансу (`OWNS3_REPLICATION_TRUST_CA`); приватный ключ CA — только воркеру (выпуск сертификатов); ротация CA — вне (roadmap) |
| `pki/orchestrator` | JSON `{"certificate":"<PeerCertificate JSON>","private_key_pem":"…"}` | воркер (ленивый выпуск вместе с CA) | сертификат оркестратора (имя `orchestrator`, CA кластера) — подпись до-join запросов воркера (`PUT /replication/denylist`, owns3/07 §6) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; RMW при изменении состава) | клиентские адреса пула (advertised host + S3-порт из `/s3worker/portalloc/<C>`) — **точка дискавери клиентов**; существует ⟺ ≥1 инстанс |

Неизвестные ключи внутри `/s3/` — не ошибка: лог + счётчик `unknownKeys`
читателя (как pg/kafka/valkey; система развивается, парсеры не падают).

### 2.1. Канонические примеры значений (критерий приёмки парсеров)

`config` при создании (заявка, панель через API воркера):

```json
{"platform":"owns3","nodes":3,
 "created_unix":1760156800,"state":"NOT_INITIALIZED"}
```

`config` Active-кластера (после provisioning — поле `state` снято воркером):

```json
{"platform":"owns3","nodes":3,
 "created_unix":1760156800}
```

`config` с заявкой удаления: то же + `"state":"TO_REMOVE"`.

`config` кластера без инстансов (декларация без развёртывания; provisioning
проходит без docker-мутаций):

```json
{"platform":"owns3","nodes":0,
 "created_unix":1760156800}
```

`config` etcd-backup кластера (признак пишется только при `true`;
единственный в установке — 409 при коллизии):

```json
{"platform":"owns3","nodes":3,"etcd_backup":true,
 "created_unix":1760156800,"state":"NOT_INITIALIZED"}
```

`secrets` (клиентские креды кластера, §6; генерирует воркер; вместо
«<32 симв [A-Za-z0-9]>» — криптостойкие сгенерированные значения):

```json
{"root_user":"s3w-backups-root",
 "root_password":"<32 симв [A-Za-z0-9]>",
 "access_keys":[
   {"name":"backups","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-write"},
   {"name":"panel","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-only"}]}
```

`placement` — plain: `{"docker_host":"h1"}`; swarm: `{"swarm_node":"n1"}` и
`{"swarm_labels":{"geo":"dc1"}}` (комбинация атрибутов — конъюнкция, §4).

`resources`: `{"cpu":"2","mem":"4Gi","disk":"200Gi"}`.

`state`: `"RUNNING"`; `placed_on`: `"h1"`.

`nodes/<I>/pki` (значение — сертификат как экранированная JSON-строка):

```json
{"certificate":"{\"name\":\"east1\",\"publicKeyPem\":\"-----BEGIN PUBLIC KEY-----…\",\"issuedAtUnixMs\":1760156800000,\"expiresAtUnixMs\":0,\"caName\":\"backups\",\"signature\":\"…\"}",
 "private_key_pem":"-----BEGIN PRIVATE KEY-----…"}
```

`links` (частичный граф из трёх инстансов: east1↔east2, east1↔west1, связи
east2↔west1 нет):

```json
[{"a":"east1","b":"east2"},{"a":"east1","b":"west1"}]
```

`denylist`: `["west1"]`.

`pki/ca`:

```json
{"name":"backups",
 "publicKeyPem":"-----BEGIN PUBLIC KEY-----…",
 "private_key_pem":"-----BEGIN PRIVATE KEY-----…"}
```

`usage`:

```json
{"used_bytes":107374182400,"total_bytes":214748364800,
 "updated_unix":1760156860}
```

`endpoints`: `"h1:18001,h2:18002,h3:18003"` (advertised host + S3-порт из
`/s3worker/portalloc/<C>`).

## 3. Статусы наблюдения (связность и синхронность)

Контракт ключей фиксируется этим каноном; запись реализуется задачами трека
([roadmap/s3.md](roadmap/s3.md): зонд — t33, процесс F
[24-s3worker.md](24-s3worker.md) §5).

| Ключ | Формат | Кто пишет |
|---|---|---|
| `/s3/clusters/<C>/connectivity` | JSON `{"state":"OK"\|"DEGRADED"\|"DOWN","checked_unix":T,"links":{"node1":{"node2":"ok"\|"offline",...},...},"probe_error"?}` | зонд t33 (периодически + по команде API): связность нод между собой — активные репликационные ping-пробы: зонд опрашивает `POST /admin/replication/probe` ([owns3/07-replication.md](../OwnS3/arch/owns3/07-replication.md) §2/§6) каждой ноды — параллельный ping её соседей исходящим репликационным транспортом; матрица `links` — только НАСТРОЕННЫЕ линки (targets каждой ноды; полнота меша и зеркальность состава НЕ контролируются — настройка состава — зона оркестратора G, не зонда); ping-ок = «канал жив» — отдельно от доставки (лаг/очередь статуса) и сверки данных (sync-половина): в `connectivity` ложится только линк; линков не настроено — OK с пустой матрицей, отсутствие репликации отражает sync-агрегат (PUT-пробы связность ownS3 не детектируют: путь записи без синхронных взаимодействий с соседями — PUT всегда 0 с, [owns3/07-replication.md](../OwnS3/arch/owns3/07-replication.md) §1) |
| `/s3/clusters/<C>/sync` | JSON `{"state":"SYNCED"\|"LAGGING"\|"DIVERGED","checked_unix":T,"pairs":[{"from":"node1","to":"node2","missing":N,"extra":N,"lag_bytes"?,"lag_objects"?}...],"last_error"?}` | зонд t33: сверка содержимого bucket между нодами — пары по настроенным линкам (объединение targets, как у connectivity; `from`/`to` — упорядоченная пара по Ordinal); элемент сверки — `(key, modTimeUnixMs)`: `missing` — элементы from, отсутствующие у to (нет ключа или другая версия), `extra` — наоборот; листинги/хвосты (штатные метрики репликации лаг данных не отражают — факт 7 `../OwnS3/docs/s3-replication.md`; админ-грань ownS3 sync-report — R5 `../OwnS3/docs/owns3-replication-requirements.md`) |

`state`-агрегаты (по настроенным линкам): connectivity `DOWN` — нет живых
среди настроенных линков при непустом их составе; `DEGRADED` — часть
настроенных линков offline; пустой состав настроенных линков — `OK` с пустой
матрицей; sync `LAGGING` — догон известен и мал/растёт; `DIVERGED` —
устойчивое расхождение/дыра (алерты панели: расхождение, отставание,
нода вне контура — по `nodes/*/state`).

Форматы толерантны к расширению полей: парсеры панели читают известные
поля, новые не ломают чтение.

Состав/здоровье нод наблюдается `nodes/*/state` + `usage` — отдельного
health-ключа нет: состояние живости несёт `state`, детальная телеметрия —
метрики [18-metrics.md](18-metrics.md).

`connectivity` OK-кластера из 3 нод (full-mesh: строка ноды — исходы её
probe по собственным targets; линки симметричны):

```json
{"state":"OK","checked_unix":1760156900,
 "links":{"node1":{"node2":"ok","node3":"ok"},
          "node2":{"node1":"ok","node3":"ok"},
          "node3":{"node1":"ok","node2":"ok"}}}
```

`sync` SYNCED (пары без расхождений):

```json
{"state":"SYNCED","checked_unix":1760156900,
 "pairs":[{"from":"node1","to":"node2","missing":0,"extra":0},
          {"from":"node1","to":"node3","missing":0,"extra":0},
          {"from":"node2","to":"node3","missing":0,"extra":0}]}
```

## 4. Модель размещения (атрибуты хостов, предикат привязки)

- **Хост — набор атрибутов**: plain-режим — атрибут `docker_host` (имя из
  таблицы `S3Worker:Docker:Hosts[]` воркера, [24-s3worker.md](24-s3worker.md)
  §2); swarm — атрибуты `swarm_node` (hostname ноды swarm) и `swarm_labels`
  (`node.labels.*` — произвольные пары ключ-значение, в т.ч. гео-метки
  площадок).
- **Привязка ноды = предикат**: `placement`-селектор — конъюнкция равенств
  по атрибутам; нода кластера обязана иметь селектор (валидация API: пустой
  или невалидный — 400).
- **Резолюция** (воркер, фаза provisioning): по селектору — список
  кандидатов-хостов. Ноль кандидатов — transient-фейл фазы (journal, ретрай
  тиком: хост может появиться в конфигурации). Несколько кандидатов —
  детерминированный выбор (least-loaded по числу занятых портов/тасков,
  tie-break по имени; journal-запись с перечнем кандидатов). Результат —
  `placed_on`; повторные проходы идемпотентны (тот же хост, пока селектор
  не изменён выводом ноды).
- **Анти-аффинити** по отношению к другим нодам того же кластера НЕ
  вводится автоматикой: раснос по площадкам — обязанность оператора
  (гео-схема — явные селекторы); канон фиксирует рекомендацию «ноды одного
  кластера — на разных хостах/площадках» и индикацию совпадений
  панели.

## 5. Платформа инстансов

- **owns3** — единственная платформа пула: образ `owns3:dev` — внешний,
  собирается репозиторием `../OwnS3` (`deploy/build-image.sh` там);
  в registry не кладётся; инстанс — ownS3 single-drive (`OWNS3_DATA_DIR=/data`);
  bucket per-install ([19-backups.md](19-backups.md) §5) на каждом
  инстансе (CreateBucket admin-кредом, идемпотентно);
  mesh-репликация — внутренняя механика ownS3
  ([owns3/07-replication.md](../OwnS3/arch/owns3/07-replication.md); конфиг per-install
  — R6 `../OwnS3/docs/owns3-replication-requirements.md`): состав mesh инстансов
  кластера доводит S3Worker по декларации `links` (PKI — §6; процесс
  G — [24-s3worker.md](24-s3worker.md) §5). Health — `/healthz`
  ([owns3/05-access-config.md](../OwnS3/arch/owns3/05-access-config.md) §5);
  занятость диска — админ-грань `GET /admin/storage` (там же).
  Удаления ownS3 — физические (versioning отсутствует): ретенция
  arch/19 §4 работает напрямую, чистки версий не требуется.
- **Общее**: S3-порт контейнера 9000 → host-порт из portalloc; bucket
  per-install — единое имя на всех инстансах кластера (клиент с одним
  комплектом кредов работает с любым инстансом); создание/поддержание
  bucket — обязанность оркестратора (идемпотентно).

## 6. Секреты (контрактная часть)

Два независимых контура секретов кластера, оба — **per-cluster в etcd**
(переживают пересоздание контейнеров и смерть воркера): **креды клиентской
S3-грани** (ключ `secrets`, подраздел ниже) и **PKI репликации** (подраздел
«PKI репликации»): PKI репликации клиентские креды НЕ заменяет.

### Креды клиентской грани (per-cluster, etcd)

- Креды клиентской S3-грани (root-пара админ-грани + статические access
  keys) генерирует S3Worker при provisioning кластера и хранит в ключе
  `/s3/clusters/<C>/secrets` (формат — §2.1): пишет только воркер, ленивый
  txn put-if-absent (проигрыш гонки → re-read, первый записавший — истина);
  ротация — замена целиком, txn по `mod_revision` (§8).
- Состав: root-пара (`root_user` — детерминированное имя `s3w-<C>-root`,
  не секрет; `root_password` — генерируется) + access keys `backups`
  (read-write — джобы/агенты бэкапов PgWorker) и `panel` (read-only — грань
  «Хранилище бэкапов» панели). Генерация значений — криптостойкие 32 симв
  [A-Za-z0-9] (алфавит без спецсимволов — безопасно для env контейнера,
  JSON-значений и SigV4).
- **etcd-backup кластер** (`config.etcd_backup=true`, §2): состав тот же,
  формат един для всех кластеров, но ключ `backups` НЕ генерируется —
  подставляется из спец-ключа бэкапа etcd: env-пары воркера
  `S3W_ETCD_BACKUP_ACCESSKEY` / `S3W_ETCD_BACKUP_SECRETKEY` (полупара —
  fail-fast старта воркера, arch/24 §8; создание кластера с признаком без
  полной пары — 422, arch/24 §1.1). Root-пара и ключ `panel` генерируются
  как у всех. Потребитель спец-ключа — etcdworker (t32): читает значение из
  secrets-ключа однотипно с остальными клиентами.
- В инстансы передаётся env контейнера (значения из secrets-ключа):
  `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` +
  `OWNS3_ACCESS_KEYS__<i>__*` (формат — прецедент
  [owns3/05-access-config.md](../OwnS3/arch/owns3/05-access-config.md) §2);
  env — транспорт доставки значений, не хранилище (источник правды — etcd).
- Экспозиция: парсеры панели разбирают secrets для внутреннего S3-клиента
  грани «Хранилище бэкапов» (ключ `panel`); в UI/API значения не отдаются —
  только имена/политики (adminpanel/02 §12). PgWorker читает ключ `backups`
  (§7).
- Отсутствие/битость/неполнота secrets-ключа существующего кластера —
  отказ фазы/диагностика процессов воркера (ретраи тиком), НЕ тихая
  перегенерация: живые инстансы с прежним env разошлись бы с ключом.
- Ротация — заявка `/s3worker/rotations/<C>` (§8): генерация новых
  root+ключей (etcd-backup — спец-ключ пере-подставляется из env) → замена
  secrets целиком (txn по `mod_revision`) → rolling-пересоздание инстансов
  по одному за тик с новым env; env-сверка контейнера — по `root_user` +
  составу `OWNS3_ACCESS_KEYS__<i>` (имена/состав неизменны, значения
  новые); поддержка рестарт-семантики платформ: ownS3 — переконфигурация
  env + рестарт
  ([owns3/05-access-config.md](../OwnS3/arch/owns3/05-access-config.md) §2).

### PKI репликации (per-cluster, etcd)

- **CA кластера и выданные им сертификаты инстансов генерирует S3Worker и
  хранит в etcd** (ключи §2: `pki/ca`, `pki/orchestrator`,
  `nodes/<I>/pki`) — ключи переживают provision/переезд контейнера:
  контейнер пересоздаётся с тем же сертификатом (том и env пересоздаются,
  материал в etcd). Криптомеханика — канон owns3/07 §2 (ECDSA P-256,
  peer-сертификаты + trust-anchor CA + join): `CreateCa`/`IssueServer`
  клиентской копии peer-PKI ownS3 (`S3Worker.Core/OwnS3/Protocol/PeerAuth`;
  канон — `../OwnS3/arch/owns3/07` §2).
- Выпуск — лениво, по мере надобности (первый инстанс / deny-доведение),
  txn put-if-absent: CA (имя = `<C>`), сертификат оркестратора (имя
  `orchestrator`, для до-join контура `PUT /replication/denylist`,
  owns3/07 §6), сертификаты инстансов (имя = `<I>`). Сертификаты —
  **бессрочные** (`expiresAtUnixMs=0`): отзыва через deny-list достаточно,
  плановая ротация ключей/CA — вне (roadmap, owns3/07 §12).
- В инстансы передаётся env контейнера (значения, не пути — прецедент
  owns3/07 §11): `OWNS3_REPLICATION_CERT` (JSON сертификата инстанса),
  `OWNS3_REPLICATION_PRIVATE_KEY` (PEM),
  `OWNS3_REPLICATION_TRUST_CA` (trust-anchor JSON `pki/ca`:
  `{"name","publicKeyPem"}`),
  `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` (`scheme://host:port` —
  advertised host + S3-порт инстанса из `/s3worker/portalloc/<C>`).
- Состав mesh НЕ передаётся env (env-seed — legacy-only, owns3/07 §2):
  доводка состава — админ-гранью/join (процесс G, arch/24 §5).
- Приватные ключи CA/инстансов читает только воркер (панель управление
  ключами не показывает — полностью скрыто бэком); deny-list кластера
  (§2) — операция отзыва: имя = ключ инстанса в
  `PUT /replication/denylist` всех инстансов.

## 7. Клиентская грань пула

Пул для клиентов — `endpoints`-набор обычных S3-серверов: запись — перебор
«любой доступный» с per-попыточным таймаутом PUT (health-проба
недоступность не доказывает — факт 8 `../OwnS3/docs/s3-replication.md`; быстрый
отказ/успех, не молчание — R3
`../OwnS3/docs/owns3-replication-requirements.md`); чтение/восстановление — по
приоритетам per-cluster (страховка на лаг доставки); синхронизация между
серверами — не забота клиентов. Layout объектов —
[19-backups.md](19-backups.md) §5 без изменений. Канон фиксирует границу;
реализация — PgWorker, правки контракта — arch/19/arch/14.

Реализация (фиксируется): PgWorker — читатель
`/s3/clusters/<C>/{config,nodes/<I>/state,endpoints,secrets}` (ключ
`secrets` — креды `backups`, read-write, [19-backups.md](19-backups.md) §7)
плюс `/s3worker/portalloc/<C>` для резолва имён инстансов в адреса;
PgWorker в `/s3/` НЕ пишет. Списки приоритетов записи per-PG-кластер живут в policy
бэкапов ([19-backups.md](19-backups.md) §4) ссылками на имена инстансов
(`write_master`/`write_replica` — по роли источника, не по роли ноды);
чтение — все ноды пула без приоритетов (порядок `endpoints`-ключа).

## 8. Координация воркера `/s3worker/`

Порт схемы `/kafkaworker/` ([15-kafka-clusters.md](15-kafka-clusters.md)
§4) / `/valkeyworker/` ([20-valkey-clusters.md](20-valkey-clusters.md) §3)
один в один, свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/s3worker/leader` | lease TTL 15 с | лидер singleton-процессов |
| `/s3worker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/s3worker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` — сохранение накопленных треков S6 ([17-synchronization-principles.md](17-synchronization-principles.md)) |
| `/s3worker/portalloc/<C>` | обычный | `{"<I>":{"host":"h","s3":18001}}` — закрепление S3-портов за инстансами, переживает rebuild |
| `/s3worker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (t90-паттерн: взаимоисключение секции довыделения портов; txn `version==0` + put-with-lease) |
| `/s3worker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/s3worker/api/<id>` | lease TTL 15 с | дискавери API воркера: `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; читает панель |
| `/s3worker/rotations/<C>` | обычный | заявка ротации клиентских кредов кластера `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn; del воркером по завершении): генерация новых root+ключей → замена `secrets` целиком (txn по `mod_revision`) → rolling-пересоздание инстансов по одному за тик с новым env (§6) |
| `/s3worker/probes/<C>` | обычный | заявка внепланового зонда синхронности/связности `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn put-if-absent; del зонда F по исполнении, [24-s3worker.md](24-s3worker.md) §5 F) |

Панель читает из `/s3worker/` только `api/` и `rotations/`;
остальные ключи — координационные, вне панельного контракта.

`portalloc`:

```json
{"east1":{"host":"h1","s3":18001},
 "east2":{"host":"h2","s3":18002}}
```

## 9. Панель (проекция)

Панель читает `/s3/` и `/s3worker/{api,rotations}/`, мутирует только через
API S3Worker (панель в etcd домена S3 не пишет ничего); проекция —
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §12.

Совместимость с механикой панели: unknownKeys-толерантность (§2), битый
JSON — parseError-запись без падения тика, `state`-семантика
«отсутствие = Active».

---

→ Указатель — [README.md](README.md). Исполнительная сторона контракта —
[24-s3worker.md](24-s3worker.md); мутации панели — через его HTTP API
(§1.1 там).
