# S3-кластеры: ключи кластера `/s3/clusters/<C>/`

Уровень 3. Полный контракт ключей контроль-плейна домена, кто и когда
пишет, канонические примеры значений. Координация воркера —
[08-coordination.md](08-coordination.md); секреты и их ротация —
[06-secrets.md](06-secrets.md); статусы зонда — [03-observation-statuses.md](03-observation-statuses.md).

## 1. Ключи

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"platform":"owns3","nodes":N,"etcd_backup"?:true,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE), воркер (снимает `state` после инициализации, актуализирует `nodes` при изменении состава (D/E), txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka/valkey); `platform` — единственное значение `"owns3"` (фиксируется при создании навсегда; точка расширения контракта); `nodes` — число инстансов заявки при создании (0..9 — кластер без инстансов валиден), после D/E — поддерживается воркером равным фактическому составу (структурный предел 0..9; вывод последнего инстанса запрещён — [s3worker/03-node-scaling.md](../s3worker/03-node-scaling.md) §2, 0 достигается только созданием; операционная рекомендация 2–3 — гео-минимум и бюджет); `etcd_backup` — признак «бэкапный кластер etcd» (bool, default false, пишется только при `true`; фиксируется при создании навсегда — меняется пересозданием кластера, как `platform`; единственный в `/s3/clusters/` — валидация API создания, 409 при коллизии; требует полной спец-ключевой env-пары воркера — [06-secrets.md](06-secrets.md) §1, [s3worker/07-api.md](../s3worker/07-api.md) §3) |
| `secrets` | JSON `{"root_user","root_password","access_keys":[{"name","access_key","secret_key","policy"}…]}` — формат §2 | воркер (лениво при provisioning, txn put-if-absent; ротация — замена целиком, txn по `mod_revision`) | клиентские креды кластера ([06-secrets.md](06-secrets.md) §1): root-пара админ-грани + статические access keys (`backups` read-write, `panel` read-only; etcd-backup кластер — `backups` из спец-ключа env); читатели — воркер (env инстансов, root админ-граней), PgWorker (ключ `backups`, [07-client-pool.md](07-client-pool.md) §2), панель (ключ `panel`), etcdworker (ключ `backups`); удаляется deprovisioning'ом (префикс кластера); значения — только env контейнеров и S3-клиенты процессов, никогда журнал/логи/API/UI |
| `nodes/<I>/placement` | JSON-селектор атрибутов хоста: `{"docker_host":"h1"}` (plain) \| `{"swarm_node":"n1"}` \| `{"swarm_labels":{"geo":"dc1",...}}` \| комбинация атрибутов | панель через API воркера | явная привязка инстанса; модель «хост = набор атрибутов» ([04-placement.md](04-placement.md)); привязка = предикат равенства по всем указанным атрибутам; изменение привязки живого инстанса — только выводом/перевозом инстанса (перепривязка на месте не допускается: данные тома привязаны к хосту) |
| `nodes/<I>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"200Gi"}` | панель через API воркера (мутация ресурсов) | форматы — канон pg ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.3; kafka — §10.3): cpu — десятичные ядра 0.01..64, mem/disk — целые GiB, в etcd `"<n>Gi"`; cpu/mem — лимиты контейнера; `disk` — целевой объём volume данных инстанса (декларация: создание volume и операционная квота-наблюдение; жёсткого enforcement-примитива у docker-volume нет — занятость наблюдается `usage` и алертами панели) |
| `nodes/<I>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — **только панель** (заявка / one-way маркер демонтажа инстанса); остальные — воркер | жизненный цикл инстанса — семантика kafka/valkey; `UNREACHABLE` ставит надзор по health-пробам (платформенные health-грани — [05-platform.md](05-platform.md) §1) |
| `nodes/<I>/placed_on` | строка — имя docker-хоста `"h1"` | воркер (при резолюции placement) | фактическое размещение после резолва селектора ([04-placement.md](04-placement.md) §3); переживает rebuild (том инстанса живёт на этом хосте) |
| `nodes/<I>/usage` | JSON `{"used_bytes":N,"total_bytes":N,"updated_unix":T}` | воркер (надзор, периодически) | занятость диска инстанса из платформенной админ-грани/метрик; кормит квота-наблюдение `resources.disk` и панельные алерты |
| `nodes/<I>/pki` | JSON `{"certificate":"<PeerCertificate JSON, owns3/07 §2>","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}` | воркер (ленивый выпуск, txn put-if-absent) | материал инстанса репликационного PKI ([06-secrets.md](06-secrets.md) §2): сертификат выпущен CA кластера, имя = `<I>`; **переживает provision/переезд контейнера** (контейнер пересоздаётся с тем же сертификатом); удаляется вместе с инстансом |
| `links` | JSON `[{"a":"<I1>","b":"<I2>"},…]` | панель через API воркера (замена целиком) | **граф репликации** — произвольный набор связей (пар) между инстансами; full-mesh НЕ инвариант (частичный/незеркальный состав нормален: полнота и симметричность состава соседей у разных инстансов не контролируются — настройка состава зона оркестратора, зонд сверяет только настроенные линки); связь пары реализуется join-механикой двусторонне (owns3/07 §2); применяет воркер ([s3worker/05-mesh-governance.md](../s3worker/05-mesh-governance.md)): join недостающих пар, снос лишних target-записей; валидация API: имена существуют, `a≠b`, уникальность пар |
| `denylist` | JSON `["<I1>",…]` | панель через API воркера (deny/allow инстанса) | **отзыв ключей** кластера — источник истины в etcd; операция «инстанс в deny-list» = отзыв его ключа: join с ним невозможен (owns3/07 §2 deny-list), существующие связи с ним сносятся воркером; список доводится воркером до всех RUNNING-инстансов, КРОМЕ DenyList-имён (отозванному собственный список отзывов не нужен — изоляцию обеспечивают записи у соседей и запрет будущих join его сертификата; `PUT /replication/denylist`, owns3/07 §2/§6); валидация API: имена существуют |
| `pki/ca` | JSON `{"name":"<C>","publicKeyPem":"-----BEGIN PUBLIC KEY-----…","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}` | воркер (ленивый выпуск при первом инстансе/deny, txn put-if-absent) | **CA кластера** репликации — ECDSA P-256 (механика owns3/07 §2: PeerCertificateAuthority); `publicKeyPem` — trust-anchor, передаётся каждому инстансу (`OWNS3_REPLICATION_TRUST_CA`); приватный ключ CA — только воркеру (выпуск сертификатов); ротация CA — вне (roadmap) |
| `pki/orchestrator` | JSON `{"certificate":"<PeerCertificate JSON>","private_key_pem":"…"}` | воркер (ленивый выпуск вместе с CA) | сертификат оркестратора (имя `orchestrator`, CA кластера) — подпись до-join запросов воркера (`PUT /replication/denylist`, owns3/07 §6) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; RMW при изменении состава) | клиентские адреса пула (advertised host + S3-порт из `/s3worker/portalloc/<C>`) — **точка дискавери клиентов**; существует ⟺ ≥1 инстанс |

## 2. Канонические примеры значений (критерий приёмки парсеров)

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

`secrets` (клиентские креды кластера, [06-secrets.md](06-secrets.md) §1;
генерирует воркер; вместо «<32 симв [A-Za-z0-9]>» — криптостойкие
сгенерированные значения):

```json
{"root_user":"s3w-backups-root",
 "root_password":"<32 симв [A-Za-z0-9]>",
 "access_keys":[
   {"name":"backups","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-write"},
   {"name":"panel","access_key":"<32 симв>","secret_key":"<32 симв>","policy":"read-only"}]}
```

`placement` — plain: `{"docker_host":"h1"}`; swarm: `{"swarm_node":"n1"}` и
`{"swarm_labels":{"geo":"dc1"}}` (комбинация атрибутов — конъюнкция,
[04-placement.md](04-placement.md) §1–§2).

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

## 3. Толерантность парсеров

- **Неизвестные ключи** внутри `/s3/` — не ошибка: лог + счётчик
  `unknownKeys` читателя (как pg/kafka/valkey; система развивается, парсеры
  не падают).
- **Новые поля JSON** не ломают чтение: парсеры читают известные поля
  ([03-observation-statuses.md](03-observation-statuses.md) §3,
  [09-panel.md](09-panel.md) §2).
- **Битый JSON** — parseError-запись панели без падения тика
  ([09-panel.md](09-panel.md) §2).
