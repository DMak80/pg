# Valkey-кластеры: ключи кластера `/valkey/clusters/<C>/`

Уровень 3. Полный контракт ключей контроль-плейна домена, кто и когда
пишет, канонические примеры значений. Координация воркера —
[03-coordination.md](03-coordination.md); клиентский дискавери —
[04-client-discovery.md](04-client-discovery.md).

## 1. Ключи

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"nodes":N,"maxmemory_bytes":M,"maxmemory_policy":P,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka); `nodes` фиксируется при создании (=1 в v1; реплики — roadmap); `maxmemory_bytes` — число байт; `maxmemory_policy` — управляемое поле (8 значений Valkey: `allkeys-lru`\|`allkeys-lfu`\|`volatile-lru`\|`volatile-lfu`\|`allkeys-random`\|`volatile-random`\|`volatile-ttl`\|`noeviction`), канон-дефолт `allkeys-lru` (все ключи выселимы — семантика разделяемого кеша без обязательных TTL); оба поля — mutable-конфиги кластера (converge воркером через `CONFIG SET`, без рестартов) |
| `nodes/node<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — **только панель** (заявка/маркер демонтажа, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа ноды; в v1 (nodes=1) демонтаж ноды = демонтаж кластера, маркер на всю декларацию через `config.state=TO_REMOVE` |
| `nodes/node<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера cpu/mem; форматы как pg §9.3); `disk` — инфо-поле (томов нет — действий не вызывает); инвариант валидации: `maxmemory_bytes < mem`-лимит — иначе OOM-килл контейнера (риск R3 [valkeyworker/06-operability.md](../valkeyworker/06-operability.md) §6) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` (при nodes=1 — один адрес) | воркер (после подъёма; RMW при изменениях нод) | клиентские адреса (advertised host + клиентский порт из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **приложений** (права: чтение/запись ключей — `~* +@read +@write`, без админ-команд) |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль приложений; в UI/API панели не отдаётся (как app-креды pg/kafka) |
| `admin_user` | `"admin"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **администратора** (воркер: converge/ACL/пробы; панель: пробы; `+@all`) |
| `admin_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль администратора; панель читает для проб, в UI/API не отдаёт |
| `ca_pem` | PEM-серт CA одной строкой с `\n` | воркер (ensure, txn put-if-absent; ротация — CaRotator) | публичный CA per-cluster — **точка дискавери TLS** внешних клиентов; панель читает для live-проб (internal); парсеры читателей не падают ([05-fault-tolerance.md](05-fault-tolerance.md)). **В окне ротации CA — bundle: конкатенация PEM OLD+NEW** (фаза D CaRotator, [valkeyworker/04-security.md](../valkeyworker/04-security.md) §6; конкатенация «\n»-разделителем — образец kafka [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md) §2), после коммита — только NEW; читатели используют значение как есть |
| `ca_key` | PEM PKCS#8 приватного ключа CA | воркер (ensure, txn put-if-absent; ротация — CaRotator фаза C) | подпись серверных сертов нод; секрет воркера — панель и приложения НЕ читают; в коммите ротации уничтожается перезаписью значением NEW |
| `ca_next_key` | PEM PKCS#8 приватного ключа НОВОЙ CA | воркер (CaRotator фаза P, txn put-if-absent; фаза C — del) | staging ротации CA: подпись серверного серта ноды в фазе R до коммита; живёт ТОЛЬКО в окне ротации (P→C) — ensure/provisioning не создаёт; вне окна отсутствует, у читателей — unknownKeys |
| `ca_next_pem` | PEM-серт НОВОЙ CA | воркер (CaRotator фаза P; фаза C — del) | staging ротации CA: источник bundle для `ca_pem` (фаза D) и якорь пробы готовности R; в UI/API панели не отдаётся |

Неизвестные ключи внутри `/valkey/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg/kafka; система развивается, парсеры не падают,
[05-fault-tolerance.md](05-fault-tolerance.md)).

**Отличия от kafka-таблицы [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md) §1** (фиксируются
явно): нет `role` (нет ролей нод — standalone), `ca_pem`/`ca_key` есть,
`ca_next_*` есть (ротация CA, окно двойного доверия); нет `topics/`
(Valkey ключи данных хранит сам, контроль-плейн их не реестрирует).

## 2. Канонические примеры значений (критерий приёмки парсеров)

`config` при создании (заявка, панель через API воркера):

```json
{"nodes":1,"maxmemory_bytes":536870912,
 "maxmemory_policy":"allkeys-lru",
 "created_unix":1756500000,"state":"NOT_INITIALIZED"}
```

`config` Active-кластера (после provisioning — поле `state` снято воркером):

```json
{"nodes":1,"maxmemory_bytes":536870912,
 "maxmemory_policy":"allkeys-lru",
 "created_unix":1756500000}
```

`config` с заявкой удаления: то же + `"state":"TO_REMOVE"`.

`endpoints`: `"host.docker.internal:17001"` (advertised-хост по правилу
[valkeyworker/01-placement.md](../valkeyworker/01-placement.md) §4; порт —
клиентский host-порт ноды из `/valkeyworker/portalloc/<C>`).

`app_user`: `"app"`; `admin_user`: `"admin"`; пароли — 32 симв
`[A-Za-z0-9]` (генератор воркера).

`ca_pem`/`ca_key` — PEM одной строкой с `\n` (канон значений etcd,
формат kafka [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md) §2): `ca_pem` —
`"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n"`;
`ca_key` — `"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"`
(PKCS#8). CA self-signed RSA-2048, 10 лет; CN CA — `vwk-<C>-ca-<8hex>`
(отпечаток ключа — subject уникален на генерацию).
Серт ноды — подпись `ca_key`: CN=`node<k>`, SAN только advertised-хост
(DNS либо IP по правилу [valkeyworker/01-placement.md](../valkeyworker/01-placement.md) §4),
EKU ServerAuth, NotAfter зажат в CA (детали генерации —
[valkeyworker/04-security.md](../valkeyworker/04-security.md) §1).

`ca_pem` **в окне ротации CA** — bundle двух PEM-сертификатов,
`"\n"`-разделитель, каждый своей однострочной формой:

```
"-----BEGIN CERTIFICATE-----\nMIIB...(OLD)...\n-----END CERTIFICATE-----\n
-----BEGIN CERTIFICATE-----\nMIIB...(NEW)...\n-----END CERTIFICATE-----\n"
```

(конкатенация «OLD + "\n" + NEW» — как kafka [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md) §2).
`ca_next_*` — та же PEM-форма, что `ca_pem`/`ca_key`. Критерий приёмки
читателей: значение `ca_pem` используется как есть — доверие строится по
ВСЕМ блокам CERTIFICATE (окно двойного доверия), вне окна блок один.
