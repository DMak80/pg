# 20. Valkey-кластеры: контракт etcd (контроль-плейн + дискавери) ★

Канон ключей Valkey-домена: контроль-плейн кластеров `/valkey/` (декларирует
панель AdminPanel через HTTP API воркера — исполняет ValkeyWorker, канон —
[21-valkeyworker.md](21-valkeyworker.md)), координация воркера
`/valkeyworker/` и **клиентский дискавери** (приложение читает
endpoints/ACL-креды исключительно отсюда — аналогия pg dsn/`app_password`
и kafka endpoints, [11-bucket-sharding.md](11-bucket-sharding.md) §2).
Назначение домена — **разделяемый кеш набора инстансов приложения**:
топология standalone, `nodes=1` (реплики/sentinel/cluster — вне канона,
[roadmap/valkey.md](roadmap/valkey.md)).

Имена: кластер `<C>` — `^[a-z][a-z0-9_]{0,62}$` (как pg/kafka; без дефиса);
ноды `node1..nodeN` (имя генерирует панель, `node<max+1>`, ≤ 9); в v1 всегда
`nodes=1` — нода `node1`.

## 1. Транспорт: HTTP JSON gateway `/v3/*`

Как панель и PgWorker ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§1, [14-pgworker.md](14-pgworker.md) §3): `HttpClient` против gRPC-gateway
etcd (JSON+base64), `POST /v3/kv/range` / `/v3/kv/put` / `/v3/kv/txn` /
`/v3/lease/*`. Один общий etcd-кластер со стендом pg. **Poll, без watch** —
тик воркера 5 с / панели 3 с покрывают динамику.

Два новых корневых префикса: `/valkey/` (контроль-плейн кластеров) и
`/valkeyworker/` (координация воркера; панель читает избирательно — §3).

## 2. Ключи кластера `/valkey/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"nodes":N,"maxmemory_bytes":M,"maxmemory_policy":P,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka); `nodes` фиксируется при создании (=1 в v1; реплики — roadmap); `maxmemory_bytes` — число байт; `maxmemory_policy` — управляемое поле (8 значений Valkey: `allkeys-lru`\|`allkeys-lfu`\|`volatile-lru`\|`volatile-lfu`\|`allkeys-random`\|`volatile-random`\|`volatile-ttl`\|`noeviction`), канон-дефолт `allkeys-lru` (все ключи выселимы — семантика разделяемого кеша без обязательных TTL); оба поля — mutable-конфиги кластера (converge воркером через `CONFIG SET`, без рестартов) |
| `nodes/node<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — **только панель** (заявка/маркер демонтажа, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа ноды; в v1 (nodes=1) демонтаж ноды = демонтаж кластера, маркер на всю декларацию через `config.state=TO_REMOVE` |
| `nodes/node<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера cpu/mem; форматы как pg §9.3); `disk` — инфо-поле (томов нет — действий не вызывает); инвариант валидации (t02/t03): `maxmemory_bytes < mem`-лимит — иначе OOM-килл контейнера (риск R3 [21](21-valkeyworker.md)) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` (при nodes=1 — один адрес) | воркер (после подъёма; RMW при изменениях нод) | клиентские адреса (advertised host + клиентский порт из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **приложений** (права: чтение/запись ключей — `~* +@read +@write`, без админ-команд) |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль приложений; в UI/API панели не отдаётся (как app-креды pg/kafka) |
| `admin_user` | `"admin"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **администратора** (воркер: converge/ACL/пробы; панель: пробы; `+@all`) |
| `admin_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль администратора; панель читает для проб, в UI/API не отдаёт |
| `ca_pem` | PEM-серт CA одной строкой с `\n` | воркер (ensure, txn put-if-absent; ротация — CaRotator) | публичный CA per-cluster — **точка дискавери TLS** внешних клиентов; панель читает для live-проб (internal); парсеры читателей не падают (unknownKeys-толерантность §5). **В окне ротации CA (t07) — bundle: конкатенация PEM OLD+NEW** (фаза D CaRotator, [21](21-valkeyworker.md) §5 K; конкатенация «\n»-разделителем — образец kafka [15](15-kafka-clusters.md) §2), после коммита — только NEW; читатели используют значение как есть |
| `ca_key` | PEM PKCS#8 приватного ключа CA | воркер (ensure, txn put-if-absent; ротация — CaRotator фаза C) | подпись серверных сертов нод; секрет воркера — панель и приложения НЕ читают; в коммите ротации уничтожается перезаписью значением NEW |
| `ca_next_key` | PEM PKCS#8 приватного ключа НОВОЙ CA | воркер (CaRotator фаза P, txn put-if-absent; фаза C — del) | staging ротации CA (t07): подпись серверного серта ноды в фазе R до коммита; живёт ТОЛЬКО в окне ротации (P→C) — ensure/provisioning не создаёт; вне окна отсутствует, у читателей — unknownKeys |
| `ca_next_pem` | PEM-серт НОВОЙ CA | воркер (CaRotator фаза P; фаза C — del) | staging ротации CA: источник bundle для `ca_pem` (фаза D) и якорь пробы готовности R; в UI/API панели не отдаётся |

Неизвестные ключи внутри `/valkey/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg/kafka; система развивается, парсеры не падают).

**Отличия от kafka-таблицы [15](15-kafka-clusters.md) §2** (фиксируются
явно): нет `role` (нет ролей нод — standalone), `ca_pem`/`ca_key` есть
(t06), `ca_next_*` есть (t07 — ротация CA, окно двойного доверия); нет
`topics/` (Valkey ключи данных хранит сам, контроль-плейн их не реестрирует).

### 2.1. Канонические примеры значений (критерий приёмки парсеров)

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
[21](21-valkeyworker.md) §2; порт — клиентский host-порт ноды из
`/valkeyworker/portalloc/<C>`).

`app_user`: `"app"`; `admin_user`: `"admin"`; пароли — 32 симв
`[A-Za-z0-9]` (генератор воркера).

`ca_pem`/`ca_key` (t06) — PEM одной строкой с `\n` (канон значений etcd,
формат kafka [15](15-kafka-clusters.md) §2.1): `ca_pem` —
`"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n"`;
`ca_key` — `"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"`
(PKCS#8). CA self-signed RSA-2048, 10 лет; CN CA — `vwk-<C>-ca-<8hex>`
(отпечаток ключа — subject уникален на генерацию, фикс t07 kafka).
Серт ноды — подпись `ca_key`: CN=`node<k>`, SAN только advertised-хост
(DNS либо IP по правилу [21](21-valkeyworker.md) §2), EKU ServerAuth,
NotAfter зажат в CA (детали генерации — [21](21-valkeyworker.md) §2).

`ca_pem` **в окне ротации CA (t07)** — bundle двух PEM-сертификатов,
`"\n"`-разделитель, каждый своей однострочной формой:

```
"-----BEGIN CERTIFICATE-----\nMIIB...(OLD)...\n-----END CERTIFICATE-----\n
-----BEGIN CERTIFICATE-----\nMIIB...(NEW)...\n-----END CERTIFICATE-----\n"
```

(конкатенация «OLD + "\n" + NEW» — как kafka [15](15-kafka-clusters.md)
§2). `ca_next_*` — та же PEM-форма, что `ca_pem`/`ca_key`. Критерий
приёмки читателей: значение `ca_pem` используется как есть — доверие
строится по ВСЕМ блокам CERTIFICATE (окно двойного доверия), вне окна
блок один.

## 3. Координация воркера `/valkeyworker/`

Порт схемы `/kafkaworker/` ([15-kafka-clusters.md](15-kafka-clusters.md) §4)
один в один, свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/valkeyworker/leader` | lease TTL 15 с | лидер singleton-задач (регулярные снапшоты контроль-плейна `/valkey/`+`/valkeyworker/`) |
| `/valkeyworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/valkeyworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` (сохранение накопленных треков — S6, arch/17). Вложенный ключ `work/<C>/rotation` — стейт доигрывания ротации E: `{"phase":"e1-pending"\|"e1-added"\|"e2-committed","role","old","new","requested_by","request"?}` (`e1-pending` — транзитная: стейт пишется до E1, чтобы NEW, добавленный на ноду, всегда доигрывался тем же значением; фаза+пароли переживают тик — ключ журнала `work/<C>` надзор перезаписывает каждый тик, фаза ротации в нём не живёт; `request` — исходный payload заявки `rotations/<C>`: compare условного del в E2 — чужая/снятая панелью заявка не трогается; del ротатором после E3, чистка — X2 демонтажа) |
| `/valkeyworker/portalloc/<C>` | обычный | `{"node<k>":{"host":"h","client":17001}}` — закрепление клиентских портов (переживает пересоздание контейнера) |
| `/valkeyworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** (t90-паттерн): взаимоисключение секции довыделения клиентских портов «чтение занятости → выбор портов → запись `/valkeyworker/portalloc/<C>`» — пер-кластерные клэймы кросс-кластерную гонку не закрывают; txn `version==0` + put-with-lease, del + revoke lease по завершении секции; не взял → InProgress (следующий тик) |
| `/valkeyworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/valkeyworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** (паттерн [16](16-kafkaworker.md) §1.1): `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; ключ жив = инстанс жив и URL валиден. Читает панель (мутации valkey-домена — через API воркера) |
| `/valkeyworker/rotations/<C>` | обычный | заявка ротации креда `{"role":"app"\|"admin","requested_unix","requested_by"}` (панель через API воркера — клэйм-txn `version==0`; del воркером по завершении; отмены из панели нет — t03) |
| `/valkeyworker/ca_rotations/<C>` | обычный | заявка ротации per-cluster CA/сертов `{"requested_unix","requested_by"}` (t07; панель через API воркера — клэйм-txn `version==0`, протокол §9.8 один в один с ротациями кредов; del воркером в коммите фазы C; отмены из панели нет — зависшая заявка/осиротевший staging — runbook, etcdctl) |

Заявка ротации — **один ключ с полем `role`** (не два, как у kafka — там
разделение app/admin-ротаций наследие JAAS-механики пересозданий; здесь
ротация без рестартов единая для обеих ролей, [21](21-valkeyworker.md) §5 E).

Панель читает из `/valkeyworker/` только `rotations/` (очередь ротаций в UI;
реализация — t03), `ca_rotations/` (очередь ротаций CA в UI; t07) и
`api/` (дискавери API — §3 таблица выше, мутации панели
идут через HTTP-грань воркера); остальные ключи не читает и не пишет.
Снятие заявки ротации — только воркером (del по завершении процесса E;
rotations — del в коммите фазы C CaRotator);
отмены из панели нет (t03: окно «передумать» мало — заявка исполняется
тиками за секунды; зависшая заявка — runbook, etcdctl).

Реализация координации — переиспользование `Shared.Etcd` (`ClaimStore`/
`PortAllocLock`/`WorkJournal` — `keyPrefix="/valkeyworker"`, префикс уже
параметр конструктора; t02 подключает сборку, новый код не пишется).

## 4. Клиентский дискавери (приложения)

Приложение читает из etcd и только из него (аналогия pg dsn/`app_password`
и kafka endpoints):

1. `/valkey/clusters/<C>/endpoints` → адрес(а) подключения;
2. `/valkey/clusters/<C>/app_user` + `app_password` → ACL-креды
   (username+password, роль app);
3. `/valkey/clusters/<C>/config` → только `state` (raw-строка; отсутствие
   = Active — клиент видит заявочные переходы);
4. `/valkey/clusters/<C>/ca_pem` → TLS-доверие клиента (публичный CA
   per-cluster — аутентификация сервера). **Окно ротации CA (t07)**:
   значение — bundle OLD+NEW; клиент строит доверие по ВСЕМ блокам
   CERTIFICATE значения (двойное доверие), после коммита — снова один
   блок; читатель использует значение как есть, специальной ветки нет.

`GetClientConfig()` внешней библиотеки дискавери (t04, образец HA.Kafka —
`docs/01.19-ha-kafka.md` в репозитории Puzzle) после t06 отдаёт
`ssl=true` + CA для StackExchange.Redis. Правило: **`ssl=true ⟺ ca_pem`
прочитан** (снапшот без `ca_pem` — переходное/миграционное состояние, см.
§5: окно миграции `ssl=false→true` прозрачно — старые контуры не ломаются,
актуализация подтянет). Неполный набор кредов (есть `app_user`, нет
`app_password`, и наоборот) → `App = null` → `GetClientConfig() = null`
(потребитель обязан проверить).

## 5. Обработка сбоев (толерантность читателей)

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, заявка ротации) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `valkey-key-malformed` |
| Неизвестный ключ внутри `/valkey/` | лог-строка + счётчик `unknownKeys`; парсер не падает |
| Active-кластер без `endpoints` | критический алерт `valkey-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| Неполный набор кредов (`app_user` без `app_password` и наоборот) | `App = null`, `GetClientConfig() = null` — потребитель проверяет |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Пустой/пробельный `endpoints` | трактуется как отсутствующий (алерт `valkey-endpoints-missing`) |
| Битый PEM в `ca_pem`/`ca_key` | parseError-запись + warning `valkey-key-malformed`; парсер не падает, поле в снапшоте null (кластер жив) |
| `ca_pem` с несколькими CERTIFICATE-блоками (bundle окна ротации) | не ошибка: парсер читает ПЕРВЫЙ блок (канон t06), доверие клиента — по всем (§4); окно закрывается коммитом ротации |
| `ca_next_*` вне живой ротации (осиротевший staging после ручного снятия заявки) | unknownKeys-толерантность: лог + счётчик, парсер не падает; живая ротация переиспользует staging (put-if-absent), чистка — runbook |
| Active-кластер без `ca_pem` | critical-алерт `valkey-security-missing` (миграция TLS не доиграна / ключ потерян) |

---

→ Указатель — [README.md](README.md). Исполнительная сторона контракта —
[21-valkeyworker.md](21-valkeyworker.md); мутации панели — через его HTTP
API (§1.1 там).
