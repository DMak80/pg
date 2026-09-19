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

Неизвестные ключи внутри `/valkey/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg/kafka; система развивается, парсеры не падают).

**Отличия от kafka-таблицы [15](15-kafka-clusters.md) §2** (фиксируются
явно): нет `role` (нет ролей нод — standalone), нет
`ca_pem`/`ca_key`/`ca_next_*` (TLS — `t06-valkey-tls`; после t06 добавятся
`ca_pem`/`ca_key`), нет `topics/` (Valkey ключи данных хранит сам,
контроль-плейн их не реестрирует).

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

Заявка ротации — **один ключ с полем `role`** (не два, как у kafka — там
разделение app/admin-ротаций наследие JAAS-механики пересозданий; здесь
ротация без рестартов единая для обеих ролей, [21](21-valkeyworker.md) §5 E).

Панель читает из `/valkeyworker/` только `rotations/` (очередь ротаций в UI;
реализация — t03) и `api/` (дискавери API — §3 таблица выше, мутации панели
идут через HTTP-грань воркера); остальные ключи не читает и не пишет.
Снятие заявки ротации — только воркером (del по завершении процесса E);
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
   = Active — клиент видит заявочные переходы).

`GetClientConfig()` внешней библиотеки дискавери (t04, образец HA.Kafka —
`docs/01.19-ha-kafka.md` в репозитории Puzzle) — plain-поля для
StackExchange.Redis: `endpoints`, `username`, `password`, `ssl=false`
(v1 без TLS; `ssl=true` + CA — после t06). Неполный набор кредов (есть
`app_user`, нет `app_password`, и наоборот) → `App = null` →
`GetClientConfig() = null` (потребитель обязан проверить).

TLS в v1 отсутствует осознанно (решение пользователя): доверенная закрытая
docker-сеть + ACL-креды; шифрование трафика и аутентификация сервера —
`t06-valkey-tls` ([roadmap/valkey.md](roadmap/valkey.md)); контракт
кред/endpoints при этом не меняется — добавится только ключ `ca_pem`
(обратная совместимость читателя).

## 5. Обработка сбоев (толерантность читателей)

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, заявка ротации) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `valkey-key-malformed` |
| Неизвестный ключ внутри `/valkey/` | лог-строка + счётчик `unknownKeys`; парсер не падает |
| Active-кластер без `endpoints` | критический алерт `valkey-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| Неполный набор кредов (`app_user` без `app_password` и наоборот) | `App = null`, `GetClientConfig() = null` — потребитель проверяет |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Пустой/пробельный `endpoints` | трактуется как отсутствующий (алерт `valkey-endpoints-missing`) |

---

→ Указатель — [README.md](README.md). Исполнительная сторона контракта —
[21-valkeyworker.md](21-valkeyworker.md); мутации панели — через его HTTP
API (§1.1 там).
