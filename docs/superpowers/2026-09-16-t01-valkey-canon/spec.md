# Spec: канон Valkey-домена (t01-valkey-canon)

Задача roadmap: [`t01-valkey-canon`](../../../arch/roadmap/valkey.md) —
арх-канон Valkey-домена: два новых канон-документа `arch/20-valkey-clusters.md`
(контракт etcd `/valkey/` + клиентский дискавери + координация `/valkeyworker/`)
и `arch/21-valkeyworker.md` (оркестратор: декларативный жизненный цикл
Valkey-кластеров), плюс указатели и roadmap-правки (перенос незакоммиченных
файлов трека + новая задача `t06-valkey-tls`). Реализация воркера/панели/
библиотеки/метрик/TLS — t02–t06; t01 ничего не исполняет, только канон.

Назначение домена (из roadmap): **разделяемый кеш набора инстансов
приложения**. Топология — standalone (реплики/sentinel/cluster — вне скоупа:
кеш восполним, шардирование не нужно). Канон-образцы: arch/15 (контракт
`/kafka/` + дискавери), arch/16 (воркер), arch/17 (принципы синхронизации).

## 1. Цель

1. **Канон `arch/20-valkey-clusters.md`**: контракт контроль-плейна
   `/valkey/` (state/endpoints/креды кластеров), координация `/valkeyworker/`,
   клиентские точки дискавери и толерантность читателей — по структуре
   arch/15 (транспорт → ключи кластера → канонические примеры → координация →
   дискавери → сбои).
2. **Канон `arch/21-valkeyworker.md`**: оркестратор ValkeyWorker — роль и
   разделение ответственности, модель размещения (docker), контракт etcd
   (зеркало 20), секреты (ACL-модель), пять процессов (машины состояний),
   надёжность, наблюдаемость, конфигурация (заготовка t02), риски — по
   структуре arch/16.
3. **Указатели**: `arch/README.md` (структура + «Дальше»), шапка
   `arch/roadmap/valkey.md` (канон появился — ссылки на 20/21).
4. **Roadmap**: перенос незакоммиченных `arch/roadmap/valkey.md` +
   `arch/roadmap/README.md` из рабочей копии главного репозитория в ветку
   (предусловие, §6 фаза 1); новая задача `t06-valkey-tls` с подробным
   описанием (§5.3).
5. **Мерж-гейт**: удалить пункт `t01-valkey-canon` из `arch/roadmap/valkey.md`
   и снять `← t01-valkey-canon` из зависимостей t02–t06 тем же мерж-коммитом.

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Модель топологии контракта | **nodes-поддерево с `nodes=1`** — `/valkey/clusters/<C>/{config (nodes=1, maxmemory_bytes, maxmemory_policy, created_unix), nodes/node1/{state,resources}, endpoints, app_user/app_password, admin_user/admin_password}` — симметрия с kafka/pg, задел на реплики без миграции контракта |
| Модель кред | **ACL**: `admin_user`/`admin_password` (воркер: converge/пробы; панель: пробы) + `app_user`/`app_password` (приложения); ensure `ACL SETUSER` при provisioning; ротация **без рестартов** через окно двух паролей |
| TLS | **Без TLS в v1** (доверенная docker-сеть + ACL-креды достаточно, кеш восполним) — НО с новым roadmap-пунктом `t06-valkey-tls` (подробно §5.3) |

Остальные решения — по образцам канона (не оспорены пользователем):
координация `/valkeyworker/` портом `/kafkaworker/` 1:1; порты 17000–17999;
образ `valkey/valkey:<пин>` (зеркало в 192.168.0.1:5000 — забота t02); имена
`vwk-<C>-node<k>`; без volume и без per-cluster сети; пять процессов воркера;
панель — декларатор через HTTP API воркера (etcd домена пишет только воркер);
один ключ заявки ротации; наблюдаемость по arch/18 §2.2 + t05.

## 2. Принципы

1. **arch-first**: контракты — сначала `arch/20` + `arch/21` + указатели,
   затем (в t02+) код. t01 кода не касается вовсе.
2. **Образец-канон — переносить буквально, отклонения обосновывать**: где
   Valkey-домен совпадает с kafka (state-семантика, координация, txn-протоколы,
   толерантность читателей) — формулировки/механики переносятся 1:1 из
   arch/15/16; отличия (нет кворума/топиков, креды — runtime ACL, кеш
   восполним) — упрощения фиксируются явно.
3. **Единый etcd-контур и существующие паттерны** (arch/14/16/17): один etcd
   установки (S1); префиксы `/valkey/` и `/valkeyworker/` пишет ТОЛЬКО
   ValkeyWorker (панель и сиды — через HTTP API воркера, паттерн t03-mTLS);
   идемпотентность каждого шага; journal-before-manipulations; клэймы
   lease TTL 15 с; флап ≠ смерть (S7); фазовые записи не затирают треки (S6);
   глобальный portalloc-клэйм (S5/t90); лестница E9 для потерянного portalloc.
4. **Кеш восполним — данные не дороже доступности**: без persistence-томов
   (RDB/AOF off), надзор пересоздаёт контейнер свободно (холодный старт кеша —
   документированное поведение, не авария); деструктив по-прежнему только по
   положительному свидетельству (S7).
5. **Контракт пригоден внешнему читателю**: клиентская дискавери-библиотека
   (Puzzle `PuzzleServer.Infrastructure.App.HA.Valkey`, t04, по образцу
   HA.Kafka `docs/01.19-ha-kafka.md`) читает ОДИН префикс
   `/valkey/clusters/<C>/` и не заходит в `/valkeyworker/`; fail-open,
   снапшот+watch/poll, `GetClientConfig()` для StackExchange.Redis.
6. **Топология standalone — простота как свойство**: один контейнер на
   кластер, нет inter-node трафика → нет per-cluster сети (не копим осиротевшие
   подсети — урок t05); `nodes=1` в заявке фиксирует топологию контрактом,
   реплики/sentinel/cluster — roadmap.
7. Язык: документация — русский, идентификаторы — английский; стиль и тон —
   окружающих arch/15/16.

## 3. Канон `arch/20-valkey-clusters.md` (содержание)

### 3.1. Транспорт и имена

Как у kafka (15 §1): HTTP JSON gateway etcd `/v3/*` (`HttpClient`,
`POST /v3/kv/range|put|txn`, `/v3/lease/*`), один общий etcd-кластер со
стендом. **Poll, без watch** — тик воркера 5 с / панели 3 с покрывают
динамику. Два новых корневых префикса: `/valkey/` (контроль-плейн кластеров)
и `/valkeyworker/` (координация воркера; панель читает избирательно — §3.4).

Имена: кластер `<C>` — `^[[a-z][a-z0-9_]{0,62}$` (как pg/kafka; без дефиса);
ноды `node1..nodeN` (генерирует панель, `node<max+1>`, ≤ 9); в t02 всегда
`nodes=1` — нода `node1`.

### 3.2. Ключи кластера `/valkey/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"nodes":N,"maxmemory_bytes":M,"maxmemory_policy":P,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka); `nodes` — фиксируется при создании (=1 в t02; реплики — roadmap); `maxmemory_bytes` — число байт; `maxmemory_policy` — управляемое поле (8 значений Valkey: `allkeys-lru`\|`allkeys-lfu`\|`volatile-lru`\|`volatile-lfu`\|`allkeys-random`\|`volatile-random`\|`volatile-ttl`\|`noeviction`), канон-дефолт `allkeys-lru` (все ключи выселимы — семантика разделяемого кеша без обязательных TTL); оба поля — mutable-конфиги кластера (converge воркером через `CONFIG SET`, без рестартов) |
| `nodes/node<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — только панель (заявка/маркер, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа ноды; в t02 (nodes=1) демонтаж ноды = демонтаж кластера, маркер на всю декларацию через `config.state=TO_REMOVE` |
| `nodes/node<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера cpu/mem; форматы как pg §9.3); `disk` — инфо-поле (томов нет — действий не вызывает); инвариант валидации (t02/t03): `maxmemory_bytes < mem`-лимит — иначе OOM-килл контейнера (риск R3 arch/21) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` (при nodes=1 — один адрес) | воркер (после подъёма; RMW при изменениях нод) | клиентские адреса (advertised host + клиентский порт из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **приложений** (права: чтение/запись ключей — `~* +@read +@write` без админ-команд) |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль приложений; в UI/API панели не отдаётся (как app-креды pg/kafka) |
| `admin_user` | `"admin"` | воркер (ensure, txn put-if-absent) | per-cluster ACL-пользователь **администратора** (воркер: converge/ACL; панель: пробы; `+@all`) |
| `admin_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster ACL-пароль администратора; панель читает для проб, в UI/API не отдаёт |

Неизвестные ключи внутри `/valkey/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg/kafka; система развивается, парсеры не падают).

**Отличия от kafka-таблицы 15 §2** (фиксируются в каноне явно): нет
`role` (нет ролей нод — standalone), нет `ca_pem`/`ca_key`/`ca_next_*`
(TLS — t06; после t06 добавятся), нет `topics/` (нет реестра — Valkey
ключи данных хранит сам, контроль-плейн их не реестрирует).

### 3.3. Канонические примеры значений (критерий приёмки парсеров)

`config` при создании (заявка):

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
arch/21 §2.1; порт — клиентский host-порт ноды из
`/valkeyworker/portalloc/<C>`).

`app_user`: `"app"`; `admin_user`: `"admin"`; пароли — 32 симв `[A-Za-z0-9]`.

### 3.4. Координация воркера `/valkeyworker/`

Порт схемы `/kafkaworker/` (15 §4) один в один, свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/valkeyworker/leader` | lease TTL 15 с | лидер singleton-задач (регулярные снапшоты контроль-плейна) |
| `/valkeyworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/valkeyworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` (сохранение накопленных треков — S6) |
| `/valkeyworker/portalloc/<C>` | обычный | `{"node<k>":{"host":"h","client":17001}}` — закрепление клиентских портов (переживает пересоздание контейнера) |
| `/valkeyworker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (t90-паттерн): взаимоисключение секции «чтение занятости → выбор портов → запись» при довыделении |
| `/valkeyworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/valkeyworker/api/<id>` | lease TTL 15 с | дискавери API воркера: `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; читает панель (мутации — через API) |
| `/valkeyworker/rotations/<C>` | обычный | заявка ротации креда `{"role":"app"\|"admin","requested_unix","requested_by"}` (панель, клэйм-txn `version==0`; del воркером по завершении или панелью — отмена). **Один ключ с полем role** — механика ротации унифицирована (не два ключа как у kafka: там разделение — наследие JAAS-механики пересозданий, здесь ротация без рестартов одна для обеих ролей) |

Панель читает из `/valkeyworker/` только `rotations/` (реализация — t03);
остальные ключи не читает и не пишет.

Реализация координации — переиспользование `Shared.Etcd`
(`ClaimStore`/`PortAllocLock`/`WorkJournal` с `keyPrefix="/valkeyworker"` —
префикс уже параметр конструктора; t02 подключает сборку, новый код не
пишется).

### 3.5. Клиентский дискавери (приложения)

Приложение читает из etcd и только из него (аналогия pg dsn/kafka
endpoints):

1. `/valkey/clusters/<C>/endpoints` → адрес(а) подключения;
2. `/valkey/clusters/<C>/app_user` + `app_password` → ACL-креды
   (username+password, роль app);
3. `/valkey/clusters/<C>/config` → только `state` (raw-строка; отсутствие
   = Active — клиент видит заявочные переходы).

`GetClientConfig()` внешней библиотеки (t04, образец HA.Kafka) — plain-поля
для StackExchange.Redis: `endpoints`, `username`, `password`,
`ssl=false` (v1 без TLS; `ssl=true` + CA — после t06). Неполный набор кредов
(есть `app_user`, нет `app_password`, и наоборот) → `App = null` →
`GetClientConfig() = null` (потребитель обязан проверить).

TLS в v1 отсутствует осознанно (решение пользователя): доверенная закрытая
docker-сеть + ACL-креды; шифрование трафика и аутентификация сервера —
`t06-valkey-tls` (§5.3), контракт кред/endpoints при этом не меняется —
добавится только ключ `ca_pem` (обратная совместимость читателя).

### 3.6. Обработка сбоев (толерантность читателей)

Порт 15 §6 один в один:

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, заявка ротации) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `valkey-key-malformed` |
| Неизвестный ключ внутри `/valkey/` | лог-строка + счётчик `unknownKeys`; парсер не падает |
| Active-кластер без `endpoints` | критический алерт `valkey-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| Неполный набор кредов (`app_user` без `app_password` и наоборот) | `App = null`, `GetClientConfig() = null` — потребитель проверяет |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Пустой/пробельный `endpoints` | трактуется как отсутствующий (алерт `valkey-endpoints-missing`) |

## 4. Канон `arch/21-valkeyworker.md` (содержание)

### 4.1. Роль и границы

**ValkeyWorker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта arch/20: панель AdminPanel **заявляет** кластер
через **HTTP API воркера** (mTLS-грань по t03-канону воркеров, ключ
`/valkeyworker/api/<id>`; `state=NOT_INITIALIZED`) — воркер **поднимает**
standalone-контейнер Valkey, обеспечивает per-cluster ACL-креды, пишет факт
(`endpoints`, `nodes/node1/state`) и снимает `state`; перевод в `TO_REMOVE` —
воркер демонтирует кластер полностью. **Ответственность изменений etcd**:
префиксы `/valkey/`, `/valkeyworker/` пишет ТОЛЬКО ValkeyWorker (панель и
сиды ходят через его API); панель etcd только читает.

Пять процессов (машины состояний):

1. **Provisioning** (A, V0–V5) — от `NOT_INITIALIZED` до рабочего кластера;
2. **Deprovisioning** (B, X0–X3) — от `TO_REMOVE` до чистого etcd и
   удалённого контейнера;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся,
   молчащая нода помечается/пересоздаётся;
4. **ConfigConverger** (D) — converge `maxmemory`/`maxmemory_policy` к
   декларации (`CONFIG SET`, без рестартов) + converge ACL-плана
   (пользователи admin/app с правами канона);
5. **PasswordRotator** (E) — ротация per-cluster ACL-паролей (app/admin) без
   рестартов через окно двух паролей.

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы, §3.4); смерть контролирующего инстанса не роняет процессы —
takeover ≤ TTL 15 с + тик; все операции идемпотентны; значимое состояние
переживает смерть контроллера (etcd).

Границы (что НЕ входит): TLS клиентских подключений (`t06-valkey-tls` —
per-cluster CA `ca_pem`/`ca_key`, tls-port, дискавери-ключ `ca_pem`);
реплики/sentinel/cluster-топологии (кеш восполним, шардирование не нужно);
коллектор доменных метрик INFO и дашборд (t05, arch/18 §2.2/§4-паттерн);
панель valkey-домена (t03); клиентская библиотека Puzzle (t04); persistence
(RDB/AOF) — off по канону (кеш восполним); квоты томов — томов нет.

### 4.2. Модель размещения

- **Образ** — официальный `valkey/valkey:<пин версии>` (настройка
  `Images:Node`), полностью конфигурируется аргументами командной строки;
  кастомный образ НЕ собирается. Зеркалирование в локальный registry
  `192.168.0.1:5000` (строка в `images.txt` + `mirror-image.sh`, мульти-арх)
  — забота t02 (ранбук `docs/runbook.md`).
- **Нода кластера = один контейнер** `vwk-<C>-node<k>` (имя
  детерминировано; при nodes=1 — `vwk-<C>-node1`), restart-политика
  `unless-stopped`.
- **ACL при старте**: аргументы командной строки
  `--user default off --user admin on ><пароль> ~* +@all --user app on
  ><пароль> ~* +@read +@write` (детерминированы от кредов etcd — пересоздание
  контейнера собирает актуальные пароли; default-пользователь отключён —
  вход только по именованным ACL-пользователям). Ротация паролей на живой
  ноде — через `ACL SETUSER` по соединению (процесс E), без пересоздания.
- **maxmemory**: аргументы `--maxmemory <bytes> --maxmemory-policy <policy>`
  при создании + converge D (`CONFIG SET`) при мутациях декларации — без
  рестартов.
- **Persistence off**: без volume, `--save ""`/`--appendonly no` (детали
  флагов — t02; канон фиксирует: без тома, снапшоты RDB/AOF не пишутся).
  Потеря контейнера = холодный старт кеша (документированное поведение).
- **Сеть**: per-cluster сеть НЕ создаётся (нет inter-node трафика; клиентский
  доступ — публикация host-порта из portalloc; контейнер живёт в сети
  запуска воркера — деталь t02). Прямое следствие: домен не порождает
  `*-net-*` объектов, pool подсетей docker не расходует (урок инцидента
  t05 про kfw-net).
- **Порты**: диапазон `17000–17999` (продолжение ряда pg 15000–15999 / kafka
  16000–16999), **1 клиентский порт на ноду** (контейнерный 6379 →
  выделенный host-порт), закрепление `/valkeyworker/portalloc/<C>`;
  занятость для довыделения = docker-публикации ∪ portalloc ВСЕХ чужих
  кластеров; довыделение — только держателем `/valkeyworker/locks/portalloc`
  (t90-паттерн, §3.4).
- **Advertised-правило** (порт 16 §2.1): advertised-хост ноды =
  `ValkeyWorker:AdvertisedClientHost`, если задан; иначе — имя docker-хоста
  размещения. Требование: значение обязано резолвиться КЛИЕНТАМИ (попадает в
  `endpoints`). Для локальных стендов — `host.docker.internal`.
- Лимиты контейнера из `resources` (cpu/mem; disk — инфо). Инвариант:
  `maxmemory_bytes < mem`-лимит (валидация заявки; иначе OOM-килл — риск R3).
- Сам воркер — контейнер с `docker.sock`, поставляется через
  `deploy/docker-compose.yml` (сборка — t02, по образцу KafkaWorker).

### 4.3. Контракт etcd (зеркало arch/20)

Таблицы читаемых (config/states/resources/rotations/api_tls) и пишемых
(states, endpoints, креды, config без state, del --prefix при TO_REMOVE,
координация) — зеркало §3.2/§3.4 этого spec; канон приводит их в формате
16 §3. Смежный ключ вне префикса — `/workers/api_tls/valkeyworker`
(серверный серт mTLS-грани; пишет ТОЛЬКО панель, воркер читает при старте —
канон adminpanel/02 §9.9, применяется с t02).

### 4.4. Секреты

Per-cluster, в etcd, генерирует воркер (ensure txn put-if-absent):

- **app** — `app_user`=`"app"`, `app_password` (32 симв `[A-Za-z0-9]`):
  приложения (ACL-роль app). Ротация — процесс E по заявке панели.
- **admin** — `admin_user`=`"admin"`, `admin_password` (32 симв): воркер
  (converge/ACL/пробы), панель (read-only пробы). Ротация — процесс E.

Env-секреты per-install — только TLS HTTP API воркера
(`VWK_API_TLS_{CERT,KEY,CLIENT_CA}`, паттерн 16 §4); per-cluster-секреты
живут в etcd (зона доверия контроль-плейна; парольная защита кеша —
достаточный уровень для домашнего контура, TLS-транспорт — t06).

### 4.5. Процессы (машины состояний)

**Состояния ноды** (`nodes/node<k>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт контейнер) → `RUNNING`
(PING-проба отвечает); `UNREACHABLE` (молчит дольше NodeDeadSec);
`REMOVING` (демонтаж); `TO_REMOVE` (маркер панели, one-way).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning (A);
`TO_REMOVE` → Deprovisioning (B); иначе Active-ветка: надзор (C) →
converger (D) → ротация (E). Все операции — только под живым клэймом `<C>`;
journal-before-manipulations. Канон фиксирует классификацию без
kafka-специфики (нет премиграционных кластеров/security-миграций — домен
рождается с ACL-каноном).

**A. ProvisioningProcess (V0–V5)**: V0 claim + journal(op=provision); V1
план: placement + порт-аллокация (под глобальным portalloc-клэймом; не взял
→ journal waiting-portalloc-lock, следующий тик); V2 ensure секретов
(admin+app, txn put-if-absent; проигрыш → re-read существующих); V3 контейнер
(аргументы ACL/maxmemory из декларации и кредов, лимиты resources,
клиентский host-порт) + state=PROVISIONING; существующий (re-run) — сверка
и пропуск; V4 ждать готовности: PING с admin-кредом отвечает (бюджет
`NodeBootSec`, транзиент-толерантно) → state=RUNNING; V5 put `endpoints`
(advertised host:clientPort); config: txn (compare mod_revision) → put
канонического JSON без state; journal done. Гонка «панель пишет TO_REMOVE
посреди provisioning»: перечитывание config перед фазами — смена state
безопасно прекращает процесс.

**B. DeprovisioningProcess (X0–X3)**: X0 claim + journal(op=deprovision);
X1 docker: удалить контейнер `vwk-<C>-*` (404 = ок; порядок «сначала docker,
потом etcd»; томов нет); X2 etcd: del --prefix `/valkey/clusters/<C>/` + del
`/valkeyworker/{claims,work,portalloc,rotations}/<C>*` (очистка координации
включает заявки — остаточные заявки не переживают удаление кластера); X3
клэйм снят явно (del + revoke lease).

**C. NodeSupervisor (надзор)**: сверка декларации с фактом docker +
PING-проба (с admin-кредом). Снесённый контейнер (docker-факт, не зависит от
пробы) → пересоздание с аргументами из текущей декларации и кредов etcd,
state=PROVISIONING; в RUNNING переводит следующий цикл по PING. Нода молчит
дольше `NodeDeadSec` → state=UNREACHABLE + пересоздание контейнера **без
тома — данных не жалко** (кеш восполним; слепая проба — никаких действий:
собственная слепота воркера не повод трогать ноду, S7; capability-гейт
честной running-инспекции — S7). Одно пересоздание за тик. **Кеш
неприкосновенен на etcd-уровне**: надзор никогда не чистит ключи домена из-за
недоступности ноды (декларация/креды переживают всё; содержимое кеша —
дело приложений). Лестница E9: нода без записи portalloc → реконструкция из
inspect живого контейнера (published-порт + host, put-if-absent под
`locks/portalloc`) → новая аллокация (свидетельство смерти по S7).

**D. ConfigConverger**: Active-ветка, лёгкий: `CONFIG GET
maxmemory/maxmemory-policy` (по admin-креду) vs `config.{maxmemory_*}` → при
отличии `CONFIG SET` (без рестартов); ACL-план: `ACL LIST` vs канон
(пользователи admin/app с правами §4.2; default off) → идемпотентный
`ACL SETUSER`-converge. Маппинг: `maxmemory_bytes`→`maxmemory`,
`maxmemory_policy`→`maxmemory-policy`. Расхождение `maxmemory_bytes` ≥
mem-лимита — journal-warning (ответственность оператора, по образцу R7
arch/16).

**E. PasswordRotator (окно двух паролей, без рестартов; роли app|admin)**:
заявка `/valkeyworker/rotations/<C>` (`role`); NEW = генерация (32 симв).

```
E1 ACL SETUSER <role> >NEW    — оба пароля (OLD+NEW) валидны, клиенты работают со OLD
E2 ОДНА txn: [compare value(<role>_password)==OLD][put NEW; del заявки]
   — клиенты перечитывают etcd и переподключаются с NEW
E3 ACL SETUSER <role> <OLD    — удаление старого пароля
```

Отказ между фазами безопасен (оба пароля валидны; повтор тика доигрывает по
journal-фазе). В отличие от kafka (16 §5 H) — **без пересозданий контейнеров**
(ACL — runtime); пересоздание надзором в окне ротации безопасно: аргументы
собираются из etcd-актуальных кредов (фаза E1 уже закоммитила NEW — контейнер
соберётся с NEW, OLD-пароль доочистит следующий тик E3; расхождение
самолечится converge D). Ротация admin не трогает app-кред и наоборот.
Битая заявка — мусор: del с journal (панель до того получает 409 «уже
запрошена»).

### 4.6. Надёжность

Идемпотентность (каждый шаг перепроверяет факт: контейнер есть? PING
отвечает? конфиг == декларация? ACL-план == канону?); takeover (состояние в
etcd: journal/states/portalloc/endpoints/креды; двойной контроллер
невозможен — операции под клэймом); атомарность etcd (txn с compare:
mod_revision config, `version==0` клэймы, RMW endpoints); снапшоты
контроль-плейна `/valkey/`+`/valkeyworker/` — лидер регулярно + «до/после»
в provisioning/deprovisioning (переиспользуемый `SnapshotJob` Shared.Etcd,
retention); ретраи Polly jitter поверх оркестрации; отказ etcd —
контроль-плейн заморожен, живые Valkey-ноды от него не зависят (клиенты
работают по последнему снапшоту дискавери — fail-open).

### 4.7. Наблюдаемость

Health `/healthz` по канону честного health (t09): последнее состояние
цикла, а не первый сбой; структура всегда; SocketsHttpHandler +
IPv4-first-резолв (паттерн 16 §7). Секции: `etcd-reachable`, `docker-hosts`,
`loops-alive`, `claims`, `snapshot-freshness`. Единая правда для панели —
опрос `/healthz` живых инстансов по URL из `/valkeyworker/api/<id>`.
Prometheus-метрики — единый каркас arch/18: воркер-паттерн §2.2 (циклы/
клэймы/фазы/операции/снапшоты), `ValkeyWorker` в Meter-именах; коллектор
доменных метрик Valkey-нод (INFO) и дашборд — t05 (вне t01). Diag-ключи:
`/valkeyworker/work/<C>`, `nodes/node<k>/state`.

### 4.8. Конфигурация (заготовка для t02; канон фиксирует форму)

```
ValkeyWorker:Etcd { Endpoints[] }
ValkeyWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                      PortRange{From=17000,To=17999}, Images{Node="valkey/valkey:<пин>"} }
ValkeyWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000 }
ValkeyWorker:Thresholds { NodeBootSec=120, NodeDeadSec=90 }
ValkeyWorker:Parallelism { MaxClusters=4 }
ValkeyWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
ValkeyWorker:AdvertisedClientHost=null   # правило §4.2; стенды — host.docker.internal
ValkeyWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false,
                   Tls { ... } }         # mTLS-грань, паттерн 16 §1.1
# env-секреты: VWK_API_TLS_{CERT,KEY,CLIENT_CA} (§4.4)
```

### 4.9. Риски (заготовка таблицы канона)

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `valkey/valkey` сторонний (CVE/брейкинг-чейнджи) | версия пин (`Images:Node`); обновление — правкой настройки |
| R2 | Порт-коллизии 17000–17999 с ручными контейнерами | portalloc проверяет фактическую занятость; коллизия → сдвиг порта |
| R3 | `maxmemory_bytes` ≥ mem-лимит контейнера → OOM-килл | валидация заявки (t02/t03) + journal-warning конвергера; ответственность оператора (UI-предупреждение — t03); OOM-рестарт подхватит надзор C (кеш восполним) |
| R4 | Ротация: отказ между фазами E1–E3 | оба пароля валидны (окно двух паролей); повтор тика доигрывает; пересоздание контейнера в окне безопасно (аргументы из etcd) |
| R5 | Без TLS: трафик/креды в docker-сети | доверенная закрытая сеть установки (домашний контур, AGENTS п.8); зона доверия контроль-плейна; TLS — t06-valkey-tls |
| R6 | Потеря кеша при пересоздании контейнера (persistence off) | документированное поведение: холодный старт, кеш восполним приложениями |
| R7 | ACL-пароли в etcd — компрометация etcd = доступ к кешам | etcd — уже хранилище per-cluster-секретов (pg/kafka); закрытая сеть установки; ротация — процесс E |

## 5. Roadmap-изменения

### 5.1. Предусловие: перенос файлов трека в ветку

`arch/roadmap/valkey.md` и правка `arch/roadmap/README.md` (таблица треков —
строка valkey.md) существуют ТОЛЬКО незакоммиченными в рабочей копии
главного репозитория (`/Users/demakaev/ZCodeProject/pg/arch/roadmap/`); в
ветке worktree их НЕТ. Первый шаг исполнения — перенести текущее содержимое
обоих файлов из repo_root в ветку (git-снапшот: копия байт-в-байт, коммит
«roadmap: перенос valkey-трека»), и уже поверх — правки t01. Без этого
переноса мерж-гейт и шапка-ссылка не имеют объекта.

### 5.2. Шапка `arch/roadmap/valkey.md`

После переноса: шапка трека получает ссылку «канон появился:
[../20-valkey-clusters.md](…) + [../21-valkeyworker.md](…)» (по образцу
backup-трека после t01-backup-canon).

### 5.3. Новая задача `t06-valkey-tls` (требование пользователя)

В `arch/roadmap/valkey.md` (следующий свободный NN трека — t06) добавить
пункт с ПОДРОБНЫМ описанием «что для чего»:

> **`t06-valkey-tls`** `← t01-valkey-canon` — TLS клиентских подключений
> Valkey-кластеров (образец — kafka t03, arch/16 §2.3). **Что нужно
> сделать**: per-cluster CA (`ca_pem`/`ca_key` в
> `/valkey/clusters/<C>/`, ensure воркером при provisioning, подпись
> серверного сертификата ноды CN=`node<k>` + SAN advertised-хоста);
> tls-port контейнера (порт из portalloc; plain-порт закрывается);
> дискавери-ключ `ca_pem` для внешнего читателя — доверие клиентов
> StackExchange.Redis (`GetClientConfig()` получает `ssl=true` + CA);
> advertised/SAN-правило по 16 §2.1; окно двойного доверия при ротации CA —
> по потребности (образец CaRotator 16 §5 K). **Зачем**: шифрование
> клиентского трафика и аутентификация сервера (сейчас — доверенная
> docker-сеть + ACL-креды: пароли ходят по сети открыто в пределах закрытого
> контура). **Почему отложено**: кеш восполним и не содержит данных дороже
> секрета доступа; v1 живёт в доверенной закрытой docker-сети домашней
> установки (enterprise-защиты не нужны — AGENTS базовые правила п.8);
> контракт кред/endpoints не меняется — добавятся только CA-ключи, внешняя
> библиотека t04 совместима без переделок (обратная совместимость
> дискавери §3.5).

### 5.4. Мерж-гейт

Тем же мерж-коммитом: удалить пункт `t01-valkey-canon` из
`arch/roadmap/valkey.md` и снять `← t01-valkey-canon` из зависимостей
t02–t06 (правила `arch/roadmap/README.md`). Сам, без команды и без вопросов.

## 6. Фазы (порядок исполнения)

1. **Roadmap-перенос** (предусловие §5.1): valkey.md + README.md из repo_root
   → ветка, снапшот-коммит. Проверить: содержимое совпадает байт-в-байт,
   других расхождений трека нет.
2. **`arch/20-valkey-clusters.md`**: полный контракт по §3 (транспорт/имена,
   таблица ключей, канонические примеры, координация, дискавери,
   толерантность). Номер 20 свободен (проверено).
3. **`arch/21-valkeyworker.md`**: оркестратор по §4 (роль/границы, модель
   размещения, контракт-зеркало, секреты, процессы A–E, надёжность,
   наблюдаемость, конфигурация, риски). Номер 21 свободен (проверено).
4. **Указатели + roadmap**: `arch/README.md` (строки 20/21 в структуру +
   «Дальше»), шапка `arch/roadmap/valkey.md` (§5.2), новая задача
   `t06-valkey-tls` (§5.3).
5. **Ревью канонов** пользователем (гейт dev-flow): полнота против roadmap
   t01 и решений §1.
6. **Мерж-гейт** (§5.4): roadmap-правки тем же коммитом.

## 7. Ограничения

- t01 НЕ реализует: сервис ValkeyWorker и любой код/тесты (t02), панель
  valkey-домена и правки adminpanel-канона `arch/adminpanel/*` (t03),
  дискавери-библиотеку Puzzle (t04), метрики/коллектор/дашборд (t05),
  TLS (t06), зеркалирование образа в registry и `images.txt` (t02),
  deploy/dev-stand.
- Никаких изменений существующих канонов, кроме оговоренных указателей
  (`arch/README.md`) — домен самодостаточен в 20/21.
- Порты 17000–17999 — канон-решение: не пересекаются с pg 15000–15999,
  kafka 16000–16999 и стендовыми публикациями; тестовые диапазоны t02+
  обязаны следовать правилам динамических портов AGENTS.md.
- Канон фиксирует поведение/контракты, а не реализацию: имена классов/
  сборок t02 (ValkeyWorker.Core/Etcd/Provisioning/Docker по образцу
  KafkaWorker) — предмет plan-шага t02, в arch/21 — только конфиг-форма и
  переиспользование `Shared.{Core,Etcd,Metrics,Tls}`.
- Русский язык документации, английские идентификаторы; стиль arch/15/16.

## 8. Критерии приёмки

1. `arch/20-valkey-clusters.md` существует и покрывает: транспорт и имена
   (§3.1), таблицу ключей `/valkey/clusters/<C>/` с nodes-поддеревом nodes=1
   и всеми полями config (§3.2), канонические примеры значений (§3.3),
   координацию `/valkeyworker/` — порт `/kafkaworker/` 1:1 + единый
   `rotations/<C>` с полем `role` (§3.4), клиентский дискавери (§3.5:
   endpoints + app-креды + state; `GetClientConfig()` для
   StackExchange.Redis; ssl=false v1 + отсылка к t06), толерантность
   читателей (§3.6: parseError/`valkey-key-malformed`, `unknownKeys`,
   `valkey-endpoints-missing`, неполные креды → null, state raw-строка).
2. `arch/21-valkeyworker.md` существует и фиксирует: роль/границы с
   пятью процессами A–E (§4.1), модель размещения — образ
   `valkey/valkey:<пин>`, имена `vwk-<C>-node<k>`, без volume (persistence
   off), без per-cluster сети, порты 17000–17999, ACL-старт контейнера,
   advertised-правило (§4.2), контракт-зеркало (§4.3), секреты ACL admin+app
   (§4.4), машины состояний — provisioning PING-ready, deprovisioning
   «docker→etcd», надзор с S7/E9, converge maxmemory/policy+ACL, ротация
   окном двух паролей без рестартов (§4.5), надёжность/наблюдаемость/
   конфигурацию/риски (§4.6–4.9).
3. Указатели согласованы: `arch/README.md` (структура + «Дальше» — строки
   20/21), шапка `arch/roadmap/valkey.md` ссылается на arch/20+21.
4. Roadmap: `arch/roadmap/valkey.md` + `arch/roadmap/README.md` перенесены
   в ветку (снапшот §5.1 — фаза 1); добавлена `t06-valkey-tls` с полным
   описанием «что нужно / зачем / почему отложено» и зависимостью
   `← t01-valkey-canon` (§5.3).
5. Решения пользователя отражены в канонах: nodes-поддерево с nodes=1
   (20 §2); ACL-модель кред admin+app с ensure и ротацией без рестартов
   (20 §2, 21 §4.4/§4.5 E); без TLS в v1 с отсылками к `t06-valkey-tls`
   в 20 §5 (дискавери) и 21 §границы/риски.
6. Никакого кода/тестов/стенда/deploy не тронуто: изменения только в
   `arch/`, `arch/roadmap/` и `docs/superpowers/2026-09-16-t01-valkey-canon/`.
7. Мерж-коммит удалил пункт `t01-valkey-canon` и все `← t01-valkey-canon`
   из t02–t06 (§5.4).
