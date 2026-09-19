# 21. ValkeyWorker: оркестратор Valkey-кластеров ★

**ValkeyWorker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта [20-valkey-clusters.md](20-valkey-clusters.md):
панель AdminPanel **заявляет** кластер через **HTTP API воркера** (§1.1;
`state=NOT_INITIALIZED`) — воркер **поднимает** standalone-контейнер
Valkey, обеспечивает per-cluster ACL-креды, пишет факт (`endpoints`,
`nodes/node1/state`) и снимает `state`; перевод в `TO_REMOVE` — воркер
демонтирует кластер полностью. **Ответственность изменений etcd**: префиксы
`/valkey/`, `/valkeyworker/` пишет ТОЛЬКО ValkeyWorker (панель и сиды ходят
через его API); панель etcd только читает.

Пять процессов (машины состояний):
1. **Provisioning** (A, V0–V5) — от `NOT_INITIALIZED` до рабочего кластера;
2. **Deprovisioning** (B, X0–X3) — от `TO_REMOVE` до чистого etcd и
   удалённого контейнера;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся,
   молчащая нода помечается/пересоздаётся;
4. **ConfigConverger** (D) — converge `maxmemory`/`maxmemory_policy` к
   декларации (`CONFIG SET`, без рестартов) + converge ACL-плана
   (пользователи admin/app с правами канона);
5. **PasswordRotator** (E) — ротация per-cluster ACL-паролей (app/admin)
   без рестартов через окно двух паролей.

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы, [20](20-valkey-clusters.md) §3); смерть контролирующего
инстанса не роняет процессы — takeover ≤ TTL 15 с + тик; все операции
идемпотентны; значимое состояние переживает смерть контроллера (etcd).

Границы (что НЕ входит): TLS клиентских подключений (`t06-valkey-tls` —
per-cluster CA, tls-port, ключ `ca_pem`); реплики/sentinel/cluster-топологии
(кеш восполним, шардирование не нужно); панель valkey-домена (t03);
клиентская библиотека Puzzle (t04); persistence RDB/AOF — off по канону
(кеш восполним); квоты томов — томов нет.

---

## 1. Роль в системе и разделение ответственности

```
AdminPanel (UI)          ValkeyWorker (исполнитель)             docker-хосты
─────────────            ──────────────────────               ────────────
мутации (создание/ ──►   HTTP API воркера (§1.1)                контейнер
удаление кластера/       ──пишет──► /valkey/clusters/<C>/config  vwk-<C>-node<k>
конфиг-мутации;          .state=NOT_INITIALIZED/TO_REMOVE,      valkey/valkey:<пин>
ротация кред; сид)       nodes/node<k>/state (заявки),          (ACL, maxmemory,
                         /valkeyworker/rotations/<C>            standalone)
                         ──читает──► декларации
                         ──создаёт/удаёт──►
                         endpoints, states, креды,
                         снятие state
инспекция (read-   ◄──                                           
only, всё видит;          ключ /valkeyworker/api/<id> =
URL API — из etcd)        URL воркера (§1.1)
```

- **Панель** — декларатор и наблюдатель: **etcd только читает**
  (valkey-снапшот); все мутации valkey-домена отправляет в HTTP API
  воркера (§1.1).
- **ValkeyWorker** — исполнитель: единственный, кто создаёт/удаляет
  контейнеры нод, пишет `endpoints`, `nodes/node<k>/state`, креды
  app/admin, снимает `state` у config, чистит префикс кластера при
  TO_REMOVE — и единственный, кто **записывает декларации/заявки** в etcd
  (приёмник мутаций панели и сида через свой API, §1.1).

### 1.1. HTTP API воркера (мутации панели, сиды)

Та же HTTP-грань, что `/healthz` (порт `:8080`); mTLS по канону воркеров
(t03): клиенты (панель) аутентифицируются клиентским сертификатом
per-install API-CA; клиенты без валидного серта — отказ на TLS-хендшейке.
Префикс `/api` — приёмник ВСЕХ мутаций valkey-домена (панель в etcd
домена не пишет ничего). Плюс стендовый сид —
`POST /api/seed/demo` (демо-кластер; флаг
`ValkeyWorker:Api:EnableSeedEndpoint`, default `false`; идемпотентен;
паттерн [16](16-kafkaworker.md) §1.1).

**Дискавери API**: ключ `/valkeyworker/api/<id>` ([20](20-valkey-clusters.md)
§3) — ставит сам инстанс при старте; URL — из `ValkeyWorker:Api:AdvertiseUrl`
(достижим панелью). `cert_thumbprint` — SHA-256 серта, фактически
применённого на грани. Панель зовёт любой живой ключ (failover на
следующий; все умерли — 503 + critical-алерт `worker-api-unreachable`).

**Серверный серт API** — ключ `/workers/api_tls/valkeyworker` (JSON
`{"cert_pem","key_pem","updated_unix","updated_by"}`; пишет ТОЛЬКО панель —
канон ключа и протокол —
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.9):
воркер читает при старте и поднимает грань на этом серте; env-секреты
`VWK_API_TLS_{CERT,KEY,CLIENT_CA}` — бутстрап-фоллбек (применяется с t02).

Детальные контракты эндпоинтов — панельная проекция
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §11.2
(зафиксированы t03; в t01 фиксировалась только грань и её транспорт).

## 2. Модель размещения

- **Образ** — официальный `valkey/valkey:<пин версии>` (настройка
  `Images:Node`), полностью конфигурируется аргументами командной строки.
  Кастомный образ НЕ собирается. Зеркалирование в локальный registry
  `192.168.0.1:5000` (строка в `images.txt` + `mirror-image.sh`,
  мульти-арх) — забота t02 (ранбук `docs/runbook.md`).
- **Нода кластера = один контейнер** `vwk-<C>-node<k>` (имя
  детерминировано; при nodes=1 — `vwk-<C>-node1`), restart-политика
  `unless-stopped`.
- **ACL при старте**: аргументы командной строки
  `--user default off --user admin on ><пароль> ~* +@all --user app on
  ><пароль> ~* +@read +@write +ping +echo` (детерминированы от кредов etcd —
  пересоздание контейнера собирает актуальные пароли; default-пользователь
  отключён — вход только по именованным ACL-пользователям; `+ping +echo`
  обязательны: клиенты StackExchange.Redis шлют ECHO/PING при
  handshake/keepalive — вне @read/@write, без них соединение не поднимается).
  Ротация паролей
  на живой ноде — через `ACL SETUSER` по соединению (процесс E), без
  пересоздания.
- **maxmemory**: аргументы `--maxmemory <bytes> --maxmemory-policy <policy>`
  при создании (политика — из декларации, канон-дефолт `allkeys-lru`,
  [20](20-valkey-clusters.md) §2) + converge D (`CONFIG SET`) при мутациях
  декларации — без рестартов.
- **Persistence off**: без volume, `--save ""`/`--appendonly no` (детали
  флагов — t02; канон фиксирует: без тома, снапшоты RDB/AOF не пишутся).
  Потеря контейнера = холодный старт кеша (документированное поведение).
- **Сеть**: per-cluster сеть НЕ создаётся (нет inter-node трафика;
  клиентский доступ — публикация host-порта из portalloc; контейнер живёт
  в сети запуска воркера — деталь t02). Прямое следствие: домен не порождает
  `*-net-*` объектов, пул подсетей docker не расходует (урок инцидента t05
  про kfw-net).
- **Порты**: диапазон `17000–17999` (продолжение ряда pg 15000–15999 /
  kafka 16000–16999), **1 клиентский порт на ноду** (контейнерный 6379 →
  выделенный host-порт), закрепление `/valkeyworker/portalloc/<C>`;
  занятость для довыделения = docker-публикации ∪ portalloc ВСЕХ чужих
  кластеров; довыделение — только держателем
  `/valkeyworker/locks/portalloc` (t90-паттерн, [20](20-valkey-clusters.md)
  §3).
- **Advertised-правило** (порт [16](16-kafkaworker.md) §2.1):
  advertised-хост ноды = `ValkeyWorker:AdvertisedClientHost`, если задан;
  иначе — имя docker-хоста размещения. Требование: значение обязано
  резолвиться КЛИЕНТАМИ (оно попадает в `endpoints`). Для локальных стендов
  — `host.docker.internal`.
- **Лимиты** контейнера из `resources` (cpu/mem; disk — инфо-поле).
  Инвариант: `maxmemory_bytes < mem`-лимит (валидация заявки; иначе
  OOM-килл — риск R3).
- Сам воркер — контейнер с `docker.sock`, поставляется через
  `deploy/docker-compose.yml` (сборка — t02, по образцу KafkaWorker).

## 3. Контракт etcd

Транспорт и схема ключей — [20-valkey-clusters.md](20-valkey-clusters.md)
(зеркало §2/§3). Читаемые/пишемые воркером — таблицы ниже. Смежный ключ вне
префикса — `/workers/api_tls/valkeyworker` (серверный серт mTLS-грани API;
пишет ТОЛЬКО панель, воркер читает при старте — adminpanel/02 §9.9;
применяется с t02).

### 3.1. Читаемые ключи

| Ключ | Зачем |
|---|---|
| `/valkey/clusters/<C>/config` | заявка (nodes/maxmemory_*/created_unix) + `state` (NOT_INITIALIZED/TO_REMOVE/отсутствует=Active) — целиком, вкл. state-заявки |
| `/valkey/clusters/<C>/nodes/node<k>/state` | заявки панели NOT_INITIALIZED/TO_REMOVE (+ свои записи — сверка) |
| `/valkey/clusters/<C>/nodes/node<k>/resources` | лимиты контейнера (cpu/mem; disk — инфо) |
| `/valkeyworker/rotations/<C>` | заявка ротации креда (`role`: app\|admin) — процесс E |
| `/workers/api_tls/valkeyworker` | серверный серт mTLS-грани API (§1.1): читает ТОЛЬКО при старте; пишет панель (adminpanel/02 §9.9) |

### 3.2. Пишемые ключи

| Ключ | Когда | Значение |
|---|---|---|
| `/valkey/clusters/<C>/nodes/node<k>/state` | весь жизненный цикл | PROVISIONING/RUNNING/UNREACHABLE/REMOVING |
| `/valkey/clusters/<C>/endpoints` | после подъёма; RMW при изменениях нод | `h1:p1,...` — advertised-хост + клиентский порт из portalloc |
| `/valkey/clusters/<C>/app_user` + `app_password` | provisioning ensure; ротация E | `"app"` / 32 симв; txn put-if-absent / txn-коммит ротации |
| `/valkey/clusters/<C>/admin_user` + `admin_password` | provisioning ensure; ротация E | `"admin"` / 32 симв; txn put-if-absent / txn-коммит ротации |
| `/valkey/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без** `state` (compare mod_revision) |
| `/valkeyworker/*` (координация) | весь жизненный цикл | leader, claims, work (+ вложенный work/&lt;C&gt;/rotation — стейт доигрывания E, §5 E), portalloc, locks/portalloc, instances, api — префикс `/valkeyworker/` ([20](20-valkey-clusters.md) §3) |
| `/valkeyworker/rotations/<C>` | по завершении ротации (E) | del заявки (или панелью — отмена) |
| `/valkey/clusters/<C>/` (префикс) | TO_REMOVE, финал X2 | `del --prefix` |
| `/valkeyworker/{claims,work,portalloc,rotations}/<C>*` | TO_REMOVE, финал X2 | del — очистка координации ВКЛЮЧАЯ заявки ротаций: остаточные заявки не переживают удаление кластера |

## 4. Секреты

Per-cluster, в etcd, генерирует воркер (ensure txn put-if-absent):

- **app** — `app_user`=`"app"`, `app_password` (32 симв `[A-Za-z0-9]`):
  приложения (ACL-роль app). Ротация — процесс E по заявке панели.
- **admin** — `admin_user`=`"admin"`, `admin_password` (32 симв): воркер
  (converge/ACL/пробы), панель (read-only пробы). Ротация — процесс E.

Env-секреты per-install — только TLS HTTP API воркера
(`VWK_API_TLS_{CERT,KEY,CLIENT_CA}`, паттерн [16](16-kafkaworker.md) §4);
per-cluster-секреты живут в etcd (зона доверия контроль-плейна; парольная
защита кеша — достаточный уровень для домашнего контура, TLS-транспорт —
t06).

## 5. Процессы (машины состояний)

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

### A. ProvisioningProcess (V0–V5)

```
V0 claim + journal(op=provision); снапшот «до»
V1 план: placement + порт-аллокация — под глобальным portalloc-клэймом
   /valkeyworker/locks/portalloc: занято = docker-публикации ∪ portalloc
   чужих кластеров; не взял клэйм → journal waiting-portalloc-lock
   (следующий тик); journal phase=planned
V2 ensure секретов: admin + app — txn put-if-absent
   (проигрыш → re-read существующих)
V3 контейнер (аргументы ACL/maxmemory из декларации и кредов, лимиты
   resources, клиентский host-порт) + state=PROVISIONING;
   существующий (re-run) — сверка и пропуск
V4 ждать готовности: PING с admin-кредом отвечает (бюджет NodeBootSec,
   транзиент-толерантно) → state=RUNNING
V5 put endpoints (advertised host:clientPort); config: txn
   (compare mod_revision) → put канонического JSON без state;
   снапшот «после»; journal done
```

Гонка «панель пишет TO_REMOVE посреди provisioning»: перечитывание config
перед фазами — смена state безопасно прекращает процесс.

### B. DeprovisioningProcess (X0–X3)

```
X0 claim + journal(op=deprovision); снапшот «до»
X1 docker: удалить контейнер vwk-<C>-* (404 = ок); порядок «сначала
   docker, потом etcd»; томов нет
X2 etcd: del --prefix /valkey/clusters/<C>/ + del
   /valkeyworker/{claims,work,portalloc,rotations}/<C>* — очистка
   координации ВКЛЮЧАЯ заявки ротаций
X3 снапшот «после»; клэйм снят явно (del + revoke lease)
```

### C. NodeSupervisor (надзор)

Сверка декларации с фактом docker + PING-проба (с admin-кредом).

- **Снесённый контейнер** (docker-факт, не зависит от пробы) →
  пересоздание с аргументами из текущей декларации и кредов etcd,
  `state=PROVISIONING`; в `RUNNING` переводит следующий цикл по PING.
- **Автоконверге лимитов `resources`** (применение изменений заявки): тик
  сверяет лимиты живого контейнера (inspect: NanoCpus/Memory) с
  `nodes/node<k>/resources` (cpu/mem; disk — инфо-поле, не сверяется) —
  расхождение → пересоздание контейнера с лимитами декларации (docker не
  меняет лимиты живого контейнера; кеш восполним — пересоздание дёшево),
  `state=PROVISIONING`. Дисциплина та же — **одно пересоздание за тик**
  (по любой причине); слепой inspect (docker-хост молчит) — ошибка тика,
  пересозданий вслепую нет.
- **Нода молчит** дольше `NodeDeadSec` → `state=UNREACHABLE` +
  пересоздание контейнера **без тома — данных не жалко** (кеш восполним;
  документированное поведение). **Слепая проба — никаких действий**:
  собственная слепота воркера не повод трогать ноду (S7, arch/17).
- **Одно пересоздание за тик** — никаких массовых пересозданий подряд.
- **Кеш неприкосновенен на etcd-уровне**: надзор никогда не чистит ключи
  домена из-за недоступности ноды (декларация/креды переживают всё;
  содержимое кеша — дело приложений).
- **Лестница E9** (arch/17): нода без записи portalloc — до любых
  деструктивных действий — реконструкция из inspect живого контейнера
  (published-порт + host, put-if-absent под `locks/portalloc`, проигрыш
  txn → re-read) → новая аллокация (свидетельство смерти по S7).
- Ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` чужих процессов надзор не
  трогает.

### D. ConfigConverger

Active-ветка, лёгкий: `CONFIG GET maxmemory`/`maxmemory-policy` (по
admin-креду) vs `config.{maxmemory_bytes,maxmemory_policy}` → при отличии
`CONFIG SET` (без рестартов). Маппинг: `maxmemory_bytes`→`maxmemory`,
`maxmemory_policy`→`maxmemory-policy`. **ACL-план**: `ACL LIST` vs канон
(пользователи admin/app с правами §2; `default off`) → идемпотентный
`ACL SETUSER`-converge. Расхождение `maxmemory_bytes` ≥ mem-лимита —
journal-warning (ответственность оператора, по образцу R7
[16](16-kafkaworker.md)).

### E. PasswordRotator (окно двух паролей, без рестартов; роли app|admin)

Заявка `/valkeyworker/rotations/<C>` (`role`); NEW = генерация (32 симв).

```
E1 ACL SETUSER <role> >NEW    — оба пароля (OLD+NEW) валидны, клиенты работают со OLD
E2 txn: [compare value(<role>_password)==OLD][put NEW]
   — клиенты перечитывают etcd и переподключаются с NEW;
   del заявки — ОТДЕЛЬНАЯ условная txn [compare value(rotations/<C>)==payload из стейта][del]:
   чужая/снятая панелью заявка не трогается (коммит E2/переход стейта от del не зависят)
E3 ACL SETUSER <role> <OLD    — удаление старого пароля
```

Отказ между фазами безопасен (оба пароля валидны; повтор тика доигрывает
по journal-фазе). Стейт доигрывания (фаза + OLD/NEW) ротатор держит в
**отдельном ключе** `work/<C>/rotation`: ключ журнала `work/<C>` надзор (C)
перезаписывает каждый тик — фаза ротации в нём не переживает тик, а NEW,
добавленный на ноду в E1, обязан доигрываться тем же значением (свежая
генерация на доигрывании оставила бы на ноде валидный «осиротевший» пароль).
E3 доигрывается по фазе `e2-committed` **даже без заявки** (краш между E2 и
E3: заявка уже снята — без стейта OLD остался бы валидным навсегда). Сравнение
E2 не прошло из-за того, что пароль уже NEW, — не тупик: прошлый тик мог
закоммитить txn и упасть до записи стейта (краш/отказ put) — ротатор
перечитывает `<role>_password`: совпало с NEW из стейта → стейт промотится
в `e2-committed`, доигрывание продолжится; иное значение — параллельная
ротация (Failed, ретрай тиком). В
отличие от kafka ([16](16-kafkaworker.md) §5 H) —
**без пересозданий контейнеров** (ACL — runtime); пересоздание надзором в
окне ротации безопасно: аргументы собираются из etcd-актуальных кредов
(фаза E1 уже закоммитила NEW — контейнер соберётся с NEW, OLD-пароль
доочистит следующий тик E3; расхождение самолечится converge D). Ротация
admin не трогает app-кред и наоборот. Битая заявка — мусор: del с journal
(панель до того получает 409 «уже запрошена»).

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть? PING
  отвечает? конфиг == декларация? ACL-план == канону?); именование
  детерминировано (`vwk-<C>-node<k>`, порты в portalloc).
- **Takeover**: состояние в etcd (journal, states, portalloc, endpoints,
  креды); смерть инстанса гасит lease ≤ 15 с — следующий продолжает с
  journal-фазы. Двойной контроллер невозможен: операции — только под живым
  клэймом.
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы/заявки, RMW endpoints).
- **Снапшоты** контроль-плейна `/valkey/`+`/valkeyworker/`: лидер регулярно
  + «до/после» в provisioning/deprovisioning (переиспользуемый
  `SnapshotJob` Shared.Etcd, retention).
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма ноды —
  транзиент-толерантный цикл с бюджетом `NodeBootSec`.
- **Отказ etcd**: контроль-плейн заморожен; живые Valkey-ноды от него не
  зависят (клиенты работают по последнему снапшоту дискавери — fail-open).

## 7. Наблюдаемость

Health `/healthz` по канону честного health (t09): последнее состояние
цикла, а не первый сбой; структура всегда. Секции: `etcd-reachable`,
`docker-hosts`, `loops-alive`, `claims`, `snapshot-freshness`. Etcd-клиент
— SocketsHttpHandler + IPv4-first-резолв (паттерн [16](16-kafkaworker.md)
§7). **Единая правда для панели**: опрос `/healthz` живых инстансов по URL
из `/valkeyworker/api/<id>`. Prometheus-метрики — единый каркас
[18-metrics.md](18-metrics.md) §2.2 (воркер-паттерн: циклы/клэймы/фазы/
операции/снапшоты; `ValkeyWorker` в Meter-именах); коллектор доменных
метрик INFO и дашборд — [18-metrics.md](18-metrics.md) §4.2/§2.6. Diag-ключи:
`/valkeyworker/work/<C>`, `nodes/node<k>/state`.

## 8. Конфигурация (appsettings + env-оверрайды)

```
ValkeyWorker:Etcd { Endpoints[] }
ValkeyWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                      PortRange{From=17000,To=17999}, Images{Node="valkey/valkey:<пин>"} }
ValkeyWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000 }
ValkeyWorker:Thresholds { NodeBootSec=120, NodeDeadSec=90 }
ValkeyWorker:Parallelism { MaxClusters=4 }
ValkeyWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
ValkeyWorker:AdvertisedClientHost=null   # правило §2; стенды — host.docker.internal
ValkeyWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false,
                   Tls { ... } }         # mTLS-грань, паттерн 16 §1.1
# env-секреты: VWK_API_TLS_{CERT,KEY,CLIENT_CA} (§4)
```

`AdvertisedClientHost=null` допустим только когда имя docker-хоста
резолвимо клиентами само по себе; для локальных стендов —
`host.docker.internal` (правило §2).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `valkey/valkey` сторонний (CVE/брейкинг-чейнджи) | версия пин (`Images:Node`); обновление — правкой настройки |
| R2 | Порт-коллизии 17000–17999 с ручными контейнерами | portalloc проверяет фактическую занятость; коллизия → сдвиг порта |
| R3 | `maxmemory_bytes` ≥ mem-лимит контейнера → OOM-килл | валидация заявки (t02/t03) + journal-warning конвергера; ответственность оператора (UI-предупреждение — t03); OOM-рестарт подхватит надзор C (кеш восполним) |
| R4 | Ротация: отказ между фазами E1–E3 | оба пароля валидны (окно двух паролей); повтор тика доигрывает; пересоздание контейнера в окне безопасно (аргументы из etcd) |
| R5 | Без TLS: трафик/креды в docker-сети | доверенная закрытая сеть установки (домашний контур, AGENTS п.8); зона доверия контроль-плейна; TLS — t06-valkey-tls |
| R6 | Потеря кеша при пересоздании контейнера (persistence off) | документированное поведение: холодный старт, кеш восполним приложениями |
| R7 | ACL-пароли в etcd — компрометация etcd = доступ к кешам | etcd — уже хранилище per-cluster-секретов (pg/kafka); закрытая сеть установки; ротация — процесс E |

---

→ Возврат к [README.md](README.md). Контракт etcd —
[20-valkey-clusters.md](20-valkey-clusters.md); отложенные задачи —
[roadmap/valkey.md](roadmap/valkey.md).
