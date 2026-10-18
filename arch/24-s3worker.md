# 24. S3Worker: оркестратор S3-кластеров ★

**S3Worker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта [23-s3-clusters.md](23-s3-clusters.md): панель
AdminPanel **заявляет** кластер через **HTTP API воркера** (§1.1;
`state=NOT_INITIALIZED`) — воркер **поднимает** инстансы (volume данных +
контейнер ownS3 на хосте из placement-селектора, репликационный PKI из
etcd — arch/23 §6), создаёт bucket per-install, доводит граф репликации
(links → join) и deny-list до инстансов, пишет `endpoints` и снимает
`state`; перевод в `TO_REMOVE` — воркер демонтирует кластер полностью.
**Ответственность изменений etcd**: префиксы `/s3/`, `/s3worker/` пишет
ТОЛЬКО S3Worker (панель и сиды ходят через его API); панель etcd домена
S3 только читает.

Семь процессов (машины состояний):
1. **ProvisioningProcess** (A, P0–P5) — от `NOT_INITIALIZED` до рабочего
   кластера (инстансы, bucket, mesh по links, `endpoints`; 0 инстансов —
   без docker-мутаций);
2. **DeprovisioningProcess** (B, X0–X3) — от `TO_REMOVE` до чистого etcd
   и снесённых docker-объектов;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся,
   недоступный инстанс помечается, занятость диска собирается;
4. **AddNodeProcess** (D) — добавление инстанса в живой кластер (подъём
   по A; включение в mesh — доводкой G);
5. **RemoveNodeProcess** (E) — вывод инстанса (гварды, снос, чистка;
   связи сносятся доводкой G);
6. **SyncProbeProcess** (F) — зонд связности/синхронности
   (`connectivity`/`sync` arch/23 §3; каркас канонизируется, реализация —
   t33);
7. **MeshGovernance** (G, доводка состава) — приведение фактического
   mesh инстансов к декларации: links → join/удаление target-записей,
   deny-list → отзовы (§5 G).

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы, arch/23 §8); смерть контролирующего инстанса не роняет
процессы — takeover ≤ TTL 15 с + тик; все операции идемпотентны; значимое
состояние переживает смерть контроллера (etcd).

Границы (что НЕ входит): механика mesh-репликации ownS3 (канон
[owns3/07-replication.md](../OwnS3/arch/owns3/07-replication.md)); зонды
синхронности (t33); панель домена S3 (канон adminpanel/02 §12).

---

## 1. Роль и разделение ответственности

```
AdminPanel (UI)          S3Worker (исполнитель)                 docker-хосты
──────────────           ──────────────────────                 ────────────
мутации (создание/ ──►   HTTP API воркера (§1.1)                 контейнер
удаление кластера/       ──пишет──► /s3/clusters/<C>/config      s3w-<C>-<I>
состава инстансов/       .state=NOT_INITIALIZED|TO_REMOVE,       (ownS3),
ресурсы; граф links;     nodes/<I>/state (заявки),               volume данных,
deny/allow; ротация      /s3worker/rotations/<C>                 S3-порт из portalloc
секретов; зонд           ──читает──► декларации
по команде)              ──создаёт/удаляет──► контейнеры s3w-<C>-<I>,
                         endpoints, states (PROVISIONING/RUNNING/...),
                         PKI (pki/*), снятие state
                         ──доводит──► join/targets/deny-list инстансов
                         (mesh по links, процесс G)
инспекция (read-   ◄──
only, всё видит;          ключ /s3worker/api/<id> =
URL API — из etcd)        URL воркера (§1.1)
```

Отдельная строка: **PgWorker — клиент пула** (читатель `endpoints`),
участником контракта НЕ является — в `/s3/` и `/s3worker/` не пишет и
docker-объектов домена не создаёт.

- **Панель** — декларатор и наблюдатель: **etcd только читает**
  (арх.23 §9); все мутации S3-домена отправляет в HTTP API воркера (§1.1).
- **S3Worker** — исполнитель: единственный, кто создаёт/удаляет
  контейнеры и volumes инстансов, пишет `endpoints`, `nodes/<I>/state`,
  `placed_on`, `usage`, выпускает/хранит репликационный PKI (`pki/*`,
  `nodes/<I>/pki`), доводит mesh (join/targets/deny-list) к декларациям
  `links`/`denylist`, снимает `state` у config, чистит префикс кластера
  при TO_REMOVE — и единственный, кто **записывает декларации/заявки** в
  etcd (приёмник мутаций панели и сида через свой API, §1.1).

### 1.1. HTTP API воркера (мутации панели, сиды)

Та же HTTP-грань, что `/healthz` и `/metrics`. mTLS по общему паттерну
воркеров ([14-pgworker.md](14-pgworker.md) §1.1): клиенты (панель)
аутентифицируются клиентским сертификатом, подписанным единой per-install
API-CA `kfw-install-ca` (единая пакета воркеров установки — отдельные
клиентские серты панели); клиент без валидного серта — отказ на
TLS-хендшейке. Префикс `/api` — приёмник ВСЕХ мутаций S3-домена (панель в
etcd домена не пишет ничего).

**Серверный серт API** — ключ `/workers/api_tls/s3worker` (канон ключа и
протокол — [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§9.9; пишет ТОЛЬКО панель): воркер читает при старте и поднимает
грань на этом серте; env-секреты `S3W_API_TLS_{CERT,KEY,CLIENT_CA}` —
бутстрап-фоллбек при отсутствии ключа. `/healthz`, `/metrics` — та же
mTLS-грань (скрейп Prometheus — по mTLS, arch/18).

**Дискавери API**: ключ `/s3worker/api/<id>` (arch/23 §8) — ставит сам
инстанс; URL — из `S3Worker:Api:AdvertiseUrl` (достижим панелью);
`cert_thumbprint` — SHA-256 серта, фактически применённого на грани.
Панель зовёт любой живой ключ (failover на следующий; все умерли — 503 +
critical-алерт).

**Эндпоинты** (сигнатуры/гварды — канон поведения исполнителя, панель —
прокси; протоколы записи — по образцу
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§9.2/§9.4/§9.5/§9.6):

| Метод+путь | Назначение |
|---|---|
| `POST /api/s3/clusters` | создание кластера: клэйм-txn `version(config)==0` + пакет PUT (config c `nodes=len(instances)` + instances × {placement, resources, state=NOT_INITIALIZED}; 0 инстансов — только config); запись — ОДНА атомарная txn: сбой midway частичного состояния не оставляет, компенсация не требуется |
| `DELETE /api/s3/clusters/{c}` | перевод `config.state=TO_REMOVE` (RMW; already → 204 без записи) |
| `POST /api/s3/clusters/{c}/nodes` | добавление инстанса с заданным именем (тело `{name, placement, resources}`): клэйм-txn `version(nodes/<I>/state)==0` + put `placement`/`resources`/`state=NOT_INITIALIZED` |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | маркер демонтажа инстанса `nodes/<n>/state=TO_REMOVE` (one-way; гварды: не последний инстанс кластера — 0 инстансов валиден только декларацией при создании, вывод последнего → 409; путь к кластеру без инстансов — снос кластера целиком) |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | мутация ресурсов инстанса (cpu/mem — rolling-пересоздание контейнера надзором; disk — декларация/наблюдение) — образец: kafka-мутация №15 «Изменение ресурсов брокера» ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §10.2) |
| `PUT /api/s3/clusters/{c}/links` | замена графа репликации целиком (тело `{links:[{a,b}…]}`; валидация — ниже) → 202; применяет доводка G (§5) |
| `POST /api/s3/clusters/{c}/nodes/{n}/deny` | отзыв ключа инстанса: имя в `denylist` кластера → 202 (доводка G: `PUT /replication/denylist` всем инстансам, связи с ним сносятся) |
| `POST /api/s3/clusters/{c}/nodes/{n}/allow` | снятие отзыва (имя из `denylist`) → 202 |
| `POST /api/s3/clusters/{c}/rotations` | заявка ротации секретов профиля кластера (`/s3worker/rotations/<C>`, клэйм-txn — §9.8-паттерн) |
| `POST /api/s3/clusters/{c}/sync-probe` | внеплановый зонд синхронности/связности (202 — заявка `/s3worker/probes/<C>` put-if-absent, already → 202 идемпотентно; исполняет зонд F, результат — ключи arch/23 §3) — реализация t33 |

**Валидация** (сервер — источник истины): `name` кластера — паттерн
arch/23; `platform` — единственное значение `owns3`; инстансы — 0..9
(рекомендация 2–3 — гео-минимум и бюджет R1, arch/23 §2); имя инстанса —
паттерн кластера `^[a-z][a-z0-9_]{0,62}$`, НЕ зарезервировано
(`orchestrator` — PKI оркестратора, arch/23 §6), уникально в кластере
(409 при коллизии); `links` — имена существуют в кластере, `a≠b`, пары
уникальны (400); `deny/allow` — имя существует (404); `placement` —
обязательный валидный селектор для каждого инстанса (400); `resources` —
cpu 0.01..64, mem/disk 1..65536 GiB (канон форматов — adminpanel/02
§9.3); `secret_profile` — непустое имя, существующее в env воркера (422 с
перечнем доступных имён, без значений секретов).

## 2. Модель размещения

- **Транспорт docker** — общий клиент `Shared.Docker` (канон-суперсет,
  образец [14-pgworker.md](14-pgworker.md) §2.2.1): plain/swarm, транспорт
  unix/tcp+TLS/SSH; env `S3W_DOCKER_TLS_{CA,CERT,KEY}`,
  `S3W_DOCKER_SSH_KEY[_PATH]`, `S3W_DOCKER_SSH_FINGERPRINT`.
- **Конфиг** (§8): `S3Worker:Docker { Mode, Hosts[{Name,Endpoint}],
  SwarmManager, PortRange{From,To}, Images{OwnS3}, Tls, Ssh,
  ContourNetwork, AdvertisedHost }`.
- **Атрибуты хостов** для placement-селекторов (модель — arch/23 §4):
  plain — имя из `Hosts[]` (атрибут `docker_host`); swarm — hostname нод и
  `node.labels.*` (атрибуты `swarm_node`/`swarm_labels`, `GET /nodes`).
- **Portalloc**: S3-порт ноды (контейнерный 9000) → последовательные
  host-слоты диапазона `PortRange`; закрепление
  `/s3worker/portalloc/<C>` (переживает rebuild); довыделение — только
  держателем глобального клэйма `/s3worker/locks/portalloc` (t90-паттерн:
  занятость = docker-публикации ∪ portalloc всех кластеров — образец
  [14-pgworker.md](14-pgworker.md) §2.4).
- **Advertised-адреса** — только от фактических публикаций
  (`S3Worker:Docker:AdvertisedHost` по образцу
  [14-pgworker.md](14-pgworker.md) §2.4 п.5 — single-host/стенд; multi-host —
  фактические имена хостов). Требование: значение обязано резолвиться
  клиентами — оно попадает в `endpoints`.
- **Сеть**: per-cluster сеть НЕ создаётся (межнодовая коммуникация mesh и
  клиенты — по advertised `host:port`, единый namespace адресов; отказ от
  аналога `pgw-net-<C>` осознанный — Patroni-подобного внутреннего
  кластерного протокола у нод S3 нет). Опция `ContourNetwork` (стенд/E2E) —
  ноды создаются в сети контура поставки, адресация по именам сети, сеть
  воркером не создаётся/не удаляется (как
  [14-pgworker.md](14-pgworker.md) §2.1). Не поддерживается в Mode=Swarm —
  fail-fast старта: сеть overlay назначает сам Swarm.
- **Имена docker-объектов**: контейнер инстанса `s3w-<C>-<I>`
  (детерминирован: имя инстанса из декларации), volume данных
  `s3w-<C>-<I>-data`; restart-политика `unless-stopped`. Перечисление
  объектов кластера (сироты X2, занятость) — по префиксу `s3w-<C>-` +
  label `s3worker` (имена инстансов произвольные — перебор
  детерминированных имён невозможен).
  Сам воркер — контейнер с `docker.sock`, поставка через
  `deploy/docker-compose.yml`; **два инстанса по умолчанию** (образец pg
  t07 — [14-pgworker.md](14-pgworker.md) §2.4): уникальный хост-порт API и
  уникальный `Api:AdvertiseUrl` на инстанс, уникальность — случайный
  `InstanceId`.

## 3. Контракт etcd

Транспорт и схема ключей — [23-s3-clusters.md](23-s3-clusters.md) (§2/§3 —
декларации и статусы, §8 — координация `/s3worker/`); читаемые/пишемые
ключи — ссылками туда (источник истины — не пересказ). Здесь — только
координационное поведение:

- **Инвариант мутаций**: любые записи в `/s3/` — только держателем клэйма
  `<C>` (`/s3worker/claims/<C>`); API воркера пишет декларации/заявки под
  клэйм-txn `version==0` (создание кластера/ноды, заявка ротации) —
  панель через него, никогда напрямую.
- **Txn с compare**: снятие `config.state` (P5) и перевод в `TO_REMOVE`
  сравниваются по `mod_revision` config — внешняя правка между read и
  write закрывается ретраем тика; RMW `endpoints` — по value
  прочитанного значения.

## 4. Секреты

- **Per-install env-профили** `S3W_NODE_SECRETS__<PROFILE>__*` (формат и
  имена переменных — arch/23 §6): root-пара + опциональные статические
  access keys (read-write бэкапам / read-only панели); профиль един на
  кластер (поле `config.secret_profile`); креды клиентской S3-грани.
- **PKI репликации per-cluster — etcd** (arch/23 §6 «PKI репликации»):
  CA кластера + сертификаты инстансов + сертификат оркестратора
  выпускает воркер (ECDSA P-256, механика owns3/07 §2), хранит в
  `/s3/clusters/<C>/pki/*` и `nodes/<I>/pki`; бессрочные; переживают
  provision/переезд контейнера.
- **Передача в инстансы** — env контейнера при создании:
  `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` + `OWNS3_ACCESS_KEYS__<i>__*` +
  репликационный материал `OWNS3_REPLICATION_{CERT,PRIVATE_KEY,TRUST_CA}`
  / `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` (значения; arch/23 §6);
  значения секретов никогда не попадают в журнал.
- **Ротация**: заявка `/s3worker/rotations/<C>` (§1.1) →
  rolling-пересоздание инстансов (по одной за тик, данные volume не
  трогаются; PKI-материал переиспользуется из etcd — ключи переживают
  пересоздание; рестарт-семантика платформ: ownS3 — переконфигурация env +
  рестарт, owns3/05 §2) → del заявки по завершении. Ротация ключей
  репликации/CA — вне (roadmap, owns3/07 §12); отзыв — deny-list (§5 G).
- **Транспортные секреты** API/docker — env `S3W_*` (§1.1/§2).
  Серверный серт API — ключ `/workers/api_tls/s3worker` (панель пишет;
  env-фоллбек `S3W_API_TLS_*`).

## 5. Процессы (машины состояний)

**Состояния инстанса** (`nodes/<I>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт) → `RUNNING` (health-грань
отвечает); `UNREACHABLE` (молчит дольше NodeDeadSec); `REMOVING`
(демонтаж); `TO_REMOVE` (маркер панели, one-way).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning (A);
`TO_REMOVE` → Deprovisioning (B); иначе Active-ветка: надзор (C) →
add-node (D) → remove-node (E) → доводка состава (G) → зонд (F). Все
операции — только под живым клэймом `<C>`; journal-before-manipulations
(`/s3worker/work/<C>`); все шаги идемпотентны (перепроверка факта).

### A. ProvisioningProcess (P0–P5)

```
P0 claim + journal(op=provision); перечитка config
   (гонка TO_REMOVE посреди provisioning — безопасная остановка)
P1 резолв placement всех инстансов (arch/23 §4: селектор → кандидаты →
   детерминированный выбор → placed_on) + portalloc — под глобальным
   клэймом /s3worker/locks/portalloc (§2); не взял клэйм → journal
   waiting-portalloc-lock (следующий тик); валидация профиля секретов
   (env воркера; неполный → permanent-отказ фазы с диагностикой, §4);
   0 инстансов — фазы P1–P4 пустые (нет docker-мутаций)
P2 создание инстансов: PKI — ленивый выпуск (txn put-if-absent):
   pki/ca + pki/orchestrator (нет), затем nodes/<I>/pki каждого (нет);
   volume данных + контейнер ownS3 (образ из Images{OwnS3}; env: креды
   профиля — OWNS3_ROOT_*/OWNS3_ACCESS_KEYS__*, репликационный материал —
   OWNS3_REPLICATION_{CERT,PRIVATE_KEY,TRUST_CA} +
   OWNS3_REPLICATION_ADVERTISED_ENDPOINT (arch/23 §6), OWNS3_DATA_DIR=/data;
   состав mesh НЕ env — доводится P4/G; лимиты resources; S3-порт из
   portalloc);
   health-ожидание — бюджет NodeBootSec, transient-толерантный цикл
   (поллинг-инвариант t19 — без одиночных блокирующих вызовов)
P3 bucket per-install на каждом инстансе — идемпотентно (exists → пропуск;
   CreateBucket root-кредом профиля — права admin, owns3/05 §3)
P4 доводка состава mesh по links (arch/23 §2) — та же механика, что G
   (§5 G): deny-доведение + join недостающих пар + снос лишних
   target-записей; links пуст/0 инстансов — no-op
P5 put endpoints (advertised host:S3-порт; 0 инстансов — ключ не
   пишется) + config: txn (compare mod_revision) → put
   канонического JSON без state; journal done
```

### B. DeprovisioningProcess (X0–X3)

```
X0 claim + journal(op=deprovision)
X1 маркеры REMOVING всем инстансам
X2 docker: снос контейнеров s3w-<C>-* и volumes данных (все хосты
   Hosts[]; 404 = ок) + чистка portalloc <C>; порядок «сначала docker,
   потом etcd»
X3 etcd: del --prefix /s3/clusters/<C>/ + del
   /s3worker/{work,portalloc,rotations}/<C>; journal done
```

Снос кластера уничтожает данные бэкапов его нод — осознанное действие
оператора (панель — confirm); фиксируется в рисках §9.

### C. NodeSupervisor (надзор, тик)

- **Health-проба каждого инстанса** — платформенная health-грань (arch/23
  §5) по `host:port` из portalloc; проба — не доказательство доступности
  записи, только живость контейнера/грани.
- **Снесённый контейнер** (положительное свидетельство docker-инспекции,
  S7 [17-synchronization-principles.md](17-synchronization-principles.md))
  → пересоздание на том же хосте (`placed_on` фиксирован; volume
  сохраняется — данные и targets/cursors репликации не теряются;
  PKI-материал — тот же из etcd), `state=PROVISIONING`; в `RUNNING`
  переводит следующий цикл по health.
- **Недоступная нода** (проба молчит дольше NodeDeadSec) →
  `state=UNREACHABLE` + ретраи тиками; деструктивных действий по факту
  «не отвечает» нет (S7: флап ≠ смерть).
- **Сбор `usage`** — периодический (HealthIntervalSec): занятость диска из
  платформенной админ-грани/метрик → ключ `nodes/<I>/usage` —
  квота-наблюдение `resources.disk` (enforcement-примитива нет, arch/23
  §2) и алерты панели.
- Слепая проба (docker-хост молчит) — никаких действий; одно пересоздание
  за тик.

### D. AddNodeProcess

Заявка панели (`nodes/<I>/state=NOT_INITIALIZED`, имя из тела запроса —
§1.1): подъём нового инстанса (по A: PKI/placement/portalloc/volume/
контейнер/bucket) + RMW `endpoints`. Включение в mesh — доводка G по
`links` (backfill существующих объектов — reconcile-механика ownS3 при
добавленном соседе, owns3/07 §2/§9). Финал — `state=RUNNING`.

### E. RemoveNodeProcess

Маркер панели `nodes/<I>/state=TO_REMOVE`: гварды (не последний инстанс
кластера — 0 инстансов валиден только декларацией при создании, вывод
последнего запрещён — путь к кластеру без инстансов это снос кластера
целиком; при мёртвых остальных — нет живой RUNNING-ноды кроме выводимой —
отказ фазы: вывод не рвёт последнюю точку записи пула) → остановка/снос
контейнера и volume → чистка ключей инстанса (`nodes/<I>/…` включая
`pki`), записи portalloc и имени из `denylist` кластера → RMW
`endpoints`. Связи выводимого инстанса сносятся доводкой G
(target-записи у соседей; имя исчезло из состава — записи «лишние»).
Отказ данных осознан: volume инстанса сносится (копия — на остальных
инстансах mesh при настроенных связях; без связей объекты выводимого
теряются — R1 §9); фиксируется в рисках §9.

### G. MeshGovernance (доводка состава, тик Active-ветки; она же — фаза P4 provisioning)

Приводит фактический mesh инстансов к декларациям `links` + `denylist`
(arch/23 §2). Источник истины — etcd; фактическое состояние — админ-грань
инстансов `GET /admin/replication/status` (состав targets). Шаги за один
тик (идемпотентны, ретраи тиками; один инстанс/пара за тик — структурные
операции не пачкой):

1. **Deny-доведение**: для каждого RUNNING-инстанса, КРОМЕ DenyList-имён
   (отозванному собственный список отзывов не нужен — изоляцию обеспечивают
   записи у его соседей и запрет будущих join его сертификата), — полная
   перезапись `PUT /replication/denylist` (owns3/07 §2/§6) телом
   `{entries:[{name, publicKeyPem}]}` из `denylist` кластера (ключи — из
   `nodes/<I>/pki`); подпись до-join контура — сертификатом оркестратора
   (`pki/orchestrator`, схема OWNS3-PEER). Инстанс недоступен →
   transient, следующий тик.
2. **Сверка состава**: желаемые target-записи инстанса X = имена соседей
   по `links` минус `denylist` минус несуществующие/не-RUNNING имена
   (связь с ещё PROVISIONING-инстансом доводится после его RUNNING).
   - недостающая связь {a,b} → `POST /admin/replication/join`
     `{"endpoint": <b>}` на a (root-креды профиля; join двусторонний —
     обе стороны получают записи, owns3/07 §2; курсор нового соседа −1 +
     reconcile-забор — механика ownS3). 409 (имя с другим ключом) →
     DELETE target-записи конфликтующей стороны, ретрай следующим тиком;
     403/502 — transient, ретрай.
   - лишняя запись (имя не в желаемых: убрана из `links`, deny, инстанс
     выведен) → `DELETE /admin/replication/targets/{name}` на каждой
     стороне пары (курсор сохраняется — возврат соседа продолжится с
     места, owns3/07 §2).
3. **Изоляция deny-инстанса**: связи с ним не джойнятся, существующие
   записи его имени у соседей и его собственные записи сносятся (п.2 —
   имя не в желаемых).

Живость сторон перед join — по `state` (RUNNING) и health-факту надзора;
join к недоступному — transient-ретрай. Advertised-endpoint для join — из
portalloc (`AdvertisedHost` §2). Пределы: full-mesh не инвариант —
доводка строит ровно заданный `links` (частичный/незеркальный состав
нормален; зеркальность пары обеспечивает join-механика ownS3).
Диагностика — journal-фазы (`op=mesh-governance`) и метрики §7.

### F. SyncProbeProcess (реализация — t33)

Периодический (гейт `SyncIntervalSec` по `checked_unix`, §8) и
по-командный (заявка `/s3worker/probes/<C>` из `POST …/sync-probe` §1.1 —
не эксклюзивная ветка: исполняется ближайшим тиком любого держателя
клэйма, заявка сносится по исполнении) зонд; все операции — под клэймом
`<C>`, journal (`op=sync-probe`), идемпотентен. Две независимые половины:

- **connectivity** — ping-матрица настроенных линков: `POST
  /admin/replication/probe` каждой RUNNING-ноды (owns3/07 §2/§6 — состав
  её targets и живость каждого линка параллельным ping исходящим
  репликационным транспортом); только настроенные линки, полнота меша и
  зеркальность состава не контролируются (зона G); ping-ок = «канал жив»,
  отдельно от доставки и сверки данных; PUT-пробы не используются (путь
  записи ownS3 без синхронных взаимодействий с соседями — owns3/07 §1).
- **sync** — сверка содержимого bucket между нодами по настроенным парам
  (объединение targets, как у connectivity): постраничная сверка
  sync-report (порядко-независимая свёртка, owns3/07 §10) → при
  расхождении страницы — количественный diff листингов S3-грани
  (`missing`/`extra`/`lag_bytes`, элемент сверки — `(key, modTime)`);
  лаг доставки — статус приёмника (fetch-очередь, `lag_objects`).

Форматы и state-агрегаты — arch/23 §3. Источник истины о синхронности —
собственный зонд: штатные метрики репликации лаг данных не отражают (факт
7 `../OwnS3/docs/s3-replication.md`); S3-операции зонда — per-попыточный таймаут +
пульс (§6).

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть?
  health отвечает? bucket существует? правило mesh стоит? `state` уже
  снят?); отказ между фазами — ретрай тиком по journal-фазе.
- **Takeover** ≤ TTL 15 с: клэймы/journal/states/portalloc — в etcd;
  смерть инстанса гасит lease — другой инстанс продолжает с записанной
  фазы; двойной контроллер невозможен (операции — только под живым
  клэймом).
- **Отказ docker-хоста**: ноды на нём `UNREACHABLE`, размещение не
  меняется (`placed_on` фиксирован — перепривязка на месте не допускается,
  arch/23 §2), новые ноды — только на резолвируемые хосты.
- **Что теряется при отключении машины X**: volume инстанса — локальное
  хранение без гарантий durability, доступность её доли теряется до
  возврата машины; переживание отказа — с точностью до лага доставки
  (RPO = лаг репликации, R2 `../OwnS3/docs/owns3-replication-requirements.md`)
  ТОЛЬКО для связей, настроенных в `links` (частичный граф — инстанс без
  входящих связей теряет долю безвозвратно, R1 §9);
  восстановление полноты — backfill из живых инстансов (R4 там же,
  reconcile-механика ownS3); гарантии строятся от реплицированных
  источников (etcd + mesh), не от локальных volume.
- **Отказ etcd**: контрол-плейн заморожен; живые ноды S3 от него не
  зависят (клиенты пишут по последним `endpoints`).
- **Watchdog зависших циклов** — общий каркас `LoopWatchdog` (Shared.Core,
  [14-pgworker.md](14-pgworker.md) §6): инварианты живости/поллинга
  обязательны и для циклов S3Worker; «пульс конечного внешнего вызова»
  (S3-операции зонда — per-попыточный таймаут + пульс) — тот же канон.

## 7. Наблюдаемость

`/healthz` — честный health по канону воркеров: `etcd-reachable`,
`docker-hosts` (per-host ping), `loops-alive`, `claims`. Prometheus-
метрики — единый каркас [18-metrics.md](18-metrics.md) (`/metrics` на
порту API): циклы/клэймы/фазы процессов/операции воркера
(`s3worker_*` в Meter-именах). Телеметрия самих нод — метрики ownS3
(owns3/05 §5); scrape-механика — вне канона. Диагностика фаз —
`/s3worker/work/<C>` и `nodes/<I>/state`; операции доводки G — счётчики
`mesh_*` (join/удаления/deny-перезаписи: accepted/conflict/cleaned);
операции зонда F — счётчики `sync_probe_*` (прогоны по исходам, линки
ok/offline, пары synced/lagging/diverged).

## 8. Конфигурация (appsettings + env)

```
S3Worker:Etcd { Endpoints[] }                      # единый etcd-контур (arch/17 S1)
S3Worker:Bucket                                    # имя per-install bucket (arch/19 §5;
                                                   #   создаётся оркестратором на каждой ноде)
S3Worker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                  PortRange{From,To}, Images{OwnS3="owns3:dev"},
                  Tls, Ssh, ContourNetwork, AdvertisedHost }
S3Worker:Loops { ScanIntervalSec=5, KeepaliveSec=5, Watchdog{...} }
S3Worker:Thresholds { NodeBootSec=100, NodeDeadSec=90, HealthIntervalSec=15,
                      SyncIntervalSec=60 }   # SyncIntervalSec — период зонда F (§5 F)
S3Worker:Parallelism { MaxClusters=4 }
S3Worker:Api { AdvertiseUrl, EnableSeedEndpoint=false, Tls{...} }   # mTLS-грань, §1.1
# env-секреты S3W_*: S3W_API_TLS_{CERT,KEY,CLIENT_CA}, S3W_DOCKER_TLS_{CA,CERT,KEY},
#   S3W_DOCKER_SSH_KEY[_PATH], S3W_NODE_SECRETS__<PROFILE>__* (§4)
```

Fail-fast валидации старта — по образцу pg (неполные секретные пары,
отрицательные пороги); `EnableSeedEndpoint` — при необходимости
стендового сида, default `false` (паттерн
[14-pgworker.md](14-pgworker.md) §1.1.1).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Инстанс без входящих связей в `links` (частичный граф) или единственный инстанс: гибель его диска — потеря доли данных без реплики | рекомендация N≥2/3 и full-связность гео-групп (операционная, arch/23 §2); панель — индикация состава связей; риск осознан декларативностью, не запретом |
| R2 | Снос кластера/вывод инстанса уничтожает данные бэкапов его инстансов | осознанное действие оператора: панель — confirm (§5 B/E); вывод последнего инстанса запрещён гвардой E (кластер без инстансов — только созданием); риск-строка канона |
| R3 | Собственный зонд — единственный честный источник синхронности | зонд — процесс F (t33), форматы arch/23 §3; метрики доставки ownS3 (R2 `../OwnS3/docs/owns3-replication-requirements.md`) — наблюдаемость механики, не источник истины о данных |
| R4 | Отсутствие per-cluster сети — межнодовые линки зависят от advertised-адресов и открытых межхостовых портов | ограничение фиксируется явно (§2); раснос по площадкам — явные селекторы оператора |
| R5 | Смена per-install advertised-адресов (AdvertisedHost) — target-записи соседей держат прежние endpoint'ы | join идемпотентно обновляет endpoint (owns3/07 §2); лечение — переприсвоение пары в `links` (снос + повторный join) или пересоздание связей; плановая смена адресов — вне штатных операций |
| R6 | Ротация ключей репликации/CA не автоматизирована (сертификаты бессрочные) | отзыв компрометации — deny-list (§5 G, мгновенный); плановая ротация — roadmap (owns3/07 §12); enterprise-CRL — вне |

## 10. Дальше

- Контракт домена — [23-s3-clusters.md](23-s3-clusters.md) (ключи, статусы,
  дискавери).
- Потребитель пула — [19-backups.md](19-backups.md) (bucket per-install,
  креды).
- Образцы воркеров — [14-pgworker.md](14-pgworker.md) (паттерны API/
  placement/watchdog), [16-kafkaworker.md](16-kafkaworker.md),
  [21-valkeyworker.md](21-valkeyworker.md).
- Принципы синхронизации —
  [17-synchronization-principles.md](17-synchronization-principles.md).
- Отложенные задачи — [roadmap/s3.md](roadmap/s3.md).
- Требования mesh-ownS3 —
  `../OwnS3/docs/owns3-replication-requirements.md`; изыскания платформ
  репликации (MinIO/Garage) — справочник фактов `../OwnS3/docs/s3-replication.md`.

---

→ Указатель — [README.md](README.md). Контракт домена —
[23-s3-clusters.md](23-s3-clusters.md).
