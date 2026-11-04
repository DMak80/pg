# S3Worker: внешний HTTP API

Уровень 3. Функция «внешний интерфейс»: приём мутаций s3-домена от панели и
стендового сида, аутентификация грани, серверный серт и дискавери.
Контракт мутаций — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
§12; ключи etcd — [s3-clusters/02-keys.md](../s3-clusters/02-keys.md). Какие процессы запускает
мутация — [02](02-cluster-lifecycle.md), [03](03-node-scaling.md),
[04](04-security.md), [05](05-mesh-governance.md), [06](06-sync-probes.md).

## 1. Приём мутаций

Та же HTTP-грань, что `/healthz` и `/metrics` (TLS — §2). Префикс `/api` —
приёмник ВСЕХ мутаций S3-домена (панель в etcd домена не пишет ничего):
сигнатуры, валидации и протоколы записи — канон поведения исполнителя
([adminpanel/02](../adminpanel/02-etcd-contract.md) §9.2/§9.4/§9.5/§9.6);
guard'ы читают etcd напрямую, без панельного снапшота.

| Метод+путь | Назначение | Коды |
|---|---|---|
| `POST /api/s3/clusters` | создание кластера: клэйм-txn `version(config)==0` + пакет PUT (config c `nodes=len(instances)` + instances × {placement, resources, state=NOT_INITIALIZED}; 0 инстансов — только config); запись — ОДНА атомарная txn (сбой midway частичного состояния не оставляет, компенсация не требуется) | 201 / 400 / 409 / 422 / 503 |
| `DELETE /api/s3/clusters/{c}` | перевод `config.state=TO_REMOVE` (RMW; already → 204 без записи) | 202 / 204 / 404 / 503 |
| `POST /api/s3/clusters/{c}/nodes` | добавление инстанса с заданным именем (тело `{name, placement, resources}`): клэйм-txn `version(nodes/<I>/state)==0` + put `placement`/`resources`/`state=NOT_INITIALIZED` | 201 / 400 / 404 / 409 / 503 |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | маркер демонтажа инстанса `nodes/<n>/state=TO_REMOVE` (one-way; гварды: не последний инстанс кластера — 0 инстансов валиден только декларацией при создании, вывод последнего → 409; путь к кластеру без инстансов — снос кластера целиком) | 202 / 204 / 404 / 409 / 503 |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | мутация ресурсов инстанса (cpu/mem — rolling-пересоздание надзором, disk — декларация/наблюдение); образец — kafka-мутация «Изменение ресурсов брокера» ([adminpanel/02](../adminpanel/02-etcd-contract.md) §10.2) | 200 / 400 / 404 / 503 |
| `PUT /api/s3/clusters/{c}/links` | замена графа репликации целиком (тело `{links:[{a,b}…]}`; валидация — §3); применяет доводка G ([05](05-mesh-governance.md)) | 202 / 400 / 404 / 503 |
| `POST /api/s3/clusters/{c}/nodes/{n}/deny` | отзыв ключа инстанса: имя в `denylist` кластера (доводка G: `PUT /replication/denylist` всем инстансам, связи с ним сносятся) | 202 / 204 / 404 / 409 / 503 |
| `POST /api/s3/clusters/{c}/nodes/{n}/allow` | снятие отзыва (имя из `denylist`) | 202 / 204 / 404 / 409 / 503 |
| `POST /api/s3/clusters/{c}/rotations` | заявка ротации клиентских кредов кластера (`/s3worker/rotations/<C>`, клэйм-txn §9.8-паттерн); исполнение — [H](processes/h-credentials-rotator.md) | 201 / 200 / 404 / 503 |
| `POST /api/s3/clusters/{c}/sync-probe` | внеплановый зонд синхронности/связности (заявка `/s3worker/probes/<C>` put-if-absent, already → 202 идемпотентно; исполняет зонд F, результат — [s3-clusters/03-observation-statuses.md](../s3-clusters/03-observation-statuses.md)) | 202 / 404 / 409 / 503 |
| `GET /api/config` | эффективный конфиг установки (read-only; [adminpanel/02](../adminpanel/02-etcd-contract.md) §2.3.5): плоский JSON групп `thresholds`/`loops`/`parallelism` (фактические поля POCO Options), пометка `source:"env"` — применяется рестартом воркера; БЕЗ секретов/TLS/endpoints; etcd не читается | 200 |
| `POST /api/seed/demo` | стендовый сид демо-кластера (флаг `S3Worker:Api:EnableSeedEndpoint`, default `false`; идемпотентен; флаг off → 404) | 201 / 200 / 404 |

## 2. Аутентификация и TLS

**Аутентификация — mTLS на `/api`**: грань — один HTTPS-порт `:8080`;
клиентский сертификат на TLS-хендшейке опционален, но для префикса `/api`
обязателен — клиенты (панель, стендовый сид) аутентифицируются клиентским
сертификатом, подписанным единой per-install API-CA (единая пакета воркеров
установки, отдельные клиентские серты панели); запрос `/api` без валидного
серта — 401.

`/healthz` и `/metrics` — открытая грань доверенной docker-сети И
хост-публикаций портов API docker-хоста (`localhost:{PORT}`; публикации нужны
панели — AdvertiseUrl `host.docker.internal` — и хостовым потребителям: чеки,
скрейп), без клиентского серта (паттерн Patroni REST `:8008` —
[pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4); скрейп Prometheus —
`https` + `ca_file` ([metrics/README.md](../metrics/05-storage.md) §2). Граница доверия
грани — периметр docker-хоста, а не только docker-сеть; опциональное сужение
(bind `127.0.0.1` отдельным env-флагом; ломает `host.docker.internal`-
потребителей) — роадмап, каноном не предписывается.

## 3. Валидация

Сервер — источник истины:

- `name` кластера — паттерн [s3-clusters/02-keys.md](../s3-clusters/02-keys.md); `platform` —
  единственное значение `owns3`; инстансы — 0..9 (рекомендация 2–3 —
  гео-минимум и бюджет, [s3-clusters/02-keys.md](../s3-clusters/02-keys.md)); имя инстанса —
  паттерн кластера `^[a-z][a-z0-9_]{0,62}$`, НЕ зарезервировано
  (`orchestrator` — PKI оркестратора, [s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md)),
  уникально в кластере (409 при коллизии);
- `links` — имена существуют в кластере, `a≠b`, пары уникальны (400);
  `deny/allow` — имя существует (404);
- `placement` — обязательный валидный селектор для каждого инстанса (400);
  `resources` — cpu 0.01..64, mem/disk 1..65536 GiB (канон форматов —
  [adminpanel/02](../adminpanel/02-etcd-contract.md) §9.3);
- `etcdBackup` — опциональный bool (default false); для etcd-backup кластера
  гварды: единственность признака в `/s3/clusters/` (чтение существующих
  config'ов перед txn) — 409; полнота спец-ключа env
  `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}` (обе переменные непустые;
  отсутствие пары = неполнота) — 422.

## 4. Серверный серт и дискавери

**Серверный серт API** — ключ `/workers/api_tls/s3worker` (канон ключа и
протокол — [adminpanel/02](../adminpanel/02-etcd-contract.md) §9.9; пишет
ТОЛЬКО панель): воркер читает при старте и поднимает грань на этом серте;
env-секреты `S3W_API_TLS_{CERT,KEY,CLIENT_CA}` — бутстрап-фоллбек при
отсутствии ключа. Серт — материал входящей грани: исходящие коммуникации
воркера (docker Engine API, админ-грань инстансов, etcd) его не используют.

**Дискавери API** — ключ `/s3worker/api/<id>` ([s3-clusters/08-coordination.md](../s3-clusters/08-coordination.md)):
ставит сам инстанс при старте; URL — из `S3Worker:Api:AdvertiseUrl`
(достижим панелью); `cert_thumbprint` — SHA-256 серта, фактически
применённого на грани. Панель зовёт любой живой ключ (failover на следующий;
все умерли — 503 + critical-алерт `worker-api-unreachable`).

## 5. Потребители

- **AdminPanel** — единственный штатный клиент мутаций; читает дискавери-ключ,
  `/healthz`, `/metrics` и проекцию `/s3/` ([s3-clusters/09-panel.md](../s3-clusters/09-panel.md)).
- **Стендовый сид** — `POST /api/seed/demo` (флаг включения).
- **Приложения** в API не ходят: клиенты пула (PgWorker) читают etcd
  ([s3-clusters/07-client-pool.md](../s3-clusters/07-client-pool.md)).
