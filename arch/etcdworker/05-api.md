# EtcdWorker: внешний HTTP API

Уровень 3. Функция «внешний интерфейс»: приём декларации состава от панели,
управление инстансом, серверный серт и дискавери. Ключи домена —
[00-etcd-contract.md](00-etcd-contract.md); панельная проекция —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.5,
[adminpanel/03-panels.md](../adminpanel/03-panels.md).

## 1. Грань и аутентификация

Та же mTLS-грань, что `/healthz` и `/metrics` (порт `:8080`, канон
[pgworker/07-api.md](../pgworker/07-api.md) §2): клиентский сертификат на
TLS-хендшейке опционален, но для префикса `/api` обязателен — клиенты
(панель) аутентифицируются клиентским сертом per-install API-CA; запрос
`/api` без валидного серта — 401. `/healthz` и `/metrics` — открытая грань
доверенной docker-сети и хост-публикаций портов API (паттерн Patroni REST,
[pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4; скрейп — `https` +
`ca_file`, [18-metrics.md](../18-metrics.md) §5.2).

## 2. Эндпоинты

| Метод+путь | Назначение | Коды |
|---|---|---|
| `PUT /api/etcd/config` | декларация состава: тело `{"nodes":N}` → put `/etcd/config` (замещение целиком; 0 — managed-узлов нет, только наблюдение). Валидация `N` — целое 0..7; гвард кворума — итоговое число членов (external + N) ≥ 3; понижение N запускает rolling-вывод managed-узлов (по одному за тик, [02-membership.md](02-membership.md) §3) | 200 / 400 / 409 / 503 |
| `POST /api/restart` | graceful self-stop инстанса (паттерн [pgworker/07-api.md](../pgworker/07-api.md) §2): 202, затем остановка хоста; контейнер поднимает docker-политика, lease гаснут ≤ 15 с, takeover вторым инстансом | 202 |
| `GET /api/config` | эффективный конфиг установки (read-only, mTLS-зона `/api`): тело-объект `source:"env"` + группы camelCase — `thresholds{nodeBootSec,nodeDeadSec,healthIntervalSec}`, `loops{scanIntervalSec,keepaliveSec,watchdog{...}}`, `snapshots{intervalMin,minIntervalSec,maintenanceIntervalMin,retentionFiles,retentionObjects,retryIntervalSec,timeoutSec}`, `docker{mode,network,advertisedHost,portRange{from,to},images{etcd}}`. Наружу НЕ выходят: etcd-endpoints, `Api`/TLS, `Docker.Tls`/`Ssh`, `Snapshots.Dir`. Применяется рестартом воркера; etcd не читается | 200 |

Протокол записи декларации: RMW по `mod_revision` (ключа нет — txn
`version==0`), `created_unix` исходной декларации сохраняется; проигранная
гонка — 503, клиент повторяет ([00-etcd-contract.md](00-etcd-contract.md) §2).

## 3. Серверный серт и дискавери

**Серверный серт API** — ключ `/workers/api_tls/etcdworker` (канон ключа и
протокол — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
§9.9; пишет только панель): воркер читает при старте и поднимает грань на
этом серте; env-секреты `ETCDW_API_TLS_{CERT,KEY,CLIENT_CA}` — бутстрап-фолбэк
при отсутствии ключа. Серт — материал входящей грани: исходящие коммуникации
воркера (etcd, docker, S3) его не используют.

**Дискавери API** — lease-ключ `/etcdworker/api/<id>` (TTL 15 с;
[00-etcd-contract.md](00-etcd-contract.md) §1): воркер ставит его сам при
старте; URL — из `EtcdWorker:Api:AdvertiseUrl` (достижим панелью, `https`).
Панель зовёт любой живой ключ (failover на следующий; все умерли — 503 +
critical-алерт `worker-api-unreachable`).

## 4. Потребители

- **AdminPanel** — единственный штатный клиент мутаций (декларация состава,
  restart) и читатель `GET /api/config`; наблюдает `/healthz`, `/metrics` и
  проекцию домена.
- **Приложения** в API не ходят: потребители контура подключаются к etcd по
  env endpoints поставки (их адресация — вне домена воркера).
