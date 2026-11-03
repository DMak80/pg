# ValkeyWorker: внешний HTTP API

Уровень 3. Функция «внешний интерфейс»: приём мутаций valkey-домена от панели
и стендового сида, управление инстансом, дискавери и аутентификация грани.
Контракт мутаций — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
(глава Valkey, §11.2); ключи etcd — [valkey-clusters/02-keys.md](../valkey-clusters/02-keys.md).

## 1. Приём мутаций

Та же HTTP-грань, что `/healthz` (порт `:8080`). Префикс `/api` — приёмник
ВСЕХ мутаций valkey-домена (панель в etcd домена не пишет ничего). Мутации
покрывают: создание/удаление кластера
([A](processes/a-provisioning.md)/[B](processes/b-deprovisioning.md)),
конфиг-мутации ([D](processes/d-config-converger.md)), ресурсы ноды
([C](processes/c-node-supervisor.md)), ротации кредов и CA
([E](processes/e-password-rotator.md)/[K](processes/k-ca-rotator.md)).

Плюс стендовый сид — `POST /api/seed/demo` (демо-кластер; флаг
`ValkeyWorker:Api:EnableSeedEndpoint`, default `false`; идемпотентен;
паттерн [kafkaworker/README.md](../kafkaworker/README.md)).

Плюс (adminpanel/02 §11.2-7/8) DELETE-отмена живых заявок ротаций:

- `DELETE /api/valkey/clusters/{c}/password/rotate` — ключ
  `/valkeyworker/rotations/<C>`, один на обе роли app|admin; гвард «не
  начата» по `/valkeyworker/work/<C>`: `op=rotate` в переходных фазах
  E1/E2 ([E](processes/e-password-rotator.md)) → 409;
- `DELETE /api/valkey/clusters/{c}/ca/rotate` — ключ
  `/valkeyworker/ca_rotations/<C>`; `op=rotate-ca` в P/D/R/C
  ([K](processes/k-ca-rotator.md)) → 409.

Отмена — del ключа воркером (txn compare по значению; чужая/снятая заявка
не трогается); заявки нет → 404 (порт
[adminpanel/02](../adminpanel/02-etcd-contract.md) §9.7.5). Коды: 204 / 404 /
409 / 503.

Плюс read-only `GET /api/config` ([adminpanel/02](../adminpanel/02-etcd-contract.md)
§2.3.5): эффективный конфиг установки — плоский JSON групп
`thresholds`/`loops`/`parallelism` (фактические поля POCO Options), пометка
`source:"env"` — применяется рестартом воркера; БЕЗ секретов/TLS/endpoints;
etcd не читается. Коды: 200.

## 2. Аутентификация и TLS

- HTTPS-грань `:8080`; канон воркеров — клиентский сертификат (mTLS)
  обязателен только на `/api`: клиенты (панель, стендовый сид)
  аутентифицируются сертификатом per-install API-CA, запрос `/api` без
  валидного серта — 401.
- `/healthz` и `/metrics` — открытая грань доверенной docker-сети И
  хост-публикаций портов API docker-хоста (`localhost:{PORT}`; публикации
  нужны панели — AdvertiseUrl `host.docker.internal` — и хостовым
  потребителям: чеки, скрейп), без клиентского серта (паттерн Patroni REST
  `:8008` — [13-network-security.md](../13-network-security.md) §4; скрейп
  Prometheus — `https` + `ca_file`, [18-metrics.md](../18-metrics.md) §5.2).
- Граница доверия грани — периметр docker-хоста, а не только docker-сеть;
  опциональное сужение (bind `127.0.0.1` отдельным env-флагом; ломает
  `host.docker.internal`-потребителей) — роадмап, каноном не предписывается.

## 3. Серверный серт и дискавери

- **Серверный серт API** — ключ `/workers/api_tls/valkeyworker` (JSON
  `{"cert_pem","key_pem","updated_unix","updated_by"}`; пишет ТОЛЬКО панель —
  канон ключа и протокол —
  [adminpanel/02](../adminpanel/02-etcd-contract.md) §9.9): воркер читает
  при старте и поднимает грань на этом серте; env-секреты
  `VWK_API_TLS_{CERT,KEY,CLIENT_CA}` — бутстрап-фоллбек. Применение нового
  серта — перезапуском инстанса (docker-политика `restart: unless-stopped`).
- **Дискавери API** — ключ `/valkeyworker/api/<id>`
  ([valkey-clusters/03-coordination.md](../valkey-clusters/03-coordination.md)): ставит сам инстанс
  при старте; URL — из `ValkeyWorker:Api:AdvertiseUrl` (достижим панелью);
  `cert_thumbprint` — SHA-256 серта, фактически применённого на грани.
  Панель зовёт любой живой ключ (failover на следующий; все умерли — 503 +
  critical-алерт `worker-api-unreachable`).

## 4. Потребители

- **AdminPanel** — единственный штатный клиент мутаций; читает
  дискавери-ключ, `/healthz`, `/metrics`.
- **Стендовый сид** — `POST /api/seed/demo` (флаг включения).
- **Приложения** в API не ходят: читают только дискавери-ключи etcd
  (`endpoints`, `ca_pem`, `app_user`/`app_password`,
  [valkey-clusters/04-client-discovery.md](../valkey-clusters/04-client-discovery.md) §1).
