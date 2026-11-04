# KafkaWorker: внешний HTTP API

Уровень 3. Функция «внешний интерфейс»: приём мутаций kafka-домена от панели
и стендового сида, управление инстансом, дискавери и аутентификация грани.
Контракт мутаций — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
(глава Kafka, §10.2); ключи etcd — [kafka-clusters/README.md](../kafka-clusters/README.md).
Какие процессы запускает мутация — [02](02-cluster-lifecycle.md),
[03](03-broker-scaling.md), [04](04-security.md), [05](05-topics.md),
[06](06-rebalancing.md).

## 1. Приём мутаций

Та же HTTP-грань, что `/healthz` (порт `:8080`, TLS — §2). Префикс `/api` —
приёмник ВСЕХ мутаций kafka-домена (панель в etcd не пишет ничего): 15
мутаций контракта [adminpanel/02](../adminpanel/02-etcd-contract.md) §10.2 —
сигнатуры/валидации/протоколы записи 1:1 (меняется исполнитель: была панель,
стал воркер; guard'ы читают etcd напрямую, без панельного снапшота).
Мутации покрывают: создание/удаление кластера
([A](processes/a-provisioning.md)/[B](processes/b-deprovisioning.md)),
добавление/снятие брокера
([F](processes/f-add-broker.md)/[G](processes/g-remove-broker.md)),
ресурсы брокера ([J](processes/j-node-regenerator.md)), ротации паролей и CA
([H](processes/h-password-rotator.md)/[K](processes/k-ca-rotator.md)),
ребалансировку ([I](processes/i-partition-reassigner.md)), топики
([D](processes/d-topic-sync.md)).

Плюс управление инстансом — `POST /api/restart` (graceful self-stop, §3;
применяет серверный серт из `/workers/api_tls/kafkaworker` при следующем
старте) и стендовый сид — `POST /api/seed/demo` (2 кластера
`events`/`pending`, топики-архетипы, заявка ротации, ребалансировка,
drain-прогресс; набор — 1:1 сид-фикстуры интеграционных тестов; флаг
`KafkaWorker:Api:EnableSeedEndpoint`, default `false`; идемпотентен: живой
`/kafka/clusters/events/config` → 200 `{"seeded":false}`).

Плюс (adminpanel/02 §10.2-18/19/20): DELETE-отмена живых заявок ротаций —
`DELETE /api/kafka/clusters/{c}/app-password/rotate`
(ключ `/kafkaworker/rotations/<C>`),
`DELETE /api/kafka/clusters/{c}/admin-password/rotate`
(`/kafkaworker/admin_rotations/<C>`),
`DELETE /api/kafka/clusters/{c}/ca/rotate`
(`/kafkaworker/ca_rotations/<C>`): del ключа заявки воркером (txn compare по
значению — гонка с клэймом ловится отказом txn), гвард «ротация не начата» по
work-журналу `/kafkaworker/work/<C>` (`op=rotate` в переходных фазах A/B/C —
[H](processes/h-password-rotator.md), `op=rotate-ca` в P/D/R/C —
[K](processes/k-ca-rotator.md): 409 «rotation is already executing»;
терминальные/waiting-фазы и отсутствие журнала — отмена разрешена), заявки
нет → 404 (идемпотентность повтора отсутствует осознанно — порт
[adminpanel/02](../adminpanel/02-etcd-contract.md) §9.7.5). Коды: 204 / 404 /
409 / 503.

Плюс read-only `GET /api/config` ([adminpanel/02](../adminpanel/02-etcd-contract.md)
§2.3.5): эффективный конфиг установки — плоский JSON групп `thresholds`
(BrokerBoot/NodeDead/ReassignSubmit/ReassignRetrySubmit/RotationTicketTimeout),
`loops` (Scan/Keepalive/TopicSync/Reassign/ReassignBatch/Snapshot/ErrorDelay),
`parallelism` (MaxClusters), пометка `source:"env"` — применяется рестартом
воркера; БЕЗ секретов/TLS/endpoints; etcd не читается. Коды: 200.

## 2. Аутентификация и TLS

**Аутентификация — mTLS на `/api`**: грань — один HTTPS-порт `:8080`
(серверный серт — §3); клиентский сертификат на TLS-хендшейке опционален.
Клиентский сертификат (mTLS) ОБЯЗАТЕЛЕН только для префикса `/api` — клиенты
(панель, стендовый сид) аутентифицируются клиентским сертификатом,
подписанным per-install API-CA; запрос `/api` без валидного клиентского серта
— 401. `/healthz` и `/metrics` — открытая грань доверенной docker-сети И
хост-публикаций портов API docker-хоста (`localhost:{PORT}`; публикации нужны
панели — AdvertiseUrl `host.docker.internal` — и хостовым потребителям: чеки,
скрейп), без клиентского серта (паттерн Patroni REST `:8008` —
[pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4); скрейп Prometheus —
`https` + `ca_file` ([metrics/README.md](../metrics/05-storage.md) §2). Граница доверия
грани — периметр docker-хоста, а не только docker-сеть; опциональное сужение
(bind `127.0.0.1` отдельным env-флагом; ломает `host.docker.internal`-
потребителей) — роадмап, каноном не предписывается. `X-Api-Key`/`KFW_API_KEY`
удалён. Отключение TLS (`KafkaWorker:Api:Tls:AllowInsecureHttp`, default
false) — только для in-memory WAF-тестов; в deploy/стенде грань всегда HTTPS.

## 3. Серверный серт и перезапуск

**Серверный серт API — источник etcd (перезапуск применяет)**: воркер при
СТАРТЕ читает ключ `/workers/api_tls/kafkaworker` (JSON
`{"cert_pem","key_pem","updated_unix","updated_by"}`; пишет ТОЛЬКО панель —
канон ключа и протокол — [adminpanel/02](../adminpanel/02-etcd-contract.md)
§9.9) и поднимает грань на этом серте; env-секреты `KFW_API_TLS_{CERT,KEY}`
(PEM/`…_PATH` из volume) — бутстрап-фоллбек при отсутствии ключа (приоритет:
etcd > env; источник — в логе старта). `KFW_API_TLS_CLIENT_CA` остаётся только
env. Применение нового серта — перезапуском: `POST /api/restart` — 202
Accepted, затем graceful stop хоста (`IHostApplicationLifetime.StopApplication`
с короткой задержкой на доставку ответа); контейнер перезапускается
docker-политикой (`restart: unless-stopped`). Без docker-restart-политики
процесс останется остановленным — панель увидит `worker-api-unreachable`.
Серт — материал входящей грани: исходящие коммуникации воркера (docker Engine
API, kafka-брокеры SASL_SSL per-cluster CA, etcd) его не используют.

## 4. Дискавери API

Ключ `/kafkaworker/api/<instanceId>` (lease TTL 15 с, паттерн
`instances/<id>`; [kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md)) — value
`{"url":"https://<host>:<port>","instance":"<id>","since_unix":…,
"cert_thumbprint"?:"<sha256-hex>"}`. Воркер ставит ключ сам при старте; URL —
из `KafkaWorker:Api:AdvertiseUrl` (достижим панелью: compose-сеть стенда
`https://kafkaworker:8080` или `host.docker.internal:<порт>`; в deploy —
`https://host.docker.internal:8081`). `cert_thumbprint` — SHA-256 серта,
фактически применённого на грани (из etcd-ключа
`/workers/api_tls/kafkaworker` или env-фоллбека) — панель сверяет с целевым
(applied / pending restart); поле опционально (старые инстансы не пишут —
«неизвестно»). Панель кеширует живые ключи в kafka-снапшоте и зовёт любой
живой (failover на следующий; все умерли — 503 + critical-алерт
`worker-api-unreachable`).

## 5. Потребители

- **AdminPanel** — единственный штатный клиент мутаций; читает дискавери-ключ,
  `/healthz`, `/metrics`.
- **Стендовый сид** — `POST /api/seed/demo` (флаг включения).
- **Приложения** в API не ходят: читают только дискавери-ключи etcd
  ([kafka-clusters/05-client-discovery.md](../kafka-clusters/05-client-discovery.md)): `endpoints`, `app_user`/`app_password`,
  реестр `topics/`. Напрямую в docker/Kafka не ходят.
