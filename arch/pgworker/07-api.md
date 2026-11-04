# PgWorker: внешний HTTP API

Уровень 3. Функция «внешний интерфейс»: приём мутаций pg-домена от панели
и стендового сида, управление инстансом, дискавери и аутентификация грани.
Контракт мутаций — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
(глава PgWorker, §9); ключи etcd — [00-etcd-contract.md](00-etcd-contract.md).
Какие процессы запускает мутация — [02](02-cluster-lifecycle.md),
[03](03-shard-scaling.md), [04](04-security.md), [05](05-bucket-evacuation.md),
[06](06-bucket-moves.md).

## 1. Приём мутаций

Та же HTTP-грань, что `/healthz` (порт `:8080`, Kestrel воркера; TLS —
§2). Префикс `/api` — приёмник ВСЕХ мутаций декларативного контракта:
панель (и только она, плюс стендовые сиды) не пишет в etcd ничего — она
отправляет команды воркеру, воркер валидирует и записывает ключи в etcd
сам (протоколы записи и форматы значений —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9, без
изменений; меняется исполнитель: была панель напрямую, стал воркер).
Воркер — «хозяин» префиксов `/clusters/`, `/pgworker/` и заявок
`/service/<C>-<X>/request_*`.

Guard'ы и валидации переносятся из панельных команд как есть; источником
данных вместо панельного снапшота — прямые чтения etcd воркером (config,
routing, `/service/<scope>/members`, очереди заявок): это устраняет гонку
«снапшот отстал» — воркер читает авторитетно перед записью.

**Эндпоинты** (сигнатуры/коды — 1:1 UI-контракт панели
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9,
[adminpanel/03-panels.md](../adminpanel/03-panels.md) §1; тело и ответы не
менялись):

| Метод+путь | Назначение | Протокол записи |
|---|---|---|
| `POST /api/clusters` | создание кластера (декларация) | 02 §9.1–§9.3: claim-txn + пакет PUT + компенсация |
| `DELETE /api/clusters/{c}` | перевод в `TO_REMOVE` | 02 §9.4 |
| `POST /api/clusters/{c}/shards` | декларация add-shard | 02 §9.5 |
| `DELETE /api/clusters/{c}/shards/{x}` | маркер демонтажа шарда | 02 §9.6 |
| `POST /api/clusters/{c}/moves` | заявки на переезды бакетов | 02 §9.7.1 |
| `POST /api/clusters/{c}/moves/rollback` | заявки на откат бакетов | 02 §9.7.2 |
| `POST /api/clusters/{c}/moves/finalize` | заявка уборки старого шарда | 02 §9.7.3 |
| `POST /api/clusters/{c}/moves/abort` | заявка отмены переезда | 02 §9.7.4 |
| `DELETE /api/clusters/{c}/moves/{bucket}` | отмена стоящей заявки (del ключа) | 02 §9.7.5 |
| `POST /api/clusters/{c}/secrets/rotate` | заявка ротации per-cluster секретов (app + bucket_admin + mover + `rest_password`) | 02 §9.8 |
| `PUT /api/clusters/{c}/config` | мутация per-cluster опции `synchronous_mode_strict` | 02 §9.10: RMW-txn по `mod_revision`; гварды — кластер Active |
| `POST /api/clusters/{c}/shards/{x}/restore` | заявка восстановления шарда из бэкапа (PITR latest/target_time, source-override; `confirm` = имя шарда; опциональное `source_pool` — имя пула `/s3/clusters/` DR-источника: данные старого кластера лежат в другом пуле; отсутствие — собственный пул кластера `policy.s3`; валидация поля — живое чтение `/s3/clusters/<C>/config`: пул существует и не `TO_REMOVE`, иначе 400 по полю `source_pool`) | пишет статус `/pgworker/backups/<C>/<X>/restore/<id>` сам (клэйм `<C>`; [backups/03-restore.md](../backups/03-restore.md)): гварды — кластер Active, шард заявлен, максимум один активный restore на шард |
| `POST /api/clusters/{c}/backups/policy` | приём per-cluster политики бэкапов (замещение целиком — [backups/05-etcd-contract.md](../backups/05-etcd-contract.md)); тело дополнительно принимает `"drill":{"interval_days":int}` (валидация [0..3650]; поле отсутствует → секция `drill` в записываемую policy не кладётся — кластер живёт на глобальном дефолте; замещение целиком сохраняется); тело дополнительно принимает `"s3":{"pool","write_master","write_replica"}` ([backups/05-etcd-contract.md](../backups/05-etcd-contract.md); замещение целиком — как drill): валидация — 400 по полям: `pool` существует в `/s3/clusters/` (живое чтение) и не `TO_REMOVE`, имена списков — инстансы пула (существует `nodes/<I>/placement`), без дублей внутри списка, списки непустые (пустой список = «не выбрано» — это отсутствие секции целиком), `pool` обязателен при заданных списках; отсутствие секции `s3` — удаление из policy (бэкапы кластера выключаются); тело дополнительно принимает `"garbage_min_age_sec":long` (валидация ≥ 600 — иначе 400 по полю; поле отсутствует → в policy НЕ кладётся — кластер живёт на дефолте конфига супервизора бэкапов 600 с, [backups/05-etcd-contract.md](../backups/05-etcd-contract.md)) | пишет ключ `/pgworker/backups/<C>/policy` сам (клэйм `<C>`; [backups/05-etcd-contract.md](../backups/05-etcd-contract.md)) |
| `GET /api/config` | эффективный конфиг установки (read-only; mTLS-зона `/api` — обычный доступ панели): плоский JSON групп `thresholds` (NodeDead/ShardDead/PatroniBoot/CutoverTimeout/ConnFailBudget/ProvisionRetry*), `loops` (Scan/Keepalive/Snapshot/ErrorDelay), `moves` (Poll/FreezeWait/FreezeLock*/AbortMinAge), `parallelism` (MaxClusters), `backups` (только `enabled`), `snapshots.export` (enabled/intervalMin), `pgtune` (входы профиля DbVersion/DbType/HdType/DbSize/Connections); каждое поле — значение env при старте, пометка группы `source:"env"` — применяется рестартом воркера; БЕЗ секретов/TLS/endpoints (секреты env наружу не выходят никогда); etcd не читается, отдельного DI не требуется | 200 `application/json`; etcd НЕ пишет |
| `POST /api/ha/{scope}/nodes/{node}/recreate` | маркеры `TO_RECREATE`+`recreate=soft\|hard` | как §9.6-подобный маркер (02 §9, 03 §2): guards по `/service/<scope>/members` |
| `POST /api/restart` | graceful self-stop инстанса (перезапуск контейнера — docker-политикой) | etcd НЕ пишет: 202 → `StopApplication` (§2 ниже); применение серверного серта из `/workers/api_tls/pgworker` — при следующем старте |
| `POST /api/seed/demo` | стендовый демо-сид pg-контура | §5 |

## 2. Аутентификация, TLS и перезапуск

**Аутентификация — mTLS на `/api`**: грань — один HTTPS-порт `:8080`
(серверный серт — §3); клиентский сертификат на TLS-хендшейке опционален.
Клиентский сертификат (mTLS) ОБЯЗАТЕЛЕН только для префикса `/api` —
мутации (панель, стендовый сид) аутентифицируются клиентским сертификатом,
подписанным per-install API-CA (единая пакета с KafkaWorker —
[kafkaworker/07-api.md](../kafkaworker/07-api.md) §2: CA `kfw-install-ca`,
отдельные клиентские серты панели `panel.crt` и сида `seed.crt`); запрос
`/api` без валидного клиентского серта — 401. `/healthz` и `/metrics` —
открытая грань доверенной docker-сети И хост-публикаций портов API
docker-хоста (`localhost:{PORT}`; публикации нужны панели — AdvertiseUrl
`host.docker.internal` — и хостовым потребителям: чеки, скрейп), без
клиентского серта (паттерн Patroni REST `:8008` —
[pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4, verify_client нет):
скрейп Prometheus — `https` + `ca_file` без `cert_file`/`key_file`
([metrics/README.md](../metrics/05-storage.md) §2), docker-HEALTHCHECK — без
клиентского серта. Граница доверия грани — периметр docker-хоста, а не
только docker-сеть; опциональное сужение (bind `127.0.0.1` отдельным
env-флагом; ломает `host.docker.internal`-потребителей) — роадмап, каноном
не предписывается. `X-Api-Key`/`PGW_API_KEY` удалён; отдельные креды
панели и сида — разные клиентские серты одной клиентской CA (различимы в
журналах сервера, отзываются независимо). Отключение TLS
(`PgWorker:Api:Tls:AllowInsecureHttp`, default `false`) — только для
in-memory WAF-тестов; в deploy/стенде грань всегда HTTPS.

**Перезапуск воркера** — `POST /api/restart`: 202 Accepted (запрос
принят), затем graceful stop хоста (`IHostApplicationLifetime.StopApplication`
с короткой задержкой на доставку ответа); процесс завершается, контейнер
перезапускается docker-политикой (`restart: unless-stopped`). Рестарт
безопасен для кластеров: клэймы/журнал/заявки — в etcd, lease истекает
≤15 с, операции продолжает этот же или другой инстанс. Без
docker-restart-политики (не deploy-канон) процесс останется остановленным
— панель увидит `worker-api-unreachable`. Заголовок `X-Requested-By`
(оператор панели) попадает в журнал воркера.

## 3. Серверный серт API

**Серверный серт API — источник etcd (перезапуск применяет)**: воркер при
СТАРТЕ читает ключ `/workers/api_tls/pgworker` (JSON
`{"cert_pem","key_pem","updated_unix","updated_by"}`; пишет ТОЛЬКО панель —
канон ключа и протокол —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.9) и
поднимает грань на этом серте; env-секреты `PGW_API_TLS_{CERT,KEY}`
(PEM/`…_PATH` из volume `/tls:ro`) — бутстрап-фоллбек при ОТСУТСТВИИ ключа
(обратная совместимость установок без управляемого серта). Приоритет: ключ
etcd > env (воркер логирует, какой источник применил). CLIENT_CA
(`PGW_API_TLS_CLIENT_CA`) остаётся только env — валидатор клиентов менять
панелью нельзя. Применение нового серта — только перезапуском
(перезагружает панель — [adminpanel/03-panels.md](../adminpanel/03-panels.md)
§1/§3, команда `POST /api/restart`); замена серта БЕЗ перезапуска на живой
грани не происходит (серты живут всё приложение).

## 4. Дискавери API

Ключ `/pgworker/api/<instanceId>` (lease TTL 15 с, паттерн
`instances/<id>`; [00-etcd-contract.md](00-etcd-contract.md) §4) — value
`{"url":"https://<host>:<port>","instance":"<id>","since_unix":…,
"cert_thumbprint"?:"<sha256-hex>"}`. Воркер ставит ключ сам при старте
(keepalive-контур, вместе с `instances/<id>`); URL — из
`PgWorker:Api:AdvertiseUrl` (адрес, ДОСТИЖИМЫЙ клиентами API — прежде всего
панелью; docker-сети стендов — `https://host.docker.internal:<порт>`).
`cert_thumbprint` — SHA-256 серта, ФАКТИЧЕСКИ применённого на грани (серта
из etcd-ключа `/workers/api_tls/pgworker` §3 или env-фоллбека); панель
сверяет с целевым сертом → статус применения (applied / pending restart);
поле опционально (инстансы до введения его не пишут — панель показывает
«неизвестно»). Ключ жив = инстанс жив и URL валиден (lease гасит мёртвые).
Панель читает ключи refresher-тиком, кеширует в снапшоте и при мутации
зовёт любой живой (при ошибке соединения — следующий; все умерли — 503 +
critical-алерт `worker-api-unreachable`,
[adminpanel/03-panels.md](../adminpanel/03-panels.md) §4.1).

## 5. Стендовый сид

`POST /api/seed/demo` — наливка ДЕМО-контроль-плейна pg-контура (кластер
`demo`: config, 2 шарда dsn/replicas/master, routing 16 бакетов, статусы
переездов bucket_3/7/11, heal, заявка move bucket_13,
`/service/demo-s{1,2}/*` (вкл. обязательные заявки `request_{cpu,mem}`
обоих шардов — канон значений панели «2»/«2Gi»,
[01-placement.md](01-placement.md) §5 п.4; без них adopt-репарация на
свежем etcd фейлит каждый тик), `/cluster/nodes/*`; времена — динамические
от now, набор — 1:1 сид-фикстуры интеграционных тестов панели). Пишет
ключи, которые в живой системе пишут ДРУГИЕ субъекты (Patroni, стендовые
эмуляторы): это осознанная стендовая эмуляция, поэтому эндпоинт закрыт
флагом `PgWorker:Api:EnableSeedEndpoint` (default `false`; включается
только в dev-стенде env-ом). Транспорт — общий mTLS-канал API (§2); вызовы
сида аутентифицируются ОТДЕЛЬНЫМ клиентским сертификатом `seed.crt` (не
сертом панели — различимость в журналах/независимый отзыв). Идемпотентен:
существующий `/clusters/demo/config` → 200 `{"seeded":false}` без записи.
Служит «стендом части» для панели без живых PG-нод; полный стенд
(`00-up.sh`) пользуется тем же эндпоинтом.

## 6. Потребители

- **AdminPanel** — единственный штатный клиент мутаций; читает
  дискавери-ключ, `/healthz`, `/metrics`.
- **Стендовый сид** — `POST /api/seed/demo` (флаг включения).
- **Приложения** в API не ходят: читают только дискавери-ключи etcd
  (`dsn`, `app_user`/`app_password`, `app_params`) —
  [pg-clusters/11-buckets.md](../pg-clusters/11-buckets.md) §3.
