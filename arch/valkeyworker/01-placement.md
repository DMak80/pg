# ValkeyWorker: модель размещения и нода

Уровень 3. Как устроена нода Valkey-кластера: контейнер, аргументы, TLS,
сеть, порты, лимиты и адресация. Процессы, использующие модель, —
[жизненный цикл](02-cluster-lifecycle.md), [миграция TLS](processes/t-tls-migrator.md),
[ротация CA](processes/k-ca-rotator.md); PKI и ACL — [безопасность](04-security.md).

## 1. Контейнер и аргументы

- **Образ** — официальный `valkey/valkey:<пин версии>` (настройка
  `Images:Node`), полностью конфигурируется аргументами командной строки.
  Кастомный образ НЕ собирается. Зеркалирование в локальный registry
  `192.168.0.1:5000` (строка в `images.txt` + `mirror-image.sh`,
  мульти-арх) — забота оператора (ранбук `../operations/02-registry-images.md`).
- **Нода кластера = один контейнер** `vwk-<C>-node<k>` (имя
  детерминировано; при `nodes=1` — `vwk-<C>-node1`), restart-политика
  `unless-stopped`.
- **ACL при старте**: аргументы
  `--user default off --user admin on ><пароль> ~* +@all --user app on
  ><пароль> ~* +@read +@write +ping +echo` (детерминированы от кредов etcd —
  пересоздание контейнера собирает актуальные пароли; `default` отключён —
  вход только по именованным ACL-пользователям; `+ping +echo` обязательны:
  клиенты StackExchange.Redis шлют ECHO/PING при handshake/keepalive — вне
  `@read`/`@write`, без них соединение не поднимается). Ротация паролей на
  живой ноде — через `ACL SETUSER` по соединению
  ([E](processes/e-password-rotator.md)), без пересоздания.
- **maxmemory**: аргументы `--maxmemory <bytes> --maxmemory-policy <policy>`
  при создании (политика — из декларации, канон-дефолт `allkeys-lru`,
  [valkey-clusters/02-keys.md](../valkey-clusters/02-keys.md) §1) + converge
  ([D](processes/d-config-converger.md), `CONFIG SET`) при мутациях
  декларации — без рестартов.
- **Persistence off**: без тома данных, `--save ""` / `--appendonly no`
  (снапшоты RDB/AOF не пишутся). Потеря контейнера = холодный старт кеша
  (документированное поведение).

## 2. TLS клиентского порта

- Нода слушает `--tls-port 6379 --port 0` — тот же клиентский host-порт из
  portalloc (контейнерный 6379 слушает TLS, plain закрыт);
  `endpoints`/portalloc не меняются (адреса стабильны, меняется только
  транспорт).
- Канонические аргументы:
  `--tls-cert-file /tls/node.crt --tls-key-file /tls/node.key
  --tls-ca-cert-file /tls/ca.pem --tls-auth-clients no --tls-replication no`
  (клиентские серты — нет, принципалы из ACL; реплик нет — standalone).
- Серт ноды (CN, SAN, срок, подпись) — [04-security.md](04-security.md) §1.
- Доставка — PEM в env контейнера ноды (`VALKEY_TLS_CERT`/`VALKEY_TLS_KEY`/
  `VALKEY_TLS_CA`, значения — многострочный PEM): при старте cmd-обёртка
  `["sh","-c","umask 077; mkdir -p /tls; printf %s \"$VALKEY_TLS_CERT\" >
  /tls/node.crt; printf %s \"$VALKEY_TLS_KEY\" > /tls/node.key;
  printf %s \"$VALKEY_TLS_CA\" > /tls/ca.pem; exec valkey-server <args>"]`
  раскатывает PEM в `/tls` (каталог в ФС контейнера, не том) и exec'ает
  `valkey-server` с каноническим набором аргументов (args — прежний канон
  `NodeArgsBuilder.Build`, shell-экранированный; единая точка канона
  обёртки — `NodeArgsBuilder.BuildCmd`). Entrypoint образа при `$1 == sh`
  делает `exec "$@"` как есть. Uid: gosu-ветка entrypoint срабатывает только
  при `$1 == valkey-server` — с обёрткой `valkey-server` стартует от root,
  файлы `/tls/*` — 0600 root (umask 077); изоляция секрета — периметром
  контейнера (env виден из `docker inspect` ноды — доступ к inspect/демону =
  доступ к docker-хосту).
- Перевыпуск серта (смена CA/SAN/истечение) — пересозданием контейнера со
  свежим env. Объекты домена: кластер = контейнер(ы) `vwk-<C>-node<k>`.
- **Ротация CA** ([K](processes/k-ca-rotator.md)): серт ноды в фазе R
  перевыпускается от НОВОЙ CA (`ca_next_key` staging), `VALKEY_TLS_CA` в env
  — НОВЫЙ CA (НЕ bundle — отличие от kafka truststore: `--tls-auth-clients
  no`, нода клиентов не валидирует, CA — только материал issuer'а; после
  коммита C env-CA == `ca_pem`(etcd) == NEW — переиспользование при
  следующих пересозданиях).

## 3. Сеть

- Per-cluster сеть НЕ создаётся (нет inter-node трафика; клиентский доступ —
  публикация host-порта из portalloc; без контурного режима контейнер живёт
  в default bridge docker-хоста запуска). Следствие: домен не порождает
  `*-net-*` объектов, пул подсетей docker не расходует.
- Опция `ContourNetwork` (стенд/E2E) — ноды создаются в сети контура
  поставки (стенд — единая стендовая сеть всего контура; E2E-класс — своя
  изолированная сеть): сеть воркером не создаётся и не удаляется (владелец —
  поставка/сценарий), advertised-правило и portalloc НЕ меняются —
  клиентская адресация остаётся публикациями host-портов (образец режима —
  [pgworker/01-placement.md](../pgworker/01-placement.md) §1). Пусто (прод/умолчание) —
  прежнее поведение. Не поддерживается в `Mode=Swarm` — fail-fast старта:
  сеть overlay назначает сам Swarm.

## 4. Порты, лимиты, инстансы

- **Порты**: диапазон `17000–17999` (продолжение ряда pg 15000–15999 /
  kafka 16000–16999), **1 клиентский порт на ноду** (контейнерный 6379 →
  выделенный host-порт), закрепление `/valkeyworker/portalloc/<C>`;
  занятость для довыделения = docker-публикации ∪ portalloc ВСЕХ чужих
  кластеров; довыделение — только держателем `/valkeyworker/locks/portalloc`
  ([valkey-clusters/03-coordination.md](../valkey-clusters/03-coordination.md)).
- **Advertised-правило**: advertised-хост ноды = `ValkeyWorker:
  AdvertisedClientHost`, если задан; иначе — имя docker-хоста размещения.
  Требование: значение обязано резолвиться КЛИЕНТАМИ (оно попадает в
  `endpoints`). Для локальных стендов — `host.docker.internal`.
- **Лимиты** контейнера из `resources` (cpu/mem; disk — инфо-поле).
  Инвариант: `maxmemory_bytes < mem`-лимит (валидация заявки; иначе
  OOM-килл — риск R3).
- Сам воркер — контейнер с `docker.sock`, поставляется через
  `deploy/docker-compose.yml` (по образцу KafkaWorker).
- **Два инстанса по умолчанию**: как у KafkaWorker — второй сервис
  `valkeyworker-2` того же образа (`VWK_API_HOST_PORT2`, ряд 8082/8085,
  уникальный `Api:AdvertiseUrl`), общие на оба инстанса тома `vw-snapshots`
  и `vw-api-tls:ro`, уникальность — случайный `InstanceId` без
  env-идентификации. Dev-стенд — `as-valkeyworker-2` (advertise
  `https://valkeyworker-2:8080` по compose-DNS, без хост-публикации порта).

Клиент Engine API — общий `Shared.Docker` (унификация движка;
канон-суперсет: start 304-идемпотентен, create при отсутствии образа —
pull+retry, exec-ошибка включает stdout, label контейнеров/сервисов —
`valkeyworker`); env-инспекция контейнеров/сервисов
(`InspectContainerEnvAsync`/`InspectServiceEnvAsync`) — источник факта env
нод для сверок (V3 re-run, надзор C, фаза R ротации).

## 5. Где это используется

- Создание кластера — [A. ProvisioningProcess](processes/a-provisioning.md)
  (аргументы, TLS-env, порт, лимиты).
- Надзор и пересоздание — [C. NodeSupervisor](processes/c-node-supervisor.md).
- Миграция plain→TLS — [T. TlsMigrator](processes/t-tls-migrator.md).
- Ротация CA — [K. CaRotator](processes/k-ca-rotator.md) (перевыпуск серта).
