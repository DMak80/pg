# t09-ha-etcd-canon — канон + чеки HA-etcd для прода (3-нодовый контур)

- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), трек сквозной надёжности, приоритет P2, целевая характеристика **R** (быстрая самовосстанавливаемость); строка отчёта — [`arch/roadmap/reliability-report.md`](../../../arch/roadmap/reliability-report.md) (таблица «Осталось»).
- **Формулировка**: «канон + чеки HA-etcd для прода (3-нодовый контур): деплой-дефолт один endpoint, HA etcd — «зона оператора» без рецепта и проверок (arch/09 §4); воркеры умеют перебирать endpoints, но контур из одного etcd — единая точка отказа контроль-плейна».
- **Решения пользователя (2026-10-01)**:
  1. Канон — **расширение `arch/04-deploy-etcd.md`** (не новый файл): arch/04 становится рецептом etcd-контура для обеих архитектур — Patroni-стенда (ноды pg1/pg2/pg3) и контроль-плейна воркер-инсталляции.
  2. Деплой — **рецепт + конфиги в `deploy/`**: шаблон узла 3-нодового контура (`deploy/etcd/`), массив endpoints (`Endpoints__0..2`) в `deploy/docker-compose.yml`, HA-дефолты в `deploy/.env.example`.
  3. Дев-стенд — **дефолт стенда = 3 узла**: quick/full поднимают `etcd1/etcd2/etcd3` одним кластером (контур по-прежнему ОДИН — 3 узла, не 3 контура); чек kill узла.
  4. `docker/node/master-lease.py` — **чиним в рамках t09** (перебор списка `PGW_ETCD`): единственное место кода, не готовое к нескольким endpoints.
  5. Тесты — **E2E-сценарий kill узла** (опция `haEtcd` окружения) + интеграционный тест lease-скрипта + стендовый чек.

## 1. Контекст и цель

### 1.1. Проблема

Контроль-плейн установки (декларации кластеров `/clusters/`, `/kafka/`,
`/valkey/`, координация `/pgworker/`, `/kafkaworker/`, `/valkeyworker/`,
 Patroni-DCS `/service/*`, мастер-ключи шардов) живёт в едином etcd-контуре
(правило AGENTS.md: etcd-кластер всегда один). Клиенты контура готовы к HA:

| Потребитель | Механизм | Где |
|---|---|---|
| PgWorker | `Etcd:Endpoints[]` + failover (`RangeWithFailoverAsync`, `EtcdFailover`, `WithFailoverAsync` в процессах) | `src/PgWorker.App`, `src/PgWorker.Provisioning` |
| KafkaWorker / ValkeyWorker | `Etcd:Endpoints[]` + тот же паттерн | `src/KafkaWorker.*`, `src/ValkeyWorker.*` |
| AdminPanel | `Etcd:Endpoints[]`, активный endpoint + круг живых (`SnapshotRefresher` §3.10) | `src/AdminPanel.Etcd` |
| Patroni-ноды (Spilo, создаёт PgWorker) | `ETCD3_HOSTS` — список `host:port` (Patroni etcd3 умеет несколько) | `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` |
| Healthz воркеров | `etcd-reachable` — range по ВСЕМ endpoints, достаточно одного живого | `src/PgWorker.App/HealthChecks/ServiceProbes.cs` |

Но сам контур в деплее — **один экземпляр без избыточности**:

- `deploy/docker-compose.yml` задаёт ровно один endpoint
  (`PgWorker__Etcd__Endpoints__0: ${PGW_ETCD_ENDPOINT:-…}` и аналоги
  KFW/VWK) с комментарием «etcd запускается отдельно … или свой кластер» —
  рецепт «своего кластера» для контроль-плейна **не существует**: arch/04
  описывает 3-нодовый etcd только для Patroni-стенда старой архитектуры
  (arch/01–13, ноды pg1/pg2/pg3, host-network).
- Дев-стенд (`dev-stand/adminpanel/docker-compose.yml`) поднимает один
  `as-etcd`; E2E/интеграционные фикстуры — один etcd-контейнер.
- Чеков контура нет вовсе (ни health/quorum, ни member list, ни отказоустойчивости);
  восстановление после потери кворума описано только для Patroni-стенда
  (arch/09 §4) — HA-контур остаётся «зоной оператора» без рецепта и проверок.

Отказ единственного etcd = заморозка контроль-плейна всей установки:
надзор (provisioning/rebuild/ротации/бэкапы/снапшоты), панель (читает
снапшот из etcd), Patroni-ноды (DCS). Датаплейн (PG/Kafka/Valkey) живёт сам,
но деградации копятся без наблюдения и ремонта — единая точка отказа
контроль-плейна (строка reliability-report, характеристика R).

Единственный **код-зазор** при переходе на 3 endpoints:
`docker/node/master-lease.py` (lease мастер-ключа Patroni-ноды, P11) читает
`PGW_ETCD` как ровно один URL (`ETCD + path` в `urllib.urlopen`) — а
`SpiloEnvBuilder` уже передаёт список через запятую
(`PGW_ETCD="http://e1:2379,http://e2:2379"` — ассерт
`NodeConfigBuildersTests`): при отказе первого endpoint (или при списке из
более одного) lease мастер-ключа гаснет — панель теряет `masterLeaseAlive`
(ложная деградация), doorman-маршрутизация мастер-ключа не обновляется.

### 1.2. Цель

1. **Канон**: arch/04 получает раздел «HA-контур контроль-плейна
   воркер-инсталляции» — рецепт 3-нодового etcd-кластера (static bootstrap,
   параметры, кворум, анти-аффинити хостов, advertised-правила для всех
   потребителей, процедуры потери узла/кворума, чек-лист контура).
2. **Деплой**: `deploy/etcd/` — шаблон узла контура (compose + env);
   `deploy/docker-compose.yml` воркеров принимает списки endpoints
   (`Endpoints__0..2` для PGW/KFW/VWK); `deploy/.env.example` — HA-дефолты.
3. **Стенд**: quick/full всегда поднимают 3-узловой контур
   (`etcd1/etcd2/etcd3`, один кластер); все потребители стенда переключены
   на списки endpoints; эмуляторы Patroni переживают отказ узла.
4. **Чеки**: стендовый чек `43-etcd-ha.sh` — кворум/health/member list;
   stop узла → контур пишет, воркеры/панель/master-ключ живы; start узла →
   кластер снова 3/3.
5. **Код (минимум)**: `master-lease.py` перебирает список `PGW_ETCD`
   (первый живый; отказ активного → следующий) — lease мастер-ключа
   переживает отказ одного узла контура.
6. **Тесты**: docker-E2E HA-сценарий (kill узла посреди provisioning) +
   интеграционный тест lease-скрипта на перебор.

### 1.3. НЕ-цели (границы)

- **Не** трогаем `src/PgWorker.*`, `src/KafkaWorker.*`, `src/ValkeyWorker.*`,
  `src/Shared.*`, `src/AdminPanel.*` — failover endpoints у воркеров/панели
  уже есть (§1.1). Если в ходе реализации выяснится, что код C# всё же
  нужен — СТОП и пересогласование с пользователем. Единственные код-правки:
  `docker/node/master-lease.py` (lease-скрипт в образе ноды) и
  `dev-stand/adminpanel/sidecar/emulator.py` (стендовый эмулятор) — оба
  Python-артефакта периферии, не C#.
- **Не** решаем t08 (выгрузка etcd-снапшотов в S3) — соседняя задача трека;
  снапшот-механика (SnapshotJob, локальный том) не меняется.
- **Не** вводим TLS/аутентификацию etcd-контура (клиентские серты, peer
  TLS) — контур остаётся открытым HTTP внутренней зоны, как сейчас.
- **Не** делаем 5 узлов и динамическое расширение (member add/remove):
  канон фиксирует ровно 3 (кворум 2/3).
- **Не** переводим `dev-stand/compose.yaml` («стенд части», изолированный
  прогон PgWorker) и дефолт `E2eFixture` на 3 узла: HA-режим E2E — опция
  только HA-сценария (§6.6); «стенд части» — дев-инструмент одиночных
  прогонов, скорость/простота важнее зеркала прода.
- **Не** добавляем Prometheus-алерты по etcd (`etcd_server_has_leader` и
  т.п.) — наблюдаемость трека N (t13–t17); чеки t09 — операторские скрипты
  и healthz, которые уже существуют.

## 2. Принципы

1. **arch-first**: сначала канон в `arch/04` (+ссылки из arch/09 §4 и
   `arch/adminpanel/04-local-stand.md`), затем deploy/стенд/чеки/код/тесты.
2. **Контур всегда один**: 3 узла = ОДИН etcd-кластер (единый DCS для
   воркеров, панели, Patroni-нод); никаких «вторых etcd» и никаких
   раздельных контуров per-domain (правило AGENTS.md сохраняется).
3. **Стенд зеркалит прод по дефолту**: 3 узла в quick/full — HA виден
   каждому прогону чеков, а не опциональному профилю (решение
   пользователя; прецедент t07 — «надёжность дефолтом, а не опцией»).
4. **Список endpoints — канон конфигурации**: каждый клиент контура
   получает СПИСОК всех клиентских URL (воркеры — env `*_ETCD_ENDPOINT_0..2`;
   панель — `Endpoints__0..2`; Patroni-ноды — `ETCD3_HOSTS`/`PGW_ETCD` из
   `Etcd:AdvertisedEndpoints`, fallback на `Endpoints` — уже работает).
5. **Отказ одного узла — не-событие**: кворум 2/3; клиенты (failover
   готов) даже не обязаны переключаться, если активный endpoint жив; чек
   доказывает, что stop любого одного узла не ломает ни запись, ни
   надзор, ни мастер-ключ.
6. **Прод-контур — на разных хостах**: узлы etcd размещаются на 3 разных
   docker-хостах (анти-аффинити: потеря одного хоста ≠ потеря кворума);
   на стенде 3 контейнера одного docker-хоста — осознанное упрощение
   (проверяется кворум/отказоустойчивость процесса, не отказ хоста —
   мульти-хостовые сценарии — t21).
7. **Только существующие образы**: `quay.io/coreos/etcd:v3.5.21` — уже в
   `dev-stand/images/images.txt` (локальный registry 192.168.0.1:5000,
   ранбук); новых внешних образов нет.
8. **E2E-изоляция и чистота**: HA-сценарий — по правилам
   [`docs/e2e-isolation.md`](../../e2e-isolation.md) (guid-имена, own-only
   teardown, ассерт чистоты) и телеметрии
   [`docs/e2e-launch.md`](../../e2e-launch.md); хост-порты — только
   динамические (зонд), таймауты фикстур короткие (кворум-бюджет ≤100 с).

## 3. Канон HA-контура (arch/04)

`arch/04-deploy-etcd.md` перестраивается из «рецепта Patroni-стенда» в
**рецепт etcd-контура вообще** с двумя применениями:

### 3.1. Новая структура arch/04

- §0 (новый): «Два применения одного рецепта» — (а) DCS Patroni-стенда
  (arch/01–13: etcd на тех же 3 нодах, что и PG); (б) контроль-плейн
  воркер-инсталляции (arch/14+: etcd — внешний контур, «запускается
  отдельно», потребители — воркеры/панель/Patroni-ноды, создаваемые
  воркером). Параметры кластера одни; различаются размещение и
  advertised-правила.
- §1–7 — существующий рецепт Patroni-стенда (нумерация/текст сохраняются,
  правки минимальные).
- §8 (новый): «HA-контур контроль-плейна воркер-инсталляции» — см. §3.2.
- Ссылки: из `arch/09` §4 и §0 — на §8 (восстановление кворума — общее);
  из `arch/14` §2.2/§4 (Etcd/AdvertisedEndpoints) — на §8 (список всех
  узлов контура); из `arch/16` §2.1 / `arch/21` §2 — на §8 (env
  `*_ETCD_ENDPOINT_0..2`).

### 3.2. Содержание нового раздела arch/04 §8

1. **Топология**: 3 узла (static bootstrap `--initial-cluster`, токен
   один), кворум 2/3, по одному узлу на docker-хост, хосты РАЗНЫЕ
   (анти-аффинити; допустимо совмещение с docker-хостами воркера, но не
   всех трёх на одном). Требования к хосту: SSD под data-dir, стабильные
   IP/DNS (как arch/02 §2), NTP.
2. **Параметры**: те же, что для Patroni-стенда (§3):
   `--heartbeat-interval=250`, `--election-timeout=2000`,
   `--auto-compaction-retention=1`, `--quota-backend-bytes=8GiB`,
   образ v3.5.21; `--initial-cluster-state=new` только на первом старте,
   затем `existing` (§5 рецепта — общий).
3. **Advertised-правила потребителей** (ключевая специфика (б)):

   | Потребитель | Откуда берёт адреса | Формат |
   |---|---|---|
   | Воркеры (deploy) | env `PGW/KFW/VWK_ETCD_ENDPOINT_0..2` | полные URL |
   | Панель | `AdminPanel__Etcd__Endpoints__0..2` | полные URL |
   | Patroni-ноды (Spilo) | `PgWorker:Etcd:AdvertisedEndpoints` (fallback `Endpoints`) → `ETCD3_HOSTS` | `host:port` без scheme, список |
   | lease мастер-ключа нод | тот же источник → `PGW_ETCD` | полные URL, список через запятую |

   Правило: клиенты контура всегда получают список ВСЕХ клиентских URL;
   единственный endpoint — допустим только для стендов/разработки.
   Advertise каждого узла — адрес(а), резолвимые из КАЖДОЙ сети
   потребителей (на стенде: compose-DNS `etcdN:2379` для сети стенда +
   `host.docker.internal:PORT` для per-cluster сетей воркера — etcd
   допускает список в `--advertise-client-urls`; в проде: IP хоста узла,
   host-network как в §3).
4. **Кворум-семантика**: 1 узел недоступен — всё работает (клиенты даже
   могут не заметить); 2 узла — кворума нет: контроль-плейн заморожен
   (надзор/панель/DCS), датаплейн живёт сам; восстановление — arch/09 §4.
   Операция «потеря узла навсегда» (замена хоста): member remove + member
   add + data-dir заново (рецепт пошагово; ссылка на стандартную
   процедуру etcd, краткие команды в arch/04).
5. **Чек-лист контура** (прод): member list = 3 started; endpoint health
   --cluster = 3 healthy; все воркеры/панель видят один и тот же список
   endpoints; healthz воркеров `etcd-reachable` жив; панель
   `GET /api/etcd/status` показывает 3 члена с единым leader/term.
6. **Стендовое зеркало**: дев-стенд поднимает тот же 3-узловой контур
   (§5 спеки) — чек `43-etcd-ha.sh` гоняет отказоустойчивость на каждом
   прогоне стенда.

## 4. Деплой (`deploy/`)

1. **`deploy/etcd/docker-compose.yml` + `deploy/etcd/etcd.env.example`** —
   шаблон ОДНОГО узла HA-контура (по образцу `arch/configs/etcd/`, но с
   шапкой «зона оператора воркер-инсталляции, arch/04 §8»):
   `network_mode: host`, static bootstrap из env (`NODE_NAME`, `NODE_IP`,
   `PEERS` — все три узла, `CLUSTER_TOKEN`, `INITIAL_CLUSTER_STATE`),
   тайминги/квота §3.2 п.2, `restart: unless-stopped`, logging-лимиты.
   Копируется на каждый из 3 хостов, env редактируется под узел
   (идентично паттерну arch/04 §2). Образ — из локального registry
   (`192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21`, ранбук).
2. **`deploy/docker-compose.yml`**: для каждого воркера три слота
   endpoints вместо одного:
   `PgWorker__Etcd__Endpoints__0/1/2: ${PGW_ETCD_ENDPOINT_0/1/2:-…}` (и
   симметрично `KafkaWorker__…`, `ValkeyWorker__…`). Дефолты — стендовые
   (host.docker.internal:2379/2381/2383 — ряд портов стенда §5); прод
   переопределяет в `deploy/.env` на IP узлов. Комментарий: узла 2/3 может
   не быть (не-HA прогон) — failover клиентов просто переберёт список.
3. **`deploy/.env.example`**: `PGW_ETCD_ENDPOINT` → `PGW_ETCD_ENDPOINT_0..2`
   (KFW/VWK — симметрично), HA-дефолты + комментарий «прод: адреса трёх
   узлов контура deploy/etcd (arch/04 §8)». `00-up.sh` стенда обновляется
   под новые имена (сегодня пишет `PGW_ETCD_ENDPOINT=…`).

## 5. Дев-стенд

1. **`dev-stand/adminpanel/docker-compose.yml`**: сервис `etcd` →
   `etcd1/etcd2/etcd3` (`container_name: as-etcd-1/2/3`), один static
   bootstrap-кластер в сети стенда:
   - peer-URL — compose-DNS (`etcdN:2380`), порты внутренние;
   - клиентские публикации на хост: `2379` (etcd1 — совместимость всего
     существующего инструментария), `2381` (etcd2), `2383` (etcd3);
     параметризация env (`ETCD*_HOST_PORT`, прецедент `METRICS_*_PORT`);
   - `--advertise-client-urls=http://etcdN:2379,http://host.docker.internal:PORT`
     (первый — для потребителей сети стенда, второй — для Patroni-нод в
     per-cluster сетях воркера; паттерн текущего as-etcd, расширенный на
     кластер);
   - healthcheck (`etcdctl endpoint health`) — 00-up ждёт готовности
     кворума (3 узла стартуют в пределах election-timeout).
2. **Потребители стенда — списки endpoints**:
   - панель: `AdminPanel__Etcd__Endpoints__0..2 = http://etcd1:2379,
     http://etcd2:2379, http://etcd3:2379`;
   - `kafkaworker`, `kafkaworker-2`, `valkeyworker`, `valkeyworker-2`:
     `Endpoints__0..2` те же; `depends_on` — на все три узла;
   - Patroni-эмуляторы (`hc1a/hc1b/hc2a/hc2b`): `ETCD_ENDPOINTS` — список
     через запятую;
   - PgWorker (deploy-проект, поднимает 00-up.sh) — через
     `PGW_ETCD_ENDPOINT_0..2` (§4 п.3).
3. **`dev-stand/adminpanel/sidecar/emulator.py`**: `ETCD_ENDPOINTS` —
   список: активный endpoint кешируется, транспортная ошибка → перебор
   (паттерн master-lease.py §6; single-URL значения остаются валидными).
4. **Упоминания `as-etcd`** обновить на кластер: `checks/00-up.sh`,
   `checks/66-kafka-worker-churn.sh`, `dev-stand/adminpanel/README.md`,
   `arch/adminpanel/04-local-stand.md` (исторические спеки/планы в
   `docs/superpowers/` не трогаем — это архив).
5. **README стенда**: строка о 3-узловом контуре + новый чек в списке.

## 6. Код: lease мастер-ключа нод

`docker/node/master-lease.py` (и только он; `docker-entrypoint.sh` уже
прокидывает `PGW_ETCD` как есть):

- `PGW_ETCD` парсится как список URL через запятую (один URL — частный
  случай; пустой/невалидный элемент отбрасывается);
- `etcd_post` идёт на АКТИВНЫЙ endpoint (кеш); транспортная ошибка/5xx —
  сброс активного и перебор списка до первого живого (timeout 5 с
  сохраняется); все мертвы — цикл lease_loop продолжает попытки (TTL 5 с
  семантика P11 не меняется: ключ гаснет, восстановится при оживании
  любого узла);
- печать лога: какой endpoint активен (диагностика «через кого пишет»).

Интеграционный тест (новый, интеграционные тесты образа ноды —
`src/tests/PgWorker.IntegrationTests`): testcontainers, ДВА etcd-узла
(отдельные контейнеры), запуск `master-lease.py` как процесс с
`PGW_ETCD=url1,url2`; AAA:
- Arrange: два живых узла, ключ `/test/master` пуст;
- Act: скрипт пишет lease-ключ; останавливаем ПЕРВЫЙ контейнер;
- Assert: ключ продолжает продлеваться через второй узел (get с rev-idle
  > TTL 5 c подряд), лог скрипта сообщает переключение.
Комментарии тестов — по нотации AAA (правило AGENTS.base).

## 7. Чеки стенда

Новый `dev-stand/adminpanel/checks/43-etcd-ha.sh` (после 40-live-probes:
к моменту есть сиды, кластеры, master-ключи):

1. **Контур цел**: `docker exec as-etcd-1 etcdctl … member list` = 3
   started; `endpoint health --cluster` = 3 healthy (endpoints по
   публикациям 2379/2381/2383); панель `GET /api/etcd/status` = 3 члена,
   единый leader.
2. **Отказ одного узла — не-событие**: `docker stop as-etcd-2` →
   - put/del тестового ключа через панель/api воркера проходит (кворум
     пишет);
   - healthz обоих инстансов PgWorker (`etcd-reachable`) жив; панель
     отвечает;
   - master-ключ демо-кластера жив (`masterLeaseAlive == true` в
     `/api/clusters/demo` — Patroni-эмулятор пишет через уцелевший узел);
   - `endpoint health --cluster`: 2 healthy, 1 unreachable — и это НЕ
     ошибка чека (ожидаемое состояние).
3. **Возврат узла**: `docker start as-etcd-2` → member list снова 3
   started, health 3/3 (с retry-ожиданием вхождения в кворум).
4. Чек самодостаточен: поднимает/убирает своё и не оставляет
   тестовый ключ (чистка в trap).

## 8. Тесты: docker-E2E HA-сценарий

`E2eEnvironment` получает опцию `haEtcd` (по умолчанию false — остальные
сценарии не дорожают):

- true: вместо одного — ТРИ etcd-контейнера в сети окружения (имена с
  guid-префиксом), static bootstrap по внутренним DNS-именам, хост-порты —
  зонд свободных (никаких литералов, AGENTS.md), advertise
  `http://host.docker.internal:PORT_N` (Patroni-ноды окружения ходят из
  per-cluster сетей, прецедент текущей фикстуры);
- готовность — wait-стратегия: кворум собран (health + hasLeader через
  `/v3/maintenance/status`), бюджет фикстуры ≤100 с (аналог
  BrokerBootSec — падение быстрое);
- teardown/чистота — по правилам `docs/e2e-isolation.md`: остановили
  логи, удалили всё своё (контейнеры/тома/сеть/ключи), ассерт чистоты.

Сценарий-класс (docker-E2E, `PGW_TEST_DOCKER=1`), AAA:

- Arrange: haEtcd-окружение, воркер на списке endpoints, кластер
  запровиженен, master-ключ жив;
- Act: останавливаем узел №1 (кворум 2/3 живёт); посреди отказа —
  добавляем шард (provisioning идёт через уцелевшие endpoints);
- Assert: шард запровиженен; healthz воркера жив на всём интервале;
  master-ключ пережил отказ (lease-скрипт ноды писал через второй
  endpoint; `masterLeaseAlive` не гас дольше TTL); возвращаем узел —
  member list снова 3, health 3/3.

Мерж-гейт задачи (код воркер-домена — `docker/node` трогаем):
docker-E2E на свежем Release: кейс-маркер
`Scale_AddEmptyShard` (обязательный минимум AGENTS.md) + новый HA-сценарий.

## 9. Документация и roadmap

- `docs/runbook.md`: раздел «HA-etcd контур» — подъём `deploy/etcd`
  (3 хоста), стендовое зеркало, чек 43, поведение при потере узла/кворума
  (ссылка arch/09 §4); строка `quay.io/coreos/etcd:v3.5.21` в таблице
  образов дополняется «×3 в HA-контуре».
- Мерж-гейт трека (тем же мерж-коммитом): тег `t09-ha-etcd-canon`
  удаляется из `arch/roadmap/reliability.md` (список и `←`-зависимости);
  в `arch/roadmap/reliability-report.md` строка t09 уходит из «Осталось» в
  «Сделано» (деплой-уровень: «etcd один endpoint» → «3-узловой контур,
  чеки»).

## 10. Фазы реализации

1. **arch**: arch/04 §0+§8, ссылки из arch/09/arch/14/arch/16/arch/21,
   `arch/adminpanel/04-local-stand.md` (arch-first, отдельный коммит).
2. **deploy**: `deploy/etcd/{docker-compose.yml,etcd.env.example}`,
   `deploy/docker-compose.yml` (Endpoints__0..2 ×3 воркера),
   `deploy/.env.example`.
3. **стенд**: compose стенда (etcd1/2/3 + потребители), sidecar-эмулятор,
   `00-up.sh`, README, упоминания as-etcd.
4. **код**: `master-lease.py` (перебор) + интеграционный тест §6.
5. **чеки**: `43-etcd-ha.sh`.
6. **E2E**: `haEtcd`-опция окружения + HA-сценарий §8.
7. **docs/roadmap**: runbook §9; roadmap-чистка — в мерж-гейте.

## 11. Ограничения и риски

| Риск | Митигация |
|---|---|
| Стенд: 3 etcd тяжелее старта (кворум должен собраться) | healthcheck + retry в 00-up; election-timeout 2 с — сбор за секунды; чеки не зависят от порядка старта узлов |
| Хост-порты 2381/2383 заняты на хосте | параметризация env (как METRICS_*), дефолты задокументированы |
| Панель/воркеры при «пропавшем» 2-м endpoint (не-HA прогон стенда части) | failover уже реализован (первый живый выигрывает, недоступный пропускается); healthz «хотя бы один» — уже так |
| E2E-сеть кластера etcd не подбирается ryuk | guid-имена + own-only teardown + чистка сетей по правилам e2e-isolation (уже правила проекта) |
| Rename env `PGW_ETCD_ENDPOINT` ломает чужие скрипты | все упоминания в репо обновляются в этом же флоу (deploy, 00-up, arch/adminpanel/04); внешних потребителей нет (env локального деплоя) |
| Сеть стенда: etcd2/3 advertise host.docker.internal | тот же механизм, что у текущего as-etcd (Docker Desktop проксирует публикацию) — риск не новый |
| Спека не покрывает отказ ХОСТА (single-host стенд) | осознанное ограничение (принцип 6); мульти-хост — t21 |

## 12. Критерии приёмки

1. **Канон**: arch/04 содержит §0 (два применения) и §8 (HA-контур
   контроль-плейна: топология/параметры/кворум/анти-аффинити/advertised-
   правила/потеря узла/чек-лист); ссылки из arch/09 §4, arch/14, arch/16,
   arch/21, arch/adminpanel/04-local-stand; arch-коммит первее кода.
2. **Деплой**: `deploy/etcd/` — шаблон узла (compose+env, static
   bootstrap); `deploy/docker-compose.yml` передаёт `Endpoints__0..2`
   всем трём воркерам; `.env.example` — HA-дефолты и прод-комментарий.
3. **Стенд**: `docker compose up` (quick и full) поднимает
   `as-etcd-1/2/3` одним кластером; панель/kafkaworker×2/valkeyworker×2/
   эмуляторы настроены на списки endpoints; PgWorker стенда получает
   `PGW_ETCD_ENDPOINT_0..2`; эмулятор переживает stop узла (перебор).
4. **Код**: `master-lease.py` пишет/продлевает мастер-ключ при недоступном
   первом endpoint из `PGW_ETCD`-списка (интеграционный тест зелёный);
   C#-код не изменён.
5. **Чек**: `43-etcd-ha.sh` на стенде проходит: 3/3 healthy → stop узла →
   запись/healthz/master-ключ живы → start → снова 3/3.
6. **E2E**: docker-E2E HA-сценарий зелёный на свежем Release (kill узла
   посреди add-shard: надзор продолжен, мастер-ключ жив, узел вернулся);
   изоляция и чистота по e2e-isolation (ассерты чистоты в сценарии);
   хост-порты только динамические.
7. **Мерж-гейт**: `Scale_AddEmptyShard` + HA-сценарий зелёные;
   roadmap-тег t09 снят, отчёт надёжности обновлён тем же мерж-коммитом.
8. **Runbook**: раздел «HA-etcd контур» (подъём/чеки/потеря узла).
