# Спецификация: ответственность изменений etcd — webapi воркеров, ключи доступа, объяснимые алерты

Ветка: `feat-etcd-via-worker-api` · Дата: 2026-08-31 · Фаза dev-flow: spec.
Канон контрактов уже обновлён этой задачей (arch-first, см. §8 «Изменения
arch/»): `arch/14-pgworker.md` §1.1, `arch/15-kafka-clusters.md` §4,
`arch/16-kafkaworker.md` §1.1, `arch/adminpanel/02-etcd-contract.md`,
`arch/adminpanel/01-architecture.md`, `arch/adminpanel/03-panels.md` §4.1,
`arch/adminpanel/04-local-stand.md` §2.2.

## 0. Контекст (что сейчас и почему меняется)

Сегодня декларативный контракт исполняется «толпой писателей»:

- **Панель** сама пишет декларации в etcd (HTTP JSON gateway `/v3/*`):
  7 мутаций pg-домена (создание кластера, TO_REMOVE, add/remove шарда,
  заявки переездов, ротация app-пароля, recreate ноды) и 14 мутаций
  kafka-домена (`src/AdminPanel.Api/Operations/**`, планы в
  `src/AdminPanel.Etcd/Writing/`). Данные при этом читает из того же etcd.
- **Сиды** пишут контроль-плейн напрямую etcdctl'ом
  (`dev-stand/adminpanel/seed.sh`, `kafka-seed.sh`, `dev-stand/seed.sh`) —
  в обход любого владельца.
- **Алерты** панели (`AdminPanel.Core/Alerting/Rules/**`, kafka-аналог)
  сообщают факт («кластер demo без config-ключа»), но не объясняют, **что
  должно быть**, **для чего** ключ существует и **кто** закроет алерт —
  «движителя» нет: часть алертов висит вечно без ответа «что делать».
- У воркеров (`src/PgWorker.App`, `src/KafkaWorker.App`) HTTP-грань — только
  `/healthz`; адреса их никто в etcd не публикует.

Принцип «PgWorker — хозяин кластера» (воркер обязан репарировать записанное
извне) не работает, пока «извне» — это сама панель и сиды: воркер вынужден
терпеть чужие записи в своих префиксах.

## 1. Цель

1. **Единая ответственность изменений etcd**: префиксы `/clusters/`,
   `/pgworker/` и заявки `/service/<C>-<X>/request_*` пишет только
   PgWorker; `/kafka/`, `/kafkaworker/` — только KafkaWorker. Панель etcd
   **только читает** (инспекция — как сегодня).
2. **Webapi обоих воркеров**: мутации декларативного контракта принимает
   HTTP API исполнителя (`/api/*`, та же грань Kestrel, что `/healthz`).
3. **Ключи доступа в etcd**: каждый воркер при старте сам ставит lease-ключ
   `/pgworker/api/<id>` (соотв. `/kafkaworker/api/<id>`) со своим URL;
   панель резолвит API воркеров только по этим ключам.
4. **Сид через API**: стендовые сиды обоих контуров наливаются вызовом
   API воркера (`POST /api/seed/demo`), прямая запись etcdctl'ом из
   стендовых скриптов упраздняется.
5. **Объяснимые алерты с движителем**: каждый kind алерта несёт `Hint`
   (что не так / как должно быть / для чего ключ) и `Remedy`
   (worker-auto | operator-api | operator-runbook); добавлен алерт
   доступности API исполнителей `worker-api-unreachable`.

Не-цели: смена UI-контракта панели (фронт не меняется), унификация дублей
панель/воркеры (t08), TLS/mTLS API (t03), репарация брошенных переездов и
резолв мастера (параллельная задача feat-pgworker-adopt-repair).

## 2. Принципы

- **arch-first**: контракт изменён в `arch/` до кода; код — отражение
  контракта (§8).
- **Протоколы записи не изобретаются заново**: claim-txn, пакеты PUT,
  компенсации, RMW-txn, валидации — переносятся из панельных команд 1:1;
  сигнатуры UI и коды ответов (400/404/409/503/201/204) не меняются.
- **Воркер — авторитет валидации**: guards читают etcd напрямую воркером
  (уходит гонка «панельный снапшот отстал»); панель не дублирует
  серверную валидацию, только проксирует.
- **Живость = lease**: ключ доступа с TTL 15 c в одном keepalive-контуре с
  `instances/<id>`; ключ есть — инстанс жив и URL валиден.
- **Стенд = полный контур**: сид требует живого воркера; исключения
  (остановка kafkaworker после наливки) — сохранение прежней семантики
  «сид и живой воркер кафки не смешиваются».
- **Дубли DTO панель↔воркер осознанны** (как AdminPanel.Etcd/PgWorker.Etcd);
  унификация — t08, не эта задача.
- .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`;
  пакеты — `Directory.Packages.props`; воркеры и панель — только docker.

## 3. Структура и компоненты

### 3.1. Ключи доступа воркеров (etcd-контракт)

```
/pgworker/api/<instanceId>      lease TTL 15 c
  {"url":"http://<host>:<port>","instance":"<id>","since_unix":<unix>}
/kafkaworker/api/<instanceId>   lease TTL 15 c   (формат тот же)
```

- Ставит **сам воркер** в keepalive-контуре (`ClaimStore.StartAsync` /
  `KeepaliveLoop` — рядом с `instances/<id>`); смерть процесса гасит lease
  ≤15 c, takeover не нужен (ключ описывает инстанс, не роль).
- URL — из новых настроек `PgWorker:Api:AdvertiseUrl` /
  `KafkaWorker:Api:AdvertiseUrl` (env-оверрайды `PGW_API_ADVERTISE_URL`,
  `KFW_API_ADVERTISE_URL`). Значение обязано быть достижимо **клиентами
  API** (прежде всего панелью):
  - PgWorker в `deploy/docker-compose.yml`: `http://host.docker.internal:8080`
    (порт опубликован, панель живёт в соседней compose-сети — тот же
    паттерн, каким она ходит в `as-etcd` на host-2379);
  - KafkaWorker в dev-stand (та же сеть, что панель): `http://kafkaworker:8080`;
    в `deploy/`: `http://host.docker.internal:8081`.
- Пустой `AdvertiseUrl` — fail-fast старта воркера (ключ без URL бессмысленен).
- Панель читает префиксы в refresher-тик: `EtcdSnapshot` получает
  `IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints`, `KafkaSnapshot` —
  `WorkerEndpoints` (тип `WorkerEndpoint(InstanceId, Url, SinceUnix)`;
  парсинг толерантен: битый JSON → parseError, не роняет тик).

### 3.2. API PgWorker (`src/PgWorker.App/Api/`, префикс `/api`, порт `:8080`)

Каркас: minimal API-модуль `MapWorkerApi()` в `Program.cs` (после
`MapHealthChecks`); обработчики — plain-сервисы (воркер не использует
CQRS-каркас панели). Транспорт HTTP JSON, ProblemDetails с теми же
кодами/телами, что сегодня у панели (перенос текстов ошибок и `errors[]`
валидации — 1:1).

| Метод+путь | Что делает | Откуда переносится |
|---|---|---|
| `POST /api/clusters` | декларация создания кластера (claim-txn, пакет PUT, компенсация) | `CreateClusterCommand` + `ClusterCreatePlan` |
| `DELETE /api/clusters/{c}` | `config.state=TO_REMOVE` (RMW-PUT) | `DeleteClusterCommand` |
| `POST /api/clusters/{c}/shards` | декларация add-shard | `AddShardCommand` + `ShardScalePlan` |
| `DELETE /api/clusters/{c}/shards/{x}` | маркер демонтажа шарда + пред-проверки guard'ов | `DeleteShardCommand` |
| `POST /api/clusters/{c}/moves` | заявки переездов (упорядочивание `requested_unix`, клэйм-txn на заявку) | `MoveBucketsCommand` |
| `POST /api/clusters/{c}/app-password/rotate` | заявка ротации | `RotateAppPasswordCommand` |
| `POST /api/ha/{scope}/nodes/{node}/recreate` | маркеры `TO_RECREATE`+`recreate=soft\|hard` | `RecreateNodeCommand` |
| `POST /api/seed/demo` | демо-сид pg-контура (§3.5) | новый (логика из `dev-stand/adminpanel/seed.sh`) |

Особенности:

- Валидаторы и планы переносятся в `PgWorker.Core` (namespace
  `PgWorker.Core.Writing`): `ClusterCreatePlan`, `CreateClusterValidator`,
  `ShardScalePlan`, recreate-валидация. Guards, читавшие панельный
  снапшот (routing>0 на шарде, незавершённые переезды, QUARANTINED-ноды,
  `/service/<scope>/members` для recreate), переписываются на **прямые
  чтения etcd** воркером (range по префиксам) — семантика та же.
- Активный etcd-endpoint — из своего конфига воркера
  (`PgWorker:Etcd:Endpoints`), а не «из снапшота панели» (как было);
  отказ etcd → 503.
- Мутации API **не обязаны** выполняться держателем клэйма `<C>`: это
  записи деклараций (как раньше у панели), а не процессы над кластером.
  Инварианты атомарности — те же txn-клэймы по ключам деклараций.
- Аутентификация: middleware проверяет заголовок `X-Api-Key` против
  env `PGW_API_KEY` (пуст/отсутствует — проверка отключена: доверенная
  docker-сеть; стенд так и живёт). 401 c ProblemDetails.

### 3.3. API KafkaWorker (`src/KafkaWorker.App/Api/`, префикс `/api`, порт `:8080`)

Зеркально §3.2: 14 мутаций контракта `arch/adminpanel/02` §10.2 (таблица
сигнатур там же) — перенос из `src/AdminPanel.Api/Operations/Kafka/**`
(`KafkaCommands.cs`, `RebalanceCommands.cs`) и
`src/AdminPanel.Etcd/Writing/KafkaWriting.cs` в `KafkaWorker.Core.Writing`
+ `KafkaWorker.App/Api/`. Плюс `POST /api/seed/demo` (§3.5). Аутентификация —
`KFW_API_KEY`. Особенности те же (guards по прямым чтениям etcd; kafka-снапшот
панели не участвует).

### 3.4. Панель — прокси мутаций (не пишет в etcd вообще)

- Новый `AdminPanel.Etcd/Workers/WorkerApiGateway` (+ интерфейс): по
  живым `WorkerEndpoint` из стора (pg — `EtcdSnapshot`, kafka —
  `KafkaSnapshot`) выполняет HTTP-вызов: `HttpClient` (factory, таймаут
  10 c), заголовок `X-Api-Key` из `AdminPanel:Workers:PgApiKey` /
  `:KafkaApiKey` (секция новая; на стенде пусто). Failover: ошибка
  соединения/таймаут с одного URL → следующий живой ключ; живых нет или
  все недоступны → 503 ProblemDetails «API воркера недоступен
  (живых ключей /pgworker/api/ нет)».
- Все command-хендлеры `Operations/**` переписываются: тело запроса
  (DTO не меняются) сериализуется и уходит воркеру; ответ воркера
  (status + JSON) возвращается панелью как есть — фронт-контракт
  (`arch/adminpanel/03-panels` §1/§7.1) неизменен. Ошибочные ответы
  (400/404/409/503 ProblemDetails) — тоже как есть; панель добавляет
  только собственный 503 доступности API.
- `AdminPanel.Etcd/Writing/*` удаляется (планы/валидаторы переехали
  воркерам); из панельных интеграционных тестов контракта записи тесты
  переносятся воркерам, у панели остаются: read-тесты без изменений +
  новые тесты прокси (§6).
- `IEtcdGateway` в панели остаётся (refresher), но вызовов записи в коде
  панели больше нет.

### 3.5. Сид через API (демо-контроль-плейн стенда)

- **`PgWorker.Core/Seed/PostgresDemoSeedPlan`** — детерминированный план
  всех ключей сегодняшнего `dev-stand/adminpanel/seed.sh` (кластер demo:
  config, s1/s2 dsn/replicas/master-статика, routing 16 фикс-раскладкой
  10/6, статусы bucket_3/7/11, heal bucket_5, заявка move bucket_13,
  `/service/demo-s{1,2}/*`, `/cluster/nodes/*`); времена динамические от
  now. `POST /api/seed/demo` применяет план пакетами; идемпотентность:
  живой `/clusters/demo/config` → 200 `{"seeded":false}`.
- **`KafkaWorker.Core/Seed/KafkaDemoSeedPlan`** — план `kafka-seed.sh`
  (events Active + pending NOT_INITIALIZED, топики-архетипы, lifecycle-
  заявки, ротация, ребалансировка, drain-прогресс); идемпотентность по
  `/kafka/clusters/events/config`.
- Сид пишет ключи, которые в живой системе пишут другие субъекты
  (Patroni, эмуляторы, скрипты) — это стендовая эмуляция, поэтому оба
  сид-эндпоинта закрыты флагом `PgWorker:Api:EnableSeedEndpoint` /
  `KafkaWorker:Api:EnableSeedEndpoint` (default `false`; в стендовых
  compose он `true` через env). Ключ API (`X-Api-Key`) действует и на сид.
- **Стенд** (`dev-stand/adminpanel/`):
  - сервисы `seed` и `kafka-seed` из `docker-compose.yml` удаляются
    (прямая запись etcdctl'ом упразднена); каталог `dev-stand/adminpanel/seed/`
    (образ etcdctl) — удаляется;
  - новый `checks/05-seed.sh [pg|kafka|all]` (default `all`): идемпотентная
    наливка сидов через API — поднимает нужного воркера, если не поднят
    (pgworker — `deploy/`, kafkaworker — `--profile kafka`), ждёт
    `/healthz`, зовёт `POST /api/seed/demo`, в kafka-режиме дожидается
    живого lease-ключа `/kafkaworker/api/<id>`. Скрипт НЕ управляет жизнью
    воркера после наливки (не останавливает): потребитель сида решает сам —
    так, чек 50 после наливки гоняет мутации через живой API и останавливает
    kafkaworker в конце; end-state полного прогона (00-up + чеки) —
    «kafkaworker остановлен». Изолированный прогон `05-seed.sh kafka`
    оставляет воркера поднятым — это безопасно: у сида нет контейнеров
    брокеров, пробы воркера слепые (arch/16 §5 C: слепая проба = бездействие),
    lifecycle/rotate/rebalance-заявки не исполняются, данные сида стабильны.
    `all` используется как quick-режим
    (`90-down.sh -v && 05-seed.sh && 10-smoke-api.sh && 20-alerts.sh`);
  - `00-up.sh`: после подъёма PgWorker (шаг переупорядочивается до сида)
    зовёт `05-seed.sh pg` (kafka-сид по-прежнему не входит в полный
    подъём — e2e 55-го идёт на чистом `/kafka/`); шаг «сид не появился за
    30 c» меняет источник проверки (curl API вместо сервиса seed);
  - `checks/50-kafka-api.sh`: шаг наливки сида заменяется на
    `05-seed.sh kafka`, далее чек УПРАВЛЯЕТ жизнью воркера сам: (1) наливка
    (воркер поднят); (2) ожидание живого ключа `/kafkaworker/api/<id>`
    (поллинг до 30 c) и тика kafka-снапшота панели (WorkerEndpoints — без
    него мутации 503); (3) все шаги мутаций — через панель→прокси→API
    живого воркера, ожидания шагов не меняются (слепые пробы воркера
    сидовые заявки не исполняют — п. выше; поллинг готовности покрывает
    гонку подъёма); (4) финальный шаг чека — `docker compose stop
    kafkaworker` (end-state «после сида воркер остановлен»);
  - `kafka-seed.sh`/`seed.sh` удаляются (логика — в SeedPlan'ах);
  - env воркеров в стенд-compose: `KafkaWorker__Api__AdvertiseUrl=http://kafkaworker:8080`,
    `KafkaWorker__Api__EnableSeedEndpoint=true`; `deploy/docker-compose.yml`
    и `deploy/.env.example`: `PGW_API_ADVERTISE_URL` (default
    `http://host.docker.internal:8080`), `PGW_API_KEY` (default пуст),
    `KFW_API_ADVERTISE_URL` (default `http://host.docker.internal:8081`),
    `KFW_API_KEY`, `PGW_API_ENABLE_SEED=true` для стендового прогона.
  - `dev-stand/seed.sh` (стенд части, shop-кластер) заменяется тонкой
    curl-обёрткой: `POST /api/clusters` с теми же параметрами
    (декларативное создание — публичный эндпоинт, флаг сида не нужен).

### 3.6. Алерты: объяснения и движитель

- Модель `Alert` расширяется: `string Hint` (что не так / как должно быть /
  для чего ключ), `AlertRemedy Remedy` (enum: `WorkerAuto`,
  `OperatorApi`, `OperatorRunbook`), `string RemedyText` (конкретное
  действие). Параметры — обязательные в конструкторе `Alert`: правил,
  оставляющих поля пустыми, нет (покрыто unit-тестами).
- Все существующие правила `AdminPanel.Core/Alerting/Rules/**` и
  `Kafka/KafkaAlerting/**` наполняются: формулировки — по канону
  `arch/adminpanel/03-panels.md` §4.1 (маппинг классов → движитель;
  эталонные тексты фиксируются unit-тестами).
- Новое правило `WorkerApiUnreachableRule` (обе граны: pg по
  `EtcdSnapshot.PgWorkerEndpoints`, kafka по `KafkaSnapshot.WorkerEndpoints`):
  нет живых ключей → critical `worker-api-unreachable:<pg|kafka>` с Hint
  («воркер не поднялся/умер: lease-ключи /pgworker/api/ протухли; мутации
  из панели недоступны — 503; данные (чтение) не страдают») и Remedy
  `OperatorRunbook` («запустите контейнер воркера (deploy/docker-compose
  или профиль kafka); проверьте /healthz и AdvertiseUrl»).
- Движитель `WorkerAuto` на move-алгоритмах (`move-stale`,
  `move-frozen-long`, `move-flipped-status-stuck`, `shard-no-master`)
  **ссылается** на репаратор параллельной задачи feat-pgworker-adopt-repair
  (RemedyText: «репаратор переездов PgWorker закроет; если висит — дефект
  воркера»): реализация репарации — та задача, здесь только текст.
- UI/API: `/api/alerts` отдаёт `hint`, `remedy`, `remedyText` (обратно
  совместимо — старые поля на месте); карточка алерта в SPA раскрывает
  пояснение и бейдж движителя.

### 3.7. Согласование форматов (что НЕ меняется)

- Форматы значений etcd-ключей декларативного контракта — прежние
  (`arch/adminpanel/02` §9.1/§10): панельные e2e-чеки (15-й, 50-й)
  продолжают проверять те же ключи/значения — меняется только путь
  записи (через API воркера).
- Снапшотные модели панели чтения — прежние + `WorkerEndpoints`.

## 4. Граница с параллельной задачей feat-pgworker-adopt-repair

| Зона | Эта задача (etcd-via-worker-api) | feat-pgworker-adopt-repair |
|---|---|---|
| Ключи доступа воркеров, webapi, прокси панели, сид через API, Hint/Remedy алертов | да | нет |
| Резолв мастера (master-ключ vs Patroni факт) | нет | да |
| Репарация брошенных статусов переездов (SYNCING/FROZEN/ABORTING без заявки), adopt-move | нет | да |
| Движитель move-алертов | только текст Remedy (WorkerAuto → репаратор) | реализация закрытия алертов |

Конфликтная поверхность: оба трогают `arch/14` (у них — §5 F/§6 C) и
AlertEngine-тексты; при мерже вторым — пере-прогон текстовки Remedy
move-алертов на фактическое имя репаратора.

## 5. Фазы исполнения (план будет детализировать)

1. **Ф1 Воркеры — каркас API + ключи доступа**: настройки `Api`, lease-ключ
   `/api/<id>` в keepalive, `MapWorkerApi`, X-Api-Key middleware, DTO/планы
   переносятся; unit-тесты (планы — перенос панельных фиксстур),
   integration (Testcontainers etcd + WebApplicationFactory): мутации
   пишут контрактные ключи 1:1, ключ api появляется/исчезает с lease.
2. **Ф2 Полный набор мутаций**: 7 pg + 14 kafka эндпоинтов, guards на
   прямых чтениях; интеграционные тесты контракта (перенос панельных
   §9/§10.2-тестов, негативы 400/404/409/идемпотентность).
3. **Ф3 Сид-эндпоинты**: SeedPlan'ы (значения 1:1 прежним скриптам),
   флаги EnableSeedEndpoint, идемпотентность; integration-тесты сида
   (фикстуры = ожидаемые ключи).
4. **Ф4 Панель — прокси**: WorkerApiGateway + перепись команд, удаление
   `AdminPanel.Etcd/Writing`, снапшот-поля WorkerEndpoints + парсинг
   `/pgworker/api/`, `/kafkaworker/api/`; интеграционные тесты прокси на
   стаб-воркере (маппинг 201/204/400/404/409/503, failover между двумя
   живыми ключами, 503 при нуле), read-тесты не трогаются.
5. **Ф5 Алерты**: расширение `Alert`, тексты всех правил,
   `worker-api-unreachable`; unit-тесты Hint/Remedy каждого kind; UI
   (карточка + бейдж) и `/api/alerts`.
6. **Ф6 Стенд + e2e**: compose/deploy/README/04-arch sync, `05-seed.sh`,
   переупорядочивание `00-up.sh`, правка `50-kafka-api.sh`, расширение
   `20-alerts.sh` (503 мутаций + `worker-api-unreachable` при
   остановленном воркере, возврат к живому), прогон полного e2e-набора.
7. **Ф7 Ревью и мерж** (dev-flow гейты; roadmap-гейт не требуется —
   новых несделанных пунктов задача не оставляет, расширение t03 уже
   внесено).

## 6. Тесты (сводно)

- **PgWorker/KafkaWorker.IntegrationTests**: Testcontainers etcd +
  WebApplicationFactory воркера — по каждой мутации: happy-path (ключи
  etcd совпадают с прежними панельными фикстурами), негативы, повторная
  идемпотентность (409/204), claim-txn гонки (два параллельных POST);
  сид: идемпотентность, полнота набора; ключ `/api/<id>` жив при старте,
  гаснет с lease.
- **AdminPanel.IntegrationTests**: чтение — без изменений; мутации —
  стаб-воркер (внутрипроцессный HTTP-хост в тесте с заготовленными
  ответами/ProblemDetails) + подмена резолва WorkerEndpoints: проверка
  1:1 маппинга кодов/тел, failover на второй URL и 503 при пустых
  WorkerEndpoints.
- **AdminPanel.UnitTests**: Hint/Remedy каждого правила (в т.ч.
  kafka-домен), `worker-api-unreachable`, парсер ключей api.
- **e2e-чеки стенда**: `15-cluster-create.sh` и `50-kafka-api.sh` зелёные
  (ожидания самих мутаций не меняются — значения etcd те же; у 50-го
  допустимы managing-шаги: подъём воркера, ожидание живого ключа, финальный
  stop — §3.5/§9.3); новые шаги в
  `20-alerts.sh`; `00-up.sh`/`05-seed.sh` — сквозная наливка через API.

## 7. Ограничения и риски

- Воркеры и панель запускаются ТОЛЬКО в докере; API воркеров
  публикуются существующими портами (8080 pgworker, 8080/8081 kafkaworker);
  достижимость из сети панели — через host.docker.internal (прецедент:
  as-etcd) или общую compose-сеть.
- etcd-контур всегда один (as-etcd стенда); никаких вторых etcd.
- `TreatWarningsAsErrors=true` — новый код без варнингов; новые пакеты не
  вводятся (HttpClient/Json уже в стеке).
- Риски:
  - **Сеть панель→pgworker**: панель в `adminpanel-stand_default`,
    pgworker в `deploy_default` — связь только через host-публикацию
    8080 (как etcd 2379); при занятом хост-порту 8080 стенд не соберётся —
    проверяется на 00-up.sh шаге healthz (уже есть).
  - **Расползание контрактов DTO**: панель и воркеры имеют зеркальные
    DTO — дубли осознанны (t08), фиксируется интеграционными тестами
    обеих сторон на одних фикстурах.
  - **Quick-режим без воркеров**: сид теперь требует живого воркера —
    quick-цикл разработки панели по данным сида удлиняется (подъём
    воркера в 05-seed.sh); приемлемо, т.к. полный стенд и так поднимает
    обоих.
  - **Поведение живого PgWorker на демо-сиде** (заявка move bucket_13,
    чужие dsn): прежнее (воркер ретраит transient-ошибки в journal) —
    не усугубляется, сид всего лишь меняет исполнителя записи.

## 8. Изменения arch/, внесённые этой spec-фазой (канон)

- `arch/14-pgworker.md`: шапка (ответственность изменений etcd), §1
  диаграмма/роли, **новый §1.1** «HTTP API воркера» (+ §1.1.1 сид),
  §3.3 ключ `/pgworker/api/<id>`, §8 конфиг `PgWorker:Api`.
- `arch/15-kafka-clusters.md` §4: ключ `/kafkaworker/api/<id>`.
- `arch/16-kafkaworker.md`: шапка, §1, **новый §1.1** (API, сид, дискавери,
  аутентификация), §8 конфиг `KafkaWorker:Api`.
- `arch/adminpanel/02-etcd-contract.md`: шапка (панель не пишет; мутации —
  через API воркеров), §2.3.1 + `/pgworker/api/`, **новый §2.3.2**
  `/kafkaworker/api/`, §3 `WorkerEndpoint`, §4 тик, §9/§9.2/§10.2 —
  исполнитель воркер.
- `arch/adminpanel/01-architecture.md`: §1 диаграмма и правило потоков
  (панель — прокси мутаций).
- `arch/adminpanel/03-panels.md`: **новый §4.1** Hint/Remedy + kind
  `worker-api-unreachable`.
- `arch/adminpanel/04-local-stand.md`: §1 quick-профиль, §2.2 сид через
  API.
- `arch/roadmap/pgworker.md` t03, `arch/roadmap/kafkaworker.md` t03:
  расширены транспортной безопасностью API (mTLS) — единственные
  отложенные куски этой темы.

## 9. Критерии приёмки

1. **Панель не пишет в etcd**: в `src/AdminPanel.*` нет вызовов записи
   (`PutAsync`/`TxnAsync`/`DeleteAsync` вне transport-клиента); каталог
   `AdminPanel.Etcd/Writing` удалён.
2. **Ключи доступа**: после старта воркеров в etcd есть живые
   `/pgworker/api/<id>` и `/kafkaworker/api/<id>` с URL из AdvertiseUrl;
   `docker stop` воркера → ключи исчезают ≤15 c; панель видит их в
   снапшоте (fields в `/api/etcd/status`/модели — по тестам).
3. **Мутации сквозь API**: e2e `15-cluster-create.sh` и `50-kafka-api.sh`
   зелёные: ожидания самих мутаций (URL, коды, тела, значения etcd) НЕ
   меняются — допустимы лишь managing-шаги чека 50 вокруг мутаций (подъём
   kafkaworker после сида, ожидание живого ключа `/kafkaworker/api/<id>` и
   тика снапшота панели, остановка воркера в конце чека); вручную/чеком:
   остановленный воркер → мутация из панели 503, чтение живо.
4. **Сид через API**: `00-up.sh` (pg) и `05-seed.sh` (оба контура)
   наливают сиды выполнением `POST /api/seed/demo`; `etcdctl get
   /clusters/demo/`, `/kafka/clusters/` — то же содержимое, что до
   перехода (фикстуры); повторный вызов — no-op; в репозитории нет
   стендовых скриптов, наливающих сиды/декларации напрямую etcdctl'ом
   (`dev-stand/adminpanel/seed.sh`, `kafka-seed.sh`, каталог `seed/`
   удалены; `dev-stand/seed.sh` — curl-обёртка над POST /api/clusters).
5. **Алерты**: каждый kind (pg+kafka) отдаёт непустые `hint`/`remedy`/
   `remedyText`; при остановленных воркерах появляется critical
   `worker-api-unreachable` (обе граны), после подъёма — гаснет;
   unit-тесты фиксируют тексты-эталоны.
6. **Воркеры не сломаны**: `/healthz` и все процессы (provisioning,
   supervision, moves, ротации, автосинк, reassignment) — без изменений;
   существующие тесты воркеров зелёные.
7. **Качество**: `dotnet build` без варнингов (TreatWarningsAsErrors),
   все тесты (`src/tests/*`) зелёные; полный e2e-набор стенда зелёный с
   чистого состояния (`90-down.sh -v` → все чеки).
8. **Docker-only**: воркеры/панель запускаются только compose-стендами;
   новых хост-процессных путей не появилось.

## 10. Принятые решения (без вопроса пользователю — из принципов проекта)

1. Сид-эндпоинт — за флагом `EnableSeedEndpoint` (default false), набор —
   типизированный `SeedPlan` в коде воркера (не произвольная запись ключей
   через API): единственный писатель остаётся типизированным.
2. Жизнь kafkaworker при kafka-сиде управляет потребитель сида, не скрипт
   наливки: `05-seed.sh kafka` НЕ останавливает воркера (решение
   пользователя по ревью Фазы 4); чек 50 после наливки сам поднимает его,
   дожидается живого ключа `/kafkaworker/api/<id>`, гоняет мутации через
   панель→прокси→API и останавливает в конце — end-state полного прогона
   «после сида воркер остановлен» сохраняется. PgWorker после pg-сида
   остаётся жить (полная система, прежнее поведение). Совместимость сида с
   живым воркером: у сида нет контейнеров брокеров → слепые пробы (arch/16
   §5 C) → сидовые заявки не исполняются, данные стабильны.
3. Аутентификация API — `X-Api-Key` (env), в стенде допускается пусто
   (закрытая docker-сеть); mTLS — roadmap t03 (уже расширен).
4. Панель не делает серверную валидацию мутаций (только прокси) —
   источник истины воркер; фронтовые UX-проверки не трогаются.
5. Lease-ключ доступа ставится per-instance (не глобальный): много
   инстансов — много живых URL, панель берёт любой с failover.
