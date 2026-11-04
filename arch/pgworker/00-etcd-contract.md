# PgWorker: контракт etcd и координация

Уровень 3. Ключи домена, которые читает и пишет воркер, координационный
префикс `/pgworker/` и инварианты мутаций. Контракт домена кластеров
(`/clusters/…`) — [11-bucket-sharding.md](../11-bucket-sharding.md) §2;
ключи бэкапов — [19-backups.md](../19-backups.md) §4; протоколы записи
панели — [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.
Здесь — состав ключей с точки зрения исполнителя-воркера, без дублирования
форматов домена.

## 1. Транспорт и модель чтения

Транспорт — HTTP JSON gateway `/v3/*` (как панель,
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §1): тот же
клиент + расширение lease-операциями (`/v3/lease/grant`,
`/v3/lease/keepalive`, `/v3/kv/put` с lease, txn с compare по
value/mod_revision и delete в success-ветке).

Чтение состояния — по принципу
[S10](../17-synchronization-principles.md): периодический опрос наблюдаемых
префиксов (цикл тика) сравнивается с локальным кэшем последнего
прочитанного; применяется и пересчитывается ТОЛЬКО различие (ключи кэша,
отсутствующие в прочитанном — удаления). Пере-применение без факта
изменения запрещено (реестр E10); watch не используется (выбор владельца —
S10).

**Чтение тика**: тик читает контроль-плейн из опросных кэшей наблюдаемых
префиксов (`/clusters/`, `/service/`, `/pgworker/backups/`,
`/pgworker/claims/` — интервал опроса = интервал тика;
`/pgworker/claims/` наблюдается, чтобы смерть инстанса — исчезновение его
клэймов-lease — доезжала до тика и takeover «≤ TTL 15 с + тик»
сохранялся) — etcd-чтений в самом тике ноль (реестр E10). Сам цикл тикает
ВСЕГДА: self-healing (provisioning в waiting-фазах, restore, надзор) ждёт
НЕ-etcd события (готовность Patroni, docker-состояние), гейт по дельте
заморозил бы недоведённую работу навсегда. «Применять только изменения»
(S10) относится к применениям etcd-значений: ensure кредов put-if-absent,
выравнивания ролей по проверкам (G2, [19-backups.md](../19-backups.md) §7),
пересборки панели — все по кэшу/проверкам, без пере-применения
неизменённых значений. Чтения ensurer'ов (P1.5) идут из кэша `/clusters/`;
живой etcd — только txn добора put-if-absent и re-read после него.

## 2. Читаемые ключи

Существующая схема — [11-bucket-sharding.md](../11-bucket-sharding.md) §2.

| Ключ | Зачем |
|---|---|
| `/clusters/<C>/config` | константы (buckets=N, dbname) + `state` (NOT_INITIALIZED/TO_REMOVE/отсутствует) |
| `/clusters/<C>/shards/<X>/replicas` | плановое число нод шарда |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | состояние нод (свои же записи — проверка идемпотентности) |
| `/clusters/<C>/shards/<X>/master` | живой мастер (lease TTL 5 с) — маршрутизация SQL-операций |
| `/clusters/<C>/buckets/routing/bucket_<i>` | раскладка бакетов (какие схемы создавать на каком шарде) |
| `/clusters/<C>/buckets/status/bucket_<i>` | `NOT_INITIALIZED` — снять после создания схемы; SYNCING/FROZEN/ABORTING — незавершённый переезд (эвакуация такого бакета запрещена) |
| `/service/<scope>/…` (scope=`<C>-<X>`) | Patroni DCS: `leader`, `members/<name>`, `initialize` — подтверждение поднятия HA-кластера |
| `/service/<scope>/request_{cpu,mem,disk}` | заявки ресурсов на ноду (лимиты контейнера/сервиса) |
| `/clusters/<C>/shards/<X>/state` | маркер демонтажа шарда `TO_REMOVE` (пишет ТОЛЬКО панель; отсутствие = обычный шард) |
| `/clusters/<C>/app_user` | per-cluster логин приложения; значение `"app"`; пишет ТОЛЬКО PgWorker (P1.5 ensure, txn put-if-absent); читают PgWorker (роли) и приложение |
| `/clusters/<C>/app_password` | per-cluster пароль приложения; строка 32 симв `[A-Za-z0-9]`; те же писатель/читатели; удаляется с префиксом кластера (D2); ротация — процесс [I](processes/i-secret-rotator.md) по заявке `/pgworker/rotations/<C>` (§4) |
| `/clusters/<C>/mover_password` | per-cluster пароль роли `bucket_mover` (REPLICATION — подписки переездов P2/P3); строка 32 симв `[A-Za-z0-9]`; имя роли фиксировано — отдельного user-ключа нет; пишет ТОЛЬКО PgWorker (ensure P1.5/adopt, txn put-if-absent; env `PGW_BUCKET_MOVER_PASSWORD` — fallback до ensure); читает PgWorker (mover-DSN переездов/проб); ротация — [I](processes/i-secret-rotator.md) |
| `/clusters/<C>/bucket_admin_user` | per-cluster логин DSN-точки входа; канон — ключ etcd: ensure P1.5/adopt (значение из config или `"bucket_admin"`, txn put-if-absent); порядок чтения: ключ → config → env `PGW_BUCKET_ADMIN_USER`; пишет/читает PgWorker |
| `/clusters/<C>/bucket_admin_password` | per-cluster пароль DSN-точки входа; тот же ensure и порядок чтения (ключ → config → env `PGW_BUCKET_ADMIN_PASSWORD`); попадает в dsn-ключи шардов (перезапись при ротации, [I](processes/i-secret-rotator.md)); панель не читает |
| `/clusters/<C>/shards/<X>/nodes/<n>/app_params` | per-node серверные параметры подключения (libpq `keyword=value` через пробел, дефолт `sslmode=require` из `PgWorker:AppParams:Default`); пишет ТОЛЬКО PgWorker put-if-absent (существующее значение — ручные правки оператора — НЕ перезаписывается); ensure в provisioning P2.5'/AddShard A5 и миграционно в надзоре C; читает приложение (concat к DSN, [11-bucket-sharding.md](../11-bucket-sharding.md) §3); панель не читает |

Формат config-JSON дополнен полем `"synchronous_mode_strict"?: true|false`
— per-cluster HA-режим: отсутствие поля или не-bool значение = `true`
(durability-first: strict-кластер при отсутствии sync-standby блокирует
запись). Пишут ТОЛЬКО API создания (`POST /api/clusters`) и мутации
(`PUT /api/clusters/{c}/config`,
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.10);
воркер поле читает (bootstrap + конвергенция), но никогда не пишет.

## 3. Пишемые ключи

| Ключ | Когда | Значение |
|---|---|---|
| `/clusters/<C>/shards/<X>/dsn` | после поднятия нод шарда | `host=h1,h2 port=15432,15433 dbname=<C> user=<bucket_admin> password=<per-cluster bucket_admin>` (multi-host; креды bucket_admin per-cluster — канон ключи `bucket_admin_*`, порядок чтения ключи → config → env; порты — выделенные аллокатором, [01-placement.md](01-placement.md) §5; app-секрет в DSN не попадает никогда) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | весь жизненный цикл | таблица состояний — [processes/enums.md](processes/enums.md) |
| `/clusters/<C>/shards/<X>/nodes/<n>/bootstrap` | каждый EnsureNode-путь (provision/add-shard/надзор/rebuild) и ротация `rest_password` (процесс I) | per-node данные доставки секретов ([04-security.md](04-security.md) §4): JSON из трёх полей, других полей НЕТ — `spilo_configuration` (string, НЕпустой YAML SPILO_CONFIGURATION, rest_password внутри), `cert_pem` (string, НЕпустой PEM серта ноды), `key_pem` (string, НЕпустой PEM ключа) — серверный серт ноды per-install REST-CA, SAN — [01-placement.md](01-placement.md) §5. Валидность для старта (агент): пара cert/key парсится X509-провайдером, YAML непуст; невалидный/битый ключ агент перечитывает (воркер перезапишет). Пишет ТОЛЬКО PgWorker (секретный ключ: класс экспозиции per-cluster секретов — etcd без per-key ACL); читает агент ноды (PgNode, по сигналу канала — [04-security.md](04-security.md) §3) при старте контейнера и по сигналу проверки/замены; ревизия применения — `mod_revision` ключа (конвергенция надзора); per-install пароли PG сюда НЕ входят (прямая передача по mTLS-каналу). Удаляется с префиксом кластера (D2) и ноды (S3) |
| `/clusters/<C>/buckets/status/bucket_<i>` | DELETE при завершении provisioning | снятие = бакет ACTIVE (семантика [11-bucket-sharding.md](../11-bucket-sharding.md) §2) |
| `/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без поля `state`** (инициализирован = поле отсутствует; compare по `mod_revision`); `synchronous_mode_strict` — переносится из прочитанного config БЕЗ изменения (per-cluster опция не стирается: созданный со strict=false кластер не «молча» становится strict) |
| `/clusters/<C>/…` (весь префикс) | TO_REMOVE, финал | `del --prefix` |
| `/service/<C>-shard<k>/request_*` | TO_REMOVE, финал | точечные `del` (свои заявки; остальное пространство Patroni не трогаем) |
| `/service/<C>-<X>/` (весь scope) | TO_REMOVE, после удаления нод | `del --prefix` (guard: контейнеров/сервисов нет) |
| `/clusters/<C>/shards/<X>/master` | ТОЛЬКО при рассинхроне (P11-сверка) | lease-put `host:<doorman-port>` по фактическому primary из Patroni REST |
| `/clusters/<C>/shards/<X>/nodes/<n>/app_params` | provisioning P2.5'/AddShard A5 (после dsn), миграционно в надзоре C (ноды шардов с dsn без ключа) | put-if-absent (txn NotExists): значение по умолчанию `PgWorker:AppParams:Default`; существующий ключ не перезаписывается (ручные правки живы) |
| `/clusters/<C>/app_password` | ротация (процесс I): после успешного ALTER ROLE на всех шардах | txn `[compare value==старый] [put новый, del /pgworker/rotations/<C>]` — атомарный коммит с удалением заявки |
| `/clusters/<C>/mover_password` | ensure (P1.5/adopt, put-if-absent); ротация (процесс I) | при ротации — put нового значения в общем txn-коммите |
| `/clusters/<C>/bucket_admin_user` | ensure (P1.5/adopt, put-if-absent: из config или `"bucket_admin"`) | ротацией не меняется (только пароль) |
| `/clusters/<C>/bucket_admin_password` | ensure (put-if-absent); ротация (процесс I) | при ротации — put в общем txn-коммите + перезапись dsn-ключей всех шардов |
| `/clusters/<C>/shards/<X>/dsn` | ротация (процесс I) | перезапись: пароль bucket_admin в conninfo заменяется (regex `password=…`), compare `value==прочитанное` — гонка с внешней записью dsn закрывается ретраем тика |
| `/pgworker/rotations/<C>` | успех ротации или битая заявка-мусор (процесс I) | del (в той же txn, что и put app_password; мусор — отдельным del с journal); TO_REMOVE-финал D2 тоже чистит |
| `/pgworker/moves/<C>/` (префикс) | TO_REMOVE, финал (D2) | `del --prefix` — заявки переездов не переживают удаление кластера |

**Финальное состояние кластера после provisioning**: `config` без поля
`state` (формат значения единый со скриптовым путём,
[11-bucket-sharding.md](../11-bucket-sharding.md) §4.5 — панель видит
«Active» без правок), все `status/bucket_<i>` удалены (бакеты ACTIVE), у
каждого шарда есть `dsn`, у каждой ноды `state=RUNNING`.

Дельта add/remove шарда живого кластера (процессы
[G](processes/g-add-shard.md)/[H](processes/h-remove-shard.md)):

| Ключ | Когда | Действие |
|---|---|---|
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | add-shard A3/A4 | `PROVISIONING` → `RUNNING` (те же значения) |
| `/clusters/<C>/shards/<X>/dsn` | add-shard A5 | put multi-host (порты из portalloc, с per-cluster bucket_admin user+password) |
| `/clusters/<C>/shards/<X>/nodes/<n>/state` | remove-shard S2 | `REMOVING` |
| `/clusters/<C>/shards/<X>/` (весь префикс) | remove-shard S3, после удаления docker-объектов | del prefix (state/replicas/nodes/dsn/master — всё) |
| `/service/<C>-<X>/` (весь scope) | remove-shard S3, guard: docker-объектов нет | del prefix |
| `/service/<C>-<X>/request_{cpu,mem,disk}` | remove-shard S3 | точечные del (даже если scope ещё жив) |
| `/pgworker/portalloc/<C>` | remove-shard S3 | read-modify-write: из JSON удалить записи `"<X>/<n>"` (merge под клэймом) |
| `/pgworker/evacuations/<C>/<X>` | remove-shard S3 | del (журнал эвакуации не переживает демонтаж шарда) |

Контрактное **финальное состояние после add-shard**: у шарда есть `dsn`,
все ноды `RUNNING`, `nodes`-ключи = декларации, routing/status/schema-мир
кластера не изменён НИКАК. После remove-shard: префиксы `shards/<X>/` и
`/service/<C>-<X>/` пусты, записей шарда в portalloc нет, остальные шарды
кластера не затронуты.

## 4. Координационные ключи `/pgworker/`

Панель читает префикс избирательно (`portalloc`, `moves`, `api`, `work` —
[adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1);
координационные `leader`/`claims`/`instances` ей не видны и не мешают
контракту.

| Ключ | Тип | Назначение |
|---|---|---|
| `/pgworker/leader` | lease TTL 15 с | глобальный лидер для singleton-задач (регулярные снапшоты P12). Value: `{"instance":"<id>","since_unix":…}`. Захват: txn `version==0` + put-with-lease; продление keepalive раз в 5 с. Умер лидер → lease истёк → любой другой захватывает. |
| `/pgworker/claims/<C>` | lease TTL 15 с | **пер-кластерный клэйм** работы: exclusivity обработки кластера одним инстансом. Value: `{"instance":"<id>","since_unix":…,"phase":…}`. Захват txn `version==0` + put-with-lease; держатель продлевает. Takeover: lease истёк → ключ исчез сам → txn другого инстанса succeeds. |
| `/pgworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм** ([01-placement.md](01-placement.md) §5): взаимоисключение секции довыделения портов «чтение занятости → выбор троек → запись `/pgworker/portalloc/<C>`» (provision P1 / add-shard / adoption-реплан) — пер-кластерные клэймы кросс-кластерную гонку не закрывают. Value: `{"instance":"<id>","since_unix":…}`. Захват txn `version==0` + put-with-lease; освобождение по завершении секции (del + revoke lease), смерть держателя — TTL. Не взял → InProgress (следующий тик). Без keepalive: секция короткая (единицы секунд ≪ TTL). |
| `/pgworker/work/<C>` | обычный | журнал текущего процесса кластера (journal-before-manipulations, по образцу P7): `{"op":"provision\|deprovision\|evacuate\|rebuild\|rotate","phase":"…","updated_unix":…,"instance":"<id>","last_error"?,"fail_count"?,"fail_first_unix"?,"retry_not_before_unix"?,"unreachable"?,"last_failover"?,"last_rebuild"?,"rest_window_open"?}` (`rest_window_open` — bool, маркер ОТКРЫТОГО окна REST-ротации процесса I: true кладётся ТОЙ ЖЕ txn R2 (put work-ключа в ops наряду с NEW×4/dsn/del заявки — атомарность «заявка снята ⇔ флаг стоит»), false — при `done` R4; ПЕРЕНОСИМ всеми писателями журнала (carry-forward, как `unreachable`/факты HA) — журнал last-write-wins и надзор пишет supervise-фазы каждый тик, непереносимый маркер затирался бы; предикат окна и гварды строятся ОТ ФЛАГА, фазы `rotate-*` — только прогресс). Крах оставляет самодокументирующийся след; следующий инстанс продолжает с записанной фазы. Поля ретраев: `fail_count` — подряд идущие фейлы (сбрасывается при успехе/`done`), `fail_first_unix` — первый фейл серии (возраст проблемы; живёт до закрытия серии), `retry_not_before_unix` — до какого времени тики процесса skip (бэкофф provisioning); переносятся записями фаз внутри серии. Читает панель (алерт `provision-stuck`, [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1) и оператор. Одна запись на процесс — «последняя фаза побеждает»: каждая фаза/тик перезаписывает ключ, журнал — НЕ лог-история (счёт событий/вхождений по нему невозможен); наблюдаемость «мутация происходит/не происходит» — по mod_revision целевого ключа (идемпотентность конвергенции DCS проверяется стабильностью mod_revision `/service/<scope>/config`). Поля HA-фактов надзора (последний факт каждого вида, без истории): `last_failover` — недоступность лидера HA-scope, завершившаяся сменой лидера: `{"shard":"<X>","node":"<n>","cause":"accelerated"\|"elections","detected_unix":<unix>,"resolved_unix"?,"duration_sec"?}`; `last_rebuild` — пересоздание ноды: `{"shard":"<X>","node":"<n>","cause":"auto-dead"\|"operator-recreate","detected_unix":<unix>,"resolved_unix"?,"duration_sec"?}`. `detected_unix` — первый тик недоступности из трека `unreachable` (для operator-recreate живой ноды — момент исполнения маркера); `resolved_unix`/`duration_sec` отсутствуют — событие открыто (идёт сейчас; takeover продолжает от сохранённого `detected_unix`). Длительность — от первого тика недоступности до работоспособности (окно детекции входит — честный RTO). Новое событие того же вида перезаписывает поле. Владелец полей — надзор (единственный писатель); фазовые записи процессов переносят оба поля (как `unreachable`). |
| `/pgworker/evacuations/<C>/<X>` | обычный | журнал эвакуации шарда: `{"evacuated_unix","reason","buckets":{...старый→новый владелец...},"state":"DONE\|QUARANTINED"}` — истина для разбора после возврата шарда. |
| `/pgworker/portalloc/<C>` | обычный | закрепление выделенных портов за нодами ([01-placement.md](01-placement.md) §5): `{"<shard>/<node>":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}` (+опц. `"object"` для усыновлённых, [J](processes/j-adoption.md); +опц. `"alias"`/`"net"` сетевой идентичности при PgWorker:Docker:ScrapeNetwork) — переживает смерть инстанса, переиспользуется при rebuild; пишется также усыновлением (read-modify-write merge под клэймом). |
| `/pgworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика; необязательно для работы) |
| `/pgworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** ([07-api.md](07-api.md) §4): `{"url":"https://<host>:<port>","instance":"<id>","since_unix":…,"cert_thumbprint"?}` — ставит сам инстанс при старте; ключ жив = инстанс жив и его URL валиден. Читает панель (единственный способ найти API воркера) и оператор; в UI — только сводка инстансов на грани «Воркеры». |
| `/pgworker/moves/<C>/bucket_<i>` | обычный | заявка на плановый переезд/откат/уборку/отмену: `{"op":"move\|rollback\|finalize\|abort","to":…,"old_shard":…,"skip_reverse":…,"resume":…,"force":…,"requested_unix":…,"requested_by":…}`. Успех или перманентный валидационный отказ → ключ удаляется; transient-сбой → остаётся, фазы — в статус-ключе бакета. Обрабатывается только держателем клэйма `<C>`; одновременно — старейшая заявка кластера. Deprovisioning D2 чистит `/pgworker/moves/<C>/` (префикс). |
| `/pgworker/rotations/<C>` | обычный | заявка на ротацию per-cluster секретов ВСЕГО кластера — app, bucket_mover, bucket_admin И `rest_password` (панель, клэйм-txn `version==0` + put): JSON из двух полей, других полей НЕТ — `requested_unix` (number, unix-секунды, ОБЯЗАТЕЛЬНО), `requested_by` (string, username панели; пишется всегда при ротации). Валидность заявки = парсится JSON И `requested_unix` число; не-JSON/без числового `requested_unix` — битая заявка-мусор: процесс удаляет с journal-записью. Выполняет держатель клэйма `<C>` (процесс I, порядок ETCD-FIRST): txn-коммит NEW-четвёрки в etcd В НАЧАЛЕ окна (put `app_password`+`mover_password`+`bucket_admin_password`+`rest_password`, перезапись dsn-ключей, del заявки) → rolling-применение `rest_password` каналом агента БЕЗ пересоздания контейнеров ([04-security.md](04-security.md) §4) → финальный ALTER ROLE трёх ролей на мастере каждого поднятого шарда («etcd — источник, PG следует»). Уже стоит → панель получает 409 (идемпотентность повтора). Deprovisioning D2 удаляет ключ точечно. |
| `/pgworker/nodes/<C>/<X>/<N>/status` | lease TTL 15 с | **статусная машина агента PgNode** ([04-security.md](04-security.md) §5). Значение — JSON с ЗАКРЫТЫМ набором полей: `state` (string; множество значений — ТОЛЬКО `not_initialized\|initializing\|ready\|start_failed`), `applied_revision` (number\|null: null в `not_initialized` и в `initializing`; = `mod_revision` применённого bootstrap-ключа в `ready`, а также в `start_failed`, когда сбой произошёл после начала применения — рендер/старт компонентов/рестарты), `components` (объект с ФИКСИРОВАННЫМ набором ключей `pg` и `doorman` — других компонент нет; значение каждой — из множества `starting\|ready\|failed\|stopped\|prepared`: `starting` — фаза старта/ожидания SELECT 1, `ready` — проба SELECT 1 успешна (в tool-пути не используется), `failed` — исчерпаны 3 попытки старта, `stopped` — не запускался (рендер-ошибка/применение), `prepared` — параметры готовы, запуск за tool-командой (`is_tool_start=true`, обе компоненты); в `not_initialized` и сразу после txn-перехода в `initializing` — пустой объект `{}`), `updated_unix` (number, unix-секунды каждой публикации), `error` (string\|null, однострочная диагностика БЕЗ значений секретов; null/отсутствует всегда, кроме: `start_failed` (причина сбоя) и `ready` после упавшего рестарта применения — факт в error, машина остаётся `ready`). Пишет ТОЛЬКО агент ноды (lease-сессия как у статуса приёмника — смерть процесса гасит ключ, статус не застревает); читает-мониторит PgWorker (poll-тик надзора — реакции [C](processes/c-node-supervisor.md): доставка по `not_initialized`, команда применения по расхождению `applied_revision` на `ready`, пересоздание ноды с новым диском по `start_failed`); панелью не разбирается. Атомарность перехода `not_initialized`→`initializing` — txn compare по value. Deprovisioning D2 чистит с префиксом `/pgworker/nodes/<C>/`. |
| `/pgworker/etcd-snapshots` | обычный | **статус выгрузки снапшотов etcd в S3**: `{"enabled":bool,"state":"OK"\|"FAILED","last_uploaded_unix":N,"last_object":"etcd/snapshot-<id>.db","last_sha256":"<hex>","size_bytes":N,"interval_min":360,"error"?:"…"}`. Каждый снятый слепок выгружается; статус-ключ обновляется после каждой выгрузки. `last_uploaded_unix` — семантика «покрытия», не времени put-запроса: метка снятия последнего слепка, чьё содержимое подтверждённо доставлено в S3 фактическим upload'ом; поле двигается каждым успешным проходом sink'а (иначе детект отставания и stale-алерт врут на живом контуре). `last_object` — объект последней выгрузки, реально лежащий в S3; `last_sha256` — sha256 этой выгрузки (наблюдаемость). Пишет ТОЛЬКО инстанс PgWorker, выполнивший экспорт-операцию (лидер снапшотов или процесс в точках изменений — put одним ключом без RMW); панель читает ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §2.3.1). Наблюдаемость; источником для восстановления НЕ является (etcd мёртв — ключа нет). |

## 5. Смежный ключ серверного серта API

**`/workers/api_tls/pgworker`** (обычный, без lease): серверный серт API
воркера (`cert_pem`+`key_pem`+аудит) — пишет ТОЛЬКО панель
([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §9.9),
воркер ТОЛЬКО читает при старте ([07-api.md](07-api.md) §3). Воркер ключ
не удаляет и не перезаписывает.

## 6. Инварианты мутаций

- Любая мутация чужих данных (`/clusters/`, docker) выполняется **только
  держателем клэйма** `<C>`; txn-записи в `/clusters/` сопровождаются
  compare (routing=старое значение, config.mod_revision) — «применилось, а
  контрол-плейн не знает» невозможно (как flip в
  [11-bucket-sharding.md](../11-bucket-sharding.md) §5 шаг 4.7).
- Владение клэймом держится на всём протяжении мутации: потеря lease
  прерывает исполняющийся тик (cancellation-гвард), критические мутации
  длинных процессов — только после живой сверки клэйма (механика — S5
  [17-synchronization-principles.md](../17-synchronization-principles.md));
  журнал `/pgworker/work/<C>` несёт фазы `lease-lost`/`claim-lost/<фаза>`.
- Глобальный portalloc-клэйм (`/pgworker/locks/portalloc`) закрывает
  кросс-кластерную гонку довыделения портов; пер-кластерные клэймы её не
  видят.
