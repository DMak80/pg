# Spec: AdminPanel — закрытие etcdworker-части по отчёту 06 (полная ревизия + хвосты)

Дата: 2026-11-01. Worktree: `feat-adminpanel-etcdworker`. Канон: `arch/25-etcdworker.md`,
`arch/adminpanel/{02-etcd-contract,03-panels,04-local-stand}.md`, отчёт
`docs/adminpanel/06-ui-analysis.md`.

## 1. Цель и контекст

Выделенный EtcdWorker (t32, слит) — оркестратор etcd-контура (arch/25). Панельная
проекция домена (чтение `/etcd/config`, `/etcd/nodes/*`, `/etcdworker/snapshots`,
`/etcdworker/api/*`; страница «etcd» с составом контура и формой «Поддерживать N
узлов»; DR-карточки; ряд EtcdWorker на «Воркерах»; алерты выгрузки слепков;
прокси `PUT /api/etcd/config`) функционально реализована панельными шагами t32
(коммиты `cff0a2f6`, `cdf705f2`) и лежит в `main`.

Остаток по отчёту 06 §6 — не «написать секцию», а закрыть хвосты и расхождения:

1. Отчёт 06 и часть канона/практик описывают СТАРОЕ состояние («etcdworker не
   реализован», «состав контура — runbook», `enabled` в статусе слепков).
2. Полная ревизия реализации против канона выявила расхождения код↔канон
   (классификация managed/external; «Эффективный конфиг» EtcdWorker) и пробелы
   тестов/стенда (см. §5).
3. Канон панелей обещает события etcd-домена в ленте `/events` — в коде их нет.

Рамка согласована с пользователем: **полная ревизия** etcdworker-части панели
(сверка с arch/25 + arch/adminpanel, устранение любых расхождений) **плюс хвосты**
(актуализация отчёта 06, канона и практик, тесты, чек стенда), включая
добавление `GET /api/config` в сам EtcdWorker.

## 2. Принципы

- **arch-first**: каждая правка контракта — сначала в `arch/`, затем код, тем же
  коммитом. Источник истины по домену EtcdWorker — `arch/25`; по панельной
  проекции — `arch/adminpanel/02` (§2.3.5, §9.14) и `03-panels`.
- Панель в etcd домена НЕ пишет: все мутации — прокси в HTTP API воркера.
- Документы — только текущее состояние, без исторических пассажей («после t32»,
  «в рамках задачи X» — не оставлять).
- .NET 10, `TreatWarningsAsErrors=true``, `Nullable=enable`; P/Invoke запрещён;
  бюджеты/таймауты не менять.
- Доки — по-русски, идентификаторы — по-английски; тесты — с AAA-комментариями.
- Реализованный пункт roadmap/отчёта удаляется из списка тем же мерж-коммитом.

## 3. Текущее состояние (реализовано, НЕ переделывать)

Сверено с каноном; работает и остаётся как есть (перечислено, чтобы исполнитель
не трогал и знал границы):

- **Чтение** (SnapshotRefresher, тик 3 c): range `/etcd/config` (точечный),
  `/etcd/nodes/`, `/etcdworker/api/`, точечный `/etcdworker/snapshots`,
  `/workers/api_tls/etcdworker`; транспортный провал любого KV-чтения роняет тик.
  Парсеры `EtcdDomainParser` (config+nodes, unknown-листы игнорируются, битый
  JSON → parseError), `EtcdSnapshotsParser` (формат arch/25 §5, БЕЗ `enabled`).
- **Модель**: `EtcdSnapshot` с полями `EtcdDomain`, `EtcdSnapshots`,
  `EtcdWorkerEndpoints`, `EtcdWorkerHealth`, `EtcdBackupTarget`,
  `EtcdWorkerApiCert`.
- **Алерты**: `etcd-snapshot-export-failed`/-`stale` (без enabled, stale-порог
  2×`interval_min`, панельный дефолт 360 мин), `etcd-backup-cluster-missing`
  (молчит, когда S3-домен не читался), ряды `worker-api-unreachable`/
  `worker-unhealthy` для etcdworker.
- **Поллер**: `WorkerHealthPoller` пробит `/healthz` инстансов EtcdWorker из
  основного снапшота (`EtcdWorkerHealthStore`).
- **API панели**: `GET /api/etcd/domain` (EtcdDomainQuery: declared/created/
  managed-узлы + статус слепков), `PUT /api/etcd/config` (прокси воркеру телом
  `{"nodes":N}`, панельная дубль-валидация 0..7 → 400, коды 1:1: 200/400/409/503).
- **UI**: `EtcdPage` (endpoints, блок «Contour membership»: declared N /
  «observation only» / managed-таблица K/state/placed_on / фактическое число
  членов; модал «Maintain N nodes»; members + leader; карточка «DR snapshots»;
  alarms; баннер quorumSuspected); ряд etcdworker на `/workers` (инстансы/health/
  серт/рестарт; блок координации скрыт — coordination null, правильно по
  контракту §2.3.5 «координационные ключи вне панельного контракта»); карточка
  etcd-снапшотов в грани «Хранилище бэкапов» и в Overview (`backups.etcdSnapshots`);
  `alertRoutes`: `etcd-*`/`snapshot-export-*` → `/etcd`.
- **Стенд**: `00-up.sh` публикует EtcdWorker.App, ждёт живость ×2 (mTLS-healthz,
  порты 8087/8088) и ≥2 ключей `/etcdworker/api/`; наблюдает ключ
  `/etcdworker/snapshots` (отсутствие — не fail); сервис — deploy-compose
  (`x-etcdworker-env`).

## 4. Развилки задачи — все закрыты (решения фиксирую как каноничные для плана)

| # | Развилка | Решение |
|---|---|---|
| 1 | Рамка: «хвосты» vs «только доки» vs «полная ревизия» | **Полная ревизия** etcdworker-части + хвосты (ответ пользователя) |
| 2 | «Эффективный конфиг» EtcdWorker: скрыть блок или добавить API | **Добавить `GET /api/config` в EtcdWorker** (ответ пользователя): канон arch/25 §1.1 + код воркера по образцу S3Worker |
| 3 | Лента `/events`: добавить события etcd или убрать etcd из канона | **Добавить события etcd-домена** (ответ пользователя) |
| 4 | Чек стенда etcdworker-грани | **Да, read-only чек** `checks/44-etcdworker.sh` (ответ пользователя), без мутаций |
| 5 | Классификация managed/external: по префиксу имени `etcdw` (код сегодня) или по peer-URL-паттерну (arch/25 §3.1) | **По peer-URL-паттерну** `/etcdw-node-<k>:`: arch/25 — источник истины; текущий код дефектен — unstarted-learner (имя члена пусто до старта контейнера) классифицируется «external». Правка кода + уточнение формулировки 03 §3 |
| 6 | `enabled` в OverviewDto канона 03 §2 | Убрать из канона: arch/25 §5 — поле не пишется, выгрузка безусловна; код уже без него |
| 7 | Состав тела `GET /api/config` EtcdWorker | Группы thresholds/loops/snapshots/docker — БЕЗ Etcd-endpoints, Api/TLS, Docker Tls/Ssh, Snapshots.Dir (секреты/эндпоинты наружу не выходят; образец — `S3Worker.App/Api/Operations/EffectiveConfig.cs`, camelCase, `source:"env"`) |
| 8 | Интеграционные тесты чтения домена: расширять `EtcdSeed.Demo` или точечные `PutAsync` | Точечные `PutAsync` в тесте (образец — `BackupsStorageApiTests` для `/etcdworker/snapshots`); `Demo` не расширять; API-тест `/api/etcd/domain` — через `TestSnapshotStore`-фикстуру (паттерн `InspectionApiTests`), не через реальный etcd |
| 9 | Когда эмитятся события etcd | Только пока опрашивается запрос `etcdDomain` (страница `/etcd` открыта) — общая модель ленты «живёт в открытой вкладке»; зафиксировать комментарием в коде и в каноне 03 §3 |
| 10 | Номер чека стенда | `44-etcdworker.sh` (44 свободен, рядом с `43-etcd-ha.sh`) |

## 5. Структура работ (фазы для плана)

### Фаза A — канон (arch/) — ПЕРВОЙ

- **A1. `arch/25-etcdworker.md` §1.1**: дополнить таблицу HTTP API строкой
  `GET /api/config` — эффективный конфиг установки (read-only, та же mTLS-грань
  `/api`): тело-объект `source:"env"` + группы (camelCase, по образцу arch/24 §1.1):
  - `thresholds { nodeBootSec, nodeDeadSec, healthIntervalSec }`
  - `loops { scanIntervalSec, keepaliveSec, watchdog { enabled, multiplier, checkIntervalSec } }`
  - `snapshots { intervalMin, minIntervalSec, maintenanceIntervalMin, retentionFiles, retentionObjects, retryIntervalSec, timeoutSec }`
  - `docker { mode, network, advertisedHost, portRange { from, to }, images { etcd } }`
  - Наружу НЕ выходят: Etcd-endpoints, секция Api/TLS, Docker Tls/Ssh, Snapshots.Dir.
- **A2. `arch/adminpanel/02-etcd-contract.md`** §2.3.5 (on-demand прокси
  «effective-config»): в перечне источников контракта тела
  «arch/14/16/21/24 §1.1» добавить **arch/25 §1.1**.
- **A3. `arch/adminpanel/03-panels.md` §2**: `OverviewDto.backups.etcdSnapshots`
  — убрать `enabled`: `etcdSnapshots{state}?` (выгрузка безусловна, arch/25 §5;
  поле статуса — OK/FAILED).
- **A4. `arch/adminpanel/03-panels.md` §3.7**: перечень карточек дополнить
  EtcdWorker (сейчас «PgWorker, KafkaWorker, ValkeyWorker и S3Worker»; строка §3
  «Воркеры» его уже упоминает); явно: блок «Координация» — домены
  pg/kafka/valkey/s3 (etcdworker — координационные ключи вне панельного контракта,
  02 §2.3.5; блок скрыт).
- **A5. `arch/adminpanel/03-panels.md` §3 (строка «etcd»)**: пометки
  managed/external — классификация по peer-URL-паттерну `/etcdw-node-<k>:`
  (arch/25 §3.1), не по префиксу имени.
- **A6. `arch/adminpanel/03-panels.md` §3 (строка «События»)**: события домена
  etcd — диффы ответа `/api/etcd/domain`: state-change/appeared/gone managed-узла
  (`etcdw-node-<k>`), state-change статуса DR-слепков (OK↔FAILED), change
  декларации «поддерживать N» (null↔N, N→N'); фильтр etcd в списке доменов;
  события эмитятся, пока запрос `/api/etcd/domain` опрашивается (страница /etcd).
- **A7. `arch/adminpanel/04-local-stand.md`**: состав стенда дополнить
  EtcdWorker — сервис deploy-compose (publish-образ на хосте, порты
  `ETCDW_API_HOST_PORT/2` 8087/8088, mTLS), режим «внешний контур as-etcd →
  только наблюдение/слепки», чек `44-etcdworker.sh` в порядке проверок §3.

### Фаза B — код EtcdWorker: `GET /api/config`

- **B1.** `src/EtcdWorker.App/Api/Operations/EffectiveConfig.cs` — статический
  маппер `EtcdWorkerEffectiveConfig.From(EtcdWorkerOptions)` c составом A1
  (аналог `S3Worker.App/Api/Operations/EffectiveConfig.cs`).
- **B2.** В `src/EtcdWorker.App/Api/ApiModule.cs` рядом с `/api/etcd/config`:
  `endpoints.MapGet("/api/config", ...)` → 200 JSON (та же mTLS-грань `/api`,
  как у прочих эндпоинтов модуля).
- **B3.** Юнит-тест в `src/tests/EtcdWorker.UnitTests/`: из канонических
  `EtcdWorkerOptions` — все поля групп на месте и равны значениям Options
  (AAA); сериализованное тело НЕ содержит ключей секретных секций
  (`tls`, `ssh`, `endpoints`, `apiKey`-подобных; проверка по JSON-тексту).

### Фаза C — код панели (правки ревизии)

- **C1.** `frontend/src/pages/EtcdPage.tsx`: `isManagedMember` заменить на
  классификацию по peer-URL: member считается managed ⟺ хотя бы один его
  `peerUrls` имеет вид `http://etcdw-node-<k>:2380` (префикс хоста
  `etcdw-node-`; K — цифры). Член без name (unstarted learner) при этом
  корректно классифицируется managed. Бейдж «external» — все прочие.
  Комментарий обновить (arch/25 §3.1).
- **C2.** Комментарии в `EtcdPage.tsx` (3 места: шапка файла, `ContourCard`,
  `MaintainNodesModal`): ссылку «02 §9.13» → «02 §9.14» (нумерация контракта:
  §9.13 — restore, §9.14 — мутация состава).
- **C3.** События etcd (`frontend/src/events/`):
  - `eventsDiff.ts`: чистая функция `diffEtcdDomain(prev, next)` по
    `EtcdDomainDto`: узлы — state-change/appeared/gone (entity
    `etcdw-node-<k>`, kind `node`); snapshots — state-change (kind `snapshot`,
    entity `etcd`); declaredNodes — change (kind `config`, entity `etcd`;
    null↔N отобразить как `—`↔N).
  - `EventsProvider.tsx`: ветка для `queryKeys.etcdDomain` → `diffEtcdDomain`.
  - `EventsPage.tsx`: `DOMAIN_OPTIONS` + `{ value: 'etcd', label: 'etcd' }`;
    `ENTITY_KIND_OPTIONS` + `snapshot`, `config`; deep-link: в fakeKind-маппинге
    `EventRow` добавить ветку `e.domain === 'etcd'` → любой `etcd-*`-kind
    (например `etcd-alarm`; все `etcd-*` маршрутизируются на `/etcd`) — иначе
    fallback `cluster-incomplete` уведёт на `/clusters/...` (404).
  - Комментарий «etcd-домен диффы не эмитят» — удалить (устарел).
- **C4.** `frontend/src/events/eventsDiff.test.ts`: кейсы AAA — state-change
  узла, appeared/gone узла, OK→FAILED слепков, декларация 0→2.

### Фаза D — тесты панели (пробелы)

- **D1.** `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs`:
  `PutEtcdConfigCommandHandler` по паттерну класса (StubWorkerApi):
  (а) 200 `{"nodes":3}` → `EtcdConfigPanelDto.Nodes==3`, единственный Call
  `worker=="etcdworker"`, `Method==Put`, `Path=="/api/etcd/config"`, body
  `{"nodes":3}`, `RequestedBy==null`; (б) 409 ProblemDetails воркера →
  `WorkerProblemDetails.StatusCode==409`; (в) `WorkerApiUnavailableException` →
  прокидывается (503-семантика модуля).
- **D2.** Интеграционный API-тест `GET /api/etcd/domain` в
  `src/tests/AdminPanel.IntegrationTests/` (паттерн `InspectionApiTests`:
  `TestSnapshotStore` со снапшотом, собранным с `EtcdDomainInfo(2, …,
  [ManagedNodeView(1,"RUNNING","local"), ManagedNodeView(2,"PROVISIONING",null)])`
  + `EtcdSnapshotExportInfo("OK", …)`) → после логина GET возвращает
  declaredNodes=2, createdUnix, nodes[k/state/placedOn], snapshots.state=«OK».
- **D3.** Интеграционный refresher-кейс (паттерн
  `EtcdSnapshotIntegrationTests` + точечные `EtcdSeed.PutAsync`, как у
  `BackupsStorageApiTests` для `/etcdworker/snapshots`): против живого etcd
  записать `/etcd/config` `{"nodes":2,"created_unix":…}`, `/etcd/nodes/1/state`
  `RUNNING`, `/etcd/nodes/1/placed_on` `local`, `/etcd/nodes/2/state`
  `PROVISIONING` → после тика `SnapshotStore.Current.EtcdDomain` заполнен
  (declared=2; два узла; placedOn узла 1 == "local"). `EtcdSeed.Demo` НЕ
  расширять (решение §4-8).

### Фаза E — стенд и практики (docs/adminpanel)

- **E1.** Новый `dev-stand/adminpanel/checks/44-etcdworker.sh` (read-only, без
  мутаций; стиль/логин-сессия — по образцу `43-etcd-ha.sh`/`80-s3-panel.sh`):
  1) `GET /api/workers` → в ответе есть ряд `etcdworker` с ≥1 инстансом, health
  инстансов healthy (стенд поднимает 2); 2) `GET /api/etcd/domain` → 200;
  стендовый инвариант: `declaredNodes` null (внешний контур — «только
  наблюдение»), `nodes` пуст, `snapshots` — отсутствует или OK (отсутствие не
  fail, выводить значение — как 00-up); 3) `GET /api/etcd/status` → members ≥ 3,
  все external (as-etcd), — сверка с членами контура. Самодостаточен: чистка —
  только сессионный JAR (trap).
- **E2.** `docs/adminpanel/05-dev-stand.md`: состав стенда — дополнить EtcdWorker
  (и S3Worker, тоже отсутствует в описании при живом сервисе); порядок e2e —
  включить `checks/44-etcdworker.sh` (после `43-etcd-ha.sh`).
- **E3.** `docs/adminpanel/02-etcd-snapshot.md`: описание тика рефрешера
  дополнить etcdworker-чтениями (пять range-запросов: snapshots/config/nodes/
  api/cert; провал любого роняет тик; отказный тик переносит домен),
  парсеры `EtcdDomainParser`/`EtcdSnapshotsParser`, стор
  `EtcdWorkerHealthStore`; в чек-лист «добавить ключ» — пример домена `/etcd/`.
- **E4.** `docs/adminpanel/03-probes-alerts.md`: каталог алертов — группа
  «etcd-здоровье» дополнить `etcd-backup-cluster-missing` (и сверить весь список
  группы с фактическим набором правил: `etcd-snapshot-export-failed`,
  `etcd-snapshot-export-stale` уже упомянуты); `docs/adminpanel/INDEX.md` —
  счётчик «25 правил» заменить на фактическое число правил AlertEngine
  (пересчитать по `src/AdminPanel.Core/Alerting/Rules/` на момент правки).

### Фаза F — отчёт 06-ui-analysis.md (актуализация по его же чек-листу)

- **F1.** §1.1 таблица: строка «etcdworker (план, t32)… Не реализован» → текущее
  состояние: реализован (арх-канон arch/25, оркестратор состава контура и
  DR-слепков), убрать «план».
- **F2.** §2.3 «Меняется только руками»: пункты 6 (экспорт etcd-слепков) и 12
  (состав etcd-контура) — реализованы (выгрузка безусловна — управления не
  требуется; состав — мутация через панель) — удалить из списка кандидатов.
- **F3.** §4.2 дерево: строка «etcd-контур /etcd» — убрать «после t32 — состав
  узлов/кворум (etcdworker)», отразить фактический состав секции (endpoints/
  members/alarms; состав/форма N; DR-слепки).
- **F4.** §4.3 таблица: строка «etcd-контур» — управление: мутация состава
  «поддерживать N узлов» (02 §9.14), убрать «runbook; после t32».
- **F5.** §5: снять «Известное остаточное — секция etcd-контура под выделенный
  etcdworker»; перечислить фактическую реализацию секции.
- **F6.** §6: удалить блок «Будущее (после roadmap t32-etcdworker)» с пунктом —
  секция реализована (правило отчёта: реализованный пункт удаляется тем же
  мерж-коммитом; пустой §6 оставить с формулировкой «нет открытых задач»).
- **F7.** Точечные сверки формулировок §3.1/§3.2/§3.4, где источником статуса
  выгрузки слепков назван PgWorker — заменить на EtcdWorker
  (`/etcdworker/snapshots`).

### Фаза G — прогоны и зачистка

1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 errors/warnings.
2. Юниты: `AdminPanel.UnitTests`, `EtcdWorker.UnitTests` — зелёные.
3. Интеграции: `AdminPanel.IntegrationTests`, `EtcdWorker.IntegrationTests` —
   зелёные; после КАЖДОЙ серии — зачистка контейнеров/сетей по правилам AGENTS.md
   (docker-E2E PgWorker не требуется: код PgWorker не трогаем).
4. Фронт: `npm run build` + `npm test` (vitest) в `frontend/` — зелёные.
5. Стенд: полный прогон чеков по `docs/adminpanel/05-dev-stand.md` включая новый
   `44-etcdworker.sh`; после — `90-down.sh -v`.
6. Каждый прогон серии — по правилам телеметрии/зачистки AGENTS.md; упавшие
   тесты вне области — в `arch/roadmap/bugs.md`, не чинить в этой задаче.

## 6. Ограничения

- Не трогать: работающую реализацию §3 (чтение/алерты/поллер/UI-грань/прокси);
  бюджеты и таймауты любых подсистем; механику зачистки/изоляции тестов.
- `GET /api/config` — read-only, без секретов; расширение другими группами —
  вне объёма (roadmap при необходимости).
- События etcd — только на данных уже читаемого `/api/etcd/domain`; новых
  etcd-чтений для ленты НЕ вводить.
- Чек 44 — read-only: без `PUT /api/etcd/config` (мутация на стенде не
  проверяется; прокси покрыт юнитами D1).
- В docs/** не оставлять исторических пассажей (упоминания t32 в отчёте 06
  остаются только как указатели на канон arch/25, без «план/будущее»).

## 7. Критерии приёмки

1. Канон актуален: arch/25 §1.1 содержит `GET /api/config` с составом групп;
   02 §2.3.5-прокси упоминает arch/25; 03 §2 — `etcdSnapshots{state}?` без
   `enabled`; 03 §3.7 — пять карточек включая EtcdWorker (координация — 4
   домена); 03 §3 «etcd» — peer-URL-классификация; 03 §3 «События» — события
   etcd; 04-local-stand описывает EtcdWorker-стенд и чек 44.
2. Воркер: `GET /api/config` отдаёт состав A1 (проверено юнит-тестом B3,
   включая отсутствие секретных секций); сборка и тесты EtcdWorker зелёные.
3. Панель: managed/external по peer-URL (unstarted-learner — managed);
   комментарии ссылаются на 02 §9.14; лента /events показывает события etcd
   трёх видов с фильтром и deep-link на /etcd; `PUT /api/etcd/config` покрыт
   юнитами (D1); `GET /api/etcd/domain` — интеграционным API-тестом (D2);
   refresher-путь домена — интеграционным кейсом (D3); eventsDiff-кейсы (C4)
   зелёные.
4. Отчёт 06: без «план/не реализован» (§1.1), без пп. 6/12 в §2.3, §4.2/§4.3
   фактические, §5 без «остаточного», §6 пуст («нет открытых задач»).
5. Практики: 05-dev-stand (состав + порядок с чеком 44), 02-etcd-snapshot
   (тик с etcdworker-чтениями), 03-probes-alerts (+`etcd-backup-cluster-missing`),
   INDEX (фактическое число правил) — актуальны.
6. Стенд: `checks/44-etcdworker.sh` проходит на свежем подъёме (00-up → … → 44
   → 90-down), вывод чека содержит фактические значения (инстансы, health,
   declaredNodes, snapshots-статус).
7. Все серии прогонов зелёные (или падения — только вне области, оформлены в
   `arch/roadmap/bugs.md` с полным составом сведений); зачистка после каждой
   серии выполнена.

## 8. Ключевые ссылки исполнителю

- Канон: `arch/25-etcdworker.md` (§1.1 API, §3.1 peer-URL-паттерн, §5 ключи),
  `arch/adminpanel/02-etcd-contract.md` (§2.3.5 домен, §9.14 мутация, §9.9 серты),
  `arch/adminpanel/03-panels.md` (§2 DTO, §3 панели, §3.7 Воркеры, §4 алерты),
  `arch/adminpanel/04-local-stand.md`.
- Образцы кода: `src/S3Worker.App/Api/Operations/EffectiveConfig.cs` (B1),
  `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs` (D1),
  `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (D2,
  TestSnapshotStore/ApiTestLogin), `src/tests/AdminPanel.IntegrationTests/
  BackupsStorageApiTests.cs` (D3, PutAsync-паттерн), `frontend/src/events/
  {eventsDiff,EventsProvider}.ts` + `frontend/src/pages/EventsPage.tsx` (C3).
- Стенд: `dev-stand/adminpanel/checks/{00-up,43-etcd-ha,80-s3-panel}.sh`,
  `dev-stand/adminpanel/docker-compose.yml`, `deploy/docker-compose.yml`
  (`x-etcdworker-env`).
- Отчёт: `docs/adminpanel/06-ui-analysis.md` (+ его чек-лист).
- Базовые правила: `AGENTS.base.md`, `AGENTS.md` (корень pg) — обязательны к
  прочтению перед началом работ.
