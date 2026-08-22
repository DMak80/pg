# Спецификация t09-frontend-ha — панели HA и Alerts, HA-сводка в Overview

Дата: 2026-08-23. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/frontend.md` (пункт `t09-frontend-ha` ← t06, t07 — оба
слиты), `arch/01-architecture.md` §5 (стек, polling, каркас t07,
страницы), `arch/03-panels.md` §1–3 (эндпоинты, DTO, панели — уточнён
этой задачей, см. §11) и §4 (каталог алертов — severity-канон),
`arch/02-etcd-contract.md` §2.2 (ключи `/service/` — семантика
leader/members/optime/raw config), §7 (unmatched-скоп — норма,
отображается с пометкой). Фактическое состояние кода: каркас t07 +
панели t08 (`frontend/src`: api/dto.ts — все DTO включая
`HaScopeSummaryDto`/`HaScopeDto`/`HaMemberDto`/`AlertDto`,
api/queries.ts — `fetchHaScopes`/`fetchHaScope`/`fetchAlerts` готовы;
OverviewPage с карточкой-HA-заглушкой «t09», HaPage/AlertsPage —
заглушки; LoadState, BucketStateBadge, форматтеры, PollingContext);
инспекция t04–t06 (`InspectionModule`: `/api/ha`, `/api/ha/{scope}` с
404/503, `/api/alerts?severity=&kind=` с валидацией severity).
Референс `../Puzzle` фронтенда не содержит (подтверждено в t07/t08
spec-шапках; `Puzzle/docs` — только бэкенд-паттерны) — паттерны
берутся из каркаса t07 и панелей t08.

## 1. Цель

Наполнить последние две панели SPA и закрыть HA-сводку дашборда
(замена заглушек t07):

- **HA** (`/ha`): список scope'ов (cluster/shard, лидер, члены,
  макс. лаг, пометка unmatched) → **детали** (`/ha/:scope`): шапка
  (лидер, optime, кластер/шард), таблица members (role/state/timeline/
  lag/probe-статус), raw config свёрнуто;
- **Alerts** (`/alerts`): таблица всех алертов с severity-цветами,
  kind, target, message, since; фильтр по severity; счётчики
  critical/warning — в навигации у пункта «Алерты»;
- **Overview**: HA-карточка наполняется сводкой (скопов всего / без
  лидера / unmatched + ссылки на детали).

**Бэкенд не меняется вовсе**: контракт t06 уже отдаёт всё
необходимое; решение t06 §3.19 («HA-сводка Overview не добавляется в
`OverviewDto`; фронтенду HA-список даёт `GET /api/ha`») реализуется
этой задачей клиентской агрегацией. Режим — строго read-only/polling
через существующий `usePollingIntervalMs`.

После t09 трек frontend закрыт полностью (`arch/roadmap/frontend.md`
опустеет); в roadmap остаётся только stand (t10-dev-stand, t11).

## 2. Принципы

- Источник истины — `arch/` (после правок §11); DTO фронта — строго
  фактические C#-records (сверка проведена: `HaQuery.cs`,
  `AlertsQuery.cs`, `OverviewQuery.cs`); слой `api/` t07 не
  перекраивается и не правится — все типы и fetch-функции уже на
  месте.
- Каркас t07/t08 не ломается: guard/401-редирект, polling-контекст,
  stale-бейдж, тёмная тема, паттерн состояний страниц (t08 §4.15) —
  переиспользуются дословно.
- Идентификаторы — английские; комментарии и все тексты UI — русские.
  Значения внешних систем (state/role Patroni, kind алертов,
  severity-канон) показаются как есть — это идентификаторы, не наши
  русские подписи (прецедент: лента алертов t08 показывает
  `kind`/`severity` без перевода).
- Polling — только `refetchInterval: usePollingIntervalMs()` на
  каждом запросе; одинаковые `queryKey` дедуплицируются TanStack:
  ключ `['alerts', {severity: undefined, kind: undefined}]` делится
  между Overview-лентой, Alerts-страницей и навигационными
  счётчиками; ключ `['ha-scopes']` — между HA-списком и Overview-картой.
- YAGNI: без URL-параметров фильтров, пагинации, сортировок колонок,
  фильтра по kind (контракт есть — фронтенду не заказан), mute/ack,
  истории алертов, фронтенд-тестов (прецедент t07 §3.5, t08 §9);
  каждая позиция зафиксирована в §4 или §10.
- Отображение «как есть»: панель немая (arch/03 §3); вычисления на
  клиенте — только агрегация сводки (подсчёты), форматирование и
  фильтрация; никакой интерпретации данных сверх DTO.

## 3. Данные (запросы страниц)

| Страница/элемент | Запросы (`queryKeys`) | refetchInterval |
|---|---|---|
| HA (список) | `haScopes` (`fetchHaScopes`) | `usePollingIntervalMs()` |
| HA details | `haScope(scope)` (`fetchHaScope(scope)`) | он же |
| Alerts | `alerts(undefined, undefined)` (`fetchAlerts()`) | он же |
| Overview HA-карточка | `haScopes` (тот же ключ — дедупликация с HA-списком) | он же |
| Навигация: счётчики alerts | `alerts(undefined, undefined)` (тот же ключ — дедупликация с Overview-лентой и Alerts) | он же |

Серверные `?severity=`/`?kind=` остаются контрактом API; фронт их не
использует: фильтр severity — клиентский по одному общему запросу
(решение §4.9; N алертов — десятки строк, дешёвое чтение снапшота из
памяти). 404 `/api/ha/{scope}` (скоп исчез между тиками) — через
`notFound`-контент `ErrorSection` (прецедент деталей кластера t08
§4.15).

## 4. Принятые решения (уточнения неоднозначностей; все — в рамках апрува «решать самому»)

### Маршруты и навигация

1. **Новый маршрут** `/ha/:scope` → `HaScopeDetailsPage` (файл
   `pages/HaScopeDetailsPage.tsx`); параметр `:scope` — имя скопа как
   в API; `encodeURIComponent` уже применён в `fetchHaScope`
   (прецедент `/clusters/:cluster` t08 §4.1). Маршрут добавляется в
   `App.tsx` рядом с `ha`; активность NavLink «HA» по префиксу
   `startsWith('/ha')` работает для деталей без правок (t08 §4.2).
2. **Счётчики critical/warning в навигации** (единственная правка
   `AppLayout.tsx`): новый компонент `layout/AlertsNavCounters.tsx` —
   `useQuery({ queryKey: queryKeys.alerts(), queryFn: () =>
   fetchAlerts(), refetchInterval: usePollingIntervalMs() })`;
   подсчёт `critical`/`warning` по массиву (info не считается).
   Рендер — в `rightSection` NavLink «Алерты»: `Group gap={4}` из
   двух Badge `variant="light" size="xs"`: critical — red, warning —
   yellow, текст — число. Показываются только при N > 0; при
   pending/ошибке/пустом массиве — rightSection пуст (навигация не
   мигает и не пугает нулями). Частота опроса — текущий
   polling-интервал без смягчения: ключ дедуплицируется с Overview и
   Alerts (один HTTP-запрос на тик на всё приложение), эндпоинт —
   чтение из памяти снапшота; отдельные query-опции (индивидуальный
   интервал/staleTime) не заводятся — YAGNI.

### HA: список

3. **HaPage** — запрос §3; Table: Scope (Anchor → `/ha/:scope`,
   моноширинно) | Кластер/шард (`cluster/shard`; при `matched=false`
   — «—» и жёлтый Badge «unmatched» с Tooltip «scope не сопоставлен
   кластеру (arch/02 §7)») | Лидер (`leaderName`; null у matched →
   красный Badge «нет лидера» (рифма с critical-алертом
   `shard-no-leader`); null у unmatched → приглушённый текст «нет
   лидера» без красного — чужой скоп не наша зона ответственности и
   не алерт (arch/02 §7); иначе моноширинно) | Члены
   (`membersHealthy/membersTotal` моноширинно «2/2»; при healthy <
   total — жёлтым цветом текста) | Макс. лаг (`lagMaxBytes` →
   `formatBytes`; null → «—»). Сортировка — как отдаёт API (порядок
   снапшота, Scope Ordinal), кнопок сортировки нет (прецедент t08
   §4.6). Пусто → «HA-scope'ы не найдены». Состояния — паттерн §4.15
   t08 (503 → «Данные ещё не собраны», сеть → текст ошибки, «Повторить»).

### HA: детали

4. **Шапка**: back-link «← HA», `Title` scope (моноширинно),
   Badge «unmatched» (yellow-light) при `matched=false`; строка
   метаданных: `Кластер/шард: cluster/shard` (кластер — Anchor →
   `/clusters/:cluster` при matched; иначе «—»), `Лидер: leaderName`
   (null → Badge «нет лидера»: красный для matched (рифма со списком
   и алертом `shard-no-leader`), серый для unmatched — §4.3), `optime лидера: <число>` —
   моноширинно как есть (десятичное LSN-число; hex-конвертация —
   интерпретация сверх DTO, не делаем; null → «—»).
5. **Таблица members** (`members[]`): Имя (моноширинно; при `name ===
   leaderName` — зелёная точка-метка/Badge «лидер» рядом, лидер
   опознаётся по имени) | Адрес (`host:port` моноширинно; port null →
   только host) | Роль (§4.6) | Состояние (§4.7) | Timeline (число;
   null → «—») | Лаг (`lagBytes` → `formatBytes`; null → «—») |
   Проба (§4.8).
6. **Роль Patroni** — Badge `variant="light"` с русской подписью
   известных значений и каноном в Tooltip: `master` → violet
   «мастер» (визуальная рифма с Badge «лидер» etcd-панели — violet,
   t08 EtcdPage), `replica` → blue «реплика»,
   `sync_standby` → teal «sync-standby»; null/неизвестные — серый
   Badge с исходной строкой (или «—» при null).
7. **Состояние Patroni** — Badge с исходной строкой (внешний
   идентификатор, без перевода — §2), цвет по карте: `running`,
   `streaming` → teal; `stopped` → red; известные переходные
   (`starting`, `creating replica`, `restart`, `crash reinit`,
   `waiting`) → yellow; null → «—» серым; прочие/неизвестные — gray
   (текст как есть: панель не знает полный словарь состояний Patroni,
   неизвестное — не ошибка). Карта — локальная константа
   `HaScopeDetailsPage` (используется одной таблицей; отдельный
   компонент-бейдж не заводится — YAGNI, в отличие от severity §4.12,
   который нужен трём местам).
8. **Probe-статус члена**: `probeError != null` → красный Badge
   «ошибка» с Tooltip-текстом ошибки + возраст `probeAtUtc` рядом
   приглушённо; `probeAtUtc != null && probeError == null` →
   приглушённый относительный возраст («12 с» —
   `formatIsoAge`, §4.16) с абсолютом в Tooltip; оба null → «—»
   (проб не было — выключены или тика ещё не случилось, t06 §3.15).
9. **Raw config свёрнуто**: Mantine `Accordion` c одним элементом
   «Raw config» (chevron-down, default закрыт) под таблицей members;
   внутри — `Text ff="monospace" size="sm"` с `whiteSpace: 'pre-wrap'`
   и `style={{ wordBreak: 'break-all' }}`, содержимое `rawConfig`
   как есть (raw JSON ключа `/service/<scope>/config`, arch/02 §2.2;
   pretty-print — интерпретация, не делаем). `rawConfig === null` →
   Accordion не рисуется вовсе.

### Alerts

10. **AlertsPage** — запрос §3 (все алерты, без серверных фильтров);
    сортировка на клиенте: severity-ранг (critical → warning → info),
    внутри — `sinceUnix` по убыванию, null — в конец (расширение
    `sortAlertRows` Overview: ранг info = 2). Фильтр severity —
    клиентский: `SegmentedControl` (в стиле PollingToggle) «все |
    critical | warning | info», default «все»; отфильтрованные строки
    считаются `filter(a => a.severity === picked || picked === 'all')`;
    рядом счётчик «N из M» (приглушённо). Обоснование против
    серверного `?severity=`: тот же ключ-кэш дедуплицируется с
    Overview-лентой и навигационными счётчиками (один запрос на тик),
    переключение фильтра мгновенно без loading-состояния; объём —
    десятки строк.
11. **Таблица алертов**: Severity (Badge §4.12) | Kind (моноширинно;
    при непустых `details` — Tooltip с построчным «k: v» — единственное
    место деталей: цифры порогов из каталога t06 §5.2 полезны, отдельная
    колонка раздувает таблицу) | Target (моноширинно, приглушённо) |
    Message (обычный текст) | Since (`formatUnixAge` — «с 3 мин»;
    абсолют `formatUnix` в Tooltip; `sinceUnix === null` → «—»).
    Пусто после фильтра: «все» → зелёная строка «Алертов нет»
    (спокойное состояние контрол-плейна); конкретный severity →
    приглушённое «Нет алертов этого уровня».
12. **Цветовая карта severity** — новый общий компонент
    `components/AlertSeverityBadge.tsx` (единый источник, паттерн
    `BucketStateBadge`): critical → red, warning → yellow, info →
    gray; текст — канон-строка (`critical`/`warning`/`info`) без
    перевода (идентификатор канона arch/03 §4; прецедент ленты t08),
    Tooltip не нужен. Применяется в: таблица Alerts, лента Overview
    (замена inline-условия t08 — устранение дублирования карты),
    навигационные счётчики используют цвета той же карты без
    компонента (числа, не строки).

### Overview: HA-карточка

13. **Карточка «HA»** (замена заглушки t08 §4.3): запрос `haScopes` §3
    с собственными состояниями по паттерну `AlertsFeedSection` t08
    (isPending → «Загрузка HA…»; isError → Alert red «Нет данных HA»
    + «Повторить»; ошибка не роняет остальные карточки Overview).
    Контент: строка счётчиков — `скопов: N` (приглушённо), `без
    лидера: K` (Badge/Badge-текст red при K > 0, серый при 0),
    `unmatched: U` (yellow при U > 0, серый при 0) — агрегация по
    `HaScopeSummaryDto[]`: `K = matched && leaderName === null`
    (только matched-скопы — согласовано с алертом `shard-no-leader`
    t06 §3.10, чтобы красный счётчик не расходился с лентой алертов),
    `U = matched === false`; ниже — компактная строка на каждый скоп
    (паттерн карточки «Кластеры» t08): имя-Anchor → `/ha/:scope`
    (моноширинно, truncate) + направо Badge: «нет лидера» red при
    null у matched (у unmatched — не рисуется: там жёлтый
    «unmatched»), жёлтый «unmatched» при false (оба — только при
    наличии). Пусто → «HA-scope'ы не найдены». Кластер скопа в
    карточке не показывается (влезает на карточку лишним шумом; он
    в таблице HA).
14. **Лента алертов Overview** — единственная правка t08-кода на
    странице: Badge severity → `AlertSeverityBadge` (§4.12). Прочий
    код OverviewPage (карточки/переезды/лента-логика) не трогается.

### Общие элементы

15. **Состояния страниц** — паттерн t08 §4.15 без изменений
    (`LoadingSection`/`ErrorSection`; 503-текст; тихий polling-сбой
    при данных; для `/ha/:scope` 404 → notFound-контент «Скоп не
    найден» + Anchor «← HA» — прецедент «Кластер не найден»).
16. **Форматтеры** (`utils/format.ts`, расширение; существующие не
    меняются): `formatIsoAge(iso: string | null): string` —
    относительный возраст от ISO-штампа `Date.now() − Date.parse(iso)`
    через `formatAge`, для `probeAtUtc` (DateTimeOffset-строка);
    null → «—». `formatBytes`/`formatUnix`/`formatUnixAge`/
    `formatIso` — переиспользуются как есть.
17. **Таблицы** — Mantine `Table` + `Table.ScrollContainer` (именно
    так называется компонент в Mantine 9 — заметка плана t08),
    `highlightOnHover`; полные строки без пагинации (объёмы —
    единицы скопов, ≤ десятков членов и алертов).

## 5. Макеты панелей (сводно)

- **HA**: `[Table: Scope | Кластер/шард (+unmatched Badge) | Лидер
  (null у matched → «нет лидера» red; у unmatched — приглушённо) |
  Члены healthy/total | Макс. лаг]`.
- **HA details**: `[Шапка: ← HA, scope, unmatched, кластер-ссылка,
  лидер, optime]` → `[Table members: Имя (+«лидер») | Адрес | Роль
  (Badge) | Состояние (Badge, Patroni-строка) | Timeline | Лаг |
  Проба (возраст/ошибка/«—»)]` → `[Accordion «Raw config» — закрыт,
  моноширинно]`.
- **Alerts**: `[SegmentedControl: все|critical|warning|info] [N из M]`
  → `[Table: Severity (Badge) | Kind (details в Tooltip) | Target |
  Message | Since («с 3 мин», абсолют в Tooltip)]`.
- **Навигация**: `Алерты [red N] [yellow M]` — Badge-числа справа от
  пункта, только при N/M > 0.
- **Overview**: карточка «HA» = `[скопов: N · без лидера: K ·
  unmatched: U]` + строки-ссылки на скопы с их бейджами.

Цвета: критичное — red, деградация — yellow, здоровое — teal,
мастер/лидер — violet (рифма с etcd-панелью t08), реплика/поток —
blue, нейтральное/info — gray.

## 6. Состав изменений (дерево файлов)

```
frontend/src/
├── App.tsx                                  [правка] маршрут ha/:scope
├── layout/
│   ├── AppLayout.tsx                        [правка] AlertsNavCounters в rightSection
│   │                                        пункта «Алерты» (§4.2)
│   └── AlertsNavCounters.tsx                [новый] счётчики critical/warning (§4.2)
├── pages/
│   ├── OverviewPage.tsx                     [правка] HA-карточка (§4.13) + AlertSeverityBadge
│   │                                        в ленте (§4.14)
│   ├── HaPage.tsx                           [замена заглушки] список скопов (§4.3)
│   ├── HaScopeDetailsPage.tsx               [новый] шапка + members + raw config (§4.4–4.9)
│   └── AlertsPage.tsx                       [замена заглушки] таблица + фильтр (§4.10–4.11)
├── components/
│   └── AlertSeverityBadge.tsx               [новый] карта severity (§4.12)
└── utils/format.ts                          [правка] +formatIsoAge (§4.16)
arch/03-panels.md                            [правка] §3 (§11, внесено до spec)
```

`api/dto.ts`, `api/queries.ts`, `api/client.ts`, PollingContext,
StaleBadge, PollingToggle, LoginPage, LoadState, BucketStateBadge,
EtcdPage, ClustersPage, ClusterDetailsPage + cluster-details/*,
весь бэкенд (`src/**`, включая Inspection/*), тесты .NET — без
изменений.

## 7. Фазы (укрупнённо; детализация — в plan)

1. **Общий слой**: `AlertSeverityBadge`, `formatIsoAge`, маршрут
   `/ha/:scope` — `npm run build` зелёный.
2. **AlertsNavCounters** в AppLayout — счётчики в навигации.
3. **AlertsPage** (таблица + фильтр + сортировка) — самая простая,
   прогон паттерна состояний.
4. **HaPage** (список) + **HaScopeDetailsPage** (шапка, members,
   raw config).
5. **OverviewPage** — HA-карточка + замена severity-бейджа ленты
   (последней: использует оба готовых визуальных словаря).
6. Сквозная проверка состояний (503/404/empty/off) и приёмка §10.

## 8. Данные/контракт (фиксация неизменности)

- HTTP-контракт не меняется: используются `GET /api/ha`,
  `GET /api/ha/{scope}` (200/404/503 — t06 §6.3) и `GET /api/alerts`
  без query-параметров (200/503; severity-фильтр — клиентский §4.10).
- `OverviewDto` не расширяется (решение t06 §3.19; зафиксировано в
  arch/03 §3 правкой §11.2): HA-сводка — агрегация `HaScopeSummaryDto[]`
  на клиенте.
- DTO фронта (`dto.ts`) — без правок: `HaScopeSummaryDto{scope,
  cluster, shard, matched, leaderName, membersTotal, membersHealthy,
  lagMaxBytes}`, `HaScopeDto{scope, cluster, shard, matched,
  leaderName, optimeLeader, members[], rawConfig}`,
  `HaMemberDto{name, host, port, role, state, timeline, lagBytes,
  probeAtUtc, probeError}`, `AlertDto{id, severity, kind, target,
  message, details, sinceUnix}` — сверены с C#-records t06.
- Семантика полей: `leaderName === null` — нет leader-ключа (алерт
  `shard-no-leader` critical, arch/03 §4); `matched === false` —
  «чужой service, норма, не алерт» (arch/02 §7); `optimeLeader` —
  LSN-число позиции репликации лидера; `probeAtUtc/probeError` —
  результат последнего тика проб (t06 §3.5: при ошибке лаг/timeline
  скрыты как null — фронт это отражает прочерками автоматически);
  `details` алерта — словарь строк каталога (t06 §5.2), может быть
  null.

## 9. Ограничения (что НЕ делается в t09)

- Операции/мутации (switchover, patronictl, подавление/mute/ack
  алертов) — никогда (arch/01 §9); все новые экраны только отображают.
- Бэкенд-правки — нет вообще: ни DTO, ни эндпоинтов, ни настроек;
  `?severity=`/`?kind=` остаются неиспользуемым контрактом.
- Фильтр по kind, поиск по тексту, пагинация, сортировки колонок,
  URL-синхронизация фильтра severity (диплинки) — нет (§4.10;
  прецедент t08 §4.14: URL — только идентификатор ресурса).
- История алертов/проб, «присутствует с» глубже `sinceUnix`,
  графики лагов — нет (arch/01 §9).
- Русификация severity/kind/state/role Patroni — нет (каноны и
  внешние идентификаторы показываются как есть, §2; исключение —
  role-подписи §4.6: ограниченный словарь Patroni-ролей).
- Pretty-print rawConfig, hex-вид LSN — нет (интерпретация сверх DTO).
- Новые npm/NuGet-пакеты — нет; Mantine `Accordion`/
  `SegmentedControl`/`Table.ScrollContainer` уже в `@mantine/core`.
- Правки EtcdPage/Clusters-страниц, StaleBadge, api-слоя — нет
  (§6); OverviewPage — ровно две правки §4.13–4.14.

## 10. Тесты и критерии приёмки

Фронтенд-тестов нет — прецедент t07 §3.5/t08 §9: типобезопасность
гарантирует `tsc --noEmit` (в `npm run build`), поведение — ручной
сценарий п.4. Бэкенд не меняется — тесты .NET служат регрессионным
барьером (зелёные до и после).

1. `cd frontend && npm ci && npm run build` — `tsc --noEmit` без
   ошибок, бандл собирается; `npm run typecheck` — чисто.
2. `dotnet build src/AdminPanel.slnx` — 0 warnings; `dotnet test` —
   все зелёные без правок тестов (изменений бэкенда нет).
3. HTTP-сценарий (ручной смоук по API-контракту, используемому
   панелью): на запущенном хосте после логина — `GET /api/ha` → 200
   список (или 503 до первого тика), `GET /api/ha/<неизвестный>` →
   404 ProblemDetails, `GET /api/alerts` → 200 (проверка что фронт
   не шлёт severity-параметров — фильтр клиентский).
4. Ручной сценарий `dotnet run --project src/AdminPanel.Api` +
   `npm run dev` (Development admin/admin):
   - без etcd: HA/Alerts/Overview показывают 503-состояние «Данные
     ещё не собраны» + «Повторить», HA-карточка Overview — своя
     ошибка без падения остальных карточек, панель жива;
   - со стендом `arch/04`/сидом (если поднят): HA-список — скопы с
     лидерами/членами/лагом, unmatched с жёлтой пометкой; клик по
     scope → детали: шапка, таблица members (роли/состояния/лаги/
     probe-возраст), «Raw config» закрыт, раскрывается; `/ha/nope` →
     «Скоп не найден» + «← HA»; навигация «HA» подсвечена на списке
     и деталях;
   - Alerts: таблица с severity-цветами, фильтр «все/critical/
     warning/info» мгновенно переключает строки, счётчик «N из M»
     корректен, since тикает при polling; details-числа видны в
     Tooltip у kind (при наличии);
   - навигация: при наличии critical/warning алертов у пункта
     «Алерты» — красный/жёлтый счётчики; пропадают при нулях; на
     Overview лента и навигация согласованы (один запрос в Network
     на тик — дедупликация ключа);
   - polling 2/5/15/off меняет частоту `/api/ha`+`/api/alerts`
     (Network), off останавливает; F5 сохраняет интервал;
   - Overview: HA-карточка показывает счётчики скопов/без лидера/
     unmatched, строки-ссылки ведут на детали HA.
5. `git status`: изменений ровно §6; wwwroot/node_modules не
   отслеживаются.
6. Пункт `t09-frontend-ha` удалён из `arch/roadmap/frontend.md`
   мерж-коммитом задачи (деливерабл roadmap; `arch/roadmap/stand.md`
   не трогается — его зависимости чистит владелец).

## 11. Правки arch/ (внесены в worktree до spec, arch-first)

`arch/03-panels.md` §3, две минимальные правки:

1. Строка **Overview**: сводка HA дополнена источником — «клиентская
   агрегация `GET /api/ha` — `OverviewDto` HA-полей не содержит» —
   фиксация в arch решения t06 §3.19 (которое иначе живёт только в
   docs/superpowers t06): читающий arch видит, что HA-сводка не
   требует правки overview-эндпоинта.
2. **Общие элементы**: добавлены счётчики critical/warning у пункта
   «Алерты» в навигации (клиентский подсчёт по `/api/alerts`,
   опрашиваемому с тем же polling-интервалом; скрыты при
   нуле/ошибке) — новый сквозной UI-элемент layout'а, заказанный
   roadmap («количество critical/warning — в навигации»).

Контракт §1/§2/§4 не меняется (бэкенд не трогается). Макетные
детали (колонки, цвета ролей/состояний Patroni, Tooltip-правила,
поведение фильтра) — реализация, в arch не поднимаются (источник —
этот spec, прецедент t07 §11, t08 §11).

## 12. Риски и заметки

- **Карты цветов role/state Patroni неполны по природе** (Patroni
  свободно расширяет state): неизвестные значения — серый Badge с
  исходной строкой (§4.7), ложных «ошибок» не будет; canonical
  список состояний не заказан — при необходимости расширим карту без
  контракта.
- **Навигационные счётчики держат alerts-запрос всегда активным**
  (layout монтируется на всех защищённых страницах): осознанно —
  ключ дедуплицируется (Overview/Alerts дают бесплатный прогрев),
  чтение из памяти снапшота дёшево; при polling off запросов нет.
- **404 деталей при исчезновении скопа между тиками** (etcd
  ключ ушёл): notFound-контент со ссылкой назад (§4.15) — то же
  поведение, что у кластеров t08; polling не роняет страницу.
- **`details` алерта может быть null/пустым** (правила без чисел):
  Tooltip просто не навешивается (§4.11) — без пустых поповеров.
- **`optimeLeader` как десятичное число** выглядит непривычно для
  LSN-глаз (hex-канон PG), но DTO — number и конвертация была бы
  интерпретацией; зафиксировано прочерком в решении (§4.4), при
  желании hex — отдельной минимальной задачей.
- **Mantine 9**: `Accordion`/`SegmentedControl` — стабильные
    компоненты `@mantine/core`; таблицы — только
  `Table.ScrollContainer` (не ScrollArea — заметка плана t08);
  при расхождении сигнатур — сверка с документацией в фазе кода
  (не контрактное).
- **Замена severity-бейджа в ленте Overview** — единственное
  прикосновение к t08-коду: визуально идентично (красный/жёлтый
  те же), риск регресса нулевой, устраняет дублирование карты
  цветов (§4.14).
- **Длина raw config** не ограничена (JSON Patroni-конфига ~сотни
  байт; патологически большой — редкость): `pre-wrap` +
  `break-all` держат верстку; виртуализация не заводится (YAGNI).
