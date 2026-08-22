# Спецификация t08-frontend-clusters — панели Overview, etcd, Clusters

Дата: 2026-08-23. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/frontend.md` (пункт `t08-frontend-clusters` ← t05, t07 —
оба слиты), `arch/01-architecture.md` §5 (стек, polling, guard, каркас
t07), `arch/03-panels.md` §1–3 (эндпоинты, DTO, панели — уточнён этой
задачей, см. §11), `arch/02-etcd-contract.md` §2.1–2.3 (семантика ключей
шардинга, стендовый топо-реестр, lease-семантика master-ключа).
Фактическое состояние кода: каркас t07 (`frontend/src`: api/dto.ts —
все DTO, api/queries.ts — все fetch-функции, PollingContext +
`usePollingIntervalMs`, AppLayout/StaleBadge/PollingToggle, страницы-
заглушки), инспекция t04–t06 (`InspectionModule`: overview/etcd/status/
clusters/ha/alerts + C#-DTO). Референс `../Puzzle` фронтенда не содержит
(подтверждено в t07 §шапка и перепроверено: `Puzzle/docs` — только
бэкенд-паттерны) — паттерны берутся из каркаса t07.

## 1. Цель

Наполнить три панели SPA реальными данными (замена заглушек t07):
**Overview** (дашборд: карточки etcd/кластеров, активные переезды, лента
алертов critical/warning, место под HA-сводку t09), **etcd** (endpoints
c меткой «активный», members/лидер, alarms, lastRefresh) и **Clusters**
(сводный список → страница деталей с вкладками Шарды / Бакеты / Переезды
/ Heals + блок «Стендовая топология»). Единственная содержательная
правка бэкенда — доставка `StandNodes` снапшота в детали кластера
(контрактная правка arch/03 §2 внесена до написания spec, см. §11).
Режим — строго read-only/polling через существующий `usePollingIntervalMs`.

Не входит (границы трека): панели HA и Alerts, HA-сводка в Overview,
детали HA-scope — t09; операции/мутации — никогда (arch/01 §9).

## 2. Принципы

- Источник истины — `arch/` (после правок §11); DTO фронта — строго
  фактические C#-DTO (сверка проведена: `OverviewQuery.cs`,
  `EtcdStatusQuery.cs`, `ClustersQuery.cs`, `ClusterDetailsQuery.cs`);
  слой `api/` t07 не перекраивается, `dto.ts` дополняется единственным
  полем `standNodes`.
- Каркас t07 не ломается: guard/401-редирект, polling-контекст,
  stale-бейдж, тема dark — без изменений (единственное исключение —
  подсветка навигации по префиксу, §4.2).
- Идентификаторы — английские; комментарии и все тексты UI — русские.
- Polling — только `refetchInterval: usePollingIntervalMs()` на каждом
  запросе страницы; одинаковые `queryKey` дедуплицируются TanStack
  (Overview и StaleBadge делят ключ `['overview']`).
- YAGNI: без URL-параметров фильтров, пагинации/виртуализации, тестов
  фронта (прецедент t07 §3.5), графиков, автоскролла; каждая позиция
  зафиксирована в §4 или §11.
- Отображение «как есть»: панель немая (arch/03 §3) — никаких форм,
  кроме логина; вычисления на клиенте — только форматирование и
  фильтрация, никакой интерпретации данных сверх DTO.

## 3. Данные (запросы страниц)

| Страница | Запросы (`queryKeys`) | refetchInterval |
|---|---|---|
| Overview | `overview` (`fetchOverview`), `alerts(undefined, undefined)` (`fetchAlerts`) | `usePollingIntervalMs()` |
| etcd | `etcdStatus` (`fetchEtcdStatus`) | он же |
| Clusters (список) | `clusters` (`fetchClusters`) | он же |
| Cluster details | `cluster(name)` (`fetchClusterDetails(name)` — без `owner`/`state`: грид фильтруется на клиенте, arch/03 §1) | он же |

Серверные `?owner=`/`?state=` остаются контрактом API, фронт их не
использует (N ≤ тысяч — клиентская фильтрация; решение §4.9). Запрос
`alerts` на Overview — без severity-фильтра, отбор critical/warning и
сортировка на клиенте (§4.4); ключ `['alerts', {severity: undefined,
kind: undefined}]` совместим с будущей Alerts-страницей t09 без фильтров.

## 4. Принятые решения (уточнения неоднозначностей; все — в рамках апрува «решать самому»)

### Маршруты и навигация

1. **Новый маршрут** `/clusters/:cluster` → `ClusterDetailsPage`
   (файл `pages/ClusterDetailsPage.tsx`; параметр `:cluster` — имя
   кластера как в API, без encode особых случаев — имя уже паттерна
   `[a-z0-9-]+`, `encodeURIComponent` применён в `fetchClusterDetails`).
   Существующие маршруты не меняются; маршрут добавляется в `App.tsx`
   рядом с `clusters`.
2. **Активность навигации по префиксу** (малая правка `AppLayout.tsx`):
   `active = item.to === '/' ? pathname === '/' : pathname.startsWith(item.to)`
   — иначе на `/clusters/demo` пункт «Кластеры» гаснет. Существующее
   поведение остальных пунктов не меняется.

### Overview

3. **Карточки** (SimpleGrid `{base: 1, sm: 2, lg: 4}`):
   - «etcd»: Badge «доступен»/«недоступен» (зелёный/красный),
     «endpoints: ok/total», при `reachable=false` — карточка с красной
     рамкой-оттенком; ссылка «Детали →» на `/etcd`. Числа alarms на
     карточке НЕТ — поля нет в `OverviewEtcdDto`, etcd-алерты видны в
     ленте ниже (противоречие arch/03 §2↔§3 устранено правкой §11);
   - «Кластеры»: если кластеров нет — текст «Кластеры не найдены»;
     иначе компактная строка на каждый кластер: имя-ссылка →
     `/clusters/:name`, `шарды S`, `бакеты B`, `переезды M`
     (M > 0 — жёлтым), `без мастера: K` красным только при K > 0;
   - «Алерты»: `alertsCritical` (красный) и `alertsWarning` (жёлтый)
     числами; нули — приглушённо; ссылка «Все алерты →» на `/alerts`
     (заглушка t09, страница откроется);
   - «HA»: placeholder «Сводка HA будет реализована в t09» (без
     запросов) — место под HA-сводку заложено, детали не реализуем
     (граница трека по roadmap).
4. **Лента алертов** — секция ниже карточек: запрос §3, на клиенте
   `filter(a => a.severity !== 'info')`, сортировка: critical раньше
   warning, внутри — по `sinceUnix` по убыванию (null — в конец).
   Отображение: список строк (Badge severity, `kind`, `target`,
   `message`, возраст «с N мин» — из `sinceUnix` пересчётом
   `Date.now()−sinceUnix×1000` на каждом рендере polling-тика).
   Показываются все critical/warning (не топ-N: их количество
   ограничено здравым смыслом контрол-плейна, обрезка скрывает суть
   дашборда); пусто → зелёная строка «Критических алертов нет».
5. **Активные переезды** — секция «Активные переезды» между карточками
   и лентой: таблица `activeMoves[]`: Кластер (ссылка) | Бакет |
   Состояние (Badge §7) | Маршрут `owner → target` («—» при null) |
   Обновлён (возраст от `updatedUnix`, абсолютное время в Tooltip).
   Пусто → «Активных переездов нет». Числа-счётчики из карточки
   «Кластеры» согласованы с этой таблицей (один DTO).

### Clusters

6. **Список** — Table: Кластер (Anchor → `/clusters/:name`) | БД
   (`dbName`, null → «—») | Бакеты (`bucketsCount`) | Шарды
   (`shardsWithMaster/shardsTotal`; если есть шард без мастера —
   значение красным + Badge «K без мастера») | Переезды
   (`activeMoves`, > 0 — жёлтым) | Пометки (Badge «incomplete»
   yellow-light при `incomplete=true`). Пустой список → «Кластеры не
   найдены». Сортировка — как отдаёт API (порядок снапшота), кнопок
   сортировки нет (YAGNI).

### Cluster details

7. **Шапка**: back-link «← Кластеры», `Title` имя кластера, Badge
   «incomplete» при `incomplete`, строка метаданных: БД, `bucketsCount`,
   «создан: <дата>» из `createdUnix` (null → «—»).
8. **Вкладки** — Mantine `Tabs`: «Шарды», «Бакеты», «Переезды»,
   «Heals». Выбор вкладки — локальный `useState` (default «Шарды»),
   без URL (§4.14). Ниже вкладок — блок «Стендовая топология» (§4.10).
9. **Вкладка «Бакеты»** — грид `buckets[]` (все, включая ACTIVE):
   - колонки: Id | Owner (null → «—», это «дыра» карты по arch/02 §2.1)
     | Состояние (Badge) | Переезд (`owner → target` при `move`, иначе
     «—») | Фаза (`phase`, Tooltip `lastError` при наличии) | Возраст
     (`ageSec` → `formatAge(ageSec×1000)`, только у не-ACTIVE, иначе «—»);
   - фильтры (Group над таблицей, локальные `useState`): `Select`
     состояние: «все» (default) / «не-ACTIVE» / ACTIVE / SYNCING /
     FROZEN / ABORTING; `Select` владелец: «все» (default) + уникальные
     `owner` из данных; счётчик «N из M» после фильтрации;
   - подсветка: строки с `state !== 'ACTIVE'` — фон строки
     `var(--mantine-color-yellow-light)` (лёгкий тёмно-тематический
     оттенок) + цветной Badge состояния; владельца null — `owner`
     красным (routing-дыра);
   - все строки без пагинации/виртуализации: N ≤ тысяч (arch/03 §1),
     рендер в `Table.ScrollArea` — приемлемо для админ-панели;
     перерисовка раз в polling-тик; если на реальном объёме проявится
     деградация — пагинация отдельной задачей (не контракт).
10. **Вкладка «Шарды»** — Table `shards[]`: Шард | DSN (hosts через
    запятую; полный `dsn` — Tooltip моноширинно) | Реплики
    (`replicasDeclared`, null → «—») | Мастер (`masterAddress`; null →
    красный Badge «нет мастера» — lease-семантика arch/02 §1; иначе
    адрес + зелёный Badge «lease») | Sync-standby (`runtime.
    standbiesSync`) | Лаг слотов (`slotsLagMaxBytes` → `formatBytes`)
    | WAL lost (`walStatusLost.length`, Tooltip со списком имён) |
    Подписки (`subscriptions.length`) | Схемы (`bucketSchemas.length`).
    `runtime === null` (пробы выключены/ещё не тикали) — прочерки во всех
    runtime-колонках + сноска под таблицей «Пробы отключены — runtime-
    данные отсутствуют» (arch/01 §8: SQL-поля скрыты с пометкой).
    `runtime.error` ненулевого шарда — красная строка ошибки под
    таблицей или Tooltip в колонке «Мастер» (решение: сноска-список
    «Ошибки проб: шард X: error»).
11. **Вкладка «Переезды»** — таблица только не-ACTIVE (`state !==
    'ACTIVE'`, тот же источник): Id | Состояние | Маршрут | Фаза |
    Начат (`startedUnix`) | Обновлён (`updatedUnix`, возраст рядом) |
    Возраст (`ageSec`) | Ошибка (`lastError`, усечён, полный — Tooltip).
    Пусто → «Активных переездов нет».
12. **Вкладка «Heals»** — журнал `heals[]` (уже `tsUnix` desc от API):
    Бакет | Было (`was`) | Стало (`now`) | Причина (`reason`) | Время
    (`tsUnix` → формат). null-поля → «—». Пусто → «Журнал пуст».
13. **Блок «Стендовая топология»** — `Card` ниже вкладок, рисуется
    только при `standNodes.length > 0` (в проде префикса нет — блок
    скрыт, arch/02 §2.3): Table Нода | Адрес (null → «есть ключ, адрес
    пуст»). Источник — новое поле `standNodes` DTO (§8).
14. **Фильтры/вкладки без URL**: состояние фильтров, выбранная вкладка,
    сортировки не синхронизируются с URL — URL содержит только
    идентификатор ресурса (`/clusters/:cluster`). Мотивация: панель
    read-only с polling, диплинки на состояние фильтра не заказаны;
    F5-потеря фильтра приемлема; меньше движущихся частей. Если позже
    понадобится — отдельной задачей.

### Общие элементы

15. **Состояния страниц** — единый паттерн:
    - первый загрузочный рендер (`isPending && !data`) — центрированный
      `Loader`;
    - ошибка без данных: `ApiError.status === 503` → «Данные ещё не
      собраны (etcd-снапшот пуст)» + Button «Повторить» (`refetch`);
      `status === 404` (только детали кластера) → «Кластер не найден» +
      ссылка «← Кластеры»; прочее → `error.message` + «Повторить»;
    - ошибка при наличии данных (polling-сбой) — тихо: показываем
      предыдущие данные (TanStack keeps data), верхний StaleBadge уже
      сигнализирует о проблеме; отдельный баннер не дублируем;
    - пустые наборы — тексты-заглушки по месту (§4.4–4.13).
16. **Форматтеры** (`utils/format.ts`, расширение; существующий
    `formatAge(ms)` не меняется):
    - `formatBytes(bytes: number | null): string` — «823 Б», «4.1 МБ»,
      «1.2 ГБ» (бинарные 1024, null → «—»);
    - `formatUnix(unix: number | null): string` — локальная дата-время
      `dd.MM.yyyy HH:mm:ss` (`Intl.DateTimeFormat('ru-RU')`, кэш
      форматтера в модуле); null → «—»;
    - `formatUnixAge(unix: number | null): string` — относительный
      возраст `formatAge(Date.now() − unix×1000)`; null → «—».
17. **Цветовая карта состояний** (единый источник — компонент
    `BucketStateBadge`): ACTIVE → teal «активен», SYNCING → blue
    «синхронизация», FROZEN → yellow «заморожен», ABORTING → red
    «отменяется»; `variant="light"`. Русские подписи; англ. канон
    значения — в Tooltip.
18. **Общие компоненты** — `frontend/src/components/`:
    - `LoadState.tsx`: `LoadingSection` (Loader по центру) и
      `ErrorSection({ error, onRetry, children404? })` — логика §4.15;
    - `BucketStateBadge.tsx`: Badge по `BucketStateName` (карта §4.17);
    - `NilText` не заводится — «—» inline (простота).
    Вкладки деталей кластера — отдельные презентационные компоненты в
    `frontend/src/pages/cluster-details/` (`ShardsTab`, `BucketsTab`,
    `MovesTab`, `HealsTab`, `StandNodesBlock`): получают данные
    пропсами, запрос одна в `ClusterDetailsPage` (изоляция: смена
    макета вкладки не трогает слой запросов).

## 5. Макеты панелей (сводно; детализация — §4)

- **Overview**: [Карточка etcd | Карточка Кластеры | Карточка Алерты |
  Карточка HA(t09)] → [Активные переезды: таблица] → [Лента алертов:
  critical/warning].
- **etcd**: [Alert «нет кворума» при `quorumSuspected`] → [Endpoints:
  URL | Статус | Задержка | Версия | raft term | Размер БД | Ошибки |
  «активный»] → [Members: ID | Имя | peer URLs | client URLs | «лидер»]
  → [Alarms: member | тип] → подпись «Обновлено: <lastRefreshUtc>».
- **Clusters**: [Table сводки: Кластер | БД | Бакеты | Шарды | Переезды
  | Пометки].
- **Cluster details**: [Шапка: имя, incomplete, метаданные] → [Tabs:
  Шарды | Бакеты | Переезды | Heals] → [Стендовая топология, если есть].

Колонка «Ошибки» в endpoints: `errors[]` пусто → «—», иначе красный
Badge `N` с Tooltip-списком строк. «Задержка» — `latencyMs` с «мс»,
null → «—». «Статус» — Badge ok/down (зелёный/красный).

## 6. Состав изменений (дерево файлов)

```
frontend/src/
├── App.tsx                                  [правка] маршрут clusters/:cluster
├── api/dto.ts                               [правка] ClusterDto.standNodes + StandNodeDto
├── layout/AppLayout.tsx                     [правка] active по префиксу (§4.2)
├── pages/
│   ├── OverviewPage.tsx                     [замена заглушки]
│   ├── EtcdPage.tsx                         [замена заглушки]
│   ├── ClustersPage.tsx                     [замена заглушки: список]
│   ├── ClusterDetailsPage.tsx               [новый] запрос + шапка + вкладки
│   └── cluster-details/
│       ├── ShardsTab.tsx                    [новый]
│       ├── BucketsTab.tsx                   [новый] грид + фильтры + подсветка
│       ├── MovesTab.tsx                     [новый]
│       ├── HealsTab.tsx                     [новый]
│       └── StandNodesBlock.tsx              [новый]
├── components/
│   ├── LoadState.tsx                        [новый] Loading/ErrorSection (§4.15)
│   └── BucketStateBadge.tsx                 [новый] карта состояний (§4.17)
└── utils/format.ts                          [правка] +formatBytes, formatUnix,
                                                     formatUnixAge
src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs
                                             [правка] StandNodeDto, поле
                                             ClusterDto.StandNodes, маппер (§8)
src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs
                                             [правка] сигнатура Map + кейс standNodes
src/tests/AdminPanel.IntegrationTests/
├── InspectionApiTests.cs (InspectionSnapshots.Clustered)
│                                            [правка] фикстура + StandNodes
└── ClustersApiTests.cs                      [правка] ассерт standNodes в деталях
arch/03-panels.md                            [правка] §2/§3 (§11, внесено до spec)
```

HaPage/AlertsPage, api/queries.ts, api/client.ts, PollingContext,
StaleBadge, LoginPage, Program.cs, прочие Inspection-файлы — без
изменений.

## 7. Фазы (укрупнённо; детализация — в plan)

1. **Бэкенд-дельта** (§8): `StandNodeDto` + `ClusterDto.StandNodes` +
   маппер/хендлер; обновить юнит- и API-тесты; `dotnet build/test` зелёные.
2. **Общий слой фронта**: `dto.ts` (standNodes), форматтеры,
   `LoadState`, `BucketStateBadge`, маршрут `clusters/:cluster`,
   active-по-префиксу; `npm run build` зелёный.
3. **EtcdPage** (самая простая таблица — прогон паттерна §4.15).
4. **ClustersPage** (список) + **ClusterDetailsPage** (шапка + вкладки
   Шарды/Бакеты/Переезды/Heals + StandNodesBlock).
5. **OverviewPage** (карточки, переезды, лента) — последней: использует
   оба уже готовых визуальных словаря.
6. Сквозная проверка состояний (503/404/empty/off) и приёмка §14.

## 8. Бэкенд-дельта: standNodes (единственная правка контракта)

Проблема: блок «Стендовая топология» заказан roadmap'ом на странице
кластера, arch/02 §2.3 кладёт `StandNodes` в снапшот, но ни один DTO
t04–t06 их не отдаёт (t05 §11 сознательно отложил: «блок фронта над
StandNodes снапшота; API не расширяется» — без расширения фронту данные
взять неоткуда). Решение (arch-first, §11 уже внесён):

- `arch/03 §2`: `ClusterDto` += `standNodes[{name,address}]` — поле
  глобального топо-реестра снапшота, одинаково во всех ответах деталей
  (обычно пусто; в проде пусто всегда). Альтернативы отклонены:
  `OverviewDto.standNodes` — скрытая зависимость страницы кластера от
  чужого запроса и семантически «сводка не о кластере»; отдельный
  эндпоинт `/api/stand/nodes` — новый эндпоинт ради ≤4 строк данных.
  Дублирование по кластерам тривиально (кластеров единицы, нод ≤4).
- `ClusterDetailsQuery.cs`: `record StandNodeDto(string Name, string? Address)`;
  `ClusterDto(…, IReadOnlyList<StandNodeDto> StandNodes)`;
  `ClusterDetailsMapper.Map(cluster, nowUnix, owner, state, standNodes)` —
  последний параметр `IReadOnlyList<StandNode>`; хендлер передаёт
  `snapshot.StandNodes`. Сериализация camelCase — как у всех полей.
- `/api/clusters` (сводка), прочие эндпоинты — без изменений.

## 9. Тесты

- **Юнит** (`ClustersMappersTests`): обновить вызовы `Map` на новую
  сигнатуру (пустой список нод); новый кейс: `standNodes` фикстуры →
  `dto.StandNodes` (name/address, null-адрес маппится).
- **Интеграция** (`ClustersApiTests` + фикстура `InspectionSnapshots.
  Clustered`): добавить в снапшот 2 `StandNode` (один с null-адресом);
  в `ClusterDetails_ReturnsConfigShardsBucketsHeals` — ассерты
  `standNodes` (длина, name, address, null-адрес). Прочие API-тесты не
  меняются (поле аддитивное).
- **Фронтенд-тестов нет** — прецедент t07 §3.5: приёмка ручным
  сценарием §14 + `tsc --noEmit` в `npm run build`.

## 10. Ограничения (что НЕ делается в t08)

- Панели HA и Alerts (в т.ч. сводка HA в Overview — карточка-заглушка),
  детали HA-scope — t09; `HaPage`/`AlertsPage` остаются заглушками t07.
- Операции/мутации (move/abort/heal кнопки) — никогда (arch/01 §9);
  панели только отображают.
- URL-синхронизация фильтров/вкладок, пагинация, сортировки колонок,
  текстовый поиск по бакетам, экспорт CSV — нет (§4.9, §4.14).
- Виртуализация грида бакетов, `React.memo`-оптимизации — нет;
  все строки (§4.9).
- Топ-N/обрезка ленты алертов, mute/ack, история — нет (§4.4).
- Новые npm/NuGet-пакеты — нет; используются уже подключённые
  Mantine/TanStack/ReactRouter.
- Правки `arch/01`, `arch/02`, `arch/04`, `InspectionModule.cs`,
  `Program.cs`, auth — нет.

## 11. Правки arch/ (внесены в worktree до spec, arch-first)

`arch/03-panels.md`, три минимальные правки:

1. §2 `ClusterDto` += `standNodes[{name,address}]` с пометкой
   «глобальный топо-реестр 02 §2.3, обычно пусто, UI-блок при наличии» —
   отражение в коде §8 (контрактное; отражается в C#-DTO и `dto.ts`).
2. §3 Overview-строка: из карточки etcd убрано упоминание alarms —
   поля нет в `OverviewEtcdDto` (§2), алерты etcd видны в ленте и на
   панели etcd. Устранение внутреннего противоречия §2↔§3, DTO-канон
   (§2) выигрывает.
3. §3 Cluster details: «Стендовая топология» — источник уточнён как
   поле `standNodes` DTO (реестр `/cluster/nodes/`), блок скрыт при
   пустом.

Макетные детали (колонки/фильтры/цвета) — реализация, в arch не
поднимаются (источник — этот spec, прецедент t07 §11).

## 12. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t08-frontend-clusters` из
`arch/roadmap/frontend.md` (правила `arch/roadmap/README.md`). Строку
`t09-frontend-ha` (зависимости `← t06, t07`) не трогать — прецедент
t07 §12.

## 13. Критерии приёмки

1. `cd frontend && npm ci && npm run build` — `tsc --noEmit` без ошибок,
   бандл собирается; `npm run typecheck` — чисто.
2. `dotnet build src/AdminPanel.slnx` — 0 warnings; `dotnet test` — все
   зелёные, включая обновлённые §9 (Docker — как обычно для integration).
3. `curl /api/clusters/{c}` на фикстуре стенда возвращает `standNodes`
   (camelCase) — смоук интеграционными тестами §9; ручной curl —
   опционально (на усмотрение исполнителя, e2e-стенд — t10).
4. Ручной сценарий `dotnet run --project src/AdminPanel.Api` +
   `npm run dev` (или собранный бандл), Development admin/admin:
   - без etcd: все три панели показывают 503-состояние «Данные ещё не
     собраны» + «Повторить», панель не падает, StaleBadge красный;
   - со стендом `arch/04` (если поднят): Overview — карточки с числами,
     переезды/лента; etcd — endpoints с меткой «активный», лидер в
     members, lastRefresh; Clusters → детали: 4 вкладки с данными
     стенда, фильтры Бакетов работают (state + owner + «не-ACTIVE»),
     подсветка не-ACTIVE строк, возраст ticking при polling;
     «Стендовая топология» видна (стенд сеет `/cluster/nodes/`);
   - `/clusters/unknown` → «Кластер не найден» + ссылка назад;
     навигация «Кластеры» подсвечена и на списке, и на деталях;
   - polling 2/5/15/off меняет частоту запросов всех открытых страниц
     (Network), `off` останавливает; F5 сохраняет интервал.
5. `git status`: новых артефактов нет (wwwroot/node_modules игнорируются);
   изменения — ровно §6.

## 14. Риски и заметки

- **Грид бакетов на тысячи строк**: полный ререндер каждые 5 с; на
  стенде (16 бакетов) и реальных объёмах (сотни–едины тысяч) приемлемо;
  фикс-порог: если профилирование покажет лаги — пагинация отдельной
  задачей (не контракта).
- **`Intl.DateTimeFormat` в SSR нет** — SPA-only, безопасно; форматтер
  кэшируется на уровне модуля.
- **Mantine 9 API** (`Tabs`, `SimpleGrid`, `Table.ScrollArea`,
  `SegmentedControl`) стабилен; при расхождении сигнатур — сверка с
  документацией в фазе кода (не контрактное).
- **`startsWith`-активность навигации**: `/clusters` и `/clusters/x` —
  единственные пары префикса; `/` обрабатывается отдельно (§4.2),
  взаимных коллизий нет.
- **Поле `standNodes` в каждом ответе деталей**: дублирование по
  кластерам сознательно (§8); при будущей множественности кластеров
  объём остаётся копеечным.
- **Лента алертов грузит все алерты** (включая info) и фильтрует на
  клиенте: один запрос вместо двух (critical+warning), объём — десятки
  строк; info выпадает только на Overview — Alerts-страница t09 покажет
  всё.
