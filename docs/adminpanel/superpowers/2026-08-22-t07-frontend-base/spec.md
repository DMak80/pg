# Спецификация t07-frontend-base — каркас SPA-фронта AdminPanel

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины:
`arch/roadmap/frontend.md` (пункт `t07-frontend-base`),
`arch/01-architecture.md` §2 (проект `frontend/`), §4 (auth: cookie, статика
без авторизации), §5 (фронтенд: стек, сборка, polling, guard, раздача SPA —
уточнён этой задачей), §7 (сборка и запуск), `arch/03-panels.md` §1
(эндпоинты), §2 (DTO), §3 (панели, общие элементы). Фактическое состояние
кода: auth-модуль t02 (`AuthModule` с default-deny guard `/api/*`,
cookie `adminpanel_session`, чистые 401 без redirect), инспекция t04–t06
(`InspectionModule`: overview/etcd/status/clusters/alerts/ha + DTO),
`Program.cs` без StaticFiles/wwwroot, Kestrel dev-профиль
`http://localhost:5000` (`launchSettings.json`). Референс `../Puzzle`
фронтенда не содержит (проверено: `Puzzle/docs/*` — только бэкенд-паттерны;
`Puzzle/arch/` — bus) — каркас проектируется по arch-документам.

Версии npm-пакетов выверены по registry 2026-08-22 (§6.1).

## 1. Цель

Каркас SPA: каталог `frontend/` (Vite + React + TypeScript, Mantine, React
Router, TanStack Query) с production-сборкой в `src/AdminPanel.Api/wwwroot`,
dev-сервером с proxy `/api` на Kestrel, layout'ом с навигацией и
переключателем polling-интервала (2/5/15 c/off, default 5 c), страницей
Login, guard'ом сессии по `GET /api/auth/me` (401 → редирект на `/login`),
страницами-заглушками остальных панелей и общим API-клиентом (типизированные
DTO + обработка stale-бейджа по `snapshotAgeMs`). На стороне хоста — раздача
SPA: `UseDefaultFiles`/`UseStaticFiles` без авторизации, SPA-fallback на
`index.html` (кроме неизвестных `/api/*` → 404), предупреждение при
несобранном бандле. Проверка roadmap: `npm run build` + `dotnet run` отдаёт
SPA и login работает.

Задача — только каркас: наполнение панелей — t08 (Overview/etcd/Clusters) и
t09 (HA/Alerts); детали кластера/scope, e2e-стенд, Dockerfile — не входят.

Новых NuGet-пакетов нет (StaticFiles — shared framework ASP.NET Core);
npm-пакеты — §6.1.

## 2. Принципы

- Источник истины — `arch/`; всё, что arch не оговаривает, решено минимальным
  способом и зафиксировано в §3. Расхождение с arch запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде и UI-тексты — русские
  (панель русскоязычная — прецедент t04 про `message` алертов).
- Бэкенд-паттерны t01–t06 не ломаются: модульная композиция `Program.cs`,
  default-deny guard `/api/*` без ослаблений, чистые 401/404 без redirect.
- Фронтенд — `strict` TypeScript, сборка без ошибок (`tsc --noEmit` входит в
  `npm run build`); каркас обязан быть готов к наполнению t08/t09 без правок
  своих файлов (страницы и API-функции добавляются рядом).
- YAGNI: без ESLint/Prettier, vitest, MSW, i18n, переключателя темы, SSR
  (обоснования — §3, §11).
- В git — исходники и `package-lock.json`; `node_modules` и бандл `wwwroot` —
  артефакты, не коммитятся (§3.17–3.18).

## 3. Принятые решения (уточнения неоднозначностей arch/)

### Инструменты и экосистема

1. **Точные версии** — последние стабильные на 2026-08-22 из
   `registry.npmjs.org`: react/react-dom 19.2.8, react-router 8.3.0,
   @tanstack/react-query 5.101.4, @mantine/core+hooks 9.5.2, vite 8.2.1,
   @vitejs/plugin-react 6.0.5, typescript 7.0.2, @types/react 19.2.18,
   @types/react-dom 19.2.4 (полный §6.1). Диапазоны `^`/`~` в
   `package.json` + коммит `package-lock.json` — точная фиксация. Vite 8
   требует node `^20.19.0 || >=22.12.0` — рабочая машина node v26.7.0
   подходит; фиксирую `engines.node >=22.12`.
2. **TypeScript 7.0.2**: беру актуальный latest (мажор уже стабильный, не
   rc). Риск совместимости с конфигом шаблона Vite низкий (CLI/конфиг
   совместимы), но зафиксирован fallback: при ошибках `tsc --noEmit` на
   фазе плана/кода — откат на `typescript@~5.9` (последняя 5.9.x = 5.9.3)
   с записью в рисках плана. Решение о статусе TS фиксируется в plan.
3. **`frontend/.npmrc` с `registry=https://registry.npmjs.org`** (единственная
   строка): пользовательский npm на этой машине указывает на корпоративный
   Artifactory `artifactory.s.o3.ru`, недоступный извне этой сети (ENOTFOUND
   при `npm view`). Проектный `.npmrc` перекрывает только default registry
   (scoped-registry'и пользователя не трогает), все пакеты проекта публичные
   — установка становится детерминированной на любой машине.
4. **ESLint/Prettier не вводятся**: минимум «типобезопасность и сборка без
   ошибок» достигается `tsc --noEmit` (входит в `npm run build`) +
   `noUnusedLocals`/`noUnusedParameters`. Объём кода каркаса мал, CI нет;
   линтер имеет смысл вводить вместе с реальными страницами t08/t09, если
   понадобится.
5. **Фронтенд-тестов (vitest/RTL) в t07 нет**: roadmap-проверка — сборка +
   ручной сценарий; логика каркаса (guard-редирект, polling-переключатель)
  покрывается ручным приёмочным сценарием §14.5. Автотесты на SPA — не
   обязательство трека (t10 — e2e стенда curl-скриптами по API).

### Архитектура каркаса

6. **Структура каталогов** `frontend/src` (детально §7):

   ```
   src/
   ├── main.tsx            провайдеры (Mantine, QueryClient, Router)
   ├── App.tsx             маршруты (createBrowserRouter)
   ├── api/                client.ts (fetch-обёртка), dto.ts (типы), queries.ts
   ├── polling/            PollingContext.tsx (интервал 2/5/15/off + localStorage)
   ├── layout/             AppLayout.tsx (guard+AppShell), PollingToggle.tsx, StaleBadge.tsx
   ├── auth/               LoginPage.tsx
   ├── pages/              Overview/Etcd/Clusters/Ha/Alerts — заглушки
   └── utils/format.ts     formatAge(snapshotAgeMs) для бейджа
   ```

   Слои: `api` не знает React; `layout`/`auth`/`pages` используют `api` и
   `polling`; t08/t09 добавляют страницы в `pages/` и запросы в `queries.ts`
   не трогая каркас.
7. **Роутинг — React Router 8, data-режим** (`createBrowserRouter` +
   `RouterProvider`): маршруты `/login` (без layout) и `/` с `AppLayout`
   как root-элементом защищённой зоны: index → OverviewPage, `/etcd`,
   `/clusters`, `/ha`, `/alerts` — заглушки. Unknown-путь → `<Navigate
   to="/">`. Вложенные маршруты деталей (`/clusters/:cluster`, `/ha/:scope`)
   добавят t08/t09 — маршрутные заглушки под них в t07 не создаются.
8. **Mantine — тёмная тема по умолчанию** (`MantineProvider
   defaultColorScheme="dark"`), CSS-слой без emotion: один импорт
   `@mantine/core/styles.css` в `main.tsx`. Переключателя темы нет (arch/03
   §3 — просто «тёмная тема»). Layout — `AppShell` (navbar: навигация;
   header: stale-бейдж, polling-переключатель, username, logout).
9. **Polling-интервал — React Context + localStorage**: тип
   `PollingInterval = '2' | '5' | '15' | 'off'`, default `'5'`, ключ
   localStorage `adminpanel.pollingInterval` (невалидное значение → default).
   Хук `usePollingIntervalMs(): number | false` (`'off'` → `false`) — единая
   точка применения в `refetchInterval` (TanStack Query принимает `false`).
   В t07 интервал реально применяется к overview-запросу stale-бейджа (§3.12);
   страницы t08/t09 подключают его к своим запросам без правок контекста.
10. **TanStack Query defaults** (в `main.tsx`): `retry: (count, err) =>
    !(err instanceof ApiError && err.status === 401) && count < 2` (401 не
    ретраится — сразу guard-реакция), `refetchOnWindowFocus: false`
    (обновление — только polling, arch/01 §5). Ключи запросов — константы
    `queryKeys` в `queries.ts`.

### Guard и сессия

11. **Guard — два уровня, один механизм редиректа**:
    - редирект выполняет fetch-обёртка `apiFetch` (§3.13): любой ответ 401,
      если текущий путь ≠ `/login`, → `window.location.replace('/login?from='
      + encodeURIComponent(path + search))`. Полная перезагрузка SPA при
      истечении сессии допустима (админ-панель, состояние UI не критично);
      параметр `from` возвращает на исходную страницу после логина.
      Исключение `/login` — иначе 401 от неверных кредов редиректил бы сам
      на себя;
    - проверка сессии при входе: `AppLayout` (монтируется на всех
      защищённых маршрутах) делает `useQuery(queryKeys.session, fetchSession,
      { retry: false, staleTime: Infinity })`: pending → полноэкранная
      загрузка; 401 → редирект уже из обёртки; ошибка сети (не-401) →
      сообщение «Панель недоступна» с кнопкой повторить (refetch); успех →
      `AppShell` + `Outlet`. username из этого же запроса рисуется в шапке.
    Альтернатива «guard в router loader» отклонена: дублирование механизма
    401-обработки (loader + обёртка) и расхождение состояний; компонентная
    схема использует единственный источник — кэш `['session']`.
12. **Stale-бейдж по `snapshotAgeMs`** (в шапке): `StaleBadge` —
    `useQuery(queryKeys.overview, fetchOverview, { refetchInterval:
    usePollingIntervalMs() })`: успех → серый Badge `данные: <formatAge(
    snapshotAgeMs)>`; `stale === true` → жёлтый `stale: <age>`; ошибка
    (503 «снапшот не собран», сеть) → красный `нет данных`; 401 → редирект
    обёртки. Бейдж всегда виден — это также наглядная демонстрация работы
    переключателя polling в t07.
13. **API-клиент** — `api/client.ts`, единственная точка HTTP:

    ```ts
    // Ошибка API: HTTP-статус + разобранные ProblemDetails.
    export class ApiError extends Error {
      constructor(readonly status: number, readonly title?: string,
                  readonly detail?: string, readonly retryAfterSeconds?: number)
    }
    // GET/POST JSON; relative-пути от корня (/api/...); cookie — same-origin
    // (dev — через vite proxy, prod — same-origin); 204 → undefined.
    export async function apiFetch<T>(path: string, init?: { method?: 'POST';
      body?: unknown }): Promise<T>
    ```

    Поведение: `credentials: 'same-origin'`, `Accept: application/json`;
    POST-тело сериализуется JSON'ом; 2xx → `res.json()` (кроме 204 →
    `undefined`); не-2xx → попытка разобрать ProblemDetails (`title`,
    `detail`) и `Retry-After` (секунды → `retryAfterSeconds`, для 429
    логина), всегда бросается `ApiError`; 401 → редирект §3.11 (до броска,
    если путь ≠ `/login`). Никакого базового URL/env — путь всегда
    относительный: dev `/api` проксируется Vite, prod — same-origin.
14. **Типы DTO** — `api/dto.ts`, camelCase-интерфейсы по фактическим DTO
    t04–t06 (не по абстрактной схеме arch/03 §2 — она совпадает, но источник
    точных полей — код): `OverviewDto` (alertsCritical, alertsWarning, etcd
    {reachable, endpointsOk, endpointsTotal}, clusters[], activeMoves[],
    snapshotAgeMs, stale), `EtcdStatusDto` (+ endpoints/members/alarms),
    `ClusterSummaryDto` (именно сводка — так называется `ClusterDto` в arch/03
    §2, реальный тип `GET /api/clusters`), `ClusterDto`/`ShardDto`/
    `ShardRuntimeDto`/`BucketDto`/`MoveDto`/`HealDto` (детали),
    `HaScopeSummaryDto`/`HaScopeDto`/`HaMemberDto`, `AlertDto`,
    `SessionDto { username }`. Nullable-поля C# (`string?`, `long?`,
    `DateTimeOffset?`) → `| null`; unix-времена → `number | null`;
    `DateTimeOffset` → `string`. Полный список — §7.3.
15. **Query-функции** — `api/queries.ts`: `queryKeys` (session, overview,
    etcdStatus, clusters, cluster(name), haScopes, haScope(scope),
    alerts(filters)) и fetch-обёртки над `apiFetch` для всех эндпоинтов
    arch/03 §1 (в t07 используются session/login/logout/overview; остальные
    готовы для t08/t09 — каркас отдаёт их сразу, чтобы следующие задачи не
    трогали слой `api`).

### Хост: Program.cs

16. **Backend-правка** (единственная содержательная — Program.cs, §8):
    после `builder.Build()` и warning-блока, до auth:
    `app.UseDefaultFiles(); app.UseStaticFiles();` — статика SPA без
    авторизации (arch/01 §4: в бандле секретов нет). Если
    `Directory.Exists(app.Environment.WebRootPath)` — false, один
    `LogWarning` «SPA-бандл не собран (cd frontend && npm run build) —
    отдаётся только API» (WebRootPath — вычисляемый путь, каталога может не
    быть; хост не падает). В конце пайплайна:

    ```csharp
    // Неизвестные /api/* — 404 ProblemDetails, а не SPA-fallback (arch/01 §5).
    app.MapFallback("/api/{**_}", () => Results.Problem(
        statusCode: StatusCodes.Status404NotFound, title: "Not found"));
    // SPA-fallback: неизвестные пути отдают index.html (роутинг на клиенте).
    app.MapFallbackToFile("index.html");
    ```

    Специфичный `/api`-fallback выигрывает у файлового (правило приоритета
    fallback-эндпоинтов), а default-deny guard возвращает 401 до любого
    fallback'а — API-семантика не меняется (т02 §3.8: guard middleware стоит
    раньше эндпоинтов).

### Git и артефакты

17. **`.gitignore`** — добавить `src/AdminPanel.Api/wwwroot/` (бандл —
    артефакт сборки; поставка — Dockerfile t11, в репо бандла нет).
    `node_modules/` и `dist/` уже покрыты глобальными правилами корневого
    `.gitignore` (проверено).
18. **Коммитится**: весь `frontend/` кроме `node_modules` (включая
    `package-lock.json`, `.npmrc`, tsconfig'и, `vite.config.ts`); правка
    `Program.cs`; `.gitignore`; integration-тесты §10; правки arch/ §12.
19. **Проверка «dotnet run отдаёт SPA»** — сценарий §14.4: `npm run build`
    кладёт `wwwroot/index.html` + `assets/`, `dotnet run --project
    src/AdminPanel.Api` → `curl /` 200 `text/html`, unknown-путь → 200
    `index.html`, `/api/unknown` без cookie → 401, с cookie → 404, браузером
    — полный login-флоу.

## 4. Контракт (фиксируется для t08/t09)

### 4.1. Маршруты SPA

| Путь | Компонент | Доступ |
|---|---|---|
| `/login` | `LoginPage` (без AppLayout) | открытый |
| `/` | `AppLayout` → `OverviewPage` (заглушка) | guard |
| `/etcd` | `AppLayout` → `EtcdPage` (заглушка) | guard |
| `/clusters` | `AppLayout` → `ClustersPage` (заглушка) | guard |
| `/ha` | `AppLayout` → `HaPage` (заглушка) | guard |
| `/alerts` | `AppLayout` → `AlertsPage` (заглушка) | guard |
| прочие | `<Navigate to="/">` | — |

### 4.2. API-поведение фронта

- Все запросы — `apiFetch` с относительными путями; авторизация — только
  cookie (same-origin); никаких токенов/заголовков.
- 401 от любого эндпоинта (кроме запросов со страницы `/login`) →
  `/login?from=<path+search>`; после успешного логина — возврат по `from`.
- `POST /api/auth/login`: 204 → сессия в кэш `['session']` + navigate;
  401 → форма «Неверный логин или пароль»; 429 → форма «Слишком много
  попыток, подождите <retryAfterSeconds> с».
- `POST /api/auth/logout`: 204 → очистка кэша (`queryClient.clear()`) +
  `/login`.
- Polling: только `refetchInterval` (2/5/15 c/off, default 5 c, persist в
  localStorage `adminpanel.pollingInterval`); `refetchOnWindowFocus`
  отключён глобально.

## 5. Состав изменений (дерево файлов)

```
frontend/                                   [новый каталог]
├── .npmrc                                  registry=https://registry.npmjs.org
├── index.html                              lang=ru, title AdminPanel, #root, /src/main.tsx
├── package.json                            §6.2
├── package-lock.json                       генерируется npm install; коммитится
├── tsconfig.json                           solution-references (app + node)
├── tsconfig.app.json                       код src/ (strict, noEmit)
├── tsconfig.node.json                      vite.config.ts
├── vite.config.ts                          outDir→wwwroot, proxy /api→:5000
└── src/
    ├── main.tsx                            MantineProvider(dark) + styles.css +
    │                                       QueryClientProvider + PollingProvider +
    │                                       RouterProvider
    ├── App.tsx                             createBrowserRouter (§4.1)
    ├── api/
    │   ├── client.ts                       ApiError + apiFetch (§3.13)
    │   ├── dto.ts                          типы DTO (§7.3)
    │   └── queries.ts                      queryKeys + fetch-функции (§3.15)
    ├── polling/PollingContext.tsx          PollingInterval + localStorage + usePollingIntervalMs
    ├── layout/
    │   ├── AppLayout.tsx                   guard (session-query) + AppShell + Outlet
    │   ├── PollingToggle.tsx               SegmentedControl 2 c/5 c/15 c/off
    │   └── StaleBadge.tsx                  overview-polling → Badge (§3.12)
    ├── auth/LoginPage.tsx                  форма логина (§7.6)
    ├── pages/
    │   ├── OverviewPage.tsx                заглушка «t08»
    │   ├── EtcdPage.tsx                    заглушка «t08»
    │   ├── ClustersPage.tsx                заглушка «t08»
    │   ├── HaPage.tsx                      заглушка «t09»
    │   └── AlertsPage.tsx                  заглушка «t09»
    └── utils/format.ts                     formatAge(ms): «12 с», «3 мин 5 с»
src/AdminPanel.Api/Program.cs               [правка] §8: UseDefaultFiles/UseStaticFiles +
│                                           wwwroot-warning + /api-fallback 404 + SPA-fallback
src/tests/AdminPanel.IntegrationTests/
└── SpaHostingTests.cs                      [новый] §10
.gitignore                                  [правка] + src/AdminPanel.Api/wwwroot/
arch/01-architecture.md                     [правка] §5 (этой задачей, см. §12)
arch/03-panels.md                           [правка] §3 (этой задачей, см. §12)
```

`AdminPanel.slnx`, csproj-файлы, `Directory.Packages.props`,
`Directory.Build.props`, auth/inspection-код — без изменений.

## 6. Конфигурация frontend/

### 6.1. package.json (зависимости и скрипты)

```json
{
  "name": "adminpanel-frontend",
  "private": true,
  "type": "module",
  "engines": { "node": ">=22.12" },
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit -p tsconfig.app.json && tsc --noEmit -p tsconfig.node.json && vite build",
    "typecheck": "tsc --noEmit -p tsconfig.app.json && tsc --noEmit -p tsconfig.node.json"
  },
  "dependencies": {
    "@mantine/core": "^9.5.2",
    "@mantine/hooks": "^9.5.2",
    "@tanstack/react-query": "^5.101.4",
    "react": "^19.2.8",
    "react-dom": "^19.2.8",
    "react-router": "^8.3.0"
  },
  "devDependencies": {
    "@types/react": "^19.2.18",
    "@types/react-dom": "^19.2.4",
    "@vitejs/plugin-react": "^6.0.5",
    "typescript": "~7.0.2",
    "vite": "^8.2.1"
  }
}
```

Версии — последние стабильные registry.npmjs.org на 2026-08-22 (запрос
`npm view <pkg> version --registry=…`); peer-совместимость проверена:
@mantine/core 9.5.2 → react ^19.2.0; vite 8.2.1 → node ^20.19 || >=22.12.
Fallback TS7 → `~5.9` (5.9.3) — §3.2.

### 6.2. vite.config.ts

```ts
// Сборка SPA: prod-бандл кладём в wwwroot Api, dev — проксируем /api на Kestrel.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': { target: 'http://localhost:5000', changeOrigin: true } },
  },
  build: {
    outDir: '../src/AdminPanel.Api/wwwroot', // вне root → явный emptyOutDir
    emptyOutDir: true,
  },
});
```

`http://localhost:5000` — dev-профиль Kestrel (`launchSettings.json` t01);
cookie через прокси остаётся same-origin → CORS не нужен (arch/01 §5).

### 6.3. tsconfig

- `tsconfig.json` — references на `tsconfig.app.json` и
  `tsconfig.node.json` (схема актуального шаблона Vite react-ts).
- `tsconfig.app.json` (код `src/`): `target: ES2023`, `lib:
  [ES2023, DOM, DOM.Iterable]`, `module: ESNext`, `moduleResolution:
  bundler`, `jsx: react-jsx`, `useDefineForClassFields: true`, `strict:
  true`, `noUnusedLocals`, `noUnusedParameters`, `noFallthroughCasesInSwitch`,
  `isolatedModules`, `moduleDetection: force`, `noEmit`,
  `allowImportingTsExtensions` (требует `noEmit` — соблюдено),
  `verbatimModuleSyntax` (стиль: type-only импорты через `import type`;
  усиливает `isolatedModules`), `skipLibCheck: true` (мажорный TS против
  типов зависимостей). Поле `types` не ограничиваем (`import.meta.env` и
  ambient-типы не используются — `vite/client` не нужен).
- `tsconfig.node.json` (`vite.config.ts`): те же флаги (без DOM-окружения:
  `lib: [ES2023]`, без `jsx`), `noEmit: true`.
- Корневой `tsconfig.json` (`files: []` + references) — только для IDE;
  CLI-проверки всегда с явным `-p` на субконфиги (скрипты §6.1) — иначе
  проверка пуста.

### 6.4. index.html

`lang="ru"`, `<title>AdminPanel</title>`, `<div id="root">`, скрипт-модуль
`/src/main.tsx`. Иконок/шрифтов не подключаем (системные — Mantine default).

## 7. Компоненты каркаса

### 7.1. `main.tsx`

Создание `QueryClient` с defaults §3.10; дерево провайдеров:
`MantineProvider` (`defaultColorScheme="dark"`) → `QueryClientProvider` →
`PollingProvider` → `RouterProvider` (роутер из `App.tsx`). Импорт
`@mantine/core/styles.css` первым.

### 7.2. `App.tsx`

```ts
// Маршруты SPA: /login открыт, остальное — под AppLayout-guard (§4.1).
createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'etcd', element: <EtcdPage /> },
      { path: 'clusters', element: <ClustersPage /> },
      { path: 'ha', element: <HaPage /> },
      { path: 'alerts', element: <AlertsPage /> },
      { path: '*', element: <Navigate to="/" replace /> },
    ],
  },
]);
```

### 7.3. `api/dto.ts` (полный состав)

```ts
// GET /api/auth/me
export interface SessionDto { username: string }

// GET /api/overview
export interface OverviewDto {
  alertsCritical: number; alertsWarning: number;
  etcd: { reachable: boolean; endpointsOk: number; endpointsTotal: number };
  clusters: { name: string; shards: number; buckets: number;
    activeMoves: number; masterlessShards: number }[];
  activeMoves: { cluster: string; bucket: number; state: string;
    owner: string | null; target: string | null; updatedUnix: number | null }[];
  snapshotAgeMs: number; stale: boolean;
}

// GET /api/etcd/status
export interface EtcdStatusDto {
  endpoints: { url: string; reachable: boolean; latencyMs: number | null;
    version: string | null; dbSizeBytes: number | null; leaderMemberId: string | null;
    raftTerm: number | null; errors: string[]; active: boolean }[];
  members: { id: string; name: string | null; peerUrls: string[];
    clientUrls: string[]; isLeader: boolean }[];
  alarms: { memberId: string; type: string }[];
  quorumSuspected: boolean; lastRefreshUtc: string;
}

// GET /api/clusters (сводка)
export interface ClusterSummaryDto { name: string; dbName: string | null;
  bucketsCount: number; incomplete: boolean; shardsTotal: number;
  shardsWithMaster: number; activeMoves: number }

// GET /api/clusters/{cluster} (детали)
export interface ClusterDto { name: string; dbName: string | null;
  bucketsCount: number; createdUnix: number | null; incomplete: boolean;
  shards: ShardDto[]; buckets: BucketDto[]; heals: HealDto[] }
export interface ShardDto { name: string; dsn: string; hosts: string[];
  replicasDeclared: number | null; masterAddress: string | null;
  masterLeaseAlive: boolean; runtime: ShardRuntimeDto | null }
export interface ShardRuntimeDto { standbiesSync: number | null;
  slotsLagMaxBytes: number | null; walStatusLost: string[];
  subscriptions: string[]; bucketSchemas: string[]; error: string | null }
export interface BucketDto { id: number; owner: string | null; state: string;
  move: MoveDto | null; ageSec: number | null }
export interface MoveDto { owner: string | null; target: string | null;
  startedUnix: number | null; updatedUnix: number | null; phase: string | null;
  lastError: string | null }
export interface HealDto { bucket: string; was: string | null; now: string | null;
  reason: string | null; tsUnix: number | null }

// GET /api/ha (сводка), /api/ha/{scope} (детали)
export interface HaScopeSummaryDto { scope: string; cluster: string | null;
  shard: string | null; matched: boolean; leaderName: string | null;
  membersTotal: number; membersHealthy: number; lagMaxBytes: number | null }
export interface HaScopeDto { scope: string; cluster: string | null;
  shard: string | null; matched: boolean; leaderName: string | null;
  optimeLeader: number | null; members: HaMemberDto[]; rawConfig: string | null }
export interface HaMemberDto { name: string; host: string; port: number | null;
  role: string | null; state: string | null; timeline: number | null;
  lagBytes: number | null; probeAtUtc: string | null; probeError: string | null }

// GET /api/alerts
export interface AlertDto { id: string; severity: string; kind: string;
  target: string; message: string;
  details: Record<string, string> | null; sinceUnix: number | null }
```

`state` бакета — строковый канон `"ACTIVE" | "SYNCING" | "FROZEN" |
"ABORTING"` (тип-алиас `BucketStateName`); `severity` —
`"critical" | "warning" | "info"` (алиас `AlertSeverityName`) — как в
HTTP-контракте, без enum'ов (сериализация строковая).

### 7.4. `api/queries.ts`

```ts
export const queryKeys = {
  session: ['session'] as const,
  overview: ['overview'] as const,
  etcdStatus: ['etcd-status'] as const,
  clusters: ['clusters'] as const,
  cluster: (name: string) => ['clusters', name] as const,
  haScopes: ['ha-scopes'] as const,
  haScope: (scope: string) => ['ha-scopes', scope] as const,
  alerts: (severity?: string, kind?: string) => ['alerts', { severity, kind }] as const,
};
export const fetchSession = /* () => apiFetch<SessionDto>('/api/auth/me') */
export const fetchOverview = /* ... GET /api/overview */
export const fetchEtcdStatus = /* ... */
export const fetchClusters = /* ... */
export const fetchClusterDetails = /* (name, owner?, state?) → GET /api/clusters/{name}?owner&state */
export const fetchHaScopes = /* ... */
export const fetchHaScope = /* (scope) → GET /api/ha/{scope} */
export const fetchAlerts = /* (severity?, kind?) → GET /api/alerts?severity&kind */
export const loginRequest = /* (username, password) → POST /api/auth/login */
export const logoutRequest = /* () → POST /api/auth/logout */
```

### 7.5. `polling/PollingContext.tsx`

```ts
export type PollingInterval = '2' | '5' | '15' | 'off';
export const DEFAULT_POLLING: PollingInterval = '5';
export const POLLING_STORAGE_KEY = 'adminpanel.pollingInterval';
// Provider: useState c чтением/валидацией localStorage; запись в onChange.
export function usePollingInterval(): { interval: PollingInterval;
  setInterval: (v: PollingInterval) => void };
export function usePollingIntervalMs(): number | false;
```

### 7.6. `auth/LoginPage.tsx`

Форма Mantine (`TextInput` + `PasswordInput` + `Button`, submit по Enter):
`loginRequest(username, password)` → успех: `queryClient.setQueryData(
queryKeys.session, { username })` + `navigate(from ?? '/')`; `ApiError`
401 → инлайн-ошибка «Неверный логин или пароль»; 429 → «Слишком много
попыток, подождите N с» (из `retryAfterSeconds`); ошибка сети → «Панель
недоступна». `from` — из `useSearchParams` (§3.11). При уже активной
сессии (`queryKeys.session` в кэше) Login не redirect'ит на `/` сам —
пользователь может перелогиниться (простота; guard работает в другую
сторону).

### 7.7. `layout/AppLayout.tsx`

Guard-логика §3.11 (session-query: pending → `LoaderOverlay`; ошибка сети →
`Text` + `Button` «Повторить»; успех → AppShell). AppShell: `navbar` —
`NavLink`-навигация (Обзор `/`, etcd `/etcd`, Кластеры `/clusters`,
HA `/ha`, Алерты `/alerts` — активность по маршруту); `header` —
`StaleBadge`, `PollingToggle`, `Text username`, `Button` «Выйти» →
`logoutRequest()` + `queryClient.clear()` + `navigate('/login')`.

### 7.8. `layout/StaleBadge.tsx` и `layout/PollingToggle.tsx`

По §3.12 и §3.9: Badge-варианты `данные: 4 с` (grey) / `stale: 2 мин 3 с`
(yellow) / `нет данных` (red); `SegmentedControl` со значениями
`2 c | 5 c | 15 c | off`.

### 7.9. Страницы-заглушки

Каждая — `Container` + `Title` (человекочитаемое имя панели) + `Text`
«Панель будет реализована в t08/t09» (Overview/etcd/Clusters → t08;
Ha/Alerts → t09). Никаких запросов данных.

### 7.10. `utils/format.ts`

`formatAge(ms: number): string` — `< 60 с` → `«N с»`; `"< 60 мин" →
«N мин M с»`; иначе `«N мин»` (округления вниз). Используется бейджем;
t08+ — переиспользуется для `ageSec`/`sinceUnix`.

## 8. `Program.cs` после t07 (дельта)

После блока OpenAPI и до `UseAuthentication`:

```csharp
// [t07] SPA из wwwroot — без авторизации (в бандле секретов нет, arch/01 §4/§5).
// Бандла нет (npm run build не запускался) — хост жив, отдаётся только API.
if (!Directory.Exists(app.Environment.WebRootPath))
    app.Logger.LogWarning("wwwroot пуст — SPA-бандл не собран (cd frontend && npm run build)");

// [t07] default-документ + статика; auth-guard /api/* ниже не затрагивает их.
app.UseDefaultFiles();
app.UseStaticFiles();
```

После `MapHealthChecks` (конец пайплайна):

```csharp
// [t07] неизвестные /api/* — 404 ProblemDetails (не SPA-fallback), arch/01 §5.
app.MapFallback("/api/{**_}", () =>
    Results.Problem(statusCode: StatusCodes.Status404NotFound, title: "Not found"));

// [t07] SPA-fallback: клиентская маршрутизация unknown-путей через index.html.
app.MapFallbackToFile("index.html");
```

Порядок исполнения: статика → auth → guard `/api/*` → эндпоинты →
fallback'и (специфичный `/api`-fallback приоритетнее файлового). `WebRootPath`
не null и при отсутствующем каталоге — проверка `Directory.Exists` корректна.

## 9. Интеграционные тесты .NET (Docker не нужен)

Новый файл в существующей коллекции `"api"` (общий хост, t02 §10; правок
фабрики нет — auth-настройки `UseSetting` уже задают admin/adminpw):

- `UnknownApiPath_WithoutCookie_Returns401` — `GET /api/whatever` без
  cookie → 401 (guard раньше fallback; не 404 и не html).
- `UnknownApiPath_WithCookie_Returns404ProblemDetails` — после логина
  `GET /api/whatever` → 404 `application/problem+json` (fallback `/api`
  приоритетнее SPA-fallback; регрессия «API не отдаёт index.html»).
- `RootPath_WithoutCookie_IsNotUnauthorized` — `GET /` без cookie → статус
  ≠ 401 (статика/SPA-fallback без auth; при пустом wwwroot в CI — 404,
  при собранном бандле — 200; ассерт только «не 401 и не 5xx» — устойчиво
  к обоим состояниям бандла).

## 10. Ограничения (что НЕ делается)

- Наполнение панелей данными, детальные маршруты `/clusters/:cluster`,
  `/ha/:scope` — t08/t09; каркас отдаёт только заглушки.
- ESLint/Prettier, vitest/RTL, MSW-моки, e2e-браузерные тесты — нет
  (§3.4–3.5; e2e — t10 по API).
- Dockerfile/CI для npm-сборки, встраивание `npm run build` в MSBuild —
  нет (поставка — t11); бандл собирается вручную по §14.4.
- Переключатель темы, светлая тема, i18n, SSR — нет (тёмная тема фиксирована).
- Изменения auth/inspection-кода бэкенда — нет; Program.cs — только
  хостинг-мидлвари §8.
- Мутации `arch/02`, `arch/04`, `arch/roadmap/*` (кроме деливерабла §13),
  NuGet-пакеты — нет.
- Фронтенд не мультиязычен и не конфигурируется env'ами (кроме registry
  npm) — все строки в коде, русский.

## 11. Правки arch/ (внесены этой задачей в worktree до spec)

Задача уточняет контракт фронтенда — правки внесены в arch/ до написания
кода (arch-first), минимально:

1. `arch/01-architecture.md` §5: polling-пункт дополнен сохранением выбора
   в localStorage; новые пункты «Каркас SPA (t07)» (структура каркаса,
   guard-схема: me-проверка при монтировании + 401 → `/login`) и «Раздача
   SPA» (статика без авторизации; неизвестные `/api/*` → 404, не fallback;
   поведение при пустом wwwroot).
2. `arch/03-panels.md` §3 «Общие элементы»: дополнены persist интервала в
   localStorage и stale-бейдж в шапке layout'а (по `snapshotAgeMs`/`stale`
   `/api/overview`, опрос с текущим интервалом; «нет данных» при недоступности).

Версии пакетов, структура каталогов `frontend/`, tsconfig-детали —
реализация, в arch не поднимаются (источник — этот spec).

## 12. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t07-frontend-base` из
`arch/roadmap/frontend.md`. Зависимости `← t07-frontend-base` у
`t08-frontend-clusters`/`t09-frontend-ha` не трогаются — прецедент t02
§12 (зависимость в самой строке после мержа-предка не очищалась).

## 13. Критерии приёмки

1. `cd frontend && npm ci` — успех (registry из `frontend/.npmrc`).
2. `npm run build` — `tsc --noEmit` без ошибок + `vite build` пишет
   `src/AdminPanel.Api/wwwroot/index.html` и `assets/` (emptyOutDir
   очищает каталог).
3. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings; `dotnet test`
   — все тесты зелёные, включая новые `SpaHostingTests` (Docker для них
   не нужен; прочие integration — как раньше).
4. Ручной сценарий `dotnet run --project src/AdminPanel.Api` (Development:
   admin/admin; etcd может отсутствовать — панель жива):
   - `curl -i http://localhost:5000/` → 200 `text/html` (index.html);
   - `curl -i http://localhost:5000/clusters` → 200 `index.html`
     (SPA-fallback);
   - `curl -i http://localhost:5000/api/whatever` → 401 без cookie;
     с cookie (после логина) → 404 ProblemDetails;
   - браузер `http://localhost:5000/` → редирект на `/login` (guard);
     admin/admin → layout: навигация (Обзор/etcd/Кластеры/HA/Алерты),
     заглушки открываются, username в шапке;
   - «Выйти» → `/login`, повторный вход — ок;
   - без логина прямой заход на `/ha` → `/login?from=%2Fha`, после логина —
     возврат на `/ha`.
5. Polling и бейдж: переключатель 2/5/15/off — выбор переживает F5
   (localStorage); в Network вкладке `/api/overview` опрашивается с
   выбранным интервалом (default 5 c), при `off` — не опрашивается;
   бейдж показывает возраст данных / «нет данных» (etcd выключен → 503 →
   красный бейдж, панель не падает).
6. `git status`: `src/AdminPanel.Api/wwwroot/` и `frontend/node_modules/`
   не отслеживаются; `frontend/package-lock.json` отслеживается.
7. Правки arch/ — ровно §11; пункт `t07-frontend-base` удалён из
   `arch/roadmap/frontend.md` мерж-коммитом (§12); `← t07-frontend-base`
   у t08/t09 сохранён.
8. `grep PackageReference` по csproj — без изменений.
9. Все решения §3 не противоречат arch/01 §4–5/§7 и arch/03 §1–3.

## 14. Риски и заметки

- **TypeScript 7.0.2** — свежий мажор: при несовместимости с конфигом
  шаблона или `@types/react` откат на `~5.9` (5.9.3), §3.2 — решение
  фиксируется в plan. Сборка Vite типов не проверяет — typecheck только
  через `tsc --noEmit` (в `npm run build` он первый).
- **API Mantine 9 / React Router 8** — базовые компоненты (MantineProvider,
  AppShell, NavLink, SegmentedControl, TextInput/PasswordInput, Badge;
  createBrowserRouter/RouterProvider/Outlet/Navigate) стабильны с v7/v6.4
  соответственно; при расхождении сигнатур — сверка с официальной
  документацией в фазе кода (это не контрактное изменение).
- **401-редирект через `window.location.replace`** перезагружает SPA:
  принят осознанно (§3.11); TanStack-кэш при этом сбрасывается естественно.
  Возврат по `?from=` компенсирует потерю маршрута.
- **Пустой wwwroot в CI/чистом чекауте**: `dotnet test` не зависит от
  бандла (ассерты §9 это учитывают); `dotnet run` без `npm run build`
  отдаёт только API + warning в лог.
- **`MapFallbackToFile` и `/api`**: без специфичного `/api`-fallback
  авторизованный `GET /api/whatever` получил бы `index.html` с 200 —
  именно это исключается парой fallback'ов и фиксируется тестом
  `UnknownApiPath_WithCookie_Returns404ProblemDetails`.
- **Guard-мидлварь t02 до fallback'ов**: `/api/whatever` без cookie — 401
  (не 404) — уже зафиксировано поведением t02 §14 «Гарды и routing»,
  новый тест лишь закрепляет.
- **Artifactory в пользовательском npm**: project `.npmrc` перекрывает
  default registry только внутри `frontend/`; глобальные настройки
  пользователя не меняются (§3.3).
- **Proxy-куки в dev**: vite proxy сохраняет same-origin — cookie
  `adminpanel_session` работает без CORS/credentials-настроек; проверено
  сценарием §13.4 в dev-режиме (`npm run dev` → http://localhost:5173,
  необязательная проверка).
- **Обновление данных при смене интервала**: `refetchInterval` читается
  реактивно из контекста — смена интервала применяется к следующему тику
  без перемонтирования; `off` → `false` останавливает опрос (TanStack
  Query принимает функцию-значение — используется статическое из хука,
  пересчёт на рендере).
