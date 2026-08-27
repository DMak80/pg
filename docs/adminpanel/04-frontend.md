# 04 — Фронтенд: каркас SPA

> Назад: [INDEX.md](INDEX.md) · Подсистема: `frontend/` (React+Vite+TS7+Mantine,
  вне .slnx). Контракт: [arch/01](../../arch/adminpanel/01-architecture.md) §5, [arch/03](../../arch/adminpanel/03-panels.md).

Кратко: `npm run build` кладёт бандл в `src/AdminPanel.Api/wwwroot` (vite `outDir`
вне корня проекта — `emptyOutDir: true` обязателен), Kestrel раздаёт его без auth и
делает SPA-fallback; `npm run dev` — vite:5173 с proxy `/api` → `http://localhost:5050`
(cookie same-origin, CORS не нужен). Данные — только polling (TanStack Query,
`refetchInterval` из контекста), WebSocket/SSE нет.

## Слои

- `api/client.ts`: `apiFetch<T>` + `ApiError`; 401 → редирект `/login?from=…`
  (кроме формы логина), 429 — `Retry-After`; ProblemDetails → `title/detail`.
- `api/dto.ts`: типы DTO (camelCase — как JSON API); `api/queries.ts`: queryKeys +
  fetch-функции.
- `polling/PollingContext.tsx`: `'2'|'5'|'15'|'off'`, default `'5'`, localStorage
  `adminpanel.pollingInterval` (невалидное → default); `usePollingIntervalMs()` →
  `number | false`.
- `main.tsx`: `MantineProvider` (dark) → `QueryClientProvider` (retry без 401,
  refetchOnWindowFocus: false) → `PollingProvider` → `RouterProvider`;
  `@mantine/core/styles.css` импортируется первым.
- `layout/AppLayout.tsx`: guard через session-query (`GET /api/auth/me`), AppShell,
  StaleBadge, PollingToggle; страницы `pages/`.

## Чек-лист «добавить страницу/эндпоинт-клиент»

1. DTO: поля в `api/dto.ts` (camelCase, строго по arch/03 §2 — не по C#-типам).
2. Запрос: queryKey + fetch в `api/queries.ts`; polling-интервал — через
   `usePollingIntervalMs()` в `refetchInterval`.
3. Страница: `pages/<Name>Page.tsx`, маршрут в `App.tsx` (+ подсветка навигации по
   префиксу для деталей), LoadState для загрузки/ошибки.
4. `npm run typecheck` зелёный; при изменении бандла для локальной проверки —
   `npm run build` и перезапуск/обновление Kestrel.

## Грабли

- **TS7 и css-импорт**: typescript 7 (tsgo) проверяет side-effect-импорты строже —
  `@mantine/core/styles.css` требует ambient-декларацию `vite-env.d.ts`
  (`declare module '*.css'`; t07, коммит f4edda4). Удалять `vite-env.d.ts` нельзя.
- **`.npmrc` с публичным registry обязателен**: дефолтный registry окружения может
  быть приватным — `npm ci` в Docker/чистом окружении идёт через
  `registry.npmjs.org` из `frontend/.npmrc` (t07, c0c5ac9). Копировать в сборке
  вместе с package*.json.
- **wwwroot не в git** (артефакт vite): «SPA не отдаётся» в свежем клоне — это
  warning в логе хоста и `npm run build`, не поломка API.
- **Корневой `tsconfig.json` — только для IDE** (`files: []` + references); CLI-проверки
  всегда с явным `-p tsconfig.app.json`/`tsconfig.node.json` (скрипты package.json).
- **Node-версия**: engines `>=22.12` (peer vite 8); локально/Docker — node 22.
