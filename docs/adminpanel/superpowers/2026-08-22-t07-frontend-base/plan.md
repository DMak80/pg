# t07-frontend-base — план исполнения (каркас SPA AdminPanel)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Каркас SPA `frontend/` (Vite + React + TS + Mantine + React Router + TanStack Query) со сборкой в `src/AdminPanel.Api/wwwroot`, guard'ом по `/api/auth/me`, polling-переключателем и stale-бейджем, плюс раздача SPA хостом (`Program.cs`).

**Architecture:** Фронтенд — одностраничное React-приложение в `frontend/` (не dotnet-проект): prod-сборка Vite пишет бандл в wwwroot Api, dev — vite-сервер с proxy `/api` на Kestrel `:5000`. Guard: fetch-обёртка при 401 редиректит на `/login?from=…`, AppLayout при монтировании проверяет `GET /api/auth/me`. Бэкенд-дельта — только хостинг-мидлвари в `Program.cs` (статика без auth + пара fallback'ов: `/api/*` → 404, прочее → `index.html`).

**Tech Stack:** React 19.2.8, react-router 8.3.0, @tanstack/react-query 5.101.4, @mantine/core+hooks 9.5.2, Vite 8.2.1, TypeScript 7.0.2 (fallback ~5.9.3 — критерий в Задаче 2), .NET 10 ASP.NET Core.

**Spec:** `docs/superpowers/2026-08-22-t07-frontend-base/spec.md` (в корне worktree; исполнитель читает spec и этот план вместе — план аргументируется от spec).

## Global Constraints

- Рабочий каталог всех команд — корень worktree: `/Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base`.
- Node v26.7.0 / npm 12.0.2; registry зафиксирован `frontend/.npmrc` → `https://registry.npmjs.org`.
- Версии npm-пакетов — точно из spec §6.1: react `^19.2.8`, react-dom `^19.2.8`, react-router `^8.3.0`, @tanstack/react-query `^5.101.4`, @mantine/core `^9.5.2`, @mantine/hooks `^9.5.2`, vite `^8.2.1`, @vitejs/plugin-react `^6.0.5`, typescript `~7.0.2` (fallback `~5.9.3`), @types/react `^19.2.18`, @types/react-dom `^19.2.4`. Обновлять зависимости в процессе — запрещено.
- TypeScript: `strict: true`, `noUnusedLocals`, `noUnusedParameters`, `isolatedModules`, `noEmit`; проверка типов — всегда с явным проектом: `npm run build` = `tsc --noEmit -p tsconfig.app.json && tsc --noEmit -p tsconfig.node.json && vite build`; `npm run typecheck` — то же без vite. Корневой `tsconfig.json` (references) — только для IDE, в CLI-проверках не участвует.
- Идентификаторы — английские; комментарии в коде и UI-тексты — русские. Тесты .NET — комментарии AAA (`// Arrange` / `// Act` / `// Assert`) на русском.
- Auth/inspection-код бэкенда не меняется; из Program.cs — только мидлвари Задачи 9. NuGet-пакеты не добавляются (`Directory.Packages.props` не трогаем).
- `TreatWarningsAsErrors=true` — dotnet-сборка с 0 warnings.
- В git не попадают: `frontend/node_modules/`, `src/AdminPanel.Api/wwwroot/` (бандл). `frontend/package-lock.json` — коммитится.
- Правки `arch/` — ровно те, что уже внесены (spec §11); новые мутации arch/02, arch/04 запрещены; roadmap-деливерабл — Задача 10.
- Один шаг плана ≈ одно действие; каждая задача завершается своим коммитом в feature-ветку (текущая ветка worktree, коммитить свободно).
- Если реальный API Mantine 9 / react-router 8 отличается от использованных ниже сигнатур базовых компонентов (AppShell, NavLink, SegmentedControl, TextInput, PasswordInput, Badge, createBrowserRouter, RouterProvider, Outlet, Navigate, useNavigate, useSearchParams, Link) — свериться с официальной документацией и адаптировать вызов, не меняя контракт spec (риск зафиксирован в spec §14). Это не повод останавливаться.

---

### Задача 1: Коммит документации и arch-правок

**Files:**
- Modify: ничего. Commit: `docs/superpowers/2026-08-22-t07-frontend-base/spec.md` (создан в Фазе 1), `docs/superpowers/2026-08-22-t07-frontend-base/plan.md` (этот файл), `arch/01-architecture.md` (§5, правка Фазы 1), `arch/03-panels.md` (§3, правка Фазы 1).

**Interfaces:**
- Consumes: ничего.
- Produces: чистый рабочий каталог для кода; прецедент — коммит `438549b` задачи t06 («spec/plan задачи + roadmap-деливерабл» отдельным коммитом ветки).

- [ ] **Шаг 1.1: Проверить состав незакоммиченного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git status --short
```
Ожидание: ` M arch/01-architecture.md`, ` M arch/03-panels.md`, `?? docs/superpowers/2026-08-22-t07-frontend-base/`. Если состав другой — остановиться и разобраться (не коммитить чужое).

- [ ] **Шаг 1.2: Посмотреть diff arch и убедиться в соответствии spec §11**

```bash
git diff arch/
```
Ожидание: только правки §5 `01-architecture.md` (localStorage в polling-пункте + пункты «Каркас SPA (t07)» и «Раздача SPA») и §3 `03-panels.md` (persist интервала + stale-бейдж).

- [ ] **Шаг 1.3: Коммит**

```bash
git add docs/superpowers/2026-08-22-t07-frontend-base arch/01-architecture.md arch/03-panels.md
git commit -m "t07: spec/plan задачи + arch-правки §5/§3 (каркас SPA, guard, раздача) (spec §11)"
```

**Выход:** история содержит документацию задачи; `git status` чист. **Проверка:** `git log --oneline -1` показывает новый коммит; `git status --short` — пусто. **Spec:** §11.

---

### Задача 2: Каркас frontend-проекта (npm + Vite + TS + сборка в wwwroot)

**Files:**
- Create: `frontend/.npmrc`, `frontend/package.json`, `frontend/index.html`, `frontend/tsconfig.json`, `frontend/tsconfig.app.json`, `frontend/tsconfig.node.json`, `frontend/vite.config.ts`, `frontend/src/main.tsx` (минимальная версия, заменяется в Задаче 8).
- Modify: `.gitignore` (корень репо).

**Interfaces:**
- Consumes: ничего.
- Produces: работающий `npm run build`, пишущий `src/AdminPanel.Api/wwwroot/index.html` + `assets/`; каталог `frontend/src/` для последующих задач; зафиксированная рабочая версия TypeScript (7.0.2 либо 5.9.3).

- [ ] **Шаг 2.1: `.gitignore` — исключить бандл wwwroot (до первой сборки)**

В корневом `.gitignore` заменить последний блок (сейчас файл кончается им):

```gitignore
# AdminPanel specifics
.dev-flow/
.DS_Store
node_modules/
dist/
```

на:

```gitignore
# AdminPanel specifics
.dev-flow/
.DS_Store
node_modules/
dist/
# t07: SPA-бандл — артефакт vite-сборки, поставка собирает его заново (spec §3.17)
src/AdminPanel.Api/wwwroot/
```

- [ ] **Шаг 2.2: `frontend/.npmrc`** — одна строка (spec §3.3: пользовательский npm указывает на недоступный Artifactory):

```
registry=https://registry.npmjs.org
```

- [ ] **Шаг 2.3: `frontend/package.json`** — зависимости и версии по spec §6.1; скрипты с явными `-p`-проектами (фикс ревью Фазы 4, spec §6.1 синхронизирован):

```json
{
  "name": "adminpanel-frontend",
  "private": true,
  "type": "module",
  "engines": {
    "node": ">=22.12"
  },
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

- [ ] **Шаг 2.4: `frontend/index.html`**:

```html
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>AdminPanel</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Шаг 2.5: `frontend/tsconfig.json`**:

```json
{
  "files": [],
  "references": [
    { "path": "./tsconfig.app.json" },
    { "path": "./tsconfig.node.json" }
  ]
}
```

Корневой конфиг — только для IDE (переходы по проекту); CLI-проверки плана всегда идут с явным `-p tsconfig.app.json` / `-p tsconfig.node.json` (скрипты Шага 2.3) — пустой `files: []` корня не участвует и не может «съесть» проверку.

- [ ] **Шаг 2.6: `frontend/tsconfig.app.json`** (spec §6.3 — все флаги обязаны присутствовать):

```json
{
  "compilerOptions": {
    "target": "ES2023",
    "useDefineForClassFields": true,
    "lib": ["ES2023", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src"]
}
```

- [ ] **Шаг 2.7: `frontend/tsconfig.node.json`** (spec §6.3: те же флаги, окружение node):

```json
{
  "compilerOptions": {
    "target": "ES2023",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "verbatimModuleSyntax": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["vite.config.ts"]
}
```

- [ ] **Шаг 2.8: `frontend/vite.config.ts`** (spec §6.2):

```ts
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Сборка SPA: prod-бандл кладём в wwwroot Api, dev — проксируем /api на Kestrel (spec §6.2).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:5000', changeOrigin: true },
    },
  },
  build: {
    outDir: '../src/AdminPanel.Api/wwwroot', // вне root — явно очищаем
    emptyOutDir: true,
  },
});
```

- [ ] **Шаг 2.9: `frontend/src/main.tsx`** — временная минимальная версия (полная — Задача 8; здесь только доказательство сборки):

```tsx
// Точка входа SPA (минимальная версия каркаса; заменяется полной в задаче 8).
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <h1>AdminPanel</h1>
  </StrictMode>,
);
```

- [ ] **Шаг 2.10: Установка зависимостей (создаёт package-lock.json)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm install
```
Ожидание: exit 0, создан `frontend/package-lock.json`, `frontend/node_modules/`. Если network-ошибка — проверить, что `frontend/.npmrc` из Шага 2.2 существует (registry npmjs.org).

- [ ] **Шаг 2.11: Решение TypeScript 7 vs 5.9 (однозначный критерий, spec §3.2)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npx tsc --version && npx tsc --noEmit -p tsconfig.app.json && npx tsc --noEmit -p tsconfig.node.json && echo TS_CHECK_OK
```

Обе `-p`-команды обязаны реально прогонять компилятор по входным файлам (`src/**` и `vite.config.ts` соответственно) — признак этого: при намеренно дописанной ошибке (не делать, только понимать критерий) tsc упал бы. `TS_CHECK_OK` в конце — маркер, что оба проекта прошли.
Решение:
- Если `npx tsc --version` печатает версию (например `7.0.2`) и обе проверки проходят — остаёмся на `~7.0.2`.
- **Критерий отката:** невозможно запустить tsc, либо ошибки семейства TS5xxx (конфигурационные: unknown compiler option / invalid tsconfig и т.п.). Тогда: в `package.json` заменить `"typescript": "~7.0.2"` на `"typescript": "~5.9.3"`, снова `npm install`, повторить проверку до `TS_CHECK_OK`.
- Ошибки типов в коде (TS2xxx) — **не** повод для отката, исправляются кодом.
Принятое решение (7.0.2 или 5.9.3) упомянуть в сообщении коммита Шага 2.13.

- [ ] **Шаг 2.12: Полная сборка в wwwroot**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run build && test -f ../src/AdminPanel.Api/wwwroot/index.html && echo SPA_OK
```
Ожидание: vite напечатал `dist`… нет — целевой каталог `../src/AdminPanel.Api/wwwroot`; в конце `SPA_OK`. Плюс:

```bash
git status --short
```
Ожидание: `src/AdminPanel.Api/wwwroot/` **не** появляется в untracked (правка `.gitignore` работает).

- [ ] **Шаг 2.13: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add .gitignore frontend/.npmrc frontend/package.json frontend/package-lock.json frontend/index.html frontend/tsconfig.json frontend/tsconfig.app.json frontend/tsconfig.node.json frontend/vite.config.ts frontend/src/main.tsx
git commit -m "t07: каркас frontend — vite+react+ts, сборка в wwwroot, registry .npmrc (spec §6; typescript <версия по Шагу 2.11>)"
```

**Выход:** `npm ci`/`npm run build` воспроизводимы; бандл в wwwroot вне git. **Проверка:** Шаг 2.12 зелёный. **Spec:** §3.1–3.3, §3.17, §6, §13.1–13.2.

---

### Задача 3: API-слой — fetch-обёртка, типы DTO, query-функции

**Files:**
- Create: `frontend/src/api/client.ts`, `frontend/src/api/dto.ts`, `frontend/src/api/queries.ts`.

**Interfaces:**
- Consumes: ничего (чистый TS без React).
- Produces (для Задач 5–8): `apiFetch<T>(path, init?): Promise<T>`, `class ApiError { status; title?; detail?; retryAfterSeconds? }`, все DTO-интерфейсы §7.3 spec, `queryKeys` и функции `fetchSession/fetchOverview/fetchEtcdStatus/fetchClusters/fetchClusterDetails/fetchHaScopes/fetchHaScope/fetchAlerts/loginRequest/logoutRequest`.

- [ ] **Шаг 3.1: `frontend/src/api/dto.ts`** — по составу полей и типам spec §7.3 (инлайн-типы `OverviewDto` разложены на именованные интерфейсы `OverviewEtcdDto`/`OverviewClusterDto`/`OverviewMoveDto` — семантически эквивалентно):

```ts
// Типы DTO REST API (arch/03 §2; фактические поля — C#-DTO t04–t06).
// Nullable-поля C# → '| null'; unix-время → number | null; DateTimeOffset → string.

// Строковый канон статусов бакета (arch/02 §2.1).
export type BucketStateName = 'ACTIVE' | 'SYNCING' | 'FROZEN' | 'ABORTING';

// Строковый канон severity алертов (arch/03 §1).
export type AlertSeverityName = 'critical' | 'warning' | 'info';

// GET /api/auth/me
export interface SessionDto {
  username: string;
}

// GET /api/overview
export interface OverviewDto {
  alertsCritical: number;
  alertsWarning: number;
  etcd: OverviewEtcdDto;
  clusters: OverviewClusterDto[];
  activeMoves: OverviewMoveDto[];
  snapshotAgeMs: number;
  stale: boolean;
}

export interface OverviewEtcdDto {
  reachable: boolean;
  endpointsOk: number;
  endpointsTotal: number;
}

export interface OverviewClusterDto {
  name: string;
  shards: number;
  buckets: number;
  activeMoves: number;
  masterlessShards: number;
}

export interface OverviewMoveDto {
  cluster: string;
  bucket: number;
  state: BucketStateName;
  owner: string | null;
  target: string | null;
  updatedUnix: number | null;
}

// GET /api/etcd/status
export interface EtcdStatusDto {
  endpoints: EtcdEndpointDto[];
  members: EtcdMemberDto[];
  alarms: EtcdAlarmDto[];
  quorumSuspected: boolean;
  lastRefreshUtc: string;
}

export interface EtcdEndpointDto {
  url: string;
  reachable: boolean;
  latencyMs: number | null;
  version: string | null;
  dbSizeBytes: number | null;
  leaderMemberId: string | null;
  raftTerm: number | null;
  errors: string[];
  active: boolean;
}

export interface EtcdMemberDto {
  id: string;
  name: string | null;
  peerUrls: string[];
  clientUrls: string[];
  isLeader: boolean;
}

export interface EtcdAlarmDto {
  memberId: string;
  type: string;
}

// GET /api/clusters — сводный список.
export interface ClusterSummaryDto {
  name: string;
  dbName: string | null;
  bucketsCount: number;
  incomplete: boolean;
  shardsTotal: number;
  shardsWithMaster: number;
  activeMoves: number;
}

// GET /api/clusters/{cluster} — детали.
export interface ClusterDto {
  name: string;
  dbName: string | null;
  bucketsCount: number;
  createdUnix: number | null;
  incomplete: boolean;
  shards: ShardDto[];
  buckets: BucketDto[];
  heals: HealDto[];
}

export interface ShardDto {
  name: string;
  dsn: string;
  hosts: string[];
  replicasDeclared: number | null;
  masterAddress: string | null;
  masterLeaseAlive: boolean;
  runtime: ShardRuntimeDto | null;
}

export interface ShardRuntimeDto {
  standbiesSync: number | null;
  slotsLagMaxBytes: number | null;
  walStatusLost: string[];
  subscriptions: string[];
  bucketSchemas: string[];
  error: string | null;
}

export interface BucketDto {
  id: number;
  owner: string | null;
  state: BucketStateName;
  move: MoveDto | null;
  ageSec: number | null;
}

export interface MoveDto {
  owner: string | null;
  target: string | null;
  startedUnix: number | null;
  updatedUnix: number | null;
  phase: string | null;
  lastError: string | null;
}

export interface HealDto {
  bucket: string;
  was: string | null;
  now: string | null;
  reason: string | null;
  tsUnix: number | null;
}

// GET /api/ha — сводный список.
export interface HaScopeSummaryDto {
  scope: string;
  cluster: string | null;
  shard: string | null;
  matched: boolean;
  leaderName: string | null;
  membersTotal: number;
  membersHealthy: number;
  lagMaxBytes: number | null;
}

// GET /api/ha/{scope} — детали.
export interface HaScopeDto {
  scope: string;
  cluster: string | null;
  shard: string | null;
  matched: boolean;
  leaderName: string | null;
  optimeLeader: number | null;
  members: HaMemberDto[];
  rawConfig: string | null;
}

export interface HaMemberDto {
  name: string;
  host: string;
  port: number | null;
  role: string | null;
  state: string | null;
  timeline: number | null;
  lagBytes: number | null;
  probeAtUtc: string | null;
  probeError: string | null;
}

// GET /api/alerts
export interface AlertDto {
  id: string;
  severity: AlertSeverityName;
  kind: string;
  target: string;
  message: string;
  details: Record<string, string> | null;
  sinceUnix: number | null;
}
```

- [ ] **Шаг 3.2: `frontend/src/api/client.ts`** (spec §3.13, §7):

```ts
// Ошибка API: HTTP-статус + разобранные ProblemDetails (+ Retry-After для 429).
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly title?: string,
    readonly detail?: string,
    readonly retryAfterSeconds?: number,
  ) {
    super(detail ?? title ?? `HTTP ${status}`);
    this.name = 'ApiError';
  }
}

// Опции запроса: только то, что нужно каркасу (метод + JSON-тело).
export interface ApiFetchInit {
  method?: 'GET' | 'POST';
  body?: unknown;
}

// Разбор тела ошибки: ProblemDetails (title/detail) и заголовок Retry-After.
async function toApiError(response: Response): Promise<ApiError> {
  let title: string | undefined;
  let detail: string | undefined;
  try {
    const problem = (await response.json()) as { title?: string; detail?: string };
    title = problem.title;
    detail = problem.detail;
  } catch {
    // Тело не JSON (пустое/HTML) — поля остаются undefined.
  }

  const retryAfter = response.headers.get('Retry-After');
  const retryAfterSeconds =
    retryAfter !== null && /^\d+$/.test(retryAfter) ? Number(retryAfter) : undefined;
  return new ApiError(response.status, title, detail, retryAfterSeconds);
}

// Guard-реакция: 401 от любого вызова (кроме страницы логина) → /login с возвратом (spec §3.11).
function redirectOnUnauthorized(): void {
  if (window.location.pathname === '/login') return;
  const from = encodeURIComponent(window.location.pathname + window.location.search);
  window.location.replace(`/login?from=${from}`);
}

// Единственная точка HTTP фронта: относительные пути, cookie same-origin (spec §3.13).
export async function apiFetch<T>(path: string, init?: ApiFetchInit): Promise<T> {
  const hasBody = init?.body !== undefined;
  const response = await fetch(path, {
    method: init?.method ?? 'GET',
    credentials: 'same-origin',
    headers: hasBody
      ? { Accept: 'application/json', 'Content-Type': 'application/json' }
      : { Accept: 'application/json' },
    body: hasBody ? JSON.stringify(init?.body) : undefined,
  });

  if (response.status === 401) redirectOnUnauthorized();
  if (!response.ok) throw await toApiError(response);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}
```

- [ ] **Шаг 3.3: `frontend/src/api/queries.ts`** (spec §3.15, §7.4):

```ts
// Query-ключи и fetch-функции всех эндпоинтов (arch/03 §1); t08/t09 используют без правок слоя api.
import { apiFetch } from './client';
import type {
  AlertDto,
  ClusterDto,
  ClusterSummaryDto,
  EtcdStatusDto,
  HaScopeDto,
  HaScopeSummaryDto,
  OverviewDto,
  SessionDto,
} from './dto';

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

export function fetchSession(): Promise<SessionDto> {
  return apiFetch<SessionDto>('/api/auth/me');
}

export function fetchOverview(): Promise<OverviewDto> {
  return apiFetch<OverviewDto>('/api/overview');
}

export function fetchEtcdStatus(): Promise<EtcdStatusDto> {
  return apiFetch<EtcdStatusDto>('/api/etcd/status');
}

export function fetchClusters(): Promise<ClusterSummaryDto[]> {
  return apiFetch<ClusterSummaryDto[]>('/api/clusters');
}

export function fetchClusterDetails(
  name: string,
  owner?: string,
  state?: string,
): Promise<ClusterDto> {
  const params = new URLSearchParams();
  if (owner !== undefined) params.set('owner', owner);
  if (state !== undefined) params.set('state', state);
  const query = params.size > 0 ? `?${params.toString()}` : '';
  return apiFetch<ClusterDto>(`/api/clusters/${encodeURIComponent(name)}${query}`);
}

export function fetchHaScopes(): Promise<HaScopeSummaryDto[]> {
  return apiFetch<HaScopeSummaryDto[]>('/api/ha');
}

export function fetchHaScope(scope: string): Promise<HaScopeDto> {
  return apiFetch<HaScopeDto>(`/api/ha/${encodeURIComponent(scope)}`);
}

export function fetchAlerts(severity?: string, kind?: string): Promise<AlertDto[]> {
  const params = new URLSearchParams();
  if (severity !== undefined) params.set('severity', severity);
  if (kind !== undefined) params.set('kind', kind);
  const query = params.size > 0 ? `?${params.toString()}` : '';
  return apiFetch<AlertDto[]>(`/api/alerts${query}`);
}

export function loginRequest(username: string, password: string): Promise<void> {
  return apiFetch<void>('/api/auth/login', { method: 'POST', body: { username, password } });
}

export function logoutRequest(): Promise<void> {
  return apiFetch<void>('/api/auth/logout', { method: 'POST' });
}
```

Примечание: `URLSearchParams.size` — свойство появилось в браузерах 2023+ (lib ES2023 + DOM включают тип); при ошибке типизации заменить на `params.toString() !== ''`.

- [ ] **Шаг 3.4: Проверка типизации**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run typecheck
```
Ожидание: exit 0 (неиспользуемые экспорты — не ошибка; `noUnusedLocals` про локальные переменные).

- [ ] **Шаг 3.5: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/api
git commit -m "t07: api-слой — apiFetch/ApiError с 401-редиректом, типы DTO, query-функции (spec §3.13–3.15, §7.3–7.4)"
```

**Выход:** слой `api` готов целиком (t08/t09 не трогают его). **Проверка:** Шаг 3.4 зелёный. **Spec:** §3.13–3.15, §4.2, §7.3–7.4.

---

### Задача 4: Polling-контекст и форматирование возраста

**Files:**
- Create: `frontend/src/polling/PollingContext.tsx`, `frontend/src/utils/format.ts`.

**Interfaces:**
- Consumes: React (`useState/useMemo/useContext/createContext`).
- Produces: `type PollingInterval = '2' | '5' | '15' | 'off'`, `DEFAULT_POLLING = '5'`, `POLLING_STORAGE_KEY = 'adminpanel.pollingInterval'`, `<PollingProvider>`, `usePollingInterval(): { interval; setInterval }`, `usePollingIntervalMs(): number | false`, `formatAge(ms: number): string`.

- [ ] **Шаг 4.1: `frontend/src/polling/PollingContext.tsx`** (spec §3.9, §7.5):

```tsx
// Состояние переключателя polling-интервала: Context + localStorage (spec §3.9, arch/01 §5).
import { createContext, useContext, useMemo, useState } from 'react';
import type { ReactNode } from 'react';

// Допустимые значения переключателя; 'off' — опрос выключен.
export type PollingInterval = '2' | '5' | '15' | 'off';
export const DEFAULT_POLLING: PollingInterval = '5';
export const POLLING_STORAGE_KEY = 'adminpanel.pollingInterval';

// Чтение persisted-значения с валидацией: неизвестное — default.
function readStored(): PollingInterval {
  const raw = window.localStorage.getItem(POLLING_STORAGE_KEY);
  return raw === '2' || raw === '5' || raw === '15' || raw === 'off' ? raw : DEFAULT_POLLING;
}

export interface PollingState {
  interval: PollingInterval;
  setInterval: (value: PollingInterval) => void;
}

const PollingContext = createContext<PollingState | null>(null);

// Провайдер: любое изменение пишется в localStorage (выбор переживает перезагрузку).
export function PollingProvider({ children }: { children: ReactNode }) {
  const [interval, setIntervalState] = useState<PollingInterval>(readStored);
  const value = useMemo<PollingState>(
    () => ({
      interval,
      setInterval: (next) => {
        window.localStorage.setItem(POLLING_STORAGE_KEY, next);
        setIntervalState(next);
      },
    }),
    [interval],
  );
  return <PollingContext.Provider value={value}>{children}</PollingContext.Provider>;
}

// Доступ к переключателю; использование вне провайдера — ошибка программирования.
export function usePollingInterval(): PollingState {
  const ctx = useContext(PollingContext);
  if (ctx === null) throw new Error('usePollingInterval: нет PollingProvider выше по дереву');
  return ctx;
}

// Интервал для refetchInterval TanStack Query; 'off' → false (spec §3.9).
export function usePollingIntervalMs(): number | false {
  const { interval } = usePollingInterval();
  return interval === 'off' ? false : Number(interval) * 1000;
}
```

- [ ] **Шаг 4.2: `frontend/src/utils/format.ts`** (spec §7.10):

```ts
// Возраст данных в человекочитаемом виде (spec §7.10): «12 с», «3 мин 5 с», «62 мин».
export function formatAge(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  if (totalSeconds < 60) return `${totalSeconds} с`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return seconds === 0 ? `${minutes} мин` : `${minutes} мин ${seconds} с`;
  return `${minutes} мин`;
}
```

- [ ] **Шаг 4.3: Проверка типизации**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run typecheck
```
Ожидание: exit 0.

- [ ] **Шаг 4.4: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/polling frontend/src/utils
git commit -m "t07: polling-контекст (2/5/15/off + localStorage) и formatAge (spec §3.9, §7.5, §7.10)"
```

**Выход:** polling-механика готова к использованию StaleBadge (Задача 7). **Проверка:** Шаг 4.3 зелёный. **Spec:** §3.9, §7.5, §7.10.

---

### Задача 5: Страницы-заглушки панелей

**Files:**
- Create: `frontend/src/pages/OverviewPage.tsx`, `frontend/src/pages/EtcdPage.tsx`, `frontend/src/pages/ClustersPage.tsx`, `frontend/src/pages/HaPage.tsx`, `frontend/src/pages/AlertsPage.tsx`.

**Interfaces:**
- Consumes: `@mantine/core` (Container, Title, Text).
- Produces: именованные экспорты `OverviewPage`, `EtcdPage`, `ClustersPage`, `HaPage`, `AlertsPage` (компоненты без пропсов) — использует App.tsx Задачи 8.

- [ ] **Шаг 5.1: `frontend/src/pages/OverviewPage.tsx`**:

```tsx
// Заглушка панели Обзор — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function OverviewPage() {
  return (
    <Container>
      <Title order={2}>Обзор</Title>
      <Text c="dimmed">Дашборд будет реализован в t08-frontend-clusters.</Text>
    </Container>
  );
}
```

- [ ] **Шаг 5.2: `frontend/src/pages/EtcdPage.tsx`**:

```tsx
// Заглушка панели etcd — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function EtcdPage() {
  return (
    <Container>
      <Title order={2}>etcd</Title>
      <Text c="dimmed">Панель будет реализована в t08-frontend-clusters.</Text>
    </Container>
  );
}
```

- [ ] **Шаг 5.3: `frontend/src/pages/ClustersPage.tsx`**:

```tsx
// Заглушка панели Кластеры — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function ClustersPage() {
  return (
    <Container>
      <Title order={2}>Кластеры</Title>
      <Text c="dimmed">Панель будет реализована в t08-frontend-clusters.</Text>
    </Container>
  );
}
```

- [ ] **Шаг 5.4: `frontend/src/pages/HaPage.tsx`**:

```tsx
// Заглушка панели HA — наполнение в t09 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function HaPage() {
  return (
    <Container>
      <Title order={2}>HA</Title>
      <Text c="dimmed">Панель будет реализована в t09-frontend-ha.</Text>
    </Container>
  );
}
```

- [ ] **Шаг 5.5: `frontend/src/pages/AlertsPage.tsx`**:

```tsx
// Заглушка панели Алерты — наполнение в t09 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function AlertsPage() {
  return (
    <Container>
      <Title order={2}>Алерты</Title>
      <Text c="dimmed">Панель будет реализована в t09-frontend-ha.</Text>
    </Container>
  );
}
```

- [ ] **Шаг 5.6: Проверка типизации**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run typecheck
```
Ожидание: exit 0.

- [ ] **Шаг 5.7: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/pages
git commit -m "t07: страницы-заглушки Обзор/etcd/Кластеры/HA/Алерты (spec §7.9)"
```

**Выход:** все маршрутизируемые страницы существуют. **Проверка:** Шаг 5.6 зелёный. **Spec:** §4.1, §7.9.

---

### Задача 6: Страница Login

**Files:**
- Create: `frontend/src/auth/LoginPage.tsx`.

**Interfaces:**
- Consumes: `loginRequest`, `queryKeys` (Задача 3), `ApiError` (Задача 3), react-router (`useNavigate`, `useSearchParams`), TanStack (`useQueryClient`), Mantine.
- Produces: именованный экспорт `LoginPage` (без пропсов) — использует App.tsx Задачи 8.

- [ ] **Шаг 6.1: `frontend/src/auth/LoginPage.tsx`** (spec §7.6):

```tsx
// Страница логина — единственная форма ввода панели (arch/03 §3; spec §7.6).
import { useState } from 'react';
import type { FormEvent } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useNavigate, useSearchParams } from 'react-router';
import { Alert, Button, Card, Center, Container, PasswordInput, Stack, TextInput, Title } from '@mantine/core';
import { ApiError } from '../api/client';
import { loginRequest, queryKeys } from '../api/queries';

export function LoginPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // Возврат на исходную страницу после 401-редиректа (spec §3.11).
  const from = searchParams.get('from');

  async function handleSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      // При успехе username совпадает с каноническим (проверка логина точная).
      await loginRequest(username, password);
      queryClient.setQueryData(queryKeys.session, { username });
      navigate(from ?? '/', { replace: true });
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) setError('Неверный логин или пароль');
      else if (e instanceof ApiError && e.status === 429)
        setError(`Слишком много попыток, подождите ${e.retryAfterSeconds ?? 60} с`);
      else setError('Панель недоступна');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Container size="xs" pt="xl">
      <Center>
        <Card withBorder shadow="sm" padding="lg" radius="md" w="100%">
          <Title order={2} mb="md">Вход в AdminPanel</Title>
          <form onSubmit={(event) => void handleSubmit(event)}>
            <Stack>
              <TextInput
                label="Логин"
                value={username}
                onChange={(e) => setUsername(e.currentTarget.value)}
                autoComplete="username"
                required
              />
              <PasswordInput
                label="Пароль"
                value={password}
                onChange={(e) => setPassword(e.currentTarget.value)}
                autoComplete="current-password"
                required
              />
              {error !== null && <Alert color="red" variant="light">{error}</Alert>}
              <Button type="submit" loading={submitting}>Войти</Button>
            </Stack>
          </form>
        </Card>
      </Center>
    </Container>
  );
}
```

- [ ] **Шаг 6.2: Проверка типизации**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run typecheck
```
Ожидание: exit 0.

- [ ] **Шаг 6.3: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/auth
git commit -m "t07: страница Login — форма, 401/429/network-ошибки, возврат по from (spec §7.6, §4.2)"
```

**Выход:** login-флоу готов (используется в Задаче 8). **Проверка:** Шаг 6.2 зелёный. **Spec:** §3.11, §4.2, §7.6.

---

### Задача 7: Layout — guard, навигация, StaleBadge, PollingToggle

**Files:**
- Create: `frontend/src/layout/AppLayout.tsx`, `frontend/src/layout/PollingToggle.tsx`, `frontend/src/layout/StaleBadge.tsx`.

**Interfaces:**
- Consumes: `fetchSession/logoutRequest/queryKeys/fetchOverview` (Задача 3), `usePollingInterval/usePollingIntervalMs` (Задача 4), `formatAge` (Задача 4), `Outlet/useLocation/useNavigate/Link` из react-router.
- Produces: именованный экспорт `AppLayout` — root-элемент защищённой зоны (Задача 8).

- [ ] **Шаг 7.1: `frontend/src/layout/PollingToggle.tsx`** (spec §7.8):

```tsx
// Переключатель polling-интервала: 2 c / 5 c / 15 c / off (spec §3.9, §7.8).
import { SegmentedControl } from '@mantine/core';
import { usePollingInterval } from '../polling/PollingContext';
import type { PollingInterval } from '../polling/PollingContext';

export function PollingToggle() {
  const { interval, setInterval } = usePollingInterval();
  return (
    <SegmentedControl
      size="xs"
      value={interval}
      onChange={(value) => setInterval(value as PollingInterval)}
      data={[
        { value: '2', label: '2 c' },
        { value: '5', label: '5 c' },
        { value: '15', label: '15 c' },
        { value: 'off', label: 'off' },
      ]}
    />
  );
}
```

- [ ] **Шаг 7.2: `frontend/src/layout/StaleBadge.tsx`** (spec §3.12, §7.8):

```tsx
// Stale-бейдж в шапке: возраст данных / stale / нет данных (spec §3.12, arch/03 §3).
import { useQuery } from '@tanstack/react-query';
import { Badge } from '@mantine/core';
import { fetchOverview, queryKeys } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatAge } from '../utils/format';

export function StaleBadge() {
  // Опрос overview текущим polling-интервалом — демонстрация переключателя в t07.
  const { data, isError } = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: usePollingIntervalMs(),
  });

  if (isError) return <Badge color="red" variant="light">нет данных</Badge>;
  if (data === undefined) return null;
  if (data.stale)
    return <Badge color="yellow" variant="light">stale: {formatAge(data.snapshotAgeMs)}</Badge>;
  return <Badge color="gray" variant="light">данные: {formatAge(data.snapshotAgeMs)}</Badge>;
}
```

- [ ] **Шаг 7.3: `frontend/src/layout/AppLayout.tsx`** (spec §3.11, §7.7):

```tsx
// Layout защищённой зоны: guard по сессии + AppShell (nav, header, outlet) (spec §3.11, §7.7).
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AppShell, Button, Group, Loader, NavLink, Stack, Text } from '@mantine/core';
import { Link, Outlet, useLocation, useNavigate } from 'react-router';
import { fetchSession, logoutRequest, queryKeys } from '../api/queries';
import { PollingToggle } from './PollingToggle';
import { StaleBadge } from './StaleBadge';

// Пункты навигации: маршрут + человекочитаемое имя (arch/03 §3).
const NAV_ITEMS = [
  { to: '/', label: 'Обзор' },
  { to: '/etcd', label: 'etcd' },
  { to: '/clusters', label: 'Кластеры' },
  { to: '/ha', label: 'HA' },
  { to: '/alerts', label: 'Алерты' },
];

export function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();

  // Guard: session-запрос при монтировании; 401 уже редиректит apiFetch (spec §3.11).
  const session = useQuery({
    queryKey: queryKeys.session,
    queryFn: fetchSession,
    retry: false,
    staleTime: Infinity,
  });

  async function handleLogout(): Promise<void> {
    await logoutRequest();
    queryClient.clear();
    navigate('/login');
  }

  if (session.isPending)
    return (
      <Group justify="center" pt="xl">
        <Loader />
      </Group>
    );

  // Ошибка сети (не-401: 401 уже уехал редиректом) — панель недоступна, повтор.
  if (session.isError)
    return (
      <Stack align="center" pt="xl" gap="sm">
        <Text c="red">Панель недоступна</Text>
        <Button variant="light" onClick={() => void session.refetch()}>Повторить</Button>
      </Stack>
    );

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{ width: 220, breakpoint: 'sm' }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group gap="sm">
            <Text fw={700}>AdminPanel</Text>
            <StaleBadge />
          </Group>
          <Group gap="sm">
            <PollingToggle />
            <Text c="dimmed" size="sm">{session.data?.username}</Text>
            <Button size="xs" variant="light" onClick={() => void handleLogout()}>Выйти</Button>
          </Group>
        </Group>
      </AppShell.Header>
      <AppShell.Navbar p="xs">
        <Stack gap={2}>
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              label={item.label}
              component={Link}
              to={item.to}
              active={location.pathname === item.to}
            />
          ))}
        </Stack>
      </AppShell.Navbar>
      <AppShell.Main>
        <Outlet />
      </AppShell.Main>
    </AppShell>
  );
}
```

- [ ] **Шаг 7.4: Проверка типизации**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run typecheck
```
Ожидание: exit 0.

- [ ] **Шаг 7.5: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/layout
git commit -m "t07: AppLayout (guard session-query, AppShell, logout) + StaleBadge + PollingToggle (spec §3.11–3.12, §7.7–7.8)"
```

**Выход:** защищённая зона и общие элементы готовы. **Проверка:** Шаг 7.4 зелёный. **Spec:** §3.11–3.12, §7.7–7.8.

---

### Задача 8: Сборка приложения — маршрутизация и провайдеры

**Files:**
- Modify: `frontend/src/main.tsx` (полная версия вместо минимальной).
- Create: `frontend/src/App.tsx`.

**Interfaces:**
- Consumes: `LoginPage` (Задача 6), `AppLayout` (Задача 7), страницы (Задача 5), `PollingProvider` (Задача 4), `ApiError` (Задача 3).
- Produces: собранное SPA (`npm run build`).

- [ ] **Шаг 8.1: `frontend/src/App.tsx`** (spec §7.2, §4.1):

```tsx
// Маршруты SPA: /login открыт; остальное — под AppLayout-guard (spec §4.1, §7.2).
import { createBrowserRouter, Navigate } from 'react-router';
import { LoginPage } from './auth/LoginPage';
import { AppLayout } from './layout/AppLayout';
import { AlertsPage } from './pages/AlertsPage';
import { ClustersPage } from './pages/ClustersPage';
import { EtcdPage } from './pages/EtcdPage';
import { HaPage } from './pages/HaPage';
import { OverviewPage } from './pages/OverviewPage';

export const router = createBrowserRouter([
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

- [ ] **Шаг 8.2: `frontend/src/main.tsx`** — полная версия (spec §7.1, §3.8, §3.10):

```tsx
// Точка входа SPA: провайдеры Mantine (тёмная тема) → QueryClient → Polling → Router (spec §7.1).
import '@mantine/core/styles.css';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { MantineProvider } from '@mantine/core';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from 'react-router';
import { ApiError } from './api/client';
import { router } from './App';
import { PollingProvider } from './polling/PollingContext';

// Defaults (spec §3.10): 401 не ретраим (guard-реакция сразу), фокус окна не рефечит — только polling.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error) =>
        !(error instanceof ApiError && error.status === 401) && failureCount < 2,
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MantineProvider defaultColorScheme="dark">
      <QueryClientProvider client={queryClient}>
        <PollingProvider>
          <RouterProvider router={router} />
        </PollingProvider>
      </QueryClientProvider>
    </MantineProvider>
  </StrictMode>,
);
```

- [ ] **Шаг 8.3: Полная сборка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && npm run build && test -f ../src/AdminPanel.Api/wwwroot/index.html && echo SPA_OK
```
Ожидание: tsc 0 ошибок, vite build успешен, `SPA_OK`.

- [ ] **Шаг 8.4: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add frontend/src/main.tsx frontend/src/App.tsx
git commit -m "t07: маршрутизация (createBrowserRouter) и провайдеры main — тёмная Mantine, QueryClient-defaults (spec §3.7–3.10, §7.1–7.2)"
```

**Выход:** SPA собирается целиком. **Проверка:** Шаг 8.3 зелёный. **Spec:** §3.7–3.10, §4.1, §7.1–7.2.

---

### Задача 9: Backend — раздача SPA в Program.cs + integration-тесты

**Files:**
- Modify: `src/AdminPanel.Api/Program.cs`.
- Create: `src/tests/AdminPanel.IntegrationTests/SpaHostingTests.cs`.

**Interfaces:**
- Consumes: существующая коллекция `"api"` и `AuthWebFactory` (`AuthTests.cs`; креды admin/adminpw, `FixedTimeProvider Time`), `WebApplicationFactoryClientOptions`.
- Produces: хостинг-мидлвари (статика без auth + пара fallback'ов); 3 теста `SpaHostingTests`.

- [ ] **Шаг 9.1: Program.cs — статика до auth** (spec §8). Вставить после блока OpenAPI (`if (app.Environment.IsDevelopment()) { app.MapOpenApi(); }`) и ПЕРЕД строкой `app.UseAuthentication();`:

```csharp
// [t07] SPA из wwwroot — без авторизации (в бандле секретов нет, arch/01 §4/§5).
// Бандла нет (npm run build не запускался) — хост жив, отдаётся только API.
if (!Directory.Exists(app.Environment.WebRootPath))
    app.Logger.LogWarning("wwwroot пуст — SPA-бандл не собран (cd frontend && npm run build)");

// [t07] default-документ и статика; guard /api/* ниже статике не мешает.
app.UseDefaultFiles();
app.UseStaticFiles();
```

- [ ] **Шаг 9.2: Program.cs — пара fallback'ов в конце пайплайна** (spec §8). Вставить сразу ПОСЛЕ блока `app.MapHealthChecks(...);` (перед `app.Run();`):

```csharp
// [t07] неизвестные /api/* — 404 ProblemDetails, а не SPA-fallback (arch/01 §5).
app.MapFallback(
    "/api/{**_}",
    () => Results.Problem(statusCode: StatusCodes.Status404NotFound, title: "Not found"));

// [t07] SPA-fallback: клиентская маршрутизация неизвестных путей через index.html.
app.MapFallbackToFile("index.html");
```

Проверить итоговый порядок пайплайна: static files → `UseAuthentication` → `UseApiAuthorization` → эндпоинты (`MapAuthApi`, `MapInspectionApi`, `MapHealthChecks`) → два fallback'а → `app.Run()`.

- [ ] **Шаг 9.3: `src/tests/AdminPanel.IntegrationTests/SpaHostingTests.cs`** (spec §9):

```csharp
using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Раздача SPA и fallback-семантика /api/* (spec t07 §9): guard раньше fallback,
// авторизованный unknown-API-путь — 404 ProblemDetails, статика — без auth.
[Collection("api")]
public class SpaHostingTests
{
    private readonly AuthWebFactory _factory;

    public SpaHostingTests(AuthWebFactory factory) => _factory = factory;

    // Свежее окно лимитера: сдвиг времени — fixed window сбрасывается по windowId.
    private void NewRateWindow() => _factory.Time.Utc += TimeSpan.FromSeconds(61);

    [Fact]
    public async Task UnknownApiPath_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient(
            new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/api/whatever", TestContext.Current.CancellationToken);

        // Assert: guard /api/* раньше fallback'ов — 401, а не 404/index.html.
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task UnknownApiPath_WithCookie_Returns404ProblemDetails()
    {
        // Arrange: default-клиент хранит cookie; логин открывает сессию.
        NewRateWindow();
        using var client = _factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);

        // Act
        var response = await client.GetAsync("/api/whatever", TestContext.Current.CancellationToken);

        // Assert: специфичный /api-fallback бьёт SPA-fallback — 404 ProblemDetails, не index.html.
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task RootPath_WithoutCookie_IsNotUnauthorized()
    {
        // Arrange: без cookie; wwwroot в чистом чекауте пуст (ожидаем 404), с бандлом — 200.
        using var client = _factory.CreateClient(
            new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/", TestContext.Current.CancellationToken);

        // Assert: статика/SPA-fallback не требуют авторизации; устойчиво к наличию бандла.
        response.StatusCode.Should().NotBe(HttpStatusCode.Unauthorized);
        ((int)response.StatusCode).Should().BeLessThan(500);
    }
}
```

- [ ] **Шаг 9.4: Сборка решения**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet build src/AdminPanel.slnx
```
Ожидание: success, 0 warnings (TreatWarningsAsErrors не сработал).

- [ ] **Шаг 9.5: Прогон новых тестов (Docker не нужен — коллекция "api" не тянет Testcontainers)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~SpaHostingTests"
```
Ожидание: 3 passed, 0 failed.

- [ ] **Шаг 9.6: Регресс auth/inspection (без Docker — только не-Testcontainers наборы; если фильтр подтянет контейнеры и их нет — Docker поднять или отложить на Шаг 10.4 полный прогон)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AuthTests|FullyQualifiedName~HealthzTests"
```
Ожидание: все passed (поведение guard/healthz не изменилось).

- [ ] **Шаг 9.7: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add src/AdminPanel.Api/Program.cs src/tests/AdminPanel.IntegrationTests/SpaHostingTests.cs
git commit -m "t07: раздача SPA (UseDefaultFiles/UseStaticFiles без auth, wwwroot-warning) + /api-fallback 404 + SPA-fallback + 3 integration-теста (spec §8–9)"
```

**Выход:** хост отдаёт SPA; fallback-семантика зафиксирована тестами. **Проверка:** Шаги 9.4–9.6 зелёные. **Spec:** §3.16, §8, §9, §13.3.

---

### Задача 10: Roadmap-деливерабл и финальный прогон

**Files:**
- Modify: `arch/roadmap/frontend.md` (удалить пункт `t07-frontend-base`).
- Проверочные запуски без правок кода.

**Interfaces:**
- Consumes: всё из Задач 2–9.
- Produces: финальное состояние ветки — готово к ревью и мержу.

- [ ] **Шаг 10.1: Roadmap-деливерабл** (spec §12). В `arch/roadmap/frontend.md` удалить пункт целиком (строки `- \`t07-frontend-base\` ← …` — 8 строк описания до конца пункта). НЕ трогать `← t07-frontend-base` в пунктах t08/t09 (правило roadmap-README: зависимость от слитой задачи сохраняется до мержа зависимых; прецедент t02 §12). Убедиться, что t08/t09 остались.

- [ ] **Шаг 10.2: Чистая сборка фронта (воспроизводимость npm ci)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base/frontend && rm -rf node_modules && npm ci && npm run build && test -f ../src/AdminPanel.Api/wwwroot/index.html && echo SPA_OK
```
Ожидание: npm ci успешен по lock-файлу, build успешен, `SPA_OK`. Это же оставляет свежий бандл для Шага 10.5.

- [ ] **Шаг 10.3: Полная сборка .NET**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet build src/AdminPanel.slnx
```
Ожидание: success, 0 warnings.

- [ ] **Шаг 10.4: Полный прогон тестов (нужен Docker — Testcontainers в существующих integration-тестах)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet test src/AdminPanel.slnx
```
Ожидание: все зелёные; ориентир состава: 203 unit + 62 существующих integration + 3 новых SpaHostingTests = 268 total (критерий приёмки — «0 failed»; если фактическое число тестов отличается от ориентира — зафиксировать фактическое в отчёте исполнения, это не блокер).

- [ ] **Шаг 10.5: Ручной сценарий «dotnet run отдаёт SPA» (spec §13.4)**

Запуск (в отдельном терминале/фон):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && dotnet run --project src/AdminPanel.Api
```

Затем (сервер поднялся, ~5–10 с):

```bash
curl -s -o /dev/null -w '%{http_code} %{content_type}\n' http://localhost:5000/
curl -s -o /dev/null -w '%{http_code} %{content_type}\n' http://localhost:5000/clusters
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5000/api/whatever
curl -si -X POST http://localhost:5000/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' | head -8
```

Ожидания:
- `200 text/html` (index.html);
- `200 text/html` (SPA-fallback на `/clusters`);
- `401` (guard, без cookie);
- логин → `204` + `Set-Cookie: adminpanel_session=…` (dev-профиль: admin/admin, `AllowHttp=true`).
Далее с cookie (подставить значение из Set-Cookie):

```bash
curl -s -o /dev/null -w '%{http_code} %{content_type}\n' http://localhost:5000/api/whatever -H "Cookie: adminpanel_session=<значение>"
```
Ожидание: `404 application/problem+json`.

Браузерная часть (исполнитель делает сам, фиксирует в отчёте):
- открыть `http://localhost:5000/` → редирект на `/login` → вход admin/admin → layout (навигация Обзор/etcd/Кластеры/HA/Алерты, username в шапке, бейдж, переключатель) → F5 сохраняет выбранный интервал → «Вышел» → `/login`;
- прямой заход на `/ha` без сессии → `/login?from=%2Fha`, после входа — возврат на `/ha`;
- Network-вкладка: `/api/overview` опрашивается с выбранным интервалом (default 5 с);
- выбор «off» в переключателе — опрос `/api/overview` прекращается (новых запросов нет);
- при недоступном overview (503 «снапшот не собран» при выключенном etcd, либо сеть) — бейдж красный «нет данных», панель остаётся работоспособной (навигация/страницы живут).

Опционально (dev-режим proxy): `npm run dev` в `frontend/` → `http://localhost:5173` → тот же флоу (spec §14).

Остановить сервер.

- [ ] **Шаг 10.6: Чистота git**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git status --short
```
Ожидание: только изменённый `arch/roadmap/frontend.md`; `src/AdminPanel.Api/wwwroot/` и `frontend/node_modules/` не видны (в .gitignore). `grep -r PackageReference src --include='*.csproj'` — без изменений против main.

- [ ] **Шаг 10.7: Финальный коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t07-frontend-base && git add arch/roadmap/frontend.md
git commit -m "t07: roadmap-деливерабл — удаление пункта t07-frontend-base (spec §12; мерж-гейт)"
```

**Выход:** ветка готова к ревью: бандл собирается, хост отдаёт SPA, login работает, все тесты зелёные. **Проверка:** Шаги 10.2–10.6 зелёные. **Spec:** §12–13 (все критерии приёмки закрыты; чек-лист соответствия ниже).

---

## Соответствие spec → задачи (чек-лист самопроверки плана)

| Требование spec | Задача |
|---|---|
| §3.1–3.3 версии, .npmrc, registry | 2 |
| §3.2 критерий TS 7 vs 5.9 | 2 (Шаг 2.11) |
| §3.4–3.5 без ESLint/фронт-тестов | план в целом (нет задач на них — сознательно) |
| §3.6 структура каталогов | 2–8 |
| §3.7 роутинг data-режим | 8 |
| §3.8 Mantine тёмная тема | 8 |
| §3.9 polling Context + localStorage | 4, 7 |
| §3.10 QueryClient defaults | 8 |
| §3.11 guard (401-редирект + session-query) | 3 (client), 7 (AppLayout), 6 (from) |
| §3.12 stale-бейдж | 4 (formatAge), 7 (StaleBadge) |
| §3.13 apiFetch/ApiError/ProblemDetails | 3 |
| §3.14 типы DTO | 3 |
| §3.15 queryKeys/fetch-функции | 3 |
| §3.16 backend Program.cs | 9 |
| §3.17–3.19 .gitignore, коммиты, проверка | 2, 10 |
| §4 контракт (маршруты, API-поведение) | 5–8 |
| §5 дерево файлов | 2–9 |
| §6 конфигурация frontend | 2 |
| §7.1–7.10 компоненты | 8, 8, 3, 3, 4, 6, 7, 7, 5, 4 |
| §8 Program.cs дельта | 9 |
| §9 integration-тесты | 9 |
| §10 ограничения (нет лишнего) | все задачи — состав изменений строго по дереву §5 |
| §11 arch-правки | 1 (коммит сделанного в Фазе 1) |
| §12 roadmap-деливерабл | 10 |
| §13 критерии приёмки 1–9 | 10 (Шаги 10.2–10.6 + проверки задач 2, 9) |

Замечания исполнения: план не содержит placeholder'ов — каждый шаг имеет полный код или точную команду с ожидаемым результатом; сигнатуры согласованы между задачами (`queryKeys`, `apiFetch`, `usePollingIntervalMs`, имена компонентов — единожды определены в Interfaces).
