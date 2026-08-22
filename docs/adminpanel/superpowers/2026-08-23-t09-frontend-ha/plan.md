# План реализации t09-frontend-ha

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Наполнить панели HA и Alerts SPA AdminPanel данными готового API t06, добавить счётчики critical/warning в навигацию и HA-сводку в Overview — без единой правки бэкенда.

**Architecture:** Замена двух заглушек t07 страницами на TanStack Query + polling `usePollingIntervalMs`; детали HA-скопа — новый маршрут `/ha/:scope`; фильтр severity и навигационные счётчики — клиентские по одному запросу `queryKeys.alerts()` (ключ дедуплицируется с Overview-лентой); HA-сводка Overview — агрегация `queryKeys.haScopes` на клиенте (решение t06 §3.19: `OverviewDto` не расширяется). Все таблицы — Mantine `Table` + `Table.ScrollContainer`.

**Tech Stack:** React 19 + Vite + TypeScript 7 (strict, `noUnusedLocals`), Mantine 9.5.2 (dark), TanStack Query 5, React Router 8; ASP.NET Core Minimal API (.NET 10) — без изменений; xunit v3 + FluentAssertions + Testcontainers (регрессионный барьер).

**Spec:** `docs/superpowers/2026-08-23-t09-frontend-ha/spec.md` — план аргументируется от spec; исполнитель читает оба документа. Ссылки «spec §N» ниже — на этот файл.

## Global Constraints

- Работа только в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t09-frontend-ha`; все пути ниже — от его корня.
- Один Task = один коммит `t09: <суть>` (исключения оговорены: Task 8/10 без коммита, Task 9 — два коммита roadmap по прецеденту t08). Коммитить только указанные файлы (`git add <явные пути>`), не `git add -A`.
- Идентификаторы английские; комментарии в коде и ВСЕ тексты UI — русские (spec §2). Значения внешних канонов (`severity`, `kind`, state/role Patroni) показываются как есть, без перевода (spec §2, §4.7).
- DTO фронта — строго фактические C#-records (spec §8): слой `api/` (`dto.ts`, `queries.ts`, `client.ts`) НЕ правится.
- Бэкенд (`src/**`, тесты .NET, `Directory.Packages.props`) — без изменений (spec §1, §9); `dotnet build/test` — регрессионный барьер.
- Фронтенд-тестов нет (spec §10): цикл каждого шага — код → `npm run build` (в него входит `tsc --noEmit`) → коммит; поведение проверяется HTTP-сценарием Task 10.
- Новых npm/NuGet-пакетов нет. Mantine 9 API сверено с фактическими типами в `frontend/node_modules`: `NavLink` имеет `rightSection?: React.ReactNode`; `Accordion` экспортирует статические `Accordion.Item {value}` / `Accordion.Control` / `Accordion.Panel`; `SegmentedControl {data, value, onChange}` (используется в `PollingToggle`); таблицы — `Table.ScrollContainer` (заметка плана t08). `node_modules` уже установлен в worktree (`npm ci` выполнен); для чистого чекаута: `cd frontend && npm ci`.
- Не трогать: `api/*`, `PollingContext.tsx`, `StaleBadge.tsx`, `PollingToggle.tsx`, `LoginPage.tsx`, `LoadState.tsx`, `BucketStateBadge.tsx`, `EtcdPage.tsx`, `ClustersPage.tsx`, `ClusterDetailsPage.tsx`, `pages/cluster-details/*`, `src/**` (spec §6).
- `TreatWarningsAsErrors=true` на бэкенде — сборка обязана быть без warnings.
- Известный флак: `EtcdSnapshotIntegrationTests.Refresher_EnrichesSnapshot_FromProbeState` (тег `t90-fix-probe-enrich-flaky` в `arch/roadmap/ha.md`) может падать в полном integration-прогоне — не блокирует приёмку, если изолированный перезапуск зелёный (Task 8, шаг 8.4).

---

### Task 1: Коммит документации (spec + plan + arch/03)

**Files:**
- Already created: `docs/superpowers/2026-08-23-t09-frontend-ha/spec.md`, `docs/superpowers/2026-08-23-t09-frontend-ha/plan.md`, `arch/03-panels.md` (2 правки из spec §11)

**Interfaces:** — (документация).

- [ ] **Step 1.1: Проверить состав изменений**

Run: `git status --short`
Expected: `M arch/03-panels.md` и `?? docs/superpowers/2026-08-23-t09-frontend-ha/`; `?? frontend/node_modules/` нет (игнорируется); больше ничего.

- [ ] **Step 1.2: Коммит**

```bash
git add arch/03-panels.md docs/superpowers/2026-08-23-t09-frontend-ha/
git commit -m "t09: spec/plan и правки arch/03 (сводка HA из /api/ha, счётчики алертов в навигации)"
```

**Выход:** docs+arch в истории; рабочее дерево чистое.

**Проверка:** `git log --oneline -1` → коммит `t09: spec/plan…`; `git status --short` → пусто.

**Spec:** §11 (правки arch внесены до spec), деливерабл documentation.

---

### Task 2: AlertSeverityBadge + замена inline-условия в ленте Overview

**Files:**
- Create: `frontend/src/components/AlertSeverityBadge.tsx`
- Modify: `frontend/src/pages/OverviewPage.tsx` (лента `AlertsFeedSection`: Badge → компонент + сужение типа `severity`)

**Interfaces:**
- Produces: `AlertSeverityBadge({ severity }: { severity: AlertSeverityName })` — используется Task 4 (AlertsPage); тип `AlertSeverityName` уже есть в `api/dto.ts` (не правится).

- [ ] **Step 2.1: Создать компонент**

`frontend/src/components/AlertSeverityBadge.tsx` — полный код:

```tsx
// Цветовая карта severity алертов — единый источник всех панелей (t09 spec §4.12).
// Текст — канон-строка без перевода: идентификатор канона arch/03 §4.
import { Badge } from '@mantine/core';
import type { AlertSeverityName } from '../api/dto';

const SEVERITY_COLORS: Record<AlertSeverityName, string> = {
  critical: 'red',
  warning: 'yellow',
  info: 'gray',
};

export function AlertSeverityBadge({ severity }: { severity: AlertSeverityName }) {
  return <Badge color={SEVERITY_COLORS[severity]} variant="light">{severity}</Badge>;
}
```

- [ ] **Step 2.2: Подключить в ленте Overview**

`frontend/src/pages/OverviewPage.tsx` — три правки (паттерн карты цветов как `BucketStateBadge` t08 §4.17):

1) Импорт типа — заменить строку

```tsx
import type { OverviewDto } from '../api/dto';
```

на

```tsx
import type { AlertSeverityName, OverviewDto } from '../api/dto';
```

2) Импорт компонента — после строки `import { BucketStateBadge } from '../components/BucketStateBadge';` добавить

```tsx
import { AlertSeverityBadge } from '../components/AlertSeverityBadge';
```

3) В `AlertsFeedSection` — сузить тип пропса `rows` (данные всегда `AlertDto`-производные) и заменить inline-бейдж:

```tsx
rows: { id: string; severity: AlertSeverityName; kind: string; target: string; message: string; sinceUnix: number | null }[];
```

(было `severity: string`) и в теле строку

```tsx
<Badge color={a.severity === 'critical' ? 'red' : 'yellow'} variant="light">{a.severity}</Badge>
```

заменить на

```tsx
<AlertSeverityBadge severity={a.severity} />
```

`Badge` остаётся в импортах файла — он используется карточками `EtcdCard`/`ClustersCard`/`AlertsCard`/`MovesSection`.

- [ ] **Step 2.3: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок (в т.ч. `noUnusedLocals`), `vite build` пишет `../src/AdminPanel.Api/wwwroot/`.

- [ ] **Step 2.4: Коммит**

```bash
git add frontend/src/components/AlertSeverityBadge.tsx frontend/src/pages/OverviewPage.tsx
git commit -m "t09: AlertSeverityBadge — единая карта цветов severity (+лента Overview)"
```

**Выход:** общий бейдж severity; лента визуально идентична (red/yellow те же).

**Проверка:** `git show --stat HEAD` → ровно 2 файла.

**Spec:** §4.12, §4.14.

---

### Task 3: Счётчики critical/warning в навигации (AlertsNavCounters + AppLayout)

**Files:**
- Create: `frontend/src/layout/AlertsNavCounters.tsx`
- Modify: `frontend/src/layout/AppLayout.tsx` (rightSection у пункта «Алерты»)

**Interfaces:**
- Consumes: `fetchAlerts`, `queryKeys.alerts()` из `api/queries.ts`; `usePollingIntervalMs` из `polling/PollingContext`.
- Produces: `AlertsNavCounters()` — без пропсов; монтируется только в `AppLayout`.

- [ ] **Step 3.1: Создать компонент**

`frontend/src/layout/AlertsNavCounters.tsx` — полный код:

```tsx
// Счётчики critical/warning у пункта «Алерты» в навигации (t09 spec §4.2).
// Показываются только при N > 0; pending/ошибка/пусто — секция пуста (навигация не мигает).
import { useQuery } from '@tanstack/react-query';
import { Badge, Group } from '@mantine/core';
import { fetchAlerts, queryKeys } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';

export function AlertsNavCounters() {
  // Тот же ключ, что у Overview-ленты и Alerts-страницы — один запрос на тик (t09 spec §3).
  const { data } = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: usePollingIntervalMs(),
  });

  if (data === undefined) return null;
  const critical = data.filter((a) => a.severity === 'critical').length;
  const warning = data.filter((a) => a.severity === 'warning').length;
  if (critical === 0 && warning === 0) return null;
  return (
    <Group gap={4} wrap="nowrap">
      {critical > 0 ? <Badge color="red" variant="light" size="xs">{critical}</Badge> : null}
      {warning > 0 ? <Badge color="yellow" variant="light" size="xs">{warning}</Badge> : null}
    </Group>
  );
}
```

- [ ] **Step 3.2: Вставить в AppLayout**

`frontend/src/layout/AppLayout.tsx` — две правки:

1) Импорт (после `import { StaleBadge } from './StaleBadge';`):

```tsx
import { AlertsNavCounters } from './AlertsNavCounters';
```

2) В рендере навигации — NavLink при map получает `rightSection` для пункта «Алерты» (у Mantine 9 `NavLink` имеет проп `rightSection?: React.ReactNode`):

```tsx
{NAV_ITEMS.map((item) => (
  <NavLink
    key={item.to}
    label={item.label}
    component={Link}
    to={item.to}
    active={item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to)}
    rightSection={item.to === '/alerts' ? <AlertsNavCounters /> : undefined}
  />
))}
```

(добавлена только последняя строка — остальное без изменений).

- [ ] **Step 3.3: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок, бандл собирается.

- [ ] **Step 3.4: Коммит**

```bash
git add frontend/src/layout/AlertsNavCounters.tsx frontend/src/layout/AppLayout.tsx
git commit -m "t09: счётчики critical/warning у пункта «Алерты» в навигации"
```

**Выход:** бейджи-числа справа от пункта «Алерты» при наличии алертов; запрос дедуплицируется с Overview-лентой.

**Проверка:** `git show --stat HEAD` → ровно 2 файла.

**Spec:** §4.2 (частота — текущий polling-интервал, отдельные query-опции не заводятся).

---

### Task 4: AlertsPage — таблица всех алертов с фильтром severity

**Files:**
- Modify: `frontend/src/pages/AlertsPage.tsx` (полная замена заглушки)

**Interfaces:**
- Consumes: `AlertSeverityBadge` (Task 2), `fetchAlerts`, `queryKeys.alerts()`, `ErrorSection`/`LoadingSection`, `usePollingIntervalMs`, `formatUnix`/`formatUnixAge`.
- Produces: `AlertsPage()` — уже экспортирована и подключена маршрутом `/alerts` в `App.tsx` (не правится).

- [ ] **Step 4.1: Заменить страницу**

`frontend/src/pages/AlertsPage.tsx` — полный код:

```tsx
// Панель «Алерты»: таблица всех алертов с severity-цветами и клиентским
// фильтром severity (t09 spec §4.10–4.11). Один запрос всех алертов —
// ключ дедуплицируется с Overview-лентой и навигационными счётчиками.
import { useQuery } from '@tanstack/react-query';
import { Group, SegmentedControl, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import type { AlertDto, AlertSeverityName } from '../api/dto';
import { fetchAlerts, queryKeys } from '../api/queries';
import { AlertSeverityBadge } from '../components/AlertSeverityBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix, formatUnixAge } from '../utils/format';

// Значения фильтра: «все» либо конкретный severity (t09 spec §4.10).
type SeverityFilter = 'all' | AlertSeverityName;

// Ранг severity для сортировки: critical раньше warning раньше info (t09 spec §4.10).
const SEVERITY_RANK: Record<AlertSeverityName, number> = {
  critical: 0,
  warning: 1,
  info: 2,
};

// Сортировка: severity-ранг, внутри — новые сверху, sinceUnix null — в конец.
function sortAlertRows(a: AlertDto, b: AlertDto): number {
  if (SEVERITY_RANK[a.severity] !== SEVERITY_RANK[b.severity])
    return SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity];
  return (b.sinceUnix ?? -1) - (a.sinceUnix ?? -1);
}

export function AlertsPage() {
  const intervalMs = usePollingIntervalMs();
  const [filter, setFilter] = useState<SeverityFilter>('all');
  const alerts = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: intervalMs,
  });

  if (alerts.data === undefined)
    return alerts.isError ? (
      <ErrorSection error={alerts.error} onRetry={() => void alerts.refetch()} />
    ) : (
      <LoadingSection />
    );

  const all = [...alerts.data].sort(sortAlertRows);
  const rows = filter === 'all' ? all : all.filter((a) => a.severity === filter);
  return (
    <Stack gap="md">
      <Title order={2}>Алерты</Title>
      <Group justify="space-between">
        <SegmentedControl
          value={filter}
          onChange={(value) => setFilter(value as SeverityFilter)}
          data={[
            { value: 'all', label: 'все' },
            { value: 'critical', label: 'critical' },
            { value: 'warning', label: 'warning' },
            { value: 'info', label: 'info' },
          ]}
        />
        <Text size="sm" c="dimmed">{rows.length} из {all.length}</Text>
      </Group>
      {rows.length === 0 ? (
        filter === 'all' ? (
          <Text c="teal" size="sm">Алертов нет</Text>
        ) : (
          <Text c="dimmed" size="sm">Нет алертов этого уровня</Text>
        )
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Severity</Table.Th>
                <Table.Th>Kind</Table.Th>
                <Table.Th>Target</Table.Th>
                <Table.Th>Сообщение</Table.Th>
                <Table.Th>Присутствует с</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {rows.map((a) => (
                <AlertRow key={a.id} alert={a} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Stack>
  );
}

// Строка алерта: severity-бейдж, kind с details в Tooltip, since-возраст (t09 spec §4.11).
function AlertRow({ alert }: { alert: AlertDto }) {
  const details = alert.details === null ? [] : Object.entries(alert.details);
  return (
    <Table.Tr>
      <Table.Td><AlertSeverityBadge severity={alert.severity} /></Table.Td>
      <Table.Td>
        {details.length > 0 ? (
          <Tooltip multiline label={details.map(([k, v]) => `${k}: ${v}`).join('\n')}>
            <Text ff="monospace" size="sm">{alert.kind}</Text>
          </Tooltip>
        ) : (
          <Text ff="monospace" size="sm">{alert.kind}</Text>
        )}
      </Table.Td>
      <Table.Td><Text ff="monospace" size="sm" c="dimmed">{alert.target}</Text></Table.Td>
      <Table.Td><Text size="sm">{alert.message}</Text></Table.Td>
      <Table.Td>
        <Tooltip label={formatUnix(alert.sinceUnix)}>
          <span>
            <Text size="sm" c="dimmed">
              {alert.sinceUnix === null ? '—' : `с ${formatUnixAge(alert.sinceUnix)}`}
            </Text>
          </span>
        </Tooltip>
      </Table.Td>
    </Table.Tr>
  );
}
```

- [ ] **Step 4.2: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок, бандл собирается.

- [ ] **Step 4.3: Коммит**

```bash
git add frontend/src/pages/AlertsPage.tsx
git commit -m "t09: AlertsPage — таблица алертов, клиентский фильтр severity"
```

**Выход:** панель `/alerts` показывает все алерты, фильтр переключает мгновенно (тот же кэш-ключ).

**Проверка:** `git show --stat HEAD` → 1 файл.

**Spec:** §4.10–4.11, §5 (макет Alerts).

---

### Task 5: Детали HA-скопа — форматтер, страница, маршрут `/ha/:scope`

Порядок «детали раньше списка» (отличается от ориентировки main-агента): HaPage (Task 6) и карточка Overview (Task 7) ссылаются на `/ha/:scope` — маршрут обязан существовать к их коммиту; back-link «← HA» из деталей на `/ha` работает и на заглушке. Обратная зависимость слабее (зависимость ссылок > зависимость back-link).

**Files:**
- Modify: `frontend/src/utils/format.ts` (+`formatIsoAge` в конец файла)
- Create: `frontend/src/pages/HaScopeDetailsPage.tsx`
- Modify: `frontend/src/App.tsx` (маршрут `ha/:scope`)

**Interfaces:**
- Consumes: `fetchHaScope`, `queryKeys.haScope(scope)`, `HaMemberDto` из `api/dto.ts`; `ErrorSection`/`LoadingSection` (включая ветку `notFound`); `formatBytes`/`formatIso`.
- Produces: `formatIsoAge(iso: string | null): string` — используется только этой страницей; маршрут `/ha/:scope` → `HaScopeDetailsPage` (Task 6/7 ссылаются на него).

- [ ] **Step 5.1: Добавить форматтер**

В конец `frontend/src/utils/format.ts`:

```ts
// Относительный возраст от ISO-штампа (DateTimeOffset-строка) — для probeAtUtc
// (t09 spec §4.16); null → «—».
export function formatIsoAge(iso: string | null): string {
  return iso === null ? '—' : formatAge(Date.now() - Date.parse(iso));
}
```

- [ ] **Step 5.2: Создать страницу деталей**

`frontend/src/pages/HaScopeDetailsPage.tsx` — полный код:

```tsx
// Детали HA-скопа: шапка (лидер, optime, кластер/шард), таблица членов с
// probe-статусом, raw config свёрнуто (t09 spec §4.4–4.9).
import { useQuery } from '@tanstack/react-query';
import { Accordion, Anchor, Badge, Group, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link, useParams } from 'react-router';
import type { HaMemberDto } from '../api/dto';
import { fetchHaScope, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso, formatIsoAge } from '../utils/format';

// Карта ролей Patroni: русские подписи известных, канон — в Tooltip (t09 spec §4.6);
// master — violet, рифма с Badge «лидер» etcd-панели (t08 EtcdPage).
const ROLE_META: Record<string, { color: string; label: string }> = {
  master: { color: 'violet', label: 'мастер' },
  replica: { color: 'blue', label: 'реплика' },
  sync_standby: { color: 'teal', label: 'sync-standby' },
};

// Карта цветов состояний Patroni: строка как есть, без перевода — внешний
// идентификатор (t09 spec §4.7); неизвестные — серые (не ошибка).
const STATE_COLORS: Record<string, string> = {
  running: 'teal',
  streaming: 'teal',
  stopped: 'red',
  starting: 'yellow',
  'creating replica': 'yellow',
  restart: 'yellow',
  'crash reinit': 'yellow',
  waiting: 'yellow',
};

export function HaScopeDetailsPage() {
  const { scope = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.haScope(scope),
    queryFn: () => fetchHaScope(scope),
    refetchInterval: intervalMs,
  });

  // 404 (скоп исчез между тиками) → notFound-контент; 503/сеть → ErrorSection (t08 §4.15).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection
        error={query.error}
        onRetry={() => void query.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Скоп не найден</Text>
            <Anchor component={Link} to="/ha" size="sm">← HA</Anchor>
          </Stack>
        }
      />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  return (
    <Stack gap="md">
      <div>
        <Anchor component={Link} to="/ha" size="sm">← HA</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2} ff="monospace">{data.scope}</Title>
          {data.matched ? null : (
            <Tooltip label="scope не сопоставлен кластеру (arch/02 §7)">
              <Badge color="yellow" variant="light">unmatched</Badge>
            </Tooltip>
          )}
        </Group>
        <Group gap="sm" mt={4}>
          <Text c="dimmed" size="sm">
            Кластер/шард:{' '}
            {data.cluster === null ? '—' : (
              <Anchor component={Link} to={`/clusters/${data.cluster}`} size="sm">
                {data.cluster}/{data.shard ?? '—'}
              </Anchor>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            Лидер:{' '}
            {data.leaderName === null ? (
              <Badge color={data.matched ? 'red' : 'gray'} variant="light">нет лидера</Badge>
            ) : (
              <Text ff="monospace" size="sm" span>{data.leaderName}</Text>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            optime лидера:{' '}
            <Text ff="monospace" size="sm" span>{data.optimeLeader ?? '—'}</Text>
          </Text>
        </Group>
      </div>
      <MembersTable members={data.members} leaderName={data.leaderName} />
      {data.rawConfig === null ? null : (
        <Accordion>
          <Accordion.Item value="raw-config">
            <Accordion.Control>Raw config</Accordion.Control>
            <Accordion.Panel>
              <Text ff="monospace" size="sm" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {data.rawConfig}
              </Text>
            </Accordion.Panel>
          </Accordion.Item>
        </Accordion>
      )}
    </Stack>
  );
}

// Таблица членов: имя (+метка лидера), адрес, роль, состояние, timeline, лаг,
// probe-статус (t09 spec §4.5).
function MembersTable({ members, leaderName }: { members: HaMemberDto[]; leaderName: string | null }) {
  if (members.length === 0) return <Text c="dimmed">Члены не найдены</Text>;
  return (
    <Table.ScrollContainer minWidth={900}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Имя</Table.Th>
            <Table.Th>Адрес</Table.Th>
            <Table.Th>Роль</Table.Th>
            <Table.Th>Состояние</Table.Th>
            <Table.Th>Timeline</Table.Th>
            <Table.Th>Лаг</Table.Th>
            <Table.Th>Проба</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {members.map((m) => (
            <MemberRow key={m.name} member={m} isLeader={m.name === leaderName} />
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}

function MemberRow({ member, isLeader }: { member: HaMemberDto; isLeader: boolean }) {
  return (
    <Table.Tr>
      <Table.Td>
        <Group gap="xs" wrap="nowrap">
          <Text ff="monospace" size="sm">{member.name}</Text>
          {isLeader ? <Badge color="violet" variant="light">лидер</Badge> : null}
        </Group>
      </Table.Td>
      <Table.Td>
        <Text ff="monospace" size="sm">
          {member.port === null ? member.host : `${member.host}:${member.port}`}
        </Text>
      </Table.Td>
      <Table.Td><RoleBadge role={member.role} /></Table.Td>
      <Table.Td><StateBadge state={member.state} /></Table.Td>
      <Table.Td>{member.timeline ?? '—'}</Table.Td>
      <Table.Td>{formatBytes(member.lagBytes)}</Table.Td>
      <Table.Td><ProbeCell member={member} /></Table.Td>
    </Table.Tr>
  );
}

// Роль: известные — Badge с русской подписью, канон в Tooltip; прочие — серым (t09 spec §4.6).
function RoleBadge({ role }: { role: string | null }) {
  if (role === null) return <Text c="dimmed">—</Text>;
  const meta = ROLE_META[role];
  return (
    <Tooltip label={role}>
      <Badge color={meta?.color ?? 'gray'} variant="light">{meta?.label ?? role}</Badge>
    </Tooltip>
  );
}

// Состояние: строка Patroni как есть, цвет по карте (t09 spec §4.7).
function StateBadge({ state }: { state: string | null }) {
  if (state === null) return <Text c="dimmed">—</Text>;
  return <Badge color={STATE_COLORS[state] ?? 'gray'} variant="light">{state}</Badge>;
}

// Probe-статус: ошибка / возраст с абсолютом в Tooltip / «—» — проб не было (t09 spec §4.8).
function ProbeCell({ member }: { member: HaMemberDto }) {
  if (member.probeError !== null)
    return (
      <Group gap="xs" wrap="nowrap">
        <Tooltip label={member.probeError} multiline>
          <Badge color="red" variant="light">ошибка</Badge>
        </Tooltip>
        {member.probeAtUtc === null ? null : (
          <Text size="sm" c="dimmed">{formatIsoAge(member.probeAtUtc)}</Text>
        )}
      </Group>
    );
  if (member.probeAtUtc !== null)
    return (
      <Tooltip label={formatIso(member.probeAtUtc)}>
        <span><Text size="sm" c="dimmed">{formatIsoAge(member.probeAtUtc)}</Text></span>
      </Tooltip>
    );
  return <Text c="dimmed">—</Text>;
}
```

- [ ] **Step 5.3: Добавить маршрут**

`frontend/src/App.tsx` — две правки:

1) Импорт (по алфавиту, после `HaPage`):

```tsx
import { HaScopeDetailsPage } from './pages/HaScopeDetailsPage';
```

2) Маршрут — строку

```tsx
{ path: 'ha', element: <HaPage /> },
```

заменить на

```tsx
{ path: 'ha', element: <HaPage /> },
{ path: 'ha/:scope', element: <HaScopeDetailsPage /> },
```

- [ ] **Step 5.4: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок, бандл собирается.

- [ ] **Step 5.5: Коммит**

```bash
git add frontend/src/utils/format.ts frontend/src/pages/HaScopeDetailsPage.tsx frontend/src/App.tsx
git commit -m "t09: HaScopeDetailsPage — шапка/члены/probe-статус/raw config + маршрут /ha/:scope"
```

**Выход:** `/ha/<scope>` доступен по прямому URL; NavLink «HA» активен на нём (`startsWith('/ha')` — уже работает).

**Проверка:** `git show --stat HEAD` → ровно 3 файла.

**Spec:** §4.4–4.9, §4.16, §5 (макет HA details).

---

### Task 6: HaPage — список scope'ов

**Files:**
- Modify: `frontend/src/pages/HaPage.tsx` (полная замена заглушки)

**Interfaces:**
- Consumes: `fetchHaScopes`, `queryKeys.haScopes`, `HaScopeSummaryDto`, `formatBytes`, маршрут `/ha/:scope` (Task 5).
- Produces: `HaPage()` — уже экспортирована и подключена маршрутом `ha` в `App.tsx` (не правится).

- [ ] **Step 6.1: Заменить страницу**

`frontend/src/pages/HaPage.tsx` — полный код:

```tsx
// Панель «HA»: список scope'ов — лидер, члены healthy/total, макс. лаг,
// пометка unmatched (t09 spec §4.3).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import type { HaScopeSummaryDto } from '../api/dto';
import { fetchHaScopes, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes } from '../utils/format';

export function HaPage() {
  const intervalMs = usePollingIntervalMs();
  const scopes = useQuery({
    queryKey: queryKeys.haScopes,
    queryFn: fetchHaScopes,
    refetchInterval: intervalMs,
  });

  if (scopes.data === undefined)
    return scopes.isError ? (
      <ErrorSection error={scopes.error} onRetry={() => void scopes.refetch()} />
    ) : (
      <LoadingSection />
    );

  return (
    <Stack gap="md">
      <Title order={2}>HA</Title>
      {scopes.data.length === 0 ? (
        <Text c="dimmed">HA-scope'ы не найдены</Text>
      ) : (
        <Table.ScrollContainer minWidth={700}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Scope</Table.Th>
                <Table.Th>Кластер/шард</Table.Th>
                <Table.Th>Лидер</Table.Th>
                <Table.Th>Члены</Table.Th>
                <Table.Th>Макс. лаг</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {scopes.data.map((s) => (
                <ScopeRow key={s.scope} scope={s} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Stack>
  );
}

// Строка скопа: «нет лидера» красным только у matched — рифма с алертом
// shard-no-leader (t06 §3.10); unmatched — чужой скоп, не алерт (arch/02 §7).
function ScopeRow({ scope }: { scope: HaScopeSummaryDto }) {
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/ha/${scope.scope}`} size="sm" ff="monospace">
          {scope.scope}
        </Anchor>
      </Table.Td>
      <Table.Td>
        {scope.matched ? (
          <Text size="sm" ff="monospace">{scope.cluster ?? '—'}/{scope.shard ?? '—'}</Text>
        ) : (
          <Tooltip label="scope не сопоставлен кластеру (arch/02 §7)">
            <span>
              <Text size="sm" c="dimmed" span>— </Text>
              <Badge color="yellow" variant="light">unmatched</Badge>
            </span>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        {scope.leaderName === null ? (
          scope.matched ? (
            <Badge color="red" variant="light">нет лидера</Badge>
          ) : (
            <Text size="sm" c="dimmed">нет лидера</Text>
          )
        ) : (
          <Text ff="monospace" size="sm">{scope.leaderName}</Text>
        )}
      </Table.Td>
      <Table.Td>
        <Text
          size="sm"
          ff="monospace"
          c={scope.membersHealthy < scope.membersTotal ? 'yellow' : undefined}
        >
          {scope.membersHealthy}/{scope.membersTotal}
        </Text>
      </Table.Td>
      <Table.Td>{formatBytes(scope.lagMaxBytes)}</Table.Td>
    </Table.Tr>
  );
}
```

- [ ] **Step 6.2: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок, бандл собирается.

- [ ] **Step 6.3: Коммит**

```bash
git add frontend/src/pages/HaPage.tsx
git commit -m "t09: HaPage — список HA-скопов с лидерами/членами/лагом"
```

**Выход:** `/ha` — таблица скопов, ссылки ведут на детали Task 5.

**Проверка:** `git show --stat HEAD` → 1 файл.

**Spec:** §4.3, §5 (макет HA).

---

### Task 7: HA-карточка в Overview (замена заглушки t09)

**Files:**
- Modify: `frontend/src/pages/OverviewPage.tsx` (запрос `haScopes` + компонент `HaCard` вместо заглушки)

**Interfaces:**
- Consumes: `fetchHaScopes`, `queryKeys.haScopes`, `HaScopeSummaryDto`, маршрут `/ha/:scope` (Task 5).
- Produces: — (финальный экран; других потребителей нет).

- [ ] **Step 7.1: Обновить шапку-комментарий и импорты**

Первая строка файла (шапка t08 устарела — HA-карточка реализуется здесь):

```tsx
// Панель Обзор: карточки etcd/кластеров/алертов/HA, активные переезды,
// лента алертов critical/warning (t08 spec §4.3–4.5; HA-карточка — t09 §4.13).
```

(заменяет `// Панель Обзор: карточки etcd/кластеров/алертов (HA — t09), активные переезды,` + вторую строку `// лента алертов critical/warning (t08 spec §4.3–4.5).`).

Импорты: строку

```tsx
import type { AlertSeverityName, OverviewDto } from '../api/dto';
```

заменить на

```tsx
import type { AlertSeverityName, HaScopeSummaryDto, OverviewDto } from '../api/dto';
```

и строку

```tsx
import { fetchAlerts, fetchOverview, queryKeys } from '../api/queries';
```

на

```tsx
import { fetchAlerts, fetchHaScopes, fetchOverview, queryKeys } from '../api/queries';
```

В `OverviewPage` после `alerts`-запроса добавить (тот же ключ, что HaPage — дедупликация, spec §3):

```tsx
const haScopes = useQuery({
  queryKey: queryKeys.haScopes,
  queryFn: fetchHaScopes,
  refetchInterval: intervalMs,
});
```

- [ ] **Step 7.2: Заменить заглушку на HaCard**

В JSX карточек строку-заглушку

```tsx
<Card withBorder padding="md" radius="md">
  <Text fw={600} mb="xs">HA</Text>
  <Text c="dimmed" size="sm">Сводка HA будет реализована в t09</Text>
</Card>
```

заменить на

```tsx
<HaCard
  scopes={haScopes.data}
  isPending={haScopes.isPending}
  onRetry={() => void haScopes.refetch()}
/>
```

- [ ] **Step 7.3: Добавить компонент HaCard**

В конец файла `OverviewPage.tsx` (после `AlertsFeedSection`) — полный код:

```tsx
// Карточка HA дашборда: счётчики скопов/без лидера/unmatched + строки-ссылки
// на детали (t09 spec §4.13). «Без лидера» — только matched-скопы: согласовано
// с алертом shard-no-leader, чтобы счётчик не расходился с лентой алертов.
// Ошибка без данных — своя (не роняет остальные карточки); ошибка при данных
// — тихо (StaleBadge сигнализирует), паттерн AlertsFeedSection (t08 §4.4).
function HaCard({ scopes, isPending, onRetry }: {
  scopes: HaScopeSummaryDto[] | undefined;
  isPending: boolean;
  onRetry: () => void;
}) {
  let content;
  if (scopes === undefined)
    content = isPending ? (
      <Text c="dimmed" size="sm">Загрузка HA…</Text>
    ) : (
      <Stack gap="xs" align="flex-start">
        <Alert color="red">Нет данных HA</Alert>
        <Anchor size="sm" onClick={onRetry}>Повторить</Anchor>
      </Stack>
    );
  else if (scopes.length === 0) content = <Text c="dimmed" size="sm">HA-scope'ы не найдены</Text>;
  else {
    const withoutLeader = scopes.filter((s) => s.matched && s.leaderName === null).length;
    const unmatched = scopes.filter((s) => !s.matched).length;
    content = (
      <Stack gap={4}>
        <Group gap="xs" wrap="nowrap">
          <Text size="sm" c="dimmed">скопов: {scopes.length}</Text>
          <Badge color={withoutLeader > 0 ? 'red' : 'gray'} variant="light">
            без лидера: {withoutLeader}
          </Badge>
          <Badge color={unmatched > 0 ? 'yellow' : 'gray'} variant="light">
            unmatched: {unmatched}
          </Badge>
        </Group>
        {scopes.map((s) => (
          <Group key={s.scope} justify="space-between" gap="xs" wrap="nowrap">
            <Anchor
              component={Link}
              to={`/ha/${s.scope}`}
              size="sm"
              ff="monospace"
              truncate="end"
            >
              {s.scope}
            </Anchor>
            <Group gap={5} wrap="nowrap">
              {s.matched && s.leaderName === null ? (
                <Badge color="red" variant="light">нет лидера</Badge>
              ) : null}
              {!s.matched ? <Badge color="yellow" variant="light">unmatched</Badge> : null}
            </Group>
          </Group>
        ))}
      </Stack>
    );
  }
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">HA</Text>
      {content}
    </Card>
  );
}
```

`Alert`, `Anchor`, `Badge`, `Card`, `Group`, `Stack`, `Text`, `Link` уже импортированы в файле — новых импортов не нужно.

- [ ] **Step 7.4: Проверить сборку**

Run: `cd frontend && npm run build`
Expected: `tsc --noEmit` без ошибок, бандл собирается.

- [ ] **Step 7.5: Коммит**

```bash
git add frontend/src/pages/OverviewPage.tsx
git commit -m "t09: HA-карточка Overview — счётчики скопов/без лидера/unmatched + ссылки"
```

**Выход:** карточка «HA» дашборда наполнена; заглушки t09 в проекте не осталось.

**Проверка:** `git show --stat HEAD` → 1 файл; `grep -rn "будет реализована" frontend/src` → пусто (ссылки «t09 spec §N» в комментариях кода легитимны — прецедент t08).

**Spec:** §4.13, §5 (макет Overview).

---

### Task 8: Финальный прогон — фронт + бэкенд-регрессия (коммита нет)

**Files:** — (изменений кода нет; Task верификационный).

- [ ] **Step 8.1: Фронтенд — typecheck и сборка**

Run: `cd frontend && npm run typecheck && npm run build`
Expected: оба `tsc --noEmit` прохода без ошибок; `vite build` пишет бандл в `../src/AdminPanel.Api/wwwroot/`.

- [ ] **Step 8.2: Бэкенд — сборка**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 warnings (`TreatWarningsAsErrors=true`).

- [ ] **Step 8.3: Полный прогон тестов**

Run: `dotnet test src/AdminPanel.slnx`
Expected: все тесты зелёные — ожидаемо 204 unit + 65 integration (Docker для Testcontainers должен быть запущен). Числа могут отличаться на единицы от указанных — критерий: ноль падений.

- [ ] **Step 8.4: Известный флак (если Step 8.3 упал на t90)**

Если упал только `EtcdSnapshotIntegrationTests.Refresher_EnrichesSnapshot_FromProbeState` (тег `t90-fix-probe-enrich-flaky`, `arch/roadmap/ha.md`):

Run: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~Refresher_EnrichesSnapshot_FromProbeState"`
Expected: PASS (изолированный запуск зелёный) → флак подтверждён, приёмку не блокирует (прецедент плана t08). Все прочие падения — блокер: разбирать (Systematic Debugging), не игнорировать.

- [ ] **Step 8.5: Состав изменений**

Run: `git status --short`
Expected: пусто (wwwroot/node_modules игнорируются; изменений с Task 7 нет).

**Spec:** §10.1–10.2 (критерии приёмки).

---

### Task 9: Roadmap-деливерабл — закрытие трека frontend

По правилам `arch/roadmap/README.md` (мерж-гейт): пункт удаляется из списка, а `← t09-frontend-ha` вычищается из зависимостей `t11-finalize` (`arch/roadmap/infra.md`). Прецедент структуры — два коммита t08 (`6bc181f`, `62adace`). В `frontend.md` после удаления задач не остаётся — файл остаётся с шапкой трека (заголовок + контекст + пустая секция «## Задачи»), прецедент t06 §14.

**Files:**
- Modify: `arch/roadmap/frontend.md` (удалить пункт t09)
- Modify: `arch/roadmap/infra.md` (чистка `← t09-frontend-ha` у `t11-finalize`)

- [ ] **Step 9.1: Удалить пункт t09 из frontend.md**

В `arch/roadmap/frontend.md` удалить строки (весь пункт — файл задач больше не содержит):

```
- `t09-frontend-ha` ← `t06-ha-api`, `t07-frontend-base` — панели HA и Alerts.
  HA: список scope'ов (cluster/shard, лидер, члены, max-лаг, unmatched) →
  детали (members: role/state/timeline/lag/probe-статус, optime, raw config
  свёрнуто). Alerts: таблица с severity-цветами, kind, target, since,
  фильтр severity; количество critical/warning — в навигации. Сводные поля
  HA доливаются в Overview.
```

Секция `## Задачи` остаётся пустой — никаких пометок «закрыта» (история в git, правило README).

- [ ] **Step 9.2: Коммит удаления пункта**

```bash
git add arch/roadmap/frontend.md
git commit -m "t09: пункт roadmap t09-frontend-ha выполнен"
```

- [ ] **Step 9.3: Вычистить зависимость из infra.md**

В `arch/roadmap/infra.md` строку

```
- `t11-finalize` ← `t09-frontend-ha`, `t10-dev-stand`
```

заменить на

```
- `t11-finalize` ← `t10-dev-stand`
```

- [ ] **Step 9.4: Коммит мерж-гейта**

```bash
git add arch/roadmap/infra.md
git commit -m "t09: мерж-гейт — чистка ← t09-frontend-ha из зависимостей t11 (roadmap)"
```

**Выход:** `grep -rn "t09" arch/roadmap/` → пусто; `frontend.md` валиден (шапка + пустая секция задач).

**Проверка:** `grep -rn "t09" arch/roadmap/ ; echo "exit=$?"` → `exit=1` (совпадений нет).

**Spec:** §10.6 (деливерабл roadmap).

---

### Task 10: Ручной HTTP-сценарий (коммита нет)

Браузерные проверки спецификации сведены к HTTP-проверкам раздачи SPA и API-контракта (инструкция фазы; полный e2e со стендом — t10). Фон запускается из корня worktree.

- [ ] **Step 10.1: Поднять хост с собранным бандлом**

Бандл уже собран Task 8 (`npm run build` положил wwwroot). Запуск в фоне (для агентного исполнения — `run_in_background`, вывод читается из файла задачи; для человека — отдельный терминал):

```bash
dotnet run --project src/AdminPanel.Api
```

Environment Development (admin/admin — из appsettings.Development.json). Дождаться строки запуска Kestrel (`Now listening on: http://localhost:5000`).

- [ ] **Step 10.2: Логин и cookie**

```bash
curl -s -o /dev/null -w "%{http_code}\n" -c /tmp/t09-cookies.txt \
  -H "Content-Type: application/json" -d '{"username":"admin","password":"admin"}' \
  http://localhost:5000/api/auth/login
```

Expected: `204`; файл `/tmp/t09-cookies.txt` содержит `adminpanel_session`. (Пробелы/кавычки в JSON при копировании недопустимы; при `400` — проверить тело curl.)

- [ ] **Step 10.3: API-контракт панелей**

Без cookie:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5000/api/ha
```
Expected: `401`.

С cookie:

```bash
curl -s -b /tmp/t09-cookies.txt -o /dev/null -w "%{http_code}\n" http://localhost:5000/api/ha
curl -s -b /tmp/t09-cookies.txt -o /dev/null -w "%{http_code}\n" http://localhost:5000/api/ha/unknown-scope
curl -s -b /tmp/t09-cookies.txt -o /dev/null -w "%{http_code}\n" http://localhost:5000/api/alerts
```
Expected: `200` (список скопов; без etcd на машине — `503` ProblemDetails «etcd-снапшот ещё не собран», это валидный исход), `404` ProblemDetails `Scope not found`, `200` (все алерты, без severity-параметров — фильтр клиентский, spec §4.10).

Если `/api/ha` = 200 — сверить JSON-поля ответа с `HaScopeSummaryDto` (camelCase: `scope`, `cluster`, `shard`, `matched`, `leaderName`, `membersTotal`, `membersHealthy`, `lagMaxBytes`) и посмотреть детали реального скопа: `curl -s -b /tmp/t09-cookies.txt http://localhost:5000/api/ha/<scope>`.

- [ ] **Step 10.4: SPA-раздача (маршруты новых страниц)**

```bash
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:5000/
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:5000/ha
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:5000/ha/demo-s1
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:5000/alerts
```
Expected: все `200 text/html` (SPA-fallback `index.html` — клиентская маршрутизация `/ha/:scope`, `/alerts`; содержимое — собранный бандл Task 8).

- [ ] **Step 10.5: Опционально — визуальная проверка браузером**

Открыть `http://localhost:5000/` (login admin/admin): HA-список → клик по scope → детали (Raw config закрыт, раскрывается), `/ha/nope` → «Скоп не найден» + «← HA», Alerts — фильтр и since-тики, навигация — красный/жёлтый счётчики при наличии алертов, Overview — HA-карточка. Опционально (dev-сервер `npm run dev` — те же проверки через прокси); полный UI-e2e со стендом — t10.

- [ ] **Step 10.6: Остановить хост и зафиксировать результат**

Остановить `dotnet run` (Ctrl+C или kill фонового процесса). Зафиксировать в отчёте задачи: коды ответов Steps 10.2–10.4 и исход `/api/ha` (200 с данными / 503 без etcd).

Run: `git status --short`
Expected: пусто.

**Spec:** §10.3–10.4 (HTTP-смоук + ручной сценарий, сведённый к HTTP).

---

## Самопроверка плана (выполнена автором)

1. **Покрытие spec:** §4.2 → Task 3; §4.3 → Task 6; §4.4–4.9 → Task 5; §4.10–4.11 → Task 4; §4.12 → Task 2; §4.13–4.14 → Tasks 7 и 2; §4.15 → Tasks 4/5/6 (`ErrorSection`/`notFound`); §4.16 → Task 5 (formatIsoAge); §4.17 → все таблицы (`Table.ScrollContainer`); §5 макеты → Tasks 4–7; §6 дерево файлов → состав коммитов Tasks 2–7 (ровно: AlertSeverityBadge, AlertsNavCounters, HaPage, HaScopeDetailsPage, AlertsPage, OverviewPage, App.tsx, format.ts, AppLayout.tsx); §7 фазы → порядок Tasks 2–7 (навигация раньше страниц — чтобы ключ alerts жил с первого экрана; детали HA раньше списка — зависимость маршрута); §9 ограничения → Global Constraints; §10 → Tasks 8–10; §11 → Task 1; roadmap-деливерабл → Task 9 (плюс чистка `← t09` из `t11` по правилу README — в spec §10.6 не названа явно, добавлена планом по правилу мерж-гейта).
2. **Плейсхолдеры:** отсутствуют — каждый шаг содержит полный код/команду/ожидание.
3. **Типы:** `AlertSeverityName` (dto.ts, существует) используется в Task 2 (компонент + сужение rows ленты) и Task 4 (`SeverityFilter`); `HaScopeSummaryDto`/`HaMemberDto`/`AlertDto` — существующие типы dto.ts без правок; `formatIsoAge` объявлен в Task 5 и потребляется там же; `queryKeys.alerts()`/`haScopes`/`haScope(scope)` — существующие ключи queries.ts. Сигнатуры `ErrorSection({error, onRetry, notFound})` и `LoadingSection` сверены с `LoadState.tsx`.
