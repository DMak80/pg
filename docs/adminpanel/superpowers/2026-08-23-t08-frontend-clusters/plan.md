# План реализации t08-frontend-clusters

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Наполнить панели Overview/etcd/Clusters SPA AdminPanel данными из готового API и доставить `StandNodes` снапшота в детали кластера (единственная правка бэкенда).

**Architecture:** Замена трёх заглушек t07 страницами на TanStack Query + polling `usePollingIntervalMs`; детали кластера — новый маршрут `/clusters/:cluster` с презентационными вкладками; `ClusterDto` расширяется полем `standNodes` (маппер получает на вход `snapshot.StandNodes`). Все таблицы — Mantine `Table` (+`Table.ScrollContainer` — в Mantine 9 компонент называется так, НЕ `Table.ScrollArea`).

**Tech Stack:** React 19 + Vite + TypeScript 7 (strict, `noUnusedLocals`), Mantine 9.5.2 (dark), TanStack Query 5, React Router 8; ASP.NET Core Minimal API (.NET 10), xunit v3 + FluentAssertions + Testcontainers.

**Spec:** `docs/superpowers/2026-08-23-t08-frontend-clusters/spec.md` — план аргументируется от spec; исполнитель читает оба документа. Ссылки «spec §N» ниже — на этот файл.

## Global Constraints

- Работа только в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t08-frontend-clusters`; все пути ниже — от его корня.
- Один шаг (Task) = один коммит с сообщением вида `t08: <суть>`. Коммитить только указанные в шаге файлы (`git add <явные пути>`), не `git add -A`.
- Идентификаторы английские; комментарии в коде и ВСЕ тексты UI — русские (spec §2).
- DTO фронта — строго фактические C#-records (spec §2); тип `StandNodeDto` повторяет C#-record ниже.
- Фронтенд-тестов нет (spec §9); проверка фронта — `npm run typecheck` / `npm run build` + ручной сценарий Task 11.
- Бэкенд: `TreatWarningsAsErrors=true` — сборка обязана быть без warnings; комментарии тестов — по AAA.
- Новых npm/NuGet-пакетов нет (spec §10); node_modules уже установлен в worktree (для чистого чекаута: `cd frontend && npm ci`).
- Не трогать: `HaPage.tsx`, `AlertsPage.tsx`, `api/queries.ts`, `api/client.ts`, `PollingContext.tsx`, `StaleBadge.tsx`, `LoginPage.tsx`, `Program.cs`, прочие Inspection-файлы (spec §6).
- Известный флак: один из integration-тестов (t90, тайминги Docker/Testcontainers) может быть красным — не блокирует приёмку, если остальное зелёное и перезапуск по фильтру проходил (см. Task 9).

---

### Task 1: Коммит документации (spec + arch)

**Files:**
- Already created: `docs/superpowers/2026-08-23-t08-frontend-clusters/spec.md`, `arch/03-panels.md` (3 правки из spec §11)

**Вход:** worktree содержит одобренный spec и arch-правки (незакоммичены); рабочий каталог чист кроме них.

**Interfaces:** — (документация).

- [ ] **Step 1.1: Проверить состав изменений**

Run: `git status --short`
Expected: `M arch/03-panels.md` и `?? docs/superpowers/2026-08-23-t08-frontend-clusters/`; ничего больше (node_modules игнорируется).

- [ ] **Step 1.2: Коммит**

```bash
git add arch/03-panels.md docs/superpowers/2026-08-23-t08-frontend-clusters/spec.md
git commit -m "t08: spec и правки arch/03 (standNodes в ClusterDto, alarms-карточка Overview, блок Стендовая топология)"
```

**Выход:** spec и arch в истории; рабочее дерево чистое.

**Проверка:** `git log --oneline -1` → коммит `t08: spec…`; `git status --short` → пусто.

**Spec:** §11 (arch-правки внесены до spec — фиксируются первым коммитом ветки).

---

### Task 2: Бэкенд — `standNodes` в `ClusterDto` (TDD)

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`
- Test: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (фикстура `InspectionSnapshots.Clustered`)
- Test: `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs` (тест `ClusterDetails_ReturnsConfigShardsBucketsHeals`)

**Вход:** ветка после Task 1; `dotnet build` зелёный.

**Interfaces:**
- Consumes: `EtcdSnapshot.StandNodes: IReadOnlyList<StandNode>` (Core, `StandNode(string Name, string? Address)`), `TestSnapshots.MovingCluster(DateTimeOffset)`.
- Produces: `ClusterDetailsMapper.Map(ClusterInfo, long, string?, BucketState?, IReadOnlyList<StandNode>)`; `ClusterDto.StandNodes: IReadOnlyList<StandNodeDto>`; JSON `standNodes: [{name, address}]` — то, на что опирается Task 6 (`dto.ts`) и `StandNodesBlock`.

- [ ] **Step 2.1: Красные юнит-тесты — обновить вызовы `Map` и добавить кейс**

В `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs` (комментарий шапки класса дополнить: «+ standNodes (t08 spec §8)»):

1) Все 9 существующих вызовов `ClusterDetailsMapper.Map(cluster, NowUnix, X, Y)` (по тестам: `ClusterDetailsMapper_FullDto` — 1, `ClusterDetailsMapper_AgeSec_FromMoveAge` — 1, `ClusterDetailsMapper_Filters_OwnerStateBothNull` — 5, `ClusterDetailsMapper_Heals_NewestFirst` — 1, `ClusterDetailsMapper_RuntimeMapped_WhenPresent` — 1) получают пятый аргумент `[]`:

```csharp
// было:  ClusterDetailsMapper.Map(cluster, NowUnix, null, null)
// стало: ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [])
```

2) В `ClusterDetailsMapper_FullDto` добавить ассерт (после `dto.Heals.Should().HaveCount(2);`):

```csharp
        dto.StandNodes.Should().BeEmpty(); // пустой реестр → пустой список (t08 spec §8)
```

3) Новый тест в конец класса (перед закрывающей `}`):

```csharp
    [Fact]
    public void ClusterDetailsMapper_StandNodes_MappedFromSnapshot()
    {
        // Arrange: стендовый топо-реестр глобален — передаётся в маппер отдельно от кластера (t08 spec §8).
        var nodes = new[] { new StandNode("node1", "10.0.0.5"), new StandNode("node2", null) };

        // Act
        var dto = ClusterDetailsMapper.Map(TestSnapshots.MovingCluster(Now), NowUnix, null, null, nodes);

        // Assert
        dto.StandNodes.Should().HaveCount(2);
        dto.StandNodes[0].Name.Should().Be("node1");
        dto.StandNodes[0].Address.Should().Be("10.0.0.5");
        dto.StandNodes[1].Name.Should().Be("node2");
        dto.StandNodes[1].Address.Should().BeNull();
    }
```

- [ ] **Step 2.2: Красный integration-ассерт + фикстура**

В `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs`, метод `InspectionSnapshots.Clustered` — последняя строка:

```csharp
        // было:  return Fixture(builtAt) with { Clusters = [cluster] };
        // стало (t08 spec §8): реестр /cluster/nodes/ — 2 ноды, у второй адрес пуст:
        return Fixture(builtAt) with
        {
            Clusters = [cluster],
            StandNodes = [new StandNode("node1", "10.0.0.5"), new StandNode("node2", null)],
        };
```

В `src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs`, тест `ClusterDetails_ReturnsConfigShardsBucketsHeals` — добавить в блок Assert (после строки про `heals[0]`):

```csharp
        var standNodes = dto.GetProperty("standNodes"); // стендовая топология (t08 spec §8)
        standNodes.GetArrayLength().Should().Be(2);
        standNodes[0].GetProperty("name").GetString().Should().Be("node1");
        standNodes[0].GetProperty("address").GetString().Should().Be("10.0.0.5");
        standNodes[1].GetProperty("address").ValueKind.Should().Be(JsonValueKind.Null);
```

- [ ] **Step 2.3: Убедиться, что красное**

Run: `dotnet build src/tests/AdminPanel.UnitTests 2>&1 | tail -5`
Expected: ошибка компиляции — `Map` не принимает 5 аргументов (это и есть красный для компилируемого стека).

Run: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~ClustersApiTests.ClusterDetails_ReturnsConfigShardsBucketsHeals"`
Expected: FAIL — `standNodes` в JSON отсутствует (`JsonElement.GetProperty` бросает). (Требуется Docker; если Docker не поднят — зафиксировать и вернуться к этому шагу после Step 2.4.)

- [ ] **Step 2.4: Реализация в `ClusterDetailsQuery.cs`**

1) После record `HealDto` добавить:

```csharp
// Стендовая топология (arch/02 §2.3): реестр /cluster/nodes/ — глобален для всех кластеров, обычно пуст.
public sealed record StandNodeDto(string Name, string? Address);
```

2) `ClusterDto` — добавить последний параметр:

```csharp
public sealed record ClusterDto(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    bool Incomplete,
    IReadOnlyList<ShardDto> Shards,
    IReadOnlyList<BucketDto> Buckets,
    IReadOnlyList<HealDto> Heals,
    IReadOnlyList<StandNodeDto> StandNodes);
```

3) Сигнатура и хвост `ClusterDetailsMapper.Map` (в конец конструктора `new ClusterDto(...)`):

```csharp
    public static ClusterDto Map(
        ClusterInfo cluster, long nowUnix, string? owner, BucketState? state,
        IReadOnlyList<StandNode> standNodes)
```

```csharp
            [.. cluster.Heals
                .OrderByDescending(h => h.TsUnix) // журнал: новые сверху; null — в конец (spec §3.3)
                .Select(h => new HealDto(h.Bucket, h.Was, h.Now, h.Reason, h.TsUnix))],
            [.. standNodes.Select(n => new StandNodeDto(n.Name, n.Address))]);
```

4) Хендлер `ClusterDetailsQueryHandler` — вызов маппера:

```csharp
            : Result<ClusterDto>.Success(ClusterDetailsMapper.Map(
                cluster, time.GetUtcNow().ToUnixTimeSeconds(), query.Owner, query.State, snapshot.StandNodes));
```

- [ ] **Step 2.5: Зелёный прогон**

Run: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3`
Expected: `Build succeeded`, 0 warnings (TreatWarningsAsErrors иначе уронит).

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClustersMappersTests"`
Expected: PASS, 9 тестов (8 старых + новый из этого шага).

Run: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~ClustersApiTests"`
Expected: PASS всех тестов класса (Docker запущен).

- [ ] **Step 2.6: Коммит**

```bash
git add src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs src/tests/AdminPanel.IntegrationTests/ClustersApiTests.cs
git commit -m "t08: standNodes в ClusterDto — доставка стендовой топологии в детали кластера"
```

**Выход:** `GET /api/clusters/{c}` отдаёт `standNodes` (camelCase); все тесты зелёные.

**Проверка:** шаги 2.3/2.5 выше.

**Spec:** §8 (бэкенд-дельта), §9 (тесты), §11 (контракт arch/03 §2 уже обновлён).

---

### Task 3: Общий слой фронта (DTO, форматтеры, LoadState, BucketStateBadge)

**Files:**
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/utils/format.ts`
- Create: `frontend/src/components/LoadState.tsx`
- Create: `frontend/src/components/BucketStateBadge.tsx`

**Вход:** Task 2 слит (поле `standNodes` в API); каркас t07 собирается (`npm run typecheck` зелёный на `main`-состоянии ветки).

**Interfaces:**
- Consumes: C# `StandNodeDto(string Name, string? Address)` из Task 2.
- Produces (используют Tasks 4–7):
  - `interface StandNodeDto { name: string; address: string | null }`; `ClusterDto.standNodes: StandNodeDto[]`
  - `formatBytes(bytes: number | null): string`; `formatUnix(unix: number | null): string`; `formatIso(iso: string | null): string`; `formatUnixAge(unix: number | null): string` (модуль `../utils/format`)
  - `LoadingSection()`; `ErrorSection({ error, onRetry, notFound? })` (модуль `../components/LoadState`)
  - `BucketStateBadge({ state }: { state: BucketStateName })` (модуль `../components/BucketStateBadge`)

- [ ] **Step 3.1: `dto.ts` — standNodes**

После `export interface HealDto { ... }` (блок деталей кластера) добавить:

```ts
// Стендовая топология в деталях кластера: глобальный реестр снапшота, обычно пуст (t08 spec §8).
export interface StandNodeDto {
  name: string;
  address: string | null;
}
```

В `export interface ClusterDto` добавить последним полем (после `heals: HealDto[];`):

```ts
  standNodes: StandNodeDto[];
```

- [ ] **Step 3.2: `format.ts` — три форматтера + ISO**

Дополнить файл (существующий `formatAge` не менять) после `formatAge`:

```ts
// Размер в байтах → «823 Б», «20.0 КБ», «4.1 МБ» (t08 spec §4.16); null → «—».
export function formatBytes(bytes: number | null): string {
  if (bytes === null) return '—';
  if (bytes < 1024) return `${bytes} Б`;
  const units = ['КБ', 'МБ', 'ГБ', 'ТБ'];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1024;
    unitIndex += 1;
  } while (value >= 1024 && unitIndex < units.length - 1);
  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

// Кэш форматтера локального времени: один экземпляр на модуль (t08 spec §4.16).
const dateTimeFormatter = new Intl.DateTimeFormat('ru-RU', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

// Unix-секунды → локальная дата-время «22.08.2026, 14:03:05» (t08 spec §4.16); null → «—».
export function formatUnix(unix: number | null): string {
  return unix === null ? '—' : dateTimeFormatter.format(new Date(unix * 1000));
}

// ISO-строка (DateTimeOffset) → локальная дата-время — для lastRefreshUtc (t08 spec §5).
export function formatIso(iso: string | null): string {
  return iso === null ? '—' : dateTimeFormatter.format(new Date(iso));
}

// Относительный возраст от Unix-штампа: «12 с», «3 мин 5 с» (t08 spec §4.16); null → «—».
export function formatUnixAge(unix: number | null): string {
  return unix === null ? '—' : formatAge(Date.now() - unix * 1000);
}
```

Мотивация `formatIso` (решение плана): spec §5 требует подпись «Обновлено: …» из `lastRefreshUtc` (ISO-строка), а `formatUnix` принимает секунды — переиспользуем кэшированный форматтер без дублирования.

- [ ] **Step 3.3: `components/LoadState.tsx` (новый файл)**

```ts
// Общие состояния страниц: загрузка и ошибка с повтором (t08 spec §4.15, §4.18).
import { Alert, Button, Center, Loader, Stack } from '@mantine/core';
import type { ReactNode } from 'react';
import { ApiError } from '../api/client';

// Первый загрузочный рендер запроса: центрированный спиннер.
export function LoadingSection() {
  return (
    <Center mih={160}>
      <Loader />
    </Center>
  );
}

// Ошибка запроса без данных (t08 spec §4.15): 503 — снапшот не собран; 404 — notFound-контент
// (передаёт страница, например «Кластер не найден» со ссылкой назад); прочее — текст ApiError.
export function ErrorSection({ error, onRetry, notFound }: {
  error: unknown;
  onRetry: () => void;
  notFound?: ReactNode;
}) {
  if (error instanceof ApiError && error.status === 404 && notFound !== undefined)
    return <>{notFound}</>;
  const message = error instanceof ApiError && error.status === 503
    ? 'Данные ещё не собраны (etcd-снапшот пуст)'
    : error instanceof Error
      ? error.message
      : 'Неизвестная ошибка';
  return (
    <Stack gap="sm" align="flex-start">
      <Alert color="red">{message}</Alert>
      <Button variant="light" size="xs" onClick={onRetry}>Повторить</Button>
    </Stack>
  );
}
```

- [ ] **Step 3.4: `components/BucketStateBadge.tsx` (новый файл)**

```ts
// Цветовая карта состояний бакета — единый источник всех панелей (t08 spec §4.17).
import { Badge, Tooltip } from '@mantine/core';
import type { BucketStateName } from '../api/dto';

// Подпись — русская; каноническое значение — в Tooltip.
const STATE_META: Record<BucketStateName, { color: string; label: string }> = {
  ACTIVE: { color: 'teal', label: 'активен' },
  SYNCING: { color: 'blue', label: 'синхронизация' },
  FROZEN: { color: 'yellow', label: 'заморожен' },
  ABORTING: { color: 'red', label: 'отменяется' },
};

export function BucketStateBadge({ state }: { state: BucketStateName }) {
  const meta = STATE_META[state];
  return (
    <Tooltip label={state}>
      <Badge color={meta.color} variant="light">{meta.label}</Badge>
    </Tooltip>
  );
}
```

- [ ] **Step 3.5: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0, без вывода ошибок.

```bash
git add frontend/src/api/dto.ts frontend/src/utils/format.ts frontend/src/components/LoadState.tsx frontend/src/components/BucketStateBadge.tsx
git commit -m "t08: общий слой фронта — StandNodeDto, форматтеры, LoadState, BucketStateBadge"
```

**Выход:** общие модули для страниц; typecheck зелёный.

**Проверка:** Step 3.5.

**Spec:** §4.15–§4.18, §6 (дерево: components/, utils/format.ts, dto.ts).

---

### Task 4: EtcdPage

**Files:**
- Modify: `frontend/src/pages/EtcdPage.tsx` (полная замена заглушки)

**Вход:** Task 3 слит.

**Interfaces:**
- Consumes: `queryKeys.etcdStatus`, `fetchEtcdStatus`; `EtcdStatusDto`/`EtcdEndpointDto`/`EtcdMemberDto` из `../api/dto`; `formatBytes`, `formatIso`; компоненты Task 3.
- Produces: маршрут `/etcd` (уже в `App.tsx`).

- [ ] **Step 4.1: Полная замена файла**

```tsx
// Панель etcd: endpoints (+метка «активный»), members/лидер, alarms, lastRefresh,
// баннер подозрения кворума (t08 spec §5).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Badge,
  Card,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import type { EtcdEndpointDto, EtcdStatusDto } from '../api/dto';
import { fetchEtcdStatus, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso } from '../utils/format';

export function EtcdPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.etcdStatus,
    queryFn: fetchEtcdStatus,
    refetchInterval: intervalMs,
  });

  // Паттерн состояний (t08 spec §4.15): нет данных + ошибка → ErrorSection;
  // нет данных без ошибки → загрузка; ошибка ПРИ данных (polling-сбой) — тихо,
  // показываем предыдущие данные (StaleBadge в шапке уже сигнализирует).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  return (
    <Stack gap="md">
      <Title order={2}>etcd</Title>
      {data.quorumSuspected ? (
        <Alert color="red" title="Подозрение на отсутствие кворума">
          Признаки отсутствия raft-лидера — проверьте статус членов кластера
        </Alert>
      ) : null}
      <EndpointsCard data={data} />
      <MembersCard data={data} />
      <AlarmsCard data={data} />
      <Text c="dimmed" size="sm">Обновлено: {formatIso(data.lastRefreshUtc)}</Text>
    </Stack>
  );
}

// Таблица endpoints: доступность, латентность, версия, raft term, размер БД, ошибки (t08 spec §5).
function EndpointsCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Endpoints</Text>
      <Table.ScrollContainer minWidth={900}>
        <Table highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>URL</Table.Th>
              <Table.Th>Статус</Table.Th>
              <Table.Th>Задержка</Table.Th>
              <Table.Th>Версия</Table.Th>
              <Table.Th>raft term</Table.Th>
              <Table.Th>Размер БД</Table.Th>
              <Table.Th>Ошибки</Table.Th>
              <Table.Th>Роль</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.endpoints.map((e) => <EndpointRow key={e.url} endpoint={e} />)}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Card>
  );
}

function EndpointRow({ endpoint }: { endpoint: EtcdEndpointDto }) {
  return (
    <Table.Tr>
      <Table.Td ff="monospace">{endpoint.url}</Table.Td>
      <Table.Td>
        <Badge color={endpoint.reachable ? 'teal' : 'red'} variant="light">
          {endpoint.reachable ? 'ok' : 'недоступен'}
        </Badge>
      </Table.Td>
      <Table.Td>{endpoint.latencyMs === null ? '—' : `${endpoint.latencyMs.toFixed(1)} мс`}</Table.Td>
      <Table.Td>{endpoint.version ?? '—'}</Table.Td>
      <Table.Td>{endpoint.raftTerm === null ? '—' : endpoint.raftTerm}</Table.Td>
      <Table.Td>{formatBytes(endpoint.dbSizeBytes)}</Table.Td>
      <Table.Td>
        {endpoint.errors.length === 0 ? '—' : (
          <Tooltip multiline label={endpoint.errors.join('\n')}>
            <Badge color="red" variant="light">{endpoint.errors.length}</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        {endpoint.active ? <Badge color="blue" variant="light">активный</Badge> : null}
      </Table.Td>
    </Table.Tr>
  );
}

// Члены кластера etcd: id, имена, URL; лидер — меткой (t08 spec §5).
function MembersCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Члены кластера</Text>
      {data.members.length === 0 ? (
        <Text c="dimmed" size="sm">Нет данных о членах</Text>
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>ID</Table.Th>
                <Table.Th>Имя</Table.Th>
                <Table.Th>Peer URLs</Table.Th>
                <Table.Th>Client URLs</Table.Th>
                <Table.Th>Роль</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.members.map((m) => (
                <Table.Tr key={m.id}>
                  <Table.Td ff="monospace">{m.id}</Table.Td>
                  <Table.Td>{m.name ?? '—'}</Table.Td>
                  <Table.Td ff="monospace">{m.peerUrls.length === 0 ? '—' : m.peerUrls.join(', ')}</Table.Td>
                  <Table.Td ff="monospace">{m.clientUrls.length === 0 ? '—' : m.clientUrls.join(', ')}</Table.Td>
                  <Table.Td>
                    {m.isLeader ? <Badge color="violet" variant="light">лидер</Badge> : null}
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

// Alarms: member → тип; пусто — зелёная строка (t08 spec §5).
function AlarmsCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Alarms</Text>
      {data.alarms.length === 0 ? (
        <Text c="teal" size="sm">Активных alarm'ов нет</Text>
      ) : (
        <Table highlightOnHover w="50%">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Member ID</Table.Th>
              <Table.Th>Тип</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.alarms.map((a) => (
              <Table.Tr key={`${a.memberId}-${a.type}`}>
                <Table.Td ff="monospace">{a.memberId}</Table.Td>
                <Table.Td><Badge color="red" variant="light">{a.type}</Badge></Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Card>
  );
}
```

Примечание: `Table w="50%"` — style-проп; при несоответствии typecheck — заменить на оборачивание в `Box w="50%"` или убрать.

- [ ] **Step 4.2: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0.

```bash
git add frontend/src/pages/EtcdPage.tsx
git commit -m "t08: панель etcd — endpoints, members/лидер, alarms, lastRefresh"
```

**Выход:** `/etcd` — таблицы endpoints (с «активный»), members (с «лидер»), alarms, баннер кворума, подпись обновления.

**Проверка:** typecheck; визуально — Task 11.

**Spec:** §5 (макет etcd), §3.

---

### Task 5: ClustersPage (список)

**Files:**
- Modify: `frontend/src/pages/ClustersPage.tsx` (полная замена заглушки)

**Вход:** Task 3 слит.

**Interfaces:**
- Consumes: `queryKeys.clusters`, `fetchClusters`; `ClusterSummaryDto`; компоненты Task 3.
- Produces: маршрут `/clusters` (уже в `App.tsx`); ссылки на `/clusters/:name` (маршрут появится в Task 6 — до него клики уводят на `/` через catch-all `Navigate`, это ожидаемое промежуточное состояние ветки).

- [ ] **Step 5.1: Полная замена файла**

```tsx
// Панель Кластеры: сводный список кластеров снапшота (t08 spec §4.6).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Card, Table, Text, Title } from '@mantine/core';
import { Link } from 'react-router';
import type { ClusterSummaryDto } from '../api/dto';
import { fetchClusters, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';

export function ClustersPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.clusters,
    queryFn: fetchClusters,
    refetchInterval: intervalMs,
  });

  // Паттерн состояний — как на остальных страницах (t08 spec §4.15).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const clusters = query.data;
  return (
    <>
      <Title order={2} mb="md">Кластеры</Title>
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">Кластеры не найдены</Text>
        ) : (
          <Table.ScrollContainer minWidth={800}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Кластер</Table.Th>
                  <Table.Th>БД</Table.Th>
                  <Table.Th>Бакеты</Table.Th>
                  <Table.Th>Шарды</Table.Th>
                  <Table.Th>Переезды</Table.Th>
                  <Table.Th>Пометки</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {clusters.map((c) => <ClusterRow key={c.name} cluster={c} />)}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        )}
      </Card>
    </>
  );
}

function ClusterRow({ cluster }: { cluster: ClusterSummaryDto }) {
  const mastersMissing = cluster.shardsTotal - cluster.shardsWithMaster;
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/clusters/${cluster.name}`}>{cluster.name}</Anchor>
      </Table.Td>
      <Table.Td>{cluster.dbName ?? '—'}</Table.Td>
      <Table.Td>{cluster.bucketsCount}</Table.Td>
      <Table.Td>
        <Text c={mastersMissing > 0 ? 'red' : undefined}>
          {cluster.shardsWithMaster}/{cluster.shardsTotal}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text c={cluster.activeMoves > 0 ? 'yellow' : undefined}>{cluster.activeMoves}</Text>
      </Table.Td>
      <Table.Td>
        {cluster.incomplete ? <Badge color="yellow" variant="light">incomplete</Badge> : null}
        {mastersMissing > 0 ? (
          <Badge color="red" variant="light" ml={cluster.incomplete ? 5 : 0}>
            {mastersMissing} без мастера
          </Badge>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
```

- [ ] **Step 5.2: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0 (включая `import type { ClusterSummaryDto }` — используется в `ClusterRow`).

- [ ] **Step 5.3: Коммит**

```bash
git add frontend/src/pages/ClustersPage.tsx
git commit -m "t08: список кластеров — сводная таблица с переходом на детали"
```

**Выход:** `/clusters` — таблица сводки: имя-ссылка, БД, бакеты, шард мастеровых/всего, переезды, пометки.

**Проверка:** typecheck; визуально — Task 11.

**Spec:** §4.6, §5.

---

### Task 6: ClusterDetailsPage + вкладки + маршрут

**Files:**
- Create: `frontend/src/pages/ClusterDetailsPage.tsx`
- Create: `frontend/src/pages/cluster-details/ShardsTab.tsx`
- Create: `frontend/src/pages/cluster-details/BucketsTab.tsx`
- Create: `frontend/src/pages/cluster-details/MovesTab.tsx`
- Create: `frontend/src/pages/cluster-details/HealsTab.tsx`
- Create: `frontend/src/pages/cluster-details/StandNodesBlock.tsx`
- Modify: `frontend/src/App.tsx` (маршрут)

**Вход:** Tasks 2–3 слиты (`standNodes` в API и `dto.ts`); Task 5 слит.

**Interfaces:**
- Consumes: `queryKeys.cluster(name)`, `fetchClusterDetails(name)`; `ClusterDto`, `ShardDto`, `BucketDto`, `HealDto`, `StandNodeDto`, `BucketStateName`; `formatAge/formatUnix/formatUnixAge/formatBytes`; `BucketStateBadge`.
- Produces: маршрут `/clusters/:cluster`; компоненты вкладок принимают данные пропсами (сигнатуры в шагах).

- [ ] **Step 6.1: `ShardsTab.tsx` (новый файл)**

```tsx
// Вкладка «Шарды»: dsn, реплики, master+lease, runtime-колонки проб (t08 spec §4.10).
import { Badge, Stack, Table, Text, Tooltip } from '@mantine/core';
import type { ShardDto } from '../../api/dto';
import { formatBytes } from '../../utils/format';

export function ShardsTab({ shards }: { shards: ShardDto[] }) {
  if (shards.length === 0) return <Text c="dimmed">Шарды не найдены</Text>;
  const probesOff = shards.every((s) => s.runtime === null);
  const probeErrors = shards.filter((s) => s.runtime?.error != null);
  return (
    <Stack gap="xs">
      <Table.ScrollContainer minWidth={1000}>
        <Table highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Шард</Table.Th>
              <Table.Th>DSN</Table.Th>
              <Table.Th>Реплики</Table.Th>
              <Table.Th>Мастер</Table.Th>
              <Table.Th>Sync-standby</Table.Th>
              <Table.Th>Лаг слотов</Table.Th>
              <Table.Th>WAL lost</Table.Th>
              <Table.Th>Подписки</Table.Th>
              <Table.Th>Схемы</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {shards.map((s) => <ShardRow key={s.name} shard={s} />)}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {probesOff ? (
        <Text c="dimmed" size="sm">Пробы отключены — runtime-данные отсутствуют</Text>
      ) : null}
      {probeErrors.map((s) => (
        <Text key={s.name} c="red" size="sm">Ошибки проб: шард {s.name}: {s.runtime?.error}</Text>
      ))}
    </Stack>
  );
}

function ShardRow({ shard }: { shard: ShardDto }) {
  const runtime = shard.runtime;
  return (
    <Table.Tr>
      <Table.Td>{shard.name}</Table.Td>
      <Table.Td>
        <Tooltip label={shard.dsn} position="top">
          <Text ff="monospace" size="sm">{shard.hosts.join(', ')}</Text>
        </Tooltip>
      </Table.Td>
      <Table.Td>{shard.replicasDeclared ?? '—'}</Table.Td>
      <Table.Td>
        {shard.masterAddress === null ? (
          <Badge color="red" variant="light">нет мастера</Badge>
        ) : (
          <Tooltip label="master-lease жив (ключ присутствует)">
            <span>
              <Text size="sm" ff="monospace" span>{shard.masterAddress} </Text>
              <Badge color="teal" variant="light">lease</Badge>
            </span>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.standbiesSync ?? '—'}</Table.Td>
      <Table.Td>{runtime === null ? '—' : formatBytes(runtime.slotsLagMaxBytes)}</Table.Td>
      <Table.Td>
        {runtime === null || runtime.walStatusLost.length === 0 ? '—' : (
          <Tooltip multiline label={runtime.walStatusLost.join('\n')}>
            <Badge color="red" variant="light">{runtime.walStatusLost.length}</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.subscriptions.length}</Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.bucketSchemas.length}</Table.Td>
    </Table.Tr>
  );
}
```

- [ ] **Step 6.2: `BucketsTab.tsx` (новый файл)**

```tsx
// Вкладка «Бакеты»: грид id×owner×state, локальные фильтры, подсветка не-ACTIVE,
// возраст не-ACTIVE статуса (t08 spec §4.9).
import { useMemo, useState } from 'react';
import { Badge, Group, Select, Stack, Table, Text, Tooltip } from '@mantine/core';
import type { BucketDto, BucketStateName } from '../../api/dto';
import { BucketStateBadge } from '../../components/BucketStateBadge';
import { formatAge } from '../../utils/format';

// Значения фильтра состояния: «все», «не-ACTIVE» и канонические состояния.
const STATE_FILTERS = [
  { value: 'all', label: 'все' },
  { value: 'non-active', label: 'не-ACTIVE' },
  { value: 'ACTIVE', label: 'ACTIVE' },
  { value: 'SYNCING', label: 'SYNCING' },
  { value: 'FROZEN', label: 'FROZEN' },
  { value: 'ABORTING', label: 'ABORTING' },
];

export function BucketsTab({ buckets }: { buckets: BucketDto[] }) {
  const [stateFilter, setStateFilter] = useState('all');
  const [ownerFilter, setOwnerFilter] = useState('all');

  // Уникальные владельцы из данных — источник фильтра owner (t08 spec §4.9).
  const owners = useMemo(
    () => [...new Set(buckets.map((b) => b.owner).filter((o): o is string => o !== null))].sort(),
    [buckets],
  );
  const rows = useMemo(
    () => buckets.filter((b) => {
      const byState = stateFilter === 'all'
        ? true
        : stateFilter === 'non-active'
          ? b.state !== 'ACTIVE'
          : b.state === (stateFilter as BucketStateName);
      const byOwner = ownerFilter === 'all' ? true : b.owner === ownerFilter;
      return byState && byOwner;
    }),
    [buckets, stateFilter, ownerFilter],
  );

  return (
    <Stack gap="xs">
      <Group gap="sm">
        <Select
          label="Состояние"
          value={stateFilter}
          onChange={(value) => setStateFilter(value ?? 'all')}
          data={STATE_FILTERS}
          w={180}
        />
        <Select
          label="Владелец"
          value={ownerFilter}
          onChange={(value) => setOwnerFilter(value ?? 'all')}
          data={[{ value: 'all', label: 'все' }, ...owners.map((o) => ({ value: o, label: o }))]}
          w={180}
        />
        <Text size="sm" c="dimmed" style={{ alignSelf: 'flex-end' }}>
          Показано {rows.length} из {buckets.length}
        </Text>
      </Group>
      <Table.ScrollContainer minWidth={800} maxHeight={480}>
        <Table highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Id</Table.Th>
              <Table.Th>Owner</Table.Th>
              <Table.Th>Состояние</Table.Th>
              <Table.Th>Переезд</Table.Th>
              <Table.Th>Фаза</Table.Th>
              <Table.Th>Возраст</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((b) => <BucketRow key={b.id} bucket={b} />)}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}

function BucketRow({ bucket }: { bucket: BucketDto }) {
  const nonActive = bucket.state !== 'ACTIVE';
  const phase = bucket.move?.phase ?? null;
  const lastError = bucket.move?.lastError ?? null;
  return (
    <Table.Tr
      style={{ backgroundColor: nonActive ? 'var(--mantine-color-yellow-light)' : undefined }}
    >
      <Table.Td>{bucket.id}</Table.Td>
      <Table.Td>
        {bucket.owner === null ? (
          <Text c="red" size="sm">—</Text>
        ) : (
          <Text size="sm">{bucket.owner}</Text>
        )}
      </Table.Td>
      <Table.Td><BucketStateBadge state={bucket.state} /></Table.Td>
      <Table.Td>
        {bucket.move === null ? '—' : `${bucket.move.owner ?? '—'} → ${bucket.move.target ?? '—'}`}
      </Table.Td>
      <Table.Td>
        {phase === null ? '—' : lastError === null ? (
          <Text size="sm">{phase}</Text>
        ) : (
          <Tooltip label={lastError}>
            <Badge color="red" variant="light">{phase}</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>{bucket.ageSec === null ? '—' : formatAge(bucket.ageSec * 1000)}</Table.Td>
    </Table.Tr>
  );
}
```

- [ ] **Step 6.3: `MovesTab.tsx` (новый файл)**

```tsx
// Вкладка «Переезды»: только не-ACTIVE бакеты — фаза, штампы, lastError (t08 spec §4.11).
import { Table, Text, Tooltip } from '@mantine/core';
import type { BucketDto } from '../../api/dto';
import { BucketStateBadge } from '../../components/BucketStateBadge';
import { formatAge, formatUnix, formatUnixAge } from '../../utils/format';

// Усечение длинной ошибки: до limit символов + «…»; полный текст — в Tooltip (t08 spec §4.11).
function truncateText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

export function MovesTab({ buckets }: { buckets: BucketDto[] }) {
  const moves = buckets.filter((b) => b.state !== 'ACTIVE');
  if (moves.length === 0) return <Text c="dimmed">Активных переездов нет</Text>;
  return (
    <Table.ScrollContainer minWidth={900}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Id</Table.Th>
            <Table.Th>Состояние</Table.Th>
            <Table.Th>Маршрут</Table.Th>
            <Table.Th>Фаза</Table.Th>
            <Table.Th>Начат</Table.Th>
            <Table.Th>Обновлён</Table.Th>
            <Table.Th>Возраст</Table.Th>
            <Table.Th>Ошибка</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {moves.map((b) => (
            <Table.Tr key={b.id}>
              <Table.Td>{b.id}</Table.Td>
              <Table.Td><BucketStateBadge state={b.state} /></Table.Td>
              <Table.Td>{b.move === null ? '—' : `${b.move.owner ?? '—'} → ${b.move.target ?? '—'}`}</Table.Td>
              <Table.Td>{b.move?.phase ?? '—'}</Table.Td>
              <Table.Td>
                <Tooltip label={formatUnix(b.move?.startedUnix ?? null)}>
                  <span>{formatUnix(b.move?.startedUnix ?? null)}</span>
                </Tooltip>
              </Table.Td>
              <Table.Td>
                <Tooltip label={formatUnix(b.move?.updatedUnix ?? null)}>
                  <span>{formatUnixAge(b.move?.updatedUnix ?? null)}</span>
                </Tooltip>
              </Table.Td>
              <Table.Td>{b.ageSec === null ? '—' : formatAge(b.ageSec * 1000)}</Table.Td>
              <Table.Td>
                {b.move?.lastError == null ? '—' : (
                  <Tooltip label={b.move.lastError}>
                    <Text size="sm" c="red">{truncateText(b.move.lastError, 20)}</Text>
                  </Tooltip>
                )}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
```

- [ ] **Step 6.4: `HealsTab.tsx` (новый файл)**

```tsx
// Вкладка «Heals»: журнал авто-починки, новые сверху (t08 spec §4.12).
import { Table, Text } from '@mantine/core';
import type { HealDto } from '../../api/dto';
import { formatUnix } from '../../utils/format';

export function HealsTab({ heals }: { heals: HealDto[] }) {
  if (heals.length === 0) return <Text c="dimmed">Журнал пуст</Text>;
  return (
    <Table.ScrollContainer minWidth={700}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Бакет</Table.Th>
            <Table.Th>Было</Table.Th>
            <Table.Th>Стало</Table.Th>
            <Table.Th>Причина</Table.Th>
            <Table.Th>Время</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {heals.map((h) => (
            <Table.Tr key={`${h.bucket}-${h.tsUnix ?? 'null'}`}>
              <Table.Td>{h.bucket}</Table.Td>
              <Table.Td>{h.was ?? '—'}</Table.Td>
              <Table.Td>{h.now ?? '—'}</Table.Td>
              <Table.Td>{h.reason ?? '—'}</Table.Td>
              <Table.Td>{formatUnix(h.tsUnix)}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
```

- [ ] **Step 6.5: `StandNodesBlock.tsx` (новый файл)**

```tsx
// Блок «Стендовая топология»: реестр /cluster/nodes/ из снапшота, скрыт при пустом (t08 spec §4.13).
import { Card, Table, Text } from '@mantine/core';
import type { StandNodeDto } from '../../api/dto';

export function StandNodesBlock({ standNodes }: { standNodes: StandNodeDto[] }) {
  if (standNodes.length === 0) return null;
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Стендовая топология</Text>
      <Table w="50%" highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Нода</Table.Th>
            <Table.Th>Адрес</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {standNodes.map((n) => (
            <Table.Tr key={n.name}>
              <Table.Td>{n.name}</Table.Td>
              <Table.Td>{n.address ?? 'есть ключ, адрес пуст'}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Card>
  );
}
```

- [ ] **Step 6.6: `ClusterDetailsPage.tsx` (новый файл)**

```tsx
// Детали кластера: шапка + вкладки Шарды/Бакеты/Переезды/Heals + стендовая топология (t08 spec §4.7–4.8).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Group, Stack, Tabs, Text, Title } from '@mantine/core';
import { Link, useParams } from 'react-router';
import { fetchClusterDetails, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix } from '../utils/format';
import { BucketsTab } from './cluster-details/BucketsTab';
import { HealsTab } from './cluster-details/HealsTab';
import { MovesTab } from './cluster-details/MovesTab';
import { ShardsTab } from './cluster-details/ShardsTab';
import { StandNodesBlock } from './cluster-details/StandNodesBlock';

export function ClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.cluster(cluster),
    queryFn: () => fetchClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  // Паттерн состояний (t08 spec §4.15): 404 → notFound-контент; 503/прочее при
  // отсутствии данных → ErrorSection; polling-сбой при данных — тихо.
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection
        error={query.error}
        onRetry={() => void query.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Кластер не найден</Text>
            <Anchor component={Link} to="/clusters" size="sm">← Кластеры</Anchor>
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
        <Anchor component={Link} to="/clusters" size="sm">← Кластеры</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2}>{data.name}</Title>
          {data.incomplete ? <Badge color="yellow" variant="light">incomplete</Badge> : null}
        </Group>
        <Text c="dimmed" size="sm">
          БД: {data.dbName ?? '—'} · Бакеты: {data.bucketsCount} · Создан: {formatUnix(data.createdUnix)}
        </Text>
      </div>
      <Tabs defaultValue="shards">
        <Tabs.List>
          <Tabs.Tab value="shards">Шарды</Tabs.Tab>
          <Tabs.Tab value="buckets">Бакеты</Tabs.Tab>
          <Tabs.Tab value="moves">Переезды</Tabs.Tab>
          <Tabs.Tab value="heals">Heals</Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel value="shards" pt="sm"><ShardsTab shards={data.shards} /></Tabs.Panel>
        <Tabs.Panel value="buckets" pt="sm"><BucketsTab buckets={data.buckets} /></Tabs.Panel>
        <Tabs.Panel value="moves" pt="sm"><MovesTab buckets={data.buckets} /></Tabs.Panel>
        <Tabs.Panel value="heals" pt="sm"><HealsTab heals={data.heals} /></Tabs.Panel>
      </Tabs>
      <StandNodesBlock standNodes={data.standNodes} />
    </Stack>
  );
}
```

- [ ] **Step 6.7: `App.tsx` — маршрут**

В `frontend/src/App.tsx`: добавить импорт (по алфавиту после `ClustersPage`):

```ts
import { ClusterDetailsPage } from './pages/ClusterDetailsPage';
```

и маршрут после `{ path: 'clusters', element: <ClustersPage /> },`:

```ts
      { path: 'clusters/:cluster', element: <ClusterDetailsPage /> },
```

- [ ] **Step 6.8: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0 (все 6 новых файлов + App.tsx компилируются).

```bash
git add frontend/src/pages/ClusterDetailsPage.tsx frontend/src/pages/cluster-details/ frontend/src/App.tsx
git commit -m "t08: детали кластера — вкладки Шарды/Бакеты/Переезды/Heals и стендовая топология"
```

**Выход:** `/clusters/:cluster` работает: шапка, 4 вкладки, блок топологии; 404-состояние для неизвестного кластера.

**Проверка:** typecheck; визуально (вкл. фильтры/подсветку/404) — Task 11.

**Spec:** §4.7–§4.14, §5, §4.2 (маршрут).

---

### Task 7: OverviewPage (последняя панель — по spec §7)

**Files:**
- Modify: `frontend/src/pages/OverviewPage.tsx` (полная замена заглушки)

**Вход:** Tasks 3–6 слит (компоненты/форматтеры готовы, паттерн состояний §4.15 обкатан на EtcdPage, визуальный словарь таблиц/бейджей — на Clusters/Details). Порядок по spec §7: Overview реализуется последней из панелей — использует оба готовых словаря; перестановка из ревью Фазы 4 (изначально план имел Overview раньше — противоречие §7 устранено).

**Interfaces:**
- Consumes: `queryKeys.overview`, `fetchOverview`, `queryKeys.alerts()`, `fetchAlerts()`; `usePollingIntervalMs()`; `OverviewDto`, `AlertDto` из `../api/dto`; компоненты Task 3.
- Produces: маршрут `/` показывает дашборд (маршрут уже существует в `App.tsx`).

- [ ] **Step 7.1: Полная замена файла**

```tsx
// Панель Обзор: карточки etcd/кластеров/алертов (HA — t09), активные переезды,
// лента алертов critical/warning (t08 spec §4.3–4.5).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Anchor,
  Badge,
  Card,
  Group,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { Link } from 'react-router';
import type { OverviewDto } from '../api/dto';
import { fetchAlerts, fetchOverview, queryKeys } from '../api/queries';
import { BucketStateBadge } from '../components/BucketStateBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix, formatUnixAge } from '../utils/format';

// Сортировка ленты: critical раньше warning, внутри — новые сверху (t08 spec §4.4).
function sortAlertRows(a: { severity: string; sinceUnix: number | null }, b: { severity: string; sinceUnix: number | null }): number {
  const rankA = a.severity === 'critical' ? 0 : 1;
  const rankB = b.severity === 'critical' ? 0 : 1;
  if (rankA !== rankB) return rankA - rankB;
  return (b.sinceUnix ?? 0) - (a.sinceUnix ?? 0);
}

export function OverviewPage() {
  const intervalMs = usePollingIntervalMs();
  // Тот же ключ, что у StaleBadge — TanStack дедуплицирует опрос (t08 spec §3).
  const overview = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: intervalMs,
  });
  const alerts = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: intervalMs,
  });

  if (overview.data === undefined)
    return overview.isError ? (
      <ErrorSection error={overview.error} onRetry={() => void overview.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = overview.data;
  return (
    <Stack gap="md">
      <Title order={2}>Обзор</Title>
      <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }}>
        <EtcdCard data={data} />
        <ClustersCard data={data} />
        <AlertsCard data={data} />
        <Card withBorder padding="md" radius="md">
          <Text fw={600} mb="xs">HA</Text>
          <Text c="dimmed" size="sm">Сводка HA будет реализована в t09</Text>
        </Card>
      </SimpleGrid>
      <MovesSection data={data} />
      <AlertsFeedSection
        isPending={alerts.isPending}
        isError={alerts.isError}
        onRetry={() => void alerts.refetch()}
        rows={(alerts.data ?? []).filter((a) => a.severity !== 'info').sort(sortAlertRows)}
      />
    </Stack>
  );
}

// Карточка etcd: доступность и endpoints ok/total; alarms — в ленте и на панели etcd (t08 spec §4.3).
function EtcdCard({ data }: { data: OverviewDto }) {
  const etcd = data.etcd;
  return (
    <Card
      withBorder
      padding="md"
      radius="md"
      style={{ borderColor: etcd.reachable ? undefined : 'var(--mantine-color-red-6)' }}
    >
      <Group justify="space-between" mb="xs">
        <Text fw={600}>etcd</Text>
        <Badge color={etcd.reachable ? 'teal' : 'red'} variant="light">
          {etcd.reachable ? 'доступен' : 'недоступен'}
        </Badge>
      </Group>
      <Text size="sm" c="dimmed">endpoints: {etcd.endpointsOk}/{etcd.endpointsTotal}</Text>
      <Anchor component={Link} to="/etcd" size="sm" mt="xs" display="inline-block">Детали →</Anchor>
    </Card>
  );
}

// Карточка кластеров: строка на кластер, счётчики (t08 spec §4.3).
function ClustersCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Кластеры</Text>
      {data.clusters.length === 0 ? (
        <Text c="dimmed" size="sm">Кластеры не найдены</Text>
      ) : (
        <Stack gap={4}>
          {data.clusters.map((c) => (
            <Group key={c.name} justify="space-between" gap="xs" wrap="nowrap">
              <Anchor component={Link} to={`/clusters/${c.name}`} size="sm" truncate="end">{c.name}</Anchor>
              <Group gap={5} wrap="nowrap">
                <Text size="sm" c="dimmed">шарды {c.shards}</Text>
                <Text size="sm" c="dimmed">бакеты {c.buckets}</Text>
                {c.activeMoves > 0 ? (
                  <Badge color="yellow" variant="light">переезды: {c.activeMoves}</Badge>
                ) : null}
                {c.masterlessShards > 0 ? (
                  <Badge color="red" variant="light">без мастера: {c.masterlessShards}</Badge>
                ) : null}
              </Group>
            </Group>
          ))}
        </Stack>
      )}
    </Card>
  );
}

// Карточка алертов: счётчики severity, нули — приглушённо (t08 spec §4.3).
function AlertsCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Алерты</Text>
      <Group gap="md">
        <Badge
          color={data.alertsCritical > 0 ? 'red' : 'gray'}
          variant="light"
          size="lg"
        >
          critical: {data.alertsCritical}
        </Badge>
        <Badge
          color={data.alertsWarning > 0 ? 'yellow' : 'gray'}
          variant="light"
          size="lg"
        >
          warning: {data.alertsWarning}
        </Badge>
      </Group>
      <Anchor component={Link} to="/alerts" size="sm" mt="xs" display="inline-block">Все алерты →</Anchor>
    </Card>
  );
}

// Секция активных переездов: таблица не-ACTIVE бакетов всех кластеров (t08 spec §4.5).
function MovesSection({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Активные переезды</Text>
      {data.activeMoves.length === 0 ? (
        <Text c="dimmed" size="sm">Активных переездов нет</Text>
      ) : (
        <Table.ScrollContainer minWidth={700}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Кластер</Table.Th>
                <Table.Th>Бакет</Table.Th>
                <Table.Th>Состояние</Table.Th>
                <Table.Th>Маршрут</Table.Th>
                <Table.Th>Обновлён</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.activeMoves.map((m) => (
                <Table.Tr key={`${m.cluster}-${m.bucket}`}>
                  <Table.Td>
                    <Anchor component={Link} to={`/clusters/${m.cluster}`} size="sm">{m.cluster}</Anchor>
                  </Table.Td>
                  <Table.Td>{m.bucket}</Table.Td>
                  <Table.Td><BucketStateBadge state={m.state} /></Table.Td>
                  <Table.Td>{m.owner ?? '—'} → {m.target ?? '—'}</Table.Td>
                  <Table.Td>
                    <Tooltip label={formatUnix(m.updatedUnix)}>
                      <span>{formatUnixAge(m.updatedUnix)}</span>
                    </Tooltip>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

// Лента алертов: critical/warning, все (без топ-N); info отфильтрован (t08 spec §4.4).
function AlertsFeedSection({ isPending, isError, onRetry, rows }: {
  isPending: boolean;
  isError: boolean;
  onRetry: () => void;
  rows: { id: string; severity: string; kind: string; target: string; message: string; sinceUnix: number | null }[];
}) {
  let content;
  if (isPending) content = <Text c="dimmed" size="sm">Загрузка алертов…</Text>;
  else if (isError)
    content = (
      <Stack gap="xs" align="flex-start">
        <Alert color="red">Нет данных об алертах</Alert>
        <Anchor size="sm" onClick={onRetry}>Повторить</Anchor>
      </Stack>
    );
  else if (rows.length === 0) content = <Text c="teal" size="sm">Критических алертов нет</Text>;
  else
    content = (
      <Stack gap={4}>
        {rows.map((a) => (
          <Group key={a.id} gap="sm" wrap="nowrap" align="flex-start">
            <Badge color={a.severity === 'critical' ? 'red' : 'yellow'} variant="light">{a.severity}</Badge>
            <Text size="sm" ff="monospace">{a.kind}</Text>
            <Text size="sm" ff="monospace" c="dimmed">{a.target}</Text>
            <Text size="sm" style={{ flex: 1 }}>{a.message}</Text>
            <Tooltip label={formatUnix(a.sinceUnix)}>
              <span>
                <Text size="sm" c="dimmed" nowrap>{a.sinceUnix === null ? '—' : `с ${formatUnixAge(a.sinceUnix)}`}</Text>
              </span>
            </Tooltip>
          </Group>
        ))}
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Лента алертов</Text>
        <Anchor component={Link} to="/alerts" size="sm">Все алерты →</Anchor>
      </Group>
      {content}
    </Card>
  );
}
```

Примечания к коду (для исполнителя): `truncate="end"` и `nowrap` — style-пропы Mantine 9 (если typecheck укажет на несоответствие — убрать и оставить обычный `Text`, это не контракт); `Alert` (mantine) импортирован и используется в `AlertsFeedSection`; приглушение нулевых счётчиков алертов (`color="gray"`) — требование spec §4.3 «нули — приглушённо».

- [ ] **Step 7.2: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0. Если ругается на style-пропы `truncate`/`nowrap` — убрать их (см. примечание), повторить.

```bash
git add frontend/src/pages/OverviewPage.tsx
git commit -m "t08: панель Обзор — карточки, активные переезды, лента алертов"
```

**Выход:** `/` — дашборд с карточками etcd/кластеров/алертов (нули приглушённо)/HA-заглушкой, таблицей переездов, лентой critical/warning.

**Проверка:** typecheck; визуально — Task 11.

**Spec:** §4.3–§4.5, §5 (макет Overview), §3 (запросы), §7 (порядок фаз).

---

### Task 8: Подсветка навигации по префиксу

**Files:**
- Modify: `frontend/src/layout/AppLayout.tsx` (строка с `active=` у NavLink)

**Вход:** Task 6 слит (маршрут `/clusters/:cluster` существует).

**Interfaces:**
- Consumes/Produces: только поведение `NavLink active` — сигнатур нет.

- [ ] **Step 8.1: Правка**

В `AppLayout.tsx` заменить у `NavLink`:

```tsx
              active={location.pathname === item.to}
```

на:

```tsx
              active={item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to)}
```

(комментарий над NAV_ITEMS дополнить строкой: «Активность: '/' — точное совпадение, остальные — по префиксу (t08 spec §4.2).»)

- [ ] **Step 8.2: Проверка и коммит**

Run: `cd frontend && npm run typecheck`
Expected: exit 0.

```bash
git add frontend/src/layout/AppLayout.tsx
git commit -m "t08: подсветка навигации по префиксу — Кластеры активны и на деталях"
```

**Выход:** на `/clusters/demo` пункт «Кластеры» подсвечен.

**Проверка:** typecheck; визуально — Task 11.

**Spec:** §4.2.

---

### Task 9: Финальный прогон (весь стек)

**Вход:** Tasks 2–8 слиты.

**Files:** — (без изменений кода; если проверка вскроет дефект — точечная правка отдельным коммитом `t08: фикс <суть>` и повтор прогона).

- [ ] **Step 9.1: Фронтенд**

Run:
```bash
cd frontend && npm run typecheck && npm run build
```
Expected: оба exit 0; `vite build` пишет бандл в `src/AdminPanel.Api/wwwroot/` (index.html + assets/).

- [ ] **Step 9.2: Бэкенд — сборка**

Run: `dotnet build src/AdminPanel.slnx`
Expected: `Build succeeded`, 0 Error / 0 Warning.

- [ ] **Step 9.3: Юнит-тесты**

Run: `dotnet test src/tests/AdminPanel.UnitTests`
Expected: все зелёные (~204: 203 прежних + 1 новый из Task 2; точное число не критерий — критерий «0_failed»).

- [ ] **Step 9.4: Интеграционные тесты**

Run: `dotnet test src/tests/AdminPanel.IntegrationTests` (Docker запущен)
Expected: подавляющее большинство зелёные (~65). Допустим единственный красный — известный флак t90 (тайминги Docker/Testcontainers): перезапустить точечно `dotnet test src/tests/AdminPanel.IntegrationTests --filter "<имя флак-теста>"`; если точечный прогон зелёный — дефектом не считать, приёмку не блокирует. Любой ДРУГОЙ красный — чинить (это регрессия t08).

- [ ] **Step 9.5: Regression-границы**

Run: `git status --short`
Expected: пусто (wwwroot/node_modules игнорируются); `git log --oneline` — коммиты `t08: …` по Tasks 1–8.

**Выход:** ветка полностью зелёная; known-флак задокументирован в отчёте исполнения.

**Проверка:** сами шаги 9.1–9.5.

**Spec:** §13.1–13.2 (критерии приёмки сборки/тестов).

---

### Task 10: Roadmap-деливерабл

**Files:**
- Modify: `arch/roadmap/frontend.md`

**Вход:** Task 9 пройден (функциональность завершена и проверена).

- [ ] **Step 10.1: Удалить пункт t08**

Из `arch/roadmap/frontend.md` удалить целиком запись (строки вида):

```markdown
- `t08-frontend-clusters` ← `t05-sharding-api`, `t07-frontend-base` — панели
  Overview, etcd, Clusters. Overview: карточки etcd/кластеров/HA-сводки,
  активные переезды, лента алертов. etcd: endpoints (+метка «активный»),
  members/лидер, alarms, lastRefresh. Clusters: список → детали с вкладками
  Шарды / Бакеты (грид id×owner×state, фильтры, подсветка не-ACTIVE,
  возраст) / Переезды / Heals (+блок «Стендовая топология», если есть).
```

Запись `t09-frontend-ha` НЕ трогать (её зависимости — t06, t07; от t08 она не зависит — обновлений `←` не требуется). Никаких пометок «сделано» не оставлять (правила `arch/roadmap/README.md`, spec §12).

- [ ] **Step 10.2: Коммит**

```bash
git add arch/roadmap/frontend.md
git commit -m "t08: пункт roadmap t08-frontend-clusters выполнен"
```

**Выход:** в backlog остаются только несделанные задачи.

**Проверка:** `grep -n "t08" arch/roadmap/*.md` → нет вхождений в списках задач.

**Spec:** §12.

---

### Task 11: Ручной HTTP-сценарий (dotnet run + curl)

**Вход:** Task 9 пройден (бандл в wwwroot); Docker-стенд `arch/04` опционален.

**Files:** — (только проверки; дефекты — отдельные коммиты-фиксы).

- [ ] **Step 11.1: Запуск хоста**

```bash
dotnet run --project src/AdminPanel.Api
```
(в фоне или отдельном терминале; Development-профиль: admin/admin, `http://localhost:5000`; ждём строку запуска в логе).

- [ ] **Step 11.2: SPA-раздача**

```bash
curl -si http://localhost:5000/ | head -3
curl -si http://localhost:5000/clusters | head -3
```
Expected: оба `200` + `text/html` (index.html — корень и SPA-fallback).

- [ ] **Step 11.3: API без сессии**

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5000/api/overview
```
Expected: `401`.

- [ ] **Step 11.4: Логин и инспекция**

```bash
curl -si -c /tmp/t08-cookies.txt -X POST -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' http://localhost:5000/api/auth/login | head -1
curl -sb /tmp/t08-cookies.txt http://localhost:5000/api/clusters/demo
```
Expected: логин `204`; детали кластера: без etcd — `503` ProblemDetails «Snapshot not ready» (валидное состояние: страница деталей в браузере покажет «Данные ещё не собраны» + «Повторить»); с поднятым стендом `arch/04` — `200` JSON, содержащий `"standNodes":[...]` (в стенде реестр `/cluster/nodes/` засеян).

- [ ] **Step 11.5: Браузерная проверка (опционально, если стенд поднят; иначе — dev-режим без данных)**

Открыть `http://localhost:5000/`: логин admin/admin → Обзор (карточки/переезды/лента), etcd (метки «активный»/«лидер»), Кластеры → детали demo: вкладки с данными; фильтры Бакетов («не-ACTIVE», владелец) и жёлтая подсветка строк; `/clusters/unknown` → «Кластер не найден»; пункт «Кластеры» в навигации подсвечен на деталях; переключатель polling 2/5/15/off меняет частоту запросов (DevTools Network), выбор переживает F5.

- [ ] **Step 11.6: Останов**

Остановить `dotnet run` (Ctrl-C); удалить `/tmp/t08-cookies.txt`.

**Выход:** ручная приёмка spec §13.4 пройдена (или зафиксированы дефекты с коммитами-фиксами).

**Проверка:** сами curl-шаги.

**Spec:** §13.4–13.5.

---

## Итоговая самопроверка (выполнена при написании плана; порядок задач приведён к spec §7 по итогам ревью Фазы 4)

1. **Покрытие spec**: §8/§9 → Task 2; §4.16/§4.18 → Task 3; §5 → Task 4 (EtcdPage); §4.6 → Task 5; §4.7–4.14 → Task 6 (маршрут — Step 6.7); §4.3–4.5 → Task 7 (Overview — последняя панель, порядок spec §7); §4.2 → Task 8; §13.1–13.2 → Task 9; §12 → Task 10; §13.4–13.5 → Task 11; §11 → Task 1. Ограничения §10 — не реализуются ни в одном шаге (проверено). Minor-рекомендации ревью Фазы 4 учтены: нули AlertsCard приглушены (`color="gray"`, spec §4.3); `lastError` в MovesTab — усечённый текст + Tooltip (spec §4.11); порядок панелей — по spec §7; подсчёты вызовов/тестов в Task 2 точные (9 вызовов `Map`; 8 старых [Fact] → 9).
2. **Плейсхолдеры**: отсутствуют; все файлы приведены полным кодом.
3. **Типы/имена**: `StandNodeDto` (C# и TS), `ClusterDto.StandNodes`/`standNodes`, `Map(..., IReadOnlyList<StandNode>)`, `formatBytes/formatUnix/formatIso/formatUnixAge`, `LoadingSection/ErrorSection/BucketStateBadge`, `truncateText` (MovesTab), `queryKeys.cluster(cluster)` — согласованы между задачами; Mantine-вызовы сверены с фактическими типами 9.5.2 (`Table.ScrollContainer`, `Tabs.*`, `SimpleGrid cols responsive`, `Tooltip multiline`); `--mantine-color-yellow-light` существует в styles.css.
