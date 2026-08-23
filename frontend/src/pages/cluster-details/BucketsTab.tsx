// Вкладка «Бакеты»: грид id×owner×state, локальные фильтры, подсветка переездов +
// нейтральная для NOT_INITIALIZED, возраст не-ACTIVE статуса (t08 spec §4.9; t12).
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
  { value: 'NOT_INITIALIZED', label: 'NOT_INITIALIZED' },
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
  // Жёлтый фон — только реальные переезды (SYNCING/FROZEN/ABORTING);
  // NOT_INITIALIZED — нейтральный серый: это начальное состояние
  // создаваемого кластера, не деградация (spec t12 §3.8).
  const moveRow = bucket.state === 'SYNCING' || bucket.state === 'FROZEN' || bucket.state === 'ABORTING';
  const notInitialized = bucket.state === 'NOT_INITIALIZED';
  const phase = bucket.move?.phase ?? null;
  const lastError = bucket.move?.lastError ?? null;
  return (
    <Table.Tr
      style={{
        backgroundColor: moveRow
          ? 'var(--mantine-color-yellow-light)'
          : notInitialized
            ? 'var(--mantine-color-gray-light)'
            : undefined,
      }}
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
