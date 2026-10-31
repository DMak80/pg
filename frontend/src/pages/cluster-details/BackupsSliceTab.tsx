// Вкладка «Бэкапы (срез)» pg-кластера (u04-связь «кластер → его бэкапы/RPO»):
// RPO/WAL/полные шардов кластера из существующего GET /api/backups/storage
// (клиентский фильтр по кластеру) + policy кластера; ссылки на /backups-storage
// и /reliability; строки полных — на детали шарда бэкапов (arch/03 §3).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Button, Group, Stack, Table, Text } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import { backupsQueryKeys, fetchBackupsStorage, fetchClusterDetails, fetchS3Pools, queryKeys } from '../../api/queries';
import { PolicyBackupsModal } from '../backups-storage/PolicyBackupsModal';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { formatBytes, formatUnix } from '../../utils/format';

export function BackupsSliceTab({ cluster }: { cluster: string }) {
  const intervalMs = usePollingIntervalMs();
  const storage = useQuery({
    queryKey: backupsQueryKeys.storage,
    queryFn: fetchBackupsStorage,
    refetchInterval: intervalMs,
  });
  const details = useQuery({
    queryKey: queryKeys.cluster(cluster),
    queryFn: () => fetchClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  const storageData = storage.data;
  const clusters = storageData?.clusters ?? [];
  const [policyOpened, setPolicyOpened] = useState(false);
  const poolsQuery = useQuery({
    queryKey: backupsQueryKeys.pools,
    queryFn: fetchS3Pools,
    enabled: policyOpened,
    staleTime: 30_000,
  });
  const slice = clusters.find((c) => c.cluster === cluster) ?? null;
  const policy = slice?.policy ?? null;
  const shards = details.data?.shards.map((s) => s.name) ?? [];

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Text fw={500}>Бэкапы кластера (срез)</Text>
        <Group gap="xs">
          {/* u16/D14: та же форма Policy, что на /backups-storage */}
          <Button size="xs" variant="light" onClick={() => setPolicyOpened(true)}>
            Policy бэкапов
          </Button>
          <Anchor component={Link} to="/backups-storage" size="sm">Хранилище бэкапов →</Anchor>
          <Anchor component={Link} to="/reliability" size="sm">Надёжность (RPO/RTO) →</Anchor>
        </Group>
      </Group>

      {storage.data?.configured === false ? (
        <Text c="dimmed" size="sm">Хранилище бэкапов не настроено — подсистема выключена.</Text>
      ) : slice === null ? (
        <Text c="dimmed" size="sm">
          Кластера нет в дереве бэкапов — ключей /pgworker/backups/{cluster}/ нет
          (подсистема для него не включена) либо данные ещё не тикали.
        </Text>
      ) : (
        <>
          {/* policy кластера (arch/19 §4): null-поля — дефолт воркера. */}
          <Group gap="lg">
            <Text size="sm">
              Policy: retention {policy?.retentionDays ?? '—'}d/{policy?.retentionWeeks ?? '—'}w/
              {policy?.retentionMonths ?? '—'}m · полный ≤ {policy?.fullMaxAgeSec ?? '—'} с ·
              verify: {policy?.verifyOnCreate === null || policy?.verifyOnCreate === undefined
                ? '—'
                : policy.verifyOnCreate
                  ? 'вкл'
                  : 'выкл'} ·
              дрилл: {policy?.drillIntervalDays ?? '—'} сут
            </Text>
          </Group>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Шард</Table.Th>
                <Table.Th>Полных</Table.Th>
                <Table.Th>WAL-сегментов</Table.Th>
                <Table.Th>Размер</Table.Th>
                <Table.Th>Сверка</Table.Th>
                <Table.Th>Детали</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {(shards.length > 0 ? shards : Object.keys(byShard(slice))).map((shard) => {
                const row = slice.shards.find((x) => x.shard === shard);
                return (
                  <Table.Tr key={shard}>
                    <Table.Td>{shard}</Table.Td>
                    <Table.Td>{row ? String(row.fullsCount) : '—'}</Table.Td>
                    <Table.Td>{row ? String(row.walSegmentCount) : '—'}</Table.Td>
                    <Table.Td>{formatBytes(row?.sizeBytes ?? 0)}</Table.Td>
                    <Table.Td>
                      {row ? <Text size="sm">{row.orphan ? 'сирота' : row.hasS3Only ? 'объекты без ключа' : 'ok'}</Text> : '—'}
                    </Table.Td>
                    <Table.Td>
                      <Anchor component={Link} size="sm"
                        to={`/backups-storage/${cluster}/${shard}`}>детали →</Anchor>
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
          <Text size="xs" c="dimmed">Штамп инвентаря: {formatUnix(storageData?.inventoryUpdatedUnix ?? 0)}</Text>
        </>
      )}
      {policyOpened ? (
        <PolicyBackupsModal
          cluster={cluster}
          policy={storageData?.clusters.find((c) => c.cluster === cluster)?.policy ?? null}
          pools={poolsQuery.data ?? []}
          onClose={() => setPolicyOpened(false)}
          onSaved={() => {
            void storage.refetch();
            void details.refetch();
          }}
        />
      ) : null}
    </Stack>
  );
}

// Шарды из дерева бэкапов (если декларация ещё не тикала).
function byShard(slice: { shards: { shard: string }[] }): Record<string, null> {
  return Object.fromEntries(slice.shards.map((s) => [s.shard, null]));
}
