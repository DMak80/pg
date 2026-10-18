// Панель Кластеры: сводный список кластеров снапшота (t08 spec §4.6) + создание (t12).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import type { ClusterSummaryDto } from '../api/dto';
import { fetchClusters, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { ClusterCreateModal } from './clusters/ClusterCreateModal';

export function ClustersPage() {
  const intervalMs = usePollingIntervalMs();
  const [createOpened, setCreateOpened] = useState(false);
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
      <Group justify="space-between" mb="md">
        <Title order={2}>Clusters</Title>
        <Button onClick={() => setCreateOpened(true)}>Create cluster</Button>
      </Group>
      <ClusterCreateModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">No clusters found</Text>
        ) : (
          <Table.ScrollContainer minWidth={800}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Cluster</Table.Th>
                  <Table.Th>DB</Table.Th>
                  <Table.Th>Buckets</Table.Th>
                  <Table.Th>Shards</Table.Th>
                  <Table.Th>Moves</Table.Th>
                  <Table.Th>Flags</Table.Th>
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
      {/* Нешардированная БД = 1 бакет: счётчик не информативен — прочерк. */}
      <Table.Td>{cluster.bucketsCount === 1 ? '—' : cluster.bucketsCount}</Table.Td>
      {/* Нешардированная БД = единственный вырожденный шард — прочерк. */}
      <Table.Td>
        {cluster.sharded ? (
          <Text c={mastersMissing > 0 ? 'red' : undefined}>
            {cluster.shardsWithMaster}/{cluster.shardsTotal}
          </Text>
        ) : (
          '—'
        )}
      </Table.Td>
      <Table.Td>
        <Text c={cluster.activeMoves > 0 ? 'yellow' : undefined}>{cluster.activeMoves}</Text>
      </Table.Td>
      <Table.Td>
        {cluster.incomplete ? <Badge color="yellow" variant="light">incomplete</Badge> : null}
        {/* t06: бейдж режима strict/availability (arch/03 §3, компактный) */}
        {cluster.synchronousModeStrict
          ? <Badge color="red" variant="light" ml={cluster.incomplete ? 5 : 0}>strict</Badge>
          : <Badge color="teal" variant="light" ml={cluster.incomplete ? 5 : 0}>avail.</Badge>}
        {cluster.notInitialized ? (
          <Tooltip label="cluster requested, nodes not up">
            <Badge color="gray" variant="light" ml={cluster.incomplete ? 5 : 0}>not initialized</Badge>
          </Tooltip>
        ) : null}
        {cluster.toRemove ? (
          <Tooltip label="moved to TO_REMOVE, waiting for orchestrator cleanup">
            <Badge color="red" variant="light" ml={cluster.incomplete || cluster.notInitialized ? 5 : 0}>
              to remove
            </Badge>
          </Tooltip>
        ) : null}
        {mastersMissing > 0 ? (
          <Badge color="red" variant="light" ml={cluster.incomplete || cluster.notInitialized ? 5 : 0}>
            {mastersMissing} masterless
          </Badge>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
