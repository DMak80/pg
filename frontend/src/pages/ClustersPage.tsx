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
        <Title order={2}>Кластеры</Title>
        <Button onClick={() => setCreateOpened(true)}>Создать кластер</Button>
      </Group>
      <ClusterCreateModal opened={createOpened} onClose={() => setCreateOpened(false)} />
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
      {/* Нешардированная БД = 1 бакет: счётчик не информативен — прочерк. */}
      <Table.Td>{cluster.bucketsCount === 1 ? '—' : cluster.bucketsCount}</Table.Td>
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
        {cluster.notInitialized ? (
          <Tooltip label="кластер заявлен, ноды не подняты">
            <Badge color="gray" variant="light" ml={cluster.incomplete ? 5 : 0}>не инициализирован</Badge>
          </Tooltip>
        ) : null}
        {cluster.toRemove ? (
          <Tooltip label="переведён в TO_REMOVE, ждёт очистки оркестратором">
            <Badge color="red" variant="light" ml={cluster.incomplete || cluster.notInitialized ? 5 : 0}>
              к удалению
            </Badge>
          </Tooltip>
        ) : null}
        {mastersMissing > 0 ? (
          <Badge color="red" variant="light" ml={cluster.incomplete || cluster.notInitialized ? 5 : 0}>
            {mastersMissing} без мастера
          </Badge>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
