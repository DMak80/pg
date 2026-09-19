// Панель Valkey: сводный список valkey-кластеров снапшота (arch/03 §8.3) + создание.
// Порт KafkaClustersPage с усечением: одна нода, нет топиков/ребалансов/CA.
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import type { ValkeyClusterSummaryDto } from '../api/dto';
import { fetchValkeyClusters, valkeyQueryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { CreateValkeyClusterModal } from './valkey-cluster/CreateValkeyClusterModal';

const MIB = 1024 * 1024;

// maxmemory человекочитаемо: ≥ 1 GiB — в GiB, иначе в MiB (создание — в MiB).
function formatBytes(bytes: number): string {
  return bytes >= 1024 * MIB
    ? `${Math.round(bytes / MIB / 1024)} GiB`
    : `${Math.round(bytes / MIB)} MiB`;
}

export function ValkeyClustersPage() {
  const intervalMs = usePollingIntervalMs();
  const [createOpened, setCreateOpened] = useState(false);
  const query = useQuery({
    queryKey: valkeyQueryKeys.clusters,
    queryFn: fetchValkeyClusters,
    refetchInterval: intervalMs,
  });

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
        <Title order={2}>Valkey-кластеры</Title>
        <Button onClick={() => setCreateOpened(true)}>Создать кластер</Button>
      </Group>
      <CreateValkeyClusterModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">Valkey-кластеры не найдены</Text>
        ) : (
          <Table.ScrollContainer minWidth={900}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Кластер</Table.Th>
                  <Table.Th>Состояние</Table.Th>
                  <Table.Th>Нода</Table.Th>
                  <Table.Th>Endpoints</Table.Th>
                  <Table.Th>maxmemory</Table.Th>
                  <Table.Th>Пометки</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {clusters.map((c) => <ValkeyClusterRow key={c.name} cluster={c} />)}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        )}
      </Card>
    </>
  );
}

function ValkeyClusterRow({ cluster }: { cluster: ValkeyClusterSummaryDto }) {
  const down = cluster.nodesTotal - cluster.nodesRunning;
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/valkey/${cluster.name}`}>{cluster.name}</Anchor>
      </Table.Td>
      <Table.Td>
        {cluster.state === 'ACTIVE' ? (
          <Badge color="green" variant="light">ACTIVE</Badge>
        ) : cluster.state === 'NOT_INITIALIZED' ? (
          <Tooltip label="кластер заявлен, нода не поднята">
            <Badge color="gray" variant="light">не инициализирован</Badge>
          </Tooltip>
        ) : (
          <Tooltip label="TO_REMOVE: воркер демонтирует контейнер и ключи">
            <Badge color="red" variant="light">к удалению</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        <Text c={down > 0 ? 'red' : undefined}>
          {cluster.nodesRunning}/{cluster.nodesTotal}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" ff="monospace">{cluster.endpoints ?? '—'}</Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm">
          {formatBytes(cluster.maxmemoryBytes)} · {cluster.maxmemoryPolicy}
        </Text>
      </Table.Td>
      <Table.Td>
        {cluster.rotationPending ? (
          <Tooltip label="заявка ротации пароля жива: воркер применяет окно двух паролей">
            <Badge color="blue" variant="light">ротация</Badge>
          </Tooltip>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
