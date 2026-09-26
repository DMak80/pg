// Детали valkey-кластера: шапка (state-бейджи, ротация role+возраст, мутации)
// + вкладка Нода (resources/live) (arch/03 §8.3). Порт KafkaClusterDetailsPage
// с усечением: одна нода node1, нет топиков/групп/ребалансов/CA.
import { useQuery } from '@tanstack/react-query';
import { Badge, Button, Card, Group, SimpleGrid, Stack, Text, Title, Tooltip } from '@mantine/core';
import { useParams } from 'react-router';
import { useState } from 'react';
import { fetchValkeyClusterDetails, valkeyQueryKeys } from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { DeleteValkeyClusterButton } from './DeleteValkeyClusterButton';
import { EditClusterConfigModal } from './EditClusterConfigModal';
import { EditNodeResourcesModal } from './EditNodeResourcesModal';
import { RotateCaButton } from './RotateCaButton';
import { RotatePasswordButton } from './RotatePasswordButton';
import type { ValkeyNodeDto } from '../../api/dto';

const MIB = 1024 * 1024;

export function ValkeyClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const [resourcesNode, setResourcesNode] = useState<ValkeyNodeDto | null>(null);
  const query = useQuery({
    queryKey: valkeyQueryKeys.cluster(cluster),
    queryFn: () => fetchValkeyClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const c = query.data;
  // Мутации — только у Active (TO_REMOVE скрыты: воркер уже демонтирует).
  const canMutate = c.state === 'ACTIVE';
  const node = c.nodesList[0] ?? null;

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Group gap="sm">
          <Title order={2}>Valkey: {c.name}</Title>
          {c.state === 'TO_REMOVE' ? (
            <Tooltip label="воркер демонтирует контейнер ноды и ключи etcd">
              <Badge color="red" variant="light">к удалению</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">не инициализирован</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label={`заявка ротации ${c.rotation.role}-пароля жива: воркер применяет окно двух паролей`}>
              <Badge color="blue" variant="light">
                ротация {c.rotation.role}-пароля · {rotationAgeMinutes(c.rotation.requestedUnix)}
              </Badge>
            </Tooltip>
          ) : null}
          {c.caRotation != null ? (
            <Tooltip label={`заявка ротации CA жива: воркер применяет окно двойного доверия (${c.caRotation.requestedBy ?? '—'})`}>
              <Badge color="violet" variant="light">
                ротация CA · {rotationAgeMinutes(c.caRotation.requestedUnix)}
              </Badge>
            </Tooltip>
          ) : null}
        </Group>
        {canMutate ? (
          <Group gap="sm">
            <EditClusterConfigModal cluster={c} />
            <RotatePasswordButton cluster={c.name} role="app" disabled={c.rotation !== null} />
            <RotatePasswordButton cluster={c.name} role="admin" disabled={c.rotation !== null} />
            <RotateCaButton cluster={c.name} disabled={c.rotation !== null || c.caRotation != null} />
            <DeleteValkeyClusterButton cluster={c.name} />
          </Group>
        ) : null}
      </Group>

      <Card withBorder padding="md" radius="md">
        <SimpleGrid cols={{ base: 2, md: 4 }}>
          <Field label="Нод">{String(c.nodesTotal)}</Field>
          <Field label="maxmemory">
            {`${Math.round(c.maxmemoryBytes / MIB)} MiB · ${c.maxmemoryPolicy}`}
          </Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Создан">
            {c.createdUnix !== null ? new Date(c.createdUnix * 1000).toLocaleString() : '—'}
          </Field>
        </SimpleGrid>
      </Card>

      {/* Вкладка Нода (одна node1 в v1 — без Tabs-обёртки, образец BrokersTab). */}
      {node !== null ? (
        <Card withBorder padding="md" radius="md">
          <Group justify="space-between" mb="sm">
            <Title order={4}>Нода</Title>
            {canMutate ? (
              <Button size="xs" onClick={() => setResourcesNode(node)}>Изменить ресурсы</Button>
            ) : null}
          </Group>
          {resourcesNode !== null ? (
            <EditNodeResourcesModal
              cluster={cluster}
              node={resourcesNode}
              opened
              onClose={() => setResourcesNode(null)}
            />
          ) : null}
          <SimpleGrid cols={{ base: 2, md: 5 }}>
            <Field label="Имя">{node.name}</Field>
            <Field label="Состояние"><NodeStateBadge state={node.state} /></Field>
            <Field label="Ресурсы">
              {`${node.cpu ?? '—'} cpu · ${node.memGi ?? '—'} Gi · ${node.diskGi ?? '—'} Gi`}
            </Field>
            <Field label="Live">
              {node.live === null ? (
                <Tooltip label={node.probeError ?? 'проба молчит (кредов нет или тик не дошёл)'}>
                  <Text c="dimmed">проба молчит</Text>
                </Tooltip>
              ) : node.live ? (
                <Badge color="green" variant="light">жив</Badge>
              ) : (
                <Tooltip label={node.probeError ?? 'PING не ответил'}>
                  <Badge color="red" variant="light">не отвечает</Badge>
                </Tooltip>
              )}
            </Field>
            <Field label="Проба">
              {node.probeError !== null ? (
                <Tooltip label={node.probeError}>
                  <Text size="sm" c="orange" style={{ maxWidth: 220 }} truncate>ошибка пробы</Text>
                </Tooltip>
              ) : (
                <Text c="dimmed">—</Text>
              )}
            </Field>
          </SimpleGrid>
        </Card>
      ) : (
        <Card withBorder padding="md" radius="md">
          <Text c="dimmed">Ноды не заявлены</Text>
        </Card>
      )}
    </Stack>
  );
}

// Возраст живой заявки ротации в минутах (бейдж шапки).
function rotationAgeMinutes(requestedUnix: number): string {
  const minutes = Math.max(0, Math.floor((Date.now() / 1000 - requestedUnix) / 60));
  return `${minutes} мин`;
}

function NodeStateBadge({ state }: { state: string | null }) {
  const color = state === 'RUNNING'
    ? 'green'
    : state === 'PROVISIONING'
      ? 'blue'
      : state === 'UNREACHABLE'
        ? 'red'
        : state === 'TO_REMOVE' || state === 'REMOVING'
          ? 'orange'
          : 'gray';
  return <Badge color={color} variant="light">{state ?? '—'}</Badge>;
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Stack gap={2}>
      <Text size="xs" c="dimmed" tt="uppercase">{label}</Text>
      {typeof children === 'string' ? <Text>{children}</Text> : children}
    </Stack>
  );
}
