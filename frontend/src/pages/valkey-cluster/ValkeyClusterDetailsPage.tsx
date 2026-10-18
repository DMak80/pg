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
import type { TicketOutcomeDto, ValkeyNodeDto } from '../../api/dto';

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
            <Tooltip label="the worker tears down the node container and etcd keys">
              <Badge color="red" variant="light">to remove</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">not initialized</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label={`the ${c.rotation.role}-password rotation ticket is live: the worker applies the two-password window`}>
              <Badge color="blue" variant="light">
                {c.rotation.role}-password rotation · {rotationAgeMinutes(c.rotation.requestedUnix)}
              </Badge>
            </Tooltip>
          ) : null}
          {c.caRotation != null ? (
            <Tooltip label={`the CA rotation ticket is live: the worker applies the dual-trust window (${c.caRotation.requestedBy ?? '—'})`}>
              <Badge color="violet" variant="light">
                CA rotation · {rotationAgeMinutes(c.caRotation.requestedUnix)}
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
          <Field label="Nodes">{String(c.nodesTotal)}</Field>
          <Field label="maxmemory">
            {`${Math.round(c.maxmemoryBytes / MIB)} MiB · ${c.maxmemoryPolicy}`}
          </Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Created">
            {c.createdUnix !== null ? new Date(c.createdUnix * 1000).toLocaleString() : '—'}
          </Field>
        </SimpleGrid>
      </Card>

      {/* Вкладка Нода (одна node1 в v1 — без Tabs-обёртки, образец BrokersTab). */}
      {node !== null ? (
        <Card withBorder padding="md" radius="md">
          <Group justify="space-between" mb="sm">
            <Title order={4}>Node</Title>
            {canMutate ? (
              <Button size="xs" onClick={() => setResourcesNode(node)}>Edit resources</Button>
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
            <Field label="Name">{node.name}</Field>
            <Field label="State"><NodeStateBadge state={node.state} /></Field>
            <Field label="Resources">
              {`${node.cpu ?? '—'} cpu · ${node.memGi ?? '—'} Gi · ${node.diskGi ?? '—'} Gi`}
            </Field>
            <Field label="Live">
              {node.live === null ? (
                <Tooltip label={node.probeError ?? 'the probe is silent (no credentials or the tick has not arrived)'}>
                  <Text c="dimmed">probe silent</Text>
                </Tooltip>
              ) : node.live ? (
                <Badge color="green" variant="light">alive</Badge>
              ) : (
                <Tooltip label={node.probeError ?? 'PING did not respond'}>
                  <Badge color="red" variant="light">not responding</Badge>
                </Tooltip>
              )}
            </Field>
            <Field label="Probe">
              {node.probeError !== null ? (
                <Tooltip label={node.probeError}>
                  <Text size="sm" c="orange" style={{ maxWidth: 220 }} truncate>probe error</Text>
                </Tooltip>
              ) : (
                <Text c="dimmed">—</Text>
              )}
            </Field>
          </SimpleGrid>
        </Card>
      ) : (
        <Card withBorder padding="md" radius="md">
          <Text c="dimmed">No nodes declared</Text>
        </Card>
      )}

      {/* Последний исход заявки (t10): expired подсвечен warning-цветом. */}
      {c.ticketOutcome !== null ? (
        <TicketOutcomeLine outcome={c.ticketOutcome} />
      ) : null}
    </Stack>
  );
}

// Возраст живой заявки ротации в минутах (бейдж шапки).
function rotationAgeMinutes(requestedUnix: number): string {
  const minutes = Math.max(0, Math.floor((Date.now() / 1000 - requestedUnix) / 60));
  return `${minutes} min`;
}

// Строка «Последний исход заявки» (t10) — порт kafka-деталей.
function TicketOutcomeLine({ outcome }: { outcome: TicketOutcomeDto }) {
  const expired = outcome.outcome === 'expired';
  return (
    <Text size="sm" c={expired ? 'yellow' : undefined}>
      Last ticket outcome: {outcome.kind} → {outcome.outcome} (
      {new Date(outcome.finishedUnix * 1000).toLocaleString()})
      {outcome.reason ? ` — reason: ${outcome.reason}` : ''}
    </Text>
  );
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
