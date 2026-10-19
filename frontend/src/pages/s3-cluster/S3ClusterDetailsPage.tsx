// Детали s3-кластера: шапка (state-бейджи, endpoints, etcd-признак, кнопки
// зонд/ротация/удаление) + вкладки Ноды/Репликация/Связность и синхронность
// (arch/03 §12.3). Порт ValkeyClusterDetailsPage; canMutate = ACTIVE.
import { useQuery } from '@tanstack/react-query';
import { Badge, Card, Group, SimpleGrid, Stack, Tabs, Text, Title, Tooltip } from '@mantine/core';
import { useParams } from 'react-router';
import { fetchS3ClusterDetails, s3QueryKeys } from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { ConnectivitySyncTab } from './ConnectivitySyncTab';
import { DeleteS3ClusterButton } from './DeleteS3ClusterButton';
import { NodesTab } from './NodesTab';
import { ReplicationTab } from './ReplicationTab';
import { RotateSecretsButton } from './RotateSecretsButton';
import { SyncProbeButton } from './SyncProbeButton';

export function S3ClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: s3QueryKeys.cluster(cluster),
    queryFn: () => fetchS3ClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const c = query.data;
  // Мутации — только у Active (у NOT_INITIALIZED нечего мутировать; у
  // TO_REMOVE воркер уже демонтирует — кнопки скрыты, симметрия valkey).
  const canMutate = c.state === 'ACTIVE';

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Group gap="sm">
          <Title order={2}>S3: {c.name}</Title>
          {c.state === 'TO_REMOVE' ? (
            <Tooltip label="the worker tears down instances and etcd keys">
              <Badge color="red" variant="light">to remove</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">not initialized</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label="secrets rotation ticket is live: the worker re-issues the cluster credentials via rolling recreation">
              <Badge color="blue" variant="light">
                secrets rotation · {c.rotation.requestedBy ?? '—'}
              </Badge>
            </Tooltip>
          ) : null}
        </Group>
        {canMutate ? (
          <Group gap="sm">
            <SyncProbeButton cluster={c.name} />
            <RotateSecretsButton cluster={c.name} disabled={c.rotation !== null} />
            <DeleteS3ClusterButton cluster={c.name} />
          </Group>
        ) : null}
      </Group>

      <Card withBorder padding="md" radius="md">
        <SimpleGrid cols={{ base: 2, md: 4 }}>
          <Field label="Platform">{c.platform}</Field>
          <Field label="Etcd backup">{c.etcdBackup ? 'yes' : 'no'}</Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Created">
            {c.createdUnix !== null ? new Date(c.createdUnix * 1000).toLocaleString() : '—'}
          </Field>
        </SimpleGrid>
      </Card>

      {/* Креды клиентской грани (arch/23 §2): состав без значений — их в API нет. */}
      <Card withBorder padding="md" radius="md">
        <Group justify="space-between" mb="xs">
          <Text size="sm" fw={500}>Client credentials</Text>
          {c.etcdBackup ? (
            <Tooltip label="the backups key of this cluster is external: the etcd backup special key (per-install env pair)">
              <Badge color="violet" variant="light">backups key is external</Badge>
            </Tooltip>
          ) : null}
        </Group>
        {c.rootUser === null ? (
          <Text c="dimmed">Not provisioned yet</Text>
        ) : (
          <SimpleGrid cols={{ base: 1, md: 2 }}>
            <Field label="Root user">
              <Text size="sm" ff="monospace">{c.rootUser}</Text>
            </Field>
            <Field label="Access keys">
              <Stack gap={2}>
                {(c.accessKeys ?? []).map((k) => (
                  <Text key={k.name} size="sm" ff="monospace">{k.name} ({k.policy})</Text>
                ))}
              </Stack>
            </Field>
          </SimpleGrid>
        )}
      </Card>

      <Tabs defaultValue="nodes" keepMounted={false}>
        <Tabs.List>
          <Tabs.Tab value="nodes">Nodes</Tabs.Tab>
          <Tabs.Tab value="replication">Replication</Tabs.Tab>
          <Tabs.Tab value="probes">Connectivity and sync</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="nodes" pt="md">
          {c.nodesList.length === 0 && c.state === 'NOT_INITIALIZED' ? (
            <Card withBorder padding="md" radius="md">
              <Text c="dimmed">No nodes declared — the cluster is a declaration without deployment</Text>
            </Card>
          ) : (
            <Card withBorder padding="md" radius="md">
              <NodesTab cluster={c} canMutate={canMutate} />
            </Card>
          )}
        </Tabs.Panel>

        <Tabs.Panel value="replication" pt="md">
          <Card withBorder padding="md" radius="md">
            <ReplicationTab cluster={c} canMutate={canMutate} />
          </Card>
        </Tabs.Panel>

        <Tabs.Panel value="probes" pt="md">
          <Card withBorder padding="md" radius="md">
            <ConnectivitySyncTab cluster={c} />
          </Card>
        </Tabs.Panel>
      </Tabs>
    </Stack>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Stack gap={2}>
      <Text size="xs" c="dimmed" tt="uppercase">{label}</Text>
      {typeof children === 'string' ? <Text>{children}</Text> : children}
    </Stack>
  );
}
