// Детали s3-кластера: шапка (state-бейджи, endpoints, secretProfile, кнопки
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
            <Tooltip label="воркер демонтирует инстансы и ключи etcd">
              <Badge color="red" variant="light">к удалению</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">не инициализирован</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label="заявка ротации секретов жива: воркер переиздаёт профиль rolling-пересозданием">
              <Badge color="blue" variant="light">
                ротация секретов · {c.rotation.requestedBy ?? '—'}
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
          <Field label="Платформа">{c.platform}</Field>
          <Field label="Профиль секретов">{c.secretProfile || '—'}</Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Создан">
            {c.createdUnix !== null ? new Date(c.createdUnix * 1000).toLocaleString() : '—'}
          </Field>
        </SimpleGrid>
      </Card>

      <Tabs defaultValue="nodes" keepMounted={false}>
        <Tabs.List>
          <Tabs.Tab value="nodes">Ноды</Tabs.Tab>
          <Tabs.Tab value="replication">Репликация</Tabs.Tab>
          <Tabs.Tab value="probes">Связность и синхронность</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="nodes" pt="md">
          {c.nodesList.length === 0 && c.state === 'NOT_INITIALIZED' ? (
            <Card withBorder padding="md" radius="md">
              <Text c="dimmed">Ноды не заявлены — кластер в декларации без развёртывания</Text>
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
