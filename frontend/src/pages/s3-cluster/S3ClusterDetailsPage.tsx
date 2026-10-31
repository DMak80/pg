// Детали s3-кластера (u01/u13): единый шаблон DomainPageLayout — вкладки
// Ноды|Репликация|Связность и синхронность|Журнал; шапка — state-бейджи
// (серым), etcd-признак, endpoints, живая ротация; canMutate = ACTIVE.
import { useQuery } from '@tanstack/react-query';
import { Badge, Card, Group, SimpleGrid, Stack, Text, Tooltip } from '@mantine/core';
import { useParams } from 'react-router';
import { fetchS3ClusterDetails, s3QueryKeys } from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { DomainPageLayout, Field } from '../../components/DomainPageLayout';
import { WorkJournalBlock } from '../../components/WorkJournalBlock';
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
      <DomainPageLayout
        title={`S3: ${c.name}`}
        statusBadges={
          <>
            {/* Штатная тишина серым (arch/03 §12.3) */}
            {c.state === 'TO_REMOVE' ? (
              <Tooltip label="the worker tears down instances and etcd keys">
                <Badge color="gray" variant="light">TO_REMOVE</Badge>
              </Tooltip>
            ) : c.state === 'NOT_INITIALIZED' ? (
              <Badge color="gray" variant="light">NOT_INITIALIZED</Badge>
            ) : null}
          </>
        }
        modeBadges={
          <>
            {/* u10-стиль бейджа etcdBackup (режим кластера) */}
            {c.etcdBackup ? (
              <Tooltip label="backups key of this cluster is external (etcd backup special key)">
                <Badge color="violet" variant="light">etcd-backup</Badge>
              </Tooltip>
            ) : null}
            {c.rotation !== null ? (
              <Tooltip label="secrets rotation ticket is live: the worker re-issues the cluster credentials via rolling recreation">
                <Badge color="blue" variant="light">ротация… · {c.rotation.requestedBy ?? '—'}</Badge>
              </Tooltip>
            ) : null}
          </>
        }
        endpoints={c.endpoints}
        actions={
          canMutate ? (
            <Group gap="sm">
              <SyncProbeButton cluster={c.name} />
              <RotateSecretsButton cluster={c.name} disabled={c.rotation !== null} />
              <DeleteS3ClusterButton cluster={c.name} />
            </Group>
          ) : null
        }
        infoFields={[
          { label: 'Platform', value: c.platform },
          { label: 'Etcd backup', value: c.etcdBackup ? 'yes' : 'no' },
          {
            label: 'Created',
            value: c.createdUnix !== null ? new Date(c.createdUnix * 1000).toLocaleString() : '—',
          },
        ]}
        tabs={[
          {
            value: 'nodes',
            label: 'Ноды',
            content:
              c.nodesList.length === 0 && c.state === 'NOT_INITIALIZED' ? (
                <Card withBorder padding="md" radius="md">
                  <Text c="dimmed">No nodes declared — the cluster is a declaration without deployment</Text>
                </Card>
              ) : (
                <Card withBorder padding="md" radius="md">
                  <NodesTab cluster={c} canMutate={canMutate} />
                </Card>
              ),
          },
          {
            value: 'replication',
            label: 'Репликация',
            content: (
              <Card withBorder padding="md" radius="md">
                <ReplicationTab cluster={c} canMutate={canMutate} />
              </Card>
            ),
          },
          {
            value: 'probes',
            label: 'Связность и синхронность',
            content: (
              <Card withBorder padding="md" radius="md">
                <ConnectivitySyncTab cluster={c} />
              </Card>
            ),
          },
          {
            value: 'journal',
            label: 'Журнал',
            // u13: work-журнал /s3worker/work/<C>
            content: (
              <Stack gap="md">
                <WorkJournalBlock work={c.work} />
                {/* Креды клиентской грани (arch/23 §2): состав без значений */}
                <Card withBorder padding="md" radius="md">
                  <Group justify="space-between" mb="xs">
                    <Text size="sm" fw={500}>Креды клиентской грани</Text>
                    {c.etcdBackup ? (
                      <Tooltip label="the backups key of this cluster is external: the etcd backup special key (per-install env pair)">
                        <Badge color="violet" variant="light">backups key is external</Badge>
                      </Tooltip>
                    ) : null}
                  </Group>
                  {c.rootUser === null ? (
                    <Text c="dimmed">Не провижинены</Text>
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
              </Stack>
            ),
          },
        ]}
      />
    </Stack>
  );
}
