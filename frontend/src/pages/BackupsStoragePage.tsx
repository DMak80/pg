// Грань «Хранилище бэкапов» (t08, arch/03 §3): карточки Health/Место/Buckets,
// дерево «Кластеры → шарды» с пометками сверки, блок сирот с защитой (t04) и
// кнопками Hold/Unhold/Delete. Данные — снапшот (live-инвентарь S3-хранилища вносится тиком).
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { BackupsPoolModal } from './backups-storage/BackupsPoolModal';
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  Progress,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { useState } from 'react';
import { Link } from 'react-router';
import type { BackupStorageDto, EtcdSnapshotsDto, S3HealthDto, BackupOrphanDto } from '../api/dto';
import { backupsQueryKeys, fetchBackupsStorage, fetchS3Pools, holdOrphan, unholdOrphan } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatUnix, formatUnixAge } from '../utils/format';
import { DeleteOrphanModal } from './backups-storage/DeleteOrphanModal';

export function BackupsStoragePage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: backupsQueryKeys.storage,
    queryFn: fetchBackupsStorage,
    refetchInterval: intervalMs,
  });
  // t36 (arch/adminpanel/03 §3): бейджи пула + модал «Пул бэкапов».
  const [poolModalCluster, setPoolModalCluster] = useState<string | null>(null);
  const poolsQuery = useQuery({
    queryKey: ['backups', 's3-pools'],
    queryFn: fetchS3Pools,
    refetchInterval: intervalMs,
  });

  // Паттерн состояний (t08 spec §4.15): как EtcdPage.
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  // AC1: грань не настроена — только заглушка, больше контента нет.
  if (!data.configured)
    return (
      <Stack gap="md">
        <Title order={2}>Backup Storage</Title>
        <Alert color="gray" title="Backup storage is not configured">
          {data.notConfiguredReason ?? 'AdminPanel:Backups:S3 is not set'} — the edge is disabled
        </Alert>
      </Stack>
    );

  return (
    <Stack gap="md">
      <Group gap="sm">
        <Title order={2}>Backup Storage</Title>
        <Text c="dimmed" size="sm" ff="monospace">
          {data.endpoints.join(', ')} / {data.bucket}
        </Text>
      </Group>
      {data.inventoryError ? (
        <Alert color="yellow" title="Inventory lagging">
          The last successful S3 storage inventory tick has not been updated:{' '}
          {data.inventoryError}
        </Alert>
      ) : null}
      <Group align="flex-start" gap="md">
        <HealthCard health={data.health} />
        <StorageCard data={data} />
        <EtcdSnapshotsCard etcd={data.etcdSnapshots} />
        <BucketsCard buckets={data.buckets} />
      </Group>
      <ClustersTable data={data} onOpenPool={(cluster) => setPoolModalCluster(cluster)} />
      {poolModalCluster ? (
        <BackupsPoolModal
          cluster={poolModalCluster}
          policy={data.clusters.find((c) => c.cluster === poolModalCluster)?.policy ?? null}
          pools={poolsQuery.data ?? []}
          onClose={() => setPoolModalCluster(null)}
          onSaved={() => void query.refetch()}
        />
      ) : null}
      <OrphansCard orphans={data.orphans} />
      <Text c="dimmed" size="sm">
        Inventory updated:{' '}
        {data.inventoryUpdatedUnix === 0
          ? 'still collecting (first tick)'
          : `${formatUnix(data.inventoryUpdatedUnix)} (${formatUnixAge(data.inventoryUpdatedUnix)} ago)`}
      </Text>
    </Stack>
  );
}

// Health S3: api/live/cluster + drives-поля при наличии (spec §4.7).
function HealthCard({ health }: { health: S3HealthDto | null }) {
  return (
    <Card withBorder padding="md" radius="md" w={340}>
      <Text fw={600} mb="xs">Health S3</Text>
      {health === null ? (
        <Text c="dimmed" size="sm">No data (first tick was still running)</Text>
      ) : (
        <Stack gap="xs">
          <Group gap="xs">
            <Badge color={health.apiOk ? 'teal' : 'red'} variant="light">
              api {health.apiOk ? 'ok' : 'down'}
            </Badge>
            <Badge color={health.liveOk ? 'teal' : 'red'} variant="light">
              live {health.liveOk ? 'ok' : 'down'}
            </Badge>
            <Badge color={health.clusterOk === null ? 'gray' : health.clusterOk ? 'teal' : 'red'} variant="light">
              cluster {health.clusterOk === null ? 'n/a' : health.clusterOk ? 'ok' : 'degraded'}
            </Badge>
          </Group>
          {health.apiError ? (
            <Tooltip multiline label={health.apiError}>
              <Text size="sm" c="red" lineClamp={1}>{health.apiError}</Text>
            </Tooltip>
          ) : null}
          {health.totalDrives !== null ? (
            <Text size="sm" c="dimmed">
              Drives: healthy {health.healthyDrives ?? '—'} / offline {health.offlineDrives ?? '—'} / healing {health.healingDrives ?? '—'} / total {health.totalDrives}
            </Text>
          ) : null}
        </Stack>
      )}
    </Card>
  );
}

// Место: etcd-ключ storage (вердикт воркера) + live-факт инвентаря (AC6).
function StorageCard({ data }: { data: BackupStorageDto }) {
  const etcd = data.etcd;
  // state-бейдж воркера: OK/WARN/CRIT → green/yellow/red (spec §4.7).
  const stateColor = etcd === null ? 'gray' : etcd.state === 'OK' ? 'green' : etcd.state === 'WARN' ? 'yellow' : 'red';
  const percent: number | null = etcd?.usedPercent ?? null;
  return (
    <Card withBorder padding="md" radius="md" w={380}>
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Space</Text>
        {etcd === null ? null : <Badge color={stateColor} variant="light">{etcd.state}</Badge>}
      </Group>
      {etcd === null ? (
        <Text c="dimmed" size="sm">Key /pgworker/backups/storage is missing in etcd</Text>
      ) : (
        <Stack gap="xs">
          <Progress
            value={percent === null ? 0 : Math.min(100, Math.max(0, percent))}
            color={stateColor}
            animated={etcd.state !== 'OK'}
          />
          <Text size="sm">
            {formatBytes(etcd.usedBytes)}
            {etcd.quotaBytes === null ? '' : ` of ${formatBytes(etcd.quotaBytes)}`}
            {percent === null ? '' : ` (${percent.toFixed(1)}%)`}
          </Text>
        </Stack>
      )}
      <Text size="sm" c="dimmed" mt="xs">
        Live inventory: {data.liveUsedBytes === null ? '—' : formatBytes(data.liveUsedBytes)}
      </Text>
    </Card>
  );
}

// Карточка «etcd-снапшоты» (t08): последняя выгрузка слепка etcd в S3 —
// статус OK/FAILED, время/возраст, sha256-префикс, размер, ошибка; read-only.
function EtcdSnapshotsCard({ etcd }: { etcd: EtcdSnapshotsDto | null }) {
  if (etcd === null) {
    return (
      <Card withBorder padding="md" radius="md" w={340}>
        <Text fw={600} mb="xs">etcd snapshots</Text>
        <Text c="dimmed" size="sm">Upload not enabled (/pgworker/etcd-snapshots key missing)</Text>
      </Card>
    );
  }
  const ok = etcd.state === 'OK';
  return (
    <Card withBorder padding="md" radius="md" w={340}>
      <Group justify="space-between" mb="xs">
        <Text fw={600}>etcd snapshots</Text>
        <Badge color={ok ? 'green' : 'red'} variant="light">{etcd.state ?? '?'}</Badge>
      </Group>
      <Stack gap="xs">
        <Text size="sm">
          Last upload:{' '}
          {etcd.lastUploadedUnix === null
            ? '—'
            : `${formatUnix(etcd.lastUploadedUnix)} (${formatUnixAge(etcd.lastUploadedUnix)} ago)`}
        </Text>
        <Text size="sm" c="dimmed" ff="monospace">
          sha256: {etcd.lastSha256 === null ? '—' : etcd.lastSha256.slice(0, 12)}
        </Text>
        <Text size="sm" c="dimmed">
          size: {etcd.sizeBytes === null ? '—' : formatBytes(etcd.sizeBytes)}
        </Text>
        {!ok && etcd.error !== null ? <Text size="sm" c="red">{etcd.error}</Text> : null}
      </Stack>
    </Card>
  );
}

// Бакеты установки из live-инвентаря (AC2).
function BucketsCard({ buckets }: { buckets: string[] }) {
  return (
    <Card withBorder padding="md" radius="md" w={280}>
      <Text fw={600} mb="xs">Buckets</Text>
      {buckets.length === 0 ? (
        <Text c="dimmed" size="sm">No data</Text>
      ) : (
        <Stack gap={4}>
          {buckets.map((b) => (
            <Text key={b} ff="monospace" size="sm">{b}</Text>
          ))}
        </Stack>
      )}
    </Card>
  );
}

// Дерево «Кластеры → шарды»: размер, полные шт., WAL-сегменты шт., пометки
// сверки (объекты без ключа / сирота); клик по шарду → детали (spec §4.7).
function ClustersTable({ data, onOpenPool }: {
  data: BackupStorageDto;
  onOpenPool: (cluster: string) => void;
}) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Clusters → shards</Text>
      {data.clusters.length === 0 ? (
        <Text c="dimmed" size="sm">Tree is empty (inventory not collected yet or bucket is empty)</Text>
      ) : (
        <Table.ScrollContainer minWidth={760}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Cluster</Table.Th>
                <Table.Th>Shard</Table.Th>
                <Table.Th>Size</Table.Th>
                <Table.Th>Fulls</Table.Th>
                <Table.Th>WAL segments</Table.Th>
                <Table.Th>Reconcile</Table.Th>
                <Table.Th>Pool</Table.Th>
                <Table.Th>Actions</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.clusters.map((c) =>
                c.shards.length === 0 ? (
                  // Кластер без шардов в инвентаре — одна строка-заглушка.
                  <Table.Tr key={c.cluster}>
                    <Table.Td ff="monospace">
                      <Group gap="xs">
                        {c.cluster}
                        <DrillIntervalBadge intervalDays={c.policy?.drillIntervalDays ?? null} />
                      </Group>
                    </Table.Td>
                    <Table.Td colSpan={5}><Text c="dimmed" size="sm">—</Text></Table.Td>
                    <Table.Td><PoolBadge poolName={c.policy?.s3?.pool ?? null} /></Table.Td>
                    <Table.Td>
                      <Button size="compact-sm" variant="light" onClick={() => onOpenPool(c.cluster)}>
                        Backup pool
                      </Button>
                    </Table.Td>
                  </Table.Tr>
                ) : (
                  c.shards.map((s) => (
                    <Table.Tr key={`${s.cluster}/${s.shard}`}>
                      <Table.Td ff="monospace">
                        <Group gap="xs">
                          {s.cluster}
                          <DrillIntervalBadge intervalDays={c.policy?.drillIntervalDays ?? null} />
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <Text
                          component={Link}
                          to={`/backups-storage/${encodeURIComponent(s.cluster)}/${encodeURIComponent(s.shard)}`}
                          size="sm"
                          ff="monospace"
                        >
                          {s.shard}
                        </Text>
                      </Table.Td>
                      <Table.Td>{formatBytes(s.sizeBytes)}</Table.Td>
                      <Table.Td>{s.fullsCount}</Table.Td>
                      <Table.Td>{s.walSegmentCount}</Table.Td>
                      <Table.Td>
                        <Group gap="xs">
                          {s.hasS3Only ? (
                            <Tooltip label="full objects without an etcd key exist">
                              <Badge color="yellow" variant="light">objects without key</Badge>
                            </Tooltip>
                          ) : null}
                          {s.orphan ? (
                            <Tooltip label="prefix without an owner in /clusters/">
                              <Badge color="red" variant="light">orphan</Badge>
                            </Tooltip>
                          ) : null}
                          {!s.hasS3Only && !s.orphan ? <Text c="dimmed" size="sm">—</Text> : null}
                        </Group>
                      </Table.Td>
                      <Table.Td>{s.shard === c.shards[0]?.shard ? <PoolBadge poolName={c.policy?.s3?.pool ?? null} /> : null}</Table.Td>
                      <Table.Td>
                        {s.shard === c.shards[0]?.shard ? (
                          <Button size="compact-sm" variant="light" onClick={() => onOpenPool(s.cluster)}>
                            Backup pool
                          </Button>
                        ) : null}
                      </Table.Td>
                    </Table.Tr>
                  ))
                ),
              )}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

// t36 (arch/adminpanel/03 §3): бейдж пула кластера либо красный «пул не выбран».
function PoolBadge({ poolName }: { poolName: string | null }) {
  return poolName ? (
    <Tooltip label={`policy.s3.pool = ${poolName}`}>
      <Badge color="blue" variant="light" ff="monospace">{poolName}</Badge>
    </Tooltip>
  ) : (
    <Tooltip label="policy without an s3 section — cluster backups are not written (backup-pool-missing)">
      <Badge color="red" variant="light">no pool selected</Badge>
    </Tooltip>
  );
}

// Осиротевшие префиксы: реестр воркера (OBSERVED/DELETING + TTL у незащищённых)
// или «панель видит, в реестре нет»; бейджи защиты (полный/hold/к удалению) и
// кнопки Hold/Unhold (обратимы, без модала) / Delete (confirm-модал) — t04.
// Компактный индикатор интервала дрилов кластера (reliability t02):
// «N сут» / «выкл» (0) / нет бейджа (policy-ключа нет — дефолт конфига).
function DrillIntervalBadge({ intervalDays }: { intervalDays: number | null }) {
  if (intervalDays === null) return null;
  return (
    <Tooltip label="recovery drill interval (policy)">
      <Badge color={intervalDays === 0 ? 'gray' : 'blue'} variant="light">
        drill: {intervalDays === 0 ? 'off' : `${intervalDays} d`}
      </Badge>
    </Tooltip>
  );
}

function OrphansCard({ orphans }: { orphans: BackupOrphanDto[] }) {
  const [deleting, setDeleting] = useState<BackupOrphanDto | null>(null);
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Orphaned prefixes</Text>
      {orphans.length === 0 ? (
        <Text c="teal" size="sm">No orphans found</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={860}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Prefix</Table.Th>
                  <Table.Th>Type</Table.Th>
                  <Table.Th>Size</Table.Th>
                  <Table.Th>Worker registry</Table.Th>
                  <Table.Th>Protection</Table.Th>
                  <Table.Th>Actions</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {orphans.map((o) => (
                  <OrphanRow key={o.prefix} orphan={o} onDelete={() => setDeleting(o)} />
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          {deleting !== null ? (
            <DeleteOrphanModal
              prefix={deleting.prefix}
              cluster={deleting.prefix.split('/')[0]}
              shard={deleting.prefix.split('/')[1] ?? ''}
              onClose={() => setDeleting(null)}
              onDone={() => setDeleting(null)}
            />
          ) : null}
        </>
      )}
    </Card>
  );
}

// Бейджи защиты строки сироты (t04): автозащита полным / hold / к удалению.
function ProtectionBadges({ orphan }: { orphan: BackupOrphanDto }) {
  if (!orphan.hasValidFull && !orphan.held && !orphan.deleteRequested)
    return <Text c="dimmed" size="sm">—</Text>;
  return (
    <Group gap="xs">
      {orphan.hasValidFull ? (
        <Tooltip label="a valid full exists in the prefix — TTL automation will not delete it">
          <Badge color="teal" variant="light">full: protected</Badge>
        </Tooltip>
      ) : null}
      {orphan.held ? (
        <Tooltip label="operator hold flag — excluded from TTL selection">
          <Badge color="indigo" variant="light">
            hold{orphan.heldBy !== null ? ` (${orphan.heldBy})` : ''}
          </Badge>
        </Tooltip>
      ) : null}
      {orphan.deleteRequested ? (
        <Tooltip label="explicit delete ticket — sweeper will execute it on the next pass">
          <Badge color="red" variant="light">to remove</Badge>
        </Tooltip>
      ) : null}
    </Group>
  );
}

// Кнопки Hold/Unhold/Delete строки (t04): hold/unhold — без модала (спиннер
// до 204, после — refetch хранилища); Delete — confirm-модал, только для
// строк реестра; в DELETING кнопки скрыты (доводку не остановить).
function OrphanRow({ orphan, onDelete }: { orphan: BackupOrphanDto; onDelete: () => void }) {
  const queryClient = useQueryClient();
  const refresh = () => void queryClient.invalidateQueries({ queryKey: backupsQueryKeys.storage });
  const hold = useMutation({ mutationFn: () => holdOrphan(...orphanPrefix(orphan)), onSuccess: refresh });
  const unhold = useMutation({ mutationFn: () => unholdOrphan(...orphanPrefix(orphan)), onSuccess: refresh });
  const pending = hold.isPending || unhold.isPending;
  const deleting = orphan.registryState === 'DELETING';

  return (
    <Table.Tr key={orphan.prefix}>
      <Table.Td ff="monospace">{orphan.prefix}</Table.Td>
      <Table.Td>{orphan.kind}</Table.Td>
      <Table.Td>{formatBytes(orphan.sizeBytes)}</Table.Td>
      <Table.Td>
        {orphan.inWorkerRegistry ? (
          <Group gap="xs">
            <Badge color={orphan.registryState === 'DELETING' ? 'orange' : 'blue'} variant="light">
              {orphan.registryState ?? '—'}
            </Badge>
            <Text size="sm" c="dimmed">
              first seen {formatUnix(orphan.firstSeenUnix)}
              {orphan.ttlLeftSec === null
                ? ''
                : `, TTL deletion in ${formatTtlLeft(orphan.ttlLeftSec)}`}
            </Text>
          </Group>
        ) : (
          <Text size="sm" c="yellow">seen by the panel, not in registry</Text>
        )}
      </Table.Td>
      <Table.Td><ProtectionBadges orphan={orphan} /></Table.Td>
      <Table.Td>
        {orphan.inWorkerRegistry && !deleting ? (
          <Group gap="xs">
            {orphan.held ? (
              <Button size="xs" variant="default" loading={unhold.isPending} disabled={pending}
                onClick={() => unhold.mutate()}>
                Unhold
              </Button>
            ) : (
              <Button size="xs" variant="default" loading={hold.isPending} disabled={pending}
                onClick={() => hold.mutate()}>
                Hold
              </Button>
            )}
            <Button size="xs" color="red" variant="light" onClick={onDelete}>
              Delete
            </Button>
          </Group>
        ) : (
          <Text c="dimmed" size="sm">—</Text>
        )}
      </Table.Td>
    </Table.Tr>
  );
}

// «<C>/<X>» → пара (cluster, shard) для мутаций (префикс канона).
function orphanPrefix(orphan: BackupOrphanDto): [string, string] {
  const [cluster, shard] = orphan.prefix.split('/');
  return [cluster, shard ?? ''];
}

// Остаток TTL в человекочитаемом виде: «2 д 3 ч», «45 мин».
function formatTtlLeft(sec: number): string {
  if (sec < 60) return `${sec} s`;
  const minutes = Math.floor(sec / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours} h`;
  const days = Math.floor(hours / 24);
  return `${days} d ${hours % 24} h`;
}
