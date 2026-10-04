// Грань «Хранилище бэкапов» (t08, arch/03 §3): карточки Health/Место/Buckets,
// дерево «Кластеры → шарды» с пометками сверки, блок сирот с защитой (t04) и
// кнопками Hold/Unhold/Delete. Данные — снапшот (live-инвентарь MinIO вносится тиком).
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
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
import type { BackupStorageDto, EtcdSnapshotsDto, MinioHealthDto, BackupOrphanDto } from '../api/dto';
import { backupsQueryKeys, fetchBackupsStorage, holdOrphan, unholdOrphan } from '../api/queries';
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
        <Title order={2}>Хранилище бэкапов</Title>
        <Alert color="gray" title="Хранилище бэкапов не настроено">
          {data.notConfiguredReason ?? 'AdminPanel:Backups:S3 не задан'} — грань выключена
        </Alert>
      </Stack>
    );

  return (
    <Stack gap="md">
      <Group gap="sm">
        <Title order={2}>Хранилище бэкапов</Title>
        <Text c="dimmed" size="sm" ff="monospace">
          {data.endpoint} / {data.bucket}
        </Text>
      </Group>
      {data.inventoryError ? (
        <Alert color="yellow" title="Инвентарь устаревает">
          Последний успешный тик инвентаря MinIO не обновлялся:{' '}
          {data.inventoryError}
        </Alert>
      ) : null}
      <Group align="flex-start" gap="md">
        <HealthCard health={data.health} />
        <StorageCard data={data} />
        <EtcdSnapshotsCard etcd={data.etcdSnapshots} />
        <BucketsCard buckets={data.buckets} />
      </Group>
      <ClustersTable data={data} />
      <OrphansCard orphans={data.orphans} />
      <Text c="dimmed" size="sm">
        Инвентарь обновлён:{' '}
        {data.inventoryUpdatedUnix === 0
          ? 'ещё собирается (первый тик)'
          : `${formatUnix(data.inventoryUpdatedUnix)} (${formatUnixAge(data.inventoryUpdatedUnix)} назад)`}
      </Text>
    </Stack>
  );
}

// Health MinIO: api/live/cluster + drives-поля при наличии (spec §4.7).
function HealthCard({ health }: { health: MinioHealthDto | null }) {
  return (
    <Card withBorder padding="md" radius="md" w={340}>
      <Text fw={600} mb="xs">Health MinIO</Text>
      {health === null ? (
        <Text c="dimmed" size="sm">Нет данных (первый тик ещё шёл)</Text>
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
              Диски: здоровых {health.healthyDrives ?? '—'} / offline {health.offlineDrives ?? '—'} / healing {health.healingDrives ?? '—'} / всего {health.totalDrives}
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
        <Text fw={600}>Место</Text>
        {etcd === null ? null : <Badge color={stateColor} variant="light">{etcd.state}</Badge>}
      </Group>
      {etcd === null ? (
        <Text c="dimmed" size="sm">Ключ /pgworker/backups/storage в etcd отсутствует</Text>
      ) : (
        <Stack gap="xs">
          <Progress
            value={percent === null ? 0 : Math.min(100, Math.max(0, percent))}
            color={stateColor}
            animated={etcd.state !== 'OK'}
          />
          <Text size="sm">
            {formatBytes(etcd.usedBytes)}
            {etcd.quotaBytes === null ? '' : ` из ${formatBytes(etcd.quotaBytes)}`}
            {percent === null ? '' : ` (${percent.toFixed(1)}%)`}
          </Text>
        </Stack>
      )}
      <Text size="sm" c="dimmed" mt="xs">
        Live-инвентарь: {data.liveUsedBytes === null ? '—' : formatBytes(data.liveUsedBytes)}
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
        <Text fw={600} mb="xs">etcd-снапшоты</Text>
        <Text c="dimmed" size="sm">Выгрузка не включена (ключа /pgworker/etcd-snapshots нет)</Text>
      </Card>
    );
  }
  const ok = etcd.state === 'OK';
  return (
    <Card withBorder padding="md" radius="md" w={340}>
      <Group justify="space-between" mb="xs">
        <Text fw={600}>etcd-снапшоты</Text>
        <Badge color={ok ? 'green' : 'red'} variant="light">{etcd.state ?? '?'}</Badge>
      </Group>
      <Stack gap="xs">
        <Text size="sm">
          Последняя выгрузка:{' '}
          {etcd.lastUploadedUnix === null
            ? '—'
            : `${formatUnix(etcd.lastUploadedUnix)} (${formatUnixAge(etcd.lastUploadedUnix)} назад)`}
        </Text>
        <Text size="sm" c="dimmed" ff="monospace">
          sha256: {etcd.lastSha256 === null ? '—' : etcd.lastSha256.slice(0, 12)}
        </Text>
        <Text size="sm" c="dimmed">
          размер: {etcd.sizeBytes === null ? '—' : formatBytes(etcd.sizeBytes)}
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
        <Text c="dimmed" size="sm">Нет данных</Text>
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
function ClustersTable({ data }: { data: BackupStorageDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Кластеры → шарды</Text>
      {data.clusters.length === 0 ? (
        <Text c="dimmed" size="sm">Дерево пусто (инвентарь ещё не собран или bucket пуст)</Text>
      ) : (
        <Table.ScrollContainer minWidth={760}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Кластер</Table.Th>
                <Table.Th>Шард</Table.Th>
                <Table.Th>Размер</Table.Th>
                <Table.Th>Полные</Table.Th>
                <Table.Th>WAL-сегменты</Table.Th>
                <Table.Th>Сверка</Table.Th>
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
                            <Tooltip label="есть объекты полного без etcd-ключа">
                              <Badge color="yellow" variant="light">объекты без ключа</Badge>
                            </Tooltip>
                          ) : null}
                          {s.orphan ? (
                            <Tooltip label="префикс без владельца в /clusters/">
                              <Badge color="red" variant="light">сирота</Badge>
                            </Tooltip>
                          ) : null}
                          {!s.hasS3Only && !s.orphan ? <Text c="dimmed" size="sm">—</Text> : null}
                        </Group>
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

// Осиротевшие префиксы: реестр воркера (OBSERVED/DELETING + TTL у незащищённых)
// или «панель видит, в реестре нет»; бейджи защиты (полный/hold/к удалению) и
// кнопки Hold/Unhold (обратимы, без модала) / Delete (confirm-модал) — t04.
// Компактный индикатор интервала дрилов кластера (reliability t02):
// «N сут» / «выкл» (0) / нет бейджа (policy-ключа нет — дефолт конфига).
function DrillIntervalBadge({ intervalDays }: { intervalDays: number | null }) {
  if (intervalDays === null) return null;
  return (
    <Tooltip label="интервал дрилов восстановимости (policy)">
      <Badge color={intervalDays === 0 ? 'gray' : 'blue'} variant="light">
        дрилл: {intervalDays === 0 ? 'выкл' : `${intervalDays} сут`}
      </Badge>
    </Tooltip>
  );
}

function OrphansCard({ orphans }: { orphans: BackupOrphanDto[] }) {
  const [deleting, setDeleting] = useState<BackupOrphanDto | null>(null);
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Осиротевшие префиксы</Text>
      {orphans.length === 0 ? (
        <Text c="teal" size="sm">Сирот не обнаружено</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={860}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Префикс</Table.Th>
                  <Table.Th>Тип</Table.Th>
                  <Table.Th>Размер</Table.Th>
                  <Table.Th>Реестр воркера</Table.Th>
                  <Table.Th>Защита</Table.Th>
                  <Table.Th>Действия</Table.Th>
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
        <Tooltip label="в префиксе есть валидный полный — TTL-автоматика не удаляет">
          <Badge color="teal" variant="light">полный: защита</Badge>
        </Tooltip>
      ) : null}
      {orphan.held ? (
        <Tooltip label="hold-флаг оператора — TTL-отбор исключён">
          <Badge color="indigo" variant="light">
            hold{orphan.heldBy !== null ? ` (${orphan.heldBy})` : ''}
          </Badge>
        </Tooltip>
      ) : null}
      {orphan.deleteRequested ? (
        <Tooltip label="заявка явного удаления — исполнит sweeper ближайшим проходом">
          <Badge color="red" variant="light">к удалению</Badge>
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
              замечен {formatUnix(orphan.firstSeenUnix)}
              {orphan.ttlLeftSec === null
                ? ''
                : `, удаление по TTL через ${formatTtlLeft(orphan.ttlLeftSec)}`}
            </Text>
          </Group>
        ) : (
          <Text size="sm" c="yellow">панель видит, в реестре нет</Text>
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
  if (sec < 60) return `${sec} с`;
  const minutes = Math.floor(sec / 60);
  if (minutes < 60) return `${minutes} мин`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours} ч`;
  const days = Math.floor(hours / 24);
  return `${days} д ${hours % 24} ч`;
}
