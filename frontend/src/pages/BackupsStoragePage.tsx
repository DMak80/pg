// Грань «Хранилище бэкапов» (t08, arch/03 §3): карточки Health/Место/Buckets,
// дерево «Кластеры → шарды» с пометками сверки, блок сирот; read-only, без
// форм ввода. Данные — снапшот (live-инвентарь MinIO вносится тиком).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Badge,
  Card,
  Group,
  Progress,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { Link } from 'react-router';
import type { BackupStorageDto, MinioHealthDto, BackupOrphanDto } from '../api/dto';
import { backupsQueryKeys, fetchBackupsStorage } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatUnix, formatUnixAge } from '../utils/format';

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
                    <Table.Td ff="monospace">{c.cluster}</Table.Td>
                    <Table.Td colSpan={5}><Text c="dimmed" size="sm">—</Text></Table.Td>
                  </Table.Tr>
                ) : (
                  c.shards.map((s) => (
                    <Table.Tr key={`${s.cluster}/${s.shard}`}>
                      <Table.Td ff="monospace">{s.cluster}</Table.Td>
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

// Осиротевшие префиксы: реестр воркера (OBSERVED/DELETING + TTL) или
// «панель видит, в реестре нет» (AC5; удаление — супервизор воркера).
function OrphansCard({ orphans }: { orphans: BackupOrphanDto[] }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Осиротевшие префиксы</Text>
      {orphans.length === 0 ? (
        <Text c="teal" size="sm">Сирот не обнаружено</Text>
      ) : (
        <Table.ScrollContainer minWidth={760}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Префикс</Table.Th>
                <Table.Th>Тип</Table.Th>
                <Table.Th>Размер</Table.Th>
                <Table.Th>Реестр воркера</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {orphans.map((o) => (
                <Table.Tr key={o.prefix}>
                  <Table.Td ff="monospace">{o.prefix}</Table.Td>
                  <Table.Td>{o.kind}</Table.Td>
                  <Table.Td>{formatBytes(o.sizeBytes)}</Table.Td>
                  <Table.Td>
                    {o.inWorkerRegistry ? (
                      <Group gap="xs">
                        <Badge color={o.registryState === 'DELETING' ? 'orange' : 'blue'} variant="light">
                          {o.registryState ?? '—'}
                        </Badge>
                        <Text size="sm" c="dimmed">
                          замечен {formatUnix(o.firstSeenUnix)}
                          {o.ttlLeftSec === null
                            ? ''
                            : `, удаление по TTL через ${formatTtlLeft(o.ttlLeftSec)}`}
                        </Text>
                      </Group>
                    ) : (
                      <Text size="sm" c="yellow">панель видит, в реестре нет</Text>
                    )}
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
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
