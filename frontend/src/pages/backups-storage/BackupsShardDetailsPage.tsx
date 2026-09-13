// Детали шарда «Хранилище бэкапов» (t08, arch/03 §3): таблица полных
// (id/размер/дата + etcd state/verify + статус сверки), блок WAL (etcd-статус
// + S3-факт), бейдж активного restore, блок «Объекты» с on-demand пагинацией
// («Загрузить ещё» — единственный прямой выход панели в MinIO на действие).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Card,
  Group,
  Loader,
  Stack,
  Table,
  Text,
  Title,
} from '@mantine/core';
import { Link, useParams } from 'react-router';
import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  BackupFullDto,
  BackupObjectsPageDto,
  BackupReconcileStatusName,
  BackupWalDto,
} from '../../api/dto';
import {
  backupsQueryKeys,
  fetchBackupObjectsPage,
  fetchBackupShardStorage,
} from '../../api/queries';
import { ApiError } from '../../api/client';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { formatBytes, formatUnix } from '../../utils/format';

// Цвета статусов сверки per-full (план Task 12 Step 3).
const RECONCILE_COLORS: Record<BackupReconcileStatusName, string> = {
  Ok: 'green',
  S3Only: 'yellow',
  EtcdOnly: 'red',
  InProgress: 'blue',
  Deleting: 'gray',
};

const RECONCILE_LABELS: Record<BackupReconcileStatusName, string> = {
  Ok: 'ok',
  S3Only: 'объекты без ключа',
  EtcdOnly: 'ключ без объектов',
  InProgress: 'идёт',
  Deleting: 'удаляется',
};

// Размер страницы on-demand list («Загрузить ещё»); потолок API — 1000.
const OBJECTS_PAGE_MAX_KEYS = 200;

export function BackupsShardDetailsPage() {
  const { cluster = '', shard = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: backupsQueryKeys.shard(cluster, shard),
    queryFn: () => fetchBackupShardStorage(cluster, shard),
    refetchInterval: intervalMs,
  });

  // 404 (шард исчез) → notFound-контент; 503/сеть → ErrorSection (как HA-детали).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection
        error={query.error}
        onRetry={() => void query.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Шард {cluster}/{shard} отсутствует и в etcd, и в S3-дереве инвентаря</Text>
            <Anchor component={Link} to="/backups-storage" size="sm">← Хранилище бэкапов</Anchor>
          </Stack>
        }
      />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  return (
    <Stack gap="md">
      <div>
        <Anchor component={Link} to="/backups-storage" size="sm">← Хранилище бэкапов</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2} ff="monospace">{data.cluster}/{data.shard}</Title>
          {/* Бейдж активного restore: только статус, кнопок нет (arch/19 §3.5). */}
          {data.activeRestore === null ? null : (
            <Group gap="xs">
              <Badge color="orange" variant="light">
                Restore: {data.activeRestore.state}{data.activeRestore.phase ? ` ${data.activeRestore.phase}` : ''}
              </Badge>
              {data.activeRestore.error ? (
                <Text size="sm" c="red">{data.activeRestore.error}</Text>
              ) : null}
            </Group>
          )}
        </Group>
        {data.reconcileNote ? (
          <Text size="sm" c="yellow" mt={4}>Сверка: {data.reconcileNote}</Text>
        ) : null}
      </div>
      <FullsTable fulls={data.fulls} />
      <WalCard wal={data.wal} />
      <ObjectsBlock cluster={data.cluster} shard={data.shard} />
    </Stack>
  );
}

// Таблица полных: S3-факт (id/размер/объекты/дата) + etcd-факт (state/verify)
// + статус сверки (AC4/AC9 — джойн глазами человека).
function FullsTable({ fulls }: { fulls: BackupFullDto[] }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Полные бэкапы</Text>
      {fulls.length === 0 ? (
        <Text c="dimmed" size="sm">Полных нет ни в S3, ни в etcd</Text>
      ) : (
        <Table.ScrollContainer minWidth={900}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>ID</Table.Th>
                <Table.Th>Размер (S3)</Table.Th>
                <Table.Th>Объектов</Table.Th>
                <Table.Th>Дата (S3)</Table.Th>
                <Table.Th>etcd state</Table.Th>
                <Table.Th>Verify</Table.Th>
                <Table.Th>etcd размер</Table.Th>
                <Table.Th>Сверка</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {fulls.map((f) => (
                <Table.Tr key={f.id}>
                  <Table.Td ff="monospace">{f.id}</Table.Td>
                  <Table.Td>{formatBytes(f.sizeBytes)}</Table.Td>
                  <Table.Td>{f.objectCount ?? '—'}</Table.Td>
                  <Table.Td>{formatUnix(f.lastModifiedUnix)}</Table.Td>
                  <Table.Td>
                    {f.etcdState === null ? (
                      <Text c="dimmed" size="sm">—</Text>
                    ) : (
                      <Badge
                        color={f.etcdState === 'COMPLETED' ? 'teal' : f.etcdState === 'FAILED' ? 'red' : 'blue'}
                        variant="light"
                      >
                        {f.etcdState}
                      </Badge>
                    )}
                  </Table.Td>
                  <Table.Td>
                    {f.verifyState === null ? (
                      <Text c="dimmed" size="sm">—</Text>
                    ) : (
                      <Badge color={f.verifyState === 'OK' ? 'teal' : 'red'} variant="light">{f.verifyState}</Badge>
                    )}
                  </Table.Td>
                  <Table.Td>{formatBytes(f.etcdSizeBytes)}</Table.Td>
                  <Table.Td>
                    <Badge color={RECONCILE_COLORS[f.reconcile] ?? 'gray'} variant="light">
                      {RECONCILE_LABELS[f.reconcile] ?? f.reconcile}
                    </Badge>
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

// Блок WAL: etcd-статус цепочки + S3-факт — только факты, без вердикта (spec §4.4).
function WalCard({ wal }: { wal: BackupWalDto | null }) {
  if (wal === null)
    return (
      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">WAL</Text>
        <Text c="dimmed" size="sm">WAL-данных нет ни в etcd, ни в S3</Text>
      </Card>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">WAL</Text>
      <Table w="auto">
        <Table.Tbody>
          <Table.Tr>
            <Table.Td c="dimmed" w={220}>etcd-статус цепочки</Table.Td>
            <Table.Td>
              <Group gap="xs">
                {wal.etcdState === null ? (
                  <Text size="sm">—</Text>
                ) : (
                  <Badge
                    color={wal.etcdState === 'ACTIVE' ? 'teal' : wal.etcdState === 'BROKEN' ? 'red' : 'blue'}
                    variant="light"
                  >
                    {wal.etcdState}
                  </Badge>
                )}
                <Text size="sm">
                  последний сегмент: <Text span ff="monospace">{wal.etcdLastSegment ?? '—'}</Text>
                  {wal.etcdLastUnix === null ? '' : ` (${formatUnix(wal.etcdLastUnix)})`}
                </Text>
              </Group>
            </Table.Td>
          </Table.Tr>
          <Table.Tr>
            <Table.Td c="dimmed">S3-факт</Table.Td>
            <Table.Td>
              <Text size="sm">
                сегментов {wal.s3SegmentCount}, историй {wal.s3HistoryCount}, размер {formatBytes(wal.s3SizeBytes)}
                {wal.s3LastModifiedUnix === 0 ? '' : `, обновлён ${formatUnix(wal.s3LastModifiedUnix)}`}
              </Text>
              {wal.s3LastObject === null ? null : (
                <Text size="sm" ff="monospace" c="dimmed">{wal.s3LastObject}</Text>
              )}
            </Table.Td>
          </Table.Tr>
        </Table.Tbody>
      </Table>
    </Card>
  );
}

// Блок «Объекты»: on-demand пагинация — polling НЕ трогает (spec §4.7):
// первая страница грузится один раз при монтировании, следующие — по кнопке
// «Загрузить ещё»; стек страниц в useState, токен продолжения — из последней.
function ObjectsBlock({ cluster, shard }: { cluster: string; shard: string }) {
  const prefix = `${cluster}/${shard}/`;
  const [pages, setPages] = useState<BackupObjectsPageDto[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const startedRef = useRef(false);

  const loadMore = useCallback(async () => {
    const token = pages.length === 0 ? null : pages[pages.length - 1].nextContinuationToken;
    setLoading(true);
    setError(null);
    try {
      const page = await fetchBackupObjectsPage(prefix, OBJECTS_PAGE_MAX_KEYS, token);
      setPages((prev) => [...prev, page]);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Не удалось загрузить объекты');
    } finally {
      setLoading(false);
    }
  }, [pages, prefix]);

  // Автозагрузка первой страницы — один раз при монтировании (on-demand).
  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    void loadMore();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const items = pages.flatMap((p) => p.items);
  const nextToken = pages.length === 0 ? null : pages[pages.length - 1].nextContinuationToken;

  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Объекты ({items.length} загружено)</Text>
        <Group gap="xs">
          {loading ? <Loader size="xs" /> : null}
          {nextToken !== null ? (
            <Button
              size="xs"
              variant="light"
              loading={loading}
              onClick={() => void loadMore()}
            >
              Загрузить ещё
            </Button>
          ) : null}
        </Group>
      </Group>
      {error ? (
        <Alert color="red" variant="light" mb="xs">
          {error}
          <Button size="xs" variant="subtle" ml="sm" onClick={() => void loadMore()}>
            Повторить
          </Button>
        </Alert>
      ) : null}
      {items.length === 0 && !loading && error === null ? (
        <Text c="dimmed" size="sm">Объектов под префиксом {prefix} нет</Text>
      ) : (
        <Table.ScrollContainer minWidth={700} maxHeight={420}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Ключ</Table.Th>
                <Table.Th>Размер</Table.Th>
                <Table.Th>Изменён</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {items.map((o) => (
                <Table.Tr key={o.key}>
                  <Table.Td ff="monospace">{o.key}</Table.Td>
                  <Table.Td>{formatBytes(o.sizeBytes)}</Table.Td>
                  <Table.Td>{formatUnix(o.lastModifiedUnix)}</Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}
