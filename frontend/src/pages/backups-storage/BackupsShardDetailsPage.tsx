// Детали шарда «Хранилище бэкапов» (t08, arch/03 §3): таблица полных
// (id/размер/дата + etcd state/verify + статус сверки), блок WAL (etcd-статус
// + S3-факт), бейдж активного restore, блок «Объекты» с on-demand пагинацией
// («Загрузить ещё» — единственный прямой выход панели в S3-хранилище на действие).
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Card,
  Group,
  Loader,
  NumberInput,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { Link, useParams } from 'react-router';
import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  BackupDrillBadgeDto,
  BackupFullDto,
  BackupObjectsPageDto,
  BackupReconcileStatusName,
  BackupWalDto,
} from '../../api/dto';
import {
  backupsQueryKeys,
  fetchBackupObjectsPage,
  fetchBackupShardStorage,
  fetchBackupsStorage,
  restoreShard,
  updateBackupsPolicy,
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
  S3Only: 'objects without key',
  EtcdOnly: 'key without objects',
  InProgress: 'in progress',
  Deleting: 'deleting',
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
            <Text>Shard {cluster}/{shard} is missing from both etcd and the inventory S3 tree</Text>
            <Anchor component={Link} to="/backups-storage" size="sm">← Backup Storage</Anchor>
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
        <Anchor component={Link} to="/backups-storage" size="sm">← Backup Storage</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2} ff="monospace">{data.cluster}/{data.shard}</Title>
          {/* Бейдж активного restore: только статус, кнопок нет (arch/backups/03-restore.md). */}
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
          <Text size="sm" c="yellow" mt={4}>Reconcile: {data.reconcileNote}</Text>
        ) : null}
      </div>
      {/* u-restore: кнопка «Восстановить из бэкапа» — модал 02 §9.13 */}
      <RestoreBlock cluster={data.cluster} shard={data.shard} />
      <FullsTable fulls={data.fulls} />
      <WalCard wal={data.wal} />
      {/* u11: приёмники WAL per-node (arch/backups/05-etcd-contract.md) */}
      <AgentsBlock agents={data.agents ?? null} />
      <DrillCard drill={data.drill} cluster={data.cluster} />
      <ObjectsBlock cluster={data.cluster} shard={data.shard} />
    </Stack>
  );
}

// Таблица полных: S3-факт (id/размер/объекты/дата) + etcd-факт (state/verify)
// + статус сверки (AC4/AC9 — джойн глазами человека).
function FullsTable({ fulls }: { fulls: BackupFullDto[] }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Full backups</Text>
      {fulls.length === 0 ? (
        <Text c="dimmed" size="sm">No fulls in either S3 or etcd</Text>
      ) : (
        <Table.ScrollContainer minWidth={900}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>ID</Table.Th>
                <Table.Th>Size (S3)</Table.Th>
                <Table.Th>Objects</Table.Th>
                <Table.Th>Date (S3)</Table.Th>
                <Table.Th>etcd state</Table.Th>
                <Table.Th>Verify</Table.Th>
                <Table.Th>etcd size</Table.Th>
                <Table.Th>Reconcile</Table.Th>
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
        <Text c="dimmed" size="sm">No WAL data in either etcd or S3</Text>
      </Card>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">WAL</Text>
      <Table w="auto">
        <Table.Tbody>
          <Table.Tr>
            <Table.Td c="dimmed" w={220}>etcd chain state</Table.Td>
            <Table.Td>
              <Group gap="xs">
                {wal.etcdState === null ? (
                  <Text size="sm">—</Text>
                ) : (
                  <Badge
                    color={wal.etcdState === 'ACTIVE' ? 'teal'
                      : wal.etcdState === 'WAL_CHAIN_ERROR' ? 'red' : 'blue'}
                    variant="light"
                  >
                    {wal.etcdState}
                  </Badge>
                )}
                <Text size="sm">
                  last segment: <Text span ff="monospace">{wal.etcdLastSegment ?? '—'}</Text>
                  {wal.etcdLastUnix === null ? '' : ` (${formatUnix(wal.etcdLastUnix)})`}
                </Text>
              </Group>
            </Table.Td>
          </Table.Tr>
          <Table.Tr>
            <Table.Td c="dimmed">S3 facts</Table.Td>
            <Table.Td>
              <Text size="sm">
                segments {wal.s3SegmentCount}, histories {wal.s3HistoryCount}, size {formatBytes(wal.s3SizeBytes)}
                {wal.s3LastModifiedUnix === 0 ? '' : `, updated ${formatUnix(wal.s3LastModifiedUnix)}`}
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

// Цвет бейджа дрилла по состоянию ключа (arch/backups/04-drill.md).
function drillColor(state: string): string {
  return state === 'SUCCEEDED' ? 'green' : state === 'FAILED' ? 'red' : 'blue';
}

// Блок «Дрилл восстановимости» (reliability t02, arch/backups/04-drill.md): статус
// последнего/текущего дрилла (state/фаза/возраст/LSN/error) + форма «Дрилл
// каждые N суток» (0 = выкл) — PUT ПОЛНОЙ policy через панельную команду-прокси
// (канон замещения целиком; панель в etcd не пишет).
function DrillCard({ drill, cluster }: { drill: BackupDrillBadgeDto | null; cluster: string }) {
  const queryClient = useQueryClient();
  // policy кластера — из СРЕЗА своего кластера (?cluster=, arch/03 §1;
  // форма читает текущие значения/дефолтирует).
  const storage = useQuery({
    queryKey: backupsQueryKeys.storageCluster(cluster),
    queryFn: () => fetchBackupsStorage(cluster),
  });
  const policy = storage.data?.clusters.find((c) => c.cluster === cluster)?.policy ?? null;

  // Дефолты фронта при null-полях policy (7/4/6/86400/true/1 — канон arch/backups/09-operability.md §1).
  const [drillDays, setDrillDays] = useState<number | ''>(1);
  const initialized = useRef(false);
  useEffect(() => {
    // Инициализация — когда политика известна ИЛИ загрузка завершилась (F3):
    // на pending-прогоне policy ещё null — не застываем на дефолте.
    // policy нет (fresh-кластер) → дефолт 1 — тот же, что уходит в save
    // при policy=null (полное замещение фиксирует значение).
    if (initialized.current || (policy === null && storage.isPending)) return;
    initialized.current = true;
    setDrillDays(policy?.drillIntervalDays ?? 1);
  }, [policy, storage.isPending]);

  const save = useMutation({
    mutationFn: () =>
      updateBackupsPolicy(cluster, {
        retentionDays: policy?.retentionDays ?? 7,
        retentionWeeks: policy?.retentionWeeks ?? 4,
        retentionMonths: policy?.retentionMonths ?? 6,
        fullMaxAgeSec: policy?.fullMaxAgeSec ?? 86400,
        verifyOnCreate: policy?.verifyOnCreate ?? true,
        drillIntervalDays: drillDays === '' ? 1 : drillDays,
        // t36: полный policy — s3 возвращаем как есть (сброс — только модалом).
        s3: policy?.s3 ?? null,
      }),
    onSuccess: () => {
      notifications.show({
        message: 'Drill interval applied — the next worker pass, no restart',
        color: 'teal',
      });
      void queryClient.invalidateQueries({ queryKey: backupsQueryKeys.storage });
    },
    onError: (e) => {
      notifications.show({
        message: e instanceof ApiError ? e.message : 'Failed to apply the interval',
        color: 'red',
      });
    },
  });

  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Recovery drill</Text>
      {drill === null ? (
        <Text c="dimmed" size="sm">No drills yet (no /drill key)</Text>
      ) : (
        <Group gap="xs" mb="xs" wrap="wrap">
          <Badge color={drillColor(drill.state)} variant="light">
            Drill: {drill.state}
            {drill.phase ? ` ${drill.phase}` : ''}
          </Badge>
          <Text size="sm">
            started {formatUnix(drill.startedUnix)}
            {drill.finishedUnix === null ? '' : `, finished ${formatUnix(drill.finishedUnix)}`}
          </Text>
          {drill.restoredToLsn ? (
            <Text size="sm" ff="monospace">restored_to_lsn: {drill.restoredToLsn}</Text>
          ) : null}
        </Group>
      )}
      {drill?.error ? <Text size="sm" c="red" mb="xs">{drill.error}</Text> : null}
      <Group gap="xs" align="end">
        <NumberInput
          label="Drill every N days"
          description="0 — disabled; applied on the next worker pass"
          value={drillDays}
          onChange={(v) => setDrillDays(typeof v === 'number' ? v : '')}
          min={0}
          max={3650}
          w={220}
        />
        <Button
          size="sm"
          loading={save.isPending}
          onClick={() => save.mutate()}
        >
          Save
        </Button>
      </Group>
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
      setError(e instanceof ApiError ? e.message : 'Failed to load objects');
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
        <Text fw={600}>Objects ({items.length} loaded)</Text>
        <Group gap="xs">
          {loading ? <Loader size="xs" /> : null}
          {nextToken !== null ? (
            <Button
              size="xs"
              variant="light"
              loading={loading}
              onClick={() => void loadMore()}
            >
              Load more
            </Button>
          ) : null}
        </Group>
      </Group>
      {error ? (
        <Alert color="red" variant="light" mb="xs">
          {error}
          <Button size="xs" variant="subtle" ml="sm" onClick={() => void loadMore()}>
            Retry
          </Button>
        </Alert>
      ) : null}
      {items.length === 0 && !loading && error === null ? (
        <Text c="dimmed" size="sm">No objects under the prefix {prefix}</Text>
      ) : (
        <Table.ScrollContainer minWidth={700} maxHeight={420}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Key</Table.Th>
                <Table.Th>Size</Table.Th>
                <Table.Th>Modified</Table.Th>
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
// ——— Restore-заявка (u-restore, 02 §9.13): модал targetTime (опц., RFC3339) +
// подтверждение вводом имени шарда; предупреждение «разрушающая и долгая»;
// 202 → баннер «Заявка restore поставлена»; 400/409 — текстом ProblemDetails. ———
function RestoreBlock({ cluster, shard }: { cluster: string; shard: string }) {
  const [opened, setOpened] = useState(false);
  const [targetTime, setTargetTime] = useState('');
  const [confirm, setConfirm] = useState('');
  const [banner, setBanner] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const confirmed = confirm === shard;
  const targetTimeValid = targetTime === ''
    || !Number.isNaN(Date.parse(targetTime)); // RFC3339-парсится

  const submit = async (): Promise<void> => {
    setSaving(true);
    setError(null);
    try {
      await restoreShard(cluster, shard, shard, targetTime === '' ? undefined : targetTime);
      setBanner('Заявка restore поставлена — исполнит джоб воркера (статус: бейдж Restore выше).');
      setOpened(false);
      setConfirm('');
      setTargetTime('');
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Stack gap="xs">
      <Group justify="space-between">
        <Button color="orange" variant="light" onClick={() => setOpened(true)}>
          Восстановить из бэкапа
        </Button>
        {banner !== null ? <Text size="sm" c="teal">{banner}</Text> : null}
      </Group>
      {opened ? (
        <Card withBorder padding="md" radius="md" style={{ borderColor: 'var(--mantine-color-orange-6)' }}>
          <Stack gap="sm">
            <Text size="sm" c="orange">
              Операция разрушающая и долгая: шард восстанавливается из бэкапа
              (PITR latest или target_time; arch/backups/03-restore.md).
            </Text>
            <Text size="sm" c="dimmed">
              Подтвердите вводом имени шарда <Text span ff="monospace">{shard}</Text>:
            </Text>
            <input
              value={confirm}
              onChange={(e) => setConfirm(e.currentTarget.value)}
              placeholder={shard}
              style={{
                padding: 8, borderRadius: 4, border: '1px solid var(--mantine-color-gray-6)',
                background: 'transparent', color: 'inherit',
              }}
            />
            <Text size="sm" c="dimmed">targetTime (опционально, RFC3339, пусто = latest):</Text>
            <input
              value={targetTime}
              onChange={(e) => setTargetTime(e.currentTarget.value)}
              placeholder="2026-01-01T12:00:00Z"
              style={{
                padding: 8, borderRadius: 4, border: '1px solid var(--mantine-color-gray-6)',
                background: 'transparent', color: 'inherit',
              }}
            />
            {error !== null ? <Text size="sm" c="red">{error}</Text> : null}
            <Group justify="flex-end">
              <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
              <Button color="orange" disabled={!confirmed || !targetTimeValid} loading={saving}
                onClick={() => void submit()}>
                Поставить заявку
              </Button>
            </Group>
          </Stack>
        </Card>
      ) : null}
    </Stack>
  );
}

// ——— Приёмники WAL (u11): таблица agents[] — Нода|Роль|State-бейдж|Ошибка|Штамп;
// state-бейджи: streaming=green, waiting_*=yellow, failed_permanent=red. ———
function AgentsBlock({ agents }: { agents: { node: string; role: string | null; state: string;
  lastDeliveredSegment: number | null; lastDeliveredUnix: number | null; poolExhausted: boolean;
  tliJump: number | null; error: string | null; updatedUnix: number }[] | null }) {
  // agent/<N> — lease-ключи: смерть приёмников = пустой agents[]. Раздел НЕ
  // пропадает молча (это единственное место наблюдения агентов) — серая
  // строка «приёмников нет» (#26).
  if (agents === null || agents.length === 0) {
    return (
      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">Приёмники WAL (агенты нод)</Text>
        <Text c="dimmed" size="sm">Приёмников нет (ключи agent/&lt;N&gt; не живы — приёмники WAL не работают).</Text>
      </Card>
    );
  }
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Приёмники WAL (агенты нод)</Text>
      <Table.ScrollContainer minWidth={720}>
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Нода</Table.Th>
              <Table.Th>Роль</Table.Th>
              <Table.Th>State</Table.Th>
              <Table.Th>Последний сегмент</Table.Th>
              <Table.Th>Ошибка</Table.Th>
              <Table.Th>Штамп</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {agents.map((a) => (
              <Table.Tr key={a.node}>
                <Table.Td><Text size="sm" ff="monospace">{a.node}</Text></Table.Td>
                <Table.Td>{a.role ?? '—'}</Table.Td>
                <Table.Td>
                  <Badge
                    color={agentColor(a.state)}
                    variant="light"
                  >
                    {a.state}
                    {a.poolExhausted ? ' · pool exhausted' : ''}
                  </Badge>
                  {a.tliJump !== null ? (
                    <Tooltip label={`tli_jump: ${a.tliJump}`}>
                      <Badge color="yellow" variant="light" size="xs" ml={4}>tli {a.tliJump}</Badge>
                    </Tooltip>
                  ) : null}
                </Table.Td>
                <Table.Td>
                  {a.lastDeliveredSegment === null ? '—' : (
                    <Tooltip label={a.lastDeliveredUnix === null ? '' : formatUnix(a.lastDeliveredUnix)}>
                      <Text size="sm" ff="monospace">{a.lastDeliveredSegment}</Text>
                    </Tooltip>
                  )}
                </Table.Td>
                <Table.Td>
                  {a.error !== null ? <Text size="sm" c="red">{a.error}</Text> : <Text c="dimmed">—</Text>}
                </Table.Td>
                <Table.Td>{formatUnix(a.updatedUnix)}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Card>
  );
}

function agentColor(state: string): string {
  if (state === 'streaming') return 'green';
  if (state === 'failed_permanent') return 'red';
  if (state.startsWith('waiting_')) return 'yellow';
  return 'gray'; // starting/paused — нейтрально
}
