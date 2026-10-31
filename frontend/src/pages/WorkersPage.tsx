// Грань «Воркеры» (arch/adminpanel/03 §3.7): карточки PgWorker/KafkaWorker —
// инстансы, целевой серт API, статус применения, действия с сертом и рестарт.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Card, Group, Modal, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../api/client';
import {
  deleteWorkerApiCert,
  fetchWorkerEffectiveConfig,
  fetchWorkers,
  generateWorkerApiCert,
  restartWorker,
  workerQueryKeys,
} from '../api/queries';
import type { WorkerApplyStatus, WorkerInstanceDto, WorkerViewDto } from '../api/dto';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { formatUnix, formatUnixAge } from '../utils/format';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { UploadWorkerCertModal } from './workers/UploadWorkerCertModal';

const WORKER_TITLES: Record<string, string> = {
  pgworker: 'PgWorker (PostgreSQL)',
  kafkaworker: 'KafkaWorker (Kafka)',
  valkeyworker: 'ValkeyWorker (Valkey)',
  s3worker: 'S3Worker (S3)',
};

// Действия с confirm-модалом (тексты предупреждений — arch/adminpanel/03 §3.7).
type ConfirmAction = { kind: 'generate' | 'delete' | 'restart'; worker: string } | null;

export function WorkersPage() {
  const intervalMs = usePollingIntervalMs();
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: workerQueryKeys.workers,
    queryFn: fetchWorkers,
    refetchInterval: intervalMs,
  });
  const [uploadWorker, setUploadWorker] = useState<string | null>(null);
  const [confirm, setConfirm] = useState<ConfirmAction>(null);
  const [banner, setBanner] = useState<string | null>(null);

  const refresh = () => queryClient.invalidateQueries({ queryKey: workerQueryKeys.workers });
  const showError = (e: unknown) =>
    setBanner(e instanceof ApiError ? `${e.title ?? `HTTP ${e.status}`}: ${e.detail ?? e.message}` : String(e));

  const generateMutation = useMutation({
    mutationFn: generateWorkerApiCert,
    onSuccess: async (dto) => {
      await refresh();
      // 201 с warning — не блокирующая ошибка, но оператор обязан её увидеть.
      if (dto.warning) setBanner(`${WORKER_TITLES[dto.worker] ?? dto.worker}: ${dto.warning}`);
    },
    onError: showError,
  });
  const deleteMutation = useMutation({
    mutationFn: deleteWorkerApiCert,
    onSuccess: refresh,
    onError: showError,
  });
  const restartMutation = useMutation({
    mutationFn: restartWorker,
    onSuccess: async (dto) => {
      await refresh();
      const failed = dto.results.filter((r) => !r.accepted);
      if (failed.length > 0)
        setBanner(`restart: not all instances accepted the command — ${failed.map((r) => `${r.instance}: ${r.error ?? 'refused'}`).join('; ')}`);
    },
    onError: showError,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const confirmBusy =
    generateMutation.isPending || deleteMutation.isPending || restartMutation.isPending;
  const confirmText: Record<NonNullable<ConfirmAction>['kind'], string> = {
    generate: 'Generate a self-signed certificate? It will be applied only after the worker restart.',
    delete: 'Remove the managed certificate? The worker will fall back to the env certificate after restart.',
    restart: 'Restart the worker? Running operations will continue after startup (claims and journal live in etcd); brief API unavailability window (seconds).',
  };

  return (
    <>
      <Group justify="space-between" mb="md">
        <Title order={2}>Workers</Title>
      </Group>
      {banner !== null && (
        <Alert color="red" mb="md" withCloseButton onClose={() => setBanner(null)}>{banner}</Alert>
      )}
      <Stack gap="md">
        {query.data.workers.map((w) => (
          <WorkerCard
            key={w.worker}
            view={w}
            onGenerate={() => setConfirm({ kind: 'generate', worker: w.worker })}
            onUpload={() => setUploadWorker(w.worker)}
            onDelete={() => setConfirm({ kind: 'delete', worker: w.worker })}
            onRestart={() => setConfirm({ kind: 'restart', worker: w.worker })}
          />
        ))}
      </Stack>
      <UploadWorkerCertModal
        worker={uploadWorker ?? ''}
        opened={uploadWorker !== null}
        onClose={() => setUploadWorker(null)}
      />
      <Modal
        opened={confirm !== null}
        onClose={() => setConfirm(null)}
        title={confirm === null ? '' : `${WORKER_TITLES[confirm.worker] ?? confirm.worker}`}
        size="md"
      >
        <Stack gap="sm">
          <Text>{confirm === null ? '' : confirmText[confirm.kind]}</Text>
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setConfirm(null)}>Cancel</Button>
            {confirm?.kind === 'generate' && (
              <Button
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) generateMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Generate
              </Button>
            )}
            {confirm?.kind === 'delete' && (
              <Button
                color="red"
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) deleteMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Remove certificate
              </Button>
            )}
            {confirm?.kind === 'restart' && (
              <Button
                color="red"
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) restartMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Restart
              </Button>
            )}
          </Group>
        </Stack>
      </Modal>
    </>
  );
}

// Бейдж статуса применения (applied/pending restart/unmanaged/unknown, 03 §3.7).
function ApplyStatusBadge({ status }: { status: WorkerApplyStatus }) {
  const pair: Record<WorkerApplyStatus, [string, string]> = {
    'applied': ['green', 'target cert applied on the edge'],
    'pending restart': ['yellow', 'restart required'],
    'unmanaged': ['gray', 'env certificate, no key in etcd'],
    'unknown': ['gray', 'instance does not report a thumbprint (old version)'],
  };
  const [color, tooltip] = pair[status];
  return <Tooltip label={tooltip}><Badge color={color} variant="light">{status}</Badge></Tooltip>;
}

function HealthBadge({ health }: { health: string | null | undefined }) {
  const color = health === 'healthy' ? 'green' : health === 'degraded' ? 'yellow' : 'gray';
  return <Badge color={color} variant="light">{health ?? 'unknown'}</Badge>;
}

function Thumb({ value }: { value: string | null | undefined }) {
  if (!value) return <>—</>;
  return (
    <Tooltip label={value}>
      <span style={{ fontFamily: 'monospace', fontSize: 12 }}>{value.slice(0, 16)}…</span>
    </Tooltip>
  );
}

function WorkerCard({ view, onGenerate, onUpload, onDelete, onRestart }: {
  view: WorkerViewDto;
  onGenerate: () => void;
  onUpload: () => void;
  onDelete: () => void;
  onRestart: () => void;
}) {
  const cert = view.targetCert;
  return (
    <Card withBorder padding="md" radius="md">
      <Stack gap="sm">
        <Group justify="space-between">
          <Title order={3}>{WORKER_TITLES[view.worker] ?? view.worker}</Title>
          <Group gap="xs">
            <Button size="xs" variant="light" onClick={onGenerate}>Generate certificate</Button>
            <Button size="xs" variant="light" onClick={onUpload}>Upload certificate</Button>
            <Button size="xs" color="red" variant="light" onClick={onRestart}>Restart worker</Button>
            {cert !== null && cert !== undefined && (
              <Button size="xs" color="red" variant="light" onClick={onDelete}>Remove managed certificate</Button>
            )}
          </Group>
        </Group>

        <Table.ScrollContainer minWidth={720}>
          <Table>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Instance</Table.Th>
                <Table.Th>URL</Table.Th>
                <Table.Th>Up since</Table.Th>
                <Table.Th>Health</Table.Th>
                <Table.Th>Edge thumbprint</Table.Th>
                <Table.Th>Status</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {view.instances.length === 0 ? (
                <Table.Tr><Table.Td colSpan={6}><Text c="dimmed">no live instances</Text></Table.Td></Table.Tr>
              ) : (
                view.instances.map((i) => <InstanceRow key={i.instance} instance={i} />)
              )}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>

        {/* u14: координация — лидер домена + claims «кто держит кластер» */}
        <CoordinationBlock view={view} />
        {/* u15: эффективный конфиг — read-only, on-demand (без polling) */}
        <EffectiveConfigBlock worker={view.worker} />

        {cert !== null && cert !== undefined && (
          <Stack gap={4}>
            <Text size="sm" fw={600}>Target certificate</Text>
            <Text size="sm">Subject: {cert.subject}</Text>
            <Text size="sm">Issuer: {cert.issuer}</Text>
            <Text size="sm">SAN: {cert.san.join(', ')}</Text>
            <Text size="sm">
              Valid: {formatUnix(cert.notBeforeUnix)} — {formatUnix(cert.notAfterUnix)}
            </Text>
            <Text size="sm">Thumbprint: <Thumb value={cert.thumbprint} /></Text>
            <Text size="sm" c="dimmed">
              updated {cert.updatedBy ?? '—'} · {formatUnix(cert.updatedUnix)}
              ({formatUnixAge(cert.updatedUnix)} ago)
            </Text>
          </Stack>
        )}
      </Stack>
    </Card>
  );
}

function InstanceRow({ instance }: { instance: WorkerInstanceDto }) {
  return (
    <Table.Tr>
      <Table.Td>{instance.instance}</Table.Td>
      <Table.Td style={{ fontFamily: 'monospace', fontSize: 12 }}>{instance.url}</Table.Td>
      <Table.Td>
        {instance.sinceUnix === 0 ? '—' : `${formatUnix(instance.sinceUnix)} (${formatUnixAge(instance.sinceUnix)} ago)`}
      </Table.Td>
      <Table.Td><HealthBadge health={instance.health} /></Table.Td>
      <Table.Td><Thumb value={instance.certThumbprint} /></Table.Td>
      <Table.Td><ApplyStatusBadge status={instance.applyStatus} /></Table.Td>
    </Table.Tr>
  );
}

// ——— Координация (u14, read-only): лидер домена + claims «кто держит кластер». ———
function CoordinationBlock({ view }: { view: WorkerViewDto }) {
  const coordination = view.coordination;
  if (coordination === null || coordination === undefined) return null;
  return (
    <Stack gap={4}>
      <Text size="sm" fw={600}>Координация</Text>
      <Text size="sm">
        Лидер:{' '}
        {coordination.leaderInstance === null
          ? <Text span c="dimmed">—</Text>
          : <Text span ff="monospace">{coordination.leaderInstance}</Text>}
      </Text>
      <Table.ScrollContainer minWidth={480}>
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Кластер</Table.Th>
              <Table.Th>Кто держит (instance)</Table.Th>
              <Table.Th>Since</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {coordination.claims.length === 0 ? (
              <Table.Tr><Table.Td colSpan={3}><Text c="dimmed" size="sm">claims нет (домен простаивает)</Text></Table.Td></Table.Tr>
            ) : (
              coordination.claims.map((claim) => (
                <Table.Tr key={claim.cluster}>
                  <Table.Td><Text size="sm" ff="monospace">{claim.cluster}</Text></Table.Td>
                  <Table.Td><Text size="sm" ff="monospace">{claim.instance}</Text></Table.Td>
                  <Table.Td>
                    {claim.sinceUnix === null || claim.sinceUnix === undefined
                      ? '—'
                      : `${formatUnix(claim.sinceUnix)} (${formatUnixAge(claim.sinceUnix)} ago)`}
                  </Table.Td>
                </Table.Tr>
              ))
            )}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}

// ——— Эффективный конфиг (u15, read-only, on-demand): кнопка «Показать конфиг»
// → GET /api/workers/{worker}/effective-config → дерево групп ключ-значение;
// пометка «env, применяется рестартом воркера» (source === 'env'); форм ввода
// НЕТ; живых инстансов нет (503) → «Живые инстансы не найдены».
function EffectiveConfigBlock({ worker }: { worker: string }) {
  const [opened, setOpened] = useState(false);
  const config = useQuery({
    queryKey: workerQueryKeys.effectiveConfig(worker),
    queryFn: () => fetchWorkerEffectiveConfig(worker),
    enabled: opened,
    retry: false,
    staleTime: 30_000,
  });

  if (!opened) {
    return (
      <Button size="xs" variant="light" onClick={() => setOpened(true)}>
        Показать конфиг
      </Button>
    );
  }

  let content: React.ReactNode;
  if (config.isPending) content = <Text size="sm" c="dimmed">Загрузка…</Text>;
  else if (config.isError) {
    const message = config.error instanceof ApiError
      ? (config.error.detail ?? config.error.message)
      : String(config.error);
    content = (
      <Text size="sm" c={config.error instanceof ApiError && config.error.status === 503 ? 'dimmed' : 'red'}>
        {config.error instanceof ApiError && config.error.status === 503
          ? 'Живые инстансы не найдены'
          : message}
      </Text>
    );
  } else {
    content = <ConfigTree config={config.data} />;
  }

  return (
    <Stack gap={4}>
      <Group justify="space-between">
        <Text size="sm" fw={600}>Эффективный конфиг</Text>
        <Group gap="xs">
          <Badge color="gray" variant="light" size="xs">env, применяется рестартом воркера</Badge>
          <Button size="xs" variant="default" onClick={() => setOpened(false)}>Скрыть</Button>
        </Group>
      </Group>
      {content}
    </Stack>
  );
}

// Дерево групп ключ-значение: source-группа и плоские словари (thresholds/loops/…).
function ConfigTree({ config }: { config: Record<string, unknown> }) {
  const entries = Object.entries(config).filter(([k]) => k !== 'source');
  return (
    <Stack gap={6}>
      {entries.map(([group, value]) => (
        <Stack key={group} gap={2}>
          <Text size="xs" c="dimmed" tt="uppercase">{group}</Text>
          {value !== null && typeof value === 'object' && !Array.isArray(value) ? (
            <Table.ScrollContainer minWidth={420}>
              <Table>
                <Table.Tbody>
                  {Object.entries(value as Record<string, unknown>).map(([k, v]) => (
                    <Table.Tr key={k}>
                      <Table.Td w={220}><Text size="sm" ff="monospace">{k}</Text></Table.Td>
                      <Table.Td>
                        <Text size="sm" ff="monospace">
                          {v !== null && typeof v === 'object' ? JSON.stringify(v) : String(v)}
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          ) : (
            <Text size="sm" ff="monospace">{String(value)}</Text>
          )}
        </Stack>
      ))}
    </Stack>
  );
}
