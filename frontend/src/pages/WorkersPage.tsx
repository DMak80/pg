// Грань «Воркеры» (arch/adminpanel/03 §3.7): карточки PgWorker/KafkaWorker —
// инстансы, целевой серт API, статус применения, действия с сертом и рестарт.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Card, Group, Modal, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../api/client';
import { deleteWorkerApiCert, fetchWorkers, generateWorkerApiCert, restartWorker, workerQueryKeys } from '../api/queries';
import type { WorkerApplyStatus, WorkerInstanceDto, WorkerViewDto } from '../api/dto';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { formatUnix, formatUnixAge } from '../utils/format';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { UploadWorkerCertModal } from './workers/UploadWorkerCertModal';

const WORKER_TITLES: Record<string, string> = {
  pgworker: 'PgWorker (бд)',
  kafkaworker: 'KafkaWorker (кафки)',
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
        setBanner(`перезапуск: не все инстансы приняли команду — ${failed.map((r) => `${r.instance}: ${r.error ?? 'отказ'}`).join('; ')}`);
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
    generate: 'Сгенерировать self-signed сертификат? Применится только после перезапуска воркера.',
    delete: 'Убрать управляемый сертификат? Воркер вернётся к env-серту после перезапуска.',
    restart: 'Перезапустить воркера? Идущие операции продолжатся после подъёма (клэймы и журнал — в etcd); краткое окно недоступности API (секунды).',
  };

  return (
    <>
      <Group justify="space-between" mb="md">
        <Title order={2}>Воркеры</Title>
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
            <Button variant="default" onClick={() => setConfirm(null)}>Отмена</Button>
            {confirm?.kind === 'generate' && (
              <Button
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) generateMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Сгенерировать
              </Button>
            )}
            {confirm?.kind === 'delete' && (
              <Button
                color="red"
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) deleteMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Убрать сертификат
              </Button>
            )}
            {confirm?.kind === 'restart' && (
              <Button
                color="red"
                loading={confirmBusy}
                onClick={() => { if (confirm !== null) restartMutation.mutate(confirm.worker); setConfirm(null); }}
              >
                Перезапустить
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
    'applied': ['green', 'целевой серт применён на грани'],
    'pending restart': ['yellow', 'требуется перезапуск'],
    'unmanaged': ['gray', 'env-сертификат, ключа в etcd нет'],
    'unknown': ['gray', 'инстанс не сообщает thumbprint (старая версия)'],
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
            <Button size="xs" variant="light" onClick={onGenerate}>Сгенерировать сертификат</Button>
            <Button size="xs" variant="light" onClick={onUpload}>Загрузить сертификат</Button>
            <Button size="xs" color="red" variant="light" onClick={onRestart}>Перезапустить воркера</Button>
            {cert !== null && cert !== undefined && (
              <Button size="xs" color="red" variant="light" onClick={onDelete}>Убрать управляемый сертификат</Button>
            )}
          </Group>
        </Group>

        <Table.ScrollContainer minWidth={720}>
          <Table>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Инстанс</Table.Th>
                <Table.Th>URL</Table.Th>
                <Table.Th>Поднят</Table.Th>
                <Table.Th>Health</Table.Th>
                <Table.Th>Thumbprint грани</Table.Th>
                <Table.Th>Статус</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {view.instances.length === 0 ? (
                <Table.Tr><Table.Td colSpan={6}><Text c="dimmed">живых инстансов нет</Text></Table.Td></Table.Tr>
              ) : (
                view.instances.map((i) => <InstanceRow key={i.instance} instance={i} />)
              )}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>

        {cert !== null && cert !== undefined && (
          <Stack gap={4}>
            <Text size="sm" fw={600}>Целевой сертификат</Text>
            <Text size="sm">Subject: {cert.subject}</Text>
            <Text size="sm">Issuer: {cert.issuer}</Text>
            <Text size="sm">SAN: {cert.san.join(', ')}</Text>
            <Text size="sm">
              Действует: {formatUnix(cert.notBeforeUnix)} — {formatUnix(cert.notAfterUnix)}
            </Text>
            <Text size="sm">Thumbprint: <Thumb value={cert.thumbprint} /></Text>
            <Text size="sm" c="dimmed">
              обновлён {cert.updatedBy ?? '—'} · {formatUnix(cert.updatedUnix)}
              ({formatUnixAge(cert.updatedUnix)} назад)
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
        {instance.sinceUnix === 0 ? '—' : `${formatUnix(instance.sinceUnix)} (${formatUnixAge(instance.sinceUnix)} назад)`}
      </Table.Td>
      <Table.Td><HealthBadge health={instance.health} /></Table.Td>
      <Table.Td><Thumb value={instance.certThumbprint} /></Table.Td>
      <Table.Td><ApplyStatusBadge status={instance.applyStatus} /></Table.Td>
    </Table.Tr>
  );
}
