// Форма создания kafka-кластера (arch/02 §10.2-1/§10.3): модал полей с дефолтами
// 3/3/2/12/7д/2/2/20; клиентская валидация — зеркало серверной, сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, TextInput } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createKafkaCluster } from '../../api/queries';
import type { CreateKafkaClusterRequestDto } from '../../api/dto';

// Границы — зеркало KafkaLimits (arch/02 §10.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;
const DAY_MS = 86_400_000;

interface FormState {
  name: string;
  brokers: number;
  replicationFactor: number;
  minInSyncReplicas: number;
  defaultPartitions: number;
  retentionDays: number;
  cpu: number;
  memGi: number;
  diskGi: number;
}

const EMPTY: FormState = {
  name: '',
  brokers: 3,
  replicationFactor: 3,
  minInSyncReplicas: 2,
  defaultPartitions: 12,
  retentionDays: 7,
  cpu: 2,
  memGi: 2,
  diskGi: 20,
};

export function CreateKafkaClusterModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createKafkaCluster,
    onSuccess: async () => {
      setForm(EMPTY);
      setFieldErrors({});
      onClose();
      // Список обновит следующий тик kafka-refresher'а — инвалидация ключа ускоряет.
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  function validate(): string | null {
    if (!NAME_RE.test(form.name)) return 'имя: ^[a-z][a-z0-9_]{0,62}$ (без дефиса)';
    if (form.brokers < 1 || form.brokers > 9) return 'брокеры: 1..9';
    if (form.replicationFactor < 1 || form.replicationFactor > 9
      || form.replicationFactor > form.brokers)
      return 'replicationFactor: 1..9 и не больше брокеров';
    if (form.minInSyncReplicas < 1 || form.minInSyncReplicas > form.replicationFactor)
      return 'minInSyncReplicas: 1..replicationFactor';
    if (form.defaultPartitions < 1 || form.defaultPartitions > 1000)
      return 'партиции по умолчанию: 1..1000';
    if (form.retentionDays < 1) return 'retention: минимум 1 день';
    if (form.cpu < 0.01 || form.cpu > 64) return 'cpu: 0.01..64 ядер';
    if (form.memGi < 1 || form.memGi > 65536) return 'память: 1..65536 GiB';
    if (form.diskGi < 1 || form.diskGi > 65536) return 'диск: 1..65536 GiB';
    return null;
  }

  function submit() {
    const error = validate();
    setFieldErrors(error ? { name: error } : {});
    if (error !== null) return;
    const request: CreateKafkaClusterRequestDto = {
      name: form.name,
      brokers: form.brokers,
      replicationFactor: form.replicationFactor,
      minInSyncReplicas: form.minInSyncReplicas,
      defaultPartitions: form.defaultPartitions,
      defaultRetentionMs: Math.round(form.retentionDays * DAY_MS),
      cpu: form.cpu,
      memGi: form.memGi,
      diskGi: form.diskGi,
    };
    mutation.mutate(request);
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Создать kafka-кластер" centered>
      <Stack gap="sm">
        <TextInput
          label="Имя"
          placeholder="events"
          value={form.name}
          onChange={(e) => set('name', e.currentTarget.value)}
          error={fieldErrors.name}
        />
        <Group grow>
          <NumberInput label="Брокеры (1..9)" value={form.brokers}
            min={1} max={9} onChange={(v) => set('brokers', Number(v ?? 0))} />
          <NumberInput label="Replication factor" value={form.replicationFactor}
            min={1} max={9} onChange={(v) => set('replicationFactor', Number(v ?? 0))} />
        </Group>
        <Group grow>
          <NumberInput label="Min ISR" value={form.minInSyncReplicas}
            min={1} onChange={(v) => set('minInSyncReplicas', Number(v ?? 0))} />
          <NumberInput label="Партиций по умолчанию" value={form.defaultPartitions}
            min={1} max={1000} onChange={(v) => set('defaultPartitions', Number(v ?? 0))} />
        </Group>
        <NumberInput label="Retention, дней" value={form.retentionDays}
          min={1} onChange={(v) => set('retentionDays', Number(v ?? 0))} />
        <Group grow>
          <NumberInput label="CPU на брокера" value={form.cpu} step={0.5}
            min={0.01} max={64} onChange={(v) => set('cpu', Number(v ?? 0))} />
          <NumberInput label="Память, GiB" value={form.memGi}
            min={1} onChange={(v) => set('memGi', Number(v ?? 0))} />
          <NumberInput label="Диск, GiB" value={form.diskGi}
            min={1} onChange={(v) => set('diskGi', Number(v ?? 0))} />
        </Group>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Создать</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
