// Форма создания valkey-кластера (arch/03 §8.3.1; валидации 02 §11.3): имя,
// maxmemory в MiB (в API — байты), policy из 8 канонических, ресурсы ноды.
// Клиентская валидация — зеркало серверной (ValkeyLimits), сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Select, Stack, Text, TextInput } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createValkeyCluster, valkeyQueryKeys } from '../../api/queries';
import type { CreateValkeyClusterRequestDto } from '../../api/dto';

// Границы — зеркало ValkeyLimits (arch/02 §11.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;
const KNOWN_POLICIES = [
  'allkeys-lru',
  'allkeys-lfu',
  'volatile-lru',
  'volatile-lfu',
  'allkeys-random',
  'volatile-random',
  'volatile-ttl',
  'noeviction',
];
const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

interface FormState {
  name: string;
  maxmemoryMib: number;
  policy: string;
  cpu: number;
  memGi: number;
  diskGi: number;
}

const EMPTY: FormState = {
  name: '',
  maxmemoryMib: 512,
  policy: 'allkeys-lru',
  cpu: 1,
  memGi: 1,
  diskGi: 10,
};

export function CreateValkeyClusterModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [formError, setFormError] = useState<string | null>(null);
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createValkeyCluster,
    onSuccess: async () => {
      setForm(EMPTY);
      setFormError(null);
      onClose();
      // Список обновит следующий тик valkey-refresher'а — инвалидация ключа ускоряет.
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  // Валидация-зеркало 02 §11.3 (сервер — источник истины, это UX); maxmemory —
  // в MiB (UI), в API уходит maxmemoryBytes = MiB * 1048576.
  function validate(): string | null {
    if (!NAME_RE.test(form.name)) return 'имя: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)';
    if (!Number.isInteger(form.maxmemoryMib) || form.maxmemoryMib < 1)
      return 'maxmemory: целое ≥ 1 MiB';
    if (!KNOWN_POLICIES.includes(form.policy)) return 'maxmemoryPolicy: выберите из списка';
    if (form.cpu < 0.01 || form.cpu > 64) return 'cpu: 0.01..64 ядер';
    if (!Number.isInteger(form.memGi) || form.memGi < 1 || form.memGi > 65536)
      return 'memGi: целое 1..65536';
    if (!Number.isInteger(form.diskGi) || form.diskGi < 1 || form.diskGi > 65536)
      return 'diskGi: целое 1..65536';
    // Инвариант R3: maxmemoryBytes < mem-лимит (иначе OOM-килл контейнера).
    if (form.maxmemoryMib * MIB >= form.memGi * GIB)
      return 'maxmemory обязан быть меньше mem-лимита — иначе OOM-килл (R3)';
    return null;
  }

  function submit() {
    const error = validate();
    setFormError(error);
    if (error !== null) return;
    const request: CreateValkeyClusterRequestDto = {
      name: form.name,
      maxmemoryBytes: form.maxmemoryMib * MIB,
      maxmemoryPolicy: form.policy,
      resources: { cpu: form.cpu, memGi: form.memGi, diskGi: form.diskGi },
    };
    mutation.mutate(request);
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Создать valkey-кластер" centered>
      <Stack gap="sm">
        <TextInput
          label="Имя"
          placeholder="cache"
          value={form.name}
          onChange={(e) => set('name', e.currentTarget.value)}
          error={formError !== null && formError.startsWith('имя') ? formError : undefined}
        />
        <Group grow>
          <NumberInput label="maxmemory, MiB" value={form.maxmemoryMib}
            min={1} onChange={(v) => set('maxmemoryMib', Number(v ?? 0))} />
          <Select label="Политика вытеснения" data={KNOWN_POLICIES} value={form.policy}
            onChange={(v) => set('policy', v ?? 'allkeys-lru')} />
        </Group>
        {formError !== null && !formError.startsWith('имя') ? (
          <Alert color="red" variant="light">{formError}</Alert>
        ) : null}
        <Text size="sm" c="dimmed">Ресурсы ноды node1:</Text>
        <Group grow>
          <NumberInput label="CPU" value={form.cpu} step={0.5}
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
