// Форма создания кластера — единственная мутация панели (spec t12 §3.8).
// Клиентская валидация — зеркало серверной (arch/02 §9.3); сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Group,
  Modal,
  NumberInput,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createCluster } from '../../api/queries';
import type { CreateClusterRequestDto } from '../../api/dto';

// Границы — зеркало CreateClusterLimits (arch/02 §9.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

const EMPTY: CreateClusterRequestDto = {
  name: '',
  buckets: 16,
  shards: 2,
  replicas: 2,
  requestCpu: 2,
  requestMem: 8,
  requestDisk: 100,
};

export function ClusterCreateModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<CreateClusterRequestDto>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof CreateClusterRequestDto>(key: K, value: CreateClusterRequestDto[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createCluster,
    onSuccess: async () => {
      // Список кластеров обновит следующий тик refresher'а — инвалидация ключа
      onClose();
      setForm(EMPTY);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
  });

  // Зеркало серверной валидации: по полям, до отправки (spec t12 §2).
  function validate(): boolean {
    const errors: Record<string, string> = {};
    if (!NAME_RE.test(form.name)) errors.name = 'a-z, 0-9, _; начинается с буквы; без дефиса';
    if (!Number.isInteger(form.buckets) || form.buckets < 1 || form.buckets > 8192)
      errors.buckets = 'целое 1..8192';
    if (!Number.isInteger(form.shards) || form.shards < 1 || form.shards > 128 || form.shards > form.buckets)
      errors.shards = 'целое 1..128 и не больше бакетов';
    if (!Number.isInteger(form.replicas) || form.replicas < 1 || form.replicas > 26)
      errors.replicas = 'целое 1..26 (1 = только мастер)';
    if (form.requestCpu < 0.01 || form.requestCpu > 64) errors.requestCpu = '0.01..64';
    if (!Number.isInteger(form.requestMem) || form.requestMem < 1 || form.requestMem > 65536)
      errors.requestMem = 'целое 1..65536';
    if (!Number.isInteger(form.requestDisk) || form.requestDisk < 1 || form.requestDisk > 65536)
      errors.requestDisk = 'целое 1..65536';
    setFieldErrors(errors);
    return Object.keys(errors).length === 0;
  }

  function submit() {
    if (validate()) mutation.mutate(form);
  }

  // Ошибка сервера: 409 «имя занято» / 400 по полям / 503 «etcd» (ProblemDetails).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Создать кластер" centered>
      <Stack gap="sm">
        <TextInput
          label="Имя кластера"
          description="уникально; dbname = имя"
          placeholder="shop"
          value={form.name}
          error={fieldErrors.name}
          onChange={(e) => set('name', e.currentTarget.value)}
        />
        <Group grow gap="sm">
          {/* NBSP-заглушка: резервирует высоту description, чтобы ряд совпадал с «Репликами» */}
          <NumberInput label="Бакеты" min={1} max={8192} value={form.buckets}
            description={'\u00A0'}
            error={fieldErrors.buckets} onChange={(v) => set('buckets', Number(v ?? 0))} />
          <NumberInput label="Шарды" min={1} max={128} value={form.shards}
            description={'\u00A0'}
            error={fieldErrors.shards} onChange={(v) => set('shards', Number(v ?? 0))} />
          <NumberInput label="Реплики" min={1} max={26} value={form.replicas}
            description="2 = мастер + реплика"
            error={fieldErrors.replicas} onChange={(v) => set('replicas', Number(v ?? 0))} />
        </Group>
        <Text size="sm" c="dimmed">Ресурсы нод (заявка, на каждую ноду)</Text>
        <Group grow gap="sm">
          <NumberInput label="CPU (ядра)" min={0.01} max={64} step={0.1} decimalScale={2}
            value={form.requestCpu} error={fieldErrors.requestCpu}
            onChange={(v) => set('requestCpu', Number(v ?? 0))} />
          <NumberInput label="Память (GiB)" min={1} max={65536} value={form.requestMem}
            error={fieldErrors.requestMem} onChange={(v) => set('requestMem', Number(v ?? 0))} />
          <NumberInput label="Диск (GiB)" min={1} max={65536} value={form.requestDisk}
            error={fieldErrors.requestDisk} onChange={(v) => set('requestDisk', Number(v ?? 0))} />
        </Group>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? 'Имя уже занято — выберите другое'
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Создать</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
