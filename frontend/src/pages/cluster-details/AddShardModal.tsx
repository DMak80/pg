// Форма добавления шарда (t06, arch/03 §3.2): имя генерирует сервер (shard<max+1>);
// шард стартует ПУСТЫМ — никакого перераспределения бакетов (граница t06 §2.1).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { addShard, queryKeys } from '../../api/queries';

interface FormState {
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

const EMPTY: FormState = { replicas: 2, requestCpu: 2, requestMem: 8, requestDisk: 100 };

export function AddShardModal({ cluster, opened, onClose }: {
  cluster: string; opened: boolean; onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: (body: FormState) => addShard(cluster, body),
    onSuccess: async () => {
      // Список и детали обновит следующий тик refresher'а — инвалидация ключей.
      onClose();
      setForm(EMPTY);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Зеркало серверной валидации §9.3 (replicas/cpu/mem/disk — те же границы).
  function validate(): boolean {
    const errors: Record<string, string> = {};
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

  // Ошибка сервера: 409 «не Active / имя занято» / 400 по полям / 503 «etcd» (ProblemDetails).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Добавить шард" centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">Имя генерируется автоматически (shard&lt;N+1&gt;).</Text>
        <NumberInput label="Реплики" min={1} max={26} value={form.replicas}
          description="2 = мастер + реплика"
          error={fieldErrors.replicas} onChange={(v) => set('replicas', Number(v ?? 0))} />
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
        <Text size="sm" c="dimmed">
          Шард стартует пустым — перераспределение бакетов выполняется отдельными
          явными переездами (UI переездов — t07).
        </Text>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Кластер не Active или имя шарда занято')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending}
            onClick={() => validate() && mutation.mutate(form)}>Добавить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
