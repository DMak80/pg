// Форма создания кластера — единственная мутация панели (spec t12 §3.8).
// Переключатель типа БД (spec cluster-sharded-toggle §3.4): нешардированная =
// вырожденный случай 1×1 — поля бакетов/шардов не запрашиваются вовсе.
// Клиентская валидация — зеркало серверной (arch/02 §9.3); сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Group,
  Modal,
  NumberInput,
  Paper,
  SegmentedControl,
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

// Состояние формы: buckets/shards живут всегда — переживают переключение
// типа туда-обратно (блок лишь скрывается, spec §3.4).
interface FormState {
  name: string;
  sharded: boolean;
  buckets: number;
  shards: number;
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

const EMPTY: FormState = {
  name: '',
  sharded: true, // дефолт — текущее поведение модалки (spec §8.5)
  buckets: 16,
  shards: 2,
  replicas: 2,
  requestCpu: 2,
  requestMem: 8,
  requestDisk: 100,
};

export function ClusterCreateModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
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
  // buckets/shards проверяются только для шардированной (spec §3.4).
  function validate(): boolean {
    const errors: Record<string, string> = {};
    if (!NAME_RE.test(form.name)) errors.name = 'a-z, 0-9, _; начинается с буквы; без дефиса';
    if (form.sharded) {
      if (!Number.isInteger(form.buckets) || form.buckets < 1 || form.buckets > 8192)
        errors.buckets = 'целое 1..8192';
      if (!Number.isInteger(form.shards) || form.shards < 1 || form.shards > 128 || form.shards > form.buckets)
        errors.shards = 'целое 1..128 и не больше бакетов';
    }
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
    if (!validate()) return;
    // Тело запроса: при sharded=false поля бакетов/шардов не передаются вовсе —
    // сервер нормализует в 1/1 (arch/02 §9.3).
    const body: CreateClusterRequestDto = form.sharded
      ? { ...form }
      : {
          name: form.name,
          sharded: false,
          replicas: form.replicas,
          requestCpu: form.requestCpu,
          requestMem: form.requestMem,
          requestDisk: form.requestDisk,
        };
    mutation.mutate(body);
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
        {/* Тип БД: Mantine 9.5 SegmentedControl без label-prop — подпись Text
            над контролом (паттерн как «Ресурсы нод»), выбор — spec §8.4 */}
        <Stack gap={4}>
          <Text size="sm" fw={500}>Тип базы данных</Text>
          <SegmentedControl
            fullWidth
            data={[
              { value: 'sharded', label: 'Шардированная' },
              { value: 'single', label: 'Нешардированная' },
            ]}
            value={form.sharded ? 'sharded' : 'single'}
            onChange={(v) => set('sharded', v === 'sharded')}
          />
        </Stack>
        {form.sharded ? (
          // Paper вместо Box: у Box в Mantine 9.5 нет withBorder (spec §3.4 —
          // визуальный блок с рамкой; отклонение от сниппета плана по API)
          <Paper withBorder radius="md" p="sm">
            <Text size="sm" fw={500} mb="xs">Шардирование</Text>
            <Group grow gap="sm">
              <NumberInput label="Бакеты" min={1} max={8192} value={form.buckets}
                error={fieldErrors.buckets} onChange={(v) => set('buckets', Number(v ?? 0))} />
              <NumberInput label="Шарды" min={1} max={128} value={form.shards}
                error={fieldErrors.shards} onChange={(v) => set('shards', Number(v ?? 0))} />
            </Group>
          </Paper>
        ) : null}
        {/* Реплики — общий ряд обоих типов; description больше не ломает
            выравнивание соседей (NBSP-хак d2549ba удалён, spec §8.8) */}
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
