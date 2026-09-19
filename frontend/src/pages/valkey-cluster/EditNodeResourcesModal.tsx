// Модалка «Изменить ресурсы ноды» valkey (arch/03 §8.3.2; 02 §11.2): PUT
// декларации через панель → API воркера; применяется автоконверге воркера
// ПЕРЕСОЗДАНИЕМ контейнера (кеш восполним, volume нет — arch/20 §1).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { updateValkeyNodeResources, valkeyQueryKeys } from '../../api/queries';
import type { ValkeyNodeDto } from '../../api/dto';

export function EditNodeResourcesModal({
  cluster,
  node,
  opened,
  onClose,
}: {
  cluster: string;
  node: ValkeyNodeDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [cpu, setCpu] = useState<number>(node.cpu ?? 1);
  const [memGi, setMemGi] = useState<number>(node.memGi ?? 1);
  const [diskGi, setDiskGi] = useState<number>(node.diskGi ?? 10);

  const mutation = useMutation({
    mutationFn: () => updateValkeyNodeResources(cluster, node.name, { cpu, memGi, diskGi }),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Декларация ресурсов обновлена',
        message: 'Воркер применит её автоматически пересозданием контейнера ноды '
          + '(PROVISIONING → RUNNING; кеш восполним — volume нет).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  // Кнопка дизейблена, пока значения не менялись (заявка ресурсов требует поля).
  const unchanged = cpu === (node.cpu ?? 1)
    && memGi === (node.memGi ?? 1)
    && diskGi === (node.diskGi ?? 10);

  return (
    <Modal opened={opened} onClose={onClose}
      title={`Ресурсы ${node.name} — ${cluster}`} centered>
      <Stack gap="sm">
        <Group grow>
          <NumberInput label="CPU" value={cpu} step={0.5} min={0.01} max={64}
            onChange={(v) => setCpu(Number(v ?? 0))} />
          <NumberInput label="Память, GiB" value={memGi} min={1} max={65536}
            onChange={(v) => setMemGi(Number(v ?? 0))} />
          <NumberInput label="Диск, GiB" value={diskGi} min={1} max={65536}
            onChange={(v) => setDiskGi(Number(v ?? 0))} />
        </Group>
        <Text size="sm" c="dimmed">
          Применяется пересозданием контейнера — кеш восполним; disk — инфо-поле
          (квот нет). Держите maxmemory меньше нового mem-лимита (R3).
        </Text>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button disabled={unchanged} loading={mutation.isPending}>Применить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
