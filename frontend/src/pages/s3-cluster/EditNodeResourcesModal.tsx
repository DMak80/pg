// Модалка «Изменить ресурсы ноды» S3 (arch/03 §12.3; 02 §12.2-5): PUT
// декларации через панель → API воркера; null-поля = не менять (сервер RMW);
// применяет автоконверге надзора C пересозданием контейнера.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { s3QueryKeys, updateS3NodeResources } from '../../api/queries';
import type { S3NodeDto } from '../../api/dto';

export function EditNodeResourcesModal({
  cluster,
  node,
  opened,
  onClose,
}: {
  cluster: string;
  node: S3NodeDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [cpu, setCpu] = useState<number | null>(node.resources?.cpu ?? null);
  const [memGi, setMemGi] = useState<number | null>(node.resources?.memGi ?? null);
  const [diskGi, setDiskGi] = useState<number | null>(node.resources?.diskGi ?? null);

  const mutation = useMutation({
    mutationFn: () => updateS3NodeResources(cluster, node.name, { cpu, memGi, diskGi }),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Декларация ресурсов обновлена',
        message: 'Воркер применит её автоконверге надзора — пересозданием контейнера инстанса.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose}
      title={`Ресурсы ${node.name} — ${cluster}`} centered>
      <Stack gap="sm">
        <Group grow>
          <NumberInput label="CPU" value={cpu ?? ''} step={0.1} min={0.01} max={64}
            onChange={(v) => setCpu(v === undefined || v === '' ? null : Number(v))} />
          <NumberInput label="Память, GiB" value={memGi ?? ''} min={1} max={65536}
            onChange={(v) => setMemGi(v === undefined || v === '' ? null : Number(v))} />
          <NumberInput label="Диск, GiB" value={diskGi ?? ''} min={1} max={65536}
            onChange={(v) => setDiskGi(v === undefined || v === '' ? null : Number(v))} />
        </Group>
        <Text size="sm" c="dimmed">
          Пустое поле = не менять (сервер merge'ит с текущей декларацией).
          Применяется пересозданием контейнера инстанса — данные на volume нетронуты.
        </Text>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>Применить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
