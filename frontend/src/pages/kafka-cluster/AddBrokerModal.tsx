// Форма «Добавить брокера» (arch/02 §10.2-4): только ресурсы — имя генерирует
// сервер (broker<max+1>); новый брокер — broker-only, кворум не меняется.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { addKafkaBroker } from '../../api/queries';
import type { AddKafkaBrokerRequestDto } from '../../api/dto';

export function AddBrokerModal({
  cluster,
  opened,
  onClose,
}: {
  cluster: string;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [cpu, setCpu] = useState(2);
  const [memGi, setMemGi] = useState(2);
  const [diskGi, setDiskGi] = useState(20);

  const mutation = useMutation({
    mutationFn: (request: AddKafkaBrokerRequestDto) => addKafkaBroker(cluster, request),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  function submit() {
    mutation.mutate({ cpu, memGi, diskGi });
  }

  return (
    <Modal opened={opened} onClose={onClose} title={`Добавить брокера — ${cluster}`} centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Имя сгенерирует сервер (broker&lt;max+1&gt;). Новый брокер подключается как
          broker-only — кворум KRaft не меняется.
        </Text>
        <Group grow>
          <NumberInput label="CPU" value={cpu} step={0.5} min={0.01} max={64}
            onChange={(v) => setCpu(Number(v ?? 0))} />
          <NumberInput label="Память, GiB" value={memGi} min={1}
            onChange={(v) => setMemGi(Number(v ?? 0))} />
          <NumberInput label="Диск, GiB" value={diskGi} min={1}
            onChange={(v) => setDiskGi(Number(v ?? 0))} />
        </Group>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Добавить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
