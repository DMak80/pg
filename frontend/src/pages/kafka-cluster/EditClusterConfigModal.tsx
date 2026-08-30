// Форма «Изменить параметры» — default-конфиги кластера (arch/02 §10.2-3):
// применяет воркер как dynamic broker configs (converge, без рестартов).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { updateKafkaConfig } from '../../api/queries';
import type { KafkaClusterDto } from '../../api/dto';

const DAY_MS = 86_400_000;

export function EditClusterConfigModal({ cluster }: { cluster: KafkaClusterDto }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const [rf, setRf] = useState(cluster.replicationFactor);
  const [minIsr, setMinIsr] = useState(cluster.minInSyncReplicas);
  const [partitions, setPartitions] = useState(cluster.defaultPartitions);
  const [retentionDays, setRetentionDays] = useState(cluster.defaultRetentionMs / DAY_MS);

  const mutation = useMutation({
    mutationFn: () => updateKafkaConfig(cluster.name, {
      replicationFactor: rf,
      minInSyncReplicas: minIsr,
      defaultPartitions: partitions,
      defaultRetentionMs: Math.round(retentionDays * DAY_MS),
    }),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Изменить параметры</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Параметры — ${cluster.name}`} centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Изменения применяет воркер как dynamic broker configs — без рестартов брокеров.
          </Text>
          <Group grow>
            <NumberInput label="Replication factor" value={rf} min={1} max={9}
              onChange={(v) => setRf(Number(v ?? 0))} />
            <NumberInput label="Min ISR" value={minIsr} min={1}
              onChange={(v) => setMinIsr(Number(v ?? 0))} />
          </Group>
          <Group grow>
            <NumberInput label="Партиций по умолчанию" value={partitions} min={1} max={1000}
              onChange={(v) => setPartitions(Number(v ?? 0))} />
            <NumberInput label="Retention, дней" value={retentionDays} min={1}
              onChange={(v) => setRetentionDays(Number(v ?? 0))} />
          </Group>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Сохранить
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
