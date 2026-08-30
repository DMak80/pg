// Модал создания топика (arch/02 §10.2-9, t01): lifecycle-заявка desired.create;
// дефолты из config кластера; валидация-зеркало §10.3 (имя-паттерн, partitions,
// RF ≤ brokers, retention, minISR ≤ RF).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text, TextInput } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createKafkaTopic } from '../../api/queries';

const DAY_MS = 86_400_000;
const TOPIC_PATTERN = /^[a-zA-Z0-9._-]{1,249}$/;

export function TopicCreateModal({
  cluster,
  defaults,
  onClose,
}: {
  cluster: string;
  defaults: { defaultPartitions: number; replicationFactor: number; brokers: number };
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [name, setName] = useState('');
  const [partitions, setPartitions] = useState<number | ''>(defaults.defaultPartitions);
  const [rf, setRf] = useState<number | ''>(defaults.replicationFactor);
  const [retentionDays, setRetentionDays] = useState<number | ''>('');
  const [minIsr, setMinIsr] = useState<number | ''>('');

  const mutation = useMutation({
    mutationFn: () => createKafkaTopic(cluster, {
      name: name.trim(),
      partitions: partitions === '' ? undefined : partitions,
      replicationFactor: rf === '' ? undefined : rf,
      retentionMs: retentionDays === '' ? undefined : Math.round(retentionDays * DAY_MS),
      minInSyncReplicas: minIsr === '' ? undefined : minIsr,
    }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  // Клиентская валидация-зеркало (§10.3): сервер — источник истины.
  const nameInvalid = !TOPIC_PATTERN.test(name.trim()) || name.trim().startsWith('__');
  const rfInvalid = rf !== '' && (rf < 1 || rf > 9 || rf > defaults.brokers);
  const partitionsInvalid = partitions !== '' && (partitions < 1 || partitions > 1000);
  const retentionInvalid = retentionDays !== '' && (retentionDays < 1 || Math.round(retentionDays * DAY_MS) > 2_147_483_647);
  const minIsrInvalid = minIsr !== '' && (minIsr < 1 || (rf !== '' && minIsr > rf));

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened onClose={onClose} title="Создать топик" centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Заявку исполняет воркер (≤ 2 тиков): CreateTopics с указанными параметрами;
          пустые retention/min ISR — брокерные дефолты кластера.
        </Text>
        <TextInput
          label="Имя"
          placeholder="orders"
          value={name}
          error={name.trim().length > 0 && nameInvalid ? 'a-zA-Z0-9._- до 249 симв, без __' : null}
          onChange={(e) => setName(e.currentTarget.value)}
        />
        <Group grow>
          <NumberInput
            label="Партиции"
            value={partitions}
            min={1}
            max={1000}
            error={partitionsInvalid ? '1..1000' : null}
            onChange={(v) => setPartitions(v === '' ? '' : Number(v))}
          />
          <NumberInput
            label={`RF (≤ ${defaults.brokers} брокеров)`}
            value={rf}
            min={1}
            max={Math.min(9, defaults.brokers)}
            error={rfInvalid ? `1..9 и ≤ ${defaults.brokers}` : null}
            onChange={(v) => setRf(v === '' ? '' : Number(v))}
          />
        </Group>
        <Group grow>
          <NumberInput
            label="Retention, дней (опц.)"
            value={retentionDays}
            min={1}
            error={retentionInvalid ? '1..2147483647 мс' : null}
            onChange={(v) => setRetentionDays(v === '' ? '' : Number(v))}
          />
          <NumberInput
            label="Min ISR (опц.)"
            value={minIsr}
            min={1}
            error={minIsrInvalid ? `1..RF (${rf === '' ? '?' : rf})` : null}
            onChange={(v) => setMinIsr(v === '' ? '' : Number(v))}
          />
        </Group>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button
            loading={mutation.isPending}
            disabled={name.trim().length === 0 || nameInvalid || rfInvalid
              || partitionsInvalid || retentionInvalid || minIsrInvalid}
            onClick={() => mutation.mutate()}
          >
            Создать топик
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
