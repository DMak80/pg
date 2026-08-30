// Модал конфиг-заявки топика (arch/02 §10.2-7, план C4): partitions↑/
// retention/minISR; валидация-зеркало сервера (хотя бы одно поле, partitions
// строго больше фактических — уменьшение Kafka не поддерживает).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useEffect, useState } from 'react';
import { ApiError } from '../../api/client';
import { upsertTopicDesired } from '../../api/queries';
import type { KafkaTopicDto } from '../../api/dto';

const DAY_MS = 86_400_000;

export function TopicDesiredModal({
  cluster,
  topic,
  onClose,
}: {
  cluster: string;
  topic: KafkaTopicDto;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [partitions, setPartitions] = useState<number | ''>('');
  const [retentionDays, setRetentionDays] = useState<number | ''>(
    topic.retentionMs !== null ? topic.retentionMs / DAY_MS : '',
  );
  const [minIsr, setMinIsr] = useState<number | ''>(
    topic.minInSyncReplicas !== null ? topic.minInSyncReplicas : '',
  );

  useEffect(() => {
    setPartitions('');
    setRetentionDays(topic.retentionMs !== null ? topic.retentionMs / DAY_MS : '');
    setMinIsr(topic.minInSyncReplicas !== null ? topic.minInSyncReplicas : '');
  }, [topic]);

  const mutation = useMutation({
    mutationFn: () => upsertTopicDesired(cluster, topic.name, {
      partitions: partitions === '' ? undefined : partitions,
      retentionMs: retentionDays === '' ? undefined : Math.round(retentionDays * DAY_MS),
      minInSyncReplicas: minIsr === '' ? undefined : minIsr,
    }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  // Клиентская валидация-зеркало (§10.3): хотя бы одно поле; partitions↑.
  const partitionsInvalid = partitions !== '' && partitions <= topic.partitions;
  const empty =
    partitions === '' && retentionDays === '' && minIsr === '';

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened onClose={onClose} title={`Заявка конфигов — ${topic.name}`} centered>
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Заявку применяет автосинк воркера: сначала конфиги, затем увеличение
          партиций. Фактический RF не меняется (reassignment — roadmap).
        </Text>
        <Group grow>
          <NumberInput
            label={`Партиций (факт ${topic.partitions}, только ↑)`}
            value={partitions}
            min={topic.partitions + 1}
            max={1000}
            error={partitionsInvalid ? 'только увеличение' : null}
            onChange={(v) => setPartitions(v === '' ? '' : Number(v))}
          />
          <NumberInput
            label="Retention, дней"
            value={retentionDays}
            min={1}
            onChange={(v) => setRetentionDays(v === '' ? '' : Number(v))}
          />
        </Group>
        <NumberInput
          label="Min ISR"
          value={minIsr}
          min={1}
          onChange={(v) => setMinIsr(v === '' ? '' : Number(v))}
        />
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button
            loading={mutation.isPending}
            disabled={empty || partitionsInvalid}
            onClick={() => mutation.mutate()}
          >
            Поставить заявку
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
