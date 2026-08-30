// Вкладка Топики деталей kafka-кластера (arch/03 §7.3, t01): факт из etcd
// (автосинк воркера) + desired-бейдж с возрастом + missing-подсветка + бейджи
// lifecycle-заявок (создание/удаление) с отменой; «Создать топик» и красное
// «Удалить топик» (подтверждение с вводом имени).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { cancelTopicDesired, cancelTopicLifecycle } from '../../api/queries';
import type { KafkaTopicDto } from '../../api/dto';
import { DeleteTopicModal } from './DeleteTopicModal';
import { TopicCreateModal } from './TopicCreateModal';
import { TopicDesiredModal } from './TopicDesiredModal';

export function TopicsTab({
  cluster,
  topics,
  canMutate,
  defaults,
}: {
  cluster: string;
  topics: KafkaTopicDto[];
  canMutate: boolean;
  defaults: { defaultPartitions: number; replicationFactor: number; brokers: number };
}) {
  const [desiredTopic, setDesiredTopic] = useState<KafkaTopicDto | null>(null);
  const [createOpened, setCreateOpened] = useState(false);
  const [deleteTopic, setDeleteTopic] = useState<string | null>(null);

  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="sm">
        <Title order={4}>Топики</Title>
        {canMutate ? (
          <Button size="compact-sm" onClick={() => setCreateOpened(true)}>+ Создать топик</Button>
        ) : null}
      </Group>
      <Text size="sm" c="dimmed" mb="sm">
        Создание/удаление топиков — заявками панели; внешние изменения
        (CLI/клиенты) подхватываются автосинком.
      </Text>
      {topics.length === 0 ? (
        <Text c="dimmed">Топиков в реестре нет</Text>
      ) : (
        <Table.ScrollContainer minWidth={900}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Топик</Table.Th>
                <Table.Th>Партиции</Table.Th>
                <Table.Th>RF</Table.Th>
                <Table.Th>Retention</Table.Th>
                <Table.Th>Min ISR</Table.Th>
                <Table.Th>USR</Table.Th>
                <Table.Th>Заявка</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {topics.map((t) => (
                <TopicRow
                  key={t.name}
                  cluster={cluster}
                  topic={t}
                  canMutate={canMutate}
                  onDesired={() => setDesiredTopic(t)}
                  onDelete={() => setDeleteTopic(t.name)}
                />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
      {desiredTopic !== null ? (
        <TopicDesiredModal cluster={cluster} topic={desiredTopic} onClose={() => setDesiredTopic(null)} />
      ) : null}
      {createOpened ? (
        <TopicCreateModal cluster={cluster} defaults={defaults} onClose={() => setCreateOpened(false)} />
      ) : null}
      {deleteTopic !== null ? (
        <DeleteTopicModal cluster={cluster} topic={deleteTopic} onClose={() => setDeleteTopic(null)} />
      ) : null}
    </Card>
  );
}

function TopicRow({
  cluster,
  topic,
  canMutate,
  onDesired,
  onDelete,
}: {
  cluster: string;
  topic: KafkaTopicDto;
  canMutate: boolean;
  onDesired: () => void;
  onDelete: () => void;
}) {
  const queryClient = useQueryClient();
  const [error, setError] = useState<string | null>(null);
  const cancel = useMutation({
    mutationFn: () => cancelTopicDesired(cluster, topic.name),
    onSuccess: async () => {
      setError(null);
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : 'ошибка отмены'),
  });
  const cancelLifecycle = useMutation({
    mutationFn: () => cancelTopicLifecycle(cluster, topic.name, topic.lifecycle!.op),
    onSuccess: async () => {
      setError(null);
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : 'ошибка отмены заявки'),
  });

  // Виртуальная строка create-заявки: факта нет — прочерки в факт-полях.
  const virtual = topic.partitions === 0 && topic.lifecycle !== null;

  return (
    <Table.Tr>
      <Table.Td>{topic.name}</Table.Td>
      <Table.Td>{virtual ? '—' : String(topic.partitions)}</Table.Td>
      <Table.Td>{topic.replicationFactor !== null ? String(topic.replicationFactor) : '—'}</Table.Td>
      <Table.Td>{formatRetention(topic.retentionMs)}</Table.Td>
      <Table.Td>{topic.minInSyncReplicas !== null ? String(topic.minInSyncReplicas) : '—'}</Table.Td>
      <Table.Td>
        {topic.underReplicatedPartitions === null ? (
          <Text c="dimmed">—</Text>
        ) : topic.underReplicatedPartitions > 0 ? (
          <Tooltip label="партиции с ISR < RF (данные live-пробы)">
            <Badge color="yellow" variant="light">USR: {topic.underReplicatedPartitions}</Badge>
          </Tooltip>
        ) : (
          <Badge color="green" variant="light" size="sm">ok</Badge>
        )}
      </Table.Td>
      <Table.Td>
        {topic.lifecycle !== null ? (
          <Group gap="xs" wrap="nowrap">
            <Tooltip label={`${formatAge(topic.lifecycle.requestedUnix)} · автор: ${topic.lifecycle.requestedBy ?? '—'}`}>
              {topic.lifecycle.op === 'create' ? (
                <Badge color="blue" variant="light">
                  создание{topic.lifecycle.partitions !== null ? `: ${topic.lifecycle.partitions} партиций` : ''}
                  {topic.lifecycle.replicationFactor !== null ? `, RF ${topic.lifecycle.replicationFactor}` : ''}
                </Badge>
              ) : (
                <Badge color="red" variant="light">удаление — ожидает тика воркера</Badge>
              )}
            </Tooltip>
            {canMutate ? (
              <Button
                size="compact-xs"
                variant="light"
                color="orange"
                loading={cancelLifecycle.isPending}
                onClick={() => cancelLifecycle.mutate()}
              >
                Отменить заявку
              </Button>
            ) : null}
          </Group>
        ) : topic.missing ? (
          <Tooltip label="топик отсутствует в Kafka при живой заявке; отмените заявку, чтобы убрать ключ, или пересоздайте топик заявкой создания">
            <Badge color="red" variant="light">missing: топик отсутствует</Badge>
          </Tooltip>
        ) : topic.desired !== null ? (
          <Tooltip label={`заявка ${formatAge(topic.desired.requestedUnix)} · автор: ${topic.desired.requestedBy ?? '—'}`}>
            <Badge color="blue" variant="light">
              заявка{topic.desired.partitions !== null ? `: партиций ${topic.desired.partitions}` : ''}
              {topic.desired.retentionMs !== null ? `, retention ${formatRetention(topic.desired.retentionMs)}` : ''}
            </Badge>
          </Tooltip>
        ) : (
          <Text c="dimmed">—</Text>
        )}
      </Table.Td>
      <Table.Td>
        <Group gap="xs" wrap="nowrap">
          {canMutate && !topic.missing && !virtual ? (
            <Button size="compact-xs" variant="light" onClick={onDesired}>Изменить конфиги</Button>
          ) : null}
          {canMutate && topic.desired !== null ? (
            <Button
              size="compact-xs"
              variant="light"
              color="orange"
              loading={cancel.isPending}
              onClick={() => cancel.mutate()}
            >
              Отменить заявку
            </Button>
          ) : null}
          {canMutate && !topic.missing && topic.lifecycle?.op !== 'create' ? (
            <Button size="compact-xs" variant="light" color="red" onClick={onDelete}>
              Удалить топик
            </Button>
          ) : null}
        </Group>
        {error !== null ? (
          <Text size="xs" c="red">{error}</Text>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}

const DAY_MS = 86_400_000;

function formatRetention(ms: number | null): string {
  if (ms === null)
    return '—';
  const days = ms / DAY_MS;
  return Number.isInteger(days) ? `${days} д` : `${ms} мс`;
}

function formatAge(unix: number | null): string {
  if (unix === null)
    return '';
  const minutes = Math.max(0, Math.round((Date.now() / 1000 - unix) / 60));
  return minutes < 60 ? `${minutes} мин назад` : `${Math.round(minutes / 60)} ч назад`;
}
