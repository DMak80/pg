// Кнопка «Перебалансировать» kafka-кластера (t02, arch/02 §10.2-9/10):
// заявка /kafkaworker/rebalances/<C>; воркер переносит реплики батчами;
// при живой заявке превращается в «Отменить ребалансировку» (поданные
// батчи Kafka доиграет сама).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import type { KafkaRebalanceTicketDto } from '../../api/dto';
import { cancelKafkaRebalance, requestKafkaRebalance } from '../../api/queries';

export function RebalanceButton({
  cluster,
  rebalance,
  disabled,
}: {
  cluster: string;
  rebalance: KafkaRebalanceTicketDto | null;
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);

  const request = useMutation({
    mutationFn: () => requestKafkaRebalance(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка отправлена',
        message: 'Ребалансировку выполнит воркер (перенос реплик батчами, без даунтайма).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const cancel = useMutation({
    mutationFn: () => cancelKafkaRebalance(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'blue',
        title: 'Заявка отменена',
        message: 'Новые батчи не подаются; уже поданные Kafka доиграет сама.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  // Живая заявка: единственное действие — отмена (повторная заявка — 409).
  if (rebalance !== null) {
    const cancelError = cancel.error instanceof ApiError ? cancel.error : null;
    return (
      <>
        <Button
          variant="light"
          color="orange"
          disabled={disabled}
          onClick={() => setOpened(true)}
        >
          Отменить ребалансировку
        </Button>
        <Modal opened={opened} onClose={() => setOpened(false)}
          title={`Отменить ребалансировку — ${cluster}`} centered>
          <Stack gap="sm">
            <Text>
              Заявка ребалансировки (от {new Date(rebalance.requestedUnix * 1000).toLocaleString()}
              {rebalance.requestedBy !== null ? `, ${rebalance.requestedBy}` : ''}) будет снята.
            </Text>
            <Alert color="yellow" variant="light" title="Внимание">
              Поданные батчи Kafka доиграет сама — уже идущие переносы реплик завершатся.
            </Alert>
            {cancelError ? <Alert color="red" variant="light">{cancelError.message}</Alert> : null}
            <Group justify="flex-end">
              <Button variant="default" onClick={() => setOpened(false)}>Закрыть</Button>
              <Button color="orange" loading={cancel.isPending} onClick={() => cancel.mutate()}>
                Отменить заявку
              </Button>
            </Group>
          </Stack>
        </Modal>
      </>
    );
  }

  const serverError = request.error instanceof ApiError ? request.error : null;

  return (
    <>
      <Button
        variant="light"
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        Перебалансировать
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Перебалансировать — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер пересчитает целевое размещение реплик по всем партициям
            (RF = min(config, брокеров), равномерный добор) и сойдётся к нему
            батчами через kafka-reassign-partitions.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            Перенос данных между брокерами: длительность зависит от объёма;
            доступность сохраняется (reassignment без даунтайма).
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={request.isPending} onClick={() => request.mutate()}>
              Отправить
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
