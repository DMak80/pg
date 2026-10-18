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
        title: 'Ticket submitted',
        message: 'The rebalance will be performed by the worker (replica moves in batches, no downtime).',
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
        title: 'Ticket cancelled',
        message: 'No new batches are submitted; Kafka will finish the submitted ones itself.',
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
          Cancel rebalance
        </Button>
        <Modal opened={opened} onClose={() => setOpened(false)}
          title={`Cancel rebalance — ${cluster}`} centered>
          <Stack gap="sm">
            <Text>
              The rebalance ticket (from {new Date(rebalance.requestedUnix * 1000).toLocaleString()}
              {rebalance.requestedBy !== null ? `, ${rebalance.requestedBy}` : ''}) will be withdrawn.
            </Text>
            <Alert color="yellow" variant="light" title="Warning">
              Kafka will finish the submitted batches itself — in-flight replica moves will complete.
            </Alert>
            {cancelError ? <Alert color="red" variant="light">{cancelError.message}</Alert> : null}
            <Group justify="flex-end">
              <Button variant="default" onClick={() => setOpened(false)}>Close</Button>
              <Button color="orange" loading={cancel.isPending} onClick={() => cancel.mutate()}>
                Withdraw ticket
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
        Rebalance
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Rebalance — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will recompute the target replica placement across all partitions
            (RF = min(config, brokers), even fill) and converge to it in batches via
            kafka-reassign-partitions.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            Data moves between brokers: duration depends on volume; availability is
            preserved (reassignment with no downtime).
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={request.isPending} onClick={() => request.mutate()}>
              Submit
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
