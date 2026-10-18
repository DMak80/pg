// Кнопка «Ротация CA» kafka-кластера (мутация №17, t07, arch/02 §10.2-17):
// заявка /kafkaworker/ca_rotations/<C>; исполняет CaRotator воркера окном
// двойного доверия (P/D/R/C, arch/16 §5 K) — клиенты, перечитавшие ca_pem
// из etcd, работают непрерывно; кэш старого CA после коммита отваливается.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateKafkaCa } from '../../api/queries';

export function RotateCaButton({
  cluster,
  disabled,
}: {
  cluster: string;
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateKafkaCa(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: 'The CA/cert rotation will be performed by the worker (phases P/D/R/C: staging, '
          + 'dual-trust window, rolling broker recreation, commit).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button
        variant="light"
        color="grape"
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        CA rotation
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`CA rotation — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will rotate the per-cluster CA and broker server certs without
            stopping writes: (P) staging of the new CA, (D) dual-trust window —
            <b> ca_pem</b> = OLD+NEW bundle, (R) rolling broker recreation with certs
            from the new CA, (C) atomic commit.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            Rolling broker recreation — perform this in a quiet window.
            <List size="sm" mt={4}>
              <List.Item>applications reading ca_pem from etcd run continuously</List.Item>
              <List.Item>after the commit, clients with the cached old CA fail —
                re-read ca_pem</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Submit ticket
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
