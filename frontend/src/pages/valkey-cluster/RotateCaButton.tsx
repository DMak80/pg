// Кнопка «Ротация CA» valkey-кластера (мутация №6, t07): заявка
// /valkeyworker/ca_rotations/<C>; исполняет CaRotator воркера окном двойного
// доверия (P/D/R/C) — клиенты, перечитавшие ca_pem из etcd, работают
// непрерывно; нода пересоздаётся с холодным стартом кеша (persistence off).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateValkeyCa } from '../../api/queries';

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
    mutationFn: () => rotateValkeyCa(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: 'The CA/cert rotation will be performed by the worker (phases P/D/R/C: staging, '
          + 'dual-trust window, node recreation, commit).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: ['valkey-clusters'] });
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
            The worker will rotate the per-cluster CA and the node server cert without
            stopping service: (P) staging of the new CA, (D) dual-trust window —
            <b> ca_pem</b> = OLD+NEW bundle, (R) node recreation with a cert from the
            new CA, (C) atomic commit.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            The node will be recreated — the cache starts cold
            (persistence off), the window is seconds.
            <List size="sm" mt={4}>
              <List.Item>applications reading ca_pem from etcd run continuously</List.Item>
              <List.Item>after the commit, clients with the cached old CA re-read ca_pem</List.Item>
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
