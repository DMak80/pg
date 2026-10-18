// Кнопка «Сменить per-cluster секреты» в шапке деталей кластера: подтверждение →
// POST /api/clusters/{cluster}/secrets/rotate → заявка /pgworker/rotations/<C>
// (arch/02 §9.8, t02). Выполняет PgWorker (ALTER ROLE app/bucket_admin/
// bucket_mover на всех шардах + новые креды и dsn в etcd); после применения
// подключения со старыми паролями отвергаются, пока клиент не перечитает креды
// из etcd — предупреждение в модалке (spec О2) и success-нотификация после 201.
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateClusterSecrets } from '../../api/queries';

export function RotateClusterSecretsButton({ name }: { name: string }) {
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateClusterSecrets(name),
    onSuccess: () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: 'The per-cluster secrets rotation (app, bucket_admin, bucket_mover) will be '
          + 'performed by PgWorker (background ticks). After it is applied, clients must '
          + 're-read the credentials and dsn from etcd.',
        autoClose: 8000,
      });
    },
  });

  // Ошибка сервера: 409 «уже запрошена» / 503 etcd / прочие ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Rotate per-cluster secrets</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Rotate per-cluster secrets" centered>
        <Stack gap="sm">
          <Text>
            Cluster <b>{name}</b>: PgWorker will rotate the passwords of the <b>app</b>,{' '}
            <b>bucket_admin</b> and <b>bucket_mover</b> roles on all shards, update the
            app_password/mover_password/bucket_admin_password keys and rewrite the dsn in etcd.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            After it is applied (seconds), connections with the old passwords will start
            being rejected until clients re-read the credentials (app) and dsn
            (bucket_admin) from etcd. Perform this in a quiet window.
            <List size="sm" mt={4}>
              <List.Item>the ticket is queued and executed in the background (PgWorker ticks)</List.Item>
              <List.Item>if a shard is unavailable, the rotation retries automatically</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Rotate password
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
