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
        title: 'Заявка отправлена',
        message: 'Ротацию CA/сертов выполнит воркер (фазы P/D/R/C: staging, '
          + 'окно двойного доверия, пересоздание ноды, коммит).',
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
        Ротация CA
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Ротация CA — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер выполнит ротацию per-cluster CA и серверного серта ноды
            без остановки обслуживания: (P) staging новой CA, (D) окно двойного
            доверия — <b>ca_pem</b> = bundle OLD+NEW, (R) пересоздание ноды с
            сертом от новой CA, (C) атомарный коммит.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            Нода будет пересоздана — кеш начнётся с холодного старта
            (persistence off), окно — секунды.
            <List size="sm" mt={4}>
              <List.Item>приложения, читающие ca_pem из etcd, работают непрерывно</List.Item>
              <List.Item>после коммита клиенты с закешированным старым CA перечитывают ca_pem</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Отправить заявку
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
