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
        title: 'Заявка отправлена',
        message: 'Ротацию CA/сертов выполнит воркер (фазы P/D/R/C: staging, '
          + 'окно двойного доверия, rolling-пересоздание брокеров, коммит).',
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
        Ротация CA
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Ротация CA — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер выполнит ротацию per-cluster CA и серверных сертов брокеров
            без остановки записи: (P) staging новой CA, (D) окно двойного
            доверия — <b>ca_pem</b> = bundle OLD+NEW, (R) rolling-пересоздание
            брокеров с сертами от новой CA, (C) атомарный коммит.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            Rolling-пересоздание брокеров — выполняйте в тихое окно.
            <List size="sm" mt={4}>
              <List.Item>приложения, читающие ca_pem из etcd, работают непрерывно</List.Item>
              <List.Item>после коммита клиенты с закешированным старым CA отваливаются —
                перечитайте ca_pem</List.Item>
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
