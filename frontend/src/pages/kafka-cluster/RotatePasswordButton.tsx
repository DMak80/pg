// Кнопка «Сменить app-пароль» kafka-кластера (arch/02 §10.2-8): заявка
// /kafkaworker/rotations/<C>; исполняет воркер фазами A/B/C — rolling-перезапуск
// брокеров без окна недоступности; предупреждение в модалке.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateKafkaPassword } from '../../api/queries';

export function RotatePasswordButton({
  cluster,
  disabled,
}: {
  cluster: string;
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateKafkaPassword(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка отправлена',
        message: 'Ротацию выполнит воркер (фазы A/B/C, rolling-перезапуск брокеров). '
          + 'После применения клиенты должны перечитать app_password из etcd.',
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
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        Сменить app-пароль
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Сменить app-пароль — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер выполнит ротацию фазами: A) rolling-перезапуск брокеров с ДВУМЯ
            кредами (старым и новым), B) атомарная замена <b>app_password</b> в etcd,
            C) rolling-снятие старого креда.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            Rolling-перезапуск брокеров — выполняйте в тихое окно.
            <List size="sm" mt={4}>
              <List.Item>окна «брокер не принимает рабочий кред» нет по построению</List.Item>
              <List.Item>после фазы C подключения со старым паролем отвергаются</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Сменить пароль
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
