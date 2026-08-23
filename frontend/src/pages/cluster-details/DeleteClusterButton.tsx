// Кнопка «Удалить кластер» на странице деталей: подтверждение → DELETE
// /api/clusters/{name} → кластер переходит в DELETING (arch/02 §9.4, arch/03 §3).
// Панель не удаляет ключи etcd — только помечает состояние; обратного
// перехода из DELETING нет, поэтому у удаляемого кластера кнопка не рисуется.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { deleteCluster } from '../../api/queries';

export function DeleteClusterButton({ name }: { name: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => deleteCluster(name),
    onSuccess: async () => {
      // Следующий тик refresher'а (≤3 с) подхватит DELETING; шапка перерисуется.
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
  });

  // Ошибка сервера: 503 «etcd недоступен» / прочие ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" onClick={() => setOpened(true)}>Удалить кластер</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Удалить кластер" centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{name}</b> перейдёт в состояние <b>DELETING</b>. Ключи etcd и ноды панель
            не удаляет — снятие ресурсов выполняет внешний оркестратор.
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Удалить
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
