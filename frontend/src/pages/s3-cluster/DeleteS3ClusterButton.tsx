// Красная кнопка «Удалить кластер» S3 (arch/03 §12.3; 02 §12.2-2): DELETE
// проксируется панелью в API S3Worker (202 — TO_REMOVE, демонтаж асинхронный;
// 204 — уже в удалении). Confirm — про уничтожение данных бэкапов.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useNavigate } from 'react-router';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { deleteS3Cluster, s3QueryKeys } from '../../api/queries';

export function DeleteS3ClusterButton({ cluster }: { cluster: string }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => deleteS3Cluster(cluster),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.clusters });
      navigate('/s3');
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" onClick={() => setOpened(true)}>Удалить кластер</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Удалить s3-кластер ${cluster}`} centered>
        <Stack gap="sm">
          <Alert color="red" variant="light" title="Данные бэкапов будут уничтожены">
            Снос кластера уничтожает данные бэкапов на дисках всех его инстансов —
            это осознанное действие; восстановление невозможно.
          </Alert>
          <Text>
            Кластер <b>{cluster}</b> уйдёт в TO_REMOVE (обратного перехода нет).
            Демонтаж асинхронный: S3Worker снимет контейнеры инстансов, их тома
            и весь префикс /s3/clusters/{cluster}/ (включая заявку ротации).
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
