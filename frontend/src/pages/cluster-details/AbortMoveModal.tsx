// Форма «Отменить переезд» (t07, arch/03 §3.6): abort незавершённого переезда;
// чекбокс force ломает защиты свежести и routing==target; серверные 409 —
// текстом ProblemDetails в теле формы.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Checkbox, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { abortMove, queryKeys } from '../../api/queries';
import { formatUnixAge } from '../../utils/format';
import type { BucketDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucket: BucketDto;
  opened: boolean;
  onClose: () => void;
}

export function AbortMoveModal({ cluster, bucket, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [force, setForce] = useState(false);
  const mutation = useMutation({
    mutationFn: () => abortMove(cluster, { bucket: bucket.id, force: force || undefined }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Отменить переезд bucket_${bucket.id}`} centered>
      <Stack gap="sm">
        <Text>
          Маршрут: <b>{bucket.move?.owner ?? '—'} → {bucket.move?.target ?? '—'}</b>,
          фаза <b>{bucket.move?.phase ?? '—'}</b>, статус обновлён{' '}
          <b>{bucket.move?.updatedUnix != null ? formatUnixAge(bucket.move.updatedUnix) : '—'}</b>.
        </Text>
        <Alert color="yellow" variant="light" title="Внимание">
          Артефакты переезда убираются, бакет возвращается владельцу.
        </Alert>
        <Checkbox
          checked={force}
          onChange={(e) => setForce(e.currentTarget.checked)}
          label="force — ломает защиту свежести (переезд, возможно, ещё жив) и разрешает
            доведение перевода, когда flip уже прошёл (уборка старого шарда, как
            finalize); включайте только если mover точно мёртв"
        />
        {serverError !== null ? (
          <Alert color="yellow" variant="light">{serverError.detail ?? serverError.message}</Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
            Отменить переезд
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
