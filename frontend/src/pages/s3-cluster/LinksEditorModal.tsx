// Модал-редактор графа репликации «таблица пар» (arch/03 §12.3; 02 §12.2-6):
// строки a/b — селекты имён нод, «+ строка»/«−»; PUT links целиком (202 —
// применит доводка воркера). Валидация: имена существуют, a≠b, пары уникальны.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Select, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { putS3Links, s3QueryKeys } from '../../api/queries';
import type { S3ClusterDto } from '../../api/dto';

export function LinksEditorModal({
  cluster,
  opened,
  onClose,
}: {
  cluster: S3ClusterDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const names = cluster.nodesList.map((n) => n.name);
  const [pairs, setPairs] = useState<{ a: string; b: string }[]>(
    cluster.links.map((l) => ({ a: l.a, b: l.b })));
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () => putS3Links(cluster.name, { links: pairs }),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Граф заявлен',
        message: 'Доводка воркера применит связи — значение links обновится следующим тиком.',
        autoClose: 6000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster.name) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  function submit() {
    // Валидация-зеркало 02 §12.3: имена существуют, a≠b, пары уникальны.
    for (const [i, p] of pairs.entries()) {
      if (!names.includes(p.a) || !names.includes(p.b))
        return void setError(`строка ${i + 1}: имена должны существовать у кластера`);
      if (p.a === p.b)
        return void setError(`строка ${i + 1}: a ≠ b`);
    }
    const seen = new Set<string>();
    for (const p of pairs) {
      const key = [p.a, p.b].sort().join('|');
      if (seen.has(key))
        return void setError('пары должны быть уникальны');
      seen.add(key);
    }
    setError(null);
    mutation.mutate();
  }

  return (
    <Modal opened={opened} onClose={onClose}
      title={`Граф репликации — ${cluster.name} (таблица пар)`} centered size="lg">
      <Stack gap="sm">
        {pairs.length === 0 ? (
          <Text c="dimmed">линков не настроено</Text>
        ) : (
          pairs.map((p, i) => (
            <Group key={i} align="flex-end" gap="xs">
              <Select
                label={`Пара ${i + 1}: a`}
                data={names}
                value={p.a}
                onChange={(v) => setPairs(pairs.map((x, j) => (j === i ? { ...x, a: v ?? '' } : x)))}
                maw={200}
              />
              <Text pb={6}>→</Text>
              <Select
                label="b"
                data={names}
                value={p.b}
                onChange={(v) => setPairs(pairs.map((x, j) => (j === i ? { ...x, b: v ?? '' } : x)))}
                maw={200}
              />
              <Button
                variant="default"
                size="xs"
                mb={4}
                onClick={() => setPairs(pairs.filter((_, j) => j !== i))}
              >
                − строка
              </Button>
            </Group>
          ))
        )}
        <Group>
          <Button
            size="xs"
            variant="default"
            disabled={names.length < 2}
            onClick={() => setPairs([...pairs, { a: names[0] ?? '', b: names[1] ?? '' }])}
          >
            + строка
          </Button>
        </Group>
        <Text size="sm" c="dimmed">
          Полный меш не обязателен; ноды одного кластера — на разных хостах/площадках
          (гео-раснос — обязанность оператора). Замена целиком: 202 — применит доводка воркера.
        </Text>
        {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Применить граф</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
