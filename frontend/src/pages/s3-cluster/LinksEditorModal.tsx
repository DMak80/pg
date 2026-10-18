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
        title: 'Graph submitted',
        message: 'Worker convergence will apply the links — the links value will update on the next tick.',
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
        return void setError(`row ${i + 1}: the names must exist in the cluster`);
      if (p.a === p.b)
        return void setError(`row ${i + 1}: a ≠ b`);
    }
    const seen = new Set<string>();
    for (const p of pairs) {
      const key = [p.a, p.b].sort().join('|');
      if (seen.has(key))
        return void setError('pairs must be unique');
      seen.add(key);
    }
    setError(null);
    mutation.mutate();
  }

  return (
    <Modal opened={opened} onClose={onClose}
      title={`Replication graph — ${cluster.name} (pair table)`} centered size="lg">
      <Stack gap="sm">
        {pairs.length === 0 ? (
          <Text c="dimmed">no links configured</Text>
        ) : (
          pairs.map((p, i) => (
            <Group key={i} align="flex-end" gap="xs">
              <Select
                label={`Pair ${i + 1}: a`}
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
                − row
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
            + row
          </Button>
        </Group>
        <Text size="sm" c="dimmed">
          A full mesh is not required; nodes of one cluster live on different hosts/sites
          (geo separation is the operator's responsibility). Full replacement: 202 —
          worker convergence will apply it.
        </Text>
        {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button loading={mutation.isPending} onClick={submit}>Apply graph</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
