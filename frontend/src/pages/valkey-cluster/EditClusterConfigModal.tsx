// Форма «Изменить конфиг» valkey-кластера (arch/03 §8.3.2; 02 §11.3):
// maxmemory (MiB → байты в API) + policy; применяется converger'ом воркера
// CONFIG SET-механикой без рестарта ноды. Предупреждение R3 при mem-лимите.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Select, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { updateValkeyConfig, valkeyQueryKeys } from '../../api/queries';
import type { ValkeyClusterDto } from '../../api/dto';

const KNOWN_POLICIES = [
  'allkeys-lru',
  'allkeys-lfu',
  'volatile-lru',
  'volatile-lfu',
  'allkeys-random',
  'volatile-random',
  'volatile-ttl',
  'noeviction',
];
const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

export function EditClusterConfigModal({ cluster }: { cluster: ValkeyClusterDto }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const [maxmemoryMib, setMaxmemoryMib] = useState(Math.max(1, Math.round(cluster.maxmemoryBytes / MIB)));
  const [policy, setPolicy] = useState(cluster.maxmemoryPolicy);
  const [formError, setFormError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () =>
      updateValkeyConfig(cluster.name, { maxmemoryBytes: maxmemoryMib * MIB, maxmemoryPolicy: policy }),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.cluster(cluster.name) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  function validate(): string | null {
    if (!Number.isInteger(maxmemoryMib) || maxmemoryMib < 1) return 'maxmemory: integer ≥ 1 MiB';
    if (!KNOWN_POLICIES.includes(policy)) return 'maxmemoryPolicy: choose from the list';
    return null;
  }

  function submit() {
    const error = validate();
    setFormError(error);
    if (error !== null) return;
    mutation.mutate();
  }

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Edit config</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Config — ${cluster.name}`} centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Applied by the worker (CONFIG + ACL converge) without a node restart.
          </Text>
          <Group grow>
            <NumberInput label="maxmemory, MiB" value={maxmemoryMib} min={1}
              onChange={(v) => setMaxmemoryMib(Number(v ?? 0))} />
            <Select label="Eviction policy" data={KNOWN_POLICIES} value={policy}
              onChange={(v) => setPolicy(v ?? 'allkeys-lru')} />
          </Group>
          {/* Инвариант R3: maxmemory обязан оставаться меньше mem-лимита ноды. */}
          {cluster.nodesList[0]?.memGi !== null && cluster.nodesList[0]?.memGi !== undefined
            && maxmemoryMib * MIB >= cluster.nodesList[0].memGi! * GIB ? (
            <Alert color="yellow" variant="light">
              maxmemory ≥ node mem limit ({cluster.nodesList[0].memGi} GiB) — the worker will reject it:
              container OOM kill (R3).
            </Alert>
          ) : null}
          {formError !== null ? <Alert color="red" variant="light">{formError}</Alert> : null}
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={mutation.isPending} onClick={submit}>Save</Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
