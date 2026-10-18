// Переключатель strict-режима в шапке деталей кластера (t06, arch/03 §3):
// мутация только для Active (не-Active — контрол заблокирован с подсказкой);
// подтверждение — модальный диалог с предупреждением о последствиях
// (включение: запись блокируется при потере sync-standby; выключение:
// возможна потеря «хвоста» при failover). После 204 — инвалидация деталей
// (значение опции обновит снапшот; применение к Patroni — конвергенция DCS
// воркера, 02 §9.10).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Group, Modal, Stack, Switch, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { queryKeys, updateClusterConfig } from '../../api/queries';

export function StrictModeSwitch({ cluster, state, strict }: {
  cluster: string;
  state: string;
  strict: boolean;
}) {
  const queryClient = useQueryClient();
  const [pending, setPending] = useState<boolean | null>(null); // значение из диалога подтверждения
  const active = state === 'ACTIVE';

  const mutation = useMutation({
    mutationFn: (value: boolean) => updateClusterConfig(cluster, { synchronousModeStrict: value }),
    onSuccess: async () => {
      setPending(null);
      notifications.show({
        color: 'green',
        title: 'Config updated',
        message: 'The value is written to etcd; the worker DCS convergence will apply it to the live '
          + 'Patroni (supervision ticks, no restarts). Monitoring — the HA page /service/<scope>/config.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
    onError: () => setPending(null),
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Group gap={6}>
        <Text size="sm" span c="dimmed">mode:</Text>
        {/* Бейдж режима (arch/03 §3): strict — красный, availability — teal */}
        {strict
          ? <Badge color="red" variant="light">strict</Badge>
          : <Badge color="teal" variant="light">availability</Badge>}
        <Switch
          size="md"
          onLabel="strict"
          offLabel="avail."
          checked={strict}
          disabled={!active || mutation.isPending}
          onChange={(e) => setPending(e.currentTarget.checked)}
        />
        {!active ? (
          <Text size="sm" c="dimmed">available after initialization</Text>
        ) : null}
      </Group>
      <Modal opened={pending !== null} onClose={() => setPending(null)}
        title="Switch strict mode?" centered>
        <Stack gap="sm">
          {pending === true ? (
            <Alert color="yellow" variant="light" title="Enabling strict (durability)">
              Writes will be blocked if the sync replica is lost: Patroni keeps a
              synchronous standby — no commit is acknowledged without the replica.
              Requires at least 2 replicas on every shard.
            </Alert>
          ) : (
            <Alert color="yellow" variant="light" title="Disabling strict (availability)">
              On failover, unacknowledged transactions (the "tail") may be lost:
              the master will keep accepting writes asynchronously without the sync replica.
            </Alert>
          )}
          {serverError ? (
            <Alert color="red" variant="light">
              {serverError.status === 400
                ? (serverError.detail ?? 'Validation failed')
                : serverError.status === 409
                  ? (serverError.detail ?? 'Cluster is not Active')
                  : (serverError.detail ?? 'Retry later')}
            </Alert>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setPending(null)}>Cancel</Button>
            <Button loading={mutation.isPending}
              onClick={() => pending !== null && mutation.mutate(pending)}>
              Switch
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
