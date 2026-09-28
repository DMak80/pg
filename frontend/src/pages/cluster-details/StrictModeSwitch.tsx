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
        title: 'Config обновлён',
        message: 'Значение записано в etcd; к живому Patroni его применит конвергенция DCS воркера '
          + '(тики надзора, без рестартов). Наблюдение — HA-страница /service/<scope>/config.',
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
        <Text size="sm" span c="dimmed">режим:</Text>
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
          <Text size="sm" c="dimmed">доступно после инициализации</Text>
        ) : null}
      </Group>
      <Modal opened={pending !== null} onClose={() => setPending(null)}
        title="Переключить strict-режим?" centered>
        <Stack gap="sm">
          {pending === true ? (
            <Alert color="yellow" variant="light" title="Включение strict (durability)">
              Запись будет блокироваться при потере sync-реплики: Patroni держит
              синхронный standby — ни один коммит не подтверждается без реплики.
              Требуются реплики ≥ 2 на каждом шарде.
            </Alert>
          ) : (
            <Alert color="yellow" variant="light" title="Выключение strict (availability)">
              При failover возможна потеря неподтверждённых транзакций («хвост»):
              мастер продолжит принимать запись асинхронно без sync-реплики.
            </Alert>
          )}
          {serverError ? (
            <Alert color="red" variant="light">
              {serverError.status === 400
                ? (serverError.detail ?? 'Валидация не прошла')
                : serverError.status === 409
                  ? (serverError.detail ?? 'Кластер не Active')
                  : (serverError.detail ?? 'Повторите позже')}
            </Alert>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setPending(null)}>Отмена</Button>
            <Button loading={mutation.isPending}
              onClick={() => pending !== null && mutation.mutate(pending)}>
              Переключить
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
