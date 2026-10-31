// Общая кнопка ротации секретов (u02): кнопка + confirm-модал с текстом
// предупреждения; при живой заявке (liveTicketText) — бейдж «ротация…» и
// опциональная кнопка «Отменить» (u17: onCancel; 409/404 — текстом
// ProblemDetails через ApiError). Заменяет 6 доменных копий Rotate*-кнопок.
import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { Badge, Button, Group, List, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ConfirmModal } from './ConfirmModal';

export function RotateSecretButton({
  title,
  warningText,
  liveTicketText = null,
  confirmText = 'Сменить пароль',
  onRotate,
  onCancel,
  invalidateKeys = [],
  disabled = false,
}: {
  title: string;
  // Предупреждение о последствиях (разрыв подключений/rolling-перезапуск).
  warningText: string;
  // Живая заявка: бейдж «ротация…» + (при onCancel) кнопка «Отменить» (u17).
  liveTicketText?: string | null;
  confirmText?: string;
  onRotate: () => Promise<unknown>;
  onCancel?: () => Promise<unknown>;
  invalidateKeys?: readonly (readonly unknown[])[];
  disabled?: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const [cancelOpened, setCancelOpened] = useState(false);
  const [cancelError, setCancelError] = useState<unknown>(null);

  const rotate = useMutation({
    mutationFn: onRotate,
    onSuccess: () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка поставлена',
        message: 'Ротация выполняется воркером в фоне; статус — бейдж «ротация…» и журнал домена.',
        autoClose: 8000,
      });
      void invalidateAll(queryClient, invalidateKeys);
    },
  });

  // u17: отмена живой заявки — DELETE воркеру; 409 «начата»/404 «нет заявки»
  // — текстом ProblemDetails (ApiError.message).
  const cancel = useMutation({
    mutationFn: onCancel ?? (() => Promise.resolve(null)),
    onSuccess: () => {
      setCancelOpened(false);
      setCancelError(null);
      notifications.show({
        color: 'green',
        title: 'Заявка отменена',
        message: 'Не начатая заявка снята (начатая ротация доедет — отменить её нельзя).',
        autoClose: 8000,
      });
      void invalidateAll(queryClient, invalidateKeys);
    },
    onError: (e: unknown) => {
      // Модал остаётся открытым — оператор читает текст 409/404.
      setCancelError(e);
    },
  });

  const hasCancel = onCancel !== undefined && liveTicketText !== null;

  return (
    <Group gap="xs" wrap="nowrap">
      {liveTicketText !== null ? (
        <Badge color="blue" variant="light">{liveTicketText}</Badge>
      ) : null}
      <Button variant="light" disabled={disabled} onClick={() => setOpened(true)}>
        {title}
      </Button>
      {hasCancel ? (
        <Button
          color="orange"
          variant="light"
          onClick={() => {
            setCancelError(null);
            setCancelOpened(true);
          }}
        >
          Отменить
        </Button>
      ) : null}

      <ConfirmModal
        opened={opened}
        onClose={() => setOpened(false)}
        title={title}
        text={
          <>
            <Text>{warningText}</Text>
            <List size="sm" mt={4}>
              <List.Item>заявка ставится в очередь и исполняется воркером в фоне</List.Item>
              <List.Item>если сущность недоступна — воркер повторяет автоматически</List.Item>
            </List>
          </>
        }
        confirmLabel={confirmText}
        busy={rotate.isPending}
        error={rotate.error}
        onConfirm={() => void rotate.mutate()}
      />

      <ConfirmModal
        opened={cancelOpened}
        onClose={() => setCancelOpened(false)}
        title={`Отменить заявку: ${title}`}
        text={'Начатая ротация доедет; отменяется только не начатая заявка '
          + '(гвард воркера: 409 «уже исполняется»; del без тела — аудит в host-логе воркера, spec §7).'}
        confirmLabel="Отменить заявку"
        busy={cancel.isPending}
        error={cancelError}
        onConfirm={() => {
          if (onCancel !== undefined) void cancel.mutate();
        }}
      />
    </Group>
  );
}

async function invalidateAll(
  queryClient: QueryClient,
  keys: readonly (readonly unknown[])[],
): Promise<void> {
  for (const key of keys) {
    await queryClient.invalidateQueries({ queryKey: key });
  }
}
