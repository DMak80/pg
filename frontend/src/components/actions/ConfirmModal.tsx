// Общий confirm-модал (u02, arch/03 §3.4-таблица): опциональное подтверждение
// вводом ожидаемой строки (кнопка заблокирована до совпадения) — образец
// delete-сироты/restore. Все домены используют его вместо своих копий.
import { Alert, Button, Group, Modal, Stack, Text, TextInput } from '@mantine/core';
import { useEffect, useState } from 'react';
import { ApiError } from '../../api/client';

// Ошибка сервера (ProblemDetails текстом) — общая для всех действий.
export function ActionErrorAlert({ error }: { error: unknown }) {
  if (error === null || error === undefined) return null;
  const message = error instanceof ApiError ? error.message : String(error);
  return <Alert color="red" variant="light">{message}</Alert>;
}

export function ConfirmModal({
  opened,
  onClose,
  title,
  text,
  expectedInput,
  confirmLabel,
  busy,
  error,
  onConfirm,
}: {
  opened: boolean;
  onClose: () => void;
  title: string;
  text: React.ReactNode;
  // Задан → поле ввода, подтверждение разблокируется совпадением.
  expectedInput?: string;
  confirmLabel: string;
  busy: boolean;
  error?: unknown;
  onConfirm: () => void;
}) {
  const [typed, setTyped] = useState('');
  // FINDING 4: сброс ввода при закрытии — повторное открытие не наследует
  // набранный текст (как RestoreBlock).
  useEffect(() => {
    if (!opened) setTyped('');
  }, [opened]);
  const confirmed = expectedInput === undefined || typed === expectedInput;
  return (
    <Modal opened={opened} onClose={onClose} title={title} centered>
      <Stack gap="sm">
        <Text>{text}</Text>
        {expectedInput !== undefined ? (
          <TextInput
            label="Подтвердите вводом"
            placeholder={expectedInput}
            value={typed}
            onChange={(e) => setTyped(e.currentTarget.value)}
            data-autofocus
          />
        ) : null}
        <ActionErrorAlert error={error} />
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button
            color={expectedInput !== undefined ? 'red' : undefined}
            disabled={!confirmed}
            loading={busy}
            onClick={() => {
              onConfirm();
              setTyped('');
            }}
          >
            {confirmLabel}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
