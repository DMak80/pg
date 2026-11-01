// Общая кнопка удаления сущности (u02): красная кнопка + ConfirmModal
// (с подтверждением вводом expectedInput при заданном). Заменяет доменные
// копии DeleteClusterButton/DeleteKafkaClusterButton/DeleteValkeyClusterButton/
// DeleteS3ClusterButton — эндпоинты/тексты передаются вызывающей стороной.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from '@mantine/core';
import { useState } from 'react';
import { ConfirmModal, ActionErrorAlert } from './ConfirmModal';
import { ApiError } from '../../api/client';

export function DeleteEntityButton({
  title,
  confirmText,
  expectedInput,
  canMutate = true,
  onConfirm,
  invalidateKeys = [],
  label = 'Удалить',
}: {
  title: string;
  confirmText: string;
  // Задан → подтверждение вводом строки (delete-сироты/топика).
  expectedInput?: string;
  canMutate?: boolean;
  onConfirm: () => Promise<unknown>;
  // Ключи query-кэша для инвалидации после успеха.
  invalidateKeys?: readonly (readonly unknown[])[];
  label?: string;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: onConfirm,
    onSuccess: async () => {
      setOpened(false);
      for (const key of invalidateKeys) {
        await queryClient.invalidateQueries({ queryKey: key });
      }
    },
  });

  if (!canMutate) return null;
  const serverError = mutation.error instanceof ApiError ? mutation.error : mutation.error;

  return (
    <>
      <Button color="red" variant="light" onClick={() => setOpened(true)}>{label}</Button>
      <ConfirmModal
        opened={opened}
        onClose={() => setOpened(false)}
        title={title}
        text={confirmText}
        expectedInput={expectedInput}
        confirmLabel={label}
        busy={mutation.isPending}
        error={serverError}
        onConfirm={() => void mutation.mutate()}
      />
      {/* Ошибка вне модала тоже видна (модал может закрыт быть оператором) */}
      {opened ? null : <ActionErrorAlert error={serverError} />}
    </>
  );
}
