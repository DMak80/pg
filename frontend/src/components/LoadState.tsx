// Общие состояния страниц: загрузка и ошибка с повтором (t08 spec §4.15, §4.18).
import { Alert, Button, Center, Loader, Stack } from '@mantine/core';
import type { ReactNode } from 'react';
import { ApiError } from '../api/client';

// Первый загрузочный рендер запроса: центрированный спиннер.
export function LoadingSection() {
  return (
    <Center mih={160}>
      <Loader />
    </Center>
  );
}

// Ошибка запроса без данных (t08 spec §4.15): 503 — снапшот не собран; 404 — notFound-контент
// (передаёт страница, например «Кластер не найден» со ссылкой назад); прочее — текст ApiError.
export function ErrorSection({ error, onRetry, notFound }: {
  error: unknown;
  onRetry: () => void;
  notFound?: ReactNode;
}) {
  if (error instanceof ApiError && error.status === 404 && notFound !== undefined)
    return <>{notFound}</>;
  const message = error instanceof ApiError && error.status === 503
    ? 'Данные ещё не собраны (etcd-снапшот пуст)'
    : error instanceof Error
      ? error.message
      : 'Неизвестная ошибка';
  return (
    <Stack gap="sm" align="flex-start">
      <Alert color="red">{message}</Alert>
      <Button variant="light" size="xs" onClick={onRetry}>Повторить</Button>
    </Stack>
  );
}
