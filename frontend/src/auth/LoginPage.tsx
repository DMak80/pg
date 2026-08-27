// Страница логина — единственная форма ввода панели (arch/03 §3; spec §7.6).
import { useState } from 'react';
import type { FormEvent } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useNavigate, useSearchParams } from 'react-router';
import { Alert, Button, Card, Center, Container, PasswordInput, Stack, TextInput, Title } from '@mantine/core';
import { ApiError } from '../api/client';
import { loginRequest, queryKeys } from '../api/queries';

export function LoginPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // Возврат на исходную страницу после 401-редиректа (spec §3.11).
  const from = searchParams.get('from');

  async function handleSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      // При успехе username совпадает с каноническим (проверка логина точная).
      await loginRequest(username, password);
      queryClient.setQueryData(queryKeys.session, { username });
      navigate(from ?? '/', { replace: true });
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) setError('Неверный логин или пароль');
      else if (e instanceof ApiError && e.status === 429)
        setError(`Слишком много попыток, подождите ${e.retryAfterSeconds ?? 60} с`);
      else setError('Панель недоступна');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Container size="xs" pt="xl">
      <Center>
        <Card withBorder shadow="sm" padding="lg" radius="md" w="100%">
          <Title order={2} mb="md">Вход в AdminPanel</Title>
          <form onSubmit={(event) => void handleSubmit(event)}>
            <Stack>
              <TextInput
                label="Логин"
                value={username}
                onChange={(e) => setUsername(e.currentTarget.value)}
                autoComplete="username"
                required
              />
              <PasswordInput
                label="Пароль"
                value={password}
                onChange={(e) => setPassword(e.currentTarget.value)}
                autoComplete="current-password"
                required
              />
              {error !== null && <Alert color="red" variant="light">{error}</Alert>}
              <Button type="submit" loading={submitting}>Войти</Button>
            </Stack>
          </form>
        </Card>
      </Center>
    </Container>
  );
}
