// Заглушка панели Алерты — наполнение в t09 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function AlertsPage() {
  return (
    <Container>
      <Title order={2}>Алерты</Title>
      <Text c="dimmed">Панель будет реализована в t09-frontend-ha.</Text>
    </Container>
  );
}
