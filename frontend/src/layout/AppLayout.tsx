// Layout защищённой зоны: guard по сессии + AppShell (nav, header, outlet) (spec §3.11, §7.7).
// Навигация — 5 групп (u01, arch/03 §3): перегруппировка только меню, все
// существующие URL сохранены без изменений. Подписи — русские (D2);
// имена собственных систем не переводятся.
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AppShell, Box, Button, Group, Loader, NavLink, Stack, Text, Title } from '@mantine/core';
import { Link, Outlet, useLocation, useNavigate } from 'react-router';
import { fetchSession, logoutRequest, queryKeys } from '../api/queries';
import { PollingToggle } from './PollingToggle';
import { StaleBadge } from './StaleBadge';
import { AlertsNavCounters } from './AlertsNavCounters';

// Структура навигации: секции (u01) + пункты (маршрут + подпись); активность
// '/' — точное совпадение, остальные — по префиксу (t08 spec §4.2).
const NAV_SECTIONS: { section: string; items: { to: string; label: string }[] }[] = [
  {
    section: 'ОБЗОР',
    items: [{ to: '/', label: 'Дашборд' }],
  },
  {
    section: 'ДОМЕНЫ',
    items: [
      { to: '/clusters', label: 'PostgreSQL' },
      { to: '/kafka', label: 'Kafka' },
      { to: '/valkey', label: 'Valkey' },
      { to: '/s3', label: 'S3-хранилища' },
    ],
  },
  {
    section: 'ДАННЫЕ И ЗАЩИТА',
    items: [
      { to: '/backups-storage', label: 'Бэкапы' },
      { to: '/reliability', label: 'Надёжность' },
    ],
  },
  {
    section: 'НАБЛЮДЕНИЕ',
    items: [
      { to: '/alerts', label: 'Алерты' },
      { to: '/events', label: 'События' },
      { to: '/ha', label: 'Репликация' },
    ],
  },
  {
    section: 'ИНФРАСТРУКТУРА',
    items: [
      { to: '/workers', label: 'Воркеры' },
      { to: '/etcd', label: 'etcd-контур' },
    ],
  },
];

export function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();

  // Guard: session-запрос при монтировании; 401 уже редиректит apiFetch (spec §3.11).
  const session = useQuery({
    queryKey: queryKeys.session,
    queryFn: fetchSession,
    retry: false,
    staleTime: Infinity,
  });

  async function handleLogout(): Promise<void> {
    await logoutRequest();
    queryClient.clear();
    navigate('/login');
  }

  if (session.isPending)
    return (
      <Group justify="center" pt="xl">
        <Loader />
      </Group>
    );

  // Ошибка сети (не-401: 401 уже уехал редиректом) — панель недоступна, повтор.
  if (session.isError)
    return (
      <Stack align="center" pt="xl" gap="sm">
        <Text c="red">Панель недоступна</Text>
        <Button variant="light" onClick={() => void session.refetch()}>Повторить</Button>
      </Stack>
    );

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{ width: 220, breakpoint: 'sm' }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group gap="sm">
            <Text fw={700}>AdminPanel</Text>
            <StaleBadge />
          </Group>
          <Group gap="sm">
            <PollingToggle />
            <Text c="dimmed" size="sm">{session.data?.username}</Text>
            <Button size="xs" variant="light" onClick={() => void handleLogout()}>Выйти</Button>
          </Group>
        </Group>
      </AppShell.Header>
      <AppShell.Navbar p="xs">
        <Stack gap="xs">
          {NAV_SECTIONS.map((section) => (
            <Box key={section.section}>
              {/* Заголовок группы (u01) */}
              <Title order={6} c="dimmed" tt="uppercase" px="xs" pb={4}>{section.section}</Title>
              <Stack gap={2}>
                {section.items.map((item) => (
                  <NavLink
                    key={item.to}
                    label={item.label}
                    component={Link}
                    to={item.to}
                    active={item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to)}
                    rightSection={item.to === '/alerts' ? <AlertsNavCounters /> : undefined}
                  />
                ))}
              </Stack>
            </Box>
          ))}
        </Stack>
      </AppShell.Navbar>
      <AppShell.Main>
        <Outlet />
      </AppShell.Main>
    </AppShell>
  );
}
