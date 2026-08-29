// Точка входа SPA: провайдеры Mantine (тёмная тема) → Notifications → QueryClient → Polling → Router (spec §7.1).
import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { MantineProvider } from '@mantine/core';
import { Notifications } from '@mantine/notifications';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from 'react-router';
import { ApiError } from './api/client';
import { router } from './App';
import { PollingProvider } from './polling/PollingContext';

// Defaults (spec §3.10): 401 не ретраим (guard-реакция сразу), фокус окна не рефечит — только polling.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error) =>
        !(error instanceof ApiError && error.status === 401) && failureCount < 2,
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MantineProvider defaultColorScheme="dark">
      <Notifications />
      <QueryClientProvider client={queryClient}>
        <PollingProvider>
          <RouterProvider router={router} />
        </PollingProvider>
      </QueryClientProvider>
    </MantineProvider>
  </StrictMode>,
);
