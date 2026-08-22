// Маршруты SPA: /login открыт; остальное — под AppLayout-guard (spec §4.1, §7.2).
import { createBrowserRouter, Navigate } from 'react-router';
import { LoginPage } from './auth/LoginPage';
import { AppLayout } from './layout/AppLayout';
import { AlertsPage } from './pages/AlertsPage';
import { ClustersPage } from './pages/ClustersPage';
import { EtcdPage } from './pages/EtcdPage';
import { HaPage } from './pages/HaPage';
import { OverviewPage } from './pages/OverviewPage';

export const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'etcd', element: <EtcdPage /> },
      { path: 'clusters', element: <ClustersPage /> },
      { path: 'ha', element: <HaPage /> },
      { path: 'alerts', element: <AlertsPage /> },
      { path: '*', element: <Navigate to="/" replace /> },
    ],
  },
]);
