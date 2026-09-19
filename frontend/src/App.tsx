// Маршруты SPA: /login открыт; остальное — под AppLayout-guard (spec §4.1, §7.2).
import { createBrowserRouter, Navigate } from 'react-router';
import { LoginPage } from './auth/LoginPage';
import { AppLayout } from './layout/AppLayout';
import { AlertsPage } from './pages/AlertsPage';
import { BackupsStoragePage } from './pages/BackupsStoragePage';
import { BackupsShardDetailsPage } from './pages/backups-storage/BackupsShardDetailsPage';
import { ClusterDetailsPage } from './pages/ClusterDetailsPage';
import { ClustersPage } from './pages/ClustersPage';
import { KafkaClusterDetailsPage } from './pages/kafka-cluster/KafkaClusterDetailsPage';
import { KafkaClustersPage } from './pages/KafkaClustersPage';
import { ValkeyClusterDetailsPage } from './pages/valkey-cluster/ValkeyClusterDetailsPage';
import { ValkeyClustersPage } from './pages/ValkeyClustersPage';
import { EtcdPage } from './pages/EtcdPage';
import { HaPage } from './pages/HaPage';
import { HaScopeDetailsPage } from './pages/HaScopeDetailsPage';
import { OverviewPage } from './pages/OverviewPage';
import { WorkersPage } from './pages/WorkersPage';

export const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'etcd', element: <EtcdPage /> },
      { path: 'clusters', element: <ClustersPage /> },
      { path: 'clusters/:cluster', element: <ClusterDetailsPage /> },
      { path: 'backups-storage', element: <BackupsStoragePage /> },
      { path: 'backups-storage/:cluster/:shard', element: <BackupsShardDetailsPage /> },
      { path: 'kafka', element: <KafkaClustersPage /> },
      { path: 'kafka/:cluster', element: <KafkaClusterDetailsPage /> },
      { path: 'valkey', element: <ValkeyClustersPage /> },
      { path: 'valkey/:cluster', element: <ValkeyClusterDetailsPage /> },
      { path: 'ha', element: <HaPage /> },
      { path: 'ha/:scope', element: <HaScopeDetailsPage /> },
      { path: 'alerts', element: <AlertsPage /> },
      { path: 'workers', element: <WorkersPage /> },
      { path: '*', element: <Navigate to="/" replace /> },
    ],
  },
]);
