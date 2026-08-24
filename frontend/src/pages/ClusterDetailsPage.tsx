// Детали кластера: шапка + вкладки Шарды/Бакеты/Переезды/Heals + стендовая топология (t08 spec §4.7–4.8).
// Вкладка «Бакеты» скрыта для нешардированных (sharded=false, arch/03 §3; spec
// bucket-block-distribution §4.4): у БД 1×1 нет карты бакетов.
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Group, Stack, Tabs, Text, Title } from '@mantine/core';
import { Link, useParams } from 'react-router';
import { fetchClusterDetails, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix } from '../utils/format';
import { BucketsTab } from './cluster-details/BucketsTab';
import { DeleteClusterButton } from './cluster-details/DeleteClusterButton';
import { HealsTab } from './cluster-details/HealsTab';
import { MovesTab } from './cluster-details/MovesTab';
import { ShardsTab } from './cluster-details/ShardsTab';
import { StandNodesBlock } from './cluster-details/StandNodesBlock';

export function ClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.cluster(cluster),
    queryFn: () => fetchClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  // Паттерн состояний (t08 spec §4.15): 404 → notFound-контент; 503/прочее при
  // отсутствии данных → ErrorSection; polling-сбой при данных — тихо.
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection
        error={query.error}
        onRetry={() => void query.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Кластер не найден</Text>
            <Anchor component={Link} to="/clusters" size="sm">← Кластеры</Anchor>
          </Stack>
        }
      />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  const toRemove = data.state === 'TO_REMOVE';
  // Кнопки add/remove шарда — только у шардированной Active-БД (t06 spec §6.3;
  // нешардированная — просто кластер, шкалирование шардов ей недоступно);
  // счётчик бакетов шарда — по routing (диалог удаления, Д4).
  const canScale = data.state === 'ACTIVE' && data.sharded;
  const bucketCounts = Object.fromEntries(
    data.shards.map((s) => [s.name, data.buckets.filter((b) => b.owner === s.name).length]),
  );
  return (
    <Stack gap="md">
      <div>
        <Anchor component={Link} to="/clusters" size="sm">← Кластеры</Anchor>
        <Group gap="sm" mt={4} justify="space-between">
          <Group gap="sm">
            <Title order={2}>{data.name}</Title>
            {data.incomplete ? <Badge color="yellow" variant="light">incomplete</Badge> : null}
            {toRemove ? <Badge color="red" variant="light">к удалению</Badge> : null}
          </Group>
          {/* Обратного перехода из TO_REMOVE нет — у удаляемого кластера кнопки нет (arch/02 §9.4). */}
          {toRemove ? null : <DeleteClusterButton name={data.name} />}
        </Group>
        <Text c="dimmed" size="sm">
          {/* Бакеты — только у шардированных (arch/03 §2): нешардированная = 1
              вырожденный бакет, счётчик не информативен — прочерк. */}
          БД: {data.dbName ?? '—'} · Бакеты: {data.sharded ? data.bucketsCount : '—'} · Создан: {formatUnix(data.createdUnix)}
        </Text>
      </div>
      <Tabs defaultValue="shards">
        <Tabs.List>
          <Tabs.Tab value="shards">Шарды</Tabs.Tab>
          {data.sharded ? <Tabs.Tab value="buckets">Бакеты</Tabs.Tab> : null}
          <Tabs.Tab value="moves">Переезды</Tabs.Tab>
          <Tabs.Tab value="heals">Heals</Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel value="shards" pt="sm">
          <ShardsTab cluster={data.name} canScale={canScale} shards={data.shards}
            bucketCounts={bucketCounts} />
        </Tabs.Panel>
        {data.sharded ? (
          <Tabs.Panel value="buckets" pt="sm"><BucketsTab buckets={data.buckets} /></Tabs.Panel>
        ) : null}
        <Tabs.Panel value="moves" pt="sm"><MovesTab buckets={data.buckets} /></Tabs.Panel>
        <Tabs.Panel value="heals" pt="sm"><HealsTab heals={data.heals} /></Tabs.Panel>
      </Tabs>
      <StandNodesBlock standNodes={data.standNodes} />
    </Stack>
  );
}
