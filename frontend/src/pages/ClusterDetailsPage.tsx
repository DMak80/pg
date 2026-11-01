// Детали pg-кластера (u01/u04/u07/u12): единый шаблон DomainPageLayout —
// шапка-статус + вкладки Шарды|Бакеты|Переезды|Бэкапы (срез)|Секреты|Журнал;
// «Стендовая топология» — блоком ПОД вкладками (D5, статус-кво). Вкладка
// «Бакеты» скрыта для нешардированных (sharded=false, arch/03 §3). Клик по
// не-ACTIVE бакету — переход к вкладке «Переезды» с подсветкой (u04).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Group, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { deleteCluster, fetchClusterDetails, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { DomainPageLayout } from '../components/DomainPageLayout';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix } from '../utils/format';
import { BackupsSliceTab } from './cluster-details/BackupsSliceTab';
import { BucketsTab } from './cluster-details/BucketsTab';
import { DeleteEntityButton } from '../components/actions/DeleteEntityButton';
import { JournalTab } from './cluster-details/JournalTab';
import { MovesTab } from './cluster-details/MovesTab';
import { SecretsTab } from './cluster-details/SecretsTab';
import { ShardsTab } from './cluster-details/ShardsTab';
import { StandNodesBlock } from './cluster-details/StandNodesBlock';
import { StrictModeSwitch } from './cluster-details/StrictModeSwitch';

export function ClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.cluster(cluster),
    queryFn: () => fetchClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  // u04: локальный state активной вкладки + подсветка бакета (клик из «Бакетов»).
  // Хуки — ДО early return: вызов useState не может зависеть от наличия данных
  // (инвариант React: одинаковое число хуков на каждом рендере).
  const [activeTab, setActiveTab] = useState('shards');
  const [highlightBucket, setHighlightBucket] = useState<number | null>(null);

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
      <Anchor component={Link} to="/clusters" size="sm">← Кластеры</Anchor>
      <DomainPageLayout
        title={data.name}
        statusBadges={
          <>
            {data.incomplete ? <Badge color="yellow" variant="light">incomplete</Badge> : null}
            {/* Штатная тишина серым (arch/03 §3): TO_REMOVE — не авария */}
            {toRemove ? <Badge color="gray" variant="light">TO_REMOVE</Badge> : null}
            {data.state === 'NOT_INITIALIZED' ? (
              <Badge color="gray" variant="light">NOT_INITIALIZED</Badge>
            ) : null}
          </>
        }
        modeBadges={
          <>
            {/* t06: бейдж режима в шапке (arch/03 §3) */}
            {data.synchronousModeStrict
              ? <Badge color="red" variant="light">strict</Badge>
              : <Badge color="teal" variant="light">availability</Badge>}
          </>
        }
        // Strict-переключатель — режим кластера, остаётся в шапке (D4).
        actions={
          toRemove ? null : (
            <Group gap="sm">
              {/* t06: переключатель strict-режима (мутация только Active) */}
              <StrictModeSwitch cluster={data.name} state={data.state}
                strict={data.synchronousModeStrict} />
              {/* u02: общая кнопка удаления (подтверждение модалом) */}
              <DeleteEntityButton
                title={`Удалить кластер ${data.name}`}
                confirmText={`Кластер ${data.name} перейдёт в состояние TO_REMOVE. Панель не удаляет ключи etcd и ноды — снятие ресурсов выполняет внешний оркестратор. Обратного перехода нет.`}
                onConfirm={() => deleteCluster(data.name)}
                invalidateKeys={[['clusters'], [data.name]]}
                label="Удалить кластер"
              />
            </Group>
          )
        }
        inFlightBadges={
          <>
            {/* u07-бейджи заявок в полёте */}
            {data.secretsRotationPending ? (
              <Badge color="blue" variant="light">ротация…</Badge>
            ) : null}
            {data.pendingMoves.length > 0 ? (
              <Badge color="violet" variant="light">в очереди: {data.pendingMoves.length} заявок</Badge>
            ) : null}
          </>
        }
        infoFields={[
          { label: 'DB', value: data.dbName ?? '—' },
          {
            label: 'Buckets',
            value: data.sharded ? String(data.bucketsCount) : '—',
          },
          { label: 'Created', value: formatUnix(data.createdUnix) },
        ]}
        activeTab={activeTab}
        onTabChange={(v) => setActiveTab(v ?? 'shards')}
        tabs={[
          {
            value: 'shards',
            label: 'Шарды',
            content: (
              <ShardsTab cluster={data.name} canScale={canScale} shards={data.shards}
                bucketCounts={bucketCounts} strict={data.synchronousModeStrict} />
            ),
          },
          {
            value: 'buckets',
            label: 'Бакеты',
            content: (
              <BucketsTab cluster={data.name} canScale={canScale} shards={data.shards}
                buckets={data.buckets} pendingMoves={data.pendingMoves}
                highlightBucket={highlightBucket}
                onBucketClick={(id) => {
                  // u04: клик по не-ACTIVE бакету — вкладка «Переезды» + подсветка.
                  setHighlightBucket(id);
                  setActiveTab('moves');
                }}
              />
            ),
          },
          {
            value: 'moves',
            label: 'Переезды',
            content: (
              <MovesTab cluster={data.name} canScale={canScale} buckets={data.buckets}
                pendingMoves={data.pendingMoves} highlightBucket={highlightBucket} />
            ),
          },
          {
            value: 'backups',
            label: 'Бэкапы (срез)',
            content: <BackupsSliceTab cluster={data.name} />,
          },
          {
            value: 'secrets',
            label: 'Секреты',
            content: (
              <SecretsTab cluster={data.name} state={data.state}
                rotationPending={data.secretsRotationPending ?? false} />
            ),
          },
          {
            value: 'journal',
            label: 'Журнал',
            content: (
              <JournalTab work={data.work} heals={data.heals}
                evacuations={data.evacuations} />
            ),
          },
        ]}
        hideTabs={data.sharded ? [] : ['buckets']}
      />
      {/* «Стендовая топология» — блок под вкладками (D5: статус-кво, не в дереве вкладок) */}
      <StandNodesBlock standNodes={data.standNodes} />
    </Stack>
  );
}
