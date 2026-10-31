// Детали kafka-кластера: шапка (state-бейджи, конфиг-мутация/ротация/удаление)
// + вкладки Брокеры/Топики/Группы (arch/03 §7.3).
import { useQuery } from '@tanstack/react-query';
import { Alert, Badge, Group, Stack, Text, Tooltip } from '@mantine/core';
import { useParams } from 'react-router';
import {
  cancelKafkaRotation,
  deleteKafkaCluster,
  rotateKafkaAdminPassword,
  rotateKafkaCa,
  rotateKafkaPassword,
  fetchKafkaClusterDetails,
  kafkaQueryKeys,
} from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { DomainPageLayout } from '../../components/DomainPageLayout';
import { WorkJournalBlock } from '../../components/WorkJournalBlock';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { BrokersTab } from './BrokersTab';
import { DeleteEntityButton } from '../../components/actions/DeleteEntityButton';
import { EditClusterConfigModal } from './EditClusterConfigModal';
import { GroupsTab } from './GroupsTab';
import { RebalanceButton } from './RebalanceButton';
import { RotateSecretButton } from '../../components/actions/RotateSecretButton';
import { TopicsTab } from './TopicsTab';
import type { TicketOutcomeDto } from '../../api/dto';

export function KafkaClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: kafkaQueryKeys.cluster(cluster),
    queryFn: () => fetchKafkaClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const c = query.data;
  const active = c.state === 'ACTIVE';
  const DAY_MS = 86_400_000;
  return (
    <Stack gap="md">
      <DomainPageLayout
        title={`Kafka: ${c.name}`}
        statusBadges={
          <>
            {/* Штатная тишина серым (arch/03 §3) */}
            {c.state === 'TO_REMOVE' ? (
              <Tooltip label="the worker tears down containers/volumes and etcd keys">
                <Badge color="gray" variant="light">TO_REMOVE</Badge>
              </Tooltip>
            ) : c.state === 'NOT_INITIALIZED' ? (
              <Badge color="gray" variant="light">NOT_INITIALIZED</Badge>
            ) : null}
          </>
        }
        modeBadges={
          <>
            {/* u07-бейджи живых ротаций; кнопка «Отменить» — RotateSecretButton (шаг 10) */}
            {c.rotation !== null ? (
              <Tooltip label="rotation ticket is live: rolling broker restart (phases A/B/C)">
                <Badge color="blue" variant="light">ротация app…</Badge>
              </Tooltip>
            ) : null}
            {c.adminRotation !== null && c.adminRotation !== undefined ? (
              <Tooltip label="admin rotation ticket is live: rolling broker restart (phases A/B/C)">
                <Badge color="blue" variant="light">ротация admin…</Badge>
              </Tooltip>
            ) : null}
            {c.caRotation !== null ? (
              <Tooltip label="CA rotation ticket is live: dual-trust window (P/D/R/C), brokers restart one by one">
                <Badge color="grape" variant="light">ротация CA…</Badge>
              </Tooltip>
            ) : null}
          </>
        }
        endpoints={c.endpoints}
        inFlightBadges={
          <>
            {c.reassignment !== null ? (
              <Tooltip label="reassignment is executed by the worker: replica moves in batches">
                <Badge color="violet" variant="light">
                  {c.reassignment.mode === 'drain'
                    ? `drain ${c.reassignment.drainBroker ?? ''}`.trim()
                    : 'rebalance'}
                  : {c.reassignment.partitionsRemaining}/{c.reassignment.partitionsTotal} partitions
                </Badge>
              </Tooltip>
            ) : null}
            {c.regen !== null ? (
              <Tooltip label="rolling broker recreation is executed by the worker (volumes are preserved)">
                <Badge color="indigo" variant="light">
                  regeneration {c.regen.brokersTotal - c.regen.brokersRemaining}/{c.regen.brokersTotal}
                </Badge>
              </Tooltip>
            ) : null}
          </>
        }
        actions={
          active ? (
            <Group gap="sm">
              <EditClusterConfigModal cluster={c} />
              {/* u02: общая кнопка ротации; u17: у живой заявки — «Отменить» */}
              <RotateSecretButton
                title="Сменить app-пароль"
                warningText={'Воркер ротирует пароль фазами: A) rolling-перезапуск брокеров с ДВУМЯ кредами '
                  + '(старый и новый), B) атомарная замена app_password в etcd, C) rolling-удаление старого креда. '
                  + 'Rolling-перезапуск — выполняйте в тихое окно.'}
                liveTicketText={c.rotation !== null ? 'ротация app…' : null}
                onRotate={() => rotateKafkaPassword(c.name)}
                onCancel={() => cancelKafkaRotation(c.name, 'app-password/rotate')}
                invalidateKeys={[kafkaQueryKeys.clusters, kafkaQueryKeys.cluster(c.name)]}
                disabled={!active}
              />
              <RotateSecretButton
                title="Сменить admin-пароль"
                warningText={'Воркер ротирует admin-кред фазами A/B/C (rolling-перезапуск брокеров, атомарная '
                  + 'замена admin_password, удаление старого). Приложения с ролью app не затронуты.'}
                liveTicketText={c.adminRotation !== null && c.adminRotation !== undefined ? 'ротация admin…' : null}
                onRotate={() => rotateKafkaAdminPassword(c.name)}
                onCancel={() => cancelKafkaRotation(c.name, 'admin-password/rotate')}
                invalidateKeys={[kafkaQueryKeys.clusters, kafkaQueryKeys.cluster(c.name)]}
                disabled={!active}
              />
              <RotateSecretButton
                title="Ротация CA"
                warningText={'Воркер ротирует per-cluster CA и серверные серты брокеров без остановки записи: '
                  + '(P) staging нового CA, (D) окно двойного доверия — ca_pem = OLD+NEW bundle, '
                  + '(R) rolling-пересоздание брокеров с сертами нового CA, (C) атомарный коммит. '
                  + 'Клиенты перечитывают ca_pem из etcd.'}
                liveTicketText={c.caRotation !== null ? 'ротация CA…' : null}
                onRotate={() => rotateKafkaCa(c.name)}
                onCancel={() => cancelKafkaRotation(c.name, 'ca/rotate')}
                invalidateKeys={[kafkaQueryKeys.clusters, kafkaQueryKeys.cluster(c.name)]}
                disabled={!active}
              />
              <RebalanceButton cluster={c.name} rebalance={c.rebalance} disabled={!active} />
              <DeleteEntityButton
                title={`Удалить kafka-кластер ${c.name}`}
                confirmText={`Кластер ${c.name} перейдёт в TO_REMOVE: воркер снимет контейнеры брокеров, тома и ключи etcd (финал X2). Обратного перехода нет.`}
                onConfirm={() => deleteKafkaCluster(c.name)}
                invalidateKeys={[kafkaQueryKeys.clusters, kafkaQueryKeys.cluster(c.name)]}
                label="Удалить кластер"
              />
            </Group>
          ) : null
        }
        infoFields={[
          { label: 'Brokers', value: String(c.brokers) },
          { label: 'RF', value: String(c.replicationFactor) },
          { label: 'Min ISR', value: String(c.minInSyncReplicas) },
          { label: 'Default partitions', value: String(c.defaultPartitions) },
          { label: 'Retention, days', value: String(c.defaultRetentionMs / DAY_MS) },
          {
            label: 'Topics',
            value: String(c.topics.length),
          },
          { label: 'Live probe', value: c.probeOk === null ? '—' : c.probeOk ? 'ok' : 'unavailable' },
        ]}
        tabs={[
          {
            value: 'brokers',
            label: 'Брокеры',
            content: (
              <BrokersTab cluster={c.name} brokers={c.brokersList} canScale={active}
                reassignment={c.reassignment} regen={c.regen} />
            ),
          },
          {
            value: 'topics',
            label: 'Топики',
            content: (
              <TopicsTab
                cluster={c.name}
                topics={c.topics}
                canMutate={active}
                defaults={{ defaultPartitions: c.defaultPartitions, replicationFactor: c.replicationFactor, brokers: c.brokers }}
              />
            ),
          },
          {
            value: 'groups',
            label: 'Группы',
            content: <GroupsTab groups={c.groups} probeOk={c.probeOk} />,
          },
          {
            value: 'journal',
            label: 'Журнал',
            content: (
              <Stack gap="md">
                {/* u13: work-журнал /kafkaworker/work/<C> */}
                <WorkJournalBlock work={c.work} />
                {c.ticketOutcome !== null ? <TicketOutcomeLine outcome={c.ticketOutcome} /> : null}
              </Stack>
            ),
          },
        ]}
      />

      {c.probeOk === false ? (
        <Alert color="orange" variant="light" title="Live probe unavailable">
          {c.probeError ?? 'DescribeCluster is not responding'} — etcd data is up to date, the live part is hidden.
        </Alert>
      ) : null}
    </Stack>
  );
}

// Строка «Последний исход заявки» (t10): kind → outcome (finished); expired
// подсвечен warning-цветом, причина — в тексте.
export function TicketOutcomeLine({ outcome }: { outcome: TicketOutcomeDto }) {
  const expired = outcome.outcome === 'expired';
  return (
    <Text size="sm" c={expired ? 'yellow' : undefined}>
      Last ticket outcome: {outcome.kind} → {outcome.outcome} (
      {new Date(outcome.finishedUnix * 1000).toLocaleString()})
      {outcome.reason ? ` — reason: ${outcome.reason}` : ''}
    </Text>
  );
}
