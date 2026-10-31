// Панель Обзор: карточки etcd/кластеров/алертов/HA, активные переезды,
// лента алертов critical/warning (t08 spec §4.3–4.5; HA-карточка — t09 §4.13).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Anchor,
  Badge,
  Card,
  Group,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { Link } from 'react-router';
import type { AlertSeverityName, OverviewBackupsDto, OverviewDto, OverviewHaDto, OverviewOperationDto, OverviewReliabilityDto } from '../api/dto';
import { fetchAlerts, fetchOverview, queryKeys } from '../api/queries';
import { AlertSeverityBadge } from '../components/AlertSeverityBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatAge, formatBytes, formatUnix, formatUnixAge } from '../utils/format';

// Сортировка ленты: critical раньше warning, внутри — новые сверху (t08 spec §4.4).
function sortAlertRows(a: { severity: string; sinceUnix: number | null }, b: { severity: string; sinceUnix: number | null }): number {
  const rankA = a.severity === 'critical' ? 0 : 1;
  const rankB = b.severity === 'critical' ? 0 : 1;
  if (rankA !== rankB) return rankA - rankB;
  return (b.sinceUnix ?? 0) - (a.sinceUnix ?? 0);
}

export function OverviewPage() {
  const intervalMs = usePollingIntervalMs();
  // Тот же ключ, что у FreshnessBadges — TanStack дедуплицирует опрос (t08 spec §3).
  const overview = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: intervalMs,
  });
  const alerts = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: intervalMs,
  });
  // u06/D6: HA/бэкапы/надёжность — серверные поля OverviewDto; клиентские
  // доборы /api/ha, /api/backups/storage, /api/reliability удалены (дашборд —
  // ОДИН запрос /api/overview); сами эндпоинты остаются страницам-потребителям.

  if (overview.data === undefined)
    return overview.isError ? (
      <ErrorSection error={overview.error} onRetry={() => void overview.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = overview.data;
  return (
    <Stack gap="md">
      <Title order={2}>Обзор</Title>
      {/* u10: глобальный стоп бэкапов — жёлтый баннер над карточками */}
      {data.backupsDisabled ? (
        <Alert color="yellow" variant="light" title="Бэкапы остановлены (глобальный стоп)">
          Ключ /pgworker/backups-disabled жив (поставлен {formatUnix(data.backupsDisabled.updatedUnix)}
          {data.backupsDisabled.updatedBy ? `, кем: ${data.backupsDisabled.updatedBy}` : ''}) —
          приёмники WAL всех кластеров замирают. Запуск — снять ключ или включить Backups:Enabled.
        </Alert>
      ) : null}
      {/* Мин. ширина карточки 330px = 1.5× проектного минимума старой 4-колоночной
          сетки (220px при lg); колонки перестраиваются auto-fill'ом по ширине. */}
      <SimpleGrid minColWidth={330}>
        <EtcdCard data={data} />
        <ClustersCard data={data} />
        <KafkaCard data={data} />
        <ValkeyCard data={data} />
        <S3Card data={data} />
        {/* u06: карточки на серверных агрегатах /api/overview */}
        <BackupsCard data={data.backups ?? null} />
        <ReliabilityCard data={data.reliability ?? null} />
        <AlertsCard data={data} />
        <HaCard data={data.ha ?? null} />
      </SimpleGrid>
      {/* u07: активные операции всех доменов (замена Active moves) */}
      <OperationsSection operations={data.operations ?? []} />
      <AlertsFeedSection
        isPending={alerts.isPending}
        isError={alerts.isError}
        onRetry={() => void alerts.refetch()}
        rows={(alerts.data ?? []).filter((a) => a.severity !== 'info').sort(sortAlertRows)}
        operatorOnly={false}
      />
      {/* u18-дашборд: «Требуют оператора» рядом с лентой */}
      <AlertsFeedSection
        isPending={alerts.isPending}
        isError={alerts.isError}
        onRetry={() => void alerts.refetch()}
        rows={(alerts.data ?? []).filter((a) => a.remedy === 'operator-api' || a.remedy === 'operator-runbook').sort(sortAlertRows)}
        operatorOnly
      />
    </Stack>
  );
}

// Карточка «Надёжность» (arch/03 §3): worst RPO-потенциал по Active-кластерам,
// счётчики шардов mode=full («RPO держится только полным») и mode=off,
// длительность последнего failover установки (клиентская агрегация GET /api/reliability).
// Карточка «Надёжность» (u06/D6): серверное поле OverviewDto.reliability —
// перенос TS-агрегации ReliabilityCard 1:1 (worst RPO по Active-кластерам,
// mode=full/off, самый недавний закрытый failover).
function ReliabilityCard({ data }: { data: OverviewReliabilityDto | null }) {
  let content;
  if (data === null)
    content = <Text c="dimmed" size="sm">Снапшот ещё не собран</Text>;
  else
    content = (
      <Stack gap="xs">
        <Text size="sm">
          Worst RPO potential: {data.worstRpoPotentialSec === null ? '—' : formatAge(data.worstRpoPotentialSec * 1000)}
        </Text>
        <Text size="sm" c={data.fullOnlyCount > 0 ? 'orange' : undefined}>
          RPO держится только полными: {data.fullOnlyCount}{data.offCount > 0 ? ` · off: ${data.offCount}` : ''}
        </Text>
        <Text size="sm" c="dimmed">
          Последний failover: {data.lastFailover === null
            ? '—'
            : `${formatAge(data.lastFailover.durationSec * 1000)} (${formatUnixAge(data.lastFailover.atUnix)} назад)`}
        </Text>
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Надёжность</Text>
        <Anchor size="sm" component={Link} to="/reliability">Подробнее</Anchor>
      </Group>
      {content}
    </Card>
  );
}

// Карточка etcd: доступность и endpoints ok/total; alarms — в ленте и на панели etcd (t08 spec §4.3).
function EtcdCard({ data }: { data: OverviewDto }) {
  const etcd = data.etcd;
  return (
    <Card
      withBorder
      padding="md"
      radius="md"
      style={{ borderColor: etcd.reachable ? undefined : 'var(--mantine-color-red-6)' }}
    >
      <Group justify="space-between" mb="xs">
        <Text fw={600}>etcd</Text>
        <Badge color={etcd.reachable ? 'teal' : 'red'} variant="light">
          {etcd.reachable ? 'reachable' : 'unreachable'}
        </Badge>
      </Group>
      <Text size="sm" c="dimmed">endpoints: {etcd.endpointsOk}/{etcd.endpointsTotal}</Text>
      <Anchor component={Link} to="/etcd" size="sm" mt="xs" display="inline-block">Details →</Anchor>
    </Card>
  );
}

// Карточка кластеров: строка на кластер, счётчики (t08 spec §4.3).
function ClustersCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Clusters</Text>
      {data.clusters.length === 0 ? (
        <Text c="dimmed" size="sm">No clusters found</Text>
      ) : (
        <Stack gap={4}>
          {data.clusters.map((c) => (
            <Group key={c.name} justify="space-between" gap="xs" wrap="nowrap">
              <Group gap="xs" wrap="nowrap">
                <Anchor component={Link} to={`/clusters/${c.name}`} size="sm" truncate="end">{c.name}</Anchor>
                {c.notInitialized ? (
                  <Badge color="gray" variant="light">not initialized</Badge>
                ) : null}
              </Group>
              <Group gap={5} wrap="nowrap">
                <Text size="sm" c="dimmed">shards {c.shards}</Text>
                <Text size="sm" c="dimmed">buckets {c.buckets}</Text>
                {c.activeMoves > 0 ? (
                  <Badge color="yellow" variant="light">moves: {c.activeMoves}</Badge>
                ) : null}
                {c.masterlessShards > 0 ? (
                  <Badge color="red" variant="light">masterless: {c.masterlessShards}</Badge>
                ) : null}
              </Group>
            </Group>
          ))}
        </Stack>
      )}
    </Card>
  );
}

// Карточка kafka-домена: сводка из OverviewDto.kafka (arch/03 §7.1; null до
// первого тика kafka-refresher'а).
function KafkaCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" wrap="nowrap" mb="xs">
        <Text fw={600}>Kafka</Text>
        <Anchor component={Link} to="/kafka" size="sm">all clusters →</Anchor>
      </Group>
      {data.kafka === null ? (
        <Text c="dimmed" size="sm">Kafka snapshot not collected yet</Text>
      ) : (
        <Group gap="md">
          <Group gap={5}>
            <Text size="xl" fw={600}>{data.kafka.clustersTotal}</Text>
            <Text size="sm" c="dimmed">clusters</Text>
          </Group>
          <Group gap={5}>
            <Text size="xl" fw={600} c={data.kafka.clustersCritical > 0 ? 'red' : undefined}>
              {data.kafka.clustersCritical}
            </Text>
            <Text size="sm" c="dimmed">critical</Text>
          </Group>
        </Group>
      )}
    </Card>
  );
}

// Карточка Valkey (u05/D2): порт KafkaCard — сводка из OverviewDto.valkey
// (null до первого тика valkey-refresher'а).
function ValkeyCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" wrap="nowrap" mb="xs">
        <Text fw={600}>Valkey</Text>
        <Anchor component={Link} to="/valkey" size="sm">все кластеры →</Anchor>
      </Group>
      {data.valkey === null ? (
        <Text c="dimmed" size="sm">Снапшот valkey ещё не собран</Text>
      ) : (
        <Group gap="md">
          <Group gap={5}>
            <Text size="xl" fw={600}>{data.valkey.clustersTotal}</Text>
            <Text size="sm" c="dimmed">кластеров</Text>
          </Group>
          <Group gap={5}>
            <Text size="xl" fw={600} c={data.valkey.clustersCritical > 0 ? 'red' : undefined}>
              {data.valkey.clustersCritical}
            </Text>
            <Text size="sm" c="dimmed">critical</Text>
          </Group>
        </Group>
      )}
    </Card>
  );
}

// Карточка s3-домена (t34, arch/03 §12.1): порт KafkaCard — сводка из
// OverviewDto.s3 (null до первого тика s3-refresher'а).
function S3Card({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" wrap="nowrap" mb="xs">
        <Text fw={600}>S3</Text>
        <Anchor component={Link} to="/s3" size="sm">all clusters →</Anchor>
      </Group>
      {data.s3 === null ? (
        <Text c="dimmed" size="sm">S3 snapshot not collected yet</Text>
      ) : (
        <Group gap="md">
          <Group gap={5}>
            <Text size="xl" fw={600}>{data.s3.clustersTotal}</Text>
            <Text size="sm" c="dimmed">clusters</Text>
          </Group>
          <Group gap={5}>
            <Text size="xl" fw={600} c={data.s3.clustersCritical > 0 ? 'red' : undefined}>
              {data.s3.clustersCritical}
            </Text>
            <Text size="sm" c="dimmed">critical</Text>
          </Group>
        </Group>
      )}
    </Card>
  );
}

// Карточка бэкапов: health S3 (api/live/cluster) + место (used/quota/state
// воркера + live-факт) — компактная сводка грани «Хранилище бэкапов»,
// клиентская агрегация GET /api/backups/storage (по образцу HA-агрегации,
// arch/03 §3). Ошибка без данных — своя (не роняет остальные карточки),
// паттерн HaCard.
// Карточка «Бэкапы» (u06): серверное поле OverviewDto.backups (null — снапшот
// pg не тикал); health S3 + место (etcd-ключ storage) + etcd-снапшоты.
function BackupsCard({ data }: { data: OverviewBackupsDto | null }) {
  let content;
  if (data === null)
    content = <Text c="dimmed" size="sm">Снапшот ещё не собран</Text>;
  else if (!data.configured)
    content = <Text c="dimmed" size="sm">Хранилище бэкапов не настроено</Text>;
  else
    content = (
      <Stack gap="xs">
        <Group gap="xs" wrap="nowrap">
          {data.health === null ? (
            <Text size="sm" c="dimmed">health: нет данных (первый тик)</Text>
          ) : (
            <>
              <Badge color={data.health.apiOk ? 'teal' : 'red'} variant="light">
                api {data.health.apiOk ? 'ok' : 'down'}
              </Badge>
              <Badge color={data.health.liveOk ? 'teal' : 'red'} variant="light">
                live {data.health.liveOk ? 'ok' : 'down'}
              </Badge>
              {data.health.clusterOk === null || data.health.clusterOk === undefined ? (
                <Badge color="gray" variant="light">cluster n/a</Badge>
              ) : (
                <Badge color={data.health.clusterOk ? 'teal' : 'red'} variant="light">
                  cluster {data.health.clusterOk ? 'ok' : 'down'}
                </Badge>
              )}
            </>
          )}
          {data.etcdSnapshots === null ? null : (
            <Badge color={data.etcdSnapshots.state === 'OK' ? 'green' : data.etcdSnapshots.state === 'WARN' ? 'yellow' : 'red'} variant="light">
              {data.etcdSnapshots.state ?? '—'}
            </Badge>
          )}
          {data.storageState !== null ? (
            <Badge color={data.storageState === 'OK' ? 'green' : data.storageState === 'WARN' ? 'yellow' : 'red'} variant="light">
              {data.storageState}
            </Badge>
          ) : null}
        </Group>
        <Text size="sm">
          {data.usedBytes === 0 && data.quotaBytes === null
            ? 'Место: ключ /pgworker/backups/storage отсутствует в etcd'
            : `Место: ${formatBytes(data.usedBytes)}${data.quotaBytes === null ? '' : ` из ${formatBytes(data.quotaBytes)}`}`}
        </Text>
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" wrap="nowrap" mb="xs">
        <Text fw={600}>Бэкапы</Text>
        <Anchor component={Link} to="/backups-storage" size="sm">Подробнее →</Anchor>
      </Group>
      {content}
    </Card>
  );
}

// Бейджи health S3 — цветовая логика 1:1 с HealthCard страницы
// /backups-storage (api/live teal/red, cluster gray при n/a).

// Карточка алертов: счётчики severity, нули — приглушённо (t08 spec §4.3).
function AlertsCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Алерты</Text>
      <Group gap="md">
        <Badge
          color={data.alertsCritical > 0 ? 'red' : 'gray'}
          variant="light"
          size="lg"
        >
          critical: {data.alertsCritical}
        </Badge>
        <Badge
          color={data.alertsWarning > 0 ? 'yellow' : 'gray'}
          variant="light"
          size="lg"
        >
          warning: {data.alertsWarning}
        </Badge>
      </Group>
      <Anchor component={Link} to="/alerts" size="sm" mt="xs" display="inline-block">All alerts →</Anchor>
    </Card>
  );
}

// Секция «Активные операции» (u07): таблица data.operations всех доменов
// (переезды — подмножество операций pg). kind — словарь сервера, UI не парсит.
function OperationsSection({ operations }: { operations: OverviewOperationDto[] }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Активные операции</Text>
      {operations.length === 0 ? (
        <Text c="dimmed" size="sm">Нет активных операций</Text>
      ) : (
        <Table.ScrollContainer minWidth={760}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Домен</Table.Th>
                <Table.Th>Тип</Table.Th>
                <Table.Th>Кластер</Table.Th>
                <Table.Th>Сущность</Table.Th>
                <Table.Th>Детали</Table.Th>
                <Table.Th>Обновлено</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {operations.map((o) => (
                <Table.Tr key={`${o.domain}-${o.kind}-${o.cluster}-${o.entity ?? ''}-${o.detail}`}>
                  <Table.Td>
                    <Badge color={DOMAIN_COLORS[o.domain] ?? 'gray'} variant="light">{o.domain}</Badge>
                  </Table.Td>
                  <Table.Td><Text size="sm" ff="monospace">{o.kind}</Text></Table.Td>
                  <Table.Td>
                    <Anchor component={Link} size="sm"
                      to={o.domain === 'pg' ? `/clusters/${o.cluster}`
                        : o.domain === 'kafka' ? `/kafka/${o.cluster}`
                          : o.domain === 'valkey' ? `/valkey/${o.cluster}`
                            : `/s3/${o.cluster}`}>
                      {o.cluster}
                    </Anchor>
                  </Table.Td>
                  <Table.Td>{o.entity ?? '—'}</Table.Td>
                  <Table.Td><Text size="sm" ff="monospace">{o.detail}</Text></Table.Td>
                  <Table.Td>
                    <Tooltip label={o.updatedUnix === null ? '' : formatUnix(o.updatedUnix)}>
                      <span>{o.updatedUnix === null ? '—' : formatUnixAge(o.updatedUnix)}</span>
                    </Tooltip>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

// Цвет бейджа домена (фиксированный словарь карточек).
const DOMAIN_COLORS: Record<string, string> = {
  pg: 'teal',
  kafka: 'blue',
  valkey: 'orange',
  s3: 'grape',
};

// Лента алертов: critical/warning, все (без топ-N); info отфильтрован (t08 spec §4.4).
function AlertsFeedSection({ isPending, isError, onRetry, rows, operatorOnly = false }: {
  isPending: boolean;
  isError: boolean;
  onRetry: () => void;
  rows: { id: string; severity: AlertSeverityName; kind: string; target: string; message: string; sinceUnix: number | null }[];
  operatorOnly?: boolean;
}) {
  let content;
  if (isPending) content = <Text c="dimmed" size="sm">Загрузка алертов…</Text>;
  else if (isError)
    content = (
      <Stack gap="xs" align="flex-start">
        <Alert color="red">Нет данных алертов</Alert>
        <Anchor size="sm" onClick={onRetry}>Повторить</Anchor>
      </Stack>
    );
  else if (rows.length === 0)
    content = operatorOnly
      ? <Text c="dimmed" size="sm">Нет алертов, требующих оператора</Text>
      : <Text c="teal" size="sm">Нет критичных алертов</Text>;
  else
    content = (
      <Stack gap={4}>
        {rows.map((a) => (
          <Group key={a.id} gap="sm" wrap="nowrap" align="flex-start">
            <AlertSeverityBadge severity={a.severity} />
            <Text size="sm" ff="monospace">{a.kind}</Text>
            <Text size="sm" ff="monospace" c="dimmed">{a.target}</Text>
            <Text size="sm" style={{ flex: 1 }}>{a.message}</Text>
            <Tooltip label={formatUnix(a.sinceUnix)}>
              <span>
                <Text size="sm" c="dimmed">{a.sinceUnix === null ? '—' : `for ${formatUnixAge(a.sinceUnix)}`}</Text>
              </span>
            </Tooltip>
          </Group>
        ))}
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>{operatorOnly ? 'Требуют оператора' : 'Лента алертов'}</Text>
        <Anchor component={Link} to="/alerts" size="sm">Все алерты →</Anchor>
      </Group>
      {content}
    </Card>
  );
}

// Карточка HA дашборда: счётчики скопов/без лидера/unmatched + строки-ссылки
// на детали (t09 spec §4.13). «Без лидера» — только matched-скопы: согласовано
// с алертом shard-no-leader, чтобы счётчик не расходился с лентой алертов.
// Ошибка без данных — своя (не роняет остальные карточки); ошибка при данных
// — тихо (FreshnessBadges сигнализирует), паттерн AlertsFeedSection (t08 §4.4).
// Карточка HA (u06): серверное поле OverviewDto.ha (без лидера — только
// matched, как в прежней TS-агрегации).
function HaCard({ data }: { data: OverviewHaDto | null }) {
  let content;
  if (data === null)
    content = <Text c="dimmed" size="sm">Снапшот ещё не собран</Text>;
  else if (data.scopesTotal === 0)
    content = <Text c="dimmed" size="sm">HA-скопов нет</Text>;
  else
    content = (
      <Stack gap={4}>
        <Group gap="xs" wrap="nowrap">
          <Text size="sm" c="dimmed">скопов: {data.scopesTotal}</Text>
          <Badge color={data.withoutLeader > 0 ? 'red' : 'gray'} variant="light">
            без лидера: {data.withoutLeader}
          </Badge>
          {data.unmatched > 0 ? (
            <Badge color="yellow" variant="light">unmatched: {data.unmatched}</Badge>
          ) : null}
        </Group>
        <Anchor size="sm" component={Link} to="/ha">Репликация →</Anchor>
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">HA</Text>
      {content}
    </Card>
  );
}
