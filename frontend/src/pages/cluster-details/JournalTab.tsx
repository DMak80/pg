// Вкладка «Журнал» pg-кластера (u12/D5): work-журнал /pgworker/work/<C> +
// heals (перенос прежней вкладки) + блок «Эвакуации» (u12, arch/14 §3.2).
// Журналы воркера перезаписываются — видна последняя фаза.
import { Badge, Card, Group, Stack, Text } from '@mantine/core';
import type { ClusterWorkDto, EvacuationDto, HealDto } from '../../api/dto';
import { formatUnix } from '../../utils/format';
import { HealsTab } from './HealsTab';
import { WorkJournalBlock } from '../../components/WorkJournalBlock';

export function JournalTab({ work, heals, evacuations }: {
  work: ClusterWorkDto | null | undefined;
  heals: HealDto[];
  evacuations: EvacuationDto[] | null | undefined;
}) {
  return (
    <Stack gap="md">
      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">Что делает воркер (/pgworker/work/)</Text>
        <WorkJournalBlock work={work} />
      </Card>

      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">Heals (авто-починка routing)</Text>
        <HealsTab heals={heals} />
      </Card>

      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">Эвакуации (BucketEvacuator, arch/14 §5 D)</Text>
        {evacuations === null || evacuations === undefined || evacuations.length === 0 ? (
          <Text c="dimmed" size="sm">Журналов эвакуаций нет (шарды не пропадали).</Text>
        ) : (
          <Stack gap="sm">
            {evacuations.map((e) => (
              <Stack key={`${e.shard}-${e.evacuatedUnix}`} gap={4}>
                <Group gap="sm">
                  <Text fw={500} size="sm">шард <Text span ff="monospace">{e.shard}</Text></Text>
                  {/* state-бейдж: DONE — штатная тишина серым, QUARANTINED — yellow */}
                  <EvacuationStateBadge state={e.state} />
                  <Text size="sm" c="dimmed">{formatUnix(e.evacuatedUnix)}</Text>
                </Group>
                <Text size="sm">Причина: {e.reason}</Text>
                <Text size="sm">
                  План (бакет → новый шард):{' '}
                  {Object.entries(e.buckets).map(([bucket, target]) => (
                    <Text key={bucket} span ff="monospace" size="sm">
                      {bucket}→{target}{' '}
                    </Text>
                  ))}
                </Text>
                {e.returnedUnix !== null ? (
                  <Text size="sm" c="dimmed">Возврат шарда: {formatUnix(e.returnedUnix)}</Text>
                ) : null}
              </Stack>
            ))}
          </Stack>
        )}
      </Card>
    </Stack>
  );
}

// State-бейдж эвакуации (u12): DONE=gray (штатная тишина), QUARANTINED=yellow.
function EvacuationStateBadge({ state }: { state: 'DONE' | 'QUARANTINED' }) {
  return (
    <Badge color={state === 'DONE' ? 'gray' : 'yellow'} variant="light">{state}</Badge>
  );
}
