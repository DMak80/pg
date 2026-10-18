import { useState } from 'react';
import { Badge, Button, Checkbox, Group, Modal, Select, Stack, Text } from '@mantine/core';
import { updateBackupsPolicy } from '../../api/queries';
import type { S3PoolDto } from '../../api/dto';

// Модал «Пул бэкапов» (t36, arch/adminpanel/03 §3.8): Select пула + два
// упорядоченных списка нод (мастер/реплика) — чекбоксы (добавление в КОНЕЦ
// массива — приоритет) + кнопки ↑/↓; сохранение — полный policy (замещение
// целиком, 02 §9.12); «Не выбрано» — валидный сброс (policy без s3).
interface Props {
  cluster: string;
  policy: {
    retentionDays: number | null;
    retentionWeeks: number | null;
    retentionMonths: number | null;
    fullMaxAgeSec: number | null;
    verifyOnCreate: boolean | null;
    drillIntervalDays: number | null;
    s3: { pool: string; writeMaster: string[]; writeReplica: string[] } | null;
  } | null;
  pools: S3PoolDto[];
  onClose: () => void;
  onSaved: () => void;
}

// Упорядоченный список нод: чекбокс добавляет имя в конец массива (приоритет),
// снятие удаляет; кнопки ↑/↓ двигают элемент (без DnD-библиотек).
function OrderedList({
  label, nodes, value, onChange,
}: {
  label: string;
  nodes: { name: string; state: string | null }[];
  value: string[];
  onChange: (next: string[]) => void;
}) {
  const toggle = (name: string, checked: boolean) =>
    onChange(checked ? [...value, name] : value.filter((n) => n !== name));
  const move = (index: number, delta: -1 | 1) => {
    const next = [...value];
    const target = index + delta;
    if (target < 0 || target >= next.length) return;
    [next[index], next[target]] = [next[target], next[index]];
    onChange(next);
  };

  return (
    <div>
      <Text size="sm" fw={600}>{label}</Text>
      {nodes.length === 0 ? (
        <Text c="dimmed" size="sm">no nodes in the pool</Text>
      ) : (
        nodes.map((n) => {
          const index = value.indexOf(n.name);
          return (
            <Group key={n.name} gap="xs" wrap="nowrap">
              <Checkbox
                aria-label={`${label}: ${n.name}`}
                checked={index >= 0}
                onChange={(e) => toggle(n.name, e.currentTarget.checked)}
                label={n.name}
              />
              {index >= 0 ? (
                <>
                  <Button size="compact-xs" variant="default" onClick={() => move(index, -1)}>↑</Button>
                  <Button size="compact-xs" variant="default" onClick={() => move(index, 1)}>↓</Button>
                  {index === 0 ? <Badge size="sm" variant="light">priority 1</Badge> : null}
                </>
              ) : null}
            </Group>
          );
        })
      )}
    </div>
  );
}

export function BackupsPoolModal({ cluster, policy, pools, onClose, onSaved }: Props) {
  const [pool, setPool] = useState<string | null>(policy?.s3?.pool ?? null);
  const [master, setMaster] = useState<string[]>(policy?.s3?.writeMaster ?? []);
  const [replica, setReplica] = useState<string[]>(policy?.s3?.writeReplica ?? []);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const poolNodes = pools.find((p) => p.name === pool)?.nodes ?? [];

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      // Полный policy (замещение целиком): retention/full_max_age/verify/drill —
      // из текущего значения; s3 — только при выбранном пуле (сброс — БЕЗ секции).
      await updateBackupsPolicy(cluster, {
        retentionDays: policy?.retentionDays ?? 1,
        retentionWeeks: policy?.retentionWeeks ?? 1,
        retentionMonths: policy?.retentionMonths ?? 1,
        fullMaxAgeSec: policy?.fullMaxAgeSec ?? 86400,
        verifyOnCreate: policy?.verifyOnCreate ?? true,
        drillIntervalDays: policy?.drillIntervalDays ?? 1,
        s3: pool
          ? { pool, writeMaster: master, writeReplica: replica }
          : null,
      });
      onSaved();
      onClose();
    } catch (e) {
      // 400 — ProblemDetails по полям; 503 — воркер/etcd недоступен.
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal opened onClose={onClose} title={`Backup pool — ${cluster}`} size="lg">
      <Stack gap="md">
        <Select
          label="Pool"
          placeholder="Not selected (backups disabled)"
          data={pools.map((p) => ({
            value: p.name,
            label: `${p.name} (${p.nodes.length} ${p.nodes.length === 1 ? 'node' : 'nodes'})`,
          }))}
          value={pool}
          onChange={(v) => {
            setPool(v);
            // Смена пула сбрасывает списки — имена принадлежат другому пулу.
            setMaster([]);
            setReplica([]);
          }}
          clearable
        />
        {pool ? (
          <>
            <OrderedList label="Master writes (write_master)" nodes={poolNodes} value={master} onChange={setMaster} />
            <OrderedList label="Replica writes (write_replica)" nodes={poolNodes} value={replica} onChange={setReplica} />
            <Text c="dimmed" size="xs">
              Different master/replica nodes separate the write flows (a node failure
              with a disk does not kill both); a single node is the common case.
              Order = priority.
            </Text>
          </>
        ) : null}
        {error ? <Text c="red" size="sm">{error}</Text> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button onClick={save} disabled={saving} loading={saving}>Save</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
