import { useState } from 'react';
import { Badge, Button, Checkbox, Group, Modal, NumberInput, Select, Stack, Switch, Text } from '@mantine/core';
import { updateBackupsPolicy } from '../../api/queries';
import type { BackupPolicyDto, S3PoolDto } from '../../api/dto';

// Единая форма «Policy бэкапов кластера» (u16/D14, arch/adminpanel/03 §3.8):
// retention + full_max_age_sec + verify + drill + garbage_min_age_sec + секция
// s3 — ОДНИМ телом (канон замещения целиком, 02 §9.12); кнопка на
// /backups-storage per-кластер и на вкладке «Бэкапы (срез)» pg-кластера.
interface Props {
  cluster: string;
  policy: BackupPolicyDto | null;
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

export function PolicyBackupsModal({ cluster, policy, pools, onClose, onSaved }: Props) {
  const [pool, setPool] = useState<string | null>(policy?.s3?.pool ?? null);
  const [master, setMaster] = useState<string[]>(policy?.s3?.writeMaster ?? []);
  const [replica, setReplica] = useState<string[]>(policy?.s3?.writeReplica ?? []);
  // u16: полный набор policy-полей (из policy-ключа или дефолты формы).
  const [retentionDays, setRetentionDays] = useState(policy?.retentionDays ?? 1);
  const [retentionWeeks, setRetentionWeeks] = useState(policy?.retentionWeeks ?? 1);
  const [retentionMonths, setRetentionMonths] = useState(policy?.retentionMonths ?? 1);
  const [fullMaxAgeSec, setFullMaxAgeSec] = useState(policy?.fullMaxAgeSec ?? 86400);
  const [verifyOnCreate, setVerifyOnCreate] = useState(policy?.verifyOnCreate ?? true);
  const [drillIntervalDays, setDrillIntervalDays] = useState(policy?.drillIntervalDays ?? 1);
  const [garbageMinAgeSec, setGarbageMinAgeSec] = useState<number | null>(
    policy?.garbageMinAgeSec ?? null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const poolNodes = pools.find((p) => p.name === pool)?.nodes ?? [];

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      // Полный policy (замещение целиком — 02 §9.12): все поля формы; s3 —
      // только при выбранном пуле (сброс — БЕЗ секции); garbageMinAgeSec null —
      // поле не шлётся (дефолт воркера 600 с). Валидация ≥600 — граница A1
      // (зеркало панели, истина — сервер воркера).
      if (fullMaxAgeSec < 600) { setError('fullMaxAgeSec ≥ 600'); return; }
      if (garbageMinAgeSec !== null && garbageMinAgeSec < 600) {
        setError('garbageMinAgeSec ≥ 600');
        return;
      }
      await updateBackupsPolicy(cluster, {
        retentionDays,
        retentionWeeks,
        retentionMonths,
        fullMaxAgeSec,
        verifyOnCreate,
        drillIntervalDays,
        garbageMinAgeSec,
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
    <Modal opened onClose={onClose} title={`Policy бэкапов — ${cluster}`} size="lg">
      <Stack gap="md">
        {/* u16/D14: полный набор policy-полей одним телом (замещение целиком) */}
        <Group grow>
          <NumberInput label="Retention, дней" min={1} value={retentionDays}
            onChange={(v) => setRetentionDays(Number(v ?? 1))} />
          <NumberInput label="Retention, недель" min={0} value={retentionWeeks}
            onChange={(v) => setRetentionWeeks(Number(v ?? 0))} />
          <NumberInput label="Retention, месяцев" min={0} value={retentionMonths}
            onChange={(v) => setRetentionMonths(Number(v ?? 0))} />
        </Group>
        <Group grow>
          <NumberInput label="Полный не старше (сек, ≥600)" min={600} value={fullMaxAgeSec}
            onChange={(v) => setFullMaxAgeSec(Number(v ?? 86400))} />
          <NumberInput label="Дрилл каждые N суток (0 — выкл)" min={0} max={3650} value={drillIntervalDays}
            onChange={(v) => setDrillIntervalDays(Number(v ?? 0))} />
          <NumberInput label="Мин. возраст мусора (сек, ≥600)" min={600} value={garbageMinAgeSec ?? ''}
            onChange={(v) => setGarbageMinAgeSec(v === '' ? null : Number(v))}
            description="пусто — дефолт воркера (600 с)" />
        </Group>
        <Switch
          label="Verify при создании (on_create)"
          checked={verifyOnCreate}
          onChange={(e) => setVerifyOnCreate(e.currentTarget.checked)}
        />
        <Select
          label="Пул"
          placeholder="Не выбрано (бэкапы кластера выключены)"
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
