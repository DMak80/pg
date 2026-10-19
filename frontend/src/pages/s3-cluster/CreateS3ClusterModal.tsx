// Форма создания s3-кластера (arch/03 §12.3; валидации 02 §12.3): имя,
// etcdBackup (переключатель), платформа read-only «owns3», динамический список
// инстансов 0..9 (имя + placement + ресурсы) с fallback на общие ресурсы.
// Клиентская валидация — зеркало серверной (arch/24 §1.1), сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Group,
  Modal,
  NumberInput,
  Radio,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createS3Cluster, s3QueryKeys } from '../../api/queries';
import type { CreateS3ClusterRequestDto, S3PlacementDto, S3ResourcesDto } from '../../api/dto';

// Границы — зеркало валидаций воркера (arch/24 §1.1; 02 §12.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;
const RESERVED_NAME = 'orchestrator';

// Placement-форма инстанса:.radio-режимы канона arch/23 §4 (конъюнкция указанных,
// в UI — один выбранный атрибут; минимум один обязателен). swarm_labels —
// key-value-редактор строк («+ метка»/«−»; минимум одна метка — канон §4).
type PlacementMode = 'dockerHost' | 'swarmNode' | 'swarmLabels';

interface LabelRow {
  key: string;
  value: string;
}

interface InstanceForm {
  name: string;
  mode: PlacementMode;
  dockerHost: string;
  swarmNode: string;
  labels: LabelRow[];
  cpu: string;
  memGi: string;
  diskGi: string;
}

interface FormState {
  name: string;
  etcdBackup: boolean;
  instances: InstanceForm[];
  commonCpu: string;
  commonMemGi: string;
  commonDiskGi: string;
}

const EMPTY_INSTANCE: InstanceForm = {
  name: '',
  mode: 'dockerHost',
  dockerHost: '',
  swarmNode: '',
  labels: [{ key: '', value: '' }],
  cpu: '',
  memGi: '',
  diskGi: '',
};

const EMPTY: FormState = {
  name: '',
  etcdBackup: false,
  instances: [{ ...EMPTY_INSTANCE }],
  commonCpu: '',
  commonMemGi: '',
  commonDiskGi: '',
};

// Ресурсы строки → DTO или null (пусто = общий fallback).
function parseResources(cpu: string, memGi: string, diskGi: string): S3ResourcesDto | null {
  if (cpu === '' && memGi === '' && diskGi === '') return null;
  return {
    cpu: cpu === '' ? null : Number(cpu),
    memGi: memGi === '' ? null : Number(memGi),
    diskGi: diskGi === '' ? null : Number(diskGi),
  };
}

// Placement формы → DTO (минимум один атрибут валидируется выше); строки
// с пустым ключом в swarm_labels не включаются.
function parsePlacement(instance: InstanceForm): S3PlacementDto {
  if (instance.mode === 'dockerHost') return { dockerHost: instance.dockerHost };
  if (instance.mode === 'swarmNode') return { swarmNode: instance.swarmNode };
  const labels = Object.fromEntries(instance.labels
    .filter((l) => l.key.trim() !== '')
    .map((l) => [l.key.trim(), l.value]));
  return { swarmLabels: labels };
}

export function CreateS3ClusterModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<FormState>(EMPTY);
  const [formError, setFormError] = useState<string | null>(null);
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createS3Cluster,
    onSuccess: async () => {
      setForm(EMPTY);
      setFormError(null);
      onClose();
      // Список обновит следующий тик s3-refresher'а — инвалидация ключа ускоряет.
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.clusters });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  function setInstance(index: number, patch: Partial<InstanceForm>) {
    setForm((f) => ({
      ...f,
      instances: f.instances.map((inst, i) => (i === index ? { ...inst, ...patch } : inst)),
    }));
  }

  // key-value-редактор swarm_labels инстанса.
  function setLabel(instanceIndex: number, labelIndex: number, patch: Partial<LabelRow>) {
    setForm((f) => ({
      ...f,
      instances: f.instances.map((inst, i) => (i === instanceIndex
        ? { ...inst, labels: inst.labels.map((l, j) => (j === labelIndex ? { ...l, ...patch } : l)) }
        : inst)),
    }));
  }

  // Валидация-зеркало 02 §12.3 (сервер — источник истины, это UX).
  function validate(): string | null {
    if (!NAME_RE.test(form.name)) return 'cluster name: [a-z][a-z0-9_]{0,62} (lowercase, no hyphen)';
    if (form.instances.length > 9) return 'at most 9 instances';
    for (const [i, inst] of form.instances.entries()) {
      if (!NAME_RE.test(inst.name))
        return `instance ${i + 1}: name [a-z][a-z0-9_]{0,62}`;
      if (inst.name === RESERVED_NAME)
        return `instance ${i + 1}: the orchestrator name is reserved`;
      // placement — обязательный валидный селектор (минимум один атрибут).
      if (inst.mode === 'dockerHost' && inst.dockerHost.trim() === '')
        return `instance ${i + 1}: specify docker_host`;
      if (inst.mode === 'swarmNode' && inst.swarmNode.trim() === '')
        return `instance ${i + 1}: specify swarm_node`;
      if (inst.mode === 'swarmLabels' && !inst.labels.some((l) => l.key.trim() !== ''))
        return `instance ${i + 1}: add at least one swarm_labels label`;
      // resources — cpu 0.01..64, mem/disk 1..65536 GiB (пусто = fallback).
      if (inst.cpu !== '' && (Number(inst.cpu) < 0.01 || Number(inst.cpu) > 64))
        return `instance ${i + 1}: cpu 0.01..64`;
      if (inst.memGi !== '' && (!Number.isInteger(Number(inst.memGi)) || Number(inst.memGi) < 1 || Number(inst.memGi) > 65536))
        return `instance ${i + 1}: memGi — integer 1..65536`;
      if (inst.diskGi !== '' && (!Number.isInteger(Number(inst.diskGi)) || Number(inst.diskGi) < 1 || Number(inst.diskGi) > 65536))
        return `instance ${i + 1}: diskGi — integer 1..65536`;
    }

    if (form.commonCpu !== '' && (Number(form.commonCpu) < 0.01 || Number(form.commonCpu) > 64))
      return 'common resources: cpu 0.01..64';
    if (form.commonMemGi !== '' && (!Number.isInteger(Number(form.commonMemGi)) || Number(form.commonMemGi) < 1 || Number(form.commonMemGi) > 65536))
      return 'common resources: memGi — integer 1..65536';
    if (form.commonDiskGi !== '' && (!Number.isInteger(Number(form.commonDiskGi)) || Number(form.commonDiskGi) < 1 || Number(form.commonDiskGi) > 65536))
      return 'common resources: diskGi — integer 1..65536';
    return null;
  }

  function submit() {
    const error = validate();
    setFormError(error);
    if (error !== null) return;
    const common = parseResources(form.commonCpu, form.commonMemGi, form.commonDiskGi);
    const request: CreateS3ClusterRequestDto = {
      name: form.name,
      etcdBackup: form.etcdBackup,
      instances: form.instances.map((inst) => ({
        name: inst.name,
        placement: parsePlacement(inst),
        resources: parseResources(inst.cpu, inst.memGi, inst.diskGi),
      })),
      resources: common,
    };
    mutation.mutate(request);
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Create S3 cluster" centered size="lg">
      <Stack gap="sm">
        <TextInput
          label="Cluster name"
          placeholder="backups"
          value={form.name}
          onChange={(e) => set('name', e.currentTarget.value)}
          error={formError !== null && formError.startsWith('cluster name') ? formError : undefined}
        />
        <Checkbox
          label="Etcd backup cluster"
          description="provision this cluster as the installation's etcd backup target; requires the worker's S3W_ETCD_BACKUP_* env pair; at most one per installation"
          checked={form.etcdBackup}
          onChange={(e) => set('etcdBackup', e.currentTarget.checked)}
        />
        <Text size="sm" c="dimmed">Platform: owns3 (the only canonical value)</Text>

        <Card withBorder padding="sm" radius="md">
          <Group justify="space-between" mb="xs">
            <Text size="sm" fw={500}>Instances ({form.instances.length})</Text>
            <Group gap="xs">
              <Button
                size="xs"
                variant="default"
                disabled={form.instances.length >= 9}
                onClick={() => set('instances', [...form.instances, { ...EMPTY_INSTANCE }])}
              >
                + instance
              </Button>
              <Button
                size="xs"
                variant="default"
                disabled={form.instances.length === 0}
                onClick={() => set('instances', form.instances.slice(0, -1))}
              >
                − instance
              </Button>
            </Group>
          </Group>
          <Text size="xs" c="dimmed" mb="sm">
            0 instances is allowed — a declaration without deployment
          </Text>
          {form.instances.map((inst, index) => (
            <Card key={index} withBorder padding="sm" radius="md" mb="xs">
              <TextInput
                label="Instance name"
                placeholder="node1"
                value={inst.name}
                onChange={(e) => setInstance(index, { name: e.currentTarget.value })}
                mb="sm"
              />
              <Radio.Group
                label="Placement"
                value={inst.mode}
                onChange={(v) => setInstance(index, { mode: v as PlacementMode })}
                mb="xs"
              >
                <Group mt="xs">
                  <Radio value="dockerHost" label="docker_host" />
                  <Radio value="swarmNode" label="swarm_node" />
                  <Radio value="swarmLabels" label="swarm_labels" />
                </Group>
              </Radio.Group>
              {inst.mode === 'dockerHost' ? (
                <TextInput
                  label="docker_host"
                  placeholder="h1"
                  value={inst.dockerHost}
                  onChange={(e) => setInstance(index, { dockerHost: e.currentTarget.value })}
                  mb="sm"
                />
              ) : inst.mode === 'swarmNode' ? (
                <TextInput
                  label="swarm_node"
                  placeholder="n1"
                  value={inst.swarmNode}
                  onChange={(e) => setInstance(index, { swarmNode: e.currentTarget.value })}
                  mb="sm"
                />
              ) : (
                <Stack gap="xs" mb="sm">
                  {inst.labels.map((label, j) => (
                    <Group key={j} align="flex-end" gap="xs">
                      <TextInput
                        label={j === 0 ? 'Label: key' : undefined}
                        placeholder="geo"
                        value={label.key}
                        onChange={(e) => setLabel(index, j, { key: e.currentTarget.value })}
                        maw={160}
                      />
                      <TextInput
                        label={j === 0 ? 'value' : undefined}
                        placeholder="dc1"
                        value={label.value}
                        onChange={(e) => setLabel(index, j, { value: e.currentTarget.value })}
                        maw={160}
                      />
                      <Button
                        variant="default"
                        size="xs"
                        mb={4}
                        disabled={inst.labels.length === 1}
                        onClick={() => setInstance(index, {
                          labels: inst.labels.filter((_, k) => k !== j),
                        })}
                      >
                        −
                      </Button>
                    </Group>
                  ))}
                  <Button
                    variant="default"
                    size="xs"
                    onClick={() => setInstance(index, {
                      labels: [...inst.labels, { key: '', value: '' }],
                    })}
                  >
                    + label
                  </Button>
                </Stack>
              )}
              <Group grow>
                <NumberInput label="CPU" placeholder="0.5" value={inst.cpu || ''} step={0.1}
                  min={0.01} max={64} onChange={(v) => setInstance(index, { cpu: v === undefined ? '' : String(v) })} />
                <NumberInput label="Memory, GiB" placeholder="4" value={inst.memGi || ''}
                  min={1} onChange={(v) => setInstance(index, { memGi: v === undefined ? '' : String(v) })} />
                <NumberInput label="Disk, GiB" placeholder="200" value={inst.diskGi || ''}
                  min={1} onChange={(v) => setInstance(index, { diskGi: v === undefined ? '' : String(v) })} />
              </Group>
            </Card>
          ))}
        </Card>

        <Text size="sm" c="dimmed">Common resources (fallback for instances without their own):</Text>
        <Group grow>
          <NumberInput label="CPU" value={form.commonCpu || ''} step={0.1}
            min={0.01} max={64} onChange={(v) => set('commonCpu', v === undefined ? '' : String(v))} />
          <NumberInput label="Memory, GiB" value={form.commonMemGi || ''}
            min={1} onChange={(v) => set('commonMemGi', v === undefined ? '' : String(v))} />
          <NumberInput label="Disk, GiB" value={form.commonDiskGi || ''}
            min={1} onChange={(v) => set('commonDiskGi', v === undefined ? '' : String(v))} />
        </Group>

        {formError !== null && !formError.startsWith('cluster name') ? (
          <Alert color="red" variant="light">{formError}</Alert>
        ) : null}
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button loading={mutation.isPending} onClick={submit}>Create</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
