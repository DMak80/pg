// Форма создания s3-кластера (arch/03 §12.3; валидации 02 §12.3): имя,
// secret_profile (текст), платформа read-only «owns3», динамический список
// инстансов 0..9 (имя + placement + ресурсы) с fallback на общие ресурсы.
// Клиентская валидация — зеркало серверной (arch/24 §1.1), сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
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
// в UI — один выбранный атрибут; минимум один обязателен).
type PlacementMode = 'dockerHost' | 'swarmNode';

interface InstanceForm {
  name: string;
  mode: PlacementMode;
  dockerHost: string;
  swarmNode: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}

interface FormState {
  name: string;
  secretProfile: string;
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
  cpu: '',
  memGi: '',
  diskGi: '',
};

const EMPTY: FormState = {
  name: '',
  secretProfile: '',
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

// Placement формы → DTO (минимум один атрибут валидируется выше).
function parsePlacement(instance: InstanceForm): S3PlacementDto {
  return instance.mode === 'dockerHost'
    ? { dockerHost: instance.dockerHost }
    : { swarmNode: instance.swarmNode };
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

  // Валидация-зеркало 02 §12.3 (сервер — источник истины, это UX).
  function validate(): string | null {
    if (!NAME_RE.test(form.name)) return 'имя кластера: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)';
    if (form.secretProfile.trim() === '')
      return 'профиль секретов: непустое имя per-install env-профиля S3Worker';
    if (form.instances.length > 9) return 'инстансов не больше 9';
    for (const [i, inst] of form.instances.entries()) {
      if (!NAME_RE.test(inst.name))
        return `инстанс ${i + 1}: имя [a-z][a-z0-9_]{0,62}`;
      if (inst.name === RESERVED_NAME)
        return `инстанс ${i + 1}: имя orchestrator зарезервировано`;
      // placement — обязательный валидный селектор (минимум один атрибут).
      if (inst.mode === 'dockerHost' && inst.dockerHost.trim() === '')
        return `инстанс ${i + 1}: укажите docker_host`;
      if (inst.mode === 'swarmNode' && inst.swarmNode.trim() === '')
        return `инстанс ${i + 1}: укажите swarm_node`;
      // resources — cpu 0.01..64, mem/disk 1..65536 GiB (пусто = fallback).
      if (inst.cpu !== '' && (Number(inst.cpu) < 0.01 || Number(inst.cpu) > 64))
        return `инстанс ${i + 1}: cpu 0.01..64`;
      if (inst.memGi !== '' && (!Number.isInteger(Number(inst.memGi)) || Number(inst.memGi) < 1 || Number(inst.memGi) > 65536))
        return `инстанс ${i + 1}: memGi — целое 1..65536`;
      if (inst.diskGi !== '' && (!Number.isInteger(Number(inst.diskGi)) || Number(inst.diskGi) < 1 || Number(inst.diskGi) > 65536))
        return `инстанс ${i + 1}: diskGi — целое 1..65536`;
    }

    if (form.commonCpu !== '' && (Number(form.commonCpu) < 0.01 || Number(form.commonCpu) > 64))
      return 'общие ресурсы: cpu 0.01..64';
    if (form.commonMemGi !== '' && (!Number.isInteger(Number(form.commonMemGi)) || Number(form.commonMemGi) < 1 || Number(form.commonMemGi) > 65536))
      return 'общие ресурсы: memGi — целое 1..65536';
    if (form.commonDiskGi !== '' && (!Number.isInteger(Number(form.commonDiskGi)) || Number(form.commonDiskGi) < 1 || Number(form.commonDiskGi) > 65536))
      return 'общие ресурсы: diskGi — целое 1..65536';
    return null;
  }

  function submit() {
    const error = validate();
    setFormError(error);
    if (error !== null) return;
    const common = parseResources(form.commonCpu, form.commonMemGi, form.commonDiskGi);
    const request: CreateS3ClusterRequestDto = {
      name: form.name,
      secretProfile: form.secretProfile.trim(),
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
    <Modal opened={opened} onClose={onClose} title="Создать s3-кластер" centered size="lg">
      <Stack gap="sm">
        <TextInput
          label="Имя кластера"
          placeholder="backups"
          value={form.name}
          onChange={(e) => set('name', e.currentTarget.value)}
          error={formError !== null && formError.startsWith('имя') ? formError : undefined}
        />
        <TextInput
          label="Профиль секретов"
          description="имя per-install env-профиля S3Worker; 422 воркера покажет перечень доступных"
          placeholder="backups-main"
          value={form.secretProfile}
          onChange={(e) => set('secretProfile', e.currentTarget.value)}
        />
        <Text size="sm" c="dimmed">Платформа: owns3 (единственное значение канона)</Text>

        <Card withBorder padding="sm" radius="md">
          <Group justify="space-between" mb="xs">
            <Text size="sm" fw={500}>Инстансы ({form.instances.length})</Text>
            <Group gap="xs">
              <Button
                size="xs"
                variant="default"
                disabled={form.instances.length >= 9}
                onClick={() => set('instances', [...form.instances, { ...EMPTY_INSTANCE }])}
              >
                + инстанс
              </Button>
              <Button
                size="xs"
                variant="default"
                disabled={form.instances.length === 0}
                onClick={() => set('instances', form.instances.slice(0, -1))}
              >
                − инстанс
              </Button>
            </Group>
          </Group>
          <Text size="xs" c="dimmed" mb="sm">
            допустимо 0 инстансов — декларация без развёртывания
          </Text>
          {form.instances.map((inst, index) => (
            <Card key={index} withBorder padding="sm" radius="md" mb="xs">
              <TextInput
                label="Имя инстанса"
                placeholder="node1"
                value={inst.name}
                onChange={(e) => setInstance(index, { name: e.currentTarget.value })}
                mb="sm"
              />
              <Radio.Group
                label="Размещение (placement)"
                value={inst.mode}
                onChange={(v) => setInstance(index, { mode: v as PlacementMode })}
                mb="xs"
              >
                <Group mt="xs">
                  <Radio value="dockerHost" label="docker_host" />
                  <Radio value="swarmNode" label="swarm_node" />
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
              ) : (
                <TextInput
                  label="swarm_node"
                  placeholder="n1"
                  value={inst.swarmNode}
                  onChange={(e) => setInstance(index, { swarmNode: e.currentTarget.value })}
                  mb="sm"
                />
              )}
              <Group grow>
                <NumberInput label="CPU" placeholder="0.5" value={inst.cpu || ''} step={0.1}
                  min={0.01} max={64} onChange={(v) => setInstance(index, { cpu: v === undefined ? '' : String(v) })} />
                <NumberInput label="Память, GiB" placeholder="4" value={inst.memGi || ''}
                  min={1} onChange={(v) => setInstance(index, { memGi: v === undefined ? '' : String(v) })} />
                <NumberInput label="Диск, GiB" placeholder="200" value={inst.diskGi || ''}
                  min={1} onChange={(v) => setInstance(index, { diskGi: v === undefined ? '' : String(v) })} />
              </Group>
            </Card>
          ))}
        </Card>

        <Text size="sm" c="dimmed">Общие ресурсы (fallback для инстансов без своих):</Text>
        <Group grow>
          <NumberInput label="CPU" value={form.commonCpu || ''} step={0.1}
            min={0.01} max={64} onChange={(v) => set('commonCpu', v === undefined ? '' : String(v))} />
          <NumberInput label="Память, GiB" value={form.commonMemGi || ''}
            min={1} onChange={(v) => set('commonMemGi', v === undefined ? '' : String(v))} />
          <NumberInput label="Диск, GiB" value={form.commonDiskGi || ''}
            min={1} onChange={(v) => set('commonDiskGi', v === undefined ? '' : String(v))} />
        </Group>

        {formError !== null && !formError.startsWith('имя') ? (
          <Alert color="red" variant="light">{formError}</Alert>
        ) : null}
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Создать</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
