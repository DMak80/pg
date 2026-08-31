// Бейдж движителя алерта (arch/03 §4.1, task etcd-via-worker-api): кто закроет —
// воркер сам / оператор через API / оператор по runbook. Текст — канон-строка
// без перевода (идентификатор канона), цветовая карта по образцу severity.
import { Badge } from '@mantine/core';
import type { AlertRemedyName } from '../api/dto';

const REMEDY_COLORS: Record<AlertRemedyName, string> = {
  'worker-auto': 'grape',
  'operator-api': 'indigo',
  'operator-runbook': 'orange',
};

const REMEDY_LABELS: Record<AlertRemedyName, string> = {
  'worker-auto': 'воркер закроет',
  'operator-api': 'действие API',
  'operator-runbook': 'runbook',
};

export function AlertRemedyBadge({ remedy }: { remedy: AlertRemedyName }) {
  return (
    <Badge color={REMEDY_COLORS[remedy]} variant="light" title={remedy}>
      {REMEDY_LABELS[remedy]}
    </Badge>
  );
}
