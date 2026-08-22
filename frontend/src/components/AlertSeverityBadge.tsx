// Цветовая карта severity алертов — единый источник всех панелей (t09 spec §4.12).
// Текст — канон-строка без перевода: идентификатор канона arch/03 §4.
import { Badge } from '@mantine/core';
import type { AlertSeverityName } from '../api/dto';

const SEVERITY_COLORS: Record<AlertSeverityName, string> = {
  critical: 'red',
  warning: 'yellow',
  info: 'gray',
};

export function AlertSeverityBadge({ severity }: { severity: AlertSeverityName }) {
  return <Badge color={SEVERITY_COLORS[severity]} variant="light">{severity}</Badge>;
}
