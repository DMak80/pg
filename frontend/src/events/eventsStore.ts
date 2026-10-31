// In-memory store ленты событий (u09, D8): массив EventItem, cap 500, новые
// сверху; живёт с открытой вкладкой — перезагрузка = пустая лента (persistence
// нет). Подписка через useSyncExternalStore.
export type EventDomain = 'pg' | 'kafka' | 'valkey' | 's3' | 'etcd';

export type EventEntityKind =
  | 'cluster' | 'shard' | 'node' | 'broker' | 'bucket'
  | 'topic' | 'ticket' | 'heal' | 'evacuation' | 'rotation' | 'instance';

export type EventType = 'state-change' | 'appeared' | 'gone' | 'outcome';

// Событие ленты (arch/03 §3-События): type state-change несёт from→to,
// appeared/gone — появление/исчезновение сущности, outcome — исход заявки.
export interface EventItem {
  id: string;
  tsUnix: number;
  domain: EventDomain;
  entityKind: EventEntityKind;
  entity: string;
  type: EventType;
  from?: string;
  to?: string;
  detail?: string;
}

const CAP = 500;

let events: EventItem[] = [];
const listeners = new Set<() => void>();

export function getEvents(): EventItem[] {
  return events;
}

export function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

// Эмит события: новые сверху, cap 500. Дедуп по id (prev→next-диффы одного
// тика могут повторить событие) — событие с тем же id не перезатирается.
export function emit(item: Omit<EventItem, 'id'> & { id?: string }): void {
  const id = item.id ?? `${item.tsUnix}-${item.domain}-${item.entityKind}-${item.entity}-${item.type}`;
  if (events.some((e) => e.id === id)) return;
  events = [{ ...item, id }, ...events].slice(0, CAP);
  listeners.forEach((l) => l());
}

// Сброс ленты (оператор может очистить вручную; перезагрузка очищает сама).
export function clearEvents(): void {
  events = [];
  listeners.forEach((l) => l());
}
