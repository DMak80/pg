// Чистые функции diff prev→next снимков опрашиваемых ключей (u09, D8): события
// из ПОЛНЫХ ответов polling'а. Что считаем (arch/03 §3-События): переходы
// state кластеров/нод/шардов/брокеров/бакетов/инстансов s3 (вкл. исчезновение =
// удалена); заявки — появление «поставлена» / исчезновение «исполнена или
// отвергнута»; heals — новые записи; эвакуации — появление/изменение/исчезновение.
// freshness-события НЕ эмитим (шум).
import type {
  BackupStorageDto,
  BucketDto,
  ClusterDto,
  ClusterSummaryDto,
  KafkaClusterDto,
  OverviewDto,
  S3ClusterDto,
  ValkeyClusterDto,
  HaScopeSummaryDto,
  MoveTicketDto,
} from '../api/dto';
import { emit } from './eventsStore';
import type { EventItem } from './eventsStore';

const now = (): number => Math.floor(Date.now() / 1000);

// ——— pg: список кластеров (state-переходы) ———

export function diffClusters(prev: ClusterSummaryDto[] | undefined, next: ClusterSummaryDto[]): void {
  if (prev === undefined) return;
  const prevByName = new Map(prev.map((c) => [c.name, c]));
  for (const c of next) {
    const before = prevByName.get(c.name);
    if (before === undefined) {
      emit({ tsUnix: now(), domain: 'pg', entityKind: 'cluster', entity: c.name, type: 'appeared' });
      continue;
    }
    const stateOf = (x: ClusterSummaryDto): string =>
      x.toRemove ? 'TO_REMOVE' : x.notInitialized ? 'NOT_INITIALIZED' : 'ACTIVE';
    if (stateOf(before) !== stateOf(c)) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'cluster', entity: c.name,
        type: 'state-change', from: stateOf(before), to: stateOf(c),
      });
    }
  }
  for (const c of prev) {
    if (!next.some((x) => x.name === c.name)) {
      emit({ tsUnix: now(), domain: 'pg', entityKind: 'cluster', entity: c.name, type: 'gone' });
    }
  }
}

// ——— pg: детали кластера (шарды/бакеты — state-переходы, заявки переездов) ———

export function diffClusterDetails(prev: ClusterDto | undefined, next: ClusterDto): void {
  if (prev === undefined) return;
  const cluster = next.name;

  // Шарды: state ACTIVE↔TO_REMOVE.
  const prevShards = new Map(prev.shards.map((x) => [x.name, x]));
  for (const shard of next.shards) {
    const before = prevShards.get(shard.name);
    if (before !== undefined && before.state !== shard.state) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'shard', entity: `${cluster}/${shard.name}`,
        type: 'state-change', from: before.state, to: shard.state,
      });
    }
  }

  // Бакеты: state-переходы (ACTIVE↔SYNCING/FROZEN/ABORTING/NOT_INITIALIZED).
  const prevBuckets = new Map(prev.buckets.map((x) => [x.id, x]));
  for (const bucket of next.buckets) {
    const before: BucketDto | undefined = prevBuckets.get(bucket.id);
    if (before !== undefined && before.state !== bucket.state) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'bucket', entity: `${cluster}/bucket_${bucket.id}`,
        type: 'state-change', from: before.state, to: bucket.state,
        detail: bucket.move !== null ? `${bucket.move.owner ?? '—'} → ${bucket.move.target ?? '—'}` : undefined,
      });
    }
  }

  // Заявки переездов: появление = «поставлена», исчезновение = «исполнена или отвергнута».
  diffTickets(prev.pendingMoves, next.pendingMoves, (t) => `${t.bucket}`, (t) => t.op, cluster);
}

function diffTickets(
  prev: MoveTicketDto[], next: MoveTicketDto[],
  keyOf: (t: MoveTicketDto) => string, opOf: (t: MoveTicketDto) => string, cluster: string,
): void {
  const prevKeys = new Set(prev.map(keyOf));
  const nextKeys = new Set(next.map(keyOf));
  for (const t of next) {
    if (!prevKeys.has(keyOf(t))) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'ticket', entity: `${cluster}/${keyOf(t)}`,
        type: 'appeared', detail: `заявка ${opOf(t)} поставлена`,
      });
    }
  }
  for (const t of prev) {
    if (!nextKeys.has(keyOf(t))) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'ticket', entity: `${cluster}/${keyOf(t)}`,
        type: 'gone', detail: `заявка ${opOf(t)} исполнена или отвергнута`,
      });
    }
  }
}

// ——— pg: heals (новые записи по ts) + эвакуации ———

export function diffClusterExtras(
  prev: ClusterDto | undefined, next: ClusterDto,
): void {
  if (prev === undefined) return;

  // Heals: записи с ts, которых не было раньше.
  const prevHealTs = new Set(prev.heals.map((h) => `${h.bucket}-${h.tsUnix}`));
  for (const heal of next.heals) {
    if (!prevHealTs.has(`${heal.bucket}-${heal.tsUnix}`)) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'heal', entity: `${next.name}/${heal.bucket}`,
        type: 'appeared', detail: `${heal.was ?? '—'} → ${heal.now ?? '—'}: ${heal.reason ?? ''}`,
      });
    }
  }

  // Эвакуации (u12): появление/изменение/исчезновение.
  const prevEvac = new Map((prev.evacuations ?? []).map((e) => [e.shard, e]));
  const nextEvac = next.evacuations ?? [];
  for (const e of nextEvac) {
    const before = prevEvac.get(e.shard);
    if (before === undefined) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'evacuation', entity: `${next.name}/${e.shard}`,
        type: 'appeared', detail: `эвакуация: ${e.reason}, state ${e.state}`,
      });
    } else if (before.state !== e.state || before.returnedUnix !== e.returnedUnix) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'evacuation', entity: `${next.name}/${e.shard}`,
        type: 'state-change', from: before.state, to: e.state,
        detail: e.returnedUnix !== null ? 'шард возвращён' : undefined,
      });
    }
  }
  for (const e of prev.evacuations ?? []) {
    if (!nextEvac.some((x) => x.shard === e.shard)) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'evacuation', entity: `${next.name}/${e.shard}`,
        type: 'gone', detail: 'журнал эвакуации исчез (демонтаж шарда)',
      });
    }
  }
}

// ——— HA: скопы (лидер — state-переход) ———

export function diffHa(prev: HaScopeSummaryDto[] | undefined, next: HaScopeSummaryDto[]): void {
  if (prev === undefined) return;
  const prevByScope = new Map(prev.map((s) => [s.scope, s]));
  for (const scope of next) {
    const before = prevByScope.get(scope.scope);
    if (before === undefined) continue;
    if (before.leaderName !== scope.leaderName) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'cluster',
        entity: scope.scope, type: 'state-change',
        from: before.leaderName ?? 'no leader', to: scope.leaderName ?? 'no leader',
        detail: 'лидер HA-скопа сменился',
      });
    }
  }
}

// ——— kafka: кластеры/брокеры/топики + тикеты + исходы ———

export function diffKafka(
  prev: KafkaClusterDto | undefined, next: KafkaClusterDto,
): void {
  if (prev === undefined) return;
  const cluster = next.name;

  // Брокеры: state-переходы.
  const prevBrokers = new Map(prev.brokersList.map((b) => [b.name, b]));
  for (const broker of next.brokersList) {
    const before = prevBrokers.get(broker.name);
    if (before !== undefined && before.state !== broker.state) {
      emit({
        tsUnix: now(), domain: 'kafka', entityKind: 'broker', entity: `${cluster}/${broker.name}`,
        type: 'state-change', from: before.state ?? '—', to: broker.state ?? '—',
      });
    }
  }

  // Топики: появление/исчезновение + missing-переход.
  const prevTopics = new Map(prev.topics.map((t) => [t.name, t]));
  for (const topic of next.topics) {
    const before = prevTopics.get(topic.name);
    if (before === undefined) {
      emit({ tsUnix: now(), domain: 'kafka', entityKind: 'topic', entity: `${cluster}/${topic.name}`, type: 'appeared' });
    } else if (before.missing !== topic.missing) {
      emit({
        tsUnix: now(), domain: 'kafka', entityKind: 'topic', entity: `${cluster}/${topic.name}`,
        type: 'state-change', from: before.missing ? 'missing' : 'present',
        to: topic.missing ? 'missing' : 'present',
      });
    }
  }
  for (const topic of prev.topics) {
    if (!next.topics.some((t) => t.name === topic.name)) {
      emit({ tsUnix: now(), domain: 'kafka', entityKind: 'topic', entity: `${cluster}/${topic.name}`, type: 'gone' });
    }
  }

  // Заявки: ротации/rebalance — появление/исчезновение.
  diffTicketLike(prev.rotation !== null, next.rotation !== null, cluster, 'rotation', 'app-пароль', 'kafka');
  diffTicketLike((prev.adminRotation ?? null) !== null, (next.adminRotation ?? null) !== null, cluster, 'rotation', 'admin-пароль', 'kafka');
  diffTicketLike(prev.caRotation !== null, next.caRotation !== null, cluster, 'rotation', 'CA', 'kafka');
  diffTicketLike(prev.rebalance !== null, next.rebalance !== null, cluster, 'ticket', 'rebalance', 'kafka');

  // Исход заявки — напрямую (ticket_outcomes).
  if (prev.ticketOutcome === null && next.ticketOutcome !== null) {
    emit({
      tsUnix: now(), domain: 'kafka', entityKind: 'ticket', entity: cluster,
      type: 'outcome', to: next.ticketOutcome.outcome,
      detail: `${next.ticketOutcome.kind}: ${next.ticketOutcome.reason ?? ''}`,
    });
  }
}

// ——— valkey: нода/ротации/исходы ———

export function diffValkey(prev: ValkeyClusterDto | undefined, next: ValkeyClusterDto): void {
  if (prev === undefined) return;
  const cluster = next.name;

  const prevNode = prev.nodesList[0];
  const node = next.nodesList[0];
  if (prevNode !== undefined && node !== undefined && prevNode.state !== node.state) {
    emit({
      tsUnix: now(), domain: 'valkey', entityKind: 'node', entity: `${cluster}/${node.name}`,
      type: 'state-change', from: prevNode.state ?? '—', to: node.state ?? '—',
    });
  }

  diffTicketLike(prev.rotation !== null, next.rotation !== null, cluster, 'rotation', 'пароль', 'valkey');
  diffTicketLike(prev.caRotation !== null, next.caRotation !== null, cluster, 'rotation', 'CA', 'valkey');

  if (prev.ticketOutcome === null && next.ticketOutcome !== null) {
    emit({
      tsUnix: now(), domain: 'valkey', entityKind: 'ticket', entity: cluster,
      type: 'outcome', to: next.ticketOutcome.outcome,
      detail: `${next.ticketOutcome.kind}: ${next.ticketOutcome.reason ?? ''}`,
    });
  }
}

// ——— s3: кластер/ноды/ротация ———

export function diffS3(prev: S3ClusterDto | undefined, next: S3ClusterDto): void {
  if (prev === undefined) return;
  const cluster = next.name;

  if (prev.state !== next.state) {
    emit({
      tsUnix: now(), domain: 's3', entityKind: 'cluster', entity: cluster,
      type: 'state-change', from: prev.state, to: next.state,
    });
  }

  const prevNodes = new Map(prev.nodesList.map((n) => [n.name, n]));
  for (const node of next.nodesList) {
    const before = prevNodes.get(node.name);
    if (before === undefined) {
      emit({ tsUnix: now(), domain: 's3', entityKind: 'node', entity: `${cluster}/${node.name}`, type: 'appeared' });
    } else if (before.state !== node.state) {
      emit({
        tsUnix: now(), domain: 's3', entityKind: 'node', entity: `${cluster}/${node.name}`,
        type: 'state-change', from: before.state ?? '—', to: node.state ?? '—',
      });
    }
  }
  for (const node of prev.nodesList) {
    if (!next.nodesList.some((n) => n.name === node.name)) {
      emit({ tsUnix: now(), domain: 's3', entityKind: 'node', entity: `${cluster}/${node.name}`, type: 'gone' });
    }
  }

  diffTicketLike(prev.rotation !== null, next.rotation !== null, cluster, 'rotation', 'секреты', 's3');
}

// Заявка-like: появление = «поставлена», исчезновение = «исполнена или
// отвергнута» (detail по work.lastError при наличии — передаёт вызывающий).
function diffTicketLike(
  hadBefore: boolean, hasNow: boolean,
  cluster: string, entityKind: 'rotation' | 'ticket', label: string,
  domain: EventItem['domain'],
): void {
  if (!hadBefore && hasNow) {
    emit({
      tsUnix: now(), domain, entityKind, entity: `${cluster}/${label}`,
      type: 'appeared', detail: `заявка ${label} поставлена`,
    });
  } else if (hadBefore && !hasNow) {
    emit({
      tsUnix: now(), domain, entityKind, entity: `${cluster}/${label}`,
      type: 'gone', detail: `заявка ${label} исполнена или отвергнута`,
    });
  }
}

// ——— backups-storage: restore/drill факты шардов + сироты (FINDING 1, u09;
// arch/03 §3 «События» прямо перечисляет backups-storage среди наблюдаемых
// ключей). restore: появление state = «заявка поставлена», исчезновение =
// «исполнена или отвергнута» (gone — исход), смена state = state-change;
// drill: появление RUNNING = appeared, терминальный исход = outcome;
// сироты: появление/исчезновение префикса + смена state (OBSERVED→DELETING). ———

export function diffBackupsStorage(
  prev: BackupStorageDto | undefined, next: BackupStorageDto,
): void {
  if (prev === undefined) return;

  // restore/drill per-shard: ключи сводки clusters[].shards[].
  interface ShardFact { restoreState: string | null; drillState: string | null }
  const factsOf = (dto: BackupStorageDto): Map<string, ShardFact> => {
    const map = new Map<string, ShardFact>();
    for (const cluster of dto.clusters) {
      for (const shard of cluster.shards) {
        map.set(`${shard.cluster}/${shard.shard}`, {
          restoreState: shard.restoreState ?? null,
          drillState: shard.drillState ?? null,
        });
      }
    }
    return map;
  };
  const before = factsOf(prev);
  const after = factsOf(next);

  for (const [key, was] of before) {
    const nowFact = after.get(key);
    if (nowFact === undefined) continue; // шард исчез из сводки — не событие шарда

    // restore-заявка: активный restore появился / сменил фазу / завершился.
    if (was.restoreState === null && nowFact.restoreState !== null) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'restore', entity: key,
        type: 'appeared', detail: `restore-заявка поставлена (${nowFact.restoreState})`,
      });
    } else if (was.restoreState !== null && nowFact.restoreState === null) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'restore', entity: key,
        type: 'gone', detail: `restore исполнен или отклонён (было ${was.restoreState})`,
      });
    } else if (was.restoreState !== null
      && nowFact.restoreState !== null
      && was.restoreState !== nowFact.restoreState) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'restore', entity: key,
        type: 'state-change', from: was.restoreState, to: nowFact.restoreState,
      });
    }

    // drill: появление RUNNING; терминальный исход (RUNNING → SUCCEEDED/FAILED);
    // новый FAILED после SUCCEEDED — outcome тоже (восстановимость не доказана).
    if (was.drillState !== 'RUNNING' && nowFact.drillState === 'RUNNING') {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'drill', entity: key,
        type: 'appeared', detail: 'drill запущен',
      });
    } else if (was.drillState === 'RUNNING'
      && (nowFact.drillState === 'SUCCEEDED' || nowFact.drillState === 'FAILED')) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'drill', entity: key,
        type: 'outcome', to: nowFact.drillState,
        detail: `drill ${nowFact.drillState === 'SUCCEEDED' ? 'подтвердил' : 'не подтвердил'} восстановимость`,
      });
    }
  }

  // Сироты: появление/исчезновение префикса + смена state.
  const orphansOf = (dto: BackupStorageDto): Map<string, string | null> => {
    const map = new Map<string, string | null>();
    for (const o of dto.orphans) map.set(o.prefix, o.registryState ?? null);
    return map;
  };
  const orphansBefore = orphansOf(prev);
  const orphansAfter = orphansOf(next);
  for (const [prefix, state] of orphansAfter) {
    const prevState = orphansBefore.get(prefix);
    if (prevState === undefined) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'orphan', entity: prefix,
        type: 'appeared', detail: 'сирота обнаружена (S3-префикс без владельца в etcd)',
      });
    } else if (prevState !== state && state !== null && prevState !== null) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'orphan', entity: prefix,
        type: 'state-change', from: prevState, to: state,
      });
    }
  }
  for (const prefix of orphansBefore.keys()) {
    if (!orphansAfter.has(prefix)) {
      emit({
        tsUnix: now(), domain: 'pg', entityKind: 'orphan', entity: prefix,
        type: 'gone', detail: 'сирота удалена (sweeper воркера)',
      });
    }
  }
}

// ——— overview: глобальный стоп бэкапов (u10) — появление/снятие ключа
// /pgworker/backups-disabled. Остальные агрегаты overview не эмитим
// (кластеры/HA уже покрыты своими ключами, freshness — шум). ———

export function diffOverview(prev: OverviewDto | undefined, next: OverviewDto): void {
  // u09/D8: событие = изменение МЕЖДУ тиками; при первом тике (prev нет —
  // снапшот открытой вкладки ещё не собран) все диффы молчат — иначе ложное
  // appeared на уже стоявший стоп при открытии панели.
  if (prev === undefined) return;
  const was = prev.backupsDisabled ?? null;
  const isNow = next.backupsDisabled ?? null;
  if (was === null && isNow !== null) {
    emit({
      tsUnix: now(), domain: 'pg', entityKind: 'ticket', entity: 'backups-disabled',
      type: 'appeared', detail: 'глобальный стоп бэкапов: приёмники WAL всех кластеров замирают',
    });
  } else if (was !== null && isNow === null) {
    emit({
      tsUnix: now(), domain: 'pg', entityKind: 'ticket', entity: 'backups-disabled',
      type: 'gone', detail: 'глобальный стоп снят — бэкапы возобновлены',
    });
  }
}
