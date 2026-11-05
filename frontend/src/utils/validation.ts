// Единый источник границ валидации форм (D17, u02): значения дословно из
// arch/adminpanel/02 §9.3/§10.3/§11.3/§12.3. Сервер остаётся источником
// истины (ProblemDetails по полям) — этот модуль — UX-зеркало для быстрых
// ошибок у поля. Размещение-селекторы s3 в LIMITS не входят: placement —
// не числовая граница, его валидация (минимум один атрибут selector'а)
// остаётся в S3-компонентах (CreateS3ClusterModal/AddNodeModal) как есть —
// осознанное решение re-плана (уточнение 2).
export const CLUSTER_NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

export const LIMITS = {
  cpu: { min: 0.01, max: 64 },
  memGi: { min: 1, max: 65536 },
  diskGi: { min: 1, max: 65536 },
  replicas: { min: 1, max: 26 },
  buckets: { min: 1, max: 8192 },
  shards: { min: 1, max: 128 },
  kafkaBrokers: { min: 1, max: 9 },
  kafkaRf: { min: 1, max: 9 },
  kafkaPartitions: { min: 1, max: 1000 },
  kafkaRetentionMs: { min: 1, max: 2147483647 },
  valkeyMaxmemoryBytes: { min: 1 },
  s3Instances: { min: 0, max: 9 },
} as const;

// 8 значений policy valkey — канон arch/02 §11.3 (arch/valkey-clusters/02-keys.md §1).
export const VALKEY_POLICIES = [
  'noeviction',
  'allkeys-lru',
  'allkeys-lfu',
  'allkeys-random',
  'volatile-lru',
  'volatile-lfu',
  'volatile-random',
  'volatile-ttl',
] as const;

// Каждая функция возвращает строку ошибки (RU) или null — валидно.

export function validateCpu(v: number): string | null {
  if (v < LIMITS.cpu.min || v > LIMITS.cpu.max) {
    return `CPU: ${LIMITS.cpu.min}..${LIMITS.cpu.max} ядер`;
  }
  return null;
}

export function validateMemGi(v: number): string | null {
  if (v < LIMITS.memGi.min || v > LIMITS.memGi.max) {
    return `Память: ${LIMITS.memGi.min}..${LIMITS.memGi.max} GiB`;
  }
  return null;
}

export function validateDiskGi(v: number): string | null {
  if (v < LIMITS.diskGi.min || v > LIMITS.diskGi.max) {
    return `Диск: ${LIMITS.diskGi.min}..${LIMITS.diskGi.max} GiB`;
  }
  return null;
}

export function validateReplicas(v: number): string | null {
  if (v < LIMITS.replicas.min || v > LIMITS.replicas.max) {
    return `Реплики: ${LIMITS.replicas.min}..${LIMITS.replicas.max} (буквы нод a..z)`;
  }
  return null;
}

export function validateBuckets(v: number): string | null {
  if (v < LIMITS.buckets.min || v > LIMITS.buckets.max) {
    return `Бакеты: ${LIMITS.buckets.min}..${LIMITS.buckets.max}`;
  }
  return null;
}

export function validateShards(v: number, buckets?: number): string | null {
  if (v < LIMITS.shards.min || v > LIMITS.shards.max) {
    return `Шарды: ${LIMITS.shards.min}..${LIMITS.shards.max}`;
  }
  if (buckets !== undefined && v > buckets) {
    return 'Шарды не больше бакетов';
  }
  return null;
}

export function validateKafkaBrokers(v: number): string | null {
  if (v < LIMITS.kafkaBrokers.min || v > LIMITS.kafkaBrokers.max) {
    return `Брокеры: ${LIMITS.kafkaBrokers.min}..${LIMITS.kafkaBrokers.max}`;
  }
  return null;
}

export function validateKafkaRf(v: number, brokers?: number): string | null {
  if (v < LIMITS.kafkaRf.min || v > LIMITS.kafkaRf.max) {
    return `RF: ${LIMITS.kafkaRf.min}..${LIMITS.kafkaRf.max}`;
  }
  if (brokers !== undefined && v > brokers) {
    return 'RF не больше числа брокеров';
  }
  return null;
}

// minISR: 1..RF (arch/02 §10.3).
export function validateMinIsr(rf: number, minIsr: number): string | null {
  if (minIsr < 1 || minIsr > rf) {
    return `Min ISR: 1..RF (RF = ${rf})`;
  }
  return null;
}

export function validatePartitions(v: number): string | null {
  if (v < LIMITS.kafkaPartitions.min || v > LIMITS.kafkaPartitions.max) {
    return `Партиции: ${LIMITS.kafkaPartitions.min}..${LIMITS.kafkaPartitions.max}`;
  }
  return null;
}

export function validateRetentionMs(v: number): string | null {
  if (v < LIMITS.kafkaRetentionMs.min || v > LIMITS.kafkaRetentionMs.max) {
    return `Retention: ${LIMITS.kafkaRetentionMs.min}..${LIMITS.kafkaRetentionMs.max} мс`;
  }
  return null;
}

// maxmemory ≥ 1 байт + инвариант R3 (arch/02 §11.3): maxmemory < mem-лимита
// ноды (иначе OOM-килл контейнера).
export function validateValkeyMaxmemory(bytes: number, memGi?: number): string | null {
  if (bytes < LIMITS.valkeyMaxmemoryBytes.min) {
    return 'maxmemory: не меньше 1 байта';
  }
  if (memGi !== undefined && bytes >= memGi * 1024 * 1024 * 1024) {
    return 'Инвариант R3: maxmemory меньше mem-лимита ноды (иначе OOM-килл)';
  }
  return null;
}

export function validateS3Instances(v: number): string | null {
  if (v < LIMITS.s3Instances.min || v > LIMITS.s3Instances.max) {
    return `Инстансы: ${LIMITS.s3Instances.min}..${LIMITS.s3Instances.max} (0 — декларация без развёртывания)`;
  }
  return null;
}

// Имя кластера/инстанса (arch/02 §9.3/§10.3/§11.3/§12.3): канон + подсказка.
export function validateClusterName(v: string): string | null {
  if (!CLUSTER_NAME_RE.test(v)) {
    return 'Имя: строчные латиница/цифры/подчёркивание, с буквы, до 63 символов (^[a-z][a-z0-9_]{0,62}$)';
  }
  return null;
}
