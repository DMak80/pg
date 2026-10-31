// Тесты границ валидации форм (D17): значения дословно из arch/adminpanel/02
// §9.3/§10.3/§11.3/§12.3 — границы включительные, инвариант R3 valkey.
import { describe, expect, it } from 'vitest';
import {
  LIMITS, VALKEY_POLICIES, validateBuckets, validateCpu, validateClusterName,
  validateDiskGi, validateKafkaBrokers, validateKafkaRf, validateMemGi,
  validateMinIsr, validatePartitions, validateReplicas, validateRetentionMs,
  validateS3Instances, validateShards, validateValkeyMaxmemory,
} from './validation';

describe('validation', () => {
  it.each([
    ['demo', null],
    ['demo_1', null],
    ['a', null],
    ['Demo', 'строка ошибки'],
    ['1demo', 'строка ошибки'],
    ['-demo', 'строка ошибки'],
    ['demo/s1', 'строка ошибки'],
    ['', 'строка ошибки'],
  ])('validateClusterName(%j)', (value, expected) => {
    // Arrange/Act/Assert — канон ^[a-z][a-z0-9_]{0,62}$
    expect(validateClusterName(value as string) !== null).toBe(expected !== null);
  });

  it('validateClusterName: 63 символа валидно, 64 нет', () => {
    // Arrange — граница длины канона
    const ok63 = 'a'.repeat(63);
    const bad64 = 'a'.repeat(64);

    // Act/Assert
    expect(validateClusterName(ok63)).toBeNull();
    expect(validateClusterName(bad64)).not.toBeNull();
  });

  it.each([
    [0.01, null],
    [64, null],
    [0, 'строка ошибки'],
    [64.1, 'строка ошибки'],
  ])('validateCpu(%j)', (v, expected) => {
    // Arrange/Act/Assert — 0.01..64 включительно
    expect(validateCpu(v) !== null).toBe(expected !== null);
  });

  it.each([
    [1, null],
    [65536, null],
    [0, 'строка ошибки'],
    [65537, 'строка ошибки'],
  ])('validateMemGi/validateDiskGi(%j)', (v, expected) => {
    // Arrange/Act/Assert — 1..65536 GiB
    expect(validateMemGi(v) !== null).toBe(expected !== null);
    expect(validateDiskGi(v) !== null).toBe(expected !== null);
  });

  it.each([
    [1, undefined, null],
    [26, undefined, null],
    [0, undefined, 'строка ошибки'],
    [27, undefined, 'строка ошибки'],
  ])('validateReplicas границы 1..26', (v, _buckets, expected) => {
    // Arrange/Act/Assert — replicas 1..26 (buckets — артефакт общей таблицы)
    expect(validateReplicas(v) !== null).toBe(expected !== null);
  });

  it.each([
    [1, undefined, null],
    [128, undefined, null],
    [129, undefined, 'строка ошибки'],
    [3, 2, 'строка ошибки'],
    [2, 8192, null],
  ])('validateShards: 1..128 и ≤ бакетов', (v, buckets, expected) => {
    // Arrange/Act/Assert — шарды 1..128, инвариант «шарды не больше бакетов»
    expect(validateShards(v, buckets) !== null).toBe(expected !== null);
  });

  it.each([
    [1, undefined, null],
    [9, undefined, null],
    [10, undefined, 'строка ошибки'],
    [5, 4, 'строка ошибки'],
  ])('validateKafkaRf', (rf, brokers, expected) => {
    // Arrange/Act/Assert — RF 1..9 и ≤ брокеров
    expect(validateKafkaRf(rf, brokers) !== null).toBe(expected !== null);
    expect(validateKafkaBrokers(rf) !== null).toBe(rf > 9);
  });

  it('validateMinIsr: 1..RF (arch/02 §10.3)', () => {
    // Arrange/Act/Assert
    expect(validateMinIsr(3, 1)).toBeNull();
    expect(validateMinIsr(3, 3)).toBeNull();
    expect(validateMinIsr(3, 4)).not.toBeNull();
    expect(validateMinIsr(3, 0)).not.toBeNull();
  });

  it.each([
    [1, null],
    [1000, null],
    [1001, 'строка ошибки'],
    [0, 'строка ошибки'],
  ])('validatePartitions(%j)', (v, expected) => {
    // Arrange/Act/Assert — 1..1000
    expect(validatePartitions(v) !== null).toBe(expected !== null);
  });

  it.each([
    [1, null],
    [2147483647, null],
    [0, 'строка ошибки'],
    [2147483648, 'строка ошибки'],
  ])('validateRetentionMs(%j)', (v, expected) => {
    // Arrange/Act/Assert — 1..2^31-1 мс
    expect(validateRetentionMs(v) !== null).toBe(expected !== null);
  });

  it('validateValkeyMaxmemory: ≥1 байт + инвариант R3 (maxmemory < memGi)', () => {
    // Arrange — 1 GiB-нода: граница ровно на лимите — нарушение (не строго <)
    // Act/Assert
    expect(validateValkeyMaxmemory(1)).toBeNull();
    expect(validateValkeyMaxmemory(0)).not.toBeNull();
    expect(validateValkeyMaxmemory(1024 ** 3 - 1, 1)).toBeNull();
    expect(validateValkeyMaxmemory(1024 ** 3, 1)).not.toBeNull();
  });

  it.each([
    [0, null],
    [9, null],
    [10, 'строка ошибки'],
  ])('validateS3Instances(%j)', (v, expected) => {
    // Arrange/Act/Assert — 0..9 (0 — декларация)
    expect(validateS3Instances(v) !== null).toBe(expected !== null);
  });

  it('validateBuckets: 1..8192', () => {
    // Arrange/Act/Assert
    expect(validateBuckets(1)).toBeNull();
    expect(validateBuckets(8192)).toBeNull();
    expect(validateBuckets(8193)).not.toBeNull();
  });

  it('канон границ — константы LIMITS из arch/02', () => {
    // Arrange/Act/Assert — числа дословно из §9.3/§10.3/§11.3/§12.3
    expect(LIMITS.cpu).toEqual({ min: 0.01, max: 64 });
    expect(LIMITS.kafkaBrokers).toEqual({ min: 1, max: 9 });
    expect(LIMITS.kafkaRetentionMs).toEqual({ min: 1, max: 2147483647 });
    expect(LIMITS.s3Instances).toEqual({ min: 0, max: 9 });
    expect(VALKEY_POLICIES).toHaveLength(8);
    expect(VALKEY_POLICIES).toContain('noeviction');
    expect(VALKEY_POLICIES).toContain('volatile-ttl');
  });
});
