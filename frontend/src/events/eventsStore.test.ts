// Тесты store ленты (u09): cap 500, дедуп, монотонный счётчик в id (#12),
// O(1)-дедуп через Set (#30d), clearEvents.
import { afterEach, describe, expect, it } from 'vitest';
import { clearEvents, emit, getEvents, subscribe } from './eventsStore';

afterEach(() => clearEvents());

const base = {
  tsUnix: 1761900000,
  domain: 'pg',
  entityKind: 'bucket',
  entity: 'demo/bucket_3',
  type: 'state-change',
} as const;

describe('eventsStore', () => {
  it('новые сверху + уведомление подписчика на emit', () => {
    // Arrange
    let notified = 0;
    const unsubscribe = subscribe(() => {
      notified += 1;
    });

    // Act
    emit({ ...base, entity: 'a' });
    emit({ ...base, entity: 'b' });

    // Assert — b сверху; два уведомления
    expect(getEvents().map((e) => e.entity)).toEqual(['b', 'a']);
    expect(notified).toBe(2);
    unsubscribe();
  });

  it('явный дубликат id не перезатирает событие', () => {
    // Arrange/Act — одно и то же событие дважды (диффы одного тика)
    emit({ ...base, id: 'dup-1' });
    emit({ ...base, id: 'dup-1' });

    // Assert — одна запись, одно уведомление на уникальный id
    expect(getEvents()).toHaveLength(1);
  });

  it('два разных события одной сущности/типа в одну секунду — оба живут (#12)', () => {
    // Arrange/Act — без явного id: монотонный счётчик делает id уникальными
    emit({ ...base });
    emit({ ...base });

    // Assert — дедуп не отбрасывает второе событие той же секунды
    expect(getEvents()).toHaveLength(2);
    expect(getEvents()[0].id).not.toBe(getEvents()[1].id);
  });

  it('cap 500: старые вытесняются, индекс дедупа чистится', () => {
    // Arrange/Act — 502 события с уникальными entity
    for (let i = 0; i < 502; i += 1) {
      emit({ ...base, entity: `demo/bucket_${i}` });
    }

    // Assert — кап держится; самая свежая — 501; повтор emit самой старой
    // (id вытеснен из индекса) снова входит
    expect(getEvents()).toHaveLength(500);
    expect(getEvents()[0].entity).toBe('demo/bucket_501');
    expect(getEvents()[499].entity).toBe('demo/bucket_2');
  });

  it('clearEvents очищает ленту и уведомляет подписчиков', () => {
    // Arrange
    emit({ ...base });
    let notified = 0;
    const unsubscribe = subscribe(() => {
      notified += 1;
    });

    // Act
    clearEvents();

    // Assert
    expect(getEvents()).toEqual([]);
    expect(notified).toBe(1);
    unsubscribe();
  });
});
