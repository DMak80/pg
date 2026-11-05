// Табличные тесты классификации managed/external (arch/etcdworker/02-membership.md): каноническая
// peer-URL-константа http://etcdw-node-<k>:2380 против отклонений формы.
// AAA — на каждый кейс: Arrange (member) → Act (isManagedMember) → Assert.
import { describe, expect, it } from 'vitest';
import { isManagedMember } from './etcdMembers';

describe('isManagedMember', () => {
  it('канонический managed-URL — managed', () => {
    // Arrange
    const member = { peerUrls: ['http://etcdw-node-1:2380'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(true);
  });

  it('несколько peerUrls: managed среди прочих — managed', () => {
    // Arrange — смешанный список (второй URL канонический)
    const member = { peerUrls: ['http://etcd1:2380', 'http://etcdw-node-2:2380'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(true);
  });

  it('чужой хост — external', () => {
    // Arrange
    const member = { peerUrls: ['http://etcd1:2380'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(false);
  });

  it('чужой порт (host-мапнутый) — external', () => {
    // Arrange
    const member = { peerUrls: ['http://etcdw-node-1:32790'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(false);
  });

  it('https-схема — external', () => {
    // Arrange
    const member = { peerUrls: ['https://etcdw-node-1:2380'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(false);
  });

  it('trailing slash — external (отклонение от канонической формы)', () => {
    // Arrange
    const member = { peerUrls: ['http://etcdw-node-1:2380/'] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(false);
  });

  it('пустой peerUrls (unstarted-learner без ключей) — external', () => {
    // Arrange
    const member = { peerUrls: [] };

    // Act/Assert
    expect(isManagedMember(member)).toBe(false);
  });
});
