// Классификация членов etcd-контура (arch/etcdworker/02-membership.md): managed — peer-URL
// канонической формы http://etcdw-node-<k>:2380 (адресация ТОЛЬКО по именам
// узлов); external — все прочие (seed, стендовые etcdN). Имя члена не
// дискриминант: у unstarted-learner'а оно пусто — классификация по peerUrls.
// Потребитель: EtcdPage (бейджи managed/external в MembersCard).
export function isManagedMember(member: { peerUrls: string[] }): boolean {
  return member.peerUrls.some((u) => /^http:\/\/etcdw-node-\d+:2380$/.test(u));
}
