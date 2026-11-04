# Valkey-кластеры: транспорт etcd

Уровень 3. Как читатели и писатель домена общаются с etcd. Ключи —
[02-keys.md](02-keys.md).

## 1. HTTP JSON gateway `/v3/*`

Как панель и PgWorker ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §1,
[pgworker/00-etcd-contract.md](../pgworker/00-etcd-contract.md) §1): `HttpClient` против gRPC-gateway
etcd (JSON+base64), `POST /v3/kv/range` / `/v3/kv/put` / `/v3/kv/txn` /
`/v3/lease/*`. Один общий etcd-кластер со стендом pg.

**Poll, без watch** — тик воркера 5 с / панели 3 с покрывают динамику.

## 2. Корневые префиксы

Два префикса домена:

- `/valkey/` — контроль-плейн кластеров (декларации, факт, дискавери);
- `/valkeyworker/` — координация воркера ([03-coordination.md](03-coordination.md));
  панель читает избирательно.
