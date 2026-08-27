# 02 — etcd-клиент и снапшот

> Назад: [docs/README.md](README.md) · Подсистема: `src/AdminPanel.Etcd`.
> Контракт ключей/модели: [arch/02](../arch/02-etcd-contract.md) — здесь только
> реализация и её грабли.

Кратко: `SnapshotRefresher` (тик 3 c, `EtcdOptions.RefreshIntervalSeconds`) —
единственный писатель; строит immutable `EtcdSnapshot` и атомарно кладёт в
`SnapshotStore.Current` (volatile-swap). API читает только стор. Живой цикл:
endpoint-status → range `/clusters/` + `/service/` + `/cluster/nodes/` →
member/list + alarm → парсеры → `ProbeEnricher` → `AlertEngine` → `store.Replace`.

## Gateway: HTTP JSON `/v3/*`

`Client/IEtcdGateway.cs` (реализация `EtcdGateway`): `RangeAsync(endpoint, prefix)`,
`StatusAsync`, `MemberListAsync`, `AlarmAsync` — POST JSON, ключи/значения base64.
Sticky+failover: активный endpoint держится, отказ → следующий (тест
`Refresher_Failover_DeadFirstEndpoint`: мёртвый `http://localhost:1` + живой).

Парсеры (`Etcd/Parsing/`): `ClustersParser` (`config`/`shards`/`routing`/`status`/
`heals`), `ServiceParser` (`leader`/`members`/`optime`), `StandNodesParser`
(`/cluster/nodes/`), `DsnParser` (multi-host DSN шарда). Префиксы — константы
`SnapshotRefresher.Prefixes` (`/clusters/`, `/service/`, `/cluster/nodes/`).

## Снапшот и отказы

`EtcdSnapshot` — immutable record: `Etcd`-статус, `Clusters[]`, `HaScopes[]`,
`Probes`, `Alerts`, `StandNodes`, `BuiltAtUtc`. Отказный тик: прежний снапшот
сохраняется (тот же экземпляр), `ConsecutiveFailures` растёт, `Etcd.Reachable=false`;
алерт `etcd-unreachable` — с порога 2 отказов. `EtcdHealthCheck` (readiness-семантика)
не входит в liveness `/api/healthz`.

## Чек-лист «добавить ключ/поле снапшота»

1. Контракт: правка [arch/02](../arch/02-etcd-contract.md) (формат ключа, семантика) —
   первой.
2. Модель: поле в `Core` (`ClusterInfo`/`ShardInfo`/…), immutable.
3. Парсер: чтение ключа/поля (`Parsing/*`, толерантный `JsonValues`).
4. Сид синхронно в 3 местах: `seed.sh` (стенд), `EtcdSeed` (integration),
   `EtcdFixtures/*.json` (unit) — расхождение ломает тесты и e2e.
5. DTO API + фронт (arch/03 §2 → `api/dto.ts`) — по потребности.
6. Тесты: unit-парсер (fixture-JSON), integration-сценарий на живом etcd.

## Грабли

- **API не ходит в etcd на запрос** — только `SnapshotStore.Current`; иначе латентность
  etcd ломает UI, а отказ etcd — панель (инвариант arch/01 §1).
- **Числа int64 из gateway — decimal-строки** (`mod_revision`, `dbSize`, `raftTerm`,
  lease-ID): DTO читаются `System.Text.Json` с `JsonNumberHandling.
  AllowReadingFromString|WriteAsString` (t03 §3.17); lease-ID — десятичная строка
  (урок rolecheck `../pg`).
- **«Тесты недоступности»**: `http://localhost:1` даёт мгновенный connection refused —
  сценарий отказа не флакает по таймауту (t03).
- **Мутации сида в тестах** — только в классе со **своим** контейнером
  (`EtcdRoutingMutationTests`): перевладение routing в общем контейнере меняет
  ACTIVE-раскладку и ломает ожидания инвентаря соседних тестов класса (t90,
  «лишний bucket_0»).
- **Пустые `Endpoints`** — не падение: снапшот пуст, панель и healthz живы (норма
  для старта без ENV).
