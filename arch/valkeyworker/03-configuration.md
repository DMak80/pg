# ValkeyWorker: конфигурация и ACL

Уровень 3. Функция «конфигурация»: converge `maxmemory`/`maxmemory_policy` и
ACL-плана к декларации без рестартов. Полный автомат —
[D. ConfigConverger](processes/d-config-converger.md); контракт полей —
[valkey-clusters/02-keys.md](../valkey-clusters/02-keys.md) §1; ACL-канон —
[04-security.md](04-security.md) §2.

## 1. Converge maxmemory (D)

`CONFIG GET maxmemory` / `maxmemory-policy` (по admin-креду, по TLS) vs
`config.{maxmemory_bytes, maxmemory_policy}` → при отличии `CONFIG SET` без
рестартов. Маппинг: `maxmemory_bytes`→`maxmemory`,
`maxmemory_policy`→`maxmemory-policy`.

Расхождение `maxmemory_bytes` ≥ mem-лимита контейнера — journal-warning:
ответственность оператора (OOM-килл, риск R3); OOM-рестарт подхватит надзор
[C](processes/c-node-supervisor.md) (кеш восполним).

## 2. Converge ACL-плана (D)

`ACL LIST` vs канон (пользователи admin/app с правами
[04-security.md](04-security.md) §2; `default off`) → идемпотентный
`ACL SETUSER`-converge. ACL-план живёт в runtime-памяти ноды; при пересоздании
контейнера аргументы ACL собираются из etcd-актуальных кредов
([01-placement.md](01-placement.md) §1), а расхождение самолечится converge D.

Оба converge — лёгкий шаг Active-ветки, только под клэймом `<C>`; выполняются
по одному соединению на кластер. Детали и порядок — [D](processes/d-config-converger.md).
