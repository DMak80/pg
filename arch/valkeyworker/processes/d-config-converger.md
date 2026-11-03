# D. ConfigConverger

Уровень 4. Converge `maxmemory`/`maxmemory_policy` и ACL-плана без рестартов.
Функция — [03-configuration.md](../03-configuration.md); ACL-канон —
[04-security.md](../04-security.md) §2.

Active-ветка, лёгкий: `CONFIG GET maxmemory`/`maxmemory-policy` (по
admin-креду, по TLS-соединению) vs `config.{maxmemory_bytes,
maxmemory_policy}` → при отличии `CONFIG SET` (без рестартов). Маппинг:
`maxmemory_bytes`→`maxmemory`, `maxmemory_policy`→`maxmemory-policy`.
**ACL-план**: `ACL LIST` vs канон (пользователи admin/app с правами
[04-security.md](../04-security.md) §2; `default off`) → идемпотентный
`ACL SETUSER`-converge. Расхождение `maxmemory_bytes` ≥ mem-лимита —
journal-warning (ответственность оператора, по образцу R7
[kafkaworker/README.md](../../kafkaworker/README.md)).
