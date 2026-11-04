# S3-кластеры: координация воркера `/s3worker/`

Уровень 3. Служебный слой S3Worker. Порт схемы `/kafkaworker/`
([kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md)) / `/valkeyworker/`
([valkey-clusters/03-coordination.md](../valkey-clusters/03-coordination.md)) один в один, свой
префикс.

## 1. Ключи

| Ключ | Тип | Назначение |
|---|---|---|
| `/s3worker/leader` | lease TTL 15 с | лидер singleton-процессов |
| `/s3worker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/s3worker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` — сохранение накопленных треков S6 ([synchronization/README.md](../synchronization/README.md)) |
| `/s3worker/portalloc/<C>` | обычный | `{"<I>":{"host":"h","s3":18001}}` — закрепление S3-портов за инстансами, переживает rebuild |
| `/s3worker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (взаимоисключение секции довыделения портов; txn `version==0` + put-with-lease) |
| `/s3worker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/s3worker/api/<id>` | lease TTL 15 с | дискавери API воркера: `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; читает панель |
| `/s3worker/rotations/<C>` | обычный | заявка ротации клиентских кредов кластера `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn; del воркером по завершении): генерация новых root+ключей → замена `secrets` целиком (txn по `mod_revision`) → rolling-пересоздание инстансов по одному за тик с новым env ([06-secrets.md](06-secrets.md) §1) |
| `/s3worker/probes/<C>` | обычный | заявка внепланового зонда синхронности/связности `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn put-if-absent; del зонда по исполнении, [s3worker/06-sync-probes.md](../s3worker/06-sync-probes.md) §1) |

`portalloc`:

```json
{"east1":{"host":"h1","s3":18001},
 "east2":{"host":"h2","s3":18002}}
```

## 2. Писатель и читатели

- **Пишет** весь префикс — только S3Worker. Исключение вне префикса:
  серверный серт API `/workers/api_tls/s3worker` пишет панель
  ([s3worker/07-api.md](../s3worker/07-api.md) §4).
- **Панель читает** из `/s3worker/` только `api/` и `rotations/`; остальные
  ключи — координационные, вне панельного контракта.
