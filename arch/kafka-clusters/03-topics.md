# Kafka-кластеры: топики (автосинк, конфиг-заявка, lifecycle)

Уровень 3. Формат ключа топика: фактическое состояние из Kafka, конфиг-заявка
панели (`desired`) и lifecycle-заявки создания/удаления. Ключи —
[02-keys.md](02-keys.md) §1; исполнение воркером —
[kafkaworker/05-topics.md](../kafkaworker/05-topics.md).

## 1. Каноническое значение `topics/<T>`

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"604800000","min.insync.replicas":"2"},
 "desired":{"partitions":16,"configs":{"retention.ms":"86400000"}}?,
 "desired_unix":1750000000?,"desired_by":"admin"?,
 "synced_unix":1750000100,"missing":false}
```

- **Факт** (`partitions`, `replication_factor`, `configs`, `synced_unix`) —
  пишет только воркер (автосинк): `replication_factor` — read-only факт
  (мутация RF — roadmap reassignment); `configs` — фактические значения
  управляемых конфигов (строковые, как отдаёт Kafka).
- **`desired`** — конфиг-заявка панели (управляемые поля: `partitions` —
  только увеличение; `configs.retention.ms`, `configs.min.insync.replicas`;
  лишние поля — 400). Панель пишет RMW: read → set `desired`/`desired_unix`/
  `desired_by` → txn `compare mod_revision == прочитанной` → put.
  `desired` отсутствует (null) — заявки нет.
- **Протокол автосинка** (воркер, тик `TopicSyncIntervalSec`): ListTopics+
  Describe → для каждого не-`__` топика read-modify-write ключа: обновить
  факт, сохранить `desired`; если `desired` отличается от факта по
  управляемым полям → применить к Kafka (`IncrementalAlterConfigs`,
  `CreatePartitions`) и снять `desired` (записать факт = заявке,
  `desired=null`) — тот же txn RMW. Проигрыш compare → re-read, следующий тик
  (панель успела переписать desired — применится свежий).
- **Топик исчез из Kafka**: `desired` нет → воркер удаляет ключ (реестр =
  факт); `desired` есть → ключ НЕ удаляется, `missing=true` (заявка не
  исполнима: создавать топики воркер не умеет — roadmap). Панель показывает
  «топик отсутствует, заявка не исполнена» + warning-алерт; отмена заявки —
  мутация панели (`desired=null`) → следующий автосинк удалит ключ. Топик
  появился → `missing=false`, `desired` применяется штатно.
- **Появление нового топика** (создан CLI/клиентом) → воркер кладёт ключ с
  фактом (`desired` отсутствует). Исчезновение и появление — штатные циклы
  реестра.

## 2. Lifecycle-заявки создания/удаления

Ключи `topics/<T>/desired.create` / `topics/<T>/desired.delete`
([02-keys.md](02-keys.md) §1): ставит панель клэйм-txn `version==0` (повтор
при живой заявке — 409; отмена — del ключа заявки), воркер после
исполнения/чистки делает del. Обе заявки на один `<T>` запрещены панелью;
etcd-мусор — delete авторитетен, create чистится ДО исполнения delete.

Исполнение — тем же тиком автосинка
([kafkaworker/05-topics.md](../kafkaworker/05-topics.md) §1), порядок decide:
чистка create (коллизия) → delete → create → факт-синк; один топик — одно
lifecycle-действие за тик:

| Ситуация | Действие воркера |
|---|---|
| `desired.delete` + топик в факте | journal → DeleteTopics → одной txn: del `topics/<T>` (факт-ключ; живой `desired` гасится с ним) + del заявки |
| `desired.delete` + топика нет | del заявки (+ del факт-ключа, если висит missing-ключ) — «исполнено внешне» |
| `desired.create` + топика нет | journal → CreateTopics(partitions, RF, configs?) → del заявки; факт-ключ кладёт следующий автосинк-тик |
| `desired.create` + топик есть | del заявки + journal-note «уже существует, параметры не применены» |
| обе живы | del `desired.create` + journal-warning; исполняется delete |
| заявка на `__`-имя | del + журнал, не исполняя |

Идемпотентность: CreateTopics → AlreadyExists = исполнено; DeleteTopics →
TopicDoesNotExist = исполнено; отказ между мутацией и del заявки — следующий
тик сходится по факту. `missing`-семантика не меняется; create на
missing-топике — «пересоздание» (панель требует отменить живой `desired`
раньше; обход etcd не ломается: после create `missing=false`, `desired`
применится штатно).
