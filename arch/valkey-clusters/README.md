# Valkey-кластеры — контракт домена

Уровень 2. Контракт etcd Valkey-домена: контроль-плейн кластеров
`/valkey/`, координация воркера `/valkeyworker/` и клиентский дискавери.
Уровень 3 — разделы контракта (`01`–`05`); исполнительная сторона —
[valkeyworker/README.md](../valkeyworker/README.md).

## 1. Назначение домена

**Разделяемый кеш набора инстансов приложения**: топология standalone,
`nodes=1` (реплики/sentinel/cluster — вне канона,
[roadmap/valkey.md](../roadmap/valkey.md)).

Приложение читает адрес и ACL-креды **исключительно отсюда** — аналогия
pg dsn/`app_password` и kafka endpoints
([pg-clusters/11-buckets.md](../pg-clusters/11-buckets.md) §2).

**Разделение ответственности**: панель AdminPanel **декларирует** кластер
через HTTP API воркера, **ValkeyWorker исполняет**
([valkeyworker/README.md](../valkeyworker/README.md)); в etcd домена пишет
только воркер.

## 2. Имена

- **Кластер `<C>`** — `^[a-z][a-z0-9_]{0,62}$` (как pg/kafka; без дефиса).
- **Ноды `node1..nodeN`** — имя генерирует панель (`node<max+1>`, ≤ 9);
  в v1 всегда `nodes=1` — нода `node1`.

## 3. Состав канона

| Раздел | Содержание | Документ |
|---|---|---|
| Транспорт | HTTP JSON gateway `/v3/*`, poll без watch, два корневых префикса | [01-transport.md](01-transport.md) |
| Ключи кластера | `/valkey/clusters/<C>/…`: форматы, писатели, канонические примеры значений, отличия от kafka | [02-keys.md](02-keys.md) |
| Координация воркера | `/valkeyworker/…`: клэймы, журнал и стейт ротации, portalloc, заявки, исходы, дискавери | [03-coordination.md](03-coordination.md) |
| Клиентский дискавери | что и в каком порядке читает приложение; TLS-доверие и окно ротации CA | [04-client-discovery.md](04-client-discovery.md) |
| Толерантность читателей | таблица сбойных случаев и поведения парсеров/алертов | [05-fault-tolerance.md](05-fault-tolerance.md) |

## 4. Свойства, на которые опираются все разделы

- **Один общий etcd-контур** со стендом pg
  ([17-synchronization-principles.md](../17-synchronization-principles.md) S1);
  динамика покрывается тиками воркера (5 с) и панели (3 с) — watch не
  используется ([01-transport.md](01-transport.md) §1).
- **Декларативная модель**: отсутствие `config.state` = Active-кластер;
  `NOT_INITIALIZED`/`TO_REMOVE` — заявки панели ([02-keys.md](02-keys.md) §1).
- **Толерантность парсеров**: битый JSON, неизвестные ключи и новые поля не
  роняют читателя ([05-fault-tolerance.md](05-fault-tolerance.md)).
- **Кеш восполним**: persistence off — потеря контейнера означает холодный
  старт, данных, требующих репликации/бэкапа, домен не несёт
  ([valkeyworker/02-cluster-lifecycle.md](../valkeyworker/02-cluster-lifecycle.md)).
