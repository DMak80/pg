# t31-s3-clusters-canon — план реализации (arch-only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** создать канон кластеров S3-хранилища бэкапов — два новых arch/-документа
(контракт etcd `arch/23-s3-clusters.md` + воркер `arch/24-s3worker.md`) и
синхронизировать трек `arch/roadmap/s3.md` и указатели — без единой строчки кода.

**Архитектура:** документационная задача по готовой спеке: содержание обоих
документов зафиксировано спекой (§3.1/§3.2), план переносит его в arch/-канон
оформлением портов проверенных образцов (arch/15/16/20/21 — таблицы ключей,
канонические примеры значений, §-нумерация, риски-таблица). Три фазы спеки = три
задачи = три коммита feature-ветки; порядок фиксирован (контракт → воркер →
трек/указатели).

**Стек:** Markdown (документация `arch/`), git. Код, тесты, docker, compose — НЕ
трогаются (реализация — t32+).

**Spec:** `docs/superpowers/2026-10-11-t31-s3-clusters-canon/spec.md` — план
аргументирует от спеки; исполнители читают спеку и план вместе.

**Рабочий каталог (worktree):** `/Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon`.
Все пути ниже относительны его корня. Ветка — `docs-t31-s3-clusters-canon`
(коммиты в ней свободны); мерж в `main` — вне плана (гейт dev-flow).

## Глобальные ограничения

Из спеки (§1.3 НЕ-цели, §2 принципы, §3.4 границы, §7 критерии) — действуют на
все задачи без исключения:

1. **Только документы**: изменения допустимы исключительно в `arch/**` и
   `docs/superpowers/**`; итоговый `git status` — только `arch/23-s3-clusters.md`,
   `arch/24-s3worker.md`, `arch/roadmap/s3.md`, `arch/README.md`,
   `arch/22-owns3.md`, `docs/superpowers/**` (спека/план).
2. **Запрет касания** (спека §3.4): `arch/19-backups.md`, `arch/14-pgworker.md`,
   `arch/adminpanel/**`, `arch/owns3/**`, `docs/s3-replication.md`,
   `docs/owns3-replication-requirements.md` — не правятся вообще; на них
   ссылаемся, не пересказывая и не меняя.
3. **Язык и стиль**: русский текст, английские идентификаторы; структура и тон —
   как у arch/15/16/20/21: таблицы ключей (колонки как у соседей), канонические
   примеры значений в json-блоках, §-нумерация, таблица рисков, маркер `★` в
   заголовке, хвост-указатель «→ Указатель — README.md …».
4. **Только текущее/планируемое состояние**: никаких исторических пассажей,
   атрибуций («решение пользователя», «в рамках t31», даты обсуждений).
   Указатели на НЕслитые roadmap-задачи (`t32`–`t36`, `t35`, `t41`) как маркеры
   будущей реализации — допустимы (стиль соседей: arch/20 «(t07)»).
5. **Решения спеки — канон без атрибуции**: «оркестратор — новый отдельный
   воркер S3Worker (симметрия доменов монорепо kafka/valkey)» и «привязка ноды =
   предикат по атрибутам хоста» фиксируются как нормы канона.
6. **Ссылки**: на arch/-файлы — относительные markdown-ссылки
   (`[24-s3worker.md](24-s3worker.md)`,
   `[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)`,
   `[roadmap/s3.md](roadmap/s3.md)`); на `docs/*` — код-строкой без линка
   (`` `docs/s3-replication.md` `` — тон arch/19/22); из `arch/roadmap/*` —
   ссылками `../23-s3-clusters.md` (тон соседних строк трека).
7. **Roadmap**: пункт `t31` из `arch/roadmap/s3.md` НЕ удалять (снимается
   мерж-гейтом при мерже в `main`); границы и зависимости `←` задач t32–t36 не
   меняются — только ссылки на канон и синхронизация терминологии.
8. **Формулировки терминов** едины по всем артефактам (спека §7.5):
   `placement`-селектор, `secret_profile`, `endpoints`, `connectivity`/`sync`,
   процессы A–F, env-префикс `S3W_*`, конфиг-секции `S3Worker:*`.

---

### Задача 1: `arch/23-s3-clusters.md` — контракт etcd домена S3 (фаза 1 спеки)

**Вход (предусловие):** ветка `docs-t31-s3-clusters-canon`; рабочий каталог
чист, кроме `docs/superpowers/**` (спека/план); спека прочитана исполнителем
целиком.

**Обязательное чтение перед правкой:**

- спека: §3.1 (содержание документа постатейно), §2 (принципы), §1.3 (границы),
  §7 п.1 (критерий приёмки);
- образцы структуры/тона: `arch/20-valkey-clusters.md` (главный образец — тот же
  жанр «контракт кластеров»), `arch/15-kafka-clusters.md` §2/§2.1/§4/§6;
- источники фактов (читать по диагонали, сверять формулировки отсылок):
  `docs/s3-replication.md` (факты 7/8/12), `docs/owns3-replication-requirements.md`
  (R1–R6), `arch/19-backups.md` §5/§7 (потребитель: bucket/креды),
  `arch/22-owns3.md` + `arch/owns3/05-access-config.md` §2/§5/§6 (платформа
  owns3: access keys, `/healthz`, рестарт-семантика), `arch/14-pgworker.md` §2.4
  (прецедент advertised/portalloc).

**Files:**

- Create: `arch/23-s3-clusters.md` (новый файл; ориентир объёма — arch/20, глав
  больше (9), раздувания не допускать: каждый § несёт канон, пересказов
  arch/19/22 нет — только ссылки).

- [ ] **Шаг 1.1. Проверить свободность номеров 23/24** (риск коллизии из спеки
  §6, стр. «Номера документов»)

Run:
```bash
ls /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon/arch/ | grep -E '^2[34]-' ; echo "exit=$?"
```
Ожидание: пустой вывод, `exit=1` (файлов 23/24 нет). Если файл(ы) есть — СТОП,
НЕ перенумеровывать самостоятельно: задача требует решения пользователя
(спека допускает перенумерацию, но выбор номеров — не решение исполнителя).

- [ ] **Шаг 1.2. Каркас, преамбула и §1 «Транспорт»**

Создать `arch/23-s3-clusters.md`:

- Заголовок: `# 23. S3-кластеры хранилища бэкапов: контракт etcd (контроль-плейн + дискавери) ★`
- Преамбула (2 абзаца по образцу arch/20, строки 3–15): канон ключей S3-домена:
  контроль-плейн кластеров `/s3/` (декларирует панель AdminPanel через HTTP API
  воркера — исполняет S3Worker, канон — [24-s3worker.md](24-s3worker.md)),
  координация воркера `/s3worker/` и клиентский дискавери (`endpoints`).
  Назначение домена — пул S3-серверов хранилища бэкапов с гео-распределением
  (привязка нод к docker/swarm-хостам) и mesh-репликацией между нодами;
  потребитель — подсистема бэкапов ([19-backups.md](19-backups.md)); для
  клиентов пул — обычный S3 (§7). PgWorker — клиент пула (читатель `endpoints`),
  участником контракта не является.
- Имена (как в arch/20): кластер `<C>` — `^[a-z][a-z0-9_]{0,62}$` (как
  pg/kafka/valkey, без дефиса); ноды `node1..nodeN` (имя генерирует панель,
  `node<max+1>`, ≤ 9).
- `## 1. Транспорт: HTTP JSON gateway /v3/*` — порт arch/20 §1 дословно по
  смыслу: `HttpClient` против gRPC-gateway etcd (JSON+base64), `POST /v3/kv/range`
  / `put` / `txn` / `/v3/lease/*`; один общий etcd-контур со стендом; **poll, без
  watch** (тик воркера 5 с / панели 3 с). Два корневых префикса: `/s3/`
  (контроль-плейн) и `/s3worker/` (координация; панель читает избирательно — §8).

- [ ] **Шаг 1.3. §2 «Ключи кластера `/s3/clusters/<C>/`» + §2.1 канонические примеры**

Таблица с колонками как у arch/20 §2 (`| Ключ | Формат значения | Пишет | Примечание |`),
7 строк — содержание переносится из таблицы спеки §3.1 §2 (форматы JSON —
дословно из спеки):

1. `config` — `{"platform":"minio"|"owns3","nodes":N,"secret_profile":"<имя>","created_unix":T,"state"?:"NOT_INITIALIZED"|"TO_REMOVE"}`; пишут: панель через API воркера (создание, TO_REMOVE), воркер снимает `state` после инициализации (txn по `mod_revision`). Примечания: `state` — только у невыполненных заявок, отсутствие = Active (семантика pg/kafka/valkey); `platform` фиксируется при создании навсегда; `nodes` — плановое число нод; `secret_profile` — имя per-install env-профиля кредов нод (§6, секреты вне etcd); валидация состава: `minio` — 1..3 (факт 12 `docs/s3-replication.md`: 4+ нод непригодны для кластера бэкапов), `owns3` — 1..9 (структурный предел; операционная рекомендация 2–3 — гео-минимум и бюджет R1).
2. `nodes/node<k>/placement` — JSON-селектор атрибутов хоста: `{"docker_host":"h1"}` (plain) | `{"swarm_node":"n1"}` | `{"swarm_labels":{"geo":"dc1",...}}` | комбинация атрибутов; пишет панель через API воркера. Примечания: явная привязка ноды; модель «хост = набор атрибутов» (§4); изменение привязки живой ноды — только выводом/перевозом ноды (перепривязка на месте не допускается: данные тома привязаны к хосту).
3. `nodes/node<k>/resources` — `{"cpu":"2","mem":"4Gi","disk":"200Gi"}`; панель через API воркера (мутация ресурсов). Примечания: форматы как pg §9.3; cpu/mem — лимиты контейнера; `disk` — целевой объём volume данных ноды (декларация: создание volume и операционная квота-наблюдение; жёсткого enforcement-примитива у docker-volume нет — занятость наблюдается `usage` и алертами панели t34).
4. `nodes/node<k>/state` — `NOT_INITIALIZED|PROVISIONING|RUNNING|UNREACHABLE|REMOVING|TO_REMOVE`; `NOT_INITIALIZED`/`TO_REMOVE` — только панель (заявка / one-way маркер демонтажа ноды), остальные — воркер. Примечание: жизненный цикл ноды — семантика kafka/valkey; `UNREACHABLE` ставит надзор по health-пробам (платформенные health-грани — §5).
5. `nodes/node<k>/placed_on` — строка, имя docker-хоста `"h1"`; воркер (при резолвции placement). Примечание: фактическое размещение после резолвции селектора (§4); переживает rebuild (том ноды живёт на этом хосте).
6. `nodes/node<k>/usage` — `{"used_bytes":N,"total_bytes":N,"updated_unix":T}`; воркер (надзор, периодически). Примечание: занятость диска ноды из платформенной админ-грани/метрик; кормит квота-наблюдение `resources.disk` и панельные алерты (t34).
7. `endpoints` — строка `"h1:p1,h2:p2,..."`; воркер (после подъёма; RMW при изменении состава). Примечание: клиентские адреса пула (advertised host + S3-порт из `/s3worker/portalloc/<C>`) — точка дискавери клиентов (t36).

После таблицы — абзац: неизвестные ключи внутри `/s3/` — не ошибка: лог +
счётчик `unknownKeys` читателя (как pg/kafka/valkey).

Подраздел `### 2.1. Канонические примеры значений (критерий приёмки парсеров)`
(образец — arch/20 §2.1; json-блоки):

- `config` при создании (заявка): `{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800,"state":"NOT_INITIALIZED"}`
- `config` Active (после provisioning — поле `state` снято воркером): тот же JSON без `"state"`
- `config` с заявкой удаления: Active + `"state":"TO_REMOVE"`
- `placement` plain: `{"docker_host":"h1"}`; swarm: `{"swarm_node":"n1"}` и `{"swarm_labels":{"geo":"dc1"}}`
- `resources`: `{"cpu":"2","mem":"4Gi","disk":"200Gi"}`
- `state`: `RUNNING`
- `placed_on`: `"h1"`
- `usage`: `{"used_bytes":107374182400,"total_bytes":214748364800,"updated_unix":1760156860}`
- `endpoints`: `"h1:18001,h2:18002,h3:18003"`

- [ ] **Шаг 1.4. §3 «Статусы наблюдения (связность и синхронность)»**

Вводная строка: контракт ключей фиксируется здесь, запись реализуется задачами
трека (зонд — t33, процесс F арх/24 §5). Таблица (колонки `| Ключ | Формат | Кто пишет |`):

1. `/s3/clusters/<C>/connectivity` — `{"state":"OK"|"DEGRADED"|"DOWN","checked_unix":T,"links":{"node1":{"node2":"ok"|"offline",...},...},"probe_error"?}`; зонд t33 (периодически + по команде API). Связность нод между собой — PUT-пробы межнодовых линков (health-проба недоступность записи не доказывает: изолированная от всех целей нода отвечает health, но не принимает запись — факт 8 `docs/s3-replication.md`).
2. `/s3/clusters/<C>/sync` — `{"state":"SYNCED"|"LAGGING"|"DIVERGED","checked_unix":T,"pairs":[{"from":"node1","to":"node2","missing":N,"extra":N,"lag_bytes"?,"lag_objects"?}...],"last_error"?}`; зонд t33. Сверка содержимого bucket между нодами (list/хвосты; штатные метрики репликации лаг данных не отражают — факт 7; админ-грань ownS3 «стартовая позиция/хвост per-target» — R5).

После таблицы:
- определения state-агрегатов: connectivity `DOWN` — нет ни одного живого линка записи; `DEGRADED` — часть линков offline; sync `LAGGING` — догон известен и мал/растёт; `DIVERGED` — устойчивое расхождение/дыра (алерты панели — t34);
- форматы толерантны к расширению полей (парсеры панели читают известные поля);
- состав/здоровье нод наблюдается `nodes/*/state` + `usage` — отдельного
  health-ключа нет (состояние живости несёт `state`, детальная телеметрия —
  метрики [18-metrics.md](18-metrics.md));
- канонические примеры значений (json-блоки): `connectivity` OK-кластера из
  3 нод и `sync` SYNCED (пары с `"missing":0,"extra":0`).

- [ ] **Шаг 1.5. §4 «Модель размещения (атрибуты хостов, предикат привязки)»**

Четыре пункта списком (содержание — спека §3.1 §4):
- **Хост — набор атрибутов**: plain-режим — атрибут `docker_host` (имя из
  таблицы `S3Worker:Docker:Hosts[]` воркера, [24-s3worker.md](24-s3worker.md) §2);
  swarm — атрибуты `swarm_node` (hostname ноды swarm) и `swarm_labels`
  (`node.labels.*` — произвольные пары ключ-значение, в т.ч. гео-метки площадок).
- **Привязка ноды = предикат**: `placement`-селектор — конъюнкция равенств по
  атрибутам; нода кластера обязана иметь селектор (валидация API: пустой или
  невалидный — 400).
- **Резолюция** (воркер, фаза provisioning): по селектору — список
  кандидатов-хостов. Ноль кандидатов — transient-фейл фазы (journal, ретрай
  тиком: хост может появиться в конфигурации). Несколько кандидатов —
  детерминированный выбор (least-loaded по числу занятых портов/тасков, tie-break
  по имени; journal-запись с перечнем кандидатов). Результат — `placed_on`;
  повторные проходы идемпотентны (тот же хост, пока селектор не изменён выводом
  ноды).
- **Анти-аффинити** по отношению к другим нодам того же кластера НЕ вводится
  автоматикой: раснос по площадкам — обязанность оператора (гео-схема — явные
  селекторы); канон фиксирует рекомендацию «ноды одного кластера — на разных
  хостах/площадках» и панельную индикацию совпадений (t34).

- [ ] **Шаг 1.6. §5 «Платформы нод»**

Три блока (содержание — спека §3.1 §5; пограничник платформ, настройка механик — t32):
- **minio** (переходная): образ — фиксированный RELEASE из локального registry
  `192.168.0.1:5000` (строка `images.txt`, зеркалирование — ранбук
  `docs/runbook.md`; CE — end-of-life, обновлений нет); нода — контейнер
  `minio server` с volume данных; bucket per-install (arch/19 §5) создаётся на
  каждой ноде с **versioning** (replication без него не работает); mesh — правила
  bucket replication fan-out с уникальными приоритетами, флаги
  `delete,delete-marker,existing-objects` (backfill подключаемой ноды); предел
  состава 1..3 ноды (факт 12). Health-грани — `/minio/health/live|cluster`
  (200/503).
- **owns3** (целевая): образ `owns3:dev` (правила E2E-образов: сборка .NET на
  хосте, publish-COPY, в registry не кладётся); нода — ownS3 single-drive
  (`OWNS3_DATA_DIR=/data`); bucket per-install на каждой ноде (CreateBucket
  admin-кредом, идемпотентно); mesh — конфиг ownS3 per-install (адреса/креды
  целей — R6) — реализация и глава канона `arch/owns3/07-replication.md` — t35;
  до неё кластеры `platform=owns3` с N≥2 декларативно валидны, но
  mesh-синхронизация отсутствует (статусы sync это отразят). Health — `/healthz`
  ([owns3/05-access-config.md](owns3/05-access-config.md) §5).
- **Общее**: S3-порт контейнера 9000 → host-порт из portalloc; bucket
  per-install — единое имя на всех нодах кластера (клиент с одним комплектом
  кредов работает с любой нодой); создание/поддержание bucket — обязанность
  оркестратора (идемпотентно).

- [ ] **Шаг 1.7. §6 «Секреты (контрактная часть)»**

Содержание — спека §3.1 §6; конкретика:
- креды нод — per-install env-профили воркера, вне etcd и git (направление
  [14-pgworker.md](14-pgworker.md) §4 / [19-backups.md](19-backups.md) §7):
  профиль = имя + root-пара + опциональные статические access keys (read-write
  для записи бэкапов, read-only для панели — симметрия действующего стенда,
  owns3/05 §6);
- кластер ссылается на профиль полем `config.secret_profile`; профиль един на
  все ноды кластера (клиент пишет в любую ноду одним ключом; mesh-настройка —
  один комплект);
- формат env (прецедент `OWNS3_ACCESS_KEYS__<i>__*`, owns3/05 §2) — канон
  фиксирует имена переменных профиля:
  `S3W_NODE_SECRETS__<PROFILE>__ROOT_USER`, `S3W_NODE_SECRETS__<PROFILE>__ROOT_PASSWORD`,
  `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__ACCESSKEY`,
  `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__SECRETKEY`,
  `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__POLICY` (`read-write`|`read-only`);
- в ноды передаётся env контейнера: minio — `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`;
  owns3 — `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` + `OWNS3_ACCESS_KEYS__<i>__*`;
- отсутствие/неполнота профиля — отказ фазы provisioning с диагностикой
  (валидация API — 422 по факту env воркера);
- ротация — заявка `/s3worker/rotations/<C>` (§8): rolling-пересоздание нод с
  новым env профиля; поддержка рестарт-семантики платформ: ownS3 —
  переконфигурация env + рестарт (owns3/05 §2).

- [ ] **Шаг 1.8. §7 «Клиентская грань пула (границы для t36)»»

Один компактный § (содержание — спека §3.1 §7): пул для клиентов —
`endpoints`-набор обычных S3-серверов; запись — перебор «любой доступный» с
per-попыточным таймаутом PUT (health-проба недоступность не доказывает — факт 8
`docs/s3-replication.md`; быстрый отказ/успех, не молчание — R3 требований
ownS3); чтение/восстановление — по приоритетам per-cluster (страховка на лаг
доставки); синхронизация между серверами — не забота клиентов. Layout объектов —
arch/19 §5 без изменений. Канон фиксирует границу; реализация и правки
arch/19/arch/14 — t36.

- [ ] **Шаг 1.9. §8 «Координация воркера `/s3worker/`»**

Вводная: порт схемы `/kafkaworker/` ([15-kafka-clusters.md](15-kafka-clusters.md)
§4) / `/valkeyworker/` ([20-valkey-clusters.md](20-valkey-clusters.md) §3) один
в один, свой префикс. Таблица (колонки `| Ключ | Тип | Назначение |`), 8 строк:

| Ключ | Тип | Назначение (сводка) |
|---|---|---|
| `/s3worker/leader` | lease TTL 15 с | лидер singleton-процессов |
| `/s3worker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/s3worker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` — сохранение накопленных треков S6 [17-synchronization-principles.md](17-synchronization-principles.md) |
| `/s3worker/portalloc/<C>` | обычный | `{"node<k>":{"host":"h","s3":18001}}` — закрепление S3-портов, переживает rebuild |
| `/s3worker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (t90-паттерн: взаимоисключение секции довыделения портов; txn `version==0` + put-with-lease) |
| `/s3worker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/s3worker/api/<id>` | lease TTL 15 с | дискавери API воркера: `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; читает панель |
| `/s3worker/rotations/<C>` | обычный | заявка ротации секретов профиля кластера `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn; del воркером по завершении) |

После таблицы: панель читает из `/s3worker/` только `api/` и `rotations/`
(t34); остальные ключи — координационные, вне панельного контракта. Пример
канонического значения `portalloc` — json-блок
`{"node1":{"host":"h1","s3":18001},"node2":{"host":"h2","s3":18002}}`.

- [ ] **Шаг 1.10. §9 «Панель (проекция — t34)»» + хвост документа**

- Фиксация: панель читает `/s3/` и `/s3worker/{api,rotations}/`, мутирует только
  через API S3Worker; проекция в [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
  (глава домена S3 по образцу §10 kafka / §11 valkey) — задача t34.
- Совместимость с механикой панели: unknownKeys-толерантность, битый JSON —
  parseError-запись без падения тика, `state`-семантика «отсутствие = Active».
- Хвост (по образцу arch/20, строки 184–186):
  `→ Указатель — [README.md](README.md). Исполнительная сторона контракта — [24-s3worker.md](24-s3worker.md); мутации панели — через его HTTP API (§1.1 там).`

- [ ] **Шаг 1.11. Проверка задачи 1**

Run (из корня worktree; все команды обязаны пройти):
```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
test -f arch/23-s3-clusters.md && echo OK-FILE
grep -c '^## ' arch/23-s3-clusters.md                 # ожидание: 9
grep -c '^### 2\.1\.' arch/23-s3-clusters.md          # ожидание: 1
for k in 'config' 'nodes/node<k>/placement' 'nodes/node<k>/resources' 'nodes/node<k>/state' 'nodes/node<k>/placed_on' 'nodes/node<k>/usage' 'endpoints' 'connectivity' 'sync' 'S3W_NODE_SECRETS__' 'secret_profile' 'docker_host' 'swarm_labels' 'NOT_INITIALIZED' 'TO_REMOVE' 'unknownKeys' 'parseError' 'locks/portalloc' 'rotations/<C>' '/minio/health/live' '/healthz' 'versioning' 'existing-objects' 'OWNS3_DATA_DIR' 'per-попыточн'; do
  grep -q -- "$k" arch/23-s3-clusters.md || echo "MISSING: $k"
done                                                      # ожидание: ни одной строки MISSING
grep -n 'minio.*1\.\.3\|owns3.*1\.\.9' arch/23-s3-clusters.md   # ожидание: строки о пределах состава
# ссылки на существующие файлы arch/ (24-s3worker.md появится в задаче 2 — на него ссылка допустима):
for f in $(grep -oE '\]\([0-9A-Za-z/._-]+\.md\)' arch/23-s3-clusters.md | tr -d ']()' | cut -d'#' -f1 | sort -u | grep -v '^24-s3worker'); do
  [ -f "arch/$f" ] || echo "BROKEN LINK: $f"
done                                                      # ожидание: ни одной строки BROKEN
# границы: исторические пассажи/атрибуции запрещены:
grep -nE 'пользовател|2026-10|в рамках t31|спека реши' arch/23-s3-clusters.md   # ожидание: пусто
```
Критерий достижения: все проверки зелёные; документ читается как соседний канон
(сверка тона по arch/20).

- [ ] **Шаг 1.12. Коммит фазы 1**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
git add arch/23-s3-clusters.md
git commit -m "docs(t31): arch/23-s3-clusters — контракт etcd домена S3 (/s3/ + /s3worker/): декларации кластеров (config: platform/nodes/secret_profile), placement-селекторы по атрибутам хостов, ресурсы/диск-наблюдение, жизненный цикл нод, endpoints-дискавери, статусы связности/синхронности, платформы minio/owns3, env-профили секретов, клиентская грань, координация воркера, проекция панели (t34)"
```

**Выход:** создан `arch/23-s3-clusters.md` — полный контракт etcd домена S3
(9 глав), закоммичен в feature-ветку. Источник истины для задачи 2 (воркер
ссылается на него).

**Связь со spec:** §3.1 (всё содержание), §2 п.1/3/5/6/7 (принципы), §5 фаза 1,
§7 п.1 (критерий приёмки документа), §6 (риски коллизии номеров/квоты диска —
отражены в §2/§9 канона).

---

### Задача 2: `arch/24-s3worker.md` — воркер S3Worker (фаза 2 спеки)

**Вход (предусловие):** задача 1 закоммичена (`arch/23-s3-clusters.md`
существует); спека прочитана.

**Обязательное чтение перед правкой:**

- спека: §3.2 (содержание постатейно), §7 п.2 (критерий приёмки);
- образцы: `arch/21-valkeyworker.md` (главный образец жанра «воркер»: §1/§1.1
  роль+API, §2 размещение, §4 секреты, §5 процессы A–E, §6 надёжность, §7
  наблюдаемость, §8 конфигурация, §9 риски), `arch/16-kafkaworker.md` §1.1/§2.5/§5;
- контракты-соседи: готовый `arch/23-s3-clusters.md` (задача 1),
  `arch/14-pgworker.md` §1.1/§2.2.1/§2.4/§6 (API/mTLS-паттерн, транспорт
  docker, advertised, LoopWatchdog), `arch/17-synchronization-principles.md`
  (S5/S6/S7), `arch/adminpanel/02-etcd-contract.md` §9.2/§9.4/§9.5/§9.6/§9.8
  (протоколы записи: клэйм-txn / RMW / one-way / заявка ротации);
- платформа: `arch/22-owns3.md`, `arch/owns3/05-access-config.md` §2/§5.

**Files:**

- Create: `arch/24-s3worker.md` (новый файл; ориентир объёма — между arch/21
  (648 строк) и arch/16 (970); процессов 6 (A–F), раздувания не допускать:
  ключи/форматы — ссылками на arch/23, без пересказа).

- [ ] **Шаг 2.1. §1 «Роль и разделение ответственности» — каркас и ASCII-схема**

- Заголовок: `# 24. S3Worker: оркестратор S3-кластеров ★`
- ASCII-схема взаимодействия по образцу arch/21 §1 (блоки: AdminPanel (UI) /
  S3Worker (исполнитель) / docker-хосты; потоки: мутации → HTTP API воркера →
  пишет `/s3/clusters/<C>/config.state=NOT_INITIALIZED|TO_REMOVE`,
  `nodes/node<k>/state`, `/s3worker/rotations/<C>`; читает декларации;
  создаёт/удаляет контейнеры `s3w-<C>-node<k>` (minio/owns3), пишет `endpoints`,
  снимает `state`; инспекция панели — read-only; дискавери API — ключ
  `/s3worker/api/<id>`). Отдельной строкой: PgWorker — клиент пула (читатель
  `endpoints` в t36), участник контракта НЕ является.
- Текст роли: S3Worker — фоновый сервис (.NET 10), исполнительная сторона
  декларативного контракта `/s3/`: панель заявляет кластер через HTTP API
  воркера → воркер записывает декларацию (`state=NOT_INITIALIZED`) → поднимает
  ноды (volume + контейнер платформы на хосте из placement-селектора), создаёт
  bucket per-install, настраивает mesh (платформенным средством), пишет
  `endpoints` и снимает `state`; перевод в `TO_REMOVE` — демонтаж.
- Ответственность изменений: префиксы `/s3/` и `/s3worker/` пишет ТОЛЬКО
  S3Worker (панель и сиды — через его API).
- Несколько инстансов одновременно; координация — клэймы etcd; свойства — как у
  PgWorker (идемпотентность, takeover ≤ TTL 15 с, состояние переживает смерть
  инстанса).

- [ ] **Шаг 2.2. §1.1 «HTTP API воркера» — таблица эндпоинтов и валидация**

- mTLS по общему паттерну воркеров (arch/14 §1.1): единая per-install API-CA
  `kfw-install-ca`, клиентские серты панели; серверный серт — ключ
  `/workers/api_tls/s3worker` (канон ключа — adminpanel/02 §9.9), env-фоллбек
  `S3W_API_TLS_*`; `/healthz`, `/metrics` — та же грань; дискавери —
  `/s3worker/api/<id>` (arch/23 §8).
- Таблица эндпоинтов (колонки `| Метод+путь | Назначение |`), 7 строк — из
  спеки §3.2 §1.1 (протоколы записи — по образцу adminpanel/02
  §9.2/§9.4/§9.5/§9.6):

| Метод+путь | Назначение |
|---|---|
| `POST /api/s3/clusters` | создание кластера: клэйм-txn `version(config)==0` + пакет PUT (config + nodes × {placement,resources,state=NOT_INITIALIZED}); сбой — компенсация `del --prefix /s3/clusters/<C>/` |
| `DELETE /api/s3/clusters/{c}` | перевод `config.state=TO_REMOVE` (RMW; already → 204 без записи) |
| `POST /api/s3/clusters/{c}/nodes` | добавление ноды (`node<max+1>`): клэйм-txn + put `placement`/`resources`/`state=NOT_INITIALIZED` |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | маркер демонтажа ноды `nodes/<n>/state=TO_REMOVE` (one-way; гварды: не последняя нода кластера) |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | мутация ресурсов ноды (cpu/mem — rolling-пересоздание контейнера надзором; disk — декларация/наблюдение) — образец: kafka-мутация №15 «Изменение ресурсов брокера» (adminpanel/02 §10.2) |
| `POST /api/s3/clusters/{c}/rotations` | заявка ротации секретов профиля кластера (`/s3worker/rotations/<C>`, клэйм-txn — §9.8-паттерн) |
| `POST /api/s3/clusters/{c}/sync-probe` | внеплановый зонд синхронности/связности (202; результат — ключи §3 arch/23) — реализация t33, эндпоинт канонизируется сейчас |

- Валидация (сервер — источник истины): `name` — паттерн; `platform` ∈
  {minio,owns3}; `nodes` — 1..3 (minio) / 1..9 (owns3), рекомендуемый состав 3
  (гео-минимум); `placement` — обязательный валидный селектор для каждой ноды
  (400); `resources` — cpu 0.01..64, mem/disk 1..65536 GiB; `secret_profile` —
  непустое имя, существующее в env воркера (422 с перечнем доступных имён, без
  значений секретов).

- [ ] **Шаг 2.3. §2 «Модель размещения»**

- Docker plain/swarm через `Shared.Docker` (транспорт unix/tcp+TLS/SSH — как
  arch/14 §2.2.1; env `S3W_DOCKER_TLS_*`, `S3W_DOCKER_SSH_*`).
- Конфиг (ссылка вперёд на §8): `S3Worker:Docker { Mode, Hosts[{Name,Endpoint}],
  SwarmManager, PortRange{From,To}, Images{Minio,OwnS3}, Tls, Ssh, ContourNetwork }`.
- Атрибуты хостов для placement-селекторов (ссылка на arch/23 §4): plain — имя
  из `Hosts[]`; swarm — hostname нод и `node.labels.*` (`GET /nodes`).
- Portalloc: S3-порт (контейнерный 9000) → последовательные host-слоты
  диапазона, закрепление `/s3worker/portalloc/<C>`, глобальный клэйм
  `/s3worker/locks/portalloc` на довыделение (занятость = docker-публикации ∪
  portalloc всех кластеров — t90-паттерн arch/14 §2.4).
- Advertised-адреса — только от фактических публикаций
  (`S3Worker:Docker:AdvertisedHost` по образцу arch/14 §2.4 п.5 —
  single-host/стенд; multi-host — фактические имена хостов).
- Сеть: per-cluster сеть НЕ создаётся (межнодовая коммуникация mesh и клиенты —
  по advertised `host:port`, единый namespace адресов; отказ от аналога
  `pgw-net-<C>` осознанный — Patroni-подобного внутреннего кластерного
  протокола у нод S3 нет); опция `ContourNetwork` (стенд/E2E) — ноды создаются
  в сети контура поставки, адресация по именам сети, сеть воркером не
  создаётся/не удаляется (как arch/14 §2.1).

- [ ] **Шаг 2.4. §3 «Контракт etcd»**

Короткий § (образец — arch/21 §3, но ещё короче): читаемые/пишемые ключи —
ссылками на arch/23 (источник истины — не пересказ). Здесь — только
координационное поведение: инвариант «мутации `/s3/` — только держателем клэйма
`<C>`»; txn с compare по `mod_revision` config (снятие `state`, перевод
`TO_REMOVE`).

- [ ] **Шаг 2.5. §4 «Секреты»**

- per-install env-профили `S3W_NODE_SECRETS__<PROFILE>__*` (формат — arch/23 §6):
  root-пара + статические access keys; профиль един на кластер.
- Передача в ноды — env контейнера при создании (значения никогда не попадают в
  etcd/журнал).
- Ротация: заявка `rotations/<C>` → rolling-пересоздание нод (по одной за тик,
  данные volume не трогаются) → del заявки.
- Транспортные секреты API/docker — env `S3W_*` (§1.1/§2). Серверный серт API —
  ключ `/workers/api_tls/s3worker` (панель пишет — t34; до того env-фоллбек).

- [ ] **Шаг 2.6. §5 «Процессы (машины состояний)» — A–F**

Вводная: все шаги идемпотентны (перепроверка факта),
journal-before-manipulations (`/s3worker/work/<C>`), клэйм `<C>` обязателен.
Шесть подразделов (по образцу arch/21 §5 A–E; фазные префиксы как там):

- **A. ProvisioningProcess (P0–P5)**: P0 claim+journal → P1 резолв placement
  всех нод + portalloc → P2 создание нод: volume данных + контейнер платформы
  (env: креды профиля, адреса mesh-соседей — для minio это
  advertised-эндпоинты целей правил, для ownS3 — конфиг mesh t35), порты,
  health-ожидание (бюджет, transient-толерантно; поллинг-инвариант t19 — без
  одиночных блокирующих вызовов) → P3 bucket per-install на каждой ноде
  (идемпотентно; minio — с versioning) → P4 mesh-настройка платформенным
  средством (minio: `mc replicate add` fan-out, уникальные приоритеты,
  `delete,delete-marker,existing-objects`; owns3: конфиг поставки t35) → P5
  `endpoints` + снятие `config.state` txn по `mod_revision`.
- **B. DeprovisioningProcess (X0–X3)**: X0 claim → X1 маркеры `REMOVING` нодам →
  X2 снос контейнеров/volumes/portalloc → X3 `del --prefix /s3/clusters/<C>/` +
  чистка `/s3worker/{work,portalloc}/<C>`. Отдельная строка: снос кластера
  уничтожает данные бэкапов его нод — осознанное действие оператора (панель t34
  — confirm); фиксируется в рисках §9.
- **C. NodeSupervisor (надзор, тик)**: health-проба каждой ноды
  (платформенная health-грань по host:port из portalloc; проба — не
  доказательство доступности записи, только живость контейнера/грани); снесённый
  контейнер (положительное свидетельство docker-инспекции, S7) → пересоздание на
  том же хосте (volume сохраняется); недоступная нода → `UNREACHABLE` + ретраи;
  периодический сбор `usage` (платформенная админ-грань/метрики) — квота-наблюдение
  `resources.disk`.
- **D. AddNodeProcess**: подъём новой ноды (по A) + включение в mesh + backfill
  существующих объектов (minio — `existing-objects`; ownS3 — R4) + RMW
  `endpoints`.
- **E. RemoveNodeProcess**: гварды (не последняя нода; вывод не рвёт единственную
  копию — при живых остальных) → исключение из mesh → остановка/снос контейнера
  и volume → чистка ключей/portalloc → RMW `endpoints`.
- **F. SyncProbeProcess (контракт — t33, каркас канонизируется)**:
  периодический и по-командный зонд: PUT-пробы межнодовых линков (connectivity)
  и сверка листингов bucket между нодами (sync) — ключи arch/23 §3; источник
  истины о синхронности — собственный зонд (штатные метрики репликации лаг не
  отражают — факт 7 `docs/s3-replication.md`).

- [ ] **Шаг 2.7. §6 «Надёжность»**

Содержание — спека §3.2 §6: идемпотентность каждого шага; takeover ≤ TTL 15 с;
отказ docker-хоста — ноды на нём `UNREACHABLE`, размещение не меняется
(`placed_on` фиксирован), новые ноды — только на резолвируемые хосты;
подраздел **«что теряется при отключении машины X»**: volume ноды — локальное
хранение без гарантий durability, доступность её доли теряется до возврата
машины; данные бэкапов переживают отказ машины благодаря mesh-репликации на
остальные ноды с точностью до лага доставки (RPO = лаг репликации: секунды
штатно, минуты после отказов — факты 6/12 `docs/s3-replication.md`); полная
гибель диска ноды — потеря её доли, восстановление полноты — backfill из живых
нод (процесс D); гарантии строятся от реплицированных источников (etcd + mesh),
не от локальных volume. Отказ etcd — контрол-плейн заморожен, живые ноды S3 от
него не зависят. Watchdog зависших циклов — общий каркас `LoopWatchdog`
(Shared.Core, arch/14 §6): инварианты живости/поллинга обязательны и для циклов
S3Worker; «пульс конечного внешнего вызова» (S3-операции зонда —
per-попыточный таймаут + пульс) — тот же канон.

- [ ] **Шаг 2.8. §7 «Наблюдаемость»**

`/healthz`: `etcd-reachable`, `docker-hosts` (per-host ping), `loops-alive`,
`claims`. Prometheus-метрики — каркас arch/18 (`/metrics` на порту API):
циклы/клэймы/фазы процессов/операции воркера. Телеметрия самих нод — метрики
платформ (ownS3 — owns3/05 §5; minio — собственные метрики; scrape-механика —
вне канона t31).

- [ ] **Шаг 2.9. §8 «Конфигурация (appsettings + env)»**

Таблица/список секций с каноническими дефолтами (из спеки §3.2 §8):
`S3Worker:Etcd { Endpoints[] }`; `S3Worker:Docker { Mode, Hosts[], SwarmManager,
PortRange, Images{Minio,OwnS3}, Tls, Ssh, ContourNetwork, AdvertisedHost }`;
`S3Worker:Loops { ScanIntervalSec=5, KeepaliveSec=5, Watchdog{...} }`;
`S3Worker:Thresholds { NodeBootSec=100, NodeDeadSec=90, HealthIntervalSec=15 }`;
`S3Worker:Parallelism { MaxClusters=4 }`; секреты — env `S3W_*` (API TLS,
docker TLS/SSH, node-secrets-профили). Fail-fast валидации старта — по образцу
pg (неполные секретные пары); `EnableSeedEndpoint`-флаг — при необходимости
стендового сида, default `false`. Однострочный env-пример (тон arch/21 §8).

- [ ] **Шаг 2.10. §9 «Риски» + §10 «Дальше» + хвост**

- Таблица рисков (`| Риск | Митигация |` — тон arch/21 §9), 7 строк: EOL MinIO
  (переходная платформа — образ закреплён в registry, миграция на owns3 —
  t41-путь); 4+ нод minio непригодны (валидация состава); потеря единственной
  ноды кластера N=1 = потеря данных бэкапов (рекомендация N≥2/3, алерт панели
  t34); снос кластера уничтожает данные (confirm, риск-строка); собственный зонд
  как единственный честный источник синхронности (штатные метрики врут — факт
  7); отсутствие per-cluster сети — зависимость межнодовой связности от
  advertised-адресов (ограничение фиксируется явно); зависимость mesh-owns3 от
  t35.
- §10 «Дальше» — указатели: arch/23 (контракт), arch/19 (потребитель),
  arch/14/16/21 (образцы воркеров), arch/17 (принципы),
  [roadmap/s3.md](roadmap/s3.md), `docs/s3-replication.md` (факты),
  `docs/owns3-replication-requirements.md`.
- Хвост: `→ Указатель — [README.md](README.md). Контракт домена — [23-s3-clusters.md](23-s3-clusters.md).`

- [ ] **Шаг 2.11. Проверка задачи 2**

Run (из корня worktree):
```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
test -f arch/24-s3worker.md && echo OK-FILE
grep -c '^## ' arch/24-s3worker.md                 # ожидание: 10
for k in 'POST /api/s3/clusters' 'DELETE /api/s3/clusters/{c}' 'nodes/{n}/resources' 'sync-probe' 'rotations' 'kfw-install-ca' '/workers/api_tls/s3worker' 'S3W_API_TLS' 'S3W_DOCKER_TLS' 'S3W_DOCKER_SSH' 'S3W_NODE_SECRETS__' 'Shared.Docker' 'SwarmManager' 'PortRange' 'AdvertisedHost' 'ContourNetwork' 'locks/portalloc' 'ProvisioningProcess' 'DeprovisioningProcess' 'NodeSupervisor' 'AddNodeProcess' 'RemoveNodeProcess' 'SyncProbeProcess' 'journal-before-manipulations' 'takeover' 'LoopWatchdog' 'RPO' 'backfill' 'etcd-reachable' 'NodeBootSec=100' 'MaxClusters=4' 'mc replicate add' 'existing-objects' 'versioning' 'UNREACHABLE' 'mod_revision'; do
  grep -q -- "$k" arch/24-s3worker.md || echo "MISSING: $k"
done                                                      # ожидание: ни одной строки MISSING
# роль PgWorker — только клиент:
grep -c 'PgWorker' arch/24-s3worker.md                  # ожидание: >=1
grep -n 'PgWorker' arch/24-s3worker.md | grep -v 'клиент\|читатель\|t36'   # ожидание: пусто (PgWorker не участник контракта)
# ссылки:
for f in $(grep -oE '\]\([0-9A-Za-z/._-]+\.md\)' arch/24-s3worker.md | tr -d ']()' | cut -d'#' -f1 | sort -u); do
  [ -f "arch/$f" ] || echo "BROKEN LINK: $f"
done                                                      # ожидание: ни одной строки BROKEN (в т.ч. 23-s3-clusters.md из задачи 1)
# исторические пассажи:
grep -nE 'пользовател|2026-10|в рамках t31' arch/24-s3worker.md     # ожидание: пусто
```
Критерий достижения: все проверки зелёные.

- [ ] **Шаг 2.12. Коммит фазы 2**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
git add arch/24-s3worker.md
git commit -m "docs(t31): arch/24-s3worker — воркер S3Worker: роль (единственный писатель /s3/+/s3worker/), mTLS-API (7 эндпоинтов, валидация), модель размещения (атрибуты хостов, portalloc+глобальный клэйм, advertised, отказ от per-cluster сети), env-профили секретов с rolling-ротацией, процессы A–F (provisioning/deprovisioning/надзор/add-remove/зонд), надёжность (RPO=лаг mesh, backfill, LoopWatchdog), наблюдаемость, конфигурация S3Worker:*/S3W_*, риски"
```

**Выход:** создан `arch/24-s3worker.md` (10 глав, процессы A–F), закоммичен.
Взаимные ссылки arch/23 ↔ arch/24 целы.

**Связь со spec:** §3.2 (всё содержание), §2 п.2/4 (симметрия доменов,
паттерны arch/17), §5 фаза 2, §7 п.2 (критерий приёмки), §4 (таблица будущих
задач — §5/§10 согласованы с ней).

---

### Задача 3: трек `arch/roadmap/s3.md` и указатели (фаза 3 спеки)

**Вход (предусловие):** задачи 1–2 закоммичены (`arch/23-s3-clusters.md`,
`arch/24-s3worker.md` существуют).

**Обязательное чтение перед правкой:**

- спека §3.3 (точные правки), §7 п.3–6 (критерии), §6 (риск «расхождение
  формулировок»);
- текущие файлы: `arch/roadmap/s3.md` (целиком), `arch/README.md` (блок
  «Структура репозитория» строки ~82–155 и список «Дальше»), `arch/22-owns3.md`
  (хвост «Развитие»), `arch/roadmap/README.md` (правила ведения).

**Files:**

- Modify: `arch/roadmap/s3.md` (шапка + формулировки t32–t36)
- Modify: `arch/README.md` (дерево структуры + список «Дальше»)
- Modify: `arch/22-owns3.md` (одна строка-указатель)

- [ ] **Шаг 3.1. `arch/roadmap/s3.md` — шапка: ссылки на канон**

После абзаца «Исходные изыскания платформы … контекст потребителя хранилища —
[../19-backups.md](../19-backups.md).» (строки 17–23 текущего файла) добавить
абзац:

```markdown
Канон домена — [../23-s3-clusters.md](../23-s3-clusters.md) (контракт etcd
`/s3/` + `/s3worker/`: декларации кластеров, статусы, дискавери) и
[../24-s3worker.md](../24-s3worker.md) (оркестратор S3Worker:
provisioning/надзор/изменение состава/зонд синхронности) — результат
`t31-s3-clusters-canon`; задачи трека исполняются по канону.
```

- [ ] **Шаг 3.2. `arch/roadmap/s3.md` — синхронизация формулировок t32–t36**

Правки строго в границах: НЕ менять зависимости `←`, НЕ менять состав работ
задач, НЕ удалять пункт t31. Конкретные правки:

1. **t32** — фрагмент «создание и управление контуром по декларации: подъём
   MinIO-нод контейнерами» заменить на «создание и управление контуром по
   декларации (процессы A–E канона [../24-s3worker.md](../24-s3worker.md) §5;
   платформенные границы — [../23-s3-clusters.md](../23-s3-clusters.md) §5):
   подъём нод платформы из декларации контейнерами»; фрагмент «путь
   выравнивания фиксируется спекой:» заменить на «путь выравнивания
   фиксируется спекой t32 в рамках канона arch/24 §5 A–E:».
2. **t33** — после «контроль синхронности и связности кластера:» вставить
   «(форматы ключей — [../23-s3-clusters.md](../23-s3-clusters.md) §3; процесс
   F канона [../24-s3worker.md](../24-s3worker.md) §5)».
3. **t34** — фрагмент «панель — немой читатель etcd по канону adminpanel/02»
   дополнить: «; проекция домена — [../23-s3-clusters.md](../23-s3-clusters.md)
   §8–§9 (глава домена S3 в adminpanel/02 — по образцу §10 kafka / §11 valkey)».
4. **t36** — в начале пункта после «клиентская интеграция пула S3-серверов
   (ownS3) в подсистему бэкапов:» вставить «(клиентская грань пула — канон
   [../23-s3-clusters.md](../23-s3-clusters.md) §7;)».

После правки перечитать каждый пункт: смысл и границы неизменны, добавлены
только ссылки/термины канона.

- [ ] **Шаг 3.3. `arch/README.md` — две строки в дереве структуры**

В блоке «Структура репозитория» после строк про `22-owns3.md`
(`├── 22-owns3.md ← ★ ownS3: собственное S3-хранилище … главы канона — owns3/)`)
вставить (соблюдая ASCII-ветки дерева: `├──` у новых строк, `└──` остаётся у
`scripts/`):

```
├── 23-s3-clusters.md          ← ★ S3-кластеры хранилища бэкапов: контракт etcd /s3/ + /s3worker/
│                                  (декларации кластеров, привязка нод placement-селекторами,
│                                  статусы связности/синхронности, дискавери endpoints)
├── 24-s3worker.md             ← ★ S3Worker: оркестратор S3-кластеров
│                                  (provisioning/deprovisioning/надзор/изменение состава/
│                                   зонд синхронности)
```

- [ ] **Шаг 3.4. `arch/README.md` — два пункта в списке «Дальше»**

После пункта 19 (`22-owns3.md … главы канона (01 обзор … 05 доступ/эксплуатация)
— [owns3/](owns3/)`) добавить:

```markdown
20. [23-s3-clusters.md](23-s3-clusters.md) — S3-кластеры хранилища бэкапов:
    контракт etcd `/s3/` (декларации кластеров, привязка нод к хостам
    placement-селекторами по атрибутам, статусы связности/синхронности,
    дискавери endpoints) и координация `/s3worker/`.
21. [24-s3worker.md](24-s3worker.md) — оркестратор S3Worker: декларативный
    жизненный цикл S3-кластеров (provisioning/надзор/изменение состава, зонд
    синхронности; задачи — [roadmap/s3.md](roadmap/s3.md)).
```

- [ ] **Шаг 3.5. `arch/22-owns3.md` — строка-указатель**

После хвостового предложения «Развитие — трек
[roadmap/owns3.md](roadmap/owns3.md); новые задачи после приёмочной грани
(миграция с MinIO).» добавить одну строку:

```markdown
ownS3 — платформа нод пула S3-кластеров хранилища бэкапов: управление пулом —
трек [roadmap/s3.md](roadmap/s3.md), канон [23-s3-clusters.md](23-s3-clusters.md)
+ [24-s3worker.md](24-s3worker.md); межсерверная mesh-репликация ownS3 — задача
`t35-owns3-replication`.
```

- [ ] **Шаг 3.6. Сквозная проверка границ и согласованности (критерии спеки §7.3–7.6)**

Run (из корня worktree):
```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
# 1) указатели на канон появились во всех трёх файлах:
grep -c '23-s3-clusters\|24-s3worker' arch/roadmap/s3.md    # ожидание: >=5 (шапка + t32..t36)
grep -c '23-s3-clusters' arch/README.md                     # ожидание: 2 (дерево + Дальше)
grep -c '24-s3worker' arch/README.md                        # ожидание: 2
grep -c '23-s3-clusters' arch/22-owns3.md                   # ожидание: >=1
# 2) t31 остаётся в треке (снимает мерж-гейт, не эта задача):
grep -c 't31-s3-clusters-canon' arch/roadmap/s3.md          # ожидание: >=2 (пункт + зависимости t32/t34/t36)
# 3) зависимости задач не изменены:
git diff arch/roadmap/s3.md | grep -E '^\-.*←'              # ожидание: пусто (строки с ← не удалялись)
git diff arch/roadmap/s3.md | grep -E '^\+.*←' | grep -v 't31-s3-clusters-canon'  # ожидание: пусто (новых ← нет)
# 4) границы задачи (критерий §7.4) — изменения только ожидаемых файлов:
git status --short                                          # только arch/README.md, arch/22-owns3.md, arch/roadmap/s3.md, docs/superpowers/**
# 5) нетронутость запретных документов (от точки ветвления; git diff виден только
#    tracked-файлам — незакоммиченные spec/plan проверяются п.4 через git status):
MB=$(git merge-base main HEAD)
git diff --name-only "$MB" -- arch/19-backups.md arch/14-pgworker.md arch/adminpanel arch/owns3 docs/s3-replication.md docs/owns3-replication-requirements.md
# ожидание: пусто (по всем задачам 1–3 суммарно)
git diff --name-only "$MB" | sort                           # ожидание: ровно arch/22-owns3.md, arch/23-s3-clusters.md,
                                                            # arch/24-s3worker.md, arch/README.md, arch/roadmap/s3.md
# 6) согласованность терминов между arch/23 и arch/24 (критерий §7.5):
for t in '/s3worker/rotations/<C>' 'S3W_NODE_SECRETS__' 'secret_profile' 'placement' 'endpoints'; do
  grep -q -- "$t" arch/23-s3-clusters.md && grep -q -- "$t" arch/24-s3worker.md || echo "INCONSISTENT: $t"
done                                                        # ожидание: ни одной строки INCONSISTENT
```

- [ ] **Шаг 3.7. Коммит фазы 3**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t31-s3-clusters-canon
git add arch/roadmap/s3.md arch/README.md arch/22-owns3.md
git commit -m "docs(t31): трек s3 и указатели — ссылки на канон arch/23+arch/24 (шапка трека + синхронизация формулировок t32–t36 со схемой контракта, границы задач не изменены), строки 23/24 в arch/README (дерево + Дальше), строка-указатель в 22-owns3 (управление пулом — трек s3)"
```

**Выход:** трек и указатели синхронизированы с каноном; все три фазы спеки
закоммичены; суммарный дифф ветки — только разрешённые файлы.

**Связь со spec:** §3.3 (все правки), §5 фаза 3, §7 п.3–6 (критерии), §6
(риск расхождения формулировок — митигация исполнена).

---

## Самопроверка плана (выполнена автором плана)

- **Покрытие спеки:** фаза 1 → задача 1 (§3.1); фаза 2 → задача 2 (§3.2);
  фаза 3 → задача 3 (§3.3: roadmap п.1 = шаги 3.1–3.2, README п.2 = шаги
  3.3–3.4, 22-owns3 п.3 = шаг 3.5). НЕ-цели §1.3 и границы §3.4 — глобальные
  ограничения + шаг 3.6 п.5. Критерии §7.1–7.6 разложены в проверки задач
  1–3. Разделы спеки §4 (будущие задачи) и §6 (риски) в план задач не
  конвертируются — это справочный контекст спеки, а не работа t31.
- **Плейсхолдеры:** отсутствуют; каждый шаг имеет конкретное содержание
  (таблицы/JSON/точные вставки) или ссылку на раздел спеки как единственный
  источник дословного содержания.
- **Согласованность имён:** ключи/термины (`placement`, `secret_profile`,
  `S3W_NODE_SECRETS__*`, `/s3worker/rotations/<C>`, процессы A–F) едины в
  задачах 1–2; проверка консистентности — шаг 3.6 п.6.
