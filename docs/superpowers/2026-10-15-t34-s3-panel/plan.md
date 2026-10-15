# t34-s3-panel — план реализации (пятый домен панели: S3-кластеры)

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ СУБ-СКИЛЛ: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять план задача-за-задачей. Шаги используют чекбоксы (`- [ ]`) для трекинга.

**Цель:** пятый домен AdminPanel — S3-кластеры хранилища бэкапов: чтение `/s3/` + `/s3worker/{api,rotations}/` в отдельный `S3Snapshot`, 10 мутаций-прокси в API S3Worker, панель UI (список/детали/вкладки/модалы), карточка S3Worker на грани «Воркеры», владение серверным сертом `/workers/api_tls/s3worker`, каталог s3-алертов, e2e-чек стенда.

**Архитектура:** точный порт valkey-домена (t03) на данные S3: отдельный домен-снапшот со своим refresher'ом (тик 3 с, общий транспорт `Shared.Etcd`/`IEtcdGateway`), панель — «немой читатель» `/s3/` и `/s3worker/` (все мутации — зеркальные REST-эндпоинты через `IWorkerApiGateway.SendAsync("s3worker", …)` с маппингом кодов 1:1). Единственная etcd-запись панели — инфра-ключ `/workers/api_tls/s3worker` (канон §9.9). Arch-first: сначала главы `arch/adminpanel/02` §2.3.4/§9.9/§12 и `arch/adminpanel/03` §12/§3.7/списки, затем код.

**Стек:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), CQRS-каркас `Shared.Core` (IQuery/ICommand/IHandler, attribute-DI), React 19 + Mantine + react-query (`frontend/`), xUnit + FluentAssertions, Testcontainers-etcd.

**Spec:** `docs/superpowers/2026-10-15-t34-s3-panel/spec.md` — план аргументирует от спеки; исполнители читают ОБА документа.

## Глобальные ограничения (действуют на каждую задачу)

- **Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t34-s3-panel` — ВСЯ работа только здесь. Базовые правила: `/Users/demakaev/ZCodeProject/AGENTS.base.md` (обязателен к прочтению каждым исполнителем).
- **Arch-first:** канон (`arch/adminpanel/02`, `arch/adminpanel/03`) правится РАНЬШЕ кода (Задачи 1–3); утверждения о механиках — только по `arch/23-s3-clusters.md`/`arch/24-s3worker.md`.
- **Немая панель:** панель НЕ пишет в `/s3/` и `/s3worker/` никогда (исключений нет). Единственная etcd-запись задачи — `/workers/api_tls/s3worker` через существующий `WorkerCertService`.
- **Не трогаем:** грань §2.5 (MinIO live-инвентарь), код S3Worker (`src/S3Worker.*`), pki-ключи в парсере (пропуск молча), координационные ключи `/s3worker/{leader,claims,work,portalloc,locks,instances,probes}`.
- **Секреты** (значения профилей, PKI, PEM) — никогда в снапшоте/DTO/UI/логах.
- **Сборка:** `TreatWarningsAsErrors=true` — сборка без warning'ов; `Directory.Packages.props` — новые пакеты НЕ добавляем (всё уже есть).
- **Тесты:** комментарии по AAA; docker-порты только динамические (`assignRandomHostPort: true`); таймауты ожиданий ≤ 30 с (правило базовое п.12); после КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (`docker rm -f` остатков + `docker network prune -f` при осиротевших `kfw-net-*`).
- **Команды/скрипты/сборки запускать ТОЛЬКО в фоне** (`run_in_background: true`), фиксировать task_id, результат забирать по готовности (базовое правило п.4).
- **Язык:** документация/UI-тексты русские, идентификаторы английские.
- **Коммиты:** свободно в feature-ветке; мерж в `main` — только по явной просьбе пользователя.
- **Прогоны тестов:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test …` из корня worktree; решение — `src/PgWorker.slnx`.

---

### Task 1: arch — adminpanel/02 §2.3.4 + §9.9 + преамбула

**Files:**
- Modify: `arch/adminpanel/02-etcd-contract.md` (преамбула, после §2.3.3 — новая §2.3.4, §9.9)

**Interfaces:**
- Consumes: каноны `arch/23` §8 (`/s3worker/api/`, `/s3worker/rotations/`), `arch/24` §1.1 (серверный серт `/workers/api_tls/s3worker`, env-фоллбек `S3W_API_TLS_*`).
- Produces: канонические формулировки §2.3.4/§9.9/преамбулы, на которые ссылается §12 (Task 2) и код (Задачи 7–10).

- [ ] **Шаг 1: преамбула — домен S3 в перечень**

В вводном абзаце после предложения про домен Valkey (строка, начинающаяся «Отдельный домен **Valkey** (§11, t03)…») добавить предложение:

```markdown
Отдельный домен **S3** (§12): чтение
`/s3/clusters/` + `/s3worker/{api,rotations}/` и 10 мутаций через API
S3Worker (arch/24 §1.1) — панель в etcd домена S3 не пишет ничего.
```

- [ ] **Шаг 2: новая §2.3.4 «`/s3worker/api/…` — дискавери API S3Worker»**

Вставить после §2.3.3 (симметрично её структуре, дословно по канону arch/23 §8 / arch/24 §1.1):

```markdown
### 2.3.4. `/s3worker/api/…` — дискавери API S3Worker

Симметрично §2.3.3: lease-ключи `/s3worker/api/<id>` (ставит сам воркер,
arch/23 §8; формат `{"url","instance","since_unix","cert_thumbprint"?}`)
читаются s3-refresher'ом в `S3Snapshot.WorkerEndpoints` — источник URL
для s3-мутаций §12.2. API — mTLS той же пакеты per-install API-CA
(`AdminPanel:Workers:WorkerTls`, env `WORKERS_PANEL_TLS_*`), серверный серт —
ключ `/workers/api_tls/s3worker` (§9.9). По тем же URL тик опроса
`/healthz` (`AdminPanel:Workers:HealthIntervalSec`, тем же поллером, что у
pg/kafka/valkey-инстансов) пробит живые инстансы S3Worker → `WorkerHealth[]`
в `S3Snapshot.WorkerHealth`, warning-алерт `worker-unhealthy` (03 §12.4).
Отсутствие живых ключей → 503 мутаций + critical-алерт
`worker-api-unreachable` (03 §12.4). Остальные ключи `/s3worker/` панель
не читает и не пишет (кроме `rotations/` — §12.1).
```

- [ ] **Шаг 3: §9.9 — расширить перечень воркеров**

В §9.9 заменить фразу «(`pgworker`, `kafkaworker`, `valkeyworker` — t03)» на «(`pgworker`, `kafkaworker`, `valkeyworker`, `s3worker`)», в блок ключей добавить строку:

```markdown
/workers/api_tls/s3worker   (симметрично; env-фоллбек воркера — S3W_API_TLS_{CERT,KEY,CLIENT_CA})
```

- [ ] **Шаг 4: проверка и коммит**

Проверка (критерий): `grep -n "2.3.4\|s3worker" arch/adminpanel/02-etcd-contract.md` показывает новую главу, три упоминания s3worker; форматы ключей даны ССЫЛКАМИ на arch/23 §8 (пересказа значений `/s3worker/api/` нет — только формат-строка как у §2.3.3).

```bash
git add arch/adminpanel/02-etcd-contract.md
git commit -m "t34 arch: adminpanel/02 — преамбула S3, §2.3.4 дискавери /s3worker/api/, §9.9 +s3worker"
```

---

### Task 2: arch — adminpanel/02 §12 «S3 (чтение + мутации)»

**Files:**
- Modify: `arch/adminpanel/02-etcd-contract.md` (новая глава §12 после §11)

**Interfaces:**
- Consumes: §2.3.4 (Task 1), §9.9, канон `arch/23` §2/§2.1/§3/§8 (форматы ключей — источник истины), `arch/24` §1.1 (эндпоинты/валидация исполнителя).
- Produces: §12.1 (читаемые ключи), §12.2 (таблица 10 мутаций + общие правила), §12.3 (валидация) — контракт для кода Задач 5–10 и для §12 03-panels (Task 3).

- [ ] **Шаг 1: вставить главу §12 целиком**

Вставить в КОНЕЦ файла `arch/adminpanel/02-etcd-contract.md` (сразу после §11.3 — последней главы файла; файл главой «Версионирование» не кончается, §11 — последняя) главу (содержание — из spec раздел 3.1 п.4 ДОСЛОВНО: таблицы §12.1/§12.2/§12.3 и пояснительные абзацы; заголовок «## 12. S3 (чтение + мутации)», вводка «Пятый домен панели. Канон ключей — [23-s3-clusters.md](../23-s3-clusters.md) … панельная проекция. Декларатор — панель, исполнитель — S3Worker ([24-s3worker.md](../24-s3worker.md)); панель никогда не трогает контейнеры и ноды S3 напрямую. Отдельный домен-снапшот `S3Snapshot` (не `EtcdSnapshot`/`KafkaSnapshot`/`ValkeySnapshot`) — своя механика тика §4, теми же транспортом §1 и настройками endpoints.»):

- §12.1 «Читаемые ключи» — таблица из spec §3.1 п.4 подпункт «§12.1» (10 строк: `config`, `nodes/<I>/{placement,resources,state,placed_on,usage}`, `links`, `denylist`, `endpoints`, `connectivity`, `sync`, `/s3worker/rotations/<C>`, `pki/*`+`nodes/<I>/pki` — пропуск молча) + абзац «Неизвестные ключи внутри `/s3/` — лог + `unknownKeys`; битый JSON — parseError-запись + warning-алерт `s3-key-malformed`.».
- §12.2 «Мутации панели (10; исполняет S3Worker API — arch/24 §1.1)» — общие правила (панель отправляет команду в API воркера, URL — живой `/s3worker/api/<id>` (§2.3.4); воркер валидирует и пишет etcd; панель маппит коды 1:1 (503 — в т.ч. когда живых ключей нет); протоколы записи — канон исполнителя arch/24 §1.1 (клэйм-txn, атомарность, идемпотентность already→204/202); панель — прокси, тело команды передаёт как есть; `requested_by` — username сессии панели (заголовок `X-Requested-By`; мутации 9–10 — с аудитом); отмена заявок ротации из панели НЕТ (снятие — только воркером)) + таблица 10 мутаций из spec §3.1 п.4 (колонки №/Мутация/Эндпоинт панели → эндпоинт воркера/Отказы).
- §12.3 «Валидация» — абзац «Сервер — источник истины (arch/24 §1.1), фронт дублирует для UX: имя кластера/инстанса `^[a-z][a-z0-9_]{0,62}$`; имя `orchestrator` зарезервировано; инстансы 0..9; placement — обязательный валидный селектор (минимум один атрибут `dockerHost`|`swarmNode`|`swarmLabels`); resources — cpu 0.01..64, mem/disk 1..65536 GiB; `secretProfile` — непустое имя (422 с перечнем доступных); links — имена существуют, `a≠b`, пары уникальны. `platform` — единственное значение `owns3` (панель подставляет всегда).»

- [ ] **Шаг 2: проверка и коммит**

Проверка: `grep -c "s3" arch/adminpanel/02-etcd-contract.md` растёт; в §12.1 форматы значений НЕ пересказаны (ссылки на arch/23 §2/§3), в §12.2 все 10 строк таблицы на месте (`grep -c "→"` в пределах §12 = 10). Связь со spec: закрывает §3.1 п.4 (критерий приёмки 1 частично).

```bash
git add arch/adminpanel/02-etcd-contract.md
git commit -m "t34 arch: adminpanel/02 §12 — S3: чтение + 10 мутаций через API S3Worker"
```

---

### Task 3: arch — adminpanel/03 §12 + §3.7 + точечные правки; сверка arch/23 §9 / arch/24 §1.1

**Files:**
- Modify: `arch/adminpanel/03-panels.md` (шапка, §1, §3, §3.7, новая §12)
- Modify: `arch/23-s3-clusters.md` (§9 — точечная ссылка)
- Modify: `arch/24-s3worker.md` (§1.1 — точечная ссылка)

**Interfaces:**
- Consumes: adminpanel/02 §12 (Task 2); spec разделы 3.2 (содержание глав) и 4.4 (UI).
- Produces: канон REST/UI/алертов для Задач 9–14.

- [ ] **Шаг 1: новая глава §12 в 03-panels**

Вставить в КОНЕЦ файла `arch/adminpanel/03-panels.md`, сразу после §9 «Версионирование контракта» (последняя глава файла; после §8 valkey идёт именно §9 — вставка «после §8» сломала бы порядок глав на §8 → §12 → §9, глава §12 становится последней) главу «## 12. S3: панель и REST API (`/api/s3/*`)» по образцу §8 — содержание из spec §3.2 п.1 ДОСЛОВНО:
- вводка: источник — отдельный снапшот `S3Snapshot` (свой refresher, тик 3 с); live-проб нод нет (живость — `nodes/*/state`, связность/синхронность — ключи зонда — без t-меток).
- §12.1 — таблица 12 эндпоинтов из spec §3.2 п.1 подпункт §12.1 + абзац про `GET /api/alerts` (kind `s3-*`) и `GET /api/overview` (clustersTotal, clustersCritical — critical-алерты `s3-node-not-running`/`s3-endpoints-missing`/`s3-connectivity-down`/`s3-sync-diverged`).
- §12.2 «DTO (ключевые поля)» — блок из spec §3.2 п.1 подпункт §12.2 (S3ClusterSummaryDto, S3ClusterDto, S3NodeDto, CreateS3ClusterPanelRequest, AddS3NodePanelRequest, UpdateS3ResourcesPanelRequest, PutS3LinksPanelRequest) + строка «Секреты (значения профилей) в DTO/API/UI не отдаются никогда.».
- §12.3 «Панели UI» — таблица из spec раздела 4.4 (S3Clusters / S3ClusterDetails со вкладками Ноды/Репликация/Связность и синхронность, модалы, confirm-тексты деструктива — про уничтожение данных бэкапов).
- §12.4 «Каталог s3-алертов (`S3AlertEngine`)» — вводка (чистая функция `S3Snapshot (prev, next) → Alert[]`; пороги `AdminPanel:S3Alerts`; sinceUnix по стабильному `id = kind:target`; Hint/Remedy обязательны §4.1) + таблица 17 kinds из spec §3.2 п.1 подпункт §12.4.

- [ ] **Шаг 2: §3.7 + точечные правки списков в 03-panels**

- §3.7: в первый абзац добавить S3Worker в перечень карточек («Страница `/workers` — карточки PgWorker, KafkaWorker, ValkeyWorker и S3Worker (02 §9.9).»); в списке сущностей ничего менять не надо — карточка строится по общим правилам §3.7 (инстансы/health/целевой серт/статусы/кнопки).
- Шапка документа: после предложения про модуль Kafka добавить: «Отдельный модуль **S3** (`/api/s3/*` — §12): свои GET и 10 мутаций через API S3Worker (протоколы — 02 §12).».
- §1: последней строкой таблицы добавить: `| /api/s3/* | модуль S3 (§12): инспекция + 10 мутаций через API S3Worker (02 §12.2) |`.
- §3: в таблицу панелей добавить строку `| **S3Clusters** / **S3ClusterDetails** | модуль S3 — §12.3 (вкладки Ноды/Репликация/Связность и синхронность) |`; абзац «Общие элементы» (конец §3.3): предложение «Форм ввода семь: логин, создание кластера (§3.1), добавление шарда (§3.2), перенос бакетов (§3.3), откат бакета (§3.4), финализация бакета (§3.5) и отмена переезда (§3.6) — всё остальное панель немая…» заменить на: «Формы ввода — по доменам: pg (логин; создание кластера §3.1; добавление шарда §3.2; перенос бакетов §3.3; откат §3.4; финализация §3.5; отмена переезда §3.6), kafka (§7.3.1 и модалы §7.3), valkey (§8.3.1 и модалы §8.3), s3 (§12.3: создание кластера, добавление ноды, ресурсы ноды, редактор links) — всё остальное панель немая по отношению к данным.»

- [ ] **Шаг 3: arch/23 §9 и arch/24 §1.1 — точечные ссылки на новую главу**

- `arch/23-s3-clusters.md` §9: фразу «проекция в [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) (глава домена S3 по образцу §10 kafka / §11 valkey) — задача t34.» заменить на «проекция — [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §12.».
- `arch/24-s3worker.md` §1.1: «(канон ключа и протокол — [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.9; пишет ТОЛЬКО панель — t34)» → «(канон ключа и протокол — [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.9; пишет ТОЛЬКО панель)». Больше в arch/23/arch/24 ничего не менять.

- [ ] **Шаг 4: проверка и коммит**

Проверка: `grep -n "## 12\|### 12.1\|### 12.2\|### 12.3\|### 12.4" arch/adminpanel/03-panels.md` — 5 заголовков; `grep -n "^## " arch/adminpanel/03-panels.md | tail -2` — порядок глав …§9 → §12 (возрастающий, §12 — последняя); таблица §12.4 содержит 17 строк kinds; расхождений формулировок с arch/23/24 нет (ручная сверка форматов ключей — ссылки, не пересказ). Остаточные t34-вхождения в arch/23/24 ПОСЛЕ шага 3 (чистит их мерж-коммит — Задача 16): arch/23 — §2 (дважды: «…наблюдается `usage` и алертами панели t34», «…панельные алерты (t34)»), §3 («алерты панели — t34…»), §4 («панельную индикацию совпадений (t34)»), §6 («полностью скрыто бэком, t34»), §8 («только `api/` и `rotations/` (t34)»), §9 (заголовок «(проекция — t34)»); arch/24 — §1 («панель домена S3 (t34)»), §1.1 (дважды: «пишет ТОЛЬКО панель — t34» правится шагом 3 → останется «…critical-алерт, t34)»; и §1.1 дискавери), §4 «Секреты» («панель пишет — t34; до того env-фоллбек»), §5 B («панель t34 — confirm»), §5 C («алерты панели (t34)»), §9 R1/R2 (дважды «панель t34»), §10 «Дальше» (roadmap-строка «t34 панель, t36 клиентская интеграция»). Связь со spec: §3.1 п.4/§3.2/§3.3 (критерии приёмки 1, 4 частично).

```bash
git add arch/adminpanel/03-panels.md arch/23-s3-clusters.md arch/24-s3worker.md
git commit -m "t34 arch: adminpanel/03 §12 (эндпоинты/DTO/UI/алерты S3), §3.7 +карточка S3Worker, списки; ссылки arch/23 §9 / arch/24 §1.1"
```

---

### Task 4: Core — S3Snapshot (модель домена)

**Files:**
- Create: `src/AdminPanel.Core/S3/S3Snapshot.cs`
- Test: `src/tests/AdminPanel.UnitTests/S3ModelTests.cs`

**Interfaces:**
- Consumes: `AdminPanel.Core` базовые типы `WorkerEndpoint`, `WorkerHealth`, `Alert`, `KeyParseError` (существуют), `WorkerApiCert`.
- Produces (для Задач 5–9): типы ниже — имена/поля фиксированы, их используют парсер, refresher, алерт-движок и мапперы.

- [ ] **Шаг 1: тест модели (AAA)**

`src/tests/AdminPanel.UnitTests/S3ModelTests.cs` (по образцу `ValkeyModelTests.cs`):

```csharp
using AdminPanel.Core.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Маппинг состояний S3-домена (arch/23 §2: отсутствие state = Active).
public sealed class S3ModelTests
{
    // Arrange: raw-строки state. Act: S3ClusterStates.Parse. Assert: канон §2.
    [Theory]
    [InlineData(null, S3ClusterState.Active)]
    [InlineData("", S3ClusterState.Active)]
    [InlineData("NOT_INITIALIZED", S3ClusterState.NotInitialized)]
    [InlineData("TO_REMOVE", S3ClusterState.ToRemove)]
    [InlineData("WEIRD_FUTURE", S3ClusterState.Active)] // толерантность к новым
    public void Parse_State_MapsCanonically(string? raw, S3ClusterState expected)
    {
        S3ClusterStates.Parse(raw).Should().Be(expected);
    }
}
```

- [ ] **Шаг 2: прогнать тест — падает (нет типа)**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3ModelTests"`
Ожидание: FAIL — `AdminPanel.Core.S3` не существует (CS0246).

- [ ] **Шаг 3: реализация S3Snapshot.cs**

`src/AdminPanel.Core/S3/S3Snapshot.cs` (порт `ValkeySnapshot.cs`; state-поля connectivity/sync — raw-строки `string?` — толерантность к расширению форматов arch/23 §3, прецедент — `S3NodeInfo.State`):

```csharp
using AdminPanel.Core;

namespace AdminPanel.Core.S3;

// Домен-снапшот S3 (arch/02 §12): отдельный от EtcdSnapshot/KafkaSnapshot/
// ValkeySnapshot — своя механика тика, теми же настройками endpoints.
// Immutable; refresher строит новый и атомарно заменяет в S3SnapshotStore.
public sealed record S3Snapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<S3ClusterInfo> Clusters,           // /s3/clusters/
    IReadOnlyList<S3RotationTicket> Rotations,       // /s3worker/rotations/ (arch/23 §8)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,   // живые ключи /s3worker/api/ (arch/02 §2.3.4)
    IReadOnlyList<WorkerHealth> WorkerHealth,        // опрос /healthz живых инстансов
    IReadOnlyList<Alert> Alerts,                     // S3AlertEngine (arch/03 §12.4)
    IReadOnlyList<KeyParseError> ParseErrors,        // битые JSON s3-ключей (arch/23 §2)
    int UnknownKeyCount,
    WorkerApiCert? WorkerApiCert = null);            // целевой серт /workers/api_tls/s3worker (arch/02 §9.9)

// Кластер /s3/clusters/<C>/ (arch/23 §2): config + ноды + граф + факты.
public sealed record S3ClusterInfo(
    string Name,
    S3ClusterState State,                    // config.state; отсутствие = Active
    string Platform,                         // "owns3" (единственное значение)
    int NodesDeclared,                       // config.nodes (0..9)
    string SecretProfile,                    // имя per-install env-профиля (значение скрыто)
    long? CreatedUnix,
    string? Endpoints,                       // точка дискавери клиентов; null/пусто — критично у Active с нодами
    IReadOnlyList<S3NodeInfo> NodesList,
    IReadOnlyList<S3ReplicationLink> Links,  // граф репликации
    IReadOnlyList<string> DenyList,          // отозванные инстансы
    S3ConnectivityInfo? Connectivity = null, // зонд (arch/23 §3)
    S3SyncInfo? Sync = null,                 // зонд
    S3RotationTicket? Rotation = null);      // живая заявка ротации (джойн по кластеру)

// Инстанс <I>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|
// UNREACHABLE|REMOVING|TO_REMOVE; толерантно к новым).
public sealed record S3NodeInfo(
    string Name,
    string? State,
    string? PlacedOn,                        // фактический docker-хост (резолвция placement)
    S3NodePlacement? Placement,              // селектор arch/23 §4
    S3NodeResources? Resources,              // cpu/mem/disk декларации
    S3NodeUsage? Usage);                     // занятость диска (надзор C)

// Селектор размещения: конъюнкция указанных атрибутов (arch/23 §4).
public sealed record S3NodePlacement(
    string? DockerHost,
    string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);

// Декларация ресурсов: cpu — decimal; mem/disk — GiB (etcd хранит "<n>Gi").
public sealed record S3NodeResources(decimal? Cpu, int? MemGi, int? DiskGi);

// Занятость диска nodes/<I>/usage (arch/23 §2).
public sealed record S3NodeUsage(long UsedBytes, long TotalBytes, long UpdatedUnix);

// Связь графа репликации links (arch/23 §2).
public sealed record S3ReplicationLink(string A, string B);

// Связность connectivity (arch/23 §3): state — raw-строка OK|DEGRADED|DOWN;
// Links — матрица «нода → сосед → ok|offline» по настроенным линкам.
public sealed record S3ConnectivityInfo(
    string? State,
    long? CheckedUnix,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Links,
    string? ProbeError);

// Синхронность sync (arch/23 §3): state — raw-строка SYNCED|LAGGING|DIVERGED.
public sealed record S3SyncInfo(
    string? State,
    long? CheckedUnix,
    IReadOnlyList<S3SyncPair> Pairs,
    string? LastError);

// Пара сверки sync: from→to, missing/extra — количество расхождений.
public sealed record S3SyncPair(
    string From, string To, long Missing, long Extra, long? LagBytes, long? LagObjects);

// Заявка ротации /s3worker/rotations/<C> (arch/23 §8).
public sealed record S3RotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);

// Состояние кластера: config.state; отсутствие = Active.
public enum S3ClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

// Маппинг config.state → enum (толерантно: незнакомое значение — Active).
public static class S3ClusterStates
{
    public static S3ClusterState Parse(string? raw) => raw switch
    {
        "NOT_INITIALIZED" => S3ClusterState.NotInitialized,
        "TO_REMOVE" => S3ClusterState.ToRemove,
        _ => S3ClusterState.Active,
    };
}
```

- [ ] **Шаг 4: прогнать тест — зелёный**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3ModelTests"`
Ожидание: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Core/S3/S3Snapshot.cs src/tests/AdminPanel.UnitTests/S3ModelTests.cs
git commit -m "t34: S3Snapshot — модель s3-домена (порт ValkeySnapshot, arch/02 §12)"
```

---

### Task 5: Etcd — S3Parser + канонические фикстуры arch/23

**Files:**
- Create: `src/AdminPanel.Etcd/Parsing/S3Parser.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/S3/clusters-canonical.json`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/S3/clusters-tolerance.json`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/S3/rotations.json`
- Test: `src/tests/AdminPanel.UnitTests/S3ParserTests.cs`

**Interfaces:**
- Consumes: `Shared.Etcd.Client.Kv`, `AdminPanel.Core.KeyParseError`, модель Task 4, `JsonValues`-хелперы (`AdminPanel.Etcd.Parsing.JsonValues`: `ReadString/ReadLong`).
- Produces: `S3Parser.ParseClusters(IReadOnlyList<Kv>) → S3ClustersParseResult(Clusters, Errors, UnknownKeyCount)`; `S3Parser.ParseRotations(IReadOnlyList<Kv>) → S3RotationsParseResult(Tickets, Errors)` — их вызывает refresher (Task 7).

- [ ] **Шаг 1: фикстуры (эталоны — дословно из arch/23 §2.1/§3)**

`EtcdFixtures/S3/clusters-canonical.json` — формат как у `EtcdFixtures/Valkey/clusters-canonical.json` (массив `[{ "key": "...", "value": "..." }]`, грузится `EtcdFixtures.LoadKv`). Ключи:

```json
[
  { "key": "/s3/clusters/backupsmain/config", "value": "{\"platform\":\"owns3\",\"nodes\":3,\"secret_profile\":\"backups-main\",\"created_unix\":1760156800}" },
  { "key": "/s3/clusters/backupsmain/endpoints", "value": "h1:18001,h2:18002,h3:18003" },
  { "key": "/s3/clusters/backupsmain/links", "value": "[{\"a\":\"east1\",\"b\":\"east2\"},{\"a\":\"east1\",\"b\":\"west1\"}]" },
  { "key": "/s3/clusters/backupsmain/denylist", "value": "[\"west1\"]" },
  { "key": "/s3/clusters/backupsmain/connectivity", "value": "{\"state\":\"OK\",\"checked_unix\":1760156900,\"links\":{\"node1\":{\"node2\":\"ok\",\"node3\":\"ok\"},\"node2\":{\"node1\":\"ok\",\"node3\":\"ok\"},\"node3\":{\"node1\":\"ok\",\"node2\":\"ok\"}}}" },
  { "key": "/s3/clusters/backupsmain/sync", "value": "{\"state\":\"SYNCED\",\"checked_unix\":1760156900,\"pairs\":[{\"from\":\"node1\",\"to\":\"node2\",\"missing\":0,\"extra\":0},{\"from\":\"node1\",\"to\":\"node3\",\"missing\":0,\"extra\":0}]}" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/placement", "value": "{\"docker_host\":\"h1\"}" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/resources", "value": "{\"cpu\":\"2\",\"mem\":\"4Gi\",\"disk\":\"200Gi\"}" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/state", "value": "RUNNING" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/placed_on", "value": "h1" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/usage", "value": "{\"used_bytes\":107374182400,\"total_bytes\":214748364800,\"updated_unix\":1760156860}" },
  { "key": "/s3/clusters/backupsmain/nodes/east1/pki", "value": "{\"certificate\":\"{}\",\"private_key_pem\":\"x\"}" },
  { "key": "/s3/clusters/backupsmain/nodes/east2/placement", "value": "{\"swarm_labels\":{\"geo\":\"dc1\"}}" },
  { "key": "/s3/clusters/backupsmain/nodes/east2/state", "value": "PROVISIONING" },
  { "key": "/s3/clusters/backupsmain/pki/ca", "value": "{\"name\":\"backupsmain\",\"publicKeyPem\":\"x\",\"private_key_pem\":\"y\"}" },
  { "key": "/s3/clusters/backupsmain/pki/orchestrator", "value": "{\"certificate\":\"{}\",\"private_key_pem\":\"z\"}" },
  { "key": "/s3/clusters/newclaim/config", "value": "{\"platform\":\"owns3\",\"nodes\":0,\"secret_profile\":\"backups-main\",\"created_unix\":1760156800,\"state\":\"NOT_INITIALIZED\"}" },
  { "key": "/s3/clusters/dying/config", "value": "{\"platform\":\"owns3\",\"nodes\":1,\"secret_profile\":\"backups-main\",\"created_unix\":1760156800,\"state\":\"TO_REMOVE\"}" },
  { "key": "/s3/clusters/dying/nodes/node1/placement", "value": "{\"swarm_node\":\"n1\"}" },
  { "key": "/s3/clusters/swarmy/nodes/s1/placement", "value": "{\"docker_host\":\"h2\",\"swarm_labels\":{\"geo\":\"dc2\",\"tier\":\"fast\"}}" },
  { "key": "/s3/clusters/backupsmain/future_feature", "value": "1" }
]
```

`clusters-tolerance.json`:

```json
[
  { "key": "/s3/clusters/bad/config", "value": "{битый json" },
  { "key": "/s3/clusters/bad/links", "value": "не-массив" },
  { "key": "/s3/clusters/odd/config", "value": "{\"platform\":\"owns3\",\"state\":\"STRANGE_NEW\"}" },
  { "key": "/s3/clusters/odd/endpoints", "value": "  " },
  { "key": "/s3/clusters/odd/nodes/n1/resources", "value": "{\"cpu\":\"x\",\"mem\":\"4Gi\",\"disk\":\"?\"}" },
  { "key": "/s3/clusters/odd/nodes/n1/usage", "value": "{\"used_bytes\":10}" },
  { "key": "/s3/clusters/odd/connectivity", "value": "{\"state\":\"DOWN\",\"checked_unix\":1760156900,\"probe_error\":\"все ноды молчат\"}" },
  { "key": "/s3/clusters/odd/sync", "value": "{\"state\":\"DIVERGED\",\"checked_unix\":1760156900,\"pairs\":[{\"from\":\"a\",\"to\":\"b\",\"missing\":5,\"extra\":2,\"lag_bytes\":1024,\"lag_objects\":3}],\"last_error\":\"diff page\"}" }
]
```

`rotations.json`:

```json
[
  { "key": "/s3worker/rotations/backupsmain", "value": "{\"requested_unix\":1760157000,\"requested_by\":\"admin\"}" },
  { "key": "/s3worker/rotations/brokenrot", "value": "{битый" },
  { "key": "/s3worker/rotations/nofields", "value": "{}" }
]
```

- [ ] **Шаг 2: тесты парсера (AAA)**

`S3ParserTests.cs` (порт `ValkeyParserTests.cs`; все фикстуры — AAA-комментарии):

```csharp
using AdminPanel.Core.S3;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер s3-домена: канон arch/23 §2.1/§3 (приёмочные) + толерантность §2.
public sealed class S3ParserTests
{
    // Arrange: канонические фикстуры §2.1/§3. Act: ParseClusters.
    // Assert: поля дословно; pki — молча (unknownKeys не растит).
    [Fact]
    public void ParseClusters_Canonical_BuildsModel()
    {
        var result = S3Parser.ParseClusters(EtcdFixtures.LoadKv("S3/clusters-canonical.json"));

        var main = Assert.Single(result.Clusters, c => c.Name == "backupsmain");
        Assert.Equal(S3ClusterState.Active, main.State);
        Assert.Equal("owns3", main.Platform);
        Assert.Equal(3, main.NodesDeclared);
        Assert.Equal("backups-main", main.SecretProfile);
        Assert.Equal(1760156800L, main.CreatedUnix);
        Assert.Equal("h1:18001,h2:18002,h3:18003", main.Endpoints);
        Assert.Equal(2, main.Links.Count);
        Assert.Equal(("east1", "east2"), (main.Links[0].A, main.Links[0].B));
        Assert.Equal(["west1"], main.DenyList);
        Assert.Equal("OK", main.Connectivity!.State);
        Assert.Equal(1760156900L, main.Connectivity.CheckedUnix);
        Assert.Equal("ok", main.Connectivity.Links["node1"]["node2"]);
        Assert.Equal("SYNCED", main.Sync!.State);
        Assert.Equal(0, main.Sync.Pairs[0].Missing);

        var east1 = Assert.Single(main.NodesList, n => n.Name == "east1");
        Assert.Equal("RUNNING", east1.State);
        Assert.Equal("h1", east1.PlacedOn);
        Assert.Equal("h1", east1.Placement!.DockerHost);
        Assert.Equal(2m, east1.Resources!.Cpu);
        Assert.Equal(4, east1.Resources.MemGi);
        Assert.Equal(200, east1.Resources.DiskGi);
        Assert.Equal(107374182400L, east1.Usage!.UsedBytes);
        Assert.Equal(214748364800L, east1.Usage.TotalBytes);

        // swarm-селектор и комбинированный (конъюнкция атрибутов).
        Assert.Equal("dc1", Assert.Single(main.NodesList, n => n.Name == "east2").Placement!.SwarmLabels!["geo"]);
        var swarmy = Assert.Single(result.Clusters, c => c.Name == "swarmy");
        var s1 = Assert.Single(swarmy.NodesList, n => n.Name == "s1");
        Assert.Equal("h2", s1.Placement!.DockerHost);
        Assert.Equal(2, s1.Placement.SwarmLabels!.Count);

        // заявка NOT_INITIALIZED (0 инстансов — декларация без развёртывания) и TO_REMOVE.
        var claim = Assert.Single(result.Clusters, c => c.Name == "newclaim");
        Assert.Equal(S3ClusterState.NotInitialized, claim.State);
        Assert.Equal(0, claim.NodesDeclared);
        Assert.Empty(claim.NodesList);
        Assert.Equal(S3ClusterState.ToRemove, Assert.Single(result.Clusters, c => c.Name == "dying").State);

        // pki-ключи пропущены МОЛЧА: unknownKeys считает только future_feature.
        Assert.Equal(1, result.UnknownKeyCount);
        Assert.Empty(result.Errors);
    }

    // Arrange: вырожденные. Act: ParseClusters. Assert: parseError-записи,
    // незнакомое state → Active, пустой endpoints → null, битые ресурсы → null-поля.
    [Fact]
    public void ParseClusters_Tolerance_TableRows()
    {
        var result = S3Parser.ParseClusters(EtcdFixtures.LoadKv("S3/clusters-tolerance.json"));

        Assert.Contains(result.Errors, e => e.Key == "/s3/clusters/bad/config");
        Assert.Contains(result.Errors, e => e.Key == "/s3/clusters/bad/links");
        var odd = Assert.Single(result.Clusters, c => c.Name == "odd");
        Assert.Equal(S3ClusterState.Active, odd.State);          // STRANGE_NEW → Active
        Assert.Null(odd.Endpoints);                              // пробельный → null
        var n1 = Assert.Single(odd.NodesList, n => n.Name == "n1");
        Assert.Null(n1.Resources);                               // cpu не число → весь resources битый (parseError)
        Assert.Null(n1.Usage);                                   // нет total_bytes → битый usage
        Assert.Equal("DOWN", odd.Connectivity!.State);
        Assert.Equal("все ноды молчат", odd.Connectivity.ProbeError);
        Assert.Equal("DIVERGED", odd.Sync!.State);
        Assert.Equal(1024L, odd.Sync.Pairs[0].LagBytes);
        Assert.Equal("diff page", odd.Sync.LastError);
    }

    // Arrange: rotations-фикстура. Act: ParseRotations. Assert: тикет жив,
    // битый JSON / нет requested_unix → parseError.
    [Fact]
    public void ParseRotations_CanonicalAndTolerance()
    {
        var result = S3Parser.ParseRotations(EtcdFixtures.LoadKv("S3/rotations.json"));

        var ticket = Assert.Single(result.Tickets);
        Assert.Equal("backupsmain", ticket.Cluster);
        Assert.Equal(1760157000L, ticket.RequestedUnix);
        Assert.Equal("admin", ticket.RequestedBy);
        Assert.Equal(2, result.Errors.Count);
    }
}
```

Примечание к ассертам: поведение «битый cpu валит весь resources-ключ в parseError» фиксируется реализацией шага 4 (как у valkey: cpu не число → parseError, поля null); «usage без total_bytes → null + parseError» — так же.

- [ ] **Шаг 3: прогнать тесты — падают (нет S3Parser)**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3ParserTests"`
Ожидание: FAIL (CS0103 `S3Parser` не найден).

- [ ] **Шаг 4: реализация S3Parser.cs**

`src/AdminPanel.Etcd/Parsing/S3Parser.cs` — порт `ValkeyParser.cs` на схему arch/23 §2/§3:

```csharp
using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.S3;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /s3/clusters/ (arch/02 §12.1).
public sealed record S3ClustersParseResult(
    IReadOnlyList<S3ClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Результат разбора очереди ротаций /s3worker/rotations/ (arch/23 §8).
public sealed record S3RotationsParseResult(
    IReadOnlyList<S3RotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Парсер s3-домена: чистые функции Kv[] → модель, битые значения не бросают
// исключений — порождают KeyParseError (порт ValkeyParser; arch/23 §2).
// PKI-ключи (pki/*, nodes/<I>/pki) пропускаются МОЛЧА — материал только
// воркеру (arch/23 §6), без unknownKeys-счётчика.
public static class S3Parser
{
    private sealed class NodeAcc(string name)
    {
        public readonly string Name = name;
        public string? State;
        public string? PlacedOn;
        public string? PlacementRaw;
        public string? ResourcesRaw;
        public string? UsageRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? Endpoints;
        public string? LinksRaw;
        public string? DenyListRaw;
        public string? ConnectivityRaw;
        public string? SyncRaw;
        public readonly Dictionary<string, NodeAcc> Nodes = [];
    }

    public static S3ClustersParseResult ParseClusters(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/s3/clusters/<C>/leaf…" → ["", "s3", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "s3" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments[4])
            {
                // PKI репликации — только воркеру (arch/23 §6): молча, без unknownKeys.
                case "pki":
                    break;

                case "config" when segments.Length == 5:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "endpoints" when segments.Length == 5:
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                case "links" when segments.Length == 5:
                    acc.LinksRaw = kv.Value;
                    break;

                case "denylist" when segments.Length == 5:
                    acc.DenyListRaw = kv.Value;
                    break;

                case "connectivity" when segments.Length == 5:
                    acc.ConnectivityRaw = kv.Value;
                    break;

                case "sync" when segments.Length == 5:
                    acc.SyncRaw = kv.Value;
                    break;

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "placement" or "resources" or "state" or "placed_on"
                        or "usage" or "pki":
                {
                    if (segments[6] == "pki")
                        break; // PKI инстанса — молча (arch/23 §6)

                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    switch (segments[6])
                    {
                        case "placement":
                            node.PlacementRaw = kv.Value;
                            break;
                        case "resources":
                            node.ResourcesRaw = kv.Value;
                            break;
                        case "state":
                            node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "placed_on":
                            node.PlacedOn = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                            break;
                        case "usage":
                            node.UsageRaw = kv.Value;
                            break;
                    }

                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/23 §2)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new S3ClustersParseResult(clusters, errors, unknown);
    }

    // Ротации: {"requested_unix","requested_by"}; битый JSON / нет
    // requested_unix → parseError-запись (ключ не трогаем).
    public static S3RotationsParseResult ParseRotations(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<S3RotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/s3worker/rotations/<C>" → ["", "s3worker", "rotations", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /s3worker/rotations/<cluster>"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                var requested = JsonValues.ReadLong(root, "requested_unix");
                if (requested is null)
                {
                    errors.Add(new(kv.Key, "нет поля requested_unix"));
                    continue;
                }

                tickets.Add(new S3RotationTicket(
                    segments[3], requested.Value, JsonValues.ReadString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    // Сборка кластера из накопленных raw-значений (толерантно: битое — parseError,
    // поле пустое, кластер-скелет строится).
    private static S3ClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (platform, nodes, profile, created, state) = ParseConfig(acc.Name, acc.ConfigRaw, errors);
        return new S3ClusterInfo(
            acc.Name,
            state,
            platform,
            nodes,
            profile,
            created,
            acc.Endpoints,
            [.. acc.Nodes.OrderBy(p => p.Key, StringComparer.Ordinal)
                .Select(p => BuildNode(acc.Name, p.Value, errors))],
            ParseLinks(acc.Name, acc.LinksRaw, errors),
            ParseDenyList(acc.Name, acc.DenyListRaw, errors),
            ParseConnectivity(acc.Name, acc.ConnectivityRaw, errors),
            ParseSync(acc.Name, acc.SyncRaw, errors));
    }

    // config: {"platform","nodes","secret_profile","created_unix","state"?}.
    private static (string Platform, int Nodes, string SecretProfile, long? CreatedUnix,
        S3ClusterState State) ParseConfig(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return ("", 0, "", null, S3ClusterState.Active);

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return (
                JsonValues.ReadString(root, "platform") ?? "",
                AsInt(JsonValues.ReadLong(root, "nodes")),
                JsonValues.ReadString(root, "secret_profile") ?? "",
                JsonValues.ReadLong(root, "created_unix"),
                S3ClusterStates.Parse(JsonValues.ReadString(root, "state")));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/config", "битый JSON config"));
            return ("", 0, "", null, S3ClusterState.Active);
        }
    }

    // placement: {"docker_host"?,"swarm_node"?,"swarm_labels"?{…}} — конъюнкция
    // указанных атрибутов (arch/23 §4); пустой объект → null (не ошибка).
    private static S3NodePlacement? ParsePlacement(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            IReadOnlyDictionary<string, string>? labels = null;
            if (root.TryGetProperty("swarm_labels", out var labelsEl)
                && labelsEl.ValueKind == JsonValueKind.Object)
            {
                var dict = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var prop in labelsEl.EnumerateObject())
                    dict[prop.Name] = prop.Value.GetString() ?? "";
                labels = dict;
            }

            return new S3NodePlacement(
                JsonValues.ReadString(root, "docker_host"),
                JsonValues.ReadString(root, "swarm_node"),
                labels);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/placement", "битый JSON placement"));
            return null;
        }
    }

    // resources: cpu — decimal invariant; mem/disk — "<int>Gi" → int (как valkey).
    private static S3NodeResources? ParseResources(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var cpuRaw = JsonValues.ReadString(root, "cpu");
            if (cpuRaw is null
                || !decimal.TryParse(cpuRaw, NumberStyles.Number, CultureInfo.InvariantCulture, out var cpu))
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/resources", "поле cpu не число"));
                return null;
            }

            var memGi = ParseGi(JsonValues.ReadString(root, "mem"));
            var diskGi = ParseGi(JsonValues.ReadString(root, "disk"));
            if (memGi is null || diskGi is null)
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/resources", "mem/disk не формата <int>Gi"));
                return null;
            }

            return new S3NodeResources(cpu, memGi, diskGi);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/resources", "битый JSON resources"));
            return null;
        }
    }

    // usage: {"used_bytes","total_bytes","updated_unix"} — все три обязательны.
    private static S3NodeUsage? ParseUsage(string cluster, string node, string? raw,
        List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var used = JsonValues.ReadLong(root, "used_bytes");
            var total = JsonValues.ReadLong(root, "total_bytes");
            var updated = JsonValues.ReadLong(root, "updated_unix");
            if (used is null || total is null || updated is null)
            {
                errors.Add(new KeyParseError(
                    $"/s3/clusters/{cluster}/nodes/{node}/usage",
                    "нет обязательных полей used_bytes/total_bytes/updated_unix"));
                return null;
            }

            return new S3NodeUsage(used.Value, total.Value, updated.Value);
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError(
                $"/s3/clusters/{cluster}/nodes/{node}/usage", "битый JSON usage"));
            return null;
        }
    }

    private static S3NodeInfo BuildNode(string cluster, NodeAcc acc, List<KeyParseError> errors)
        => new(
            acc.Name,
            acc.State,
            acc.PlacedOn,
            ParsePlacement(cluster, acc.Name, acc.PlacementRaw, errors),
            ParseResources(cluster, acc.Name, acc.ResourcesRaw, errors),
            ParseUsage(cluster, acc.Name, acc.UsageRaw, errors));

    // links: [{"a","b"},…] — произвольный набор пар (arch/23 §2).
    private static IReadOnlyList<S3ReplicationLink> ParseLinks(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return [];
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                errors.Add(new KeyParseError($"/s3/clusters/{cluster}/links", "значение не массив"));
                return [];
            }

            return [.. doc.RootElement.EnumerateArray().Select(el => new S3ReplicationLink(
                JsonValues.ReadString(el, "a") ?? "",
                JsonValues.ReadString(el, "b") ?? ""))];
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/links", "битый JSON links"));
            return [];
        }
    }

    // denylist: ["<I>",…] — отозванные инстансы.
    private static IReadOnlyList<string> ParseDenyList(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return [];
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                errors.Add(new KeyParseError($"/s3/clusters/{cluster}/denylist", "значение не массив"));
                return [];
            }

            return [.. doc.RootElement.EnumerateArray()
                .Where(el => el.ValueKind == JsonValueKind.String)
                .Select(el => el.GetString() ?? "")];
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/denylist", "битый JSON denylist"));
            return [];
        }
    }

    // connectivity (arch/23 §3): state/checked_unix/links-матрица/probe_error?.
    private static S3ConnectivityInfo? ParseConnectivity(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var matrix = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.Ordinal);
            if (root.TryGetProperty("links", out var linksEl)
                && linksEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var row in linksEl.EnumerateObject())
                {
                    var cells = new Dictionary<string, string>(StringComparer.Ordinal);
                    if (row.Value.ValueKind == JsonValueKind.Object)
                        foreach (var cell in row.Value.EnumerateObject())
                            cells[cell.Name] = cell.Value.GetString() ?? "";
                    matrix[row.Name] = cells;
                }
            }

            return new S3ConnectivityInfo(
                JsonValues.ReadString(root, "state"),
                JsonValues.ReadLong(root, "checked_unix"),
                matrix,
                JsonValues.ReadString(root, "probe_error"));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/connectivity", "битый JSON connectivity"));
            return null;
        }
    }

    // sync (arch/23 §3): state/checked_unix/pairs/last_error?.
    private static S3SyncInfo? ParseSync(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var pairs = new List<S3SyncPair>();
            if (root.TryGetProperty("pairs", out var pairsEl)
                && pairsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in pairsEl.EnumerateArray())
                    pairs.Add(new S3SyncPair(
                        JsonValues.ReadString(el, "from") ?? "",
                        JsonValues.ReadString(el, "to") ?? "",
                        JsonValues.ReadLong(el, "missing") ?? 0,
                        JsonValues.ReadLong(el, "extra") ?? 0,
                        JsonValues.ReadLong(el, "lag_bytes"),
                        JsonValues.ReadLong(el, "lag_objects")));
            }

            return new S3SyncInfo(
                JsonValues.ReadString(root, "state"),
                JsonValues.ReadLong(root, "checked_unix"),
                pairs,
                JsonValues.ReadString(root, "last_error"));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/s3/clusters/{cluster}/sync", "битый JSON sync"));
            return null;
        }
    }

    private static int? ParseGi(string? raw)
        => raw is not null && raw.EndsWith("Gi", StringComparison.Ordinal)
           && int.TryParse(raw[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static int AsInt(long? value) => value is null ? 0 : checked((int)value.Value);

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
```

- [ ] **Шаг 5: прогнать тесты — зелёные**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3ParserTests"`
Ожидание: PASS (все 3 теста).

- [ ] **Шаг 6: коммит**

```bash
git add src/AdminPanel.Etcd/Parsing/S3Parser.cs src/tests/AdminPanel.UnitTests/S3ParserTests.cs src/tests/AdminPanel.UnitTests/EtcdFixtures/S3/
git commit -m "t34: S3Parser — парсер /s3/ и /s3worker/rotations/ (канон arch/23 §2.1/§3, pki молча)"
```

---

### Task 6: Core — S3AlertEngine + пороги

**Files:**
- Create: `src/AdminPanel.Core/S3/S3Alerting/S3AlertsOptions.cs`
- Create: `src/AdminPanel.Core/S3/S3Alerting/S3AlertEngine.cs`
- Test: `src/tests/AdminPanel.UnitTests/S3AlertRulesTests.cs`

**Interfaces:**
- Consumes: модель Task 4, `Alert`/`AlertSeverity`/`AlertRemedy` (Core/Alert.cs), паттерн `ValkeyAlertEngine`.
- Produces: `IS3AlertEngine.Evaluate(S3Snapshot next, S3Snapshot? previous) → IReadOnlyList<Alert>` — вызывает refresher (Task 7); пороги `AdminPanel:S3Alerts`.

- [ ] **Шаг 1: пороги**

`S3AlertsOptions.cs` (порт `ValkeyAlertsOptions.cs`; пороги — spec §3.2 §12.4):

```csharp
using Shared.Core.DI;

namespace AdminPanel.Core.S3.S3Alerting;

// [Config]-POCO порогов s3-алертов: секция AdminPanel:S3Alerts (arch/03 §12.4).
// Регистрация — автоскан AddCore().
[Config("AdminPanel:S3Alerts")]
public class S3AlertsOptions
{
    // s3-node-not-running: PROVISIONING младше N секунд не алертится.
    public int FreshProvisioningSeconds { get; set; } = 60;

    // s3-rotation-stale: живая заявка ротации старше N секунд.
    public int RotationStaleSeconds { get; set; } = 1800;

    // s3-sync-stale: connectivity/sync отсутствуют или checked_unix старше N c.
    public int SyncStaleSec { get; set; } = 300;

    // s3-node-disk-usage-high: used/total > N процентов.
    public int DiskUsagePercent { get; set; } = 85;
}
```

- [ ] **Шаг 2: тесты правил (AAA; таблицы условий)**

`S3AlertRulesTests.cs` — по образцу `ValkeyAlertRulesTests.cs` (снапшоты строятся хелпером). Каждый тест — три блока AAA-комментариев. Обязательный набор кейсов (условие → kind/severity):

```csharp
using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.S3.S3Alerting;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Каталог s3-алертов (arch/03 §12.4): таблицы правил условие → kind/severity.
public sealed class S3AlertRulesTests
{
    private static readonly S3AlertsOptions Options = new()
    {
        FreshProvisioningSeconds = 60,
        RotationStaleSeconds = 1800,
        SyncStaleSec = 300,
        DiskUsagePercent = 85,
    };

    private static S3Snapshot Evaluate(S3Snapshot next, S3Snapshot? prev = null)
        => next with { Alerts = new S3AlertEngine(OptionsOf()).Evaluate(next, prev) };

    private static IOptions<S3AlertsOptions> OptionsOf() => Microsoft.Extensions.Options.Options.Create(Options);

    private static S3Snapshot Snapshot(
        IReadOnlyList<S3ClusterInfo>? clusters = null,
        IReadOnlyList<S3RotationTicket>? rotations = null,
        IReadOnlyList<WorkerEndpoint>? endpoints = null,
        IReadOnlyList<WorkerHealth>? health = null,
        IReadOnlyList<KeyParseError>? parseErrors = null,
        DateTimeOffset? at = null)
        => new(
            at ?? DateTimeOffset.FromUnixTimeSeconds(1760160000),
            EtcdReachable: true, ConsecutiveFailures: 0,
            clusters ?? [], rotations ?? [], endpoints ?? [], health ?? [],
            [], parseErrors ?? [], 0);

    private static S3ClusterInfo Cluster(
        string name = "demo",
        S3ClusterState state = S3ClusterState.Active,
        IReadOnlyList<S3NodeInfo>? nodes = null,
        string? endpoints = "h1:18001",
        IReadOnlyList<S3ReplicationLink>? links = null,
        S3ConnectivityInfo? connectivity = null,
        S3SyncInfo? sync = null)
        => new(name, state, "owns3", nodes?.Count ?? 0, "backups-main", 1760156800L,
            endpoints, nodes ?? [], links ?? [], [], connectivity, sync);

    private static S3NodeInfo Node(
        string name = "east1",
        string? state = "RUNNING",
        string? placedOn = "h1",
        S3NodeResources? resources = null,
        S3NodeUsage? usage = null)
        => new(name, state, placedOn, new S3NodePlacement("h1", null, null), resources, usage);

    // Arrange: кластеры в NOT_INITIALIZED/TO_REMOVE. Act: Evaluate.
    // Assert: info-алерты lifecycle (§12.4 строки 1–2).
    [Fact]
    public void LifecycleStates_ProduceInfoAlerts()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("claim", S3ClusterState.NotInitialized),
            Cluster("dying", S3ClusterState.ToRemove),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-cluster-not-initialized"
            && a.Severity == AlertSeverity.Info && a.Target == "claim");
        Assert.Contains(alerts, a => a.Kind == "s3-cluster-to-remove"
            && a.Severity == AlertSeverity.Info && a.Target == "dying");
    }

    // Arrange: Active-кластер, нода не RUNNING (несвежий PROVISIONING — тик назад
    // тоже PROVISIONING, окно истекло) и свежий PROVISIONING (prev на 50 c раньше).
    // Act: Evaluate. Assert: critical только у несвежей (строка 3).
    [Fact]
    public void NodeNotRunning_Critical_ExceptFreshProvisioning()
    {
        var prev = Snapshot([Cluster(nodes: [Node("booting", "PROVISIONING")])],
            at: DateTimeOffset.FromUnixTimeSeconds(1760159950));
        var next = Snapshot([Cluster(nodes: [
            Node("stuck", "UNREACHABLE"),
            Node("booting", "PROVISIONING"),
        ])]);
        var alerts = Evaluate(next, prev).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-node-not-running"
            && a.Severity == AlertSeverity.Critical && a.Target == "demo/stuck");
        Assert.DoesNotContain(alerts, a => a.Target == "demo/booting");
    }

    // Arrange: Active с нодами без endpoints / DEGRADED / DOWN / LAGGING / DIVERGED.
    // Act: Evaluate. Assert: строки 4, 5–8 каталога.
    [Fact]
    public void ObservabilityRules_Table()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("noep", nodes: [Node()], endpoints: null),
            Cluster("deg", connectivity: new S3ConnectivityInfo("DEGRADED", 1760159900, [], null)),
            Cluster("down", connectivity: new S3ConnectivityInfo("DOWN", 1760159900, [], null)),
            Cluster("lag", sync: new S3SyncInfo("LAGGING", 1760159900, [], null)),
            Cluster("div", sync: new S3SyncInfo("DIVERGED", 1760159900, [], null)),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-endpoints-missing"
            && a.Severity == AlertSeverity.Critical && a.Target == "noep");
        Assert.Contains(alerts, a => a.Kind == "s3-connectivity-degraded"
            && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-connectivity-down"
            && a.Severity == AlertSeverity.Critical);
        Assert.Contains(alerts, a => a.Kind == "s3-sync-lagging"
            && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-sync-diverged"
            && a.Severity == AlertSeverity.Critical);
    }

    // Arrange: Active с RUNNING-нодами: зонд молчит (ключей нет) и протухший
    // checked_unix (1760156900 при now=1760160000 > 300 c). Act: Evaluate.
    // Assert: s3-sync-stale warning оба кластера (строка 9).
    [Fact]
    public void SyncStale_Warning_WhenSilentOrOld()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("silent", nodes: [Node()]),
            Cluster("stale", nodes: [Node()],
                connectivity: new S3ConnectivityInfo("OK", 1760156900, [], null)),
        ])).Alerts;

        var stale = alerts.Where(a => a.Kind == "s3-sync-stale").ToList();
        Assert.Equal(2, stale.Count);
        Assert.All(stale, a => Assert.Equal(AlertSeverity.Warning, a.Severity));
    }

    // Arrange: usage 90% от total и used > декларации disk (200 GiB = 214748364800).
    // Act: Evaluate. Assert: s3-node-disk-usage-high warning оба (строка 10).
    [Fact]
    public void DiskUsageHigh_Warning()
    {
        var alerts = Evaluate(Snapshot([
            Cluster(nodes: [
                Node("pct", usage: new S3NodeUsage(90, 100, 1760159900)),
                Node("over", resources: new S3NodeResources(2m, 4, 200),
                    usage: new S3NodeUsage(300_000_000_000, 400_000_000_000, 1760159900)),
            ]),
        ])).Alerts;

        var disk = alerts.Where(a => a.Kind == "s3-node-disk-usage-high").ToList();
        Assert.Equal(2, disk.Count);
    }

    // Arrange: две ноды на одном placed_on; нода без единой связи в links.
    // Act: Evaluate. Assert: s3-nodes-same-host и s3-node-not-in-mesh warning (11–12).
    [Fact]
    public void AntiAffinityAndMeshRules()
    {
        var alerts = Evaluate(Snapshot([
            Cluster(links: [new S3ReplicationLink("east1", "east2")], nodes: [
                Node("east1"), Node("east2", placedOn: "h1"), Node("lonely"),
            ]),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-nodes-same-host" && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-node-not-in-mesh" && a.Target == "demo/lonely");
    }

    // Arrange: живая заявка ротации свежая и старше порога; parseError.
    // Act: Evaluate. Assert: pending info, stale warning, malformed warning (13, 14, 15).
    [Fact]
    public void RotationAndMalformedRules()
    {
        var alerts = Evaluate(Snapshot(
            rotations: [new S3RotationTicket("demo", 1760158000, "admin")],
            parseErrors: [new KeyParseError("/s3/clusters/bad/config", "битый JSON")])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-rotation-pending" && a.Severity == AlertSeverity.Info);
        Assert.Contains(alerts, a => a.Kind == "s3-rotation-stale" && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-key-malformed" && a.Severity == AlertSeverity.Warning);
    }

    // Arrange: нет живых /s3worker/api/; живой ключ с Degraded-health.
    // Act: Evaluate. Assert: worker-api-unreachable critical / worker-unhealthy warning (16–17).
    [Fact]
    public void WorkerAvailabilityRules()
    {
        var unreachable = Evaluate(Snapshot()).Alerts;
        Assert.Contains(unreachable, a => a.Kind == "worker-api-unreachable"
            && a.Severity == AlertSeverity.Critical && a.Target == "s3worker");

        var unhealthy = Evaluate(Snapshot(
            endpoints: [new WorkerEndpoint("i1", "https://s3worker:8080", 1760150000)],
            health: [new WorkerHealth("i1", "https://s3worker:8080",
                WorkerHealthStatus.Degraded,
                DateTimeOffset.FromUnixTimeSeconds(1760159900), "HTTP 503")])).Alerts;
        Assert.Contains(unhealthy, a => a.Kind == "worker-unhealthy"
            && a.Severity == AlertSeverity.Warning && a.Target == "s3worker/i1");
    }

    // Arrange: алерт живёт два тика. Act: Evaluate(next, prev). Assert:
    // sinceUnix стабильный id=kind:target (механика §2 03-panels).
    [Fact]
    public void SinceUnix_StableAcrossTicks()
    {
        var first = Evaluate(Snapshot([Cluster("down2",
            connectivity: new S3ConnectivityInfo("DOWN", 1760159900, [], null))]));
        var prevAlert = Assert.Single(first.Alerts, a => a.Kind == "s3-connectivity-down");
        var firstSince = prevAlert.SinceUnix;

        var second = Evaluate(Snapshot([Cluster("down2",
            connectivity: new S3ConnectivityInfo("DOWN", 1760159950, [], null))]), first);
        var nextAlert = Assert.Single(second.Alerts, a => a.Kind == "s3-connectivity-down");
        Assert.Equal(firstSince, nextAlert.SinceUnix);
    }

    // Arrange: все алерты. Act: проверка Hint/Remedy. Assert: пустых нет (§4.1).
    [Fact]
    public void EveryAlert_CarriesHintAndRemedy()
    {
        var alerts = Evaluate(Snapshot(
            clusters: [Cluster("x", S3ClusterState.NotInitialized)],
            rotations: [new S3RotationTicket("x", 1, null)],
            parseErrors: [new KeyParseError("/s3/clusters/b/config", "битый")])).Alerts;

        Assert.All(alerts, a =>
        {
            Assert.False(string.IsNullOrWhiteSpace(a.Hint));
            Assert.False(string.IsNullOrWhiteSpace(a.RemedyText));
        });
    }
}
```

- [ ] **Шаг 3: прогнать — падают (нет движка)**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3AlertRulesTests"`
Ожидание: FAIL (нет `S3AlertEngine`).

- [ ] **Шаг 4: реализация S3AlertEngine.cs**

`S3AlertEngine.cs` — точный порт скелета `ValkeyAlertEngine.cs` (Evaluate + ResolveSince + IsFreshProvisioning — дословно та же механика, типы заменены на S3*). Каталог `Enumerate` — 17 правил, каждое `yield return new Alert(...)` с Hint/Remedy. Содержание правил (kind → условие → параметры Alert):

1. `worker-api-unreachable` (critical, target `s3worker`): `next.WorkerEndpoints.Count == 0` — «нет живых ключей /s3worker/api/ — s3-мутации панели 503»; Remedy `OperatorRunbook` («запустите контейнер s3worker (профиль s3 deploy-контура), проверьте /healthz и S3Worker:Api:AdvertiseUrl»).
2. `worker-unhealthy` (warning, target `s3worker/<id>`): по `next.WorkerHealth` со статусом ≠ Healthy (порт valkey: Degraded → «/healthz ≠ 200», Unreachable → «недостижим по URL lease-ключа»); Remedy `OperatorRunbook`.
3. `s3-cluster-not-initialized` (info): state=NotInitialized; Remedy `WorkerAuto` («дождитесь provisioning — воркер снимет NOT_INITIALIZED; висит — смотрите /s3worker/work/<C>»).
4. `s3-cluster-to-remove` (info): state=ToRemove; Remedy `WorkerAuto`.
5. `s3-node-not-running` (critical, target `<C>/<I>`): Active-кластер, `node.State ∉ {null, "RUNNING"}`, кроме fresh-PROVISIONING (порт `IsFreshProvisioning`); Remedy `WorkerAuto` («надзор C пересоздаст контейнер; висит — проверьте контейнер s3w-<C>-<I>»).
6. `s3-endpoints-missing` (critical): Active-кластер с `NodesList.Count > 0` и `Endpoints` null/пусто; Remedy `WorkerAuto`.
7. `s3-connectivity-degraded` (warning): `Connectivity?.State == "DEGRADED"`.
8. `s3-connectivity-down` (critical): `Connectivity?.State == "DOWN"`.
9. `s3-sync-lagging` (warning): `Sync?.State == "LAGGING"`.
10. `s3-sync-diverged` (critical): `Sync?.State == "DIVERGED"`.
11. `s3-sync-stale` (warning): Active-кластер с RUNNING-нодами, где `Connectivity` или `Sync` отсутствуют, ЛИБО `CheckedUnix` любой из них старше `now − SyncStaleSec` — «зонд молчит» (один алерт на кластер, id `s3-sync-stale:<C>`); Remedy `OperatorRunbook` («проверьте зонд воркера: /s3worker/work/<C> op=sync-probe»).
12. `s3-node-disk-usage-high` (warning, target `<C>/<I>`): `Usage.TotalBytes > 0 && UsedBytes*100/TotalBytes > DiskUsagePercent` ИЛИ `Resources.DiskGi` есть и `UsedBytes > DiskGi * 1024^3` — «занятость превышает порог/декларацию»; Remedy `OperatorApi` («увеличьте диск: PUT /api/s3/clusters/{c}/nodes/{n}/resources»).
13. `s3-nodes-same-host` (warning, target `<C>`): ≥ 2 нод Active-кластера с одинаковым непустым `PlacedOn` (группировка по PlacedOn, details — список имён и хоста); Remedy `OperatorRunbook` («гео-раснос — обязанность оператора, arch/23 §4; вывод/перевоз инстанса»).
14. `s3-node-not-in-mesh` (warning, target `<C>/<I>`): Active-кластер, нода (state RUNNING или null) не встречается ни в одной паре `Links`; Remedy `OperatorApi` («добавьте связь: PUT /api/s3/clusters/{c}/links»).
15. `s3-rotation-pending` (info): живая заявка `/s3worker/rotations/<C>` кластера из снапшота; Remedy `WorkerAuto`.
16. `s3-rotation-stale` (warning): та же заявка старше `RotationStaleSeconds` (порт `RotationStaleAlert` valkey); Remedy `WorkerAuto`.
17. `s3-key-malformed` (warning, target = ключ): по `next.ParseErrors`; Remedy `OperatorRunbook`.

Интерфейс и класс:

```csharp
public interface IS3AlertEngine
{
    IReadOnlyList<Alert> Evaluate(S3Snapshot next, S3Snapshot? previous);
}

[InjectAsSingleton(typeof(IS3AlertEngine))]
public sealed class S3AlertEngine(IOptions<S3AlertsOptions> options) : IS3AlertEngine
{
    // Evaluate: сортировка severity → kind → target; sinceUnix по стабильному id
    // из prev.Alerts — порт ValkeyAlertEngine.Evaluate дословно (замена типов).
}
```

Тексты Hint у каждого правила — конкретные русские формулировки по образцу valkey («что не так / как должно быть / зачем ключ»; короткие, 1–2 предложения). Пустых Hint/Remedy нет — это ассерт теста `EveryAlert_CarriesHintAndRemedy`.

- [ ] **Шаг 5: прогнать — зелёные**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3AlertRulesTests"`
Ожидание: PASS (10 тестов).

- [ ] **Шаг 6: коммит**

```bash
git add src/AdminPanel.Core/S3/S3Alerting/ src/tests/AdminPanel.UnitTests/S3AlertRulesTests.cs
git commit -m "t34: S3AlertEngine — каталог 17 правил (arch/03 §12.4), пороги AdminPanel:S3Alerts"
```

---

### Task 7: Etcd — стор + refresher + модуль AddS3()

**Files:**
- Create: `src/AdminPanel.Etcd/S3PanelOptions.cs`
- Create: `src/AdminPanel.Etcd/S3SnapshotStore.cs`
- Create: `src/AdminPanel.Etcd/Workers/S3WorkerHealthStore.cs`
- Create: `src/AdminPanel.Etcd/S3SnapshotRefresher.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (+`AddS3()`)
- Test: `src/tests/AdminPanel.UnitTests/S3RefresherTests.cs`

**Interfaces:**
- Consumes: модель Task 4, парсер Task 5, движок Task 6, `IEtcdGateway`, `WorkerEndpointsParser`, `WorkerCertParser`, `EtcdOptions` (sticky/failover-паттерн `ValkeySnapshotRefresher`).
- Produces: `IS3SnapshotStore`/`IS3SnapshotReader` (`Current`), `S3SnapshotRefresher.RefreshOnceAsync(ct)` (публичен для тестов), `AddS3()` — регистрация; `IS3WorkerHealthStore`.

- [ ] **Шаг 1: тест refresher'а на фейковом шлюзе (AAA)**

`S3RefresherTests.cs` — порт `ValkeyRefresherTests.cs` (мок `IEtcdGateway` отвечает префиксными Kv по `EtcdFixtures/S3/*` + `/s3worker/api/` + `/workers/api_tls/s3worker`; его устройство — открыть `src/tests/AdminPanel.UnitTests/ValkeyRefresherTests.cs` и скопировать механизм стаба шлюза 1:1). Кейсы:

```csharp
// Arrange: стаб шлюза с каноническими /s3/-ключами + ключ api + ключ серта.
// Act: RefreshOnceAsync. Assert: снапшот собран: кластеры, ротация сджойнена,
// WorkerEndpoints/WorkerApiCert заполнены, UnknownKeyCount перенесён.
[Fact] public void RefreshOnce_Success_BuildsSnapshot()

// Arrange: первый тик успешен; второй — все range-ответы Failed.
// Act: два RefreshOnceAsync. Assert: второй вернул Failed, Current — прежний
// снапшот (BuiltAtUtc не изменился), EtcdReachable=false, ConsecutiveFailures=1.
[Fact] public void RefreshOnce_TransportFail_KeepsPreviousData()

// Arrange: пустой /s3worker/api/. Act: RefreshOnceAsync. Assert: WorkerEndpoints
// пуст, алерт worker-api-unreachable в снапшоте.
[Fact] public void RefreshOnce_NoWorkerKeys_AlertsUnreachable()
```

- [ ] **Шаг 2: прогнать — падают (нет классов)**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3RefresherTests"`
Ожидание: FAIL (типы не найдены).

- [ ] **Шаг 3: реализация**

`S3PanelOptions.cs` — точный порт `ValkeyPanelOptions.cs`:

```csharp
using Shared.Core.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO s3-домена панели: секция AdminPanel:S3 (arch/02 §12).
// Endpoints etcd — общие с pg-циклом через AdminPanel:Etcd (EtcdOptions).
[Config("AdminPanel:S3")]
public class S3PanelOptions
{
    // Тик S3SnapshotRefresher. <= 0 — fallback 3 c (симметрия kafka/valkey).
    public double RefreshIntervalSeconds { get; set; } = 3;
}
```

`S3SnapshotStore.cs` — точный порт `ValkeySnapshotStore.cs` (замена типов Valkey→S3; интерфейсы `IS3SnapshotReader`, `IS3SnapshotStore : IS3SnapshotReader` c `Replace(S3Snapshot)`; комментарий «Регистрация — явно в ModuleExtensions.AddS3()»).

`Workers/S3WorkerHealthStore.cs` — точный порт `ValkeyWorkerHealthStore.cs` (`[InjectAsSingleton(typeof(IS3WorkerHealthStore))]`, `IS3WorkerHealthStore { IReadOnlyList<WorkerHealth>? Current; void Replace(...); }`).

`S3SnapshotRefresher.cs` — порт `ValkeySnapshotRefresher.cs` БЕЗ probe/secrets-механики (у S3 live-проб нет, кредов панель не читает):

```csharp
using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.S3.S3Alerting;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель s3-снапшота (arch/02 §12): тик RefreshIntervalSeconds,
// range /s3/clusters/ + /s3worker/rotations/ + /s3worker/api/ и точечный ключ
// /workers/api_tls/s3worker на активном endpoint (sticky + failover, опции
// общие с pg-циклом EtcdOptions). Транспортный провал любого чтения роняет тик:
// прежние данные, EtcdReachable=false, счётчик отказов. Регистрация — явно в
// ModuleExtensions.AddS3().
public sealed class S3SnapshotRefresher(
    IEtcdGateway gateway,
    IS3AlertEngine alertEngine,
    IS3SnapshotStore store,
    IOptions<EtcdOptions> etcdOptions,
    IOptions<S3PanelOptions> s3Options,
    TimeProvider time,
    ILogger<S3SnapshotRefresher> logger,
    AdminPanel.Etcd.Workers.IS3WorkerHealthStore workerHealthStore) : BackgroundService
{
    private string? _activeEndpoint;
    private bool _endpointsWarned;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Порт ValkeySnapshotRefresher.ExecuteAsync: <=0 → 3 c; первый тик сразу.
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста.
    public async Task<Result> RefreshOnceAsync(CancellationToken ct)
    {
        // 1) endpoints валидация (порт valkey: IsValidEndpoint + warn-once).
        // 2) 4 чтения RangeWithFailoverAsync (порт valkey-метода дословно):
        //    Prefixes.Clusters, Prefixes.Rotations, Prefixes.WorkerApi,
        //    Prefixes.WorkerApiCert (точечный ключ).
        //    Любой неуспех → FailTick(previous, now, "KV-чтения etcd не удались").
        // 3) Парсинг: S3Parser.ParseClusters / ParseRotations /
        //    WorkerEndpointsParser.Parse(workerApiKv) /
        //    WorkerCertParser.Parse(Prefixes.WorkerApiCert, certKv.FirstOrDefault()).
        // 4) Джойн ротаций по кластеру (rotationsByCluster, порт valkey):
        //    clusters.Clusters.Select(c => c with { Rotation = byCluster[c.Name] }).
        // 5) Сборка S3Snapshot(now, true, 0, clustersJoined, rotations.Tickets,
        //    workerApi.Endpoints, workerHealthStore.Current ?? [],
        //    Alerts: [], [.. errors всех парсеров], clusters.UnknownKeyCount,
        //    WorkerApiCert: certParsed.Cert).
        // 6) store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        //    return Result.Success();
    }

    // FailTick: порт valkey (прежние данные, Reachable=false, счётчик+1,
    // алерты пересчитываются и на отказном тике).
    // RangeWithFailoverAsync / IsValidEndpoint: порт valkey дословно.

    private static class Prefixes
    {
        public const string Clusters = "/s3/clusters/";
        public const string Rotations = "/s3worker/rotations/";
        public const string WorkerApi = "/s3worker/api/";
        public const string WorkerApiCert = "/workers/api_tls/s3worker";
    }
}
```

Тела методов ExecuteAsync/RefreshOnceAsync/FailTick/RangeWithFailoverAsync/IsValidEndpoint — копия `ValkeySnapshotRefresher.cs` (файл-образец в worktree) с точными заменами: `ValkeySnapshot→S3Snapshot`, `ValkeyPanelOptions→S3PanelOptions`, `ValkeyParser→S3Parser`, префиксы из `Prefixes` выше; блок ReadSecrets/MergeProbes/probeReader НЕ переносится; конструктор без `IValkeySecretsStore`/`IValkeyProbeReader`.

`ModuleExtensions.cs` — добавить метод (симметрия `AddValkey`):

```csharp
// Модуль s3-домена (arch/02 §12): hosted-service refresher'а + стор снапшота.
// Отдельный HttpClient не заводится — транспорт общий с pg/kafka/valkey-циклами.
public static IServiceCollection AddS3(this IServiceCollection services)
{
    services.AddSingleton<S3SnapshotStore>();
    services.AddSingleton<IS3SnapshotStore>(sp => sp.GetRequiredService<S3SnapshotStore>());
    services.AddSingleton<IS3SnapshotReader>(sp => sp.GetRequiredService<S3SnapshotStore>());

    services.AddSingleton<S3SnapshotRefresher>();
    services.AddHostedService(sp => sp.GetRequiredService<S3SnapshotRefresher>());
    return services;
}
```

- [ ] **Шаг 4: прогнать — зелёные; вся сборка без warning'ов**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` затем
`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3"`
Ожидание: build 0 warnings/errors; тесты PASS (Model+Parser+Alert+Refresher).

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Etcd/S3PanelOptions.cs src/AdminPanel.Etcd/S3SnapshotStore.cs src/AdminPanel.Etcd/S3SnapshotRefresher.cs src/AdminPanel.Etcd/Workers/S3WorkerHealthStore.cs src/AdminPanel.Etcd/ModuleExtensions.cs src/tests/AdminPanel.UnitTests/S3RefresherTests.cs
git commit -m "t34: s3-refresher/store/options — доменный тик 3 c, failover, серт /workers/api_tls/s3worker; AddS3()"
```

---

### Task 8: интеграция с гранью «Воркеры» (шлюз/поллер/thumbprints/серты)

**Files:**
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs` (конструктор + `ResolveEndpoints`)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs` (конструктор + s3-блок)
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (`TrustedWorkerThumbprints` + s3 в mTLS-билдере)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerCertService.cs` (гварды `"s3worker"`)
- Modify: `src/AdminPanel.Api/Operations/WorkersCommands.cs` (restart-гвард; `GenerateWorkerApiCertCommandHandler` — конструктор + `LiveHosts`)
- Modify: `src/AdminPanel.Api/Operations/WorkersModule.cs` (`GetWorkersQueryHandler` — конструктор + четвёртая карточка)
- Modify: `src/AdminPanel.Api/Program.cs` (`.AddS3()`)
- Test: `src/tests/AdminPanel.UnitTests/Workers/S3WorkerCardTests.cs`

**Interfaces:**
- Consumes: `IS3SnapshotStore`/`IS3SnapshotReader` (Task 7), `AddS3()`.
- Produces: `IWorkerApiGateway.SendAsync("s3worker", …)` работает (для Task 10); `GET /api/workers` отдаёт карточку s3worker; `/api/workers/s3worker/api-cert/*` и `/restart` валидны (критерий приёмки 6).

- [ ] **Шаг 1: тест карточки и гвардов (AAA)**

`Workers/S3WorkerCardTests.cs` (по образцу существующих тестов в `src/tests/AdminPanel.UnitTests/Workers/`):

```csharp
// Arrange: снапшот s3 с endpoint+cert. Act: GetWorkersQueryHandler.Handle
// (хендлер живёт в WorkersModule.cs). Assert: карточка s3worker с инстансом
// и TargetCert; ApplyStatus по thumbprint.
[Fact] public void WorkersView_ContainsS3WorkerCard()

// Arrange: worker="s3worker". Act: WorkerCertService.GenerateAndPutAsync гвард /
// RestartWorkerCommandHandler.Handle. Assert: не WorkerNotFoundException; мусорное
// имя по-прежнему 404 (WorkerNotFoundException).
[Fact] public void S3Worker_AcceptedByGuards_TrashStill404()

// Arrange: LiveHosts("s3worker", null, null, s3Snapshot с URL https://s3w:8080).
// Act: GenerateWorkerApiCertCommandHandler.LiveHosts. Assert: ["s3w"].
[Fact] public void LiveHosts_S3Worker_ResolvesAdvertiseHosts()
```

- [ ] **Шаг 2: прогнать — падают**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3WorkerCardTests"`
Ожидание: FAIL.

- [ ] **Шаг 3: правки (все — минимальные, по образцу valkeyworker-строк)**

1. `WorkerApiGateway.cs`: в конструктор добавить `IS3SnapshotStore s3Store`; в `ResolveEndpoints` — case `"s3worker" => s3Store.Current?.WorkerEndpoints` и обновить сообщение `ArgumentOutOfRangeException` («ожидался pgworker|kafkaworker|valkeyworker|s3worker»); обновить doc-comment шлюза.
2. `WorkerHealthPoller.cs`: в конструктор добавить `IS3SnapshotReader s3SnapshotReader, IS3WorkerHealthStore s3Store`; в `RunOnceAsync` после valkey-блока — s3-блок (копия valkey-блока: endpoints из `s3SnapshotReader.Current?.WorkerEndpoints ?? []`, `Task.WhenAll(ProbeAsync…)`, `s3Store.Replace(отсортированный список)`; комментарий «S3Worker-инстансы (arch/02 §2.3.4): тот же тик/клиент/семантика»).
3. `ModuleExtensions.cs` `AddEtcd()`: в `TrustedWorkerThumbprints` добавить `if (sp.GetRequiredService<IS3SnapshotStore>().Current?.WorkerApiCert is { } s3w) thumbs.Add(s3w.Thumbprint);`.
4. `WorkerCertService.cs`: три гварда `worker is not ("pgworker" or "kafkaworker" or "valkeyworker")` → добавить `or "s3worker"` (методы `GenerateAndPutAsync`/`PutAsync`/`DeleteAsync`); комментарий `WorkerNotFoundException` — дополнить перечень.
5. `WorkersCommands.cs`: (а) `RestartWorkerCommandHandler`: гвард `c.Worker is not ("pgworker" or "kafkaworker" or "valkeyworker" or "s3worker")`; (б) `GenerateWorkerApiCertCommandHandler`: конструктор `+ IS3SnapshotReader s3`, в `LiveHosts` — case `"s3worker" => s3?.WorkerEndpoints ?? []`.
6. `WorkersModule.cs`: `GetWorkersQueryHandler` — конструктор `+ IS3SnapshotReader s3`; в сборку `WorkersViewDto` добавить четвёртую карточку `new("s3worker", …)` по образцу valkeyworker (инстансы из `s?.WorkerEndpoints`, health из `s?.WorkerHealth`, серт `Cert(s?.WorkerApiCert)`).
7. `Program.cs`: после `.AddValkey()` добавить `.AddS3() // t34: s3-домен: refresher + стор (arch/02 §12)`.

- [ ] **Шаг 4: прогнать — зелёные**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3WorkerCardTests"`
Ожидание: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs src/AdminPanel.Etcd/Workers/WorkerCertService.cs src/AdminPanel.Etcd/ModuleExtensions.cs src/AdminPanel.Api/Operations/WorkersCommands.cs src/AdminPanel.Api/Operations/WorkersModule.cs src/AdminPanel.Api/Program.cs src/tests/AdminPanel.UnitTests/Workers/S3WorkerCardTests.cs
git commit -m "t34: грань Воркеры — s3worker в шлюзе/health-поллере/thumbprints/cert-сервисе/карточке; AddS3() в Program"
```

---

### Task 9: Api — инспекция S3 (GET /api/s3/clusters, alerts, overview)

**Files:**
- Create: `src/AdminPanel.Api/Inspection/S3Query.cs`
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs` (+`MapS3InspectionApi`)
- Modify: `src/AdminPanel.Api/Inspection/AlertsQuery.cs` (merge s3)
- Modify: `src/AdminPanel.Api/Inspection/OverviewQuery.cs` (+`OverviewS3Dto`)
- Modify: `src/AdminPanel.Api/Program.cs` (`MapS3InspectionApi()`)
- Test: `src/tests/AdminPanel.UnitTests/S3QueryHandlerTests.cs`

**Interfaces:**
- Consumes: `IS3SnapshotReader` (Task 7); паттерны `ValkeyQuery`/`ValkeyMappers`.
- Produces: `GET /api/s3/clusters → 200 [S3ClusterSummaryDto] | 503`; `GET /api/s3/clusters/{cluster} → 200 S3ClusterDto | 404 | 503`; s3-алерты в `/api/alerts`; s3-сводка в `/api/overview` (для Task 11–13 фронтенда).

- [ ] **Шаг 1: тесты хендлеров (AAA)**

`S3QueryHandlerTests.cs` (по образцу `InspectionQueryHandlerTests.cs`/valkey-частей):

```csharp
// Arrange: стор с одним Active-кластером (nodes/endpoints/connectivity/sync/
// rotation из TestSnapshots-подобного хелпера). Act: S3ClustersQueryHandler.
// Assert: сводка с nodesRunning, connectivityState, syncState, rotationPending.
[Fact] public void Summaries_MapFromSnapshot()

// Arrange: тот же стор. Act: S3ClusterDetailsQuery("demo"/"nope").
// Assert: детали со всеми секциями; "nope" → Failed(S3ClusterNotFound).
[Fact] public void Details_FoundAndNotFound()

// Arrange: null-стор. Act: хендлеры. Assert: Failed(SnapshotNotReadyException).
[Fact] public void NoSnapshot_503Semantics()
```

- [ ] **Шаг 2: прогнать — падают**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3QueryHandlerTests"`
Ожидание: FAIL.

- [ ] **Шаг 3: реализация**

`S3Query.cs` (порт `ValkeyQuery.cs`; DTO — дословно по arch/03 §12.2, который написан в Task 3):

```csharp
using AdminPanel.Core.S3;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запросы инспекции s3-домена (arch/03 §12.1): сводный список и детали.
public sealed record S3ClustersQuery : IQuery<IReadOnlyList<S3ClusterSummaryDto>>;

public sealed record S3ClusterDetailsQuery(string Cluster) : IQuery<S3ClusterDto>;

// Сводная строка списка кластеров (arch/03 §12.2).
public sealed record S3ClusterSummaryDto(
    string Name,
    string State,
    int NodesTotal,
    int NodesRunning,
    string? Endpoints,
    string? ConnectivityState,   // OK|DEGRADED|DOWN|null
    string? SyncState,           // SYNCED|LAGGING|DIVERGED|null
    bool RotationPending);

// Детали кластера: config, ноды, links, denylist, endpoints, зонды, ротация.
public sealed record S3ClusterDto(
    string Name,
    string State,
    string Platform,
    int NodesDeclared,
    string SecretProfile,
    long? CreatedUnix,
    string? Endpoints,
    IReadOnlyList<S3NodeDto> NodesList,
    IReadOnlyList<S3LinkDto> Links,
    IReadOnlyList<string> Denylist,
    S3ConnectivityDto? Connectivity,
    S3SyncDto? Sync,
    S3RotationDto? Rotation);

// Нода: raw state + размещение + ресурсы + занятость + denied (denylist).
public sealed record S3NodeDto(
    string Name,
    string? State,
    string? PlacedOn,
    S3PlacementDto? Placement,
    S3ResourcesDto? Resources,
    S3UsageDto? Usage,
    bool Denied);

public sealed record S3PlacementDto(
    string? DockerHost, string? SwarmNode, IReadOnlyDictionary<string, string>? SwarmLabels);
public sealed record S3ResourcesDto(decimal? Cpu, int? MemGi, int? DiskGi);
public sealed record S3UsageDto(long UsedBytes, long TotalBytes, long UpdatedUnix);
public sealed record S3LinkDto(string A, string B);
public sealed record S3ConnectivityDto(
    string? State, long? CheckedUnix,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> Links, string? ProbeError);
public sealed record S3SyncDto(
    string? State, long? CheckedUnix, IReadOnlyList<S3SyncPairDto> Pairs, string? LastError);
public sealed record S3SyncPairDto(
    string From, string To, long Missing, long Extra, long? LagBytes, long? LagObjects);
public sealed record S3RotationDto(long RequestedUnix, string? RequestedBy);

// Core → DTO: чистые функции (arch/03 §12.2; camelCase-зеркало модели).
public static class S3Mappers
{
    public static IReadOnlyList<S3ClusterSummaryDto> MapSummaries(S3Snapshot snapshot)
        => [.. snapshot.Clusters.Select(c => new S3ClusterSummaryDto(
            c.Name,
            StateName(c.State),
            c.NodesList.Count,
            c.NodesList.Count(n => n.State == "RUNNING"),
            c.Endpoints,
            c.Connectivity?.State,
            c.Sync?.State,
            c.Rotation is not null))];

    public static S3ClusterDto MapDetails(S3ClusterInfo cluster) => new(
        cluster.Name,
        StateName(cluster.State),
        cluster.Platform,
        cluster.NodesDeclared,
        cluster.SecretProfile,
        cluster.CreatedUnix,
        cluster.Endpoints,
        [.. cluster.NodesList.Select(n => new S3NodeDto(
            n.Name, n.State, n.PlacedOn,
            n.Placement is null ? null : new S3PlacementDto(
                n.Placement.DockerHost, n.Placement.SwarmNode, n.Placement.SwarmLabels),
            n.Resources is null ? null : new S3ResourcesDto(
                n.Resources.Cpu, n.Resources.MemGi, n.Resources.DiskGi),
            n.Usage is null ? null : new S3UsageDto(
                n.Usage.UsedBytes, n.Usage.TotalBytes, n.Usage.UpdatedUnix),
            cluster.DenyList.Contains(n.Name, StringComparer.Ordinal)))],
        [.. cluster.Links.Select(l => new S3LinkDto(l.A, l.B))],
        cluster.DenyList,
        cluster.Connectivity is null ? null : new S3ConnectivityDto(
            cluster.Connectivity.State, cluster.Connectivity.CheckedUnix,
            cluster.Connectivity.Links, cluster.Connectivity.ProbeError),
        cluster.Sync is null ? null : new S3SyncDto(
            cluster.Sync.State, cluster.Sync.CheckedUnix,
            [.. cluster.Sync.Pairs.Select(p => new S3SyncPairDto(
                p.From, p.To, p.Missing, p.Extra, p.LagBytes, p.LagObjects))],
            cluster.Sync.LastError),
        cluster.Rotation is null ? null : new S3RotationDto(
            cluster.Rotation.RequestedUnix, cluster.Rotation.RequestedBy));

    public static string StateName(S3ClusterState state) => state switch
    {
        S3ClusterState.NotInitialized => "NOT_INITIALIZED",
        S3ClusterState.ToRemove => "TO_REMOVE",
        _ => "ACTIVE",
    };
}

// Список: отказ «снапшота нет» — 503-семантика pg/kafka/valkey.
[InjectAsScoped]
public sealed class S3ClustersQueryHandler(IS3SnapshotReader store)
    : IQueryHandler<S3ClustersQuery, IReadOnlyList<S3ClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<S3ClusterSummaryDto>>> Handle(
        S3ClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<S3ClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<S3ClusterSummaryDto>>.Success(S3Mappers.MapSummaries(snapshot)));
    }
}

// Детали: 404 кластера нет в снапшоте (парсер собирает даже неполные префиксы).
[InjectAsScoped]
public sealed class S3ClusterDetailsQueryHandler(IS3SnapshotReader store)
    : IQueryHandler<S3ClusterDetailsQuery, S3ClusterDto>
{
    public ValueTask<Result<S3ClusterDto>> Handle(S3ClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<S3ClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<S3ClusterDto>.Failed(new S3ClusterNotFound(query.Cluster))
            : Result<S3ClusterDto>.Success(S3Mappers.MapDetails(cluster)));
    }
}

// Кластер отсутствует в s3-снапшоте — 404 (детали).
public sealed class S3ClusterNotFound(string cluster)
    : Exception($"S3-кластер {cluster} не найден в снапшоте");
```

`InspectionModule.cs` — добавить `MapS3InspectionApi` (точный порт `MapValkeyInspectionApi`: GET `/api/s3/clusters` → 200/503; GET `/api/s3/clusters/{cluster}` → 200 / 404 `S3ClusterNotFound` / 503 `S3 snapshot not ready`).

`AlertsQuery.cs`: в `AlertsQueryHandler` конструктор `+ IS3SnapshotReader s3Store`; `Merge(snapshot.Alerts, kafka…, valkey…, s3Store.Current?.Alerts)` — добавить четвёртый параметр в `Merge` и concat.

`OverviewQuery.cs`: добавить поле `OverviewS3Dto? S3` в `OverviewDto`; `OverviewMapper.MapS3(S3Snapshot?)` (порт `MapValkey`; critical-kinds — `s3-node-not-running`, `s3-endpoints-missing`, `s3-connectivity-down`, `s3-sync-diverged`); хендлер — конструктор `+ IS3SnapshotReader s3Store`, `S3 = OverviewMapper.MapS3(s3Store.Current)`.

`Program.cs`: после `app.MapValkeyInspectionApi();` → `app.MapS3InspectionApi(); // t34: инспекция s3-домена (arch/03 §12.1)`.

- [ ] **Шаг 4: прогнать — зелёные (юниты целиком)**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug`
Ожидание: PASS всей сборки тестов (регрессий нет).

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Api/Inspection/S3Query.cs src/AdminPanel.Api/Inspection/InspectionModule.cs src/AdminPanel.Api/Inspection/AlertsQuery.cs src/AdminPanel.Api/Inspection/OverviewQuery.cs src/AdminPanel.Api/Program.cs src/tests/AdminPanel.UnitTests/S3QueryHandlerTests.cs
git commit -m "t34: GET /api/s3/clusters[/{c}] + s3-алерты в /api/alerts + s3-сводка в /api/overview"
```

---

### Task 10: Api — 10 мутаций-прокси + интеграционные тесты

**Files:**
- Create: `src/AdminPanel.Api/Operations/S3/S3Requests.cs`
- Create: `src/AdminPanel.Api/Operations/S3/S3Commands.cs`
- Create: `src/AdminPanel.Api/Operations/S3/S3OperationsModule.cs`
- Modify: `src/AdminPanel.Api/Operations/WorkerProblemDetails.cs` (+`WorkerRaw<T>` и `SendWithStatusAsync`)
- Modify: `src/AdminPanel.Api/Program.cs` (`MapS3OperationsApi()`)
- Test: `src/tests/AdminPanel.IntegrationTests/S3ApiTests.cs`

**Interfaces:**
- Consumes: `IWorkerApiGateway.SendAsync("s3worker", …)` (Task 8), `WorkerProxy.SendAsync<T>` (существует, `Operations/WorkerProblemDetails.cs`), DTO воркера `src/S3Worker.App/Api/Operations/S3Api.cs` (зеркало тел).
- Produces: REST `/api/s3/*` — 12 эндпоинтов по arch/03 §12.1 (коды 1:1 с ApiModule S3Worker).

- [ ] **Шаг 1: S3Requests.cs — тела панельных запросов**

```csharp
namespace AdminPanel.Api.Operations.S3;

// Тела запросов — зеркало DTO S3Worker (arch/24 §1.1; панель НЕ валидирует —
// сервер источник истины, фронт дублирует для UX). platform="owns3" панель
// подставляет всегда (единственное значение канона arch/23 §2; UI поле не показывает).
public sealed record CreateS3ClusterPanelRequest(
    string? Name,
    string? SecretProfile,
    IReadOnlyList<S3InstancePanelRequest>? Instances,
    S3ResourcesPanelRequest? Resources,
    string? Platform = "owns3");

public sealed record S3InstancePanelRequest(
    string? Name,
    S3PlacementPanelRequest? Placement,
    S3ResourcesPanelRequest? Resources);

public sealed record S3PlacementPanelRequest(
    string? DockerHost,
    string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);

public sealed record S3ResourcesPanelRequest(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record AddS3NodePanelRequest(
    string? Name,
    S3PlacementPanelRequest? Placement,
    S3ResourcesPanelRequest? Resources);

// PUT resources: null-поля = не менять (сервер RMW).
public sealed record UpdateS3ResourcesPanelRequest(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record PutS3LinksPanelRequest(IReadOnlyList<S3LinkPanelDto>? Links);

public sealed record S3LinkPanelDto(string? A, string? B);
```

- [ ] **Шаг 2: S3Commands.cs — 10 команд-прокси (порт ValkeyCommands)**

```csharp
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.S3;

// ===== S3-мутации — прокси в API S3Worker (arch/02 §12.2, arch/24 §1.1):
// панель не пишет в etcd; воркер валидирует и пишет. Оператор (requestedBy) —
// ротация и зонд (заявки с аудитом) — заголовком X-Requested-By. =====

// 1. Создание кластера (§12.2-1) — 201.
public sealed record CreateS3ClusterCommand(CreateS3ClusterPanelRequest Request)
    : ICommand<S3ClusterCreatedPanelDto>;

// Ответ 201 POST /api/s3/clusters (зеркало S3ClusterCreatedDto воркера).
public sealed record S3ClusterCreatedPanelDto(
    string Name, string State, int Nodes, string SecretProfile, IReadOnlyList<string> Instances);

[InjectAsScoped]
public sealed class CreateS3ClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateS3ClusterCommand, S3ClusterCreatedPanelDto>
{
    public async ValueTask<Result<S3ClusterCreatedPanelDto>> Handle(
        CreateS3ClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3ClusterCreatedPanelDto>(
            api, "s3worker", HttpMethod.Post, "/api/s3/clusters",
            command.Request, requestedBy: null, ct);
}

// 2. Удаление — TO_REMOVE, демонтаж асинхронный (§12.2-2): 202 | 204 (already) —
// код зеркалится: SendWithStatusAsync (шаг 3), модуль маппит по StatusCode.
public sealed record DeleteS3ClusterCommand(string Cluster)
    : ICommand<WorkerRaw<S3ClusterDeletedPanelDto>>;

public sealed record S3ClusterDeletedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteS3ClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteS3ClusterCommand, WorkerRaw<S3ClusterDeletedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3ClusterDeletedPanelDto>>> Handle(
        DeleteS3ClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3ClusterDeletedPanelDto>(
            api, "s3worker", HttpMethod.Delete, $"/api/s3/clusters/{command.Cluster}",
            body: null, requestedBy: null, ct);
}

// 3. Добавление инстанса (§12.2-3) — 201 {cluster, node}.
public sealed record AddS3NodeCommand(string Cluster, AddS3NodePanelRequest Request)
    : ICommand<S3NodeAddedPanelDto>;

public sealed record S3NodeAddedPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class AddS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AddS3NodeCommand, S3NodeAddedPanelDto>
{
    public async ValueTask<Result<S3NodeAddedPanelDto>> Handle(
        AddS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3NodeAddedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/nodes",
            command.Request, requestedBy: null, ct);
}

// 4. Маркер демонтажа инстанса (§12.2-4, one-way): 202 | 204 (already).
public sealed record RemoveS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeRemovedPanelDto>>;

public sealed record S3NodeRemovedPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class RemoveS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RemoveS3NodeCommand, WorkerRaw<S3NodeRemovedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeRemovedPanelDto>>> Handle(
        RemoveS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeRemovedPanelDto>(
            api, "s3worker", HttpMethod.Delete,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}",
            body: null, requestedBy: null, ct);
}

// 5. Ресурсы инстанса (§12.2-5) — 200 {cpu, mem, disk} (строки-каноны воркера).
public sealed record UpdateS3ResourcesCommand(
    string Cluster, string Node, UpdateS3ResourcesPanelRequest Request)
    : ICommand<S3ResourcesUpdatedPanelDto>;

public sealed record S3ResourcesUpdatedPanelDto(string Cpu, string Mem, string Disk);

[InjectAsScoped]
public sealed class UpdateS3ResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateS3ResourcesCommand, S3ResourcesUpdatedPanelDto>
{
    public async ValueTask<Result<S3ResourcesUpdatedPanelDto>> Handle(
        UpdateS3ResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<S3ResourcesUpdatedPanelDto>(
            api, "s3worker", HttpMethod.Put,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/resources",
            command.Request, requestedBy: null, ct);
}

// 6. Граф репликации — замена целиком (§12.2-6) — 202 (применит доводка G);
// код зеркалится через SendWithStatusAsync.
public sealed record PutS3LinksCommand(string Cluster, PutS3LinksPanelRequest Request)
    : ICommand<WorkerRaw<S3LinksPutPanelDto>>;

public sealed record S3LinksPutPanelDto(string Cluster);

[InjectAsScoped]
public sealed class PutS3LinksCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<PutS3LinksCommand, WorkerRaw<S3LinksPutPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3LinksPutPanelDto>>> Handle(
        PutS3LinksCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3LinksPutPanelDto>(
            api, "s3worker", HttpMethod.Put, $"/api/s3/clusters/{command.Cluster}/links",
            command.Request, requestedBy: null, ct);
}

// 7. Отзыв ключа инстанса deny (§12.2-7): 202 | 204 (already).
public sealed record DenyS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeDenyPanelDto>>;

public sealed record S3NodeDenyPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class DenyS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DenyS3NodeCommand, WorkerRaw<S3NodeDenyPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeDenyPanelDto>>> Handle(
        DenyS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeDenyPanelDto>(
            api, "s3worker", HttpMethod.Post,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/deny",
            body: null, requestedBy: null, ct);
}

// 8. Снятие отзыва allow (§12.2-8): 202 | 204 (already).
public sealed record AllowS3NodeCommand(string Cluster, string Node)
    : ICommand<WorkerRaw<S3NodeAllowPanelDto>>;

public sealed record S3NodeAllowPanelDto(string Cluster, string Node);

[InjectAsScoped]
public sealed class AllowS3NodeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AllowS3NodeCommand, WorkerRaw<S3NodeAllowPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3NodeAllowPanelDto>>> Handle(
        AllowS3NodeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3NodeAllowPanelDto>(
            api, "s3worker", HttpMethod.Post,
            $"/api/s3/clusters/{command.Cluster}/nodes/{command.Node}/allow",
            body: null, requestedBy: null, ct);
}

// 9. Заявка ротации секретов профиля (§12.2-9): 201 (создана) | 200 (уже стоит —
// идемпотентность воркера, 409 НЕ отдаётся); аудит requestedBy.
public sealed record RotateS3SecretsCommand(string Cluster, string RequestedBy)
    : ICommand<WorkerRaw<S3RotationRequestedPanelDto>>;

public sealed record S3RotationRequestedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class RotateS3SecretsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateS3SecretsCommand, WorkerRaw<S3RotationRequestedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3RotationRequestedPanelDto>>> Handle(
        RotateS3SecretsCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3RotationRequestedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/rotations",
            body: null, command.RequestedBy, ct);
}

// 10. Внеплановый зонд (§12.2-10): 202; аудит.
public sealed record S3SyncProbeCommand(string Cluster, string RequestedBy)
    : ICommand<WorkerRaw<S3ProbeAcceptedPanelDto>>;

public sealed record S3ProbeAcceptedPanelDto(string Cluster);

[InjectAsScoped]
public sealed class S3SyncProbeCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<S3SyncProbeCommand, WorkerRaw<S3ProbeAcceptedPanelDto>>
{
    public async ValueTask<Result<WorkerRaw<S3ProbeAcceptedPanelDto>>> Handle(
        S3SyncProbeCommand command, CancellationToken ct)
        => await WorkerProxy.SendWithStatusAsync<S3ProbeAcceptedPanelDto>(
            api, "s3worker", HttpMethod.Post, $"/api/s3/clusters/{command.Cluster}/sync-probe",
            body: null, command.RequestedBy, ct);
}
```

- [ ] **Шаг 3: WorkerProblemDetails.cs — `WorkerRaw<T>` + перегрузка с исходным кодом**

В `Operations/WorkerProblemDetails.cs` (internal static class `WorkerProxy`) добавить:

```csharp
// Результат прокси с исходным кодом воркера (для зеркалирования 202|204/201|200).
// Объявляется на уровне namespace AdminPanel.Api.Operations (НЕ внутри WorkerProxy —
// команды Operations.S3 ссылаются на него без квалификации).
public sealed record WorkerRaw<T>(int StatusCode, T? Value);

// Прокси с сохранением исходного статуса: 2xx → (код, DTO|null-тела); DELETE- и
// deny/allow-мутации различают 202 (записано) и 204 (already), ротация — 201/200
// — модуль зеркалит код воркера 1:1 (arch/02 §12.2).
public static async ValueTask<Result<WorkerRaw<T>>> SendWithStatusAsync<T>(
    IWorkerApiGateway api, string worker, HttpMethod method, string path,
    object? body, string? requestedBy, CancellationToken ct) where T : class
{
    // Тело — копия SendAsync<T>, но Success несёт new WorkerRaw<T>(resp.StatusCode, dto).
}
```

- [ ] **Шаг 4: S3OperationsModule.cs — 12 эндпоинтов (порт ValkeyOperationsModule)**

`MapS3OperationsApi(this IEndpointRouteBuilder endpoints)` — успех зеркалит КОД воркера (мутации 2/4/6/7/8/9/10 идут через `SendWithStatusAsync` → `WorkerRaw<T>.StatusCode`; 1/3/5 — `SendAsync` с фиксированным кодом):

| Эндпоинт панели | Успех (статус = статусу воркера) |
|---|---|
| `POST /api/s3/clusters` (тело `CreateS3ClusterPanelRequest`) | 201 `Results.Created($"/api/s3/clusters/{value.Name}", value)` |
| `DELETE /api/s3/clusters/{cluster}` | 202 `Results.Accepted()` при 202; 204 `Results.NoContent()` при 204 (already) |
| `POST /api/s3/clusters/{cluster}/nodes` | 201 `Results.Created(..., value)` |
| `DELETE /api/s3/clusters/{cluster}/nodes/{node}` | 202 `Results.Accepted()` / 204 `Results.NoContent()` |
| `PUT /api/s3/clusters/{cluster}/nodes/{node}/resources` | 200 `Results.Ok(value)` |
| `PUT /api/s3/clusters/{cluster}/links` | 202 `Results.Accepted()` |
| `POST /api/s3/clusters/{cluster}/nodes/{node}/deny` | 202 `Results.Accepted()` / 204 `Results.NoContent()` |
| `POST /api/s3/clusters/{cluster}/nodes/{node}/allow` | 202 `Results.Accepted()` / 204 `Results.NoContent()` |
| `POST /api/s3/clusters/{cluster}/rotations` (ClaimsPrincipal user) | 201 `Results.Created((string?)null, value)` / 200 `Results.Ok(value)` |
| `POST /api/s3/clusters/{cluster}/sync-probe` (ClaimsPrincipal user) | 202 `Results.Accepted()` |

`requestedBy` мутаций 9–10 — `user.Identity?.Name ?? "adminpanel"` (порт valkey-rotate).

Приватная `Error(Result)` — копия из `ValkeyOperationsModule` (WorkerApiUnavailableException → 503 «API воркера недоступен»; WorkerProblemDetails → тело как есть; прочее → 503).

`Program.cs`: после `app.MapValkeyOperationsApi();` → `app.MapS3OperationsApi(); // t34: s3-мутации (arch/02 §12.2)`.

- [ ] **Шаг 5: интеграционные тесты**

`src/tests/AdminPanel.IntegrationTests/S3ApiTests.cs` — точный порт `ValkeyApiTests.cs` (фабрика `S3ApiFactory : WebApplicationFactory<Program>` + `S3ApiFixture : IAsyncLifetime`: свой etcd-контейнер `EtcdContainerFixture` (динамический порт), сид guid-префикса `/s3/clusters/<guid>demo/…` — config Active, nodes/east1 state RUNNING + placement + resources, links, endpoints, connectivity OK, sync SYNCED, denylist, `/s3worker/api/<guid>i1` живой ключ; hosted сняты; `RefreshOnceAsync` вручную; teardown — prefix-delete своих ключей + ассерт чистоты + контейнер). Кейсы (все AAA):

```csharp
// Arrange: живой ключ /s3worker/api/ + стаб воркера 201. Act: POST /api/s3/clusters.
// Assert: 201; стаб получил POST /api/s3/clusters с телом как есть (json-сравнение,
// вкл. platform:"owns3").
[Fact] public async Task Create_ProxiesToWorker_201()

// Arrange: стаб отвечает 404 ProblemDetails / 400 / 409 / 422 (подCase-блоки
// внутри одного теста, каждый — AAA-комментарием). Act: DELETE-узел /
// POST-кластер / POST-ротация. Assert: код и тело 1:1
// (ProblemDetails application/problem+json как есть).
[Fact] public async Task WorkerErrors_MappedOneToOne_404_400_409_422()

// Arrange: сид БЕЗ ключа /s3worker/api/. Act: любая мутация.
// Assert: 503 "API воркера недоступен" (панельный ProblemDetails).
[Fact] public async Task NoLiveWorkerKeys_Mutations_503()

// Arrange: сид + refresher-тик. Act: GET /api/s3/clusters и /{c}.
// Assert: сводка/детали со всеми секциями (connectivity/sync/denylist/denied).
[Fact] public async Task Inspection_ReadsS3Snapshot()

// Arrange: стаб 202/204 (delete-кластер: первый вызов 202, повтор 204) и
// 201/200 (ротация: первый 201, повтор 200). Act: две мутации × 2.
// Assert: панель зеркалит код оба раза (202→202, 204→204; 201→201, 200→200).
[Fact] public async Task IdempotentRepeats_MirrorWorkerCodes_202_204_201_200()

// Arrange: стаб 202. Act: PUT links / sync-probe / deny / allow / resources.
// Assert: 202; X-Requested-By передан стабу у ротации/зонда, отсутствует у прочих.
[Fact] public async Task AsyncMutations_202_RequestedByAuditing()
```

Стаб `TestWorkerApi` — существующий в `AdminPanel.IntegrationTests` (переиспользовать: он фиксирует путь/метод/тело/заголовки).

- [ ] **Шаг 6: прогнать интеграции — зелёные; зачистка серии**

Run (в фоне): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~S3ApiTests"`
Ожидание: PASS. После финальной строки — зачистка серии: `docker ps -a --format '{{.Names}}'` (testcontainers-etcd подобран ryuk; остатков нет) и `docker network ls | grep -c kfw-net` (0 при нуле контейнеров, иначе `docker network prune -f`).

- [ ] **Шаг 7: коммит**

```bash
git add src/AdminPanel.Api/Operations/S3/ src/AdminPanel.Api/Operations/WorkerProblemDetails.cs src/AdminPanel.Api/Program.cs src/tests/AdminPanel.IntegrationTests/S3ApiTests.cs
git commit -m "t34: 10 s3-мутаций-прокси /api/s3/* (коды 1:1, X-Requested-By на ротации/зонде) + интеграции"
```

---

### Task 11: фронтенд — s3-типы и запросы

**Files:**
- Modify: `frontend/src/api/dto.ts` (+s3-типы; +`OverviewS3Dto` и поле `s3` в `OverviewDto`)
- Modify: `frontend/src/api/queries.ts` (+s3-запросы)

**Interfaces:**
- Consumes: REST Task 9–10 (DTO из arch/03 §12.2; бэковый `OverviewS3Dto` — Task 9), `apiFetch`-хелпер, react-query invalidation-паттерн valkey.
- Produces: типы/функции для страниц (Tasks 12–14): `fetchS3Clusters`, `fetchS3ClusterDetails`, `createS3Cluster`, `deleteS3Cluster`, `addS3Node`, `removeS3Node`, `updateS3NodeResources`, `putS3Links`, `denyS3Node`, `allowS3Node`, `rotateS3Secrets`, `s3SyncProbe`, `s3QueryKeys`; расширенный `OverviewDto.s3` для карточки Task 14.

- [ ] **Шаг 1: dto.ts — типы (camelCase-зеркало REST)**

Две правки в `frontend/src/api/dto.ts`:

(а) S3-доменные типы — в конец файла (по образцу valkey-блока):

```typescript
// ===== S3-домен (arch/03 §12.2) =====
export type S3ClusterSummaryDto = {
  name: string;
  state: 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';
  nodesTotal: number;
  nodesRunning: number;
  endpoints: string | null;
  connectivityState: 'OK' | 'DEGRADED' | 'DOWN' | null;
  syncState: 'SYNCED' | 'LAGGING' | 'DIVERGED' | null;
  rotationPending: boolean;
};
export type S3PlacementDto = {
  dockerHost?: string | null;
  swarmNode?: string | null;
  swarmLabels?: Record<string, string> | null;
};
export type S3ResourcesDto = { cpu: number | null; memGi: number | null; diskGi: number | null };
export type S3UsageDto = { usedBytes: number; totalBytes: number; updatedUnix: number };
export type S3NodeDto = {
  name: string;
  state: string | null;
  placedOn: string | null;
  placement: S3PlacementDto | null;
  resources: S3ResourcesDto | null;
  usage: S3UsageDto | null;
  denied: boolean;
};
export type S3LinkDto = { a: string; b: string };
export type S3ConnectivityDto = {
  state: string | null;
  checkedUnix: number | null;
  links: Record<string, Record<string, string>>;
  probeError: string | null;
};
export type S3SyncPairDto = {
  from: string; to: string; missing: number; extra: number;
  lagBytes: number | null; lagObjects: number | null;
};
export type S3SyncDto = {
  state: string | null;
  checkedUnix: number | null;
  pairs: S3SyncPairDto[];
  lastError: string | null;
};
export type S3RotationDto = { requestedUnix: number; requestedBy: string | null };
export type S3ClusterDto = {
  name: string;
  state: 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';
  platform: string;
  nodesDeclared: number;
  secretProfile: string;
  createdUnix: number | null;
  endpoints: string | null;
  nodesList: S3NodeDto[];
  links: S3LinkDto[];
  denylist: string[];
  connectivity: S3ConnectivityDto | null;
  sync: S3SyncDto | null;
  rotation: S3RotationDto | null;
};
export type CreateS3ClusterRequestDto = {
  name: string;
  secretProfile: string;
  instances: { name: string; placement: S3PlacementDto; resources?: S3ResourcesDto | null }[];
  resources?: S3ResourcesDto | null;
};
export type AddS3NodeRequestDto = {
  name: string; placement: S3PlacementDto; resources?: S3ResourcesDto | null;
};
export type UpdateS3ResourcesRequestDto = {
  cpu?: number | null; memGi?: number | null; diskGi?: number | null;
};
export type PutS3LinksRequestDto = { links: { a: string; b: string }[] };
```

(б) расширить `OverviewDto` (интерфейс `GET /api/overview`, dto.ts ~строки 140–149) — после поля `valkey` добавить поле `s3` и рядом с `OverviewKafkaDto`/`OverviewValkeyDto` (~строки 154–160) — новый интерфейс (зеркало бэкового `OverviewS3Dto` из Task 9):

```typescript
// Внутри interface OverviewDto (после поля valkey):
  // Сводка s3-домена (arch/03 §12.1); null до первого тика s3-refresher'а.
  s3: OverviewS3Dto | null;

// Рядом с OverviewKafkaDto/OverviewValkeyDto (по их образцу):
export interface OverviewS3Dto {
  clustersTotal: number;
  clustersCritical: number;
}
```

Без (б) карточка Task 14 не скомпилируется (`data.s3` — TS2339).

- [ ] **Шаг 2: queries.ts — запросы (порт valkey-блока)**

```typescript
// ===== S3-домен (arch/03 §12.1) =====
export const s3QueryKeys = {
  clusters: ['s3-clusters'] as const,
  cluster: (name: string) => ['s3-clusters', name] as const,
};

export function fetchS3Clusters(): Promise<S3ClusterSummaryDto[]> {
  return apiFetch<S3ClusterSummaryDto[]>('/api/s3/clusters');
}
export function fetchS3ClusterDetails(name: string): Promise<S3ClusterDto> {
  return apiFetch<S3ClusterDto>(`/api/s3/clusters/${encodeURIComponent(name)}`);
}
// POST /api/s3/clusters — создание (arch/02 §12.2-1): заявку доигрывает S3Worker.
export function createS3Cluster(request: CreateS3ClusterRequestDto): Promise<unknown> {
  return apiFetch('/api/s3/clusters', { method: 'POST', body: request });
}
// DELETE /api/s3/clusters/{c} — TO_REMOVE (§12.2-2): демонтаж асинхронный.
export function deleteS3Cluster(cluster: string): Promise<void> {
  return apiFetch<void>(`/api/s3/clusters/${encodeURIComponent(cluster)}`, { method: 'DELETE' });
}
export function addS3Node(cluster: string, request: AddS3NodeRequestDto): Promise<unknown> {
  return apiFetch(`/api/s3/clusters/${encodeURIComponent(cluster)}/nodes`, { method: 'POST', body: request });
}
export function removeS3Node(cluster: string, node: string): Promise<void> {
  return apiFetch<void>(
    `/api/s3/clusters/${encodeURIComponent(cluster)}/nodes/${encodeURIComponent(node)}`,
    { method: 'DELETE' });
}
export function updateS3NodeResources(
  cluster: string, node: string, request: UpdateS3ResourcesRequestDto,
): Promise<unknown> {
  return apiFetch(
    `/api/s3/clusters/${encodeURIComponent(cluster)}/nodes/${encodeURIComponent(node)}/resources`,
    { method: 'PUT', body: request });
}
export function putS3Links(cluster: string, request: PutS3LinksRequestDto): Promise<void> {
  return apiFetch<void>(
    `/api/s3/clusters/${encodeURIComponent(cluster)}/links`, { method: 'PUT', body: request });
}
export function denyS3Node(cluster: string, node: string): Promise<void> {
  return apiFetch<void>(
    `/api/s3/clusters/${encodeURIComponent(cluster)}/nodes/${encodeURIComponent(node)}/deny`,
    { method: 'POST' });
}
export function allowS3Node(cluster: string, node: string): Promise<void> {
  return apiFetch<void>(
    `/api/s3/clusters/${encodeURIComponent(cluster)}/nodes/${encodeURIComponent(node)}/allow`,
    { method: 'POST' });
}
export function rotateS3Secrets(cluster: string): Promise<unknown> {
  return apiFetch(`/api/s3/clusters/${encodeURIComponent(cluster)}/rotations`, { method: 'POST' });
}
export function s3SyncProbe(cluster: string): Promise<void> {
  return apiFetch<void>(`/api/s3/clusters/${encodeURIComponent(cluster)}/sync-probe`, { method: 'POST' });
}
```

(импорты типов добавить в шапку файла; `apiFetch` — существующий хелпер `api/client.ts`.)

- [ ] **Шаг 3: проверка сборки**

Run (в фоне): `cd frontend && npm run build`
Ожидание: сборка без ошибок типов (tsc + vite) — `OverviewDto.s3` и S3-типы резолвятся.

- [ ] **Шаг 4: коммит**

```bash
git add frontend/src/api/dto.ts frontend/src/api/queries.ts
git commit -m "t34 front: s3-типы DTO (+OverviewS3Dto/OverviewDto.s3) и api-запросы (12 эндпоинтов)"
```

---

### Task 12: фронтенд — S3ClustersPage + CreateS3ClusterModal

**Files:**
- Create: `frontend/src/pages/S3ClustersPage.tsx`
- Create: `frontend/src/pages/s3-cluster/CreateS3ClusterModal.tsx`

**Interfaces:**
- Consumes: Task 11 (запросы/типы), компоненты `LoadState`, badge-паттерны `ValkeyClustersPage.tsx`.
- Produces: страница списка `/s3` (маршрут в Task 14) и модал создания.

- [ ] **Шаг 1: S3ClustersPage.tsx**

Порт `ValkeyClustersPage.tsx` (открыть как образец структуры: useQuery + Mantine Table + LoadState). Таблица списка (arch/03 §12.3): колонки — имя (ссылка `/s3/{name}`), state-бейдж (ACTIVE зелёный/NOT_INITIALIZED серый/TO_REMOVE красный), `nodesRunning/nodesTotal`, endpoints (сокращённо, `truncate`), бейдж связности (`connectivityState`: OK/DEGRADED/DOWN/—), бейдж синхронности (`syncState`: SYNCED/LAGGING/DIVERGED/—), бейдж ротации (`rotationPending` → «ротация…»). Кнопка «Создать кластер» открывает `CreateS3ClusterModal`. Пустой список — «S3-кластеров нет».

- [ ] **Шаг 2: CreateS3ClusterModal.tsx**

Порт `CreateValkeyClusterModal.tsx` + динамический список инстансов (паттерн строк kafka-модалов). Поля:

- «Имя кластера» (TextInput; клиентская валидация `^[a-z][a-z0-9_]{0,62}$`).
- «Профиль секретов» (TextInput; подпись: «имя per-install env-профиля S3Worker; 422 воркера покажет перечень доступных»).
- «Платформа» — read-only текст «owns3».
- Инстансы (динамический список, `0..9`): для каждого — имя (валидация паттерна + не `orchestrator`), placement (радио `docker_host` текст / `swarm_node` текст / `swarm_labels` key-value-редактор с кнопкой «+ метка»; минимум один атрибут), ресурсы cpu/mem/disk (NumberInput; cpu 0.01..64 шаг 0.1, mem/disk 1..65536 GiB; пусто = общий fallback). Подпись: «допустимо 0 инстансов — декларация без развёртывания».
- Общие ресурсы (fallback для инстансов без своих): cpu/mem/disk.
- Отправка: `createS3Cluster` → инвалидация `s3QueryKeys.clusters` → закрыть; ошибка — ProblemDetails в теле модала (409 «имя занято», 422 — перечень профилей текстом, 400 — по полям, 503 «API воркера недоступен»); двойной клик — блокировка кнопки (паттерн valkey).

- [ ] **Шаг 3: проверка сборки**

Run (в фоне): `cd frontend && npm run build` — без ошибок.

- [ ] **Шаг 4: коммит**

```bash
git add frontend/src/pages/S3ClustersPage.tsx frontend/src/pages/s3-cluster/CreateS3ClusterModal.tsx
git commit -m "t34 front: S3ClustersPage + CreateS3ClusterModal (instances/placement/валидация-зеркало §12.3)"
```

---

### Task 13: фронтенд — детали кластера (вкладки + модалы)

**Files:**
- Create: `frontend/src/pages/s3-cluster/S3ClusterDetailsPage.tsx`
- Create: `frontend/src/pages/s3-cluster/NodesTab.tsx`
- Create: `frontend/src/pages/s3-cluster/ReplicationTab.tsx`
- Create: `frontend/src/pages/s3-cluster/ConnectivitySyncTab.tsx`
- Create: `frontend/src/pages/s3-cluster/AddNodeModal.tsx`
- Create: `frontend/src/pages/s3-cluster/EditNodeResourcesModal.tsx`
- Create: `frontend/src/pages/s3-cluster/LinksEditorModal.tsx`
- Create: `frontend/src/pages/s3-cluster/RemoveNodeButton.tsx`
- Create: `frontend/src/pages/s3-cluster/DenyNodeButton.tsx`
- Create: `frontend/src/pages/s3-cluster/AllowNodeButton.tsx`
- Create: `frontend/src/pages/s3-cluster/RotateSecretsButton.tsx`
- Create: `frontend/src/pages/s3-cluster/SyncProbeButton.tsx`
- Create: `frontend/src/pages/s3-cluster/DeleteS3ClusterButton.tsx`

**Interfaces:**
- Consumes: Task 11 запросы, Task 12 badge-паттерны; образец `ValkeyClusterDetailsPage.tsx`/`KafkaClusterDetailsPage.tsx` (шапка/вкладки/confirm-модалы).
- Produces: страница `/s3/:cluster` (маршрут в Task 14).

- [ ] **Шаг 1: S3ClusterDetailsPage.tsx — шапка + вкладки**

Шапка (порт valkey-деталей): имя, state-бейджи (TO_REMOVE/NOT_INITIALIZED), `endpoints` (моно-шрифтом), `secretProfile` (только имя), кнопки:
- «Проверить сейчас» → `SyncProbeButton` (202 → инвалидация деталей; 409 «кластер не Active» — текстом ProblemDetails).
- «Ротация секретов» → `RotateSecretsButton` (модал-предупреждение: «rolling-пересоздание инстансов с новым env профиля; PKI переиспользуется (ключи переживают пересоздание)»; успех — 201 или 200 (идемпотентный повтор при живой заявке — коды воркера §12.2-9), бейдж ротации придёт следующим тиком; 404/503 — ProblemDetails текстом).
- «Удалить кластер» (красная) → `DeleteS3ClusterButton` (confirm: «Снос кластера уничтожает данные бэкапов на дисках всех его инстансов — осознанное действие; демонтаж асинхронный (воркер)»; при state=TO_REMOVE скрыта).
Вкладки Mantine Tabs: «Ноды» (`NodesTab`), «Репликация» (`ReplicationTab`), «Связность и синхронность» (`ConnectivitySyncTab`). `canMutate = state === 'ACTIVE'` (кнопки мутаций скрыты у не-Active, как у valkey).

- [ ] **Шаг 2: NodesTab.tsx**

Кнопка «Добавить ноду» в заголовке вкладки → `AddNodeModal` (поля: имя, placement — как в CreateS3ClusterModal, ресурсы; валидация-зеркало; 409 «инстанс уже существует» — текстом). Таблица: name / state (бейдж) / placed_on / placement (человекочитаемый селектор: `docker_host=h1` / `swarm_node=n1` / `swarm_labels: geo=dc1,tier=fast`) / resources (cpu/mem/disk) / usage («used / total» байтами из `utils/format.ts` + процент от `total`; при наличии `resources.diskGi` — доп. процент `used / diskGi·GiB`; пороговая подсветка > 85%) / бейдж «отозван» (`denied`) + кнопки Deny (`DenyNodeButton`, confirm: «связи с инстансом сносятся, будущие join запрещены») / Allow (`AllowNodeButton`, доступна только denied) / колонка действий: «Изменить ресурсы» (`EditNodeResourcesModal` — cpu/mem/disk, null = не менять), красная «Убрать ноду» (`RemoveNodeButton`, confirm: «volume инстанса сносится; копия данных — на остальных инстансах при настроенных связях; без связей объекты теряются»; серверный 409 «последняя нода» — текстом ProblemDetails).

- [ ] **Шаг 3: ReplicationTab.tsx — таблица пар + матрица смежности**

Просмотр (данные `cluster.links` + `denylist` + `placed_on`):
- Таблица пар: строки a → b; deny-имена — бейдж «отозван».
- Матрица смежности нода×нода (таблица: заголовки строк/столбцов — имена нод; клетка отмечена (●/фон), если пара есть в links; deny-нода — строка/столбец приглушены с пометкой). Анти-аффинити-индикация: cells/подпись — если у двух нод одной связи одинаковый `placed_on`, подсветить и подписать «один хост: h1».
- Подсказка: «полный меш не обязателен; ноды одного кластера — на разных хостах/площадках (гео-раснос — обязанность оператора)». Пустые links — «линков не настроено».
Редактор: кнопка «Изменить граф» → `LinksEditorModal` (модал «таблица пар»: строки с двумя Select (имена нод кластера), «+ строка»/«−»; клиентская валидация: имена существуют, `a≠b`, пары уникальны) → `putS3Links` (202 — подпись «применит доводка воркера»; инвалидация деталей).

- [ ] **Шаг 4: ConnectivitySyncTab.tsx**

- Connectivity: state-бейдж (OK/DEGRADED/DOWN), `checked_unix`-штамп (возраст), `probe_error` (текстом); матрица `links` — строки «нода → сосед: ok/offline» (значения клеток бейджем ok/offline). Пустая матрица — «линков не настроено».
- Sync: state-бейдж (SYNCED/LAGGING/DIVERGED), `checked_unix`, `last_error`; таблица пар: from / to / missing / extra / lag_bytes / lag_objects (форматирование байтов).
- Обе секции с подписью «источник — зонд S3Worker (ключи /s3/clusters/<C>/{connectivity,sync})».

- [ ] **Шаг 5: проверка сборки**

Run (в фоне): `cd frontend && npm run build` — без ошибок типов/импортов.

- [ ] **Шаг 6: коммит**

```bash
git add frontend/src/pages/s3-cluster/
git commit -m "t34 front: детали S3-кластера — вкладки Ноды/Репликация (пары+матрица)/Связность и синхронность, модалы и кнопки с confirm-предупреждениями"
```

---

### Task 14: фронтенд — маршруты, навигация, Overview, карточка S3Worker

**Files:**
- Modify: `frontend/src/App.tsx` (маршруты `/s3`, `/s3/:cluster`)
- Modify: `frontend/src/layout/AppLayout.tsx` (пункт «S3»)
- Modify: `frontend/src/pages/OverviewPage.tsx` (карточка S3)
- Modify: `frontend/src/pages/WorkersPage.tsx` (WORKER_TITLES + s3worker)

**Interfaces:**
- Consumes: страницы Tasks 12–13; `OverviewDto.s3`/`OverviewS3Dto` (добавлены в Task 11 Шаг 1(б)), `WorkersViewDto` (Task 8).
- Produces: готовая навигация SPA (критерии приёмки 5/6/7).

- [ ] **Шаг 1: App.tsx** — импорты `S3ClustersPage`, `S3ClusterDetailsPage`; в children AppLayout после valkey-маршрутов: `{ path: 's3', element: <S3ClustersPage /> }, { path: 's3/:cluster', element: <S3ClusterDetailsPage /> }`.
- [ ] **Шаг 2: AppLayout.tsx** — в `NAV_ITEMS` добавить пункт `{ to: '/s3', label: 'S3' }` сразу после Valkey (пункты навигации БЕЗ иконок — по образцу соседних `{ to: '/kafka', label: 'Kafka' }`; никаких иконок не вводить).
- [ ] **Шаг 3: OverviewPage.tsx** — карточка «S3»: точный порт `KafkaCard` (`frontend/src/pages/OverviewPage.tsx`, функция `KafkaCard({ data }: { data: OverviewDto })`): `data.s3 === null →` заглушка «S3-снапшот ещё не собран» (как «Kafka-снапшот ещё не собран»); иначе `clustersTotal` «кластеров» + `clustersCritical` «критических» (красный при >0) + `Anchor` на `/s3` «все кластеры →». Карточку вставить после `KafkaCard` в разметке. Тип `data.s3` уже расширен в `OverviewDto` (Task 11 Шаг 1(б)) — самовольное расширение типов здесь НЕ нужно.
- [ ] **Шаг 4: WorkersPage.tsx** — в `WORKER_TITLES` добавить `s3worker: 'S3Worker (хранилище S3)'`; карточка рендерится автоматически из `WorkersViewDto` (данные Task 8); кнопки generate/upload/delete/restart — общие, уже работают.
- [ ] **Шаг 5: сборка фронтенда целиком**

Run (в фоне): `cd frontend && npm run build`
Ожидание: успех (tsc проходит по `data.s3` — типы из Task 11); артефакт `frontend/dist` собран (для docker-образа панели).

- [ ] **Шаг 6: коммит**

```bash
git add frontend/src/App.tsx frontend/src/layout/AppLayout.tsx frontend/src/pages/OverviewPage.tsx frontend/src/pages/WorkersPage.tsx
git commit -m "t34 front: маршруты /s3, пункт навигации, карточка S3 в Overview (порт KafkaCard), S3Worker в Воркерах"
```

---

### Task 15: e2e-чек стенда 80-s3-panel.sh + прогон серий

**Files:**
- Create: `dev-stand/adminpanel/checks/80-s3-panel.sh`
- Modify: `dev-stand/adminpanel/README.md` (если ведётся перечень чеков — добавить строку; перечня нет — не трогать)

**Interfaces:**
- Consumes: полный стек: стенд (`00-up.sh`: панель :5050, s3worker, etcd), REST Tasks 9–10, refresher Task 7.
- Produces: мерж-гейт-чей задачи (критерий приёмки 9).

- [ ] **Шаг 1: чек-скрипт (порт 51-valkey-api.sh + 75-s3worker.sh)**

`dev-stand/adminpanel/checks/80-s3-panel.sh` — каркас как у `51-valkey-api.sh` (`set -euo pipefail`, `cd "$(dirname "$0")/.."`, `BASE="${ADMINPANEL_URL:-http://localhost:5050}"`, `TAG="s80$(date +%s)"`, `JAR`/`TMP` + `trap`-чистка, etcd-хелперы `etcd_key`/`ect`). Own-only зачистка в `cleanup()` (порт 75): `docker rm -f` контейнеров `s3w-$TAG*`, `docker volume rm` по фильтру `s3w-$TAG`, `ect del "/s3/$TAG/" --prefix` (флаг сбоя — не падаем). Сценарий:

1. Arrange: ожидание `GET $BASE/api/healthz` (30 × 1 c); cookie-логин `POST /api/auth/login` `{"username":"admin","password":"admin"}`; хелперы `api()`/`code()`.
2. Создание через панель: `POST /api/s3/clusters` c телом `{"name":"$TAG","secretProfile":"BACKUPS-MAIN","instances":[{"name":"node1","placement":{"dockerHost":"local"},"resources":{"cpu":0.5,"memGi":1,"diskGi":5}}]}` → 201.
3. Ожидание Active (≤ 150 × 2 s = 300 c — бюджет 51-чека с запасом на portalloc/загрузку образа; 75-чек ждёт меньше — 75 × 2 c): `api /api/s3/clusters/$TAG | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"'`; проверка `endpoints` непустой в сводке.
4. Инспекция: детали содержат `secretProfile`, `nodesList[0].placedOn == "local"`, `links == []`, `denylist == []`.
5. PUT links через панель: с 1 нодой links пуст (`{"links":[]}` → 202); добавить вторую ноду `POST …/nodes` (`{"name":"node2","placement":{"dockerHost":"local"},"resources":{"cpu":0.5,"memGi":1,"diskGi":5}}` → 201; ожидание RUNNING) → `PUT …/links` `[{"a":"node1","b":"node2"}]` → 202; в деталях links отражается (≤ 2-3 тика).
6. deny/allow: `POST …/nodes/node2/deny` → 202; детали: `node2.denied == true`; `POST …/node2/allow` → 202; `denied == false`.
7. sync-probe: `POST …/sync-probe` → 202; ожидание (≤ 60 × 2 s) `connectivity.state` и `sync.state` непустые в `GET /api/s3/clusters/$TAG` (зонд пишет ключи; 2 ноды на одном хосте — связность меряется по факту).
8. Ротация: `POST …/rotations` → 201 (повтор при живой заявке → 200 — идемпотентность воркера, это ожидаемо); детали `.rotation != null`.
9. Снос через панель: `DELETE /api/s3/clusters/$TAG` → 202; ожидание чистоты (≤ 60 × 2 s): `ect get "/s3/$TAG/" --prefix` пусто И нет контейнеров/томов `s3w-$TAG` (ассерт чистоты — канон `docs/e2e-isolation.md`).
10. Финал: `echo "✅ 80-s3-panel: s3-домен через панель (создание/инспекция/links/deny/зонд/ротация/снос) — зелёный, за собой убран"`.

- [ ] **Шаг 2: прогон серий (каждая — в фоне, между сериями зачистка)**

1. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug` → PASS. Зачистка: проверка `docker ps -a` / `docker network ls | grep kfw-net`.
2. Интеграции: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug` → PASS. Зачистка серии.
3. Стенд: `dev-stand/adminpanel/checks/00-up.sh` (если стенд не поднят) → `dev-stand/adminpanel/checks/80-s3-panel.sh` → зелёный. Если стенд уже поднят — сразу чек.
4. Регрессия `75-s3worker.sh` — опционально и с ОГОВОРКОЙ: чек (строки 43/67/70) и `src/tests/S3Worker.IntegrationTests/E2e/S3E2eApiTests.cs` шлют устаревший формат тела создания (`"nodes":1,"placements":[…]` вместо `instances[]`) — актуальный `CreateS3ClusterRequest` воркера читает только `Instances`, поэтому create в 75-чке создаёт «0 инстансов» — это ИЗВЕСТНОЕ расхождение t40, НЕ регрессия t34. Чек и тесты S3Worker НЕ править молча (spec §6: код S3Worker вне задачи; дефект API — отдельное обсуждение); прогон трактовать с этой поправкой.

Критерий задачи: прогоны 1–3 зелёные, после каждой серии остатков нет.

- [ ] **Шаг 3: коммит**

```bash
git add dev-stand/adminpanel/checks/80-s3-panel.sh
git commit -m "t34: e2e-чек 80-s3-panel — полный цикл s3-домена через панель с самоочисткой"
```

---

### Task 16: финал — roadmap-чистка, t34-упоминания, итоговая сборка

**Files:**
- Modify: `arch/roadmap/s3.md` (удалить пункт `t34-s3-panel`)
- Modify: `arch/23-s3-clusters.md` (чистка t34-упоминаний: §2, §3, §4, §6, §8, §9)
- Modify: `arch/24-s3worker.md` (чистка t34-упоминаний: §1, §1.1, §4, §5, §9, §10)
- Modify: `docs/superpowers/…` — НЕ трогать (история задач живёт там).

**Interfaces:**
- Consumes: всё слито (Tasks 1–15).
- Produces: мерж-готовая ветка (критерии приёмки 1–10).

- [ ] **Шаг 1: roadmap-гейт** — из `arch/roadmap/s3.md` удалить пункт `t34-s3-panel` целиком (проверено: `←`-зависимости t34 у других пунктов НЕТ — `grep -rn "←.*t34\|t34.*←" arch/roadmap/` пуст; t36 зависимости не несёт).
- [ ] **Шаг 2: чистка t34-упоминаний в arch/** — по фактическим вхождениям (`grep -n "t34" arch/23-s3-clusters.md arch/24-s3worker.md`; перечень после Задачи 3):
  - `arch/23-s3-clusters.md`: §2 — строка `nodes/<I>/resources` («…занятость наблюдается `usage` и алертами панели t34» → «…алертами панели») и строка `nodes/<I>/usage` («…кормит квота-наблюдение `resources.disk` и панельные алерты (t34)» → без скобок); §3 — «(алерты панели — t34: расхождение, отставание, нода вне контура…)» → без «t34:»; §4 — «…панельную индикацию совпадений (t34)» → «…индикацию совпадений панели»; §6 — «…полностью скрыто бэком, t34)» → без t34; §8 — «только `api/` и `rotations/` (t34)» → без «(t34)»; §9 — заголовок «## 9. Панель (проекция — t34)» → «## 9. Панель (проекция)».
  - `arch/24-s3worker.md`: §1 — «панель домена S3 (t34)» в перечне границ → «панель домена S3»; §1.1 — «…503 + critical-алерт, t34)» → без t34; §4 «Секреты» — «(панель пишет — t34; до того env-фоллбек `S3W_API_TLS_*`)» → «(панель пишет; env-фоллбек `S3W_API_TLS_*`)»; §5 B — «оператора (панель t34 — confirm)» → «оператора (панель — confirm)»; §5 C — «…квота-наблюдение `resources.disk` (…) и алерты панели (t34)» → без «(t34)»; §9 — R1 «панель t34 — индикация состава связей» → «панель — индикация состава связей», R2 «панель t34 — confirm» → «панель — confirm»; §10 «Дальше» — строка «(roadmap/s3.md: t33 зонды, t34 панель, t36 клиентская интеграция)» → перечислить только несделанные (t36).
  - Критерий: `grep -rn "t34" arch/ | grep -v roadmap` — пусто.
- [ ] **Шаг 3: практики (условный шаг)** — если в ходе исполнения пережиты изолированные грабли: заметка в `docs/adminpanel/` — каталог существует (`01-framework.md` … `05-dev-stand.md`, `INDEX.md`): файл по образцу существующих + строка в `INDEX.md`. Если на момент исполнения каталога не окажется — создать; грабель не было — шаг пропустить.
- [ ] **Шаг 4: итоговая сборка и полный прогон**

1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
2. Юниты + интеграции Release (в фоне, серии с зачисткой): PASS.
3. `cd frontend && npm run build` — успех.
4. Чек `80-s3-panel.sh` на свежем Release-стенде — зелёный.

- [ ] **Шаг 5: коммит**

```bash
git add arch/roadmap/s3.md arch/23-s3-clusters.md arch/24-s3worker.md docs/adminpanel/
git commit -m "t34 финал: roadmap-гейт (тег t34-s3-panel удалён), t34-упоминания arch/23-24 почищены"
```

---

## Самопроверка плана (выполнена составителем)

**Покрытие spec:** §3.1 (02-правки) → Tasks 1–2; §3.2 (03-правки) → Task 3; §3.3 (сверка arch/23/24) → Task 3 шаг 3; §4.1 → Task 4; §4.2 → Tasks 5/7/8; §4.3 → Tasks 9/10; §4.4 → Tasks 11–14; §4.5 → Tasks 5/6/7/10/15; Ф5 → Task 16; критерии приёмки 1–10 распределены по задачам (1→T1-3, 2→T5, 3→T10, 4→T6, 5→T12-14, 6→T8/T14, 7→T9/T14, 8→невыполнение правок §2.5 — контроль Глобальных ограничений, 9→T15/T16, 10→T16).
**Открытых вопросов к пользователю нет** (spec §8: принципиальные решены).
**Типы согласованы:** `S3ClusterInfo/Parse/…` (T4) ↔ парсер (T5) ↔ движок (T6) ↔ refresher (T7) ↔ мапперы (T9) ↔ фронт DTO (T11) — имена сверены по тексту задач; `IS3SnapshotStore/Reader` (T7) ↔ потребители (T8/T9); команды (T10) ↔ module ↔ queries.ts (T11); бэковый `OverviewS3Dto` (T9) ↔ фронтовый `OverviewS3Dto`/`OverviewDto.s3` (T11 Шаг 1(б)) ↔ карточка Task 14 (шаг 3).

**Правки по ревью Фазы 4, прогон 1 (все 8 замечаний внесены):**
1. Task 8: `GetWorkersQueryHandler` — правка перенесена в `WorkersModule.cs` (Files + Шаг 3 п.6 + коммит); в `WorkersCommands.cs` остались restart-гвард и `LiveHosts`/cert-хендлеры.
2. Task 13 Шаг 1: у ротации 409-ветка убрана — успех 201/200 (идемпотентность воркера), 404/503 — ProblemDetails; согласовано с Task 10 (§12.2-9) и чеком 80 (Шаг 1 п.8).
3. Task 2 Шаг 1: якорь вставки §12 — «в конец файла, сразу после §11.3» (главы «Версионирование» в 02-файле нет).
4. Task 14 Шаг 3: образец карточки — `KafkaCard` из `OverviewPage.tsx` (valkey-карточки на фронте не существует; `OverviewDto.valkey` бэк отдаёт, фронт не рисует).
5. Task 14 Шаг 2: пункт `{ to: '/s3', label: 'S3' }` в `NAV_ITEMS` после Valkey, без иконок (в проекте иконок нет, `IconDatabase` не используется).
6. Task 3 Шаг 4 + Task 16: перечень t34-упоминаний переписан по фактическим вхождениям (arch/23: §2×2/§3/§4/§6/§8/§9-заголовок; arch/24: §1/§1.1×2/§4/§5×2/§9 R1+R2/§10; «## 10. Дальше» — arch/24).
7. Task 15 Шаг 2 п.4: пометка — 75-чек и S3E2eApiTests шлют устаревший формат тела («nodes»/«placements») — известное расхождение t40, не регрессия t34; не править молча (spec §6).
8. Task 16 Шаг 3: шаг условный; фактическая проверка показала, что `docs/adminpanel/` в worktree СУЩЕСТВУЕТ (01-framework.md…05-dev-stand.md, INDEX.md) — формулировка ссылается на существующие образцы с фоллбеком «если каталога не окажется — создать».

**Правки по ревью Фазы 4, прогон 2 (все 3 замечания внеслены):**
1. Task 11 Шаг 1 разбит на (а)/(б): (б) расширяет фронтовый `OverviewDto` полем `s3: OverviewS3Dto | null` и добавляет интерфейс `OverviewS3Dto { clustersTotal; clustersCritical }` по образцу `OverviewKafkaDto`/`OverviewValkeyDto` (dto.ts ~154–160) — зеркало бэкового `OverviewS3Dto` (Task 9); Task 14 Шаг 3/Interfaces/Шаг 5 ссылаются на это расширение (самовольное расширение типов исполнителем исключено, `data.s3` компилируется).
2. Task 3 Шаг 1: якорь вставки §12 в 03-panels — «в конец файла, после §9 „Версионирование контракта" (последняя глава)»; в Шаге 4 проверки добавлен критерий порядка глав (`tail -2` → §9 → §12).
3. Task 15 Шаг 1 п.3: атрибуция бюджета исправлена — «≤ 150 × 2 s = 300 c — бюджет 51-чека с запасом…; 75-чек ждёт меньше — 75 × 2 c».
