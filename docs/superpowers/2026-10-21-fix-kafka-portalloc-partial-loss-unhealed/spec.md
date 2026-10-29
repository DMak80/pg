# Spec: Kafka PortAllocHealer, ветка 2 — частичная утеря записи portalloc не чинится (ревью проекта #5)

**Дата:** 2026-10-21 · **Источник:** пункт «🟡 #5» ревью проекта `.my/review_project.md` ·
**Worktree:** `worktrees/fix-kafka-portalloc-partial-loss-unhealed`

---

## 1. Контекст и цель

`src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs`, ветка 2 лестницы E9
(`ReconstructAsync`, строки 69–115): при живом контейнере брокера без закрепления в
portalloc восстановленный из docker inspect адрес закрепляется в etcd txn
`TxnCompare.NotExists` на ВЕСЬ ключ кластера (строки 88–89).

Дефект: если ключ `/kafkaworker/portalloc/<C>` уже жив (записи других брокеров
сохранились, записи данного брокера нет — операторская правка/частичная утеря),
compare `NotExists` проигрывает ВСЕГДА. Reread (строки 92–100) покрывает только
случай «сосед записал нашего» — тогда адрес берётся из чужой записи. Если нашего
брокера в reread нет, `ReconstructAsync` возвращает успех с адресом инспекции,
но в etcd ничего не записано. Каждый тик надзора повторяет цикл
«инспекция → started → проигрыш txn → reconstructed» без закрепления —
бесконечный journal-шум, инвариант «portalloc = факт» нарушен (адрес жив только
в памяти тика). Формально тик зелёный (кластер работает), но самолечение не
завершается никогда.

**Цель:** ветка 2 закрепляет восстановленный адрес в etcd при любом состоянии
ключа — пустой, частичный (живые записи соседей без нашей), конкурентный.
Успех `ResolveAsync` (ветка 2) допустим только если адрес данного брокера
фактически присутствует в `/kafkaworker/portalloc/<C>` (наша запись или чужая).
Записи соседних брокеров в ключе не затираются.

**Способ (из ревью):** RMW по `mod_revision` с merge — паттерн уже реализован в
этот же ветке 3 (`ReallocateAsync`, строки 174–183: чтение с ревизией → merge →
`CompareKeyUnchanged`) — вместо NotExists-put на весь ключ.

## 2. Принципы

- **arch-first:** контракт лестницы E9 зафиксирован в `arch/16-kafkaworker.md`
  §5 C (строки 534–540: «реконструкция … put-if-absent под `locks/portalloc`,
  проигрыш txn → re-read»). Правка контракта — СНАЧАЛА в `arch/16`, затем код.
- **S5 «первый записавший — истина»** (arch/17): при конкурентной записи
  выигрывает первый; проигравший перечитывает (reread) и принимает чужую истину.
  Семантика сохраняется.
- **Инвариант «portalloc = факт»** (arch/15 §4): закрепление — это запись в etcd,
  не результат в памяти. Успех без записи в etcd — баг.
- **Записи соседей неприкосновенны:** merge дописывает/обновляет ТОЛЬКО запись
  данного брокера; чужие записи переносятся в put как есть.
- **Multi-host/durability:** источник истины — реплицированный etcd; локальных
  кэшей ветка 2 не вводит.
- **Данные неприкосновенны:** контейнер в ветке 2 не трогается (без пересозданий)
  — это не меняется.

## 3. Структура/компоненты (что меняется)

### 3.1. `arch/16-kafkaworker.md` §5 C (строки 534–540)

Фраза «(published-порт CLIENT + host, put-if-absent под `locks/portalloc`,
проигрыш txn → re-read)» заменяется на семантику RMW-merge: реконструкция из
inspect живого контейнера (published-порт CLIENT + host), закрепление — RMW
`/kafkaworker/portalloc/<C>` под `locks/portalloc`: merge восстановленной записи
с живыми записями соседей, txn по `mod_revision` (отсутствие ключа — ревизия 0 =
NotExists); проигрыш txn → re-read (первый записавший — истина, S5); записи
соседей не затираются. Точная формулировка — в тоне окружающего текста, смысл
обязан передавать именно это.

`arch/17` (E9, строка 256) деталей txn ветки 2 не содержит — НЕ трогается.

### 3.2. `src/KafkaWorker.Provisioning/Processes/PortAllocHealer.cs` — `ReconstructAsync`

Новая последовательность секции под клэймом `locks/portalloc` (внутри
существующего try/finally, порядок сохраняется):

1. `ReadPortAllocWithRevisionAsync(cluster, ct)` (уже существует, строки 275–288)
   вместо `ReadPortAllocAsync` — получить `(revision, pinnedAddresses)`.
   Отсутствие ключа даёт `(0, {})` — как в ветке 3.
2. `merged = new Dictionary<string, NodeAddress>(pinnedAddresses); merged[broker] = address;`
   — merge: чужие записи переносятся, наша дописывается.
3. Txn: `TxnRequest.Of([CompareKeyUnchanged(PortAllocKey(cluster), revision)], [TxnOp.Put(key, SerializePortAlloc(merged), null)])`
   — существующий хелпер `CompareKeyUnchanged` (строки 271–272): ревизия 0 →
   `NotExists`, иначе `ModRevisionEqual`. Полный аналог строк 176–178 ветки 3.
4. Транспортный фейл txn → `Failed(txn.Error!)` (как сейчас).
5. Проигрыш compare (`!txn.Value.Succeeded`) → reread `ReadPortAllocAsync` (как
   сейчас, строки 95–100):
   - reread содержит нашего брокера → `address = foreign` (S5: первый записавший
     — истина); успех.
   - reread НЕ содержит нашего брокера → `Failed` с `ApplicationException`
     вида «portalloc {cluster} changed under the claim — retry on next tick»
     (текст — в стиле ветки 3, строка 182). Успех без записи в etcd — запрещён.
6. AdvertisedClient-контроль и журнальная фаза `reconstructed` (строки 107–112) —
   без изменений.

Обновляемые комментарии (семантика put-if-absent → RMW-merge; атрибуции задач не
трогать — пункт #7 вне scope):

- шапка класса, строки 14–15: «реконструкция из docker inspect (published-порт +
  host) клэйм-txn version==0» → RMW-merge по mod_revision;
- комментарий над `ReconstructAsync`, строки 65–68: «запись восстановленного
  закрепления put-if-absent (version==0) … проигрыш txn → re-read» → RMW-merge
  (merge с живыми записями соседей, compare mod_revision; проигрыш → re-read,
  без нашей записи у соседа — retry следующим тиком).

Ветки 1 и 3, `NodeSupervisor`, журнал, метрики — НЕ трогаются.

### 3.3. `src/tests/KafkaWorker.UnitTests/Provisioning/PortAllocHealerTests.cs` — новый юнит-тест

Имя: `Resolve_ContainerAlive_PartialLossMergesIntoLiveKey`. AAA-комментарий по
канону. Механика:

- Arrange: `NewRig()` (порт-окно 21000–21100, broker1 декларирован), затем в
  теле теста напрямую `rig.Etcd.Seed("/kafkaworker/portalloc/events", "...broker2...")`
  — ключ ЖИВОЙ с записью соседского `broker2` (например
  `{"broker2":{"host":"h2","client":21020}}`), записи `broker1` нет; инспекция
  `rig.Driver.Endpoints["broker1"] = new NodeEndpointInspection("h1", 21037, ...)`.
  Сидирование после `NewRig` допустимо: `rig.Addresses` нужен только для ветки 1
  (`broker1` там отсутствует) — ветка выбирается по инспекции.
- Act: `rig.Healer.ResolveAsync(rig.Snapshot, "broker1", rig.Addresses, ct)`.
- Assert:
  - успех; `Address == ("h1", 21037)` (адрес из инспекции); `Recreated == false`;
  - portalloc JSON в etcd содержит И `broker1` с портом 21037, И исходную запись
    `broker2` (host/порт соседа не затёрты) — merge, а не перезапись ключа;
  - `rig.Driver.Ensured`/`Removed` пусты (контейнер не тронут).

Существующие тесты не меняются и остаются зелёными без правок:

- `Resolve_ContainerAlive_ReconstructsPortAlloc`: ключа нет → ревизия 0 →
  `NotExists` → txn выигрывает → запись восстановлена.
- `Resolve_ContainerAlive_TxnLostTakesForeignTruth`: сосед сеет до compare →
  compare (ревизия 0 → `NotExists`) проигрывает → reread находит `broker1`
  (21050) → истина соседа, успех. Комментарий теста про NotExists остаётся
  корректным (при ревизии 0 compare = NotExists).

## 4. Фазы

1. **arch:** правка `arch/16-kafkaworker.md` §5 C (§3.1). Коммит arch-first.
2. **код:** `ReconstructAsync` по §3.2.
3. **тесты:** новый юнит-тест по §3.3.
4. **прогоны:** юниты `KafkaWorker.UnitTests` (полностью), затем серия
   `KafkaWorker.IntegrationTests` (полностью, docker; зачистка контейнеров/сетей
   после серии по AGENTS.md), затем E2E-маркер PgWorker на свежем Release
   (критерии приёмки §7). Сборка и тесты — двумя отдельными командами.

## 5. Ограничения (scope)

- ТОЛЬКО пункт ревью #5 (ветка 2 `ReconstructAsync`). Пункты #3, #6–#22 — вне
  задачи; в том числе НЕ трогаются: дубли PortAllocHealer ×3 в S3/Valkey (#8),
  метрики `healing-portalloc` (#9), атрибуции `(tNN)` в комментариях (#7).
- Ветки 1/3, `NodeSupervisor`, формат ключа portalloc (arch/15 §4), журнальные
  фазы, метрики — без изменений.
- Интеграционный сценарий частичной утери НЕ добавляется (решение пользователя:
  механика покрыта юнитом, ветка 2 в полном стеке уже покрыта существующим
  `LostPortAlloc_Heals_ClusterStaysAlive` Act 1).
- Коммиты — в feature-ветке worktree; мерж в main — только по явному приказу.

## 6. Принятые решения (закрытые развилки)

| # | Развилка | Решение | Основание |
|---|---|---|---|
| 1 | Механика закрепления в ветке 2 | RMW по `mod_revision` с merge (`ReadPortAllocWithRevisionAsync` + `CompareKeyUnchanged` + put merged), ревизия 0 → `NotExists` | Указано ревью; паттерн ветки 3 (строки 174–183) уже в коде |
| 2 | Проигрыш compare при живом ключе | Reread; наш брокер есть → чужой адрес = истина (S5); нет → `Failed` «retry on next tick» | S5-семантика канона + тест `TxnLostTakesForeignTruth`; успех без записи в etcd = сам баг |
| 3 | Порядок arch↔код | Сначала `arch/16` §5 C, затем код | arch-first (AGENTS.base §1) |
| 4 | Где reread | Внутри клэйма (как сейчас, try/finally) | Минимальный дифф; секция короткая (arch/15 §4) |
| 5 | Пробел тестирования | Один новый юнит-тест `Resolve_ContainerAlive_PartialLossMergesIntoLiveKey`; интеграционный сценарий НЕ добавляется | Ответ пользователя (2026-10-21) |
| 6 | Состав мерж-гейта | Сборка Release (отдельная команда) → E2E-маркер `Scale_AddEmptyShard` (`--no-build`) + полная серия `KafkaWorker.IntegrationTests`; полный E2eFixture PgWorker НЕ требуется | Ответ пользователя (2026-10-21): изменение — portalloc-процесс KafkaWorker, PgWorker portalloc/moves не тронуты |
| 7 | Сидирование частичной утери в тесте | Напрямую `rig.Etcd.Seed(...)` с записью `broker2` в теле теста, риг не расширяется | `rig.Addresses` нужен только ветке 1; минимальный дифф тестов |
| 8 | Записи соседей в put | Переносятся как есть из прочитанного значения (merge), сериализация — существующий `SerializePortAlloc` | Инвариант неприкосновенности соседей; §2 |

## 7. Критерии приёмки

1. `arch/16-kafkaworker.md` §5 C описывает закрепление ветки 2 как RMW-merge по
   `mod_revision` под `locks/portalloc` (put-if-absent-формулировка удалена);
   правка закоммичена раньше кода.
2. `ReconstructAsync`: при живом ключе с чужими записями без нашей — txn
   выигрывает, ключ после вызова содержит и соседей, и нашего брокера с адресом
   инспекции; контейнер не пересоздан.
3. `ReconstructAsync`: успех возможен только если адрес данного брокера есть в
   etcd (наш RMW или чужая запись); при проигрыше без нашей записи в reread —
   `Failed` (следующий тик).
4. Ветки 1/3, `NodeSupervisor` — без изменений (дифф не трогает).
5. Новый юнит-тест зелёный; вся серия `KafkaWorker.UnitTests` зелёная.
6. Полная серия `KafkaWorker.IntegrationTests` (docker) зелёная; после серии —
   зачистка контейнеров/сетей (включая осиротевшие `kfw-net-*`).
7. Мерж-гейт: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   (отдельная команда), затем
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
   — зелёный. Свежесть бинаря видна отдельной сборкой.
8. Пункт #5 удалён из `.my/review_project.md` (правило AGENTS.base §16;
   перенумерация остальных пунктов не выполняется).
