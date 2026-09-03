# t90-portalloc-parallel-race — глобальный portalloc-клэйм (устранение гонки параллельного provisioning)

- Дата: 2026-09-03
- Roadmap: `arch/roadmap/pgworker.md`, тег `t90-portalloc-parallel-race`
- Контракт (источник истины): `arch/14-pgworker.md` §2.4, §3.3 — обновлены вместе с этим spec
- Worktree: `fix-t90-portalloc-parallel-race`

## 1. Цель

Устранить гонку ПАРАЛЛЕЛЬНОГО выделения портов: два свежих кластера на одном
docker-хосте, засеянные одновременно, получают одинаковые порты, потому что
оба инстанса читают префикс `/pgworker/portalloc/*` ДО первой записи друг
друга — общей картины занятости нет. Воспроизведено на dev-станде 2026-08-25:
контейнеры второго кластера остаются в Created с «port is already allocated».

Механизм (выбор пользователя 2026-09-03): **глобальный lease-клэйм
`/pgworker/locks/portalloc`** — критическая секция довыделения портов
«чтение занятости → выбор троек → запись portalloc» выполняется только
держателем. Альтернативы (курсор per-host с txn-CAS; in-process семафор +
глобальный слой) отклонены: клэйм переиспользует проверенный примитив
ClaimStore, не меняет формат portalloc и модель «первый свободный», не требует
wrap-логики при исчерпании диапазона.

Решения пользователя (зафиксированы):

| Вопрос | Решение |
|---|---|
| Механизм | Глобальный клэйм-лока по паттерну ClaimStore (txn `version==0` + put-with-lease) |
| Охват | Все три точки довыделения: ProvisioningProcess (P1), AddShardProcess, AdoptionProcess (реплан) |
| KafkaWorker (тот же класс гонки) | Отдельная задача roadmap `t91-kafka-portalloc-race` (добавлена в `arch/roadmap/kafkaworker.md`), вне scope t90 |
| Тесты | Юнит + интеграционный тест гонки на EtcdFixture (реальный etcd); e2e-стенд — не требуется |

## 2. Принципы

- **arch/-first**: контракт arch/14 §2.4 + §3.3 обновлён ДО кода; код —
  отражение контракта.
- **Минимальность**: один новый ключ координации, один новый класс; формат
  `/pgworker/portalloc/<C>` и модель аллокации «первый свободный» не меняются;
  закрепления (rebuild) и усыновление (факт контейнера — канон) работают как
  раньше.
- **Переиспользование примитива**: захват/освобождение — паттерн
  `ClaimStore.TryPutLeasedKeyAsync` (txn `NotExists` + lease TTL 15 с),
  отличие — короткая секция без keepalive и с явным release по завершении.
- **Тиковая модель**: не взял клэйм — не ошибка, а InProgress (журнальная фаза
  `waiting-portalloc-lock`); следующий тик (~5 с) повторяет. Никаких
  внутритиковых поллов и ожиданий захвата.
- **Второй эшелон остаётся**: самолечение коллизий (DetachColliding +
  EnsureNode-реплан, arch/14 §8 D) не удаляется — клэйм закрывает
  профилактику, самолечение страхует legacy-стенды и внешние контейнеры.
- **Панель не затронута**: новый подпрефикс `/pgworker/locks/` панелью не
  читается (SnapshotRefresher читает `/pgworker/` избирательно:
  portalloc/moves/api/work).

## 3. Структура / компоненты

### 3.1. PortAllocLock (новый класс)

Расположение: `src/PgWorker.Etcd/Coordination/PortAllocLock.cs` (рядом с
ClaimStore — тот же примитив leased-ключа).

```csharp
public sealed class PortAllocLock(
    string[] endpoints, IEtcdGateway gateway, TimeProvider clock,
    string instanceId) : IAsyncDisposable
{
    // Захват: txn NotExists(/pgworker/locks/portalloc) + put-with-lease TTL 15 с.
    // false = занят другим инстансом (НЕ ошибка). Ошибка etcd → Result.Failed.
    public Task<Result<bool>> TryAcquireAsync(CancellationToken ct);

    // Освобождение: txn [ValueEqual(instance)] → del (чужой лок не трогаем),
    // затем revoke lease (best-effort). Повторный вызов — no-op.
    public Task ReleaseAsync();
}
```

Контракт ключа (arch/14 §3.3):

- Ключ: `/pgworker/locks/portalloc`, lease TTL 15 с.
- Value: `{"instance":"<id>","since_unix":<unix>}` — `instance` = InstanceId
  держателя (единый с ClaimStore, сквозная диагностика).
- Захват: txn `version==0` + put-with-lease (как `/pgworker/leader`).
- Освобождение: явное по завершении секции — del под compare
  `ValueEqual(instance)` (lease истёк и лок перехвачен → чужой не удаляем) +
  revoke lease. Смерть держателя → ключ гасит TTL ≤ 15 с → takeover следующим
  тиком без оператора.
- Без keepalive: критическая секция — единицы секунд ≪ TTL 15 с.

### 3.2. Точки интеграции (три процесса)

Единый паттерн: лок покрывает всю секцию работы с кросс-кластерной картой
занятости — от чтения busy до записи portalloc включительно. Усыновление
(docker-инспекция своих контейнеров) и ранние выходы «менять нечего» (в т.ч.
пред-выход по `PortPlanConvergence.AllConfirmed`) — вне лока, чтобы
rebuild-кластеры и тики ожидания не соперничали за лок без нужды.

1. **ProvisioningProcess.PlanPortsAsync** (P1, `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs`):
   - до лока: чтение pinned, `AdoptRunningContainersAsync`, быстрый пред-выход
     «всё закреплено, adoption ничего не изменил И каждая запись подтверждена
     фактом своей ноды либо object» (`PortPlanConvergence.AllConfirmed`:
     такие записи detach никогда не отцепит — чтение busy ничего бы не
     изменило; без этого условия пред-выход пропускал бы detach-реплан
     коллизий — запись без факта, порт занят соседом). Тики waiting-patroni
     не соперничают за глобальный клэйм;
   - под локом: `GetBusyPortsAsync` → `ReadBusyAsync` → `DetachColliding` →
     ранний выход «ничего не изменилось» (уже с busy) / commit
     существующего / `PlacementPlanner.Plan` + `PortAllocator.Allocate`
     (недобор) → `CommitPortAllocAsync`;
   - не взял лок → `Finish(cluster, "waiting-portalloc-lock", InProgress)`;
   - release — в finally секции.
2. **AddShardProcess** (планирование нод нового шарда): под локом секция
   `GetHostsAsync` → `GetBusyPortsAsync` → `ReadBusyAsync` → `Plan` →
   `Allocate` → put portalloc; ранний выход «всё есть» — до лока; не взял →
   InProgress (журнальная фаза `waiting-portalloc-lock`).
3. **AdoptionProcess** (реплан коллизий/недобор): под локом секция
   `GetBusyPortsAsync` → `ReadBusyAsync` → `DetachColliding` →
   пере-`Allocate` → put portalloc; без изменений (changed=false) — до лока;
   не взял → InProgress.

DI: регистрация в `src/PgWorker.App/Program.cs`, прокидывается ctor-параметром
в три процесса (InstanceId — из ClaimStore). Failover-обёртка по endpoints —
как у соседних компонентов.

Инвариант (для ревью): **любое чтение кросс-кластерной занятости с
последующей записью `/pgworker/portalloc/<C>` происходит под глобальным
portalloc-клэймом**. Взаимоблокировок нет — лок один, вложенности нет,
кластерный клэйм `<C>` удерживается независимо и не порождает циклов
ожидания (лока не берётся внутри чужой критической секции).

### 3.3. Отражение в контракте (сделано в этой ветке)

- `arch/14-pgworker.md` §2.4 п.2 — абзац «Глобальный portalloc-клэйм»:
  семантика взаимоисключения, мотивация (пер-кластерные клэймы кросс-кластерную
  гонку не закрывают), поведение при «не взял»/смерти держателя.
- `arch/14-pgworker.md` §3.3 — строка таблицы ключей `/pgworker/locks/portalloc`.
- `arch/roadmap/kafkaworker.md` — новая задача `t91-kafka-portalloc-race`
  (тот же класс гонки у KafkaWorker; `← t90-portalloc-parallel-race`).

## 4. Фазы реализации

1. **Контракт** — arch/14 §2.4 + §3.3, roadmap t91 (готово, см. §3.3).
2. **PortAllocLock** — класс + DI-регистрация; юнит-тесты (фейк IEtcdGateway:
   захват ок / второй false / release → снова ок / del только своего /
   ошибка etcd → Failed).
3. **Интеграция в ProvisioningProcess** + юнит-тесты фазы (лок занят →
   `waiting-portalloc-lock`, мутаций portalloc нет; лок взят → обычный путь
   P1, включая ранний выход без записи).
4. **Интеграция в AddShardProcess и AdoptionProcess** + юнит-тесты тех же
   инвариантов.
5. **Интеграционный тест гонки** (EtcdFixture, реальный etcd): две параллельные
   критические секции (барьер старта, обе выполняют «ReadBusy → Allocate
   недобора → CommitPortAlloc» для разных «кластеров») — порты в двух ключах
   portalloc НЕ пересекаются; ключ лока появляется и исчезает после release.
6. **Прогон** — `dotnet build` + `dotnet test` всего решения
   (TreatWarningsAsErrors=true), устранение замечаний.

## 5. Ограничения и не-цели

- KafkaWorker не трогается (отдельная задача `t91-kafka-portalloc-race`).
- Формат `/pgworker/portalloc/<C>` и модель «первый свободный» не меняются
  (порты не фрагментируются, wrap-логика не появляется).
- Лок один глобальный на диапазон конфига (не per-host): multi-host
  сериализация избыточна, но редка и коротка — per-host локи YAGNI.
- Лок удерживается только на секцию планирования портов; EnsureNode, фазы
  P2–P5 и ожидания Patroni — вне лока.
- Поведение панели не меняется; e2e-стенд и админ-UI — вне scope.
- Массовый параллельный посев N кластеров растягивается на N тиков (~5 с на
  кластер) — осознанный компромисс (обход сегодня — ручной последовательный
  посев).

## 6. Риски

- **Двойное владение** (держатель завис > TTL 15 с, лок перехвачен, старый
  дописал): окно теории; секция ≪ TTL, страховка — самолечение коллизий
  (DetachColliding + EnsureNode-реплан) и кластерный клэйм при перезаписи.
- **Шум журнала** `waiting-portalloc-lock` при параллельном посеве —
  наблюдаемость (не ошибка), гаснет первым успешным тиком.
- **Отказ etcd при release** — лок живёт до TTL 15 с, конкурент ждёт 1–3
  тика: деградация доступна, корректность сохранена.

## 7. Критерии приёмки

1. arch/14 §2.4 + §3.3 описывают глобальный portalloc-клэйм; roadmap
   содержит t91 (KafkaWorker) — ветка уже содержит эти правки.
2. `PortAllocLock` реализует захват txn `version==0` + put-with-lease TTL 15 с
   и освобождение del-under-`ValueEqual(instance)` + revoke lease.
3. ProvisioningProcess (P1), AddShardProcess, AdoptionProcess выполняют
   довыделение портов только под локом; «не взял» → InProgress с журнальной
   фазой `waiting-portalloc-lock`, без мутаций `/pgworker/portalloc/*`.
4. Юнит-тесты: захват/занятость/release/ошибки лока; инварианты трёх
   процессов при занятом локе.
5. Интеграционный тест (EtcdFixture): две параллельные критические секции
   дают непересекающиеся порты; ключ лока исчезает после release.
6. `dotnet build` и `dotnet test` решения зелёные (TreatWarningsAsErrors,
   порты в тестах динамические — Testcontainers `assignRandomHostPort`).
7. Обход «сеять кластеры последовательно» больше не нужен: параллельный посев
   двух кластеров на одном docker-хосте не порождает «port is already
   allocated» (проверяется интеграционным тестом).
