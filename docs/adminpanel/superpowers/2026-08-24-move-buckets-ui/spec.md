# Спека: UI запуска переноса бакетов (заявки /pgworker/moves/)

Дата: 2026-08-24 · Worktree: `feat-move-buckets-ui` · Фаза 1 dev-flow.

## 1. Цель

Дать оператору в AdminPanel интерфейс запуска переноса бакетов с одного шарда
на другой: в деталях кластера выбрать шард-источник, шард-приёмник и список
бакетов источника — панель ставит в etcd **очередь заявок** PgWorker
(`/pgworker/moves/<C>/bucket_<i>`, канон `../pg` `MoveRequest`), который
выполняет переезды. Переезды ВСЕГДА последовательные — и на источнике, и на
приёмнике: параллельных переездов на одном источнике/приёмнике не бывает.

Отображение очереди заявок (что стоит, в каком порядке, кем поставлено) —
часть задачи: без него оператор не видит результат нажатия кнопки.

## 2. Принципы

1. **arch-first**: контракт уже отражён в `arch/02` (§2.3.1, §3, §4, §7,
   §9.7) и `arch/03` (§1 таблица, §1.5, §2 DTO, §3 таблица UI, §3.3) этим же
   коммитом; код не противоречит им.
2. **Последовательность — инвариант контракта PgWorker, не логика панели.**
   PgWorker обрабатывает **старейшую заявку кластера** по `requested_unix`
   (tie-break — лексикографика ключа) и **одну за раз**; заявку после
   успеха/перманентного отказа удаляет процесс (`MoveRequestsStore`). Источник
   и приёмник всегда в одном кластере → любые заявки одного кластера
   выполняются строго по одному. Обязанность панели: корректно упорядочить
   `requested_unix`, не создавать дубликатов и конфликтных заявок. Панель НЕ
   строит собственный планировщик/семафоры.
3. **Заявка — очередь, а не декларация.** Частичная постановка заявок
   безопасна (PgWorker выполнит поставленные), поэтому сбой посередине — без
   компенсации, повтор — идемпотентен. Осознанное отличие от
   create-cluster/add-shard (там недописанная декларация — мусор).
4. **Панель не трогает чужое.** Не перезаписывает и не удаляет существующие
   заявки (etcdctl, `rollback/finalize/abort`): только клэймит пустые ключи
   своих бакетов (txn `compare version==0`).
5. Guard'ы панели — быстрые пред-проверки по снапшоту (образец DeleteShard,
   «Д4»); авторитетная валидация — на стороне PgWorker (заявку отвергнет и
   удалит процесс, кластер не повредится).
6. Русский — документация/комментарии/сообщения; идентификаторы — английские;
   `TreatWarningsAsErrors=true`; тесты с AAA-комментариями; фикстуры парсеров
   — реальные фрагменты значений (формат `MoveRequest.Serialize()` из `../pg`).

## 3. Дизайн-решения (зафиксированы, вопрос закрыт делегированием)

| # | Решение | Обоснование |
|---|---|---|
| Д1 | Последовательность переездов = контракт PgWorker (одна заявка кластера за раз); панель упорядочивает `requested_unix` | Максимальная надёжность без дублирования логики: единый исполнитель уже сериализует работу; второй «планировщик» в панели стал бы источником рассинхрона |
| Д2 | `requested_unix` (секунды): `base = max(now, 1 + max(requested_unix всего префикса /pgworker/moves/))`; k-я заявка (по возрастанию id бакета) получает `base + k` | Наши заявки строго возрастают и встают в конец существующей очереди (в т.ч. чужой) — предсказуемо и без чтения чужой семантики; шаг 1 с достаточно мал (очередь — минуты, не миллисекунды) |
| Д3 | Порядок обработки — по возрастанию id бакета, независимо от порядка в массиве запроса | Предсказуемость для оператора (сверху-вниз по гриду); желание задать exotic-порядок — YAGNI |
| Д4 | Постановка — по одной txn на заявку: `compare version(moveKey)==0` + `put`; compare не сошёлся → 409 | Клэйм-паттерн §9.5: защита от перезаписи чужой заявки в окне «прочитали префикс → пишем»; txn-операций ≤2 на заявку, лимит 128 не участвует |
| Д5 | Сбой etcd посередине → 503 **без компенсации**; повтор POST досдаёт остаток (идентичные заявки → `skipped`) | Частичная очередь валидна и полезна; удаление поставленных рискует убрать заявку, уже взятую процессом; повтор безопасен и сходится к полной очереди |
| Д6 | Идемпотентность повтора: заявка «уже стоит» ⟺ существует ключ с `op=move` и тем же `to` → в ответ `skipped`, без перезаписи | Повтор после частичного сбоя/двойного клика не дублирует и не конфликтует |
| Д7 | Чужая/иная заявка на выбранный бакет (иной `op` или `to`) → 409 всей операции до записей | Перезапись чужой заявки запрещена (принцип 4); оператор видит конфликт и решает сам |
| Д8 | `requested_by` = username сессии панели | Аудит в самом etcd (поле контракта PgWorker); без введения пользователей/ролей |
| Д9 | Источник может быть `TO_REMOVE` (эвакуация перед демонтажем — основной кейс RemoveShard «сначала перевезите»); приёмник `TO_REMOVE` → 409 | На удаляемый шард везти нельзя (PgWorker отвергнет), с удаляемого — нужно |
| Д10 | Чтение очереди `/pgworker/moves/` — в тик снапшота (образец portalloc); транспортный провал роняет тик | Консистентность с существующей политикой «частичный KV-провал = отказ тика» |
| Д11 | Битый JSON заявки/неизвестный `op` → `ParseError` снапшота (алерт `key-malformed`), ключ не трогаем | Готовый механизм панели «не падать»; ключ отвергнёт и удалит сам процесс |
| Д12 | Нешардированная БД (1 бакет и ≤1 шард) → 409 `NonShardedClusterException` | Единый guard с add/remove шарда (arch/03 §2): переездов у нешардированной не бывает |
| Д13 | Отмена/правка заявок, `abort/rollback/finalize` из UI — НЕ входят | Минимальный скоуп «запуск переноса»; отменять заявки безопасно только зная состояние процесса — runbook/roadmap |

## 4. Контракт и компоненты

### 4.1. arch-правки (уже внесены этим же коммитом — источник истины)

- `arch/02-etcd-contract.md`: вступление (пять мутаций); §2 intro (четвёртый
  источник `/pgworker/`); §2.3.1 — таблица читаемых ключей `/pgworker/`
  (portalloc — отражён фактом, moves — формат + семантика удаления процессом);
  §3 — `EtcdSnapshot.MoveTickets` + record `MoveTicket`; §4 п.2 — range
  `/pgworker/moves/` в тике; §7 — вырожденные случаи (битая заявка, заявка
  неизвестного кластера); **§9.7 — протокол мутации** (guard'ы, упорядочивание,
  txn-клэйм, отказ без компенсации); обещания «UI переездов — t07» → ссылки
  на §9.7.
- `arch/03-panels.md`: вступление (пять мутаций); §1 — строка
  `POST /api/clusters/{cluster}/moves`; **§1.5** — контракт (тело, ответ,
  коды отказов); §2 — `MoveTicketDto`, `MoveBucketsRequestDto`,
  `MovesQueuedDto`, `ClusterDto.pendingMoves`; §3 — вкладка Бакеты (кнопка
  «Перенести бакеты»), вкладка Переезды (блок «Очередь заявок»); **§3.3** —
  форма; «форм ввода три» → четыре.
- `arch/01-architecture.md`: диаграмма (мутации `clusters|shards|moves`),
  §9 YAGNI (пять канонических мутаций; abort/heal — по-прежнему вне панели).
- `arch/README.md`: «пять мутаций».

### 4.2. Модель и парсер (Core + Etcd)

- `src/AdminPanel.Core/MoveTicket.cs` (новый файл): `sealed record
  MoveTicket(string Cluster, string Bucket, int? BucketId, string Op,
  string? To, long RequestedUnix, string? RequestedBy)` — по arch/02 §3.
  `Op` — raw-строка канона (`move|rollback|finalize|abort`).
- `EtcdSnapshot` += `IReadOnlyList<MoveTicket> MoveTickets` (после
  `StandNodes`, arch/02 §3) — правится конструктор-цепочка: `SnapshotBuilder`,
  `SnapshotRefresher.FailTick` (прежние `MoveTickets` живут в отказном тике,
  как Clusters), `TestSnapshots`.
- `src/AdminPanel.Etcd/Parsing/MovesQueueParser.cs` — чистая функция
  `IReadOnlyList<Kv> → (Tickets, Errors)`: ключ
  `/pgworker/moves/<C>/<leaf>` → кластер + leaf; leaf `bucket_<i>` →
  `BucketId` (иначе null); JSON-поля `op/to/requested_unix/requested_by`
  (толерантно, как `MoveRequest.Parse`, но без отвержения: отсутствует `op`
  или неизвестен, битый JSON → `KeyParseError`, тик не роняет). Образец —
  `ClustersParser`/`MoveRequestsStore.ParseRange`.
- `SnapshotRefresher`: range `/pgworker/moves/` параллельно с portalloc
  (префикс в приватном `Prefixes`); провал — в общий отказ тика;
  `SnapshotBuilder.Build` += tickets, ошибки — в `ParseErrors` снапшота.

### 4.3. Бэкенд-мутация (Api/Operations, образец AddShardCommand)

`src/AdminPanel.Api/Operations/MoveBucketsCommand.cs`:

```text
MoveBucketsCommand(string Cluster, string From, string To,
                   IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<MovesQueuedDto>
MoveBucketsRequest(string From, string To, IReadOnlyList<int> Buckets)
MovesQueuedDto(string Cluster, string From, string To,
               IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped)
```

Исключения (маппинг в HTTP — `OperationsModule`, образец существующих):
`MoveBucketsValidationException` (400: `buckets` пуст/дубликаты/не int,
`from == to`); `ClusterNotFoundException`/`ShardNotFoundException` (404);
`ClusterNotActiveException`, `NonShardedClusterException`,
`MoveTargetRemovingException` (приёмник TO_REMOVE),
`BucketNotOnSourceException` (id, фактический owner/state),
`MoveRequestConflictException` (бакет, чужая op/to),
`MoveClaimLostException` (txn-compare) — 409; `EtcdWriteUnavailableException`
и прочие etcd-сбои — 503.

Порядок handler'а (§9.7):

1. Валидация тела (400) + имена канонические (кластер — `NamePattern`,
   шарды — образец `DeleteShardCommand.ShardNamePattern`; иначе 404).
2. Активный endpoint из снапшота (нет → 503).
3. `config` напрямую (сбой → 503; нет → 404; `state` не null → 409
   `ClusterNotActiveException`; битый JSON → 503 `InvalidClusterConfigException`).
4. Guard'ы по снапшоту (снапшот есть, кластер есть — иначе 503
   `ShardPrecheckUnavailableException`-образец «повторите запрос»):
   нешардированная (`BucketsCount==1 && Shards.Count<=1`) → 409; `from`/`to`
   существуют (по снапшоту) иначе 404; `to` в `TO_REMOVE` → 409; для каждого
   бакета: `0 ≤ id < BucketsCount`, `routing.owner == from`, state `ACTIVE`
   (не SYNCING/FROZEN/ABORTING/NOT_INITIALIZED) иначе 409 с пояснением.
5. Range `/pgworker/moves/` напрямую (весь префикс — один range; конфликт-
   проверка — по заявкам НАШЕГО кластера, база — глобальный max): на бакет
   уже стоит заявка — идентичная (`op=move`, `to` = наш) → в `skipped`;
   иная → 409 (до любых записей). Попутно `maxUnix = max(requested_unix
   всего префикса)` (глобальный ≥ кластерного — инвариант «в конец очереди
   кластера» сохраняется, чужие кластеры не затрагиваются).
6. `base = max(now, maxUnix + 1)`; по каждому оставшемуся бакету (по
   возрастанию id) txn `compare version(/pgworker/moves/<C>/bucket_<i>)==0` +
   `put` тело
   `{"op":"move","to":"<to>","requested_unix":<base+k>,"requested_by":"<user>"}`
   (сериализация — канон PgWorker: null-поля опускаются, snake_case).
   `!Succeeded` → 409 `MoveClaimLostException`; etcd-сбой → 503 без
   компенсации (уже поставленные — в силе; ответ не формируется).
7. Ответ 201 `MovesQueuedDto`.

`OperationsModule` += `POST /api/clusters/{cluster}/moves` (auth-guard
`/api/*` уже есть): команда + `RequestedBy = user.Identity?.Name ?? "adminpanel"`
(ClaimsPrincipal уже доступен в эндпоинтах — образец `MeQuery`); маппинг
исключений по образцу add-shard (validation → 400 + `errors`,
not-found → 404, guard'ы → 409 `title:"Moves rejected"`, etcd → 503).

### 4.4. Инспекция: pendingMoves в деталях кластера

`ClusterDetailsQuery`: `ClusterDto` += `IReadOnlyList<MoveTicketDto>
PendingMoves`; `MoveTicketDto(int? BucketId, string Bucket, string Op,
string? To, long RequestedUnix, string? RequestedBy)`;
`ClusterDetailsMapper.Map` += параметр `MoveTickets` снапшота → фильтр по
`Cluster`, сортировка по `RequestedUnix` затем `Bucket` (ordinal).
`ClusterDetailsQueryHandler` передаёт `snapshot.MoveTickets`.
Фильтры `?owner=&state=` на бакеты не влияют.

### 4.5. Фронт (React+Mantine, образец AddShardModal)

- `frontend/src/api/dto.ts`: `MoveTicketDto`, `MoveBucketsRequestDto`,
  `MovesQueuedDto`; `ClusterDto` += `pendingMoves: MoveTicketDto[]`.
- `frontend/src/api/queries.ts`: `moveBuckets(cluster, body)` →
  `POST /api/clusters/{cluster}/moves` (комментарий: пятая мутация, 02 §9.7).
- `frontend/src/pages/cluster-details/MoveBucketsModal.tsx` (новый, по
  arch/03 §3.3): два `Select` (источник — все шарды с счётчиком бакетов;
  приёмник — кроме источника и не TO_REMOVE), чекбокс-список бакетов
  источника (только ACTIVE активны; с уже стоящей заявкой `op=move` →
  disabled + бейдж «в очереди»; `finalize/rollback/abort` — бейдж с op),
  «выбрать все»/«снять», подпись про последовательность и порядок по id,
  клиентская валидация (from≠to, непустой выбор), отправка — `useMutation`;
  успех → сводка-Alert в открытой форме («поставлено в очередь: N, уже
  стояли: M» при `skipped.length>0`) с кнопкой «Готово», закрывающей форму;
  инвалидация `clusters` + деталей кластера выполняется сразу (решение
  РП-4 плана: notification-библиотеки в проекте нет — тянуть зависимость
  сверх минимума); ошибка —
  ProblemDetails в теле формы (409 yellow / 400 red / 503 «etcd недоступен»);
  кнопка `loading={mutation.isPending}` (двойной клик).
- `BucketsTab.tsx`: кнопка «Перенести бакеты» в заголовке вкладки Бакеты при
  `canScale` (образец кнопки «Добавить шард» в ShardsTab — кнопка живёт в
  компоненте вкладки), открывает модал; в модал пробрасываются `shards`,
  `buckets`, `pendingMoves` (props вкладки расширяются, `ClusterDetailsPage`
  пробрасывает).
- `MovesTab.tsx`: блок «Очередь заявок» (таблица по `pendingMoves`: бакет,
  op, `to`, возраст заявки `formatUnixAge(requestedUnix)`, кем поставлена);
  пустая очередь — текст «Очередь заявок пуста»; при непустой очереди
  подсказка: «Переезды выполняются по одному бакету за раз — старейшая
  заявка берётся первой». Требует проброса `pendingMoves` из
  `ClusterDetailsPage` (данные уже в `query.data`).

## 5. Фазы реализации

Фаза A — контракт/модель: arch-правки (сделаны в этой фазе, коммитятся
вместе со спекой); `MoveTicket` + `EtcdSnapshot`; `MovesQueueParser` +
фикстуры; `SnapshotRefresher`/`SnapshotBuilder` (+ `TestSnapshots`).
Фаза B — мутация: `MoveBucketsCommand`/handler/исключения;
`OperationsModule`-эндпоинт; юнит-тесты handler'а (мок `IEtcdGateway`).
Фаза C — инспекция: `ClusterDetailsQuery`/`Mapper` `pendingMoves` + тесты
маппера.
Фаза D — фронт: dto/queries, `MoveBucketsModal`, кнопка, очередь заявок в
`MovesTab`; ручная проверка против dev-станда (04: сид дополнить заявкой
`/pgworker/moves/demo/bucket_13`).
Фаза E — интеграционные тесты (Testcontainers, `EtcdSeed` += заявки):
постановка читается refresher'ом; повтор POST → `skipped`; матрица
400/404/409; txn-клэйм против существующего ключа.

Фазы B и C независимы (параллелятся после A); D после B+C; E после B.

## 6. Ограничения (что НЕ делаем)

- Не пишем/не удаляем чужие ключи: только клэйм пустых
  `/pgworker/moves/<C>/bucket_<i>`; `abort/rollback/finalize`, удаление и
  правка заявок, `old_shard/skip_reverse/resume/force` — вне панели (runbook).
- Не форсируем refresher/не ждём выполнения переездов в запросе: заявки
  асинхронны, прогресс — вкладки «Переезды»/«Бакеты» (status-ключи).
- Не добавляем алерты про очередь (длинная очередь легитимна — переезды
  последовательны; «заявка не берётся в работу» неотличима от «ждёт свою
  очередь» без знания PgWorker — в roadmap).
- Никаких изменений в `../pg` (репозиторий только для чтения).
- Roadmap-теги не добавляются (arch/roadmap — только несделанные задачи).

## 7. Критерии приёмки

1. `cd src && dotnet build AdminPanel.slnx` — без ошибок/warning
   (`TreatWarningsAsErrors=true`); `dotnet test` зелёный (юнит +
   интеграционные с Docker).
2. Парсер: фикстуры с реальными телами заявок (`{"op":"move","to":"s2",
   "requested_unix":…,"requested_by":"ops"}`, с `old_shard`/`skip_reverse`,
   op `rollback/finalize/abort`, битый JSON, неизвестный op, неканонический
   leaf) → `MoveTicket`/`KeyParseError` — AAA-тесты на каждом.
3. Handler (юнит, мок gateway): матрица — 400 (пустой `buckets`, дубликаты,
   `from==to`), 404 (нет кластера/шарда), 409 (не Active, нешардированная,
   приёмник TO_REMOVE, бакет не у источника, бакет SYNCING, чужая заявка,
   проигранный клэйм), 503 (etcd-сбой чтения/записи — без компенсации),
   201 (`queued`+`skipped`); `requested_unix` строго возрастают на 1 от
   `max(now, maxUnix+1)`; тело заявки — канон-JSON (op, to, requested_unix,
   requested_by).
4. Интеграция (Testcontainers): refresher подхватывает заявки сида в
   `MoveTickets`; POST ставит ключи в etcd; повтор POST того же тела —
   201, всё в `skipped`, ключи не перезаписаны (value неизменен); POST при
   существующей чужой заявке — 409 до записей.
5. API: `GET /api/clusters/{c}` отдаёт `pendingMoves` (сортировка по
   `requestedUnix`), поля camelCase.
6. UI (dev-стенд): кнопка «Перенести бакеты» видна только при
   Active+sharded; в модале выбор источника/приёмника/бакетов; после
   отправки заявки появляются в «Очереди заявок» вкладки Переезды; уже
   стоящие бакеты в модале disabled с бейджем; 409 показывается текстом
   ProblemDetails; кнопка блокируется на время мутации.
7. `npm run build` во `frontend/` — без ошибок TS.

## 8. В roadmap (enterprise/отложенное — не этот коммит)

- Алерт «заявка не обрабатывается» (PgWorker жив? затор?) — требует сигнала
  из work-journal/claims `/pgworker/` (расширение чтения префикса).
- UI отмены заявки (DELETE ключа, только `op=move` и только свой
  `requested_by`) и `abort` незавершённого переезда — отдельный spec.
- Массовые политики (балансировка «выровнять шарды», проценты) поверх
  заявок; предсказание времени очереди.
- Журнал отвергнутых заявок (сейчас: заявка молча исчезает из очереди —
  диагностика через status/routing).

## 9. Риски и их закрытие

| Риск | Закрытие |
|---|---|
| Снапшот отстал: бакет уже уехал/заявка уже стоит | Прямое чтение префикса moves перед записью (шаг 5); финальную гонку закрывает txn-клэйм (Д4); авторитет — PgWorker (guard'ы процесса) |
| Перезапись чужой заявки | txn `version==0`; иные заявки → 409 (Д7) |
| Двойной клик / повтор | Идемпотентность Д6 (`skipped`), блокировка кнопки |
| Сбой посередине батча | Без компенсации (Д5): поставленные валидны, повтор досдаёт |
| Очередь «зависла» (PgWorker мёртв) | Видна в UI с возрастом; алерт — roadmap (§8); данные не портятся |
| Битые заявки в etcd | ParseError → `key-malformed` (готовый механизм), ключ не трогаем (Д11) |
