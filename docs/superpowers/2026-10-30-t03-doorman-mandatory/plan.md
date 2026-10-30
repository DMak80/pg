# План t03-doorman-mandatory: пулер pg_doorman обязателен, режим «нода без пулера» снят

> **Для исполнителей:** план исполняет СИЛЬНАЯ модель (по умолчанию — субагент
> coder; при executor="main" — main-агент). Часть работы выделена в ПОДПУНКТЫ
> для СЛАБОЙ модели (субагент coder-flash, без контекста проекта): подпункт
> самодостаточен, слабая модель получает только его текст. Пункты шага
> отслеживаются чекбоксами (`- [ ]`).

**Цель:** удалить из кода/образа/тестов ветвление «нода без пулера»
(`EnableDoorman`), привести генерацию конфига и запуск pg_doorman к
фактическому формату (YAML, позиционный аргумент), пулер обязан реально
работать (проверяется клиентским SQL через :6432 в E2E).

**Архитектура:** бинарник пулера ставится в образ `pgworker-node` через
`COPY --from=$DOORMAN_IMAGE` (мульти-арх контейнер-образ, обязательный
build-arg, fail-fast без него); supervisord запускает `pg_doorman` напрямую;
конфиг — YAML от `DoormanConfigBuilder`; воркер всегда публикует тройку портов
5432/8008/6432 и ставит env `DOORMAN_CONFIG`/`PGW_DOORMAN_PORT`; отказ старта
(нет бинарника/конфига) — ненулевой exit главного entrypoint ДО supervisord.

**Технологии:** .NET 10/C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
docker multi-stage/multi-arch, supervisord, pg_doorman v3.11.0, xUnit.

**Spec:** `docs/superpowers/2026-10-30-t03-doorman-mandatory/spec.md` (в этом
же каталоге; план аргументируется от spec — исполнитель читает оба файла).

## Глобальные ограничения (из spec §2/§5, действуют на каждый шаг)

- Комментарии/документация — по-русски; идентификаторы — на английском.
- P/Invoke запрещён; `TreatWarningsAsErrors=true` — сборка обязана быть без warn.
- Таймауты/бюджеты существующих тестов НЕ менять, новых не вводить.
- `DoormanConfigBuilder.ServerConnections` (бюджет P15) не менять.
- Тестовые данные `NodePorts(…, 0)` с заполненным `Object:` (усыновлённые
  записи) НЕ трогать — для object-записей `doorman=0` легален (arch/14 §2.4 п.5).
- Локально собираемые образы в registry не класть; pg_doorman — внешний образ
  (в `images.txt`).
- Тесты — с AAA-комментариями (Arrange/Act/Assert).
- Worktree: все пути ниже — от корня
  `/Users/demakaev/ZCodeProject/worktrees/feat-t03-doorman-mandatory/`.
- Коммит в feature-ветке после каждого шага (стиль сообщений проекта:
  `тип(скоуп): подробное русское описание`).

## Review Focus (что spec подразумевает, но легко сломать)

1. **Сборка образа без `--build-arg DOORMAN_IMAGE`** обязана падать с
   диагностикой про обязательный пулер — проверено командой п. 8.5.
2. **Старт контейнера ноды без env `DOORMAN_CONFIG`** — ненулевой exit ДО
   supervisord (контейнер гаснет, никаких «not starting») — проверено
   командой п. 8.7.
3. **YAML-конфиг реально принимается фактическим pg_doorman v3.11.0**
   (формат general/pools, позиционный аргумент) — функционально доказывается
   только новым E2E-ассертом SQL через :6432 (шаг 6, п. 6.5).
4. **Легаси-запись `doorman=0` (или живой контейнер старого образа без 6432)**
   после ужесточения `MatchesFact` обязана сходить в detach → recreate
   (однократно, не вечным циклом) — пинируется юнит-тестом шага 4 (п. 4.9).
5. **Тройка портов и env пулера ставятся всегда** (ветвлений не осталось) —
   пинируется юнит-ассертами `EnsureNode_NewContainer…` (шаги 3–4).

---

## Шаг 1. arch: сверка внесённых правок + снятие остатка «doorman.ini»

**Вход:** worktree чист; arch-правки фазы spec внесены (см. ниже).

**Действие (сильная модель):**
1. Сверить наличие правок (внесены фазой spec; при написании плана
   подтверждено — они есть; если что-то отсутствует — стоп и разбор, не
   дублировать вслепую):
   - `arch/03-prerequisites.md` §1: платформенная политика «целевая платформа
     проекта — x64 (x86_64); arm64 — только платформа для разработки».
   - `arch/14-pgworker.md` §2.1: состав образа — `pg_doorman` обязателен,
     бинарник — пин мульти-арх контейнер-образа, обязательный build-arg
     `DOORMAN_IMAGE`, fail-fast без него.
   - `arch/14` §2.1 таблица процессов: строка `pg_doorman` — «обязателен —
     сборка образа без `DOORMAN_IMAGE` — fail-fast (R1)».
   - `arch/14` §8: абзац «Пулер pg_doorman — ОБЯЗАТЕЛЬНАЯ часть образа ноды…».
   - `arch/14` §9 R1: митигация через `DOORMAN_IMAGE` (мульти-арх; бинарники
     GitHub-релиза — только amd64, путём поставки не служат).
2. Точечная правка остатка (spec §6 п.6 — конфиг стал YAML; по принципу
   arch-first код обязан зеркалиться arch без остатков формата `.ini`):
   в `arch/14-pgworker.md`, §2.1, абзац после таблицы процессов
   (строка ~403), заменить фразу `(env Spilo, doorman.ini)` на
   `(env Spilo, pg_doorman.yaml)`.
3. Проверить: `grep -rn "DOORMAN_URL" arch/` — пусто вне `arch/roadmap/`
   (термин уходит из arch полностью; упоминание в `arch/roadmap/pgworker.md`
   остаётся до мерж-коммита).

**Выход:** arch согласован с будущим кодом: DOORMAN_IMAGE-канон внесён,
YAML-имя конфига в arch единообразно, DOORMAN_URL отсутствует.

**Проверка:** `grep -rn "DOORMAN_URL" arch/ | grep -v roadmap` — пусто;
`grep -n "doorman.ini" arch/14-pgworker.md` — пусто;
`grep -n "DOORMAN_IMAGE" arch/14-pgworker.md` — ≥4 вхождений (§2.1 ×2, §8, §9 R1).

**Связь со spec:** §3.1 (правки внесены фазой spec, фаза кода сверяет),
§6 п.6 (YAML — закрытая развилка), принцип arch-first §2.

**Подпункты:** нет (работа с arch-текстом — понимание проекта).

- [ ] Шаг 1 выполнен и закоммичен (`arch: t03 — сверка контракта doorman: DOORMAN_IMAGE…`)

---

## Шаг 2. Образ ноды: Dockerfile, главный entrypoint, supervisord, удаление wrapper'а

**Вход:** шаг 1 закрыт (arch согласован).

**Действие (сильная модель):** исполняет подпункты 2.1–2.4, затем ревью
результата (порядок блоков Dockerfile, синтаксис shell, отсутствие остатков
DOORMAN_URL/wrapper).

**Выход:** образ ноды несёт обязательный пулер: fail-fast сборки без
`DOORMAN_IMAGE`, fail-fast старта без бинарника/конфига, YAML-файл конфига,
self-signed TLS-серты, прямой запуск `pg_doorman` из supervisord.

**Проверка:**
- `sh -n docker/node/docker-entrypoint.sh` — выход 0 (синтаксис);
- `grep -rn "DOORMAN_URL\|docker-entrypoint-doorman" docker/` — пусто;
- `grep -n "pg_doorman.yaml" docker/node/docker-entrypoint.sh docker/node/supervisord.conf` — присутствует в обоих;
- `test ! -f docker/node/docker-entrypoint-doorman.sh && echo gone` — `gone`.
(Полная проверка сборкой — шаг 8: fail-fast без build-arg на п. 8.5,
сборка с build-arg внутри E2E-статики на п. 8.6.)

**Связь со spec:** §3.2 полностью; §6 пп.1, 3, 4, 5, 11 (закрытые развилки);
критерии приёмки 3–4.

**Подпункты (слабая модель):**

- **2.1. Переписать `docker/node/Dockerfile`.**
  Что сделать: изменить файл по правилам ниже (файл существует, 53 строки).
  1) Шапку-комментарий (строки 2–8) заменить на:
  ```dockerfile

  # Образ узла кластера pgworker-node (Д4, spec §5.1, arch/14 §2.1):
  # Spilo (PG18+Patroni; R1/Д11 закрыт: failover-слоты поддерживаются с PG17)
  # + pg_doorman (P13–P17, ОБЯЗАТЕЛЕН: клиентский вход :6432, arch/14 §8 —
  # узлов без пулера не существует) + supervisord + pgwnodetool master-lease (P11).
  # Сборка: docker build -f docker/node/Dockerfile -t pgworker-node:dev .
  #   --build-arg DOORMAN_IMAGE=<мульти-арх образ pg_doorman> — ОБЯЗАТЕЛЕН
  #   (пин: ghcr.io/ozontech/pg_doorman:v3.11.0, arch/14 §2.1 R1); без него
  #   сборка падает fail-fast.
  ```
  2) Блок `ARG DOORMAN_URL=""` + `RUN apt-get … curl …` (строки 11–19)
  заменить на:
  ```dockerfile
  # Пулер обязателен (arch/14 §2.1 R1): бинарник — COPY --from мульти-арх
  # контейнер-образа pg_doorman (docker выбирает архитектуру под платформу
  # сборки автоматически); отсутствие build-arg — отказ сборки с диагностикой.
  ARG DOORMAN_IMAGE=""
  RUN test -n "$DOORMAN_IMAGE" || { echo "DOORMAN_IMAGE build-arg is required (pg_doorman is mandatory, arch/14 §2.1 R1)"; exit 1; }

  RUN apt-get update \
      && apt-get install -y --no-install-recommends haproxy supervisor openssl \
      && rm -rf /var/lib/apt/lists/*

  # Бинарник пулера из официального образа (путь внутри него — /usr/bin/pg_doorman).
  COPY --from=$DOORMAN_IMAGE /usr/bin/pg_doorman /usr/local/bin/pg_doorman
  ```
  3) Строку `COPY docker/node/docker-entrypoint-doorman.sh /docker-entrypoint-doorman.sh` — удалить.
  4) В команде `RUN chmod +x …` убрать аргумент `/docker-entrypoint-doorman.sh`
  (остаются `/docker-entrypoint-pgw.sh /docker-entrypoint-haproxy.sh /spilo-supervised.sh`).
  Остальное (WAL/NODE_TOOLS publish-аргументы, `EXPOSE 5432 8008 6432`, `CMD`)
  не менять. В файле после правки не должно остаться подстроки `DOORMAN_URL`.

- **2.2. Изменить `docker/node/docker-entrypoint.sh`.**
  Что сделать: заменить блок DOORMAN_CONFIG (строки 26–30:
  `if [ -n "$DOORMAN_CONFIG" ]; then … printf … pg_doorman.ini … fi`) на:
  ```sh
  # Пулер pg_doorman обязателен (arch/14 §8): нет бинарника или конфига —
  # отказ старта контейнера (ненулевой exit ДО supervisord; воркер видит
  # exited-контейнер). Молчаливых «not starting» не существует.
  if [ ! -x /usr/local/bin/pg_doorman ]; then
      echo "pg_doorman: /usr/local/bin/pg_doorman binary is missing - DOORMAN_IMAGE build-arg is required"
      exit 1
  fi
  if [ -n "$DOORMAN_CONFIG" ]; then
      printf '%s\n' "$DOORMAN_CONFIG" > /etc/pg_doorman/pg_doorman.yaml
  else
      echo "pg_doorman: DOORMAN_CONFIG env is empty - cannot start the mandatory pooler"
      exit 1
  fi

  # Self-signed TLS-серт пулера при каждом старте (P17: клиенты ходят
  # sslmode=require без верификации цепочки — серт шифрования не несёт
  # идентичности); файлы вне volume — ключ не переживает рестарт контейнера,
  # это допустимо (минимум движущихся частей).
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=pg_doorman" \
      -keyout /etc/pg_doorman/tls.key -out /etc/pg_doorman/tls.crt
  ```
  Остальное (блоки HAPROXY_CONFIG, pgw-node.env, SSL_RESTAPI_CA, chown,
  exec supervisord) не менять.

- **2.3. Изменить `docker/node/supervisord.conf`.**
  Что сделать:
  1) Последние две строки шапки-комментария (строки 5–6:
  `; Обёртки делают программы условными: без бинарника/конфига сервис не стартует`
  `; (startsecs=0: быстрый exit 0 обёртки — не «неудачный старт»).`)
  заменить на:
  ```ini
  ; Условных программ нет: отказ старта (нет бинарника/конфига) — ненулевым
  ; exit главного entrypoint ДО supervisord (контейнер гаснет).
  ```
  2) Секцию `[program:pg_doorman]` (строки 43–53) заменить целиком на:
  ```ini
  [program:pg_doorman]
  ; Пулер :6432 ОБЯЗАТЕЛЕН (P13/P14/P15/P17, arch/14 §8): конфиг — позиционный
  ; аргумент (фактический CLI pg_doorman; файл пишет главный entrypoint из env
  ; DOORMAN_CONFIG). Отказ старта (нет бинарника/конфига) — главным entrypoint'ом,
  ; контейнер гаснет. Паттерн надёжности как у walreceiver: рестарты ТОЛЬКО
  ; падений проработавшего процесса; старт-отказ конфига — FATAL без ретраев.
  command=/usr/local/bin/pg_doorman /etc/pg_doorman/pg_doorman.yaml
  priority=30
  autorestart=unexpected
  exitcodes=0
  startsecs=5
  startretries=0
  stdout_logfile=/dev/stdout
  stdout_logfile_maxbytes=0
  redirect_stderr=true
  ```
  Остальные секции не менять.

- **2.4. Удалить файл `docker/node/docker-entrypoint-doorman.sh`**
  (полное удаление файла; ссылок на него после подпункта 2.1 в репозитории нет).

- [ ] Шаг 2 выполнен и закоммичен (`build(node): t03 — пулер pg_doorman обязателен: DOORMAN_IMAGE fail-fast, YAML-конфиг, прямой запуск supervisord'ом`)

---

## Шаг 3. DoormanConfigBuilder → YAML фактического формата pg_doorman v3.11.0

**Вход:** шаг 2 закрыт.

**Действие (сильная модель):** исполняет подпункты 3.1–3.2, ревью
(YAML-структура точно §3.3.1 spec; ассерты — с кавычками значений, как в
выводе билдера), затем прогон тестов.

**Выход:** `DoormanConfigBuilder.Build` генерирует YAML `general`/`pools`
(пул `<dbname>`, TLS require, бюджет `max_db_connections`); юнит-ассерты
`DOORMAN_CONFIG` проверяют YAML и `PGW_DOORMAN_PORT`.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~ClusterDriverTests" --nologo
```
Ожидание: Passed (сборка без warn; `EnsureNode_NewContainer…` и
`EnsureNode_WithTuning…` зелёные с YAML-ассертами).

**Связь со spec:** §3.3 строка `NodeConfigBuilders.cs`, §3.3.1 (целевой
YAML — закрытая развилка п.6), §3.5 юниты (ассерты `DOORMAN_CONFIG`).

**Подпункты (слабая модель):**

- **3.1. Переписать метод `Build` класса `DoormanConfigBuilder`**
  в файле `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`
  (класс начинается со строки ~261: `public static class DoormanConfigBuilder`).
  Что сделать:
  1) XML-doc класса (строки ~251–260, начинаются с
  `/// <summary>` перед `public static class DoormanConfigBuilder`, текст
  про «Конфиг pg_doorman ноды (arch/11 §4)…») заменить на:
  ```csharp
  /// <summary>
  /// Конфиг pg_doorman ноды (arch/11 §4; формат фактического pg_doorman
  /// v3.11.0 — YAML general/pools, файл /etc/pg_doorman/pg_doorman.yaml):
  /// ЕДИНСТВЕННЫЙ пул &lt;dbname&gt; (P14) с бэкендом 127.0.0.1:5432 этой ноды,
  /// transaction-режим (P13), бюджет серверных соединений — синхронизирован
  /// от рассчитанного PGTune max_connections (P15: serverConnections =
  /// max(10, max_connections − 5); 60 = 55 + 2 админ/mover + 3 reserved).
  /// Клиентский вход — :6432 c TLS (sslmode=require, P17 — серты генерирует
  /// entrypoint ноды); секции users/auth_query не задаются — passthrough-режим
  /// по умолчанию (SCRAM-доказательство клиента переиспользуется к бэкенду,
  /// паролей в конфиге нет).
  /// </summary>
  ```
  2) Тело метода `public static string Build(string dbname, int serverConnections) =>`
  (строки ~263–279, raw-string с `[pg_doorman]`, `[databases]`) заменить на:
  ```csharp
    public static string Build(string dbname, int serverConnections) =>
        $"""
        # pg_doorman: single pool {dbname} per node (P13/P14/P15/P17).
        # Clients connect on :6432 with sslmode=require (P17); SCRAM passthrough к PG.
        general:
          host: "0.0.0.0"
          port: 6432
          tls_mode: "require"
          tls_certificate: "/etc/pg_doorman/tls.crt"
          tls_private_key: "/etc/pg_doorman/tls.key"
        pools:
          {dbname}:
            server_host: "127.0.0.1"
            server_port: 5432
            server_database: {dbname}
            pool_mode: "transaction"
            max_db_connections: {serverConnections}
        """;
  ```
  Метод `ServerConnections` и всё остальное в файле не менять.

- **3.2. Обновить ассерты DOORMAN_CONFIG в
  `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`.**
  Что сделать:
  1) В тесте `EnsureNode_NewContainer_EnvFromBuildersAndVolumeAndPorts`
  строку (~607):
  `spec.Env!["DOORMAN_CONFIG"].Should().Contain("pool_mode = \"transaction\"");`
  заменить на три строки:
  ```csharp
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("pool_mode: \"transaction\"");
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("max_db_connections: 55");
        spec.Env!["PGW_DOORMAN_PORT"].Should().Be("16432");
  ```
  2) В тесте `EnsureNode_WithTuning_SpiloCarriesMergedAndDoormanSynced`
  строку (~644):
  `spec.Env!["DOORMAN_CONFIG"].Should().Contain("max_db_connections = 55");`
  заменить на:
  ```csharp
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("max_db_connections: 55");
  ```
  Другие ассерты этих тестов не менять.

- [ ] Шаг 3 выполнен и закоммичен (`feat(core): t03 — DoormanConfigBuilder: YAML фактического формата pg_doorman (general/pools, passthrough)`)

---

## Шаг 4. Удаление EnableDoorman/enableDoorman из кода и тестов; сверка портов всегда тройкой

**Вход:** шаг 3 закрыт (билдер уже YAML).

**Действие (сильная модель):** исполняет подпункты 4.1–4.12, затем:
- ревью: ветвлений не осталось, комментарии соответствуют снятому режиму;
- контрольный grep (см. Проверку): вне `src/tests/PgWorker.E2eTests` не
  осталось ни одного вхождения (E2E-файлы чистятся шагом 6 — туда на этом
  шаге НЕ залезать);
- прогон полной юнит-серии.

**Выход:** в `src/` нет `EnableDoorman`/`enableDoorman` вне
`src/tests/PgWorker.E2eTests` (E2E-файлы чистятся шагом 6; полный grep по
`src/` без исключений — проверки шагов 6 и 8); драйверы всегда публикуют
5432/8008/6432 и ставят `DOORMAN_CONFIG`/`PGW_DOORMAN_PORT`;
`MatchesFact` требует все три порта; `IsCurrent` без ветки `Doorman <= 0`;
сборка всего решения зелёная (включая тестовые проекты).

**Проверка:**
```
grep -rn "EnableDoorman\|enableDoorman" src/ | grep -v PgWorker.E2eTests   # ожидание: пусто (E2E — шаг 6)
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo   # 0 Warning(s)/0 Error(s)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build --nologo
```
Ожидание: grep (с исключением `PgWorker.E2eTests`) пуст; сборка чистая;
юниты зелёные (в т.ч. переписанный
`DetachColliding_ZeroDoormanPort_RecordNotConfirmedAndDetached`).

**Связь со spec:** §3.3 (таблица правок кода), §3.5 юниты, §6 пп.2/7
(полное удаление zero-ignore; guard `doorman > 0` в `ResolveMasterAsync`
остается); критерии приёмки 1, 5.

**Подпункты (слабая модель):**

- **4.1. Файл `src/PgWorker.App/Options.cs`**: удалить целиком две строки
  (~183–184, внутри класса `DockerOptions` между свойством `Images` и
  свойством `AdvertisedHost`):
  ```csharp
      /// <summary>R1: false → узел без pg_doorman (порт 6432 не публикуется).</summary>
      public bool EnableDoorman { get; set; } = true;
  ```

- **4.2. Файл `src/PgWorker.App/appsettings.json`**: удалить строку 21
  `      "EnableDoorman": true,` (в секции `"Docker"`).

- **4.3. Файл `src/PgWorker.App/Program.cs`**: убрать аргумент
  `docker.EnableDoorman,` из двух вызовов:
  1) строка ~331: `return new SwarmClusterDriver(docker.SwarmManager, factory, docker.EnableDoorman, docker.Images.Node,`
  → `return new SwarmClusterDriver(docker.SwarmManager, factory, docker.Images.Node,`
  2) строка ~342: `return new PlainClusterDriver(hosts, factory, docker.EnableDoorman, docker.Images.Node, docker.AdvertisedHost,`
  → `return new PlainClusterDriver(hosts, factory, docker.Images.Node, docker.AdvertisedHost,`

- **4.4. Файл `src/PgWorker.Docker/Drivers/ClusterDriver.cs`** — пять правок:
  1) Первичный конструктор `PlainClusterDriver` (строка ~112): удалить строку
  `    bool enableDoorman,` (между `DockerEngineFactory factory,` и `string nodeImage = "pgworker-node:dev",`).
  2) Первичный конструктор `SwarmClusterDriver` (строка ~657): удалить строку
  `    bool enableDoorman,`.
  3) Метод `EnsureNodeAsync` класса `SwarmClusterDriver`, создание
  внутреннего plain (строка ~712): `[], new DockerEngineFactory(), enableDoorman, nodeImage,`
  → `[], new DockerEngineFactory(), nodeImage,`.
  4) Метод `PortsMatchPlan` (строки ~302–310) заменить целиком на:
  ```csharp
      // Все ожидаемые public-биндинги контейнера совпадают с планом ноды
      // (5432→pg, 8008→patroni, 6432→doorman — тройка всегда, пулер обязателен).
      private bool PortsMatchPlan(IReadOnlyList<PortMap> actual, NodeAddress addr)
      {
          var expected = new List<PortMap>
          {
              new(5432, addr.Ports.Pg), new(8008, addr.Ports.Patroni), new(6432, addr.Ports.Doorman),
          };
          return expected.All(e => actual.Any(p => p.ContainerPort == e.ContainerPort && p.HostPort == e.HostPort));
      }
  ```
  5) В `BuildSpec` (строки ~593–612): блок
  ```csharp
          if (enableDoorman)
          {
              // P15 (решение 2026-09-12): бюджет вычисляемый —
              // max(10, max_connections − 5); tuning null (изолированные пути) —
              // прежний литерал 55.
              var serverConnections = tuning is null ? 55 : DoormanConfigBuilder.ServerConnections(tuning);
              env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections);
              env["PGW_DOORMAN_PORT"] = addr.Ports.Doorman.ToString();
          }
  ```
  заменить на тот же блок без `if` (тело с отступом на один уровень меньше):
  ```csharp
          // P15 (решение 2026-09-12): бюджет вычисляемый —
          // max(10, max_connections − 5); tuning null (изолированные пути) —
          // прежний литерал 55.
          var serverConnections = tuning is null ? 55 : DoormanConfigBuilder.ServerConnections(tuning);
          env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections);
          env["PGW_DOORMAN_PORT"] = addr.Ports.Doorman.ToString();
  ```
  и блок
  ```csharp
          var ports = new List<PortMap>
          {
              new(5432, addr.Ports.Pg), // pg: межшард-подписки + админка (P2)
              new(8008, addr.Ports.Patroni), // Patroni REST (пробы/сверка P11)
          };
          if (enableDoorman)
              ports.Add(new PortMap(6432, addr.Ports.Doorman)); // клиентский вход (P13/P14)
  ```
  заменить на:
  ```csharp
          var ports = new List<PortMap>
          {
              new(5432, addr.Ports.Pg), // pg: межшард-подписки + админка (P2)
              new(8008, addr.Ports.Patroni), // Patroni REST (пробы/сверка P11)
              new(6432, addr.Ports.Doorman), // клиентский вход (P13/P14)
          };
  ```

- **4.5. Файл `src/PgWorker.Provisioning/Processes/MasterKeyReconciler.cs`**:
  1) В XML-doc метода `IsCurrent` (строки ~93–99) удалить последнюю строку
  абзаца `    /// Без doorman (EnableDoorman=false) — точное сравнение (прежняя семантика).`
  (остальной текст doc-комментария не менять).
  2) В теле `IsCurrent` строку (~104):
  `        if (primary.Ports.Doorman <= 0 || string.IsNullOrEmpty(masterKey)) return false;`
  заменить на:
  ```csharp
        if (string.IsNullOrEmpty(masterKey)) return false;
  ```
  (остальные строки метода не менять).

- **4.6. Файл `src/PgWorker.Core/Planning/PortPlanConvergence.cs`**:
  комментарий + метод `MatchesFact` (строки ~70–76) заменить на:
  ```csharp
      // Запись подтверждена фактом: ВСЕ три её порта публикует контейнер самой
      // ноды. Нулевой doorman канонической записи невозможен (пулер обязателен,
      // arch/14 §8): устаревшая запись с doorman=0 (или живой контейнер старого
      // образа без 6432) — расхождение → detach → recreate (штатная конвергенция;
      // volume переживает). object-записи в MatchesFact не попадают (у них нет
      // self-fact) — doorman=0 у усыновлённых легален (arch/14 §2.4 п.5).
      private static bool MatchesFact(NodeAddress addr, IReadOnlySet<(string Host, int Port)> own)
          => Ports(addr).All(p => own.Contains((addr.Host, p)));
  ```
  Метод `Ports` не менять.

- **4.7. Файл `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs`**:
  только комментарий над `ResolveMasterAsync` (строки ~311–317). Фрагмент:
  ```
      // ХОСТУ недопустимо: при EnableDoorman=false ключ вырождается в host:0,
      // и все ноды одного хоста равнозначны — резолв вернул бы произвольную
  ```
  заменить на:
  ```
      // ХОСТУ недопустимо: вырожденное значение host:0 недискриминантно (S4;
      // в штатном режиме невозможно — пулер обязателен, guard ниже — защита),
      // и все ноды одного хоста равнозначны — резолв вернул бы произвольную
  ```
  Код не менять (guard `doorman > 0` в `ResolveMasterAsync` остаётся).

- **4.8. Файл `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`**:
  1) Хелпер `NewPlainDriver` (строки ~240–244): удалить строку-фрагмент
  `enableDoorman: true,` — станет:
  ```csharp
      private static PlainClusterDriver NewPlainDriver(FakeEngine engine, string? advertisedHost = null,
          string? scrapeNetwork = null, string? contourNetwork = null, NodeBackupEnv? nodeBackupEnv = null)
          => new([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine),
              advertisedHost: advertisedHost, scrapeNetwork: scrapeNetwork, contourNetwork: contourNetwork,
              restCertificates: RestCache, nodeBackupEnv: nodeBackupEnv);
  ```
  2) Во всех ОСТАЛЬНЫХ вызовах конструкторов в этом файле удалить аргумент
  `enableDoorman: true,` / `enableDoorman: false,` (позиции на момент
  написания: ~472, ~549, ~626, ~1014, ~1075, ~1115, ~1165; проверка после —
  поиск по слову `enableDoorman` в файле должен давать 0 строк).
  3) Удалить целиком тест `EnsureNode_DoormanDisabled_NoDoormanPortAndConfig`
  (строки ~647–666: атрибут `[Fact]`, комментарий Arrange про
  «флаг EnableDoorman=false (R1: узел без пулера, компромисс стенда)» и тело).

- **4.9. Файл `src/tests/PgWorker.UnitTests/Planning/PortPlanConvergenceTests.cs`**:
  тест `DetachColliding_DoormanDisabledZeroPort_RecordConfirmed` (строки
  ~85–110, включая комментарий над ним) заменить целиком на:
  ```csharp
      // AAA: пулер обязателен (arch/14 §8): каноническая запись с doorman=0 —
      // расхождение с обязательной тройкой (5432/8008/6432) — запись НЕ
      // подтверждена фактом → detach → recreate (легаси-запись или контейнер
      // старого образа без 6432 конвергирует пересозданием, volume жив).
      [Fact]
      public void DetachColliding_ZeroDoormanPort_RecordNotConfirmedAndDetached()
      {
          // Arrange: легаси-запись pg=15000/patroni=15001/doorman=0; факт
          // контейнера — те же два порта (0 в факт не собирается).
          var existing = new Dictionary<string, NodeAddress>
          {
              ["s1/n1"] = new("h1", new NodePorts(15000, 15001, 0)),
          };
          var selfFactByNode = new Dictionary<string, IReadOnlySet<(string, int)>>
          {
              ["s1/n1"] = new HashSet<(string, int)> { ("h1", 15000), ("h1", 15001) },
          };
          var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 15001) };

          // Act
          var changed = PortPlanConvergence.DetachColliding(existing, selfFactByNode, busy);

          // Assert: без 6432 в факте запись не подтверждена — detach.
          changed.Should().BeTrue();
          existing.Should().NotContainKey("s1/n1");
      }
  ```
  Другие тесты файла (в т.ч. с `Object:`) не трогать.

- **4.10. Файл `src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretRotatorTests.cs`**:
  комментарий над `Tick_SharedHostMasterKey_ResolvesByDoormanPort`
  (строки ~240–244):
  ```csharp
      // Деградировавший кейс бага гейта (finding P1): при EnableDoorman=false
      // master-ключ вырождается в <host>:0 — оба ноды шарда на одном хосте равнозначны,
      // и старый host-match возвращал ПРОИЗВОЛЬНУЮ ноду (ALTER ROLE на реплике →
      // 25006 read-only, ротация зацикливалась). Резолв обязан идти по уникальному
      // doorman-порту из ключа.
  ```
  заменить на:
  ```csharp
      // Деградировавший кейс бага гейта (finding P1): вырожденный master-ключ
      // <host>:0 делал бы обе ноды шарда на одном хосте равнозначными — старый
      // host-match возвращал ПРОИЗВОЛЬНУЮ ноду (ALTER ROLE на реплике →
      // 25006 read-only, ротация зацикливалась). Резолв обязан идти по уникальному
      // doorman-порту из ключа (S4: вырожденные значения недискриминантны; в
      // штатном режиме невозможны — пулер обязателен).
  ```
  Код теста не менять.

- **4.11. Файл `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs`**:
  метод `NewDriver` (строки ~33–35):
  ```csharp
      private static PlainClusterDriver NewDriver()
          => new([new HostEndpoint(Host, "unix:///var/run/docker.sock")], Factory, enableDoorman: false,
              AlpineImage, restCertificates: RestCertificates);
  ```
  заменить на:
  ```csharp
      private static PlainClusterDriver NewDriver()
          => new([new HostEndpoint(Host, "unix:///var/run/docker.sock")], Factory,
              AlpineImage, restCertificates: RestCertificates);
  ```

- **4.12. Файл `src/tests/PgWorker.IntegrationTests/Docker/RestTlsConvergenceTests.cs`**:
  1) В portalloc-сиде (строки ~132–136) третью компоненту троек `0` заменить
  на `FreePort()`:
  ```csharp
          var portalloc = new Dictionary<string, NodeAddress>
          {
              ["shard1/shard1a"] = new("local", new NodePorts(FreePort(), FreePort(), FreePort())),
              ["shard1/shard1b"] = new("local", new NodePorts(FreePort(), FreePort(), FreePort())),
          };
  ```
  2) В конструкторе драйвера (строки ~152–157) удалить строку
  `            enableDoorman: false,`.

- [ ] Шаг 4 выполнен и закоммичен (`feat!: t03 — EnableDoorman удалён: пулер обязателен, тройка портов всегда, MatchesFact без zero-ignore`)

---

## Шаг 5. Пин версии pg_doorman в точках сборки (поставка)

**Вход:** шаг 4 закрыт.

**Действие (сильная модель):** исполняет подпункты 5.1–5.4; ревью текста
ранбука (пин и рутина обновления описаны, локально-собираемые образы в
registry не попадают).

**Выход:** `ghcr.io/ozontech/pg_doorman:v3.11.0` закреплён в `images.txt`,
`00-up.sh`, `E2eEnvironment`; ранбук описывает сборку ноды с build-arg и
рутину обновления пина.

**Проверка:**
```
grep -rn "pg_doorman:v3.11.0" dev-stand/ src/tests/PgWorker.E2eTests/ docs/runbook.md
```
Ожидание: ≥4 вхождений (images.txt, 00-up.sh, E2eEnvironment.cs, runbook.md);
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.E2eTests -c Release --nologo` — чисто.

**Связь со spec:** §3.4 (все четыре точки), §6 пп.1–2 (мульти-арх образ,
пин v3.11.0); критерий приёмки 8.

**Подпункты (слабая модель):**

- **5.1. Файл `dev-stand/images/images.txt`**: добавить строку
  `ghcr.io/ozontech/pg_doorman:v3.11.0`
  по алфавиту — между строкой `dxflrs/garage:v2.1.0` и строкой
  `ghcr.io/zalando/spilo-18:4.1-p2`.

- **5.2. Файл `dev-stand/adminpanel/checks/00-up.sh`**: в docker build образа
  ноды (строки ~93–97) добавить build-arg и комментарий. Блок:
  ```bash
  echo ">>> [PHASE] pgworker-node:dev build (WAL_PUBLISH_DIR=artifacts/node-wal, NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools) ($(date +%H:%M:%S))"
  docker build -q -f "$ROOT/docker/node/Dockerfile" \
    --build-arg "WAL_PUBLISH_DIR=artifacts/node-wal" \
    --build-arg "NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools" -t pgworker-node:dev "$ROOT" \
    || { echo "❌ image pgworker-node:dev build failed"; exit 1; }
  ```
  заменить на:
  ```bash
  # DOORMAN_IMAGE — пин мульти-арх образа pg_doorman (arch/14 §2.1 R1; пулер
  # обязателен); ретаг канонического имени делает pull-images.sh из registry.
  echo ">>> [PHASE] pgworker-node:dev build (WAL_PUBLISH_DIR=artifacts/node-wal, NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools, DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0) ($(date +%H:%M:%S))"
  docker build -q -f "$ROOT/docker/node/Dockerfile" \
    --build-arg "WAL_PUBLISH_DIR=artifacts/node-wal" \
    --build-arg "NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools" \
    --build-arg "DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0" -t pgworker-node:dev "$ROOT" \
    || { echo "❌ image pgworker-node:dev build failed"; exit 1; }
  ```

- **5.3. Файл `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs`**:
  1) После строки `internal const string McImage = "minio/mc:RELEASE.2025-08-13T08-35-41Z";`
  (~64) добавить:
  ```csharp
      // Пин pg_doorman (arch/14 §2.1 R1): обязательный build-arg DOORMAN_IMAGE
      // при сборке образа ноды (EnsureStaticAsync); внешний мульти-арх образ —
      // зеркалируется в локальный registry (images.txt + pull-images.sh).
      internal const string DoormanImage = "ghcr.io/ozontech/pg_doorman:v3.11.0";
  ```
  2) В `EnsureStaticAsync`, в аргументы docker build образа ноды
  (строки ~1931–1937, список `["build", "-q", "-f", …, "--build-arg",
  "NODE_TOOLS_PUBLISH_DIR=artifacts/e2e/node-tools", "-t", NodeImage, _root]`),
  после пары `"NODE_TOOLS_PUBLISH_DIR=artifacts/e2e/node-tools"` добавить пару:
  ```csharp
                  "--build-arg", $"DOORMAN_IMAGE={DoormanImage}",
  ```

- **5.4. Файл `docs/runbook.md`**, раздел «Образ PG-ноды с приёмником WAL
  (двухшаговая сборка)» (~189–214):
  1) Команду сборки дополнить build-arg'ом:
  ```
      docker build -f docker/node/Dockerfile \
        --build-arg WAL_PUBLISH_DIR=artifacts/node-wal \
        --build-arg NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools \
        --build-arg DOORMAN_IMAGE=ghcr.io/ozontech/pg_doorman:v3.11.0 \
        -t pgworker-node:dev .
  ```
  2) Сразу после команды добавить абзац:
  ```markdown
  `DOORMAN_IMAGE` — ОБЯЗАТЕЛЬНЫЙ build-arg (пулер pg_doorman — неотъемлемая
  часть образа ноды, arch/14 §2.1 R1): мульти-арх (amd64+arm64) контейнер-образ
  проекта pg_doorman — docker выбирает архитектуру под платформу сборки
  автоматически (целевая платформа x64 — amd64-сборка; arm64 — платформа
  разработки, arch/03 §1). Сборка без build-arg падает fail-fast. Перед
  сборкой на офлайн-хосте — `dev-stand/images/pull-images.sh` (ретаг
  канонического имени из локального registry). Обновление версии пина —
  рутина ниже («Обновить пин pg_doorman»).
  ```
  3) В таблицу «Каталог зеркалируемых образов» (~236–253) по алфавиту
  (перед строкой `ghcr.io/zalando/spilo-18:4.1-p2`) добавить строку:
  `| ghcr.io/ozontech/pg_doorman:v3.11.0 | бинарник пулера в образе PG-ноды (обязательный build-arg DOORMAN_IMAGE, COPY --from) |`
  4) После раздела «Добавить новый внешний образ / обновить версию» (~216–224)
  добавить подраздел:
  ```markdown
  ### Обновить пин pg_doorman

  1. Изменить пин `ghcr.io/ozontech/pg_doorman:v3.11.0` на новую версию
     РОВНО в трёх точках: `dev-stand/adminpanel/checks/00-up.sh`,
     `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs` (константа
     `DoormanImage`), этот раздел (команда сборки + строка каталога ниже).
  2. Обновить строку в `dev-stand/images/images.txt`.
  3. Перемirrorить: `dev-stand/images/mirror-image.sh ghcr.io/ozontech/pg_doorman:<новая версия>`,
     затем `dev-stand/images/pull-images.sh`.
  ```

- [ ] Шаг 5 выполнен и закоммичен (`build(stand): t03 — пин ghcr.io/ozontech/pg_doorman:v3.11.0 в точках сборки + рутина ранбука`)

---

## Шаг 6. E2E: снятие env-режима, комментарии, ассерт клиентского SQL через :6432

**Вход:** шаги 4–5 закрыты (образ собирается с пулером, воркер всегда
публикует 6432).

**Действие (сильная модель):** исполняет подпункты 6.1–6.5, ревью итога:
env-строк нет, формулировки комментариев единообразны (дискриминантность S4 +
резолв по пробам), новый ассерт стоит после достижения Active базового
сценария; компиляция тестовых проектов чистая.

**Выход:** в E2E нет `PgWorker__Docker__EnableDoorman` и упоминаний снятого
режима; `Pgtune_Provision_EnvCarriesMergedParamsAndSyncedDoorman` ассертит
YAML-конфиг безусловно; базовый сценарий проверяет клиентский SQL через
опубликованный doorman-порт (`SELECT 1` app-ролью, sslmode=require).

**Проверка:**
```
grep -rn "EnableDoorman" src/   # ожидание: пусто — ПОЛНЫЙ grep без исключений (включая E2eTests)
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/tests/PgWorker.E2eTests -c Release --nologo   # чисто
```
(Функциональный прогон нового ассерта — полный E2E шага 8.)

**Связь со spec:** §3.5 E2E (все пункты, включая новый ассерт — §6 п.8);
критерии приёмки 1, 7.

**Подпункты (слабая модель):**

- **6.1. Файл `src/tests/PgWorker.E2eTests/E2e/E2eEnvironment.cs`**:
  1) Удалить строку (~1017) `            ["PgWorker__Docker__EnableDoorman"] = "false",`
  2) Комментарий над `NodeImage` (~66–68):
  ```csharp
      // Образ узла (задача 25): без DOORMAN_URL — узел без пулера, PgWorker
      // запускается с EnableDoorman=false (Д4, R1).
      public static string NodeImage => $"pgworker-node:e2e-{ImageTagSuffix}";
  ```
  заменить на:
  ```csharp
      // Образ узла: несёт ОБЯЗАТЕЛЬНЫЙ пулер (arch/14 §8) — пин DOORMAN_IMAGE
      // при сборке EnsureStaticAsync (константа DoormanImage ниже).
      public static string NodeImage => $"pgworker-node:e2e-{ImageTagSuffix}";
  ```

- **6.2. Файл `src/tests/PgWorker.E2eTests/E2e/E2eSecondInstanceScenarios.cs`**:
  удалить строку (~155) `            ["PgWorker__Docker__EnableDoorman"] = "false",`.

- **6.3. Файл `src/tests/PgWorker.E2eTests/E2e/E2ePgtuneScenarios.cs`**:
  1) Строки (~44–47):
  ```csharp
          // Doorman включаем ТОЛЬКО ради env-ассерта бюджета (образ e2e без
          // бинарника — обёртка supervisord тихо выходит, узел поднимается).
          await using var host = await Fx.StartHostAsync("s1",
              extraEnv: new Dictionary<string, string> { ["PgWorker__Docker__EnableDoorman"] = "true" }, ct: ct);
  ```
  заменить на:
  ```csharp
          await using var host = await Fx.StartHostAsync("s1", ct: ct);
  ```
  2) Ассерты doorman-бюджета (~87–89):
  ```csharp
          var doorman = (await ContainerEnvAsync($"pgw-{cluster}-shard1-shard1a", ct))["DOORMAN_CONFIG"];
          doorman.Should().Contain("max_db_connections = 55");
          doorman.Should().Contain("default_pool_size = 55");
  ```
  заменить на:
  ```csharp
          var doorman = (await ContainerEnvAsync($"pgw-{cluster}-shard1-shard1a", ct))["DOORMAN_CONFIG"];
          doorman.Should().Contain("max_db_connections: 55");
  ```

- **6.4. Комментарии «master-ключ … при EnableDoorman=false» в E2E —
  замена по файлам** (точные замены текста; код не менять):
  1) `src/tests/PgWorker.E2eTests/E2e/E2eDrillScenarios.cs` и
     `src/tests/PgWorker.E2eTests/E2e/E2eWalStreamPromoteScenarios.cs` —
     в обоих файлах трёхстрочный фрагмент
     ```
         // Резолв фактического primary — по пробам Patroni /primary (подход
         // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
         // недискриминантен); primary появляется после dsn/RUNNING — ждём.
     ```
     заменить на
     ```
         // Резолв фактического primary — по пробам Patroni /primary (подход
         // WaitForMasterAsync: master-ключ host:<doorman-port> дискриминантен
         // (S4), но резолв остаётся по пробам /primary — первоисточник, S4);
         // primary появляется после dsn/RUNNING — ждём.
     ```
  2) `src/tests/PgWorker.E2eTests/E2e/E2eWalStreamScenarios.cs`,
     `src/tests/PgWorker.E2eTests/E2e/E2eWalStreamMasterDownScenarios.cs`,
     `src/tests/PgWorker.E2eTests/E2e/E2eWalStreamTliScenarios.cs`,
     `src/tests/PgWorker.E2eTests/E2e/E2eTliSwitchScenarios.cs` — в каждом
     трёхстрочный фрагмент
     ```
         // Резолв фактического primary — по пробам Patroni /primary (t02-подход
         // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
         // недискриминантен); primary появляется после dsn/RUNNING — ждём.
     ```
     заменить на
     ```
         // Резолв фактического primary — по пробам Patroni /primary (t02-подход
         // WaitForMasterAsync: master-ключ host:<doorman-port> дискриминантен
         // (S4), но резолв остаётся по пробам /primary — первоисточник, S4);
         // primary появляется после dsn/RUNNING — ждём.
     ```
  3) `src/tests/PgWorker.E2eTests/E2e/E2eMoveScenarios.cs` — фрагмент
     (внутри метода `MasterInfoAsync`)
     ```
             // Резолв мастера по контракту §5 C: проба /primary по patroni-портам
             // portalloc. Матч master-ключа по doorman-порту при EnableDoorman=false
             // недискриминантен (ключ host:0 у всех нод, arch/14 §2.4 п.5):
             // FirstOrDefault по такому матчу отдавал произвольную ноду шарда —
             // seed уходил на реплику (25006 read-only).
     ```
     заменить на
     ```
             // Резолв мастера по контракту §5 C: проба /primary по patroni-портам
             // portalloc. Матч master-ключа по doorman-порту дискриминантен (S4),
             // но резолв остаётся по пробам /primary — первоисточник (S4):
             // FirstOrDefault по ключу на общем хосте отдавал произвольную ноду
             // шарда — seed уходил на реплику (25006 read-only).
     ```
  4) `src/tests/PgWorker.E2eTests/E2e/E2eScaleScenarios.cs` — фрагмент
     ```
     // Мастер шарда: резолв по контракту §5 C — проба /primary по patroni-портам
     // portalloc. Матч master-ключа по doorman-порту при EnableDoorman=false
     // недискриминантен (ключ host:0 у всех нод, arch/14 §2.4 п.5):
     // FirstOrDefault по такому матчу отдавал произвольную ноду шарда — seed
     // уходил на реплику (25006 read-only). Приём E2eMoveScenarios.
     ```
     заменить на
     ```
     // Мастер шарда: резолв по контракту §5 C — проба /primary по patroni-портам
     // portalloc. Матч master-ключа по doorman-порту дискриминантен (S4),
     // но резолв остаётся по пробам /primary — первоисточник (S4):
     // FirstOrDefault по ключу на общем хосте отдавал произвольную ноду шарда —
     // seed уходил на реплику (25006 read-only). Приём E2eMoveScenarios.
     ```
  5) `src/tests/PgWorker.E2eTests/E2e/E2eScenarios.cs` — две правки:
     a) фрагмент (в `AssertFailoverRebuildAsync`, ~323)
     ```
             // Master-ключ (P11: callback + reconciler). При EnableDoorman=false
             // (e2e) portalloc-записи несут doorman:0 (миграция факта инспекции,
             // arch/14 §2.4 п.5) → ключ вырождается в host:0 и НЕдискриминантен по
             // ноде; сравнивать «до/после» нельзя — ключ обязан оставаться живым.
     ```
     заменить на
     ```
             // Master-ключ (P11: callback + reconciler). Значение
             // host:<doorman-port> дискриминантно по ноде (S4); резолв мастера —
             // по пробам Patroni /primary (первоисточник, S4). Живость ключа при
             // смене лидера обязательна (точное «до/после» не сверяется).
     ```
     b) фрагмент (в `WaitForMasterAsync`, ~620)
     ```
             // Резолв мастера по контракту §5 C: проба /primary по patroni-портам
             // portalloc. Матч master-ключа по doorman-порту при EnableDoorman=false
             // недискриминантен (ключ host:0 у всех нод, arch/14 §2.4 п.5):
             // FirstOrDefault по такому матчу возвращал произвольную ноду шарда.
     ```
     заменить на
     ```
             // Резолв мастера по контракту §5 C: проба /primary по patroni-портам
             // portalloc. Матч master-ключа по doorman-порту дискриминантен (S4),
             // но резолв остаётся по пробам /primary — первоисточник (S4):
             // ключ — согласованный кэш, истина роли — в Patroni.
     ```
  6) `src/tests/PgWorker.E2eTests/E2e/E2eAppSecretScenarios.cs` — фрагмент
     (~61–63)
     ```
             // Assert 2 (критерий 2): подключение user=app паролем из etcd выполняет
             // запрос к данным. ПРЯМОЙ pg-порт ноды (пофрагментный проб multi-host DSN,
             // образец E2eScenarios: стенд без doorman, EnableDoorman=false; канон
             // деплоя doorman :6432 — spec §3.1/§7.2 — отдельно не проверяется).
     ```
     заменить на
     ```
             // Assert 2 (критерий 2): подключение user=app паролем из etcd выполняет
             // запрос к данным. ПРЯМОЙ pg-порт ноды (пофрагментный проб multi-host DSN,
             // образец E2eScenarios); клиентский вход через пулер :6432 — отдельный
             // ассерт базового сценария E2eScenarios (arch/14 §8).
     ```
  7) `src/tests/PgWorker.E2eTests/E2e/E2ePgtuneScenarios.cs` — фрагмент
     (~496–498)
     ```
         // Мастер шарда: резолв по контракту arch/14 §5 C — проба /primary по
         // patroni-портам portalloc (приём E2eScaleScenarios.MasterInfoAsync:
         // матч master-ключа по doorman-порту недискриминантен при EnableDoorman=false).
     ```
     заменить на
     ```
         // Мастер шарда: резолв по контракту arch/14 §5 C — проба /primary по
         // patroni-портам portalloc (приём E2eScaleScenarios.MasterInfoAsync:
         // матч master-ключа по doorman-порту дискриминантен (S4), но резолв
         // остаётся по пробам /primary — первоисточник (S4)).
     ```

- **6.5. Новый ассерт клиентского входа через пулер в
  `src/tests/PgWorker.E2eTests/E2e/E2eScenarios.cs`.**
  1) В методе `RunAc2ToAc7Async` сразу после строки
  `        await AssertProvisioningResultAsync(Cluster, ct);`
  вставить:
  ```csharp
              // Клиентский вход — только через пулер (arch/14 §8): SQL через
              // опубликованный doorman-порт мастера шарда.
              await AssertDoormanClientEntryAsync(Cluster, "shard1", ct);
  ```
  2) Перед методом `WaitForMasterAsync` (после метода `PrimaryNodeAsync`,
  который им предшествует) вставить новый приватный метод:
  ```csharp
      // Клиентский вход — ТОЛЬКО через пулер :6432 (arch/14 §8): SELECT 1
      // app-ролью per-cluster секрета по опубликованному doorman-порту мастера;
      // TLS require без верификации цепочки (P17), SCRAM passthrough к бэкенду.
      // Пулер стартует вместе с контейнером (supervisord startsecs=5) — ретраи.
      private async Task AssertDoormanClientEntryAsync(string cluster, string shard, CancellationToken ct)
      {
          // Arrange: мастер по пробам /primary; креды — per-cluster app-секрет.
          var master = await WaitForMasterAsync(cluster, shard, ct);
          var appUser = (await GetOrNullAsync($"/clusters/{cluster}/app_user"))!.Value;
          var appPassword = (await GetOrNullAsync($"/clusters/{cluster}/app_password"))!.Value;

          // Act: SELECT 1 через doorman-порт (бюджет как у SQL-проб нод).
          var alive = await E2ePhase.WaitAsync(Fx, "ac2-doorman-sql", async () =>
          {
              try
              {
                  var dsn = $"Host=localhost;Port={master.Doorman};Database={cluster};" +
                            $"Username={appUser};Password={appPassword};" +
                            "SSL Mode=Require;Trust Server Certificate=true;Timeout=5";
                  await using var con = new NpgsqlConnection(dsn);
                  await con.OpenAsync(ct);
                  await using var cmd = new NpgsqlCommand("SELECT 1", con);
                  return await cmd.ExecuteScalarAsync(ct) is 1;
              }
              catch (NpgsqlException)
              {
                  return false; // пулер ещё стартует — повторим
              }
          }, TimeSpan.FromSeconds(60), ct);

          // Assert: обязательный пулер обслуживает клиентский SQL.
          alive.Should().BeTrue(
              $"the mandatory pooler serves client SQL on the published doorman port {master.Doorman}");
      }
  ```
  (Запись `MasterAddr(string Node, string Host, int Pg, int Doorman)` уже
  существует в файле — `master.Doorman` доступен.)

- [ ] Шаг 6 выполнен и закоммичен (`test(e2e): t03 — снят env-режим EnableDoorman; ассерт клиентского SQL через обязательный пулер :6432`)

---

## Шаг 7. docs: architecture-review — снятие A1, правка A7

**Вход:** шаги 4–6 закрыты.

**Действие (сильная модель):** правка `docs/architecture-review.md`:
1. Удалить целиком раздел `### A1. Два канона клиентского входа в PG: doorman vs прямой порт`
   (заголовок + все буллеты; нумерацию остальных пунктов A2+ НЕ
   перенумеровывать).
2. В разделе `A7. Три писателя мастер-ключа и их вырожденные состояния`
   удалить фразу «Семантика значения зависит от `EnableDoorman` (см. A1).»
   (остальной текст A7 не менять).
3. Прочие пункты файла не трогать.

**Выход:** док описывает только текущее состояние; снятый режим не упоминается.

**Проверка:** `grep -n "EnableDoorman" docs/architecture-review.md` — пусто.

**Связь со spec:** §3.6; §6 п.9 (закрытая развилка); критерий приёмки 8.

**Подпункты:** нет (документация — сильная модель).

- [ ] Шаг 7 выполнен и закоммичен (`docs: t03 — A1 (два канона клиентского входа) закрыт обязательностью пулера`)

---

## Шаг 8. Мерж-гейт: юниты → интеграции → Release-сборка → E2ePrepare → полный docker-E2E

**Вход:** шаги 1–7 закрыты; выполнен ОПЕРАТОРСКИЙ шаг (вне кода):
`dev-stand/images/mirror-image.sh ghcr.io/ozontech/pg_doorman:v3.11.0` и
`dev-stand/images/pull-images.sh` (образ в локальном docker под каноническим
именем).

**Действие (сильная модель):** строго по порядку; между сериями — зачистка
тестовых контейнеров/сетей (страховочный гейт: остаточные контейнеры/тома
предыдущей серии искажают следующую); наблюдение за прогонами по правилам
(старт ≤15 с, контроль зависаний, лог — в файл).

**Выход:** все гейты зелёные на свежем Release; критерии приёмки spec §7
выполнены.

**Проверка (команды; каждая — отдельной командой, сборка и тесты не смешивать):**
1. Итоговый grep (критерии 1–2):
   ```
   grep -rn "EnableDoorman\|enableDoorman\|DOORMAN_URL" src/ docker/ dev-stand/ deploy/ docs/ | grep -v docs/superpowers | grep -v arch/roadmap   # пусто
   grep -rn "DOORMAN_URL" arch/ | grep -v roadmap   # пусто
   ```
2. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --nologo` — Passed.
   Зачистка docker-остатков серии.
3. Интеграционные: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --nologo` — Passed. Зачистка.
4. Сборка ОТДЕЛЬНО: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Warning(s)/0 Error(s).
5. Fail-fast сборки образа (критерий 3, первая половина; быстро, до тяжёлых
   серий): `docker build -f docker/node/Dockerfile .` без
   `--build-arg DOORMAN_IMAGE` (publish-аргументы стендовых каталогов не
   требуются — отказ происходит раньше COPY WAL) — ожидание: падение с
   сообщением
   `DOORMAN_IMAGE build-arg is required (pg_doorman is mandatory, arch/14 §2.1 R1)`.
   Примечание к критерию 3 (вторая половина — «с build-arg собирается и на
   x64-, и на arm64-хосте»): сборка с build-arg на доступном (arm64) хосте
   исполняется п. 8.6 (E2E-статика собирает образ ноды); x64-сборка —
   свойство мульти-арх манифеста апстрима (docker сам выбирает архитектуру
   под платформу сборки), проверяется на целевом x64-хосте — вне этого гейта.
6. E2E-статика: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release` — штамп готовности
   (собирает образ ноды с `--build-arg DOORMAN_IMAGE=…`).
7. Fail-fast старта контейнера (критерий 4; образ ноды уже собран п. 8.6):
   `docker run --rm --entrypoint /docker-entrypoint-pgw.sh <NodeImage>` без env —
   ожидание: ненулевой exit с диагностикой `DOORMAN_CONFIG env is empty`
   (контейнер гаснет ДО supervisord).
8. Полный docker-E2E PgWorker (изменены provisioning и portalloc-конвергенция —
   канон AGENTS.md требует полный прогон; свежий Release, `--no-build`):
   ```
   DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --nologo
   ```
   включая минимум кейс-маркер той же сессии:
   ```
   DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard --nologo
   ```
   Ожидание: Passed/Failed без скипов; новый ассерт `ac2-doorman-sql` зелёный.
   Упавший сценарий — разбор по артефактам `/tmp/pgw-e2e-artifacts-<guid>/`
   (docker-логи/inspect, host.log), перезапуск без анализа запрещён.
9. Зачистка после серий: `docker network ls | grep -c pgw-net` при нуле
   контейнеров — 0 (критерий приёмки 9); осиротевшие сети — `docker network prune -f`.

**Связь со spec:** §4.7 (порядок мерж-гейта дословно), §7 (критерии
приёмки 1–9).

**Примечание (вне этого плана):** удаление пункта `t03-doorman-mandatory` из
`arch/roadmap/pgworker.md` — мерж-коммитом (правила роадмапа), не фазой кода.

**Подпункты:** нет (запуски/разбор прогонов — сильная модель).

- [ ] Шаг 8 пройден

---

## Самопроверка плана (исполнена при написании; обновлена по итогам ревью Фазы 4)

- Покрытие spec: §3.1 → шаг 1; §3.2 → шаг 2; §3.3/§3.3.1 → шаги 3–4;
  §3.4 → шаг 5; §3.5 → шаги 3, 4 (юниты/интеграции), 6 (E2E); §3.6 → шаг 7;
  §4 → порядок шагов 1–8; §5 ограничения → Глобальные ограничения;
  §7 критерии → проверки шагов 1–8. Вне scope шагов нет.
- Развилки: решения выписаны из spec §6 (пп.1–11) в соответствующие шаги;
  формат YAML/имя файла/пин/паттерн supervisord/место ассерта — дословно из
  spec. Единственное микро-решение вне буквы spec — замена остатка «doorman.ini»
  в arch/14 §2.1 на «pg_doorman.yaml» (шаг 1 п.2): прямое следствие §6 п.6
  и принципа arch-first §2 (код обязан зеркалиться arch).
- Имена/сигнатуры — из реального кода worktree (см. текст шагов: строки и
  фрагменты сверены с текущими файлами).
- Подпункты самодостаточны: каждый содержит файл, место, точный текст/код.
- Учтено ревью Фазы 4: (1) шаг 4 — grep и «Выход» ограничены кодом+юнитами+
  интеграциями с явным исключением `PgWorker.E2eTests` (E2E чистится шагом 6;
  полный grep без исключений — шаги 6 и 8.1); (2) п. 8.5 — примечание о
  покрытии второй половины критерия 3 (сборка с build-arg на arm64 — п. 8.6,
  x64 — свойство мульти-арх апстрима, проверяется на целевом хосте);
  (3) проверка старта без `DOORMAN_CONFIG` вынесена из п. 8.5 в отдельный
  п. 8.7 (после сборки образа п. 8.6); правка arch/14 «doorman.ini» —
  оставлена (зам. 3б).
