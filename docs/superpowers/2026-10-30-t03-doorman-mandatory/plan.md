# План t03-doorman-mandatory: пулер pg_doorman обязателен, режим «нода без пулера» снят

> **Для исполнителей:** план исполняет СИЛЬНАЯ модель (по умолчанию — субагент
> coder; при executor="main" — main-агент). Часть работы выделена в ПОДПУНКТЫ
> для СЛАБОЙ модели (субагент coder-flash, без контекста проекта): подпункт
> самодостаточен, слабая модель получает только его текст. Пункты шага
> отслеживаются чекбоксами (`- [ ]`/`- [x]`).
>
> **Актуализация (Фаза 6):** spec обновлён по SPEC_DEVIATION — эмпирическая
> проверка pg_doorman v3.11.0 показала, что «passthrough без users/auth_query»
> НЕ работает (каждый клиент отклоняется) и `general.admin_username`/
> `admin_password` обязательны (без них exit 78). Исполнение уже дошло до
> мерж-гейта (8 коммитов, шаги 1–7); не зелёным остаётся только E2E-фаза
> `ac2-doorman-sql` — причина в YAML без auth_query. План ведёт ОТ текущего
> состояния кода (admin-поля уже в билдере, auth_query нет) К целевому §3.3.1:
> дополнить, не откатывать. Повторное ревью (HIGH): активный шаг 3 дополнен
> подпунктом 3.6 — `NodeConfigBuildersTests.cs` (три 2-аргументных вызова
> `Build` ломают компиляцию юнит-проекта при смене сигнатуры; тест
> `Secrets_AreInEnv_ButNeverInDoorman` ассертит снятую семантику).

**Цель:** удалить из кода/образа/тестов ветвление «нода без пулера»
(`EnableDoorman`), привести генерацию конфига и запуск pg_doorman к
фактическому формату (YAML general/pools c per-pool `auth_query` и
обязательными admin-полями, позиционный аргумент), пулер обязан реально
работать (проверяется клиентским SQL через :6432 в E2E).

**Архитектура:** бинарник пулера ставится в образ `pgworker-node` из
алиас-стадии `FROM ${DOORMAN_IMAGE}` (мульти-арх контейнер-образ, обязательный
build-arg, guard-RUN fail-fast без него); supervisord запускает `pg_doorman`
напрямую; конфиг — YAML от `DoormanConfigBuilder` (допуск клиентов — per-pool
`auth_query` от superuser'а `postgres`, админ-поля — детерминированные
литералы); воркер всегда публикует тройку портов 5432/8008/6432 и ставит env
`DOORMAN_CONFIG`/`PGW_DOORMAN_PORT`; отказ старта (нет бинарника/конфига) —
ненулевой exit главного entrypoint ДО supervisord.

**Технологии:** .NET 10/C# (`Nullable=enable`, `TreatWarningsAsErrors=true`),
docker multi-stage/multi-arch, supervisord, pg_doorman v3.11.0, xUnit.

**Spec:** `docs/superpowers/2026-10-30-t03-doorman-mandatory/spec.md` (в этом
же каталоге; план аргументируется от spec — исполнитель читает оба файла).

## Статус исполнения (на момент актуализации)

| Шаг | Статус | Коммит(ы) |
|---|---|---|
| 1 arch | ВЫПОЛНЕН | 6efdb708 |
| 2 образ ноды | ВЫПОЛНЕН (алиас-стадия — 033a857a поверх cb8ea583) | cb8ea583, 033a857a |
| 3 билдер → YAML | ВЫПОЛНЕН (доводка: auth_query, сигнатура Build(suPassword), doc-комментарии, ассерты, включая NodeConfigBuildersTests п. 3.6) | 9f996725, 91ac0ffa, df396bbc |
| 4 удаление EnableDoorman | ВЫПОЛНЕН (вызов Build с 3-м аргументом доустанавливается шагом 3) | 94b4f34d |
| 5 пины | ВЫПОЛНЕН | ed9119a5 |
| 6 E2E | ВЫПОЛНЕН (ассерты стоят; комментарий метода — доводка шагом 3) | a39236eb, 7e66fe64 |
| 7 docs | ВЫПОЛНЕН (пункт A1 и фраза A7 «Где» удалены — d39b35a9; висячие ссылки A1 вычищены доводкой 7.1: A7 «Направление», §5 пп.1/3) | d39b35a9 |
| 8 мерж-гейт | ВЫПОЛНЕН (серии зелёные); финальный проход ревью — после доводки шага 7 | — |

## Глобальные ограничения (из spec §2/§5, действуют на каждый шаг)

- Комментарии/документация — по-русски; идентификаторы — на английском.
- P/Invoke запрещён; `TreatWarningsAsErrors=true` — сборка обязана быть без warn.
- Таймауты/бюджеты существующих тестов НЕ менять, новых не вводить.
- **Provisioning SQL не меняется; выделенная роль для auth_query НЕ создаётся**
  — запрос исполняется от существующей роли superuser `postgres`
  (per-install `SuPassword`).
- **Класс экспозиции секретов не расширяется**: `SuPassword` попадает в
  `DOORMAN_CONFIG` там же, где он уже есть (env того же контейнера ноды —
  Spilo-env `PGPASSWORD_SUPERUSER`); новых мест хранения нет, в файлы
  volume'ов секрет не пишется.
- `DoormanConfigBuilder.ServerConnections` (бюджет P15) не менять.
- admin-поля `pgwd_admin`/`pgwd_admin_password` — детерминированные ЛИТЕРАЛЫ
  (конфиг обязан быть стабильным между reconcile'ами; секретом не являются).
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
3. **Механизм допуска через пулер** (per-pool `auth_query`, postgres/SuPassword,
   admin-литералы — §6 пп.6–7): конфиг БЕЗ auth_query пулер «запускает», но
   отклоняет каждого клиента — функционально доказывается только E2E-фазой
   `ac2-doorman-sql` (шаг 6, уже стоит в коде; зеленеет после доводки шага 3).
4. **Стабильность DOORMAN_CONFIG между reconcile'ами**: admin-литералы
   детерминированы, `suPassword` — per-install константа — случайные значения
   давали бы вечное пересоздание контейнеров (§6 п.7).
5. **Компиляция юнит-проекта при смене сигнатуры Build**: ВСЕ вызовы
   `DoormanConfigBuilder.Build` переведены на 3-аргументную сигнатуру —
   в т.ч. три вызова в `NodeConfigBuildersTests.cs` (иначе CS7036 ломает
   сборку тестов) — п. 3.6.
6. **Экспозиция секретов в конфиге пулера**: `su-secret` (SuPassword) входит
   ЛЕГИТИМНО (auth_query.password); standby/admin/mover-секреты — не попадают
   никогда — пинируется переписанным тестом п. 3.6.
7. **Легаси-запись `doorman=0`** после ужесточения `MatchesFact` обязана
   сходить в detach → recreate (однократно, не вечным циклом) — пинируется
   юнит-тестом шага 4 (исполнено).
8. **Тройка портов и env пулера ставятся всегда** — пинируется юнит-ассертами
   `EnsureNode_NewContainer…` + E2E-ассертом «12 уникальных портов на 4 ноды»
   (исполнено).
9. **`docs/architecture-review.md` не ссылается на удалённый A1**: после
   удаления пункта в документе не должно остаться висячих упоминаний (A7
   «Направление», §5 «Приоритеты» пп.1/3) — иначе документ позиционирует
   закрытую задачей t03 проблему как открытый приоритет; правки — только три
   фиксированных замены п. 7.1, иные места документа не трогать; контроль —
   `grep -n 'A1' docs/architecture-review.md` пусто (шаг 7, доводка).

---

## Шаг 1. arch: сверка внесённых правок + снятие остатка «doorman.ini» — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммит `6efdb708`). С обновлённым spec расходений нет —
не переделывать. Механизм auth_query — деталь реализации конфига, контракт
arch не меняется (spec §3.1).

**Что было сделано (фиксация):** сверка arch/03 §1 (платформенная политика) и
arch/14 §2.1×2/§8/§9 R1 (DOORMAN_IMAGE-канон, обязательность пулера);
`doorman.ini` → `pg_doorman.yaml` в arch/14 §2.1; `grep DOORMAN_URL arch/`
(вне roadmap) — пусто.

**Проверка (контрольная, в мерж-гейте п. 8.1):** `grep -rn "DOORMAN_URL" arch/ | grep -v roadmap` — пусто.

**Связь со spec:** §3.1; §6 п.1–2.

- [x] Шаг 1 выполнен

---

## Шаг 2. Образ ноды: Dockerfile, главный entrypoint, supervisord, удаление wrapper'а — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммиты `cb8ea583` + правка `033a857a`). Отклонение от
первоначальной редакции плана (`COPY --from=$DOORMAN_IMAGE`) — легитимная
доводка исполнителя, канонизированная обновлённым spec §3.2/§6 п.1: BuildKit
не раскрывает ARG в `COPY --from` напрямую — источник бинарника
**алиас-стадия**. С обновлённым spec расхождений нет — не переделывать.

**Целевое состояние (= фактическое, для ревью):** верх Dockerfile:
```dockerfile
ARG DOORMAN_IMAGE=""
FROM ${DOORMAN_IMAGE:-alpine:3.20} AS doorman

FROM ghcr.io/zalando/spilo-18:4.1-p2
ARG DOORMAN_IMAGE
RUN test -n "$DOORMAN_IMAGE" || { echo "DOORMAN_IMAGE build-arg is required (pg_doorman is mandatory, arch/14 §2.1 R1)"; exit 1; }
```
далее `apt-get … haproxy supervisor openssl`, `COPY --from=doorman
/usr/bin/pg_doorman /usr/local/bin/pg_doorman`; wrapper
`docker-entrypoint-doorman.sh` удалён (COPY/chmod убраны); главный entrypoint:
fail-fast бинарника/`DOORMAN_CONFIG` (пишет `/etc/pg_doorman/pg_doorman.yaml`)
+ self-signed TLS (`openssl req -x509 … /etc/pg_doorman/tls.{key,crt}`);
supervisord `[program:pg_doorman]`: `command=/usr/local/bin/pg_doorman
/etc/pg_doorman/pg_doorman.yaml`, `autorestart=unexpected`, `exitcodes=0`,
`startsecs=5`, `startretries=0`.

**Проверка (контрольная):** `grep -rn "DOORMAN_URL\|docker-entrypoint-doorman" docker/` — пусто;
полная проверка сборкой — пп. 8.5–8.7.

**Связь со spec:** §3.2; §6 пп.1, 3, 4, 5, 12.

**Подпункты:** нет (исполнено; механической работы не осталось).

- [x] Шаг 2 выполнен

---

## Шаг 3. DoormanConfigBuilder: доводка YAML до §3.3.1 — auth_query, сигнатура, doc-комментарии, юнит-ассерты — ВЫПОЛНЕН

**Вход (текущее состояние кода):** шаги 1–2, 4–7 выполнены. В
`src/PgWorker.Core/Templates/NodeConfigBuilders.cs` билдер уже генерирует YAML
`general/pools` с обязательными `admin_username: "pgwd_admin"`/
`admin_password: "pgwd_admin_password"` (коммит `91ac0ffa`), но: НЕТ per-pool
`auth_query`, сигнатура 2-аргументная `Build(string dbname, int
serverConnections)`, doc-комментарий класса содержит неверную посылку
(«секции users/auth_query не задаются — passthrough-режим по умолчанию»),
doc-комментарий `InstallSecrets` — прежний («НИКОГДА не попадают в конфиги
doorman»). Вызов в `ClusterDriver.BuildSpec` (~597):
`DoormanConfigBuilder.Build(topology.Cluster, serverConnections)`. В
`src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs` — три
2-аргументных вызова `Build` (~180, ~215, ~334; при смене сигнатуры — CS7036,
юнит-проект не соберётся) и тест `Secrets_AreInEnv_ButNeverInDoorman`
(~174–186) со старой семантикой `NotContainAny` для всех секретов, включая
su-secret. Из-за отсутствия auth_query пулер v3.11.0 отклоняет каждого
клиента — E2E-фаза `ac2-doorman-sql` красная.

**Действие (сильная модель):** исполняет подпункты 3.1–3.6 (дополнение
существующего кода, не откат: admin-поля уже стоят — их НЕ трогать);
затем ревью: YAML дословно соответствует §3.3.1, пароль — от
`InstallSecrets.SuPassword` (BuildSpec уже принимает `secrets`), алфавит
пароля DSN-safe (вставка в YAML-строку в двойных кавычках безопасна,
`InstallSecretsGuard`); все вызовы `Build` (прод + тесты) — 3-аргументные;
прогон юнитов.

**Выход:** `Build(string dbname, int serverConnections, string suPassword)`
генерирует YAML §3.3.1 (auth_query от postgres + admin-литералы);
`BuildSpec` передаёт `secrets.SuPassword`; doc-комментарии
`DoormanConfigBuilder` и `InstallSecrets` описывают фактический механизм;
юнит-ассерты ClusterDriverTests проверяют admin-поля и auth_query;
NodeConfigBuildersTests — на 3-аргументной сигнатуре, тест семантики секретов
переписан (SuPassword легитимно в конфиге, прочие — никогда); комментарий
E2E-ассерта — про auth_query. Конфиг остаётся стабильным между reconcile'ами
(все значения детерминированы: литералы + per-install константы).

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo   # 0 Warning(s)/0 Error(s) — включая юнит-проект (сигнатура Build переведена везде)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --nologo
```
Ожидание: сборка чистая (CS7036 нет); полный прогон юнитов зелёный — в т.ч.
`ClusterDriverTests` (admin/auth_query-ассерты), переписанный
`Doorman_ConfigCarriesSuPassword_OtherSecretsNeverLeak` и
`Doorman_SingleTransactionPoolWithBudget`/`Doorman_BudgetSynchronizedWithMaxConnections`
(3-аргументные вызовы).

**Связь со spec:** §3.3 (таблица: сигнатура Build, вызов из BuildSpec,
doc-комментарии DoormanConfigBuilder/InstallSecrets), §3.3.1 (целевой YAML —
закрытые развилки пп.6–7), §3.5 юниты — пункты `ClusterDriverTests` и
`Templates/NodeConfigBuildersTests.cs` (перевод вызовов + переработка теста
секретов), §5 ограничения (без новой роли, без расширения экспозиции), §7
критерий 5.

**Подпункты (слабая модель):**

- **3.1. Файл `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`** — правки
  в `DoormanConfigBuilder` (класс ~строка 267, после `InstallSecretsGuard`):
  1) XML-doc класса (блок `/// <summary>` … `/// </summary>` непосредственно
  перед `public static class DoormanConfigBuilder`) заменить целиком на:
  ```csharp
  /// <summary>
  /// Конфиг pg_doorman ноды (arch/11 §4; формат фактического pg_doorman
  /// v3.11.0 — YAML general/pools, файл /etc/pg_doorman/pg_doorman.yaml):
  /// ЕДИНСТВЕННЫЙ пул &lt;dbname&gt; (P14) с бэкендом 127.0.0.1:5432 этой ноды,
  /// transaction-режим (P13), бюджет серверных соединений — синхронизирован
  /// от рассчитанного PGTune max_connections (P15: serverConnections =
  /// max(10, max_connections − 5); 60 = 55 + 2 админ/mover + 3 reserved).
  /// Клиентский вход — :6432 c TLS (sslmode=require, P17 — серты генерирует
  /// entrypoint ноды). Допуск клиентов — per-pool auth_query (единственный
  /// работающий механизм v3.11.0: без объявленных пользователей пулер
  /// отклоняет каждого клиента): пулер верифицирует клиентский SCRAM по
  /// verifier'у из pg_shadow и переиспользует клиентское доказательство к
  /// бэкенду — пароли клиентов в конфиге не хранятся. Роль исполнения
  /// auth_query — существующий superuser postgres (per-install SuPassword;
  /// алфавит DSN-safe [A-Za-z0-9._-] по InstallSecretsGuard — вставка в
  /// YAML-строку в двойных кавычках безопасна). admin_username/admin_password —
  /// обязательные поля general фактического v3.11.0 (без них отказ парса,
  /// exit 78); админ-консоль пулера продуктом не потребляется — литералы
  /// детерминированы (конфиг обязан быть стабильным между reconcile'ами,
  /// иначе вечное пересоздание контейнеров) и границей секьюрити не являются.
  /// </summary>
  ```
  2) Метод `Build` (сигнатура + raw-string тело) заменить целиком на:
  ```csharp
      public static string Build(string dbname, int serverConnections, string suPassword) =>
          $"""
          # pg_doorman: single pool {dbname} per node (P13/P14/P15/P17).
          # Clients connect on :6432 with sslmode=require (P17); admission — auth_query.
          general:
            host: "0.0.0.0"
            port: 6432
            admin_username: "pgwd_admin"
            admin_password: "pgwd_admin_password"
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
              auth_query:
                query: "SELECT passwd FROM pg_shadow WHERE usename = $1"
                user: "postgres"
                password: "{suPassword}"
          """;
  ```
  (Секции `general`, admin-поля и блок `pools` до `max_db_connections` уже
  есть в файле — замену делать целиком по образцу выше; метод
  `ServerConnections` и всё остальное в файле не менять. Символ `$1` в
  query-строке — литерал, интерполяция в C# идёт только через фигурные
  скобки.)

- **3.2. Файл `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`** — XML-doc
  записи `InstallSecrets` (блок `/// <summary>` перед
  `public sealed record InstallSecrets`, ~строки 13–17) заменить на:
  ```csharp
  /// <summary>
  /// Секреты установки (Д7, spec §10): per-install из env PgWorker, в etcd не
  /// пишутся. Прокидываются в контейнер ноды. SuPassword дополнительно входит
  /// в DOORMAN_CONFIG (auth_query.password пула pg_doorman,
  /// DoormanConfigBuilder): класс экспозиции НЕ расширяется — секрет уже в
  /// env того же контейнера (PGPASSWORD_SUPERUSER Spilo-env, тот же docker
  /// inspect); в файлы volume'ов секреты не попадают (конфиг живёт в env,
  /// файл — внутри контейнера).
  /// </summary>
  ```

- **3.3. Файл `src/PgWorker.Docker/Drivers/ClusterDriver.cs`** — в методе
  `BuildSpec` (~строка 597) строку
  `        env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections);`
  заменить на
  ```csharp
          env["DOORMAN_CONFIG"] = DoormanConfigBuilder.Build(topology.Cluster, serverConnections, secrets.SuPassword);
  ```
  (параметр `secrets` типа `InstallSecrets` уже есть в сигнатуре `BuildSpec`).

- **3.4. Файл `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`** —
  дополнить ассерты `DOORMAN_CONFIG` (SuPassword-стаб фикстуры — `"su"`:
  `private static readonly InstallSecrets Secrets = new("su", "sb", "ba", "mv");`):
  1) В тесте `EnsureNode_NewContainer_EnvFromBuildersAndVolumeAndPorts` сразу
  ПОСЛЕ строки
  `        spec.Env!["DOORMAN_CONFIG"].Should().Contain("pool_mode: \"transaction\"");`
  вставить:
  ```csharp
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("admin_username: \"pgwd_admin\"");
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("admin_password: \"pgwd_admin_password\"");
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("auth_query:");
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("user: \"postgres\"");
        spec.Env!["DOORMAN_CONFIG"].Should().Contain("password: \"su\"");
  ```
  (существующие ассерты `max_db_connections: 55` и `PGW_DOORMAN_PORT` не трогать).
  2) В тесте `EnsureNode_WithTuning_SpiloCarriesMergedAndDoormanSynced` сразу
  ПОСЛЕ строки
  `        spec.Env!["DOORMAN_CONFIG"].Should().Contain("max_db_connections: 55");`
  вставить те же пять строк из п.1.

- **3.5. Файл `src/tests/PgWorker.E2eTests/E2e/E2eScenarios.cs`** — комментарий
  над методом `AssertDoormanClientEntryAsync` (~строки 623–626):
  ```csharp
      // Клиентский вход — ТОЛЬКО через пулер :6432 (arch/14 §8): SELECT 1
      // app-ролью per-cluster секрета по опубликованному doorman-порту мастера;
      // TLS require без верификации цепочки (P17), SCRAM passthrough к бэкенду.
      // Пулер стартует вместе с контейнером (supervisord startsecs=5) — ретраи.
  ```
  заменить на:
  ```csharp
      // Клиентский вход — ТОЛЬКО через пулер :6432 (arch/14 §8): SELECT 1
      // app-ролью per-cluster секрета по опубликованному doorman-порту мастера;
      // TLS require без верификации цепочки (P17); допуск — per-pool auth_query
      // (SCRAM по verifier'у из pg_shadow, к бэкенду — переиспользование
      // клиентского доказательства) — функциональная проверка допуска.
      // Пулер стартует вместе с контейнером (supervisord startsecs=5) — ретраи.
  ```
  Код метода не менять (ассерт уже корректен: app-креды + sslmode=require).

- **3.6. Файл `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`** —
  перевести на 3-аргументную сигнатуру `Build` и переработать тест
  семантики секретов. SuPassword-стаб фикстуры — `"su-secret"` (поле
  `Secrets` класса, ~строки 24–25:
  `private static readonly InstallSecrets Secrets = new("su-secret",
  "standby-secret", "admin-secret", "mover-secret");`). Правки:
  1) Тест `Secrets_AreInEnv_ButNeverInDoorman` (~строки 174–186) заменить
  ЦЕЛИКОМ (новое имя — `Doorman_ConfigCarriesSuPassword_OtherSecretsNeverLeak`):
  ```csharp
      [Fact]
      public void Doorman_ConfigCarriesSuPassword_OtherSecretsNeverLeak()
      {
          // Arrange: секреты установки (Д7).

          // Act: генерируем оба конфига.
          var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets, syncStrict: false, "shard1a", TestRestTls());
          var doorman = DoormanConfigBuilder.Build("shop", 55, "su-secret");

          // Assert: секреты прокинуты в env контейнера (Spilo/bootstrap);
          // su-secret ЛЕГИТИМНО входит в doorman — auth_query.password пула
          // (допуск клиентов; экспозиция не расширяется: секрет уже в env
          // того же контейнера); прочие секреты в конфиг пулера не попадают.
          env.Values.Should().Contain(new[] { "su-secret", "standby-secret", "admin-secret", "mover-secret" });
          doorman.Should().Contain("password: \"su-secret\"");
          doorman.Should().NotContainAny("standby-secret", "admin-secret", "mover-secret");
      }
  ```
  2) В тесте `Doorman_SingleTransactionPoolWithBudget` (~строка 215) вызов
  `        var config = DoormanConfigBuilder.Build("shop", 55);`
  заменить на
  ```csharp
          var config = DoormanConfigBuilder.Build("shop", 55, "su-secret");
  ```
  и сразу ПОСЛЕ строки
  `        config.Should().Contain("max_db_connections: 55");`
  вставить два ассерта:
  ```csharp
          config.Should().Contain("auth_query:");
          config.Should().Contain("user: \"postgres\"");
  ```
  3) В тесте `Doorman_BudgetSynchronizedWithMaxConnections` (~строка 334)
  вызов
  `        DoormanConfigBuilder.Build("shop", 35).Should().Contain("max_db_connections: 35");`
  заменить на
  ```csharp
          DoormanConfigBuilder.Build("shop", 35, "su-secret").Should().Contain("max_db_connections: 35");
  ```
  Другие тесты файла не трогать (вызовы `SpiloEnvBuilder.Build` — другой
  метод, сигнатуру не меняли).

- [x] Шаг 3 выполнен и закоммичен (`fix(core): t03 — DoormanConfigBuilder: per-pool auth_query от superuser postgres (единственный работающий допуск v3.11.0) + сигнатура Build(suPassword), NodeConfigBuildersTests под новую семантику секретов; InstallSecrets-doc под фактическую экспозицию`)

---

## Шаг 4. Удаление EnableDoorman/enableDoorman из кода и тестов — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммит `94b4f34d`). С обновлённым spec расхождений нет —
не переделывать. Единственное отличие от первоначальной редакции плана:
вызов генератора в `BuildSpec` теперь 3-аргументный (§3.3) — доустанавливается
подпунктом 3.3 активного шага 3.

**Что было сделано (фиксация):** удалены `EnableDoorman` (Options.cs,
appsettings.json, Program.cs ×2) и `enableDoorman` (оба драйвера + внутренний
plain в `SwarmClusterDriver.EnsureNodeAsync`); `PortsMatchPlan` — тройка
5432/8008/6432 всегда; `BuildSpec` — env `DOORMAN_CONFIG`/`PGW_DOORMAN_PORT` и
порт 6432 безусловно; `MasterKeyReconciler.IsCurrent` — без ветки
`Doorman <= 0`; `PortPlanConvergence.MatchesFact` — без `.Where(p => p > 0)`;
комментарий `ClusterSecretRotator.ResolveMasterAsync` — под S4; юниты:
хелпер/конструкторы без параметра, тест ветки без пулера удалён,
`DetachColliding_ZeroDoormanPort_RecordNotConfirmedAndDetached` (обратная
семантика), комментарий `ClusterSecretRotatorTests`; интеграционные:
`DockerDriverTests.NewDriver` без параметра, `RestTlsConvergenceTests` —
конструктор без параметра + `FreePort()`-тройки в portalloc-сиде.

**Проверка (контрольная):**
```
grep -rn "EnableDoorman\|enableDoorman" src/   # пусто — ПОЛНЫЙ grep (E2eTests уже чист, шаг 6 исполнен)
```

**Связь со spec:** §3.3 (таблица), §3.5 юниты/интеграции, §6 п.8; критерий 1.

**Подпункты:** нет (исполнено).

- [x] Шаг 4 выполнен

---

## Шаг 5. Пин версии pg_doorman в точках сборки — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммит `ed9119a5`). С обновлённым spec расхождений нет —
не переделывать.

**Что было сделано (фиксация):** `ghcr.io/ozontech/pg_doorman:v3.11.0` в
`dev-stand/images/images.txt` (по алфавиту), build-arg
`DOORMAN_IMAGE=…` + комментарий в `dev-stand/adminpanel/checks/00-up.sh`,
константа `internal const string DoormanImage = …` + пара
`"--build-arg", $"DOORMAN_IMAGE={DoormanImage}"` в
`E2eEnvironment.EnsureStaticAsync`; ранбук: команда сборки с build-arg, абзац
про DOORMAN_IMAGE, строка в каталоге зеркалируемых, подраздел «Обновить пин
pg_doorman» (три точки + images.txt + перемirror).

**Проверка (контрольная):** `grep -rn "pg_doorman:v3.11.0" dev-stand/ src/tests/PgWorker.E2eTests/ docs/runbook.md` — ≥4 вхождений.

**Связь со spec:** §3.4; §6 пп.1–2; критерий 8.

**Подпункты:** нет (исполнено).

- [x] Шаг 5 выполнен

---

## Шаг 6. E2E: снятие env-режима, комментарии, ассерты клиентского входа — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммиты `a39236eb` + `7e66fe64`). С обновлённым spec
расхождений нет; единственная доводка — комментарий метода
`AssertDoormanClientEntryAsync` (подпункт 3.5 активного шага 3: формулировка
про auth_query-допуск вместо «SCRAM passthrough»). Не откатывать.

**Что было сделано (фиксация):** env `PgWorker__Docker__EnableDoorman`
удалён (E2eEnvironment, E2eSecondInstanceScenarios); комментарий `NodeImage`
— «несёт ОБЯЗАТЕЛЬНЫЙ пулер — пин DOORMAN_IMAGE при сборке
EnsureStaticAsync»; `E2ePgtuneScenarios` — без extraEnv, ассерт
`max_db_connections: 55` безусловно; 12 файлов с комментариями
«master-ключ host:0 при EnableDoorman=false» переписаны под S4 (ключ
дискриминантен, резолв — по пробам `/primary`); в базовом
`E2eScenarios.RunAc2ToAc7Async` после `AssertProvisioningResultAsync` стоит
`AssertDoormanClientEntryAsync(Cluster, "shard1", ct)` (фаза
`ac2-doorman-sql`: Npgsql → `localhost:{master.Doorman}`, sslmode=require,
app-креды per-cluster секрета из etcd, SELECT 1, ретраи 60 с); ассерт
provisioning'а — 12 уникальных host-портов на 4 ноды (тройка портов ноды
всегда, arch/14 §8).

**Проверка (контрольная):** `grep -rn "EnableDoorman" src/` — пусто.
Функциональный прогон `ac2-doorman-sql` — шаг 8 (зеленеет после доводки
шага 3: до неё пулер отклонял каждого клиента — «FATAL: No connection pool
configured for database … user …»).

**Связь со spec:** §3.5 E2E; §6 пп.9 (ac2-doorman-sql — функциональная
проверка auth_query-допуска); критерии 1, 7.

**Подпункты:** нет (исполнено; доводка комментария — подпункт 3.5).

- [x] Шаг 6 выполнен

---

## Шаг 7. docs: architecture-review — вычистка висячих ссылок на удалённый A1 — ВЫПОЛНЕН

**Статус:** ВЫПОЛНЕН (коммит `d39b35a9` + доводка 7.1). Доводка исполнена:
три замены по обновлённому §3.6 внесены (A7 «Направление», §5 «Приоритеты»
пп.1/3), `grep -n 'A1' docs/architecture-review.md` — пусто, иных правок
документа нет. Исторический контекст (замечание merge-гейта contract/minor
о висячих ссылках) — в git.

**Вход (текущее состояние):** в `docs/architecture-review.md` осталось ровно
три упоминания A1 (проверено grep'ом, иных нет): A7 блок «Направление»
(~стр. 112–114 — оборот «, а при решении A1 — сократить число форматов»);
§5 «Приоритеты» п.1 (~стр. 191–193 — заголовок «A1 + A2 …» с хвостом «решение
снимает спец-ветки master-ключа, firewall-матрицы и doorman-бюджета»);
§5 «Приоритеты» п.3 (~стр. 197 — «решение по A1/A2»).

**Действие (сильная модель):** исполняет подпункт 7.1 — три точечные замены
фиксированного текста; иные правки документа НЕ вносятся (содержание пунктов
A2–A7 — кроме двух названных правок A7, — C1–C6, разделов §4/§6 не меняется);
затем ревью: замены дословно по spec §3.6, кавычки/переносы строк — по стилю
окружающего текста файла.

**Выход:** `docs/architecture-review.md` не содержит упоминаний A1 нигде —
ни как пункта, ни как ссылки/приоритета; целевые формулировки A7
«Направление» и §5 пп.1/3 совпадают с зафиксированными в spec §3.6
(§5 п.1 — «**A2 (судьба HAProxy)** — самое видное расхождение «канон ↔
топология»», хвост про спец-ветки удалён; §5 п.3 — «решение по A2»).

**Проверка:**
```
grep -n 'A1' docs/architecture-review.md   # пусто
```
плюс сверка трёх целевых формулировок со spec §3.6 (A7 «Направление» — без
оборота про A1; §5 п.1 — только A2/HAProxy; §5 п.3 — «по A2»).

**Связь со spec:** §3.6 (полная вычистка A1, контроль — grep пусто); §6 п.10;
критерий 8.

**Подпункты (слабая модель):**

- **7.1. Файл `docs/architecture-review.md`** (каталог `docs/`) — три
  точечные замены существующего текста, БЕЗ каких-либо иных правок файла:
  1) В пункте «A7. Три писателя мастер-ключа и их вырожденные состояния»
  (раздел отклонений) найти блок «Направление» из трёх строк:
  ```
  - **Направление**: описать в arch/14 статус ключа как «согласованный кэш с
    тремя классами писателей» (сейчас это размазано по §2.1/§5 C/R8), а при
    решении A1 — сократить число форматов.
  ```
  и заменить его целиком на:
  ```
  - **Направление**: описать в arch/14 статус ключа как «согласованный кэш с
    тремя классами писателей» (сейчас это размазано по §2.1/§5 C/R8).
  ```
  2) В разделе «5. Приоритеты» пункт 1 (три строки) целиком:
  ```
  1. **A1 + A2 (один канон входа PG и судьба HAProxy)** — самое видное
     расхождение «канон ↔ топология»; решение снимает спец-ветки master-ключа,
     firewall-матрицы и doorman-бюджета.
  ```
  заменить на одну строку:
  ```
  1. **A2 (судьба HAProxy)** — самое видное расхождение «канон ↔ топология».
  ```
  3) В разделе «5. Приоритеты» пункт 3 строку:
  ```
     тем же коммитом, что и решение по A1/A2, чтобы не тащить два канона дальше.
  ```
  заменить на:
  ```
     тем же коммитом, что и решение по A2, чтобы не тащить два канона дальше.
  ```
  Больше в файле НИЧЕГО не менять (содержание остальных пунктов — как было).
  После правок фраза «A1» в файле встречаться не должна.

- [x] Шаг 7 выполнен (доводка закрыта: висячие ссылки A1 вычищены)

---

## Шаг 8. Мерж-гейт: юниты → интеграции → Release-сборка → E2ePrepare → полный docker-E2E — ВЫПОЛНЕН

**Вход:** шаги 1–6, 8 выполнены; шаг 7 — доводка 7.1 (docs-only,
сборочно-тестовые серии гейта не перезапускаются — контроль
`grep -n 'A1' docs/architecture-review.md` пусто + повторное ревью); выполнен
ОПЕРАТОРСКИЙ шаг (вне кода):
`dev-stand/images/mirror-image.sh ghcr.io/ozontech/pg_doorman:v3.11.0` и
`dev-stand/images/pull-images.sh` (образ в локальном docker под каноническим
именем).

**Контекст прогона (SPEC_DEVIATION):** предыдущая попытка гейта упала на
E2E-фазе `ac2-doorman-sql` («No connection pool configured …») — причина
диагностирована (конфиг без auth_query), устранена шагом 3. Перезапуск —
только после доводки и СВЕЖЕЙ сборки (`--no-build` на свежем Release);
перезапуск «как есть» без анализа запрещён (артефакты упавшего прогона —
`/tmp/pgw-e2e-artifacts-<guid>/` и `$TMPDIR` — уже разобраны фазой 6).

**Действие (сильная модель):** строго по порядку; между сериями — зачистка
тестовых контейнеров/сетей (страховочный гейт); наблюдение за прогонами по
правилам (старт ≤15 с, контроль зависаний, лог — в файл).

**Выход:** все гейты зелёные на свежем Release; критерии приёмки spec §7
(актуализированные) выполнены.

**Проверка (команды; каждая — отдельной командой, сборка и тесты не смешивать):**
1. Итоговый grep (критерии 1–2):
   ```
   grep -rn "EnableDoorman\|enableDoorman\|DOORMAN_URL" src/ docker/ dev-stand/ deploy/ docs/ | grep -v docs/superpowers | grep -v arch/roadmap   # пусто
   grep -rn "DOORMAN_URL" arch/ | grep -v roadmap   # пусто
   ```
2. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --nologo` — Passed
   (критерий 5: YAML-ассерты — тройка портов всегда, `admin_username`/
   `admin_password`, `auth_query` c `user: postgres` и паролем
   SuPassword-стаба, `PGW_DOORMAN_PORT`; NodeConfigBuildersTests — на
   3-аргументной сигнатуре, семантика секретов под auth_query). Зачистка
   docker-остатков серии.
3. Интеграционные: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --nologo` — Passed
   (критерий 6: RestTlsConvergence с живой тройкой портов в portalloc-сиде;
   DOORMAN_CONFIG-стабы entrypoint-тестов — валидный YAML). Зачистка.
4. Сборка ОТДЕЛЬНО: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 Warning(s)/0 Error(s).
5. Fail-fast сборки образа (критерий 3, первая половина; быстро, до тяжёлых
   серий): `docker build -f docker/node/Dockerfile .` без
   `--build-arg DOORMAN_IMAGE` (publish-аргументы стендовых каталогов не
   требуются — отказ происходит на guard-RUN раньше COPY) — ожидание:
   падение с сообщением
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
   Ожидание (критерий 7): Passed/Failed без скипов; фаза `ac2-doorman-sql`
   зелёная — клиентский SELECT через опубликованный doorman-порт с
   `sslmode=require` и per-cluster app-кредами (функциональная проверка
   auth_query-допуска); env `PgWorker__Docker__EnableDoorman` в E2E
   отсутствует; portalloc-тройки нод — все три порта > 0 (ассерт «12
   уникальных» уже в коде). Упавший сценарий — разбор по артефактам
   `/tmp/pgw-e2e-artifacts-<guid>/` (docker-логи/inspect, host.log),
   перезапуск без анализа запрещён.
9. Зачистка после серий: `docker network ls | grep -c pgw-net` при нуле
   контейнеров — 0 (критерий 9); осиротевшие сети — `docker network prune -f`.

**Связь со spec:** §4 п.7 (порядок мерж-гейта), §7 (критерии приёмки 1–9 в
актуализированных формулировках).

**Примечание (вне этого плана):** удаление пункта `t03-doorman-mandatory` из
`arch/roadmap/pgworker.md` — мерж-коммитом (правила роадмапа), не фазой кода.

**Подпункты:** нет (запуски/разбор прогонов — сильная модель).

- [x] Шаг 8 пройден

---

## Самопроверка плана (исполнена при написании; актуализирована по SPEC_DEVIATION, состоянию ветки, повторному ревью и замечанию merge-гейта)

- Покрытие spec: §3.1 → шаг 1 (исполнен); §3.2 → шаг 2 (исполнен, алиас-стадия
  — канон нового §3.2/§6 п.1); §3.3/§3.3.1 → шаг 3 (доводка: auth_query,
  сигнатура `Build(…, suPassword)`, вызов из BuildSpec, doc-комментарии
  DoormanConfigBuilder/InstallSecrets) + шаг 4 (исполнен); §3.4 → шаг 5
  (исполнен); §3.5 → шаги 3 (юниты: ClusterDriverTests — admin/auth_query;
  NodeConfigBuildersTests — п. 3.6: перевод трёх вызовов на 3-аргументную
  сигнатуру + переработка теста секретов `Doorman_ConfigCarriesSuPassword_
  OtherSecretsNeverLeak`), 4, 6 (исполнены; ac2-doorman-sql + 12-портовый
  ассерт стоят); §3.6 → шаг 7 (частично исполнен: пункт A1 и фраза A7 «Где»
  удалены; доводка — вычистка трёх висячих ссылок на A1: A7 «Направление»,
  §5 пп.1/3 — подпункт 7.1); §4 → порядок шагов; §5 ограничения
  (включая новые: без изменения provisioning SQL, без новой роли, без
  расширения экспозиции SuPassword, admin-литералы) → Глобальные ограничения;
  §7 критерии → проверки шагов 3, 7, 8. Вне scope шагов нет.
- Развилки: решения выписаны из spec §6 (пп.1–12 в новой нумерации) —
  auth_query-блок/роль postgres/пароль SuPassword дословно из §3.3.1/п.6,
  admin-литералы `pgwd_admin`/`pgwd_admin_password` — п.7, сигнатура и вызов —
  §3.3, ассерты — §3.5/§7 п.5. Имя переписанного теста фиксировано явно:
  `Doorman_ConfigCarriesSuPassword_OtherSecretsNeverLeak` (выбор имени —
  не развилка, санкционирован ревью). Целевые тексты трёх правок
  architecture-review — дословно из §3.6 (§5 п.1: без хвоста про спец-ветки
  master-ключа/firewall-матрицы/doorman-бюджета; §5 п.3: «решение по A2»;
  A7 «Направление»: без оборота про A1); кавычки «…», завершающая точка п.1
  и переносы строк — по стилю окружающего текста файла (не развилка).
  Новых микро-решений вне буквы spec нет (прежнее «doorman.ini →
  pg_doorman.yaml» исполнено шагом 1).
- Состояние кода: план сверён с фактическим worktree (13 коммитов ветки):
  доводка шага 3 (auth_query, сигнатура `Build(…, suPassword)`,
  doc-комментарии, ассерты — включая п. 3.6 NodeConfigBuildersTests)
  закоммичена (`df396bbc`), мерж-гейт пройден; единственная открытая работа —
  доводка 7.1 (docs-only, `docs/architecture-review.md`), код и тесты не
  затрагивает.
- Имена/сигнатуры — из реального кода: `Build(string dbname, int
  serverConnections)` (текущая) → `Build(string dbname, int serverConnections,
  string suPassword)` (целевая, §3.3); `BuildSpec` принимает
  `InstallSecrets secrets`; SuPassword-стабы: ClusterDriverTests — `"su"`,
  NodeConfigBuildersTests — `"su-secret"` (поле `Secrets`, строки 24–25);
  вызовы Build в NodeConfigBuildersTests — ~180, ~215, ~334;
  `EnsureNode_NewContainer…`/`EnsureNode_WithTuning…` — существующие тесты.
- Подпункты самодостаточны: каждый содержит файл, место, точный текст/код.
  Подпункт 7.1 сверен с фактическим `docs/architecture-review.md`: три
  A1-упоминания — строки ~114, ~191–193, ~197, иных нет (grep), исходные и
  целевые тексты перенесены в подпункт дословно.
- Учтено ревью Фазы 4 (архив): grep-исключение E2eTests в промежуточном
  шаге (теперь неактуально — E2E чист, оставлен полный grep в контрольной
  проверке шага 4); примечание о покрытии x64-сборки (п. 8.5); fail-fast
  старта контейнера отдельным пунктом после сборки образа (п. 8.7).
- Учтено повторное ревью (HIGH): подпункт 3.6 — NodeConfigBuildersTests
  (CS7036 на трёх 2-аргументных вызовах устранён переводом сигнатуры;
  `Secrets_AreInEnv_ButNeverInDoorman` переработан под auth_query-семантику:
  su-secret легитимно в конфиге, NotContainAny — только для
  standby/admin/mover-секретов); «Проверка» шага 3 — полный прогон юнитов
  (достижим после перевода сигнатуры); «Связь со spec» — §3.5 пункт
  `Templates/NodeConfigBuildersTests.cs`; Review Focus — пп.5–6
  (компиляция юнитов при смене сигнатуры; экспозиция секретов).
- Учтено замечание merge-гейта (contract/minor): после удаления пункта A1 из
  `docs/architecture-review.md` в документе остались висячие ссылки на него —
  spec §3.6 актуализирован до полной вычистки; шаг 7 переформулирован как
  доводка (подпункт 7.1 — три замены фиксированного текста), Review Focus —
  п.9, статусная таблица обновлена.
