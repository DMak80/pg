# t31-s3-cluster-creds — план реализации

**Цель:** клиентские креды S3-кластеров (root-пара + access keys) переезжают из
per-install env-профилей воркеров в etcd per-cluster (ключ
`/s3/clusters/<C>/secrets`); вводится признак «бэкапный кластер etcd»
(`config.etcd_backup`) со спец-ключом из env; потребители (PgWorker, панель)
читают креды из etcd; env-профили `S3W_NODE_SECRETS__*` /
`PGW_BACKUP_S3_PROFILES__*` и поле `config.secret_profile` упраздняются.

**Spec:** `docs/superpowers/2026-10-19-t31-s3-cluster-creds/spec.md` (рядом;
план аргументирует от spec — исполнитель читает оба).

**Архитектура:** arch-first — контракт меняется сначала в `arch/` (Шаг 1),
затем код зеркалит. Пишет `/s3/clusters/<C>/secrets` только S3Worker
(ClusterSecretsStore, txn put-if-absent; ротация — RMW по mod_revision);
PgWorker и панель — читатели. Значения секретов не покидают доверенную зону
(env контейнеров, S3-клиенты) — в журнал/логи/API/UI только имена/признаки.

**Модель исполнения (двухуровневая).** Шаги исполняет СИЛЬНАЯ модель (по
умолчанию main-агент; при executor="coder" — субагент coder). Механическая
часть шага выделена ПОДПУНКТАМИ для СЛАБОЙ модели (субагент coder-flash, БЕЗ
контекста проекта): подпункт самодостаточен — слабая модель получает только его
текст. Ревью результатов подпунктов, отладка, документация, скрипты стендов —
сильная модель. Компиляция после каждого шага обязательна.

## Глобальные ограничения (из spec §5, действуют на все шаги)

- `.NET 10`, C# `LangVersion=latest`, `Nullable=enable`,
  **`TreatWarningsAsErrors=true`** — любой warning = ошибка сборки.
- Комментарии/документация — по-русски; идентификаторы — по-английски.
- Тесты — AAA (// Arrange / // Act / // Assert).
- Значения секретов — НИКОГДА в журнал `/s3worker/work/<C>`, логи, метрики,
  API-ответы, UI, stdout чеков; диагностики — только имена/признаки.
- Никакой backward-миграции: `config.secret_profile` игнорируется парсерами
  (unknownKeys-толерантность), кластеры пересоздаются, стенд пересеивается.
- etcd-контур всегда один; все txn/чтения — failover по endpoints (паттерн
  `S3ProcessCommon.GetWithFailoverAsync` / `TxnWithFailoverAsync`).
- Порты в тестах — только динамические; teardown полный при любом исходе;
  `BrokerBootSec ≤ 100 с`; таймауты ожиданий агента ≤ 30 с.
- Ветка: работа в worktree `feat-t31-s3-cluster-creds`; коммиты свободно.
- Roadmap-записи: `t31-s3-cluster-creds` и `t32-panel-bucket-etcd` уже в
  `arch/roadmap/s3.md` — ничего не заводить; удалятся мерж-коммитом задачи.

## Зафиксированные проектные решения (спека оставила исполнителю)

1. **Env спец-ключа бэкапа etcd**: `S3W_ETCD_BACKUP_ACCESSKEY` /
   `S3W_ETCD_BACKUP_SECRETKEY`; читает `ClusterSecretsStore` (инъекция getenv
   для тестов); полупара — fail-fast старта воркера (замена прежнего
   `ValidateEnvPairs` SecretProfileStore). Гвард полноты пары —
   **статический** метод `ClusterSecretsStore.ValidateEtcdBackupEnvPairs`:
   конструктор стора остаётся строгим `(IEtcdGateway, string[], getenv?)` по
   прецеденту `PkiStore` (транспорт обязателен; nullable-транспорт в
   конструкторе — запах), а fail-fast-пробе старта и гварду API экземпляр с
   транспортом не нужен.
2. **Модель secrets (S3Worker)**: `ClusterAccessKey(Name, AccessKey, SecretKey,
   Policy)` + `ClusterSecrets(RootUser, RootPassword, AccessKeys)` с проекцией
   `ToNodeSecrets() → OwnS3NodeSecrets`; имена ключей — константы
   `backups` (read-write) / `panel` (read-only); `root_user = s3w-<C>-root`
   (детерминирован, не секрет).
3. **Генерация значений**: собственный генератор в S3Worker.Core (32 симв
   `[A-Za-z0-9]`, `RandomNumberGenerator` — копия механики AppSecretGenerator;
   ссылка S3Worker → PgWorker.Core не вводится).
4. **`etcd_backup` в каноническом config**: поле пишется ТОЛЬКО при `true`
   (как `state` — опциональное); `secret_profile` из канона исчезает.
5. **Стор secrets — два режима**: `EnsureAsync` (provisioning: put-if-absent +
   подстановка спец-ключа) и `TryReadAsync` (надзор/G/F/AddNode/ротатор:
   null = ключа нет, битость/неполнота = Failed с диагностикой без значений);
   `RotateAsync` — генерация новых + txn по `mod_revision` (§3.4 spec: RMW;
   Active-кластер не пересекается с provisioning по состояниям).
6. **Пул грани «Хранилище бэкапов» панели**: первый по Ordinal Active-кластер
   `/s3/`-домена с непустым `endpoints` и валидным secrets-ключом (ключ
   `name=panel`); configured ⟺ такой кластер есть. Bucket остаётся env
   (`AdminPanel__Backups__S3__Bucket`; перенос в etcd — roadmap t32, вне).
7. **env панели**: `AdminPanel__Backups__S3__{AccessKey,SecretKey,Endpoint,
   Endpoints}` уходят (креды и цели — из снапшота S3-домена); остаётся Bucket.

## Фокус ревью (классы ошибок, которые spec не пиняет тестами явно)

- Значение секрета в журнале/логе/API/UI/stdout-выводе — ломает класс
  экспозиции. Тест-гварды: юниты store (диагностики без значений), E2E
  (Шаг 13: grep значения по журналам/логам/docker-inspect), чек 80 (вывод без
  значений), S3QueryHandlerTests (DTO без значений).
- Проигрыш txn put-if-absent (два воркера одновременно): победивший — истина;
  проигравший обязан re-read и вернуть ЧУЖОЙ материал. Тест: юнит
  ClusterSecretsStore с предзаписанным ключом.
- Полупара спец-ключа etcd-backup: fail-fast старта и 422 при создании
  кластера с признаком. Тесты: юнит ValidateEtcdBackupEnvPairs, API-тест
  создания (422 без env, 409 второй etcd-backup).
- Битый/неполный secrets-ключ у Active-кластера: процессы — Failed-диагностика
  (ретраи тиком), НЕ генерация новых кред молча (иначе живые инстансы с
  прежним env расходятся с ключом). Тест: юниты TryRead (битый JSON, нет
  ключа panel/backups, пустые значения).
- Ротация: env-сверка контейнера по `root_user`+составу
  `OWNS3_ACCESS_KEYS__<i>` не должна рассинхронизироваться с заменой secrets
  (проигрыш RMW → заявка не гасится, следующий тик повторяет). Тест:
  SecretRotatorTests (проигрыш txn, etcd-backup пере-подстановка).

---

## Шаг 1. Фаза 0 — arch-каноны (первая; без неё код не начинается)

**Вход:** spec одобрен; worktree чист; `arch/roadmap/s3.md` уже несёт t31/t32.

**Действие (сильная модель, без подпунктов — документация):** правки строго по
фазе 0 spec §4; порядок полей/примеров — по решениям выше (§«Зафиксированные
проектные решения»):

1. `arch/23-s3-clusters.md`:
   - §2 таблица: новая строка `secrets` (JSON формата из spec §3.1: root_user/
     root_password/access_keys[{name,access_key,secret_key,policy}]; пишет
     S3Worker лениво при provisioning txn put-if-absent, ротация — замена
     целиком txn по mod_revision; удаляется deprovisioning'ом X3 с префиксом
     кластера; читатели — S3Worker (env инстансов, root админ-грани),
     PgWorker (ключ `backups`), панель (ключ `panel`), etcdworker t32
     (`backups`)); из строки `config` — `secret_profile` убрать, добавить
     `etcd_backup` (bool, default false, immutable, единственность в
     `/s3/clusters/` — 409).
   - §2.1: примеры config переписать без `secret_profile`, с `etcd_backup`
     (пример etcd-backup кластера из spec §3.2); добавить канонический пример
     `secrets` (критерий приёмки парсеров; значения-заглушки).
   - §6 «Секреты»: подраздел «Креды клиентской грани» переписать: per-cluster
     в etcd, генерация воркером (root+rw+ro, 32 симв [A-Za-z0-9],
     `root_user=s3w-<C>-root`), спец-ключ etcd-backup из env
     `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}` (полупара — fail-fast; 422 при
     создании с признаком без полного спец-ключа), экспозиция — «парсеры
     панели разбирают для внутреннего S3-клиента, в UI/API не отдают»;
     env-профили `S3W_NODE_SECRETS__*` — упразднить (передача в инстансы
     остаётся env контейнера из secrets-ключа).
   - §7: PgWorker-читатель добавляет `secrets` (ключ `backups`, read-write).
   - §8: строка `rotations` — семантика «генерация новых root+ключей → замена
     secrets целиком (txn по mod_revision) → rolling-пересоздание по одной за
     тик → del заявки»; остальное без изменений.
2. `arch/24-s3worker.md`:
   - §1.1 валидация: `secret_profile`/422-перечень убрать; добавить
     `etcdBackup` (bool, опционально; гварды: единственность — 409, полнота
     спец-ключа env — 422 только для etcd-backup кластера).
   - §4 «Секреты»: перезаписать на per-cluster в etcd (источник —
     ClusterSecretsStore; Ensure/TryRead/Rotate); передача env инстансов — из
     secrets; спец-ключ; удаление прежнего абзаца env-профилей.
   - §5 A (P1/P2): P1 — «валидация профиля» → для etcd-backup кластера
     проверка спец-ключа env; P2 — «выпуск secrets (EnsureSecrets, txn
     put-if-absent) рядом с PKI; 0 инстансов — secrets генерируются (etcd,
     без docker-мутаций)»; §5 G — root-креды админ-грани из secrets.
   - §8: env-строка — `S3W_NODE_SECRETS__*` убрать, добавить
     `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}` (полупары — fail-fast).
   - §9: новая риск-строка R7 — DR-окно ротации спец-ключа (слепок etcd,
     снятый до ротации, несёт старое значение; восстановление даёт
     устаревший ключ до первой свежей записи; оперативное лечение — заявка
     ротации, runbook t32; механик DR в t31 нет).
3. `arch/19-backups.md` §7: пункт (б) — креды пулов читаются из
   `/s3/clusters/<C>/secrets` (ключ `backups`, read-write; отсутствие/битость
   — transient-отказ резолва, маркер `s3-pool-exhausted`); §9 — fail-fast
   «хотя бы один env-профиль» упраздняется (остаётся Bucket).
4. `arch/14-pgworker.md` §4: прецедент per-cluster секретов дополнить
   S3-кредами (класс экспозиции: панель разбирает для внутреннего S3-клиента,
   не отдаёт в UI/API).
5. `arch/adminpanel/02-etcd-contract.md` §12: 12.1 — `secrets` в модель
   (значения только внутреннему S3-клиенту панели, в API/UI не отдаются);
   12.2 мутация 1 — отказы: 422 «профиль секретов» уходит, добавляются 409
   (единственность etcd-backup) и 422 (спец-ключ env неполон — только для
   etcd-backup); 12.3 — валидация без `secretProfile`, с опцией etcdBackup;
   12.4 — креды/endpoint грани «Хранилище бэкапов» из снапшота S3-домена по
   правилу решения 6; env-креды панели упраздняются, Bucket остаётся (env).

**Выход:** arch-каноны описывают новый контракт; `grep -rn "secret_profile\|S3W_NODE_SECRETS" arch/` пуст (кроме git-истории).

**Проверка:** `grep -rn "secret_profile" /Users/demakaev/ZCodeProject/worktrees/feat-t31-s3-cluster-creds/arch/ | wc -l` → 0; все 5 файлов правлены; self-review текста против spec §3–§4.

**Связь со spec:** §4 фаза 0 (пп. 1–5); критерий приёмки 1.

Подпунктов нет (документация — сильная модель).

---

## Шаг 2. S3Worker: генератор + модель + ClusterSecretsStore (+юниты)

**Вход:** Шаг 1 слит (arch-каноны обновлены).

**Действие (сильная модель):** новые файлы по решениям 1–3, 5; ключ etcd —
`S3ProcessCommon.ClusterSecretsKey(cluster) => "/s3/clusters/{cluster}/secrets"`
(добавить рядом с PKI-ключами в `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`).
Сериализация — ручной `Utf8JsonWriter` (прецедент `PkiStore.SerializeCa`),
порядок полей канона: `root_user, root_password, access_keys[{name,
access_key, secret_key, policy}]` (ключи в порядке backups, panel). Парсинг —
`System.Text.Json` с валидацией полноты (root-пара непустая; присутствуют
ключи с именами `backups` policy=read-write и `panel` policy=read-only;
лишние ключи сохраняются как есть; битый JSON/не-объект → Failed). Никаких
значений в исключениях/диагностиках.

**Выход:** `ClusterSecretsStore` c `EnsureAsync` / `TryReadAsync` /
`RotateAsync` / статическим `ValidateEtcdBackupEnvPairs`; юниты зелёные.

**Проверка:**
```
cd /Users/demakaev/ZCodeProject/worktrees/feat-t31-s3-cluster-creds
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterSecretsStore|FullyQualifiedName~ClusterSecretGenerator"
```
Ожидание: PASS (AAA; кейсы — фокус ревью: put-if-absent проигрыш → re-read чужого, битый/неполный ключ, спец-ключ неполон, ротация: root_user сохранён/значения новые/RMW-проигрыш, диагностики без значений).

**Связь со spec:** §3.1 (ключ/формат/генерация/txn), §3.3 (ClusterSecretsStore
по образцу PkiStore, спец-ключ), §3.4 (RMW ротации), фаза 1.

Подпункты:

- **2.1. Создай файл `src/S3Worker.Core/OwnS3/ClusterSecretGenerator.cs`**
  (новый) с содержимым:
  ```csharp
  using System.Security.Cryptography;

  namespace S3Worker.Core.OwnS3;

  /// <summary>
  /// Генератор значений per-cluster клиентских кредов S3 (arch/23 §6):
  /// криптостойкий источник, 32 символа, алфавит [A-Za-z0-9] — без
  /// спецсимволов, чтобы значение было безопасно для env контейнера,
  /// JSON-значения etcd и SigV4 без экранирования.
  /// </summary>
  public static class ClusterSecretGenerator
  {
      public const int Length = 32;

      private const string Alphabet =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

      public static string Generate()
      {
          Span<char> chars = stackalloc char[Length];
          for (var i = 0; i < Length; i++)
              chars[i] = Alphabet[RandomNumberGenerator.GetInt32(Alphabet.Length)];
          return new string(chars);
      }
  }
  ```
- **2.2. Создай файл `src/S3Worker.Core/OwnS3/ClusterSecrets.cs`** (новый) с
  содержимым:
  ```csharp
  namespace S3Worker.Core.OwnS3;

  /// <summary>Имена статических access keys per-cluster secrets (arch/23 §6):
  /// backups — read-write (джобы/агенты бэкапов PgWorker), panel — read-only
  /// (грань «Хранилище бэкапов» панели).</summary>
  public static class ClusterAccessKeyNames
  {
      public const string Backups = "backups";
      public const string Panel = "panel";
  }

  /// <summary>Access key с именем (модель JSON secrets-ключа arch/23 §2.1).
  /// Name — не секрет (диагностика, сверка состава); Policy ∈
  /// read-write|read-only.</summary>
  public sealed record ClusterAccessKey(
      string Name, string AccessKey, string SecretKey, string Policy);

  /// <summary>Клиентские креды кластера — значение ключа
  /// /s3/clusters/&lt;C&gt;/secrets (arch/23 §2/§6): root-пара админ-грани +
  /// статические access keys. Значения передаются ТОЛЬКО в env контейнеров
  /// инстансов и в S3-клиенты процессов; в журнал/логи/API — никогда.</summary>
  public sealed record ClusterSecrets(
      string RootUser,
      string RootPassword,
      IReadOnlyList<ClusterAccessKey> AccessKeys)
  {
      /// <summary>Проекция в env-модель инстанса (OWNS3_*): имена ключей env
      /// не передаются — платформе ownS3 нужны только пары значений.</summary>
      public OwnS3NodeSecrets ToNodeSecrets()
          => new(RootUser, RootPassword,
              [.. AccessKeys.Select(k => new OwnS3AccessKey(k.AccessKey, k.SecretKey, k.Policy))]);
  }
  ```
- **2.3. Создай файл `src/S3Worker.Provisioning/ClusterSecretsStore.cs`**
  (новый), namespace `S3Worker.Provisioning`. Класс
  `public sealed class ClusterSecretsStore(IEtcdGateway gateway, string[] endpoints, Func<string?, string?>? getenv = null)`
  — ОБА первых параметра обязательны (прецедент `PkiStore`: транспорт нужен
  Ensure/TryRead/Rotate); `getenv` по умолчанию читает
  `Environment.GetEnvironmentVariable` (поле `_getenv` — прецедент
  `SecretProfileStore` в этом же каталоге). Методы:
  - `internal static string Serialize(ClusterSecrets secrets)` — ручной
    `Utf8JsonWriter` (образец `PkiStore.SerializeCa` из
    `src/S3Worker.Provisioning/PkiStore.cs`), поля в порядке `root_user`,
    `root_password`, затем массив `access_keys` объектов с полями `name`,
    `access_key`, `secret_key`, `policy` в порядке следования списка.
  - `internal static ClusterSecrets? TryParse(string json)` — null при битом
    JSON/не-объекте; валидация полноты: `root_user` и `root_password`
    непустые строки; в `access_keys` (массив объектов с непустыми
    `name`/`access_key`/`secret_key`/`policy`) есть элемент с
    `name=="backups"` и `policy=="read-write"` и элемент с
    `name=="panel"` и `policy=="read-only"`; лишние элементы сохраняются.
  - `public async Task<Result<ClusterSecrets>> EnsureAsync(string cluster, bool etcdBackup, CancellationToken ct)`:
    1) прочитать ключ (failover-чтение паттерном `S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, key, ct)`, где `key = S3ProcessCommon.ClusterSecretsKey(cluster)` — добавлен в `S3ProcessCommon` этим шагом); transport-ошибка → `Result<ClusterSecrets>.Failed(err)`;
    2) ключ есть: `TryParse` вернул модель → Success(модель) (идемпотентно); null → `Failed(new ApplicationException($"secrets {key}: corrupt/incomplete material"))`;
    3) ключа нет → `Generate(cluster, etcdBackup)` (ниже); если Failed — вернуть;
    4) txn `TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, Serialize(generated), null)])` с failover по endpoints (метод-образец `PkiStore.TxnWithFailoverAsync` — скопируй приватный хелпер); txn.IsSuccess false → Failed; `Succeeded==false` (проигрыш гонки) → повторить шаг 1 один раз: прочитанное валидно → Success(прочитанное), иначе Failed с диагностикой `"lost txn put-if-absent race, material is missing/broken"`.
  - `public async Task<Result<ClusterSecrets?>> TryReadAsync(string cluster, CancellationToken ct)`: failover-чтение; ключа нет → `Success(null)`; есть → TryParse: модель → Success(модель), null → `Failed(new ApplicationException($"secrets {key}: corrupt/incomplete material"))`. Значений в сообщениях НЕТ.
  - `public async Task<Result<ClusterSecrets>> RotateAsync(string cluster, bool etcdBackup, CancellationToken ct)`: 1) failover-чтение kv; ключа нет → `Failed(new ApplicationException($"secrets {key}: not found"))`; 2) TryParse → null → Failed(битость); 3) новые значения: `RootUser` — прежний; `RootPassword`, все `access_key`/`secret_key` — `ClusterSecretGenerator.Generate()`; при `etcdBackup==true` элемент с `name=="backups"` получает `AccessKey=_getenv("S3W_ETCD_BACKUP_ACCESSKEY")`, `SecretKey=_getenv("S3W_ETCD_BACKUP_SECRETKEY")` — если любая из них null/пустая → `Failed(new ApplicationException("etcd-backup cluster: S3W_ETCD_BACKUP_ACCESSKEY/SECRETKEY env pair is incomplete"))`; порядок ключей — `backups`, затем `panel` (лишние прежние имена НЕ переносятся — замена каноническим составом); 4) txn `TxnRequest.Of([TxnCompare.ModRevisionEqual(key, (long)kv.ModRevision)], [new TxnOp.Put(key, Serialize(fresh), null)])` failover; проигрыш compare → `Failed(new ApplicationException($"secrets {key}: changed underneath us — retried next tick"))`; успех → Success(fresh).
  - `private Result<ClusterSecrets> Generate(string cluster, bool etcdBackup)`: `rootUser = $"s3w-{cluster}-root"`; `rootPassword` и обе пары ключей — `ClusterSecretGenerator.Generate()`; `backups` policy `"read-write"`, `panel` policy `"read-only"`; при `etcdBackup==true` — пара `backups` из env `S3W_ETCD_BACKUP_ACCESSKEY`/`S3W_ETCD_BACKUP_SECRETKEY`, неполная пара → Failed с диагностикой как выше.
  - `public static IReadOnlyList<string> ValidateEtcdBackupEnvPairs(Func<string?, string?>? getenv = null)` —
    СТАТИЧЕСКИЙ (без экземпляра и транспорта: гвард нужен fail-fast старта и
    API-хендлеру ДО создания рабочего стора; getenv по умолчанию —
    `Environment.GetEnvironmentVariable`): вернуть `["S3W_ETCD_BACKUP"]`,
    если задана ровно одна из пары (null/пустая считается незаданной); иначе
    пустой список.
  - Диагностические строки — только имена переменных/ключей, никогда значения.
- **2.4. В файл `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`**
  после метода `NodePkiKey` добавь метод:
  ```csharp
  /// <summary>Клиентские креды кластера (arch/23 §2/§6): ленивая генерация
  /// при provisioning (txn put-if-absent), ротация — замена целиком; читатели
  /// — S3Worker/PgWorker/панель/etcdworker (t32).</summary>
  public static string ClusterSecretsKey(string cluster)
      => $"/s3/clusters/{cluster}/secrets";
  ```
- **2.5. Создай файл `src/tests/S3Worker.UnitTests/Provisioning/ClusterSecretsStoreTests.cs`**
  (новый), namespace `S3Worker.UnitTests.Provisioning`. Юниты по образцу
  `PkiStoreTests.cs` (тот же каталог: фейк `FakeEtcd` из
  `../Fakes/Fakes.cs`, `Result` из `Shared.Core`), стиль AAA с комментариями
  `// Arrange`, `// Act`, `// Assert`. xUnit-факты (имена свободные, каждое —
  один сценарий):
  1) `Serialize`→`TryParse` round-trip: модель с ключами backups(rw)+panel(ro) сериализуется и разбирается в эквивалентную модель (сравнение по значениям полей).
  2) TryParse битого JSON (`"not json"`) → null; неполного (нет ключа `panel`; пустой `root_password`; policy не read-write/read-only) → null.
  3) `EnsureAsync` на пустом FakeEtcd → Success; в etcd появился ключ `/s3/clusters/c1/secrets`; RootUser=="s3w-c1-root"; все значения — 32 симв `[A-Za-z0-9]`; пары ключей backups/panel с policy read-write/read-only; повторный `EnsureAsync` возвращает РАВНЫЕ значения (идемпотентность).
  4) `EnsureAsync` при предзаписанном валидном ключе → возвращает предзаписанные значения (не перегенерирует).
  5) `EnsureAsync(etcdBackup: true)` без env-пары (конструктор с getenv-лямбдой, возвращающей null) → Failed, сообщение содержит "S3W_ETCD_BACKUP" и НЕ содержит значений; с полной парой (getenv-словарь) → ключ `backups` равен env-значениям, `panel`/root сгенерированы.
  6) `TryReadAsync`: ключа нет → Success(null); битый → Failed; валидный → Success(модель).
  7) `RotateAsync`: по записанному ключу → Success; RootUser прежний, RootPassword и значения ключей НОВЫЕ (не равны старым); в etcd новое значение; txn-подмена между чтением и записью не эмулируется — вместо неё проверь, что ModRevision-сравнение используется (FakeEtcd обновляет ModRevision при Put — вторая RotateAsync после внешней перезаписи значения в FakeEtcd отрабатывает по свежему kv).
  8) `RotateAsync` с `etcdBackup: true` и env-парой: `backups` == env-пара (пере-подстановка), остальные значения новые.
  9) `ClusterSecretsStore.ValidateEtcdBackupEnvPairs(getenv)` (СТАТИЧЕСКИЙ
     вызов; getenv — лямбда `name => dict.GetValueOrDefault(name)`, dict —
     `Dictionary<string?, string?>`): обе переменные отсутствуют → пустой
     список; обе заданы → пустой; задана одна → `["S3W_ETCD_BACKUP"]`.

---

## Шаг 3. S3Worker: config `etcd_backup` — модель, канон, парсер

**Вход:** Шаг 2 принят.

**Действие (сильная модель):** поле `secret_profile` уходит из всей модели и
канона S3Worker; вводится `EtcdBackup` (bool?, null = false); чтение
`secret_profile` парсером прекращается (игнор по unknownKeys-толерантности —
поле просто не читается). Канонический config: `SerializeCanonicalConfig`
меняет сигнатуру, поле `etcd_backup` пишется только при `true`. Ключ `secrets`
— известный лист парсера (значение в снапшот тика не разбирается — читает
ClusterSecretsStore отдельным GET; прецедент `pki`).

**Выход:** `S3ClusterConfig` без `SecretProfile`, с `EtcdBackup`;
`S3SnapshotParser` понимает `etcd_backup` и молча пропускает `secrets`;
`ConvergeTopologyAsync`/`CanonicalConfig` на новой сигнатуре; юниты зелёные.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~S3SnapshotParser|FullyQualifiedName~S3ProcessCommon"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
```
Ожидание: PASS + сборка без ошибок (потребители сигнатуры чинятся в этом шаге по списку компилятора — ProvisioningProcess/MeshGovernance/AddNode/RemoveNode/Docker-драйверы: `snap.Config!.SecretProfile` заменяется на `snap.Config!.EtcdBackup ?? false` в местах, где значение реально используется; до Шага 5 это только компилируемость).

**Связь со spec:** §3.2 (признак), §3.3 (snapshot-парсер: etcd_backup — в
модель, secret_profile — игнор).

Подпункты:

- **3.1. В файле `src/S3Worker.Core/Model/S3Domain.cs`** замени запись
  `S3ClusterConfig(string? Platform, int? Nodes, string? SecretProfile,
  long? CreatedUnix, string? State)` на
  `S3ClusterConfig(string? Platform, int? Nodes, bool? EtcdBackup, long? CreatedUnix, string? State)`
  и обнови её doc-комментарий: `EtcdBackup` — признак «бэкапный кластер etcd»
  (arch/23 §2: bool, default false, immutable, единственный в установке).
- **3.2. В файле `src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs`** в методе
  `ParseConfig` замени `ReadString(root, "secret_profile")` на чтение
  булева поля: `root.TryGetProperty("etcd_backup", out var eb) &&
  eb.ValueKind == JsonValueKind.True` → `true`, иначе `null` (отсутствие =
  false; не-булево значение — как false, тик не падает). В `switch`
  разбора ключей кластера добавь ветку пропуска secrets-ключа по образцу
  ветки `pki`:
  ```csharp
  // Клиентские креды кластера (arch/23 §2/§6): ключ известный (не
  // unknownKeys), значение в снапшот не разбирается — ClusterSecretsStore
  // читает его отдельным GET по месту (как pki-материал).
  case "secrets" when segments.Length == 5:
      break;
  ```
- **3.3. В файле `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`**
  замени метод `SerializeCanonicalConfig(string platform, int nodes,
  string? secretProfile, long? createdUnix, string? state = null)` на
  `SerializeCanonicalConfig(string platform, int nodes, bool etcdBackup,
  long? createdUnix, string? state = null)`: после `writer.WriteNumber(
  "nodes", nodes)` добавь
  ```csharp
  // etcd_backup пишется только при true (опциональное поле канона arch/23 §2.1).
  if (etcdBackup)
      writer.WriteBoolean("etcd_backup", true);
  ```
  Строку `writer.WriteString("secret_profile", secretProfile)` удали. В
  методе `ConvergeTopologyAsync` замени параметр `string? secretProfile` на
  `bool etcdBackup` и передавай его в `SerializeCanonicalConfig` (два вызова).
- **3.4. В файле
  `src/S3Worker.Provisioning/Processes/ProvisioningProcess.cs`** замени
  приватную запись `CanonicalConfig` (в конце файла) на
  ```csharp
  private sealed record CanonicalConfig(
      [property: JsonPropertyName("platform")] string Platform,
      [property: JsonPropertyName("nodes")] int Nodes,
      [property: JsonPropertyName("etcd_backup")] bool? EtcdBackup,
      [property: JsonPropertyName("created_unix")] long? CreatedUnix);
  ```
  и в `CommitConfigAsync` конструируй её как `new CanonicalConfig(
  snap.Config!.Platform!, snap.Config.Nodes!.Value, snap.Config.EtcdBackup
  ?? false ? true : null, snap.Config.CreatedUnix)` (null-поле не пишется —
  `CanonicalJson` уже `WhenWritingNull`). Тело ветки P1 с валидацией профиля
  (`if (!secrets.TryGet(...)`) пока НЕ трогай — Шаг 5.
- **3.5. Обнови юниты** `src/tests/S3Worker.UnitTests/Etcd/S3SnapshotParserTests.cs`
  и `src/tests/S3Worker.UnitTests/Provisioning/S3ProcessCommonTests.cs`
  (существующие): все вхождения `"secret_profile":"backups-main"` в тестовых
  JSON config удали; добавь по одному AAA-факту: config с
  `"etcd_backup":true` разбирается в `EtcdBackup==true`; config без поля —
  `EtcdBackup==null`; `SerializeCanonicalConfig(..., etcdBackup: true, ...)`
  содержит подстроку `"etcd_backup":true`, при `false` — не содержит и не
  содержит `secret_profile`; ключ `/s3/clusters/c1/secrets` в kvs НЕ
  попадает в unknownKeys и не ломает разбор.

---

## Шаг 4. S3Worker: API создания — etcdBackup + гварды 409/422

**Вход:** Шаг 3 принят; `CreateS3ClusterRequest` ещё несёт `secretProfile`.

**Действие (сильная модель):** DTO/валидатор/хендлер: поле `secretProfile`
уходит, добавляется опциональное `etcdBackup` (bool); гварды: единственность
etcd_backup в `/s3/clusters/` (чтение существующих config'ов перед txn —
range по префиксу; txn закрывает только коллизию ИМЕНИ кластера, TOCTOU
между проверкой единственности и txn остаётся — spec §3.2 гонку принимает:
мутации установки идут через одну панель/оператора; рассинхрон признака
виден пост-фактум в config'ах) → 409; полнота спец-ключа env для etcd-backup
→ 422 (статический гвард, без экземпляра store). Исключение 422-профиля
`S3SecretProfileUnavailableException` удаляется вместе с маппингами в
`ApiModule.cs` (2 места). Интеграционные тесты ClusterMutationsApiTests —
в Шаге 12.

**Выход:** создание кластера без профиля; 409/422-семантика по §3.2/§3.3;
сборка зелёная.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~S3ClusterValidator"
```
Ожидание: сборка PASS; валидатор без secretProfile-кейса, с etcdBackup-кейсами.

**Связь со spec:** §3.2 (API создания: etcdBackup, 409/422), §3.3 (CreateClusterHandler: secretProfile уходит).

Подпункты:

- **4.1. В файле `src/S3Worker.App/Api/Operations/S3Api.cs`**:
  1) в записи `CreateS3ClusterRequest` замени параметр
     `[property: JsonPropertyName("secretProfile")] string? SecretProfile` на
     `[property: JsonPropertyName("etcdBackup")] bool? EtcdBackup`;
  2) удали класс `S3SecretProfileUnavailableException` (строки ~34–39);
  3) добавь рядом с прочими исключениями:
  ```csharp
  // Второй etcd-backup кластер установки — 409 (arch/23 §2: единственность).
  public sealed class S3EtcdBackupConflictException(string existing)
      : Exception($"etcd_backup cluster already exists: {existing} (at most one per installation)");

  // Спец-ключ бэкапа etcd неполон в env воркера — 422 (только для etcd-backup).
  public sealed class S3EtcdBackupKeyIncompleteException()
      : Exception("etcdBackup cluster requires a complete S3W_ETCD_BACKUP_ACCESSKEY/SECRETKEY env pair of the worker");
  ```
- **4.2. В файле `src/S3Worker.App/Api/Operations/CreateClusterHandler.cs`**:
  1) УДАЛИ параметр `SecretProfileStore secrets` из конструктора (гвард
     полноты спец-ключа — СТАТИЧЕСКИЙ метод
     `ClusterSecretsStore.ValidateEtcdBackupEnvPairs()`, экземпляр стора
     хендлеру не нужен; DI-регистрацию поправит Шаг 5);
  2) удали блок «2) Профиль секретов…» (`if (!secrets.ProfileNames.Contains(
     profile…S3SecretProfileUnavailableException)`), вместо него после
     валидации вставь:
  ```csharp
  // 2) Гварды etcdBackup (arch/24 §1.1): спец-ключ env полон (422) и
  //    единственность etcd-backup кластера установки (409). Единственность —
  //    чтение существующих config'ов перед клэйм-txn: txn закрывает только
  //    коллизию имени кластера; TOCTOU между чтением и txn принимается
  //    (мутации установки — одна панель/оператор, рассинхрон признака виден
  //    пост-фактум в config'ах).
  var etcdBackup = request.EtcdBackup ?? false;
  if (etcdBackup && ClusterSecretsStore.ValidateEtcdBackupEnvPairs().Count > 0)
      return Result<S3ClusterCreatedDto>.Failed(new S3EtcdBackupKeyIncompleteException());
  if (etcdBackup)
  {
      var existing = await S3ApiHelpers.ListEtcdBackupClustersAsync(gateway, endpoints, ct);
      if (!existing.IsSuccess)
          return Result<S3ClusterCreatedDto>.Failed(existing.Error!);
      if (existing.Value is { } taken)
          return Result<S3ClusterCreatedDto>.Failed(new S3EtcdBackupConflictException(taken));
  }
  ```
  (добавь `using S3Worker.Provisioning;`);
  3) `configValue` собирай как `S3ProcessCommon.SerializeCanonicalConfig(
     S3Limits.Platform, instances.Count, etcdBackup, created, "NOT_INITIALIZED")`;
  4) в ответе `S3ClusterCreatedDto` замени поле `SecretProfile` на
     `[property: JsonPropertyName("etcdBackup")] bool EtcdBackup` и передавай
     `etcdBackup`;
  5) в `S3ClusterValidator.Validate` удали блок
     `if (string.IsNullOrWhiteSpace(request.SecretProfile)) errors.Add(new(
     "secretProfile", …))`.
- **4.3. В файле `src/S3Worker.App/Api/Operations/S3Api.cs`** найди класс
  `S3ApiHelpers` (строка ~196; методы `ConfigKey`/`ListNodeNamesAsync`/
  `CallAsync`/`GetConfigAsync`/`LinksKey`) и добавь в него метод (по образцу
  `ListNodeNamesAsync`: failover-range по префиксу `/s3/clusters/`, разбор
  JSON config'ов):
  ```csharp
  /// <summary>Имя существующего etcd_backup-кластера (arch/23 §2:
  /// единственность); null — такого нет. Битые config'ы пропускаются.</summary>
  public static async Task<Result<string?>> ListEtcdBackupClustersAsync(
      IEtcdGateway gateway, string[] endpoints, CancellationToken ct)
  ```
  Реализация: `CallAsync(endpoints, endpoint => gateway.RangeAsync(endpoint,
  "/s3/clusters/", ct))` (паттерн соседних методов; префикс — константа),
  затем по каждому kv с ключом вида `/s3/clusters/<C>/config` (5 сегментов,
  последний `config`) парсить JSON: поле `etcd_backup` == true → вернуть
  `<C>`. Первый найденный; ничего — `Success(null)`. `JsonException` —
  пропустить ключ.
- **4.4. В файле `src/S3Worker.App/Api/ApiModule.cs`** удали оба блока
  маппинга `S3SecretProfileUnavailableException` (строки ~36 и ~330) и
  добавь рядом с прочими `Results.Problem`-маппингами (статусы и формат —
  скопируй у соседних: `S3ClusterAlreadyExistsException` → 409,
  валидационные → 422):
  - `S3EtcdBackupConflictException` → statusCode 409;
  - `S3EtcdBackupKeyIncompleteException` → statusCode 422.
- **4.5. В файле `src/S3Worker.App/Api/Operations/SeedDemoHandler.cs`**
  замени аргумент `"backups-main"` в `new CreateS3ClusterRequest("demo",
  S3Limits.Platform, "backups-main", …)` на `false` (третий параметр теперь
  `bool? EtcdBackup`), итого `new CreateS3ClusterRequest("demo",
  S3Limits.Platform, false, …)`; комментарий над классом поправь: кластер без
  профиля секретов (креды — per-cluster в etcd).
- **4.6. Обнови юниты валидатора**
  `src/tests/S3Worker.UnitTests/Api/` (файл с тестами `S3ClusterValidator`,
  найти по `grep -rl "S3ClusterValidator" src/tests/S3Worker.UnitTests`):
  удали факты про обязательность `SecretProfile`; добавь два AAA-факта:
  запрос с `EtcdBackup=true` и валидными полями — 0 ошибок; `EtcdBackup`
  null — 0 ошибок (поле опционально). Тип запроса после 4.1:
  `new CreateS3ClusterRequest(name, "owns3", true, instances, resources)`.

---

## Шаг 5. S3Worker: процессы на ClusterSecretsStore + Program.cs

**Вход:** Шаги 2–4 приняты; `SecretProfileStore` ещё существует (удаляется в
Шаге 6 — чтобы diff этого шага не смешивался с удалением).

**Действие (сильная модель):** замена `SecretProfileStore` →
`ClusterSecretsStore` во всех процессах и DI; ProvisioningProcess: P1-блок
валидации профиля удаляется, в RunAsync после `PlanAsync` и до
`EnsureNodesAsync` — `EnsureSecrets` (в том числе для 0-инстансовых
кластеров; отказ → Fail "ensure-secrets"); EnsureNodes/EnsureBuckets берут
`OwnS3NodeSecrets` из secrets (`ToNodeSecrets()`); надзор/G/F/AddNode —
`TryReadAsync` (отсутствие/битость → journal-отказ с диагностикой без
значений, ретраи тиком); ротатор — `TryReadAsync` + `RotateAsync` (генерация
и RMW в store; спец-ключ пере-подставляется store'ом из env). Program.cs:
регистрация `ClusterSecretsStore` (gateway+endpoints+getenv) вместо
`SecretProfileStore` во всех точках процессов; fail-fast полупар
`S3W_ETCD_BACKUP_*` — СТАТИЧЕСКИЙ гвард (без probe-экземпляра; конструктор
стора требует транспорт — решение 1). Модель диагностики: прежние фазы
`invalid-secret-profile` переименовываются в `invalid-secrets` (ротатор) /
Fail-фазу `"ensure-secrets"` (provisioning) — проверить OnJournalPhase-обработки
(`Program.cs` `m.OnJournalPhase`) на старые имена фаз и переименовать
синхронно.

**Выход:** все процессы S3Worker работают на per-cluster secrets; сборка и
юниты процессов зелёные.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~Provisioning|FullyQualifiedName~SecretRotator|FullyQualifiedName~NodeSupervisor|FullyQualifiedName~MeshGovernance|FullyQualifiedName~SyncProbe"
```
Ожидание: PASS (обновлённые тесты из подпунктов; фейк store в тестах —
реальный `ClusterSecretsStore` над `FakeEtcd` с предзаписанным ключом
`/s3/clusters/<C>/secrets` — хелпер-фабрика в тестах).

**Связь со spec:** §3.3 (ProvisioningProcess P1/P2, надзор/ротатор/
AddNode/RemoveNode, админ-грани из secrets), §3.4 (конкурентность), §2 п. 2–3
(etcd — источник правды; пишет только S3Worker).

Подпункты:

- **5.1. В файле `src/S3Worker.Provisioning/Processes/ProvisioningProcess.cs`:**
  1) в списке параметров первичного конструктора замени `SecretProfileStore
     secrets` на `ClusterSecretsStore secrets`;
  2) удали блок после `PlanAsync`:
     ```csharp
     // Профиль секретов: неполный/отсутствующий — permanent-отказ P1 …
     if (!secrets.TryGet(snap.Config!.SecretProfile!, out var nodeSecrets, out var secretDiagnostic))
         return Fail(cluster, new ApplicationException(
             $"secret profile {snap.Config.SecretProfile}: {secretDiagnostic}"), "invalid-secret-profile");
     ```
     и вставь на его место:
     ```csharp
     // P2-вход: выпуск per-cluster secrets (arch/23 §6): txn put-if-absent,
     // спец-ключ etcd-backup из env; выполняется и для 0-инстансовых
     // кластеров (etcd-операция, docker-мутаций нет). Битость/неполнота
     // существующего ключа — permanent-отказ с диагностикой без значений.
     var ensuredSecrets = await secrets.EnsureAsync(
         cluster, snap.Config?.EtcdBackup ?? false, ct);
     if (!ensuredSecrets.IsSuccess)
         return Fail(cluster, ensuredSecrets.Error!, "ensure-secrets");
     var nodeSecrets = ensuredSecrets.Value.ToNodeSecrets();
     ```
  3) тип параметра `nodeSecrets` в `EnsureNodesAsync`/`EnsureBucketsAsync`
     не меняется (`OwnS3NodeSecrets`) — источник теперь store.
- **5.2. В файлах `NodeSupervisor.cs`, `MeshGovernanceProcess.cs`,
  `AddNodeProcess.cs`, `SyncProbeProcess.cs` (каталог
  `src/S3Worker.Provisioning/Processes/`) в каждом:**
  1) параметр конструктора `SecretProfileStore secrets` →
     `ClusterSecretsStore secrets`;
  2) замени блок
     ```csharp
     if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var nodeSecrets, out var secretDiagnostic))
     ```
     (тело ветки — оставить как есть: journal-отказ/фейл по месту) на
     ```csharp
     var secretsRead = await secrets.TryReadAsync(snap.Cluster, ct);
     if (!secretsRead.IsSuccess)
         return <прежнее тело ветки отказа с diagnostic := secretsRead.Error!.Message>;
     if (secretsRead.Value is null)
         return <прежнее тело ветки отказа с diagnostic :=
             $"secrets /s3/clusters/{snap.Cluster}/secrets not found">;
     var nodeSecrets = secretsRead.Value.ToNodeSecrets();
     ```
     где `<прежнее тело ветки отказа>` — существующий в этом файле код ветки
     (journal WritePhaseAsync «invalid-secret-profile»/фейл): имя фазы в нём
     замени на `"invalid-secrets"`. Если файл использует creds под именем
     `creds` — аналогично (`SyncProbeProcess`).
- **5.3. В файле `src/S3Worker.Provisioning/Processes/SecretRotator.cs`:**
  1) `SecretProfileStore secrets` → `ClusterSecretsStore secrets`;
  2) блок чтения профиля (после EnsureClaimed) замени на:
     ```csharp
     // Secrets кластера (arch/23 §6): отсутствие/битость — journal-отказ,
     // заявка жива (ретраи тиком; значения в диагностики не попадают).
     var secretsRead = await secrets.TryReadAsync(cluster, ct);
     if (!secretsRead.IsSuccess || secretsRead.Value is null)
     {
         var reject = await journal.WritePhaseAsync(
             cluster, Op, "invalid-secrets", claims.InstanceId,
             secretsRead.Error?.Message
                 ?? $"secrets /s3/clusters/{cluster}/secrets not found", ct);
         return reject.IsSuccess ? Result.Success() : reject;
     }
     ```
  3) перед циклом по нодам (после journal "started") добавь замену secrets:
     ```csharp
     // Генерация новых root+ключей и атомарная замена secrets-ключа целиком
     // (txn по mod_revision; etcd-backup: спец-ключ пере-подставляется из
     // env). Проигрыш RMW — отложенный тик (диагностика, заявка жива).
     var rotated = await secrets.RotateAsync(
         cluster, snap.Config?.EtcdBackup ?? false, ct);
     if (!rotated.IsSuccess)
         return await journal.WritePhaseAsync(
             cluster, Op, "waiting-secrets", claims.InstanceId,
             rotated.Error!.Message, ct);
     var nodeSecrets = rotated.Value.ToNodeSecrets();
     ```
     прежний `nodeSecrets` из профиля удали; переменная `nodeSecrets`
     используется ниже в `NodeEnvMatchesAsync`/`envComposer.Compose` как
     раньше. ВАЖНО: замена secrets происходит ДО пересоздания первой ноды;
     env-сверка `NodeEnvMatchesAsync` не меняется (root_user постоянен,
     состав OWNS3_ACCESS_KEYS__<i> неизменен — сверка отрабатывает по обеим
     сторонам замены).
- **5.4. В файле `src/S3Worker.App/Program.cs`:**
  1) замени блок (строки ~73–78)
     ```csharp
     // env-профили секретов нод (arch/23 §6): читатель + fail-fast полупар …
     var secretStore = new S3Worker.Provisioning.SecretProfileStore();
     foreach (var broken in secretStore.ValidateEnvPairs())
         throw new ApplicationException($"S3W_NODE_SECRETS__{broken}: incomplete secret pair");
     builder.Services.AddSingleton(secretStore);
     ```
     на
     ```csharp
     // Клиентские креды кластеров — per-cluster в etcd (arch/23 §6):
     // ClusterSecretsStore. Fail-fast полупары спец-ключа бэкапа etcd
     // (arch/24 §8) — СТАТИЧЕСКИЙ гвард (транспорт стору не нужен; задана
     // одна из S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY} — воркер с обрезанным
     // env не стартует).
     foreach (var broken in S3Worker.Provisioning.ClusterSecretsStore.ValidateEtcdBackupEnvPairs())
         throw new ApplicationException(
             $"{broken}: incomplete etcd-backup key pair (S3W_ETCD_BACKUP_ACCESSKEY/SECRETKEY must be set together)");
     builder.Services.AddSingleton(sp => new S3Worker.Provisioning.ClusterSecretsStore(
         sp.GetRequiredService<Shared.Etcd.Client.IEtcdGateway>(),
         sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
     ```
     (probe-экземпляр НЕ создаётся: гвард статический; DI-фабрика создаёт
     рабочий экземпляр с gateway и endpoints — его получают процессы ниже);
  2) во всех точках `sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>()`
     (строки ~86, 234, 245, 259, 283, 304, 325 — кроме регистраций,
     удалённых в п.1) замени тип на
     `S3Worker.Provisioning.ClusterSecretsStore`. ВАЖНО: точка ~86 —
     регистрация `CreateClusterHandler` с параметром `SecretProfileStore` —
     после Шага 4 зависимость из хендлера удалена: убери аргумент из
     factory-лямбды (хендлер теперь `(IEtcdGateway, string[], TimeProvider)`).
- **5.5. Обнови юниты процессов** (`src/tests/S3Worker.UnitTests/Provisioning/`
  файлы `ProvisioningProcessTests.cs`, `NodeSupervisorTests.cs`,
  `MeshGovernanceTests.cs`, `AddNodeProcessTests.cs`, `SyncProbeTests.cs`,
  `SecretRotatorTests.cs`): там, где тесты конструировали
  `SecretProfileStore` с env-словарём, конструируй
  `new ClusterSecretsStore(fakeEtcd, ["http://x"], env => envDictionary.GetValueOrDefault(env))`
  и предзаписывай в `FakeEtcd` валидный ключ
  `/s3/clusters/<C>/secrets` (сериализация `ClusterSecretsStore.Serialize(
  new ClusterSecrets("s3w-<C>-root", "rootpwd32chars…", [new("backups","ak…","sk…","read-write"), new("panel","ak…","sk…","read-only")]))`;
  если `Serialize` internal — юниты того же assembly видят его при наличии
  `InternalsVisibleTo` — проверь `Directory.Build.props`; при отсутствии
  собери JSON строкой-литералом). Утверждения про фазу
  `invalid-secret-profile` переименуй в `invalid-secrets`; добавь по одному
  AAA-факту на кластер БЕЗ ключа secrets → фаза `invalid-secrets`/Failed
  (для ProvisioningProcessTests — два факта: успех Ensure на пустом etcd и
  `ensure-secrets` при битом предзаписанном ключе).

---

## Шаг 6. S3Worker: удаление SecretProfileStore и env-биндингов

**Вход:** Шаг 5 принят; `SecretProfileStore` больше никем не используется
(`grep -rn "SecretProfileStore" src/ | grep -v tests` пуст).

**Действие (сильная модель):** удалить `SecretProfileStore.cs`,
`SecretProfileStoreTests.cs`; вычистить остаточные упоминания в тестах
(в основном уже вычищены Шагом 5); в deploy-файлах этого шага НЕТ
compose-правки (Шаг 11) — только код.

**Выход:** `S3W_NODE_SECRETS` отсутствует в `src/`.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug
grep -rn "SecretProfileStore\|S3W_NODE_SECRETS" src/ | wc -l   # → 0
```

**Связь со spec:** §1 («упраздняются per-install env-профили S3W_NODE_SECRETS__*»), §3.3 (SecretProfileStore удаляется целиком), критерий 10.

Подпункты:

- **6.1. Удали файл `src/S3Worker.Provisioning/SecretProfileStore.cs`.**
- **6.2. Удали файл `src/tests/S3Worker.UnitTests/Provisioning/SecretProfileStoreTests.cs`.**
- **6.3. Скомпилируй решение и добейся нуля ошибок:**
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug`
  (ожидание: Build succeeded; 0 Error(s)). Все ссылки на удалённый тип
  (если остались в тестах) устраните точечно по списку компилятора: в
  тестовых фабриках заменяй на `ClusterSecretsStore` по образцу Шага 5.5.

---

## Шаг 7. PgWorker: пул бэкапов на secrets из etcd

**Вход:** Шаги 1–6 приняты (etcd-формат secrets стабилен).

**Действие (сильная модель):** `S3PoolStore` читает `/s3/clusters/<C>/secrets`
(ключ `name=backups`, read-write) в модель пула; `SecretProfile` уходит из
`S3Pool`/`S3PoolModel.ParseConfig`; `BackupPoolResolver` и
`BackupS3Factory.CreateForPoolAsync` берут креды из пула
(отсутствие/битость — transient-Failed `s3-pool: pool <C> has no secrets in
etcd`, РЕТРАИ тиком, не краш — arch/19 §4); `BackupSecretProfileStore`
удаляется; Program.cs: fail-fast «хотя бы один env-профиль» и полупары
упраздняются (`IsValid` остаётся только на Bucket). Передача в джобы/агенты —
без изменений (env контейнера; источник значений — резолвер).

**Выход:** PgWorker-пул бэкапов работает на per-cluster secrets; профилей
`PGW_BACKUP_S3_PROFILES__*` в коде нет; юниты зелёные.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~S3Pool|FullyQualifiedName~BackupPoolResolver|FullyQualifiedName~BackupS3Factory"
grep -rn "BackupSecretProfileStore\|PGW_BACKUP_S3_PROFILES" src/ | wc -l   # → 0
```

**Связь со spec:** §3.5 (S3PoolStore/Model, BackupPoolResolver, удаление
BackupSecretProfileStore и fail-fast), критерии 6, 10.

Подпункты:

- **7.1. В файле `src/PgWorker.Etcd/S3/S3PoolModel.cs`:**
  1) добавь тип (отдельной записью выше `S3Pool`) и дополни запись `S3Pool`:
  ```csharp
  /// <summary>Креды пула из /s3/clusters/&lt;C&gt;/secrets (ключ
  /// name=backups, read-write; arch/23 §6/§7): null — ключа нет/битый/
  /// нет ключа backups (резолвер даёт transient-диагностику, пул жив).</summary>
  public sealed record S3PoolSecrets(string AccessKey, string SecretKey);
  ```
  запись `S3Pool` примет вид `S3Pool(string Name, bool ToRemove,
  IReadOnlyList<S3PoolNode> Nodes, IReadOnlyList<string> Endpoints,
  S3PoolSecrets? Secrets = null)` — поле `SecretProfile` из записи УДАЛИТЬ;
  2) `ParseConfig`: запись `S3PoolConfig(string? SecretProfile, bool
     ToRemove)` → `S3PoolConfig(bool ToRemove)`; чтение `secret_profile`
     удалить;
  3) добавить парсер secrets:
  ```csharp
  /// <summary>secrets-JSON → креды ключа backups (arch/23 §2.1); битый
  /// JSON/нет ключа backups/пустые значения → null (пул читается, креды
  /// даёт transient-диагностика резолвера).</summary>
  public static S3PoolSecrets? ParseSecrets(string raw)
  ```
  Логика: JsonDocument.Parse (JsonException → null); root — объект;
  `access_keys` — массив; элемент с `name=="backups"`, непустые строковые
  `access_key`/`secret_key` → новая запись; несколько — первый; иначе null.
- **7.2. В файле `src/PgWorker.Etcd/S3/S3PoolStore.cs`** в `ReadAsync` после
  чтения `portAlloc` добавь чтение secrets-ключа:
  ```csharp
  var secretsKv = await RangeSingleAsync($"{ClustersPrefix}{pool}/secrets", ct);
  if (!secretsKv.IsSuccess)
      return Result<S3Pool?>.Failed(secretsKv.Error!);
  ```
  и в конструирование `S3Pool` передай `secretsKv.Value is null ? null :
  S3PoolModel.ParseSecrets(secretsKv.Value.Value)`; поле `cfg.SecretProfile`
  из конструктора `S3Pool` убери (см. 7.1).
- **7.3. В файле `src/PgWorker.Backups/BackupPoolResolver.cs`:**
  1) параметр конструктора `BackupSecretProfileStore secrets` удали (класс
     будет удалён Шагом 7.5; параметр `pools` остаётся);
  2) блок «Креды — env-профиль пула…» (строки ~53–60) замени на:
  ```csharp
  // Креды — per-cluster secrets из etcd (ключ backups, read-write;
  // arch/19 §7): отсутствие/битость — transient-диагностика (ретраи тиком,
  // маркер s3-pool-exhausted у процессов — НЕ краш тика), без значений.
  if (pool.Secrets is not { } poolSecrets)
      return Result<BackupPoolContext?>.Failed(new ApplicationException(
          $"s3-pool: pool {pool.Name} has no readable secrets in etcd (/s3/clusters/{pool.Name}/secrets, key backups)"));
  ```
  и в `BackupPoolContext(... pool.Name, accessKey!, secretKey!, ...)` подставь
  `poolSecrets.AccessKey, poolSecrets.SecretKey`.
- **7.4. В файле `src/PgWorker.Backups/BackupS3Factory.cs`:** параметр
  `BackupSecretProfileStore secrets` удали; в `CreateForPoolAsync` блок
  чтения профиля (строки ~73–79) замени на:
  ```csharp
  if (s3pool.Secrets is not { } poolSecrets)
      return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
          $"s3-pool: pool {pool} has no readable secrets in etcd (/s3/clusters/{pool}/secrets, key backups)"));
  ```
  и ниже используй `poolSecrets.AccessKey/poolSecrets.SecretKey` вместо
  `accessKey!/secretKey!`.
- **7.5. Удали файл `src/PgWorker.Backups/BackupSecretProfileStore.cs` и
  файл `src/tests/PgWorker.UnitTests/Backups/BackupSecretProfileStoreTests.cs`.**
- **7.6. В файле `src/PgWorker.App/Program.cs`:**
  1) удали `.Validate<BackupSecretProfileStore>(…)` (строки ~65–67);
  2) удали блок полупар (строки ~87–96: `var backupProfiles = new
     BackupSecretProfileStore(); … builder.Services.AddSingleton(
     backupProfiles);` целиком);
  3) в `src/PgWorker.App/Options.cs` найди метод `IsValid(int
     secretProfileCount)` секции Backups (строка ~560) и упрости до
     `IsValid()` без параметра (условие: Enabled → непустой
     `S3.Bucket`; комментарий обнови: профили упразднены, креды пулов —
     per-cluster в etcd); вызов в Program.cs поправь (`o.Backups.IsValid()`).
     Если секция `Profiles` есть в POCO Backups — удали её.
- **7.7. Обнови юниты PgWorker** (`src/tests/PgWorker.UnitTests/`):
  `Etcd/S3PoolModelTests.cs`, `Etcd/S3PoolStoreParseTests.cs`,
  `Backups/BackupPoolResolverTests.cs`, `Backups/TestPoolFactory.cs`,
  `App/UnitPoolFactory.cs`:
  1) фабрики пулов: убери аргумент SecretProfile, добавь при необходимости
     `Secrets: new("ak", "sk")`;
  2) `S3PoolModelTests`: замени факты ParseConfig про `secret_profile` на
     факты: config без `secret_profile` (поле игнорируется) → ToRemove-семантика
     прежняя; добавь AAA-факты ParseSecrets: валидный JSON с ключом backups →
     значения; нет ключа backups / битый JSON / пустые значения → null;
     лишние ключи не мешают;
  3) `BackupPoolResolverTests`: креды-кейсы перепривяжи к `pool.Secrets`:
     пул без Secrets → Failed с "no readable secrets" (транзиентность —
     прежние ассерты сообщения/маркера сохраняются); пул с Secrets → успех;
     кейсы env-профилей удали.
- **7.8. Скомпилируй и прогони:**
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug`
  (ожидание: Build succeeded); остаточные ссылки по списку компилятора чинить
  по образцу выше (FakeBackupDeps в IntegrationTests чинится Шагом 12).

---

## Шаг 8. Панель: парсер secrets + модель + API состава кредов

**Вход:** Шаги 1–7 приняты.

**Действие (сильная модель):** `S3Parser` разбирает `secrets`-ключ и
`etcd_backup` в модель; `S3ClusterInfo` получает `Secrets` (полная модель со
значениями — только для внутреннего S3-клиента) и `EtcdBackup` (bool);
`SecretProfile` из модели/API уходит; S3Query-грань отдаёт состав кредов
БЕЗ значений: `rootUser`, список `accessKeys[{name, policy}]`, признак
etcdBackup. Значения в DTO не отдаются НИКОГДА (AC8-прецедент).

**Выход:** API `/api/s3/clusters[/{c}]` несёт состав кредов без значений и
признак etcdBackup; парсерные юниты + фикстура зелёные.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~S3Parser|FullyQualifiedName~S3Query|FullyQualifiedName~S3PoolMapping"
```

**Связь со spec:** §3.6 (S3Parser, просмотр состава кредов), критерий 7.

Подпункты:

- **8.1. В файле `src/AdminPanel.Core/S3/S3Snapshot.cs`:**
  1) в запись `S3ClusterInfo` поле `string SecretProfile` замени на
     `bool EtcdBackup` (позицию в записи сохранить: 5-й параметр);
  2) добавь в САМЫЙ КОНЕЦ записи параметр
     `S3ClusterSecretsInfo? Secrets = null` — ПОСЛЕ опциональных
     `Connectivity/Sync/Rotation` (тогда позиционная передача прежних 12
     аргументов в `BuildCluster` не ломается, а Secrets передаётся
     именованным аргументом — см. 8.2);
  3) добавь типы:
  ```csharp
  // Креды кластера /s3/clusters/<C>/secrets (arch/23 §6): значения — ТОЛЬКО
  // внутреннему S3-клиенту панели (грань «Хранилище бэкапов»); в API/UI
  // отдаются исключительно root_user и имена/политики ключей (AC8).
  public sealed record S3ClusterSecretsInfo(
      string RootUser,
      IReadOnlyList<S3AccessKeyInfo> AccessKeys);

  public sealed record S3AccessKeyInfo(
      string Name, string AccessKey, string SecretKey, string Policy);
  ```
  4) обнови doc-комментарий записи: `SecretProfile` упразднён, креды —
     per-cluster в etcd.
- **8.2. В файле `src/AdminPanel.Etcd/Parsing/S3Parser.cs`:**
  1) `ClusterAcc`: добавь `public string? SecretsRaw;`; в switch разбора —
     ветки:
     ```csharp
     case "secrets" when segments.Length == 5:
         acc.SecretsRaw = kv.Value;
         break;
     ```
  2) `ParseConfig`: кортеж `(platform, nodes, profile, created, state)` →
     `(platform, nodes, etcdBackup, created, state)`; вместо чтения
     `secret_profile` — `root.TryGetProperty("etcd_backup", out var eb) &&
     eb.ValueKind == JsonValueKind.True`; дефолт false; doc-комментарий
     формата config обнови (`{"platform","nodes","etcd_backup"?,"created_unix","state"?}`).
  3) `BuildCluster`: передай `etcdBackup` позиционно (5-й аргумент) и secrets —
     ИМЕНОВАННЫМ аргументом в конце вызова (после `ParseSync(...)`):
     `secrets: ParseSecrets(acc.Name, acc.SecretsRaw, errors)`; добавь
     приватный парсер:
  ```csharp
  // secrets: {"root_user","root_password","access_keys":[{name,access_key,
  // secret_key,policy}…]} — значения только внутреннему S3-клиенту панели;
  // битый JSON → null + parseError (12.1), поля толерантно расширяются.
  private static S3ClusterSecretsInfo? ParseSecrets(
      string cluster, string? raw, List<KeyParseError> errors)
  ```
  Логика: raw null → null; JsonException/не-объект/нет непустых `root_user`
  → errors.Add(new KeyParseError($"/s3/clusters/{cluster}/secrets", "broken
  secrets JSON")) и null; `access_keys` — массив объектов со строковыми
  `name`/`access_key`/`secret_key`/`policy` (значения по умолчанию "" при
  отсутствии — ключ неполный, но парсер не падает); порядок сохраняется.
- **8.3. В файле `src/AdminPanel.Api/Inspection/S3Query.cs`** (и его
  DTO-записях в этом же файле): поле `SecretProfile` из DTO кластера
  (строки ~30, ~68, ~83, ~115) замени на:
  ```csharp
  bool EtcdBackup,
  string? RootUser,                          // s3w-<C>-root (имя — не секрет)
  IReadOnlyList<S3AccessKeyDto>? AccessKeys  // [{name, policy}] БЕЗ значений
  ```
  с записью DTO `public sealed record S3AccessKeyDto(string Name, string
  Policy);` (camelCase-JSON как соседи). Маппинг: `RootUser =
  c.Secrets?.RootUser`, `AccessKeys = c.Secrets is null ? null : [..
  c.Secrets.AccessKeys.Select(k => new S3AccessKeyDto(k.Name, k.Policy))]`.
  Значения `AccessKey`/`SecretKey` в DTO НЕ попадают.
- **8.4. Зеркала мутации создания (arch/02 §12.2-1 — панель передаёт тело как
  есть, DTO обязаны зеркалить воркера):**
  1) в файле `src/AdminPanel.Api/Operations/S3/S3Requests.cs` записи
     `CreateS3ClusterPanelRequest`: параметр `string? SecretProfile` (позиция
     2) УДАЛИ, а `bool? EtcdBackup = false` добавить ПОСЛЕДНИМ параметром —
     после `string? Platform = "owns3"` (optional-параметры в C# не могут
     стоять перед обязательными — CS1737); итоговая запись:
     `CreateS3ClusterPanelRequest(string? Name, IReadOnlyList<S3InstancePanel
     Request>? Instances, S3ResourcesPanelRequest? Resources, string?
     Platform = "owns3", bool? EtcdBackup = false)`;
  2) в файле `src/AdminPanel.Api/Operations/S3/S3Commands.cs` запись
     `S3ClusterCreatedPanelDto(string Name, string State, int Nodes, string
     SecretProfile, IReadOnlyList<string> Instances)` замени на
     `S3ClusterCreatedPanelDto(string Name, string State, int Nodes, bool
     EtcdBackup, IReadOnlyList<string> Instances)` (хендлер-прокси
     `CreateS3ClusterCommandHandler` не меняется — дженерик).
- **8.5. В файле `src/tests/AdminPanel.UnitTests/EtcdFixtures/S3/clusters-canonical.json`**
  во ВСЕХ config-ключах удали `"secret_profile":"backups-main",`; в config
  кластера `newclaim` добавь `"etcd_backup":true,` (между `nodes` и
  `created_unix`); добавь ключ
  `{ "key": "/s3/clusters/backupsmain/secrets", "value": "{\"root_user\":\"s3w-backupsmain-root\",\"root_password\":\"rootpwd\",\"access_keys\":[{\"name\":\"backups\",\"access_key\":\"ak1\",\"secret_key\":\"sk1\",\"policy\":\"read-write\"},{\"name\":\"panel\",\"access_key\":\"ak2\",\"secret_key\":\"sk2\",\"policy\":\"read-only\"}]}", "modRevision": 22 }`.
- **8.6. Обнови юниты** `src/tests/AdminPanel.UnitTests/S3ParserTests.cs`,
  `S3PoolMappingTests.cs`, `S3QueryHandlerTests.cs`: ассерты `SecretProfile`
  удали; добавь AAA-факты: secrets-ключ → `Secrets.RootUser=="s3w-backupsmain-root"`
  и `AccessKeys` из двух элементов с именами/политиками; config с
  `etcd_backup:true` → `EtcdBackup==true`; DTO кластера содержит `rootUser`
  и `accessKeys[].name/policy` и НЕ содержит значений (`access_key`/
  `secret_key`/`root_password` отсутствуют в сериализованном DTO).

---

## Шаг 9. Панель: грань «Хранилище бэкапов» на креды/endpoint из снапшота

**Вход:** Шаг 8 принят.

**Действие (сильная модель — тонкая механика, подпункты только на
механические части):** источник кредов/целей — снапшот S3-домена по правилу
решения 6 (первый по Ordinal Active-кластер с endpoints и валидным secrets
с ключом `panel`; read-only). Изменения:
- `BackupsS3Options`: `AccessKey`/`SecretKey`/`Endpoint`/`Endpoints`/
  `TargetList` — удалить; `IsConfigured` → вычисляется ОТДЕЛЬНО от опций
  (грань «включена» = найден пул в снапшоте); `EnsureValid` остаётся на
  Bucket/интервалы (только когда грань реально настроилась — валидация
  Bucket при заданном значении остаётся обязательной, пустой Bucket при
  живом пуле → диагностика грани, не крэш старта).
- Новый резолвер целей: чистая функция `S3Snapshot → S3StorageTargets?`
  (`S3StorageTargets(Cluster, Endpoints[] http-URI из endpoints-ключа пула,
  AccessKey/SecretKey ключа panel)`); null — грань выключена.
- `S3StorageClient`: вместо фиксации кредов/целей в конструкторе — метод
  `UpdateTargets(S3StorageTargets?)` (пересоздание per-цели клиентов при
  изменении состава/кредов; ротация кредов → следующий тик берёт свежие);
  вызовы гвардятся «цели не заданы» → `NotConfigured`.
- `S3InventoryLoop`: перед тиком — резолв целей из `IS3SnapshotReader`;
  целей нет → грань не настроена (лог по образцу выключенной, стор — маркер
  configured=false).
- `BackupObjectsQueryHandler`: гвард «цели не заданы» →
  `BackupsStorageNotConfiguredException` (503 прежней семантики).
- `Program.cs`: `s3.EnsureValid()` упразднить вместе с полями (Bucket
  проверяется валидацией при использовании/старте — пустой Bucket при
  поднятой грани — warning-лог).
- `MapStorage`/`BackupStorageDto`: `NotConfiguredReason` — новый текст («S3
  backups pool not found in /s3/clusters/ (no Active cluster with endpoints
  and secrets)»); `Endpoints` DTO — цели из снапшота (инвентарные, как
  прежде из TargetList).

**Выход:** грань работает без env-кредов; ротация кредов подхватывается
следующим тиком; юниты/интеграция панели зелёные.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Backups|FullyQualifiedName~S3Inventory|FullyQualifiedName~S3Storage"
grep -rn "AdminPanel__Backups__S3__AccessKey\|AdminPanel__Backups__S3__SecretKey\|Backups:S3:AccessKey" src/ | wc -l   # → 0
```

**Связь со spec:** §3.6 (грань «Хранилище бэкапов»: креды/endpoint из
снапшота; env-параметры уходят, Bucket остаётся), критерии 7, 10.

Подпункты:

- **9.1. В файле `src/AdminPanel.Probes/S3/BackupsS3Options.cs`**: из
  вложенного класса `S3Options` удали свойства `Endpoint`, `Endpoints`,
  `AccessKey`, `SecretKey` и метод `TargetList()`; из `BackupsS3Options`
  удали свойство `IsConfigured` и метод `EnsureValid()`; doc-комментарий
  класса перепиши: «креды и цели — из снапшота S3-домена (первый Active-пул
  с endpoints и secrets, ключ panel; arch/adminpanel/02 §12.4); в конфиге —
  только Bucket (env AdminPanel__Backups__S3__Bucket) и интервалы». Класс
  остаётся `[Config("AdminPanel:Backups")]` с `S3 { Region, Bucket,
  PathStyle }`, `IntervalSec`, `TimeoutSec`.
- **9.2. Создай файл `src/AdminPanel.Probes/S3/S3StorageTargets.cs`** (новый):
  ```csharp
  namespace AdminPanel.Probes.S3;

  using AdminPanel.Core.S3;

  /// <summary>Цели инвентаря грани «Хранилище бэкапов», резолвнутые из
  /// снапшота S3-домена (adminpanel/02 §12.4): креды — read-only ключ
  /// panel per-cluster secrets; endpoints — перебор с первого доступного.
  /// Значения — только S3-клиенту панели.</summary>
  public sealed record S3StorageTargets(
      string Cluster,
      IReadOnlyList<string> Endpoints, // http://host:port — порядок endpoints-ключа
      string AccessKey,
      string SecretKey);

  /// <summary>Резолвер целей: первый по Ordinal Active-кластер снапшота с
  /// непустыми endpoints и валидным secrets (ключ panel, read-only);
  /// null — грань не настроена (нет пула — configured=false).</summary>
  public static class S3TargetsResolver
  {
      public static S3StorageTargets? Resolve(S3Snapshot snapshot)
      {
          // Кластеры снапшота уже Ordinal-отсортированы парсером.
          foreach (var cluster in snapshot.Clusters)
          {
              if (cluster.State != S3ClusterState.Active
                  || string.IsNullOrWhiteSpace(cluster.Endpoints))
                  continue;
              var panel = cluster.Secrets?.AccessKeys.FirstOrDefault(
                  k => k.Name == "panel" && k.Policy == "read-only");
              if (panel is null
                  || string.IsNullOrEmpty(panel.AccessKey)
                  || string.IsNullOrEmpty(panel.SecretKey))
                  continue;
              var endpoints = cluster.Endpoints
                  .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                  .Select(e => $"http://{e}")
                  .ToList();
              if (endpoints.Count == 0)
                  continue;
              return new S3StorageTargets(cluster.Name, endpoints, panel.AccessKey, panel.SecretKey);
          }

          return null;
      }
  }
  ```
  (типы `S3Snapshot`/`S3ClusterState`/`S3ClusterSecretsInfo` — из
  `AdminPanel.Core.S3`; using уже верный.)
- **9.3. В файле `src/AdminPanel.Probes/S3/S3StorageClient.cs`** замени
  поля `_accessKey/_secretKey/_endpoints/_configured` (фиксация из options в
  конструкторе) на `private volatile S3StorageTargets? _targets;` и
  `public void UpdateTargets(S3StorageTargets? targets) => _targets =
  targets;` (при смене объекта целей — очисти `_targetClients`); все
  гварды `_configured`/`IsConfigured` замени на `_targets is null` →
  прежний `NotConfigured<T>()`; построение per-цели клиентов — от
  `_targets.Endpoints` + `_targets.AccessKey/SecretKey` (механика
  `_targetClientFactory`/`BuildClient` сохраняется; `_bucket` — по-прежнему
  из options). Конструкторы с BackupsS3Options сохраняют подписи (читают
  теперь Bucket/Region/PathStyle/TimeoutSec). Значения кредов в
  исключения/логи не попадают.
- **9.4. В файле `src/AdminPanel.Probes/S3/S3InventoryLoop.cs`:**
  1) в конструктор добавь `IS3SnapshotReader s3Snapshot`;
  2) в начале `RunOnceAsync`: `client.UpdateTargets(
     S3TargetsResolver.Resolve(s3Snapshot.Current));` затем
     `if (s3Snapshot.Current is null || S3TargetsResolver.Resolve(
     s3Snapshot.Current) is null) return;` — грань не настроена (клиент не
     зовётся; прежний гвард IsConfigured удаляется);
  3) маркер стора «configured» при первом тике и `FailTick` — условие
     прежнее (стор получает Configured: true только когда цели есть);
     в `store.Replace(...)` цели (первый аргумент-список Endpoints) — из
     резолвнутых targets; текст «Endpoint not set — S3 facet disabled»
     замени на «S3 backups pool not found in /s3/clusters/ — facet
     disabled».
- **9.5. В файле `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs`**
  (`BackupObjectsQueryHandler`): гвард `!options.Value.IsConfigured` замени
  на отсутствие резолвнутых целей:
  `if (S3TargetsResolver.Resolve(...snapshot s3...) is null) return …Failed(
  new BackupsStorageNotConfiguredException());` — источник снапшота:
  `IS3SnapshotReader` через конструктор (добавить параметр; `ISnapshotReader`
  остаётся для префикс-гвардов). Сообщение `BackupsStorageNotConfigured
  Exception` обнови: «no Active S3 backups pool with secrets in
  /s3/clusters/ — the backups storage scope is disabled».
- **9.6. В файле `src/AdminPanel.Api/Program.cs`** удали блок fail-fast
  (строки ~77–79 `var s3 = …; if (s3.IsConfigured) s3.EnsureValid();`).
  Новых DI-регистраций не добавляй: `IS3SnapshotReader` уже зарегистрирован
  (`AdminPanel.Etcd/ModuleExtensions.cs`, `AddS3`) и доставляется в
  `S3InventoryLoop`/`BackupObjectsQueryHandler` конструкторами из 9.4/9.5.
- **9.7. Юниты** (`src/tests/AdminPanel.UnitTests/` — файлы по `grep -rl
  "BackupsS3Options\|S3InventoryLoop\|S3StorageClient" src/tests/
  AdminPanel.UnitTests`): обнови конструирование клиента (options без кредов
  + `UpdateTargets(...)`), добавь AAA-факты резолвера: Active-кластер с
  endpoints+secrets(panel) → цели из первого; пул TO_REMOVE/без endpoints/
  без ключа panel/без secrets — пропущен; порядок кластеров Ordinal; факты
  инвентарь-тика: без целей — клиент не зовётся (стор без configured-
  маркера), с целями — зовётся (существующие фейки клиента).
- **9.8. Скомпилируй:**
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug`
  (ожидание: Build succeeded; правки по списку компилятора — синтаксис
  вызовов/using — по образцу соседних строк).

---

## Шаг 10. Frontend: форма создания, страница кластера, DTO

**Вход:** Шаги 8–9 приняты (API-контракт стабилен).

**Действие (сильная модель ревьюит/интегрирует; механика — подпункты):**
`frontend/src/api/dto.ts` — поля `secretProfile` → `etcdBackup`+состав кредов;
`CreateS3ClusterModal.tsx` — поле профиля уходит, переключатель «бэкапный
кластер etcd» (checkbox) с подсказкой спец-ключа; `S3ClusterDetailsPage.tsx`
— карточка «Креды клиентской грани»: root_user, список ключей name/policy,
бейдж «read-write внешний (спец-ключ бэкапа etcd)» для etcd-backup кластера
(значения НЕ показываются — их нет в API). Сборка SPA: `cd frontend && npm
run build` (проверить типы `npm run build` включает tsc; если lint — им).

**Выход:** UI создаёт кластер без профиля/с etcdBackup; страница показывает
состав кредов без значений; SPA собирается.

**Проверка:**
```
cd frontend && npm run build   # типы + бандл без ошибок
grep -rn "secretProfile" frontend/src | wc -l   # → 0
```

**Связь со spec:** §3.6 (форма создания, просмотр кредов), критерий 7.

Подпункты:

- **10.1. В файле `frontend/src/api/dto.ts`** во всех S3-записях замени поле
  `secretProfile: string` на `etcdBackup: boolean` (создание-запрос: поле
  опционально `etcdBackup?: boolean`); в DTO кластера/деталей добавь поля
  `etcdBackup: boolean; rootUser?: string; accessKeys?: { name: string;
  policy: string }[];` (типовые объявления — в стиле файла).
- **10.2. В файле `frontend/src/pages/s3-cluster/CreateS3ClusterModal.tsx`:**
  1) в состояние формы `secretProfile: ''` замени на `etcdBackup: false`;
  2) валидацию `if (form.secretProfile.trim() === '') return 'secret
     profile: …'` удали (заменители не нужны — чекбокс не требует валидации);
  3) в тело запроса `secretProfile: form.secretProfile.trim()` → `etcdBackup:
     form.etcdBackup`;
  4) поле ввода «Secret profile» (блок `label="Secret profile"` с
     description/value/onChange) замени чекбоксом в стиле соседних
     контролов файла:
     `label="Etcd backup cluster"` с description `"provision this cluster
     as the installation's etcd backup target; requires the worker's
     S3W_ETCD_BACKUP_* env pair; at most one per installation"` и
     `checked={form.etcdBackup} onChange={(e) => set('etcdBackup',
     e.currentTarget.checked)}`;
  5) комментарий-шапку файла (строка ~2 «secret_profile (текст)…») обнови:
     «etcdBackup (переключатель), платформа read-only «owns3»…».
- **10.3. В файле `frontend/src/pages/s3-cluster/S3ClusterDetailsPage.tsx`**
  добавь карточку/секцию «Client credentials» (стиль соседних секций файла):
  строки `Root user: {rootUser}`; список `accessKeys.map(k => <li>{k.name}
  ({k.policy})</li>)`; при `etcdBackup` — бейдж/строка «backups key is
  external (etcd backup special key)»; заглушка «not provisioned yet» при
  отсутствии `rootUser`. Остальные ассерты `secretProfile` в файле (таблица/
  диффы) — заменить на `etcdBackup`/убрать по месту.

---

## Шаг 11. Стенд, deploy, чеки, docs

**Вход:** Шаги 1–10 приняты; код собирается.

**Действие (сильная модель — bash-скрипты и чеки тонкие, подпункты только на
плоские env/текстовые правки):**
- `deploy/.env.example`: блоки `S3W_NODE_SECRETS__BACKUPS-MAIN__*`,
  `PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__*` и комментарии о них — удалить;
  добавить закомментированный блок опции:
  `# S3W_ETCD_BACKUP_ACCESSKEY=…\n# S3W_ETCD_BACKUP_SECRETKEY=…`
  (обязательны ТОЛЬКО при создании etcd-backup кластера).
- `deploy/docker-compose.yml`: `env_file: ./s3w-node-secrets.env` у s3worker
  и `env_file: ./pgw-s3-profiles.env` у pgworker — удалить вместе с
  комментариями о дефисе/интерполяции; добавить s3worker-сервису обычные
  env-передачи `S3W_ETCD_BACKUP_ACCESSKEY: ${S3W_ETCD_BACKUP_ACCESSKEY:-}`
  и `S3W_ETCD_BACKUP_SECRETKEY: ${S3W_ETCD_BACKUP_SECRETKEY:-}` (композ-имена
  без дефисов — интерполяция безопасна).
- `dev-stand/adminpanel/docker-compose.yml`: env
  `AdminPanel__Backups__S3__AccessKey/SecretKey/Endpoint` удалить (Bucket
  остаётся); комментарий над блоком обновить (креды/endpoint — из снапшота
  S3-домена per-cluster).
- `dev-stand/adminpanel/checks/00-up.sh`: блоки дописи
  `S3W_NODE_SECRETS__*`/`PGW_BACKUP_S3_PROFILES__*` в `.env` и сборки
  `s3w-node-secrets.env`/`pgw-s3-profiles.env` (строки ~124–166) — удалить;
  fail-fast grep'ы профилей — удалить.
- `dev-stand/adminpanel/checks/75-s3worker.sh`: из всех трёх тел создания
  кластера (строки 43, 67, 70) убрать `"secretProfile":"…",`; кейс
  «unknown profile → 422» (строки 69–71) заменить на 422-кейс t31 — создание
  с `"etcdBackup":true` без env спец-ключа у стендового воркера (по
  умолчанию .env пары не несёт — блок в .env.example закомментирован) →
  422; сообщение строки 76 обновить («etcdBackup without env key → 422»).
- `dev-stand/adminpanel/checks/80-s3-panel.sh`: тело создания без
  `secretProfile` (добавить `"etcdBackup":false` не нужно — поле
  опционально); инспекция деталей: вместо `secretProfile == "BACKUPS-MAIN"`
  — `.rootUser == "s3w-<TAG>-root"` и `.accessKeys | length == 2`; новая
  проверка: secrets-ключ появился в etcd (`etcdctl get
  /s3/clusters/<TAG>/secrets --keys-only`) и в выводе чека ЗНАЧЕНИЙ нет
  (jq-проверка только структуры).
- `dev-stand/adminpanel/checks/45-backups-storage.sh`: root-креды больше не
  из `deploy/.env` — читать `etcdctl get /s3/clusters/backups/secrets` и
  jq-извлекать `root_user`/`root_password` (значения только в переменные,
  НИКОГДА в stdout; gвард отсутствия ключа — понятный фейл).
- `dev-stand/adminpanel/README.md` (раздел «Хранилище бэкапов»): env-креды
  панели → per-cluster secrets; чек 45 — креды из etcd.
- `docs/runbook.md`: исторического блока `S3W_NODE_SECRETS__*` в нём нет —
  правка по факту: обновить упоминание «(профили/секреты —
  `dev-stand/adminpanel/`)» в строке ~8 (секреты S3-кластеров — per-cluster
  в etcd, per-install env-профилей нет) и добавить в раздел dev-станда пункт
  о спец-ключе `S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY}` (полупара — fail-fast
  старта s3worker; обязателен только при создании etcd-backup кластера;
  подставляется в `/s3/clusters/<C>/secrets` ключом `backups`).

**Выход:** стенд поднимается без env-профилей; чеки 45/75/80 зелёные на
свежем стенде (пул создаётся вручную через панель, как прежде).

**Проверка:**
```
grep -rn "S3W_NODE_SECRETS\|PGW_BACKUP_S3_PROFILES" src/ deploy/ dev-stand/ docs/runbook.md | wc -l   # → 0
bash dev-stand/adminpanel/checks/00-up.sh   # полный подъём (сильная модель, в фоне)
bash dev-stand/adminpanel/checks/75-s3worker.sh
bash dev-stand/adminpanel/checks/80-s3-panel.sh && bash dev-stand/adminpanel/checks/45-backups-storage.sh
```
Правила: команды — только в фоне (`run_in_background`), контроль по task_id;
после серии — зачистка (`docker network prune -f` при осиротевших сетях).
Объём grep — критерий AC10 (src/, deploy/, dev-stand/) + правимый runbook;
история задач `docs/superpowers/` не трогается (см. чек-лист ниже).

**Связь со spec:** §3.7 (стенд и deploy), критерии 8, 10.

Подпункты:

- **11.1. В файле `deploy/.env.example`** удали строки 95–99 (комментарий про
  `PGW_BACKUP_S3_PROFILES__*` и обе переменные
  `PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__{ACCESS_KEY,SECRET_KEY}`), строки
  ~139–145 (комментарий-блок «PGW_BACKUP_S3_PROFILES__… один комплект
  S3-секретов…») и строки 188–198 (комментарий про дефис и весь блок
  `S3W_NODE_SECRETS__BACKUPS-MAIN__*` из 8 переменных); на место блока
  S3W добавь:
  ```
  # Спец-ключ бэкапа etcd (arch/23 §6): обязателен ТОЛЬКО при создании
  # etcd-backup кластера (config.etcd_backup=true); полупара — fail-fast
  # старта воркера. Прод — per-install секрет.
  #S3W_ETCD_BACKUP_ACCESSKEY=
  #S3W_ETCD_BACKUP_SECRETKEY=
  ```
- **11.2. В файле `deploy/docker-compose.yml`** у сервиса `pgworker`
  (x-pgworker-base, строки ~99–104) удали блок `env_file: - ./
  pgw-s3-profiles.env` и комментарий над ним (5 строк про t36/дефис); у
  сервиса `s3worker` (x-s3worker-base, строки ~234–240) удали `env_file: -
  ./s3w-node-secrets.env` и комментарий над ним; в секцию `environment`
  s3worker (рядом с `S3Worker__Etcd__Endpoints__*`, строки ~206–208) добавь:
  ```yaml
        # Спец-ключ бэкапа etcd (arch/23 §6): опционален; нужен только для
        # создания etcd-backup кластера (полупара — fail-fast старта).
        S3W_ETCD_BACKUP_ACCESSKEY: ${S3W_ETCD_BACKUP_ACCESSKEY:-}
        S3W_ETCD_BACKUP_SECRETKEY: ${S3W_ETCD_BACKUP_SECRETKEY:-}
  ```
- **11.3. В файле `dev-stand/adminpanel/checks/75-s3worker.sh`:**
  1) в трёх телах создания кластера удали подстроку
     `"secretProfile":"BACKUPS-MAIN",` (строка 43) / `"secretProfile":
     "BACKUPS-MAIN",` (строка 67) / `"secretProfile":"no-such-profile",`
     (строка 70) — тела остаются без поля профиля;
  2) кейс 422 (строки 69–71) замени: тело
     `{"name":"${TAG}x","platform":"owns3","etcdBackup":true,"instances":[…]}` —
     создание etcd-backup кластера без env спец-ключа у стендового воркера
     (по умолчанию .env пары не несёт — блок в .env.example закомментирован)
     → ожидание 422 остаётся: `[ "$c" = 422 ] || { echo "❌ etcdBackup
     without env key → $c, expected 422"; exit 1; }`;
  3) строку 76 приведи к новому составу:
     `echo "  validations: name → 400, etcdBackup without env key → 422; /metrics contains s3worker_"`;
  4) комментарий секции 3 (строка 65) обнови: «невалидный name → 400;
     etcdBackup без спец-ключа env → 422».

---

## Шаг 12. Тесты: обновление существующих серий (юниты + интеграция)

**Вход:** Шаги 2–11 приняты; юниты компонентов уже обновлены по месту.

**Действие (сильная модель — аудит покрытий и граничные кейсы; механические
замены — подпункты):** привести зелёными ВСЕ существующие серии:
- S3Worker.IntegrationTests: `ClusterMutationsApiTests` (создание без
  профиля; etcdBackup: 422 без env / 409 второй / успешное создание с
  env-парой; ответ DTO без secretProfile), `SyncProbeApiTests`,
  `S3ClusterFixture` и `SyncProbeEtcdTests` (env-словари профилей →
  удалить), `S3ApiFactory/TestSecrets` (ModuleInitializer-профили → спец-
  ключ), юниты App (`TestSupport`, Program-валидации при наличии).
- S3Worker E2E-окружение (`S3E2eEnvironment` и др.): env `S3W_NODE_SECRETS__*`
  воркеров E2E убрать; фикстуры кластеров — без secret_profile.
- PgWorker.IntegrationTests: `FakeBackupDeps` (профили → secrets в etcd),
  `E2eEnvironment`/`E2eBackupPoolScenarios` (пул сидится secrets-ключом в
  etcd; воркер PgWorker без env-профилей), `BackupsPolicyApiTests`/
  `OrphansApiTests` — по месту.
- AdminPanel.IntegrationTests: `S3ApiTests` (создание без secretProfile,
  409/422-прокси), фикстуры.

**Выход:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c
Debug` зелёный целиком (юниты + интеграция НЕ-docker); фикстуры не содержат
secret_profile.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName!~E2e"
grep -rn "secret_profile\|SecretProfile\|S3W_NODE_SECRETS\|PGW_BACKUP_S3_PROFILES" src/tests/ | wc -l   # → 0
```
Между сериями (юниты → интеграция) — зачистка тестовых контейнеров/сетей по
канону AGENTS.md; после финальной строки прогона — `docker network prune -f`
страховочно при осиротевших сетях.

**Связь со spec:** фаза 5 (обновить существующие), критерий 9.

Подпункты (механические замены по списку компилятора/ассертов — сильная
модель выдаёт их подпунктами по мере разбора):

- **12.1. В файле `src/tests/S3Worker.IntegrationTests/Api/ClusterMutationsApiTests.cs`:**
  во всех телах создания кластера удали свойство `"secretProfile":
  "…"`; добавь AAA-факты (механика env процесса — см. ниже):
  (1) создание с телом `{"name":"c-etcd-noenv","platform":"owns3","etcdBackup":true,"instances":[…]}`
  при СНЯТОЙ env-паре → 422 (тело проблемы упоминает S3W_ETCD_BACKUP);
  (2) при ЗАДАННОЙ env-паре → 201, ответ `etcdBackup:true`;
  (3) повторное создание второго etcd-backup кластера при живом первом → 409.
  Утверждения DTO-ответа: поле `secretProfile` отсутствует.
  Механика env (точная): гвард читает переменные ЖИВО на каждом запросе
  (`ClusterSecretsStore.ValidateEtcdBackupEnvPairs()` →
  `Environment.GetEnvironmentVariable`), поэтому тест управляет ими вокруг
  HTTP-вызова: в `// Arrange` кейса (1) —
  `Environment.SetEnvironmentVariable("S3W_ETCD_BACKUP_ACCESSKEY", null);`
  и то же для `SECRETKEY` (снимает пару, выставленную ModuleInitializer
  `TestSecrets`); в `// Arrange` кейсов (2)/(3) — выставить обе
  (`…, "it-access"` / `"it-secret"`); в `finally` каждого кейса —
  восстановить обе переменные в значения `TestSecrets` (или null, если
  ModuleInitializer их более не ставит — см. 12.2b). Тесты одной
  xUnit-коллекции выполняются последовательно — гонки нет.
- **12.2. В файле `src/tests/S3Worker.IntegrationTests/S3/S3ClusterFixture.cs`:**
  удали установку env `S3W_NODE_SECRETS__*` (словарь строк ~89–95,
  передававшийся в SecretProfileStore) вместе с его использованием; сид-клеймы
  кластеров — без поля secretProfile.
- **12.2b. В файле `src/tests/S3Worker.IntegrationTests/Api/S3ApiFactory.cs`**
  (класс `TestSecrets`, строки ~65–86): замени массив `EnvNames` с
  `S3W_NODE_SECRETS__{Profile}__*` на пару
  `["S3W_ETCD_BACKUP_ACCESSKEY", "S3W_ETCD_BACKUP_SECRETKEY"]`; в
  `[ModuleInitializer] Set()` выставляй обе значением `"it-secret"`
  (гарантированный полный env для WAF-хостов: Program.Main fail-fast полупар
  исполняется раньше конфига WAF — прежний комментарий блока сохраняется);
  `Profile = "backups-main"` и его использование удали.
- **12.3. В файле `src/tests/S3Worker.IntegrationTests/S3/SyncProbeEtcdTests.cs`:**
  удали env-словарь `S3W_NODE_SECRETS__backups-main__*` (строки ~37–38) и
  его передачу; creds-источник — предзаписанный secrets-ключ etcd (по образцу
  Шага 5.5).
- **12.4. В файле `src/tests/PgWorker.IntegrationTests/Backups/FakeBackupDeps.cs`:**
  замени конструирование `BackupPoolResolver(pools, secrets…)` на
  `BackupPoolResolver(pools…)` (без store-параметра); пулы-фейки — с
  `Secrets: new("ak","sk")` там, где раньше указывался SecretProfile; env
  `PGW_BACKUP_S3_PROFILES__*` в тестовом окружении удали.
- **12.5. В файлах `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`
  и `E2eBackupPoolScenarios.cs`:** env `PGW_BACKUP_S3_PROFILES__*`/
  `S3W_NODE_SECRETS__*` у поднимаемых воркеров — удалить; там, где сценарий
  ожидал креды из env, пул читает `/s3/clusters/<C>/secrets` (S3Worker
  E2E-сид создаёт ключ сам); при необходимости сценарий дописывает
  secrets-ключ сам (JSON по канону, значения произвольные 32-симв).

---

## Шаг 13. E2E + мерж-гейт (свежий Release, docker)

**Вход:** Шаги 1–12 приняты; весь Debug-прогон зелёный.

**Действие (сильная модель, без подпунктов — сценарии и гейты требуют
понимания контекста):**
1. Новые/обновлённые docker-E2E S3Worker (`src/tests/S3Worker.IntegrationTests/E2e/`):
   - создание кластера без профиля → provisioning → Active; в etcd есть
     `/s3/clusters/<C>/secrets` единого формата; значения ОТСУТСТВУЮТ в
     `/s3worker/work/<C>`, логах воркера (host.log), docker-inspect-выводах
     теста (ассерт: grep значений secrets-ключа по артефактам прогона);
   - ротация (`S3E2eRotationTests`): заявка → новые root+ключи в etcd →
     rolling по одной за тик → заявка погашена; PgWorker-агенты (если в
     контуре) пересоздаются со свежими env;
   - etcd-backup кластер: спец-ключ из env воркера → `name=backups` равен
     env-паре; root/panel сгенерированы; признак в config; 422 без env; 409
     второй.
   - изоляция по `docs/e2e-isolation.md`: guid, own-only чистка, ассерт
     чистоты, динамические порты, BrokerBootSec ≤ 100 с, `[PHASE]`-метки
     фаз > 60 с, телеметрия по `docs/e2e-launch.md` (артефакты
     `/tmp/pgw-e2e-artifacts-<guid>/`, MarkFailed — контейнеры
     останавливаются, не удаляются).
2. PgWorker docker-E2E: `E2eBackupPoolScenarios` — запись/чтение бэкапов на
   кредах из etcd; рестарт-луп агентов при устаревших кредах после ротации →
   пересоздание со свежими (канон arch/19 §3).
3. Мерж-гейт (по AGENTS.md — воркеры тронуты, provisioning/ротация затронуты):
   ```
   DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2eBackupPool
   DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3E2e"
   ```
   (точные имена фильтров — по фактическим FQN классов; полный E2eFixture
   PgWorker — обязательный, provisioning/ротация затронуты.)
4. Между сериями — зачистка: контейнеры/тома/сети каждой серии; после
   финальной строки серии проверять `docker network ls | grep -c 'pgw-.*-net\|kfw-net'`
   и `docker network prune -f` при осиротевших. Упавший сценарий — разбор
   по логам БЕЗ перезапуска (канон телеметрии E2E).

**Выход:** все E2E-серии зелёные на свежем Release; артефакты сняты;
контейнеры/сети зачищены.

**Проверка:** зелёные прогоны вышеперечисленных команд; `docker ps -a |
grep -c 'pgw-\|s3w-'` → 0 после зачистки; ассерты чистоты внутри сценариев
зелёные.

**Связь со spec:** фаза 5 (интеграция/E2E, мерж-гейт), критерии 3–7, 9;
границы §1.1 (t32-etcdworker/t32-panel-bucket-etcd — НЕ входят).

Подпунктов нет.

---

## Чек-лист самопроверки перед завершением задачи

- [ ] `grep -rn "S3W_NODE_SECRETS\|PGW_BACKUP_S3_PROFILES\|secret_profile" src/ deploy/ dev-stand/ docs/runbook.md` → пусто (критерий 10: объём AC10 — src/, deploy/, dev-stand/; docs — только правимый runbook; история задач `docs/superpowers/` не входит и не трогается).
- [ ] `grep -rn "SecretProfileStore\|BackupSecretProfileStore\|S3SecretProfileUnavailableException" src/` → пусто.
- [ ] Полный Debug-прогон `src/PgWorker.slnx` зелёный; Release-E2E мерж-гейт зелёный.
- [ ] Значения секретов отсутствуют в журналах/логах/API/UI/stdout чеков (E2E-ассерты Шага 13, чеки 75/80/45).
- [ ] Roadmap: записи t31/t32 остаются до мержа (удаляются мерж-коммитом — вне плана).
- [ ] Коммиты атомарны по шагам; история читаема.
