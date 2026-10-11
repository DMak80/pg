# t40-owns3-erasure-coding — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Второй режим хранения ownS3 `XL Erasure`: multi-drive том с кодами Рида—Соломона, erasure-сеты, кворумы, bitrot-контроль и heal — при неизменном контракте 21 операции и неизменном single-режиме.

**Architecture:** Ядро EC (GF(2⁸), RS-кодек, HighwayHash-256, блочный кодер) — чистые компоненты `OwnS3.Storage/Erasure`. Размещение выносится из `XlObjectStore` в абстракцию `IXlDrive` с реализациями `SingleXlDrive` (сегодняшнее поведение) и `ErasureXlDrive` (фасад над `ErasurePool` из сетов); `XlObjectStore` остаётся единственным местом логики операций (conditional/Range/ETag/листинги/multipart/идемпотентность). Форматы: `volume.json` v2 на каждом диске, `xl.meta` RecordVersion 2 с EC-полями и bitrot-хэшами.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), `System.IO.Hashing` (SipHash, уже в `Directory.Packages.props` v10.0.12), xUnit v3 + FluentAssertions, Testcontainers (docker-E2E). Внешних NuGet-пакетов НЕ добавляется.

**Spec:** [`docs/superpowers/2026-10-11-t40-owns3-erasure-coding/spec.md`](spec.md) — план аргументируется от спеки; канон механики — [`arch/owns3/06-erasure.md`](../../../arch/owns3/06-erasure.md). При расхождении формулировок канон побеждает (спека §3).

**Обязательно к прочтению исполнителем:** [`AGENTS.base.md`](../../../../AGENTS.base.md), [`AGENTS.md`](../../../../AGENTS.md) (docker-тесты, динамические порты, изоляция E2E, AAA-нотация), [`docs/e2e-isolation.md`](../../../e2e-isolation.md), [`docs/e2e-launch.md`](../../../e2e-launch.md). Референс механики: `/Users/demakaev/ZCodeProject/minio/cmd/{erasure-*,bitrot*,format-erasure}.go` (устройство, не перенос целиком; отступления — канон 06).

## Global Constraints

- Язык: комментарии/документация — русские, идентификаторы — английские; тесты — с комментариями `// Arrange // Act // Assert` (AAA).
- `TreatWarningsAsErrors=true`: каждый шаг, пишущий код, завершается безварнинговой сборкой.
- Порты docker-контейнеров в тестах — только динамические (`FreePort()` / `WithPortBinding(..., assignRandomHostPort: true)`); никаких литералов `:9010`/`:16000` в тестах.
- E2E-изоляция: guid-имена (`os3-*-{runId}`), own-only teardown, ассерт чистоты, `[PHASE]`-метки, `MarkFailed()` — по канонам `docs/e2e-isolation.md`/`docs/e2e-launch.md`; эталон — `src/tests/OwnS3.IntegrationTests/E2e/OwnS3E2eEnvironment.cs`.
- После КАЖДОЙ тестовой серии (юниты → интеграции → E2E) — контроль зачистки: `docker ps -a --filter name=os3-` пуст (кроме оставленных `MarkFailed`-окружений — по правилам телеметрии), `docker network prune -f` страховочно. Следующая серия — только поверх зачищенной.
- E2E-образы: publish на хосте + runtime-COPY (`docker/OwnS3.Dockerfile` — без изменений); локальные образы в registry не кладутся.
- Таймауты ожидания в командах прогона — не более 30 с на шаг поллинга; бюджет контейнера wait — 30 с; любая фаза > 60 с — `[PHASE]`.
- Код и тесты — только в worktree `feat-t40-owns3-erasure-coding`; все пути ниже — от корня worktree.
- Коммит после каждого Task (feature-ветка — свободно). Правки `arch/` — всегда ДО кода, которому они нужны (arch-first).
- `BrokerBootSec`-аналогов (долгих bootstrap-бюджетов) в OwnS3-тестах не появляется; упавший прогон падает быстро.
- Single-режим: ни одно существующее поведение/тест/контур не меняется (спека §6.4 п.4) — страховкой рефакторинга служат зелёные существующие серии (115 интеграций + юниты).

## Порядок задач и зависимости

```
Task 0 (arch-правки) ──► Task 7 (xl.meta v2)
Task 1 (GF) ─► Task 2 (RS) ─► Task 4 (BlockWriter/Reader)
Task 3 (HighwayHash) ─► Task 4
Task 5 (IXlDrive + SingleXlDrive) ─► Task 6 (Pool) ─► Task 8..12 (ErasureXlDrive) ─► Task 13 (Heal) ─► Task 14 (App) ─► Task 15 (контракт-интеграции) ─► Task 16 (docker-E2E) ─► Task 18 (полный прогон)
Task 17 (compose + чек) ─► Task 18
```

Task 1–2 и Task 3 независимы (параллелимы); Task 5 не зависит от 1–4 (параллелим с ними). Порядок 5 → 8 обязателен (спека §5).

---

### Task 0: arch-правки канонов 02/06 (четыре правки планирования)

**Спека:** §3 (arch-first), §6.4 п.5. **Вход:** канон `arch/owns3/06-erasure.md` (обновлён Фазой 1) и `arch/owns3/02-operations.md`; лакуны, найденные при проектировании плана и утверждённые резолюциями ревью.

**Находка 1 (§5, bitrot-записи):** две лакуны схемы. (1) `GetObjectAttributes` (канон 02 §5) отдаёт исходные размеры частей объекта: из `blockCount` размера хвостового блока не вывести (хвост короче 1 МиБ), шардовые `part.N` не дают исходного размера — нужного поля в записи нет. (2) Размерность: схема фиксирует на часть ОДИН набор `blockCount` хэшей, а §6 требует хэши каждого шард-блока КАЖДОГО шарда, и §5 требует одинакового содержимого копий записи (кроме `EcIndex`) — чтение (bitrot-сверка каждого читаемого блока k источников), реконструкция и heal нуждаются в полном 2D-наборе `(k+m) × blockCount` в каждой копии; множитель `ecN` и порядок раскладки (шард-мажор/блок-мажор) схемой не зафиксированы — реализация «по букве» сломала бы bitrot-механику.

**Находка 2 (§2, старт живого тома):** канон различает «пустой каталог = кандидат замены» и валидный `volume.json`, но не фиксирует трактовку ОТСУТСТВУЮЩЕГО пути на живом томе. Буквальное «несуществующий путь → создание каталога» (§1) превращает отвалившийся том в мгновенную замену diskId — сценарий «возврат тома с данными → heal» (спека §6.3, канон §9) становится невозможным.

**Правка 3 (§9 + согласование §8/§10, границы heal и фантом-записи — резолюция ревью):** канон §9 предписывает heal сканировать `.trash/`/orphan; утверждённое разделение ответственности: heal реконструирует только живое по консенсусу сета, `.trash/`/orphan удаляются чистками по порогу (§10). Надёжность данных не страдает: `.trash/` — помеченное к удалению, orphan — без живой меты; дубль реконструкции/удаления исключается. Синхронные правки-согласования: §8 п.4 («мусор tmp/orphan — чистится (п. 10) и долечивается heal») и §8 абзац Delete («недоступные диски чистятся возвратившимся heal» — приписывает heal удаление, запрещённое новой нормой §9; после кворумного Delete при offline-диске на вернувшемся диске остаётся фантомный ключ, недоступный ни heal, ни чисткам — дыра). Закрывается доопределением в §10 категории «не актуальное в консенсусе» (фантом-записи) с локальной кворумной сверкой отсутствия — противоречий §8↔§9↔§10 после правок остаться не должно.

**Правка 4 (глава 02 «Недоступность кворума» + 06 §7 — синхронизация 503-нормы листингов, резолюция ревью):** формулировка «листинги и бакетные операции … 503 на весь запрос/листинг» при широком прочтении включает ListBuckets — противоречит норме 06 §8 (ListBuckets — консенсус по любому кворумному сету) и делает канон 06 внутренне противоречивым (§7↔§8). Сузить во всех трёх местах — глава 02 и два вхождения в 06 §7 (норма кворума листингов и фраза о потере > m дисков) — до «листинги объектов и бакетные мутации Create/Delete»; ListBuckets — отсылкой к 06 §8. Синхронизация выполняется до кода Task 10 (arch-first), не откладывается в Task 18.

**Файлы:**
- Modify: `arch/owns3/06-erasure.md`
- Modify: `arch/owns3/02-operations.md`

- [ ] **Шаг 1. Правка §5** — заменить строку таблицы bitrot-записей и абзац под ней:

> | bitrot-записи | для каждого `part.N` — исходный размер части (uint64), число шард-блоков (uint32) и HighwayHash-256 (32 байта) каждого шард-блока каждого шарда — полный 2D-набор ecN × blockCount в каждой копии записи (п. 6: сверка чтения, реконструкция и heal адресуют произвольный шард) |

и дополнить описание записи v2 побайтовым хвостом: «после `ecDist`: `uint16 partCount`; на каждую часть по возрастанию `part.N`: `uint64 sourceSize` (исходный размер части — источник GetObjectAttributes без декодирования), `uint32 blockCount`, затем для каждого шард-индекса `j = 0..ecN−1` — `blockCount × 32` байта HighwayHash-256 его шард-блоков по порядку блоков; раскладка шард-мажор: `hashes[shardIndex][blockIndex]`; размер bitrot-секции части — `8 + 4 + ecN × blockCount × 32` байт; копия записи на любом диске сета несёт полный набор (одинаковое содержимое, кроме `EcIndex`)».

- [ ] **Шаг 2. Правка §2** — в списке «Существующий EC-том» после пункта про «пуст (без `volume.json`) — кандидат замены» добавить:

> «Отсутствующий путь позиции на живом томе — диск **offline** (не кандидат замены): создание каталогов несуществующих путей выполняется только при создании нового тома; возврат пути с его `volume.json` возвращает диск в сет онлайн».

- [ ] **Шаг 3. Правка §9 + согласование §8/§10** (резолюция ревью):
  - §9: заменить предложение «Heal сканирует и `.trash/`/orphan (п. 10) — доливки после сбоев записи.» на:

> «`.trash/` и orphan heal не сканирует: heal реконструирует только живое — объекты, актуальные в консенсусе сета (недостающее на диске живое доливается). Помеченное к удалению (`.trash/`) и не упомянутое живой записью (orphan) удаляется чистками по порогу (п. 10). Ответственность heal и чисток не дублируется.»

  - §8, пункт 4 жизненного цикла записи: хвост «оставляют мусор tmp/orphan — чистится (п. 10) и долечивается heal.» заменить на «оставляют мусор tmp/orphan — удаляемый чистками по порогу (п. 10); недостающее на диске живое (в консенсусе) доливается heal (п. 9).».
  - §8, абзац Delete: фразу «недоступные диски чистятся возвратившимся heal» заменить на «недоступные при Delete диски выравниваются при возврате: живое-в-консенсусе доливается heal (п. 9); контент, не актуальный в консенсусе сета, удаляется чистками по порогу — фантом-записи (п. 10)».
  - §10, дополнить категорию фантом-записей (точная семантика ЛОКАЛЬНОЙ сверки):

> «- фантом-записи: ключ с локально валидной `xl.meta`, не актуальный в консенсусе сета (кворумно удалён при offline-диске либо недозакоммичен крахом записи), удаляется локальной сверкой: чистка диска читает `xl.meta` того же ключа на других онлайн-дисках сета (внутрипроцессное чтение корней, без кворумных операций); запись отсутствует на ≥ k онлайн-дисках сета и локальная запись старше порога 1 ч (mtime `xl.meta`) → каталог ключа rename в `.trash/`; кворум отсутствия недостижим (онлайн-дисков без записи < k) → ключ консервативно сохраняется до ближайшей сверки с кворумом;»

  - §10, пункт о конкуренции heal/чисток согласовать: «чистка удаляет только незакоммиченное/устаревшее локально» → «чистка удаляет только незакоммиченное/устаревшее локально и не актуальное в консенсусе сета (фантом-записи — при кворуме отсутствия ≥ k)».

- [ ] **Шаг 4. Правка главы 02 (раздел «Недоступность кворума (EC-режим)») + 06 §7** (резолюция ревью): строку главы 02 «- листинги и бакетные операции при недоступном (онлайн < k) любом сете → **503** `InsufficientReadQuorum` на весь запрос.» сузить до:

> «- листинги объектов и бакетные мутации Create/Delete при недоступном (онлайн < k) любом сете → **503** `InsufficientReadQuorum` на весь запрос (ListBuckets — консенсус по любому сету с read-кворумом, глава 06 §8: 503 только при отсутствии кворумного сета).»

Синхронно в 06 §7 строку «**Листинги и бакетные операции** требуют онлайн ≥ k **во всех сетах**: любой сет < k → 503 на весь листинг (критерий: неполный листинг для бэкапов опаснее недоступности — цепочки восстанавливаются клиентом по 503 с retry, а «пропавших» ключей листинг не показывает никогда).» заменить на:

> «**Листинги объектов и бакетные мутации Create/Delete** требуют онлайн ≥ k **во всех сетах**: любой сет < k → 503 на весь запрос (критерий: неполный листинг для бэкапов опаснее недоступности — цепочки восстанавливаются клиентом по 503 с retry, а «пропавших» ключей листинг не показывает никогда); ListBuckets — консенсус по любому сету с read-кворумом (п. 8).»

И третья синхронная замена в 06 §7 (последний абзац о потере > m дисков): «(листинги — 503 до возврата k дисков, п. выше)» → «(листинги объектов — 503 до возврата k дисков, п. выше)» — устраняет последнее неквалифицированное «листинги» в §7.

- [ ] **Шаг 5. Коммит**: `git add arch/owns3/02-operations.md arch/owns3/06-erasure.md && git commit -m "arch(owns3): 02+06 §5/§7 — offline-путь живого тома, per-part sourceSize + 2D-bitrot, границы heal/чисток (фантом-записи), сужение 503-нормы листингов (t40)"`.

**Выход:** каноны однозначны для старта с отвалившимся томом, формата bitrot-записей (2D-размерность), границ heal/чисток (включая фантом-записи) и 503-нормы листингов. **Проверка:** перечитать 02 (раздел «Недоступность кворума»), 06 §2/§5/§6/§7/§8/§9/§10 — трактовки единственны; §5↔§6 согласованы по размерности bitrot-хэшей (2D ecN × blockCount на часть, шард-мажор); §7↔§8 и 02↔06 §7 не противоречат (ListBuckets — по §8); в нормах 503-доступности §7 — пункт «Листинги объектов и бакетные мутации Create/Delete…» и абзац о потере > m дисков — и в строке главы 02 не остаётся неквалифицированных «листингов»; вхождение «запись и листинги работают» в пункте «Деградация сета» (§7) к 503-норме не относится (при онлайн ≥ k работают и листинги объектов, и ListBuckets), правками не затрагивается и остаётся как есть; §8↔§9↔§10 согласованы. **Связь со spec:** §3 (канон до кода), §6.3 (кейсы возврата/замены тома), §5 фаза 8 (heal).

---

### Task 1: GaloisField256 — арифметика GF(2⁸)

**Спека:** §4.1 (компонент), §6.1. **Вход:** чистая ветка после Task 0.

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/GaloisField256.cs`
- Test: `src/tests/OwnS3.UnitTests/Erasure/GaloisField256Tests.cs`

**Interfaces (Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// GF(2^8) с полиномом 0x11D (как референс cmd/erasure-coding.go): таблицы
// exp(512)/log(256), умножение/деление/обратный, умножение среза на коэффициент.
public static class GaloisField256
{
    public const byte Polynomial = 0x11D;
    public static byte Multiply(byte left, byte right);
    public static byte Divide(byte left, byte right);        // right != 0
    public static byte Inverse(byte value);                  // value != 0
    public static void MultiplyInPlace(byte coefficient, Span<byte> data); // data[i] *= coefficient
    public static byte[] ExpTable { get; }                   // 512 элементов
    public static byte[] LogTable { get; }                   // 256 элементов
}
```

- [ ] **Шаг 1. Пишущий тест** `GaloisField256Tests` (файл + каталог создаются; xUnit, AAA):

```csharp
[Fact]
public void Multiply_IsCommutativeAndDistributiveOverXor()
{
    // Arrange
    byte a = 0x57, b = 0x83, c = 0x29;
    // Act
    var ab = GaloisField256.Multiply(a, b);
    var ba = GaloisField256.Multiply(b, a);
    var distrib = GaloisField256.Multiply(a, (byte)(b ^ c));
    // Assert
    ab.Should().Be(ba);
    distrib.Should().Be(GaloisField256.Multiply(a, b) ^ GaloisField256.Multiply(a, c));
}

[Fact]
public void Inverse_MultipliedBack_GivesOne()
{
    // Arrange — все ненулевые элементы поля
    // Act
    for (int v = 1; v < 256; v++)
    {
        // Assert
        GaloisField256.Multiply((byte)v, GaloisField256.Inverse((byte)v)).Should().Be(1);
    }
}

[Fact]
public void Tables_SatisfyExpLogInverse()
{
    // Arrange/Act/Assert: ExpTable[i] == ExpTable[i % 255] для i >= 255;
    // ExpTable[LogTable[v]] == v для v != 0; Divide(a, b) * b == a.
}
```

Плюс кейсы: `Multiply_ByZero_IsZero`, `Divide_BySelf_IsOne`, `MultiplyInPlace_ScalesSlice` (сверка с по-элементным `Multiply`).

- [ ] **Шаг 2. Прогон — упал**: `dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~GaloisField256` — ошибка компиляции (типа нет).
- [ ] **Шаг 3. Реализация**: генерация exp/log циклом `x *= 2` по модулю полинома (референс `newGaloisField` в `cmd/erasure-coding.go`); `Multiply` через log/exp-таблицы с нулевым коротким замыканием; `MultiplyInPlace` — цикл по срезу.
- [ ] **Шаг 4. Прогон — зелёный** (та же команда).
- [ ] **Шаг 5. Коммит**: `feat(owns3): GF(2^8) для erasure-кодека (t40)`.

**Выход:** арифметика поля для RS. **Проверка:** фильтр зелёный, 0 warnings. **Связь:** §4.1 `GaloisField256`, §6.1.

---

### Task 2: ReedSolomonCodec — систематический кодек

**Спека:** §4.1, §6.1 (round-trip, все комбинации потерь ≤ m при k+m ≤ 8, систематичность, стабильность). **Вход:** Task 1.

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/ReedSolomonCodec.cs`
- Test: `src/tests/OwnS3.UnitTests/Erasure/ReedSolomonCodecTests.cs`

**Interfaces (Consumes/Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// Систематический RS-кодек k data → k+m (канон 06 §4): матрица — Vandermonde,
// приведённая к identity в верхних k×k строках (гауссово исключение над GF(2⁸)).
// Оперирует равными шард-блоками одного erasure-блока (нарезку делает
// ErasureBlockWriter). Потокобезопасен после создания (матрица иммутабельна).
public sealed class ReedSolomonCodec
{
    public const int ErasureBlockMaxSize = 1024 * 1024; // 1 МиБ исходных (канон 06 §4)
    public ReedSolomonCodec(int dataShards, int parityShards); // k>=1, m>=1, k+m<=255
    public int DataShards { get; }
    public int ParityShards { get; }
    public int TotalShards { get; }
    public byte[,] Matrix { get; } // (k+m)×k, стабильна: верх k×k == identity

    // data[k] (len равные) → parity[m] (len). Выделяет/заполняет result.
    public void Encode(ReadOnlySpan<byte>[] dataShards, Span<byte>[] parityShards);

    // Восстановление: shards[i] == null или короткий — потерян. Живых должно быть >= k,
    // иначе InvalidOperationException. Восстанавливает data и parity (все <= m потерь).
    public void Reconstruct(byte?[][] shards);
}
```

Матрица: строки 0..k−1 — identity; строки k..k+m−1 — Vandermonde `x_j = j+1` после применения того же гауссова исключения (приведение исходной Vandermonde (k+m)×k к identity сверху, преобразования применяются ко всем строкам). Стабильность: конструирование детерминировано (без RNG).

- [ ] **Шаг 1. Тесты** `ReedSolomonCodecTests`:

```csharp
[Theory]
[InlineData(0, 3, 1)]      // 0 байт
[InlineData(1, 3, 1)]      // короче шард-блока
[InlineData(64, 3, 1)]     // ровно k шард-блоков по 64
[InlineData(1000, 3, 1)]   // с хвостом
[InlineData(512, 4, 2)]    // m=2: одновременная потеря 2 шардов — C(6,2)=15 мульти-потерь
[InlineData(1000, 6, 2)]   // m=2 при k+m=8: C(8,2)=28 мульти-потерь фактическим Reconstruct
public void RoundTrip_AllLossCombinations_UpToParity(int len, int k, int m)
{
    // Arrange
    var data = RandomBytes(len);
    var codec = new ReedSolomonCodec(k, m);
    var shards = Shard(data, k);            // нарезка по ceil(len/k) с нулевой добивкой
    var parity = new byte[m][];
    codec.Encode(shards, parity);
    var all = shards.Cast<byte?[]>().Concat(parity).ToArray();
    // Act — каждая комбинация потерь до m
    foreach (var lost in Combinations(k + m, m))
    {
        var damaged = (byte?[][])all.Clone();
        foreach (var i in lost) damaged[i] = null;
        codec.Reconstruct(damaged);
        // Assert — восстановленный поток равен исходным данным
        JoinShards(damaged, k, CeilDiv(len, k)).Should().Equal(data);
    }
}

[Fact]
public void Encode_IsSystematic_DataShardsUntouched()
{
    // Arrange/Act: копия data до Encode
    // Assert: data-шарды побайтово равны входу (после нулевой добивки хвоста)
}
```

Плюс кейсы: `Matrix_TopIsIdentity`, `Matrix_AnyKRowsIndependent` (ранг по GF всех C(k+m,k) подматриц — для k+m ≤ 8 перебором; проверяется линейной независимостью гауссовым исключением), `Matrix_StableAcrossInstances` (два инстанса — равные матрицы), `Reconstruct_TooFewShards_Throws`, `RoundTrip_K8M8` (крупный блочный прогон 1 МиБ, замер throughput в выводе — ориентир, не гейт: спека §7).

- [ ] **Шаг 2. Прогон — упал** (типа нет).
- [ ] **Шаг 3. Реализация**: гауссово исключение над GF (приведение Vandermonde), `Encode` — parity_i = Σ matrix[i][j] ⊗ data_j, `Reconstruct` — построение решающей матрицы из живых строк, обратная подстановка, запись восстановленных шардов.
- [ ] **Шаг 4. Прогон — зелёный.**
- [ ] **Шаг 5. Коммит**: `feat(owns3): систематический RS-кодек GF(2^8) (t40)`.

**Выход:** кодек для шардирования. **Проверка:** фильтр `~ReedSolomonCodec` зелёный. **Связь:** §4.1, §6.1, §2 п.4.

---

### Task 3: HighwayHash256 — порт + тест-векторы

**Спека:** §4.1 (`HighwayHash256`), §2 п.5, §6.1. **Вход:** независимо от Task 1–2.

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/HighwayHash256.cs`
- Create: `src/tests/OwnS3.UnitTests/Erasure/HighwayHashVectors.cs` (зафиксированные векторы)
- Test: `src/tests/OwnS3.UnitTests/Erasure/HighwayHash256Tests.cs`

**Interfaces (Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// Порт HighwayHash-256 (github.com/minio/highwayhash, portable-ветка без SIMD).
// Ключ — 32 байта. Используется для bitrot-хэшей шард-блоков (канон 06 §6).
public static class HighwayHash256
{
    // Магический ключ bitrot-контроля референса (cmd/bitrot.go,
    // magicHighwayHash256Key) — фиксирован, сверяется тестом с исходником.
    public static ReadOnlySpan<byte> MagicKey { get; }

    public static byte[] Hash(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data); // 32 байта
    public static byte[] HashStream(ReadOnlySpan<byte> key, Stream data);       // поток, 32 байта
}
```

`MagicKey` = 32 байта `4b e7 34 fa 8e 23 8a cd 26 3e 83 e6 bb 96 85 52 04 0f 93 5d a3 9f 44 14 97 e0 9d 13 22 de 36 a0` (прочитано из `/Users/demakaev/ZCodeProject/minio/cmd/bitrot.go:37`).

- [ ] **Шаг 1. Получить тест-векторы Go-реализации** (источник истины порта; НЕ выдумывать):
  - если доступен Go: `go run`-программа с `github.com/minio/highwayhash` из локального кэша модулей референса (или `go mod download github.com/minio/highwayhash`) — прогнать `Sum256` для: ключ `MagicKey` и ключ `00..1f`; входы: пусто, 1 байт `0x61`, 31 Б, 32 Б, 33 Б, 64 Б, 1000 Б случайных с seed 42, 5 МиБ константных `0x5a`;
  - если Go недоступен: взять векторы из `highwayhash_test.go` апстрима (WebFetch github.com/minio/highwayhash) и прогнать те же входы своим мини-скриптом нельзя — тогда зафиксировать в `HighwayHashVectors.cs` ровно апстримные векторы с комментарием-источником.
  - Результат — статический класс `HighwayHashVectors` со списком `(byte[] Key, byte[] Data, string ExpectedHex256)`.
- [ ] **Шаг 2. Тесты** `HighwayHash256Tests`: прогон всех векторов (`Hash(key, data)` → hex == expected; AAA-комментарии), `HashStream_EqualsHashForMemoryStream`, `Hash_EmptyInput` (входит в векторы), `MagicKey_MatchesReferenceBytes` (литерал 32 байта).
- [ ] **Шаг 3. Прогон — упал** (типа нет).
- [ ] **Шаг 4. Реализация порта**: состояние 4×(v0..v3) ulong из ключа, обработка 32-байтовых пакетов `Update` (permutation `ZipPermute`/микс-раунды по portable-ветке референса), хвост < 32 Б с добивкой, финализация 4 раунда `Finalize`. Внутри — только `ulong`-арифметика с rotate; SIMD не используется (критерий — корректность, скорость достаточна на блоке 1 МиБ; спека §7 — производительность не оптимизируется).
- [ ] **Шаг 5. Прогон — зелёный** (все векторы). Если расхождение — разбираться по раундам против Go-кода, НЕ подгоняя векторы.
- [ ] **Шаг 6. Коммит**: `feat(owns3): порт HighwayHash-256 + векторы Go (t40)`.

**Выход:** хэш для bitrot. **Проверка:** фильтр `~HighwayHash` зелёный на векторах референса. **Связь:** §4.1, §2 п.5, §6.1, канон 06 §6.

---

### Task 4: ErasureBlockWriter / ErasureBlockReader — стриминговое шардирование

**Спека:** §4.1, §4.2, канон 06 §4/§8. **Вход:** Task 2 + Task 3.

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/ErasureBlockWriter.cs`
- Create: `src/OwnS3.Storage/Erasure/ErasureBlockReader.cs`
- Test: `src/tests/OwnS3.UnitTests/Erasure/ErasureBlockRoundTripTests.cs`

**Interfaces:**

```csharp
namespace OwnS3.Storage.Erasure;

// Итог шардирования одной части (для xl.meta v2 и дигестов записи).
public sealed record ErasurePartResult(
    string ETagHex,           // MD5 ИСХОДНОГО потока части (hex, без кавычек)
    string Sha256Hex,         // SHA-256 ИСХОДНОГО потока части
    long SourceLength,        // исходная длина части
    int BlockCount,           // число erasure-блоков части
    byte[][][] ShardBlockHashes); // [shardIndex][blockIndex] — 32 байта HighwayHash(MagicKey);
                                  // полный 2D-набор ecN × blockCount, раскладка шард-мажор —
                                  // канон 06 §5 в правке Task 0; сериализуется в
                                  // XlMetaPartBitrot.BlockHashes (Task 7) без преобразования

// Запись: тело → k+m шард-потоков (позиция потока i = позиция диска i сета,
// ДО применения distribution — маппинг distribution[j] делает вызывающий,
// открывая потоки в нужном порядке). Копит bitrot-хэши per shard per block.
public sealed class ErasureBlockWriter : IDisposable
{
    public ErasureBlockWriter(ReedSolomonCodec codec, Stream[] shardStreams);
    public Task WriteAsync(Stream body, long expectedLength, CancellationToken ct); // длина != факт → XlIntegrityException
    public Task<ErasurePartResult> CompleteAsync(); // флаш всех потоков (без dispose)
}

// Чтение: сборка исходного потока из k+m шард-потоков (null = диск офлайн),
// bitrot-сверка каждого читаемого блока по хэшам из xl.meta; битый блок →
// реконструкция из k других; битые/офлайн позиций собирает ErasureBlockReader.
public sealed class ErasureBlockReader : IDisposable
{
    public ErasureBlockReader(ReedSolomonCodec codec, Stream?[] shardStreams,
        ErasurePartResult meta, long offset, long length); // Range-окно исходного потока
    public Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct);
    public IReadOnlyList<int> CorruptedShards { get; } // позиции с несовпавшим bitrot
}
```

Семантика блоков: блок b занимает в исходном потоке `[b*1МиБ, (b+1)*1МиБ)`; шард-блок j блока b — смещение `b*shardLen + …` в шард-потоке j, где `shardLen = ceil(blockSize/k)` для полных блоков и `ceil(tail/k)` для хвостового. `distribution` здесь НЕ применяется (маппинг потоков делает вызывающий по позициям дисков) — writer получает потоки уже в порядке шард-индексов. Выбор k живых источников блока — в порядке позиций (data-позиции 0..k−1 систематического кода раньше parity k..k+m−1) — этот инвариант обеспечивает приоритет data-позиций чтения (Task 9, канон 06 §8 п.2).

- [ ] **Шаг 1. Тесты** `ErasureBlockRoundTripTests`:

```csharp
[Theory]
[InlineData(0)]
[InlineData(1)]
[InlineData(1024 * 1024)]        // ровно блок
[InlineData(1024 * 1024 + 1)]    // блок + хвост
[InlineData(5 * 1024 * 1024 + 12345)]
public void RoundTrip_ProducesOriginalBytes(int len)
{
    // Arrange
    var data = RandomBytes(len);
    var codec = new ReedSolomonCodec(3, 1);
    var shardStreams = Enumerable.Range(0, 4).Select(_ => new MemoryStream()).ToArray();
    // Act
    ErasurePartResult result;
    using (var w = new ErasureBlockWriter(codec, shardStreams.Cast<Stream>().ToArray()))
    {
        await w.WriteAsync(new MemoryStream(data), data.Length, CancellationToken.None);
        result = await w.CompleteAsync();
    }
    var readers = shardStreams.Select(s => new MemoryStream(s.ToArray())).Cast<Stream?>().ToArray();
    var assembled = new MemoryStream();
    using (var r = new ErasureBlockReader(codec, readers, result, 0, data.LongLength))
        await r.CopyToAsync(assembled);
    // Assert
    assembled.ToArray().Should().Equal(data);
    result.Sha256Hex.Should().Be(Sha256Hex(data));
}
```

Плюс кейсы: `RoundTrip_WithOfflineShard_Reconstructs` (один null-поток), `Read_RangeWindow_ReturnsSlice` (offset/length внутри), `BitrotCorruption_DetectedAndReconstructed` (порча байта в одном шард-потоке: чтение сходится, `CorruptedShards` содержит позицию), `TwoCorruptedShards_BeyondParity_Throws` (m=1, две порчи → `XlIntegrityException`), `Writer_LengthMismatch_Throws`.

- [ ] **Шаг 2. Прогон — упал.** **Шаг 3. Реализация** (writer: буфер блока 1 МиБ, нарезка на k, добивка нулями, `Encode`, запись шард-блоков, инкрементальные MD5/SHA-256 исходника, HighwayHash каждого шард-блока; reader: чтение нужных блоков Range-окна, сверка bitrot, реконструкция, отдача окна). **Шаг 4. Прогон — зелёный.**
- [ ] **Шаг 5. Коммит**: `feat(owns3): стриминговый erasure-кодер/декодер блоков (t40)`.

**Выход:** блочный слой кодирования. **Проверка:** фильтр `~ErasureBlockRoundTrip` зелёный. **Связь:** §4.1 `ErasureBlockWriter/Reader`, §4.2, канон 06 §4/§8.

---

### Task 5: IXlDrive + SingleXlDrive — рефакторинг размещения (поведение не меняется)

**Спека:** §4.1 (`XlDrive`, `SingleXlDrive`), §2 п.9, §5 фаза 3. **Вход:** чистая ветка; страховка — существующие серии.

**Файлы:**
- Create: `src/OwnS3.Storage/Drives/IXlDrive.cs` (интерфейс + `IXlStaging` + `PartDigest`)
- Create: `src/OwnS3.Storage/Drives/SingleXlDrive.cs` (перенос механики из `XlObjectStore`/`XlVolume`/`BucketMetaStore`/`ListWalker`)
- Modify: `src/OwnS3.Storage/XlObjectStore.cs` и все partial: `.Objects.cs`, `.Copy.cs`, `.Buckets.cs`, `.List.cs`, `.Multipart.cs`
- Modify: `src/OwnS3.Storage/MultipartJournals.cs` — класс и записи `UploadJournalEntry`/`PartJournalEntry` переводятся из `internal` в `public`: записи входят в сигнатуры public-интерфейса `IXlDrive` (`ReadUploads`/`WriteUploads`/`ReadParts`) — иначе CS0050, а public-члены внутри internal-класса — CS0053 (`TreatWarningsAsErrors` превратит их в ошибки сборки); доступность типов — часть контракта размещения
- Modify: `src/OwnS3.Storage/ListWalker.cs` (курсорная механика остаётся, источник — через IXlObjectCursor)
- Modify: `src/OwnS3.App/Program.cs`, `src/OwnS3.App/Pipeline/VolumeCleanupService.cs` (DI и чистки — через IXlDrive)
- Test: существующие юниты/интеграции — без правок кейсов (мелкие механические правки доступа допустимы только при неизменных ассертах)

**Interfaces (Produces — точный контракт, зафиксирован планом):**

```csharp
namespace OwnS3.Storage;

// Дайджест ИСХОДНОГО потока части (single — из файла; EC — из кодера).
public readonly record struct PartDigest(string ETagHex, string Sha256Hex, long Length);

// Staging одной готовящейся записи (opaque для XlObjectStore).
public interface IXlStaging : IDisposable { } // Dispose = удалить staging-каталоги

// Ленивый курсор ключей бакета в Utf8ByteOrder (свёртка/пагинация — ListWalker).
public interface IXlObjectCursor
{
    string? PeekKey { get; }
    void MoveNext();
    XlMetaRecord ReadCurrentMeta(); // мета ключа PeekKey (битая — исключение XlIntegrityException/FileNotFoundException)
}

// Способ размещения (механика — канон 04; EC-отличия — канон 06).
// Реализации: SingleXlDrive (глава 04), ErasureXlDrive (глава 06).
public interface IXlDrive
{
    // ── жизненный цикл тома ──
    void Initialize();                                   // 04 §3 / 06 §2 + старт-чистки
    Task RunCleanupAsync(CancellationToken ct);          // 04 §6 / 06 §10
    bool CheckHealth();                                  // 05 §5: touch-проба

    // ── бакеты (04 §1 / 06 §8) ──
    bool BucketExists(string bucket);
    void CreateBucket(string bucket);                    // каталоги + bucket.json
    void DeleteBucket(string bucket);                    // emptiness-проверка + trash
    IReadOnlyList<BucketEntry> ListBuckets();            // 06 §8: EC — консенсус по любому кворумному сету

    // ── объекты (04 §2/§4 / 06 §8) ──
    XlMetaRecord? TryReadMeta(string bucket, string key); // null — объекта нет; кворумная недоступность — ObjectStoreException(Insufficient*)
    IXlStaging CreateStaging();
    Task<PartDigest> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct);       // single: тело → part.N; EC: шардирование
    void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey,
        XlMetaRecord srcMeta);                           // Copy: побайтовый перенос частей (single: part.N; EC: шардовые part.N)
    void WriteMeta(IXlStaging staging, XlMetaRecord record); // single: xl.meta v1; EC: v2 per-disk (EcIndex диска)
    PutResult Commit(string bucket, string key, XlMetaRecord record, IXlStaging staging); // rename-схема 04 §4 (EC: по-дисково + кворум)
    Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length);
    void VerifyFullChecksum(string bucket, string key, XlMetaRecord meta); // full-SHA исходных данных
    void Delete(string bucket, string key, XlMetaRecord meta);  // trash меты+данных (EC: кворум)
    IReadOnlyList<(int Number, long Size)> EnumerateParts(string bucket, string key,
        XlMetaRecord meta);                              // исходные размеры частей (GetObjectAttributes)

    // ── листинги (04 §1 / 06 §7–8) ──
    IXlObjectCursor EnumerateObjects(string bucket);     // лениво, Utf8ByteOrder (EC: merge сетов + дедуп)

    // ── multipart-журналы (04 §5 / 06 §8) ──
    List<MultipartJournals.UploadJournalEntry> ReadUploads(string bucket, string key);
    void WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads);
    bool UploadDirExists(string bucket, string key, string uploadId);
    void CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord);
    void TrashUpload(string bucket, string key, string uploadId);
    List<MultipartJournals.PartJournalEntry> ReadParts(string bucket, string key, string uploadId);
    IXlStaging CreateUploadStaging(string bucket, string key, string uploadId); // staging части ВНУТРИ загрузки
    void CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size,
        DateTimeOffset modTime);                         // rename части + upsert parts.json (EC: по-дисково + кворум)
    CompleteResult CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord); // attempt.json + перенос частей + xl.meta + зачистка
}
```

`ObjectStoreErrorCode` — пока без изменений (Insufficient*-коды добавит Task 8). Типы `MultipartJournals.UploadJournalEntry`/`PartJournalEntry` — public вместе с классом `MultipartJournals` (см. Файлы: CS0050/CS0053 public-контракта).

- [ ] **Шаг 1. Ввести типы** `IXlDrive`/`IXlStaging`/`PartDigest`/`IXlObjectCursor` (файл `Drives/IXlDrive.cs`); сборка безварнинговая (типы пока никем не используются).
- [ ] **Шаг 2. `SingleXlDrive` + перенос бакетной зоны**: класс держит `XlVolume` и `BucketMetaStore`; методы Buckets-группы — перенос текущих тел из `XlObjectStore.Buckets.cs` (проверка emptiness — в `DeleteBucket`, включая блокирующие multipart-загурзки). `XlObjectStore.Buckets.cs` — вызовы через `IXlDrive`. Прогон: `dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~XlObjectStoreBucket` — зелёный.
- [ ] **Шаг 3. Перенос объектной зоны**: staging/`WritePartAsync`(текущая `CopyBodyToFileAsync`)/`WriteMeta`/`Commit`(схема `CommitStagedObjectAsync`+`MoveIntoExistingTarget` + `_commitLock`)/`TryReadMeta`/`OpenData`(MultipartBodyStream)/`VerifyFullChecksum`/`Delete`/`EnumerateParts`(`EnumerateDataParts`) — в `SingleXlDrive`; `XlObjectStore.Objects.cs` — логика conditional/Range поверх drive. Прогон: юниты `~XlObjectStoreObject` + интеграции `dotnet test src/tests/OwnS3.IntegrationTests -c Release` — зелёные.
- [ ] **Шаг 4. Перенос Copy-зоны**: `CopyPartsToStaging` (хардлинк-точка/фолбэк — текущий `CopyPart`+`HardLinkProbe`); `XlObjectStore.Copy.cs` — conditional/лимиты/директивы меты поверх drive. Прогон `~XlObjectStoreCopy` + интеграции.
- [ ] **Шаг 5. Перенод листинговой зоны**: `DirectoryCursor`/`IKeyCursor` остаются; `SingleXlDrive.EnumerateObjects` отдаёт `IXlObjectCursor` поверх сегодняшнего `DirectoryCursor`; `ListWalker.Walk` меняет чтение меты на `cursor.ReadCurrentMeta()` и источник корня бакета — через drive. Прогон `~XlObjectStoreList` + интеграции.
- [ ] **Шаг 6. Перенос multipart-зоны**: журнальные методы + `CommitMultipart` (текущее тело `CompleteMultipartUploadAsync` после шага валидации манифеста; `CompletePreCommitProbe`-хук сохранить); `XlObjectStore.Multipart.cs` — валидации/идемпотентность/visibility поверх drive. Прогон `~XlObjectStoreMultipart` + `~MultipartCleanup` + интеграции.
- [ ] **Шаг 7. App-DI**: `Program.cs` регистрирует `SingleXlDrive` (единственная реализация), `XlObjectStore` принимает `IXlDrive`; `VolumeCleanupService` — `IXlDrive.RunCleanupAsync` + дисковые gauge от корня single. healthz — `drive.CheckHealth()`.
- [ ] **Шаг 8. Полная страховочная серия** (последовательная, с контролем зачистки между): юниты OwnS3 → интеграции OwnS3 → docker-E2E t39 (`PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~E2e`). Все зелёные БЕЗ правок кейсов.
- [ ] **Шаг 9. Коммит**: `refactor(owns3): размещение за IXlDrive, single-путь в SingleXlDrive (t40)`.

**Выход:** контракт размещения; single-поведение идентично. **Проверка:** все три серии зелёные; `git diff` не трогает ассерты существующих тестов. **Связь:** §4.1 (`XlDrive`/`SingleXlDrive`), §2 п.9, §5 фаза 3, §6.2 последний пункт.

---

### Task 6: volume.json v2 + ErasurePool (SIPMOD, online-трекинг)

**Спека:** §4.1 (`ErasurePool`), §4.2, §6.1; канон 06 §2–3. **Вход:** Task 5 (для интеграции позже) и Task 0 (арх-правки внесены); формально пул не зависит от Task 5 — реализуется в `Erasure/` без привязки к IXlDrive.

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/ErasureTopology.cs`
- Create: `src/OwnS3.Storage/Erasure/ErasureVolumeFormat.cs` (модель+IO `volume.json` v2)
- Create: `src/OwnS3.Storage/Erasure/ErasurePool.cs` (+ `ErasureDisk`)
- Modify: `src/OwnS3.Storage/OwnS3.Storage.csproj` — `PackageReference Include="System.IO.Hashing"` (версия уже в `Directory.Packages.props`)
- Test: `src/tests/OwnS3.UnitTests/Erasure/ErasureVolumeFormatTests.cs`, `ErasurePoolTests.cs`

**Interfaces (Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// Топология из конфига (канон 06 §1): N путей, setSize 4–16, parity 1..setSize/2.
public sealed record ErasureTopology(int DriveCount, int SetSize, int Parity)
{
    public int SetCount => DriveCount / SetSize;        // требует N % SetSize == 0
    public int DataShards => SetSize - Parity;
    public static int DefaultSetSize(int driveCount);   // наибольший делитель из {16,8,4}; 0 — нет делителя
}

// Модель volume.json v2 (camelCase-JSON, канон 06 §2).
internal sealed record ErasureVolumeInfo(string Magic, int FormatVersion, string VolumeId, string Mode,
    string DeploymentId, string DiskId, int SetSize, string[][] Sets, int[] Distribution, string DistributionAlgo);

public sealed class ErasureDisk
{
    public string Root { get; }            // путь позиции
    public int SetIndex { get; }           // s = position / setSize
    public int Position { get; }           // позиция в списке путей
    public int PositionInSet { get; }      // j = position % setSize
    public string DiskId { get; internal set; }
    public bool Online { get; }            // каталог доступен и I/O жив; перепроверка тикером
}

// Пул сетов (канон 06 §2–3, §7): Initialize по правилам старта; SIPMOD;
// трекинг online (лениво + тикер 30 с); замена диска (06 §9).
public sealed class ErasurePool : IDisposable
{
    public ErasurePool(IReadOnlyList<string> roots, ErasureTopology topology,
        TimeProvider timeProvider, ILogger? logger = null);
    public void Initialize();                       // fail-fast диагностика → InvalidOperationException
    public Guid DeploymentId { get; }
    public ErasureTopology Topology { get; }
    public IReadOnlyList<ErasureDisk> Disks { get; }
    public IReadOnlyList<IReadOnlyList<ErasureDisk>> Sets { get; }
    public int PickSetIndex(string bucket, string key); // SIPMOD: SipHash(deploymentId-ключ,
                                                         // "bucket/object") % SetCount (канон 06 §3)
    public void RefreshOnline();                   // перепроверка всех дисков (вызывается тикером 30 с и heal)
    public IDisposable StartOnlineTicker();        // фон 30 с (канон 06 §7)
}
```

`PickSetIndex`: `System.IO.Hashing.SipHash.Hash` (ключ — 16 байт `DeploymentId.ToByteArray()[..16]`, данные — UTF-8 `"<bucket>/<object>"`), ulong → `% SetCount`. Distribution — детерминированная перестановка `0..setSize−1` из deploymentId (при создании: seed = SipHash(deploymentId, "distribution"), Fisher–Yates по остаткам от SipHash-потока; правило зафиксировано кодом раз навсегда — важно лишь детерминированное воспроизведение при валидации между дисками: в файле хранится готовая перестановка, воспроизводить не нужно).

Правила `Initialize()` (канон 06 §2 + правка Task 0):
- все пути без `volume.json` И пусты (или не существуют) → создание формата на каждом (общие deploymentId/volumeId/distribution, свои diskId, sets по позициям) + создание полной служебной структуры `.owns3.sys/{tmp,multipart,.trash,buckets,config}` на каждом диске (параллель `XlVolume.Initialize` в single, канон 06 §2 последний пункт);
- любой путь с посторонним содержимым без формата → отказ;
- живой том: эталонный формат — с любого диска с валидным `volume.json`; на каждой позиции: путь отсутствует → диск offline (в сет, без замены); пустой каталог → кандидат замены; валидный `volume.json` с ожидаемым diskId → ок; diskId вне sets на своей позиции / чужой magic / версия новее / несовпадение setSize/deploymentId/distributionAlgo между дисками → отказ всего сервиса;
- замена слота возможна ТОЛЬКО при живых валидных (старых) дисках сета ≥ k (`topology.DataShards`): живых < k при наличии кандидатов замены — отказ старта (fail-fast: при живых < k реконструировать не из чего, а при k == m writeQuorum = k+1 недостижим — данные сета терялись бы безвозвратно); одновременно заменяемых ≥ k — тоже отказ старта (спека §4.2, канон 06 §9); при допустимости — замена: новый diskId, кворумное обновление `sets` на онлайн-дисках (06 §9 — замена выполняется при старте, heal потом доливает);
- служебная структура до-создаётся идемпотентно на каждом онлайн-диске живого тома (как `XlVolume.Initialize`); конфиг-секция `config/` — каталог на всех дисках, содержимое как в single (сегодня фактически пустое; материализация файла ключей — вне scope t40, существующее поведение single не меняется);
- изменение числа путей против `sets` формата → отказ.

- [ ] **Шаг 1. Тесты** `ErasureVolumeFormatTests`: round-trип записи/чтения v2 (camelCase-поля); битый JSON/чужой magic/версия 3 → ясные исключения.
- [ ] **Шаг 2. Тесты** `ErasurePoolTests` (temp-каталоги, `Path.GetTempPath`+guid; teardown удаляет):

```csharp
[Fact]
public void Initialize_AllEmpty_CreatesFormatOnEveryDisk()
{
    // Arrange: 4 temp-пути
    // Act: pool.Initialize()
    // Assert: на каждом диске валидный volume.json v2, один deploymentId, разные diskId,
    // sets[0] == 4 diskId по позициям, distribution — перестановка 0..3;
    // на каждом диске создана структура .owns3.sys/{tmp,multipart,.trash,buckets,config}
}

[Fact]
public void Initialize_ExistingThenRestartOnePathMissing_DiskOfflineNoReplace()
{
    // Arrange: созданный пул; путь диска 3 удалён
    // Act: новый pool.Initialize()
    // Assert: диск 3 offline, diskId НЕ выдан новый (замены нет — правка Task 0)
}
```

Плюс кейсы: `Initialize_ForeignContentWithoutFormat_FailsFast`, `Initialize_ForeignerMagic_Fails`, `Initialize_NewerFormatVersion_Fails`, `Initialize_DiskIdOutsideSets_Fails`, `Initialize_PathCountChanged_Fails`, `Initialize_EmptyDirCandidate_ReplacesDiskIdWhenAliveGeK` (живых 3 ≥ k=3: новый diskId, `sets` обновлён на онлайн-дисках), `Initialize_EmptyCandidatesGeK_Fails` (заменяемых одновременно ≥ k — отказ), `Initialize_AliveBelowK_CandidatePresent_Fails` (setSize=4, k=3, m=1: 2 живых валидных + 2 пустых кандидата — заменяемых 2 < k, но живых 2 < k → отказ старта), `PickSetIndex_StableForSameKeyAndDeployment`, `PickSetIndex_UniformAcrossRandomKeys` (1000 ключей, 2 сета: отклонение долей < 0.1), `PickSetIndex_DependsOnDeploymentId`, `Distribution_IsPermutationOfRange`, `Topology_DefaultSetSize` (N=4→4, N=8→8, N=16→16, N=32→16, N=12→4, N=5→0).

- [ ] **Шаг 3. Прогон — упал → реализация → зелёный** (`dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~ErasurePool`).
- [ ] **Шаг 4. Коммит**: `feat(owns3): формат тома v2 и пул сетов (SIPMOD, online-трекинг) (t40)`.

**Выход:** пул для `ErasureXlDrive`. **Проверка:** фильтры `~ErasurePool`/`~ErasureVolumeFormat` зелёные. **Связь:** §4.1, §4.2, §6.1, канон 06 §2–3.

---

### Task 7: xl.meta RecordVersion 2

**Спека:** §4.1 (`XlMetaFile` v2), §6.1; канон 06 §5 + правка Task 0. **Вход:** Task 0 (арх-правка внесена).

**Файлы:**
- Modify: `src/OwnS3.Storage/XlMetaFile.cs`
- Modify: `src/OwnS3.Storage/Domain/DomainTypes.cs` (только если типы EC размещать там — иначе всё в XlMetaFile.cs)
- Test: `src/tests/OwnS3.UnitTests/Storage/XlMetaFileTests.cs` (дополнение)

**Interfaces (Produces):**

```csharp
// EC-часть записи v2 (канон 06 §5; EcIndex — позиция диска этой копии).
public sealed record XlMetaEcInfo(string EcAlgo, byte EcM, byte EcN, uint EcBSize, byte EcIndex,
    byte[] EcDist, IReadOnlyList<XlMetaPartBitrot> Parts);

// Bitrot-запись части: исходный размер + полный 2D-набор хэшей (канон 06 §5 в правке Task 0).
public sealed record XlMetaPartBitrot(long SourceSize, byte[][][] BlockHashes);
// BlockHashes[shardIndex][blockIndex] — 32 байта; shardIndex 0..ecN-1, раскладка шард-мажор

// XlMetaRecord расширяется свойством (записи v1 читаются с Ec == null):
public sealed record XlMetaRecord(...поля v1..., XlMetaEcInfo? Ec = null);
```

Побайтовая схема v2 (формат-запись: magic `OWS3`, formatVersion 1, recordVersion 2; канон 06 §5 в правке Task 0): v1-поля, затем `string ecAlgo` ("reedsolomon"), `uint8 ecM`, `uint8 ecN`, `uint32 ecBSize`, `uint8 ecIndex`, `uint8 distCount` + distCount×`uint8 ecDist`, `uint16 partCount`, на часть по возрастанию part.N: `uint64 sourceSize`, `uint32 blockCount`, затем для каждого шард-индекса `j = 0..ecN−1` — `blockCount × 32` Б хэшей (полный 2D-набор, раскладка шард-мажор `BlockHashes[shardIndex][blockIndex]`; размер bitrot-секции части — `8 + 4 + ecN × blockCount × 32` Б). Читатель: recordVersion 1 → v1-запись (EC-полей нет, `Ec == null`); 2 → v2; иное → `XlIntegrityException`. Писатель: `Ec != null` → v2, иначе v1 (single-том v2 не пишет — спека §4.1).

- [ ] **Шаг 1. Тесты** (дополнение `XlMetaFileTests`): `RoundTrip_V2` (запись с полным EcInfo → чтение → эквивалентность всех полей, включая полный 2D-набор хэшей), `RoundTrip_V2_PreservesShardMajorLayout` (ecN=4, blockCount=2 → 8 хэшей: после round-trip `BlockHashes[shard][block]` раскладка побайтово воспроизведена — уникальное значение на каждую пару), `Write_V1_ReadableByNewReader` (`Ec == null`), `Write_NullEc_WritesV1` (байты записи содержат recordVersion 1), `Read_V2Truncated_Throws` (обрыв в bitrot-хвосте → `XlIntegrityException`), `Read_V2ResidualBytes_Throws`, `RoundTrip_EmptyBlocks` (объект size 0: partCount 1, blockCount 0 — канон 06 §4 «пустой объект»).
- [ ] **Шаг 2. Прогон — упал → реализация (расширение Serialize/Parse) → зелёный** (`--filter FullyQualifiedName~XlMetaFile`).
- [ ] **Шаг 3. Коммит**: `feat(owns3): xl.meta запись v2 — EC-поля и bitrot-хэши (t40)`.

**Выход:** формат меты EC. **Проверка:** фильтр `~XlMetaFile` зелёный (старые + новые). **Связь:** §4.1, §6.1, канон 06 §5.

---

### Task 8: ErasureXlDrive — запись и кворумы (503-коды)

**Спека:** §4.1 (`ErasureXlDrive`), §4.3, §6.1 (кворумы); канон 06 §7–8. **Вход:** Task 4, 5, 6, 7.

**Файлы:**
- Create: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs` (создаётся в этом таске с зонами записи; зоны чтения/buckets/listing/multipart добавляются Task 9–11)
- Create: `src/OwnS3.Storage/Erasure/ErasureQuorum.cs`
- Modify: `src/OwnS3.Storage/ObjectStoreErrorCode.cs` (+ `InsufficientReadQuorum`, `InsufficientWriteQuorum`)
- Modify: `src/OwnS3.Protocol/Errors/S3ErrorCode.cs` (+ те же кода, статус 503, сообщения по главе 02 «Недоступность кворума»: "The read quorum of the erasure set is not available" / "...write quorum...")
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (`MapStorageCode`: два новых кейса)
- Test: `src/tests/OwnS3.UnitTests/Erasure/ErasureQuorumTests.cs`, `src/tests/OwnS3.UnitTests/Storage/ErasureXlDriveWriteTests.cs`

**Interfaces (Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// Кворумы канона 06 §7.
public static class ErasureQuorum
{
    public static int Read(int k, int m) => k;
    public static int Write(int k, int m) => k == m ? k + 1 : k;
}

namespace OwnS3.Storage;

// Драйв EC (канон 06): фасад над ErasurePool; объектные операции — на сете
// PickSetIndex(bucket, key); бакетные/листинги — на всех сетах (Task 10).
public sealed partial class ErasureXlDrive : IXlDrive
{
    public ErasureXlDrive(ErasurePool pool, int defaultParity, TimeProvider timeProvider,
        ILogger<ErasureXlDrive> logger);
    // defaultParity — env-дефолт для НОВЫХ объектов (per-object parity, канон 06 §1)
}
```

Механика записи (канон 06 §8): `ErasureStaging : IXlStaging` — на каждом онлайн-диске сета `tmp/<guid>/`; служебная структура `.owns3.sys/{tmp,multipart,.trash,buckets,config}` на каждом диске гарантирована `ErasurePool.Initialize` (Task 6: создание при создании тома, идемпотентное до-создание на старте живого тома; `config/` — каталог на всех дисках с содержимым как в single, сегодня пустым) — драйв опирается на неё: `tmp` — staging, `.trash` — откаты и удаления, `multipart` — журналы, `buckets` — bucket.json. `WritePartAsync` открывает шард-потоки `part.N` на позициях `distribution[j]` (шард j → диск `distribution[j]`), пишет через `ErasureBlockWriter`; staging хранит `Dictionary<int, ErasurePartResult>` по частям, наружу отдаёт проекцию `PartDigest(r.ETagHex, r.Sha256Hex, r.SourceLength)`; `WriteMeta` пишет `xl.meta` v2 per-disk (свой `EcIndex`); `Commit` — по-дисковые rename staging→bucket-дерево (схема 04 §4 на каждом диске независимо), успехов ≥ writeQuorum → `PutResult`; меньше → откат staging в `.trash/` на всех дисках + `ObjectStoreException(InsufficientWriteQuorum)`. Перезапись: старый dataDir → `.trash/` per-disk после кворума. Зоны интерфейса, реализуемые позже (чтение — Task 9, бакеты/листинги — Task 10, multipart — Task 11, полные чистки — Task 12), в этом таске добавляются методами с `throw new NotImplementedException()` — это инкрементальная заглушка между соседними Task, полностью заменяемая к концу Task 11 (чистки — Task 12); финальная сборка задач их не содержит.

- [ ] **Шаг 1. Тесты кворумов** `ErasureQuorumTests`: `(k=3,m=1)→(3,3)`, `(k=2,m=2)→(2,3)`, `(k=8,m=8)→(8,9)`, `(k=15,m=1)→(15,15)` (AAA).
- [ ] **Шаг 2. Тесты записи** `ErasureXlDriveWriteTests` (4 temp-корня, topология 4/1):

```csharp
[Fact]
public async Task Put_CommitAppearsOnAllOnlineDisks_MetaV2WithPerDiskEcIndex()
{
    // Arrange: пул 4 диска, initialized; staging-запись тела 2.5 МиБ
    // Act: drive.Commit(...)
    // Assert: bucket-каталог объекта есть на всех 4 дисках; xl.meta каждого диска:
    //         recordVersion 2, Ec.EcIndex == позиции диска; part.1 каждого диска —
    //         шардовый поток (суммарный размер шардов ceil-раскладки); полный 2D-набор
    //         bitrot-хэшей (ecN=4 × blockCount) одинаков во всех копиях записи
}

[Fact]
public async Task Put_WithOneOfflineDisk_CommitsWithQuorum_ObjectsReadable()
{
    // Arrange: диск 3 удалён (offline, Task 6-семантика)
    // Act: запись
    // Assert: успех (3 успеха ≥ writeQuorum 3); данные на 3 дисках
}
```

Плюс кейсы: `Put_OnlineBelowWriteQuorum_ThrowsInsufficientWriteQuorum` (2 диска offline → `ObjectStoreException` с кодом `InsufficientWriteQuorum`; staging в `.trash`), `Put_ZeroLength_EmptyShardFiles` (part.1 нулевой длины на всех дисках, blockCount 0), `Put_TailBlock_PaddedOnlyInMemory` (размеры шардовых файлов = ceil-раскладка хвоста), `WritePartAsync_DigestsAreOfOriginalStream` (ETag/SHA — исходного тела), `MapStorageCode_NewCodes_MapTo503` (юнит S3Middleware-маппинга или через каталог `S3ErrorCatalog`: статус 503).

- [ ] **Шаг 3. Прогон — упал → реализация → зелёный** (`--filter FullyQualifiedName~ErasureXlDriveWrite`).
- [ ] **Шаг 4. Коммит**: `feat(owns3): ErasureXlDrive — шардированная запись с кворумом (t40)`.

**Выход:** запись EC с 503-семантикой. **Проверка:** фильтры зелёные; `dotnet build src/OwnS3.Storage -c Release` 0 warnings. **Связь:** §4.1, §4.3, §6.1, канон 06 §7–8.

---

### Task 9: ErasureXlDrive — чтение (консенсус, реконструкция, bitrot, Range)

**Спека:** §4.1, §6.2 (деградация host-FS); канон 06 §7–8. **Вход:** Task 8.

**Файлы:**
- Modify: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/ErasureXlDriveReadTests.cs`

Механика: `TryReadMeta` — сбор `xl.meta` с онлайн-дисков сета; мажоритарный `VersionId` с порогом ≥ k: нет записей при онлайне ≥ k → null (объекта нет); есть записи, но максимум мажоритарности < k → `InsufficientReadQuorum`; онлайн < k → `InsufficientReadQuorum`. Битые записи (структура) не считаются. `OpenData(bucket, key, meta, offset, length)` — шард-потоки `part.N` онлайн-дисков по позициям `EcDist` записи; выбор k источников каждого блока — ПРИОРИТЕТ data-позиций (позиции 0..k−1 систематического кода: их шард-блоки читаются как есть), parity-позиции добираются только при офлайн/битых data — блок без потерь data-позиций не декодируется вовсе (канон 06 §8 п.2; без приоритета корректно, но каждый блок шёл бы через декодер — медленнее); `ErasureBlockReader` с bitrot-сверкой и реконструкцией; ЭК-поля записи фиксируют k/m/bSize/dist объекта (parity per-object). `VerifyFullChecksum` — полный проход декодирования против `ContentSha256` (несовпадение → `XlIntegrityException`). `EnumerateParts` — из `Ec.Parts` (sourceSize; канон Task 0).

- [ ] **Шаг 1. Тесты** `ErasureXlDriveReadTests`: `Get_FullObject_EqualsOriginal` (round-trip после Commit), `Get_Range_ReturnsSlice`, `Get_OneDiskOffline_Reconstructs`, `Get_BitrotCorruption_ReadsAndFlagsShard` (порча байта part.1 одного диска: чтение сходится), `Get_BitrotCorruption_HealableShardDetected` (помечен для heal — через CorruptedShards/лог), `TryReadMeta_NoRecordsOnlineQuorum_ReturnsNull`, `TryReadMeta_OnlineBelowReadQuorum_Throws503`, `Get_OnlineBelowReadQuorum_ThrowsInsufficientReadQuorum` (онлайн 2 < k=3), `VerifyFullChecksum_Mismatch_Throws`, `EnumerateParts_ReturnsSourceSizes` (multipart-объект из 2 частей — собирается CommitMultipart из Task 11, до него кейс через ручную сборку staging: помечен `[Fact(Skip = "до Task 11")]` — снимается в Task 11).
- [ ] **Шаг 2. Прогон — упал → реализация → зелёный.**
- [ ] **Шаг 3. Коммит**: `feat(owns3): чтение EC — консенсус меты, реконструкция, bitrot, Range (t40)`.

**Выход:** чтение EC. **Проверка:** фильтр `~ErasureXlDriveRead` зелёный. **Связь:** §4.1, §6.2, канон 06 §7–8.

---

### Task 10: ErasureXlDrive — delete, бакеты, листинги, чистки-каркас

**Спека:** §4.1, канон 06 §8 (Delete/Бакеты/листинги), §7 (кворум листингов). **Вход:** Task 9.

**Файлы:**
- Modify: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/ErasureXlDriveBucketsListTests.cs`

Механика: `Delete` — rename в `.trash/` per-disk + кворум ≥ writeQuorum (недоступные при Delete диски выравниваются при возврате: живое-в-консенсусе доливается heal — Task 13; фантом-записи, не актуальные в консенсусе сета, удаляются чистками по порогу с локальной кворумной сверкой отсутствия — Task 12 и канон 06 §10 в правке Task 0; идемпотентность — `TryReadMeta == null` → успех на уровне `XlObjectStore`). `CreateBucket` — каталоги + `bucket.json` на дисках ВСЕХ сетов, кворум ≥ writeQuorum каждого сета; `DeleteBucket` — emptiness по всем сетам (объекты/живые загрузки), trash per-disk; `ListBuckets` — консенсус по любому сету с read-кворумом (все сеты < k → 503). `EnumerateObjects(bucket)` — предусловие: во ВСЕХ сетах онлайн ≥ k, иначе `InsufficientReadQuorum` (канон 06 §7 — неполный листинг опаснее недоступности); обход: merge `DirectoryCursor`-курсоров корней бакета онлайн-дисков каждого сета (куча по PeekKey, Utf8ByteOrder), дедуп по ключу, мета — `ReadCurrentMeta` с первого диска нахождения (битая на нём — warning, пробуется следующий диск сета). `RunCleanupAsync` — каркас: перебор дисков, чистка best-effort (полная реализация per-disk — Task 12). `CheckHealth` — формат пула валиден + touch-проба на каждом онлайн-диске (спека §4.3).

- [ ] **Шаг 1. Тесты**: `Delete_RemovesOnAllDisks_Quorum`, `Delete_BelowWriteQuorum_Throws503`, `CreateBucket_DirsAndMetaOnEveryDiskOfAllSets` (2 сета: 8 путей, setSize 4), `CreateBucket_OneSetDegradedBeyondWrite_Throws`, `DeleteBucket_NotEmpty_ThrowsBucketNotEmpty` (объект в другом сете), `ListBuckets_ConsensusFromQuorumSet`, `EnumerateObjects_AllSetsRequired_ThrowsWhenAnySetBelowK`, `EnumerateObjects_UnionAcrossSets_DedupsKeys` (объекты в разных сетах — один листинг, Utf8ByteOrder), `EnumerateObjects_BrokenMetaOnOneDisk_EmitsFromHealthyDisk`.
- [ ] **Шаг 2. Прогон — упал → реализация → зелёный.**
- [ ] **Шаг 3. Коммит**: `feat(owns3): EC — delete/бакеты/листинги с межсетевыми кворумами (t40)`.

**Выход:** бакетно-листинговая зона EC. **Проверка:** фильтр `~ErasureXlDriveBucketsList` зелёный. **Связь:** §4.1, §6.1, канон 06 §7–8.

---

### Task 11: Multipart в EC

**Спека:** §4.1 (`ErasureXlDrive` — журналы multipart), §5 фаза 7; канон 06 §8 (Multipart). **Вход:** Task 10.

**Файлы:**
- Modify: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs`
- Test: `src/tests/OwnS3.UnitTests/Storage/ErasureXlDriveMultipartTests.cs`

Механика: каталоги загрузок и `uploads.json`/`parts.json`/`attempt.json` — на дисках сета ключа (`KeyDir` от корня диска), журналы пишутся tmp+rename на все онлайн-диски, читаются с любого успешного (рассинхрон выравнивается heal — Task 13); xl.meta загрузки — v1 (не объект). Части: `CreateUploadStaging` → staging на дисках сета внутри `tmp/`; `WritePartAsync` шардирует часть (ETag/size — исходные, в `parts.json`); `CommitUploadPart` — rename части в каталог загрузки per-disk + upsert `parts.json` (кворум ≥ writeQuorum). `CommitMultipart`: `attempt.json` (dataDir-маркер, идемпотентность повтора — глава 04 §5 per-disk) → по-дисковые rename частей `uploadDir/part.N` → `dataDir/part.N` целевого объекта → `xl.meta` v2 (per-disk EcIndex) → кворумный коммит → зачистка журнала/каталога на дисках. Composite ETag — из журнала (исходные etag частей); SHA-256 итога — проходом `ErasureBlockReader` по частям (декодирование, исходный поток).

- [ ] **Шаг 1. Тесты**: `Multipart_FullLifecycle_ObjectsEqualOriginal` (2 части по 6 МиБ + Complete → чтение равно исходнику; многочастевой объект: dataDir содержит part.1/part.2 шардовые), `Multipart_PartsJournalOnAllOnlineDisks`, `UploadPart_ETagOfOriginalPartBody`, `Complete_IdempotentRetry_SameResult` (повтор Complete — тот же dataDir/versionId), `Complete_BelowWriteQuorum_Throws503` (деградация до 2), `Abort_TrashesUploadOnAllDisks`, `ListParts_ReadsFromAnySuccessfulDisk` (журнал читается при одном offline). Снять `[Fact(Skip="до Task 11")]` из Task 9.
- [ ] **Шаг 2. Прогон — упал → реализация → зелёный** (`~ErasureXlDriveMultipart`).
- [ ] **Шаг 3. Коммит**: `feat(owns3): EC multipart — журналы на сетах, кворумный Complete (t40)`.

**Выход:** полный IXlDrive-контракт EC. **Проверка:** фильтр зелёный; весь `~ErasureXlDrive` зелёный. **Связь:** §4.1, §5 фаза 7, канон 06 §8.

---

### Task 12: Чистки per-disk

**Спека:** §5 фаза 7, §4.1; канон 06 §10. **Вход:** Task 11.

**Файлы:**
- Create: `src/OwnS3.Storage/XlDiskJanitor.cs` — выделение чисток из `XlVolume` (WipeTmp/CleanAged/CleanupAbandonedUploads) в переиспользуемый класс корня диска
- Modify: `src/OwnS3.Storage/XlVolume.cs` — делегирует джанитору (поведение single не меняется)
- Modify: `src/OwnS3.Storage/Drives/ErasureXlDrive.cs` — `RunCleanupAsync` per-disk
- Test: `src/tests/OwnS3.UnitTests/Storage/VolumeCleanupServiceTests.cs` (существующие — зелёные без правок кейсов), Create: `src/tests/OwnS3.UnitTests/Storage/ErasureCleanupTests.cs`

Механика (канон 06 §10, включая правку Task 0): пороги те же (tmp — только на старте; `.trash`/orphan — 1 ч; брошенные загрузки — 24 ч); orphan-dataDir сверяется с `xl.meta` ТОГО ЖЕ диска (локальная сверка); фантом-записи (правка Task 0): ключ с локально валидной `xl.meta`, не актуальный в консенсусе сета (кворумно удалён при offline-диске / недозакоммичен крахом записи), — локальная кворумная сверка отсутствия: `xl.meta` того же ключа читается на других онлайн-дисках сета внутрипроцессно (без кворумных операций); запись отсутствует на ≥ k онлайн-дисках И локальная запись старше 1 ч (mtime `xl.meta`) → каталог ключа rename в `.trash/`; кворум отсутствия недостижим (онлайн-дисков без записи < k) → ключ консервативно сохраняется до ближайшей сверки с кворумом; чистки не конкурируют с heal (heal доливает живое-в-консенсусе-отсутствующее-на-диске, чистка удаляет незакоммиченное/устаревшее локально и не актуальное в консенсусе — фантомы).

- [ ] **Шаг 1. Тесты** `ErasureCleanupTests`: `Startup_WipesTmpOnEveryDisk`, `AgedTrashRemoved_PerDisk`, `OrphanDataDir_RemovedWhenNotInLocalMeta` (Guid-каталог без упоминания в локальной xl.meta, старше 1 ч — FixedTimeProvider), `OrphanReferencedByLocalMeta_Kept`, `AbandonedUploads_24h_PerDisk`, `PhantomRecord_AbsentQuorumOnSet_Removed` (ключ с валидной локальной метой на диске 4; на дисках 1–3 записи нет, все онлайн — кворум отсутствия 3 ≥ k; mtime состарен → чистка устранила каталог ключа с диска 4), `PhantomRecord_PresentInConsensus_Kept` (запись есть на 3 других дисках — ключ жив, чистка не тронула), `PhantomRecord_NoAbsenceQuorum_Kept` (онлайн-дисков без записи < k — консервативно сохранён).
- [ ] **Шаг 2. Прогон — упал → рефакторинг-выделение джанитора → реализация EC → зелёный**; контрольная серия: `--filter FullyQualifiedName~VolumeCleanup` + `~MultipartCleanup` (single не сломан).
- [ ] **Шаг 3. Коммит**: `feat(owns3): per-disk чистки EC через общий джанитор (t40)`.

**Выход:** чистки обоих режимов одной механикой. **Проверка:** фильтры зелёные. **Связь:** §5 фаза 7, канон 06 §10, §6.2 single-зелёность.

---

### Task 13: Heal + ErasureStatusProvider

**Спека:** §4.1 (`ErasureHealService`, `ErasureStatusProvider`), §5 фаза 8; канон 06 §9. **Вход:** Task 11 (12 — желательно до).

**Файлы:**
- Create: `src/OwnS3.Storage/Erasure/ErasureHeal.cs` (ядро прохода — `ErasureHealService`)
- Create: `src/OwnS3.Storage/Erasure/ErasureStatusProvider.cs`
- Create: `src/OwnS3.App/Pipeline/ErasureHealHostedService.cs` (периодическая обёртка, прецедент `VolumeCleanupService`)
- Test: `src/tests/OwnS3.UnitTests/Storage/ErasureHealTests.cs`

**Interfaces (Produces):**

```csharp
namespace OwnS3.Storage.Erasure;

// Фоновый heal-проход (канон 06 §9): по сетам → листинг → по-дисковая сверка →
// дозаписи tmp+rename. Старт+период — hosted-обёрткой; внеплановый запуск —
// RequestPass(): идущий проход продолжается, параллельный НЕ создаётся.
public sealed class ErasureHealService(ErasurePool pool, ErasureXlDrive drive,
    ErasureStatusProvider status, TimeProvider timeProvider, ILogger<ErasureHealService> logger)
{
    public Task RunPassAsync(CancellationToken ct);
    public void RequestPass();          // POST /admin/heal
    public bool IsRunning { get; }
}

// Состояние для /admin/storage и метрик (канон 05 §5/§7).
public sealed class ErasureStatusProvider(ErasurePool pool)
{
    public ErasureStorageStatus GetStatus(); // режим/сеты/диски/деградация/heal-статистика
}
public sealed record ErasureStorageStatus(string Mode, IReadOnlyList<ErasureSetStatus> Sets,
    ErasureHealStatus Heal);
public sealed record ErasureSetStatus(int Index, bool Readable, bool Writable,
    IReadOnlyList<ErasureDriveStatus> Drives);
public sealed record ErasureDriveStatus(int Position, string Path, string DiskId, bool Online);
public sealed record ErasureHealStatus(bool Running, DateTimeOffset? LastPassAt,
    long HealedObjects, long HealedShards);
```

Проход: для каждого сета — листинг ключей (курсор Task 10) + multipart-загрузки; на объект: консенсусная мета; по-дисково: отсутствие dataDir/`xl.meta` → реконструкция шарда декодером + мета-копия с корректным EcIndex (tmp+rename); bitrot-несовпадение шарда → перезапись реконструированным; биткая мета → копия консенсусной; отсутствующие части/журналы загрузок — аналогично. Скан `.trash/`/orphan не выполняется — канон 06 §9 в правке Task 0: heal реконструирует только живое по консенсусу; `.trash/`/orphan удаляются чистками по порогу (§10) — разделение ответственности heal/чистки без дубля реконструкции/удаления. Статистика: healedObjects/healedShards. Период hosted-сервиса — 10 минут (обоснование: чаще чисток 15 мин — возврат к полноте не медленнее чисток; чаще — лишний IO).

- [ ] **Шаг 1. Тесты** `ErasureHealTests` (topология 4/1, temp-корни): `Heal_MissingShardOnOneDisk_Restored` (удалён dataDir-файл диска → pass → файл восстановлен, содержимое идентично), `Heal_BitrotShard_Rewritten` (порча байта → pass → хэш совпал), `Heal_BrokenMeta_RestoredFromConsensus` (обрыв xl.meta → pass → читается), `Heal_OfflineDiskSkipped_NoError`, `Heal_MissingJournalEntries_Reconstructed`, `RequestPass_DuringRunningPass_DoesNotDuplicate` (IsRunning + один проход), `Heal_EmptyObject_Restored` (size 0), `Status_ReflectsDegradation` (Readable/Writable при 1 offline).
- [ ] **Шаг 2. Прогон — упал → реализация → зелёный** (`~ErasureHeal`).
- [ ] **Шаг 3. Коммит**: `feat(owns3): heal-проход и статус-провайдер EC (t40)`.

**Выход:** автоматическое восстановление полноты. **Проверка:** фильтр зелёный. **Связь:** §4.1, §5 фаза 8, §6.2 (деградация→heal), канон 06 §9.

---

### Task 14: App-интеграция (env, DI, админ-грань, метрики, healthz)

**Спека:** §4.3, §5 фаза 9; канон 05 §4/§5/§7. **Вход:** Task 13.

**Файлы:**
- Modify: `src/OwnS3.App/OwnS3Options.cs` (+ `EcOptions SetSize/Parity`)
- Modify: `src/OwnS3.App/Program.cs` (env `OWNS3_EC_SET_SIZE`/`OWNS3_EC_PARITY`, парсинг списка путей, валидации fail-fast, DI-развилка, hosted-сервисы)
- Create: `src/OwnS3.App/Pipeline/OwnS3EcMetrics.cs`
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (ветки `/admin/storage`, `/admin/heal`)
- Create: `src/OwnS3.App/Handlers/AdminHandlers.cs`
- Modify: `src/OwnS3.App/Pipeline/VolumeCleanupService.cs` (через IXlDrive; EC-дисковые gauge — сумма по дискам)
- Modify: `src/tests/OwnS3.IntegrationTests/Api/OwnS3AppFactory.cs` — снять `sealed` с класса (базовый класс для `OwnS3EcAppFactory` этого Task и деградационной фабрики Task 15; ассерты и поведение single-серии не меняются)
- Test: `src/tests/OwnS3.UnitTests/App/OwnS3EcOptionsTests.cs` (парсинг/валидации), `src/tests/OwnS3.IntegrationTests/Api/AdminScenarios.cs`, `src/tests/OwnS3.IntegrationTests/Api/ErasureModeScenarios.cs`

Механика: `OwnS3Options` дополняется `EcOptions { int? SetSize; int Parity = 2 }` (дефолт parity 2 — канон 05 §4; SetSize null = авто). `OWNS3_DATA_DIR` → `OwnS3:DataDir` строка; парсинг: split `','`, trim; пустой элемент или дубликат → отказ старта (до Kestrel, лог Critical + exit 1 — по образцу сегодняшней volume-валидации). 1 путь → single-развилка DI (`SingleXlDrive` над `XlVolume`); ≥ 2 → `ErasureTopology` (setSize: env или `DefaultSetSize`, нет делителя → отказ; валидации N ≥ 2, N % setSize == 0, setSize 4–16, parity 1..setSize/2) → `ErasurePool.Initialize()` + `ErasureXlDrive` + `ErasureHealService` + `ErasureStatusProvider` + hosted `ErasureHealHostedService`. Админ-грань: `S3Middleware` до роутинга 21 операций перехватывает `GET /admin/storage` / `POST /admin/heal` — те же шаги подписи SigV4 (`S3Authenticator`), права `admin`/root (прочим 403 `AccessDenied`); single-режим (грань доступна всегда — канон 05 §7): `GET /admin/storage` — статус single-тома без сетов, `POST /admin/heal` — 202 Accepted, no-op (heal существует только в EC-режиме). Ответ `GET /admin/storage`:

```json
{"mode":"XL Erasure","sets":[{"index":0,"readable":true,"writable":true,
 "drives":[{"position":0,"path":"/data1","diskId":"...","online":true}]}],
 "heal":{"running":false,"lastPassAt":"2026-10-11T09:00:00Z","healedObjects":0,"healedShards":0}}
```

`POST /admin/heal` → 202 (+`RequestPass`). Метрики (только EC-режим; имена — канон 05 §5, конвенция `OwnS3Metrics` — точки только в инструменте Meter, серии экспорта через подчёркивания): `ownS3_ec_drive_online{set,drive}`, `ownS3_ec_set_readable{set}`, `ownS3_ec_set_writable{set}` — observable-gauge от `ErasureStatusProvider` (обновление по тику 30 с онлайн-трекера); `ownS3_heal_passes_total`, `ownS3_heal_objects_total` — counters heal-сервиса. `/healthz` — `drive.CheckHealth()` (EC: формат валиден + touch каждого онлайн-диска; деградация НЕ роняет healthz — спека §4.3).

- [ ] **Шаг 1. Юниты `OwnS3EcOptionsTests`**: парсинг списка (валидный/пустой элемент/дубликат), дефолт setSize (4→4, 12→4, 32→16, 5→отказ), диапазоны parity, отказ N < 2.
- [ ] **Шаг 2. Интеграции** (WAF-фабрика с EC-конфигом — новая `OwnS3EcAppFactory : OwnS3AppFactory` — база наследуется после снятия `sealed` (см. Файлы), override in-memory конфиг: `OwnS3:DataDir = "d1,d2,d3,d4"` (4 temp-подкаталога фабрики), `OwnS3:Ec:SetSize=4`, `OwnS3:Ec:Parity=1`; коллекция `OwnS3Ec`): `AdminScenarios` — `GET /admin/storage` 200 + схема (root/admin ключом; read-write ключом — 403), `POST /admin/heal` 202, `Healthz_EcMode_200`; single-ветка на существующей single-фабрике: `AdminEdge_SingleMode_StorageIsSingle_HealIs202NoOp` (`GET /admin/storage` → режим `XL Single`, без сетов; `POST /admin/heal` → 202 no-op — канон 05 §7).
- [ ] **Шаг 3. Прогон — упал → реализация → зелёный** (`dotnet test src/tests/OwnS3.UnitTests -c Release --filter FullyQualifiedName~OwnS3EcOptions`; `dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~Admin`).
- [ ] **Шаг 4. Коммит**: `feat(owns3): app-интеграция EC — env/DI/админ-грань/метрики/healthz (t40)`.

**Выход:** EC-режим поднимается приложением. **Проверка:** фильтры зелёные. **Связь:** §4.3, §5 фаза 9, канон 05 §4/§5/§7.

---

### Task 15: Контрактовые интеграции EC (21 операция) + деградация host-FS

**Спека:** §6.2. **Вход:** Task 14.

**Файлы:**
- Modify: `src/tests/OwnS3.IntegrationTests/Api/*.Scenarios.cs` — выделение общей логики кейсов в базовые классы (параметризация режима; существующие single-кейсы остаются зелёными без правок ассертов)
- Create: `src/tests/OwnS3.IntegrationTests/Api/ErasureContractScenarios.cs` (EC-наследники контрактовых серий на `OwnS3Ec`-коллекции)
- Create: `src/tests/OwnS3.IntegrationTests/Api/ErasureDegradationScenarios.cs`

Подход: каждый сценарный класс сегодня привязан к `[Collection("OwnS3")]` и `OwnS3AppFactory`. Рефакторинг: логика кейсов — в `partial`-базу с абстрактным доступом к клиенту/фабрике (например, `ApiScenarioBase` с `OwnS3TestClient Client` и `OwnS3AppFactory Factory`); single-классы объявляют коллекцию single, EC — коллекцию `OwnS3Ec`. Кейсы (AAA): полный контракт 21 операции (все существующие сценарные серии: Access/Auth/BodyIntegrity/ErrorFormat/Multipart/ObjectStorage/Routing) в EC-топологии 4/1.

`ErasureDegradationScenarios` (та же фабрика EC, своя 4-папочная DataDir на класс — per-class том, чтобы деградация не влияла на контрактовую серию; это делает отдельная коллекция `OwnS3EcDegr` с фабрикой-наследником):
- `OfflineDisk_ReadReconstruction` (сохранить копию каталога диска 3 → удалить → GET/Head/Range/листинги живы; байты равны);
- `OfflineDisk_WriteStillWorks` (PUT/UploadPart/Complete при 3 онлайн);
- `ReturnDisk_HealRestores` (вернуть каталог → POST /admin/heal → `GET /admin/storage` полнота; данные целы);
- `Bitrot_ReadAliveThenHeal` (порча байта в part-файле диска → GET равен исходнику (реконструкция) → heal → bitrot-хэш восстановлен);
- `TwoDisksLost_503Not404` (2 диска offline: GET/PUT/ListObjects → 503 `InsufficientReadQuorum`/`InsufficientWriteQuorum`; НЕ 404);
- `Metrics_EcSeriesPresent` (`/metrics` содержит `ownS3_ec_set_readable`, `ownS3_heal_passes_total` после heal).

- [ ] **Шаг 1. Рефакторинг параметризации** — single-серии зелёные ПОСЛЕ каждого перемещённого кейса (прогон по серии).
- [ ] **Шаг 2. EC-контрактовая серия**: `dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter Collection=OwnS3Ec` — зелёная (все контрактовые кейсы в EC-топологии).
- [ ] **Шаг 3. Деградационная серия**: `--filter Collection=OwnS3EcDegr` — зелёная.
- [ ] **Шаг 4. Контрольная single-серия**: полный `dotnet test src/tests/OwnS3.IntegrationTests -c Release` — всё зелёное.
- [ ] **Шаг 5. Коммит**: `test(owns3): контракт 21 операции и деградация в EC-топологии (t40)`.

**Выход:** контракт подтверждён в EC на host-FS. **Проверка:** фильтры зелёные; зачистка серий (`docker ps -a --filter name=os3-` пуст — host-FS docker не использует, temp-каталоги удаляет фабрика). **Связь:** §6.2.

---

### Task 16: Docker-E2E EC-серия

**Спека:** §6.3; каноны `docs/e2e-isolation.md`, `docs/e2e-launch.md`. **Вход:** Task 15.

**Файлы:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/OwnS3EcE2eEnvironment.cs`
- Create: `src/tests/OwnS3.IntegrationTests/E2e/ErasureScenarios.cs`

`OwnS3EcE2eEnvironment` — по образцу `OwnS3E2eEnvironment` (тот же контракт телеметрии: `[PHASE]`, phases.log, CollectDiagnosticsAsync, MarkFailed, own-only teardown, ассерт чистоты, retry ×3): сеть `os3-en-{runId}`, контейнер `os3-es-{runId}` (образ `owns3:e2e` — статическая сборка переиспользуется), 4 тома `os3-ev{1..4}-{runId}`, env `OWNS3_DATA_DIR=/data1,/data2,/data3,/data4`, `OWNS3_EC_SET_SIZE=4`, `OWNS3_EC_PARITY=1`, динамический хост-порт, wait `/healthz` ≤ 30 с. Специфика EC: `Task RestartWithVolumesAsync(params int[] online)` — стоп+rm контейнера, новый контейнер с подмножеством монтирований (остальные тома живут в docker — данные сохраняются); `Task ReplaceVolumeEmptyAsync(int idx)` — `docker volume rm` + новый пустой том; teardown чистит ВСЕ 4 тома с ретраями (~30 с, полл 2 с) + сеть + ассерт чистоты по runId.

Кейсы (`ErasureScenarios`, каждый — своё окружение `RunScenarioAsync`-обёрткой; AWS SDK/mc — существующие клиенты `OwnS3E2eClients`):
- `Erasure_BasicCycle`: PUT/GET/Head/Range/листинги/multipart/копия против EC-контура; ETag/conditional — семантики single (переиспользование клиентских шагов t39-сценариев);
- `Erasure_DegradedRestart`: записи → рестарт с томами 1–3 (4-й detached — путь отсутствует, диск offline, правка Task 0) → GET равен исходнику, PUT нового объекта жив; `ownS3_ec_set_*` метрики отражают деградацию (`/metrics`: `ownS3_ec_drive_online{...,drive="3"} 0`);
- `Erasure_QuorumUnavailable`: рестарт с томами 1–2 (онлайн 2 < k=3) → GET/PUT/ListObjects → 503 `InsufficientReadQuorum`/`InsufficientWriteQuorum`;
- `Erasure_VolumeReturn`: после деградированного рестарта — рестарт с 4 томами (данные на месте) → `POST /admin/heal` → `/admin/storage` полнота, все данные читаются, метрики зелёные;
- `Erasure_DiskReplace`: записи → 4-й том подменён пустым (volume rm + новый) → рестарт со всеми 4 → старт с заменой (`/admin/storage`: новый diskId) → heal доливает → все данные читаются;
- `Erasure_AdminEdge`: `/admin/storage` 200 root/admin (режим `XL Erasure`, деградация корректна после detached-рестарта); `POST /admin/heal` 202; read-write ключ → 403 на обоих.

- [ ] **Шаг 1. Окружение** `OwnS3EcE2eEnvironment` + smoke-кейс `Erasure_BasicCycle`; прогон `PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~Erasure_BasicCycle` (фон; контроль по task_id; таймауты поллинга ≤ 30 с) — зелёный; проверка зачистки (`docker ps -a --filter name=os3-` пуст, `docker volume ls --filter name=os3-` пуст).
- [ ] **Шаг 2. Кейсы деградации/кворума/возврата/замены** — по одному прогону с анализом логов между прогонами (артефакты `/tmp/pgw-e2e-artifacts-<guid>/`); упавший кейс НЕ перезапускается без анализа (правила телеметрии).
- [ ] **Шаг 3. Контрольные single-E2E**: `PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~E2e` — вся серия (18 Facts t39 + новая EC-серия) зелёная; зачистка после.
- [ ] **Шаг 4. Коммит**: `test(owns3): docker-E2E erasure-серия — деградация/возврат/замена/админ-грань (t40)`.

**Выход:** EC подтверждён реальными клиентами в докере. **Проверка:** серия зелёная, ассерты чистоты в каждом teardown. **Связь:** §6.3, критерий §6.4 п.2–3.

---

### Task 17: Поставка — compose-профиль owns3-ec и чек 47

**Спека:** §4.4; канон 05 §8. **Вход:** Task 14 (образ тот же — можно параллельно с 15–16).

**Файлы:**
- Modify: `deploy/docker-compose.yml` (+ сервис `owns3-ec`, профиль `owns3-ec`; тома `owns3-ec-data-1..4`)
- Modify: `deploy/.env.example` (+ `OWNS3_EC_HOST_PORT=9011` с комментарием)
- Create: `dev-stand/adminpanel/checks/47-owns3-ec.sh`

Compose-сервис (канон 05 §8): тот же `image: owns3:dev` (та же build-секция — свой `build` не дублируется: `extends` не использовать, просто вторая build-секция с теми же аргументами, как у существующих дублей образа; если compose ругается на пересборку — допустимо опустить build и полагаться на образ, поднятый сервисом `owns3`; выбрать вариант, при котором `--profile owns3-ec` поднимается на уже собранном образе), `profiles: ["owns3-ec"]`, порты `${OWNS3_EC_HOST_PORT:-9011}:9000`, env — та же секция OWNS3-секретов + `OWNS3_DATA_DIR=/data1,/data2,/data3,/data4`, `OWNS3_EC_SET_SIZE=4`, `OWNS3_EC_PARITY=1`, тома `owns3-ec-data-1:/data1` … `owns3-ec-data-4:/data4`. В volumes-секцию добавить 4 именованных тома.

Чек `47-owns3-ec.sh` — по модели `46-owns3.sh`: порт из `deploy/.env` (`OWNS3_EC_HOST_PORT`, дефолт 9011); проверки:
1. `curl /healthz` → 200 (без подписи).
2. `GET /admin/storage` SigV4-root: чек вычисляет подпись AWS SigV4 в bash на `openssl dgst -sha256/-hmac` (canonical request `GET\n/admin/storage\n\nhost:...\nx-amz-date:...\n` + цепочка HMAC `AWS4`-деривации — стандартный компактный паттерн ~30 строк, самодостаточный, без внешних зависимостей); ассерты: HTTP 200, тело содержит `"mode":"XL Erasure"` и `"online":true`; read-write-ключом — 403 (вторая подпись тем же паттерном).
3. mc-smoke против хост-публикации: alias root → mb → cp(128 КиБ /dev/urandom) → cat(sha256 == источник) → stat → скачивание(cmp) → rm → rb (по кейсам чека 46).
4. `/metrics` содержит `ownS3_ec_set_readable{set="0"} 1` и `ownS3_disk_total_bytes`.
Временный бакет `os3ec-$(date +%s)`, trap-чистка при любом исходе, образ `minio/mc:RELEASE.2025-08-13T08-35-41Z` (уже в images.txt), docker run с `--add-host host.docker.internal:host-gateway`.

- [ ] **Шаг 1. compose + env-шаблон**; локальная проверка синтаксиса: `docker compose -f deploy/docker-compose.yml config --quiet`.
- [ ] **Шаг 2. Чек-скрипт**; ручная проверка на стенде (позже — Task 18).
- [ ] **Шаг 3. Коммит**: `feat(deploy): профиль owns3-ec (4 тома 3+1) и чек 47 (t40)`.

**Выход:** стенд-контур EC. **Проверка:** `docker compose config` валиден; чек исполруем (`bash -n`). **Связь:** §4.4, канон 05 §8, §6.4 п.6.

---

### Task 18: Финальный прогон всех серий + сверка канона

**Спека:** §6.4. **Вход:** все предыдущие.

- [ ] **Шаг 1. Стенд**: полный подъём `dev-stand/adminpanel/checks/00-up.sh`, затем профиль `docker compose -f deploy/docker-compose.yml --profile owns3-ec up -d owns3-ec`; чек `dev-stand/adminpanel/checks/47-owns3-ec.sh` — зелёный; чек `46-owns3.sh` — зелёный (single не задет).
- [ ] **Шаг 2. Серии Release последовательно** (после каждой — финальная строка прогона и зачистка: `docker ps -a --filter name=os3-` пуст, `docker volume ls --filter name=os3-` пуст, `docker network ls | grep os3-` пуст):
  1. `dotnet test src/tests/OwnS3.UnitTests -c Release` — зелёный, 0 warnings;
  2. `dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName!~E2e` — зелёный;
  3. `PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~E2e` — зелёный (t39 + EC);
  4. регресс-контроль монорепо: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings (сборка достаточно; OwnS3 не трогает воркеров, полный прогон воркер-серий — вне задачи).
- [ ] **Шаг 3. Сверка канона**: перечитать `arch/owns3/06-erasure.md` §1–11 против реализации; каждое расхождение — arch-first правка (маленькая) или правка кода; результат — расхождений нет. Регулярные сверки-маркеры: §7 в редакции правки Task 0 — листинги объектов и бакетные мутации Create/Delete требуют онлайн ≥ k во всех сетах (503 на весь запрос), ListBuckets — консенсус по любому кворумному сету (§8); реализация Task 10 соответствует этому разделению. §5↔§6 — размерность bitrot-хэшей (2D ecN × blockCount на часть, шард-мажор); реализация Task 7/4 соответствует.
- [ ] **Шаг 4. Коммит финальных правок** (если появились): `chore(owns3): сверка канона 06 и кода (t40)`.

**Выход:** все критерии приёмки §6.4 подтверждены прогонами. **Проверка:** журналы серий зелёные; зачистки выполнены. **Связь:** §6.4 п.1–6.

---

## Правила исполнения (каждому субагенту-исполнителю)

1. Один Task = один субагент/сеанс; перед началом — прочитать спеку, этот план (свой Task + Global Constraints), указанные в Task файлы.
2. TDD: тест раньше реализации, прогон «упал → реализация → зелёный» обязателен; тесты — AAA-комментарии. Кейсы, перечисленные в Task списком (без развёрнутого кода), реализуются каждым отдельным `[Fact]`/`[Theory]` точно по описанному сценарию: Arrange/Act/Assert следуют из формулировки кейса; имя метода — как в списке.
3. Все команды — только в фоне (`run_in_background: true`), контроль по task_id, ожидания ≤ 30 с.
4. После КАЖДОЙ docker-серии — зачистка и проверка чистоты; упавшие E2E не перезапускать без анализа логов.
5. Коммит в конце Task; сообщения — по образцу `feat(owns3): ... (t40)`.
6. Сомнение в трактовке канона/спеки — НЕ выбирать самостоятельно: эскалация координатору (вопрос пользователю).
