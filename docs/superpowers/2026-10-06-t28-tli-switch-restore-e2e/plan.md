# t28-tli-switch-restore-e2e — план реализации

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять план задача за задачей. Шаги отмечаются чекбоксами (`- [ ]`).

**Цель:** docker-E2E контур, доказывающий замкнутый цикл восстанавливаемости через точку TLI-переключения в двух режимах restore (latest + PITR target_time) на ОДНОЙ WAL-цепочке и ОДНОМ контуре сценарного класса.

**Архитектура:** один тестовый файл `E2eTliSwitchScenarios.cs`: класс-фикстура `TliSwitchContext` (IClassFixture) выполняет ВЕСЬ Arrange/Act-подготовку один раз (контур → полный на TLI1 → гейт replica-sync → docker stop мастера → данные TLI2 → строгий+побайтовый гейт смешанности), два `[Fact]` кооперируются на готовом контуре в детерминированном порядке (latest → target_time, orderer). Продуктовый код НЕ трогается.

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 3.2.2 (`IClassFixture`, `ITestCaseOrderer` из `Xunit.v3` с ограничением `notnull, Xunit.Sdk.ITestCase`, `Assert.Skip`), Npgsql, testcontainers (через `E2eEnvironment`), FluentAssertions, AWSSDK.S3 (транзитивно через `PgWorker.Backups` — байтовый GET объекта сегмента).

**Spec:** [`docs/superpowers/2026-10-06-t28-tli-switch-restore-e2e/spec.md`](spec.md) — план аргументируется от спека; исполнители читают оба документа. (Редакция r4: r2 — оба S3-поля env = `Fx.S3Endpoint`; r3 — диагностический дамп гейта + `[PHASE]`-телеметрия; r4 — синхронизация с правкой spec после SPEC_DEVIATION Фазы 6: механика имени смешанного объекта — под именем TLI РОДИТЕЛЯ `00000001…S` (`SegmentAssembler.Close` берёт TLI из long page header границы), побайтовое подтверждение по page-заголовкам, ОБЯЗАТЕЛЬНЫЙ гейт replica-sync до docker stop.)

## Статус реализации (на момент редакции r4)

- **Task 1 (каркас) — РЕАЛИЗОВАН**, коммит `2ba7445`.
- **Task 2 (failover-ветка) — РЕАЛИЗОВАН в старой редакции**, коммит `2352165`; ПЛЮС в рабочем дереве НЕЗАКОММИЧЕН implements-довесок кодера: гейт replica-sync в `PrepareAsync` (сразу после шага 2, до `FailoverAsync`) + хелпер `ReplicasSyncedAsync` — доводится до spec Шагом 2.7 (бюджет ≤ 60 с).
- В рабочем дереве НЕЗАКОММИЧЕНЫ также: `AssertMixedSwitchAsync` (try/catch-обёртка с дампом — актуальна) + `AssertMixedSwitchCoreAsync` (ассерты СТАРОЙ редакции §3.3 — «объект под именем TLI2», «TLI1-версии S нет» — НЕВЕРНЫ против новой механики имени родителя), `HistoryEntryAsync`/`ParseLsn` (актуальны), `[PHASE] tlsw-ready` (актуален). Task 3 переформулирует Core-ассерты и добавляет побайтовый хелпер — оставшаяся работа кодера: довести незакоммиченное до этого плана, не переписывая с нуля.
- **Task 4, Task 5 — не начаты** (Fact'ы в файле — каркасные тела Ф1/Ф2).

## Глобальные ограничения (действуют на каждую задачу)

- Продуктовый код (`src/PgWorker.*` вне тестового дерева), контракт etcd, S3-layout, restore-механика, `WalChain`/`WalHistory`/`WalFileName` — НЕ меняются; правки только в `src/tests/PgWorker.IntegrationTests` (spec §5). Побайтовый разбор ограничен page-заголовками (`xlp_tli`/`xlp_pageaddr`, шаг 8192) — полный декодер XLOG-записей НЕ пишется (spec §5).
- Каноны E2E обязательны: [`docs/e2e-isolation.md`](../../../docs/e2e-isolation.md) (guid-окружение, own-only чистка, teardown при любом исходе + ассерт чистоты, динамические порты) и [`docs/e2e-launch.md`](../../../docs/e2e-launch.md) (телеметрия: `[PHASE]`-строки, сбор docker-логов при фазе > 60 с, `MarkFailed` → teardown останавливает не удаляя, перезапусков упавших нет).
- Истина доставки — S3: все гейты доставки поллингом `ListWalAsync` host-клиента, не по etcd-ключу (spec §2).
- Порты docker — только динамические (`E2eEnvironment` делает это сам); хардкодов-литералов вида `:16000` в тесте нет.
- Комментарии и документация — русский; идентификаторы — английский; тесты — с AAA-комментариями.
- `TreatWarningsAsErrors=true` — код без warnings (включая xunit-анализаторы).
- Любые команды прогона (сборка/тесты, > 30 с) исполнитель запускает ТОЛЬКО в фоне (`run_in_background: true`) и следит по task_id; после КАЖДОЙ тестовой серии — дожидаться финальной строки и зачищать остатки контейнеров/сетей/томов (AGENTS.base.md §4, §12).
- Коммиты — в feature-ветке `feat-t28-tli-switch-restore-e2e` (worktree), стиль репо: `test(e2e): t28 — …` (русское пояснение).
- Рабочая директория команд — корень worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t28-tli-switch-restore-e2e`.

## Структура файлов

| Файл | Ответственность | Статус |
|---|---|---|
| `src/tests/PgWorker.IntegrationTests/E2e/E2eTliSwitchScenarios.cs` | Всё: `TliSwitchFactOrderer`, `TliSwitchContext : IAsyncLifetime` (подготовка контура + все хелперы), `E2eTliSwitchScenarios : IClassFixture<TliSwitchContext>` (два Fact'а). Файл независим, хелперы — копии образцов (spec §2) | существует; Task 3–4 доводят |
| `arch/19-backups.md:844` (мерж-гейт) | Строка риска §10 «Смешанный сегмент…»: закрытие через существующий E2E-контур | Task 5 |
| `arch/roadmap/reliability.md:91-95` (мерж-гейт) | Удалить пункт `t28-tli-switch-restore-e2e` | Task 5 |
| `arch/roadmap/reliability-report.md` (мерж-гейт) | Строка t28 в раздел «Сделано» + сводка характеристики D | Task 5 |

Ключевые сигнатуры (сверены с кодом):
- `PgWorker.Backups.BackupS3` (`IBackS3`): `ListWalAsync(cluster, shard, ct:)` → `Result<IReadOnlyList<WalObject>>`; `GetObjectAsync(cluster, shard, key, ct)` → `Result<string>` — ТЕКСТОВЫЙ (StreamReader UTF8): годится для `.history`, НЕ годится для байтов WAL-сегмента; байтовый GET — прямым `AmazonS3Client` (`AmazonS3Config { ServiceURL, ForcePathStyle }` + `BasicAWSCredentials`, паттерн ctor'а `BackupS3`; AWSSDK.S3 доступен тестам транзитивно через ProjectReference на `PgWorker.Backups`).
- `PgWorker.Backups.WalHistory.Parse(string)` → `IReadOnlyList<WalHistoryEntry>?`; `WalHistoryEntry(uint ParentTli, string SwitchLsn)`.
- `PgWorker.Backups.WalFileName`: `TryParse(name)`, `FromLsn(uint tli, string lsn)`, `.Name`, `.Next()`, `.Log`, `.Seg`, `.Tli`, `const long SegmentBytes` (16 МиБ).
- `PgWorker.Backups.WalChain.Check(WalFileName chainStart, IEnumerable<string> objectNames)` → `ChainResult(IsContinuous, GapError, LastSegment)`.
- `RestoreOperationState(Id, State, BackupId, Source, Target, Node, RequestedUnix, RequestedBy, …)` — `BackupId` ТРЕТИЙ параметр; `RestoreStatusJson.Serialize(op)` (namespace `PgWorker.Backups.Restore`); `PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id)`.
- XLog-страница (postgres `XLogPageHeaderData`, C-выравнивание): `xlp_tli` = LeU32 @ +4, `xlp_pageaddr` = LeU64 @ +8; шаг страницы 8192 (`XLOG_BLCKSZ`); WAL и `BitConverter` — little-endian.
- xunit.v3 3.2.2: `Xunit.v3.ITestCaseOrderer.OrderTestCases<TTestCase>(IReadOnlyCollection<TTestCase>)` с ограничением `where TTestCase : notnull, Xunit.Sdk.ITestCase`; имя метода — `t.TestMethodName` напрямую (`ITestCase : ITestCaseMetadata`).

Команда прогона контура (далее «прогон класса»):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t28-tli-switch-restore-e2e && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~E2eTliSwitchScenarios"
```

Зачистка после КАЖДОГО прогона (страховочный гейт; дожидаться финальной строки прогона прежде, чем запускать):

```bash
docker ps -a --format '{{.Names}}' --filter name=pgw- | grep -E 'tlsw|pgw-(en|ee|em)-' || echo "контейнеров-остатков нет"
docker network ls --format '{{.Names}}' --filter name=pgw- | grep -E 'pgw-(en|kfw-net)' || echo "сетей-остатков нет"
docker volume ls --format '{{.Names}}' --filter name=pgw- || echo "томов-остатков нет"
docker network prune -f
```

Если команды нашли имена — удалить: контейнеры `docker rm -f <имёна>`, тома `docker volume rm -f <имёна>` (только остатки тестовых прогонов; сети — только осиротевшие `pgw-en-*`; живой dev-стенд prune не трогает).

---

### Task 1: Каркас контура — фикстура шагов 1–2 [РЕАЛИЗОВАН, коммит 2ba7445]

Закрыто: файл `E2eTliSwitchScenarios.cs`; `TliSwitchContext : IAsyncLifetime` (гейт docker в InitializeAsync, `Fx?.MarkFailed()`-гвард, метод `MarkFailed()`, teardown + ассерт чистоты в DisposeAsync); окружение slug `tlsw` с MinIO; кластер `tlsw{tag}` (2 шарда × 2 реплики); воркер с ОБЪЕДИНЁННЫМ env-комплектом (ОБА S3-поля = `Fx.S3Endpoint` без замены localhost — full-джоб берёт СЫРОЙ endpoint, замена только в `HostS3Client`); шаги 1–2 spec §3.2 (COMPLETED-полный на TLI1 с фиксацией `OldBackupId`/`WalStartSegment`, pre-wal до непрерывности `WalChain.Check` с `[PHASE] pre-wal-chain`, запись TLI1 в открытый сегмент S emit'ом без switch, эталон `DsnBefore`); хелперы-копии; каркасный Fact. Производит для следующих задач публичные члены `TliSwitchContext`: `Fx`, `Cluster`, `DsnBefore`, `OldBackupId`, `WalStartSegment`, `Tli2`, `TCut`, `AfterCutPos`, `LatestRestoreSucceeded`, `MarkFailed()`, `WaitPhaseAsync`, `RestoreWithRetryAsync(cluster, shard, target, backupId, ct)`, `GetOrNullAsync`, `FullKeysAsync`, `DumpDiagnosticsAsync`.

### Task 2: Failover-ветка — шаги 3–5 [РЕАЛИЗОВАНО в старой редакции, коммит 2352165 + незакоммиченный довесок]

Закрыто коммитом `2352165`: `FailoverAsync` (docker stop мастера → новый primary ≠ → `Tli2 ≥ 2`), `SeedNewTimelineAsync` (post-switch, `[PHASE] tli2-tail` с позиционным гейтом 300 с, фиксация `TCut` + пауза 2 с, after-cut с `[PHASE] after-cut-archived` + `WalArchivedAsync`).

В рабочем дереве (НЕЗАКОММИЧЕНО, кодером): гейт replica-sync в `PrepareAsync` после шага 2 + хелпер `ReplicasSyncedAsync` — Шаг 2.7 доводит до spec и фиксирует коммитом.

**Вход:** Task 1 слит; незакоммиченный довесок replica-sync в дереве.

**Действие (Шаг 2.7):** сверить/поправить гейт replica-sync до буквы spec §3.2 шаг 3; закоммитить.

**Выход:** гейт replica-sync = обязателен, бюджет ≤ 60 с, до docker stop; закоммичен.

**Проверка:** `dotnet build` 0 warnings (docker-прогон — общий с Task 3, отдельный не нужен: правка одной цифры бюджета и комментариев).

**Связь со spec:** §3.2 шаг 3 (гейт replica-sync — обязательный); §2 принцип «строгость + побайтовость» (точка промоута удерживается в открытом сегменте гейтом replica-sync); §7 риск-таблица строка 1 (реплика отстала → промоут на закрытом сегменте → профиль не смешанный).

- [ ] **Шаг 2.7a: сверить код гейта с итоговым видом**

В `PrepareAsync` между `SeedFullBackupAsync` и `FailoverAsync` (место — до docker stop, эквивалент начала шага 3; уже так в дереве):

```csharp
        // Гейт синхронизации реплик до failover (spec §3.2 шаг 3, ОБЯЗАТЕЛЕН):
        // точка промоута определяется ПОЗИЦИЕЙ РЕПЛИКИ (replay_lsn), а не
        // мастера — отставшая реплика промоутнется ВНУТРИ уже закрытого
        // мастером сегмента (точка выйдет из открытого S — профиль не
        // смешанный). Ждём replay_lsn каждой реплики ≥ позиции мастера
        // (последняя запись — emit «t28» в SeedFullBackupAsync, строго внутри
        // ОТКРЫТОГО сегмента): тогда promote-точка внутри открытого S —
        // единственный носитель позиции S: смешанный объект под именем
        // родителя (§3.3, AC1).
        var synced = await WaitPhaseAsync("replica-sync", () => ReplicasSyncedAsync(ct),
            TimeSpan.FromSeconds(60), ct);
        synced.Should().BeTrue("реплики обязаны догнать позицию мастера до failover: "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));
```

И хелпер (уже в дереве; сверить дословно):

```csharp
    // Синхронизация реплик шарда: replay_lsn КАЖДОЙ реплики ≥ текущей позиции
    // записи мастера (pg_current_wal_lsn). Гейт профиля смешанности: мастер
    // держит сегмент S открытым, догнавшая реплика промоутнется внутри S.
    private async Task<bool> ReplicasSyncedAsync(CancellationToken ct)
    {
        var (mHost, mPort, master) = await MasterPgAsync(Cluster, "shard1", ct);
        var masterPos = ParseLsn(await ScalarAsync(
            AdminDsn(mHost, mPort), "SELECT pg_current_wal_lsn()", ct));
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{Cluster}", ct);
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        foreach (var (key, addr) in entries
                     .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)))
        {
            if (key.Split('/')[1] == master)
                continue; // сам мастер — не проверяется
            var replay = ParseLsn(await ScalarAsync(
                AdminDsn(addr.GetProperty("host").GetString()!,
                    addr.GetProperty("pg").GetInt32()),
                "SELECT pg_last_wal_replay_lsn()", ct));
            if (replay < masterPos)
                return false;
        }

        return true;
    }
```

Правки доводки: бюджет `TimeSpan.FromSeconds(120)` → `TimeSpan.FromSeconds(60)` (spec §3.2 шаг 3: «бюджет ≤ 60 с»); комментарий над вызовом — привести к виду выше (упоминание имени родителя вместо «смешанный объект TLI2»).

- [ ] **Шаг 2.7b: обновить комментарий гейта `tli2-tail` в `SeedNewTimelineAsync` (новая механика имени)**

Заменить комментарий перед `WaitPhaseAsync("tli2-tail", …)` на:

```csharp
        // Шаг 4 — данные на новом TLI + доставка TLI2-хвоста. Условие
        // усилено позицией: TLI2-именованный сегмент не ниже Next(последнего
        // доставленного TLI1-именованного; после доставки смешанного объекта
        // под именем родителя это Next(S)). Строгое «первый TLI2-именованный
        // = Next(S)» ассертит гейт §3.3 п.4 (позиция S — из .history).
```

- [ ] **Шаг 2.7c: компиляция + commit**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t28-tli-switch-restore-e2e && dotnet build src/PgWorker.slnx -c Release
```
Ожидание: `Build succeeded. 0 Warning(s)`.

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eTliSwitchScenarios.cs && \
git commit -m "test(e2e): t28 — гейт replica-sync до docker stop (spec §3.2 шаг 3, обязателен): replay_lsn каждой реплики ≥ pg_current_wal_lsn мастера (≤ 60 с; отставшая реплика промоутнулась бы на закрытом сегменте — профиль не смешанный); комментарий tli2-tail — под механику имени родителя"
```

---

### Task 3: Строгий + побайтовый гейт смешанности — шаг 6 (§3.3, редакция «имя родителя»)

**Вход:** Task 2 слит (вкл. Шаг 2.7); в дереве незакоммиченный `AssertMixedSwitchAsync` (обёртка с дампом — оставить), `AssertMixedSwitchCoreAsync` (ассерты СТАРОЙ редакции — заменить), `HistoryEntryAsync`/`ParseLsn` (оставить).

**Действие:** дописать `SegmentPageTlisAsync` (побайтовый проход page-заголовков объекта S); заменить `AssertMixedSwitchCoreAsync` на 5 ассертов новой редакции §3.3; дополнить дамп провала `AssertMixedSwitchAsync` картой pageaddr→tli. Продуктовый код не трогать.

**Выход:** InitializeAsync завершается ТОЛЬКО при смешанном профиле: точка строго внутри S; S доставлен РОВНО один раз под именем TLI родителя `00000001…S`; побайтово — page-заголовки по обе стороны switchWALLSN показывают TLI1/TLI2; первый TLI2-именованный объект = Next(S); `WalChain.Check` непрерывен. Провал любого пункта — диагностическое исключение (history + карта pageaddr→tli + `DumpDiagnosticsAsync`), НЕ ретрай.

**Проверка:** прогон класса — `passed: 1`; в журнале `[PHASE] tlsw-ready: контур готов (…)`. Упавший гейт даёт в сообщении фикстуры: history-содержимое, карту pageaddr→tli, list wal/, ключи full/wal, Patroni /cluster. Зачистка.

**Связь со spec:** §3.3 (5 ассертов новой редакции — механика имени родителя: `SegmentAssembler.Close` берёт TLI из long page header границы сегмента, границу S открывал мастер TLI1); §3.1 (`SegmentPageTlisAsync`: скачивание объекта S host-клиентом и проход page-заголовков, шаг 8192, `xlp_tli`=LeU32@4 / `xlp_pageaddr`=LeU64@8); §1.2 п.1 («строгий ассерт + побайтовое подтверждение»); AC1; §5 (разбор ограничен page-заголовками, декодер XLOG не пишется); §2 (принцип «строгость + побайтовость», профиль закреплён гейтом replica-sync).

**Interfaces:** производит `SegmentPageTlisAsync(cluster, shard, segmentName, ct)` → `Task<List<(ulong PageAddr, uint Tli)>>` — используется Core-ассертами и дампом обёртки.

- [ ] **Шаг 3.1: хелпер побайтового чтения `SegmentPageTlisAsync`**

В using-блок файла добавить `using Amazon.Runtime;`, `using Amazon.S3;`, `using Amazon.S3.Model;`. Дописать в `TliSwitchContext`:

```csharp
    // Байтовое чтение объекта сегмента + проход page-заголовков (spec §3.1):
    // XLogPageHeaderData (C-выравнивание): xlp_tli = LeU32 @ +4,
    // xlp_pageaddr = LeU64 @ +8; шаг страницы 8192 (XLOG_BLCKSZ) — карта
    // pageaddr→tli для побайтового ассерта §3.3 п.3. GET — прямым
    // AmazonS3Client с теми же endpoint/кредами/path-style, что HostS3Client:
    // IBackupS3.GetObjectAsync читает объект StreamReader'ом UTF8 (текстовые
    // .history) и бинарный сегмент исказил бы; продуктовый код не трогаем
    // (spec §5). BitConverter — little-endian, как и формат WAL.
    private async Task<List<(ulong PageAddr, uint Tli)>> SegmentPageTlisAsync(
        string cluster, string shard, string segmentName, CancellationToken ct)
    {
        using var client = new AmazonS3Client(
            new BasicAWSCredentials("minioadmin", "minioadmin"),
            new AmazonS3Config
            {
                ServiceURL = Fx.S3Endpoint.Replace(
                    "host.docker.internal:", "localhost:", StringComparison.Ordinal),
                ForcePathStyle = true,
            });
        var response = await client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = Bucket,
            Key = $"{cluster}/{shard}/wal/{segmentName}",
        }, ct);
        await using var stream = response.ResponseStream;
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        var bytes = ms.ToArray();
        const int pageSize = 8192;
        var pages = new List<(ulong PageAddr, uint Tli)>();
        for (var off = 0; off + 12 <= bytes.Length; off += pageSize)
            pages.Add((BitConverter.ToUInt64(bytes, off + 8), BitConverter.ToUInt32(bytes, off + 4)));
        return pages;
    }
```

- [ ] **Шаг 3.2: заменить `AssertMixedSwitchCoreAsync` на 5 ассертов новой редакции §3.3**

Заменить тело метода (комментарий шапки — новый):

```csharp
    // Ассерты гейта (без обёртки диагностики) — редакция spec §3.3 «имя
    // родителя + побайтовость»: TLI имени закрываемого сегмента приёмник
    // берёт из long page header его ГРАНИЦЫ (SegmentAssembler.Close), а
    // границу S открывал мастер TLI1 — смешанный объект архивируется под
    // именем РОДИТЕЛЯ 00000001…S; TLI2-поток продолжается с Next(S).
    private async Task AssertMixedSwitchCoreAsync(CancellationToken ct)
    {
        var entry = await HistoryEntryAsync(Cluster, "shard1", Tli2, ct);
        entry.ParentTli.Should().Be(1u,
            $"последняя запись {Tli2:x8}.history — сам TLI2 от родителя TLI1: parent={entry.ParentTli}");
        var switchValue = ParseLsn(entry.SwitchLsn);
        var switchSeg = PgWorker.Backups.WalFileName.FromLsn(0, entry.SwitchLsn);

        // (1) точка переключения СТРОГО внутри сегмента S («на границе»
        // покрытием t28 не считается: накат шёл бы по стыку сегментов).
        (switchValue % (ulong)PgWorker.Backups.WalFileName.SegmentBytes)
            .Should().NotBe(0UL,
                $"точка переключения {entry.SwitchLsn} ВНУТРИ сегмента {switchSeg.Name} "
                + "(не на границе 16 МиБ)");

        var names = await ListWalNamesAsync(Cluster, "shard1");
        var parentName = $"{entry.ParentTli:x8}{switchSeg.Log:x8}{switchSeg.Seg:x8}";

        // (2) сегмент S доставлен РОВНО один раз — под именем TLI родителя;
        // иных имён S (в частности 00000002…S) в архиве нет.
        var sNames = names
            .Select(n => PgWorker.Backups.WalFileName.TryParse(n))
            .Where(w => w is not null
                && w!.Value.Log == switchSeg.Log && w.Value.Seg == switchSeg.Seg)
            .Select(w => w!.Value.Name)
            .ToList();
        sNames.Should().BeEquivalentTo([parentName],
            $"сегмент S доставлен РОВНО один раз, под именем TLI родителя {parentName} "
            + "(SegmentAssembler.Close именует по page header границы); иные имена S недопустимы");

        // (3) ПОБАЙТОВОЕ подтверждение смешанности: page-заголовки объекта S
        // по разные стороны switchWALLSN показывают TLI родителя / TLI нового.
        var pages = await SegmentPageTlisAsync(Cluster, "shard1", parentName, ct);
        pages.Should().NotBeEmpty("объект S обязан содержать page-заголовки");
        var segStart = (ulong)((long)switchSeg.Log * 256 + switchSeg.Seg)
            * (ulong)PgWorker.Backups.WalFileName.SegmentBytes;
        pages[0].PageAddr.Should().Be(segStart, "страница 0 — граница сегмента S");
        pages[0].Tli.Should().Be(entry.ParentTli,
            "страница 0 — long header границы (записан мастером TLI1): источник имени объекта");
        const int pageSize = 8192;
        foreach (var (pageAddr, tli) in pages)
        {
            if (pageAddr + pageSize <= switchValue)
                tli.Should().Be(entry.ParentTli,
                    $"страница 0x{pageAddr:x} целиком ниже switchWALLSN — байты TLI1 в объекте {parentName}");
            else if (pageAddr > switchValue)
                tli.Should().Be(Tli2,
                    $"страница 0x{pageAddr:x} целиком выше switchWALLSN — байты TLI2 в объекте {parentName}");
            // страница, СОДЕРЖАЩАЯ switchWALLSN (pageaddr ≤ точка < pageaddr+8192),
            // — переходная: её заголовок создан до переключения, жёстко НЕ
            // ассертится (spec §3.3 п.3)
        }
        pages.Should().Contain(p => p.PageAddr + pageSize <= switchValue && p.Tli == entry.ParentTli,
            "в объекте есть страницы TLI1 ниже точки — префикс байтов родителя");
        pages.Should().Contain(p => p.PageAddr > switchValue && p.Tli == Tli2,
            "в объекте есть страницы TLI2 выше точки — хвост байтов нового таймлайна");

        // (4) TLI2-поток продолжается со следующего сегмента: первый
        // TLI2-именованный объект = Next(S); TLI1-именованных выше S нет.
        // Профиль S (имя TLI1) → Next(S) (имя TLI2) + history-объект — в
        // точности эвристика TLI-перехода валидатора restore.
        var next = switchSeg.Next();
        var tli2Segments = names
            .Select(n => PgWorker.Backups.WalFileName.TryParse(n))
            .Where(w => w is { Tli: var t } && t == Tli2)
            .Select(w => w!.Value)
            .OrderBy(w => (long)w.Log * 256 + w.Seg)
            .ToList();
        tli2Segments.Should().NotBeEmpty("TLI2-именованные сегменты доставлены (гейт шага 4)");
        (tli2Segments[0].Log, tli2Segments[0].Seg).Should().Be((next.Log, next.Seg),
            $"первый TLI2-именованный объект = Next(S) = {next.Name}");
        var switchPos = (long)switchSeg.Log * 256 + switchSeg.Seg;
        names.Where(n => PgWorker.Backups.WalFileName.TryParse(n) is { } w
                && w.Tli == entry.ParentTli
                && (long)w.Log * 256 + w.Seg > switchPos)
            .Should().BeEmpty("TLI1-именованных объектов выше S нет");

        // (5) WalChain.Check — тем же валидатором, которым restore-заявка
        // пройдёт валидацию (S под именем TLI1 → Next(S) под TLI2 + history).
        var start = PgWorker.Backups.WalFileName.TryParse(WalStartSegment)!.Value;
        var check = PgWorker.Backups.WalChain.Check(start, names);
        check.IsContinuous.Should().BeTrue(check.GapError ??
            $"WalChain.Check от {WalStartSegment} непрерывен — тем же валидатором, что пройдёт restore-заявка");
    }
```

- [ ] **Шаг 3.3: карта pageaddr→tli в дамп провала обёртки `AssertMixedSwitchAsync`**

В catch-блоке обёртки (после чтения `history`, до throw) добавить:

```csharp
            // Карта pageaddr→tli объекта S (лучшими усилиями — входит в дамп
            // провала по букве §3.3): имя родителя — из последней записи history.
            var pageMap = "";
            try
            {
                if (PgWorker.Backups.WalHistory.Parse(history) is { Count: > 0 } es)
                {
                    var seg = PgWorker.Backups.WalFileName.FromLsn(0, es[^1].SwitchLsn);
                    var mapPages = await SegmentPageTlisAsync(Cluster, "shard1",
                        $"{es[^1].ParentTli:x8}{seg.Log:x8}{seg.Seg:x8}", ct);
                    pageMap = "pageMap=[" + string.Join("; ",
                        mapPages.Select(p => $"0x{p.PageAddr:x}:tli{p.Tli}")) + "]";
                }
            }
            catch (Exception pe)
            {
                pageMap = $"<карта недоступна: {pe.Message}>";
            }
```

И включить её в текст исключения:

```csharp
            throw new ApplicationException(
                $"гейт смешанности провален: {ex.Message}\nhistory=[{history}]\n{pageMap}\n"
                + await DumpDiagnosticsAsync(Cluster, "shard1"), ex);
```

- [ ] **Шаг 3.4: компиляция + прогон + зачистка**

`dotnet build` (0 warnings) → прогон класса в фоне (покрывает и доводку Шага 2.7) → ожидание `passed: 1, failed: 0`; в журнале `[PHASE] replica-sync: ok=True`, `[PHASE] tlsw-ready: контур готов (…)`. Провал гейта — честный красный с точной причиной (диагностика в сообщении фикстуры), разбор по артефактам, перезапуск без анализа запрещён (e2e-launch §4). После финальной строки — зачистка без остатков.

- [ ] **Шаг 3.5: Commit**

```bash
git add -A src/tests && git commit -m "test(e2e): t28 — гейт смешанности по редакции §3.3 (имя родителя + побайтовость, AC1): S доставлен РОВНО один раз под именем 00000001…S (SegmentAssembler.Close именует по page header границы — границу S открывал мастер TLI1), SegmentPageTlisAsync — карта pageaddr→tli (xlp_tli LeU32@4, xlp_pageaddr LeU64@8, шаг 8192; байтовый GET прямым AmazonS3Client — текстовый GetObjectAsync байты исказил бы, продуктовый код не тронут), ассерты страниц по обе стороны switchWALLSN + первый TLI2-именованный = Next(S) + WalChain.Check; дамп провала — карта pageaddr→tli"
```

---

### Task 4: Обе restore-заявки — Fact 1 (latest), Fact 2 (target_time), детерминированный orderer

**Вход:** Task 3 слит, контур проходит гейт смешанности.

**Действие:** заменить каркасное тело Fact 1 на финальное (заявка + 5 групп ассертов, вкл. dsn-сравнение), добавить Fact 2 со skip-гейтом, добавить `TliSwitchFactOrderer` + `[TestCaseOrderer]`, добавить хелперы `MasterReadyDsnAsync`/`ScalarRetryAsync`.

**Выход:** полное покрытие AC2/AC3: 18 строк latest / 15 строк PITR на одной цепочке, dsn не изменился; порядок заявок детерминирован.

**Проверка:** прогон класса — `passed: 2, failed: 0, skipped: 0`; в журнале порядок `Restore_Latest…` раньше `Restore_TargetTime…`, `[PHASE] restore …` с `state=COMPLETED` дважды; зачистка.

**Связь со spec:** Ф3 (§4); §3.3 (порядок заявок — инвариант); §3.4 (Fact 1, ассерты 1–5, вкл. «dsn не изменился»); §3.5 (Fact 2, гейт skip); §3.6 (телеметрия); AC2, AC3, AC4.

- [ ] **Шаг 4.1: хелперы чтения мастера**

Дописать в `TliSwitchContext`:

```csharp
    // Гейт мастера (t10) + DSN чтения: COMPLETED restore не гарантирует
    // закрытия рестарт-окна postmaster — SQL-проба SELECT 1 до возврата DSN
    // (эталон rs-dr; бюджет 120 с согласован с PatroniBootSec хоста).
    public async Task<string> MasterReadyDsnAsync(CancellationToken ct)
    {
        var ready = await WaitPhaseAsync("master-ready", async () =>
        {
            try
            {
                var (host, port, _) = await MasterPgAsync(Cluster, "shard1", ct);
                await ScalarAsync(AdminDsn(host, port), "SELECT 1", ct);
                return true;
            }
            catch (NpgsqlException)
            {
                return false; // рестарт-окно — поллинг повторит
            }
        }, TimeSpan.FromSeconds(120), ct);
        ready.Should().BeTrue("мастер обязан принять SQL до чтения данных (гейт t10): "
            + await DumpDiagnosticsAsync(Cluster, "shard1"));
        var (h, p, _) = await MasterPgAsync(Cluster, "shard1", ct);
        return AdminDsn(h, p);
    }

    // Финальное чтение с ретраями (эталон rs-latest): Patroni доводит конфиг
    // мастера после COMPLETED — рестарт рвёт соединения, переходное окно.
    public async Task<string> ScalarRetryAsync(string dsn, string sql, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await ScalarAsync(dsn, sql, ct);
            }
            catch (NpgsqlException) when (attempt < 5)
            {
                await Task.Delay(8000, ct);
            }
        }
    }
```

- [ ] **Шаг 4.2: orderer + атрибут на классе**

Дописать ПЕРЕД классом `E2eTliSwitchScenarios` (и добавить `using Xunit.v3;` в using-блок файла):

```csharp
// Порядок Fact'ов (инвариант spec §3.3): latest ОБЯЗАН идти первым — после
// target_time-restore новый TLI-поток пишет с позиций T_cut (ниже after-cut
// сегментов TLI2) и делает latest-валидацию по исходной цепочке невозможной.
public sealed class TliSwitchFactOrderer : ITestCaseOrderer
{
    private static readonly string[] Order =
    [
        "Restore_LatestThroughTliSwitch_PicksUpWholeTail",
        "Restore_TargetTimeThroughTliSwitch_StopsAtGoal",
    ];

    // Сигнатура xunit.v3 3.2.2: ограничение notnull + Xunit.Sdk.ITestCase;
    // имя метода доступно напрямую — ITestCase наследует
    // ITestCaseMetadata.TestMethodName (каст не нужен).
    public IReadOnlyCollection<TTestCase> OrderTestCases<TTestCase>(
        IReadOnlyCollection<TTestCase> tests)
        where TTestCase : notnull, Xunit.Sdk.ITestCase
        // явный список имён; неизвестные имена — в конец (стабильная сортировка)
        => [.. tests.OrderBy(t =>
        {
            var idx = Array.IndexOf(Order, t.TestMethodName);
            return idx < 0 ? int.MaxValue : idx;
        })];
}
```

И на классе сценариев:

```csharp
[TestCaseOrderer(typeof(TliSwitchFactOrderer))]
public sealed class E2eTliSwitchScenarios(TliSwitchContext Ctx) : IClassFixture<TliSwitchContext>
```

- [ ] **Шаг 4.3: финальное тело Fact 1 (AC2)**

Заменить каркасное тело `Restore_LatestThroughTliSwitch_PicksUpWholeTail` на:

```csharp
    // AAA (AC2 — полный хвост через точку): контур фикстуры (полный на TLI1 +
    // failover + гейт смешанности) → заявка latest по ЯВНОМУ OldBackupId →
    // COMPLETED с restored_to_lsn; 18 строк = 5 pre (полный) + 5 pre-wal
    // (TLI1-хвост в смешанном сегменте) + 5 post-switch (TLI2 после
    // END_OF_RECOVERY) + 3 after-cut (хвост за целью PITR) — накат прошёл
    // ВСЁ, включая хвост за целью; ноды RUNNING, dsn не изменился; wal-ключ
    // заведён заново, полный переснят, wal-агент running (инвариант §3.5).
    [Fact]
    public async Task Restore_LatestThroughTliSwitch_PicksUpWholeTail()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        try
        {
            // Act — заявка latest по исходному полному (ретраи ≤ 3 на гонки
            // доставки; валидация идёт по «чистому» list-S3 исходной цепочки).
            var done = await Ctx.RestoreWithRetryAsync(
                Ctx.Cluster, "shard1", "latest", Ctx.OldBackupId, ct);

            // Assert 1 — COMPLETED с restored_to_lsn.
            done["state"].GetString().Should().Be("COMPLETED");
            done["restored_to_lsn"].GetString().Should().NotBeNullOrEmpty();

            // Assert 2 — полный хвост через точку (гейт мастера t10 + ретраи
            // чтения: переходное окно рестарта postmaster).
            var dsn = await Ctx.MasterReadyDsnAsync(ct);
            var rows = await Ctx.ScalarRetryAsync(dsn,
                "SELECT count(*) || '|' || count(*) FILTER (WHERE note = 'pre-wal')" +
                " || '|' || count(*) FILTER (WHERE note = 'post-switch')" +
                " || '|' || count(*) FILTER (WHERE note = 'after-cut')" +
                " FROM tli_switch_probe", ct);
            var parts = rows.Split('|');
            parts[0].Should().Be("18",
                "все контрольные строки восстановлены, включая after-cut (хвост за целью PITR)");
            parts[1].Should().Be("5",
                "TLI1-хвост после полного накатился через смешанный сегмент (байты TLI1 в префиксе S)");
            parts[2].Should().Be("5",
                "данные TLI2 после END_OF_RECOVERY накатились");
            parts[3].Should().Be("3",
                "after-cut восстановлены — restore дошёл до конца цепочки TLI2");

            // Assert 3 — контур: ноды RUNNING, dsn-ключ не изменился
            // (эталон DsnBefore фиксации шага 1; образец rs-latest).
            var nodeA = await Ctx.GetOrNullAsync(
                $"/clusters/{Ctx.Cluster}/shards/shard1/nodes/shard1a/state");
            var nodeB = await Ctx.GetOrNullAsync(
                $"/clusters/{Ctx.Cluster}/shards/shard1/nodes/shard1b/state");
            nodeA!.Value.Should().Be("RUNNING");
            nodeB!.Value.Should().Be("RUNNING");
            var dsnAfter = await Ctx.GetOrNullAsync(
                $"/clusters/{Ctx.Cluster}/shards/shard1/dsn");
            dsnAfter!.Value.Should().Be(Ctx.DsnBefore, "portalloc/dsn restore не трогает (AC2)");

            // Assert 4 (инвариант §3.5 после restore) — wal-ключ сброшен и
            // заведён заново (ACTIVE), новый COMPLETED-полный ≠ OldBackupId,
            // wal-агент running (rs-latest-паттерн AC4, ≤ 300 с).
            var reinited = await Ctx.WaitPhaseAsync("wal-reinit", async () =>
            {
                var walKv = await Ctx.GetOrNullAsync(
                    $"/pgworker/backups/{Ctx.Cluster}/shard1/wal");
                if (walKv is null || !walKv.Value.Contains("ACTIVE"))
                    return false;
                var fulls = await Ctx.FullKeysAsync(Ctx.Cluster, "shard1");
                if (!fulls.Any(f => f.Value.Contains("COMPLETED")
                        && f.Key.Split('/').Last() != Ctx.OldBackupId))
                    return false;
                var agents = await Ctx.Fx.RunDockerAsync(
                    ["ps", "--format", "{{.Names}}", "--filter",
                        $"name=pgw-backup-wal-{Ctx.Cluster}-shard1"], ct);
                return agents.Length > 0;
            }, TimeSpan.FromSeconds(300), ct);
            reinited.Should().BeTrue("инвариант после restore (wal-reinit): "
                + await Ctx.DumpDiagnosticsAsync(Ctx.Cluster, "shard1"));

            // Assert 5 — фиксация исхода: гейт skip'а Fact 2.
            Ctx.LatestRestoreSucceeded = true;
        }
        catch
        {
            Ctx.MarkFailed(); // контейнеры остаются для разбора (e2e-launch)
            throw;
        }
    }
```

- [ ] **Шаг 4.4: Fact 2 (AC3) со skip-гейтом**

Дописать в `E2eTliSwitchScenarios`:

```csharp
    // AAA (AC3 — остановка в цели): контур после Fact 1 (latest прошёл,
    // TLI3-хвост выше конца TLI2) → заявка time:T_cut по ТОМУ ЖЕ OldBackupId
    // (явный backup_id обязателен: новейший COMPLETED — уже TLI3-полный) →
    // COMPLETED; 15 строк = pre + pre-wal + post-switch, after-cut = 0 —
    // цель ПОСЛЕ точки переключения соблюдена; вместе с Fact 1 (18 строк на
    // той же цепочке) — демонстрация PITR через TLI-границу. Ноды RUNNING.
    [Fact]
    public async Task Restore_TargetTimeThroughTliSwitch_StopsAtGoal()
    {
        DockerTrait.SkipIfUnavailable();
        // Гейт (spec §3.5): Fact 1 упал → повторная заявка на сломанном
        // контуре — каскадный красный без новой информации; телеметрия
        // первой заявки уже собрана.
        if (!Ctx.LatestRestoreSucceeded)
            Assert.Skip("Fact 1 (latest) упал — контур для target_time недостоверен, разбор по телеметрии Fact 1");
        var ct = TestContext.Current.CancellationToken;
        try
        {
            // Act — заявка PITR на T_cut по СТАРОМУ полному (валидация от
            // wal_start старого полного проходит: TLI3-хвост выше конца TLI2,
            // переход TLI2→TLI3 легитимен; recovery остановится на T_cut).
            var done = await Ctx.RestoreWithRetryAsync(Ctx.Cluster, "shard1",
                $"time:{Ctx.TCut:yyyy-MM-ddTHH:mm:ssZ}", Ctx.OldBackupId, ct);

            // Assert 1 — COMPLETED; LSN остановки не дальше доставки after-cut.
            done["state"].GetString().Should().Be("COMPLETED");
            var lsn = done["restored_to_lsn"].GetString();
            lsn.Should().NotBeNullOrEmpty();
            var restoredSeg = PgWorker.Backups.WalFileName.FromLsn(0, lsn!);
            var restoredPos = (long)restoredSeg.Log * 256 + restoredSeg.Seg;
            restoredPos.Should().BeLessThanOrEqualTo(Ctx.AfterCutPos,
                "цель раньше конца цепочки — recovery не уходил дальше after-cut");

            // Assert 2 — остановка в цели: 15 строк, after-cut нет.
            var dsn = await Ctx.MasterReadyDsnAsync(ct);
            var rows = await Ctx.ScalarRetryAsync(dsn,
                "SELECT count(*) || '|' || count(*) FILTER (WHERE note = 'after-cut')" +
                " FROM tli_switch_probe", ct);
            var parts = rows.Split('|');
            parts[0].Should().Be("15",
                "pre + pre-wal + post-switch восстановлены — обе стороны TLI-границы через смешанный сегмент");
            parts[1].Should().Be("0", "коммиты после T_cut не накатились — PITR-остановка в цели");

            // Assert 3 — контур: ноды RUNNING (rejoin второй заявки — тот же
            // RestoreProcess), dsn жив.
            var nodeA = await Ctx.GetOrNullAsync(
                $"/clusters/{Ctx.Cluster}/shards/shard1/nodes/shard1a/state");
            var nodeB = await Ctx.GetOrNullAsync(
                $"/clusters/{Ctx.Cluster}/shards/shard1/nodes/shard1b/state");
            nodeA!.Value.Should().Be("RUNNING");
            nodeB!.Value.Should().Be("RUNNING");
        }
        catch
        {
            Ctx.MarkFailed();
            throw;
        }
    }
```

- [ ] **Шаг 4.5: компиляция + полный прогон класса + зачистка**

`dotnet build` (0 warnings) → прогон класса (~15–20 мин, контур один на оба Fact'а) → ожидание `passed: 2, failed: 0, skipped: 0`; в журнале Fact `Restore_LatestThroughTliSwitch_PicksUpWholeTail` идёт ПЕРВЫМ (orderer), две `[PHASE] restore …: state=COMPLETED`. Зачистка без остатков. Провал — полный разбор артефактов до любых правок (e2e-launch §4).

- [ ] **Шаг 4.6: Commit**

```bash
git add -A src/tests && git commit -m "test(e2e): t28 — обе restore-заявки на одном контуре (Ф3, AC2+AC3): Fact 1 latest по явному OldBackupId → 18 строк (накат через END_OF_RECOVERY смешанного сегмента, dsn-эталон DsnBefore, wal-reinit-инвариант), Fact 2 time:T_cut по тому же полному → 15 строк/after-cut=0 + skip-гейт по исходу Fact 1; детерминированный порядок TliSwitchFactOrderer (latest → target_time, инвариант §3.3)"
```

---

### Task 5: Мерж-гейт — полный Release-прогон, кейс-маркер, канон-правки (AC5)

**Вход:** Task 4 слит, класс зелёный на обоих Fact'ах.

**Действие:** свежий Release полный E2E-прогон + кейс-маркер `Scale_AddEmptyShard` (с зачисткой между сериями, вкл. проверку томов), правки `arch/19-backups.md` §10, `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` — тем же мерж-коммитом.

**Выход:** контур зелёный на свежем Release; канон и roadmap отражают закрытие t28; финальный коммит ветки.

**Проверка:** обе серии зелёные; остатков контейнеров/сетей/томов/ключей своего guid — 0 (ассерт чистоты teardown'а в каждом сценарии + страховочные проверки между сериями); `grep t28 arch/roadmap/reliability.md` пуст; строка t28 есть в «Сделано» отчёта.

**Связь со spec:** Ф4 (§4); §6 AC5 («зачистка серий без остатков — контейнеры/сети/тома/ключи»); §7 (правки канона — формулировки «текущее состояние», без истории).

Примечание к операционализации «0 остатков своего guid»: остатки КАЖДОГО сценария (контейнеры/тома/сети его guid) проверяются самим тестом — ассерт чистоты teardown'а `E2eEnvironment.DisposeAsync` краснеет при любом остатке (ключи etcd умирают вместе с контейнером окружения). Страховочные проверки между сериями (ниже) ловят остатки УПАВШИХ серий, чей teardown не дошёл до ассерта.

- [ ] **Шаг 5.1: полный E2E-прогон на свежем Release (серия 1)**

E2eFixture собирает Release сам (инкрементальный no-op — секунды); `PGW_TEST_E2E_NOBUILD` НЕ ставить. Запускать в фоне:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t28-tli-switch-restore-e2e && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e"
```

Ожидание: все E2E-классы зелёные, вкл. `E2eTliSwitchScenarios` (2 Fact'а). После финальной строки — зачистка (глобальные ограничения: контейнеры/сети/тома + `docker network prune -f`).

- [ ] **Шаг 5.2: кейс-маркер (серия 2)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t28-tli-switch-restore-e2e && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard
```

Ожидание: `passed: 1, failed: 0`. После финальной строки — зачистка (вкл. `docker volume ls --filter name=pgw-` — пусто).

- [ ] **Шаг 5.3: правка arch/19-backups.md §10 (строка риска «Смешанный сегмент…»)**

В файле `arch/19-backups.md` строка 844 — в колонке «Закрытие» заменить хвост строки:

- было: `…восстановимость через точку переключения опирается на replay-континуальность postgres (промоутнутая нода начинает новую TLI с END_OF_RECOVERY-записи, restore-накат читает её в том же сегменте); E2E-сценарий восстановления ЦЕЛИКОМ через точку переключения (полный до failover + WAL, содержащий границу) отсутствует — отдельная roadmap-задача; AC5 закрывает склейку .history и непрерывность CheckWithRestart после промоута`
- стало: `…восстановимость через точку переключения опирается на replay-континуальность postgres (промоутнутая нода начинает новую TLI с END_OF_RECOVERY-записи, restore-накат читает её в том же сегменте); восстанавливаемость через точку переключения проверяется docker-E2E `E2eTliSwitchScenarios`: restore latest и target_time по одному исходному полному через смешанный сегмент (сегмент — единственный объект под именем TLI родителя; строгий факт + побайтовое подтверждение: switchWALLSN внутри сегмента, page-заголовки по обе стороны точки показывают TLI1/TLI2); AC5 t27 — склейка `.history` и непрерывность CheckWithRestart ПОСЛЕ промоута, t28 — restore ЧЕРЕЗ точку переключения`

Новые риски/контракт не вводятся (spec §7). Формулировка — текущее состояние, без истории.

- [ ] **Шаг 5.4: roadmap — удалить пункт t28**

`arch/roadmap/reliability.md`, строки 91–95 — удалить целиком пункт:

```markdown
- **`t28-tli-switch-restore-e2e`** `← t27-wal-staging-loss` — E2E
  восстанавливаемости через точку TLI-переключения (обещание arch/19 §10):
  полный бэкап до failover → docker stop мастера → нагрузка на новом TLI →
  restore/PITR на цель ПОСЛЕ переключения — доказать накат через смешанный
  сегмент (байты обеих TLI в одном объекте wal/).
```

Проверить: `grep -n "t28" arch/roadmap/reliability.md` — пусто (включая `←`-зависимости других пунктов; если t28 упомянут где-то ещё — удалить и это упоминание).

- [ ] **Шаг 5.5: reliability-report.md — строка в «Сделано» + сводка D**

В `arch/roadmap/reliability-report.md`:
1. В таблицу раздела «Сделано в рамках трека» добавить строку (формат колонок: тег | merge-коммит | суть):

```markdown
| `t28-tli-switch-restore-e2e` | — (мерж-коммит t28-tli-switch-restore-e2e) | восстанавливаемость через точку TLI-переключения доказана docker-E2E `E2eTliSwitchScenarios` (arch/19 §10): гейт replica-sync (replay_lsn реплик ≥ позиции мастера) → docker stop мастера → данные TLI2 → строгий+побайтовый гейт смешанности (switchWALLSN из `.history` внутри 16-МиБ сегмента; сегмент S — единственный объект wal/ под именем TLI родителя — приёмник именует по page header границы; page-заголовки по обе стороны точки показывают TLI1/TLI2; первый TLI2-именованный объект = Next(S); WalChain.Check непрерывен) → две restore-заявки по одному исходному полному на ОДНОМ контуре класса: latest — 18 строк (TLI1-хвост + END_OF_RECOVERY смешанного сегмента + весь TLI2-хвост за целью PITR), time:T_cut — 15 строк (остановка в цели ПОСЛЕ переключения, after-cut=0) |
```

2. В сводку «D — отсутствие потери данных» дополнить абзац о restore/PITR предложением: «восстанавливаемость через точку TLI-переключения (смешанный сегмент failover, побайтово подтверждённый) доказана E2E в обоих режимах цели — latest и target_time на одной WAL-цепочке».

- [ ] **Шаг 5.6: финальная самопроверка + Commit (канон-правки тем же коммитом, AC5)**

```bash
grep -n "t28" arch/roadmap/reliability.md            # пусто
grep -n "t28-tli-switch" arch/roadmap/reliability-report.md  # строка в «Сделано»
grep -n "E2eTliSwitchScenarios" arch/19-backups.md   # ссылка на контур в §10
```

```bash
git add arch/19-backups.md arch/roadmap/reliability.md arch/roadmap/reliability-report.md && \
git commit -m "arch(t28): §10 arch/19 — риск смешанного сегмента закрывается docker-E2E E2eTliSwitchScenarios (restore latest и target_time через точку переключения; сегмент — единственный объект под именем TLI родителя, побайтовое подтверждение по page-заголовкам; разграничение с AC5 t27: ПОСЛЕ промоута vs ЧЕРЕЗ точку); roadmap: тег t28 снят (мерж-гейт), reliability-report — строка в «Сделано» + сводка D. Приёмка: контур 2/2 на свежем Release, кейс-маркер Scale_AddEmptyShard зелёный, зачистка серий без остатков (контейнеры/сети/тома)"
```

Дальше — по флоу dev-flow: ревью ветки и мерж в `main` ТОЛЬКО по явной просьбе пользователя (AGENTS.base.md §7).

---

## Самопроверка плана (выполнена автором, редакция r4)

**Покрытие spec (редакция «имя родителя + побайтовость»):** §3.1 (вкл. `SegmentPageTlisAsync`: host-клиент, page-заголовки, шаг 8192, LeU32@4/LeU64@8) → Task 3 Шаг 3.1; §3.2 шаг 3 (гейт replica-sync ≤ 60 с до docker stop) → Task 2 Шаг 2.7 (гейт размещён в `PrepareAsync` между шагом 2 и `FailoverAsync` — до docker stop, семантически эквивалент начала шага 3); §3.2 шаг 4 («TLI2-именованный начиная с Next(S)») → комментарий Шага 2.7b + строгий ассерт Шага 3.2 п.4; §3.3 ассерты 1–5 (строго внутри S; S ровно один раз под именем родителя `00000001…S`, иные имена S отсутствуют; побайтово pageaddr↔SwitchLsn → TLI1/TLI2, переходная не ассертится; первый TLI2-именованный = Next(S), TLI1-именованных выше S нет; WalChain.Check) → Task 3 Шаг 3.2; дамп провала (history, list wal/, карта pageaddr→tli, ключи full/wal, Patroni /cluster) → Шаг 3.3; §3.4/§3.5 → Task 4 (без изменений против r3); §3.6 → все ожидания с `[PHASE]` (WaitPhaseAsync; ручные `pre-wal-chain`/`after-cut-archived` — из r3, реализованы); §4 Ф1–Ф4 → Tasks 1(готово)/2(2.7)/3/4/5; §5 (продуктовый код не тронут — байтовый GET прямым `AmazonS3Client` в тесте; разбор ограничен page-заголовками) → Task 3; AC1 (строгий + побайтовый, профиль закреплён replica-sync) → Tasks 2.7+3; AC2/AC3 → Task 4; AC4 → Tasks 1–4; AC5 → Task 5 (формулировки 5.3/5.5 — под новую механику имени). Пробелов нет.

**Статусная точность:** незакоммиченное состояние ветки отражено секцией «Статус реализации»; оставшаяся работа кодера — Шаги 2.7 (цифра бюджета + комментарии + коммит) и Task 3 (заменить Core-ассерты старой редакции, добавить `SegmentPageTlisAsync`, карту в дамп), существующие `HistoryEntryAsync`/`ParseLsn`/обёртка/`[PHASE] tlsw-ready`/replica-sync-каркас — без переписывания.

**Консистентность типов:** `SegmentPageTlisAsync(cluster, shard, segmentName, ct)` → `Task<List<(ulong PageAddr, uint Tli)>>` используется и Core-ассертами (п.3), и дампом обёртки (карта по `es[^1].ParentTli`); `parentName` строится из `entry.ParentTli` (после ассерта `==1` — ровно `00000001…S`); эвристика `WalChain.Check` валидирует профиль S(имя TLI1)→Next(S)(имя TLI2)+history — тот же валидатор restore; `AmazonS3Client`/`BasicAWSCredentials`/`GetObjectRequest` — using Шага 3.1, пакет транзитивен; поля/методы `TliSwitchContext` между задачами согласованы (см. Task 1).
