# Спецификация алгоритма расчёта параметров PostgreSQL (PGTune)

Документ — самодостаточная спецификация алгоритма расчёта параметров конфигурации PostgreSQL (`postgresql.conf`), воспроизводящая поведение инструмента PGTune. По ней можно реализовать программу расчёта на любом языке программирования, не обращаясь к какому-либо исходному коду: документ содержит полный перечень входных и выходных параметров, все формулы, язык-нейтральный псевдокод, формат результата и контрольные примеры для самопроверки.

Нормативными считаются: перечень входных параметров (раздел 2), формулы (раздел 4), псевдокод (раздел 6) и контрольные примеры (раздел 7). При неоднозначном толковании текста приоритет имеют контрольные примеры, затем псевдокод.

---

## 1. Общая схема алгоритма

На вход подаются характеристики оборудования и сценария использования PostgreSQL. Алгоритм вычисляет набор параметров `postgresql.conf` и формирует текстовый файл конфигурации (либо список команд `ALTER SYSTEM`).

Ключевые инварианты, которые необходимо воспроизвести в любой реализации:

1. **Внутренняя единица измерения памяти — килобайт (KB), где 1 KB = 1024 байта.** Все промежуточные вычисления объёмов памяти выполняются в KB.
2. **Все единицы — двоичные** (1 MB = 1024 KB, 1 GB = 1024 MB, 1 TB = 1024 GB).
3. **Округления**: `floor(x)` — округление вниз до целого, `ceil(x)` — округление вверх до целого. Деления вида `RAM_KB / 4`, `RAM_KB / 16`, `(RAM_KB * 3) / 4` — целочисленные: берётся `floor` частного.
4. В формуле `work_mem` (раздел 4.13) деление выполняется **вещественным**, а округления применяются после остальных арифметических шагов.
5. Выходной параметр, значение которого **не определено** (не вычисляется для данного входа), **не включается** в итоговую конфигурацию.

---

## 2. Входные параметры

| Параметр | Тип | Допустимые значения | Обязательный | Значение по умолчанию |
|---|---|---|---|---|
| `dbVersion` | целое число | 18, 17, 16, 15, 14, 13, 12, 11, 10 | да | 18 |
| `osType` | перечисление | `linux`, `windows`, `mac` | да | `linux` |
| `dbType` | перечисление | `web`, `oltp`, `dw`, `desktop`, `mixed` | да | `web` |
| `totalMemory` | целое число | ≥ 1 (при единице `MB` — ≥ 512); ≤ 999999 | да | — |
| `totalMemoryUnit` | перечисление | `MB`, `GB`, `TB` | да | `GB` |
| `cpuNum` | целое число | 1…999999 | нет | — |
| `connectionNum` | целое число | 20…999999 | нет | — |
| `hdType` | перечисление | `ssd`, `san`, `hdd`, `nvme` | да | `ssd` |
| `dbSize` | перечисление | `less_ram`, `mid_ram`, `greater_ram` | да | `mid_ram` |

Семантика входных параметров:

- `dbVersion` — версия PostgreSQL сервера (`SELECT version();`).
- `osType` — операционная система хоста PostgreSQL.
- `dbType` — тип рабочей нагрузки: `web` — веб-приложение, `oltp` — OLTP-система, `dw` — хранилище данных (data warehouse), `desktop` — настольное приложение, `mixed` — смешанный тип.
- `totalMemory` + `totalMemoryUnit` — объём памяти, доступной PostgreSQL. Число задаётся в выбранных единицах.
- `cpuNum` — число процессоров (ядер/потоков), доступных PostgreSQL: `CPU = threads per core × cores per socket × sockets`. Если не задано, часть параметров (параллельные операции, autovacuum-воркеры, io_workers) не рассчитывается.
- `connectionNum` — максимальное число клиентских подключений. Если не задано, используется значение по типу БД (см. 4.1).
- `hdType` — тип дисковой подсистемы: `ssd`, `san` (сетевое хранилище), `hdd`, `nvme`.
- `dbSize` — ожидаемый размер базы данных относительно RAM: `less_ram` — меньше RAM, `mid_ram` — от 1× до 3× RAM, `greater_ram` — больше 3× RAM.

Константы перевода единиц (байт в единице):

| Единица | Байт |
|---|---|
| KB | 1 024 |
| MB | 1 048 576 |
| GB | 1 073 741 824 |
| TB | 1 099 511 627 776 |
| PB | 1 125 899 906 842 624 |

Предварительное вычисление:

```
totalMemoryKB = totalMemory × UNIT_BYTES[totalMemoryUnit] / 1024
где UNIT_BYTES — таблица констант перевода единиц (см. выше)
```

Значение по умолчанию для числа воркеров (используется в формуле `work_mem`, см. 4.12–4.13):

```
DEFAULT_max_worker_processes = 8
```

---

## 3. Сводная матрица коэффициентов по типу БД

| Параметр | `web` | `oltp` | `dw` | `desktop` | `mixed` |
|---|---|---|---|---|---|
| делитель `shared_buffers` | 4 | 4 | 4 | 16 | 4 |
| коэффициент `effective_cache_size` | 3/4 | 3/4 | 3/4 | 1/4 | 3/4 |
| делитель `maintenance_work_mem` | 16 | 16 | 8 | 16 | 16 |
| делитель базового `work_mem` | 1 | 1 | 2 | 6 | 2 |
| `max_connections` (если не задан) | 200 | 300 | 40 | 20 | 100 |
| `default_statistics_target` | 100 | 100 | 500 | 100 | 100 |
| `min_wal_size` | 1GB | 2GB | 4GB | 100MB | 1GB |
| `max_wal_size` | 4GB | 8GB | 16GB | 2GB | 4GB |

---

## 4. Выходные параметры и формулы

Ниже `RAM_KB` = `totalMemoryKB`, `SB` = значение `shared_buffers` в KB, `MaxConn` = итоговое значение `max_connections`. Порядок разделов отражает порядок вычислений (зависимости): `shared_buffers` нужно вычислить до `huge_pages`, `wal_buffers`, `work_mem` и `autovacuum_work_mem`; `maintenance_work_mem` — до `autovacuum_work_mem`; параллельные параметры — до `work_mem`.

### 4.0. Сводка выходных параметров

Значения памяти вычисляются в KB и приводятся к формату вывода по правилу 5.1. «Условие вывода» определяет, когда параметр включается в конфигурацию; при невыполнении условия параметр пропускается.

| Параметр | Тип значения | Условие вывода |
|---|---|---|
| `max_connections` | целое | всегда |
| `shared_buffers` | целое, KB | всегда |
| `effective_cache_size` | целое, KB | всегда |
| `maintenance_work_mem` | целое, KB | всегда |
| `checkpoint_completion_target` | вещественное, константа 0.9 | всегда |
| `wal_buffers` | целое, KB | всегда |
| `default_statistics_target` | целое | всегда |
| `random_page_cost` | вещественное | всегда |
| `effective_io_concurrency` | целое | только `osType = 'linux'` |
| `work_mem` | целое, KB | всегда |
| `huge_pages` | строка: `off` / `try` | всегда |
| `jit` | строка: `off` | PG ≥ 12 и `dbType` ∈ {web, oltp, mixed} |
| `wal_compression` | строка: `lz4` / `on` | PG ≥ 10 |
| `autovacuum_max_workers` | целое: 4 или 5 | `cpuNum` ≥ 16 |
| `autovacuum_work_mem` | целое, KB | `maintenance_work_mem` ≥ 2GB |
| `io_method` | строка: `io_uring` / `worker` | PG ≥ 18 |
| `io_workers` | целое | PG ≥ 18, `cpuNum` задан, `io_method` ≠ `io_uring`, результат > 3 |
| `min_wal_size` | целое, KB | всегда |
| `max_wal_size` | целое, KB | всегда |
| `max_worker_processes` | целое | `cpuNum` ≥ 4 |
| `max_parallel_workers_per_gather` | целое | `cpuNum` ≥ 4 |
| `max_parallel_workers` | целое | `cpuNum` ≥ 4 и PG ≥ 10 (для поддерживаемых версий — всегда) |
| `max_parallel_maintenance_workers` | целое | `cpuNum` ≥ 4 и PG ≥ 11 |
| `wal_level` | строка: `minimal` | только `dbType = 'desktop'` |
| `max_wal_senders` | целое: 0 | только `dbType = 'desktop'` |

### 4.1. `max_connections`

```
если connectionNum задан (не пуст):
    max_connections = connectionNum
иначе:
    max_connections = { web: 200, oltp: 300, dw: 40, desktop: 20, mixed: 100 }[dbType]
```

Всегда выводится.

### 4.2. `shared_buffers` (KB)

```
shared_buffers = floor(RAM_KB / D), где D = { web: 4, oltp: 4, dw: 4, desktop: 16, mixed: 4 }[dbType]

если dbVersion < 10 И osType = 'windows' И shared_buffers > 512MB (524288 KB):
    shared_buffers = 524288   // ограничение Windows
```

Примечание: среди поддерживаемых версий минимальная — 10, поэтому это правило не активируется ни на одном допустимом входе; оно сохранено для точного соответствия алгоритму PGTune.

### 4.3. `huge_pages`

```
если osType = 'mac':
    huge_pages = 'off'                       // macOS не поддерживает huge pages PostgreSQL
иначе:
    huge_pages = 'try', если SB >= 2GB (2097152 KB), иначе 'off'
```

Всегда выводится (значение `'off'` или `'try'`).

### 4.4. `effective_cache_size` (KB)

```
effective_cache_size = floor(RAM_KB × K), где K = { web: 3/4, oltp: 3/4, dw: 3/4, desktop: 1/4, mixed: 3/4 }[dbType]
```

Порядок операций: `floor((RAM_KB * 3) / 4)` либо `floor(RAM_KB / 4)` — сначала умножение, потом целочисленное деление с округлением вниз.

### 4.5. `maintenance_work_mem` (KB)

```
maintenance_work_mem = floor(RAM_KB / D), где D = { web: 16, oltp: 16, dw: 8, desktop: 16, mixed: 16 }[dbType]

лимит:
    limit = 8GB (8388608 KB)                          // значение по умолчанию
    если osType = 'windows' И dbVersion <= 17:
        limit = 2GB (2097152 KB)                      // жёсткий лимит 64-бит Windows

если maintenance_work_mem >= limit:
    если osType = 'windows' И dbVersion <= 17:
        maintenance_work_mem = limit − 1MB            // = 2097152 − 1024 = 2096128 KB;
                                                      // ровно 2GB вызывает ошибку на Windows в PG ≤ 17
    иначе:
        maintenance_work_mem = limit
```

### 4.6. `min_wal_size` и `max_wal_size` (KB)

Фиксированные значения по типу БД (в KB):

| Тип БД | `min_wal_size` | `max_wal_size` |
|---|---|---|
| `web` | 1GB (1 048 576 KB) | 4GB (4 194 304 KB) |
| `oltp` | 2GB (2 097 152 KB) | 8GB (8 388 608 KB) |
| `dw` | 4GB (4 194 304 KB) | 16GB (16 777 216 KB) |
| `desktop` | 100MB (102 400 KB) | 2GB (2 097 152 KB) |
| `mixed` | 1GB (1 048 576 KB) | 4GB (4 194 304 KB) |

Оба параметра выводятся всегда.

### 4.7. `checkpoint_completion_target`

Константа: `checkpoint_completion_target = 0.9`.

### 4.8. `wal_buffers` (KB)

Вычисляется от `shared_buffers` (SB). Правила применяются в указанном порядке:

```
wal_buffers = floor(3 × SB / 100)        // 3% от shared_buffers (автонастройка, PG 9.1+)

если wal_buffers > 16MB (16384 KB):
    wal_buffers = 16384                  // максимум 16MB

если 14MB (14336 KB) < wal_buffers < 16384 KB:
    wal_buffers = 16384                  // округление вверх до «красивых» 16MB

если wal_buffers < 32 KB:
    wal_buffers = 32                     // минимум
```

### 4.9. `default_statistics_target`

```
default_statistics_target = { web: 100, oltp: 100, dw: 500, desktop: 100, mixed: 100 }[dbType]
```

### 4.10. `random_page_cost`

Правила проверяются в указанном порядке, первое совпавшее определяет результат:

```
1. если dbSize = 'less_ram':        random_page_cost = 1.1
   // база целиком в RAM — цена произвольного чтения с диска нерелевантна
2. иначе, если hdType = 'hdd':      random_page_cost = 4
3. иначе, если dbType = 'dw':       random_page_cost = 4
   // для DW, не помещающейся в RAM, низкая стоимость опасна:
   // планировщик выберет медленные index scan по SSD вместо seq scan
4. иначе:                            random_page_cost = 1.1
```

### 4.11. `effective_io_concurrency`

```
если osType ≠ 'linux':
    параметр не выводится (null)
иначе:
    effective_io_concurrency = { hdd: 2, ssd: 200, san: 300, nvme: 1000 }[hdType]
```

### 4.12. Параллельные параметры

Рассчитываются только если `cpuNum` задан и `cpuNum >= 4`; иначе набор пуст (параметры не выводятся, а в формуле `work_mem` используется значение по умолчанию, см. ниже).

```
workers_per_gather = ceil(cpuNum / 2)
если dbType ≠ 'dw' И workers_per_gather > 4:
    workers_per_gather = 4        // нет доказательств пользы большего числа воркеров на ядро

max_worker_processes               = cpuNum
max_parallel_workers_per_gather    = workers_per_gather

если dbVersion >= 10:              // всегда истинно для поддерживаемых версий (10+)
    max_parallel_workers           = cpuNum

если dbVersion >= 11:
    parallel_maintenance_workers = ceil(cpuNum / 2)
    если parallel_maintenance_workers > 4:
        parallel_maintenance_workers = 4
    max_parallel_maintenance_workers = parallel_maintenance_workers
```

Для формулы `work_mem` дополнительно определяется:

```
parallel_for_work_mem:
    если параллельные параметры рассчитаны (cpuNum >= 4):   parallel_for_work_mem = max_worker_processes = cpuNum
    иначе:                                                  parallel_for_work_mem = 8   // DEFAULT_max_worker_processes
```

### 4.13. `work_mem` (KB)

```
work_mem_value = (RAM_KB − SB) / ((MaxConn + parallel_for_work_mem) × 3)
                 // деление вещественное; округление вниз — ниже

work_mem = floor(work_mem_value × M), где M = { web: 1, oltp: 1, dw: 1/2, desktop: 1/6, mixed: 1/2 }[dbType]
           // реализация: web/oltp → floor(work_mem_value);
           //             dw/mixed → floor(work_mem_value / 2);
           //             desktop  → floor(work_mem_value / 6)

если dbSize = 'less_ram':        work_mem = floor(work_mem × 1.3)
                                 // кэш ОС недогружен — можно ускорить сортировки/хеши
иначе, если dbSize = 'greater_ram': work_mem = floor(work_mem × 0.9)
                                 // кэш ОС под давлением — запас против OOM

если work_mem < 4MB (4096 KB):   work_mem = 4096          // минимум против сброса на диск

если osType = 'windows' И dbVersion <= 17:
    если work_mem > 2GB − 1MB (2096128 KB):
        work_mem = 2096128
```

Обоснование методики: `work_mem` выделяется на каждую сортировку/хеш, может выделяться многократно одним запросом, поэтому реальное потребление ближе к `max_connections × 2…3`; из доступной памяти вычитается `shared_buffers`; запас против OOM-killer даёт делитель 3: `(RAM − shared_buffers) / ((max_connections + max_worker_processes) × 3)`.

### 4.14. `wal_level` и `max_wal_senders`

Только для `dbType = 'desktop'`:

```
wal_level        = 'minimal'
max_wal_senders  = 0        // при wal_level = minimal значение max_wal_senders должно быть 0
```

Для остальных типов БД параметры не выводятся.

### 4.15. `jit`

```
если dbVersion >= 12 И dbType ∈ { web, oltp, mixed }:
    jit = 'off'        // JIT вызывает скачки CPU и замедление планирования коротких запросов
иначе:
    параметр не выводится (null)   // DW/desktop используют поведение PostgreSQL по умолчанию
```

### 4.16. `wal_compression`

```
если dbVersion >= 15:   wal_compression = 'lz4'   // быстрее pglz; требует сборки с --with-lz4
иначе, если dbVersion >= 10:  wal_compression = 'on'   // встроенный pglz
иначе:                  параметр не выводится (null)
```

### 4.17. `autovacuum_max_workers`

```
если cpuNum не задан:            параметр не выводится (null; умолчание PostgreSQL = 3)
если cpuNum >= 32:               autovacuum_max_workers = 5
иначе, если cpuNum >= 16:        autovacuum_max_workers = 4
иначе:                           параметр не выводится (null)
```

### 4.18. `autovacuum_work_mem` (KB)

Зависит от `maintenance_work_mem` (см. 4.5).

```
autovacuum_work_mem = null
если maintenance_work_mem >= 2GB (2097152 KB):
    autovacuum_work_mem = 2097152       // кап 2GB на воркер против OOM
                                        // (иначе 3–5 воркеров по 8GB — риск OOM)

если autovacuum_work_mem ≠ null И osType = 'windows' И dbVersion <= 17:
    если autovacuum_work_mem > 2GB − 1MB (2096128 KB):
        autovacuum_work_mem = 2096128
```

Примечание: на Windows с PG ≤ 17 `maintenance_work_mem` сам ограничен 2GB−1MB (см. 4.5), поэтому условие `>= 2GB` там не выполняется и параметр не выводится (используется значение `maintenance_work_mem` по умолчанию PostgreSQL, `autovacuum_work_mem = -1`).

Выводится только если значение ≠ `null`.

### 4.19. `io_method`

```
если dbVersion < 18:    параметр не выводится (null)   // асинхронный I/O появился в PG 18
если osType = 'linux':  io_method = 'io_uring'          // требует сборки с --with-liburing
иначе:                  io_method = 'worker'            // Windows/macOS не поддерживают io_uring
```

### 4.20. `io_workers`

Зависит от `io_method` (см. 4.19).

```
если dbVersion < 18 ИЛИ cpuNum не задан ИЛИ io_method = 'io_uring':
    параметр не выводится (null)
иначе:
    v = min(32, max(3, floor(cpuNum / 4)))    // ~25% ядер, кап 32 (жёсткий максимум PG)
    если v > 3:   io_workers = v              // выводится только если отличается от умолчания (3)
    иначе:        параметр не выводится (null)
```

Примечание: при `io_method = 'io_uring'` отдельная настройка `io_workers` не задаётся.

### 4.21. Предупреждения (comments)

Формируются как список текстовых строк и выводятся комментариями перед конфигурацией. Правила:

```
warnings = []

1. Память:
   если totalMemoryBytes < 256MB:   warnings += ['this tool not being optimal', 'for low memory systems']
   иначе, если totalMemoryBytes > 100GB:
                                    warnings += ['this tool not being optimal', 'for very high memory systems']

2. Если wal_compression = 'lz4':
   если warnings не пуст: warnings += ['']
   warnings += ['wal_compression = lz4 requires PostgreSQL', 'to be compiled with --with-lz4']

3. Если io_method = 'io_uring':
   если warnings не пуст: warnings += ['']
   warnings += ['io_method = io_uring requires PostgreSQL', 'to be compiled with --with-liburing']

4. Если dbType = 'dw' И hdType ≠ 'hdd' И dbSize ≠ 'less_ram':
   имя_носителя = { ssd: 'SSDs', nvme: 'NVMe drives', san: 'SAN storage' }[hdType]
   если warnings не пуст: warnings += ['']
   warnings += [
     'Cost parameters for Data Warehouses on {имя_носителя} are left at defaults',
     'to avoid catastrophic index scan selections',
     'Monitor query planner behavior and adjust random_page_cost if necessary'
   ]

если warnings не пуст: итоговый список = ['WARNING'] + warnings
```

Пустая строка `''` между группами — разделитель, выводится как пустая строка комментария.

---

## 5. Формат итогового вывода

### 5.1. Форматирование значений памяти (`formatValue`)

Значения в KB преобразуются к наиболее крупной единице **без потери точности**:

```
если KB % 1048576 == 0 (кратно 1GB в KB):  вывести floor(KB / 1048576) и суффикс 'GB'
иначе, если KB % 1024 == 0 (кратно 1MB):    вывести floor(KB / 1024) и суффикс 'MB'
иначе:                                       вывести KB и суффикс 'kB'
```

Примеры: `1048576` → `1GB`; `262144` → `256MB`; `149796` → `149796kB`; `2096128` → `2047MB` (2096128 = 2047×1024 + 0, при этом 2096128 % 1048576 = 1047552 ≠ 0 → MB).

Проверка кратности GB выполняется первой, поэтому кратные GB значения не выводятся в MB.

### 5.2. Порядок параметров в конфигурации

Файл состоит из трёх блоков: предупреждения (если есть), заголовок с входными данными, параметры.

Заголовок (комментарии, только непустые значения):

```
# DB Version: 15
# OS Type: linux
# DB Type: web
# Total Memory (RAM): 4 GB
# CPUs num: 4
# Connections num: 300
# Data Storage: ssd
```

Далее параметры строго в следующем порядке (параметр пропускается, если его значение `null`/не вычислено):

1. `max_connections` — число
2. `shared_buffers` — formatValue
3. `effective_cache_size` — formatValue
4. `maintenance_work_mem` — formatValue
5. `checkpoint_completion_target` — `0.9`
6. `wal_buffers` — formatValue
7. `default_statistics_target` — число
8. `random_page_cost` — число
9. `effective_io_concurrency` — число (только Linux)
10. `work_mem` — formatValue
11. `huge_pages` — `off` / `try`
12. `jit` — `off` (или пропуск)
13. `wal_compression` — `lz4` / `on` (или пропуск)
14. `autovacuum_max_workers` — число (или пропуск)
15. `autovacuum_work_mem` — formatValue (или пропуск)
16. `io_method` — `io_uring` / `worker` (или пропуск)
17. `io_workers` — число (или пропуск)
18. `min_wal_size` — formatValue
19. `max_wal_size` — formatValue
20. `max_worker_processes` — число (или пропуск)
21. `max_parallel_workers_per_gather` — число (или пропуск)
22. `max_parallel_workers` — число (или пропуск)
23. `max_parallel_maintenance_workers` — число (или пропуск)
24. `wal_level` — `minimal` (только desktop)
25. `max_wal_senders` — `0` (только desktop)

### 5.3. Два режима вывода

- **postgresql.conf**: строки вида `имя = значение`, комментарии начинаются с `#`.
- **ALTER SYSTEM**: каждая строка — `ALTER SYSTEM SET имя = 'значение';` (значение всегда в одинарных кавычках, включая числа), комментарии начинаются с `--`.

---

## 6. Псевдокод (эталонный алгоритм)

Последовательный алгоритм, эквивалентный формулам раздела 4, без привязки к какому-либо языку программирования. Все деления объёмов — с учётом правил раздела 1.

```text
ВХОД: dbVersion, osType, dbType, totalMemory, totalMemoryUnit,
      cpuNum (опц.), connectionNum (опц.), hdType, dbSize

KB = 1024; MB = KB * 1024; GB = MB * 1024
RAM_KB = totalMemory * UNIT_BYTES[totalMemoryUnit] / KB

# 4.1
IF connectionNum задан THEN maxConn = connectionNum
ELSE maxConn = MAP(maxConn, dbType)                # 200/300/40/20/100

# 4.2
SB = floor(RAM_KB / MAP(div_shared_buffers, dbType))          # 4/4/4/16/4
IF dbVersion < 10 AND osType = 'windows' AND SB > 512*MB/KB THEN SB = 512*MB/KB

# 4.3
IF osType = 'mac' THEN huge_pages = 'off'
ELSE IF SB >= 2*GB/KB THEN huge_pages = 'try' ELSE huge_pages = 'off'

# 4.4
ecs = floor(RAM_KB * 3 / 4)                       # desktop: floor(RAM_KB / 4)

# 4.5
mwm = floor(RAM_KB / MAP(div_maintenance, dbType))            # 16/16/8/16/16
limit = 8*GB/KB
IF osType = 'windows' AND dbVersion <= 17 THEN limit = 2*GB/KB
IF mwm >= limit THEN
    IF osType = 'windows' AND dbVersion <= 17 THEN mwm = limit - MB/KB
    ELSE mwm = limit

# 4.6–4.9
min_wal = MAP(min_wal_size, dbType)   # в KB: 1048576/2097152/4194304/102400/1048576
max_wal = MAP(max_wal_size, dbType)   # в KB: 4194304/8388608/16777216/2097152/4194304
cct = 0.9
wb = floor(3 * SB / 100)
IF wb > 16*MB/KB THEN wb = 16*MB/KB
IF wb > 14*MB/KB AND wb < 16*MB/KB THEN wb = 16*MB/KB
IF wb < 32 THEN wb = 32
dst = MAP(stat_target, dbType)        # 100/100/500/100/100

# 4.10
IF dbSize = 'less_ram' THEN rpc = 1.1
ELSE IF hdType = 'hdd' THEN rpc = 4
ELSE IF dbType = 'dw' THEN rpc = 4
ELSE rpc = 1.1

# 4.11
IF osType ≠ 'linux' THEN eio = NULL
ELSE eio = MAP(io_concurrency, hdType)            # 2/200/300/1000

# 4.12
parallel = пусто; parallel_for_work_mem = 8
IF cpuNum задан AND cpuNum >= 4 THEN
    wpg = ceil(cpuNum / 2)
    IF dbType ≠ 'dw' AND wpg > 4 THEN wpg = 4
    parallel += ('max_worker_processes', cpuNum)
    parallel += ('max_parallel_workers_per_gather', wpg)
    parallel_for_work_mem = cpuNum
    IF dbVersion >= 10 THEN parallel += ('max_parallel_workers', cpuNum)
    IF dbVersion >= 11 THEN
        pmw = min(4, ceil(cpuNum / 2))
        parallel += ('max_parallel_maintenance_workers', pmw)

# 4.13
wmv = (RAM_KB - SB) / ((maxConn + parallel_for_work_mem) * 3)   # вещественно
# web/oltp: wm = floor(wmv); dw/mixed: wm = floor(wmv / 2); desktop: wm = floor(wmv / 6)
wm = floor(wmv * MAP(mult_work_mem, dbType))                    # 1 / 1 / 0.5 / 1/6 / 0.5
IF dbSize = 'less_ram' THEN wm = floor(wm * 1.3)
ELSE IF dbSize = 'greater_ram' THEN wm = floor(wm * 0.9)
IF wm < 4*MB/KB THEN wm = 4*MB/KB
IF osType = 'windows' AND dbVersion <= 17 AND wm > 2*GB/KB - MB/KB THEN wm = 2*GB/KB - MB/KB

# 4.14
IF dbType = 'desktop' THEN wal_level = 'minimal'; max_wal_senders = 0

# 4.15
IF dbVersion >= 12 AND dbType ∈ {web, oltp, mixed} THEN jit = 'off' ELSE jit = NULL

# 4.16
IF dbVersion >= 15 THEN wcomp = 'lz4'
ELSE IF dbVersion >= 10 THEN wcomp = 'on' ELSE wcomp = NULL

# 4.17
avw = NULL
IF cpuNum задан THEN
    IF cpuNum >= 32 THEN avw = 5
    ELSE IF cpuNum >= 16 THEN avw = 4

# 4.18
avm = NULL
IF mwm >= 2*GB/KB THEN avm = 2*GB/KB
IF avm ≠ NULL AND osType = 'windows' AND dbVersion <= 17 AND avm > 2*GB/KB - MB/KB THEN
    avm = 2*GB/KB - MB/KB

# 4.19–4.20
iom = NULL
IF dbVersion >= 18 THEN iom = IF osType = 'linux' THEN 'io_uring' ELSE 'worker'
iow = NULL
IF dbVersion >= 18 AND cpuNum задан AND iom ≠ 'io_uring' THEN
    v = min(32, max(3, floor(cpuNum / 4)))
    IF v > 3 THEN iow = v

# 4.21 — предупреждения (см. раздел 4.21)

ВЫХОД: список пар (имя, значение) в порядке раздела 5.2, пропуская NULL,
       со значениями памяти, отформатированными по правилу 5.1
```

---

## 7. Контрольные примеры

Значения ниже вычислены строго по формулам этой спецификации и служат эталоном для самопроверки реализации: программа на любом языке обязана воспроизводить их один в один. Формат — итоговые строки `postgresql.conf`.

### Пример 1

Вход: PG 15, Linux, `web`, 4GB RAM, 4 CPU, 300 подключений, SSD, база 1×–3× RAM (`mid_ram`).

```ini
# DB Version: 15
# OS Type: linux
# DB Type: web
# Total Memory (RAM): 4 GB
# CPUs num: 4
# Connections num: 300
# Data Storage: ssd

max_connections = 300
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 4MB
huge_pages = off
jit = off
wal_compression = lz4
min_wal_size = 1GB
max_wal_size = 4GB
max_worker_processes = 4
max_parallel_workers_per_gather = 2
max_parallel_workers = 4
max_parallel_maintenance_workers = 2
```

Примечания: `work_mem` = 3449 KB по формуле, но поднимается до минимума 4MB; предупреждение о `--with-lz4` выводится перед заголовком (здесь опущено для компактности).

### Пример 2

Вход: PG 18, Linux, `dw`, 64GB RAM, 16 CPU, подключения не заданы (→ 40), NVMe, `mid_ram`.

```ini
# DB Version: 18
# OS Type: linux
# DB Type: dw
# Total Memory (RAM): 64 GB
# CPUs num: 16
# Data Storage: nvme

max_connections = 40
shared_buffers = 16GB
effective_cache_size = 48GB
maintenance_work_mem = 8GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 500
random_page_cost = 4
effective_io_concurrency = 1000
work_mem = 149796kB
huge_pages = try
wal_compression = lz4
autovacuum_max_workers = 4
autovacuum_work_mem = 2GB
io_method = io_uring
min_wal_size = 4GB
max_wal_size = 16GB
max_worker_processes = 16
max_parallel_workers_per_gather = 8
max_parallel_workers = 16
max_parallel_maintenance_workers = 4
```

Примечания: `jit` не выводится (DW); `io_workers` не выводится при `io_uring`; `max_parallel_workers_per_gather` = 8 — для DW не ограничивается четвёркой; предупреждения о `--with-lz4`, `--with-liburing` и cost-параметрах DW (здесь опущены).

### Пример 3

Вход: PG 18, Windows, `desktop`, 8GB RAM, CPU не заданы, подключения не заданы (→ 20), HDD, `mid_ram`.

```ini
# DB Version: 18
# OS Type: windows
# DB Type: desktop
# Total Memory (RAM): 8 GB
# Data Storage: hdd

max_connections = 20
shared_buffers = 512MB
effective_cache_size = 2GB
maintenance_work_mem = 512MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 4
work_mem = 15603kB
huge_pages = off
wal_compression = lz4
io_method = worker
min_wal_size = 100MB
max_wal_size = 2GB
wal_level = minimal
max_wal_senders = 0
```

Примечания: параллельные параметры отсутствуют (CPU не заданы); `effective_io_concurrency` отсутствует (не Linux); `jit` отсутствует (desktop); `parallel_for_work_mem` = 8 (умолчание), поэтому `work_mem` = floor(floor((8388608 − 524288)/((20+8)×3))/6) = 15603 KB.

### Пример 4 (ALTER SYSTEM)

Тот же расчёт, что в примере 1, в режиме `ALTER SYSTEM`:

```sql
-- DB Version: 15
-- OS Type: linux
-- DB Type: web
-- Total Memory (RAM): 4 GB
-- CPUs num: 4
-- Connections num: 300
-- Data Storage: ssd

ALTER SYSTEM SET max_connections = '300';
ALTER SYSTEM SET shared_buffers = '1GB';
ALTER SYSTEM SET effective_cache_size = '3GB';
ALTER SYSTEM SET maintenance_work_mem = '256MB';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = '100';
ALTER SYSTEM SET random_page_cost = '1.1';
ALTER SYSTEM SET effective_io_concurrency = '200';
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET huge_pages = 'off';
ALTER SYSTEM SET jit = 'off';
ALTER SYSTEM SET wal_compression = 'lz4';
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET max_worker_processes = '4';
ALTER SYSTEM SET max_parallel_workers_per_gather = '2';
ALTER SYSTEM SET max_parallel_workers = '4';
ALTER SYSTEM SET max_parallel_maintenance_workers = '2';
```

---

## 8. Нюансы, важные для реализации

1. **Округления.** Все `floor` обязательны; замена на округление к ближайшему даст расхождения с контрольными примерами. В `work_mem` порядок такой: вещественное деление → `floor` → множитель типа БД (внутри того же `floor`) → корректировка по `dbSize` (с новым `floor`) → минимум → кап Windows.
2. **Кап Windows «2GB − 1MB»** уникален: ровно 2GB вызывает ошибку PostgreSQL ≤ 17 на Windows, поэтому из лимита вычитается 1MB (2096128 KB). При форматировании 2096128 KB → `2047MB`.
3. **`max_wal_senders`** выводится со значением `0` только вместе с `wal_level = minimal` (desktop). Нулевое значение не является «отсутствующим» — правило пропуска параметров (раздел 1, п. 5) применяется только к не вычисленным значениям.
4. **Порядок правил важен**: в `random_page_cost` (`less_ram` сильнее типа диска), в `wal_buffers` (кап 16MB до «округления вверх» и минимум 32KB — последним).
5. **`parallel_for_work_mem`** — это именно `max_worker_processes` (= `cpuNum` при `cpuNum ≥ 4`), а не число воркеров на gather.
6. **`autovacuum_work_mem`** вычисляется только когда `maintenance_work_mem ≥ 2GB`; в остальных случаях не выводится вовсе (PostgreSQL при этом использует значение `maintenance_work_mem`).
7. **`dbSize` не включается** в заголовок конфигурации, хотя влияет на `random_page_cost` и `work_mem`.
8. **Числовая обработка входа.** До расчёта `dbVersion`, `totalMemory`, `cpuNum` и `connectionNum` приводятся к числам (`dbVersion` — целое, `totalMemory` — целое, `cpuNum`/`connectionNum` — целые либо «не задано»). Все сравнения версий (`< 10`, `>= 10`, `>= 11`, `>= 12`, `>= 15`, `>= 18`, `<= 17`) выполняются с числовым значением версии.
9. **Границы входных значений** (раздел 2) — часть контракта входных данных; реализация должна проверять их перед расчётом. Поведение алгоритма на значениях вне границ спецификацией не определено.
