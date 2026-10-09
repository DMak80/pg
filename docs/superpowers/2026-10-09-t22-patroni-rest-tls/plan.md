# t22-patroni-rest-tls — план реализации (TLS + per-cluster basic-auth на Patroni REST :8008)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Patroni REST `:8008` нод PgWorker перестаёт быть открытым HTTP: транспорт — TLS с серверными сертификатами per-install API-CA `kfw-install-ca`, мутационная грань — basic-auth одной пары на кластер (`rest_password`).

**Architecture:** Воркер выпускает серверные серты REST-эндпоинтов нод из per-install CA (кеш процесса, без etcd — канон серверных сертов KafkaWorker-нод), Spilo поднимает REST-TLS штатно из env `SSL_RESTAPI_*` + секции `restapi` в `SPILO_CONFIGURATION`; per-cluster кред `rest_password` — седьмой ensure-ключ `ClusterSecretEnsurer`, ротация — четвёртый секрет процесса I (rolling-пересоздание нод общим шагом надзора, txn-коммит R3). Все клиенты `:8008` (воркер, Patroni-ноды, lease-скрипт, панель, Prometheus, эмуляторы) — единая https-ветка.

**Tech Stack:** .NET 10 / C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), System.Security.Cryptography (CertificateRequest, RSA-2048, PKCS#8 PEM), docker Engine API, python3 (lease-скрипт/эмуляторы), openssl (gen.sh), shell-чеки стенда.

**Spec:** `docs/superpowers/2026-10-09-t22-patroni-rest-tls/spec.md` (исполняется вместе с планом; аргумент плана — от спеки).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t22-patroni-rest-tls-2`, feature-ветка `feat-t22-patroni-rest-tls-2` (коммиты свободные, по шагам).

## Global Constraints

- **arch-first**: правки `arch/**` (задачи 1–9) — ДО кодовых задач; формулировки дельт — из spec §4, без пересказа другими словами.
- **Язык**: комментарии/доки — русский; идентификаторы — английский. `TreatWarningsAsErrors=true` — код компилируется без ворнингов.
- **Команды тестов** — всегда с `DOTNET_CLI_UI_LANGUAGE=en`; решение — `src/PgWorker.slnx` из КОРНЯ worktree: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter <фильтр>`.
- **docker-серии** — только с `PGW_TEST_DOCKER=1`; перед серией `bash dev-stand/images/pull-images.sh`; после КАЖДОЙ серии — зачистка: `docker ps -aq --filter name=pgw- | xargs -r docker rm -f` (свои остатки), страховка `docker network prune -f`; контроль: `docker network ls | grep -c 'pgw-'` → 0 при пустом `docker ps`.
- **Таймауты фикстур**: `BrokerBootSec`-аналоги ≤100 с; ожидания в тестах — поллинг с ранним выходом, никаких фиксированных sleep >30 с.
- **Порты**: только динамические/зонд свободных портов; никаких литералов вида `:16000` в expects.
- **E2E**: изоляция per-contour по канону `docs/e2e-isolation.md` (guid во всех именах, own-only чистка, ассерт чистоты, телеметрия `docs/e2e-launch.md`: `[PHASE]`-метки, артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `MarkFailed()` оставляет контейнеры для разбора). E2E-образы: publish на хосте, в контейнере — только артефакты.
- **MULTI-HOST**: серт ноды — не состояние (кеш процесса, SAN детерминирован именем); креды — реплицированный etcd. Никаких новых локальных состояний.
- **Новые NuGet-пакеты** не вводятся (всё — BCL).

---

## Фаза Д0 — arch-first (задачи 1–9)

Правки `arch/**` — только тексты, без кода. Источник формулировок — spec §4 (колонка «Правка»). Никаких исторических пассажей («в рамках t22», «найдено при…») — только текущее/планируемое состояние.

### Задача 1: arch/13 — сетевая модель (матрица, принцип 4, TLS-таблица, модель угроз)

**Spec:** §4 (строки `arch/13`), §1, §3.
**Вход:** spec утверждён; worktree чистый (`git status`).
**Files:** Modify: `arch/13-network-security.md`.

- [ ] Шаг 1. §2 матрица «кто → куда»: строку «HAProxy → Patroni REST 8008 HTTP без аутентификации» заменить на «HTTPS (серт kfw-install-ca); health-GET без basic-auth (семантика Patroni); unsafe-эндпоинты — basic-auth per-cluster». Строку «сверяющий демон (P11)» заменить на «PgWorker → Patroni REST :8008: HTTPS, basic-auth per-cluster (мутации)». Строку мониторинга → «Prometheus → Patroni :8008 HTTPS scrape (ca.pem), сеть pgw-metrics». Добавить строки: «панель → Patroni REST :8008 HTTPS (GET /cluster, ca.pem)»; «Patroni-нода → Patroni-нода :8008 HTTPS (межнодовые, basic-auth кластера)»; «lease-скрипт ноды → 127.0.0.1:8008 HTTPS (ca-файл ноды, P11)».
- [ ] Шаг 2. §3 принцип 4: состав клиентов `:8008` — прежний плюс панель, Prometheus, межнодовые вызовы Patroni и loopback lease-скрипт ноды (P11) — всё по TLS.
- [ ] Шаг 3. §4 таблица TLS: строку «Patroni `:8008`, HAProxy `:7000` — без TLS» → «Patroni `:8008` — TLS: серверные серты per-install CA (SAN — arch/14 §2.4), unsafe-эндпоинты + basic-auth per-cluster; верификация клиентами — цепочка к CA без hostname-проверки (канон P17)». HAProxy `:7000` остаётся как есть.
- [ ] Шаг 4. §6 модель угроз, строка «перехват сети»: `:8008` перенести в колонку «зашифровано» (HTTPS; мутационная грань — basic-auth).
- [ ] Шаг 5. Проверка перечитыванием: каждая формулировка совпадает со spec §4 по смыслу дословно; разделы не содержат упоминаний задачи.
- [ ] Шаг 6. Commit: `git add arch/13-network-security.md && git commit -m "arch/13: Patroni REST :8008 — TLS + basic-auth per-cluster (t22)"`.

**Выход:** arch/13 отражает TLS-состояние `:8008`.
**Проверка:** `git diff HEAD~1 --stat` — один файл; текстовый ревью против spec §4.
**Связь со spec:** §4 arch/13 (все четыре раздела).

### Задача 2: arch/14 — контракт и секреты (§1.1, §3.3, §4, §8)

**Spec:** §4 (строки arch/14 §1.1/§3.3/§4 гр.1/гр.3/§8), §5.1, §5.3.
**Вход:** задача 1 слита в ветку.
**Files:** Modify: `arch/14-pgworker.md`.

- [ ] Шаг 1. §1.1 API-таблица, строка `POST /api/clusters/{c}/secrets/rotate`: состав — app + bucket_admin + mover + `rest_password` (формат заявки/протокол — без изменений, 02 §9.8; состав ротируемого — домен воркера).
- [ ] Шаг 2. §3.3, строка `/pgworker/rotations/<C>`: заявка на ротацию per-cluster секретов ВСЕГО кластера — app, bucket_mover, bucket_admin И `rest_password`; исполнение §5 I (для rest — rolling-пересоздание + txn-коммит).
- [ ] Шаг 3. §4 группа 1: новый per-cluster секрет `rest_password` (etcd, ensure txn put-if-absent, `AppSecretGenerator`, 32 симв; username — константа `patroni`, отдельного user-ключа нет: Patroni требует пару username+password в конфиге каждой ноды). Ротация — процесс I (заявка панели, применение — rolling-пересоздание нод, §5 I). Экспозиция — класс `app_password`/`mover_password` (etcd без per-key ACL; парсеры панели его не разбирают, в UI не попадает); вынос секретов из etcd — `t02-external-secret-manager` (pgworker-трек), класс экспозиции не расширяется.
- [ ] Шаг 4. §4 группа 3: новые env `PGW_REST_TLS_CA`/`PGW_REST_TLS_CA_KEY` (`…_PATH`-варианты из TLS-тома `/tls`): per-install API-CA и его ключ для выпуска серверных сертов REST-эндпоинтов нод; отсутствие/неполнота пары — fail-fast валидации старта воркера (HTTP-режим провижининга не существует; WAF-фикстуры передают тестовый CA — двойной семантики нет).
- [ ] Шаг 5. §8 (Конфигурация): блок `PgWorker:Docker:RestTls {CaPem|CaPath, CaKeyPem|CaKeyPath}` — per-install CA + ключ выпуска сертов нод; env-имена `PGW_REST_TLS_*`; оба обязательны (fail-fast старта, симметрия запрета `Pgtune:DbType=desktop`); WAF-фикстуры — тестовый CA.
- [ ] Шаг 6. Commit: `git commit -m "arch/14: rest_password + PGW_REST_TLS_* (t22)"` (только arch/14).

**Выход:** контракт воркера знает четвёртый ротируемый секрет и TLS-опции.
**Проверка:** ревью diff против spec §4.
**Связь со spec:** §4 arch/14 §1.1/§3.3/§4/§8; §5.1 (опции), §5.3 (креды).

### Задача 3: arch/14 — механика REST-TLS ноды (§2.1, §2.4)

**Spec:** §4 (arch/14 §2.1/§2.4), §5.2, §3 (SAN-канон).
**Вход:** задача 2 в ветке.
**Files:** Modify: `arch/14-pgworker.md`.

- [ ] Шаг 1. §2.1 дополнить абзацем механики REST-TLS ноды: env `SSL_RESTAPI_CERTIFICATE`/`SSL_RESTAPI_PRIVATE_KEY`/`SSL_RESTAPI_CA` (PEM, выпускает воркер из per-install CA при сборке env; материализация файлов и `restapi.certfile/keyfile/cafile` — штатный Spilo); секция `restapi` SPILO_CONFIGURATION: `connect_address: <полное имя ноды>:8008` (DNS per-cluster сети, перекрывает шаблонный IP; api_url в DCS = DNS из SAN) + `authentication {username, password}` — per-cluster пара; серт ноды — не etcd-состояние (генерация + кеш процесса, канон серверных сертов KafkaWorker); lease-скрипт мастер-ключа (P11) в `on_start` опрашивает `https://127.0.0.1:8008/primary` с верификацией по ca-файлу ноды (entrypoint материализует PEM из env в файл и пишет путь строкой в pgw-node.env — PEM в KEY=VALUE-файл не переносится).
- [ ] Шаг 2. §2.4 дополнить SAN-каноном серта REST-эндпоинта: DNS `<n>` + DNS `pgw-<C>-<X>-<n>` + IP `127.0.0.1`; advertised-хост в SAN не входит — клиенты advertised-адресов верифицируют цепочку без hostname-проверки.
- [ ] Шаг 3. Commit: `git commit -m "arch/14: механика REST-TLS ноды и SAN-канон (t22)"`.

**Выход:** механика ноды канонизирована.
**Проверка:** ревью diff против spec §4/§5.2.
**Связь со spec:** §4 arch/14 §2.1/§2.4; §5.2; §3 SAN.

### Задача 4: arch/14 — процессы (§5 A, §5 C, §5 I)

**Spec:** §4 (arch/14 §5 A/C/I), §5.4, §5.5.
**Вход:** задача 3 в ветке.
**Files:** Modify: `arch/14-pgworker.md`.

- [ ] Шаг 1. §5 A (ProvisioningProcess): P2.1 — сборка env ноды дополнительно несёт REST-TLS-материал (`SSL_RESTAPI_*` PEM + секция `restapi` SPILO_CONFIGURATION с `connect_address`/`authentication` из per-cluster `rest_password`).
- [ ] Шаг 2. §5 C (NodeSupervisor): пробы/сверки — HTTPS к `host:patroni-port` portalloc с верификацией цепочки к kfw-install-ca без hostname-проверки; PATCH `/config` и POST `/switchover` несут `Authorization: Basic` per-cluster (GET — без заголовка). Новый общий шаг пересоздания нод с ДВУМЯ входами: (а) REST-TLS-конвергенция живых канонических нод — инспекция env существующего контейнера (не более одного пересоздания на тик надзора кластера; живой лидер — сначала graceful-switchover, затем снос следующим тиком — семантика TO_RECREATE-soft; volume сохраняется; гвард кворума); (б) заказ rolling-ротации REST-пары от процесса I (§5 I) — та же механика, критерий выбора ноды — hash пары в env.
- [ ] Шаг 3. §5 I (ClusterSecretRotator): процесс ротации пополняется четвёртым секретом — `rest_password` (заявка та же, ставит панель). R1 — ensure четвёрки (OLD). R2 — NEW×4: SQL-тройка — как сегодня (ALTER ROLE на мастере каждого dsn-шарда); `rest_password` — ROLLING-пересоздание нод кластера общим шагом надзора (§5 C: ≤1 нода/тик, лидер — soft-switchover со сносом следующим тиком, volume сохраняется, гварды кворума/restore/TO_REMOVE; шард без dsn — skip: его ноды создадутся с новой парой после R3). Пара NEW «в полёте» фиксируется полем `rest_pending` журнала работы (`/pgworker/work/<C>`, op=rotate) — повтор тиками ПРОДОЛЖАЕТ проход с той же парой; прогресс по нодам — факт env (`PGW_REST_PASSWORD_HASH`, инспекция) — takeover-безопасен. R3 — ОДНА txn пополняется compare `rest_password`==OLD и put NEW (коммит после применения на ВСЕХ нодах); сброс `rest_pending` — фазовой записью done. Проигрыш compare — re-read и повтор прохода со свежей парой. Окно rolling: ноды разных поколений несут разные пары (dual-auth у Patroni нет) → межнодовые REST-вызовы между поколениями получают 401 — HA-инициации Patroni деградированы; запись/репликация/DCS-heartbeat/master-ключ НЕ затронуты. Гварды окна: воркер не инициирует switchover и PATCH `/config` кластера по REST до R3 (мутации ретраются после txn). Сборка env нод в окне берёт пару из `rest_pending` (эффективная пара: pending ?? ключ). Длительность окна — единицы минут, записи не рвёт.
- [ ] Шаг 4. Commit: `git commit -m "arch/14: процессы — REST-TLS env, https-пробы, шаг пересоздания, ротация rest_password (t22)"`.

**Выход:** процессы описаны с rest-гранью.
**Проверка:** ревью diff против spec §4 (три строки arch/14 §5) и §5.5 спеки.
**Связь со spec:** §4 arch/14 §5 A/C/I; §5.4; §5.5.

### Задача 5: arch/14 — риск R16 (§9)

**Spec:** §4 (arch/14 §9), §3 (граница ca.key).
**Вход:** задача 4 в ветке.
**Files:** Modify: `arch/14-pgworker.md` (§9 Риски).

- [ ] Шаг 1. Добавить R16: компрометация `ca.key` = выпуск серверных сертов REST/API от имени установки. Митигация: ключ в TLS-томе поставки (ro), права 600, в etcd/registry не попадает; на стенде панель/as-prometheus монтируют `deploy/tls` целиком (bind ro — домашний контур, принято; точечное монтирование — опция поставки); потеря deploy-хоста = потеря `ca.key` — обязательный бэкап пакета `deploy/tls` (runbook), серты живых нод не инвалидируются.
- [ ] Шаг 2. Commit: `git commit -m "arch/14: R16 — граница ca.key (t22)"`.

**Выход:** риск зафиксирован.
**Проверка:** ревью diff.
**Связь со spec:** §4 arch/14 §9; §3 MULTI-HOST.

### Задача 6: arch/18 — метрики (§2.5, §5.2, §5.4, §6)

**Spec:** §4 (arch/18), §5.7.
**Вход:** задача 5 в ветке.
**Files:** Modify: `arch/18-metrics.md`.

- [ ] Шаг 1. §2.5: скрейп реальных нод — `scheme: https` + `tls_config.ca_file`; сетевые таргеты `alias:8008` верифицируются полностью (имя = SAN). Advertised-ветка (запись без alias) — деградационная зона шире прежней: не только достижимость, но и TLS-верификация имени по advertised-хосту не гарантируется (SAN его не несёт) — таргет уходит в down; в поставке таких таргетов нет (демо-кластер несёт static-джобу `patroni`).
- [ ] Шаг 2. §5.2: джоба `patroni-nodes`: `scheme: https` + `tls_config {ca_file: /tls/ca.pem}`. Джоба `patroni` (эмуляторы `hc*`): `scheme: https` + тот же `ca_file` (серт эмуляторов — из per-install CA, gen.sh).
- [ ] Шаг 3. §5.4: механика file_sd/единой сети — без изменений; фиксируется: REST-скрейп нод и эмуляторов — по TLS одного per-install CA.
- [ ] Шаг 4. §6 (Тестирование): приёмка — docker-E2E REST-TLS + https-ассерты E2E file_sd; чек 65 — patroni/patroni-nodes по https.
- [ ] Шаг 5. Commit: `git commit -m "arch/18: REST-скрейп по TLS per-install CA (t22)"`.

**Выход:** телеметрия — TLS.
**Проверка:** ревью diff.
**Связь со spec:** §4 arch/18; §5.7.

### Задача 7: arch/02 — порт 8008

**Spec:** §4 (arch/02), §1.
**Вход:** задача 6 в ветке.
**Files:** Modify: `arch/02-topology.md` (§2 таблица портов).

- [ ] Шаг 1. Строку `8008/tcp` → «Patroni REST API — HTTPS (серт kfw-install-ca; unsafe-эндпоинты — basic-auth per-cluster)».
- [ ] Шаг 2. Commit: `git commit -m "arch/02: 8008 — HTTPS (t22)"`.

**Выход:** таблица портов актуальна.
**Проверка:** ревью diff.
**Связь со spec:** §4 arch/02.

### Задача 8: arch/adminpanel — проба и эмуляторы (02 §6.1, 04 §1)

**Spec:** §4 (arch/adminpanel), §5.6, §5.8.
**Вход:** задача 7 в ветке.
**Files:** Modify: `arch/adminpanel/02-etcd-contract.md` (§6.1), `arch/adminpanel/04-local-stand.md` (§1).

- [ ] Шаг 1. 02 §6.1: проба `GET https://<host>:<port>/cluster` — TLS с доверием per-install CA (bind `deploy/tls` → `/tls-workers/ca.pem`, уже смонтирован; env `WORKERS_PANEL_TLS_SERVER_CA_PATH`), hostname-проверка не выполняется (верификация цепочки — канон P17-стиль, прецедент CustomRootTrust). Basic-auth не требуется (GET-эндпоинт вне зоны authentication); User-Agent `AdminPanel` остаётся.
- [ ] Шаг 2. 04 §1 таблица hc*: эмуляторы слушают HTTPS `:8008` (серт `hc.crt` из per-install CA, SAN `hc1a,hc1b,hc2a,hc2b` + `127.0.0.1` — host-публикации 8011–8022 для чеков curl); HostMap-значения схемы адресов не меняют (`hc1a:8008` и т.д.).
- [ ] Шаг 3. Commit: `git commit -m "arch/adminpanel: patroni-проба https, эмуляторы hc* TLS (t22)"`.

**Выход:** контракт панели и стендовая таблица актуальны.
**Проверка:** ревью diff.
**Связь со spec:** §4 adminpanel; §5.6; §5.8.

### Задача 9: сопровождающие документы (.env.example, runbook)

**Spec:** §4 (сопровождающие), §3 (бэкап deploy/tls).
**Вход:** задача 8 в ветке.
**Files:** Modify: `deploy/.env.example`, `docs/runbook.md`.

- [ ] Шаг 1. `deploy/.env.example`, раздел TLS-пакета: комментарий — наполнение deploy-тома теперь включает `ca.key` (выпуск REST-сертов нод воркером; env `PGW_REST_TLS_*` в deploy-compose).
- [ ] Шаг 2. `docs/runbook.md`: новый раздел «REST-TLS Patroni»: диагностика проб/скрейпа при TLS-отказе (включая окно REST-ротации: межпоколенческие 401 самозакрываются txn-коммитом); бэкап/восстановление пакета `deploy/tls` с явным предупреждением: `gen.sh` на пустом месте поднимет НОВЫЙ CA и сломает доверие rebuild-сертов — восстанавливать ДО новых выпусков.
- [ ] Шаг 3. Commit: `git commit -m "docs: runbook REST-TLS Patroni + env.example TLS-пакет с ca.key (t22)"`.

**Выход:** эксплуатационные документы готовы.
**Проверка:** ревью diff.
**Связь со spec:** §4 сопровождающие; §8 п.7.

---

## Фаза Д1 — PKI и креды (задачи 10–14)

### Задача 10: RestPki — выпуск серверных сертов REST-эндпоинтов (PgWorker.Core)

**Spec:** §5.1 (выпуск), §3 (SAN).
**Вход:** фаза Д0 в ветке.
**Files:**
- Create: `src/PgWorker.Core/Templates/RestPki.cs`
- Test: `src/tests/PgWorker.UnitTests/Templates/RestPkiTests.cs`

**Interfaces (Produces):**
```csharp
namespace PgWorker.Core.Templates;
public static class RestPki
{
    // SAN: DNS <node> + DNS <pgw-C-X-n> + IP 127.0.0.1; CN = полное имя ноды;
    // EKU ServerAuth; RSA-2048; срок 10 лет с зажимом NotAfter в NotAfter CA.
    public static (string CertPem, string KeyPem) IssueNodeCertificate(
        string caCertPem, string caKeyPem, string nodeName, string nodeFullName);
    public static bool TryParseCertificate(string pem, out X509Certificate2? certificate);
    public static bool TryParseRsaKey(string pem, out RSA? key);
}
```
(Порт `ClusterPki` из KafkaWorker.Core/Templates/ClusterPki.cs — механика DecodePemBlock/TryParse 1:1; EKU — только ServerAuth.)

- [ ] Шаг 1. Тест (TDD): `RestPkiTests` — кейсы: (а) SAN содержит DNS `<n>` и DNS `pgw-<C>-<X>-<n>` и IP `127.0.0.1`, CN = полное имя; (б) EKU ServerAuth (нет ClientAuth); (в) NotAfter серта ≤ NotAfter CA (зажим: CA с коротким сроком — serт зажат); (г) выпущенный серт верифицируется цепочкой против CA (`TlsChain`-паттерн: X509Chain CustomRootTrust); (д) битый PEM в TryParseCertificate/TryParseRsaKey → false. Тестовый CA — RSA-2048 self-signed .NET (образец `E2eTestPki.GenerateCa` / `ClusterPki.GenerateCa`).
- [ ] Шаг 2. Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestPkiTests` → FAIL (класса нет).
- [ ] Шаг 3. Реализация `RestPki` (порт ClusterPki: CertificateRequest RSA-2048 SHA256, SubjectAlternativeNameBuilder, зажим NotAfter, PKCS#8 PEM-ключ).
- [ ] Шаг 4. Прогон → PASS.
- [ ] Шаг 5. Commit: `git add src/PgWorker.Core/Templates/RestPki.cs src/tests/PgWorker.UnitTests/Templates/RestPkiTests.cs && git commit -m "feat: RestPki — серверные серты REST-эндпоинтов нод (t22 Д1)"`.

**Выход:** выпуск сертов доступен Core.
**Проверка:** фильтр `RestPkiTests` зелёный; компиляция без ворнингов.
**Связь со spec:** §5.1 выпуск; §3 SAN-канон; §5.10 юниты RestPki.

### Задача 11: RestCertificateCache — кеш процесса

**Spec:** §5.1 (кеш).
**Вход:** задача 10.
**Files:**
- Create: `src/PgWorker.Core/Templates/RestCertificateCache.cs`
- Test: `src/tests/PgWorker.UnitTests/Templates/RestCertificateCacheTests.cs`

**Interfaces (Produces):**
```csharp
public sealed class RestCertificateCache(string caCertPem, string caKeyPem)
{
    // Один серт на (cluster, shard, node, hash CA-ключа) в рамках жизни процесса.
    public (string CertPem, string KeyPem) GetOrCreate(string cluster, string shard, string nodeName);
}
```
(Порт `BrokerCertificateCache`: ключ словаря `(Cluster, Shard, Node, CaHash)`; CaHash = sha256(CaKeyPem) hex; полное имя ноды `pgw-{cluster}-{shard}-{node}` строится внутри — в `RestPki.IssueNodeCertificate`.)

- [ ] Шаг 1. Тест: повторный `GetOrCreate` — тот же PEM (стабильность); второй кеш с ДРУГИМ CA-ключом — другой серт (смена CA); серт из кеша проходит RestPki-верификацию против CA.
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация (ConcurrentDictionary, внутренний вызов `RestPki.IssueNodeCertificate(caCertPem, caKeyPem, nodeName, nodeFullName)`).
- [ ] Шаг 4. Прогон → PASS.
- [ ] Шаг 5. Commit: `git commit -m "feat: RestCertificateCache (t22 Д1)"`.

**Выход:** идемпотентный выпуск сертов на процессе.
**Проверка:** фильтр `RestCertificateCacheTests` зелёный.
**Связь со spec:** §5.1 кеш; §5.10 юниты кеша.

### Задача 12: опции RestTls + env-биндинги + fail-fast старта

**Spec:** §5.1 (опции/валидация), §4 (arch/14 §4 гр.3/§8).
**Вход:** задача 10 (TryParse для валидации).
**Files:**
- Modify: `src/PgWorker.App/Options.cs` (DockerOptions + новый `RestTlsOptions`)
- Modify: `src/PgWorker.App/DockerEnvBindings.cs` (биндинги `PGW_REST_TLS_*`)
- Modify: `src/PgWorker.App/Program.cs` (Validate + разбор PEM на старте + DI-материал)
- Test: `src/tests/PgWorker.UnitTests/Docker/DockerEnvBindingsTests.cs` (кейсы REST), Create `src/tests/PgWorker.UnitTests/App/RestTlsOptionsTests.cs`

**Interfaces (Produces):**
```csharp
// Options.cs
public sealed class RestTlsOptions
{
    public string? CaPem { get; set; }
    public string? CaPath { get; set; }
    public string? CaKeyPem { get; set; }
    public string? CaKeyPath { get; set; }
}
// DockerOptions: public RestTlsOptions? RestTls { get; set; }
// Program.cs: DI-синглтон RestTlsMaterial (CaPem, CaKeyPem, X509Certificate2 Ca) — после валидации.
```
Env-биндинги: `PGW_REST_TLS_CA`→`PgWorker:Docker:RestTls:CaPem`, `PGW_REST_TLS_CA_PATH`→`CaPath`, `PGW_REST_TLS_CA_KEY`→`CaKeyPem`, `PGW_REST_TLS_CA_KEY_PATH`→`CaKeyPath` (паттерн TlsBindings; `_PATH`-файл читается там же, где читаются TLS/SSH-пути — точка чтения файла существующих `_PATH`-секретов).

- [ ] Шаг 1. Тесты: DockerEnvBindingsTests — каждая env попадает в конфиг-ключ; RestTlsOptionsTests — предикат полноты/валидации: оба PEM заданы и разбираются → ok; отсутствует CA / отсутствует ключ / битый PEM → fail (точная diagnose-строка содержит `PGW_REST_TLS_*`).
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация: `RestTlsOptions` + биндинги + в Program.cs цепочку `.Validate(...)` (после Pgtune-валидации): «PgWorker:Docker:RestTls обязателен: оба PGW_REST_TLS_{CA,CA_KEY}[_PATH] (per-install CA для REST-TLS нод; HTTP-режим не существует)»; после `ValidateOnStart` — чтение/разбор PEM (`RestPki.TryParseCertificate`/`TryParseRsaKey`), при сбое — ApplicationException с diagnose; регистрация `RestTlsMaterial` в DI.
- [ ] Шаг 4. Прогон: юниты зелёные; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` без ворнингов.
- [ ] Шаг 5. Commit: `git commit -m "feat: опции PGW_REST_TLS_* + fail-fast валидации старта (t22 Д1)"`.

**Выход:** воркер без TLS-пакета не стартует; с пакетом — материал в DI.
**Проверка:** фильтры `DockerEnvBindingsTests|RestTlsOptionsTests` зелёные.
**Связь со spec:** §5.1 опции/валидация; §5.10 (валидация старта — юниты).

### Задача 13: rest_password — седьмой ensure-ключ + снапшот

**Spec:** §5.3.
**Вход:** задача 12.
**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ClusterSecretEnsurer.cs` (RawSecrets/ClusterCredentials/ключи)
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (case `rest_password`)
- Modify: `src/PgWorker.Core/Model/Domain.cs` (`ClusterSnapshot.RestPassword`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretEnsurerTests.cs`, `src/tests/PgWorker.UnitTests/EtcdFixtures.cs` (+ключ в фикстуры), парсер-тесты.

**Interfaces (Produces):**
```csharp
// ClusterCredentials + поле:
public sealed record ClusterCredentials(... , string RestPassword);
// ClusterSnapshot + поле:
public sealed record ClusterSnapshot(..., string? RestPassword = null);
// Ключ: /clusters/<C>/rest_password (RestKey), генерация AppSecretGenerator.Generate().
```

- [ ] Шаг 1. Тесты (AAA-комментарии): ClusterSecretEnsurerTests — (а) пустой etcd → ensure кладёт 7 ключей txn NotExists, re-read полный; (б) существующий `rest_password` не перезаписывается (put-if-absent); (в) кластер с шестью ключами без rest → добирает только его. Парсер: ключ `rest_password` попадает в `ClusterSnapshot.RestPassword` (фикстуру `clusters-app-secret.json` дополнить ключом).
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация: `RawSecrets(+RestPassword)`, `AddIfAbsent(RestKey...)`, `IsComplete`/`ToCredentials`/diagnose-строки; парсер: `case "rest_password" when segments.Length == 4` → `acc.RestPassword`; `BuildCluster` прокидывает в `ClusterSnapshot`.
- [ ] Шаг 4. Прогон: `--filter "FullyQualifiedName~ClusterSecretEnsurer|FullyQualifiedName~ClusterSnapshotParser"` → PASS.
- [ ] Шаг 5. Commit: `git commit -m "feat: rest_password — седьмой per-cluster секрет (t22 Д1)"`.

**Выход:** кред существует после ensure, читается снапшотом.
**Проверка:** фильтры выше зелёные + регресс всего PgWorker.UnitTests (`DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~PgWorker.UnitTests`) — компиляция потребителей ClusterCredentials/ClusterSnapshot (вызовы `.EnsureAsync`, паттерн-матчинги) чинятся в этом же шаге.
**Связь со spec:** §5.3; §5.10 юниты ensure.

### Задача 14: WAF-фикстуры с тестовым CA + интеграционный тест старта

**Spec:** §5.1 (WAF-фикстуры), §5.10 (валидация старта: интеграции).
**Вход:** задача 12; 13.
**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/Api/PgWorkerApiFactory.cs` (PgApiFixture: env тестового CA)
- Create: `src/tests/PgWorker.IntegrationTests/Hosting/RestTlsStartupTests.cs`

**Interfaces:** env процесса `PGW_REST_TLS_CA`/`PGW_REST_TLS_CA_KEY` = PEM тестового CA (RSA-2048, `E2eTestPki.GenerateCa`-механика, статик-кеш на фикстуру).

- [ ] Шаг 1. `PgApiFixture`: в `InitializeAsync` (или ctor рядом с PGW_PG_*) поставить `Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", <pem>)` и `..._CA_KEY` из статического тестового CA; снимать в `DisposeAsync`. Производные фабрики (SeedApiTests и др.) наследуют.
- [ ] Шаг 2. Тест `RestTlsStartupTests` (не docker): (а) WAF-хост с тестовым CA стартует (CreateClient → `/healthz`-маршрут жив или любой запрос без краха хоста; главно — валидация прошла); (б) фабрика-оверрайд с битым `PGW_REST_TLS_CA` → старт падает с diagnose про `PGW_REST_TLS_*` (WebApplicationFactory: ожидание исключения при первом запросе/`Server`-доступе).
- [ ] Шаг 3. Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestTlsStartupTests` → PASS; существующие API-серии не роняются: `--filter FullyQualifiedName~PgWorker.IntegrationTests.Api` → PASS.
- [ ] Шаг 4. Commit: `git commit -m "test: WAF-фикстуры с тестовым REST-CA + fail-fast старт (t22 Д1)"`.

**Выход:** интеграционные серии живут с TLS-каноном; fail-fast покрыт.
**Проверка:** фильтры выше зелёные; зачистка docker не требуется (серия без docker).
**Связь со spec:** §5.1 WAF; §5.10 интеграции «валидация старта»; §8 п.2.

---

## Фаза Д2 — нода: env, SPILO_CONFIGURATION, entrypoint, lease-скрипт (задачи 15–19)

> **Примечание о тире тестов (осознанный перенос из §5.10 спеки).** Спека относит entrypoint-ca-файл и master-lease https к «юнитам» (sh/py). План исполняет их docker-интеграционным тиром (задачи 18/19): sh-скрипт и python+ssl реально гоняются в контейнере образца MasterLeaseFailoverTests — хостовой прогон упирается в зашитый порт 8008 и отсутствие окружения образа. Итоговое требование §8 п.1 читается как «юниты Pg + docker-профиль задач 18/19».

### Задача 15: SpiloEnvBuilder — REST-TLS-env + restapi-секция SPILO_CONFIGURATION

**Spec:** §5.2 (env/секция), §2 п.2/п.3 (факты Spilo).
**Вход:** задачи 10–13.
**Files:**
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` (SpiloEnvBuilder)
- Create: `src/PgWorker.Core/Templates/RestRotation.cs` (ЕДИНСТВЕННОЕ место — нужен и Core/SpiloEnvBuilder, и Provisioning/драйвер-вызовам; из Core доступен обоим)
- Test: `src/tests/PgWorker.UnitTests/Templates/NodeConfigBuildersTests.cs`

**Interfaces (Produces):**
```csharp
// Новый вход SpiloEnvBuilder.Build (обязательный):
public sealed record NodeRestTls(string CertPem, string KeyPem, string CaPem, string RestPassword);
// Build(ShardTopology, EtcdEndpoints, InstallSecrets, bool syncStrict, NodeRestTls rest,
//       PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)
// Env: SSL_RESTAPI_CERTIFICATE / SSL_RESTAPI_PRIVATE_KEY / SSL_RESTAPI_CA — PEM с РЕАЛЬНЫМИ переносами строк
//     PGW_REST_PASSWORD_HASH — sha256(RestPassword) hex (короткий, hex-lower)
```
Общий хелпер (создаётся здесь, потребляют задачи 17–22):
```csharp
public static class RestRotation
{
    public const string RestUsername = "patroni";
    public const string EnvCert = "SSL_RESTAPI_CERTIFICATE";
    public const string EnvPasswordHash = "PGW_REST_PASSWORD_HASH";
    public static string PasswordHash(string restPassword); // sha256 hex
    // Резолвор эффективной пары: pending окна ротации (журнал) ?? ключ кластера (снапшот)
    public static string? EffectivePassword(string? snapshotRestPassword, string? pendingRestPassword);
}
```
SPILO_CONFIGURATION — в YAML добавляется секция верхнего уровня:
```yaml
restapi:
  connect_address: "<pgw-C-X-n>:8008"   # DNS per-cluster сети; из SAN
  authentication:
    username: patroni
    password: "<per-cluster rest_password>"
```
(полное имя ноды = `pgw-{Cluster}-{Shard}-{node}`; для YAML-сборки — сериализация restapi-блока тем же raw-string-стилем, значения в двойных кавычках; `listen` не переопределяется.)

- [ ] Шаг 1. Тесты: (а) env несёт `SSL_RESTAPI_CERTIFICATE`/`_PRIVATE_KEY`/`_CA` с реальными `\n`-переносами (Contains "-----BEGIN"), значение равно входному PEM байт-в-байт; (б) SPILO_CONFIGURATION парсится YAML-парсером (или regex-ассертами по образцу существующих тестов): секция `restapi` с `connect_address: "pgw-c1-shard1-shard1a:8008"`, `authentication.username: patroni`, `password` = входная пара; `bootstrap`-секция не изменена; (в) `PGW_REST_PASSWORD_HASH` = sha256-hex пароля; (г) `restapi` НЕ содержит `listen`.
- [ ] Шаг 2. Прогон → FAIL (сигнатура).
- [ ] Шаг 3. Реализация + обновить ВСЕ вызовы `SpiloEnvBuilder.Build` в юнитах/драйвере (передача `NodeRestTls` с тестовым материалом — фабрика-хелпер в тестах `TestRestTls()`).
- [ ] Шаг 4. Прогон: `--filter FullyQualifiedName~NodeConfigBuildersTests|FullyQualifiedName~PatroniTimingsTests` → PASS; вся сборка юнитов зелёная.
- [ ] Шаг 5. Commit: `git commit -m "feat: SpiloEnvBuilder — SSL_RESTAPI_* + restapi-секция SPILO_CONFIGURATION (t22 Д2)"`.

**Выход:** env ноды несёт REST-TLS-материал.
**Проверка:** фильтры выше + `FullyQualifiedName~PgWorker.UnitTests` регресс.
**Связь со spec:** §5.2; §5.10 юниты SpiloEnvBuilder.

### Задача 16: WorkJournal — поле rest_pending (перенос из Д3: нужно резолвору эффективной пары задачи 17)

**Spec:** §5.5 (фиксация пары в полёте), §4 (arch/14 §5 I).
**Вход:** задача 15 (RestRotation.EffectivePassword — её второй аргумент `pendingRestPassword` питается вводимым здесь полем).
**Files:**
- Modify: `src/Shared.Etcd/Coordination/WorkJournal.cs`
- Test: `src/tests/Shared.Etcd.UnitTests/WorkJournalTests.cs`

**Interfaces (Produces):**
```csharp
// WorkState + поле (панель незнакомые поля игнорирует):
[property: JsonPropertyName("rest_pending")] string? RestPending = null
// WritePhaseAsync + параметры (после facts):
    string? restPending = null, bool dropRestPending = false
// Семантика: не задан и не drop — carry-forward (как unreachable/facts);
// dropRestPending=true — сброс (фаза done ротатора).
// SupervisionState + string? RestPending (окно ротации для надзора).
```

- [ ] Шаг 1. Тесты: (а) запись фазы с restPending="X" → ReadAsync возвращает "X"; (б) следующая фаза без параметра — "X" сохранён (carry-forward); (в) фаза с dropRestPending=true — поле отсутствует; (г) ReadSupervisionStateAsync возвращает RestPending; (д) десериализация ключа БЕЗ поля (старые журналы) → null, ошибок нет.
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация (WorkState/WritePhaseAsync carry-forward в блоке чтения текущего состояния; WriteSupervisionAsync — тоже carry-forward: надзор не должен затирать pending супервизионной записью).
- [ ] Шаг 4. Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~WorkJournalTests` → PASS; регресс `~KafkaWorker.UnitTests` + `~AdminPanel.UnitTests` (парсер журнала панели — WorkJournalParserTests — поле игнорируется).
- [ ] Шаг 5. Commit: `git commit -m "feat: WorkJournal — rest_pending (пара в полёте окна REST-ротации) (t22 Д2)"`.

**Выход:** механизм IPC ротация→надзор готов ДО драйверных call-site'ов (линейный порядок компилируется).
**Проверка:** фильтры выше.
**Связь со spec:** §5.5 R2 фиксация; §4 arch/14 §5 I.

### Задача 17: драйверы — кеш в конструкторе, EnsureNodeAsync(restPassword), BuildSpec; EffectivePassword в call-site'ах; wiring

**Spec:** §5.1 (доставка в драйвер), §5.2 (BuildSpec/порт-карта), §5.5 (эффективная пара; EnsureNode-пути окна не расширяют).
**Вход:** задачи 11, 15 (NodeRestTls + RestRotation), 16 (WorkState.RestPending — читается в call-site'ах).
**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (IClusterDriver + Plain + Swarm + BuildSpec; внутренний plain SwarmClusterDriver)
- Modify: `src/PgWorker.App/Program.cs` (DI: RestCertificateCache в драйверы)
- Modify: вызовы EnsureNodeAsync (полный список — 5 файлов, 8 мест): `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (~493), `AddShardProcess.cs` (~297), `AdoptionProcess.cs` (~425), `NodeSupervisor.cs` (3 места: EnsureDeclared ~316, RecreateMarked ~548, SuperviseShard ~728), `src/PgWorker.Backups/Process/RestoreProcess.cs` (2 места: ~575, ~611)
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs`, `src/tests/PgWorker.IntegrationTests/Backups/RestoreProcessTests.cs` (кейс окна)

**Interfaces (Produces):**
```csharp
// IClusterDriver (новый параметр — после syncStrict):
Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
    InstallSecrets secrets, EtcdEndpoints etcd, NodeResources? resources,
    PgTuneResult? tuning, bool syncStrict, string restPassword, CancellationToken ct);
// Конструкторы Plain/Swarm: + параметр RestCertificateCache? restCertificates
```
BuildSpec: `restCertificates.GetOrCreate(cluster, shard, node)` → `NodeRestTls(cert, key, CaPem из кеша, restPassword)` → `SpiloEnvBuilder.Build(...)`; порт-карта НЕ меняется (`5432→Pg`, `8008→Patroni`, `6432→Doorman`). `restCertificates == null` (изолированные юнит-пути драйвера) — fail-fast ApplicationException в BuildSpec: HTTP-режима не существует; юниты передают кеш с тестовым CA.

ВАЖНО (m2-ревью): `SwarmClusterDriver.EnsureNodeAsync` (~889) строит ВНУТРЕННИЙ `new PlainClusterDriver([], new DockerEngineFactory(), ...)` для BuildSpec — без кеша внутренний BuildSpec упал бы fail-fast'ом. Внутренний инстанс получает СВОЙ кеш: `new PlainClusterDriver([], new DockerEngineFactory(), enableDoorman, nodeImage, pgtuneExclude: pgtuneExclude, restCertificates: restCertificates)`.

- [ ] Шаг 1. Тесты ClusterDriverTests: BuildSpec (через internal-доступ, как существующие): env несёт `SSL_RESTAPI_CERTIFICATE`, `PGW_REST_PASSWORD_HASH`=hash пароля; SAN серта (парс PEM) содержит короткое имя, полное имя, 127.0.0.1; повторный BuildSpec того же узла — тот же PEM (кеш); null-кеш → ApplicationException; Swarm-путь: `SwarmClusterDriver` с кешем → внутренний BuildSpec несёт `SSL_RESTAPI_CERTIFICATE` (юнит-проверка передачи кеша внутреннему plain; паттерн SwarmClusterDriverTlsTests у Valkey).
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация: конструкторы Plain/Swarm (`restCertificates` параметром после `scrapeNetwork`/`pgtuneExclude`) + передача кеша внутреннему plain в Swarm; `BuildSpec` сборка `NodeRestTls`; Program.cs: `new RestCertificateCache(restTls.CaPem, restTls.CaKeyPem)` → оба драйвера; ВСЕ 8 call-site'ов передают `restPassword: RestRotation.EffectivePassword(<rest кластера>, <pending>)`, где rest кластера — `snap.RestPassword` (Provisioning/AddShard/Adoption — снапшот после ensure), а pending — `journal.ReadAsync(cluster).Value?.RestPending` (NodeSupervisor — единое чтение на тик; RestoreProcess — добавить чтение журнала тем же резолвором, если процесс его ещё не читает).
- [ ] Шаг 3а. Тест-кейс RestoreProcessTests: EnsureNode-путь restore при открытом окне ротации (в FakeEtcd-журнале `rest_pending`=NEW) передаёт драйверу NEW-пару — окно не расширяется (спека §5.5 «все EnsureNode-пути окна не расширяют окно»); без окна — ключ кластера.
- [ ] Шаг 4. Прогон: `--filter FullyQualifiedName~ClusterDriverTests` → PASS; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~PgWorker.UnitTests|FullyQualifiedName~RestoreProcessTests"` → PASS (все старые вызовы EnsureNodeAsync обновлены, включая RestoreProcess).
- [ ] Шаг 5. Commit: `git commit -m "feat: драйверы выпускают REST-серт ноды в BuildSpec; эффективная REST-пара во всех EnsureNode-путях (t22 Д2)"`.

**Выход:** каждый EnsureNode-путь создаёт ноду в TLS-режиме.
**Проверка:** фильтры выше; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 ворнингов.
**Связь со spec:** §5.1 доставка; §5.2 BuildSpec; §5.5 эффективная пара; §8 п.5 (все новые ноды TLS).

### Задача 18: entrypoint ноды — ca-файл + PGW_NODE_CA

**Spec:** §5.2 (loopback-клиент: доставка CA), §4 (arch/14 §2.1).
**Вход:** задача 15 (env имя `SSL_RESTAPI_CA`).
**Files:**
- Modify: `docker/node/docker-entrypoint.sh`
- Test: `src/tests/PgWorker.IntegrationTests/Docker/NodeEntrypointRestCaTests.cs` (docker-профиль, паттерн MasterLeaseFailoverTests: alpine + bind-mount)

- [ ] Шаг 1. Тест (docker): контейнер `python:3.12-alpine` (тот же, что MasterLeaseFailoverTests — из registry). Механика прогона скрипта ДО supervisord (exec идёт по АБСОЛЮТНОМУ пути `/usr/bin/supervisord` — PATH-подмена не работает, а в alpine нет пользователя postgres для chown):
  - в C#-тесте создать tmp-каталог с: заглушкой `supervisord` (`#!/bin/sh\nexit 0`, +x) и PEM-файлом;
  - контейнер: bind-mount загушки ПОВЕРХ `/usr/bin/supervisord` (ro), bind-mount `docker/node/docker-entrypoint.sh` → `/tmp/entrypoint.sh` (ro);
  - команда контейнера: `sh -c "adduser -D postgres && sh /tmp/entrypoint.sh"` c env `SSL_RESTAPI_CA=<PEM с реальными переносами>` (через `WithEnvironment`) и заглушками остальных env скрипта (HAPROXY_CONFIG/DOORMAN_CONFIG/PGW_ETCD/… — пустые строки).
  Ассерты (после выхода): `/home/postgres/pgw-node-ca.pem` существует, содержимое == PEM, права 600, владелец postgres; в `/home/postgres/pgw-node.env` есть строка `PGW_NODE_CA=/home/postgres/pgw-node-ca.pem`; PEM НЕ попадает в pgw-node.env (grep BEGIN CERTIFICATE → пусто); ассерты — через `docker exec cat/ls -la` ДО удаления контейнера (exec после выхода заглушки supervisord: контейнер запускается с `sleep`-хвостом команды: `... && sh /tmp/entrypoint.sh; sleep 300`).
- [ ] Шаг 2. Прогон (`PGW_TEST_DOCKER=1`) → FAIL.
- [ ] Шаг 3. Реализация в entrypoint (паттерн HAPROXY_CONFIG):
```sh
# REST-TLS: CA верификации loopback-клиента (https://127.0.0.1:8008)
# материализуется в файл — PEM в KEY=VALUE-файл pgw-node.env не переносится.
if [ -n "$SSL_RESTAPI_CA" ]; then
    printf '%s\n' "$SSL_RESTAPI_CA" > /home/postgres/pgw-node-ca.pem
    chown postgres:postgres /home/postgres/pgw-node-ca.pem
    chmod 600 /home/postgres/pgw-node-ca.pem
    echo "PGW_NODE_CA=/home/postgres/pgw-node-ca.pem" >> /home/postgres/pgw-node.env
fi
```
(блок после записи pgw-node.env; файл env пишется `>` — строку добавлять после, как выше).
- [ ] Шаг 4. Прогон → PASS; зачистка серии (`docker ps -aq --filter name=pgw- | xargs -r docker rm -f; docker network prune -f`).
- [ ] Шаг 5. Commit: `git commit -m "feat: entrypoint ноды — pgw-node-ca.pem + PGW_NODE_CA (t22 Д2)"`.

**Выход:** lease-скрипту доставлен ca-файл.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~NodeEntrypointRestCaTests` зелёный.
**Связь со spec:** §5.2 loopback; §5.10 entrypoint sh-юнит.

### Задача 19: master-lease.py — https /primary

**Spec:** §5.2 (lease-скрипт), §2 п.6 (факт P11).
**Вход:** задача 18.
**Files:**
- Modify: `docker/node/master-lease.py` (local_role)
- Test: `src/tests/PgWorker.IntegrationTests/Docker/MasterLeaseHttpsTests.cs` (docker-профиль; образец MasterLeaseFailoverTests — alpine + bind-mount + python3)

- [ ] Шаг 1. Тест (docker): контейнер alpine с python3; в C#-тесте сгенерировать тестовый CA + серт с SAN `IP:127.0.0.1` (RestPki), записать в tmp-каталог, bind-mount вместе с master-lease.py; внутри поднять мини-https-сервер (python-скрипт: `ssl.SSLContext(PROTOCOL_TLS_SERVER)` на 127.0.0.1:8008, `/primary` → 200/503 по переключаемому файлу-флагу). Запуск `python3 /tmp/master-lease.py` БЕЗ аргументов (ветка on_start → local_role) с env `PGW_NODE_CA=/tmp/ca.pem` (+PGW_MASTER_KEY/PGW_NODE_HOST/PGW_ETCD на мёртвый etcd — демон не стартует из-за etcd, это ок: проверяем только выбор роли; ИЛИ подать аргументы роли — нет: тестируем именно local_role). Ассерт по логу/поведению: роль распознана по https (для 200 → ветка мастера: попытка lease; для 503 → реплика: «демон погашен»). Кейс 2: без `PGW_NODE_CA` → local_role возвращает None → лог «роль не передана и Patroni недоступен — выходим», http-запроса нет.
- [ ] Шаг 2. Прогон (`PGW_TEST_DOCKER=1`) → FAIL.
- [ ] Шаг 3. Реализация в `local_role()`:
```python
def local_role():
    """Фактическая роль этой ноды: GET /primary локального Patroni по https
    (верификация серта по ca-файлу ноды; 127.0.0.1 — в SAN)."""
    ca = os.getenv("PGW_NODE_CA", "")
    if not ca:
        print("master-lease: PGW_NODE_CA не задан — https /primary недоступен", flush=True)
        return None
    try:
        ctx = ssl.create_default_context(cafile=ca)
        req = urllib.request.Request("https://127.0.0.1:8008/primary")
        with urllib.request.urlopen(req, timeout=3, context=ctx) as r:
            return "master" if r.status == 200 else "replica"
    except Exception:
        return None  # Patroni недоступен — ничего не делаем
```
(+ `import ssl` вверху; http-ветки не остаётся).
- [ ] Шаг 4. Прогон → PASS; зачистка серии.
- [ ] Шаг 5. Commit: `git commit -m "feat: master-lease.py — https /primary по ca-файлу ноды (t22 Д2)"`.

**Выход:** P11-демон стартует на TLS-контуре.
**Проверка:** `... --filter FullyQualifiedName~MasterLeaseHttpsTests` зелёный; `FullyQualifiedName~MasterLeaseFailoverTests` регресс зелёный.
**Связь со spec:** §5.2 lease; §5.10 py-юнит; §8 п.3 (д).

---

## Фаза Д3 — клиент воркера и надзор (задачи 20–23)

### Задача 20: HttpClient «patroni» TLS + ShardProbe https/basic-auth

**Spec:** §5.4.
**Вход:** задача 12 (RestTlsMaterial), 13.
**Files:**
- Modify: `src/PgWorker.App/Program.cs` (конфигурация именованного HttpClient «patroni»)
- Modify: `src/PgWorker.Provisioning/Probes/ShardProbe.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ShardProbeTests.cs`

**Interfaces (Produces):**
```csharp
// ShardProbe:
private static Uri BuildUri(NodeAddress node, string path) => new($"https://{node.Host}:{node.Ports.Patroni}/{path}");
// Мутации принимают кред (GET — без заголовка):
public Task<Result> PatchConfigAsync(NodeAddress node, string patchJson, string restPassword, CancellationToken ct);
public Task<Result> SwitchoverAsync(NodeAddress leader, string leaderName, string restPassword, CancellationToken ct);
// Заголовок: Authorization: Basic base64($"{RestRotation.RestUsername}:{restPassword}")
```
Program.cs: `AddHttpClient("patroni")` → `.ConfigurePrimaryHttpMessageHandler(sp => new SocketsHttpHandler { SslOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = <цепочка к RestTlsMaterial.Ca, без hostname> } })` — валидация X509Chain CustomRootTrust + NoCheck (образец `Shared.Tls/TlsChain.cs`), callback возвращает результат цепочки независимо от hostname (канон P17 «require, не verify-full»); спека §5.4 — таймауты не меняются (7.5 c клиент / 3 c проба / 15 c switchover).

- [ ] Шаг 1. Тесты ShardProbeTests (мок HttpMessageHandler, как существующие): (а) все URI — `https://`; (б) PATCH /config несёт `Authorization: Basic` c ожидаемым base64(patroni:pwd); (в) POST /switchover несёт заголовок; (г) GET /cluster и /config — БЕЗ Authorization; (д) 401 на мутации → Result.Failed с кодом в сообщении.
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация: BuildUri https; параметры кредов; заголовок `request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(...))`; Program.cs — TLS-callback.
- [ ] Шаг 4. Прогон → PASS; юниты воркера целиком зелёные (вызывающие RecreateMarkedNodesAsync/конвергенцию DCS — сигнатуры обновить заглушками из снапшота; полностью гварды — задача 21).
- [ ] Шаг 5. Commit: `git commit -m "feat: ShardProbe https + basic-auth мутаций; TLS HttpClient patroni (t22 Д3)"`.

**Выход:** клиент воркера говорит с REST только по TLS, мутации — с кредом.
**Проверка:** `--filter FullyQualifiedName~ShardProbeTests` + `FullyQualifiedName~PgWorker.UnitTests`.
**Связь со spec:** §5.4; §5.10 юниты ShardProbe.

### Задача 21: NodeSupervisor — общий шаг пересоздания нод (два входа) + гварды окна

**Spec:** §5.5 (вся), §2 п.7 (факт ensure-пути).
**Вход:** задачи 17 (EnsureNodeAsync+пара), 20 (SwitchoverAsync с кредом), 16 (SupervisionState.RestPending).
**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (`IClusterDriver.InspectNodeEnvAsync`)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`

**Interfaces (Produces):**
```csharp
// IClusterDriver:
Task<Result<IReadOnlyDictionary<string, string>>> InspectNodeEnvAsync(
    string cluster, string shard, string nodeName, CancellationToken ct);
// Plain: ListContainers(lookup)→InspectContainerAsync→Env ("K=V"→dict); нет контейнера — пустой словарь.
// Swarm: пустой словарь (шаг пересоздания — no-op; симметрия заглушки InspectNodesAsync).
```
Новый приватный шаг `RecreateRestTlsNodesAsync(cluster, snap, addresses, restoring, work, ct)` в `TickAsync` — ПОСЛЕ блока app_params-миграции (строки ~99–109), ДО цикла проб (строка ~111 `// 2) Пробы`):

Алгоритм (оба входа, единый шаг, ≤1 пересоздание на тик):
1. Гварды домена: cluster Active; шard `Dsn != null`, `!shard.ToRemove`, `!restoring.Contains(shard.Name)`; нода каноническая (`addr.Object == null`, state не QUARANTINED/REMOVING).
2. Инспекция env: `driver.InspectNodeEnvAsync`. Кандидаты: вход «ротация» (work.RestPending != null): `PGW_REST_PASSWORD_HASH` отсутствует или != `RestRotation.PasswordHash(pending)`; вход «конвергенция» (work.RestPending == null): `SSL_RESTAPI_CERTIFICATE` отсутствует.
3. Перед первым пересозданием входа конвергенции — `ensured := appSecret.EnsureAsync(...)` (обеспечить `rest_password`; паттерн app_params-миграции; конструктор NodeSupervisor +параметр `IClusterSecretEnsurer`, DI в Program.cs). ИСТОЧНИК ПАРОЛЯ — результат ensure, НЕ снапшот тика (снапшот прочитан ДО ensure: у легаси-кластера `snap.RestPassword == null` — env собрался бы без пары): `effective := RestRotation.EffectivePassword(ensured.Value.RestPassword, work.RestPending)`.
4. Кандидат — живой лидер (leader scope из `/service/<C>-<X>/`): `probe.SwitchoverAsync(addr, name, effective)` → `continue` (снос следующим тиком; паттерн RecreateMarkedNodesAsync-soft). Мёртвая нода — сразу; гвард кворума: свидетель-план (`safe`-нодa) существует, иначе ждём.
5. Пересоздание с сохранением volume: `engine.StopContainerAsync` + `RemoveContainerAsync(force)` — НЕ `RemoveNodeAsync` (паттерн port-mismatch-ветки EnsureNodeAsync) → `driver.EnsureNodeAsync(..., restPassword: effective)`.
6. Фазовая запись журнала `supervise`: `rest-tls-migrated <X>/<n>` / `rest-rotate-rolled <X>/<n>` (с треком — WritePhaseAsync carry-forward).

Гварды окна (work.RestPending != null): `ConvergeDcsConfigAsync` — PATCH-skip тика (журнал-фаза `dcs-converge-skipped-rest-window`, транзиент-возврат Success); `RecreateMarkedNodesAsync` soft-switchover лидера — skip тика (журнал `recreate-skipped-rest-window`); ускорение failover мёртвого лидера — DCS-ключ, НЕ трогаем.

- [ ] Шаг 1. Тесты NodeSupervisorTests (Fakes: FakeDriver с управляемым env/контейнерами, FakeEtcd — существующие): (а) вход конвергенции: контейнер без `SSL_RESTAPI_CERTIFICATE` → пересоздан (EnsureNodeAsync вызван с парой ИЗ РЕЗУЛЬТАТА ensure — кейс легаси-кластера: снапшот БЕЗ `rest_password`, ensured.RestPassword != null, переданный пароль == ensured.RestPassword, env несёт `PGW_REST_PASSWORD_HASH`==hash(ensured.RestPassword)), volume-вызовы — Stop/Remove без RemoveNodeAsync; (б) повторный тик — no-op; (в) лидер: первый тик — только SwitchoverAsync (с кредом effective), снос — следующим тиком после смены лидера; (г) ≤1 EnsureNode на тик при двух кандидатах; (д) гварды: шард без dsn/TO_REMOVE/restore/усыновлённая (object) — не тронуты; (е) вход ротации: env с чужим hash → пересоздание с pending-парой; hash совпадает — no-op; (ж) окно: при RestPending PATCH /config не уходит (фаза skipped), RecreateMarked-soft не зовёт SwitchoverAsync; (з) ensure rest_password перед первым пересозданием конвергенции (FakeEtcd ключ появился) и пароля пересоздания — именно ensured (см. (а)).
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация (шаг + InspectNodeEnvAsync в Plain/Swarm + ctor-параметр + Program.cs DI + вызовы SwitchoverAsync/PatchConfigAsync с парой).
- [ ] Шаг 4. Прогон: `--filter FullyQualifiedName~NodeSupervisorTests` → PASS; весь PgWorker.UnitTests зелёный.
- [ ] Шаг 5. Commit: `git commit -m "feat: общий шаг пересоздания нод — REST-TLS-конвергенция + rolling-ротация (t22 Д3)"`.

**Выход:** живые ноды мигрируют на TLS; ротация исполняется надзором.
**Проверка:** фильтры выше.
**Связь со spec:** §5.5 (все пункты); §8 п.2 (конвергенция — юнит-часть).

### Задача 22: ClusterSecretRotator — rest-ветка процесса I

**Spec:** §5.3 (ротация), §5.5 (фазы R2/R3/окно), §4 (arch/14 §5 I, §1.1, §3.3).
**Вход:** задачи 16 (rest_pending), 21 (шаг исполняет rolling), 13.
**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ClusterSecretRotatorTests.cs`

**Interfaces:** ctor +параметр `IClusterDriver driver` (инспекция env через `InspectNodeEnvAsync`); ключ `/clusters/<C>/rest_password` (RestKey); фазы журнала: `rotate-rest-start` (фиксация NEW в `rest_pending`), `rotate-rest-rolling` (прогресс), `done` (сброс `dropRestPending`).

Алгоритм rest-части:
- R1: ensure четвёрки (`appSecret.EnsureAsync` уже возвращает `RestPassword` — OLD).
- R2: NEW×4; для rest: `work.RestPending` (чтение `journal.ReadAsync`) — если нет → NEW-rest + `WritePhaseAsync(..., phase: "rotate-rest-start", restPending: newRestPassword)`; далее каждый тик — инспекция env нод всех dsn-шардов: есть hash != hash(pending) → окно открыто, тик ЖДЁТ (надзор катит rolling; журнал-фаза `rotate-rest-rolling`, return Done — ретрай тиком); нет — R3.
- R3: txn пополняется `TxnCompare.ValueEqual(RestKey, oldRest)` + `TxnOp.Put(RestKey, newRest)`; после успеха — `Finish` фазой done с `dropRestPending: true`. Проигрыш compare → `FailAsync("commit-conflict")` — повтор прохода со свежей парой (новый pending).
- Шард без dsn — skip (ноды создадутся с новой парой после R3: `EffectivePassword` после закрытия окна = ключ).
- Гварды окна: rotator сам REST-мутаций кластера не делает (rolling-шаг — надзор); PATCH/switchover подавлены в NodeSupervisor (задача 21).

- [ ] Шаг 1. Тесты (AAA; Fakes): (а) заявка → ensure четверки → фаза `rotate-rest-start` с `rest_pending`==NEW; (б) повторный тик при недомигрированной ноде (env hash != NEW) — проход продолжается С ТОЙ ЖЕ парой (NEW не регенерируется), txn НЕ выполняется; (в) все ноды с hash(NEW) → txn: compare rest_password==OLD, put NEW, del заявки; pending сброшен (журнал без поля); (г) проигрыш compare — FailAsync, повторный проход с новой парой; (д) потеря pending-поля (журнал перезаписан без него) — свежая пара, повторный rolling идемпотентен; (е) no-op без заявки; (ж) шард без dsn — его ноды не инспектируются, R3 не ждёт их; (з) SQL-мок: ALTER-тройка и backup-guard выполняются как раньше (fake ISqlExecutor). Проверка гвардов окна (PATCH не уходит) — в NodeSupervisorTests (задача 21); здесь — что rotator не зовёт PatchConfigAsync/SwitchoverAsync вовсе.
- [ ] Шаг 2. Прогон → FAIL.
- [ ] Шаг 3. Реализация.
- [ ] Шаг 4. Прогон: `--filter FullyQualifiedName~ClusterSecretRotatorTests` → PASS; Program.cs — DI-параметр драйвера ротатору.
- [ ] Шаг 5. Commit: `git commit -m "feat: ротация rest_password — четвёртый секрет процесса I (t22 Д3)"`.

**Выход:** полный цикл ротации четвёртого секрета.
**Проверка:** фильтр выше + весь PgWorker.UnitTests.
**Связь со spec:** §5.3; §5.5 фазы; §5.10 юниты rotator; §8 п.1.

### Задача 23: интеграционные тесты — конвергенция и авторотация на живом контейнере + ensure rest_password с живым etcd

**Spec:** §5.10 (интеграции: три блока — конвергенция, авторотация, ensure с живым etcd), §5.5 (миграция без rebuild/UNREACHABLE), §8 п.2.
**Вход:** задачи 21, 22.
**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/Docker/RestTlsConvergenceTests.cs`
- Create: `src/tests/PgWorker.IntegrationTests/Docker/RestRotationTests.cs`
- Create: `src/tests/PgWorker.IntegrationTests/Etcd/RestPasswordEnsureEtcdTests.cs` (живой etcd, без docker-гейта — серия EtcdFixture)

Общая механика docker-классов (один класс фикстур, docker-гейт `DockerTrait.SkipIfUnavailable()`): имя контейнера `pgw-<guid8>-…` (уникальный префикс на прогон — зонд guid как ClusterTag), volume `pgw-<…>-data` с data-маркером (`docker exec touch /home/postgres/pgdata/MARKER` — alpine-заглушка контейнера: НЕ spilo, env урезанный — имитация легаси-подъёма: без `SSL_RESTAPI_CERTIFICATE`); снапшот/portalloc в FakeEtcd (юнит-паттерн инстанцирования процессов напрямую с настоящим `PlainClusterDriver` на unix-сокете — процессы принимают IEtcdGateway). Полный teardown `finally`: контейнеры+volume+сети own-only по префиксу guid; ассерт чистоты: `docker ps -a --filter name=pgw-<guid>` пуст, volume нет.

- [ ] Шаг 1. `RestTlsConvergenceTests`: (а) легаси-контейнер (env без REST-TLS) + FakeEtcd-кластер (config/shards/portalloc/state) + маркер в volume → цикл `NodeSupervisor.TickAsync` до no-op (поллинг тиков) → ассерты: контейнер пересоздан (создан после старта теста), env несёт `SSL_RESTAPI_CERTIFICATE` + `PGW_REST_PASSWORD_HASH`==hash(ключа `rest_password`, появившегося в FakeEtcd ensure'м шага — пароля из результата ensure, не из снапшота), volume и маркер на месте, `rest_password` в etcd появился; (б) повторный `TickAsync` — no-op (ID контейнера не изменился); (в) ассерт окна миграции (спека §5.5): за ВСЁ окно (от первого тика до no-op) в FakeEtcd нет rebuild-фаз и UNREACHABLE-переходов — work-ключ без записей `unreachable` по нодам шарда, state-ключи нод не покидают RUNNING, фаз rebuild в журнале нет. Если пересоздаваемый — «лидер», тест подаёт leader-ключ scope → первый тик: switchover-стаб (FakeDriver? НЕТ: живой драйвер, но Patroni нет — лидер-ветка требует живости: `probe.IsAliveAsync` → false → мёртвая нода → сразу снос; зафиксировать этот путь как «мёртвый лидер», отдельный кейс юнитами уже покрыт).
- [ ] Шаг 2. `RestRotationTests`: FakeEtcd + живой контейнер ноды с hash(OLD) в env → (а) заявка `/pgworker/rotations/<C>` → `ClusterSecretRotator.TickAsync` → `rest_pending` в журнале, NEW-пары сгенерированы; (б) цикл `NodeSupervisor.TickAsync` + `ClusterSecretRotator.TickAsync` до коммита (поллинг ≤N тиков) → ассерты: контейнер пересоздан с `PGW_REST_PASSWORD_HASH`==hash(NEW), volume/маркер на месте, txn-коммит: `rest_password`==NEW, заявка удалена, `rest_pending` сброшен; (в) идемпотентность: повторная заявка — новый проход с новой парой; (г) сбой посреди прохода: после первого тика ротатора повторить тик до завершения rolling → `rest_pending` не изменился (продолжение той же парой); (д) no-op тика без заявки; (е) гварды окна интеграционно: при открытом окне (rolling не завершён) журнал не содержит фаз патча конвергенции DCS (конфиг DCS в FakeEtcd расходится с каноном — PATCH был бы записан фазой; окно — фаза skip). SQL-тройка мокается fake ISqlExecutor (стратегия процесса).
- [ ] Шаг 3. `RestPasswordEnsureEtcdTests` (живой etcd — EtcdFixture, паттерн существующей Etcd-серии): (а) пустой префикс кластера → `ClusterSecretEnsurer.EnsureAsync` кладёт 7-й ключ `rest_password` txn put-if-absent, re-read возвращает его в `ClusterCredentials.RestPassword`; (б) повторный ensure с уже существующим ключом — значение НЕ меняется (put-if-absent), Result.Success; (в) ключ с пустым значением — добирается txn (put-if-absent на пустое).
- [ ] Шаг 4. Прогон: docker-часть `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~RestTlsConvergenceTests|FullyQualifiedName~RestRotationTests"` → PASS; etcd-часть `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~RestPasswordEnsureEtcdTests` → PASS.
- [ ] Шаг 5. Зачистка серии: `docker ps -aq --filter name=pgw- | xargs -r docker rm -f; docker volume ls -q --filter name=pgw- | xargs -r docker volume rm; docker network prune -f`.
- [ ] Шаг 6. Commit: `git commit -m "test: REST-TLS-конвергенция, авторотация rest_password (docker) и ensure с живым etcd (t22 Д3)"`.

**Выход:** мутационные пути покрытия живым docker.
**Проверка:** фильтр выше зелёный; после — `docker network ls | grep -c 'pgw-'` → 0.
**Связь со spec:** §5.10 оба интеграционных блока; §8 п.2.

---

## Фаза Д4 — панель и мониторинг (задачи 24–26)

### Задача 24: PatroniRestProbe — https + доверие CA

**Spec:** §5.6, §4 (adminpanel/02 §6.1).
**Вход:** задача 9 (arch).
**Files:**
- Modify: `src/AdminPanel.Probes/PatroniRestProbe.cs` (URL https)
- Modify: `src/AdminPanel.Probes/ModuleExtensions.cs` (TLS handler typed HttpClient)
- Test: `src/tests/AdminPanel.IntegrationTests/PatroniRestProbeTests.cs` (https-стаб)

**Interfaces:** URL: `$"https://{HostMapResolver.Resolve(...)}/cluster"`; handler: `ConfigurePrimaryHttpMessageHandler` — `SocketsHttpHandler.SslOptions.RemoteCertificateValidationCallback` = цепочка к CA из `IOptions<WorkerApiOptions>.WorkerTls` (`ServerCaPem` ?? файл `ServerCaPath` — env `WORKERS_PANEL_TLS_SERVER_CA[_PATH]` уже привязан WorkerTlsHandler'ом; ОДИН корень доверия на установку), hostname не сверяется; User-Agent `AdminPanel` остаётся; креды не вводятся.

- [ ] Шаг 1. Тесты: заменить HttpListener-стаб на Kestrel https-стаб с тестовым CA (серт SAN 127.0.0.1, RSA-2048 .NET): (а) https-проба с доверенным CA → Success, member-запись, User-Agent «AdminPanel» получен сервером; (б) серт НЕ из CA → ProbeResult fail (транспорт TLS); (в) отсутствие записи member — PatroniProbeException-путь как раньше. Хостовая панель не запускается — только Probes-сборка.
- [ ] Шаг 2. Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~PatroniRestProbeTests` → FAIL.
- [ ] Шаг 3. Реализация (probe URL + ModuleExtensions handler; чтение CA ленивое в callback, кеш X509Certificate2 на фабрику).
- [ ] Шаг 4. Прогон → PASS; регресс `~AdminPanel` серии юнитов.
- [ ] Шаг 5. Commit: `git commit -m "feat: PatroniRestProbe — https + доверие per-install CA (t22 Д4)"`.

**Выход:** панель на единой https-ветке.
**Проверка:** фильтры выше.
**Связь со spec:** §5.6; §5.10 интеграции панели; §8 п.2.

### Задача 25: эмуляторы hc* — HTTPS тем же CA

**Spec:** §5.8, §4 (adminpanel/04 §1).
**Вход:** —
**Files:**
- Modify: `deploy/tls/gen.sh` (серт `hc`)
- Modify: `dev-stand/adminpanel/sidecar/emulator.py` (TLS-обёртка)
- Modify: `dev-stand/adminpanel/docker-compose.yml` (сервисы hc*: volume + env)
- Test: ручная проверка стендом (задача 28); python-синтаксис — `python3 -m py_compile` в шаге.

- [ ] Шаг 1. gen.sh: идемпотентный блок (в секции клиентских/серверных — рядом с pgserver):
```sh
# t22: серт эмуляторов Patroni-REST стенда (hc1a..hc2b + host-публикации 8011–8022)
[ -f hc.crt ] || issue hc hc serverAuth "DNS:hc1a,DNS:hc1b,DNS:hc2a,DNS:hc2b,IP:127.0.0.1"
```
финальный echo дополнить `hc.*`.
- [ ] Шаг 2. emulator.py: HTTPS-обёртка слушателя; env `HC_TLS_CERT`/`HC_TLS_KEY`; отсутствие env — запуск падает с diagnose (HTTP-режим не оставляем):
```python
import ssl
cert = os.getenv("HC_TLS_CERT", "")
key = os.getenv("HC_TLS_KEY", "")
if not cert or not key:
    raise SystemExit("emulator: HC_TLS_CERT/HC_TLS_KEY обязательны (HTTPS-only, стенд поднимается 00-up.sh)")
httpd = ThreadingHTTPServer(("0.0.0.0", 8008), Handler)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
httpd.serve_forever()
```
- [ ] Шаг 3. compose hc* (все 4): добавить `volumes: - ../../deploy/tls:/tls:ro` и `environment: HC_TLS_CERT: /tls/hc.crt, HC_TLS_KEY: /tls/hc.key`.
- [ ] Шаг 4. Проверка: `bash deploy/tls/gen.sh` (в temp-копии каталога! НЕ трогая живой пакет: `TMP=$(mktemp -d); cp deploy/tls/gen.sh $TMP/; (cd $TMP && bash gen.sh) && ls $TMP/hc.*`) — появились hc.crt/key; `python3 -m py_compile dev-stand/adminpanel/sidecar/emulator.py` — ок.
- [ ] Шаг 5. Commit: `git commit -m "feat: эмуляторы hc* — HTTPS на серте hc из per-install CA (t22 Д4)"`.

**Выход:** демо-контур панели — TLS.
**Проверка:** шаг 4; полный — чек 30/40/65 (задача 28).
**Связь со spec:** §5.8; §8 п.6.

### Задача 26: prometheus.yml + чек 30 — https-таргеты

**Spec:** §5.7, §5.9 (чек 65).
**Вход:** задача 25.
**Files:**
- Modify: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`
- Modify: `dev-stand/adminpanel/checks/30-failover.sh`

- [ ] Шаг 1. prometheus.yml: джоба `patroni`: `scheme: https` + `tls_config: {ca_file: /tls/ca.pem}`; джоба `patroni-nodes`: `scheme: https` (убрать комментарий «без TLS — t22 вне скоупа») + тот же `tls_config` (клиентские серты НЕ добавляются — `verify_client` нет; tls_config остальных джоб не меняются).
- [ ] Шаг 2. Чек 30 (строки ~62 и ~68): `curl -fsS --cacert "$ROOT/deploy/tls/ca.pem" https://127.0.0.1:8012/primary` и аналогично `https://127.0.0.1:8011/cluster` (hostname проходит: 127.0.0.1 в SAN hc); в шапке файла `$ROOT="$(cd ../.. && pwd)"` уже есть? — проверить и добавить при отсутствии.
- [ ] Шаг 3. Commit: `git commit -m "feat: prometheus patroni/patroni-nodes https; чек 30 https-curl (t22 Д4)"`.

**Выход:** скрейп и чеки — TLS.
**Проверка:** синтаксис yml (`docker run --rm -v ... promtool check config` — опционально на стенде); полный прогон — задача 28.
**Связь со spec:** §5.7; §5.9; §8 п.6.

---

## Фаза Д5 — стенд (задачи 27–28)

### Задача 27: deploy-compose env + 00-up ca.key в томе

**Spec:** §5.9.
**Вход:** задачи 12 (env-имена), 9.
**Files:**
- Modify: `deploy/docker-compose.yml` (x-pgworker-env)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (cp-строка наполнения тома)

- [ ] Шаг 1. docker-compose.yml, в `x-pgworker-env` (обоим инстансам через якорь):
```yaml
  # REST-TLS нод (t22, arch/14 §8): per-install CA + ключ выпуска сертов
  PGW_REST_TLS_CA_PATH: /tls/ca.pem
  PGW_REST_TLS_CA_KEY_PATH: /tls/ca.key
```
- [ ] Шаг 2. 00-up.sh, docker-run alpine наполнения `deploy_pgw-api-tls`: добавить `/src/ca.key` в cp-строку: `cp /src/ca.pem /src/ca.key /src/pgserver.crt /src/pgserver.key /src/healthcheck.crt /src/healthcheck.key /tls/` (только если gen.sh его создал — создаёт всегда; предупреждение про права уже есть chmod 600 в gen.sh; комментарий строки дополнить «ca.key — выпуск REST-сертов нод (R16: бэкап пакета — runbook)»).
- [ ] Шаг 3. Commit: `git commit -m "feat: deploy-compose PGW_REST_TLS_*; 00-up — ca.key в deploy-томе (t22 Д5)"`.

**Выход:** поставка несёт TLS-материал нод.
**Проверка:** `docker compose -f deploy/docker-compose.yml config` (env-ключи в выводе).
**Связь со spec:** §5.9; §8 п.7.

### Задача 28: прогон стенда — чеки 65/40/30, идемпотентность 00-up/90-down

**Spec:** §5.9, §8 п.6/п.7.
**Вход:** задачи 25–27.
**Files:** (без правок кода — прогон; выявленные правки — отдельным коммитом в этой же задаче)

- [ ] Шаг 1. `bash dev-stand/images/pull-images.sh` (registry-образы).
- [ ] Шаг 2. Полный подъём: `bash dev-stand/adminpanel/checks/00-up.sh` (поднимает всё: панель, шарды, эмуляторы TLS, PgWorker с `PGW_REST_TLS_*`); дождаться готовности.
- [ ] Шаг 3. Чек 65: `bash dev-stand/adminpanel/checks/65-metrics.sh` — patroni/patroni-nodes https-таргеты up (ассерты прежние по форме: health==up, `patroni_up`/`net_up` — шаг 2.1 механики не меняются).
- [ ] Шаг 4. Чек 40 (live-пробы панели на демо): `bash dev-stand/adminpanel/checks/40-live-probes.sh` — пробы patroni зелёные по https.
- [ ] Шаг 5. Чек 30 (failover на https-curl): `bash dev-stand/adminpanel/checks/30-failover.sh`.
- [ ] Шаг 6. Идемпотентность: повторный `00-up.sh` — no-op по сертам (guard gen.sh; `ls -la deploy/tls/hc.crt` — mtime не изменился), подъём зелёный; `90-down.sh` — без сирот: сеть `pgw-metrics` удалена по прежним ассертам, серты deploy/tls НЕ удалены.
- [ ] Шаг 7. Зачистка хостов после серий: `docker network prune -f`.
- [ ] Шаг 8. Commit (при правках): `git commit -m "fix(stand): правки чеков по факту прогона TLS-стенда (t22 Д5)"`.

**Выход:** стенд полностью на TLS.
**Проверка:** чеки 65/40/30 зелёные; критерии §8 п.6/п.7.
**Связь со spec:** §5.9; §8 п.6–7.

---

## Фаза Д6 — E2E (задачи 29–31)

### Задача 29: E2eEnvironment — per-contour CA для REST-TLS всех контуров

**Spec:** §5.10 (docker-E2E: E2eEnvironment).
**Вход:** задачи 12 (воркер принимает env `PGW_REST_TLS_*`), 27.
**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (StartHostOnPortAsync базовый env + PEM-файлы в артефакты)
- Test: существующие E2E-серии (все контуры автоматически TLS).

- [ ] Шаг 1. В `StartHostOnPortAsync` базовый env добавить (из уже существующего per-install CA контура `_installCa` — переиспользовать, второй CA не порождать):
```csharp
// REST-TLS нод (t22): per-contour CA (тот же, что mTLS API) — все E2E-контуры
// создают ноды в TLS-режиме (дефолт поставки, http-режима в тестах нет).
["PGW_REST_TLS_CA"] = _installCa.CaPem,
["PGW_REST_TLS_CA_KEY"] = _installCa.CaKeyPem,
```
(строки после блока `PGW_API_TLS_*`).
- [ ] Шаг 2. Артефакты: в `StartAsync` (или рядом с записью существующих сертов) писать `rest-ca.pem`/`rest-ca.key` в `ArtifactsDir` (для разбора падений и ручных curl-проверок контура).
- [ ] Шаг 3. Прогон-сигарета: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` → PASS (кластер поднимается в TLS-режиме: провижининг/пробы шли по https — если падает, чинить здесь: серты/SAN/фактические порты). Зачистка серии (`docker ps -aq --filter name=pgw- | xargs -r docker rm -f; docker network prune -f`).
- [ ] Шаг 4. Commit: `git commit -m "test(e2e): все контуры — REST-TLS через per-contour CA (t22 Д6)"`.

**Выход:** каждый E2E-контур — TLS-поставка.
**Проверка:** маркер зелёный на свежем Release (E2eFixture собирает сам).
**Связь со spec:** §5.10 E2eEnvironment; §8 п.5.

### Задача 30: E2ePatroniRestTlsScenarios — профильный E2E-класс

**Spec:** §5.10 (docker-E2E: профильный класс), §8 п.3.
**Вход:** задача 29.
**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniRestTlsScenarios.cs`

Класс-сценарий (изолированное окружение E2eEnvironment — guid во всех именах, телеметрия `[PHASE]`/`MarkFailed()`/артефакты по e2e-launch):

- [ ] Шаг 1. Сценарий `E2ePatroniRestTls_HttpsOnlyAuthAndP11`: сид кластера (1 шард, 2 ноды) + `StartHostAsync` → ожидание Active (поллинг, `[PHASE]`-метки). Ассерты:
  - (а) `http://<host>:<patroni-port>/cluster` — отказ (не 200/транспорт); `https://` с ca.pem контура → 200;
  - (б) серт ноды верифицируется X509Chain против CA контура (docker cp серта? — получить серт TLS-хендшейком: `SslStream` с callback-перехватом → цепочка валидна);
  - (в) PATCH `/config` без Authorization → 401; с `Authorization: Basic base64(patroni:<rest_password из etcd>)` → 200;
  - (г) провижининг дошёл до Active (пробы/конвергенция шли по https — сам факт Active);
  - (д) P11: `docker restart` мастера → поллинг мастер-ключа `/clusters/<C>/shards/<X>/master` ≤60 c — ключ жив (callback on_start узнал роль через https /primary);
  - (е) шаг конвергенции: срезать REST-TLS-env живого контейнера нельзя in-place — имитация легаси: `docker rm -f` одной ноды + пересоздать урезанным env через `RunDockerAsync` (имя/volume те же) → тик надзора → контейнер пересоздан с `SSL_RESTAPI_CERTIFICATE` (инспекция env), volume сохранён; кластер снова Active.
- [ ] Шаг 2. Teardown: канон e2e-isolation — полный (среда чистит себя; ассерт чистоты: `docker ps -a --filter name=pgw-<tag>` пуст; сети/тома префикса удалены); упавший сценарий — `MarkFailed()` (контейнеры остаются для разбора, README-cleanup).
- [ ] Шаг 3. Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2ePatroniRestTlsScenarios` → PASS; зачистка серии после финальной строки.
- [ ] Шаг 4. Commit: `git commit -m "test(e2e): E2ePatroniRestTlsScenarios — https-only/401/P11/конвергенция (t22 Д6)"`.

**Выход:** профильный E2E маркер готов.
**Проверка:** фильтр зелёный на свежем Release; ассерт чистоты в логе.
**Связь со spec:** §5.10 профильный класс; §8 п.3.

### Задача 31: E2ePatroniFileSdScenarios — https-скрейп

**Spec:** §5.10 (E2E file_sd), §8 п.4.
**Вход:** задача 29.
**Files:**
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs`

- [ ] Шаг 1. prometheus.yml сценария: джоба `patroni-nodes` → `scheme: https` + `tls_config: {ca_file: /etc/prometheus/ca.pem}`; CA-файл контура — bind-mount в контейнер прометея (`WithBindMount(<ArtefactsDir>/rest-ca.pem, "/etc/prometheus/ca.pem")`).
- [ ] Шаг 2. Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2ePatroniFileSdScenarios` → PASS (таргеты up, серии `patroni_*` в TSDB, host-форвардинга нет — ассерты прежние); зачистка серии.
- [ ] Шаг 3. Commit: `git commit -m "test(e2e): file_sd patroni-nodes — https-скрейп с ca-файлом контура (t22 Д6)"`.

**Выход:** скрейп-канон E2E — TLS.
**Проверка:** фильтр зелёный.
**Связь со spec:** §5.10 file_sd; §8 п.4.

---

## Фаза Д7 — мерж-гейт (задача 32)

### Задача 32: полный мерж-гейт + roadmap-гейт

**Spec:** §6 Д7, §8 (все критерии).
**Вход:** задачи 1–31.
**Files:**
- Modify: `arch/roadmap/reliability.md` (снять `t22-patroni-rest-tls`), `arch/roadmap/reliability-report.md` (перенос строки из «Осталось» в «Сделано», убрать «Patroni REST без TLS/аутентификации» из формулировок).

- [ ] Шаг 1. Юниты все: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~PgWorker.UnitTests` → PASS; отдельно `~AdminPanel.UnitTests`, `~Shared.Etcd.UnitTests` → PASS. Критерий §8 п.1 читается как «юниты + docker-профиль задач 18/19» (осознанный перенос тира sh/py-«юнитов» — примечание фазы Д2). Зачистка не нужна (docker нет).
- [ ] Шаг 2. Интеграции Pg (без docker-гейта): `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~PgWorker.IntegrationTests` → PASS (включая `RestPasswordEnsureEtcdTests` — живой etcd EtcdFixture; PgApiFixture-серии WAF с тестовым CA).
- [ ] Шаг 3. Интеграции docker-профиля: `bash dev-stand/images/pull-images.sh` → `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~RestTlsConvergenceTests|FullyQualifiedName~RestRotationTests|FullyQualifiedName~NodeEntrypointRestCaTests|FullyQualifiedName~MasterLeaseHttpsTests|FullyQualifiedName~DockerDriverTests"` → PASS; зачистка серии (`docker ps -aq --filter name=pgw- | xargs -r docker rm -f; docker volume ls -q --filter name=pgw- | xargs -r docker volume rm; docker network prune -f`).
- [ ] Шаг 4. docker-E2E на свежем Release, маркер: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` → PASS; зачистка серии.
- [ ] Шаг 5. docker-E2E профильные: `... --filter "FullyQualifiedName~E2ePatroniRestTlsScenarios|FullyQualifiedName~E2ePatroniFileSdScenarios"` → PASS; зачистка серии.
- [ ] Шаг 6. Стенд финально: 00-up → чеки 65, 40, 30 → повторный 00-up (no-op) → 90-down (без сирот) — по задаче 28.
- [ ] Шаг 7. Roadmap-гейт (последний коммит ветки): из `arch/roadmap/reliability.md` удалить пункт `t22-patroni-rest-tls` и все `← t22…`-ссылки; в `arch/roadmap/reliability-report.md` перенести строку из «Осталось» в «Сделано» (merge-коммит + вклад в N — внутренняя зона кластера без открытого HTTP, мутационная грань REST за аутентификацией), убрать «Patroni REST без TLS/аутентификации» из сводки характеристики N.
- [ ] Шаг 8. Commit: `git commit -m "chore: roadmap-гейт t22 — снят с reliability (t22 Д7)"`; ветка готова к ревью/мержу (мерж в `main` — только по отдельной явной команде пользователя).

**Выход:** все критерии §8 закрыты; ветка готова.
**Проверка:** чек-лист §8 п.1–8 построчно; `git log --oneline main..HEAD` — истории шагов.
**Связь со spec:** §6 Д7; §8 все пункты.

---

## Само-ревью плана (по skill writing-plans; обновлено по итогам ревью Фазы 4)

1. **Покрытие spec**: §4 (дельты arch) — задачи 1–9 (все строки таблицы: 13×4, 14×10, 18×4, 02×1, adminpanel×2, сопровождающие); §5.1 — 10–12, 14, 17; §5.2 — 15, 18, 19; §5.3 — 13, 22; §5.4 — 20; §5.5 — 16, 17, 21, 22, 23; §5.6 — 24; §5.7 — 26; §5.8 — 25; §5.9 — 26–28; §5.10 — юниты (10, 11, 12, 13, 15, 17, 20, 21, 22; entrypoint/master-lease — docker-тир 18/19, перенос зафиксирован примечанием фазы Д2), интеграции (14, 23 ×3 блока, включая ensure с живым etcd, 24), E2E (29–31); §6 фазы Д0–Д7 = задачи 1–9 / 10–14 / 15–19 / 20–23 / 24–26 / 27–28 / 29–31 / 32; §7 не-скоуп — в плане не реализуется (allowlist/verify_client/периодическая ротация/monitoring сроков — нет); §8 — задача 32 построчно.
2. **Плейсхолдеры**: отсутствуют; каждое действие — конкретный файл/команда.
3. **Консистентность типов**: `NodeRestTls` (15→17), `RestRotation.PasswordHash/EffectivePassword/RestUsername` (15→17, 21, 22), `EnsureNodeAsync(..., string restPassword, ...)` (17→все 8 call-site'ов, включая RestoreProcess ~575/~611 и внутренний plain Swarm), `InspectNodeEnvAsync` (21→22, 23), `WorkState.RestPending`/`dropRestPending` (16→17, 21, 22), `ClusterCredentials.RestPassword`/`ClusterSnapshot.RestPassword` (13→17, 20, 22; вход конвергенции 21 — пароля из РЕЗУЛЬТАТА ensure, не снапшота), env-имена `PGW_REST_TLS_*`/`SSL_RESTAPI_*`/`PGW_REST_PASSWORD_HASH`/`PGW_NODE_CA`/`HC_TLS_*` — едины по всему плану и совпадают со spec.

## Порядок исполнения и зависимости

Линейный: 1→9 (Д0) → 10→14 (Д1) → 15→19 (Д2) → 20→23 (Д3) → 24→26 (Д4) → 27→28 (Д5) → 29→31 (Д6) → 32 (Д7). Внутри Д1: 10→11→12 (12 зависит от TryParse задачи 10), 13 параллельна 12. Внутри Д2: 15→16→17 строго по порядку (17 читает WorkState.RestPending из 16 — линейная компилируемость), 18/19 параллельны 17. Внутри Д3: 20 до 21 (кред-сигнатура SwitchoverAsync). Каждая задача завершается коммитом — ревью-гейт между задачами возможен после любой.
