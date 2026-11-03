# K. CaRotator — ротация per-cluster CA и серверных сертов

Уровень 4. Ротация per-cluster CA и сертификатов ноды. Функция —
[04-security.md](../04-security.md) §6; PKI — [04 §1](../04-security.md).

Исполнение заявки `/valkeyworker/ca_rotations/<C>` (панель через API воркера
[05-api.md](../05-api.md), клэйм-txn `version==0`; формат
[adminpanel/02](../../adminpanel/02-etcd-contract.md) §9.8 один в один с
ротациями кредов: `{"requested_unix","requested_by"}`). Исполнитель —
держатель клэйма `<C>`; **второй шаг Active-ветки** (после миграции T, до
надзора C; InProgress ⇒ C/D/E тика не идут — эксклюзивность, см.
[06-operability.md](../06-operability.md) §1). Образец — kafka
[kafkaworker/README.md](../../kafkaworker/README.md); специфика valkey:
nodes=1 — «rolling» = одно пересоздание `node1`, весь цикл P→C укладывается в
один тик (окно для внешних читателей — секунды); ACL-креды и maxmemory
ротацией CA не затрагиваются; endpoints/portalloc не меняются.

```
K0 guard'ы (ДО открытия окна; после — не проверяются — окно уже
   эксклюзивно, см. [06-operability.md](../06-operability.md) §1): клэйм наш; заявки нет и
   journal-хвоста нет — no-op (NotNeeded); хвост после коммита
   (phase=committed, заявка снята) — финал K4. Кластер не поднят (нет
   endpoints/кредов/CA) — journal waiting-cluster (премиграционный —
   миграция T доведёт; заявка жива — ротация не теряется). Живая ротация
   креда (заявка rotations/<C>, стейт work/<C>/rotation фаз e1*/e2, ИЛИ
   журнал rotate в мутационной фазе e1-added/e2-committed; терминальные
   done/expired — не живые: expired пишется только вне мутаций) —
   waiting-password-rotation (E доиграет этим же тиком ниже по ветке —
   ждущие исходы ветку НЕ блокируют; старт окна — следующим тиком).
   Ждущие исходы возвращаются БЕЗ мутаций и все — ДО открытия окна
   (staging ставится только в P): в каждом из них действует возрастная
   самозачистка — ca-заявка старше RotationTicketTimeoutSec →
   journal expired → txn [del заявки][put ticket_outcomes/<C>
   outcome=expired] → метрика expired; НАЧАТАЯ ротация (staging жив,
   фазы P–C идут) таймаутом не снимается. Перечитка config: TO_REMOVE —
   abort (journal aborted-state-changed; демонтаж B чистит всё, вкл.
   staging — X2)
P  journal phase-p (окно открыто) → генерация НОВОЙ CA
   (ValkeyPki.GenerateCa, случайно) → txn put-if-absent
   ca_next_key/ca_next_pem. Staging в etcd — переживает рестарт воркера;
   проигрыш txn (гонка) → re-read — чужая staging валидна (одна на жизнь
   ротации). Инвариант окна: серт NEW появляется на ноде (R) ТОЛЬКО
   после bundle в ca_pem (D) — фазы строго последовательны P→D→R→C
D  phase-d → txn [compare value(ca_pem)==OLD][put ca_pem = OLD + "\n" +
   nextPem] — клиенты, перечитавшие точку дискавери
   ([valkey-clusters/04-client-discovery.md](../../valkey-clusters/04-client-discovery.md) §1), доверяют сертам обоих
   поколений. Идемпотентность: повторный тик распознаёт готовность по
   вхождению nextPem в ca_pem (put пропускается); срыв compare —
   ca_pem менялся внешне → ретрай тиком
R  phase-r → одно пересоздание node1 с перевыпуском серта от NEW CA
   (лечение ЛЮБОГО состояния ноды — преф-чека живости нет: мёртвая нода
   пересоздаётся здесь же; отличие от kafka, где rolling мёртвого
   кластера ронял ISR). Детект по факту (NodeEnvAsync →
   IsValidNodeEnv(env, advertised, nextPem)): валидный NEW-серт уже
   в env контейнера ⇒ R done (идемпотентность рестарта воркера без
   in-memory трека); иначе RemoveNode → EnsureNode с env от
   ca_next_key/ca_next_pem (VALKEY_TLS_CA = NEW, НЕ bundle —
   [01-placement.md](../01-placement.md) §2) и cmd-обёрткой (args/лимиты/порт
   из декларации и portalloc — адреса не меняются) → state=PROVISIONING →
   AwaitBoot: PING по TLS с доверием nextPem (якорь OLD/bundle здесь
   неверен: одноблочный парсер доверия, серт уже NEW) в бюджете
   NodeBootSec → state=RUNNING. Перечитка config перед R: TO_REMOVE —
   abort
C  committed → ОДНА txn [compare value(ca_next_key)==staging]
   [put ca_pem=NEW; put ca_key=NEW; del ca_next_pem; del ca_next_key;
   del ca_rotations/<C>] — OLD-ключ уничтожается перезаписью (после окна
   он никем не доверяется); снятие заявки атомарно коммиту; срыв compare —
   параллельная ротация → ретрай тиком
K4 финал: снапшот P12 «после» + journal phase=done + put
   ticket_outcomes/<C> outcome=done (kind=ca; идемпотентно — хвост
   после C доигрывается тем же путём)
```

Отказ между фазами безопасен: staging/bundle-состояния в etcd стабильны,
повтор тика доигрывает по факту (staging есть? bundle содержит nextPem? env
серта ноды валиден против nextPem?). Читатели с одноблочным парсингом
`ca_pem` (панельные пробы, метрики-коллектор — OLD-first в bundle) в окне
D→C транзиентно недоверяют NEW-серту: окно = секунды (один poll-цикл),
самокоррекция после коммита C; клиенты библиотеки дискавери строят доверие
по всем блокам ([valkey-clusters/04-client-discovery.md](../../valkey-clusters/04-client-discovery.md) §1) — окно для них
прозрачно.
