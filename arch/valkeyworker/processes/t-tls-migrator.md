# T. TlsMigrator — авто-миграция plain→TLS

Уровень 4. Перевод премиграционного plain-кластера на TLS. Функция —
[04-security.md](../04-security.md) §5; первый шаг Active-ветки.

nodes=1, persistence off — окно миграции = одно пересоздание контейнера
(секунды; кеш восполним). Детект: `ca_pem`/`ca_key` отсутствуют в etcd ИЛИ
args живого контейнера без `--tls-port` (иначе — no-op: отработавший миграцию
кластер неотличим от поднятого канонически, повторный детект — no-op).

```
T0 claim + journal(op=migrate-tls, phase=started); снапшот «до»
T1 ensure CA + кредов (txn put-if-absent, единый механизм с V2);
   re-read; journal ensured-ca; перечитка config (гонка TO_REMOVE
   посреди миграции — abort: journal aborted-state-changed, клэйм жив)
T2 пересоздание контейнера с каноническими TLS-args: env со свежим
   сертом ноды (NodeTlsProvisioner.BuildNodeTlsEnv) + cmd-обёртка
   [01-placement.md](../01-placement.md) §2; RemoveNode → EnsureNode с теми же
   лимитами и портом portalloc (порт/адреса не меняются); journal
   recreated; state=PROVISIONING
T3 ждать готовности: PING по TLS (бюджет NodeBootSec, цикл 100 мс) →
   state=RUNNING; снапшот «после»; journal done
```

Идемпотентность по факту: ключи CA есть? args TLS? PING по TLS отвечает? —
отказ между фазами доигрывается повтором тика. Отработавший миграцию кластер
неотличим от поднятого канонически.
