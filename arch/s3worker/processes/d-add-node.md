# D. AddNodeProcess

Уровень 4. Подъём инстанса в Active-кластере по заявке панели. Функция —
[03-node-scaling.md](../03-node-scaling.md) §1; модель —
[01-placement.md](../01-placement.md); секреты —
[04-security.md](../04-security.md).

Заявка — `nodes/<I>/state=NOT_INITIALIZED` ([07-api.md](../07-api.md) §2).
За тик доводится ОДИН инстанс (одна docker-мутация за тик); повторный проход
идемпотентен по факту.

```
D0 заявки: NOT_INITIALIZED (могло быть несколько за один тик — по одной за
   вызов) И PROVISIONING-инстансы кластера (резюм: надзор C идёт в тике
   раньше D и переводит живой контейнер PROVISIONING → RUNNING — заявка
   исчезает из вида, а финальное окно D (endpoints + config.nodes) осталось
   невыполненным); заявок нет — no-op
D1 гварды ДО мутаций: предел состава 1..9 по фактическому числу инстансов
   (journal-отказ guard-limit); кластер Active
D2 journal(op=add-node, phase=started) + снапшот P12 «до»; чтение
   per-cluster secrets (отсутствие/битость — journal invalid-secrets,
   заявка жива, ретрай тиком; значения в диагностику не попадают)
D3 mini-A: plan — placement-резолв и portalloc-довыделение под глобальным
   клэймом /s3worker/locks/portalloc (не взял → waiting-portalloc-lock);
   state=PROVISIONING + placed_on; PKI: EnsureClusterPki (идемпотентен) +
   EnsureNodePki (put-if-absent; имя инстанса освобождается при выводе —
   повторное создание с тем же именем = новый выпуск)
D4 volume + контейнер с env из текущих секретов и PKI (адрес — advertised
   host + S3-порт из portalloc; лимиты resources; сеть контура — если
   задана)
D5 health-проба по advertised-адресу: не отвечает → waiting-node
   (InProgress, тик повторит); отвечает → state=RUNNING
D6 bucket'ы инстанса — идемпотентно ([02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §5)
D7 завершающее окно: RMW endpoints + актуализация config.nodes до
   фактического состава (txn по mod_revision; актуален — no-op);
   снапшот P12 «после»; journal done
```

Включение инстанса в mesh D НЕ выполняет: доводку по `links` подхватит
Active-G ([05-mesh-governance.md](../05-mesh-governance.md)) — backfill
существующих объектов и join недостающих пар появятся в ближайших тиках.
