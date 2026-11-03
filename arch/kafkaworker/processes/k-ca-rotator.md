# K. CaRotator (ротация per-cluster CA и сертов — окно двойного доверия)

Уровень 4. Ротация per-cluster CA и сертификатов брокеров. Функция —
[04-security.md](../04-security.md) §4; PKI-канон —
[04 §1](../04-security.md).

Исполнение заявки `/kafkaworker/ca_rotations/<C>` (панель, клэйм-txn; формат
[adminpanel/02](../../adminpanel/02-etcd-contract.md) §9.8 один в один с
ротациями [H](h-password-rotator.md)). Исполнитель — держатель клэйма `<C>`;
фазы — по канону [04-security.md](../04-security.md) §1:

```
P journal op=rotate-ca phase=phase-p → генерация НОВОЙ CA
 (ClusterPki.GenerateCa, случайно — не из сида) → txn put-if-absent
 ca_next_key/ca_next_pem. Staging живёт в etcd — переживает рестарт
 воркера (в отличие от in-memory NEW-паролей H); повторный тик re-read
D phase-d → txn put ca_pem = OLD+NEW bundle (конкатенация PEM; повторный
 тик распознаёт готовность по вхождению nextPem в ca_pem — идемпотентно).
 После фазы клиенты, перечитавшие точку дискавери
 ([kafka-clusters/05-client-discovery.md](../../kafka-clusters/05-client-discovery.md)), доверяют сертам обоих поколений
R phase-r/<broker> → rolling по одному брокеру за тик (порядок Ordinal по
 имени, как A в H): RemoveNode(том жив) → EnsureNode с env от
 BrokerEnvBuilder (подпись серта — NEW CA через кеш R3, truststore —
 bundle); ожидание сходимости DescribeCluster после каждого брокера;
 трек пересозданных — как в H
C committed → все брокеры на NEW: ОДНА txn [compare
 value(ca_next_key)==staging] [put ca_pem=NEW; put ca_key=NEW; del
 ca_next_pem; del ca_next_key; del заявку] — OLD-ключ уничтожается
 перезаписью (после окна он никем не доверяется)
финал: снапшот P12 «после» + journal phase=done
```

Guard'ы и отказоустойчивость — образец [H](h-password-rotator.md); порядок
точек — зеркало valkey K0.3→K0.5
([valkeyworker/README.md](../../valkeyworker/README.md)): ПЕРВЫМ делом после
чтения заявки/журнала — детект открытого окна (staging `ca_next_*` жив ИЛИ
журнал `rotate-ca` вне `{done, waiting-*}`) → доигрывание P→D→R→C БЕЗ ждущих
guard'ов и БЕЗ экспирации (слепой кластер посреди R — передержка: rolling
стоит на ожидании сходимости, окно не сиротеет — заявка жива). Окно не
открыто → ждущие guard'ы: клэйм-гвард; кластер не поднят (нет
endpoints/кредов/CA) → journal waiting-cluster (премиграционный кластер —
waiting до миграции [M](m-security-migrator.md)); живая пароль-ротация H
(заявка `rotations/<C>`/`admin_rotations/<C>` ИЛИ журнал `rotate` в
мутационной фазе роли `phase-a`/`rotated-commit`/`phase-c`, вкл.
`admin:`-префиксы; терминальные `done`/`expired` — не живые: expired пишется
только вне мутаций) → waiting-password-rotation (H доиграет и K продолжит —
приоритет H); живой reassignment (`reassignments/<C>`) или regen
(`regens/<C>`) → waiting без действий (rolling не смешивается с чужими).
Экспирация действует в этих waiting-точках ТОЛЬКО под тройным гвардом
«не начато» ([08-operability.md](../08-operability.md) §3: staging
отсутствует ∧ журнал rotate-ca не в мутационной фазе): ca-заявка старше
`RotationTicketTimeoutSec` → самозачистка §3. Отказ между фазами безопасен —
повтор продолжает по journal-фазе (re-entry идемпотентен: staging в etcd
стабилен, bundle-проверка, трек rolling). Truststore брокеров остаётся bundle
после коммита до следующего пересоздания (безвредно — доверие OLD не
возвращает утраченную силу сертов; env выравнивается надзором
[C](c-node-supervisor.md)). Финал K4 — вместе с journal done put
`ticket_outcomes/<C>` outcome=done. Endpoint API —
`POST /api/kafka/clusters/{c}/ca/rotate` ([07-api.md](../07-api.md) §1);
заявка уже стоит → 409.
