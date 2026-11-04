# H. RemoveShardProcess (S0–S4)

Уровень 4. Демонтаж шарда по маркеру `shards/<X>/state=TO_REMOVE`. Функция
— [03-shard-scaling.md](../03-shard-scaling.md) §3; ключи —
[../00-etcd-contract.md](../00-etcd-contract.md) §3; перечисления —
[enums.md](enums.md).

Guard'ы G1–G7 в S1 перед любым разрушающим действием:

| Гвард | Условие |
|---|---|
| G1 | кластер Active |
| G2 | шард заявлен |
| G3 | ни один routing не указывает на шард (P23) |
| G4 | ни один status-ключ не ссылается (owner ИЛИ target) |
| G5 | нет заявок `/pgworker/moves/<C>/` с `to=X` или `old_shard=X` (саморазрешающийся) |
| G6 | нет нод `QUARANTINED` |
| G7 | в кластере есть другой шард |

```
S0 claim + journal
S1 guard'ы G1–G7. Провал = journal last_error с причиной + InProgress
   (маркерное состояние живёт; после уезда бакетов демонтаж продолжится сам).
S2 nodes/<n>/state=REMOVING; остановить/удалить контейнеры/сервисы шарда,
   удалить volume (идемпотентно, 404 = ок)
S3 порядок «сначала docker, потом etcd»: del prefix shards/<X>/ + точечные
   request_* + del prefix scope (guard: docker-объектов не осталось) +
   portalloc-фильтрация "<X>/<n>" (read-modify-write под клэймом) +
   del /pgworker/evacuations/<C>/<X> (журнал эвакуации не переживает
   демонтаж шарда)
S4 journal phase=done
```

Финальное состояние: префиксы `shards/<X>/` и `/service/<C>-<X>/` пусты,
записей шарда в portalloc нет, остальные шарды кластера не затронуты.
Порядок «сначала docker, потом etcd» сохранён (мёртвые ключи при сбое
безвредны — повторный тик продолжает).

## Порядок scale-прохода

Active-ветка после надзора выполняет scale-проход: сначала remove-кандидаты
(`shards/<X>/state=TO_REMOVE`), затем add-кандидаты (declared-ноды без
`dsn`), по одному шарду за тик. Демонтаж освобождает хосты/порты до
подъёма (Д13).
