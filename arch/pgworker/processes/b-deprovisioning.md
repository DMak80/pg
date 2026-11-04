# B. DeprovisioningProcess (D0–D3)

Уровень 4. Полный демонтаж кластера от `TO_REMOVE` до чистого etcd и
удалённых docker-объектов. Функция —
[02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §3; ключи —
[../00-etcd-contract.md](../00-etcd-contract.md) §3; перечисления —
[enums.md](enums.md).

```
D0 claim + journal(op=deprovision)
D1 для каждого шарда/ноды: остановить и удалить контейнер/сервис (swarm:
   service rm), удалить volume pgw-…-data; nodes/<n>/state=REMOVING;
   идемпотентно (404 = ок); сироты-контейнеры (ключей нет, docker вернул
   имена pgw-<C>-*) — тоже удаляются
D2 удалить префикс /clusters/<C>/ (del --prefix) + точечные
   /service/<C>-shard<k>/request_* + префикс /service/<C>-<X>/
   (guard: docker-объектов не осталось) + /pgworker/moves/<C>/ +
   /pgworker/rotations/<C> + /pgworker/{portalloc,work,claims}/<C>*
   + del --prefix /pgworker/evacuations/<C>/ — журналы эвакуаций не
   переживают удаление кластера (симметрия с S3)
D3 снапшот P12; успех = пустой /clusters/<C>/ + снятый клэйм; имя
   освобождается (повторное создание панели пройдёт)
```

Удаление нод до чистки etcd — порядок осознанный: «мёртвые» ключи при
сбитом D1 безвредны (кластер в TO_REMOVE), повторный тик продолжает.
Клэйм снимается явно (del + revoke lease) — не ждём TTL.
