# B. DeprovisioningProcess (X0–X3)

Уровень 4. Полный демонтаж кластера от `TO_REMOVE` до чистого etcd и
удалённых docker-объектов. Функция —
[02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §3; контракт ключей —
[kafka-clusters/README.md](../../kafka-clusters/README.md).

```
X0 claim + journal(op=deprovision); снапшот P12 «до»
X1 docker: удалить контейнеры/сервисы и volumes kfw-<C>-* (включая
 сироты из ListAsync; 404 = ок); порядок «сначала docker, потом etcd»
X2 etcd: del --prefix /kafka/clusters/<C>/ + del
 /kafkaworker/{claims,work,portalloc}/<C>* + del /kafkaworker/rotations/<C>
X3 снапшот P12 «после»; клэйм снят явно
```

Примечание: в X2 удаляются также `admin_rotations`, `ca_rotations`,
`rebalances`, `reassignments`, `regens`, `ticket_outcomes` —
координация, заявки и прогресс не переживают удаление кластера
(остаточные ключи давали бы вечные алерты
`kafka-rotation-pending`/`kafka-rebalance-pending`).
