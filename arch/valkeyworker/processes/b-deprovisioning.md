# B. DeprovisioningProcess (X0–X3)

Уровень 4. Полный демонтаж кластера от `TO_REMOVE` до чистого etcd и
удалённых контейнеров. Функция —
[02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §3; контракт ключей —
[valkey-clusters/02-keys.md](../../valkey-clusters/02-keys.md).

```
X0 claim + journal(op=deprovision); снапшот «до»
X1 docker: удалить контейнеры vwk-<C>-* (перечисление ListNodeObjects +
   RemoveNode; 404 = ок) — TLS-материал жил в env контейнеров,
   отдельного docker-объекта у домена нет; порядок «сначала docker,
   потом etcd»
X2 etcd: del --prefix /valkey/clusters/<C>/ (заберёт и ca_next_* staging)
   + del /valkeyworker/{claims,work,portalloc,rotations,ca_rotations}/<C>*
   — очистка координации ВКЛЮЧАЯ заявки ротаций (кредов и CA)
X3 снапшот «после»; клэйм снят явно (del + revoke lease)
```
