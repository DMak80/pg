# B. DeprovisioningProcess (X0–X3)

Уровень 4. Полный демонтаж кластера от `TO_REMOVE` до чистого etcd и
снесённых docker-объектов. Функция —
[02-cluster-lifecycle.md](../02-cluster-lifecycle.md) §3; контракт ключей —
[s3-clusters/02-keys.md](../../s3-clusters/02-keys.md).

```
X0 claim + journal(op=deprovision, phase=started) + снапшот P12 «до»
X1 маркеры REMOVING всем инстансам
X2 docker: перечисление объектов кластера (ListNodeObjects по префиксу
   s3w-<C>-) → снос контейнеров и volumes данных по инстансам
   (RemoveNodeAsync с removeVolume; 404 = ок), порядок «сначала docker,
   потом etcd»; чистка portalloc <C> (docker-публикаций больше нет)
X3 etcd: del --prefix /s3/clusters/<C>/ (заберёт и staging-PKI)
   + del /s3worker/{work,rotations,probes}/<C>; снапшот P12 «после»;
   journal done; клэйм снимается явно (не ждём TTL)
```

Заявки и прогрессы (ротации, зонда) не переживают удаление кластера —
остаточные ключи давали бы вечные «живые заявки» на снесённом кластере.
Снос уничтожает данные бэкапов инстансов — осознанное действие оператора
(панель — confirm); риск — [08-operability.md](../08-operability.md) §6 R2.
Повторный provisioning того же имени стартует с чистого бюджета ожидания
подъёма.
