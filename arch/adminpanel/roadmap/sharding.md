# Трек: sharding (инспекция кластеров/шардов/бакетов)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) §2.1, §9,
[../03-panels.md](../03-panels.md) (DTO, алерты).

## Задачи

- `t12-cluster-create` — создание кластера из UI: форма (уникальное имя,
  бакеты, шарды ≤ бакетов, реплики, заявка cpu/mem/disk на ноду) + запись
  структуры в etcd (txn-клэйм, состояния NOT_INITIALIZED, request_* в
  /service/<scope>/). Поднятие нод — вне задачи (отдельный provisioning).

