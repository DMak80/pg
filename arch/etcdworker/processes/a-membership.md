# A. MembershipProcess (доводка состава)

Уровень 4. Приведение кластера etcd к декларации `/etcd/config`: недобор,
перебор, авто-замена мёртвого managed-члена. Функция —
[02-membership.md](../02-membership.md); модель узла —
[01-placement.md](../01-placement.md); контракт ключей —
[00-etcd-contract.md](../00-etcd-contract.md).

Вход тика — факты наблюдения надзора ([B](b-node-supervisor.md)). Журнал —
`/etcdworker/work`, `op=membership`.

```
A0 кворум-гейт: /v3/maintenance/status → LeaderMemberId; лидера нет —
   наблюдательный тик (состав не трогаем); member list — failover по
   endpoints; членов делим на managed (peer-URL паттерн
   http://etcdw-node-<k>:2380) и external
A1 декларация: /etcd/config нет/битая — только наблюдение (N=0), тик успешен
A2 резюм: managed-член в state=PROVISIONING — доводим (контейнер по
   закреплённому порту, затем promote); иначе — A3
A3 авто-замена: мёртвый managed-член (порог NodeDeadSec, окружение не
   восстановлено, не лидер) → вывод (фаза replacing, том сносится);
   недобор доберёт следующим тиком
A4 перебор (managed > N): кандидат — не лидер, старейший по номеру; гвард
   «итог external+managed−1 ≥ 3»; вывод (фаза removing, том сносится)
A5 недобор (managed < N): добавление —
   фаза add-learner:
     k = следующий свободный номер (имена не переиспользуются);
     placement (Plain — единственный хост; Swarm — анти-аффинити/least-loaded);
     секция под глобальным portalloc-клэймом: выбор client-порта из
     PortRange по docker-занятости ∪ portalloc → put portalloc →
     листы nodes/<k>/{state=NOT_INITIALIZED, placed_on} →
     member add learner (peer-urls по имени узла);
     отказы: клэйм занят — transient-тик; member-add не прошёл — порт и
     листы освобождаются (без утечки закрепления) → тик Failed;
   state=PROVISIONING → контейнер (флаги 01 §2, initial-cluster — полный
   состав + сам узел, сеть контура, client-порт) → health-ожидание
   (бюджет NodeBootSec); не поднялся — член выводится (том не создан),
   повтор следующим тиком новым номером;
   фаза promoting: promote learner (etcd отвергает промоут отстающего —
   transient, ретрай тиком; уже-voting — сверка по факту ДО вызова) →
   state=RUNNING → фаза done
A6 состав сошёлся: очистка осиротевших КЛЮЧЕЙ — листы /etcd/nodes/<k>/* и
   записи portalloc без члена в составе и без docker-объекта (краш-окно
   вывода не оставляет вечных ключей); фаза cleanup-orphans
```

Идемпотентность: каждый шаг сверяет факт (member в списке? контейнер жив?
learner уже voting?); takeover продолжает с фазы. Одна структурная операция
за тик. Снос осиротевших docker-объектов — не здесь, а в надзоре
([B](b-node-supervisor.md)).
