# G. MeshGovernance (доводка состава)

Уровень 4. Приведение фактического mesh инстансов к декларациям `links` +
`denylist`. Функция — [05-mesh-governance.md](../05-mesh-governance.md);
декларации — [s3-clusters/02-keys.md](../../s3-clusters/02-keys.md); механика ownS3 —
[owns3/07-replication.md](../../../../OwnS3/arch/owns3/07-replication.md).

Источник истины — etcd, факт — `GET /admin/replication/status` инстансов
(состав target-записей). Работает под клэймом `<C>`, journal
`op=mesh-governance`; она же — фаза P4 provisioning
([A](a-provisioning.md)). За тик — один инстанс/одна пара (структурные
операции не пачкой); фазовые записи — `idle`, `waiting-join` (ждут условий),
отказ по инстансу с `last_error`.

```
G1 deny-доведение: каждому RUNNING-инстансу, КРОМЕ имён из denylist, —
   полная перезапись PUT /replication/denylist телом
   {entries:[{name, publicKeyPem}]} из denylist кластера (ключи — из
   nodes/<I>/pki); подпись до-join контура — сертификатом оркестратора
   (pki/orchestrator, схема OWNS3-PEER); недоступен → transient, следующий
   тик
G2 сверка состава: желаемые target-записи X = соседи по links минус denylist
   минус несуществующие/не-RUNNING имена:
   - недостающая связь {a,b} → POST /admin/replication/join {"endpoint":<b>}
     на a (root-креды из secrets-ключа; join двусторонний; курсор нового
     соседа −1 + reconcile-забор); 409 (имя с другим ключом) → DELETE
     конфликтующей target-записи, ретрай следующим тиком; 403/502 —
     transient
   - лишняя запись (убрана из links, deny, инстанс выведен) →
     DELETE /admin/replication/targets/{name} на каждой стороне пары
     (курсор сохраняется — возврат соседа продолжится с места)
G3 изоляция deny-инстанса: связи с ним не джойнятся, его записи у соседей и
   собственные записи сносятся (имя не в желаемых — G2)
```

Живость сторон перед join — по `state` (`RUNNING`) и health-факту надзора;
join к недоступному — transient-ретрай. Advertised-endpoint для join — из
portalloc ([01-placement.md](../01-placement.md) §4). Full-mesh не инвариант:
строится ровно заданный `links`. Ошибка G прерывает Active-ветку до зонда F
([08-operability.md](../08-operability.md) §1); заявка зонда живёт до
исполнения.
