# 03 — Live-пробы и алерты

> Назад: [INDEX.md](INDEX.md) · Подсистемы: `src/AdminPanel.Probes` +
> `src/AdminPanel.Core/Alerting`. Контракт: [arch/02](../../arch/adminpanel/02-etcd-contract.md)
> §4/§6, [arch/03](../../arch/adminpanel/03-panels.md) §4–5.

Кратко: `ProbeOrchestrator` (`BackgroundService`, `[InjectAsSingleton(typeof(IHostedService))]`)
раз в `Probes.IntervalSeconds` (15 c) берёт цели из текущего снапшота и гонит пробы
параллельно; результаты — в `IProbeStateStore`; следующий KV-тик refresher'а
обогащает снапшот (`ProbeEnricher`). Обе пробы выключены (`PatroniEnabled`=
`SqlEnabled`=false) — цикл не запускается вовсе.

## Пробы

- **Patroni REST** (`PatroniRestProbe`): `GET http://<host>:8008/cluster` на каждый
  member scope'а; ответ парсит `PatroniClusterParser` (роль/state/timeline/lag).
- **SQL** (`SqlProbe`): Npgsql по DSN шарда из etcd (`host=s1a,s1b port=5432 …`);
  **один коннект на шард**; каталог arch/03 §5 (слоты, sync-standby, подписки,
  инвентарь `bucket_%`); пароль — `Probes.Password` (DSN его не несёт).
- **HostMap** (`HostMapResolver.Resolve(hostMap, host, port)`): override адреса
  «etcd-адрес ноды `host:port`» → «достижимый с панели»; точное совпадение,
  применяется к каждой цели до подключения. В `appsettings*.json` ключ словаря —
  `host__port` (`:` в ключах режут конфиг-провайдеры .NET, урок t10); в памяти/ENV —
  канонический `host:port`, он приоритетен при наличии обоих.

## AlertEngine — 25 правил

`Core/Alerting/Rules/*` — все `[InjectAsSingleton(typeof(IAlertRule))]`, движок
(`AlertEngine`, `[InjectAsSingleton(typeof(IAlertEngine))]`) собирает
`IEnumerable<IAlertRule>` через DI. Id = `kind:target` (стабилен), `SinceUnix`
переносится из предыдущего снапшота («присутствует с…»), сортировка: severity ↓,
затем kind/target (Ordinal). Пороги — `AdminPanel:Alerts` (`AlertsOptions`).

| Группа | Правила (kind) |
|---|---|
| etcd-здоровье (5) | `etcd-unreachable`, `etcd-endpoint-down`, `etcd-no-quorum`, `etcd-alarm`, `snapshot-stale` |
| шардирование/переезды (12) | `cluster-not-initialized`, `cluster-incomplete`, `key-malformed`, `shard-no-master`, `bucket-no-routing`, `bucket-lost`, `bucket-out-of-range`, `move-stale`, `move-frozen-long`, `move-aborting`, `move-flipped-status-stuck`, `inventory-mismatch` |
| HA/слоты (7) | `shard-no-leader`, `ha-member-not-streaming`, `replica-lag-high`, `sync-standby-missing`, `slot-lag-high`, `slot-invalidation-risk`, `slot-wal-lost` |
| пробы (1) | `probe-failed` (sql→critical, patroni→warning, весь скоп молчит→critical один на скоп; lifecycle-цели NOT_INITIALIZED/TO_REMOVE подавлены — arch/03 §4) |

(`inventory-mismatch` сверяет инвентарь SQL-пробы с routing, только ACTIVE-бакеты.)

## Чек-лист «добавить правило/поле пробы»

1. Контракт arch/03 §4 (kind, severity, условие) — первой; порог — в `AlertsOptions`
   + `appsettings.json`.
2. Правило: `Rules/<Kind>Rule.cs : IAlertRule` (+ `[InjectAsSingleton(typeof(IAlertRule))]`);
   `Evaluate(snapshot, ctx)` возвращает алерты со стабильным `kind:target`.
3. Unit-сценарий: fixture-снапшот (`TestSnapshots`) → ожидаемые алерты; при правке
   порога — `AlertTestRules`.
4. Порог/поле наружу: DTO (`AlertsDto`/`HaDto`…), фронт `api/dto.ts` — по arch/03 §2.
5. Живой прогон: интеграционный сценарий или чек стенда (20-alerts) на появление/гашение.

## Грабли

- **`TargetSessionAttributes=read-write`** (Npgsql 10) работает **только на multi-host
  DSN**; read-only-защита — сессионный `SET` **после** выбора мастера (t06): обе
  степени нужны, ни одну не выкидывать.
- **Пробы мимо DSN из etcd не настроить** адрес хоста руками: только HostMap (прод —
  пуст; стенд — `appsettings.Development.json`, порты 5433–5436/8011–8022).
- **Тайминги ожиданий**: тик проб 15 c — e2e-чеки ждут поля проб с запасом (≤40 c),
  алерты — ≤2 KV-тиков; «не дождались за 5 c» — не баг, а недостаток таймаута.
- **`probe-failed` ≠ пустые данные**: отказ пробы оставляет etcd-часть (поля null),
  SQL-поля в UI скрываются с пометкой (arch/01 §8).
- **`probe-failed` — severity по цели (2026-09-01)**: SQL-проба Active-шарда
  упала = critical («кластер не работает»); Patroni одного члена — warning,
  все члены скопа — один critical; NOT_INITIALIZED/TO_REMOVE не алертятся
  (подъём/демонтаж — не авария), но пробы по ним ходят и runtime-ошибки
  остаются в деталях.
- **HostMap в тестах**: интеграционные проверки резолва — на обоих форматах ключа
  (`host:port` и `host__port`, `HostMapResolverTests`).
