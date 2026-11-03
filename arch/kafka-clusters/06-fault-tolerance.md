# Kafka-кластеры: обработка сбоев (толерантность читателей)

Уровень 3. Поведение читателей домена на сбойных и переходных значениях.
Ключи — [02-keys.md](02-keys.md) §1; миграция безопасности —
[kafkaworker/04-security.md](../kafkaworker/04-security.md) §4.

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, `topics/<T>`, `topics/<T>/desired.create`/`desired.delete`, заявка ротации/ребалансировки, прогресс reassignment/regens) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `kafka-key-malformed`; заявка/прогресс с битым JSON для воркера — мусор: reassignment-оператор разбирается по факту Kafka, битый прогресс перезаписывается |
| Неизвестный ключ внутри `/kafka/` | лог-строка + счётчик `unknownKeys`; парсер не падает. В т.ч. неизвестный leaf под `topics/<T>/` |
| Active-кластер без `endpoints` | критический алерт `kafka-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| Active-кластер без `ca_pem`/`admin_password` (или `ca_key` для воркера) | критический алерт `kafka-security-missing`: премиграционный кластер (SASL_PLAINTEXT — миграция [kafkaworker/04-security.md](../kafkaworker/04-security.md) §5 доведёт) либо потеря ключа; пробы панели таким кластером деградируют (TLS-доверие/кред недоступны) |
| Битый PEM в `ca_pem`/`ca_key` | ключ пропускается, parseError-запись снапшота + warning `kafka-key-malformed`; воркер при provisioning перегенерирует ensure'ом только по NotExists — битое значение чистит миграция |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Топик без части факт-полей | читается с null-полями (desired/missing — главные для UI) |
