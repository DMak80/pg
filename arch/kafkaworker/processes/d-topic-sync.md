# D. TopicSyncProcess (автосинк + desired-converge)

Уровень 4. Автосинк реестра топиков и исполнение desired-заявок. Функция —
[05-topics.md](../05-topics.md); контракт ключей —
[kafka-clusters/03-topics.md](../../kafka-clusters/03-topics.md).

Тик `TopicSyncIntervalSec` (15 с), только под клэймом `<C>`. Протокол —
[kafka-clusters/03-topics.md](../../kafka-clusters/03-topics.md) (describe→decide→act): новый факт-топик →
put ключ с фактом; исчез без desired → del ключа; исчез с desired → put
`missing:true`; desired отличается по управляемым полям → применить
(AlterTopicConfigs — конфиги, CreatePartitions — только увеличение) и снять
desired (тот же RMW-txn по mod_revision; проигрыш → re-read, следующий тик).
Уменьшение partitions — перманентный отказ журнала (панель отсекает раньше).
`__`-топики — пропуск. Ретраи Polly jitter поверх оркестрации (повтор
безопасен).

Lifecycle-заявки ([kafka-clusters/03-topics.md](../../kafka-clusters/03-topics.md) §2): исполнение перед
факт-синком (порядок: чистка create-коллизий → delete → create → sync),
guards и идемпотентность — там же.
