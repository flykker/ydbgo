# ydbgo

Аналог распределённой SQL-базы данных в стиле YDB — один бинарник,
без внешних зависимостей (кроме `hashicorp/raft`, `github.com/cockroachdb/pebble`,
`google.golang.org/grpc` и `protobuf`).

## Статус по плану

| # | Пункт плана | Статус |
|---|-------------|--------|
| 1 | **KV миля** — `internal/kv`: байтовый MVCC KV (ревизии = raft-index, range/prefix, watch, CAS, lease) | ✅ сделано, тесты зелёные |
| 2 | **Бинарные ops в raft** — вместо SQL-текста в raft-entry пишутся сжатые бинарные ops | ✅ сделано (`internal/sql/bincodec.go`, `internal/raftsvc/batch.go`) |
| 3 | **`ENGINE=TABLE\|KV\|CSTORE`** — атрибут в парсере/AST/схеме/каталоге + фабрика хранилища; колоночный CSTORE (OLAP) | ✅ `ENGINE=` + бэкенды `KV` (MVCC) и `CSTORE` (колоночный) сделаны |
| 4 | **Шардовый `GROUP BY`** — частичные группы на шардах + merge по ключу у координатора (типизированная передача партиалов) | ✅ сделано, `TestShardedCStoreGroupByPushdown` зелёный |
| 5 | **Auto-TTL (retention)** — `RETENTION = '<window>'` в `CREATE TABLE` + фоновый цикл на лидере каждого шарда | ✅ сделано, `TestShardedCStoreAutoTTL` зелёный |
| 6 | **`LIKE`/`NOT LIKE`** — во всех движках (в т.ч. через шарды) + префиксный PK-range для `'префикс%'` в CSTORE | ✅ сделано, `TestLike`/`TestLikeStringPkPrune`/`TestShardedCStoreLike` зелёные |
| 7 | **Вторичные индексы** — `CREATE/DROP INDEX`, обслуживание в DML, fast-path для `=`/`LIKE` в колоночном планировщике | ✅ сделано, `TestSQLIndexEndToEnd`/`TestIndex*` зелёные |
| 8 | **Batch-insert + group-commit тюнинг** — `BatchInsert` (один lock/commit), адаптивное окно батча, pipelined raft-apply | ✅ сделано, `TestEngineBatchInsert`/`TestBatcherCoalescing` зелёные |
| 9 | **`ADMIN COMPACT` + прогон чтений на компактированном LSM** | ✅ сделано, `TestShardedAdminCompact` зелёный; компакция чтения не ускоряет (LSM не узкое место) |
| 10 | **Фикс write stall (FSM snapshot)** — `FSM.Snapshot` больше не сериализует состояние на FSM-горутине: быстрый пин Pebble-снапшота + сериализация в `Persist` | ✅ сделано, `TestEngineSnapshotPointInTime`/`TestEngineSnapshotKVTable`/`TestStressStallWriteLoad` зелёные; 3 стресс-прогона 0 STALL |
| 11 | **Vectorized columnar-скан** — bulk-декод числовых колонок в плотные массивы (`numVec`), vectorized агрегаты/группировка/фильтры, счёт маркеров через `kv.RangeCount` | ✅ сделано, тесты зелёные; агрегаты ~1.6–2×, `GROUP BY` 384k→~450 allocs |
| 12 | **Счётчик живых строк для `COUNT(*)`** — per-table счётчик, поддерживаемый в DML, `COUNT(*)` без WHERE — O(1) | ✅ сделано, `TestCStoreLiveRowCounter` зелёный |
| 13 | **Встроенный UI (веб-консоль)** — единый бинарник: HTTP-сервер + `go:embed` фронт; консоль в стиле Yandex Cloud (Gravity UI); админка, SQL-редактор, свои дашборды, Kibana-like логи | 🚧 UI-P1 ✅ + UI-P2 ✅ + **UI-P3 (Dashboard builder) ✅**: виджеты на `@gravity-ui/charts` (line/bar/pie/stat/gauge/table/histogram/heatmap/log_viewer), перетаскиваемая сетка `react-grid-layout` v2 (drag/resize/rename/CRUD виджетов, один `PUT` на жест, авто-выравнивание коллизий), auto-refresh, hash-роутинг `#/cluster|sql|dashboards|logs`; **UI-P4 (админ-консоль) ✅**: админ-действия (SPLIT/COMPACT/FREEZE/peers) с подтверждением, экспорт в CSV, `/metrics` (Prometheus); **Log explorer**: поиск + histogram + виртуализированный список + live tail (SSE) + bulk ingest `/api/v1/ingest`; **UI-полировка ✅**: YADBGO-логотип с молнией (молния и текст выровнены по иконкам/заголовкам меню, в свёрнутом сайдбаре остаётся одна молния по центру), круглый коллапс-шеврон внизу сайдбара, фикс вечного вертикального скролла (хард-фикс высот `html/body/#root`); **UI-P5 ✅**: живые метрики на Cluster (графики requests/s и p50/p99 per-node), композитные PK-сплиты (`ADMIN SPLIT ... AT (v1, v2)` + table-level `PRIMARY KEY (a,b)` в парсере), мульти-дашборд с активным дашбордом в URL-хеше (`#/dashboards/<id>`, deep-link/back/forward работают) |

### Что дальше (roadmap)

- [ ] **Встроенный UI (веб-консоль)** — консоль в стиле Yandex Cloud на Gravity UI (`@gravity-ui/uikit` + `navigation` + `charts` + `table`), Node 22 LTS:
  - [x] SQL-функции для BI: `time_bucket(interval, ts)`, `COUNT_IF(cond)`, `NOW()`, `percentile(col, p)` (time_bucket/COUNT_IF/NOW готовы; percentile — позже)
  - [x] `ADMIN TABLES` (список таблиц+схем+шардов из каталога) + `ADMIN METRICS-JSON` (структурированные метрики)
  - [x] HTTP-каркас в `serve`: флаг `-http :8080`, `POST /api/v1/query|admin`, `GET /api/v1/tables|shards|nodes|metrics`, `go:embed` статика
  - [x] Системная таблица `_dashboards` (JSON-конфиг) + CRUD `/api/v1/dashboards` + `/api/v1/widget/query` (LRU-кэш)
  - [x] Фронт: `AsideHeader`-каркас (Cluster / SQL / Dashboards / Logs) + страницы Cluster и SQL
  - [x] Log explorer: поиск + histogram (bar-y) + виртуализированный список + live tail (SSE, zero-deps вместо WebSocket), bulk ingest `POST /api/v1/ingest`
  - [x] Dashboard builder: виджеты (`line/bar/pie/stat/gauge/table/histogram/heatmap/log_viewer`) на `@gravity-ui/charts`, агрегация по `time_bucket`, refresh, grid (`react-grid-layout` v2: перетаскивание/ресайз/переименование/CRUD виджетов — сохранение позиций одним `PUT /api/v1/dashboards/{id}` на жест, коллизии авто-выравниваются, правый край не выходит за контейнер; измерение ширины через `useMeasuredWidth` + `ResizeObserver`, т.к. грид монтируется после async-загрузки)
  - [x] Админ-действия (SPLIT / COMPACT / FREEZE / peers) с подтверждением, экспорт, метрики, `/metrics` (Prometheus)
  - [x] Полировка сайдбара: логотип `YADBGO` с бейджем-молнией (центр молнии == центр иконок меню, текст — на одной вертикали с заголовками пунктов), круглый коллапс-шеврон внизу панели (`renderFooter`), фикс вечного вертикального скролла (`html/body/#root {height:100%}`)
  - [x] Живые метрики на Cluster: графики requests/s и p50/p99 латентности per-node на `@gravity-ui/charts` (опрос каждые 3 с, скользящее окно ~3 мин, rate из дельт счётчиков), переключатель «все узлы / конкретный узел» со статусом
  - [x] Композитные PK-сплиты: `ADMIN SPLIT ... AT (v1, v2, ...)`, парсер `CREATE TABLE` поддерживает table-level `PRIMARY KEY (a, b)`, форма Split по PK-колонкам
  - [x] Мульти-дашборд: переключатель дашбордов (Select), создание/удаление, активный дашборд в URL-хеше `#/dashboards/<id>` — deep-link, back/forward и выбор через UI синхронизируют хеш и контент (глубинная ссылка на несуществующий id откатывается к первому)
  - [ ] **UI-P6** (следующая фаза): см. раздел «Фазы» ниже — виджеты на произвольных таблицах

- [x] `internal/kv` — фундамент: байтовые ключи, ревизии, range/prefix, watch/CAS/lease
- [x] Запуск из YDB-style YAML-конфига (`-config cluster.yaml`): топология кластера в одном файле, узел выбирается по `-node-id`, join выводится из файла, приоритет «явный флаг > конфиг > дефолт» (`internal/config`, `scripts/qa-config.sh`)
- [x] Бинарные ops в raft-entry (вместо `strings.Join` SQL-текста) — `internal/sql/bincodec.go`
- [x] `ENGINE=` в парсере/AST: `CREATE TABLE ... ENGINE=TABLE|KV|CSTORE|CSTORE2`
- [x] Фабрика хранилища по движку (`newStoreFor`) + `internal/kv` в качестве бэкенда для `ENGINE=KV`
- [x] Колоночное хранилище `CSTORE` (OLAP) поверх MVCC
- [x] OLAP-оптимизации: проекция по колонкам в `SELECT` (в т.ч. через шарды) + pushdown `COUNT/SUM/MIN/MAX/AVG` в колоночный скан + бенчмарк `CSTORE` vs `TABLE`
- [x] **Нативный колоночный бэкенд `CSTORE2`** (mpart): immutable LZ4-парты, dense fixed-width колонки, bloom на PK парта, разреженный индекс гранул (`idx.bin`) — точечные/`ORDER BY DESC LIMIT` читают только нужные гранулы, гранульный кэш на парте; гранульные блоки — raw LZ4 (без framing/checksum) + параллельный декод независимых гранул (холодные SUM/GROUP ~2×)
- [x] **Пред-аллокация векторного скорачивания**: `countFor`-пред-аллокация `numVec` в `colDecodeNumeric` (обе стороны), пред-аллокация групп — alloc/op 134MB→**24MB**, локальный `GROUP BY` 92ms→**48ms**
- [x] **Группировка по int-ключу через bucket-массив**: вместо hash-map по 1M строк — bucket индексов групп (диапазон ключа ≤ 4M), merged-проход «группа+все агрегаты за один цикл»; пул `numVec` (reuse 16MB dense-буферов, alloc/op 24MB→**130KB**) — локальный `GROUP BY` 63ms→**25ms**, кластерный warm 76–85ms→**56–63ms**
- [x] **Zone maps (`CSTORE2`)** — per-гранульные min/max числовых dense-колонок в `idx.bin` (ver 3); `WHERE col = lit` в колоночном COUNT/SUM-пути пропускает целые гранулы, чьи зоны не пересекают литерал — монотонная колонка на 1M строк: **37.6ms→2.4ms** (~16×), локальный бенч `BenchmarkFilteredSumPrune`
- [x] **Фоновый пред-декод dense-колонок** — после idle-flush/merge новый парт ставится в очередь фонового префетча (`preloadDense`): первый SUM/GROUP запрос находит тёплый декодированный кэш вместо холодного LZ4-прохода (локальный холодный GROUP 43.6ms→**33.4ms**, ~23%)
- [x] **Перф-метрики по классам запросов** — `ADMIN METRICS`/`ADMIN METRICS-JSON` разбивают латентность по классам (`agg`/`group`/`order`/`scan`/`point`/`kv`), `ADMIN METRICS-JSON` + Prometheus-эндпоинт UI экспортируют per-class p50/p99 (динамические ключи `<class>_latency_ms`)
- [x] **Zero-copy dense-декод** — whole-window SUM/GROUP по одному dense-парту возвращает `numVec`, алиасящий кэш denseVals парта (borrowed) вместо 8MB-копии на колонку: warm GROUP single-part аллокации 529KB→**130KB/op**, 27.9ms→**25.6ms** (GC-давление от копий уходит из горячего пути; `putNumVec` не возвращает borrowed-векторы в пул)
- [x] **KV SQL-поверхность** для `ENGINE=KV`: `KV PUT/GET/DELETE/SCAN` через raft и шарды
- [x] OLAP-оптимизации: агрегаты с `GROUP BY` в колоночном исполнении, колоночное покрытие `WHERE` (PK-range pruning + пропуск шардов)
- [x] Pushdown цельно-табличных агрегатов на шарды (partial-merge без пересылки строк) + retention/compaction для `CSTORE` (range-`DELETE` по PK + физический `kv.Compact`)
- [x] Шардовый `GROUP BY`: частичные группы считаются на каждом шарде, координатор сливает их по ключу (включая взвешенный `AVG`)
- [x] Auto-TTL: `RETENTION = '<window>'` в `CREATE TABLE` + фоновый цикл на лидере каждого шарда (удаление через raft, идемпотентно)
- [x] `LIKE`/`NOT LIKE` в `WHERE` для всех движков + префиксная оптимизация по строковому PK в `CSTORE`
- [x] Вторичные индексы: `CREATE/DROP INDEX`, обслуживание в DML (remove-then-add, безопасно при raft-replay), fast-path для `=`/`LIKE` в columnar-планировщике
- [x] Batch-insert API (`BatchInsert` через `sqlx.BatchInsertEngine`) + group-commit тюнинг (конфигурируемое адаптивное окно, idle-флаш, pipelined raft-apply)
- [x] `ADMIN COMPACT` — forced LSM-компакция локальных сторов; прогон чтений на компактированном LSM (вывод: LSM не узкое место чтений)
- [x] Фикс write stall: `FSM.Snapshot()` пинит Pebble-снапшот за O(1), сериализация ушла в `snapshot.Persist(sink)` на отдельной raft-горутине; вотчдог `BATCH-STALL` оставлен как диагностический предохранитель
- [x] Vectorized columnar-скан: bulk-декод числовых колонок в плотные массивы (`colDecodeNumeric`/`numVec`), векторные `aggNumVec`/`colAccum.addNum` в агрегатах/`GROUP BY`/предикатах, `kv.RangeCount` для счёта маркеров строк (пропуск версий и tombstone через снапшот-`prev`)
- [x] Счётчик живых строк CSTORE: per-table счётчик (тег `'n'` в engine-сторе), exact-обслуживание в `rowPut`/`rowDelete`/`rowDeleteAll` (delta копится в tx, на commit — атомарно с row-writes), пересборка при снапшот-restore; `COUNT(*)` без WHERE = O(1) чтение счётчика

## Возможности

- **SQL**: `CREATE/DROP TABLE` (включая `ENGINE=` и `RETENTION = '<window>'`),
  `CREATE/DROP INDEX`, `INSERT`, `SELECT`
  (WHERE, `LIKE`/`NOT LIKE`, JOIN-подобные фильтры, GROUP BY, агрегаты,
  ORDER BY, LIMIT, DISTINCT), `UPDATE`, `DELETE`, транзакции
  (`BEGIN/COMMIT/ROLLBACK`).
- **Хранилище**: движок на **Pebble** (embedded LSM, чисто Go) — схема и
  строки (по sortable первичному ключу), **подключаемый** KV-бэкенд через
  интерфейс `store`/`storeTx`; durability принадлежит raft-логу, поэтому
  store коммитится без fsync (`pebble.NoSync`, WAL не синкается) — см.
  груп-коммит ниже.
- **Распределённость**: Raft-консенсус (`hashicorp/raft`). Одна **мета-группа**
  (каталог/шардирование) + отдельные **группы данных** на каждый шард.
  Записи идут через лидера каждой группы и реплицируются на фолловеров.
- **Шардирование**: таблицы разбиваются на диапазонные шарды по первичному
  ключу; каждый шард реплицируется на `rf` узлов.
- **Отказоустойчивость**: при падении лидера группы (мета или данных) кластер
  переизбирает нового; **фоновое восстановление** реплик данных после падения
  узла возвращает группу к исходному фактору репликации.
- **Авто-сплит**: переросшие шарды разделяются по медианному ключу.
- **Производительность записей**: raft **group-commit** (один raft-entry на
  батч) + **один store-commit без fsync на батч** → один fsync на батч
  (в raft-логе); постоянные **conn-пулы** между узлами вместо dial-на-запрос.
- **Свой протокол**: **gRPC** (HTTP/2) с protobuf; горячие клиенты используют
  bidirectional stream (ответы сопоставляются по id) вместо per-call
  JSON-парсинга и установки stream.

## Сборка

```bash
go build -o ydbgo ./cmd/ydbgo
```

## Один бинарник: serve / run / repl / bench

`ydbgo serve` стартует узел, `ydbgo run` — разовые SQL-запросы,
`ydbgo repl` — интерактивная оболочка, `ydbgo bench` — замер записей.

```bash
ydbgo serve -addr 127.0.0.1:2135 -data ./data
ydbgo run -addr 127.0.0.1:2135 "CREATE TABLE t (id int64 primary key, v string)"
ydbgo repl -addr 127.0.0.1:2135
```

## Кластер узлов (шардированный)

Узел запускается в кластерном режиме, если задан `-raft-addr`. Первый узел
использует `-bootstrap`, остальные подключаются через `-join`.

```bash
# узел 1 (бутстрап; RF=3, авто-сплит и восстановление включены)
ydbgo serve -addr 127.0.0.1:2135 -data ./n1 \
    -raft-addr 127.0.0.1:7001 -node-id n1 -bootstrap \
    -rf 3 -shard-size 1048576 -split-check 5s -recovery-check 2s

# узлы 2–5 присоединяются к узлу 1 (SQL 2136–2139)
for i in 2 3 4 5; do
  ydbgo serve -addr 127.0.0.1:213$((i+4)) -data ./n$i \
      -raft-addr 127.0.0.1:700$i -node-id n$i -join 127.0.0.1:2135 &
done
```

### Флаги `serve`

| Флаг | По умолчанию | Описание |
|------|--------------|----------|
| `-addr` | `:2135` | адрес SQL-сервера |
| `-data` | `./ydbgo-data` | каталог данных узла |
| `-node-id` | `raft-addr` | id узла |
| `-raft-addr` | — | адрес raft; пустой → одиночный режим без кластера |
| `-bootstrap` | false | инициализировать новый кластер (первый узел) |
| `-join HOST:PORT` | — | SQL-адрес живого узла для вступления |
| `-rf N` | 0 (все узлы) | фактор репликации data-шардов |
| `-shard-size N` | 0 (выкл.) | порог авто-сплита в байтах |
| `-split-check DUR` | 5s | интервал проверки размеров шардов |
| `-recovery-check DUR` | 0 (выкл.) | интервал проверки/восстановления реплик |
| `-ttl-tick DUR` | 0 (выкл.) | интервал авто-TTL (удаление строк старше `RETENTION` окна таблицы) |
| `-pprof ADDR` | — | слушать `net/http/pprof` (напр. `:6060`) для `go tool pprof http://host:port/debug/pprof/profile` |

## Запросы

Любой запрос можно направить на любой узел: не-лидер пересылает его
текущему лидеру соответствующей группы.

```bash
ydbgo run -addr 127.0.0.1:2135 "CREATE TABLE users (id int64 primary key, name string, age int64)"
ydbgo run -addr 127.0.0.1:2135 "INSERT INTO users VALUES (1,'Alice',25),(2,'Bob',30)"
ydbgo run -addr 127.0.0.1:2135 "SELECT * FROM users WHERE age >= 30 ORDER BY id"
ydbgo run -addr 127.0.0.1:2135 "SELECT name, COUNT(*) AS c FROM users GROUP BY name"
ydbgo run -addr 127.0.0.1:2135 "SELECT name FROM users WHERE name LIKE 'A%' ORDER BY name"
ydbgo run -addr 127.0.0.1:2135 "SELECT name FROM users WHERE name NOT LIKE '%li%'"
ydbgo run -addr 127.0.0.1:2136 "UPDATE users SET age = 31 WHERE name = 'Bob'"
```

Записи маршрутизируются к шарду по первичному ключу и проходят через лидера
группы этого шарда; чтения выполняются локально по всем шардам.

### Auto-TTL (retention)

Таблица с `RETENTION` автоматически очищается от старых строк: лидер каждого
шарда раз в `-ttl-tick` удаляет строки, чей timestamp-PK младше `now - window`
(окно задаётся строкой: `'24h'`, `'7d'` — суффикс `d` = дни). Требуется
единственная PK-колонка типа `timestamp`.

```bash
# логи живут 7 дней, затем удаляются фоном (нужен флаг serve -ttl-tick)
ydbgo run -addr 127.0.0.1:2135 \
  "CREATE TABLE logs (ts timestamp primary key, level string, lat double) ENGINE=CSTORE RETENTION = '7d'"
```

## `bench` и `ADMIN METRICS`

```bash
ydbgo bench -addr 127.0.0.1:2135 -n 10000 -rows 100 -c 8
```

Гонит конкурентный write-ворклуд (gRPC-conn-пул), выводит **клиентский**
p50/p99 по латентности записи, rows/s и stmt/s, а затем серверный `ADMIN
METRICS`. Числа зависят от fsync-скорости диска; в конфиге с
групп-коммитом N конкурентных записей ≈ 1 raft fsync на батч.

| Флаг | По умолчанию | Описание |
|------|--------------|----------|
| `-addr` | `:2135` | адрес сервера |
| `-n N` | 10000 | число write-стейтментов |
| `-rows R` | 100 | строк в одном `INSERT` |
| `-c C` | 8 | конкуренция (рабочих горутин / conn-пул) |
| `-table T` | `bench` | таблица для записи |

`ydbgo run -addr ... "ADMIN METRICS"` на любой ноде выводит серверные
счётчики и лаг-ника p50/p99 отдельно для записей и чтений.

## ADMIN — управление и диагностика

`ydbgo run -addr ... "ADMIN ..."`. Некоторые команды автоматические —
выполняются самим кластером при сплитах и восстановлении.

| Команда | Описание |
|---------|----------|
| `ADMIN JOIN <node-id> <meta-raft-addr>` | добавить узел в мета-группу |
| `ADMIN REGISTER <node-id> <sql-addr> <raft-addr>` | зарегистрировать узел в каталоге |
| `ADMIN SHARDS <table>` | список шардов таблицы: `shard,start,end,nodes,size` |
| `ADMIN SPLIT TABLE <t> AT <pk>` | вручную разбить шард по значению PK: одиночное значение (`AT 42`) или список для составного ключа (`AT (1, 'foo')`) |
| `ADMIN FREEZE-SHARD <t> <shard>` | заморозить шард (внутреннее при сплите) |
| `ADMIN UNFREEZE-SHARD <t> <shard>` | разморозить шард |
| `ADMIN UNMOUNT-SHARD <t> <shard>` | закрыть и удалить локальную реплику |
| `ADMIN SHARD-PEERS <t> <shard>` | члены группы данных (id, addr) |
| `ADMIN COMPACT` | forced полная LSM-компакция всех локальных сторов шардов |
| `ADMIN METRICS` | серверные счётчики + p50/p99 записи/чтения |

Внутренние (используются при сборке группы и восстановлении, хендлинг
форвардится на лидера группы):

```
ADMIN MOUNT-SHARD <t> <shard> <start-b64> <end-b64> <bootstrap> <node...>
ADMIN SHARD-ADD-PEER   <t> <shard> <peer-id> <peer-raft-addr>
ADMIN SHARD-REMOVE-PEER <t> <shard> <peer-id>
ADMIN SHARD-WAIT-LEADER <t> <shard>
ADMIN EXEC-SHARD <t> <shard> <sql...>
ADMIN SCAN-SHARD <t> <shard>
```

## Архитектура

```
cmd/ydbgo            — один бинарник: serve / run / repl / bench / usage
internal/kv          — байтовый MVCC KV (ревизии, range/prefix, watch/CAS/lease) — фундамент для ENGINE=KV|CSTORE
internal/sql         — лексер, парсер, AST, исполнитель, агрегаты
internal/storage     — Engine поверх pluggable store (pebble по умолчанию),
                       PK-кодирование, снапшоты (Marshal/Replace), батч-транзакции;
                       нативный колоночный бэкенд CSTORE2 (mpart.go, mpart_col.go)
                       с dense-колонками, bloom-фильтрами и разреженным индексом гранул
internal/raftsvc     — generic Raft-группа, Node + Group + FSM, group-commit
internal/shard       — Manager, каталог (meta), роутинг, авто-сплит, recovery, метрики
internal/server      — gRPC-сервер, клиент, CLI-обвязка
internal/rpc         — сгенерированные protobuf/gRPC-типы и сток (ydb.proto)
internal/proto       — удобные типы запроса/ответа, gRPC-клиент (bidi-stream), ConnPool
```

### Движок (Pebble)

Строки лежат в Pebble: каждый шард/таблица — отдельный диапазон ключей
(префикс `r\x00<table>\x00`), ключ — sortable кодировка первичного ключа,
значение — строка в порядке колонок. Схема — в диапазоне `m\x00<name>`.
Чтения (`Scan`) идут курсором в порядке ключа (LSM-итератор с границами).
Интерфейс `store`/`storeTx` (`internal/storage/store.go`) позволяет подставить
другой embedded KV без изменения верхних слоёв.

### Поток записи (одиночный узел)

`client → server → raft.Apply → FSM → storage(pebble)`.

Raft-entries переносят **бинарные ops** (`sqlx.EncodeStatements`), а не SQL-текст:
лидер один раз парсит стейтменты, реплики применяют результат без повторного
парсинга; entry компактнее на wire и в raft-логе.

### Движок таблиц: `ENGINE=`

`CREATE TABLE ... ENGINE=TABLE|KV|CSTORE|CSTORE2` выбирает бэкенд таблицы. `TABLE`
(по умолчанию) — обычный row-store на Pebble; `KV` — MVCC byte-store
(`internal/kv`, ревизии/range/prefix), подключённый через адаптер
`internal/storage/kvstore.go`; `CSTORE` — колоночный (column-major) бэкенд
`internal/storage/cstore.go` поверх того же `internal/kv`: каждая колонка —
непрерывный диапазон, строки пересобираются из своих ячеек; `CSTORE2` — нативный
колоночный бэкенд `internal/storage/mpart.go` на собственных файлах (`meta.bin`
+ по файлу на колонку + `pk.bin`/`del.bin`/`idx.bin`): части immutable,
LZ4-сжатые колонки в компактном fixed-width (dense) формате для чисел, bloom-
фильтр на PK парта и разреженный индекс гранул (`idx.bin`, ~64k строк на
гранулу). Схема таблиц лежит в default-сторе, строки — в сторе своего движка;
`MarshalState`/`ReplaceState` (raft-снапшоты) охватывают все движки.

### OLAP-оптимизации для `CSTORE`

`CSTORE` реализует опциональный интерфейс `sqlx.ColumnEngine`, который
исполнитель `SELECT` использует на колоночных таблицах:

- **Проекция по колонкам** — исполнитель вычисляет, какие колонки реально
  трогает запрос (SELECT-выражения, WHERE, GROUP BY, ORDER BY), и читает
  только их диапазоны, не пересобирая остальные ячейки. Шардированный роутер
  спускает такую проекцию в каждый шард (`SELECT <нужные колонки> ...`), так
  что выгода работает и на кластере.
- **Pushdown агрегатов** — `SELECT COUNT/SUM/MIN/MAX/AVG(...)` без
  `WHERE/GROUP BY/ORDER BY` вычисляется прямо в сторе одним проходом по
  колонке (агрегаты по одной колонке объединяются в один скан). Числовые
  колонки декодируются bulk-декодом в плотные массивы и сворачиваются
  векторно (`colDecodeNumeric`/`aggNumVec`). `COUNT(*)` без `WHERE` — O(1)
  чтение per-table счётчика живых строк (см. P2 ниже); `COUNT(*)` с PK-окном —
  `kv.RangeCount` по маркерам строк без чтения ячеек. Такой запрос не
  материализует строки вообще.
- **Pruning по PK** — `WHERE`, который сводится к диапазону первичного ключа
  (обычный случай для логов: временное окно), превращается в границы скана
  колонок; исполнитель не читает строки вне окна, а шардированный роутер
  вообще пропускает шарды, чей диапазон не пересекает окно. Литералы
  приводятся к типу PK-колонки (в т.ч. `timestamp`), границы корректны для
  `<`, `<=`, `>=`, `>`.
- **`GROUP BY` в колоночном исполнении** — `GROUP BY` по одной колонке со
  всеми агрегатами (`COUNT/SUM/MIN/MAX/AVG`) считается одним проходом по
  выровненным колонкам (hash-группировка по закодированному значению группы).
- **Pushdown цельно-табличных агрегатов на шарды** — чистый агрегат без
  `GROUP BY` шлётся в каждый затронутый шард как `SELECT SUM(col), COUNT(col)
  ...` (для `AVG`); координатор сливает частичные агрегаты (`SUM` сумм,
  взвешенное `AVG`, `MIN/MAX`) — через сеть не пересылается ни одна строка.
- **Шардовый `GROUP BY`** — `SELECT g, COUNT(*) ... GROUP BY g` считается как
  `SELECT g, COUNT(*), <partials> ... GROUP BY g` на каждом затронутом шарде;
  координатор сливает частичные группы по ключу (`SUM` сумм, `MIN/MAX`,
  взвешенное `AVG` по частичным `SUM`/`COUNT`). Частичные строки передаются
  типизированно (типы колонок приходят из плана агрегации), а не
  пересобираются по схеме таблицы.
- **`LIKE`/`NOT LIKE`** — `%`/`_` поддерживаются во всех движках (в т.ч. через
  шарды); в `CSTORE` шаблон `'префикс%'` по строковому PK превращается в
  префиксный PK-range (`[prefix, successor(prefix))`), что даёт pruning по
  колонкам и пропуск шардов так же, как диапазонное условие.
- **Разреженный индекс гранул (`CSTORE2`)** — парт хранится блоками по ~64k
  строк, каждый блок LZ4-сжат независимо; `idx.bin` держит PK-максимум каждой
  гранулы и смещения/длины её блоков в `pk.bin`/`col_N.bin`/`del.bin`. Точечный
  `WHERE pk = ...` и `ORDER BY pk DESC LIMIT n` декодируют только гранулу,
  способную держать ключ (а `ORDER BY DESC` — хвостовые гранулы с конца, как
  обратный скан sparse-индекса ClickHouse). Декодированные гранулы мемоизируются
  на парте (парты immutable), так что повторные точки/`UPDATE` не перечитывают
  блок. Это закрыло разрыв `ORDER BY ... LIMIT`/point после слияния партов в
  один: в 1M-строковом A/B (5 узлов, RF=3) против ClickHouse 25.3 тёплые
  `ORDER BY id DESC LIMIT 10` и point-get идут 19–25ms при 22–24ms у CH.
- **Auto-TTL (retention)** — `CREATE TABLE ... RETENTION = '<window>'` (напр.
  `'7d'` или `'24h'`); фоновый цикл на лидере каждого шарда раз в
  `-ttl-tick` удаляет строки, чей timestamp-PK старше окна (одна timestamp-PK
  колонка). Удаление идёт через raft (идемпотентный `DELETE WHERE pk <
  cutoff`), реплицируется и затем попадает под `kv.Compact`.
- **Retention/compaction** — `DELETE ... WHERE <окно по PK>` на `CSTORE`
  выполняется колоночным range-delete (маркеры строк и ячейки за один проход,
  по всем шардам); `kv.Compact` затем физически удаляет устаревшие ревизии.

`go test ./internal/storage -bench BenchmarkOLAP` сравнивает `TABLE` vs
`CSTORE` на 10k строк: `COUNT(*)` — ~6x быстрее, `SUM` — ~4x, агрегат с
`WHERE` по PK — ~9x, `COUNT` в окне — ~26x, `SUM` в окне — ~16x, `GROUP BY`
в окне — ~9x. Полный `SELECT *` ожидаемо хуже — колонка-мажор плохо собирает
все колонки сразу.

### KV SQL-поверхность (`ENGINE=KV`)

Прямой байтовый KV-доступ к таблице движка `KV` (в дополнение к обычным
`INSERT/SELECT/UPDATE/DELETE` по строкам):

```
KV PUT <table> '<key>' '<value>'
KV GET <table> '<key>'
KV DELETE <table> '<key>'
KV SCAN <table> ['<start>' ['<end>']]
```

Ключи/значения — сырые байты. Записи (`PUT`/`DELETE`) идут через raft как
бинарные ops (group-commit); чтения (`GET`/`SCAN`) выполняются локально. На
шардированном кластере одиночный ключ маршрутизируется к владельцу-шарду по
байтовому диапазону, `SCAN` веером опрашивает все шарды и сливает результат.
Данные KV живут в отдельной области стора (`'k'`), не пересекаясь со строками
и схемой.

### Поток записи (шардированный)

`client → server → Manager.route → выбор шарда по PK → лидер группы шарда
→ raft.Apply → FSM → storage → репликация в группу` шарда. Каталог
(таблицы, шарды, размещение `Spec.Nodes`) реплицируется через отдельную
**мета-группу**.

### Каталог (meta): copy-on-write

Каталог неизменяем между изменениями: `MetaFSM.Catalog()` возвращает текущий
объект без сериализации/копирования (read-путь дешёвый даже при каждой
записи), а каждая команда `Apply` строит **новую** копию каталога, применяет
мутацию к ней и атомарно публикует её. Это убирает JSON-сериализацию всего
каталога из горячего пути записи.

### Внутришардная параллельность

`execInsert` группирует строки батча по владельцу-шарду и отправляет пачки
**параллельно** (goroutine per шард), так что по диапазонным шардам один
INSERT не ходит последовательно. Адрес лидера шарда кэшируется
в `ManagedShard` (TTL 1s), чтобы путь записи не совершал TCP-dial для
резолва лидера на каждый запрос.

### Параллельные чтения по шардам

`execSelect` сканирует затронутые шарды **параллельно** (`parallelShardRows`:
goroutine на шард), включая все три пути — цельно-табличный pushdown агрегатов,
шардовый `GROUP BY` и обычный проекционный скан. Латентность запроса сходится
к `max` по шардам вместо `sum`. Каждый шард читается с **локальной** реплики
(при rf≥2 разные ноды коорд-узла параллельно читают у себя разные шарды);
для не-локальных шардов делается `ADMIN SCAN-SHARD` к живому размещению.
Результаты собираются детерминированно в порядке шардов (по индексу), первая
ошибка прерывает ожидание; пропуск непересекающихся шардов по PK-диапазону
работает как и раньше.

### Group-commit и персистентность

1. Лидер шарда собирает конкурентные write-стейтменты в батч (окно ~1мс,
   до 4096 ops) и пишет **один** raft-entry.
2. `fileLogStore.StoreLogs` делает один `fsync` на батч — raft-лог дюрабилен
   до ответа клиенту.
3. `FSM.Apply` выполняет все стейтменты батча внутри **одной** store-транзакции
   (`Engine.UpdateBatch`) → один `pebble.Batch.Commit(NoSync)` без fsync на
   батч.

Итого на батч: 1 raft fsync (в логе), а не по два на каждую запись. Дюрабилити
владеет raft-лог, а Pebble-файл играет роль кэша состояний: коммиты идут
`NoSync`, WAL не синкается (на чистом закрытии флашится) — на лидере нет
второго fsync. Снапшоты (`FSM.Snapshot` → `MarshalState`) и компакция raft-лога
(`DeleteRange`) не дают логу расти бесконечно; после снапшота Pebble-файл
переписывается компактным потоком состояний (`ReplaceState`).

Межнодовые вызовы идут через `proto.ConnPool` — по одному **gRPC-соединению**
(HTTP/2, один conn мультиплексирует все запросы) на адрес; все конкурентные
вызовы клиента шарится через один bidi-stream, ответы сопоставляются по id.
Сломанное соединение закрывается и пересоздаётся, так что путь записи не
платит за TCP-хендшейк или per-call stream setup на каждый запрос.

### Восстановление реплик (recovery)

Только лидер мета-группы. Раз в `-recovery-check` он:

1. пингует SQL-адреса узлов (2 неудачных подряд → узел «мёртв»);
2. для каждого шарда, у которого `Spec.Nodes` содержит мёртвый узел,
   при сохранённом кворуме группы:
   - монтирует новую реплику-фолловера на живой узел (`MOUNT-SHARD ... false`),
   - добавляет её в группу (`SHARD-ADD-PEER`),
   - удаляет мёртвого избирателя (`SHARD-REMOVE-PEER`),
   - коммитит новое размещение в каталог (`set_shard_nodes`).

RF сходится к исходному; при падении >половины реплик восстановление ждёт
возврата узла. Вернувшийся узел при рестарте монтирует только те шарды,
где он остался в `Spec.Nodes`. Новые шарды (`CREATE TABLE`, `SPLIT`)
размещаются только на живых узлах.

## Веб-консоль (UI)

Встроенный веб-интерфейс в стиле **консоли Yandex Cloud** — единый бинарник:
`serve -http :8080` поднимает HTTP-сервер рядом с gRPC, статику фронта раздаёт
`go:embed`. Бэкенд вызывает `Manager.Handle` in-process (никаких лишних сетевых
хопов): `POST /api/v1/query` — произвольный SQL, `POST /api/v1/admin` — `ADMIN`
команды, `GET /api/v1/tables|shards|nodes|metrics` — кластерная витрина,
`POST /api/v1/ingest` — bulk-вставка логов, `GET /api/v1/tail` — live-поток
результата запроса (SSE).

Фронт: **Gravity UI** — та же дизайн-система, что у консоли Yandex Cloud
(пакет `@yandex-cloud/uikit` переименован в `@gravity-ui/uikit`):
`@gravity-ui/uikit` (компоненты) + `@gravity-ui/navigation` (`AsideHeader` —
сайдбар-«полка») + `@gravity-ui/charts` (графики, включая heatmap) +
`@gravity-ui/icons`. Таблицы — собственный лёгкий компонент `ResultTable`
(`@gravity-ui/table` несовместим с React 19). Требуется Node 22 LTS
(`.nvmrc`, апгрейд с 18 EOL через `nvm`).

### Фазы

- **UI-P1 — фундамент (готово)**: SQL-функции `time_bucket(interval, ts)`, `COUNT_IF(cond)`,
  `NOW()` (для BI-запросов); `ADMIN TABLES` (список таблиц+схем+шардов из каталога)
  и `ADMIN METRICS-JSON`; HTTP-каркас `/api/v1/*` + `go:embed`; системная таблица
  `_dashboards` + CRUD + `/api/v1/widget/query` (LRU-кэш); фронт с `AsideHeader`
  (страницы Cluster и SQL). Сборка фронта: `cd internal/ui/web && npm install && npm run build`.
- **UI-P2 — Log explorer (готово)**: поиск по времени/тексту + histogram (bar-y
  на `@gravity-ui/charts`, бакеты через `time_bucket`) + виртуализированный список +
  **live tail** (SSE через `GET /api/v1/tail` — zero-зависимый транспорт вместо
  WebSocket, EventSource сам переподключается) + bulk ingest `POST /api/v1/ingest`.
  Метаданные таблиц (`columns` с типами и PK) доступны через `GET /api/v1/tables`.
- **UI-P3 — Dashboard builder (готово)**: виджеты (`line/bar/pie/stat/gauge/table/
  histogram/heatmap/log_viewer`) на `@gravity-ui/charts` (datetime/category оси,
  time_bucket-агрегация), перетаскиваемая сетка `react-grid-layout` v2
  (позиции персистятся), auto-refresh по интервалу, редактор виджетов
  (тип/заголовок/SQL/размер + примеры запросов). Данные виджетов —
  `POST /api/v1/widget/query` (LRU-кэш); конфиг дашборда (`{title,
  refresh_interval, widgets:[{id,type,title,sql,x,y,w,h}]}`) живёт в
  `_dashboards`. Позиции сохраняются только по `onDragStop`/`onResizeStop`
  (один `PUT` на жест — `onLayoutChange` в RGL v2 стреляет на каждый кадр
  жеста); коллизии авто-выравниваются vertical-компактором, двойной padding
  контейнера устранён (правое поле впритык к контейнеру, `overflow:hidden`);
  ширина грида меряется кастомным `useMeasuredWidth(active)` (RGL требует
  числовой `width`, а контейнер монтируется после async-загрузки — старый
  хук с `deps=[]` давал 0). Страницы фронта открываются по hash-роутингу
  (`#/cluster`, `#/sql`, `#/dashboards`, `#/logs`). QA-скрипты
  (`verify-grid.mjs`/`verify-dash.mjs`, puppeteer-core) проверяют геометрию
  (без пересечений, без правого выхода) и что каждая операция = один `PUT`.
- **UI-P4 — админ-консоль (готово)**: на странице **Cluster** — админ-действия с
  диалогами подтверждения: `ADMIN COMPACT` (кластерный, в шапке), `ADMIN SPLIT
  TABLE <t> AT <pk>` (пер-табличный и пер-шардовый, с вводом значения PK),
  `ADMIN FREEZE/UNFREEZE-SHARD <t> <shard>`, `ADMIN SHARD-PEERS <t> <shard>`
  (результат — таблица ID/Addr). Результат любой команды показывается в
  диалоге (rows/note; ошибки — красным), после мутаций таблицы/шарды
  перечитываются. **Экспорт**: кнопка Export CSV на страницах SQL и Logs
  (`src/export.ts` — UTF-8 BOM, экранирование `",\n`, заголовок = колонки).
  **Метрики**: `GET /metrics` — Prometheus text format из `ADMIN METRICS-JSON`
  (per-node `ydbgo_requests_total{type=write|read}`, `ydbgo_latency_milliseconds
  {quantile=p50|p99}`, `ydbgo_uptime_seconds`); standalone-режим тоже пишет
  счётчики (в `internal/server` добавлен свой `nodeMetrics`).
- **UI-P5 — метрики, продвинутые админ-операции, мульти-дашборд (план, следующий шаг — UI-P6)**:
  - [x] **Метрики в UI**: на странице Cluster — живые графики per-node
    (requests/s по type=write|read, p50/p99 латентность) из `/api/v1/metrics`
    на `@gravity-ui/charts` (line, опрос каждые 3 с, скользящее окно ~3 мин,
    rate вычисляется из дельты кумулятивных счётчиков); переключатель «все
    узлы / конкретный узел» с индикатором статуса. Проверяется
    `verify-metrics.mjs` (графики рендерятся, растут под нагрузкой,
    переключатель узлов переключает данные).
  - [x] **Композитные PK-сплиты**: `ADMIN SPLIT TABLE <t> AT (<v1>, <v2>)` для
    составного первичного ключа — парсер принимает одиночное значение и
    скобочный список через запятую (кавычки/запятые внутри строк учитываются),
    валидация числа значений против схемы PK и типа каждой колонки, тип
    timestamp теперь парсится корректно (RFC3339, граница сплита кодируется
    типом tTimestamp, а не строкой); форма Split на Cluster — по одному полю на
    PK-колонку с подсказкой по типу. Парсер CREATE TABLE научился различать
    column-level `PRIMARY KEY` и table-level `PRIMARY KEY (a, b)` для
    составного ключа. Проверяется `TestShardedCompositePKSplit` и
    `verify-admin.mjs` (диалог с двумя полями → новый шард `kvp-0-1`).
  - [x] **Мульти-дашборд**: переключатель дашбордов (Select) со списком из
    `_dashboards`, создание/удаление, активный дашборд живёт в URL-хеше
    `#/dashboards/<id>` (`src/hash.ts`: `parseDashId`/`setDashHash`). Выбор через
    UI, deep-link и кнопки браузера (back/forward) синхронизируют хеш и контент;
    хеш-обработчик подтягивает свежий список, если целевой id не был в кэше
    страницы (например дашборд создан в другой вкладке), а несуществующий id
    откатывается к первому дашборду. `App.tsx` не затирает `#/dashboards/<id>`
    при синхронизации страницы. Проверяется `verify-dashhash.mjs` (resolve
    первого дашборда, deep-link на второй, выбор через UI, back, bogus-id
    fallback).
  - [ ] **Виджеты на произвольных таблицах**: редактор виджета уже принимает
    произвольный SQL; добавить picker таблиц/колонок (схема из
    `/api/v1/tables`) и валидацию запроса перед сохранением.
  - [ ] Мелкий UX: кнопка «Отменить» в редакторе дашборда, горячая клавиша
    «Выполнить» (Ctrl/Cmd+Enter) в SQL-редакторе, история запросов.

### Тестирование UI (QA-скрипты)

UI проверяется не snapshot-тестами, а **реальным браузером** через
`puppeteer-core` (devDep фронта) — headless Chrome против работающего сервера
(`http://127.0.0.1:8080`). Причина: `--dump-dom`/SSR не дают геометрию, а
позиции, центры и скроллы нужно мерить в рантайме.

Все скрипты QA лежат в репозитории и запускаются из корня:

| Скрипт | Назначение |
|--------|------------|
| `scripts/ui-restart.sh` | собрать фронт + бинарь и (пере)запустить демо-узел (`serve -bootstrap -rf 1`); аргументы и пути — через env (`YDBGO_BIN`, `DATA`, `SQL_ADDR`, `HTTP_ADDR`, `RAFT_ADDR`, `NODE_ID`), убийство старого инстанса отдельным шагом (иначе pkill цепляет свой же shell); если старт не прошёл (напр. каталог `DATA` под `/tmp` остался без демо-таблиц после жёсткого kill во время ресида) — данные пересоздаются и запуск повторяется один раз |
| `scripts/reseed-demo.sh` | засеять чистые демо-данные против **работающего** сервера: таблица `logs ENGINE=CSTORE` (300 строк через `ydbgo run`, бинарь — `$YDBGO`, по умолч. `./bin/ydbgo`), пересоздать дашборд «Cluster overview», удалить старые |
| `scripts/ui-qa.sh` | прогнать все QA-скрипты `internal/ui/web/verify-*.mjs` в заданном порядке (или один — по glob-аргументу, напр. `./scripts/ui-qa.sh 'verify-grid*'`); порядок важен: `verify-metrics` пишет в `logs` и должен идти до `verify-admin` (тот делает SPLIT и оставляет сплит-шард); каждый печатает свою строку `RESULT: OK` |
| `scripts/qa-config.sh` | поднять двухузловой кластер из YAML-конфига (`serve -config`): bootstrap + join по топологии из файла, оба узла видны через `/api/v1/nodes`, DDL/DML/read сквозь адреса из конфига, явный `-addr` перекрывает конфиг, неизвестный `-node-id` отклоняется; свои порты/временный каталог, чужие процессы не трогает |
| `scripts/qa-mpart.sh` | A/B 5-узлового кластера RF=3: `ENGINE=CSTORE` vs `ENGINE=CSTORE2` (нативный mpart) на 1M строк (env: `QA_WORKDIR`, `QA_STATEMENTS`, `QA_ROWS`, `QA_CONC`, `QA_KEEP=1`) — колоночные пробы `COUNT/SUM/GROUP BY/ORDER BY DESC LIMIT/point` + footprint; после нагрузки `sleep 2` (CH-parity) для слияния партов |
| `internal/ui/web/verify-*.mjs` | отдельные проверки браузером (см. таблицу ниже) |

Типовой цикл:

```bash
./scripts/ui-restart.sh          # сборка + перезапуск сервера (Node берётся через nvm)
./scripts/reseed-demo.sh         # чистые демо-данные
./scripts/ui-qa.sh               # все проверки
./scripts/ui-qa.sh 'verify-grid*' # или одна конкретная
```

Важно: `verify-admin` оставляет `logs` расщеплённым, поэтому **каждый полный
прогон `ui-qa.sh` должен предваряться `reseed-demo.sh`** — иначе повторный
`SPLIT` того же ключа падает с 400 («split key is below shard start»).

QA-скрипты (`internal/ui/web/verify-*.mjs`, запуск `node verify-<name>.mjs`,
браузер — `$CHROME` или `/usr/bin/google-chrome`):

| Скрипт | Что проверяет |
|--------|---------------|
| `verify-pages.mjs` | 4 страницы (`#/cluster\|sql\|dashboards\|logs`) открываются без JS-ошибок и не-favicon 404, логотип/коллапс-кнопка на месте |
| `verify-scroll.mjs` | хард-фикс скролла: короткие страницы (sql/dashboards/logs) умещаются ровно (`scrollH == clientH`), кластер с графиками скроллится, высокая страница прокручивается (финальная проверка сама докидывает свежие строки в `logs`, т.к. демо-данные живут от начала текущего часа «вперёд» — в «1h»-окне у начала часа почти нет строк, и страница легитимно умещается в 600px) |
| `verify-logo.mjs` | выравнивание: центр молнии == центр иконок меню, текст `YADBGO` на одной вертикали с заголовками пунктов (в развёрнутом и свёрнутом сайдбаре), коллапс-шеврон внизу (≤20px от низа панели) |
| `verify-grid.mjs` | сетка дашборда: виджеты без пересечений, правый край впритык к контейнеру, каждый жест (resize/drag/rename) = ровно один `PUT /api/v1/dashboards/{id}`; единственный 404 — favicon |
| `verify-dash.mjs` | **идемпотентный**: сбрасывает первый дашборд к канонической конфигурации, прогоняет drag/rename/add (каждый = ровно один PUT), геометрию без пересечений, CRUD-раундтрип через API и в конце восстанавливает исходную конфигурацию |
| `verify-dashhash.mjs` | мульти-дашборд и URL-хеш: открытие `#/dashboards` резолвится в первый дашборд и проставляет `#/dashboards/<id>`; deep-link на созданный через API второй дашборд переключает страницу; выбор через Select обновляет хеш; кнопка back браузера возвращает хеш и контент; несуществующий id откатывается к первому (в конце удаляет свой временный дашборд) |
| `verify-admin.mjs` | админ-действия на Cluster: SPLIT логов (появляется новый шард `logs-0-1`), SHARD-PEERS (диалог с колонками ID/Addr), композитный SPLIT таблицы `kvp` с 2 PK-колонками (форма из двух полей → шард `kvp-0-1`) + DROP в конце |
| `verify-export.mjs` | кнопка Export CSV: headless Chrome требует `Browser.setDownloadBehavior` на browser-сессии (blob-URL download не даёт `download`-событие puppeteer), файл ищется опросом каталога `/tmp/ydbgo-dl`; проверяется UTF-8 BOM + заголовок + строки |
| `verify-metrics.mjs` | метрики на Cluster: 4 графика рендерятся (SVG), под нагрузкой (ingest/query через API) число точек растёт, переключатель узлов переключает данные, нет JS-ошибок |

Дополнительно: `go test ./...` (в т.ч. `TestUIPromMetrics` — формат
`GET /metrics`) и ручная проверка визуала через скриншоты, которые делают
QA-скрипты (`/tmp/ui-*.png`).

### Запуск и API

```bash
go build -o ydbgo ./cmd/ydbgo
# единый бинарник: SQL + raft + HTTP-консоль
./ydbgo serve -addr 127.0.0.1:2135 -data ./data -http :8080 \
  -raft-addr 127.0.0.1:7001 -node-id n1 -bootstrap -rf 1
# открыть http://localhost:8080
```

#### Конфигурация кластера (YAML)

Кластер описывается одним YDB-style YAML-файлом (см. `examples/cluster.yaml`);
узел находит себя по `-node-id`, адреса/каталог/join берутся из файла,
явные флаги перекрывают файл:

```yaml
config:
  hosts:
  - {host: 127.0.0.1, grpc: 2135, raft: 7001, data: ./ydbgo-data, id: n1, bootstrap: true}
  - {host: 127.0.0.1, grpc: 2136, raft: 7002, data: ./ydbgo-n2, id: n2}
```

```bash
./ydbgo serve -config examples/cluster.yaml -node-id n1   # bootstrap-узел
./ydbgo serve -config examples/cluster.yaml -node-id n2   # join — из файла
./ydbgo run  -config examples/cluster.yaml "ADMIN TABLES" # addr = bootstrap-узел
```

Правила:
- **Приоритет: явный флаг > config-файл > встроенный дефолт.** Не заданные на CLI
  значения подставляются из записи хоста (`addr←host:grpc`, `data`, `raft-addr`,
  `node-id`, `bootstrap`), а не-bootstrap-узлу — `join = grpc` bootstrap-хоста.
- `http`, `pprof`, `rf`, `shard-size`, `split-check`, `recovery-check`, `ttl-tick`
  в конфиге отсутствуют и задаются только флагами.
- Валидация при загрузке: ≥1 хост, уникальные `id`, корректные порты, `host`
  по умолчанию `127.0.0.1`, ровно один `bootstrap: true`; неизвестные ключи —
  ошибка.
- `run`/`repl`/`bench -config` используют адрес bootstrap-узла как `-addr` по
  умолчанию.

```bash
# произвольный SQL
curl -s -X POST localhost:8080/api/v1/query -d '{"sql":"SELECT * FROM logs"}'
# ADMIN-команды
curl -s -X POST localhost:8080/api/v1/admin -d '{"cmd":"ADMIN TABLES"}'
# кластерная витрина
curl -s localhost:8080/api/v1/tables | python3 -m json.tool
curl -s localhost:8080/api/v1/shards?table=logs
curl -s localhost:8080/api/v1/nodes
curl -s localhost:8080/api/v1/metrics
# Prometheus text format (per-node counters/gauges)
curl -s localhost:8080/metrics
# админ-действия из UI: ADMIN COMPACT / SPLIT / FREEZE / SHARD-PEERS
curl -s -X POST localhost:8080/api/v1/admin -d '{"cmd":"ADMIN COMPACT"}'
curl -s -X POST localhost:8080/api/v1/admin -d '{"cmd":"ADMIN SHARD-PEERS logs logs-0"}'
curl -s -X POST localhost:8080/api/v1/admin -d '{"cmd":"ADMIN SPLIT TABLE logs AT 2026-08-17T15:00:00Z"}'
# bulk ingest логов (ячейки: string/number/bool/null)
curl -s -X POST localhost:8080/api/v1/ingest -d '{
  "table":"logs",
  "columns":["ts","level","msg"],
  "rows":[["2026-08-17T10:00:00Z","INFO","started"],["2026-08-17T10:05:00Z","ERROR","boom"]]
}'
# live tail (SSE): результат запроса каждые N секунд
curl -s -N "localhost:8080/api/v1/tail?interval=2&sql=SELECT%20*%20FROM%20logs%20ORDER%20BY%20ts%20DESC%20LIMIT%205"
# дашборды: CRUD + запрос виджета (TTL-кэш 5с)
curl -s -X POST localhost:8080/api/v1/dashboards -d '{
  "name":"Demo","config":{"title":"Demo","refresh_interval":30,"widgets":[
    {"id":"w1","type":"line","title":"Logs/5m","sql":"SELECT time_bucket('\''5m'\'', ts) AS t, COUNT(*) FROM logs GROUP BY 1","x":0,"y":0,"w":6,"h":6}
  ]}}'
curl -s -X POST localhost:8080/api/v1/widget/query -d '{"sql":"SELECT COUNT(*) FROM logs","ttl":5}'
```

## Тесты

```bash
go test ./...
go test ./... -run TestShardedFiveNodeRecovery -count=5  # стабильность восстановления
go test ./internal/server -run TestBenchConcurrentWrites -v  # e2e бенчмарк + ADMIN METRICS
```

Покрытие: парсер/выражения (включая `LIKE`/`NOT LIKE` и `RETENTION` в
`CREATE TABLE`), движок (Pebble, включая переживание перезапусков и
снапшот-раундтрип), интеграция SQL+storage (`LIKE` на `TABLE`/`CSTORE`,
префиксный prune по строковому PK), сервер+клиент по gRPC, кластер
(2- и 5-узловой, RF=3, авто-сплит), шардовый `GROUP BY` (partial-merge),
авто-TTL (удаление старых строк, выживание свежих), переизбрание лидера при
падении, восстановление реплик после падения узла, конкурентные записи с
p50/p99 и `ADMIN METRICS`.

### Как тестировать 5-узловой кластер

Внутри-процессный тест кластера: `TestShardedFiveNodeCluster` поднимает 5 нод
(RF=3), создаёт 3 таблицы с разными PK (`int64`, `int64`, `string`), режет
каждую на 4 шарда, засеивает 400 строк, проверяет чтения/записи/апдейты/
удаления со всех нод:

```bash
go test ./internal/server -run TestShardedFiveNodeCluster -v -count=1
```

Диски нод можно вынести в RAM (`/dev/shm`), чтобы исключить влияние диска и
ускорить прогон:

```bash
TMPDIR=/dev/shm go test ./internal/server -run TestShardedFiveNodeCluster -v -count=1
```

Бенчмарк того же 5-узлового кластера (RF=3): `TestBenchFiveNodeWrites` гонит
тот же workload, что и etcd-сравнение (8 клиентов × 120 стейтментов × 200
строк в батче), и сверяет репликацию до всех нод:

```bash
TMPDIR=/dev/shm go test ./internal/server -run TestBenchFiveNodeWrites -v -count=1
```

### Сравнение с etcd (5-узловой кластер vs etcd RF=3)

Одинаковый workload на RAM-дисках (`/dev/shm`): 8 клиентов × 120 write-
стейтментов × 200 ключей/строк в одном батче (один raft entry на стейтмент),
RF=3. etcd гоняется тем же клиентским скриптом (один `Txn` из 200 `Put` на
свой raft entry).

| Система | Топология | p50 | p99 |
|---------|-----------|-----|-----|
| **ydbgo** | 5-узловой, RF=3, Pebble | **~17ms** | **~22ms** |
| etcd 3.7 | 3-узловой, RF=3 | ~19–21ms | ~30–35ms |
| ydbgo | одиночный узел | ~18ms | ~23ms |
| etcd 3.7 | одиночный узел | ~16–17ms | ~22–27ms |

При репликации RF=3 ydbgo быстрее etcd на ~15% по p50 и ~30% по p99. Причина:
запись не платит второй fsync — durability владеет raft-лог, а Pebble коммитится
`NoSync` (у etcd поверх WAL-fsync идёт ещё bbolt-fsync). На одиночном узле etcd
чуть впереди по p50 (меньше fsync-накладных без репликации), но отстаёт по p99.

### Сравнение с ClickHouse (колоночный эталон)

Наш `CSTORE` — колоночный движок с урезанным SQL, поэтому корректно сравнивать
его с настоящей колоночной БД. Эталон: **ClickHouse 25.3 single-node** в docker
на RAM-диске (`/dev/shm`), движок `MergeTree`, `ORDER BY ts`. Наша сторона:
**5-узловой кластер ydbgo**, RF=3, данные на `/dev/shm`, таблица `logs
(ts timestamp primary key, level string, lat double) ENGINE=CSTORE`, разрезана
на 5 шардов (по одному на узел). Одинаковый workload (`cmd/benchcol`): 192 000
строк `logs(ts, level, lat)`, 8 клиентов × 960 INSERT-батчей по 200 строк,
окно `[03:00,04:00)` → 36 000 строк; чтения — 6 запросов × 100 прогонов.

Запись (client-side, на стейтмент из 200 строк):

| Система | Топология | p50 | p99 | rows/s |
|---------|-----------|-----|-----|--------|
| **ydbgo CSTORE** | 5-узловой, RF=3 | **~32ms** | **~54ms** | **~48k** |
| ClickHouse 25.3 | single-node, MergeTree | ~24ms | ~41ms | ~65k |

Чтения (client-side, на запрос):

| Запрос | ClickHouse p50/p99 | ydbgo CSTORE p50/p99 |
|--------|--------------------|----------------------|
| `COUNT(*)` | 7.7 / 11.7ms | 41.7 / 58.8ms |
| `SUM, AVG(lat)` | 16.4 / 19.0ms | 51.7 / 57.6ms |
| окно `[03:00,04:00)` | 14.8 / 16.8ms | 17.6 / 20.6ms |
| `GROUP BY level` | 23.8 / 28.1ms | 143.2 / 168.1ms |
| `level LIKE 'erro%'` | 23.1 / 30.0ms | 77.6 / 85.2ms |
| `ORDER BY ts DESC LIMIT 10` | 9.3 / 11.1ms | **1.7 / 2.2ms** |

Результаты всех запросов **идентичны** обеим системам (count 192 000, сумма
9 593 506.08, окно 36 000 строк, `GROUP BY`: info 115 284 / warn 47 871 /
error 28 845, LIKE 28 845). Вывод честный: по записи ydbgo отстаёт ~2× — но это
с учётом raft-репликации на 5 узлов (RF=3), при этом один raft-entry на батч
вместо per-row. По чтению ClickHouse впереди на сканах/агрегациях (SIMD-векторизация,
потоковая колоночная обработка и прунинг по гранулам); особенно дорог у нас
`GROUP BY` (полный скан без вторичных индексов), а `LIKE` уже закрыт
columnar-предикатом (см. план ниже). `ORDER BY ... LIMIT` закрыт обратным
PK-сканом. Вторичные индексы теперь реализованы (см. ниже); дорожная карта —
векторный executor и прунинг гранул.

#### 5-узловой кластер ClickHouse, RF=3

Тот же workload против **5-узлового кластера ClickHouse 25.3** на RAM-дисках
(`/dev/shm`): 5 шардов × RF=3, каждый шард реплицирован на 3 из 5 узлов
(`ReplicatedMergeTree`, ZK-репликация), чтение через Distributed-таблицу
`logs_dist`. Схема повторяет нашу: запись маршрутизируется на шардовую
таблицу `logs_{shard}` напрямую (как ydbgo пишет на лидера шарда) с
`insert_quorum=2` — аналог raft-кворума 2/3.

Запись (client-side, на стейтмент из 200 строк):

| Система | Топология | p50 | p99 | rows/s |
|---------|-----------|-----|-----|--------|
| **ydbgo CSTORE** | 5-узловой, RF=3 | **~32ms** | **~54ms** | **~48k** |
| ClickHouse 25.3 | 5-узловой, RF=3 | ~209ms | ~332ms | ~7.5k |

Чтения (client-side, на запрос):

| Запрос | ClickHouse p50/p99 | ydbgo CSTORE p50/p99 |
|--------|--------------------|----------------------|
| `COUNT(*)` | 21.0 / 65.3ms | 41.7 / 58.8ms |
| `SUM, AVG(lat)` | 36.3 / 47.0ms | 51.7 / 57.6ms |
| окно `[03:00,04:00)` | 41.1 / 54.4ms | 17.6 / 20.6ms |
| `GROUP BY level` | 50.7 / 70.9ms | 143.2 / 168.1ms |
| `level LIKE 'erro%'` | 41.3 / 55.7ms | 77.6 / 85.2ms |
| `ORDER BY ts DESC LIMIT 10` | 35.6 / 65.6ms | **1.7 / 2.2ms** |

Результаты идентичны (192 000 строк, окно 36 000, `GROUP BY`: info 115 284 /
warn 47 871 / error 28 845). Вывод: при RF=3 с кворумной репликацией
ClickHouse на записи **медленнее нашей raft-записи в ~6× по p50** (ZK-
репликация + `insert_quorum` стоит двух сетевых раундов с подтверждениями на
партию), по чтению колоночный движок по-прежнему впереди на сканах/агрегациях
(1.2–9×), но на окне разрыв исчезает (41ms против 45ms) — наш window-скан с
прунингом по PK почти догнал, `ORDER BY ... LIMIT` теперь **быстрее CH**
(1.7ms против 35.6ms) за счёт bounded-скана PK-индекса; `LIKE` (78ms против
41ms, 1.9×) и `GROUP BY` (143ms против 51ms, 2.8×) закрыты columnar-предикатом
и inline-hash группировкой, `COUNT`/`SUM`/`AVG` (42–52ms) — bulk-декодом.

Запуск сравнения:

```sh
docker run -d --name ch-bench -p 8123:8123 -v /dev/shm/chdata:/var/lib/clickhouse \
  clickhouse/clickhouse-server:25.3
# наш 5-узловой кластер: см. «Кластер узлов», затем:
go run ./cmd/benchcol -ch http://127.0.0.1:8123 -ydb 127.0.0.1:2135 -stmts 960 -rows 200 -clients 8 -reads 100
# 5-узловой CH кластер RF=3 (контейнеры c0..c4, таблицы logs_0..logs_4 +
# VIEW logs + Distributed logs_dist предсозданы):
go run ./cmd/benchcol -nodes http://127.0.0.1:8120,http://127.0.0.1:8121,http://127.0.0.1:8122,http://127.0.0.1:8123,http://127.0.0.1:8124 -stmts 960 -rows 200 -clients 8 -reads 100
```

### 1M-строковый A/B: `CSTORE2` vs ClickHouse

Тот же протокол, но крупнее и на нативном движке: **1 000 000 строк** заливаются
через `ydbgo bench` в `ENGINE=CSTORE2` на 5-узловой кластер RF=3 (данные на
`/dev/shm`), после нагрузки кластер ждёт `SYSTEM SYNC REPLICA`-эквивалент
(2s idle, чтобы фоновая компакция слила парты в один), затем замеряются холодные
и тёплые колоночные чтения. Эталон: ClickHouse 25.3 single-node на `/dev/shm`
(те же 1M строк, `MergeTree`), пробы — `./scripts/qa-mpart.sh` и
`/tmp/qa-ch-1m.log`.

| Запрос | CH cold | ydbgo CSTORE2 cold | CH warm | ydbgo CSTORE2 warm |
|--------|---------|--------------------|---------|--------------------|
| `SUM(id)` | 34–37ms | **74ms** | 34–37ms | **29–34ms** |
| `GROUP BY g` | 39–41ms | **110ms** | 39–41ms | **56–63ms** |
| `ORDER BY id DESC LIMIT 10` | 22–23ms | **23ms** | 22–23ms | **21–24ms** |
| point `WHERE id=42` | 23–24ms | **46–63ms** | 23–24ms | **21–25ms** |
| `COUNT(*)` | 20–24ms | **22–28ms** | 20–24ms | **~22ms** |

Холодные пробы платят декод 1M-строкового парта с диска (как CH после
загрузки); тёплые — повторные пробы на прогретом кэше парта. После введения
разреженного индекса гранул + гранульного кэша `ORDER BY ... LIMIT` и point-get
стали **на паритете или быстрее CH** (21–25ms против 22–24ms), чего не было до
этого: на одиночном слитом парте они шли 243ms / 129ms, а на 16-партовом —
223ms / 110ms (cold). `SUM` — **паритет/лучше CH** (29–34ms против 34–37ms).
`GROUP BY` сокращён с ~2.5× до ~1.4× (56–63 против 39–41): локально
группировка 1M строк идёт **25ms** (против 63ms до оптимизаций) — int-ключ
группируется через bucket-массив вместо hash-map, группа и все агрегаты
считаются одним merged-проходом (без отдельного прохода по группам и без
`groups[p]`-косвенности), dense-декод идёт bulk-append'ом без per-row колбека,
а 16MB `numVec`-буферы переиспользуются через пул (alloc/op 24MB→**130KB**).
Остаток на кластере — базовый RTT/overhead распределённого исполнения
(21–25ms, тот же порядок, что у `COUNT`/`ORDER`/point), а не сам счёт.
Дополнительно холодные SUM/GROUP ускорены в ~2× переводом гранульных блоков
на raw LZ4 (без framing/checksum, распаковка одним `UncompressBlock` в
преаллоцированный буфер, ~6× быстрее фрейма) + параллельным декодом
независимых гранул.

Запуск:

```sh
QA_WORKDIR=/dev/shm ./scripts/qa-mpart.sh   # ydbgo: 5n RF=3, CSTORE + CSTORE2, 1M строк
```

### План оптимизаций по итогам сравнения

Разрывы с ClickHouse (5n RF=3) по p50: запись у нас быстрее в ~6×, окно почти
паритет (1.1×), полные сканы без индекса — главные потери: `GROUP BY` 2.8×,
`COUNT`/`SUM` 2.5–3×; `LIKE` закрыт (1.9×) и `ORDER BY`
уже в нашу пользу (0.05×).

| Tier | Оптимизация | Сейчас | Цель |
|------|-------------|--------|------|
| 1 | ✅ `ORDER BY pk DESC LIMIT N` — обратный PK-скан, ранний выход | 780ms → **~1.7ms** | ~5–20ms ✅ |
| 1 | ✅ `LIKE`/`=` по не-PK колонке — columnar предикатный скан/агрегат | 992ms → **~78ms** | ~60–100ms ✅ |
| 1 | ✅ `GROUP BY` — групповой columnar-агрегат (inline hash по level) | 474ms → **~143ms** | ~100–150ms ✅ |
| 2 | ✅ `COUNT`/`SUM`/`AVG` — bulk-декод колонок без per-row reader/map | 95–160ms → **~42–52ms** | ~30–60ms ✅ |
| 3 | ✅ Batch-insert API + тюнинг group-commit (адаптивное окно, idle-флаш, pipelined raft-apply) | 32ms → **~21ms (p50, 8 клиентов)** | меньше ✅ |
| 4 | ✅ Прогон чтений на compacted LSM (forced `ADMIN COMPACT`) | без изменения | LSM — не узкое место ✅ |
| P0 | ✅ Фикс write stall: `FSM.Snapshot` не блокирует FSM-горутину (пин + `Persist`) | 4–24 STALL → **0** (3 прогона) | 0 STALL ✅ |
| P1 | ✅ Vectorized columnar-скан (bulk-декод, `kv.RangeCount`) | agg 135–146ms → **~62–90ms**; `GROUP BY` 384k→~450 allocs | меньше ✅ |
| P2 | ✅ `COUNT(*)` без WHERE — счётчик живых строк | 52ms (rangecount) → **O(1) чтение счётчика** | O(1) ✅ |

Реализовано (Tier 1, пункт 1): `ORDER BY PK ... LIMIT N` больше не сканирует
всю таблицу и не сортирует — это bounded-скан обратного PK-индекса с ранним
выходом (`internal/kv.RangeDesc` → `storage.ScanTopN` → пушдаун в sql-executor
и per-shard `ORDER BY ... LIMIT` в роутере). Замер на 5-узловом кластере RF=3
в том же workload: **780ms → ~1.8ms (p50)** — быстрее ClickHouse 5n RF=3
(35.6ms). Parity сохраняется (192 000 строк, окно 36 000).

Реализовано (Tier 1, пункт 2): `WHERE level LIKE 'erro%'` / `level = 'x'` по
не-PK колонке больше не материализует строки и не строит per-row контексты.
Новый планировщик `sql.PlanWhere` раскладывает `WHERE` на PK-диапазон + один
предикат по не-PK колонке (равенство или LIKE с ведущим литералом); в storage
добавлены columnar `ColumnCountFiltered` / `ColumnAggregatesFiltered` /
`ScanColumnsFiltered`, которые сканируют колонку-предикат (keep-маска) и
считают/агрегируют/выдают только совпавшие позиции. `COUNT(*) ... LIKE` и
агрегаты пушдаунятся на шарды без пересылки строк. Замер: **992ms → ~78ms
(p50)**.

Реализовано (Tier 1, пункт 3): `GROUP BY <col>` по одной колонке больше не
строит per-row ключ (быстрый `EncodePK` с `bytes.Buffer` на строку) и не
материализует массивы ячеек. `storage.ColumnGroupedAggregates` хеширует сырые
байты ячейки группы прямо в скане (`maphash`, `map[uint64]*grp` с коллизионным
fallback по `bytes.Equal`), значение группы декодируется только для новой
группы, а агрегатные колонки аккумулируются в ранговом порядке за один скан
на колонку (SUM+COUNT одной колонки делят один скан). Аккумуляторы — битмаска
без per-row map-lookup. Плюс zero-copy-скан (`kv.RangeNoCopy`) и замена
per-key `SeekGE` на последовательный `Next()` в latest-revision-пути `kv.Range`
— это ускорило все columnar-чтения. Замер: **474ms → ~143ms (p50)** на том же
workload; parity сохраняется (info 115 284 / warn 47 871 / error 28 845).

Реализовано (Tier 2): `COUNT(*)`, `COUNT/SUM/MIN/MAX/AVG(col)` и их варианты с
предикатом больше не строят per-row reader/аккумулятор. `ColumnAggregates` /
`ColumnAggregatesFiltered` декодируют числовые колонки напрямую из сырых байтов
ячейки (`decodeNumericCell` для tInt/tFloat/tTimestamp: `[type][null][zigzag-
varint]` без `makeReader`), аккумулируя в inline-переменных по битмаске флагов;
`COUNT(*)` сканирует только PK (значение колонки не читается, zero-copy).
Замер: **95–160ms → ~42–52ms (p50)**; `LIKE`-агрегаты тоже ускорились (~78ms).

Реализовано (вторичные индексы): `CREATE INDEX [IF NOT EXISTS] name ON t (col)`
и `DROP INDEX [IF EXISTS] name [ON t]` (одна колонка; композитные пока
отклоняются). Индекс — производная структура: определение персистится в
схеме таблицы, а записи (encoded-значение → pks) живут в памяти, перестраиваются
из строк при открытии/восстановлении снапшота и поддерживаются инкрементально
каждой DML-операцией (insert/update/delete/overwrite — идемпотентный
remove-then-add, безопасный при raft-replay). На шарде DDL транслируется в
raft-группу шарда (`ddlCreateIndex`/`ddlDropIndex`), поэтому каждый шард строит
свой локальный индекс. `=`-предикат по индексной колонке — один map-hit;
leading-literal `LIKE` — префиксный скан по немногим distinct-значениям;
результат точечно читает ячейки колонок. Null не индексируется. Замер
(10k строк, CSTORE): equality/LIKE `COUNT(*)` **~4.5ms → ~60µs (p50)**, аллокации
30k → 1k.

Реализовано (Tier 3): `BatchInsert(table, rows)` — многие строки одной таблицы
записываются в одной транзакции (один write-lock, один store-commit, общий
per-row индексный maintenance; в raft-FSM это превращает N `Put` в один проход).
`sql.Executor` нормализует все строки `INSERT` и вызывает batch-путь через
опциональный интерфейс `sqlx.BatchInsertEngine` (fallback на per-row Insert).
На engine-уровне batch100/1000 быстрее ~1.5× (0.68ms против 1.00ms; 6.3ms против
9.7ms на 1000 строк). Group-commit: окно и лимит батча настраиваются через
`YDBGO_BATCH_WINDOW_MS`/`YDBGO_BATCH_MAXOPS`/`YDBGO_BATCH_HARD_WINDOW_MS`;
адаптивное quiet-gap окно держит батч открытым, пока идут соседние записи;
одиночная запись (пустая очередь) применяется сразу, без искусственной задержки;
raft-apply больше не блокирует флаш-цикл (батчи пипелятся, кворум/fsync
соседних entry перекрываются). Замер на 5-узловом кластере RF=3, один шард,
conc=8, 200 строк/стейтмент: **~60k → ~66k rows/s**, p50 **~21ms** (клиент);
одиночный клиент — p50 ~5ms (это потолок raft-круга с fsync на 3 репликах;
`fileLogStore` синкается на каждый entry, сериализуя append).

Реализовано (Tier 4): добавлен `ADMIN COMPACT` — forced полная Pebble LSM-
компакция всех локальных стора шардов (`Engine.CompactLSM` → `db.Compact` по
фактическому диапазону ключей). Контролируемый замер на 5-узловом кластере RF=3
(CSTORE, 700k строк, данные на `/dev/shm`): после загрузки большой шард держит
40–53 коротких L0-SSTable (мемтебл переполнен), после `ADMIN COMPACT` — 17
(таблицы слиты, мемтебл спущен на диск). Чтения до/после (p50, 100 прогонов):

| Запрос | fresh-state (memtable+L0) | после COMPACT |
|--------|---------------------------|---------------|
| `COUNT(*)` | 170ms | 250–350ms |
| `SUM, AVG(lat)` | 217ms | 288–382ms |
| окно `[03:00,04:00)` | 22.6ms | 28–30ms |
| `GROUP BY level` | 733ms | 700–725ms |
| `level LIKE 'erro%'` | 321ms | 374–389ms |
| `ORDER BY ts DESC LIMIT 10` | 1.9ms | 1.8–1.9ms |

Вывод: компакция чтения **не ускоряет** (полные сканы даже чуть медленнее из-за
спуска мемтебла на диск) — чтения упираются в columnar-декод и scatter/gather
по шардам, а не в LSM-амплификацию. Табличные числа выше (п. 1–3) честные и не
зависят от свежести LSM. При масштабе README-ворклоада (192k строк, ~28MB на
шард) данные вообще живут в мемтебле (0–3 SSTable), так что "амплификация 2–3×"
там отсутствует.

Реализовано (P0, фикс write stall): корень stall — `FSM.Snapshot()` синхронно
сериализовал состояние (`MarshalState`) на raft-FSM-горутине; тяжёлая
сериализация блокировала `runFSM`, фолловеры не отвечали, лидер терял кворум,
raft-future висели и клиенты упирались в таймаут. Теперь `Snapshot()` только
пинит Pebble-снапшоты всех сторов (`CaptureSnapshot`, O(1) под `db.NewSnapshot`),
а сериализация ушла в `snapshot.Persist(sink)` на отдельной raft-горутине.
В `kv` добавлены типы `Snapshot`/`iterSource`, рефакторинг `rangeIter`;
`kvTx`/`cstoreTx` держат пин и читают через него; `rollback` освобождает пин.
Вотчдог `BATCH-STALL` в `internal/raftsvc/batch.go` оставлен как диагностический
предохранитель (спит в норме). Валидация: `TestEngineSnapshotPointInTime`/
`TestEngineSnapshotKVTable` (пин точен), 3 стресс-прогона `TestStressStallWriteLoad`
— **0 STALL** (было 4–24), полный `go test ./...` зелёный.

Реализовано (P1, vectorized скан): числовые колонки декодируются не per-row
reader'ом, а bulk-декодом сырых байт ячейки в плотные массивы
(`colDecodeNumeric` → `numVec` с int/float-массивами и nulls-битмаской);
агрегаты `COUNT/SUM/MIN/MAX/AVG` и `GROUP BY` сворачивают эти массивы
векторно (`aggNumVec`, `colAccum.addNum`), без per-row map/reader и per-cell
аллокаций. Счёт маркеров строк (`COUNT(*)` с диапазоном, окна) переведён на
`kv.RangeCount`: distinct-ключи считаются через снапшот-`prev` (пропуск
старых ревизий) и tombstone-фильтр, без материализации ключей. Замеры
(192k строк, in-process, `internal/storage/vec_bench_test.go`):
`SUM` int 146→~62–90ms, `SUM` float 135→~63–89ms (~1.6–2×), `GROUP BY`
162→~138ms при 384k→~450 allocs; `COUNT(*)` в окне через `RangeCount` ~40ms
(closure-сверка). Тесты kv/storage/sql/shard/raftsvc зелёные.

Реализовано (P2, счётчик `COUNT(*)`): у каждой CSTORE-таблицы в её engine-сторе
живёт счётчик живых строк (ключ с тегом `'n'`, 8 байт BE int64), обновляемый
атомарно с row-записями в том же `kv.Apply`. `rowPut`/`rowDelete` ведут точный
existence-счёт (перезапись существующего PK не инкрементит, `rowDeleteAll`/
`DROP TABLE` сбрасывает); дельты копятся в транзакции и на `commit` складываются
с коммиченным значением (изоляция overlay даёт read-your-writes и в груп-
коммитном батче). Счётчик пересобирается из строк при снапшот-restore, поэтому
raft-snapshot/`ReplaceState` не ломают его. `COUNT(*)` без WHERE читает счётчик
за O(1) (rangecount-скан был 52ms); `COUNT` с PK-окном по-прежнему использует
`kv.RangeCount`. Проверка: `TestCStoreLiveRowCounter` (insert/dup/delete/update
со сменой и без смены PK/batch/drop/recreate), полный `go test ./...` зелёный.

Статус каждого пункта фиксируется по мере реализации; замеры обновляются в
таблицах выше.