# ydbgo

Аналог распределённой SQL-базы данных в стиле YDB — один бинарник,
без внешних зависимостей (кроме `hashicorp/raft` и `go.etcd.io/bbolt`).

## Возможности

- **SQL**: `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, `INSERT`, `SELECT`
  (WHERE, JOIN-подобные фильтры, GROUP BY, агрегаты, ORDER BY, LIMIT,
  DISTINCT), `UPDATE`, `DELETE`, транзакции (`BEGIN/COMMIT/ROLLBACK`).
- **Хранилище**: движок на **bbolt** (embedded KV, B+tree, MVCC, ACID) —
  схема и строки (по sortable первичному ключу), дурабилити на fsync при
  коммите; **подключаемый** KV-бэкенд через интерфейс `store`/`storeTx`.
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
  батч) + **один bbolt-commit на батч** → один fsync на батч; постоянные
  **conn-пулы** между узлами вместо dial-на-запрос.
- **Свой протокол**: TCP + newline-delimited JSON, без gRPC-зависимостей.

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

## Запросы

Любой запрос можно направить на любой узел: не-лидер пересылает его
текущему лидеру соответствующей группы.

```bash
ydbgo run -addr 127.0.0.1:2135 "CREATE TABLE users (id int64 primary key, name string, age int64)"
ydbgo run -addr 127.0.0.1:2135 "INSERT INTO users VALUES (1,'Alice',25),(2,'Bob',30)"
ydbgo run -addr 127.0.0.1:2135 "SELECT * FROM users WHERE age >= 30 ORDER BY id"
ydbgo run -addr 127.0.0.1:2135 "SELECT name, COUNT(*) AS c FROM users GROUP BY name"
ydbgo run -addr 127.0.0.1:2136 "UPDATE users SET age = 31 WHERE name = 'Bob'"
```

Записи маршрутизируются к шарду по первичному ключу и проходят через лидера
группы этого шарда; чтения выполняются локально по всем шардам.

## `bench` и `ADMIN METRICS`

```bash
ydbgo bench -addr 127.0.0.1:2135 -n 10000 -rows 100 -c 8
```

Гонит конкурентный write-ворклуд (пул соединений), выводит **клиентский**
p50/p99 по латентности записи, rows/s и stmt/s, а затем серверный `ADMIN
METRICS`. Числа зависят от fsync-скорости диска; в конфиге с
групп-коммитом N конкурентных записей ≈ 1 raft fsync + 1 bbolt fsync на
батч.

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
| `ADMIN SPLIT TABLE <t> AT <pk>` | вручную разбить шард по значению PK |
| `ADMIN FREEZE-SHARD <t> <shard>` | заморозить шард (внутреннее при сплите) |
| `ADMIN UNFREEZE-SHARD <t> <shard>` | разморозить шард |
| `ADMIN UNMOUNT-SHARD <t> <shard>` | закрыть и удалить локальную реплику |
| `ADMIN SHARD-PEERS <t> <shard>` | члены группы данных (id, addr) |
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
internal/sql         — лексер, парсер, AST, исполнитель, агрегаты
internal/storage     — Engine поверх pluggable store (bbolt по умолчанию),
                       PK-кодирование, снапшоты (Marshal/Replace), батч-транзакции
internal/raftsvc     — generic Raft-группа, Node + Group + FSM, group-commit
internal/shard       — Manager, каталог (meta), роутинг, авто-сплит, recovery, метрики
internal/server      — TCP-сервер, протокол, клиент, CLI-обвязка
internal/proto       — типы запроса/ответа по проводу, ConnPool
```

### Движок (bbolt)

Строки лежат в bbolt: каждый шард/таблица — отдельный бакет, ключ — sortable
кодировка первичного ключа, значение — строка в порядке колонок. Схема — в
бакете `meta`. Чтения (`Scan`) идут курсором bbolt в порядке ключа.
Интерфейс `store`/`storeTx` (`internal/storage/store.go`) позволяет подставить
другой embedded KV без изменения верхних слоёв.

### Поток записи (одиночный узел)

`client → server → raft.Apply → FSM → storage(bbolt)`.

### Поток записи (шардированный)

`client → server → Manager.route → выбор шарда по PK → лидер группы шарда
→ raft.Apply → FSM → storage → репликация в группу` шарда. Каталог
(таблицы, шарды, размещение `Spec.Nodes`) реплицируется через отдельную
**мета-группу**.

### Group-commit и персистентность

1. Лидер шарда собирает конкурентные write-стейтменты в батч (окно ~1мс,
   до 4096 ops) и пишет **один** raft-entry.
2. `fileLogStore.StoreLogs` делает один `fsync` на батч — raft-лог дюрабилен
   до ответа клиенту.
3. `FSM.Apply` выполняет все стейтменты батча внутри **одной** bbolt-транзакции
   (`Engine.UpdateBatch`) → один bbolt `fsync` на батч.

Итого на батч: 1 raft fsync + 1 bbolt fsync (как в etcd), а не по два на
каждую запись. Снапшоты (`FSM.Snapshot` → `MarshalState`) и компакция
raft-лога (`DeleteRange`) не дают логу расти бесконечно; после снапшота
bbolt-файл переписывается компактным потоком состояний (`ReplaceState`).

Межнодовые вызовы идут через `proto.ConnPool` (idle-соединения на адрес,
re-dial после сбоя), так что путь записи не платит за TCP-хендшейк на каждый
запрос.

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

## Тесты

```bash
go test ./...
go test ./... -run TestShardedFiveNodeRecovery -count=5  # стабильность восстановления
go test ./internal/server -run TestBenchConcurrentWrites -v  # e2e бенчмарк + ADMIN METRICS
```

Покрытие: парсер/выражения, движок (bbolt, включая переживание перезапусков и
снапшот-раундтрип), интеграция SQL+storage, сервер+клиент по TCP, кластер
(2- и 5-узловой, RF=3, авто-сплит), переизбрание лидера при падении,
восстановление реплик после падения узла, конкурентные записи с p50/p99 и
`ADMIN METRICS`.