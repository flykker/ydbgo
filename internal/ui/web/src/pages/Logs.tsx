import {useCallback, useEffect, useMemo, useState} from 'react';
import {Alert, Button, Select, Spin, Switch, Text, TextInput} from '@gravity-ui/uikit';
import {Chart} from '@gravity-ui/charts';
import {api, tail} from '../api';
import {exportCSV} from '../export';

const RANGES: {label: string; seconds: number; bucket: string}[] = [
  {label: '5m', seconds: 300, bucket: '10s'},
  {label: '15m', seconds: 900, bucket: '1m'},
  {label: '1h', seconds: 3600, bucket: '5m'},
  {label: '6h', seconds: 21600, bucket: '30m'},
  {label: '24h', seconds: 86400, bucket: '2h'},
];

const ROW_HEIGHT = 26;
const LIST_LIMIT = 500;

function isoAgo(seconds: number): string {
  return new Date(Date.now() - seconds * 1000).toISOString();
}

function escapeLike(q: string): string {
  return q.replace(/[%_]/g, '');
}

// findExplorerCols picks the time column and the string columns used for the
// text filter. Prefers a primary timestamp column for the time axis.
function findExplorerCols(columns: {name: string; type: string; primary: boolean}[]) {
  let time = columns.find((c) => c.primary && c.type === 'timestamp')?.name
    ?? columns.find((c) => c.type === 'timestamp')?.name
    ?? columns[0]?.name
    ?? '';
  const search = columns.filter((c) => c.name !== time && c.type === 'string').map((c) => c.name);
  if (search.length === 0) {
    time = columns[0]?.name ?? '';
  }
  return {time, search};
}

interface HistPoint {
  x: string;
  y: number;
}

interface LogState {
  columns: string[];
  rows: string[][];
}

function histSQL(table: string, time: string, where: string, bucket: string): string {
  return `SELECT time_bucket('${bucket}', ${time}) AS bucket, COUNT(*) AS n FROM ${table} WHERE ${where} GROUP BY time_bucket('${bucket}', ${time})`;
}

export function LogPage() {
  const [tables, setTables] = useState<{name: string; columns: {name: string; type: string; primary: boolean}[]}[]>([]);
  const [tableErr, setTableErr] = useState('');
  const [table, setTable] = useState('');
  const [range, setRange] = useState(RANGES[2]);
  const [query, setQuery] = useState('');
  const [state, setState] = useState<LogState | null>(null);
  const [hist, setHist] = useState<HistPoint[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [live, setLive] = useState(false);
  const [tailErr, setTailErr] = useState('');

  useEffect(() => {
    let alive = true;
    api
      .tables()
      .then((r) => {
        if (!alive) return;
        setTables(r.tables);
        if (r.tables.length > 0) {
          const preferred = r.tables.find((t) => /log/i.test(t.name)) ?? r.tables[0];
          setTable((cur) => cur || preferred.name);
        }
      })
      .catch((e) => alive && setTableErr(e instanceof Error ? e.message : String(e)));
    return () => {
      alive = false;
    };
  }, []);

  const {time, search} = useMemo(
    () => findExplorerCols(tables.find((t) => t.name === table)?.columns ?? []),
    [tables, table],
  );
  const searchCols = search.join('\u0000');

  const buildWhere = useCallback(
    (upperIso: string): string => {
      const conds: string[] = [`${time} >= '${isoAgo(range.seconds)}'`, `${time} < '${upperIso}'`];
      if (query && searchCols) {
        const like = escapeLike(query);
        const cols = searchCols.split('\u0000');
        conds.push(`(${cols.map((c) => `${c} LIKE '%${like}%'`).join(' OR ')})`);
      }
      return conds.join(' AND ');
    },
    [time, searchCols, range, query],
  );

  const refresh = useCallback(async () => {
    if (!table) return;
    setLoading(true);
    setError('');
    try {
      const whereClause = buildWhere(new Date().toISOString());
      const listSql = `SELECT * FROM ${table} WHERE ${whereClause} ORDER BY ${time} DESC LIMIT ${LIST_LIMIT}`;
      const histSql = histSQL(table, time, whereClause, range.bucket);
      const [list, h] = await Promise.all([api.query(listSql), api.query(histSql)]);
      setState({columns: list.result.columns, rows: list.result.rows});
      setHist((h.result.rows ?? []).map((r) => ({x: r[0], y: Number(r[1] ?? 0)})));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [table, time, range, buildWhere]);

  useEffect(() => {
    if (!table) return;
    void refresh();
  }, [refresh, table]);

  // Live tail: stream the list query over SSE and refresh the histogram on a
  // slower cadence while connected. The upper time bound is pinned an hour in
  // the future (a literal year-9999 breaks timestamp comparison), so freshly
  // ingested rows keep appearing in each frame.
  useEffect(() => {
    if (!live || !table) return;
    let histTick = 0;
    const whereClause = buildWhere(new Date(Date.now() + 3600 * 1000).toISOString());
    const listSql = `SELECT * FROM ${table} WHERE ${whereClause} ORDER BY ${time} DESC LIMIT ${LIST_LIMIT}`;
    const histSql = histSQL(table, time, whereClause, range.bucket);
    const close = tail(
      listSql,
      2,
      (frame) => {
        if (frame.ok && frame.result) {
          setState({columns: frame.result.columns, rows: frame.result.rows});
        }
        if (histTick++ % 3 === 0) {
          api
            .query(histSql)
            .then((h) => setHist((h.result.rows ?? []).map((r) => ({x: r[0], y: Number(r[1] ?? 0)}))))
            .catch(() => {});
        }
      },
      () => {
        setTailErr('Live tail disconnected');
        setLive(false);
      },
    );
    return close;
  }, [live, table, time, range, buildWhere]);

  const toggleLive = (checked: boolean) => {
    setTailErr('');
    setLive(checked);
  };

  const tableOptions = tables.map((t) => ({value: t.name, content: t.name}));
  const levelIdx = state?.columns.findIndex((c) => c.toLowerCase() === 'level') ?? -1;

  const chartData = {
    xAxis: {type: 'datetime' as const},
    series: {
      data: [
        {
          type: 'bar-y' as const,
          name: 'events',
          data: hist.map((p) => ({x: Date.parse(p.x), y: p.y})),
        },
      ],
    },
  };

  return (
    <div style={{display: 'flex', flexDirection: 'column', gap: 12, padding: 16}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap'}}>
        <Text variant="header-1">Logs</Text>
        <Select
          size="m"
          placeholder="Table"
          value={table ? [table] : []}
          options={tableOptions}
          onUpdate={(v) => v[0] && setTable(v[0])}
        />
        <div style={{display: 'flex', gap: 6}}>
          {RANGES.map((r) => (
            <Button
              key={r.label}
              size="m"
              view={r.label === range.label ? 'action' : 'normal'}
              onClick={() => setRange(r)}
            >
              {r.label}
            </Button>
          ))}
        </div>
        <div style={{width: 260}}>
          <TextInput size="m" placeholder="Search…" value={query} onUpdate={setQuery} />
        </div>
        <Button size="m" onClick={() => void refresh()} disabled={loading}>
          Refresh
        </Button>
        <Button
          size="m"
          view="outlined"
          disabled={!state || state.rows.length === 0}
          onClick={() =>
            state &&
            exportCSV({type: 'select', columns: state.columns, rows: state.rows, affected: 0, note: ''}, table + '.csv')
          }
        >
          Export CSV
        </Button>
        <Switch checked={live} onUpdate={toggleLive} content="Live tail" />
        {loading && <Spin size="m" />}
      </div>
      {(tableErr || error || tailErr) && (
        <Alert theme="danger" title="Logs" message={tableErr || error || tailErr} />
      )}
      {!table && tables.length === 0 && (
        <Alert theme="info" title="No tables" message="Create a logs table (e.g. ENGINE=CSTORE) to explore it here." />
      )}
      {hist.length > 0 && (
        <div style={{height: 140}}>
          <Chart data={chartData} />
        </div>
      )}
      {state && (
        <>
          <Text variant="body-1" color="secondary">
            {state.rows.length} latest rows of {table}
            {live ? ' · live' : ''}
          </Text>
          <VirtualLogs columns={state.columns} rows={state.rows} rowHeight={ROW_HEIGHT} levelIdx={levelIdx} />
        </>
      )}
    </div>
  );
}

// VirtualLogs renders only the visible slice of a tall log list.
function VirtualLogs({
  columns,
  rows,
  rowHeight,
  levelIdx,
}: {
  columns: string[];
  rows: string[][];
  rowHeight: number;
  levelIdx: number;
}) {
  const [scrollTop, setScrollTop] = useState(0);
  const height = Math.min(560, Math.max(240, rows.length * rowHeight));
  const view = Math.ceil(height / rowHeight) + 4;
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - 4);
  const slice = rows.slice(start, start + view);

  const levelColor = (v: string): string | undefined => {
    const lv = v.toUpperCase();
    if (lv === 'ERROR' || lv === 'FATAL' || lv === 'PANIC') return 'var(--g-color-text-danger)';
    if (lv === 'WARN' || lv === 'WARNING') return 'var(--g-color-text-warning)';
    return undefined;
  };

  return (
    <div
      style={{
        height,
        overflowY: 'auto',
        border: '1px solid var(--g-color-line-generic)',
        borderRadius: 8,
        background: 'var(--g-color-base-float)',
      }}
      onScroll={(e) => setScrollTop(e.currentTarget.scrollTop)}
    >
      <div style={{height: rows.length * rowHeight, position: 'relative'}}>
        <table
          style={{
            position: 'absolute',
            top: start * rowHeight,
            left: 0,
            right: 0,
            borderCollapse: 'collapse',
            tableLayout: 'fixed',
            width: '100%',
          }}
        >
          <colgroup>
            {columns.map((c) => (
              <col key={c} style={{width: c.toLowerCase() === 'level' ? 80 : 'auto'}} />
            ))}
          </colgroup>
          <tbody>
            {slice.map((row, i) => (
              <tr key={start + i}>
                {row.map((cell, ci) => (
                  <td
                    key={ci}
                    style={{
                      padding: '2px 10px',
                      height: rowHeight,
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      borderBottom: '1px solid var(--g-color-line-generic-weak)',
                      fontSize: 12,
                      fontFamily: 'var(--g-text-body-code-font-family)',
                      color: ci === levelIdx ? levelColor(cell) ?? 'var(--g-color-text-primary)' : 'var(--g-color-text-primary)',
                    }}
                  >
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
