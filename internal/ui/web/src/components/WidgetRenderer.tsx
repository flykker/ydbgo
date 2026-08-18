import type {ChartData} from '@gravity-ui/charts';
import {Chart} from '@gravity-ui/charts';
import {Text} from '@gravity-ui/uikit';
import type {QueryResult, WidgetType} from '../api';
import {ResultTable} from './ResultTable';

const DATETIME_RE = /^\d{4}-\d{2}-\d{2}T/;

function num(v: string): number | null {
  if (v === '' || v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function isDateLike(v: string): boolean {
  return !!v && DATETIME_RE.test(v) && !Number.isNaN(Date.parse(v));
}

// buildXYSeries maps a query result onto x/y chart data. The first column is
// the x axis (datetime when values parse as timestamps, otherwise category);
// every numeric column after it becomes a series.
function buildXYSeries(result: QueryResult, type: 'line' | 'bar-y') {
  const cols = result.columns;
  const rows = result.rows;
  if (cols.length === 0 || rows.length === 0) {
    return {data: {series: {data: []}} as ChartData, empty: true};
  }
  const numericIdxs = cols
    .map((_, i) => (i > 0 && num(rows[0][i]) !== null ? i : -1))
    .filter((i) => i >= 0);
  if (numericIdxs.length === 0) {
    return {data: {series: {data: []}} as ChartData, empty: true};
  }
  const datetime = rows.some((r) => isDateLike(r[0]));
  const categories = datetime ? undefined : rows.map((r) => r[0]);
  const series = numericIdxs.map((ci) => ({
    name: cols[ci],
    data: rows.map((r, ri) => ({
      x: datetime ? Date.parse(r[0]) : ri,
      y: num(r[ci]) ?? undefined,
    })),
  }));
  const data: ChartData = {
    xAxis: datetime
      ? {type: 'datetime' as const}
      : {type: 'category' as const, categories},
    series: {
      data: series.map((s) => (type === 'line' ? {type: 'line', ...s} : {type: 'bar-y', ...s})),
    },
  };
  return {data, empty: false};
}

function buildPie(result: QueryResult): ChartData {
  const rows = result.rows.map((r) => ({name: r[0] ?? '', value: num(r[1]) ?? 0}));
  return {
    series: {data: [{type: 'pie' as const, data: rows}]},
  };
}

function buildHeatmap(result: QueryResult): ChartData {
  const xs = [...new Set(result.rows.map((r) => r[0]))];
  const ys = [...new Set(result.rows.map((r) => r[1] ?? ''))];
  return {
    xAxis: {type: 'category' as const, categories: xs},
    yAxis: [{type: 'category' as const, categories: ys}],
    series: {
      data: [
        {
          type: 'heatmap' as const,
          name: result.columns[2] ?? 'value',
          data: result.rows.map((r) => ({
            x: xs.indexOf(r[0]),
            y: r[1] ?? '',
            value: num(r[2]),
          })),
        },
      ],
    },
  };
}

// levelColor mirrors the log list styling for log_viewer widgets.
function levelColor(v: string): string | undefined {
  const lv = v.toUpperCase();
  if (lv === 'ERROR' || lv === 'FATAL' || lv === 'PANIC') return 'var(--g-color-text-danger)';
  if (lv === 'WARN' || lv === 'WARNING') return 'var(--g-color-text-warning)';
  return undefined;
}

// Gauge renders a compact radial 0-100% arc. Values outside the range are
// clamped for display (the SQL author should scale to percent).
function Gauge({label, value}: {label: string; value: number}) {
  const pct = Math.max(0, Math.min(100, value));
  const r = 40;
  const c = 2 * Math.PI * r;
  const dash = (pct / 100) * c;
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 8,
        height: '100%',
      }}
    >
      <svg width={120} height={90} viewBox="0 0 120 100">
        <circle cx={60} cy={60} r={r} fill="none" stroke="var(--g-color-line-generic)" strokeWidth={10} />
        <circle
          cx={60}
          cy={60}
          r={r}
          fill="none"
          stroke="var(--g-color-brand)"
          strokeWidth={10}
          strokeLinecap="round"
          strokeDasharray={`${dash} ${c}`}
          transform="rotate(-90 60 60)"
        />
        <text x={60} y={58} textAnchor="middle" fontSize={22} fontWeight={600} fill="var(--g-color-text-primary)">
          {value.toFixed(0)}
        </text>
        <text x={60} y={78} textAnchor="middle" fontSize={10} fill="var(--g-color-text-secondary)">
          %
        </text>
      </svg>
      <Text variant="caption-2" color="secondary">
        {label}
      </Text>
    </div>
  );
}

// WidgetRenderer visualizes a query result according to the widget type.
export function WidgetRenderer({type, title, result}: {type: WidgetType; title: string; result: QueryResult}) {
  if (result.columns.length === 0 || result.rows.length === 0) {
    return (
      <div style={{display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%'}}>
        <Text variant="caption-1" color="secondary">
          no rows
        </Text>
      </div>
    );
  }
  switch (type) {
    case 'line': {
      const {data, empty} = buildXYSeries(result, 'line');
      if (empty) return <NoNumeric />;
      return <Chart data={data} />;
    }
    case 'bar':
    case 'histogram': {
      const {data, empty} = buildXYSeries(result, 'bar-y');
      if (empty) return <NoNumeric />;
      return <Chart data={data} />;
    }
    case 'pie': {
      if (result.columns.length < 2) return <NoNumeric />;
      return <Chart data={buildPie(result)} />;
    }
    case 'heatmap': {
      if (result.columns.length < 3) return <NoNumeric />;
      return <Chart data={buildHeatmap(result)} />;
    }
    case 'table':
      return <ResultTable result={result} />;
    case 'log_viewer':
      return <LogViewer result={result} />;
    case 'stat':
    case 'gauge': {
      const row = result.rows[0];
      const idx = row.findIndex((_, i) => num(row[i]) !== null);
      if (idx < 0) return <NoNumeric />;
      const value = num(row[idx]) ?? 0;
      const label = result.columns[idx] ?? title;
      if (type === 'gauge') {
        return <Gauge label={label} value={value} />;
      }
      return (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 8,
            height: '100%',
          }}
        >
          <Text variant="display-3">{value.toLocaleString()}</Text>
          <Text variant="caption-2" color="secondary">
            {label}
          </Text>
        </div>
      );
    }
    default:
      return <ResultTable result={result} />;
  }
}

function NoNumeric() {
  return (
    <div style={{display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%'}}>
      <Text variant="caption-1" color="secondary">
        no numeric column found
      </Text>
    </div>
  );
}

// LogViewer renders log-like rows in a compact monospace table, coloring the
// level column when present.
function LogViewer({result}: {result: QueryResult}) {
  const levelIdx = result.columns.findIndex((c) => c.toLowerCase() === 'level');
  const cellStyle: React.CSSProperties = {
    padding: '2px 10px',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    borderBottom: '1px solid var(--g-color-line-generic-weak)',
    fontSize: 12,
    fontFamily: 'var(--g-text-body-code-font-family)',
  };
  return (
    <div style={{overflow: 'auto', height: '100%'}}>
      <table style={{borderCollapse: 'collapse', tableLayout: 'fixed', width: '100%'}}>
        <colgroup>
          {result.columns.map((c) => (
            <col key={c} style={{width: c.toLowerCase() === 'level' ? 70 : 'auto'}} />
          ))}
        </colgroup>
        <tbody>
          {result.rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, ci) => (
                <td
                  key={ci}
                  style={{
                    ...cellStyle,
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
  );
}
