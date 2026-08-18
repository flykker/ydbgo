import {useEffect, useRef, useState} from 'react';
import {Button, Spin, Text} from '@gravity-ui/uikit';
import {Chart} from '@gravity-ui/charts';
import type {ChartData, LineSeries} from '@gravity-ui/charts';
import {api, type NodeMetrics} from '../api';

const POLL_MS = 3000;
const KEEP = 60; // rolling window: ~3 minutes at POLL_MS

// Keep in sync with internal/ui/ui.go promMetric + ADMIN METRICS-JSON payload.
interface ParsedMetrics {
  reads?: number;
  writes?: number;
  read_latency_ms?: {p50?: number; p99?: number};
  write_latency_ms?: {p50?: number; p99?: number};
}

interface Sample {
  t: number;
  reads: number;
  writes: number;
  readP50: number;
  readP99: number;
  writeP50: number;
  writeP99: number;
}

const PALETTE = [
  '#4DA2F1', '#FF3D64', '#8AD554', '#FFC636', '#FFB9DD', '#84D1EE', '#FF91A1',
  '#54A520', '#DB9100', '#BA74B3', '#1F68A9', '#ED65A9', '#0FA08D', '#FF7E00',
];

// deltas converts a cumulative counter into per-second rates between samples.
function deltas(samples: Sample[], pick: (s: Sample) => number): {x: number; y: number}[] {
  const out: {x: number; y: number}[] = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (samples[i].t - samples[i - 1].t) / 1000;
    if (dt <= 0) continue;
    const d = pick(samples[i]) - pick(samples[i - 1]);
    out.push({x: samples[i].t, y: d < 0 ? 0 : d / dt});
  }
  return out;
}

function values(samples: Sample[], pick: (s: Sample) => number): {x: number; y: number}[] {
  return samples.map((s) => ({x: s.t, y: pick(s)}));
}

function chartData(title: string, unit: string, series: LineSeries[]): ChartData {
  return {
    title: {text: title, maxRowCount: 1},
    colors: PALETTE,
    series: {data: series},
    xAxis: {type: 'datetime', labels: {dateFormat: 'HH:mm:ss'}},
    yAxis: [{type: 'linear', title: {text: unit}}],
    legend: {enabled: true},
  };
}

export function MetricsCharts() {
  const [hist, setHist] = useState<Map<string, Sample[]>>(new Map());
  const [statuses, setStatuses] = useState<Map<string, string>>(new Map());
  const histRef = useRef(new Map<string, Sample[]>());
  const [selected, setSelected] = useState(''); // '' = all nodes

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      let list: NodeMetrics[];
      try {
        list = (await api.metrics()).metrics;
      } catch {
        return; // keep the last samples on a transient error
      }
      const now = Date.now();
      const st = new Map<string, string>();
      for (const nm of list) st.set(nm.node, nm.status);
      for (const nm of list) {
        let p: ParsedMetrics | null = null;
        try {
          p = nm.json ? (JSON.parse(nm.json) as ParsedMetrics) : null;
        } catch {
          p = null;
        }
        if (!p) continue;
        const s: Sample = {
          t: now,
          reads: p.reads ?? 0,
          writes: p.writes ?? 0,
          readP50: p.read_latency_ms?.p50 ?? 0,
          readP99: p.read_latency_ms?.p99 ?? 0,
          writeP50: p.write_latency_ms?.p50 ?? 0,
          writeP99: p.write_latency_ms?.p99 ?? 0,
        };
        const arr = histRef.current.get(nm.node) ?? [];
        arr.push(s);
        if (arr.length > KEEP) arr.shift();
        histRef.current.set(nm.node, arr);
      }
      if (alive) {
        setHist(new Map(histRef.current));
        setStatuses(st);
      }
    };
    void tick();
    const id = setInterval(() => void tick(), POLL_MS);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const nodeIds = Array.from(statuses.keys()).sort();
  const shown = selected === '' ? nodeIds : [selected];

  const nodeColor = (node: string) => PALETTE[Math.max(0, nodeIds.indexOf(node)) % PALETTE.length];

  const rateSeries = (pick: (s: Sample) => number): LineSeries[] =>
    shown
      .filter((n) => hist.has(n))
      .map((n) => ({type: 'line', name: n, color: nodeColor(n), data: deltas(hist.get(n)!, pick)}));

  const latencySeries = (pick: (s: Sample) => number, quantile: 'p50' | 'p99'): LineSeries[] =>
    shown
      .filter((n) => hist.has(n))
      .map((n) => ({
        type: 'line',
        name: `${n} ${quantile}`,
        color: nodeColor(n),
        dashStyle: quantile === 'p99' ? 'Dash' : 'Solid',
        data: values(hist.get(n)!, pick),
      }));

  // @gravity-ui/charts rejects series with empty data (throws NO_DATA/INVALID_DATA),
  // so only series with at least one point are passed and the chart renders only
  // when every series is non-empty.
  const withData = (series: LineSeries[]): LineSeries[] => series.filter((s) => s.data.length > 0);
  const nonEmpty = (series: LineSeries[]) => withData(series).length > 0;

  const charts: {title: string; unit: string; data: ChartData; qa: string; hasData: boolean}[] = [
    {
      title: 'Write requests/s', unit: 'req/s', qa: 'chart-write-rps', hasData: nonEmpty(rateSeries((s) => s.writes)),
      data: chartData('Write requests/s', 'req/s', withData(rateSeries((s) => s.writes))),
    },
    {
      title: 'Read requests/s', unit: 'req/s', qa: 'chart-read-rps', hasData: nonEmpty(rateSeries((s) => s.reads)),
      data: chartData('Read requests/s', 'req/s', withData(rateSeries((s) => s.reads))),
    },
    {
      title: 'Write latency, ms', unit: 'ms', qa: 'chart-write-lat',
      hasData: nonEmpty(latencySeries((s) => s.writeP50, 'p50').concat(latencySeries((s) => s.writeP99, 'p99'))),
      data: chartData('Write latency, ms', 'ms', withData(latencySeries((s) => s.writeP50, 'p50').concat(latencySeries((s) => s.writeP99, 'p99')))),
    },
    {
      title: 'Read latency, ms', unit: 'ms', qa: 'chart-read-lat',
      hasData: nonEmpty(latencySeries((s) => s.readP50, 'p50').concat(latencySeries((s) => s.readP99, 'p99'))),
      data: chartData('Read latency, ms', 'ms', withData(latencySeries((s) => s.readP50, 'p50').concat(latencySeries((s) => s.readP99, 'p99')))),
    },
  ];

  if (hist.size === 0 && nodeIds.length === 0) {
    return <Spin size="m" />;
  }

  return (
    <div style={{display: 'flex', flexDirection: 'column', gap: 12}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap'}}>
        <Text variant="body-1" color="secondary">Nodes:</Text>
        <Button size="s" view={selected === '' ? 'normal' : 'flat'} qa="metrics-node-all" onClick={() => setSelected('')}>
          All
        </Button>
        {nodeIds.map((n) => {
          const up = statuses.get(n) === 'up';
          return (
            <Button
              key={n}
              size="s"
              view={selected === n ? 'normal' : 'flat'}
              qa={`metrics-node-${n}`}
              onClick={() => setSelected(selected === n ? '' : n)}
            >
              {n}
              <span style={{marginLeft: 6, color: up ? 'var(--g-color-text-positive)' : 'var(--g-color-text-danger)'}}>
                {up ? 'up' : 'down'}
              </span>
            </Button>
          );
        })}
      </div>
      <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(360px, 1fr))', gap: 16}}>
        {charts.map((c) => (
          <div
            key={c.qa}
            data-qa={c.qa}
            style={{
              border: '1px solid var(--g-color-line-generic)',
              borderRadius: 8,
              padding: 8,
              height: 240,
            }}
          >
            {c.hasData ? (
              <Chart data={c.data} />
            ) : (
              <Text variant="body-1" color="secondary">no data</Text>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
