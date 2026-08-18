import {useCallback, useEffect, useRef, useState} from 'react';
import {Alert, Button, Dialog, Spin, Text, TextInput} from '@gravity-ui/uikit';
import {api, type NodeInfo, type NodeMetrics, type QueryResult, type ShardInfo, type TableInfo} from '../api';
import {MetricsCharts} from './MetricsCharts';

function fmtBytes(n: number): string {
  if (n >= 1 << 30) return (n / (1 << 30)).toFixed(2) + ' GiB';
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + ' MiB';
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(0) + ' KiB';
  return String(n) + ' B';
}

function placeholderFor(type: string): string {
  switch (type) {
    case 'int64':
    case 'int':
      return 'e.g. 42';
    case 'float64':
    case 'float':
      return 'e.g. 3.14';
    case 'bool':
      return 'true / false';
    case 'timestamp':
      return 'e.g. 2026-08-17T12:00:00Z';
    default:
      return 'e.g. foo';
  }
}

function useAsync<T>(fn: () => Promise<T>, deps: unknown[]): {data: T | null; loading: boolean; error: string; reload: () => void} {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError('');
    fn()
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(e instanceof Error ? e.message : String(e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);
  const reload = useCallback(() => setTick((t) => t + 1), []);
  return {data, loading, error, reload};
}

// SplitField describes one primary-key column the Split dialog asks for.
interface SplitField {
  name: string;
  type: string;
}

// AdminConfirm describes a pending destructive admin action confirmed in a
// dialog; inputs turns on a PK-value field per primary-key column for
// ADMIN SPLIT.
interface AdminConfirm {
  title: string;
  text: string;
  confirmLabel?: string;
  inputs?: SplitField[];
  onOk: () => Promise<void>;
}

// AdminResultDialog shows the output (rows or note) of a finished ADMIN call.
function AdminResultDialog({cmd, result, onClose}: {cmd: string; result: QueryResult; onClose: () => void}) {
  return (
    <Dialog open={true} onClose={onClose} size="l">
      <Dialog.Header caption="Admin result" />
      <Dialog.Body>
        <div style={{display: 'flex', flexDirection: 'column', gap: 12}}>
          <Text variant="body-1" color="secondary">
            <code style={{fontFamily: 'var(--g-text-body-code-font-family)'}}>{cmd}</code>
          </Text>
          {result.note && (
            <pre style={preStyle}>
              <code>{result.note}</code>
            </pre>
          )}
          {result.rows.length > 0 && (
            <table style={{borderCollapse: 'collapse', width: '100%'}}>
              <thead>
                <tr>
                  {result.columns.map((c) => (
                    <th key={c} style={thStyle}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((r, i) => (
                  <tr key={i}>
                    {r.map((v, j) => (
                      <td key={j} style={tdStyle}>{v}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {result.type === 'error' && <Alert theme="danger" title="Command failed" message={result.note} />}
        </div>
      </Dialog.Body>
      <Dialog.Footer
        onClickButtonApply={onClose}
        textButtonApply="Close"
        renderButtons={(apply) => <>{apply}</>}
      />
    </Dialog>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '8px 12px',
  borderBottom: '1px solid var(--g-color-line-generic)',
  fontSize: 12,
  fontWeight: 600,
  whiteSpace: 'nowrap',
};

const tdStyle: React.CSSProperties = {
  padding: '6px 12px',
  borderBottom: '1px solid var(--g-color-line-generic)',
  fontSize: 13,
  whiteSpace: 'nowrap',
  fontFamily: 'var(--g-text-body-code-font-family)',
};

const preStyle: React.CSSProperties = {
  margin: 0,
  padding: 12,
  borderRadius: 8,
  background: 'var(--g-color-base-float)',
  fontFamily: 'var(--g-text-body-code-font-family)',
  fontSize: 13,
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
};

export function ClusterPage() {
  const tables = useAsync(() => api.tables().then((r) => r.tables), []);
  const nodes = useAsync(() => api.nodes().then((r) => r.nodes), []);
  const metrics = useAsync(() => api.metrics().then((r) => r.metrics), []);
  const [selected, setSelected] = useState('');
  const shards = useAsync(
    () => (selected ? api.shards(selected).then((r) => r.shards) : Promise.resolve([] as ShardInfo[])),
    [selected],
  );

  const [confirm, setConfirm] = useState<AdminConfirm | null>(null);
  const [inputs, setInputs] = useState<string[]>([]);
  const inputRefs = useRef<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{cmd: string; result: QueryResult} | null>(null);

  const updateInput = useCallback((i: number, v: string) => {
    inputRefs.current[i] = v;
    setInputs((cur) => cur.map((x, j) => (j === i ? v : x)));
  }, []);

  // runAdmin executes an ADMIN command via the HTTP API and shows its result.
  const runAdmin = useCallback(
    async (cmd: string) => {
      setBusy(true);
      setConfirm(null);
      try {
        const resp = await api.admin(cmd);
        setResult({
          cmd,
          result: {
            type: resp.result?.type ?? 'ok',
            columns: resp.result?.columns ?? [],
            rows: resp.result?.rows ?? [],
            affected: resp.result?.affected ?? 0,
            note: resp.result?.note ?? '',
          },
        });
      } catch (e) {
        setResult({
          cmd,
          result: {
            type: 'error',
            columns: [],
            rows: [],
            affected: 0,
            note: e instanceof Error ? e.message : String(e),
          },
        });
      } finally {
        setBusy(false);
        tables.reload();
        if (selected) shards.reload();
      }
    },
    [selected, shards.reload, tables.reload],
  );

  const openConfirm = (c: AdminConfirm) => {
    inputRefs.current = c.inputs ? c.inputs.map(() => '') : [];
    setInputs(inputRefs.current);
    setConfirm(c);
  };

  // buildSplitSql renders an ADMIN SPLIT command from per-PK-column input values.
  const buildSplitSql = (table: string, fields: SplitField[], values: string[]): string => {
    const parts = fields.map((f, i) => {
      const v = (values[i] ?? '').trim();
      if (f.type === 'string' || f.type === 'timestamp') return `'${v.replace(/'/g, "''")}'`;
      return v;
    });
    const at = parts.length === 1 ? parts[0] : `(${parts.join(', ')})`;
    return `ADMIN SPLIT TABLE ${table} AT ${at}`;
  };

  const pkFieldsFor = (table: string): SplitField[] => {
    const t = tables.data?.find((x) => x.name === table);
    if (!t) return [];
    return t.columns.filter((c) => c.primary).map((c) => ({name: c.name, type: c.type}));
  };

  const loading = tables.loading || nodes.loading || metrics.loading;
  return (
    <div style={{display: 'flex', flexDirection: 'column', gap: 16, padding: 16}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 12}}>
        <Text variant="header-1">Cluster</Text>
        <Button size="m" onClick={() => {
          tables.reload();
          nodes.reload();
          metrics.reload();
        }}>
          Refresh
        </Button>
        <Button
          size="m"
          view="outlined-danger"
          onClick={() =>
            openConfirm({
              title: 'Compact stores',
              text: 'Force a full LSM compaction over every local shard store. This is safe but can take a while on large data.',
              confirmLabel: 'Compact',
              onOk: () => runAdmin('ADMIN COMPACT'),
            })
          }
        >
          Compact
        </Button>
        {loading && <Spin size="m" />}
      </div>
      {tables.error && <Alert theme="danger" title="Tables" message={tables.error} />}
      {nodes.error && <Alert theme="danger" title="Nodes" message={nodes.error} />}
      <Text variant="header-2">Nodes</Text>
      {nodes.data && <NodesGrid nodes={nodes.data} metrics={metrics.data ?? []} />}
      <Text variant="header-2">Live metrics</Text>
      <MetricsCharts />
      <Text variant="header-2">Tables</Text>
      {tables.data && (
        <TableGrid
          tables={tables.data}
          onSelect={setSelected}
          onSplit={(t) =>
            openConfirm({
              title: `Split ${t}`,
              text: 'Enter a primary-key value for each PK column. The shard containing that key is split into two.',
              confirmLabel: 'Split',
              inputs: pkFieldsFor(t),
              onOk: () => runAdmin(buildSplitSql(t, pkFieldsFor(t), inputRefs.current)),
            })
          }
        />
      )}
      {selected && (
        <>
          <Text variant="header-2">Shards of {selected}</Text>
          {shards.data && (
            <ShardGrid
              shards={shards.data}
              onSplit={(s) =>
                openConfirm({
                  title: `Split shard ${s.id}`,
                  text: 'Enter a primary-key value for each PK column inside this shard. It is split at that key.',
                  confirmLabel: 'Split',
                  inputs: pkFieldsFor(selected),
                  onOk: () => runAdmin(buildSplitSql(selected, pkFieldsFor(selected), inputRefs.current)),
                })
              }
              onFreeze={(s) =>
                openConfirm({
                  title: `Freeze shard ${s.id}`,
                  text: `Freeze shard ${s.id} of ${selected}? It stops receiving writes (internal step before a split).`,
                  confirmLabel: 'Freeze',
                  onOk: () => runAdmin(`ADMIN FREEZE-SHARD ${selected} ${s.id}`),
                })
              }
              onUnfreeze={(s) => () => runAdmin(`ADMIN UNFREEZE-SHARD ${selected} ${s.id}`)}
              onPeers={(s) => () => runAdmin(`ADMIN SHARD-PEERS ${selected} ${s.id}`)}
            />
          )}
        </>
      )}

      {confirm && (
        <Dialog open={true} onClose={() => !busy && setConfirm(null)} size="m">
          <Dialog.Header caption={confirm.title} />
          <Dialog.Body>
            <div style={{display: 'flex', flexDirection: 'column', gap: 12}}>
              <Text variant="body-1">{confirm.text}</Text>
              {confirm.inputs && (
                <div style={{display: 'flex', flexDirection: 'column', gap: 8}}>
                  {confirm.inputs.map((f, i) => (
                    <TextInput
                      key={f.name}
                      label={f.name}
                      placeholder={placeholderFor(f.type)}
                      value={inputs[i] ?? ''}
                      onUpdate={(v) => updateInput(i, v)}
                      qa={`split-input-${f.name}`}
                    />
                  ))}
                </div>
              )}
            </div>
          </Dialog.Body>
          <Dialog.Footer
            onClickButtonApply={() => void confirm.onOk()}
            onClickButtonCancel={() => setConfirm(null)}
            textButtonApply={confirm.confirmLabel ?? 'Confirm'}
            textButtonCancel="Cancel"
            loading={busy}
            propsButtonApply={{
              disabled: confirm.inputs ? confirm.inputs.some((_, i) => !(inputs[i] ?? '').trim()) : false,
            }}
          />
        </Dialog>
      )}

      {result && <AdminResultDialog cmd={result.cmd} result={result.result} onClose={() => setResult(null)} />}
    </div>
  );
}

function TableGrid({tables, onSelect, onSplit}: {tables: TableInfo[]; onSelect: (t: string) => void; onSplit: (t: string) => void}) {
  return (
    <table style={{borderCollapse: 'collapse', width: '100%'}}>
      <thead>
        <tr>
          {['Table', 'Engine', 'Shards', 'Size', 'Retention', 'Actions'].map((h) => (
            <th key={h} style={thStyle}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {tables.map((t) => (
          <tr key={t.name}>
            <td style={tdStyle}>
              <Button view="flat" size="s" onClick={() => onSelect(t.name)}>{t.name}</Button>
            </td>
            <td style={tdStyle}>{t.engine}</td>
            <td style={tdStyle}>{t.shards}</td>
            <td style={tdStyle}>{fmtBytes(t.size)}</td>
            <td style={tdStyle}>{t.retention || '—'}</td>
            <td style={tdStyle}>
              <Button size="s" view="outlined" onClick={() => onSplit(t.name)}>Split</Button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ShardGrid({shards, onSplit, onFreeze, onUnfreeze, onPeers}: {
  shards: ShardInfo[];
  onSplit: (s: ShardInfo) => void;
  onFreeze: (s: ShardInfo) => void;
  onUnfreeze: (s: ShardInfo) => () => Promise<void>;
  onPeers: (s: ShardInfo) => () => Promise<void>;
}) {
  return (
    <table style={{borderCollapse: 'collapse', width: '100%'}}>
      <thead>
        <tr>
          {['Shard', 'Range', 'Nodes', 'Size', 'Actions'].map((h) => (
            <th key={h} style={thStyle}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {shards.map((s) => (
          <tr key={s.id}>
            <td style={tdStyle}>{s.id}</td>
            <td style={tdStyle}>
              [{s.start || '−'} … {s.end || '∞'})
            </td>
            <td style={tdStyle}>{s.nodes.join(', ')}</td>
            <td style={tdStyle}>{fmtBytes(s.size)}</td>
            <td style={tdStyle}>
              <span style={{display: 'inline-flex', gap: 6, alignItems: 'center'}}>
                <Button size="s" view="outlined" onClick={() => onSplit(s)}>Split</Button>
                <Button size="s" view="outlined" onClick={() => onFreeze(s)}>Freeze</Button>
                <Button size="s" view="flat" onClick={onUnfreeze(s)}>Unfreeze</Button>
                <Button size="s" view="flat" onClick={onPeers(s)}>Peers</Button>
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function NodesGrid({nodes, metrics}: {nodes: NodeInfo[]; metrics: NodeMetrics[]}) {
  const byId = new Map(metrics.map((m) => [m.node, m]));
  return (
    <table style={{borderCollapse: 'collapse', width: '100%'}}>
      <thead>
        <tr>
          {['Node', 'SQL', 'Raft', 'Status'].map((h) => (
            <th key={h} style={thStyle}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {nodes.map((n) => {
          const m = byId.get(n.id);
          return (
            <tr key={n.id}>
              <td style={tdStyle}>{n.id}</td>
              <td style={tdStyle}>{n.sql_addr}</td>
              <td style={tdStyle}>{n.raft_addr}</td>
              <td style={tdStyle}>
                <Text color={m && m.status === 'up' ? 'positive' : 'danger'}>{m?.status ?? 'unknown'}</Text>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
