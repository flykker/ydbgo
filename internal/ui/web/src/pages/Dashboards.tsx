import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {Alert, Button, Dialog, Select, Spin, Switch, Text, TextInput} from '@gravity-ui/uikit';
import {Pencil, Plus, TrashBin} from '@gravity-ui/icons';
import GridLayout, {type Layout, type LayoutItem} from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import {api, parseDashboardConfig, type Dashboard, type DashboardConfig, type QueryResult, type WidgetConfig, type WidgetType} from '../api';
import {parseDashId, setDashHash} from '../hash';
import {WidgetRenderer} from '../components/WidgetRenderer';

const COLS = 12;
const ROW_H = 30;

// y for a freshly created widget: RGL's vertical compactor moves it up to the
// first free row at render, and a subsequent drag/resize persists the real
// position. A finite value is required because Infinity serializes to JSON
// null and corrupts the stored config.
const NEW_WIDGET_Y = 1000000;

const WIDGET_TYPES: {value: WidgetType; content: string}[] = [
  {value: 'line', content: 'line'},
  {value: 'bar', content: 'bar'},
  {value: 'pie', content: 'pie'},
  {value: 'stat', content: 'stat'},
  {value: 'gauge', content: 'gauge'},
  {value: 'histogram', content: 'histogram'},
  {value: 'heatmap', content: 'heatmap'},
  {value: 'table', content: 'table'},
  {value: 'log_viewer', content: 'log_viewer'},
];

interface EditorState {
  index: number; // -1 = create new
  type: WidgetType;
  title: string;
  sql: string;
  w: number;
  h: number;
}

function emptyConfig(name: string): DashboardConfig {
  return {title: name, refresh_interval: 30, widgets: []};
}

let uid = 0;
function newWidgetId(): string {
  uid += 1;
  return 'w' + Date.now().toString(36) + uid;
}

// useMeasuredWidth reports the width of a container. getBoundingClientRect
// forces synchronous layout, so the first measure is reliable even before
// paint; ResizeObserver + window resize keep it current. The effect re-runs
// when `active` flips true, because the container is conditionally rendered
// after async data loads (a mount-time measure would see a null ref).
function useMeasuredWidth<T extends HTMLElement>(active: boolean) {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!active) return;
    const el = ref.current;
    if (!el) return;
    const update = () => {
      const w = Math.round(el.getBoundingClientRect().width);
      setWidth((prev) => (w > 0 ? w : prev));
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    window.addEventListener('resize', update);
    return () => {
      ro.disconnect();
      window.removeEventListener('resize', update);
    };
  }, [active]);
  return {ref, width};
}

export function DashboardPage() {
  const [dashboards, setDashboards] = useState<Dashboard[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [name, setName] = useState('');
  const [creating, setCreating] = useState(false);

  const [activeId, setActiveId] = useState<string | null>(null);
  const [config, setConfig] = useState<DashboardConfig | null>(null);
  const [results, setResults] = useState<Record<string, QueryResult | null>>({});
  const [widgetErrors, setWidgetErrors] = useState<Record<string, string>>({});
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [editName, setEditName] = useState('');
  const [refreshInterval, setRefreshInterval] = useState(30);
  const [refreshOn, setRefreshOn] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  const load = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const r = await api.dashboards();
      setDashboards(r.dashboards);
      if (r.dashboards.length === 0) {
        setActiveId(null);
        if (parseDashId()) setDashHash(null);
        return;
      }
      // Prefer the dashboard referenced by the URL hash, otherwise keep the
      // current selection, otherwise fall back to the first dashboard.
      setActiveId((cur) => {
        if (cur && r.dashboards.some((d) => d.id === cur)) return cur;
        const fromHash = parseDashId();
        return fromHash && r.dashboards.some((d) => d.id === fromHash) ? fromHash : r.dashboards[0].id;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Keep the active dashboard id in sync with the URL hash (#/dashboards/<id>):
  // selecting, creating or deleting a dashboard updates the hash automatically.
  // While activeId is null (page not loaded yet) the hash is left untouched so
  // a deep link like #/dashboards/<id> survives until load() resolves it.
  useEffect(() => {
    if (activeId != null && parseDashId() !== activeId) setDashHash(activeId);
  }, [activeId]);

  // React to hash changes from outside (browser back/forward, manual edits).
  // If the target id is not in the locally cached list (e.g. the dashboard was
  // created after this page loaded, or in another tab), refresh the list so the
  // config-load effect can resolve it; unknown ids then fall back to the first.
  const dashboardsRef = useRef<Dashboard[] | null>(null);
  dashboardsRef.current = dashboards;
  useEffect(() => {
    const onHash = () => {
      const id = parseDashId();
      if (!id) return;
      setActiveId((cur) => (cur === id ? cur : id));
      if (!dashboardsRef.current?.some((d) => d.id === id)) void load();
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, [load]);

  // Load the active dashboard config whenever the id changes.
  useEffect(() => {
    if (!activeId || !dashboards) return;
    const d = dashboards.find((x) => x.id === activeId);
    if (!d) return;
    const cfg = parseDashboardConfig(d.config);
    setConfig(cfg);
    setEditName(d.name);
    setRefreshInterval(cfg.refresh_interval);
  }, [activeId, dashboards]);

  const widgetList = useMemo(() => config?.widgets ?? [], [config]);

  // Run all widget queries (5s server-side cache per SQL).
  const runWidgets = useCallback(async (cfg: DashboardConfig) => {
    const fresh: Record<string, QueryResult | null> = {};
    const errs: Record<string, string> = {};
    await Promise.all(
      cfg.widgets.map(async (w) => {
        try {
          const r = await api.widgetQuery(w.sql, 5);
          fresh[w.id] = r.result;
        } catch (e) {
          errs[w.id] = e instanceof Error ? e.message : String(e);
        }
      }),
    );
    setResults(fresh);
    setWidgetErrors(errs);
  }, []);

  // Refetch only when the widget *set* changes (id/type/sql), not on pure
  // layout moves/resizes — otherwise every drag-stop re-runs all queries.
  const lastFpRef = useRef('');
  useEffect(() => {
    if (!config) return;
    const fp = config.widgets.map((w) => `${w.id}:${w.type}:${w.sql}`).join('|');
    if (fp === lastFpRef.current) return;
    lastFpRef.current = fp;
    void runWidgets(config);
  }, [config, runWidgets]);

  // Auto-refresh loop.
  useEffect(() => {
    if (!refreshOn || !activeId) return;
    timer.current = window.setInterval(() => {
      if (config) void runWidgets(config);
    }, refreshInterval * 1000);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
  }, [refreshOn, activeId, refreshInterval, config, runWidgets]);

  const saveConfig = useCallback(
    async (next: DashboardConfig) => {
      if (!activeId) return;
      try {
        await api.dashboardUpdate(activeId, editName, next);
        setConfig(next);
        await load();
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [activeId, editName, load],
  );

  const create = async () => {
    if (!name.trim()) return;
    setCreating(true);
    setError('');
    try {
      const r = await api.dashboardCreate(name.trim(), emptyConfig(name.trim()));
      setActiveId(r.id);
      setName('');
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  };

  const remove = async (id: string) => {
    try {
      await api.dashboardDelete(id);
      setActiveId((cur) => (cur === id ? null : cur));
      setConfig(null);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const addWidget = () =>
    setEditor({index: -1, type: 'stat', title: '', sql: 'SELECT COUNT(*) AS total FROM logs WHERE level = \'ERROR\'', w: 3, h: 4});

  const openEditor = (index: number) => {
    const w = widgetList[index];
    setEditor({index, type: w.type, title: w.title, sql: w.sql, w: w.w, h: w.h});
  };

  const submitEditor = () => {
    if (!editor || !config) return;
    const widgets = [...config.widgets];
    const next: WidgetConfig = {
      id: editor.index >= 0 ? widgets[editor.index].id : newWidgetId(),
      type: editor.type,
      title: editor.title || editor.type,
      sql: editor.sql,
      x: editor.index >= 0 ? widgets[editor.index].x : (config.widgets.length * 3) % COLS,
      y: editor.index >= 0 ? widgets[editor.index].y : NEW_WIDGET_Y,
      w: editor.w,
      h: editor.h,
    };
    if (editor.index >= 0) {
      widgets[editor.index] = next;
    } else {
      widgets.push(next);
    }
    void saveConfig({...config, widgets});
    setEditor(null);
  };

  const removeWidget = (index: number) => {
    if (!config) return;
    void saveConfig({...config, widgets: config.widgets.filter((_, i) => i !== index)});
  };

  // Persist a layout once a drag/resize gesture ends. onLayoutChange fires on
  // every frame during the gesture; writing to the backend there would spam
  // PUTs and re-sync the grid mid-drag. onDragStop/onResizeStop fire once.
  const persistLayout = (layout: Layout) => {
    if (!config || !activeId) return;
    const byId = new Map(layout.map((l) => [l.i, l]));
    let changed = false;
    const widgets = config.widgets.map((w) => {
      const l = byId.get(w.id);
      if (l && (l.x !== w.x || l.y !== w.y || l.w !== w.w || l.h !== w.h)) {
        changed = true;
        return {...w, x: l.x, y: l.y, w: l.w, h: l.h};
      }
      return w;
    });
    if (!changed) return;
    const next = {...config, widgets};
    setConfig(next);
    void api
      .dashboardUpdate(activeId, editName, next)
      .then(() => load())
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  };

  const activeDash = dashboards?.find((d) => d.id === activeId);

  const layout: LayoutItem[] = widgetList.map((w) => ({i: w.id, x: w.x, y: w.y, w: w.w, h: w.h}));
  const {ref: gridRef, width: gridWidth} = useMeasuredWidth<HTMLDivElement>(!!(activeDash && config));

  return (
    <div style={{display: 'flex', flexDirection: 'column', gap: 16, padding: 16}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap'}}>
        <Text variant="header-1">Dashboards</Text>
        <Select
          size="m"
          placeholder="Dashboard"
          value={activeId ? [activeId] : []}
          options={(dashboards ?? []).map((d) => ({value: d.id, content: d.name}))}
          onUpdate={(v) => v[0] && setActiveId(v[0])}
        />
        <div style={{width: 220}}>
          <TextInput value={name} onUpdate={setName} placeholder="New dashboard name" />
        </div>
        <Button view="action" size="m" onClick={create} disabled={creating || !name.trim()}>
          {creating ? 'Creating…' : 'Create'}
        </Button>
        <Button size="m" onClick={load}>Refresh</Button>
        {loading && <Spin size="m" />}
      </div>

      {activeDash && config && (
        <div style={{display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap'}}>
          <div style={{width: 260}}>
            <TextInput
              size="m"
              value={editName}
              onUpdate={setEditName}
              onBlur={() => {
                if (editName.trim() && editName !== activeDash.name) {
                  api
                    .dashboardUpdate(activeId!, editName.trim(), config)
                    .then(() => load())
                    .catch((e) => setError(e instanceof Error ? e.message : String(e)));
                }
              }}
            />
          </div>
          <div style={{width: 160}}>
            <Select
              size="m"
              value={[String(refreshInterval)]}
              options={[5, 10, 15, 30, 60, 300].map((s) => ({value: String(s), content: `refresh ${s}s`}))}
              onUpdate={(v) => {
                const s = Number(v[0]);
                setRefreshInterval(s);
                if (config) void saveConfig({...config, refresh_interval: s});
              }}
            />
          </div>
          <Switch checked={refreshOn} onUpdate={setRefreshOn} content="Auto-refresh" />
          <Button size="m" onClick={() => void runWidgets(config)}>Run</Button>
          <Button size="m" view="action" onClick={addWidget}>
            <span style={{display: 'inline-flex', alignItems: 'center', gap: 6}}>
              <Plus width={16} height={16} style={{flexShrink: 0}} /> Add widget
            </span>
          </Button>
          <Button size="m" view="flat-danger" onClick={() => remove(activeId!)}>
            <span style={{display: 'inline-flex', alignItems: 'center', gap: 6}}>
              <TrashBin width={16} height={16} style={{flexShrink: 0}} /> Delete
            </span>
          </Button>
        </div>
      )}

      {error && <Alert theme="danger" title="Dashboards" message={error} />}

      {activeDash && config && (
        <div
          ref={gridRef}
          style={{border: '1px solid var(--g-color-line-generic)', borderRadius: 8, overflow: 'hidden', background: 'var(--g-color-base-float)'}}
        >
          {gridWidth > 0 && (
            <GridLayout
              width={gridWidth}
              layout={layout}
              gridConfig={{cols: COLS, rowHeight: ROW_H, margin: [8, 8], containerPadding: [8, 8], maxRows: Infinity}}
              dragConfig={{enabled: true, handle: '.drag-handle'}}
              resizeConfig={{enabled: true, handles: ['se']}}
              onDragStop={(_layout) => persistLayout(_layout)}
              onResizeStop={(_layout) => persistLayout(_layout)}
              autoSize
            >
              {widgetList.map((w, index) => (
                <div key={w.id} style={{display: 'flex', flexDirection: 'column', border: '1px solid var(--g-color-line-generic)', borderRadius: 8, overflow: 'hidden', background: 'var(--g-color-base-background)'}}>
                  <div
                    className="drag-handle"
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      padding: '4px 8px',
                      background: 'var(--g-color-base-generic-soft)',
                      cursor: 'move',
                      userSelect: 'none',
                    }}
                  >
                    <Text variant="caption-1" color="secondary" ellipsis>
                      {w.title} · {w.type}
                    </Text>
                    <div style={{display: 'flex', gap: 4}}>
                      <Button size="s" view="flat" onClick={() => openEditor(index)}>
                        <span style={{display: 'inline-flex', alignItems: 'center'}}>
                          <Pencil width={14} height={14} />
                        </span>
                      </Button>
                      <Button size="s" view="flat" onClick={() => removeWidget(index)}>
                        <span style={{display: 'inline-flex', alignItems: 'center'}}>
                          <TrashBin width={14} height={14} />
                        </span>
                      </Button>
                    </div>
                  </div>
                  <div style={{flex: 1, minHeight: 0, padding: 8, overflow: 'hidden'}}>
                    {widgetErrors[w.id] ? (
                      <div style={{padding: 8}}>
                        <Text variant="caption-1" color="danger">
                          {widgetErrors[w.id]}
                        </Text>
                      </div>
                    ) : results[w.id] ? (
                      <WidgetRenderer type={w.type} title={w.title} result={results[w.id]!} />
                    ) : (
                      <div style={{display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%'}}>
                        <Spin size="m" />
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </GridLayout>
          )}
        </div>
      )}

      {activeDash && config && widgetList.length === 0 && (
        <Alert theme="info" title="Empty dashboard" message="Add a widget to start building. Every widget runs a SQL query; time_bucket() aggregates timestamps for time series." />
      )}

      {!activeDash && dashboards && dashboards.length === 0 && (
        <Alert theme="info" title="No dashboards" message="Create a dashboard to start building widgets." />
      )}

      {editor && config && (
        <Dialog open={true} onClose={() => setEditor(null)} size="m">
          <Dialog.Header caption={editor.index >= 0 ? 'Edit widget' : 'Add widget'} />
          <Dialog.Body>
            <div style={{display: 'flex', flexDirection: 'column', gap: 12}}>
              <div>
                <TextInput label="Title" value={editor.title} onUpdate={(v) => setEditor({...editor, title: v})} />
              </div>
              <div>
                <Select
                  label="Type"
                  value={[editor.type]}
                  options={WIDGET_TYPES}
                  onUpdate={(v) => v[0] && setEditor({...editor, type: v[0] as WidgetType})}
                />
              </div>
              <div>
                <TextInput label="SQL" value={editor.sql} onUpdate={(v) => setEditor({...editor, sql: v})} />
              </div>
              <div style={{display: 'flex', gap: 12}}>
                <TextInput
                  label="Width (1-12)"
                  type="number"
                  value={String(editor.w)}
                  onUpdate={(v) => setEditor({...editor, w: Math.max(1, Math.min(COLS, Number(v) || 1))})}
                />
                <TextInput
                  label="Height (units)"
                  type="number"
                  value={String(editor.h)}
                  onUpdate={(v) => setEditor({...editor, h: Math.max(2, Number(v) || 2)})}
                />
              </div>
              <Text variant="caption-1" color="secondary">
                Widget queries run through /api/v1/widget/query (5s cache). Examples:
              </Text>
              <div style={{display: 'flex', flexDirection: 'column', gap: 4}}>
                {[
                  "SELECT time_bucket('5m', ts) AS t, COUNT(*) FROM logs GROUP BY 1",
                  "SELECT COUNT(*) AS total FROM logs WHERE level = 'ERROR'",
                  'SELECT level, COUNT(*) FROM logs GROUP BY 1',
                  'SELECT ts, level, msg FROM logs ORDER BY ts DESC LIMIT 50',
                ].map((e) => (
                  <Button key={e} size="s" view="outlined" onClick={() => setEditor({...editor, sql: e})}>
                    <span style={{fontFamily: 'var(--g-text-body-code-font-family)'}}>{e}</span>
                  </Button>
                ))}
              </div>
            </div>
          </Dialog.Body>
          <Dialog.Footer
            onClickButtonApply={submitEditor}
            onClickButtonCancel={() => setEditor(null)}
            textButtonApply={editor.index >= 0 ? 'Save' : 'Add'}
            textButtonCancel="Cancel"
          />
        </Dialog>
      )}
    </div>
  );
}
