export interface QueryResult {
  type: string;
  columns: string[];
  rows: string[][];
  affected: number;
  note: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
  primary: boolean;
}

export interface TableInfo {
  name: string;
  engine: string;
  shards: number;
  size: number;
  retention?: string;
  columns: ColumnInfo[];
}

export interface ShardInfo {
  id: string;
  start: string;
  end: string;
  nodes: string[];
  size: number;
}

export interface NodeInfo {
  id: string;
  sql_addr: string;
  raft_addr: string;
}

export interface NodeMetrics {
  node: string;
  addr: string;
  status: string;
  json?: string;
}

export type WidgetType =
  | 'line'
  | 'bar'
  | 'pie'
  | 'stat'
  | 'gauge'
  | 'table'
  | 'histogram'
  | 'heatmap'
  | 'log_viewer';

export interface WidgetConfig {
  id: string;
  type: WidgetType;
  title: string;
  sql: string;
  /** grid position/size in units (cols=12, rowHeight=30) */
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface DashboardConfig {
  title: string;
  refresh_interval: number; // seconds
  widgets: WidgetConfig[];
}

export interface Dashboard {
  id: string;
  name: string;
  config: unknown;
  updated: string;
}

export function parseDashboardConfig(raw: unknown): DashboardConfig {
  const c = (raw ?? {}) as Partial<DashboardConfig>;
  return {
    title: c.title ?? '',
    refresh_interval: typeof c.refresh_interval === 'number' ? c.refresh_interval : 30,
    widgets: Array.isArray(c.widgets) ? (c.widgets as WidgetConfig[]) : [],
  };
}

interface ApiEnvelope {
  ok: boolean;
  error?: string;
}

async function call<T extends ApiEnvelope>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
    headers: {'Content-Type': 'application/json'},
    ...init,
  });
  let body: T;
  try {
    body = (await resp.json()) as T;
  } catch {
    throw new Error(`bad response ${resp.status} from ${path}`);
  }
  if (!resp.ok || !body.ok) {
    throw new Error(body.error || `request failed (${resp.status})`);
  }
  return body;
}

function post<T extends ApiEnvelope>(path: string, body: unknown): Promise<T> {
  return call<T>(path, {method: 'POST', body: JSON.stringify(body)});
}

function put<T extends ApiEnvelope>(path: string, body: unknown): Promise<T> {
  return call<T>(path, {method: 'PUT', body: JSON.stringify(body)});
}

export const api = {
  query: (sql: string) => post<ApiEnvelope & {result: QueryResult}>('/api/v1/query', {sql}),
  admin: (cmd: string) => post<ApiEnvelope & {result: QueryResult}>('/api/v1/admin', {cmd}),
  tables: () => call<ApiEnvelope & {tables: TableInfo[]}>('/api/v1/tables'),
  shards: (table: string) =>
    call<ApiEnvelope & {shards: ShardInfo[]}>(`/api/v1/shards?table=${encodeURIComponent(table)}`),
  nodes: () => call<ApiEnvelope & {nodes: NodeInfo[]}>('/api/v1/nodes'),
  metrics: () => call<ApiEnvelope & {metrics: NodeMetrics[]}>('/api/v1/metrics'),
  dashboards: () => call<ApiEnvelope & {dashboards: Dashboard[]}>('/api/v1/dashboards'),
  dashboardCreate: (name: string, config: unknown) =>
    post<ApiEnvelope & {id: string}>('/api/v1/dashboards', {name, config}),
  dashboardUpdate: (id: string, name: string, config: unknown) =>
    put<ApiEnvelope>('/api/v1/dashboards/' + id, {name, config}),
  dashboardDelete: (id: string) => call<ApiEnvelope>('/api/v1/dashboards/' + id, {method: 'DELETE'}),
  widgetQuery: (sql: string, ttl = 0) =>
    post<ApiEnvelope & {result: QueryResult; cached?: boolean}>('/api/v1/widget/query', {sql, ttl}),
  ingest: (table: string, columns: string[], rows: unknown[][]) =>
    post<ApiEnvelope & {affected: number}>('/api/v1/ingest', {table, columns, rows}),
};

// tail opens a live-tail SSE stream for sql, calling onEvent with the parsed
// payload on every frame. Returns a close function. EventSource reconnects
// automatically; onerror fires when the server drops the connection.
export function tail(
  sql: string,
  interval: number,
  onEvent: (payload: ApiEnvelope & {result?: QueryResult}) => void,
  onError: () => void,
): () => void {
  const url = `/api/v1/tail?interval=${interval}&sql=${encodeURIComponent(sql)}`;
  const es = new EventSource(url);
  es.onmessage = (e) => {
    try {
      onEvent(JSON.parse(e.data) as ApiEnvelope & {result?: QueryResult});
    } catch {
      // ignore malformed frame
    }
  };
  es.onerror = () => onError();
  return () => es.close();
}
