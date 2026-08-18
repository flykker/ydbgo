import {useState} from 'react';
import {Alert, Button, Text} from '@gravity-ui/uikit';
import {api, type QueryResult} from '../api';
import {ResultTable} from '../components/ResultTable';
import {exportCSV} from '../export';

const sample = `SELECT time_bucket('1h', ts) AS bucket, COUNT(*) AS rows, COUNT_IF(level = 'ERROR') AS errors
FROM logs
GROUP BY time_bucket('1h', ts)`;

function formatAffected(result: QueryResult): string {
  return `${result.type} — ${result.affected} row(s)`;
}

export function SqlPage() {
  const [sql, setSql] = useState(sample);
  const [rows, setRows] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const run = async () => {
    setLoading(true);
    setError('');
    try {
      const resp = await api.query(sql);
      setRows(resp.result);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRows(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{display: 'flex', flexDirection: 'column', gap: 12, padding: 16}}>
      <Text variant="header-1">SQL</Text>
      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        spellCheck={false}
        style={{
          minHeight: 140,
          padding: 12,
          fontFamily: 'var(--g-text-body-code-font-family)',
          fontSize: 13,
          border: '1px solid var(--g-color-line-generic)',
          borderRadius: 8,
          background: 'var(--g-color-base-float)',
          color: 'var(--g-color-text-primary)',
          resize: 'vertical',
        }}
      />
      <div style={{display: 'flex', alignItems: 'center', gap: 12}}>
        <Button view="action" size="m" onClick={run} disabled={loading}>
          {loading ? 'Running…' : 'Run'}
        </Button>
        {error && <Alert theme="danger" title="Query failed" message={error} />}
      </div>
      {rows && (
        <>
          <div style={{display: 'flex', alignItems: 'center', gap: 12}}>
            <Text variant="body-1" color="secondary">
              {formatAffected(rows)}
            </Text>
            <Button size="s" view="outlined" onClick={() => exportCSV(rows, 'query.csv')}>
              Export CSV
            </Button>
          </div>
          <ResultTable result={rows} />
        </>
      )}
    </div>
  );
}
