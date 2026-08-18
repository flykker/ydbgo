import type {QueryResult} from '../api';

const cellStyle: React.CSSProperties = {
  padding: '6px 10px',
  borderBottom: '1px solid var(--g-color-line-generic)',
  fontFamily: 'var(--g-text-body-code-font-family)',
  fontSize: 12,
  whiteSpace: 'nowrap',
};

const headStyle: React.CSSProperties = {
  ...cellStyle,
  fontWeight: 600,
  position: 'sticky',
  top: 0,
  background: 'var(--g-color-base-background)',
};

// ResultTable renders a query result as a lightweight monospace grid. It is
// intentionally simple (UI-P1); a virtualized grid can replace it later.
export function ResultTable({result}: {result: QueryResult}) {
  if (result.columns.length === 0) {
    return <div style={{padding: 12}}>{result.type} — {result.affected} row(s)</div>;
  }
  return (
    <div style={{overflow: 'auto', maxHeight: 'calc(100vh - 220px)'}}>
      <table style={{borderCollapse: 'collapse', width: 'max-content'}}>
        <thead>
          <tr>
            {result.columns.map((c) => (
              <th key={c} style={headStyle}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => (
            <tr key={i}>
              {row.map((v, j) => (
                <td key={j} style={cellStyle}>{v}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
