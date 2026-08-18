import type {QueryResult} from './api';

// exportCSV downloads the query result as a UTF-8 (BOM) CSV file, quoting
// cells that contain commas, quotes or newlines.
export function exportCSV(result: QueryResult, filename: string): void {
  const esc = (v: string): string => (/[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v);
  const lines: string[] = [];
  if (result.columns.length > 0) {
    lines.push(result.columns.map(esc).join(','));
    for (const row of result.rows) {
      lines.push(row.map(esc).join(','));
    }
  } else if (result.note) {
    lines.push(result.note);
  }
  const blob = new Blob(['\uFEFF' + lines.join('\n')], {type: 'text/csv;charset=utf-8'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  // revoking synchronously can cancel the download before the browser picks it up
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
