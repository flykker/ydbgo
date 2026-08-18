import {useEffect, useState} from 'react';
import {AsideHeader} from '@gravity-ui/navigation';
import {ChartLine, ChevronsLeft, Database, ListOl, Terminal, ThunderboltFill} from '@gravity-ui/icons';
import {ClusterPage} from './pages/Cluster';
import {SqlPage} from './pages/Sql';
import {DashboardPage} from './pages/Dashboards';
import {LogPage} from './pages/Logs';

type PageId = 'cluster' | 'sql' | 'dashboards' | 'logs';

const PAGE_IDS: PageId[] = ['cluster', 'sql', 'dashboards', 'logs'];

function pageFromHash(): PageId {
  const h = window.location.hash.replace(/^#\/?/, '');
  const base = h.split('/')[0];
  return (PAGE_IDS as string[]).includes(base) ? (base as PageId) : 'cluster';
}

// CollapseButton is a Jira-like circular chevron toggle rendered right under
// the logo instead of the framework's full-width bottom bar.
function CollapseButton({compact, onChange}: {compact: boolean; onChange: () => void}) {
  return (
    <div className="ydbgo-collapse-wrap">
      <button
        type="button"
        className={'ydbgo-collapse' + (compact ? ' ydbgo-collapse--closed' : ' ydbgo-collapse--open')}
        onClick={onChange}
        title={compact ? 'Expand sidebar' : 'Collapse sidebar'}
        aria-label={compact ? 'Expand sidebar' : 'Collapse sidebar'}
      >
        <ChevronsLeft width={18} height={18} />
      </button>
    </div>
  );
}

export function App() {
  const [compact, setCompact] = useState(false);
  const [page, setPage] = useState<PageId>(pageFromHash);

  // Keep the page shareable/reloadable via URL hash (#/dashboards etc).
  // The Dashboards page itself manages #/dashboards/<id> (see hash.ts); never
  // clobber that id while staying on the dashboards page.
  useEffect(() => {
    const onHash = () => setPage(pageFromHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    const cur = window.location.hash.replace(/^#\/?/, '');
    if (cur === page) return;
    if (page === 'dashboards' && cur.startsWith('dashboards/')) return;
    window.location.hash = '/' + page;
  }, [page]);

  const menuItems = [
    {id: 'cluster', title: 'Cluster', icon: Database, current: page === 'cluster'},
    {id: 'sql', title: 'SQL', icon: Terminal, current: page === 'sql'},
    {id: 'dashboards', title: 'Dashboards', icon: ChartLine, current: page === 'dashboards'},
    {id: 'logs', title: 'Logs', icon: ListOl, current: page === 'logs'},
  ].map((item) => ({
    ...item,
    onItemClick: () => setPage(item.id as PageId),
  }));

  return (
    <AsideHeader
      logo={{
        text: 'ydbgo',
        href: '/',
        wrapper: (_node, isCompact) => (
          <a
            className={'ydbgo-logo' + (isCompact ? ' ydbgo-logo--compact' : '')}
            href="/"
            onClick={() => setPage('cluster')}
          >
            <span className="ydbgo-logo__badge">
              <ThunderboltFill width={18} height={18} />
            </span>
            {!isCompact && <span className="ydbgo-logo__text">YADBGO</span>}
          </a>
        ),
      }}
      compact={compact}
      onChangeCompact={setCompact}
      menuItems={menuItems}
      hideCollapseButton
      renderFooter={() => <CollapseButton compact={compact} onChange={() => setCompact(!compact)} />}
      renderContent={() => {
        switch (page) {
          case 'sql':
            return <SqlPage />;
          case 'dashboards':
            return <DashboardPage />;
          case 'logs':
            return <LogPage />;
          default:
            return <ClusterPage />;
        }
      }}
    />
  );
}
