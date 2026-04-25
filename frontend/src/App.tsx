import {
  Activity,
  AlertTriangle,
  BarChart3,
  Database,
  PauseCircle,
  PlayCircle,
  RefreshCw,
  Search,
  ShieldCheck,
  XCircle,
} from 'lucide-react';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import {
  cancelAllOrders,
  DashboardData,
  fallbackData,
  getOperatorToken,
  loadDashboard,
  setKillSwitch,
  setOperatorToken,
} from './api';

const navItems = ['Overview', 'Streams', 'Risk', 'Orders', 'Discovery'];

export function App() {
  const [data, setData] = useState<DashboardData>(fallbackData);
  const [active, setActive] = useState('Overview');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [tokenInput, setTokenInput] = useState(() => getOperatorToken());

  async function refresh() {
    setLoading(true);
    try {
      const next = await loadDashboard();
      setData(next);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'API unavailable');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => void refresh(), 30000);
    return () => window.clearInterval(id);
  }, []);

  async function toggleKillSwitch(enabled: boolean) {
    setActionBusy(true);
    try {
      await setKillSwitch(enabled);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'control command failed');
    } finally {
      setActionBusy(false);
    }
  }

  async function submitCancelAll() {
    const confirmed = window.confirm('Cancel all open CLOB orders for the authenticated account?');
    if (!confirmed) {
      return;
    }
    setActionBusy(true);
    try {
      await cancelAllOrders();
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'cancel-all command failed');
    } finally {
      setActionBusy(false);
    }
  }

  function saveToken() {
    setOperatorToken(tokenInput);
    void refresh();
  }

  const streamTotal = useMemo(
    () => data.streams.reduce((total, stream) => total + stream.length, 0),
    [data.streams],
  );

  return (
    <main className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brandMark">PM</div>
          <div>
            <strong>Operator</strong>
            <span>Polymarket bot</span>
          </div>
        </div>
        <nav>
          {navItems.map((item) => (
            <button
              className={active === item ? 'navItem active' : 'navItem'}
              key={item}
              onClick={() => setActive(item)}
            >
              {item}
            </button>
          ))}
        </nav>
        <div className="sidebarFooter">
          <span>Execution</span>
          <strong>{data.risk.execution_mode}</strong>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Operator Dashboard</p>
            <h1>Trading control surface</h1>
          </div>
          <div className="topActions">
            {error ? <span className="apiError">{error}</span> : <span className="apiOk">API connected</span>}
            <input
              className="tokenInput"
              placeholder="Bearer token"
              type="password"
              value={tokenInput}
              onChange={(event) => setTokenInput(event.target.value)}
              onBlur={saveToken}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  saveToken();
                }
              }}
              aria-label="Operator API bearer token"
            />
            <button className="iconButton" onClick={() => void refresh()} aria-label="Refresh dashboard">
              <RefreshCw size={17} className={loading ? 'spin' : ''} />
            </button>
          </div>
        </header>

        <section className="metricGrid">
          <Metric title="Status" value={data.status.status} icon={<Activity size={18} />} tone="neutral" />
          <Metric
            title="Kill switch"
            value={data.status.kill_switch ? 'Enabled' : 'Clear'}
            icon={data.status.kill_switch ? <AlertTriangle size={18} /> : <ShieldCheck size={18} />}
            tone={data.status.kill_switch ? 'danger' : 'good'}
          />
          <Metric title="Stream events" value={streamTotal.toLocaleString()} icon={<Database size={18} />} tone="neutral" />
          <Metric title="Match rate" value={`${Math.round(data.metrics.match_rate * 100)}%`} icon={<BarChart3 size={18} />} tone="neutral" />
        </section>

        <section className="controlBand">
          <div>
            <p className="eyebrow">Runtime Control</p>
            <h2>{data.status.kill_switch ? 'Trading paused by operator' : 'Executor accepting valid signals'}</h2>
            <p>Rust still enforces final risk gates before any live order.</p>
          </div>
          <div className="segmented">
            <button
              className={data.status.kill_switch ? 'dangerButton selected' : 'dangerButton'}
              disabled={actionBusy}
              onClick={() => void toggleKillSwitch(true)}
            >
              <PauseCircle size={17} /> Pause
            </button>
            <button
              className={!data.status.kill_switch ? 'safeButton selected' : 'safeButton'}
              disabled={actionBusy}
              onClick={() => void toggleKillSwitch(false)}
            >
              <PlayCircle size={17} /> Resume
            </button>
            <button className="dangerButton" disabled={actionBusy} onClick={() => void submitCancelAll()}>
              <XCircle size={17} /> Cancel all
            </button>
          </div>
        </section>

        <section className="contentGrid">
          <Panel title="Redis Streams" subtitle="length and pending by consumer group">
            <div className="streamList">
              {data.streams.map((stream) => (
                <div className="streamRow" key={stream.stream}>
                  <div>
                    <strong>{stream.stream}</strong>
                    <span>{stream.consumer_group ?? 'no consumer group'}</span>
                  </div>
                  <div className="barWrap">
                    <div className="bar" style={{ width: `${Math.min(100, stream.length * 8)}%` }} />
                  </div>
                  <code>{stream.length}</code>
                </div>
              ))}
            </div>
          </Panel>

          <Panel title="Risk Limits" subtitle={data.risk.enforcement}>
            <div className="limitGrid">
              {Object.entries(data.risk.limits).map(([key, value]) => (
                <div className="limit" key={key}>
                  <span>{key.replaceAll('_', ' ')}</span>
                  <strong>{value}</strong>
                </div>
              ))}
            </div>
          </Panel>

          <Panel title="Open Orders" subtitle="best-effort from execution reports">
            <Table
              empty="No open orders"
              rows={data.orders.map((order) => [
                order.order_id || 'pending',
                order.status,
                order.filled_size ?? '-',
              ])}
              headers={['Order', 'Status', 'Filled']}
            />
          </Panel>

          <Panel title="Positions" subtitle="derived from matched reports">
            <Table
              empty="No positions"
              rows={data.positions.map((position) => [
                position.market_id,
                position.asset_id.slice(0, 10),
                position.position.toFixed(2),
              ])}
              headers={['Market', 'Asset', 'Position']}
            />
          </Panel>
        </section>

        <section className="widePanel">
          <div className="panelHeader">
            <div>
              <h2>Market Discovery</h2>
              <p>Read-only Gamma ranking. Advisory only.</p>
            </div>
            <Search size={18} />
          </div>
          <div className="marketGrid">
            {data.markets.length === 0 ? (
              <div className="emptyState">No ranked markets available from the API response.</div>
            ) : (
              data.markets.slice(0, 6).map((item) => (
                <article className="marketCard" key={item.market.market_id}>
                  <div>
                    <strong>{item.market.question}</strong>
                    <span>{item.reason}</span>
                  </div>
                  <div className="score">
                    <span>Score</span>
                    <b>{item.score.toFixed(2)}</b>
                  </div>
                </article>
              ))
            )}
          </div>
        </section>
      </section>
    </main>
  );
}

function Metric({ title, value, icon, tone }: { title: string; value: string; icon: ReactNode; tone: string }) {
  return (
    <article className={`metric ${tone}`}>
      <div>{icon}</div>
      <span>{title}</span>
      <strong>{value}</strong>
    </article>
  );
}

function Panel({ title, subtitle, children }: { title: string; subtitle: string; children: ReactNode }) {
  return (
    <article className="panel">
      <div className="panelHeader">
        <div>
          <h2>{title}</h2>
          <p>{subtitle}</p>
        </div>
      </div>
      {children}
    </article>
  );
}

function Table({ headers, rows, empty }: { headers: string[]; rows: Array<Array<string | number>>; empty: string }) {
  if (rows.length === 0) {
    return <div className="emptyState">{empty}</div>;
  }
  return (
    <table>
      <thead>
        <tr>{headers.map((header) => <th key={header}>{header}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.join(':')}>
            {row.map((cell, index) => <td key={`${cell}-${index}`}>{cell}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
