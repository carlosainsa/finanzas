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
  cancelBotOpenOrders,
  cancelAllOrders,
  DashboardData,
  fallbackData,
  getControlToken,
  getReadToken,
  hasControlToken,
  loadDashboard,
  previewCancelAllOrders,
  previewCancelBotOpenOrders,
  setKillSwitch,
  setControlToken,
  setReadToken,
} from './api';

const navItems = ['Overview', 'Streams', 'Risk', 'Orders', 'Discovery', 'Research'];

export function App() {
  const [data, setData] = useState<DashboardData>(fallbackData);
  const [active, setActive] = useState('Overview');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState(false);
  const [readTokenInput, setReadTokenInput] = useState(() => getReadToken());
  const [controlTokenInput, setControlTokenInput] = useState(() => getControlToken());
  const [lastCommandId, setLastCommandId] = useState<string | null>(null);

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
    setActionBusy(true);
    try {
      const preview = await previewCancelAllOrders();
      const warnings = preview.warnings.length > 0 ? `\n\n${preview.warnings.join('\n')}` : '';
      const phrase = window.prompt(
        `Emergency account-wide cancel. Known bot open orders: ${preview.affected_count}.${warnings}\n\nType CANCEL ALL OPEN ORDERS to continue.`,
      );
      if (phrase !== 'CANCEL ALL OPEN ORDERS') {
        return;
      }
      const response = await cancelAllOrders(phrase);
      setLastCommandId(response.command.command_id ?? null);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'cancel-all command failed');
    } finally {
      setActionBusy(false);
    }
  }

  async function submitCancelBotOpen() {
    setActionBusy(true);
    try {
      const preview = await previewCancelBotOpenOrders();
      const warnings = preview.warnings.length > 0 ? `\n\n${preview.warnings.join('\n')}` : '';
      const confirmed = window.confirm(
        `Cancel ${preview.affected_count} bot-tracked open orders?${warnings}`,
      );
      if (!confirmed) {
        return;
      }
      const response = await cancelBotOpenOrders();
      setLastCommandId(response.command.command_id ?? null);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'cancel bot open command failed');
    } finally {
      setActionBusy(false);
    }
  }

  function saveToken() {
    setReadToken(readTokenInput);
    setControlToken(controlTokenInput);
    void refresh();
  }

  const controlEnabled = hasControlToken() && !actionBusy;

  const streamTotal = useMemo(
    () => data.streams.reduce((total, stream) => total + stream.length, 0),
    [data.streams],
  );
  const ordersByStatus = useMemo(() => groupOrdersByStatus(data.orders, data.reports), [data.orders, data.reports]);
  const latestCommandResult = useMemo(
    () => data.controlResults.find((result) => result.command_id === lastCommandId),
    [data.controlResults, lastCommandId],
  );
  const latencyBars = [
    ['WS -> signal', data.runtime.ws_to_signal_latency_ms],
    ['Signal -> order', data.runtime.signal_to_order_latency_ms],
    ['Order -> report', data.runtime.order_to_report_latency_ms],
    ['WS -> report', data.runtime.ws_to_report_latency_ms],
  ] as const;

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
              placeholder="Read token"
              type="password"
              value={readTokenInput}
              onChange={(event) => setReadTokenInput(event.target.value)}
              onBlur={saveToken}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  saveToken();
                }
              }}
              aria-label="Operator API read bearer token"
            />
            <input
              className="tokenInput"
              placeholder="Control token"
              type="password"
              value={controlTokenInput}
              onChange={(event) => setControlTokenInput(event.target.value)}
              onBlur={saveToken}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  saveToken();
                }
              }}
              aria-label="Operator API control bearer token"
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
          <Metric
            title="Reconciliation"
            value={data.reconciliation.status}
            icon={<ShieldCheck size={18} />}
            tone={data.reconciliation.status === 'healthy' ? 'good' : data.reconciliation.status === 'diverged' ? 'danger' : 'neutral'}
          />
          <Metric title="Match rate" value={`${Math.round(data.metrics.match_rate * 100)}%`} icon={<BarChart3 size={18} />} tone="neutral" />
        </section>

        <section className="controlBand">
          <div>
            <p className="eyebrow">Runtime Control</p>
            <h2>{data.status.kill_switch ? 'Trading paused by operator' : 'Executor accepting valid signals'}</h2>
            <p>
              {latestCommandResult
                ? `Last command ${latestCommandResult.command_id}: ${latestCommandResult.status}`
                : 'Rust still enforces final risk gates before any live order.'}
            </p>
          </div>
          <div className="segmented">
            <button
              className={data.status.kill_switch ? 'dangerButton selected' : 'dangerButton'}
              disabled={!controlEnabled}
              onClick={() => void toggleKillSwitch(true)}
            >
              <PauseCircle size={17} /> Pause
            </button>
            <button
              className={!data.status.kill_switch ? 'safeButton selected' : 'safeButton'}
              disabled={!controlEnabled}
              onClick={() => void toggleKillSwitch(false)}
            >
              <PlayCircle size={17} /> Resume
            </button>
            <button className="dangerButton" disabled={!controlEnabled} onClick={() => void submitCancelAll()}>
              <XCircle size={17} /> Cancel all
            </button>
            <button className="dangerButton" disabled={!controlEnabled} onClick={() => void submitCancelBotOpen()}>
              <XCircle size={17} /> Cancel bot
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
              rows={ordersByStatus.open.map((order) => [
                order.order_id || 'pending',
                order.status,
                order.filled_size ?? '-',
                order.cumulative_filled_size ?? order.filled_size ?? '-',
                order.remaining_size ?? '-',
              ])}
              headers={['Order', 'Status', 'Last fill', 'Cum filled', 'Remaining']}
            />
          </Panel>

          <Panel title="Partial Orders" subtitle="remaining size by order">
            <Table
              empty="No partial orders"
              rows={ordersByStatus.partial.map((order) => [
                order.order_id || 'pending',
                order.cumulative_filled_size ?? order.filled_size ?? '-',
                order.remaining_size ?? '-',
                order.error ?? '-',
              ])}
              headers={['Order', 'Cum filled', 'Remaining', 'Error']}
            />
          </Panel>

          <Panel title="Closed Orders" subtitle="cancelled and failed reports">
            <Table
              empty="No closed reports"
              rows={[...ordersByStatus.cancelled, ...ordersByStatus.errors].slice(0, 8).map((order) => [
                order.order_id || 'pending',
                order.status,
                order.remaining_size ?? '-',
                order.error ?? '-',
              ])}
              headers={['Order', 'Status', 'Remaining', 'Error']}
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

          <Panel title="Control Results" subtitle="recent operator commands">
            <Table
              empty="No control results"
              rows={data.controlResults.slice(0, 6).map((result) => [
                result.command_id,
                result.command_type ?? result.type,
                result.status,
                result.operator ?? '-',
                result.reason ?? '-',
                result.canceled_count ?? result.canceled?.length ?? '-',
                result.error ?? '-',
              ])}
              headers={['Command', 'Type', 'Status', 'Operator', 'Reason', 'Canceled', 'Error']}
            />
          </Panel>

          <Panel title="Reconciliation" subtitle={data.reconciliation.source}>
            <div className="counterGrid">
              <Counter label="Open local" value={data.reconciliation.open_local_orders} />
              <Counter label="Pending cancels" value={data.reconciliation.pending_cancel_requests} />
              <Counter label="Diverged" value={data.reconciliation.diverged_cancel_requests} />
              <Counter label="Stale orders" value={data.reconciliation.stale_orders} />
            </div>
            <Table
              empty="No reconciliation events"
              rows={data.reconciliation.recent_events.slice(0, 5).map((event) => [
                event.event_type,
                event.severity,
                event.order_id ?? '-',
                event.created_at,
              ])}
              headers={['Type', 'Severity', 'Order', 'Created']}
            />
          </Panel>

          <Panel title="Runtime Metrics" subtitle="latency and controlled counters">
            <div className="latencyChart">
              {latencyBars.map(([label, value]) => (
                <div className="latencyRow" key={label}>
                  <span>{label}</span>
                  <div className="barWrap">
                    <div className="bar" style={{ width: `${latencyWidth(value)}%` }} />
                  </div>
                  <code>{formatNumber(value)} ms</code>
                </div>
              ))}
            </div>
            <div className="counterGrid">
              <Counter label="Reports" value={data.runtime.execution_reports} />
              <Counter label="Errors" value={data.runtime.clob_errors} />
              <Counter label="Submitted" value={data.runtime.orders_submitted} />
              <Counter label="Controls" value={data.runtime.control_results} />
            </div>
          </Panel>

          <Panel title="NIM Budget" subtitle={data.nimBudget.source}>
            <div className="budgetHeader">
              <span className={`budgetBadge ${budgetTone(data.nimBudget.budget_status)}`}>
                {data.nimBudget.budget_status ?? data.nimBudget.status}
              </span>
              <code>{data.nimBudget.run_id ?? 'no research run'}</code>
            </div>
            <div className="counterGrid">
              <Counter label="Tokens" value={data.nimBudget.total_tokens ?? 0} />
              <Counter label="Annotations" value={data.nimBudget.annotations ?? 0} />
              <Counter label="Failures" value={data.nimBudget.failures ?? 0} />
              <Counter label="Cost" value={Number(data.nimBudget.estimated_cost ?? 0)} />
            </div>
            <Table
              empty="No NIM budget data"
              rows={[
                [
                  data.nimBudget.nim_model ?? '-',
                  formatNumber(data.nimBudget.latency_ms_avg),
                  formatBudgetCost(data.nimBudget.estimated_cost),
                  data.nimBudget.budget_violations.length > 0
                    ? data.nimBudget.budget_violations.join(', ')
                    : '-',
                ],
              ]}
              headers={['Model', 'Avg latency ms', 'Cost', 'Violations']}
            />
          </Panel>

          <Panel title="Research Runs" subtitle="latest offline manifests">
            <Table
              empty="No research runs indexed"
              rows={data.researchRuns.slice(0, 6).map((run) => [
                run.run_id ?? '-',
                run.passed === null || run.passed === undefined ? '-' : run.passed ? 'pass' : 'fail',
                run.go_no_go_decision ?? '-',
                run.go_no_go_profile ?? '-',
                formatNumber(run.realized_edge),
                formatNumber(run.fill_rate),
                run.nim_budget_status ?? '-',
                run.feature_research_decision ?? '-',
              ])}
              headers={['Run', 'Passed', 'Go/No-Go', 'Profile', 'Edge', 'Fill rate', 'NIM budget', 'Feature decision']}
            />
          </Panel>

          <Panel title="Go/No-Go" subtitle={data.goNoGo.source}>
            <div className="budgetHeader">
              <span className={`budgetBadge ${data.goNoGo.passed ? 'good' : 'danger'}`}>
                {data.goNoGo.decision}
              </span>
              <code>{data.goNoGo.profile ?? 'no profile'} / {data.goNoGo.run_id ?? 'no research run'}</code>
            </div>
            <Table
              empty="No quantitative blockers"
              rows={data.goNoGo.blockers.slice(0, 6).map((blocker) => {
                const row = blocker as Record<string, unknown>;
                return [
                  String(row.check_name ?? '-'),
                  formatUnknown(row.metric_value),
                  formatUnknown(row.threshold),
                  String(row.passed ?? false),
                ];
              })}
              headers={['Check', 'Metric', 'Threshold', 'Passed']}
            />
          </Panel>

          <Panel title="Pre-Live Readiness" subtitle={data.preLiveReadiness.source}>
            <div className="budgetHeader">
              <span className={`budgetBadge ${readinessTone(data.preLiveReadiness.status)}`}>
                {data.preLiveReadiness.status}
              </span>
              <code>{data.preLiveReadiness.run_id ?? 'no research run'}</code>
            </div>
            <div className="counterGrid">
              <Counter
                label="Checks"
                value={data.preLiveReadiness.checks.length}
              />
              <Counter
                label="Blockers"
                value={data.preLiveReadiness.blockers.length}
              />
              <Counter
                label="Trades"
                value={data.preLiveReadiness.can_execute_trades ? 1 : 0}
              />
              <Counter
                label="Audit"
                value={String(asRecord(data.preLiveReadiness.audit).status ?? '') === 'ok' ? 1 : 0}
              />
            </div>
            <Table
              empty="No readiness checks"
              rows={data.preLiveReadiness.checks.slice(0, 6).map((check) => {
                const row = check as Record<string, unknown>;
                return [
                  String(row.check_name ?? '-'),
                  String(row.passed ?? false),
                  formatUnknown(row.metric_value),
                  String(row.threshold ?? '-'),
                ];
              })}
              headers={['Check', 'Passed', 'Metric', 'Threshold']}
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

function groupOrdersByStatus(openOrders: DashboardData['orders'], reports: DashboardData['reports']) {
  const open = openOrders.filter((order) => order.status !== 'PARTIAL');
  const partial = [
    ...openOrders.filter((order) => order.status === 'PARTIAL'),
    ...reports.filter((order) => order.status === 'PARTIAL' && !openOrders.some((openOrder) => openOrder.order_id === order.order_id)),
  ];
  return {
    open,
    partial,
    cancelled: reports.filter((order) => order.status === 'CANCELLED'),
    errors: reports.filter((order) => order.status === 'ERROR'),
  };
}

function latencyWidth(value: number | null | undefined): number {
  if (!value || value <= 0) {
    return 0;
  }
  return Math.min(100, Math.max(4, (value / 1000) * 100));
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return '-';
  }
  return value.toFixed(value >= 10 ? 0 : 2);
}

function formatBudgetCost(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return '-';
  }
  return value === 0 ? '0' : value.toFixed(6);
}

function formatUnknown(value: unknown): string {
  if (typeof value === 'number') {
    return formatNumber(value);
  }
  if (value === null || value === undefined) {
    return '-';
  }
  return String(value);
}

function budgetTone(status: string | null | undefined): string {
  if (status === 'OK' || status === 'DISABLED') {
    return 'good';
  }
  if (status === 'BUDGET_EXCEEDED') {
    return 'danger';
  }
  return 'neutral';
}

function readinessTone(status: string | null | undefined): string {
  if (status === 'ready') {
    return 'good';
  }
  if (status === 'blocked') {
    return 'danger';
  }
  return 'neutral';
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function Counter({ label, value }: { label: string; value: number }) {
  return (
    <div className="counter">
      <span>{label}</span>
      <strong>{value.toLocaleString()}</strong>
    </div>
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
