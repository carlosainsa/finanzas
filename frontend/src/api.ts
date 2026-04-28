import type { components, paths } from './generated/openapi';
import { createOperatorClient } from './generated/client';

export type PendingSummary = components['schemas']['PendingSummary'];
export type StreamSummary = components['schemas']['StreamSummary'];
export type StatusResponse = components['schemas']['StatusResponse'];
export type RiskResponse = components['schemas']['RiskResponse'];
export type ExecutionReport = components['schemas']['ExecutionReport'];
export type Position = components['schemas']['Position'];
export type MarketDiscovery = components['schemas']['ScoredMarket'];
export type StrategyMetrics = components['schemas']['StrategyMetricsResponse'];
export type ControlResult = components['schemas']['ControlResult'];
export type ControlResponse = components['schemas']['ControlResponse'];
export type ControlPreview = components['schemas']['ControlPreviewResponse'];
export type ReconciliationStatus = components['schemas']['ReconciliationStatusResponse'];
export type RuntimeMetrics = components['schemas']['RuntimeMetricsResponse'];
export type NIMBudget = components['schemas']['NIMBudgetResponse'];
export type GoNoGo = components['schemas']['GoNoGoResponse'];
export type ResearchRunSummary = components['schemas']['ResearchRunSummary'];

export type DashboardData = {
  status: StatusResponse;
  risk: RiskResponse;
  streams: StreamSummary[];
  orders: ExecutionReport[];
  positions: Position[];
  reports: ExecutionReport[];
  markets: MarketDiscovery[];
  metrics: StrategyMetrics;
  controlResults: ControlResult[];
  reconciliation: ReconciliationStatus;
  runtime: RuntimeMetrics;
  nimBudget: NIMBudget;
  goNoGo: GoNoGo;
  researchRuns: ResearchRunSummary[];
};

const API_BASE = import.meta.env.VITE_OPERATOR_API_BASE ?? '';
const LEGACY_TOKEN_STORAGE_KEY = 'polymarket.operator.token';
const READ_TOKEN_STORAGE_KEY = 'polymarket.operator.readToken';
const CONTROL_TOKEN_STORAGE_KEY = 'polymarket.operator.controlToken';
const client = createOperatorClient(API_BASE);

export function getReadToken(): string {
  return (
    import.meta.env.VITE_OPERATOR_API_TOKEN
    ?? window.sessionStorage.getItem(READ_TOKEN_STORAGE_KEY)
    ?? window.sessionStorage.getItem(LEGACY_TOKEN_STORAGE_KEY)
    ?? ''
  );
}

export function getControlToken(): string {
  return (
    import.meta.env.VITE_OPERATOR_API_TOKEN
    ?? window.sessionStorage.getItem(CONTROL_TOKEN_STORAGE_KEY)
    ?? ''
  );
}

export function hasControlToken(): boolean {
  return getControlToken().length > 0;
}

export function setReadToken(token: string): void {
  setSessionToken(READ_TOKEN_STORAGE_KEY, token);
}

export function setControlToken(token: string): void {
  setSessionToken(CONTROL_TOKEN_STORAGE_KEY, token);
}

function setSessionToken(key: string, token: string): void {
  const trimmed = token.trim();
  if (trimmed) {
    window.sessionStorage.setItem(key, trimmed);
  } else {
    window.sessionStorage.removeItem(key);
  }
}

client.use({
  onRequest({ request }) {
    const token = request.method === 'GET'
      ? getReadToken() || getControlToken()
      : getControlToken();
    if (token) {
      request.headers.set('Authorization', `Bearer ${token}`);
    }
    return request;
  },
});

function unwrap<T>(data: T | undefined, error: unknown, path: string): T {
  if (error || data === undefined) {
    throw new Error(`${path} failed`);
  }
  return data;
}

async function getJson<T>(path: keyof paths): Promise<T> {
  const { data, error } = await client.GET(path as never);
  return unwrap(data as T | undefined, error, String(path));
}

export async function loadDashboard(): Promise<DashboardData> {
  const status = await getJson<StatusResponse>('/api/status');
  const [risk, streams, orders, positions, reports, markets, metrics, controlResults, reconciliation, runtime, nimBudget, goNoGo, researchRuns] = await Promise.all([
    getJson<RiskResponse>('/api/risk'),
    getJson<{ streams: StreamSummary[] }>('/api/streams'),
    getJson<{ orders: ExecutionReport[] }>('/api/orders/open'),
    getJson<{ positions: Position[] }>('/api/positions'),
    client.GET('/api/execution-reports', { params: { query: { limit: 50 } } }),
    client.GET('/api/markets/discover', { params: { query: { limit: 12 } } }),
    client.GET('/api/strategy/metrics', { params: { query: { limit: 500 } } }),
    client.GET('/api/control/results', { params: { query: { limit: 20 } } }),
    client.GET('/api/reconciliation/status', { params: { query: { limit: 20 } } }),
    client.GET('/api/metrics', { params: { query: { limit: 500 } } }),
    optionalGet<NIMBudget>('/api/research/nim-budget', fallbackData.nimBudget),
    optionalGet<GoNoGo>('/api/research/go-no-go', fallbackData.goNoGo),
    optionalGet<{ runs: ResearchRunSummary[] }>('/api/research/runs', { runs: fallbackData.researchRuns }),
  ]);

  return {
    status,
    risk,
    streams: streams.streams,
    orders: orders.orders,
    positions: positions.positions,
    reports: unwrap(reports.data, reports.error, '/api/execution-reports').reports,
    markets: unwrap(markets.data, markets.error, '/api/markets/discover').markets,
    metrics: unwrap(metrics.data, metrics.error, '/api/strategy/metrics'),
    controlResults: unwrap(controlResults.data, controlResults.error, '/api/control/results').results,
    reconciliation: unwrap(reconciliation.data, reconciliation.error, '/api/reconciliation/status'),
    runtime: unwrap(runtime.data, runtime.error, '/api/metrics'),
    nimBudget,
    goNoGo,
    researchRuns: researchRuns.runs,
  };
}

async function optionalGet<T>(path: keyof paths, fallback: T): Promise<T> {
  try {
    return await getJson<T>(path);
  } catch {
    return fallback;
  }
}

export async function setKillSwitch(enabled: boolean): Promise<void> {
  const path = enabled ? '/control/kill-switch' : '/control/resume';
  const { error } = enabled
    ? await client.POST('/api/control/kill-switch', {
      body: { reason: 'dashboard operator pause', operator: 'dashboard' },
    })
    : await client.POST('/api/control/resume', {
      body: { confirm: true, reason: 'dashboard resume', operator: 'dashboard' },
    });
  if (error) {
    throw new Error(`${path} failed`);
  }
}

export async function cancelBotOpenOrders(): Promise<ControlResponse> {
  const { data, error } = await client.POST('/api/orders/cancel-bot-open', {
    body: { reason: 'dashboard cancel bot open orders', operator: 'dashboard' },
  });
  if (error) {
    throw new Error('/api/orders/cancel-bot-open failed');
  }
  return unwrap(data, error, '/api/orders/cancel-bot-open');
}

export async function previewCancelBotOpenOrders(): Promise<ControlPreview> {
  const { data, error } = await client.POST('/api/control/preview/cancel-bot-open');
  return unwrap(data, error, '/api/control/preview/cancel-bot-open');
}

export async function previewCancelAllOrders(): Promise<ControlPreview> {
  const { data, error } = await client.POST('/api/control/preview/cancel-all');
  return unwrap(data, error, '/api/control/preview/cancel-all');
}

export async function cancelAllOrders(confirmationPhrase: string): Promise<ControlResponse> {
  const { data, error } = await client.POST('/api/orders/cancel-all', {
    body: {
      reason: 'dashboard emergency cancel all',
      operator: 'dashboard',
      confirm: true,
      confirmation_phrase: confirmationPhrase,
    },
  });
  if (error) {
    throw new Error('/api/orders/cancel-all failed');
  }
  return unwrap(data, error, '/api/orders/cancel-all');
}

export const fallbackData: DashboardData = {
  status: {
    status: 'offline',
    kill_switch: false,
    streams: [],
    predictor: { min_spread: 0.03, order_size: 1, min_confidence: 0.55 },
  },
  risk: {
    kill_switch: false,
    source: 'operator:kill_switch',
    execution_mode: 'dry_run',
    limits: {
      max_order_size: 10,
      min_confidence: 0.55,
      signal_max_age_ms: 5000,
      max_market_exposure: 100,
      max_daily_loss: 50,
      predictor_min_confidence: 0.55,
      predictor_order_size: 1,
    },
    enforcement: 'rust-engine',
  },
  streams: [
    { stream: 'orderbook:stream', length: 0, consumer_group: 'python-predictor', pending: { pending: 0 } },
    { stream: 'signals:stream', length: 0, consumer_group: 'rust-executor', pending: { pending: 0 } },
    { stream: 'execution:reports:stream', length: 0, consumer_group: null, pending: null },
  ],
  orders: [],
  positions: [],
  reports: [],
  markets: [],
  controlResults: [],
  reconciliation: {
    status: 'healthy',
    source: 'postgres',
    open_local_orders: 0,
    pending_cancel_requests: 0,
    diverged_cancel_requests: 0,
    stale_orders: 0,
    recent_event_count: 0,
    events_by_severity: {},
    events_by_type: {},
    recent_events: [],
    last_reconciled_at_ms: null,
  },
  metrics: {
    sample_size: 0,
    matched: 0,
    open: 0,
    errors: 0,
    match_rate: 0,
    error_rate: 0,
    filled_size: 0,
    latency_ms: null,
    source: 'execution:reports:stream',
  },
  runtime: {
    signals_received: 0,
    signals_rejected: 0,
    orders_submitted: 0,
    clob_errors: 0,
    clob_errors_by_type: {},
    execution_reports: 0,
    execution_reports_by_status: {},
    control_results: 0,
    control_results_by_type: {},
    ws_to_report_latency_ms: null,
    ws_to_signal_latency_ms: null,
    signal_to_order_latency_ms: null,
    order_to_report_latency_ms: null,
    source: ['signals:stream', 'execution:reports:stream', 'operator:results:stream'],
  },
  nimBudget: {
    status: 'missing',
    source: 'data_lake/research_runs/research_runs.jsonl',
    run_id: null,
    report_root: null,
    enabled: null,
    nim_model: null,
    annotations: null,
    failures: null,
    prompt_tokens: null,
    completion_tokens: null,
    total_tokens: null,
    latency_ms_avg: null,
    estimated_cost: null,
    budget_status: null,
    budget_violations: [],
    can_execute_trades: false,
    updated_at: null,
  },
  goNoGo: {
    status: 'missing',
    source: 'data_lake/research_runs/research_runs.jsonl',
    run_id: null,
    created_at: null,
    decision: 'NO_GO',
    passed: false,
    can_execute_trades: false,
    reason: 'missing_research_run',
    blockers: [],
    metrics: {},
    checks: [],
    pre_live_gate_passed: null,
    calibration_passed: null,
    pre_live_promotion_passed: null,
    agent_advisory_acceptable: null,
    nim_budget_status: null,
  },
  researchRuns: [],
};
