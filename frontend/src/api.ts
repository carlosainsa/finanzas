import type { components } from './generated/openapi';

export type PendingSummary = components['schemas']['PendingSummary'];
export type StreamSummary = components['schemas']['StreamSummary'];
export type StatusResponse = components['schemas']['StatusResponse'];
export type RiskResponse = components['schemas']['RiskResponse'];
export type ExecutionReport = components['schemas']['ExecutionReport'];
export type Position = components['schemas']['Position'];
export type MarketDiscovery = components['schemas']['ScoredMarket'];
export type StrategyMetrics = components['schemas']['StrategyMetricsResponse'];

export type DashboardData = {
  status: StatusResponse;
  risk: RiskResponse;
  streams: StreamSummary[];
  orders: ExecutionReport[];
  positions: Position[];
  reports: ExecutionReport[];
  markets: MarketDiscovery[];
  metrics: StrategyMetrics;
};

const API_BASE = import.meta.env.VITE_OPERATOR_API_BASE ?? '/api';
const TOKEN_STORAGE_KEY = 'polymarket.operator.token';

export function getOperatorToken(): string {
  return import.meta.env.VITE_OPERATOR_API_TOKEN ?? window.sessionStorage.getItem(TOKEN_STORAGE_KEY) ?? '';
}

export function setOperatorToken(token: string): void {
  const trimmed = token.trim();
  if (trimmed) {
    window.sessionStorage.setItem(TOKEN_STORAGE_KEY, trimmed);
  } else {
    window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
  }
}

function requestHeaders(extra?: HeadersInit): HeadersInit {
  const token = getOperatorToken();
  return {
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...extra,
  };
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: requestHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export async function loadDashboard(): Promise<DashboardData> {
  const status = await getJson<StatusResponse>('/status');
  const [risk, streams, orders, positions, reports, markets, metrics] = await Promise.all([
    getJson<RiskResponse>('/risk'),
    getJson<{ streams: StreamSummary[] }>('/streams'),
    getJson<{ orders: ExecutionReport[] }>('/orders/open'),
    getJson<{ positions: Position[] }>('/positions'),
    getJson<{ reports: ExecutionReport[] }>('/execution-reports?limit=50'),
    getJson<{ markets: MarketDiscovery[] }>('/markets/discover?limit=12'),
    getJson<StrategyMetrics>('/strategy/metrics?limit=500'),
  ]);

  return {
    status,
    risk,
    streams: streams.streams,
    orders: orders.orders,
    positions: positions.positions,
    reports: reports.reports,
    markets: markets.markets,
    metrics,
  };
}

export async function setKillSwitch(enabled: boolean): Promise<void> {
  const path = enabled ? '/control/kill-switch' : '/control/resume';
  const body = enabled
    ? { reason: 'dashboard operator pause', operator: 'dashboard' }
    : { confirm: true, reason: 'dashboard resume', operator: 'dashboard' };
  const response = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: requestHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
}

export async function cancelAllOrders(): Promise<void> {
  const response = await fetch(`${API_BASE}/orders/cancel-all`, {
    method: 'POST',
    headers: requestHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify({ reason: 'dashboard cancel all', operator: 'dashboard' }),
  });
  if (!response.ok) {
    throw new Error(`/orders/cancel-all returned ${response.status}`);
  }
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
  metrics: {
    sample_size: 0,
    matched: 0,
    open: 0,
    errors: 0,
    match_rate: 0,
    error_rate: 0,
    filled_size: 0,
    source: 'execution:reports:stream',
  },
};
