export type PendingSummary = {
  pending?: number;
  min?: string | null;
  max?: string | null;
  consumers?: unknown[];
  raw?: string;
};

export type StreamSummary = {
  stream: string;
  length: number;
  consumer_group: string | null;
  pending: PendingSummary | null;
};

export type StatusResponse = {
  status: string;
  kill_switch: boolean;
  streams: StreamSummary[];
  predictor: {
    min_spread: number;
    order_size: number;
    min_confidence: number;
  };
};

export type RiskResponse = {
  kill_switch: boolean;
  source: string;
  execution_mode: string;
  limits: Record<string, number>;
  enforcement: string;
};

export type ExecutionReport = {
  signal_id: string;
  order_id: string;
  status: 'MATCHED' | 'DELAYED' | 'UNMATCHED' | 'CANCELLED' | 'ERROR';
  timestamp_ms: number;
  filled_price?: number | null;
  filled_size?: number | null;
  error?: string | null;
};

export type Position = {
  market_id: string;
  asset_id: string;
  position: number;
};

export type MarketDiscovery = {
  market: {
    market_id: string;
    question: string;
    liquidity: number;
    volume: number;
    outcome_prices: number[];
    clob_token_ids: string[];
    tags: string[];
  };
  score: number;
  liquidity_score: number;
  volume_score: number;
  price_quality_score: number;
  evidence_score: number;
  reason: string;
};

export type StrategyMetrics = {
  sample_size: number;
  matched: number;
  open: number;
  errors: number;
  match_rate: number;
  error_rate: number;
  filled_size: number;
  source: string;
};

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

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
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
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
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
