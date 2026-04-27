import { expect, test, type Page, type Route } from '@playwright/test';

const READ_TOKEN_KEY = 'polymarket.operator.readToken';
const CONTROL_TOKEN_KEY = 'polymarket.operator.controlToken';

test('read-only operators can inspect dashboard but not run controls', async ({ page }) => {
  const requests: Array<{ method: string; authorization: string | null }> = [];
  await mockOperatorApi(page, { requests });
  await page.addInitScript(([readKey]) => {
    window.sessionStorage.setItem(readKey, 'read-token');
  }, [READ_TOKEN_KEY]);

  await page.goto('/');

  await expect(page.getByText('API connected')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Pause' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Resume' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel bot' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel all' })).toBeDisabled();
  expect(requests.filter((request) => request.method === 'GET')).toContainEqual(
    expect.objectContaining({ authorization: 'Bearer read-token' }),
  );
});

test('control operators can cancel bot open orders and see control result', async ({ page }) => {
  const state = { controlResults: [] as Array<Record<string, unknown>> };
  await mockOperatorApi(page, { state });
  await page.addInitScript(([readKey, controlKey]) => {
    window.sessionStorage.setItem(readKey, 'read-token');
    window.sessionStorage.setItem(controlKey, 'control-token');
  }, [READ_TOKEN_KEY, CONTROL_TOKEN_KEY]);
  page.on('dialog', async (dialog) => {
    expect(dialog.type()).toBe('confirm');
    await dialog.accept();
  });

  await page.goto('/');
  await page.getByRole('button', { name: 'Cancel bot' }).click();

  await expect(page.getByText('Last command cmd-cancel-bot-open: SENT')).toBeVisible();
  await expect(page.getByRole('cell', { name: 'cmd-cancel-bot-open' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'CANCEL_BOT_OPEN' })).toBeVisible();
});

test('orders are grouped into open partial and closed sections', async ({ page }) => {
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Open Orders' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-open' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Partial Orders' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-partial' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Closed Orders' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-cancelled' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-error' })).toBeVisible();
});

async function mockOperatorApi(
  page: Page,
  options: {
    requests?: Array<{ method: string; authorization: string | null }>;
    state?: { controlResults: Array<Record<string, unknown>> };
  } = {},
): Promise<void> {
  const state = options.state ?? { controlResults: [] };
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    options.requests?.push({
      method: request.method(),
      authorization: request.headers().authorization ?? null,
    });

    if (request.method() === 'POST' && path === '/api/orders/cancel-bot-open') {
      const command = {
        command_id: 'cmd-cancel-bot-open',
        command_type: 'CANCEL_BOT_OPEN',
        type: 'CANCEL_BOT_OPEN',
        status: 'SENT',
        operator: 'dashboard',
        reason: 'dashboard cancel bot open orders',
        canceled_count: 1,
        canceled: ['order-open'],
        error: null,
      };
      state.controlResults.unshift(command);
      return fulfillJson(route, { command });
    }

    if (request.method() === 'POST' && path === '/api/control/preview/cancel-bot-open') {
      return fulfillJson(route, { affected_count: 1, warnings: [] });
    }
    if (request.method() === 'POST' && path === '/api/control/preview/cancel-all') {
      return fulfillJson(route, { affected_count: 2, warnings: ['Emergency account-wide command.'] });
    }
    if (request.method() === 'POST' && path === '/api/control/kill-switch') {
      return fulfillJson(route, { command: { command_id: 'cmd-pause', status: 'SENT' } });
    }
    if (request.method() === 'POST' && path === '/api/control/resume') {
      return fulfillJson(route, { command: { command_id: 'cmd-resume', status: 'SENT' } });
    }

    return fulfillJson(route, responseFor(path, state.controlResults));
  });
}

function responseFor(path: string, controlResults: Array<Record<string, unknown>>): Record<string, unknown> {
  switch (path) {
    case '/api/status':
      return {
        status: 'ok',
        kill_switch: false,
        streams: ['orderbook:stream', 'signals:stream'],
        predictor: { min_spread: 0.03, order_size: 1, min_confidence: 0.55 },
      };
    case '/api/risk':
      return {
        kill_switch: false,
        source: 'postgres',
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
      };
    case '/api/streams':
      return {
        streams: [
          { stream: 'orderbook:stream', length: 4, consumer_group: 'python-predictor', pending: { pending: 0 } },
          { stream: 'signals:stream', length: 2, consumer_group: 'rust-executor', pending: { pending: 0 } },
        ],
      };
    case '/api/orders/open':
      return {
        orders: [
          executionReport('signal-open', 'order-open', 'UNMATCHED', 0, 0, 1),
          executionReport('signal-partial', 'order-partial', 'PARTIAL', 0.5, 0.5, 0.5),
        ],
      };
    case '/api/positions':
      return { positions: [{ market_id: 'market-1', asset_id: 'asset-yes', position: 1.25 }] };
    case '/api/execution-reports':
      return {
        reports: [
          executionReport('signal-partial', 'order-partial', 'PARTIAL', 0.5, 0.5, 0.5),
          executionReport('signal-cancelled', 'order-cancelled', 'CANCELLED', 0, 0, 1),
          { ...executionReport('signal-error', 'order-error', 'ERROR', 0, 0, 1), error: 'CLOB rejected order' },
        ],
      };
    case '/api/markets/discover':
      return { markets: [] };
    case '/api/strategy/metrics':
      return {
        sample_size: 3,
        matched: 1,
        open: 1,
        errors: 1,
        match_rate: 0.33,
        error_rate: 0.33,
        filled_size: 0.5,
        latency_ms: 120,
        source: 'execution:reports:stream',
      };
    case '/api/control/results':
      return { results: controlResults };
    case '/api/reconciliation/status':
      return {
        status: 'healthy',
        source: 'postgres',
        open_local_orders: 2,
        pending_cancel_requests: 0,
        diverged_cancel_requests: 0,
        stale_orders: 0,
        recent_event_count: 0,
        events_by_severity: {},
        events_by_type: {},
        recent_events: [],
        last_reconciled_at_ms: null,
      };
    case '/api/metrics':
      return {
        signals_received: 3,
        signals_rejected: 1,
        orders_submitted: 2,
        clob_errors: 1,
        clob_errors_by_type: { rejected: 1 },
        execution_reports: 3,
        execution_reports_by_status: { PARTIAL: 1, CANCELLED: 1, ERROR: 1 },
        control_results: controlResults.length,
        control_results_by_type: { CANCEL_BOT_OPEN: controlResults.length },
        ws_to_report_latency_ms: 180,
        ws_to_signal_latency_ms: 40,
        signal_to_order_latency_ms: 60,
        order_to_report_latency_ms: 80,
        source: ['signals:stream', 'execution:reports:stream'],
      };
    default:
      return {};
  }
}

function executionReport(
  signalId: string,
  orderId: string,
  status: string,
  filledSize: number,
  cumulativeFilledSize: number,
  remainingSize: number,
): Record<string, unknown> {
  return {
    signal_id: signalId,
    order_id: orderId,
    status,
    filled_price: filledSize > 0 ? 0.5 : null,
    filled_size: filledSize,
    cumulative_filled_size: cumulativeFilledSize,
    remaining_size: remainingSize,
    error: null,
    timestamp_ms: 1_700_000_000_000,
  };
}

async function fulfillJson(route: Route, body: Record<string, unknown>): Promise<void> {
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify(body),
  });
}
