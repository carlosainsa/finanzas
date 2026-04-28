import { expect, test, type Page, type Route } from '@playwright/test';

const READ_TOKEN_KEY = 'polymarket.operator.readToken';
const CONTROL_TOKEN_KEY = 'polymarket.operator.controlToken';

test('read-only operators can inspect dashboard but not run controls', async ({ page }) => {
  const requests: Array<{ method: string; path: string; authorization: string | null }> = [];
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
  const state = initialOperatorState();
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

test('control operators can pause and resume the kill switch', async ({ page }) => {
  const state = initialOperatorState();
  await mockOperatorApi(page, { state });
  await page.addInitScript(([readKey, controlKey]) => {
    window.sessionStorage.setItem(readKey, 'read-token');
    window.sessionStorage.setItem(controlKey, 'control-token');
  }, [READ_TOKEN_KEY, CONTROL_TOKEN_KEY]);

  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Executor accepting valid signals' })).toBeVisible();
  await page.getByRole('button', { name: 'Pause' }).click();

  await expect(page.getByRole('heading', { name: 'Trading paused by operator' })).toBeVisible();
  await expect(page.getByText('Enabled')).toBeVisible();

  await page.getByRole('button', { name: 'Resume' }).click();

  await expect(page.getByRole('heading', { name: 'Executor accepting valid signals' })).toBeVisible();
  await expect(page.getByText('Clear')).toBeVisible();
});

test('cancel-all requires exact confirmation before sending command', async ({ page }) => {
  const state = initialOperatorState();
  const requests: Array<{ method: string; path: string; authorization: string | null }> = [];
  await mockOperatorApi(page, { requests, state });
  await page.addInitScript(([readKey, controlKey]) => {
    window.sessionStorage.setItem(readKey, 'read-token');
    window.sessionStorage.setItem(controlKey, 'control-token');
  }, [READ_TOKEN_KEY, CONTROL_TOKEN_KEY]);

  let promptCount = 0;
  page.on('dialog', async (dialog) => {
    expect(dialog.type()).toBe('prompt');
    promptCount += 1;
    await dialog.accept(promptCount === 1 ? 'wrong phrase' : 'CANCEL ALL OPEN ORDERS');
  });

  await page.goto('/');
  await page.getByRole('button', { name: 'Cancel all' }).click();
  expect(requests.some((request) => request.path === '/api/orders/cancel-all')).toBe(false);

  await page.getByRole('button', { name: 'Cancel all' }).click();

  await expect(page.getByText('Last command cmd-cancel-all: SENT')).toBeVisible();
  expect(requests.filter((request) => request.path === '/api/orders/cancel-all')).toHaveLength(1);
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

test('dashboard renders core controls on mobile without console errors', async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Trading control surface' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Refresh dashboard' })).toBeVisible();
  await expect(page.getByText('Runtime Control')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Runtime Metrics' })).toBeVisible();
  expect(consoleErrors).toEqual([]);
});

test('dashboard shows fallback and disables controls when status endpoint fails', async ({ page }) => {
  const browserErrors = collectBrowserErrors(page);
  await mockOperatorApi(page, {
    fail: (method, path) => method === 'GET' && path === '/api/status' ? 503 : null,
  });

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Trading control surface' })).toBeVisible();
  await expect(page.getByText('/api/status failed')).toBeVisible();
  await expect(page.getByText('offline', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Pause' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Resume' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel bot' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel all' })).toBeDisabled();
  expect(browserErrors).toEqual([]);
});

test('dashboard keeps last successful state when a later refresh fails', async ({ page }) => {
  let failMetrics = false;
  await mockOperatorApi(page, {
    fail: (method, path) => method === 'GET' && path === '/api/metrics' && failMetrics ? 500 : null,
  });

  await page.goto('/');
  await expect(page.getByRole('cell', { name: 'order-open' })).toBeVisible();

  failMetrics = true;
  await page.getByRole('button', { name: 'Refresh dashboard' }).click();

  await expect(page.getByText('/api/metrics failed')).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-open' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Runtime Metrics' })).toBeVisible();
});

test('dashboard shows NIM budget and tolerates research endpoint failure', async ({ page }) => {
  let failResearch = false;
  await mockOperatorApi(page, {
    fail: (method, path) => method === 'GET' && path === '/api/research/nim-budget' && failResearch ? 500 : null,
  });

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'NIM Budget' })).toBeVisible();
  await expect(page.getByText('deepseek-ai/deepseek-v3.2')).toBeVisible();
  await expect(page.locator('.budgetBadge').filter({ hasText: /^OK$/ })).toBeVisible();

  failResearch = true;
  await page.getByRole('button', { name: 'Refresh dashboard' }).click();

  await expect(page.getByRole('heading', { name: 'NIM Budget' })).toBeVisible();
  await expect(page.getByText('API connected')).toBeVisible();
});

test('dashboard shows recent research runs', async ({ page }) => {
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Research Runs' })).toBeVisible();
  await expect(page.getByRole('cell', { name: '20260427T000000Z' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'KEEP_DIAGNOSTIC' })).toBeVisible();
});

test('dashboard shows latest go no-go gate', async ({ page }) => {
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Go/No-Go' })).toBeVisible();
  await expect(page.locator('.budgetBadge').filter({ hasText: /^NO_GO$/ })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'positive_realized_edge' })).toBeVisible();
});

test('dashboard shows restricted blocklist ranking', async ({ page }) => {
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Restricted Blocklist Ranking' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'migrated_risk_only' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'test_migrated_risk_variant' })).toBeVisible();
});

test('dashboard shows pre-live readiness report', async ({ page }) => {
  await mockOperatorApi(page);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Pre-Live Readiness' })).toBeVisible();
  await expect(page.locator('.budgetBadge').filter({ hasText: /^blocked$/ })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'go_no_go_passed' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'pre_live', exact: true })).toBeVisible();
});

test('cancel-all preview failure does not submit destructive command', async ({ page }) => {
  const requests: Array<{ method: string; path: string; authorization: string | null }> = [];
  await mockOperatorApi(page, {
    requests,
    fail: (method, path) => method === 'POST' && path === '/api/control/preview/cancel-all' ? 500 : null,
  });
  await page.addInitScript(([readKey, controlKey]) => {
    window.sessionStorage.setItem(readKey, 'read-token');
    window.sessionStorage.setItem(controlKey, 'control-token');
  }, [READ_TOKEN_KEY, CONTROL_TOKEN_KEY]);

  await page.goto('/');
  await page.getByRole('button', { name: 'Cancel all' }).click();

  await expect(page.getByText('/api/control/preview/cancel-all failed')).toBeVisible();
  expect(requests.some((request) => request.path === '/api/orders/cancel-all')).toBe(false);
  await expect(page.getByText('Last command cmd-cancel-all: SENT')).not.toBeVisible();
});

test('cancel-bot rejection preserves visible order state', async ({ page }) => {
  const requests: Array<{ method: string; path: string; authorization: string | null }> = [];
  await mockOperatorApi(page, {
    requests,
    fail: (method, path) => method === 'POST' && path === '/api/orders/cancel-bot-open' ? 403 : null,
  });
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

  await expect(page.getByText('/api/orders/cancel-bot-open failed')).toBeVisible();
  await expect(page.getByRole('cell', { name: 'order-open' })).toBeVisible();
  await expect(page.getByText('Last command cmd-cancel-bot-open: SENT')).not.toBeVisible();
  expect(requests.filter((request) => request.path === '/api/orders/cancel-bot-open')).toHaveLength(1);
});

type OperatorState = {
  controlResults: Array<Record<string, unknown>>;
  killSwitch: boolean;
};

function initialOperatorState(): OperatorState {
  return { controlResults: [], killSwitch: false };
}

async function mockOperatorApi(
  page: Page,
  options: {
    requests?: Array<{ method: string; path: string; authorization: string | null }>;
    state?: OperatorState;
    fail?: (method: string, path: string) => number | null;
  } = {},
): Promise<void> {
  const state = options.state ?? initialOperatorState();
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    options.requests?.push({
      method: request.method(),
      path,
      authorization: request.headers().authorization ?? null,
    });
    const failureStatus = options.fail?.(request.method(), path);
    if (failureStatus) {
      return fulfillApiError(route, failureStatus);
    }

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

    if (request.method() === 'POST' && path === '/api/orders/cancel-all') {
      const command = {
        command_id: 'cmd-cancel-all',
        command_type: 'CANCEL_ALL',
        type: 'CANCEL_ALL',
        status: 'SENT',
        operator: 'dashboard',
        reason: 'dashboard emergency cancel all',
        canceled_count: 2,
        canceled: ['order-open', 'order-partial'],
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
      state.killSwitch = true;
      state.controlResults.unshift({
        command_id: 'cmd-pause',
        command_type: 'KILL_SWITCH_ON',
        type: 'KILL_SWITCH_ON',
        status: 'SENT',
        operator: 'dashboard',
        reason: 'dashboard operator pause',
        canceled_count: 0,
        canceled: [],
        error: null,
      });
      return fulfillJson(route, { command: { command_id: 'cmd-pause', status: 'SENT' } });
    }
    if (request.method() === 'POST' && path === '/api/control/resume') {
      state.killSwitch = false;
      state.controlResults.unshift({
        command_id: 'cmd-resume',
        command_type: 'RESUME',
        type: 'RESUME',
        status: 'SENT',
        operator: 'dashboard',
        reason: 'dashboard resume',
        canceled_count: 0,
        canceled: [],
        error: null,
      });
      return fulfillJson(route, { command: { command_id: 'cmd-resume', status: 'SENT' } });
    }

    return fulfillJson(route, responseFor(path, state));
  });
}

function responseFor(path: string, state: OperatorState): Record<string, unknown> {
  switch (path) {
    case '/api/status':
      return {
        status: 'ok',
        kill_switch: state.killSwitch,
        streams: ['orderbook:stream', 'signals:stream'],
        predictor: { min_spread: 0.03, order_size: 1, min_confidence: 0.55 },
      };
    case '/api/risk':
      return {
        kill_switch: state.killSwitch,
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
      return { results: state.controlResults };
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
        control_results: state.controlResults.length,
        control_results_by_type: { CANCEL_BOT_OPEN: state.controlResults.length },
        ws_to_report_latency_ms: 180,
        ws_to_signal_latency_ms: 40,
        signal_to_order_latency_ms: 60,
        order_to_report_latency_ms: 80,
        source: ['signals:stream', 'execution:reports:stream'],
      };
    case '/api/research/nim-budget':
      return {
        status: 'ok',
        source: 'data_lake/research_runs/research_runs.jsonl',
        run_id: '20260427T000000Z',
        report_root: 'data_lake/reports/20260427T000000Z',
        enabled: true,
        nim_model: 'deepseek-ai/deepseek-v3.2',
        annotations: 1,
        failures: 0,
        prompt_tokens: 200,
        completion_tokens: 66,
        total_tokens: 266,
        latency_ms_avg: 9625.576,
        estimated_cost: 0,
        budget_status: 'OK',
        budget_violations: [],
        can_execute_trades: false,
        updated_at: '2026-04-27T00:00:00+00:00',
      };
    case '/api/research/go-no-go':
      return {
        status: 'ok',
        source: 'data_lake/reports/20260427T000000Z/go_no_go.json',
        run_id: '20260427T000000Z',
        created_at: '2026-04-27T00:00:00+00:00',
        decision: 'NO_GO',
        passed: false,
        can_execute_trades: false,
        reason: 'quantitative_gate_failure',
        blockers: [
          {
            check_name: 'positive_realized_edge',
            metric_value: -0.01,
            threshold: 0,
            passed: false,
          },
        ],
        metrics: { realized_edge: -0.01, fill_rate: 0.5 },
        checks: [],
        pre_live_gate_passed: true,
        calibration_passed: true,
        pre_live_promotion_passed: false,
        agent_advisory_acceptable: true,
        nim_budget_status: 'OK',
      };
    case '/api/research/pre-live-readiness':
      return {
        report_version: 'pre_live_readiness_v1',
        status: 'blocked',
        source: 'data_lake/research_runs/research_runs.jsonl',
        run_id: '20260427T000000Z',
        created_at: '2026-04-27T00:00:00+00:00',
        report_root: 'data_lake/reports/20260427T000000Z',
        can_execute_trades: false,
        go_no_go: { decision: 'NO_GO', profile: 'pre_live', passed: false },
        checks: [
          {
            check_name: 'go_no_go_profile_pre_live',
            passed: true,
            metric_value: 'pre_live',
            threshold: 'pre_live|live_candidate',
          },
          {
            check_name: 'go_no_go_passed',
            passed: false,
            metric_value: 'NO_GO',
            threshold: 'GO',
          },
        ],
        blockers: [{ check_name: 'go_no_go_passed', passed: false }],
        audit: { status: 'ok', source: 'postgres', control_results: 1 },
        artifacts: {},
      };
    case '/api/research/restricted-blocklist-ranking':
      return {
        status: 'ok',
        source: 'data_lake/reports/20260427T000000Z/restricted_blocklist_ranking.json',
        run_id: '20260427T000000Z',
        created_at: '2026-04-27T00:00:00+00:00',
        report_root: 'data_lake/reports/20260427T000000Z',
        report_version: 'restricted_blocklist_ranking_v1',
        summary: {
          observations: 2,
          blocked_observations: 2,
          repeat_observation_candidates: 0,
        },
        top_candidate: {
          blocklist_kind: 'migrated_risk_only',
          recommendation: 'test_migrated_risk_variant',
        },
        observations: [
          {
            blocklist_kind: 'migrated_risk_only',
            score: -258.72,
            recommendation: 'test_migrated_risk_variant',
            restricted_decision: 'REJECT',
            risk_migration_status: 'risk_migration_detected',
          },
        ],
        can_execute_trades: false,
      };
    case '/api/research/runs':
      return {
        runs: [
          {
            run_id: '20260427T000000Z',
            created_at: '2026-04-27T00:00:00+00:00',
            source: 'research_loop',
            report_root: 'data_lake/reports/20260427T000000Z',
            passed: true,
            pre_live_gate_passed: true,
            calibration_passed: true,
            pre_live_promotion_passed: true,
            go_no_go_passed: true,
            go_no_go_decision: 'GO',
            feature_research_decision: 'KEEP_DIAGNOSTIC',
            realized_edge: 0.04,
            fill_rate: 0.5,
            nim_budget_status: 'OK',
            nim_total_tokens: 266,
            nim_estimated_cost: 0,
            nim_model: 'deepseek-ai/deepseek-v3.2',
            can_execute_trades: false,
          },
        ],
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

async function fulfillApiError(route: Route, status: number): Promise<void> {
  await route.fulfill({
    status,
    contentType: 'application/json',
    body: JSON.stringify({ detail: 'forced test failure' }),
  });
}

function collectBrowserErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error' && !message.text().startsWith('Failed to load resource:')) {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });
  return errors;
}
