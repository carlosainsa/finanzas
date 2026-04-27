import { expect, test } from '@playwright/test';

const READ_TOKEN_KEY = 'polymarket.operator.readToken';
const CONTROL_TOKEN_KEY = 'polymarket.operator.controlToken';

test('public dashboard loads through reverse proxy without enabling dangerous controls', async ({ page }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    pageErrors.push(error.message);
  });

  await page.addInitScript(([readKey, controlKey, readToken]) => {
    if (readToken) {
      window.sessionStorage.setItem(readKey, readToken);
    }
    window.sessionStorage.removeItem(controlKey);
  }, [READ_TOKEN_KEY, CONTROL_TOKEN_KEY, process.env.OPERATOR_READ_TOKEN ?? '']);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Trading control surface' })).toBeVisible();
  await expect(page.getByText('API connected')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Pause' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Resume' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel bot' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel all' })).toBeDisabled();

  expect(consoleErrors).toEqual([]);
  expect(pageErrors).toEqual([]);
});
