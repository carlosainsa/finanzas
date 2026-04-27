import { defineConfig, devices } from '@playwright/test';

const publicOperatorUrl = process.env.PUBLIC_OPERATOR_URL ?? process.env.OPERATOR_PUBLIC_URL;

if (!publicOperatorUrl) {
  throw new Error('PUBLIC_OPERATOR_URL is required for the public dashboard smoke test');
}

export default defineConfig({
  testDir: './tests/public',
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: publicOperatorUrl,
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
