import createClient from 'openapi-fetch';
import type { paths } from './openapi';

export function createOperatorClient(baseUrl: string) {
  return createClient<paths>({ baseUrl });
}
