import http from 'k6/http';
import { scenario } from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const config = JSON.parse(open(__ENV.SHOPLINK_CACHE_FIXTURE));
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(config.baseUrl)) throw new Error('Dedicated loopback only');
const valid = new Counter('cache_valid_responses');
const invalid = new Counter('cache_invalid_responses');
const latency = new Trend('cache_http_e2e_ms', true);

export const options = {
  scenarios: { cached_reads: {
    executor: 'constant-arrival-rate', rate: config.rate, timeUnit: '1s',
    duration: `${config.seconds}s`, preAllocatedVUs: config.vus, maxVUs: config.vus,
    gracefulStop: '10s',
  } },
  summaryTrendStats: ['min', 'med', 'max', 'p(95)', 'p(99)', 'count'],
  thresholds: { cache_invalid_responses: ['count==0'], dropped_iterations: ['count==0'],
                cache_http_e2e_ms: ['p(99)<=100'] },
};

export default function () {
  const id = config.ids[scenario.iterationInTest % config.ids.length];
  const started = Date.now();
  const response = http.get(`${config.baseUrl}/shop/${id}`, {
    redirects: 0, timeout: '5s', tags: { name: 'cached_shop_detail' },
  });
  latency.add(Date.now() - started);
  let body;
  try { body = response.json(); } catch (_) { body = null; }
  if (response.status === 200 && body?.success === true && String(body?.data?.id) === String(id)
      && body?.data?.name === config.names[String(id)]) valid.add(1);
  else invalid.add(1);
}

export function handleSummary(data) {
  return { [__ENV.SHOPLINK_CACHE_SUMMARY]: JSON.stringify(data, null, 2) };
}
