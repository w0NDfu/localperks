import http from 'k6/http';
import { sleep } from 'k6';
import { scenario } from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const config = JSON.parse(open(__ENV.SHOPLINK_K6_FIXTURE));
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(config.baseUrl)) {
  throw new Error('LocalPerks load test requires a dedicated loopback endpoint');
}
const accepted = new Counter('seckill_accepted');
const rejected = new Counter('seckill_429');
const errors = new Counter('seckill_unexpected');
const finalized = new Counter('seckill_finalized');
const statusFailures = new Counter('seckill_status_failures');
const statusReads = new Counter('seckill_status_reads');
const acceptMs = new Trend('seckill_accept_ms', true);
const rejectMs = new Trend('seckill_reject_ms', true);
const visibleMs = new Trend('seckill_order_visible_ms', true);

export const options = {
  scenarios: {
    seckill: {
      executor: 'constant-arrival-rate',
      rate: config.rate,
      timeUnit: '1s',
      // Keep the last scheduled start strictly inside the measurement interval.
      duration: `${config.seconds * 1000 - 1}ms`,
      preAllocatedVUs: config.vus,
      maxVUs: config.vus,
      gracefulStop: '12s',
    },
  },
  summaryTrendStats: ['min', 'med', 'max', 'p(95)', 'p(99)', 'count'],
  thresholds: {
    dropped_iterations: ['count==0'],
    seckill_unexpected: ['count==0'],
    seckill_status_failures: ['count==0'],
  },
};

function parse(response) {
  try {
    return response.json();
  } catch {
    return null;
  }
}

export default function () {
  const index = scenario.iterationInTest;
  const authorization = config.tokens[index];
  if (!authorization) {
    errors.add(1);
    return;
  }
  const headers = { authorization };
  if (config.distributed) {
    headers['X-Forwarded-For'] = `198.18.${Math.floor(index / 254)}.${index % 254 + 1}`;
  }
  const started = Date.now();
  const response = http.post(`${config.baseUrl}/voucher-order/seckill/${config.voucher}`, null, {
    headers, redirects: 0, timeout: '5s', tags: { name: 'seckill_submit' },
  });
  const payload = parse(response);
  if (response.status === 429) {
    rejected.add(1);
    rejectMs.add(response.timings.duration);
    return;
  }
  const id = payload?.data?.orderId;
  if (response.status !== 200 || payload?.success !== true || typeof id !== 'string' || !/^\d+$/.test(id)) {
    errors.add(1);
    console.log(JSON.stringify({ index, submitStatus: response.status, error: 'unexpected_submit' }));
    return;
  }
  accepted.add(1);
  acceptMs.add(response.timings.duration);
  const statusUrl = `${config.baseUrl}/voucher-order/status/${id}`;
  for (let attempt = 0; attempt < 20 && Date.now() - started < 6000; attempt++) {
    statusReads.add(1);
    const status = http.get(statusUrl, {
      headers, redirects: 0, timeout: '2s', tags: { name: 'seckill_status' },
    });
    const state = parse(status);
    if (status.status === 200 && state?.success && state.data?.statusCode === 12) {
      finalized.add(1);
      visibleMs.add(Date.now() - started);
      console.log(JSON.stringify({ index, orderId: id, state: 12 }));
      return;
    }
    if (status.status !== 200 || state?.success !== true || ![10, 11].includes(state.data?.statusCode)) {
      break;
    }
    sleep(0.15);
  }
  statusFailures.add(1);
  console.log(JSON.stringify({ index, orderId: id, error: 'final_state_not_seen' }));
}

export function handleSummary(data) {
  return { [__ENV.SHOPLINK_K6_SUMMARY]: JSON.stringify(data, null, 2) };
}
