"""Regression checks for arrival boundaries versus genuinely missing/invalid work."""
import copy
import unittest

from run_cache_capacity import passes_cache_gate


class CacheGateTest(unittest.TestCase):
    def setUp(self):
        self.row = {
            'target_rps': 3000, 'expected': 540000, 'completed': 540002, 'valid': 540002,
            'actual_iteration_rps': 2999.979, 'invalid': 0, 'dropped': 0,
            'http_failed_rate': 0, 'e2e_p99_ms': 3, 'k6_exit': 0,
            'cache_counters': {'requests': 540002, 'databaseFallbacks': 0,
                               'bloomBypass': 0, 'bloomErrors': 0},
            'l1_hit_ratio': .99996,
        }

    def test_should_accept_verified_extra_boundary_arrivals(self):
        self.assertTrue(passes_cache_gate(self.row))

    def test_should_reject_missing_work_even_with_low_latency(self):
        for field, value in [('dropped', 1), ('valid', 540001), ('actual_iteration_rps', 2900)]:
            with self.subTest(field=field):
                row = copy.deepcopy(self.row)
                row[field] = value
                self.assertFalse(passes_cache_gate(row))

    def test_should_reject_errors_slow_requests_and_degraded_reads(self):
        for field, value in [('invalid', 1), ('http_failed_rate', .001), ('e2e_p99_ms', 101),
                             ('k6_exit', 99), ('l1_hit_ratio', .98)]:
            with self.subTest(field=field):
                row = copy.deepcopy(self.row)
                row[field] = value
                self.assertFalse(passes_cache_gate(row))
        for field, value in [('requests', 540001), ('databaseFallbacks', 1),
                             ('bloomBypass', 1), ('bloomErrors', 1)]:
            with self.subTest(counter=field):
                row = copy.deepcopy(self.row)
                row['cache_counters'][field] = value
                self.assertFalse(passes_cache_gate(row))


if __name__ == '__main__':
    unittest.main()
