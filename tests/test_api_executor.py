import unittest
from unittest.mock import patch, MagicMock
import time
import logging
from src.utils.api_executor import FutuAPIExecutor

# Disable logging for cleaner test output
logging.disable(logging.CRITICAL)

class TestFutuAPIExecutor(unittest.TestCase):

    def test_initialization(self):
        """Test that the executor initializes with correct default and custom parameters."""
        # Test default initialization
        executor_default = FutuAPIExecutor()
        self.assertEqual(executor_default.batch_size, 10)
        self.assertEqual(executor_default.wait_time, 30)
        self.assertEqual(executor_default.max_workers, 5)

        # Test custom initialization
        executor_custom = FutuAPIExecutor(batch_size=5, wait_time=10, max_workers=2)
        self.assertEqual(executor_custom.batch_size, 5)
        self.assertEqual(executor_custom.wait_time, 10)
        self.assertEqual(executor_custom.max_workers, 2)

    def test_single_execute_success(self):
        """Test a single successful API call execution."""
        executor = FutuAPIExecutor()
        mock_api_func = MagicMock(return_value="success")
        
        result = executor.execute("test_endpoint", mock_api_func, "arg1", kwarg1="kwarg1")
        
        mock_api_func.assert_called_once_with("arg1", kwarg1="kwarg1")
        self.assertEqual(result, "success")

    def test_execute_raises_exception(self):
        """Test that `execute` properly raises exceptions from the API call."""
        executor = FutuAPIExecutor()
        mock_api_func = MagicMock(side_effect=ValueError("API Error"))
        
        with self.assertRaises(ValueError) as context:
            executor.execute("test_endpoint", mock_api_func)
        
        self.assertEqual(str(context.exception), "API Error")

    @patch('time.sleep')
    @patch('time.time')
    def test_rate_limiting_logic(self, mock_time, mock_sleep):
        """Test that the executor waits correctly when the rate limit is hit."""
        # Provide enough mock time values for all internal calls.
        # 5 for the initial calls + 2 for the 6th call (one for waiting, one for timestamping)
        mock_time.side_effect = [100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.5, 110.0]
        executor = FutuAPIExecutor(batch_size=5, wait_time=10)
        mock_api_func = MagicMock(return_value="ok")

        # Make 5 calls to fill up the request queue for the endpoint
        for _ in range(5):
            executor.execute("test_endpoint", mock_api_func)
        
        # The 6th call should trigger the rate limit wait
        executor.execute("test_endpoint", mock_api_func)
        
        # Assert that sleep was called.
        # The first request was at 100.0. The 6th call is at 100.5.
        # The wait time should be: wait_time - (current_time - first_request_time)
        # 10 - (100.5 - 100.0) = 9.5
        mock_sleep.assert_called_once()
        self.assertAlmostEqual(mock_sleep.call_args[0][0], 9.5)
        
    def test_batch_execute(self):
        """Test concurrent execution of a batch of requests."""
        executor = FutuAPIExecutor(max_workers=3)
        
        def simple_api_call(x):
            time.sleep(0.1) # Simulate a network call
            return x * 2

        requests = [
            ("endpoint1", simple_api_call, (1,), {}),
            ("endpoint1", simple_api_call, (2,), {}),
            ("endpoint2", simple_api_call, (3,), {}),
        ]
        
        start_time = time.time()
        results = executor.batch_execute(requests)
        duration = time.time() - start_time

        # With 3 workers and 3 tasks of 0.1s, it should take slightly more than 0.1s
        self.assertLess(duration, 0.3)
        self.assertEqual(len(results), 3)
        # Sort results for consistent comparison
        self.assertEqual(sorted(results), [2, 4, 6])

if __name__ == '__main__':
    unittest.main() 