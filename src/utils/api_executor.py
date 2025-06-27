import time
import logging
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Any
from threading import Lock

logger = logging.getLogger(__name__)

class FutuAPIExecutor:
    def __init__(self, batch_size: int = 10, wait_time: int = 30, max_workers: int = 5):
        """Initializes the executor, setting up batch size, wait time, and thread safety."""
        self.batch_size = batch_size
        self.wait_time = wait_time
        self.max_workers = max_workers
        self.request_timestamps = defaultdict(lambda: deque(maxlen=batch_size))
        self.locks = defaultdict(Lock)

    def _wait_for_next_request(self, endpoint: str):
        """Waits until the next request can be sent for a given endpoint."""
        timestamps = self.request_timestamps[endpoint]
        if len(timestamps) < self.batch_size:
            return
        
        now = time.time()
        time_since_first_req = now - timestamps[0]
        
        if time_since_first_req < self.wait_time:
            wait_duration = self.wait_time - time_since_first_req
            logger.info(f"Endpoint {endpoint} hit rate limit, waiting for {wait_duration:.2f} seconds...")
            time.sleep(wait_duration)

    def execute(self, endpoint: str, api_func: Callable, *args, **kwargs) -> Any:
        """Executes a single API call, ensuring rate limits are respected."""
        with self.locks[endpoint]:
            self._wait_for_next_request(endpoint)
            try:
                result = api_func(*args, **kwargs)
                self.request_timestamps[endpoint].append(time.time())
                logger.debug(f"Successfully executed request for endpoint {endpoint}")
                return result
            except Exception as e:
                logger.error(f"Error during API call to {endpoint}: {e}", exc_info=True)
                raise

    def batch_execute(self, requests: List[tuple[str, Callable, tuple, dict]]) -> List[Any]:
        """Concurrently executes a batch of API calls using a thread pool."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_req = {executor.submit(self.execute, endpoint, api_func, *args, **kwargs): (endpoint, api_func) for endpoint, api_func, args, kwargs in requests}
            results = []
            for future in as_completed(future_to_req):
                try:
                    results.append(future.result())
                except Exception as e:
                    endpoint, _ = future_to_req[future]
                    logger.error(f"Request to endpoint {endpoint} failed in batch execution: {e}")
            return results 