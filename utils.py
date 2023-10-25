import time
import requests

def safe_request(method, url, max_retries=3, backoff_factor=1, **kwargs):
    """
    Make a request, with added handling for 429 Too Many Requests.
    Retry the request up to `max_retries` times, waiting `backoff_factor` seconds between retries.
    """
    for attempt in range(max_retries):
        response = requests.request(method, url, **kwargs)
        if response.status_code != 429:  # Not rate-limited
            return response
        # If rate-limited, wait before retrying
        time.sleep(backoff_factor * (attempt + 1))
    return response
