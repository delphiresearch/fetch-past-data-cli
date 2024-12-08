import requests
from typing import Optional, Union
from datetime import datetime
import time

class PriceHistoryFetcher:
    def __init__(self, base_url: str, retry_wait: int = 5, max_retries: int = 10):
        self.base_url = base_url
        self.retry_wait = retry_wait  # 初期リトライ待機秒数
        self.max_retries = max_retries  # 最大リトライ回数

    def fetch_pricehistory(self,
                          market: str,
                          start_ts: Optional[int] = None,
                          end_ts: Optional[int] = None,
                          interval: Optional[str] = None,
                          fidelity: Optional[int] = None) -> dict:
        """
        Fetches price history data.
        
        Args:
            market: CLOB token ID
            start_ts: Start time (UTC UNIX timestamp)
            end_ts: End time (UTC UNIX timestamp)
            interval: Duration ('1m', '1w', '1d', '6h', '1h', 'max')
            fidelity: Data resolution (in minutes)
        """
        params = {'market': market}
        
        if interval:
            if start_ts is not None or end_ts is not None:
                raise ValueError("The interval parameter cannot be specified with start_ts/end_ts")
            if interval not in ['1m', '1w', '1d', '6h', '1h', 'max']:
                raise ValueError("Invalid interval value")
            params['interval'] = interval
        else:
            if start_ts is None and end_ts is None:
                raise ValueError("start_ts or end_ts must be specified")
            params['startTs'] = start_ts
            params['endTs'] = end_ts

        if fidelity is not None:
            params['fidelity'] = fidelity

        # ANSIカラーコードの定義
        RED = '\033[91m'
        GREEN = '\033[92m'
        RESET = '\033[0m'

        url = f"{self.base_url}/prices-history"

        for attempt in range(self.max_retries):
            response = requests.get(url, params=params)
            
            if response.status_code != 200:
                if attempt == self.max_retries - 1:  # 最後の試行でエラーの場合のみ表示
                    print(f"{RED}HTTP Error: Status code {response.status_code}{RESET}")
                    print(f"{RED}Failed after all retry attempts{RESET}")
                    return {"error": f"HTTP error {response.status_code}"}
                # 指数関数的バックオフ: 待機時間を2倍ずつ増やす
                wait_time = self.retry_wait * (2 ** attempt)
                time.sleep(wait_time)
                continue

            try:
                data = response.json()
                return data
            except requests.exceptions.JSONDecodeError as e:
                if attempt == self.max_retries - 1:  # 最後の試行でエラーの場合のみ表示
                    print(f"{RED}JSON Decode Error: {e}{RESET}")
                    print(response.text)
                    print(f"{RED}Failed after all retry attempts{RESET}")
                    return {"error": "Retry limit exceeded"}
                # 指数関数的バックオフ: 待機時間を2倍ずつ増やす
                wait_time = self.retry_wait * (2 ** attempt)
                time.sleep(wait_time)
                continue

        return {"error": "Retry limit exceeded"}
