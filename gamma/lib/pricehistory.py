import requests
from typing import Optional, Union
from datetime import datetime

class PriceHistoryFetcher:
    def __init__(self, base_url: str):
        self.base_url = base_url

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
        url = f"{self.base_url}/prices-history"
        response = requests.get(url, params=params)
        # # print(response.json())
        # if len(response.json()["history"]) > 0:
        #     # print("output summary: ")
        #     print("length: ", len(response.json()["history"]))
        #     print("interval: ", interval)
        #     print('pricehistory exists')
        #     # if len(response.json()["history"]) > 0:
        #     #     print("start_date: ", datetime.fromtimestamp(response.json()["history"][0]["t"]).strftime('%Y-%m-%d %H:%M:%S'))
        #     #     print("end_date: ", datetime.fromtimestamp(response.json()["history"][-1]["t"]).strftime('%Y-%m-%d %H:%M:%S'))
        # # else:
        # #     print("no data")
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print(f"JSONデコードエラー: {e}")
            return {"error": "Invalid JSON response"}