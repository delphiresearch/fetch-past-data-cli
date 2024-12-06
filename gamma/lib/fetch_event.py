import requests
from typing import Optional, List, Union
from datetime import datetime
from urllib.parse import urlencode

class EventFetcher:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_events(self,
                    limit: Optional[int] = None,
                    offset: Optional[int] = None,
                    order: Optional[str] = None,
                    ascending: Optional[bool] = None,
                    ids: Optional[List[int]] = None,
                    slugs: Optional[List[str]] = None,
                    archived: Optional[bool] = None,
                    active: Optional[bool] = None,
                    closed: Optional[bool] = None,
                    liquidity_min: Optional[float] = None,
                    liquidity_max: Optional[float] = None,
                    volume_min: Optional[float] = None,
                    volume_max: Optional[float] = None,
                    start_date_min: Optional[Union[str, datetime]] = None,
                    start_date_max: Optional[Union[str, datetime]] = None,
                    end_date_min: Optional[Union[str, datetime]] = None,
                    end_date_max: Optional[Union[str, datetime]] = None,
                    tag: Optional[str] = None,
                    tag_id: Optional[int] = None,
                    related_tags: Optional[bool] = None,
                    tag_slug: Optional[str] = None) -> dict:
        
        params = {}
        
        # 基本的なパラメータ
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset
        if order is not None:
            params['order'] = order
        if ascending is not None and order is not None:
            params['ascending'] = ascending
            
        # IDとスラッグの複数指定
        if ids:
            params['id'] = ids
        if slugs:
            params['slug'] = slugs
            
        # ブール値フィルター
        if archived is not None:
            params['archived'] = archived
        if active is not None:
            params['active'] = active
        if closed is not None:
            params['closed'] = closed
            
        # 数値範囲フィルター
        if liquidity_min is not None:
            params['liquidity_min'] = liquidity_min
        if liquidity_max is not None:
            params['liquidity_max'] = liquidity_max
        if volume_min is not None:
            params['volume_min'] = volume_min
        if volume_max is not None:
            params['volume_max'] = volume_max
            
        # 日付範囲フィルター
        if start_date_min:
            params['start_date_min'] = start_date_min if isinstance(start_date_min, str) else start_date_min.isoformat()
        if start_date_max:
            params['start_date_max'] = start_date_max if isinstance(start_date_max, str) else start_date_max.isoformat()
        if end_date_min:
            params['end_date_min'] = end_date_min if isinstance(end_date_min, str) else end_date_min.isoformat()
        if end_date_max:
            params['end_date_max'] = end_date_max if isinstance(end_date_max, str) else end_date_max.isoformat()
            
        # タグ関連フィルター
        if tag:
            params['tag'] = tag
        if tag_id:
            params['tag_id'] = tag_id
        if related_tags is not None and tag_id is not None:
            params['related_tags'] = related_tags
        if tag_slug:
            params['tag_slug'] = tag_slug

        url = f"{self.base_url}/events"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        # print("url:", url)
        response = requests.get(url)
        return response.json()