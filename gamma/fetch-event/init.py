from fetch_event import EventFetcher
from create_json import create_json_file
# EventFetcherのインスタンスを作成
fetcher = EventFetcher("https://gamma-api.polymarket.com")

# 基本的な使用例

# events = fetcher.fetch_events(limit=1)
events = fetcher.fetch_events(start_date_min="2024-11-20T00:00:00Z")
create_json_file(events, "events")

# 複数のパラメータを組み合わせた例
# events = fetcher.fetch_events(
#     limit=50,
#     order="slug",
#     ascending=False,
#     ids=[1, 2, 3],
#     tag="music",
#     start_date_min="2022-04-01T00:00:00Z"
# )
# print(events)