import sys
import os

# プロジェクトのルートディレクトリへのパスを追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)
from gamma.lib.fetch_event import EventFetcher
from gamma.lib.create_json import create_json_file
from tqdm import tqdm

# EventFetcherのインスタンスを作成
fetcher = EventFetcher("https://gamma-api.polymarket.com")

# すべてのイベントを格納するリスト
all_events = []

LIMIT = 100
MAX_EVENTS = 20000

class RangeFormatter:
    def __init__(self, n):
        self.n = n

    def format_range(self):
        start = self.n * LIMIT
        end = (self.n + 1) * LIMIT
        return f"Range: {start}-{end}"

    def __format__(self, format_spec):
        return self.format_range()

range_formatter = RangeFormatter(0)
for i in tqdm(range(0, MAX_EVENTS, LIMIT), 
             desc="Fetching events", 
             bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [eta {remaining}] ({postfix})',
             postfix=range_formatter):
    range_formatter.n = i // LIMIT
    events = fetcher.fetch_events(offset=i, limit=LIMIT)
    if events == []:
        print("No more events to fetch")
        break
    all_events.extend(events)

# 結合したイベントデータをJSONファイルとして保存
print("Summary of fetched events:")
print(f"Total events fetched: {len(all_events)}")
print(f"Oldest event fetched: {all_events[0]['createdAt']}")
print(f"Newest event fetched: {all_events[-1]['createdAt']}")
# marketsキーの存在チェックを追加
total_markets = sum([len(event.get('markets', [])) for event in all_events])
print(f"Total markets fetched: {total_markets}")
create_json_file(all_events, "events")