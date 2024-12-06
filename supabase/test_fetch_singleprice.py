import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm  # tqdmライブラリをインポート

# プロジェクトのルートディレクトリをPythonパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 一つ上のディレクトリ
sys.path.append(project_root)
import json
from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory


with open("../gamma/output/events.json", "r") as f:
    event_data = json.load(f)

print("title", event_data[4076]['markets'][0]['question'], len(fetch_all_pricehistory(event_data[3000]['markets'][0])))
