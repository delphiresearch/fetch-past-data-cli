import os
import sys
import json
import argparse
from datetime import datetime

# 外部ライブラリ
from dotenv import load_dotenv
from supabase import create_client, Client
from tqdm import tqdm
from tabulate import tabulate
from termcolor import colored
import pandas as pd

# プロジェクトルートディレクトリをPythonパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory

# .env読込
load_dotenv()

# Supabaseクライアント初期化
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key) if url and key else None

def compare_with_supabase(local_event_data, input_event_index, supabase_client: Client):
    """
    指定したイベントインデックスのローカルデータとSupabase上のデータを比較表示する関数。
    """
    try:
        event = local_event_data[input_event_index]
        print(colored(f"\n--- Test Contents [{input_event_index}] ---", 'white', attrs=['bold']))
        print(colored("Test\t\t\tSupabase\tLocalDB\t\tResult", 'white'))
        # イベントIDでSupabaseのeventsテーブルと比較
        event_db = supabase_client.table('events').select('*').eq('id', int(event['id'])).execute()
        event_match = (len(event_db.data) == 1)
        print(f"Event ID\t\t{event['id']}\t\t{event_db.data[0]['id']}\t\t", end='')
        print(colored("●", 'green') if event_match else colored("●", 'red'))

        # マーケット単位で比較
        for i, market in enumerate(event['markets']):
            market_db = supabase_client.table('markets').select('*').eq('event_id', event['id']).execute()
            market_match = (len(market_db.data) == len(event['markets']))
            print(f"Market [{market['id']}]\t\t{market_db.data[0]['id']}\t\t{event['markets'][i]['id']}\t\t", end='')
            print(colored("●", 'green') if market_match else colored("●", 'red'))
            # Price History比較
            local_price_history = (json.loads(fetch_all_pricehistory(market)))["history"]

            
            supabase_price_db = supabase_client.table('prices').select('*').eq('market_id', market['id']).execute()
            price_match = (len(local_price_history) == len(supabase_price_db.data))
            print(f"Price History\t\t{len(supabase_price_db.data)}\t\t{len(local_price_history)}\t\t", end='')
            print(colored("●", 'green') if price_match else colored("●", 'red'))

            price_match = (supabase_price_db.data[0]['timestamp'] == local_price_history[0]['t'])
            print(f"Price History[0]\t{supabase_price_db.data[0]['timestamp']}\t{local_price_history[0]['t']}\t", end='')
            print(colored("●", 'green') if price_match else colored("●", 'red'))
   
    except IndexError:
        print(colored(f"Error: Event index {input_event_index} not found in the events array", 'red'))
    except KeyError as e:
        print(colored(f"Error: Missing required field in event data - {e}", 'red'))

def display_event_structure(start_index, end_index, check_supabase=False):
    """
    指定範囲のイベント情報を表示するメイン関数。
    check_supabase=Trueの場合はSupabaseと比較表示を行う。
    """
    # events.json 読込 (相対パスは環境に応じて調整)
    with open("../gamma/output/events.json", "r") as f:
        local_event_data = json.load(f)
    total_markets = 0
    total_price_history = 0
    event_indices = range(start_index, end_index + 1)
    table_data = []
    previous_event_id = None
    for input_event_index in event_indices:
        try:
            event = local_event_data[input_event_index]
            current_event_id = event['id']
            display_event_id = current_event_id if current_event_id != previous_event_id else ''

            # イベント行
            event_row = [
                input_event_index,            # インデックス
                display_event_id,       # イベントID (重複は非表示)
                '-',                    # Market IDは後でマーケット行で出力
                '-',                    # Price length (ここは'-'で埋め)
                event.get('title', 'N/A')  # タイトル
            ]
            table_data.append(event_row)

            # マーケット情報表示
            for i, market in enumerate(event['markets']):
                local_price_history = fetch_all_pricehistory(market)
                total_markets += 1
                total_price_history += len(local_price_history)

                market_row = [
                    '',                   # インデックス重複時は空白
                    '',                   # 同じイベントIDなので空白
                    market['id'],
                    len(local_price_history),
                    market.get('question', 'N/A')
                ]
                table_data.append(market_row)

            previous_event_id = current_event_id

            # Supabaseチェックオプションが有効な場合
            if check_supabase and supabase:
                compare_with_supabase(local_event_data, input_event_index, supabase)
            elif check_supabase and not supabase:
                print(colored("Warning: Supabase client not configured. Skipping comparison.", 'yellow'))

        except KeyError as e:
            print(colored(f"Error: Missing field in event data - {e}", 'red'))
        except IndexError:
            print(colored(f"Error: Event index {input_event_index} not found", 'red'))

    # Supabase用のテーブル表示 (価格比較テーブル)
    if check_supabase and supabase:
        supabase_table_data = []
        for input_event_index in event_indices:
            try:
                event = local_event_data[input_event_index]
                for market in event['markets']:
                    local_price_history = fetch_all_pricehistory(market)
                    supabase_price_db = supabase.table('prices').select('*').eq('market_id', market['id']).execute()

                    row = [
                        input_event_index,
                        event['id'],
                        market['id'],
                        len(supabase_price_db.data),
                        len(local_price_history),
                        event.get('title', 'N/A')[:15]
                    ]
                    supabase_table_data.append(row)
            except KeyError as e:
                print(colored(f"Error: Missing data - {e}", 'red'))
            except IndexError:
                print(colored(f"Error: Event index {input_event_index} not found", 'red'))

        print(colored("\nSupabase Comparison:", 'white', attrs=['bold']))
        print(tabulate(supabase_table_data,
                      headers=['Index', 'Event ID', 'Market ID', 'Supabase (Price Count)',
                               'LocalDB (Price History)', 'Title[:15]'],
                      tablefmt='grid'))
    else:
        # デフォルト表示 (SupabaseオプションOFFの場合)
        print(colored("\nEvent Structure:", 'white', attrs=['bold']))
        print(tabulate(table_data,
                        headers=['Index', 'Event ID', 'Market ID', 'Price Length', 'Title/Question'],
                        tablefmt='grid'))
    # サマリー表示
    summary_data = [
        ['Total Events', len(event_indices)],
        ['Total Markets', total_markets],
        ['Total Price History', total_price_history]
    ]
    print(colored("\nSummary:", 'white', attrs=['bold']))
    print(tabulate(summary_data,
                  headers=['Metric', 'Count'],
                  tablefmt='grid'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Display and optionally compare event structure with Supabase')
    parser.add_argument('--start', type=int, required=True, help='Start index of events')
    parser.add_argument('--end', type=int, required=True, help='End index of events')
    parser.add_argument('--supabase', action='store_true', help='Compare events with Supabase data')

    args = parser.parse_args()

    display_event_structure(args.start, args.end, check_supabase=args.supabase)
