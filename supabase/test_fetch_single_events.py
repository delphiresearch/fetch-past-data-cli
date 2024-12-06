import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
import argparse
from tqdm import tqdm
from tabulate import tabulate
from termcolor import colored
import pandas as pd
# プロジェクトのルートディレクトリをPythonパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)


def compare_with_supabase(event_data, event_index, supabase):
    try:
        event = event_data[event_index]
        
        print(colored(f"\n--- Test Contents [{event_index}] ---", 'white', attrs=['bold']))
        print(colored("Test内容\tSupabase\tLocalDB\t結果", 'white'))
        
        # イベントの比較
        event_db = supabase.table('events').select('*').eq('id', int(event['id'])).execute()
        event_match = len(event_db.data) == 1
        
        print(f"Event ID\t1\t1\t", end='')
        print(colored("●", 'green') if event_match else colored("●", 'red'))
        
        # マーケットの比較
        for i, market in enumerate(event['markets']):
            market_db = supabase.table('markets').select('*').eq('event_id', event['id']).execute()
            market_match = len(market_db.data) == len(event['markets'])
            
            print(f"Market [{market['id']}]\t{len(market_db.data)}\t{len(event['markets'])}\t", end='')
            print(colored("●", 'green') if market_match else colored(f"●", 'red'))
            
            # Price History
            price_history = fetch_all_pricehistory(market)
            price_db = supabase.table('prices').select('*').eq('market_id', market['id']).execute()
            price_match = len(price_history) == len(price_db.data)
            
            print(f"Price History\t{len(price_db.data)}\t{len(price_history)}\t", end='')
            print(colored("●", 'green') if price_match else colored(f"●", 'red'))
            
        print(colored("---", 'white'))
        
    except IndexError:
        print(colored(f"Error: Event index {event_index} not found in the events array", 'red'))
    except KeyError as e:
        print(colored(f"Error: Missing required field in event data - {e}", 'red'))

def display_event_structure(start_index, end_index, check_supabase=False):
    with open("../gamma/output/events.json", "r") as f:
        event_data = json.load(f)
    
    total_markets = 0
    total_price_history = 0
    event_indices = range(start_index, end_index + 1)
    
    # テーブルデータの準備
    table_data = []
    previous_event_id = None
    
    # tqdmで進捗バーを表示
    for event_index in range(start_index, end_index + 1):
        try:
            event = event_data[event_index]
            current_event_id = event['id']
            
            # 前のイベントIDと同じ場合は空白を表示
            display_event_id = current_event_id if current_event_id != previous_event_id else ''
            
            event_row = [
                event_index,  # インデックスを追加
                display_event_id,
                '-',  # market_id
                '-',  # price_length
                event.get('title', 'N/A')  # titleフィールドがない場合は'N/A'
            ]
            table_data.append(event_row)
            
            total_markets += len(event['markets'])
            
            # マーケット情報を追加
            for i, market in enumerate(event['markets']):
                price_history = fetch_all_pricehistory(market)
                total_price_history += len(price_history)
                market_row = [
                    '',  # インデックスは空白
                    '',  # 同じイベントIDなので空白
                    market['id'],
                    len(price_history),
                    market['question']
                ]
                table_data.append(market_row)
            
            previous_event_id = current_event_id
            
            if check_supabase:
                results = compare_with_supabase(event_data, event_index, supabase)

                
        except KeyError as e:
            print(f"Error: Invalid event index or missing data - {e}")
        except IndexError:
            print(f"Error: Event index {event_index} not found")
    
    if check_supabase:
        # Supabaseモード用の新しいテーブルデータ
        supabase_table_data = []
        for event_index in range(start_index, end_index + 1):
            try:
                event = event_data[event_index]
                
                for market in event['markets']:
                    price_history = fetch_all_pricehistory(market)
                    price_db = supabase.table('prices').select('*').eq('market_id', market['id']).execute()
                    
                    row = [
                        event_index,
                        event['id'],
                        market['id'],
                        len(price_db.data),
                        len(price_history),
                        event.get('title', 'N/A')[:15]
                    ]
                    supabase_table_data.append(row)
                
            except KeyError as e:
                print(f"Error: Invalid event index or missing data - {e}")
            except IndexError:
                print(f"Error: Event index {event_index} not found")
        
        # Supabaseモード用のテーブル表示
        print("\nSupabase Comparison:")
        print(tabulate(supabase_table_data,
                      headers=['Index', 'Event ID', 'Market ID', 'Supabase (Price Length)',
                              'LocalDB (Price History)', 'Title'],
                      tablefmt='grid'))
    else:
        # 既存の表示ロジック
        print("\nEvent Structure:")
        print(tabulate(table_data,
                      headers=['Index', 'Event ID', 'Market ID', 'Price Length', 'Title/Question'],
                      tablefmt='grid'))
    
    # サマリーの表示
    summary_data = [
        ['Total Events', len(event_indices)],
        ['Total Markets', total_markets],
        ['Total Price History', total_price_history]
    ]
    print("\nSummary:")
    print(tabulate(summary_data, 
                  headers=['Metric', 'Count'], 
                  tablefmt='grid'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Display event structure')
    parser.add_argument('--start', type=int, help='Start index of events to analyze')
    parser.add_argument('--end', type=int, help='End index of events to analyze')
    parser.add_argument('--supabase', action='store_true', help='Compare with Supabase data')
    
    args = parser.parse_args()
    
    if args.start is not None and args.end is not None:
        display_event_structure(args.start, args.end, check_supabase=args.supabase)
    else:
        print("Please provide both --start and --end arguments")
