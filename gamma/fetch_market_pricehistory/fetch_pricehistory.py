import sys
import os
import json
import time
import csv
import datetime
from tqdm import tqdm
from requests.exceptions import RequestException
# Add the path to the project's root directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from gamma.lib.pricehistory import PriceHistoryFetcher
from gamma.lib.create_json import create_json_file
from gamma.lib.logger import setup_logger

def validate_market_fields(market, logger):
    """
    Validate required fields in market data
    
    Args:
        market (dict): Market data to validate
        logger: Logger instance
    
    Returns:
        bool: True if all fields are valid
    """
    required_fields = ['clobTokenIds', 'startDate', 'endDate']
    
    for field in required_fields:
        if field not in market or market[field] is None:
            logger.error(f"[Marketid]:{market['id']} - Error: Missing {field} field")
            return False
    
    return True


def fetch_closed_market_pricehistory(pricehistory_fetcher, market, clobTokenIds, logger):
    # print(f'fetch market : {market["id"]} - {market["question"]}')
    # print(market['startDate'], market['endDate'], market['updatedAt'], market['createdAt'],market['closedTime'] ,market['id'])
    try:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%s'))
    except ValueError:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%SZ').strftime('%s'))
    
    res = pricehistory_fetcher.fetch_pricehistory(market=clobTokenIds, start_ts=start_unix)
    if len(res['history']) > 0:
        with open('closed_exists.csv', 'a') as f:
            f.write(f"{market['id']},{market['startDate']},{market['endDate']},{market['createdAt']}\n")
        return res
    else:
        with open('closed_no.csv', 'a') as f:
            f.write(f"{market['id']},{market['startDate']},{market['endDate']},{market['createdAt']}\n")
        return None

def fetch_open_market_pricehistory(pricehistory_fetcher, market, clobTokenIds, logger):
    try:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%s'))
    except ValueError:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%SZ').strftime('%s'))

    current_unix = int(time.time())
    time_diff = current_unix - start_unix

    # 時間の定数（秒単位）
    HOUR = 3600
    HOURS_6 = HOUR * 6
    DAY = HOUR * 24
    WEEK = DAY * 7    # 168時間
    MONTH = DAY * 30  # 720時間

    fidelity_list = [1, 5, 15, 30, 60, 720]
    interval_list = ['1h', '6h','1d', '1w', '1m', 'max']
    # 時間差に基づいてintervalを決定
    if time_diff < HOUR:
        fidelity = fidelity_list[0]
        interval = interval_list[0]
    elif HOUR < time_diff < HOURS_6:
        fidelity = fidelity_list[0]
        interval = interval_list[1]
    elif HOURS_6 < time_diff < DAY:
        fidelity = fidelity_list[1]
        interval = interval_list[2]
    elif DAY < time_diff < WEEK:
        # 15分と30分のfidelityを試して、より多くのデータポイントを持つ方を選択
        test_fidelities = [fidelity_list[2], fidelity_list[3]]  # [15, 30]
        max_history_length = 0
        best_fidelity = None
        
        for test_fidelity in test_fidelities:
            test_res = pricehistory_fetcher.fetch_pricehistory(
                market=clobTokenIds,
                interval=interval_list[3],  # '1w'
                fidelity=test_fidelity
            )
            
            if test_res.get('history') is not None and len(test_res['history']) > max_history_length:
                max_history_length = len(test_res['history'])
                best_fidelity = test_fidelity
                res = test_res
        
        fidelity = best_fidelity if best_fidelity is not None else fidelity_list[3]
        interval = interval_list[3]
    elif WEEK < time_diff < MONTH:
        fidelity = fidelity_list[4]
        interval = interval_list[4]
    elif MONTH < time_diff:
        fidelity = fidelity_list[5]
        interval = interval_list[5]


    max_history_length = 0
    best_fidelity = None
    best_history = None

    # 全てのfidelityを試す

    res = pricehistory_fetcher.fetch_pricehistory(
        market=clobTokenIds, 
        interval=interval, 
        fidelity=fidelity
    )
    if res.get('history') is not None:
        return res
        # tqdm.write(f"Market ID: {market['id']} - interval: {interval} - Start timestamp: {start_unix} - Fidelity: {fidelity} - price_history_length: {len(res['history'])} - opening hours: {time_diff / HOUR} hours")
    else:
        return None
    # csv_data = [market['id'], interval, start_unix, best_fidelity, time_diff / HOUR]

    # with open('output.csv', 'a', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(csv_data)

def fetch_pricehistory(market, clobTokenIds, logger, max_retries=3, retry_delay=5):
    """
    Args:
        market: マーケットデータ
        clobTokenIds: CLOB トークンID
        logger: ロガーインスタンス
        max_retries: 最大リトライ回数(デフォルト:3)
        retry_delay: リトライ間の待機時間(秒)(デフォルト：5)
    """
    pricehistory_fetcher = PriceHistoryFetcher("https://clob.polymarket.com")
    if market['active'] == True and market['archived'] == False:
        if validate_market_fields(market, logger):
            for attempt in range(max_retries):
                try:
                    if market['closed'] == True:
                        return fetch_closed_market_pricehistory(pricehistory_fetcher, market, clobTokenIds, logger)
                    elif market['closed'] == False:
                        return fetch_open_market_pricehistory(pricehistory_fetcher, market, clobTokenIds, logger)
                    return  # 成功した場合はループを抜ける
                    
                except (RequestException, Exception) as e:
                    if attempt == max_retries - 1:  # 最後の試行の場合
                        logger.error(f"Market ID: {market['id']} - Failed after {max_retries} attempts: {str(e)}")
                        raise  # 最後の試行で失敗した場合は例外を再度発生させる
                    else:
                        logger.warning(f"Market ID: {market['id']} - Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)  # 次の試行までの待機
        else:
            logger.error(f"Market ID: {market['id']} - Market is not active or archived")
            return None
    else:
        logger.error(f"Market ID: {market['id']} - Market is not active or archived")
        return None


# if __name__ == "__main__":
#     # Setup logger for missing clobTokenIds
#     logger = setup_logger("error_log")
    
#     project_root = os.path.dirname(os.path.dirname(current_dir))
#     file_path = os.path.join(project_root, "gamma", "output", "events.json")
    
#     with open(file_path, 'r', encoding='utf-8') as f:
#         events_data = json.load(f)
    
#     # tqdmを使用して進捗バーを表示
#     for event in tqdm(events_data, desc="Processing events"):
#         if event.get('markets') is not None:
#             for market in event['markets']:
#                 if market['active'] == True and market['archived'] == False:
#                     if validate_market_fields(market, logger):
#                         clobTokenIds = json.loads(market['clobTokenIds'])[0]
#                         # 現在のmarket idを進捗バーに表示
#                         fetch_pricehistory(market, clobTokenIds, logger)
                       

