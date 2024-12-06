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


def fetch_closed_market_pricehistory(pricehistory_fetcher, market, clobTokenIds):
    try:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%s'))
    except ValueError:
        start_unix = int(datetime.datetime.strptime(market['startDate'], '%Y-%m-%dT%H:%M:%SZ').strftime('%s'))
    
    res = pricehistory_fetcher.fetch_pricehistory(market=clobTokenIds, start_ts=start_unix)
    if res.get('history') is not None and len(res['history']) > 0:
        return res
    else:
        return None

def fetch_open_market_pricehistory(pricehistory_fetcher, market, clobTokenIds):
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
    if res.get('history') is not None and len(res['history']) > 0:
        return res
    else:
        return None


def fetch_pricehistory(market, clobTokenIds, logger, max_retries=3, retry_delay=5):
    """
    Args:
        market: マーケットデータ
        clobTokenIds: CLOB トークンID
        max_retries: 最大リトライ回数(デフォルト:3)
        retry_delay: リトライ間の待機時間(秒)(デフォルト：5)
    """
    pricehistory_fetcher = PriceHistoryFetcher("https://clob.polymarket.com")
    
    for attempt in range(max_retries):
        try:
            if market['closed'] == True:
                return fetch_closed_market_pricehistory(pricehistory_fetcher, market, clobTokenIds)
            elif market['closed'] == False:
                return fetch_open_market_pricehistory(pricehistory_fetcher, market, clobTokenIds)
            return  # 成功した場合はループを抜ける
            
        except (RequestException, Exception) as e:
            if attempt == max_retries - 1:  # 最後の試行の場合
                logger.error(f"Market ID: {market['id']} - Failed after {max_retries} attempts: {str(e)}")
                raise  # 最後の試行で失敗した場合は例外を再度発生させる
            else:
                logger.warning(f"Market ID: {market['id']} - Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # 次の試行までの待機


def fetch_all_pricehistory(market):
    logger = setup_logger("error_log")
    try:
        if market['active'] == True and market['archived'] == False:
            if validate_market_fields(market, logger):
                clobTokenIds = json.loads(market['clobTokenIds'])[0]
                res = fetch_pricehistory(market, clobTokenIds, logger)
                # print(f"Market ID: {market['id']} marketStartDate: {market['startDate']} - Fetching price history: {res}")
                if res is not None:
                    return json.dumps(res)
                else:
                    return None
    except Exception as e:
        logger.error(f"Market ID: {market['id']} - Market is not active or archived: {str(e)}")
        return None
            

