import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import logging
from datetime import datetime
import argparse
from time import time, sleep
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import queue
from functools import partial
import backoff
from typing import Optional
import traceback
import subprocess
import pathlib
import threading

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CONFIG = {
    'MAX_DB_CONNECTIONS': 20,
    'PRICE_BATCH_SIZE': 100000,
    'MARKET_WORKERS': 5,
    'EVENT_WORKERS': 3,
    'MAX_RETRIES': 3,
    'LOG_LEVEL': logging.INFO,
    'BATCH_RETRY_TIMES': 3  # バッチ失敗時の再試行回数
}

def setup_logging():
    log_dir = pathlib.Path('log')
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f'sync_log_{timestamp}.log'
    error_log_filename = log_dir / f'error_log_{timestamp}.log'
    
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(CONFIG['LOG_LEVEL'])
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    
    class ConditionalFileHandler(logging.FileHandler):
        def __init__(self, filename, mode='a', encoding=None, delay=True):
            super().__init__(filename, mode, encoding, delay)
            self._created = False
        def emit(self, record):
            if not self._created and record.levelno >= logging.ERROR:
                self._created = True
                super().emit(record)
            elif self._created:
                super().emit(record)
    
    error_handler = ConditionalFileHandler(error_log_filename)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s\n'
        'Message: %(message)s\n'
        'Stack Trace: %(exc_info)s\n'
        '-------------------\n'
    ))
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    logger = logging.getLogger(__name__)
    logger.setLevel(CONFIG['LOG_LEVEL'])
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    try:
        ping_result = subprocess.run(['ping', '-c', '3', '8.8.8.8'], capture_output=True, text=True)
        logger.info(f"Network Speed Test:\n{ping_result.stdout}")
    except Exception as e:
        logger.warning(f"Failed to measure network speed: {e}")
    
    logger.info(f"@config,{','.join(f'{k}={v}' for k, v in CONFIG.items())}")
    return logger

logger = setup_logging()

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=CONFIG['MAX_RETRIES'],
    on_backoff=lambda details: logger.error(
        f"Retry attempt {details['tries']} after {details['wait']} seconds"
    )
)
def retry_wrapper(func, *args, **kwargs):
    return func(*args, **kwargs)

def insert_events(event_data):
    return supabase.table("events").upsert({
        "id": event_data["id"],
        "ticker": event_data.get("ticker"),
        "slug": event_data.get("slug"),
        "title": event_data.get("title"),
        "description": event_data.get("description"),
        "resolution_source": event_data.get("resolutionSource"),
        "start_date": event_data.get("startDate"),
        "creation_date": event_data.get("creationDate"),
        "end_date": event_data.get("endDate"),
        "image": event_data.get("image"),
        "icon": event_data.get("icon"),
        "active": event_data.get("active"),
        "closed": event_data.get("closed"),
        "archived": event_data.get("archived"),
        "new": event_data.get("new"),
        "featured": event_data.get("featured"),
        "restricted": event_data.get("restricted"),
        "liquidity": event_data.get("liquidity"),
        "volume": event_data.get("volume"),
        "open_interest": event_data.get("openInterest"),
        "sort_by": event_data.get("sortBy"),
        "created_at": event_data.get("createdAt"),
        "updated_at": event_data.get("updatedAt"),
        "competitive": event_data.get("competitive"),
        "volume_24hr": event_data.get("volume24hr"),
        "enable_order_book": event_data.get("enableOrderBook"),
        "liquidity_clob": event_data.get("liquidityClob"),
        "_sync": event_data.get("_sync"),
        "neg_risk": event_data.get("negRisk"),
        "neg_risk_market_id": event_data.get("negRiskMarketID"),
        "comment_count": event_data.get("commentCount"),
        "cyom": event_data.get("cyom"),
        "show_all_outcomes": event_data.get("showAllOutcomes"),
        "show_market_images": event_data.get("showMarketImages"),
        "enable_neg_risk": event_data.get("enableNegRisk"),
        "gmp_chart_mode": event_data.get("gmpChartMode"),
        "neg_risk_augmented": event_data.get("negRiskAugmented")
    }).execute()

def insert_market(market_data, event_id):
    return supabase.table("markets").upsert({
        "id": market_data["id"],
        "event_id": event_id,
        "question": market_data.get("question"),
        "condition_id": market_data.get("conditionId"),
        "slug": market_data.get("slug"),
        "resolution_source": market_data.get("resolutionSource"),
        "end_date": market_data.get("endDate"),
        "liquidity": market_data.get("liquidity"),
        "start_date": market_data.get("startDate"),
        "image": market_data.get("image"),
        "icon": market_data.get("icon"),
        "description": market_data.get("description"),
        "outcomes": json.loads(market_data.get("outcomes")) if isinstance(market_data.get("outcomes"), str) else market_data.get("outcomes", None),
        "outcome_prices": json.loads(market_data.get("outcomePrices")) if isinstance(market_data.get("outcomePrices"), str) else market_data.get("outcomePrices", None),
        "volume": market_data.get("volume"),
        "active": market_data.get("active"),
        "closed": market_data.get("closed"),
        "market_maker_address": market_data.get("marketMakerAddress"),
        "created_at": market_data.get("createdAt"),
        "updated_at": market_data.get("updatedAt"),
        "new": market_data.get("new"),
        "featured": market_data.get("featured"),
        "submitted_by": market_data.get("submitted_by"),
        "archived": market_data.get("archived"),
        "resolved_by": market_data.get("resolvedBy"),
        "restricted": market_data.get("restricted"),
        "group_item_title": market_data.get("groupItemTitle"),
        "group_item_threshold": market_data.get("groupItemThreshold"),
        "question_id": market_data.get("questionID"),
        "enable_order_book": market_data.get("enableOrderBook"),
        "order_price_min_tick_size": market_data.get("orderPriceMinTickSize"),
        "order_min_size": market_data.get("orderMinSize"),
        "volume_num": market_data.get("volumeNum"),
        "liquidity_num": market_data.get("liquidityNum"),
        "end_date_iso": market_data.get("endDateIso"),
        "start_date_iso": market_data.get("startDateIso"),
        "has_reviewed_dates": market_data.get("hasReviewedDates"),
        "volume_24hr": market_data.get("volume24hr"),
        "clob_token_ids": json.loads(market_data.get("clobTokenIds")) if isinstance(market_data.get("clobTokenIds"), str) else market_data.get("clobTokenIds", None),
        "uma_bond": market_data.get("umaBond"),
        "uma_reward": market_data.get("umaReward"),
        "volume_24hr_clob": market_data.get("volume24hrClob"),
        "volume_clob": market_data.get("volumeClob"),
        "liquidity_clob": market_data.get("liquidityClob"),
        "accepting_orders": market_data.get("acceptingOrders"),
        "neg_risk": market_data.get("negRisk"),
        "neg_risk_market_id": market_data.get("negRiskMarketID"),
        "neg_risk_request_id": market_data.get("negRiskRequestID"),
        "_sync": market_data.get("_sync"),
        "ready": market_data.get("ready"),
        "funded": market_data.get("funded"),
        "accepting_orders_timestamp": market_data.get("acceptingOrdersTimestamp"),
        "cyom": market_data.get("cyom"),
        "competitive": market_data.get("competitive"),
        "pager_duty_notification_enabled": market_data.get("pagerDutyNotificationEnabled"),
        "approved": market_data.get("approved"),
        "clob_rewards": market_data.get("clobRewards"),
        "rewards_min_size": market_data.get("rewardsMinSize"),
        "rewards_max_spread": market_data.get("rewardsMaxSpread"),
        "spread": market_data.get("spread"),
        "one_day_price_change": market_data.get("oneDayPriceChange"),
        "last_trade_price": market_data.get("lastTradePrice"),
        "best_bid": market_data.get("bestBid"),
        "best_ask": market_data.get("bestAsk"),
        "automatically_active": market_data.get("automaticallyActive", None),
        "clear_book_on_start": market_data.get("clearBookOnStart"),
        "series_color": market_data.get("seriesColor"),
        "show_gmp_series": market_data.get("showGmpSeries"),
        "show_gmp_outcome": market_data.get("showGmpOutcome"),
        "manual_activation": market_data.get("manualActivation"),
        "neg_risk_other": market_data.get("negRiskOther")
    }).execute()

def insert_tags(event_id, tags_data):
    if not tags_data:
        return
    for tag in tags_data:
        tag_id = tag.get("id")
        supabase.table("tags").upsert({
            "event_id": event_id,
            "id": int(tag_id) if tag_id is not None else None,
            "label": tag.get("label"),
            "slug": tag.get("slug"),
            "force_show": tag.get("forceShow"),
            "published_at": tag.get("publishedAt"),
            "updated_by": tag.get("updatedBy"),
            "created_at": tag.get("createdAt"),
            "updated_at": tag.get("updatedAt"),
            "_sync": tag.get("_sync"),
            "force_hide": tag.get("forceHide")
        }).execute()

def try_insert_price_history_batch(price_records: list, attempt=1, max_attempts=3):
    logger.debug(f"Attempt {attempt}/{max_attempts} to insert {len(price_records)} price records")
    try:
        local_supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        local_supabase.table("prices").upsert(price_records).execute()
        return True
    except Exception as e:
        logger.error(f"Price insert attempt {attempt} failed: {e}")
        if attempt < max_attempts:
            # バッチサイズ半減して再試行
            half = len(price_records)//2 or 1
            # 半分に割って2回試行
            if len(price_records) > 1:
                return (try_insert_price_history_batch(price_records[:half], attempt+1, max_attempts) and 
                        try_insert_price_history_batch(price_records[half:], attempt+1, max_attempts))
            else:
                return False
        else:
            return False

def insert_price_history_batch(price_records: list):
    logger = logging.getLogger(__name__)
    start_time = time()

    # 大きなバッチを再試行＆分割で対応
    if try_insert_price_history_batch(price_records, 1, CONFIG['BATCH_RETRY_TIMES']):
        total_time = time() - start_time
        logger.info(
            f"@batch_complete,total_records={len(price_records)},"
            f"success_batches=1/1,total_duration={total_time:.3f}"
        )
    else:
        logger.error(f"@batch_fatal_error,failed to insert {len(price_records)} price records after retries.")

    with price_bar_lock:
        price_bar.update(len(price_records))

def process_price_history(market_id: str, price_history_data: dict) -> list:
    price_records = []
    if price_history_data and "history" in price_history_data:
        for price_point in price_history_data["history"]:
            price_records.append({
                "market_id": market_id,
                "timestamp": price_point["t"],
                "price": float(price_point["p"])
            })
    return price_records

processing_lock = threading.Lock()

main_bar = None
market_bar = None
price_bar = None
price_bar_lock = threading.Lock()

def process_market(market, event_id):
    retry_wrapper(insert_market, market, event_id)
    price_history_str = retry_wrapper(fetch_all_pricehistory, market)
    if price_history_str:
        price_history = json.loads(price_history_str)
        price_records = process_price_history(market["id"], price_history)
        return {
            'market_id': market['id'],
            'price_records': price_records,
            'status': 'success'
        }
    return {
        'market_id': market['id'],
        'status': 'no_data'
    }

def process_single_event(event, shared_batch_queue: queue.Queue):
    with processing_lock:
        main_bar.set_postfix_str(f"Processing event={event['id']}...")
        main_bar.refresh()

    retry_wrapper(insert_events, event)
    if "tags" in event and event["tags"] is not None:
        insert_tags(event["id"], event["tags"])

    markets = event.get('markets', [])
    if not markets:
        logger.error(f"Event {event['id']} skipped: No markets data found in the event")
        main_bar.update(1)
        market_bar.update(0)
        return {
            'event_id': event['id'],
            'status': 'skipped',
            'stats': {
                'processed_markets': 0,
                'failed_markets': 1,
                'price_records': 0
            }
        }

    local_stats = {
        'processed_markets': 0,
        'failed_markets': 0,
        'price_records': 0
    }
    
    with ThreadPoolExecutor(max_workers=CONFIG['MARKET_WORKERS']) as market_executor:
        futures = {market_executor.submit(process_market, m, event['id']): m for m in markets}
        local_batch = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result and result.get('status') == 'success':
                local_stats['processed_markets'] += 1
                local_batch.extend(result['price_records'])
                local_stats['price_records'] += len(result['price_records'])
                market_bar.update(1)
            else:
                local_stats['failed_markets'] += 1
                market_bar.update(1)

            with processing_lock:
                main_bar.set_postfix_str(f"Processed {local_stats['processed_markets']} markets for event={event['id']}...still working")
                main_bar.refresh()

        if local_batch:
            shared_batch_queue.put(local_batch)

    return {
        'event_id': event['id'],
        'status': 'success',
        'stats': local_stats
    }

def batch_worker(shared_batch_queue: queue.Queue, stats: dict):
    logger = logging.getLogger(__name__)
    while True:
        try:
            batch = shared_batch_queue.get(timeout=5)
            if batch is None:
                # センチネルを受け取った場合終了
                shared_batch_queue.task_done()
                break
            with processing_lock:
                main_bar.set_postfix_str("Inserting price records batch...")
                main_bar.refresh()
            try:
                insert_price_history_batch(batch)
                stats['processed_price_records'] += len(batch)
            finally:
                shared_batch_queue.task_done()
        except queue.Empty:
            # キューが一時的に空だが、イベント処理中かもしれないので少し待つ
            # 定期的にステータス更新
            with processing_lock:
                main_bar.set_postfix_str("No tasks in queue...waiting")
                main_bar.refresh()
            sleep(2)  # 適度に待つ
            continue

def process_event_range(start_idx: int, end_idx: int, events_data: list):
    logger = logging.getLogger(__name__)
    start_time = time()

    # Validate events data
    if not events_data:
        logger.error("No events data provided")
        return {
            'processed_events': 0,
            'processed_markets': 0,
            'processed_price_records': 0,
            'failed_markets': 0,
            'failed_events': len(events_data),
            'execution_time': 0
        }

    stats = {
        'processed_events': 0,
        'processed_markets': 0,
        'processed_price_records': 0,
        'failed_markets': 0,
        'failed_events': 0
    }

    # Filter out events without markets before processing
    target_events = events_data[start_idx:end_idx+1]
    valid_events = []
    for event in target_events:
        if not event.get('markets'):
            logger.warning(f"Event {event.get('id')} skipped: No markets data found")
            stats['failed_events'] += 1
            continue
        valid_events.append(event)

    if not valid_events:
        logger.error("No valid events to process")
        return {**stats, 'execution_time': time() - start_time}

    total_markets = sum(len(e.get('markets', [])) for e in valid_events)

    shared_batch_queue = queue.Queue()

    global main_bar, market_bar, price_bar
    main_bar = tqdm(total=len(valid_events), desc="Events", position=0, leave=True)
    market_bar = tqdm(total=total_markets, desc="Markets", position=1, leave=True)
    price_bar = tqdm(desc="Price Records", position=2, leave=True)

    with processing_lock:
        main_bar.set_postfix_str("Starting...")
        main_bar.refresh()

    # バッチワーカー起動
    batch_executor = ThreadPoolExecutor(max_workers=4)
    batch_futures = [batch_executor.submit(batch_worker, shared_batch_queue, stats) for _ in range(4)]

    # イベント並列処理
    with ThreadPoolExecutor(max_workers=CONFIG['EVENT_WORKERS']) as event_executor:
        futures = {
            event_executor.submit(process_single_event, event, shared_batch_queue): event
            for event in valid_events
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result['status'] == 'success':
                stats['processed_events'] += 1
                stats['processed_markets'] += result['stats']['processed_markets']
                stats['failed_markets'] += result['stats']['failed_markets']
                stats['processed_price_records'] += result['stats']['price_records']
            else:
                stats['failed_events'] += 1
            main_bar.update(1)
            with processing_lock:
                main_bar.set_postfix_str("Waiting for completion...")
                main_bar.refresh()

    # 全イベント処理終了。もう新たなタスクは来ないのでセンチネルを投入
    for _ in range(4):
        shared_batch_queue.put(None)

    # 全バッチ完了待ち
    shared_batch_queue.join()
    batch_executor.shutdown(wait=True)

    execution_time = time() - start_time

    with processing_lock:
        main_bar.set_postfix_str("All tasks completed!")
        main_bar.refresh()

    main_bar.close()
    market_bar.close()
    price_bar.close()

    logger.info(f"Process complete - Execution time: {execution_time:.2f}s - Stats: {stats}")
    print(f"All tasks completed successfully in {execution_time:.2f}s - Stats: {stats}")

    if execution_time > 300:
        print("Process took quite long. Consider adjusting PRICE_BATCH_SIZE or MARKET_WORKERS.")

    return {**stats, 'execution_time': execution_time}

def main():
    parser = argparse.ArgumentParser(description='イベントデータ同期スクリプト')
    parser.add_argument('--start', type=int, help='開始インデックス')
    parser.add_argument('--end', type=int, help='終了インデックス')
    args = parser.parse_args()

    try:
        with open("../gamma/output/events.json", "r") as f:
            events_data = json.load(f)
        start_idx = args.start if args.start is not None else 0
        end_idx = args.end if args.end is not None else len(events_data) - 1

        results = process_event_range(start_idx, end_idx, events_data)
        logger.info(f"Final Result: {results}")

    except Exception as e:
        logger.error(f"Main processing error: {str(e)}")
        print(f"Main processing error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
