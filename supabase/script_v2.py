import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import logging
from datetime import datetime
import argparse
from time import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import queue
from functools import partial
import backoff
from typing import Optional
import traceback
import subprocess
import pathlib

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 一つ上のディレクトリ
sys.path.append(project_root)
from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory
# 環境変数の読み込み
load_dotenv()

# Supabase接続設定
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Supabaseクライアントの初期化
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 設定パラメータ
CONFIG = {
    # データベース接続設定
    'MAX_DB_CONNECTIONS': 20,  # データベース同時接続数
    
    # バッチ処理設定
    'PRICE_BATCH_SIZE': 5000,  # 価格データのバッチサイズ
    'MARKET_WORKERS': 100,     # マーケット処理の並列数
    
    # リトライ設定
    'MAX_RETRIES': 3,        # 最大リトライ回数
    
    # ログ設定
    'LOG_LEVEL': logging.INFO,
}

def setup_logging():
    """ログ設定の修正"""
    # ログディレクトリの作成
    log_dir = pathlib.Path('log')
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f'sync_log_{timestamp}.log'
    error_log_filename = log_dir / f'error_log_{timestamp}.log'
    
    # 通常のログハンドラー
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(CONFIG['LOG_LEVEL'])
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    
    # コラーログ用ハンドラー（エラー発生時のみ作成）
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
    
    # コンソールハンドラー（進捗のみ）
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    logger = logging.getLogger(__name__)
    logger.setLevel(CONFIG['LOG_LEVEL'])
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    # ネット速度の測定とログ記録
    try:
        ping_result = subprocess.run(['ping', '-c', '3', '8.8.8.8'], 
                                   capture_output=True, text=True)
        logger.info(f"Network Speed Test:\n{ping_result.stdout}")
    except Exception as e:
        logger.warning(f"Failed to measure network speed: {e}")
    
    # 設定情報をログに記録（1行で）
    logger.info(f"@config,{','.join(f'{k}={v}' for k, v in CONFIG.items())}")
    
    return logger

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=CONFIG['MAX_RETRIES'],
    on_backoff=lambda details: logging.error(
        f"Retry attempt {details['tries']} after {details['wait']} seconds"
    )
)
def retry_wrapper(func, *args, **kwargs):
    """リトライ機能付きの関数ラッパー"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger = logging.getLogger(__name__)
        error_details = {
            'error': str(e),
            'stack_trace': traceback.format_exc(),
            'extra': {
                'args': args,
                'kwargs': kwargs
            }
        }
        logger.error(
            f"Error in {func.__name__}",
            extra={
                'stack_trace': error_details['stack_trace'],
                'extra': json.dumps(error_details['extra'], default=str)
            }
        )
        raise

def insert_events(event_data):
    """イベントデータを挿入"""
    try:
        event_response = supabase.table("events").upsert({
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
            "tags": json.loads(event_data.get("tags")) if isinstance(event_data.get("tags"), str) else event_data.get("tags", None),
            "cyom": event_data.get("cyom"),
            "show_all_outcomes": event_data.get("showAllOutcomes"),
            "show_market_images": event_data.get("showMarketImages"),
            "enable_neg_risk": event_data.get("enableNegRisk"),
            "gmp_chart_mode": event_data.get("gmpChartMode"),
            "neg_risk_augmented": event_data.get("negRiskAugmented")
        }).execute()
        return event_response
    except Exception as e:
        print(f"Error inserting event {event_data.get('id')}: {str(e)}")
        raise

def insert_market(market_data, event_id):
    """マーケットデータを挿入"""
    try:
        market_response = supabase.table("markets").upsert({
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
        return market_response
    except Exception as e:
        print(f"Error inserting market {market_data.get('id')}: {str(e)}")
        raise

def insert_price_history_batch(price_records: list):
    """価格履歴データをバッチで並列挿入"""
    logger = logging.getLogger(__name__)
    start_time = time()
    
    # コネクションプールの設定
    MAX_WORKERS = CONFIG['MAX_DB_CONNECTIONS']
    sub_batch_size = CONFIG['PRICE_BATCH_SIZE']
    
    def process_batch(batch_number, sub_batch):
        batch_start = time()
        try:
            # 新しいクライアントインスタンスを作成
            local_supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            local_supabase.table("prices").upsert(sub_batch).execute()
            batch_time = time() - batch_start
            
            logger.info(
                f"@batch_insert,batch_number={batch_number},"
                f"record_count={len(sub_batch)},duration={batch_time:.3f}"
            )
            return True
            
        except Exception as e:
            logger.error(
                f"@batch_error,batch_number={batch_number},"
                f"record_count={len(sub_batch)},error={str(e)}"
            )
            return False

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # バッチを作成
            batches = [
                (i//sub_batch_size + 1, price_records[i:i + sub_batch_size])
                for i in range(0, len(price_records), sub_batch_size)
            ]
            
            # 並列実行
            futures = [
                executor.submit(process_batch, batch_num, batch)
                for batch_num, batch in batches
            ]
            
            # 結果を待機
            results = [future.result() for future in futures]
            
        success_count = sum(1 for r in results if r)
        total_time = time() - start_time
        
        logger.info(
            f"@batch_complete,total_records={len(price_records)},"
            f"success_batches={success_count}/{len(batches)},"
            f"total_duration={total_time:.3f}"
        )
                
    except Exception as e:
        logger.error(f"@batch_fatal_error,error={str(e)}")
        raise

def process_price_history(market_id: str, price_history_data: dict) -> list:
    """価格履歴データを処理してレコードのリストを返す"""
    price_records = []
    if price_history_data and "history" in price_history_data:
        for price_point in price_history_data["history"]:
            price_records.append({
                "market_id": market_id,
                "timestamp": price_point["t"],
                "price": float(price_point["p"])
            })
    return price_records

def process_market(market, event_id, market_pbar: Optional[tqdm] = None) -> dict:
    """マーケット処理の改善"""
    logger = logging.getLogger(__name__)
    start_time = time()
    
    try:
        # マーケットの挿入時間を記録
        market_insert_start = time()
        result = retry_wrapper(insert_market, market, event_id)
        market_insert_time = time() - market_insert_start
        
        logger.info(f"@market_insert,event_id={event_id},market_id={market['id']},duration={market_insert_time:.3f}")
        
        # 価格履歴の取得時間を記録
        fetch_start = time()
        price_history_str = retry_wrapper(fetch_all_pricehistory, market)
        fetch_time = time() - fetch_start
        
        logger.info(f"@price_history_fetch,event_id={event_id},market_id={market['id']},duration={fetch_time:.3f}")
        
        if price_history_str:
            # 価格履歴の処理時間を記録
            process_start = time()
            price_history = json.loads(price_history_str)
            price_records = process_price_history(market["id"], price_history)
            process_time = time() - process_start
            
            logger.info(
                f"@price_process,event_id={event_id},market_id={market['id']},"
                f"duration={process_time:.3f},record_count={len(price_records)}"
            )
            
            total_time = time() - start_time
            logger.info(
                f"@market_complete,event_id={event_id},market_id={market['id']},"
                f"total_duration={total_time:.3f},record_count={len(price_records)}"
            )
            
            if market_pbar:
                market_pbar.update(1)
                market_pbar.set_description(
                    f"Market {market['id']} ({len(price_records)} prices)"
                )
            
            return {
                'market_id': market['id'],
                'price_records': price_records,
                'timestamp_count': len(price_records),
                'status': 'success',
                'metrics': {
                    'market_insert_time': market_insert_time,
                    'fetch_time': fetch_time,
                    'process_time': process_time,
                    'total_time': total_time
                }
            }
        
        return {
            'market_id': market['id'],
            'status': 'no_data'
        }
        
    except Exception as e:
        total_time = time() - start_time
        logger.error(
            f"@market_error,event_id={event_id},market_id={market.get('id')},"
            f"duration={total_time:.3f},error={str(e)}"
        )
        return {
            'market_id': market['id'],
            'error': str(e),
            'status': 'error'
        }

def process_event_range(start_idx: int, end_idx: int, events_data: list):
    """イベント処理の改善 - イベントの並列処理を追加"""
    logger = logging.getLogger(__name__)
    start_time = time()
    
    # 統計情報の初期化
    stats = {
        'processed_events': 0,
        'processed_markets': 0,
        'processed_price_records': 0,
        'failed_markets': 0,
        'failed_events': 0
    }
    
    target_events = events_data[start_idx:end_idx+1]
    shared_batch_queue = queue.Queue()
    
    def process_single_event(event):
        """単一イベントの処理"""
        try:
            retry_wrapper(insert_events, event)
            local_batch = []
            
            # マーケット並列処理
            with ThreadPoolExecutor(max_workers=CONFIG['MARKET_WORKERS']) as market_executor:
                futures = {
                    market_executor.submit(
                        process_market,
                        market=market,
                        event_id=event['id'],
                        market_pbar=None  # 並列処理時はプログレスバーを無効化
                    ): market
                    for market in event['markets']
                }
                
                local_stats = {
                    'processed_markets': 0,
                    'failed_markets': 0,
                    'price_records': 0
                }
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result and result.get('status') == 'success':
                            local_stats['processed_markets'] += 1
                            local_batch.extend(result['price_records'])
                            local_stats['price_records'] += len(result['price_records'])
                            
                            # バッチサイズを超えた場合、キューに追加
                            if len(local_batch) >= CONFIG['PRICE_BATCH_SIZE']:
                                shared_batch_queue.put(local_batch[:CONFIG['PRICE_BATCH_SIZE']])
                                local_batch = local_batch[CONFIG['PRICE_BATCH_SIZE']:]
                        else:
                            local_stats['failed_markets'] += 1
                    except Exception as e:
                        local_stats['failed_markets'] += 1
                        logger.error(f"Market processing error in event {event['id']}", exc_info=True)
            
            # 残りのバッチをキューに追加
            if local_batch:
                shared_batch_queue.put(local_batch)
            
            return {
                'event_id': event['id'],
                'status': 'success',
                'stats': local_stats
            }
            
        except Exception as e:
            logger.error(f"Event processing error - ID: {event.get('id', 'unknown')}", exc_info=True)
            return {
                'event_id': event['id'],
                'status': 'error',
                'error': str(e)
            }

    # バッチ処理ワーカー
    def batch_worker():
        while True:
            try:
                batch = shared_batch_queue.get(timeout=5)  # 5秒のタイムアウト
                if batch:
                    insert_price_history_batch(batch)
                    stats['processed_price_records'] += len(batch)
                shared_batch_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error("Batch processing error", exc_info=True)

    # メインの進捗バー
    with tqdm(total=len(target_events), desc="Events") as event_pbar:
        def update_progress(status: str):
            event_pbar.set_description(f"Events - {status}")
        
        # イベント並列処理
        with ThreadPoolExecutor(max_workers=min(10, len(target_events))) as event_executor:
            update_progress("Starting batch workers")
            batch_executor = ThreadPoolExecutor(max_workers=4)
            batch_futures = [batch_executor.submit(batch_worker) for _ in range(4)]
            
            update_progress("Processing events")
            # イベント処理を実行
            event_futures = [
                event_executor.submit(process_single_event, event)
                for event in target_events
            ]
            
            for future in concurrent.futures.as_completed(event_futures):
                try:
                    result = future.result()
                    if result['status'] == 'success':
                        stats['processed_events'] += 1
                        stats['processed_markets'] += result['stats']['processed_markets']
                        stats['failed_markets'] += result['stats']['failed_markets']
                    else:
                        stats['failed_events'] += 1
                    event_pbar.update(1)
                except Exception as e:
                    stats['failed_events'] += 1
                    logger.error("Event future processing error", exc_info=True)
            
            # バッチ処理の完了を待機
            shared_batch_queue.join()
            batch_executor.shutdown(wait=True)

    execution_time = time() - start_time
    logger.info(f"Process complete - Execution time: {execution_time:.2f}s - Stats: {stats}")
    
    return {**stats, 'execution_time': execution_time}

def main():
    parser = argparse.ArgumentParser(description='イベントデータ同期スクリプト')
    parser.add_argument('--start', type=int, help='開始インデックス')
    parser.add_argument('--end', type=int, help='終了インデックス')
    args = parser.parse_args()

    logger = setup_logging()
    
    try:
        with open("../gamma/output/events.json", "r") as f:
            events_data = json.load(f)
        
        events_data = [event for event in events_data if "markets" in event]
        
        if args.start is not None and args.end is not None:
            # 特定範囲の処理
            results = process_event_range(args.start, args.end, events_data)
            logger.info(f"指定範囲の処理結果: {results}")
        else:
            # 全データの処理
            results = process_event_range(0, len(events_data) - 1, events_data)
            logger.info(f"全データの処理結果: {results}")

    except Exception as e:
        logger.error(f"メイン処理エラー: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()