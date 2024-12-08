import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

# 設定用ディクショナリ
CONFIG = {
    "BATCH_SIZE": 10000,
    "ERROR_LOG": "log/error_log.log",
    "MAX_WORKERS_EVENTS": 100,
    "MAX_WORKERS_MARKETS": 5,
    "EVENT_CHUNK_SIZE": 100,
    "RETRY_COUNT": 5,       # 再試行回数
    "RETRY_DELAY": 5        # 再試行前待機秒数
}

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gamma.fetch_market_pricehistory.fetch_pricehistory import fetch_pricehistory

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

os.makedirs("log", exist_ok=True)

logger = logging.getLogger("error_logger")
logger.setLevel(logging.ERROR)
fh = logging.FileHandler(CONFIG["ERROR_LOG"])
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

with open("../gamma/output/events.json", "r") as f:
    event_data = json.load(f)

total_markets = sum(len(event.get("markets", [])) for event in event_data)

GREEN = "\033[32m"
BLUE = "\033[34m"
YELLOW = "\033[33m"
RESET = "\033[0m"

main_pbar_events = tqdm(total=len(event_data), position=0, dynamic_ncols=True, leave=True, desc=f"{GREEN}All Events{RESET}")
main_pbar_markets = tqdm(total=total_markets, position=1, dynamic_ncols=True, leave=True, desc=f"{BLUE}All Markets{RESET}")
main_pbar_prices = tqdm(total=0, position=2, dynamic_ncols=True, leave=True, desc=f"{YELLOW}All Prices{RESET}")

pbar_threads = []
for i in range(CONFIG["MAX_WORKERS_EVENTS"]):
    p = tqdm(
        total=1,
        position=3+i,
        dynamic_ncols=True,
        leave=True,
        bar_format="{desc}"
    )
    p.set_description(f"{GREEN}Thread-{i}{RESET}: Idle")
    pbar_threads.append(p)

thread_lock = Lock()
next_chunk = 0
total_chunks = (len(event_data) + CONFIG["EVENT_CHUNK_SIZE"] - 1) // CONFIG["EVENT_CHUNK_SIZE"]

def safe_insert(table_name, record):
    """単一レコード挿入用。エラー発生時にリトライ。"""
    for attempt in range(CONFIG["RETRY_COUNT"]):
        try:
            supabase.table(table_name).insert(record).execute()
            return
        except Exception as e:
            logger.error(f"Error inserting into {table_name} (attempt {attempt+1}/{CONFIG['RETRY_COUNT']}): {e}")
            time.sleep(CONFIG["RETRY_DELAY"])
    raise Exception(f"Failed to insert into {table_name} after {CONFIG['RETRY_COUNT']} attempts")

def safe_batch_insert(table_name, records, batch_size):
    """バルクインサート用。BATCH単位で挿入し、各BATCHでエラー時にリトライ。"""
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        for attempt in range(CONFIG["RETRY_COUNT"]):
            try:
                supabase.table(table_name).insert(batch).execute()
                break
            except Exception as e:
                logger.error(f"Error inserting batch into {table_name} (attempt {attempt+1}/{CONFIG['RETRY_COUNT']}): {e}")
                time.sleep(CONFIG["RETRY_DELAY"])
        else:
            # 全てのattemptで失敗
            raise Exception(f"Failed to insert batch into {table_name} after {CONFIG['RETRY_COUNT']} attempts")

def get_number_or_none(data, key):
    value = data.get(key)
    return value if value is not None else None

def insert_event_and_tags(event):
    # eventsテーブル挿入
    try:
        safe_insert("events", {
            "id": event["id"],
            "ticker": event.get("ticker", None),
            "slug": event.get("slug", None),
            "title": event.get("title", None),
            "description": event.get("description", None),
            "resolution_source": event.get("resolutionSource", None),
            "start_date": event.get("startDate", None),
            "creation_date": event.get("creationDate", None),
            "end_date": event.get("endDate", None),
            "image": event.get("image", None),
            "icon": event.get("icon", None),
            "active": event.get("active", None),
            "closed": event.get("closed", None),
            "archived": event.get("archived", None),
            "new": event.get("new", None),
            "featured": event.get("featured", None),
            "restricted": event.get("restricted", None),
            "liquidity": event.get("liquidity", None),
            "volume": event.get("volume", None),
            "open_interest": event.get("openInterest", None),
            "sort_by": event.get("sortBy", None),
            "created_at": event.get("createdAt", None),
            "updated_at": event.get("updatedAt", None),
            "competitive": event.get("competitive", None),
            "volume_24hr": event.get("volume24hr", None),
            "enable_order_book": event.get("enableOrderBook", None),
            "liquidity_clob": event.get("liquidityClob", None),
            "_sync": event.get("_sync", None),
            "neg_risk": event.get("negRisk", None),
            "neg_risk_market_id": event.get("negRiskMarketID", None),
            "comment_count": event.get("commentCount", None),
            "cyom": event.get("cyom", None),
            "show_all_outcomes": event.get("showAllOutcomes", None),
            "show_market_images": event.get("showMarketImages", None),
            "enable_neg_risk": event.get("enableNegRisk", None),
            "automatically_active": event.get("automaticallyActive", None),
            "gmp_chart_mode": event.get("gmpChartMode", None),
            "neg_risk_augmented": event.get("negRiskAugmented", None)
        })
    except Exception as e:
        logger.error(f"Error inserting event {event['id']}: {e}")

    # tagsテーブル挿入
    if "tags" in event and event["tags"]:
        tag_records = []
        for tag in event["tags"]:
            tag_records.append({
                "event_id": event["id"],
                "id": tag.get("id", None),
                "label": tag.get("label", None),
                "slug": tag.get("slug", None),
                "force_show": tag.get("forceShow", None),
                "published_at": tag.get("publishedAt", None),
                "updated_by": tag.get("updatedBy", None),
                "created_at": tag.get("createdAt", None),
                "updated_at": tag.get("updatedAt", None),
                "_sync": tag.get("_sync", None),
                "force_hide": tag.get("forceHide", None)
            })
        try:
            safe_batch_insert("tags", tag_records, CONFIG["BATCH_SIZE"])
        except Exception as e:
            logger.error(f"Error inserting tags for event {event['id']}: {e}")

def insert_markets_and_prices(event, market):
    # markets挿入
    try:
        safe_insert("markets", {
            "id": market["id"],
            "event_id": event["id"],
            "question": market.get("question", None),
            "condition_id": market.get("conditionId", None),
            "slug": market.get("slug", None),
            "resolution_source": market.get("resolutionSource", None),
            "end_date": market.get("endDate", None),
            "liquidity": get_number_or_none(market, "liquidity"),
            "start_date": market.get("startDate", None),
            "image": market.get("image", None),
            "icon": market.get("icon", None),
            "description": market.get("description", None),
            "outcomes": json.loads(market.get("outcomes")) if isinstance(market.get("outcomes"), str) else market.get("outcomes", None),
            "outcome_prices": json.loads(market.get("outcomePrices")) if isinstance(market.get("outcomePrices"), str) else market.get("outcomePrices", None),
            "volume": get_number_or_none(market, "volume"),
            "active": market.get("active", None),
            "closed": market.get("closed", None),
            "market_maker_address": market.get("marketMakerAddress", None),
            "created_at": market.get("createdAt", None),
            "updated_at": market.get("updatedAt", None),
            "new": market.get("new", None),
            "featured": market.get("featured", None),
            "submitted_by": market.get("submitted_by", None),
            "archived": market.get("archived", None),
            "resolved_by": market.get("resolvedBy", None),
            "restricted": market.get("restricted", None),
            "group_item_title": market.get("groupItemTitle", None),
            "group_item_threshold": market.get("groupItemThreshold", None),
            "question_id": market.get("questionID", None),
            "enable_order_book": market.get("enableOrderBook", None),
            "order_price_min_tick_size": market.get("orderPriceMinTickSize", None),
            "order_min_size": market.get("orderMinSize", None),
            "volume_num": get_number_or_none(market, "volumeNum"),
            "liquidity_num": get_number_or_none(market, "liquidityNum"),
            "end_date_iso": market.get("endDateIso", None),
            "start_date_iso": market.get("startDateIso", None),
            "has_reviewed_dates": market.get("hasReviewedDates", None),
            "volume_24hr": get_number_or_none(market, "volume24hr"),
            "clob_token_ids": json.loads(market.get("clobTokenIds")) if isinstance(market.get("clobTokenIds"), str) else market.get("clobTokenIds", None),
            "uma_bond": market.get("umaBond", None),
            "uma_reward": market.get("umaReward", None),
            "volume_24hr_clob": get_number_or_none(market, "volume24hrClob"),
            "volume_clob": get_number_or_none(market, "volumeClob"),
            "liquidity_clob": get_number_or_none(market, "liquidityClob"),
            "accepting_orders": market.get("acceptingOrders", None),
            "neg_risk": market.get("negRisk", None),
            "neg_risk_market_id": market.get("negRiskMarketID", None),
            "neg_risk_request_id": market.get("negRiskRequestID", None),
            "_sync": market.get("_sync", None),
            "ready": market.get("ready", None),
            "funded": market.get("funded", None),
            "accepting_orders_timestamp": market.get("acceptingOrdersTimestamp", None),
            "cyom": market.get("cyom", None),
            "competitive": market.get("competitive", None),
            "pager_duty_notification_enabled": market.get("pagerDutyNotificationEnabled", None),
            "approved": market.get("approved", None),
            "clob_rewards": market.get("clobRewards", None),
            "rewards_min_size": market.get("rewardsMinSize", None),
            "rewards_max_spread": market.get("rewardsMaxSpread", None),
            "spread": market.get("spread", None),
            "one_day_price_change": market.get("oneDayPriceChange", None),
            "last_trade_price": market.get("lastTradePrice", None),
            "best_bid": market.get("bestBid", None),
            "best_ask": market.get("bestAsk", None),
            "automatically_active": market.get("automaticallyActive", None),
            "clear_book_on_start": market.get("clearBookOnStart", None),
            "series_color": market.get("seriesColor", None),
            "show_gmp_series": market.get("showGmpSeries", None),
            "show_gmp_outcome": market.get("showGmpOutcome", None),
            "manual_activation": market.get("manualActivation", None),
            "neg_risk_other": market.get("negRiskOther", None)
        })
    except Exception as e:
        logger.error(f"Error inserting market {market['id']} of event {event['id']}: {e}")

    # prices挿入はfetch_pricehistoryを使用
    try:
        # 価格履歴取得
        if "clobTokenIds" in market and market["clobTokenIds"]:
            token_ids = json.loads(market["clobTokenIds"]) if isinstance(market["clobTokenIds"], str) else market["clobTokenIds"]
            if token_ids and len(token_ids) > 0:
                price_data = fetch_pricehistory(market, token_ids[0], logger)
                if price_data and 'history' in price_data:
                    history = price_data['history']
                    main_pbar_prices.reset(total=len(history))
                    main_pbar_prices.set_description("Processing prices")

                    price_records = []
                    for h in history:
                        price_records.append({
                            "market_id": market["id"],
                            "timestamp": h.get("t"),
                            "price": h.get("p")
                        })

                    # バルクインサート（再試行付き）
                    for i in range(0, len(price_records), CONFIG["BATCH_SIZE"]):
                        batch = price_records[i:i+CONFIG["BATCH_SIZE"]]
                        safe_batch_insert("prices", batch, CONFIG["BATCH_SIZE"])
                        main_pbar_prices.update(len(batch))
    except Exception as e:
        logger.error(f"Error inserting prices for market {market['id']} of event {event['id']}: {e}")

def process_market_for_thread(market, event_id, markets_total, pbar_thread, thread_id, start_idx, end_idx):
    pbar_thread.set_description(
        f"{GREEN}Thread-{thread_id}{RESET} [{start_idx}-{end_idx}] {BLUE}Event:{event_id}{RESET} Processing Market:{market['id']} ({markets_total} total):"
    )
    insert_markets_and_prices({"id": event_id}, market)
    main_pbar_markets.update(1)
    return (1, 0)

def process_event_for_thread(event, thread_id, pbar_thread, start_idx, end_idx):
    event_id = event["id"]
    markets = event.get("markets", [])
    pbar_thread.set_description(
        f"{GREEN}Thread-{thread_id}{RESET} [{start_idx}-{end_idx}] {BLUE}Event:{event_id}{RESET} Processing..."
    )

    # events, tags挿入
    insert_event_and_tags(event)
    main_pbar_events.update(1)

    market_count = 0
    price_count = 0
    if markets:
        with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS_MARKETS"]) as ex:
            futures = [ex.submit(process_market_for_thread, m, event_id, len(markets), pbar_thread, thread_id, start_idx, end_idx) for m in markets]
            for f in as_completed(futures):
                m_c, p_c = f.result()
                market_count += m_c
                # price_countはmarkets挿入内で挿入済み
    else:
        pbar_thread.set_description(
            f"{GREEN}Thread-{thread_id}{RESET} [{start_idx}-{end_idx}] {BLUE}Event:{event_id}{RESET} (No markets):"
        )

    return (1, market_count, price_count)

def process_event_chunk(events_chunk, chunk_idx, start_idx, end_idx, thread_id):
    pbar_threads[thread_id].set_description(
        f"{GREEN}Thread-{thread_id}{RESET} [{start_idx}-{end_idx}] Starting Chunk {chunk_idx+1}/{total_chunks}:"
    )

    event_count = 0
    market_count = 0
    price_count = 0
    for e in events_chunk:
        e_c, m_c, p_c = process_event_for_thread(e, thread_id, pbar_threads[thread_id], start_idx, end_idx)
        event_count += e_c
        market_count += m_c
        price_count += p_c

    pbar_threads[thread_id].set_description(
        f"{GREEN}Thread-{thread_id}{RESET} [{start_idx}-{end_idx}] Chunk {chunk_idx+1}/{total_chunks} Done:"
    )
    return (event_count, market_count, price_count)

def get_next_chunk():
    global next_chunk
    with thread_lock:
        if next_chunk < total_chunks:
            c = next_chunk
            next_chunk += 1
            return c
        else:
            return None

def worker_main(thread_id):
    while True:
        c_i = get_next_chunk()
        if c_i is None:
            pbar_threads[thread_id].set_description(f"{GREEN}Thread-{thread_id}{RESET}: Idle (No more chunks)")
            break
        start_idx = c_i * CONFIG["EVENT_CHUNK_SIZE"]
        end_idx = min(start_idx + CONFIG["EVENT_CHUNK_SIZE"], len(event_data)) - 1
        events_chunk = event_data[start_idx:end_idx+1]
        process_event_chunk(events_chunk, c_i, start_idx, end_idx, thread_id)
    return

with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS_EVENTS"]) as executor:
    futures = [executor.submit(worker_main, i) for i in range(CONFIG["MAX_WORKERS_EVENTS"])]
    for f in as_completed(futures):
        f.result()

main_pbar_events.close()
main_pbar_markets.close()
main_pbar_prices.close()
