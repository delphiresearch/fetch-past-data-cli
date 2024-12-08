import json
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import sys
from tqdm import tqdm
import logging

# 設定用ディクショナリ
CONFIG = {
    "BATCH_SIZE": 10000,
    "ERROR_LOG": "log/error_log.log"
}

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory

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

# 必要に応じてスキップ
event_data = event_data[1130:]

total_markets = sum(len(event.get("markets", [])) for event in event_data)

pbar_events = tqdm(total=len(event_data), position=0, dynamic_ncols=True)
pbar_markets = tqdm(total=total_markets, position=1, dynamic_ncols=True)
pbar_prices = tqdm(total=0, position=2, dynamic_ncols=True)

def batch_insert(table_name, records, batch_size):
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        supabase.table(table_name).insert(batch).execute()

def get_number_or_none(data, key):
    value = data.get(key)
    return value if value is not None else None

for event in event_data:
    markets = event.get("markets", [])
    # イベントのプログレスバーに(event_id - (market数))を表示
    pbar_events.set_description(f"Processing event: {event['id']} - ({len(markets)} markets)")
    
    try:
        supabase.table("events").insert({
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
        }).execute()
    except Exception as e:
        logger.error(f"Error inserting event {event['id']}: {e}")

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
            batch_insert("tags", tag_records, CONFIG["BATCH_SIZE"])
        except Exception as e:
            logger.error(f"Error inserting tags for event {event['id']}: {e}")

    for market in markets:
        pbar_markets.set_description(f"Processing market: {market['id']}")
        try:
            supabase.table("markets").insert({
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
            }).execute()
        except Exception as e:
            logger.error(f"Error inserting market {market['id']} of event {event['id']}: {e}")

        # pricehistory処理
        try:
            price_history_json = fetch_all_pricehistory(market)
            if price_history_json is not None:
                price_data = json.loads(price_history_json)
                if 'history' in price_data:
                    history = price_data['history']
                    pbar_prices.reset(total=len(history))
                    pbar_prices.set_description("Processing prices")

                    price_records = []
                    for h in history:
                        price_records.append({
                            "market_id": market["id"],
                            "timestamp": h.get("t"),
                            "price": h.get("p")
                        })

                    for i in range(0, len(price_records), CONFIG["BATCH_SIZE"]):
                        batch = price_records[i:i+CONFIG["BATCH_SIZE"]]
                        supabase.table("prices").insert(batch).execute()
                        pbar_prices.update(len(batch))
        except Exception as e:
            logger.error(f"Error inserting prices for market {market['id']} of event {event['id']}: {e}")

        pbar_markets.update(1)

    pbar_events.update(1)

pbar_events.close()
pbar_markets.close()
pbar_prices.close()
