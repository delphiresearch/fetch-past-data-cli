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

from gamma.lib.fetch_single_pricehistory import fetch_all_pricehistory
# .envファイルから環境変数をロード
load_dotenv()

# Supabaseの接続情報
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# JSONデータをロード
with open("../gamma/output/events.json", "r") as f:
    event_data = json.load(f)

# 1138番目の要素までスキップ
event_data = event_data[1130:]

# tqdmを使用して進捗バーを表示
for event in tqdm(event_data, desc="Processing events"):
    print(f"Event ID: {event['id']}")

    event_response = supabase.table("events").insert({
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
        "tags": event.get("tags", None),
        "cyom": event.get("cyom", None),
        "show_all_outcomes": event.get("showAllOutcomes", None),
        "show_market_images": event.get("showMarketImages", None),
        "enable_neg_risk": event.get("enableNegRisk", None),
        "automatically_active": event.get("automaticallyActive", None),
        "gmp_chart_mode": event.get("gmpChartMode", None),
        "neg_risk_augmented": event.get("negRiskAugmented", None)
    }).execute()

# print("Event inserted:", event_response)

    # marketsデータを挿入
    markets = event["markets"]
    for market in markets:
        print(f"Market ID: {market['id']}")
        
        # 数値フィールドの取得方法を修正
        def get_number_or_none(data, key):
            value = data.get(key)
            return value if value is not None else None

        market_response = supabase.table("markets").insert({
            "id": market["id"],
            "event_id": event["id"],  # eventsテーブルとリレーション
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
            "neg_risk_other": market.get("negRiskOther", None),
            "price_history": json.dumps(fetch_all_pricehistory(market))  # プライスヒストリーデータ
        }).execute()
    # print("Market inserted:", market_response)
