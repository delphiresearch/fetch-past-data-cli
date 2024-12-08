#!/bin/bash

# カラー設定
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# エラーハンドリング
set -e

echo -e "${GREEN}1. 仮想環境を作成中...${NC}"
python3 -m venv venv

echo -e "${GREEN}2. 仮想環境を有効化...${NC}"
source venv/bin/activate

echo -e "${GREEN}3. 依存パッケージをインストール中...${NC}"
pip install -r requirements.txt

echo -e "${GREEN}4. イベントデータの取得を開始...${NC}"
python gamma/fetch-event/fetch_all_event.py

echo -e "${GREEN}5. Supabaseへのデータ投入を開始...${NC}"
python supabase/script_v1.py

echo -e "${GREEN}処理が完了しました${NC}"

# 仮想環境を無効化
deactivate
