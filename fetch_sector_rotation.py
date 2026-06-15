import os
import sys
import json
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 設定快取目錄
CACHE_DIR = "data_cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# 設定 User-Agent 防爬蟲阻擋
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def load_stock_info():
    """載入或下載全市場股票代號與標準產業分類對照表"""
    cache_file = os.path.join(CACHE_DIR, "stock_info.json")
    if os.path.exists(cache_file):
        log("自本地快取載入股票基本資料與產業分類對照表...")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
            
    log("本地快取不存在。正向 FinMind 抓取全市場股票基本資料...")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockInfo"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            result = r.json()
            if result.get("status") == 200 and result.get("data"):
                # 建立字典映射：stock_id -> {name, industry}
                stock_info = {}
                for row in result["data"]:
                    stock_id = row.get("stock_id")
                    stock_name = row.get("stock_name")
                    industry = row.get("industry_category")
                    # 只過濾正常上市公司且有分類的（過濾長度非 4 碼的 ETF、權證等）
                    if stock_id and len(stock_id) == 4 and industry:
                        stock_info[stock_id] = {
                            "name": stock_name,
                            "industry": industry
                        }
                # 寫入本機快取
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(stock_info, f, ensure_ascii=False, indent=4)
                log(f"成功快取全市場 {len(stock_info)} 檔股票之產業分類資料。")
                return stock_info
            else:
                log(f"[錯誤] FinMind 資料返回異常: {result.get('msg')}")
        else:
            log(f"[錯誤] FinMind 伺服器回傳狀態碼: {r.status_code}")
    except Exception as e:
        log(f"[錯誤] 下載股票基本資料失敗: {str(e)}")
    return {}

def fetch_twse_raw(date_str, endpoint):
    """
    動態下載 TWSE 指定日期的盤後資料。
    endpoint 可為 'T86' (三大法人買賣超) 或 'MI_INDEX' (每日收盤行情)
    """
    cache_file = os.path.join(CACHE_DIR, f"{endpoint}_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # 構造官方 URL
    if endpoint == "T86":
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALL"
    else:
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={date_str}&type=ALL"

    log(f"正在自 TWSE 官方下載 {date_str} 的 {endpoint} 資料...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            result = r.json()
            if result.get("stat") == "OK":
                # 寫入快取
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=4)
                # 強行限速限流以保護 IP
                time.sleep(3)
                return result
            else:
                # 代表該日期無交易（例如週末、假日）
                return None
        else:
            log(f"[警告] {date_str} {endpoint} 下載失敗，狀態碼: {r.status_code}")
    except Exception as e:
        log(f"[錯誤] 下載 {date_str} {endpoint} 異常: {str(e)}")
    return None

def collect_trading_days(count=20):
    """自今天開始往回推算，收集最近 count 個有效的交易日資料"""
    log(f"正在收集最近 {count} 個有效交易日的 TWSE 資料...")
    valid_days = []
    curr_date = datetime.now()
    
    # 防死鎖限制，最多往回推 45 天
    max_lookback = 45
    days_checked = 0
    
    while len(valid_days) < count and days_checked < max_lookback:
        date_str = curr_date.strftime("%Y%m%d")
        
        # 測試該日期是否為交易日：兩者皆有快取或皆下載成功
        t86_data = fetch_twse_raw(date_str, "T86")
        mi_data = fetch_twse_raw(date_str, "MI_INDEX")
        
        if t86_data and mi_data:
            valid_days.append(date_str)
            log(f"-> 成功收集交易日: {date_str} (目前共 {len(valid_days)} 天)")
        
        curr_date -= timedelta(days=1)
        days_checked += 1
        
    if len(valid_days) < count:
        log(f"[警告] 無法收集滿 {count} 天，僅收集到 {len(valid_days)} 天。")
    return sorted(valid_days) # 由舊到新排列

def parse_daily_data(date_str, stock_info):
    """解析並合併單日 T86 與 MI_INDEX 資料，依據產業分類歸戶"""
    t86_raw = fetch_twse_raw(date_str, "T86")
    mi_raw = fetch_twse_raw(date_str, "MI_INDEX")
    
    if not t86_raw or not mi_raw:
        return []

    # 1. 解析三大法人買賣超 (T86)
    # columns: 0: 證券代號, 18: 三大法人買賣超股數
    t86_dict = {}
    if "data" in t86_raw:
        for row in t86_raw["data"]:
            if len(row) > 18:
                stock_id = row[0].strip()
                try:
                    net_shares = float(row[18].replace(",", "").strip())
                    t86_dict[stock_id] = net_shares
                except ValueError:
                    continue

    # 2. 解析每日收盤行情 (MI_INDEX)
    # 找每日收盤行情(全部) 表 (通常是第 8 個 table)
    quotes_table = None
    for table in mi_raw.get("tables", []):
        if table.get("title") and "每日收盤行情" in table.get("title"):
            quotes_table = table
            break
            
    if not quotes_table:
        # 備援機制：如果 title 沒對上，直接取欄位數為 16 且含證券代號的 table
        for table in mi_raw.get("tables", []):
            fields = table.get("fields", [])
            if fields and "證券代號" in fields and len(fields) >= 16:
                quotes_table = table
                break

    if not quotes_table:
        log(f"[錯誤] 無法找到 {date_str} 的每日收盤行情表。")
        return []

    daily_records = []
    
    # 欄位索引：
    # 0: 證券代號, 1: 證券名稱, 2: 成交股數, 4: 成交金額, 8: 收盤價, 9: 漲跌記號, 10: 漲跌價差
    for row in quotes_table.get("data", []):
        if len(row) >= 16:
            stock_id = row[0].strip()
            
            # 過濾僅屬於上市公司官方標準分類的個股（四碼）
            if stock_id not in stock_info:
                continue
                
            stock_name = stock_info[stock_id]["name"]
            sector = stock_info[stock_id]["industry"]
            
            close_str = row[8].replace(",", "").strip()
            volume_str = row[2].replace(",", "").strip()
            amount_str = row[4].replace(",", "").strip()
            diff_sign_html = row[9]
            diff_val_str = row[10].replace(",", "").strip()
            
            # 排除無收盤價（如停牌）
            try:
                close = float(close_str)
                volume = float(volume_str)
                amount_m = float(amount_str) / 1000000.0 # 折算百萬
            except ValueError:
                continue

            # 計算漲跌幅
            try:
                diff_val = float(diff_val_str)
            except ValueError:
                diff_val = 0.0
                
            change_pct = 0.0
            if diff_val > 0:
                if "red" in diff_sign_html or "+" in diff_sign_html:
                    if close > diff_val:
                        change_pct = (diff_val / (close - diff_val)) * 100
                elif "green" in diff_sign_html or "-" in diff_sign_html:
                    change_pct = (-diff_val / (close + diff_val)) * 100
            
            # 獲取法人買賣超股數
            net_shares = t86_dict.get(stock_id, 0.0)
            net_amount_m = (net_shares * close) / 1000000.0 # 折算百萬

            daily_records.append({
                "date": date_str,
                "stock_id": stock_id,
                "stock_name": stock_name,
                "sector": sector,
                "price": close,
                "volume": volume,
                "amount_m": amount_m,
                "today_return_pct": change_pct,
                "today_net_flow_m": net_amount_m
            })
            
    return daily_records

def main():
    log("=== 開始全市場板塊輪動數據重構 ===")
    
    # 1. 載入股票基本對照
    stock_info = load_stock_info()
    if not stock_info:
        log("[致命錯誤] 無法取得股票基本分類，重構中止。")
        return
        
    # 2. 收集最近 20 個交易日
    trading_days = collect_trading_days(20)
    if not trading_days:
        log("[致命錯誤] 無法取得交易日資料，重構中止。")
        return
        
    log(f"已排定的交易日序列: {trading_days}")
    
    # 3. 抓取並解析所有交易日數據
    all_data = []
    for d in trading_days:
        records = parse_daily_data(d, stock_info)
        all_data.extend(records)
        log(f"已解析 {d} 共 {len(records)} 檔有效個股紀錄")
        
    if not all_data:
        log("[致命錯誤] 合併後的數據集為空，重構中止。")
        return
        
    df = pd.DataFrame(all_data)
    
    # 4. 板塊數據 Group By 加總
    log("正在進行板塊分組與指標計算...")
    
    # 取得板塊每日合計
    sector_daily = df.groupby(["sector", "date"]).agg({
        "today_net_flow_m": "sum",
        "amount_m": "sum"
    }).reset_index()
    
    # 最新一日的日期
    latest_date = trading_days[-1]
    
    # 獲取加權指數當日行情
    market_close = 0.0
    market_return_pct = 0.0
    mi_raw = fetch_twse_raw(latest_date, "MI_INDEX")
    if mi_raw and "tables" in mi_raw and len(mi_raw["tables"]) > 0:
        index_table = mi_raw["tables"][0]
        if "data" in index_table:
            for row in index_table["data"]:
                if len(row) > 4 and "發行量加權股價指數" in row[0]:
                    try:
                        market_close = float(row[1].replace(",", "").strip())
                        pct_str = row[4].replace(",", "").strip()
                        market_return_pct = float(pct_str)
                        if "green" in row[2] or "-" in row[2]:
                            market_return_pct = -market_return_pct
                    except ValueError:
                        pass
                    break
    log(f"今日加權指數收盤: {market_close} | 漲跌幅: {market_return_pct:.2f}%")
    
    # 存放最後 JSON 的陣列
    sectors_results = []
    
    # 取得所有不重複的板塊名稱
    unique_sectors = sorted(df["sector"].unique())
    
    for sector_name in unique_sectors:
        # 取該板塊的每日歷史數據
        sec_df = sector_daily[sector_daily["sector"] == sector_name].sort_values("date").copy()
        
        available_days = len(sec_df)
        if available_days == 0:
            continue
            
        n_5d = min(5, available_days)
        n_20d = min(20, available_days)
        
        # A. 5日累計流向
        flow_5d = float(sec_df["today_net_flow_m"].iloc[-n_5d:].sum())
        
        # B. 20日累計流向
        flow_20d = float(sec_df["today_net_flow_m"].iloc[-n_20d:].sum())
        
        # C. 20日資金交易總規模 (泡泡大小：20日內所有個股成交金額加總)
        size_20d = float(sec_df["amount_m"].iloc[-n_20d:].sum())
        
        # D. 資金加速度
        today_flow = float(sec_df["today_net_flow_m"].iloc[-1])
        if available_days > 1:
            prev_5d_flow = sec_df["today_net_flow_m"].iloc[-min(6, available_days):-1]
            prev_5d_avg = float(prev_5d_flow.mean()) if not prev_5d_flow.empty else 0.0
            acceleration = today_flow - prev_5d_avg
        else:
            acceleration = 0.0

        # 計算抄底得分：5日法人買超力道為正，且今日加速度為正
        if flow_5d > 0 and acceleration > 0:
            bottom_fishing_score = flow_5d * 0.6 + acceleration * 0.4
        else:
            bottom_fishing_score = 0.0
            
        # E. 篩選核心個股：今日法人買賣超絕對值前 5 大
        latest_df = df[(df["sector"] == sector_name) & (df["date"] == latest_date)].copy()
        if latest_df.empty:
            continue

        # 篩選抄底指標股：法人逆勢買超最強且今日相對抗跌（個股今日漲跌幅 >= 大盤漲跌幅，且今日淨流入為正）
        bf_candidates = latest_df[(latest_df["today_net_flow_m"] > 0) & (latest_df["today_return_pct"] >= market_return_pct)].copy()
        bf_stocks = bf_candidates.sort_values("today_net_flow_m", ascending=False).head(2)
        bottom_fishing_stocks_data = []
        for _, row in bf_stocks.iterrows():
            bottom_fishing_stocks_data.append({
                "stock_id": row["stock_id"],
                "stock_name": row["stock_name"],
                "today_net_flow_m": round(float(row["today_net_flow_m"]), 2),
                "today_return_pct": round(float(row["today_return_pct"]), 2)
            })
            
        # 以今日買賣超金額絕對值進行降序排序
        latest_df["abs_flow"] = latest_df["today_net_flow_m"].abs()
        top_stocks_df = latest_df.sort_values("abs_flow", ascending=False).head(5)
        
        individual_stocks_data = []
        for _, row in top_stocks_df.iterrows():
            stock_id = row["stock_id"]
            stock_name = row["stock_name"]
            price = float(row["price"])
            volume = float(row["volume"])
            today_return = float(row["today_return_pct"])
            today_flow_m = float(row["today_net_flow_m"])
            
            # 計算該個股近 5 日與 20 日的累計買賣超
            st_df = df[(df["stock_id"] == stock_id)].sort_values("date")
            st_n5 = min(5, len(st_df))
            st_n20 = min(20, len(st_df))
            stock_flow_5d = float(st_df["today_net_flow_m"].iloc[-st_n5:].sum())
            stock_flow_20d = float(st_df["today_net_flow_m"].iloc[-st_n20:].sum())
            
            # 計算 5 日累計漲跌幅
            idx_5d_ago = -min(6, len(st_df))
            price_5d_ago = float(st_df["price"].iloc[idx_5d_ago])
            stock_return_5d = float((price - price_5d_ago) / price_5d_ago * 100) if price_5d_ago > 0 else 0.0
            
            individual_stocks_data.append({
                "stock_id": stock_id,
                "stock_name": stock_name,
                "price": price,
                "volume": volume,
                "today_return_pct": round(today_return, 2),
                "return_5d_pct": round(stock_return_5d, 2),
                "today_net_flow_m": round(today_flow_m, 2),
                "flow_5d_m": round(stock_flow_5d, 2),
                "flow_20d_m": round(stock_flow_20d, 2)
            })
            
        sectors_results.append({
            "sector_name": sector_name,
            "flow_5d_m": round(flow_5d, 2),
            "flow_20d_m": round(flow_20d, 2),
            "size_20d_m": round(size_20d, 2),
            "acceleration_m": round(acceleration, 2),
            "bottom_fishing_score": round(bottom_fishing_score, 2),
            "bottom_fishing_stocks": bottom_fishing_stocks_data,
            "individual_stocks": individual_stocks_data
        })
        
    # 5. 輸出為 sectors_data.json
    output_data = {
        "market_info": {
            "date": latest_date,
            "market_close": market_close,
            "market_return_pct": round(market_return_pct, 2)
        },
        "sectors": sectors_results
    }
    
    output_path = "sectors_data.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)
        
    log(f"全市場抄底重構成功！共處理 {len(sectors_results)} 個產業分類板塊，已成功生成 {output_path}。")

if __name__ == "__main__":
    main()
