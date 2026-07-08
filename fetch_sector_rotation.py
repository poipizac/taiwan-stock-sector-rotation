import os
import sys
import json
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
import yfinance as yf

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

urllib3.disable_warnings()

# 設定快取目錄
CACHE_DIR = "data_cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# 自訂概念股與成分股代碼字典
CONCEPT_SECTORS = {
    "記憶體": ['2337', '2344', '2408', '3260', '8299', '4967'],
    "光通訊": ['4979', '6442', '3081', '3163', '3234', '6426'],
    "被動元件": ['2327', '2492', '6173', '6127', '2456'],
    "低軌衛星": ['3491', '3152', '2314', '6285', '2313', '3380'],
    "機器人": ['2359', '6188', '2464', '8374', '2365', '4562'],
    "台積先進封裝設備": ['3131', '6187', '3583', '6640', '5443', '2464'],
    "軍工概念股": ['3037', '6431', '1301', '4763'],
    "高值化半導體材料": ['1303', '1402', '1717', '4770', '1727']
}

# 強制歸類字典 (Ticker Override Mechanism)
CUSTOM_SECTOR_MAP = {
    "8033": "軍工概念股",      # 雷虎 (原為其他)
    "2634": "軍工概念股",      # 漢翔 (原為其他)
    "6753": "軍工概念股",      # 龍德造船 (原為其他)
    "8222": "軍工概念股",      # 寶一 (原為其他)
    "2308": "電子零組件業",    # 台達電 (從原本的自訂混合板塊移出，回歸電子零組件業)
}

STOCK_TO_CONCEPT = {}
for concept, sids in CONCEPT_SECTORS.items():
    for sid in sids:
        STOCK_TO_CONCEPT[sid] = concept

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
                time.sleep(3)
                return None
        else:
            log(f"[警告] {date_str} {endpoint} 下載失敗，狀態碼: {r.status_code}")
            time.sleep(3)
    except Exception as e:
        log(f"[錯誤] 下載 {date_str} {endpoint} 異常: {str(e)}")
        time.sleep(3)
    return None

def collect_trading_days(count=20):
    """自今天開始往回推算，收集最近 count 個有效的交易日資料"""
    log(f"正在收集最近 {count} 個有效交易日的 TWSE 資料...")
    valid_days = []
    curr_date = datetime.now()
    
    # 防死鎖限制，最多往回推 60 天
    max_lookback = 60
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
            
            # 個股強制歸類 (Ticker Override) 優先
            if stock_id in CUSTOM_SECTOR_MAP:
                sector = CUSTOM_SECTOR_MAP[stock_id]
            else:
                sector = STOCK_TO_CONCEPT.get(stock_id, stock_info[stock_id]["industry"])
            
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

def get_base_date_options():
    url = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            select = soup.find("select", {"name": "scaDate"})
            if select:
                options = [opt.get("value") for opt in select.find_all("option")]
                return options
    except Exception as e:
        log(f"[警告] 獲取集保日期選單失敗: {str(e)}")
    return []

def get_latest_whale_ratios():
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    log("正在自集保所下載最新股權分散表 CSV...")
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        if r.status_code == 200:
            lines = r.content.decode("utf-8-sig").split("\n")
            id_idx = 1
            level_idx = 2
            pct_idx = 5
            
            whale_ratios = {}
            for line in lines[1:]:
                parts = line.strip().split(",")
                if len(parts) > pct_idx:
                    stock_id = parts[id_idx].replace('"', '').strip()
                    try:
                        level = int(parts[level_idx].replace('"', '').strip())
                        if level == 15:
                            pct = float(parts[pct_idx].replace('"', '').strip())
                            whale_ratios[stock_id] = pct
                    except ValueError:
                        continue
            return whale_ratios
    except Exception as e:
        log(f"[警告] 下載集保 CSV 失敗: {str(e)}")
    return {}

def fetch_whale_ratios_for_date(base_date, stock_ids):
    if not stock_ids:
        return {}
        
    log(f"開始批次下載基期 {base_date} 的大戶比例，共 {len(stock_ids)} 檔...")
    results = {}
    sess = requests.Session()
    try:
        # 1. 取得 token
        r = sess.get("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock", headers=HEADERS, verify=False, timeout=10)
        if r.status_code != 200:
            log(f"[錯誤] 無法連線集保所取得 Token (狀態碼: {r.status_code})")
            return {}
        soup = BeautifulSoup(r.text, "html.parser")
        token_elem = soup.find("input", {"name": "SYNCHRONIZER_TOKEN"})
        if not token_elem:
            log("[錯誤] 無法解析 SYNCHRONIZER_TOKEN")
            return {}
        token = token_elem["value"]
        
        # 2. 連續 POST 查詢
        success_count = 0
        for idx, sid in enumerate(stock_ids):
            if idx > 0 and idx % 50 == 0:
                log(f"-> 基期 {base_date} 下載進度: {idx}/{len(stock_ids)} (成功: {success_count})")
                
            post_data = {
                "SYNCHRONIZER_TOKEN": token,
                "SYNCHRONIZER_URI": "/portal/zh/smWeb/qryStock",
                "method": "submit",
                "firDate": base_date,
                "sqlMethod": "StockNo",
                "stockNo": sid,
                "scaDate": base_date,
            }
            try:
                resp = sess.post("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock", data=post_data, headers=HEADERS, verify=False, timeout=3)
                if resp.status_code == 200:
                    soup2 = BeautifulSoup(resp.text, "html.parser")
                    
                    # 每次 POST 成功後都要解析並更新 token
                    token_elem = soup2.find("input", {"name": "SYNCHRONIZER_TOKEN"})
                    if token_elem:
                        token = token_elem["value"]
                        
                    table = soup2.find("table", {"class": "table"})
                    if table:
                        rows = table.find_all("tr")
                        for row_elem in rows[1:]:
                            cols = [td.text.strip() for td in row_elem.find_all("td")]
                            if cols and cols[0] == '15':
                                pct_str = cols[4].replace(",", "").strip()
                                results[sid] = float(pct_str)
                                success_count += 1
                                break
                time.sleep(0.05) # 微小延遲
            except Exception:
                pass
                
        log(f"基期 {base_date} 批次下載完成，共成功取得 {success_count}/{len(stock_ids)} 檔。")
    except Exception as e:
        log(f"[錯誤] 批次查詢基期 {base_date} 異常: {str(e)}")
    return results

def detect_whale_locked_stocks(df, stock_info):
    log("=== 開始大戶鎖碼追蹤 (方案 A) ===")
    
    options = get_base_date_options()
    if len(options) < 17:
        log("[警告] 集保日期不足，無法執行大戶鎖碼追蹤。")
        return []
        
    latest_date = options[0]
    
    # 四個基期日期與對應的標籤
    baselines = {
        "1m": {"date": options[4], "weeks": 4},
        "2m": {"date": options[8], "weeks": 8},
        "3m": {"date": options[12], "weeks": 12},
        "4m": {"date": options[16], "weeks": 16}
    }
    
    log(f"最新集保日期: {latest_date}")
    for k, v in baselines.items():
        log(f"-> 基期日期 ({k} - {v['weeks']}週前): {v['date']}")
        
    latest_whale = get_latest_whale_ratios()
    if not latest_whale:
        log("[警告] 無法取得最新集保大戶比例。")
        return []
        
    # 載入或初始化各基期的本地快取
    caches = {}
    for k, v in baselines.items():
        b_date = v["date"]
        cache_file = os.path.join(CACHE_DIR, f"whale_history_{b_date}.json")
        cached_ratios = {}
        if os.path.exists(cache_file):
            log(f"自本地快取載入基期 {b_date} ({k}) 大戶比例...")
            with open(cache_file, "r", encoding="utf-8") as f:
                cached_ratios = json.load(f)
        caches[k] = {
            "file": cache_file,
            "data": cached_ratios
        }
        
    # 篩選最新一日活躍個股：
    # 為了防止向集保所發送過多查詢導致 IP 被鎖或程式卡死，
    # 我們過濾出：今日成交量 >= 2,000張 (2,000,000股) 或者是今日成交金額 >= 50.0 (百萬)，
    # 或者是屬於我們自訂的概念股成分股，才進行大戶鎖碼追蹤。
    df_latest_date = df["date"].max()
    latest_df = df[df["date"] == df_latest_date]
    active_stocks = latest_df[
        (latest_df["volume"] >= 2000000) | 
        (latest_df["amount_m"] >= 50.0) | 
        (latest_df["stock_id"].isin(STOCK_TO_CONCEPT.keys()))
    ]["stock_id"].unique()
    
    # 按 base_date 分組收集需要查詢的股票
    date_to_stocks = {}
    for sid in active_stocks:
        if sid in latest_whale:
            for k, v in baselines.items():
                b_date = v["date"]
                if sid not in caches[k]["data"]:
                    if b_date not in date_to_stocks:
                        date_to_stocks[b_date] = {"key": k, "sids": []}
                    date_to_stocks[b_date]["sids"].append(sid)
                    
    # 進行分組批次下載並寫入快取
    for b_date, info in date_to_stocks.items():
        k = info["key"]
        sids = info["sids"]
        if sids:
            fetched_ratios = fetch_whale_ratios_for_date(b_date, sids)
            if fetched_ratios:
                caches[k]["data"].update(fetched_ratios)
                with open(caches[k]["file"], "w", encoding="utf-8") as f:
                    json.dump(caches[k]["data"], f, ensure_ascii=False, indent=4)
                log(f"已將基期 {b_date} ({k}) 新獲取的 {len(fetched_ratios)} 筆資料寫入快取。")
                
    # 計算並篩選
    whale_locked_list = []
    latest_df = df[df["date"] == df_latest_date]
    
    for sid in active_stocks:
        if sid in latest_whale:
            has_all_data = True
            diffs = {}
            for k in baselines.keys():
                if sid in caches[k]["data"]:
                    base_ratio = caches[k]["data"][sid]
                    diffs[f"diff_{k}"] = round(latest_whale[sid] - base_ratio, 2)
                else:
                    has_all_data = False
                    break
                    
            if has_all_data:
                # 篩選條件：只要在任何一個時間區間（1m, 2m, 3m, 4m）內大戶比例累計增加大於等於 4.0%，就納入大戶鎖碼股列表
                max_diff = max(diffs.values())
                if max_diff >= 4.0:
                    stock_row = latest_df[latest_df["stock_id"] == sid]
                    today_return = float(stock_row["today_return_pct"].iloc[0]) if not stock_row.empty else 0.0
                    price_today = float(stock_row["price"].iloc[0]) if not stock_row.empty else 0.0
                    stock_name = stock_info[sid]["name"]
                    sector = STOCK_TO_CONCEPT.get(sid, stock_info[sid]["industry"])
                    
                    whale_locked_list.append({
                        "stock_id": sid,
                        "stock_name": stock_name,
                        "current_ratio": round(latest_whale[sid], 2),
                        "diff_1m": diffs["diff_1m"],
                        "diff_2m": diffs["diff_2m"],
                        "diff_3m": diffs["diff_3m"],
                        "diff_4m": diffs["diff_4m"],
                        "today_return_pct": round(today_return, 2),
                        "price_today": price_today,
                        "sector": sector
                    })
                    
    if whale_locked_list:
        sids = [item["stock_id"] for item in whale_locked_list]
        tickers = [f"{sid}.TW" for sid in sids]
        
        try:
            start_date_obj = datetime.strptime(baselines["4m"]["date"], "%Y%m%d") - timedelta(days=10)
            start_date_str = start_date_obj.strftime("%Y-%m-%d")
            end_date_obj = datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=2)
            end_date_str = end_date_obj.strftime("%Y-%m-%d")
        except Exception as e:
            log(f"[錯誤] 計算 yfinance 日期區裝失敗: {str(e)}")
            start_date_str = "2026-01-01"
            end_date_str = datetime.now().strftime("%Y-%m-%d")
            
        log(f"正在自 yfinance 下載 {len(tickers)} 檔鎖碼股之歷史收盤價 ({start_date_str} ~ {end_date_str})...")
        try:
            price_data = yf.download(tickers, start=start_date_str, end=end_date_str, group_by='ticker', progress=False)
        except Exception as e:
            log(f"[錯誤] yfinance 下載歷史股價失敗: {str(e)}")
            price_data = None
            
        def get_close_price(data, ticker, target_date_str):
            if data is None or data.empty:
                return None
            if isinstance(data.columns, pd.MultiIndex):
                if ticker not in data.columns.levels[0]:
                    return None
                ticker_data = data[ticker]
            else:
                ticker_data = data
            
            try:
                t_date = pd.to_datetime(target_date_str)
                valid_data = ticker_data[ticker_data.index <= t_date].dropna(subset=["Close"])
                if not valid_data.empty:
                    close_val = valid_data.loc[valid_data.index[-1], "Close"]
                    if isinstance(close_val, pd.Series):
                        close_val = float(close_val.iloc[-1])
                    return float(close_val)
            except Exception:
                pass
            return None

        for item in whale_locked_list:
            sid = item["stock_id"]
            ticker = f"{sid}.TW"
            price_today = item["price_today"]
            
            for k in ["1m", "2m", "3m", "4m"]:
                base_date = baselines[k]["date"]
                try:
                    target_date_str = datetime.strptime(base_date, "%Y%m%d").strftime("%Y-%m-%d")
                except ValueError:
                    target_date_str = base_date
                    
                price_base = get_close_price(price_data, ticker, target_date_str)
                if price_base and price_base > 0 and price_today > 0:
                    change_pct = round(((price_today - price_base) / price_base) * 100, 2)
                else:
                    change_pct = 0.0
                item[f"{k}_price_change"] = change_pct
            
            # 刪除臨時欄位
            del item["price_today"]

    log(f"大戶多維度鎖碼偵測完成！共篩選出 {len(whale_locked_list)} 檔符合條件之個股。")
    # 預設以 2m (2個月) 的變動由大到小排序
    return sorted(whale_locked_list, key=lambda x: x["diff_2m"], reverse=True)

def main():
    log("=== 開始全市場板塊輪動數據重構 ===")
    
    # 1. 載入股票基本對照
    stock_info = load_stock_info()
    if not stock_info:
        log("[致命錯誤] 無法取得股票基本分類，重構中止。")
        return
        
    # 2. 收集最近 30 個交易日 (為計算歷史軌跡預留充足數據)
    trading_days = collect_trading_days(30)
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
            
            # 計算 5 日累計漲跌幅：使用今日收盤價 (iloc[-1]) 與 5 天前（前第 5 個交易日，即 iloc[-6]）收盤價進行百分比計算
            if len(st_df) >= 6:
                price_today = float(st_df["price"].iloc[-1])
                price_5d_ago = float(st_df["price"].iloc[-6])
                stock_return_5d = ((price_today - price_5d_ago) / price_5d_ago * 100) if price_5d_ago > 0 else 0.0
            else:
                stock_return_5d = 0.0
            
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
            
        # 計算過去 5 個交易日的歷史軌跡坐標 (不包含今日，今日為 iloc[-1])
        # 過去 5 個交易日分別是 iloc[-6] ~ iloc[-2] (即倒數第 6 到第 2 個元素)
        history_5d = []
        history_20d = []
        L = len(sec_df)
        if L >= 2:
            start_idx = max(0, L - 6)
            end_idx = L - 1
            for i in range(start_idx, end_idx):
                h_flow_5d = float(sec_df["today_net_flow_m"].iloc[max(0, i-4):i+1].sum())
                h_flow_20d = float(sec_df["today_net_flow_m"].iloc[max(0, i-19):i+1].sum())
                h_today_flow = float(sec_df["today_net_flow_m"].iloc[i])
                h_prev_5d_flow = sec_df["today_net_flow_m"].iloc[max(0, i-5):i]
                h_prev_5d_avg = float(h_prev_5d_flow.mean()) if not h_prev_5d_flow.empty else 0.0
                h_acceleration = h_today_flow - h_prev_5d_avg
                
                history_5d.append({"x": round(h_flow_5d, 2), "y": round(h_acceleration, 2)})
                history_20d.append({"x": round(h_flow_20d, 2), "y": round(h_acceleration, 2)})

        sectors_results.append({
            "sector_name": sector_name,
            "flow_5d_m": round(flow_5d, 2),
            "flow_20d_m": round(flow_20d, 2),
            "size_20d_m": round(size_20d, 2),
            "acceleration_m": round(acceleration, 2),
            "bottom_fishing_score": round(bottom_fishing_score, 2),
            "bottom_fishing_stocks": bottom_fishing_stocks_data,
            "individual_stocks": individual_stocks_data,
            "history": {
                "5d": history_5d,
                "20d": history_20d
            }
        })
        
    # 執行大戶鎖碼追蹤 (方案 A)
    whale_locked_list = detect_whale_locked_stocks(df, stock_info)
    
    # 5. 輸出為 sectors_data.json
    output_data = {
        "market_info": {
            "date": latest_date,
            "market_close": market_close,
            "market_return_pct": round(market_return_pct, 2)
        },
        "sectors": sectors_results,
        "whale_locked_stocks": whale_locked_list
    }
    
    output_path = "sectors_data.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)
        
    log(f"全市場抄底重構成功！共處理 {len(sectors_results)} 個產業分類板塊，已成功生成 {output_path}。")

if __name__ == "__main__":
    main()
