import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_close_price(data, ticker, target_date):
    if isinstance(data.columns, pd.MultiIndex):
        if ticker not in data:
            return None
        ticker_data = data[ticker]
    else:
        ticker_data = data
        
    t_date = pd.to_datetime(target_date)
    valid_data = ticker_data[ticker_data.index <= t_date].dropna(subset=["Close"])
    if not valid_data.empty:
        closest_date = valid_data.index[-1]
        close_val = valid_data.loc[closest_date, "Close"]
        if isinstance(close_val, pd.Series):
            close_val = float(close_val.iloc[-1])
        return float(close_val)
    return None

def test_single_groupby():
    tickers = ["2330.TW"]
    start_date = "2026-02-01"
    end_date = "2026-06-16"
    data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')
    print("Single Ticker with group_by='ticker':")
    print("Columns:", data.columns)
    p = get_close_price(data, "2330.TW", "2026-02-13")
    print("  2330.TW on 2026-02-13:", p)

if __name__ == "__main__":
    test_single_groupby()
