import eikon as ek
import pandas as pd
from pandas.tseries.offsets import BDay

ek.set_app_key('4d452a26ebaa48009d44197e26613dfd6795c10e')

with open('middle_data.csv', 'r') as file:
    lines = file.readlines()

earnings_dates = {}
current_stock = None
for line in lines:
    line = line.strip()
    if line.startswith("'") and line.endswith("'"):
        current_stock = line.strip("'")
        earnings_dates[current_stock] = []
    else:
        earnings_dates[current_stock].append(line)

trading_days_offset = {
    'NVDA.O': 50,  # NVDA
    'TSM': 50,  # TSM
    'AMD.O': 50,  # AMD
    'AVGO.O': 50,  # AVGO
    'ASML.O': 50,  # ASML
    'AMAT.O': 50,  # AMAT
    'LRCX.O': 50,  # LRCX
    'QCOM.O': 50,  # QCOM
    'INTC.O': 50,  # INTC
    'TXN.O': 50  # TXN
    }

dfs = []

for symbol, dates in earnings_dates.items():
    if symbol in trading_days_offset:
        print(f"Processing data for {symbol}...")
        for earnings_date in dates:
            try:
                earnings_date = pd.to_datetime(earnings_date).date()
                print(f"Earnings Date: {earnings_date}")
            except ValueError:
                print(f"Issue converting date for {symbol}: {earnings_date}")
                continue

            start_date = earnings_date - BDay(trading_days_offset[symbol])
            end_date = earnings_date + BDay(11)
            print(f"Start Date: {start_date}, End Date: {end_date}")

            try:
                data, data_err = ek.get_data(
                    instruments=symbol,
                    fields=['TR.CLOSEPRICE', 'TR.OPENPRICE', 'TR.TSVWAP', 'TR.LOWPRICE', 'TR.HIGHPRICE', 'TR.PriceCloseDate'],
                    parameters={
                        'SDate': start_date.strftime("%Y-%m-%d"),
                        'EDate': end_date.strftime("%Y-%m-%d"),
                        'Frq': 'D'
                    })
                data.dropna(inplace=True)
                # Ensure the data only includes the last 30 trading days before the earnings
                before_earnings_data = data[data['Date'] < earnings_date.strftime("%Y-%m-%d")]
                if len(before_earnings_data) > 29:
                    before_earnings_data = before_earnings_data.tail(29)

                after_earnings_data = data[data['Date'] >= earnings_date.strftime("%Y-%m-%d")]
                if len(after_earnings_data) > 11:
                    after_earnings_data = after_earnings_data.head(11)

                # Append DataFrame to the list, ensuring correct slicing
                dfs.extend([before_earnings_data, after_earnings_data])
            except Exception as e:
                print(f"Error fetching data for {symbol} from {start_date} to {end_date}: {e}")

data = pd.concat(dfs, ignore_index=True)

data.rename(columns={
    'TR.CLOSEPRICE': 'Close',
    'TR.OPENPRICE': 'Open',
    'TR.TSVWAP': 'VWAP',
    'TR.LOWPRICE': 'Low Price',
    'TR.HIGHPRICE': 'High Price',
    'TR.PriceCloseDate': 'Date'
}, inplace=True)

data.dropna(inplace=True)
data['Date'] = pd.to_datetime(data['Date']).dt.date

data.to_csv('middle_data_extraction.csv', index=False)

