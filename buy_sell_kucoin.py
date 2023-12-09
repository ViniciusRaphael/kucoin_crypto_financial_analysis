# %%
import pandas as pd
pd.set_option('display.max_rows', 500)
import requests,json

order_book = pd.read_json('order_book.json').T.reset_index()

order_book['profit_usd'] = order_book['sold_price'] * order_book['quantity'] - order_book['price'] * order_book['quantity']
order_book['profit_%'] = ((order_book['sold_price'] / order_book['price']) - 1) * 100

df_open = order_book[~order_book['sold_date'].notnull()]

def getting_data(symbol):
    print('Getting ' + symbol)

    kucoin = requests.get(f'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}')
    if kucoin.status_code == 200:  # if result is GOOD
        data = json.loads(kucoin.text)['data']  # load the json response from API
        df = pd.DataFrame([data])  # create dataframe out of json response
        df['price'] = df['price'].map(lambda x: float(x))
        df['symbol'] = f'{symbol.replace("-", "")}'
        df['dt_date'] = pd.to_datetime(df['time'].astype(int), unit='ms')
        df.sort_values(by='dt_date', inplace = True)
        df.drop(columns=['sequence', 'size', 'bestBid', 'bestBidSize', 'bestAsk', 'bestAskSize', 'time'], inplace=True)
    else:
        pass

    return df

testing = []
for index, row in df_open.iterrows():
    crypto = row['index']
    price = row['price']
    try:
        data = getting_data(crypto)
        if not data.empty:  # Check if the dataframe is not empty before appending
            data['price_bought'] = price
            data['profit_loss'] = (((data['price'] / data['price_bought']) - 1) * 100).round(2).astype(float)
            testing.append(data)
    except:
        pass

if testing:  # Check if the list is not empty before concatenating
    df_concat = pd.concat(testing, ignore_index=True)
else:
    # Handle the case when no data is available
    df_concat = pd.DataFrame()

df_concat