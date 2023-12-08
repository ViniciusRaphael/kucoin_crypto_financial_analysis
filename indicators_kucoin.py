# ## Libs
# %%
from binance.client import Client
from binance.enums import HistoricalKlinesType
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
pd.set_option('display.max_rows', 500)
import requests,json
import plotly.graph_objs as go
import plotly.express as px


# Define column names for the DataFrame
colnames = [
    'time', 'Open', 'High', 'Low', 'Close', 'vl_volume', 'Close time',
    'Quote asset vl_volume', 'Number of trades', 'Taker buy base asset vl_volume',
    'Taker buy quote asset vl_volume', 'Ignore'
]

# Get all USDT pairs
BASE_URL = 'https://api.kucoin.com'
resp = requests.get(BASE_URL+'/api/v2/symbols')
ticker_list = json.loads(resp.content)
symbols = [ticker['symbol'] for ticker in ticker_list['data'] if str(ticker['symbol'][-4:]) == 'USDT']

interval = '1d'  # Supports intervals like 1m, 1H, 1D, etc.
start = (datetime.today() - timedelta(days=90)).strftime('%m/%d/%Y 00:00:00')   # Start date
end = datetime.today().strftime('%m/%d/%Y 00:00:00') # End date


dicionario_vl_adx = {
    '0-25': '1. Absent or Weak Trend',
    '25-50': '2. Strong Trend',
    '50-75': '3. Very Strong Trend',
    '75-100': '4. Extremely Strong Trend'
}

def check_adx(value):
    """
    Checks the ADX value against predefined ranges and returns the corresponding trend category.

    Args:
        value (int): The ADX value to be categorized.

    Returns:
        str or None: The category name for the provided ADX value.
    """    
    for key, name in dicionario_vl_adx.items():
        range_start, range_end = map(int, key.split('-'))
        if range_start <= value <= range_end:
            return name
    return None

# Function to count positive values and reset count on encountering negative value
def count_positive_reset(df_column):
    """
    Counts consecutive positive values in a DataFrame column and resets count on encountering negative values.

    Args:
        df_column (pandas.Series): The DataFrame column to be processed.

    Returns:
        list: A list containing counts of consecutive positive values.
    """    
    count = 0
    counts = []

    for value in df_column:
        if value > 0:
            count += 1
        else:
            count = 0  # Reset count if a negative value is encountered
        counts.append(count)

    return counts

def getting_data(symbol):
    """
        Retrieves and processes market data for a given symbol from the KuCoin API.

        Args:
            symbol (str): The trading symbol for which data is to be fetched.

        Returns:
            pandas.DataFrame: Processed DataFrame containing market data with various technical indicators.
        """
    # Fetch data from KuCoin API
    kucoin = requests.get(f'https://api.kucoin.com/api/v1/market/candles?type=1day&symbol={symbol}')

    if kucoin.status_code == 200:  # Check if the request is successful
        data = json.loads(kucoin.text)  # Convert response to JSON
        # Create a DataFrame from the JSON response
        df = pd.DataFrame(data['data'], columns=['unix', 'price_open', 'price_close', 'price_high', 'price_low', 'volume', 'turnover'])
        df['nm_symbol'] = f'{symbol.replace("-", "")}'  # Add a formatted symbol column
        df['dt_date'] = pd.to_datetime(df['unix'].astype(int), unit='s')  # Convert UNIX time to datetime
        df.sort_values(by='dt_date', inplace=True)  # Sort DataFrame by date

        # Convert certain columns to float
        df[['price_open', 'price_close', 'price_high', 'price_low']] = df[['price_open', 'price_close', 'price_high', 'price_low']].astype(float)

        # Calculate ADX, DI+ and DI-
        df[['vl_adx', 'vl_dmp', 'vl_dmn']] = ta.adx(df['price_high'], df['price_low'], df['price_close'], length=14)
        df['nm_adx_trend'] = df['vl_adx'].apply(check_adx)

        # Calculate Ichimoku Cloud
        ichimoku_values = ta.ichimoku(df['price_high'], df['price_low'], df['price_close'])[0]
        df[['vl_leading_span_a', 'vl_leading_span_b', 'vl_conversion_line', 'vl_base_line', 'vl_lagging_span']] = ichimoku_values
        df['vl_price_over_conv_line'] = df['price_close'] - df['vl_conversion_line']
        df['qt_days_ichimoku_positive'] = count_positive_reset(df['vl_price_over_conv_line'])

        # Calculate MACD
        macd_values = ta.macd(df['price_close'])
        df[['vl_macd', 'vl_macd_hist', 'vl_macd_signal']] = macd_values.apply(lambda x: x.astype(float))
        df['vl_macd_delta'] = df['vl_macd'] - df['vl_macd_signal']
        df['qt_days_macd_delta_positive'] = count_positive_reset(df['vl_macd_delta'])

        # Calculate Supertrend
        supertrend_values = ta.supertrend(df['price_high'], df['price_low'], df['price_close'])
        df[['vl_supertrend_trend', 'vl_supertrend_direction', 'vl_supertrend_long', 'vl_supertrend_short']] = supertrend_values
        df['qt_days_supertrend_positive'] = count_positive_reset(df['vl_supertrend_direction'])

        # Calculate average days indicators are positive
        df['vl_avg_days_indicatores'] = round((df['qt_days_ichimoku_positive'] + df['qt_days_macd_delta_positive'] + df['qt_days_supertrend_positive']) / 3, 2)

        # Rename columns for clarity
        df.rename(columns={
            'price_open': 'vl_price_open',
            'price_close': 'vl_price_close',
            'price_high': 'vl_price_high',
            'price_low': 'vl_price_low',
            'volume': 'vl_volume'
        }, inplace=True)

        return df  # Return the processed DataFrame
    else:
        print('Error on connection')
        
        return None  # Return None if there is an error in the API call


testing = []

total_items = len(symbols)
for index, crypto in enumerate(symbols[:10], start=1):
    try:
        data = getting_data(crypto)
        if not data.empty:  # Check if the dataframe is not empty before appending
            testing.append(data)
            print(f"Item {index}/{total_items}: {crypto}")
    except:
        pass

if testing:  # Check if the list is not empty before concatenating
    df_concat = pd.concat(testing, ignore_index=True)
else:
    # Handle the case when no data is available
    df_concat = pd.DataFrame()

today = datetime.today().date()
df_indicators = df_concat.loc[
                (df_concat['vl_adx'] >= 25) & 
                (df_concat['dt_date'].dt.date == today) &

                # Ichimoku com menos preço maior do que linha de conversão e linha de base
                (df_concat['vl_price_close'] > df_concat['vl_conversion_line']) &
                (df_concat['vl_price_close'] > df_concat['vl_base_line']) &

                # vl_macd histograma maior do que 0 e signal maior do que vl_macd
                (df_concat['vl_macd_hist'] >= 0) &
                (df_concat['vl_macd_delta'] >= 0.01) &
                (df_concat['qt_days_supertrend_positive'] >= 1)
                ]

# ## Possibilities of Profit

df_crypto_trend = df_indicators[
    [
        'nm_symbol', 
        'dt_date', 
        'vl_price_open', 
        'vl_volume', 
        'vl_adx', 
        'nm_adx_trend',
        'qt_days_ichimoku_positive', 
        'vl_macd_delta',
        'qt_days_macd_delta_positive', 
        'qt_days_supertrend_positive',
        'vl_avg_days_indicatores'
        ]
        ].sort_values(by=['nm_adx_trend', 'vl_avg_days_indicatores'], ascending=False).reset_index()

df_crypto_trend

# ## Follow up of orders

order_book = pd.read_json('order_book.json').T.reset_index()
order_book['index'] = order_book['index'].replace('-', '', regex=True)
open_orders = order_book['index'].loc[~order_book['sold_date'].notnull()]

df_fup = df_concat.loc[
                (df_concat['nm_symbol'].isin(open_orders) ) & 
                (df_concat['dt_date'].dt.date == today)
                ]

df_fup[['nm_symbol', 'dt_date', 'vl_price_open', 'vl_volume', 'vl_adx', 'nm_adx_trend','qt_days_ichimoku_positive', 'vl_macd_delta','qt_days_macd_delta_positive', 'vl_supertrend_direction']].sort_values(by=['vl_adx'], ascending=False).reset_index()
# ## Charts

def vl_adx_chart(symbol): 
    df = df_concat.loc[df_concat['nm_symbol'] == f'{symbol}']
    
    # Create the vl_adx trace
    vl_adx_line = go.Scatter(
        x=df['dt_date'],
        y=df['vl_adx'],
        mode='lines',
        name='vl_adx',
        line=dict(color='black')  # Color for vl_adx line
    )

    # Create the DI+ trace
    plus_di_line = go.Scatter(
        x=df['dt_date'],
        y=df['vl_dmp'],
        mode='lines',
        name='DI+',
        line=dict(color='green')  # Color for DI+
    )

    # Create the DI- trace
    minus_di_line = go.Scatter(
        x=df['dt_date'],
        y=df['vl_dmn'],
        mode='lines',
        name='DI-',
        line=dict(color='red')  # Color for DI-
    )
    # Create a fixed line for value 20 in light grey
    fixed_line = go.Scatter(
        x=df['dt_date'],
        y=[20] * len(df),  # Creating a list of 20s to match the length of the DataFrame
        mode='lines',
        name='Fixed Line (20)',
        line=dict(color='lightgrey', dash='dash')  # Color and style for the fixed line
    )

    # Create the figure for vl_adx chart
    fig = go.Figure(data=[vl_adx_line, plus_di_line, minus_di_line, fixed_line])

    # Update layout and add title
    fig.update_layout(
        title='vl_adx, DI+ and DI- Line Chart with Fixed Line at 20 using Plotly',
        xaxis_title='Date',
        yaxis_title='Values'
    )

    # Show the plot
    fig.show()

def ichimoku_chart(symbol): 
    df = df_concat.loc[df_concat['nm_symbol'] == f'{symbol}']
    # Create Ichimoku Cloud chart
    fig2 = go.Figure()

    # Add candlestick chart
    fig2.add_trace(go.Candlestick(x=df['dt_date'],
                                 open=df['vl_price_open'],
                                 high=df['vl_price_high'],
                                 low=df['vl_price_low'],
                                 close=df['vl_price_close'],
                                 name='Candlestick'))

    # Add Ichimoku Cloud lines
    fig2.add_trace(go.Scatter(x=df['dt_date'], y=df['vl_conversion_line'], mode='lines', name='Conversion Line', line=dict(color='blue')))
    fig2.add_trace(go.Scatter(x=df['dt_date'], y=df['vl_base_line'], mode='lines', name='Base Line', line=dict(color='red')))
    fig2.add_trace(go.Scatter(x=df['dt_date'], y=df['vl_leading_span_a'], mode='lines', name='Leading Span A'))
    fig2.add_trace(go.Scatter(x=df['dt_date'], y=df['vl_leading_span_b'], mode='lines', name='Leading Span B'))

    # Update layout
    fig2.update_layout(title='Price Candlestick with Ichimoku Cloud Indicators',
                      xaxis_title='Date',
                      yaxis_title='Price',
                      showlegend=True)

    # Display fig2ure
    fig2.show()

def combined_charts(symbol):
    vl_adx_chart(symbol)
    ichimoku_chart(symbol)

# Chamar a função para criar os gráficos lado a lado
combined_charts('ATOMUSDT')

# %%
