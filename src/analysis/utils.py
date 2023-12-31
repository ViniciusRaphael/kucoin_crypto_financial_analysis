import json
import pandas as pd
import requests as re
import pandas_ta as ta
import plotly.graph_objs as go
from datetime import datetime

NM_ADX_GROUP = {
    '0-25': '1. Absent or Weak Trend',
    '25-50': '2. Strong Trend',
    '50-75': '3. Very Strong Trend',
    '75-100': '4. Extremely Strong Trend'
}

DT_TODAY = datetime.today().date()

def classify_adx_value(value):
    """
    Checks the ADX value against predefined ranges and returns the corresponding trend category.

    Args:
        value (int): The ADX value to be categorized.

    Returns:
        str or None: The category name for the provided ADX value.
    """    
    for key, name in NM_ADX_GROUP.items():
        range_start, range_end = map(int, key.split('-'))
        if range_start <= value <= range_end:
            return name
    return None

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


class KucoinCryptoData:
    def get_kucoin_symbols():
        resp = re.get('https://api.kucoin.com/api/v2/symbols')
        ticker_list = json.loads(resp.content)
        symbols_list = [ticker['symbol'] for ticker in ticker_list['data'] if str(ticker['symbol'][-4:]) == 'USDT']

        return symbols_list

    def get_annual_ohlcv(symbol = 'BTC-USDT'):
        """
            Retrieves and processes market data for a given symbol from the KuCoin API.

            Args:
                symbol (str): The trading symbol for which data is to be fetched.

            Returns:
                pandas.DataFrame: Processed DataFrame containing market data with various technical indicators.
            """
        # Fetch data from KuCoin API
        kucoin = re.get(f'https://api.kucoin.com/api/v1/market/candles?type=1day&symbol={symbol}')

        
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
            df['nm_adx_trend'] = df['vl_adx'].apply(classify_adx_value)

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
        
class ProcessData:
    def process_crypto_data(num_symbols = 0):
        """
        Fetches and processes data for a specified number of crypto symbols from the KuCoin API.

        Args:
            num_symbols (int): Number of symbols to fetch data for (default is 10).

        Returns:
            pandas.DataFrame: Concatenated DataFrame containing processed data for the specified symbols.
        """
        testing = []

        symbols = KucoinCryptoData.get_kucoin_symbols()
        total_items = len(symbols)
        if num_symbols != 0:
            num_symbols
        else:
            num_symbols = total_items

        for index, crypto_symbol in enumerate(symbols[:num_symbols], start=1):
            try:
                data = KucoinCryptoData.get_annual_ohlcv(symbol = crypto_symbol)
                if not data.empty:  # Check if the dataframe is not empty before appending
                    testing.append(data)
                    print(f"Item {index}/{total_items}: {crypto_symbol}")
            except Exception as e:
                print(f"Error processing {crypto_symbol}: {str(e)}")
                pass

        if testing:  # Check if the list is not empty before concatenating
            df = pd.concat(testing, ignore_index=True)
        else:
            # Handle the case when no data is available
            df = pd.DataFrame()

        return df
    
    def filter_indicators_today(dataframe, vl_adx_min = 25, date = DT_TODAY ,vl_macd_hist_min = 0, vl_macd_delta_min = 0.01, qt_days_supertrend_positive = 1):
        """
        Filters the concatenated DataFrame to select specific indicators for the current date.

        Args:
            dataframe (pandas.DataFrame): Concatenated DataFrame containing processed data.

        Returns:
            pandas.DataFrame: Filtered DataFrame based on specific indicators for the current date.
        """

        df_indicators = dataframe.loc[
            (dataframe['vl_adx'] >= vl_adx_min) &
            (dataframe['dt_date'].dt.date == date) &

            # Ichimoku with price above conversion line and base line
            (dataframe['vl_price_close'] > dataframe['vl_conversion_line']) &
            (dataframe['vl_price_close'] > dataframe['vl_base_line']) &

            # vl_macd histogram greater than 0 and signal greater than vl_macd
            (dataframe['vl_macd_hist'] >= vl_macd_hist_min) &
            (dataframe['vl_macd_delta'] >= vl_macd_delta_min) &
            (dataframe['qt_days_supertrend_positive'] >= qt_days_supertrend_positive)
        ]

        return df_indicators
    
    def extract_crypto_trend(df_indicators):
        """
        Extracts specific columns from the filtered DataFrame and sorts the data based on indicators.

        Args:
            df_indicators (pandas.DataFrame): Filtered DataFrame containing indicator data.

        Returns:
            pandas.DataFrame: Extracted and sorted DataFrame based on specific columns and indicators.
        """
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
        ].sort_values(by=['nm_adx_trend', 'vl_avg_days_indicatores'], ascending=False).reset_index(drop=True)

        return df_crypto_trend

class IndicatorsPlot:
    def vl_adx_chart(symbol = 'BTC-USDT'): 
        df = KucoinCryptoData.get_annual_ohlcv(symbol)
        
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
            title= f'ADX Value for {symbol}',
            xaxis_title='Date',
            yaxis_title='Values'
        )

        # Show the plot
        fig.show()

    def ichimoku_chart(symbol = 'BTC-USDT'): 
        df = KucoinCryptoData.get_annual_ohlcv(symbol)
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
        fig2.update_layout(title=f'Price Candlestick with Ichimoku Cloud Indicators for {symbol}',
                        xaxis_title='Date',
                        yaxis_title='Price',
                        showlegend=True)

        # Display fig2ure
        fig2.show()
        

