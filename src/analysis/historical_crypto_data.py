import yfinance as yf
from utils import KucoinCryptoData as kc
import pandas as pd

# Get a list of cryptocurrency symbols from Kucoin
crypto_list = kc.get_kucoin_symbols()

# Remove the last character from each symbol in the list
new_list = [i[:-1] for i in crypto_list]

try:
    # Try to open the historical data file, if it exists
    with open("data/historical_crypto_data.csv") as f:
        df = pd.read_csv(f)

except FileNotFoundError:
    # If the file doesn't exist, create dataframes for historical cryptocurrency data
    dataframes = []

    # Loop through the first 25 symbols in the list
    for i in new_list[:25]:
        print(f"{i}")

        # Get historical data using yfinance for the symbol
        crypto_data = yf.Ticker(f"{i}").history(start="2020-01-01")
        crypto_data['symbol'] = i

        # Add data to the dataframe if it's not empty
        if not crypto_data.empty:
            dataframes.append(crypto_data)

    # Concatenate the dataframes and save to CSV
    hist_data = pd.concat(dataframes)
    hist_data.to_csv("data/historical_crypto_data.csv", index=False)

else:
    # If the file exists, read CSV into a DataFrame
    dataframes = []

    # Loop through the first 25 symbols in the list
    for i in new_list[:25]:
        print(f"{i}")

        # Check if the symbol is already present in the existing data
        if i in df['symbol'].values:
            # Get historical data from the last recorded date in the existing data
            crypto_data = yf.Ticker(f"{i}").history(start=pd.to_datetime(df[df['symbol'] == i]['Date'].max()))
            crypto_data['symbol'] = i

            # Add data to the dataframe if it's not empty
            if not crypto_data.empty:
                dataframes.append(crypto_data)
        else:
            # If the symbol is not present, get historical data from a default start date
            crypto_data = yf.Ticker(f"{i}").history(start="2020-01-01")
            crypto_data['symbol'] = i

            # Add data to the dataframe if it's not empty
            if not crypto_data.empty:
                dataframes.append(crypto_data)

    # Concatenate the dataframes and existing data, remove duplicates, and save to CSV
    final = pd.concat([df] + dataframes)
    final.drop_duplicates(inplace=True)
    final.to_csv("data/historical_crypto_data.csv", index=False)
