import yfinance as yf
from utils import KucoinCryptoData as kc
import pandas as pd
import time

# Get a list of cryptocurrency symbols from Kucoin
kucoin_list = kc.get_kucoin_symbols()

# Remove the last character from each symbol in the list
crypto_list = [i[:-1] for i in kucoin_list]

try:
    # Try to open the historical data file, if it exists
    with open("data/historical_crypto_data.csv") as file:
        df = pd.read_csv(file)

except FileNotFoundError:
    # If the file doesn't exist, create dataframes for historical cryptocurrency data
    dataframes = []

    # Loop through the first 25 symbols in the list
    for crypto in crypto_list:
        print(crypto)

        # Get historical data using yfinance for the symbol
        crypto_data = yf.Ticker(f"{crypto}").history(start="2020-01-01")
        crypto_data['symbol'] = crypto

        # Add data to the dataframe if it's not empty
        if not crypto_data.empty:
            dataframes.append(crypto_data)
            time.sleep(2)

    # Concatenate the dataframes and save to CSV
    hist_data = pd.concat(dataframes)
    hist_data.to_csv("data/historical_crypto_data.csv")

else:
    # If the file exists, read CSV into a DataFrame
    dataframes = []

    # Loop through the first 25 symbols in the list
    for crypto in crypto_list:
        print(crypto)

        # Check if the symbol is already present in the existing data
        if crypto in df['symbol'].values:
            # Get historical data from the last recorded date in the existing data
            max_date = pd.to_datetime(df[df['symbol'] == crypto]['Date'], errors='coerce').max()
            crypto_data = yf.Ticker(crypto).history(start=max_date)
            crypto_data['symbol'] = crypto

            # Add data to the dataframe if it's not empty
            if not crypto_data.empty:
                dataframes.append(crypto_data)
                time.sleep(2)
        else:
            # If the symbol is not present, get historical data from a default start date
            crypto_data = yf.Ticker(crypto).history(start="2020-01-01")
            crypto_data['symbol'] = crypto

            # Add data to the dataframe if it's not empty
            if not crypto_data.empty:
                dataframes.append(crypto_data)
                time.sleep(2)

    # Concatenate the dataframes and existing data, remove duplicates, and save to CSV
    final = pd.concat([df] + dataframes)
    final.drop_duplicates(inplace=True)
    final.to_csv("data/historical_crypto_data.csv", index=False)

print("Finished")
