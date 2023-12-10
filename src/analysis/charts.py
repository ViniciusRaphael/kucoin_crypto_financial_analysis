
import pandas as pd
from utils import IndicatorsPlot as ip
order_book = pd.read_json('order_book.json').T.reset_index()
# order_book['index'] = order_book['index'].replace('-', '', regex=True)
open_orders = order_book['index'].loc[~order_book['sold_date'].notnull()]

order_book
for crypto in open_orders:
    ip.vl_adx_chart(crypto)
    ip.ichimoku_chart(crypto)
    