# ## Libs
# %%
from utils import ProcessData as pr

df_concat = pr.process_crypto_data(792)
df_filtered = pr.filter_indicators_today(df_concat)
crypto_trend = pr.extract_crypto_trend(df_filtered)

crypto_trend