# ## Libs
# %%
from utils import ProcessData as pr

import plotly.graph_objs as go
import plotly.express as px

df_concat = pr.process_crypto_data(100)
df_filtered = pr.filter_indicators_today(df_concat)
crypto_trend = pr.extract_crypto_trend(df_filtered)

crypto_trend