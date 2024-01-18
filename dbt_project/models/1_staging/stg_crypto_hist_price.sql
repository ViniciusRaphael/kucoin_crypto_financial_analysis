-- Name: stg_crypto_hist_price

WITH src_crypto_historical_price AS (
    SELECT * FROM {{ source('localhost', 'crypto_historical_price') }}
),

final AS (
    SELECT
        "symbol" AS nm_symbol,
        "Open" AS vl_open,
        "High" AS vl_high,
        "Low" AS vl_low,
        "Close" AS vl_close,
        "Volume" AS vl_volume,
        "Dividends" AS vl_dividends,
        CAST("Date" AS DATE) AS dt_date
    FROM src_crypto_historical_price
)

SELECT * FROM final
