-- Name: int_crypto_indicators

WITH stg_crypto_hist_price AS (
    SELECT *
    FROM {{ ref('stg_crypto_hist_price')}}
),


final AS (
    SELECT
        *,
        AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS vl_ma7,
        AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS vl_ma30,
        AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS vl_ma100,
        AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS vl_ma200,
        SUM(CASE WHEN vl_close > vl_open THEN vl_close - vl_open ELSE 0 END) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS vl_gain,
        SUM(CASE WHEN vl_open > vl_close THEN vl_open - vl_close ELSE NULL END) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS vl_loss,
        (AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) - AVG(vl_close) OVER (PARTITION BY nm_symbol ORDER BY dt_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)) AS vl_macd
    FROM stg_crypto_hist_price
)

SELECT *
FROM final

