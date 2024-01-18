-- Name: int_crypto_indicators

WITH stg_crypto_hist_price AS (
    SELECT *
    FROM {{ ref('stg_crypto_hist_price') }}
),

final AS (
    SELECT
        *,
        AVG(vl_close)
            OVER (
                {{ window_definition(6) }} -- noqa: TMP
            )
            AS vl_ma7,
        AVG(vl_close)
            OVER (
                {{ window_definition(29) }} -- noqa: TMP
            )
            AS vl_ma30,
        AVG(vl_close)
            OVER (
                {{ window_definition(99) }} -- noqa: TMP
            )
            AS vl_ma100,
        AVG(vl_close)
            OVER (
                {{ window_definition(199) }} -- noqa: TMP
            )
            AS vl_ma200,
        SUM(CASE WHEN vl_close > vl_open THEN vl_close - vl_open ELSE 0 END)
            OVER (
                {{ window_definition(23) }} -- noqa: TMP
            )
            AS vl_gain,
        SUM(
            CASE WHEN vl_open > vl_close THEN vl_open - vl_close END
        )
            OVER (
                {{ window_definition(23) }} -- noqa: TMP
            )
            AS vl_loss,
        (
            AVG(vl_close)
                OVER (
                    {{ window_definition(11) }} -- noqa: TMP
                )
            - AVG(vl_close)
                OVER (
                    {{ window_definition(23) }} -- noqa: TMP
                )
        ) AS vl_macd
    FROM stg_crypto_hist_price
)

SELECT *
FROM final
WHERE dt_date = '2024-01-15'
