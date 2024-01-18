{% macro window_definition(window_size) %}
  PARTITION BY nm_symbol
  ORDER BY dt_date ROWS BETWEEN {{ window_size }} PRECEDING AND CURRENT ROW
{% endmacro %}
