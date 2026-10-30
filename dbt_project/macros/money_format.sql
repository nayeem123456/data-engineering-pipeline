{% macro money_round(column_name) %}
    ROUND(CAST({{ column_name }} AS NUMERIC), 2)
{% endmacro %}

{% macro surrogate_key(columns) %}
    MD5(CONCAT_WS('_', {{ columns | join(', ') }}))
{% endmacro %}
