{# Fails on every row where the column is zero or negative (NULLs are left to not_null). #}
{% test positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}
