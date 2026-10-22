{# Fails when the model and compare_model have a different number of rows (a join added or lost rows). #}
{% test equal_row_count(model, compare_model) %}
with counts as (
    select
        (select count(*) from {{ model }})         as model_rows,
        (select count(*) from {{ compare_model }}) as compare_rows
)
select * from counts where model_rows <> compare_rows
{% endtest %}
