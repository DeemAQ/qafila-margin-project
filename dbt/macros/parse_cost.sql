{#
  Supplier cost text -> DOUBLE, or NULL when it isn't a number.
  Handles the formats the supplier files use: '9.49', '9,49' (decimal comma) and '٩٫٤٩'
  (Arabic-Indic digits, also the Persian/Urdu forms, with the Arabic decimal separator).
#}
{% macro parse_cost(column) -%}
    try_cast(
        replace(
            translate(trim({{ column }}), '٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫', '01234567890123456789.'),
            ',', '.'
        ) as double
    )
{%- endmacro %}
