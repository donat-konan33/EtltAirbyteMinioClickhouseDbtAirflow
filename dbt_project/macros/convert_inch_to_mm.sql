--convert precipitation in inches to mm
{% macro convert_inch_to_mm(precip) %}
    ROUND(({{ precip }} * 25.4), 0)
{% endmacro %}
