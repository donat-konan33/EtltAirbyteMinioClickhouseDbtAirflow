--convert precipitation in inches to mm
{% macro convert_inch_to_cm(precip) %}
    ROUND(({{ precip }} * 2.54), 0)
{% endmacro %}
