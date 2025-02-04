{% macro convert_mph_to_kph(speed) %}
    ROUND(({{ speed }} * 1.60934), 0)
{% endmacro %}
