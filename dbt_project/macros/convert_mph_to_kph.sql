{% macro convert_mph_to_kph(speed) %}
    ({{ speed }} * 1.60934)
{% endmacro %}
