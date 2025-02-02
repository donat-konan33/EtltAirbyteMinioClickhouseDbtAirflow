-- Convert °F temperature to °C temperature
{% macro convert_f_to_c(temperature) %}
    ROUND(({{ temperature }} - 32) * (5 / 9), 0)
{% endmacro %}
