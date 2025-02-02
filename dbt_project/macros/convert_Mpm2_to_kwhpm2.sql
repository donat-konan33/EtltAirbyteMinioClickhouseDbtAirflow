{% macro convert_mj_per_m2_to_kwh_per_m2(mj_per_m2) %}
    ROUND(({{ mj_per_m2 }} * 0.2778), 0)
{% endmacro %}
