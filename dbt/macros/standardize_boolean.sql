{% macro standardize_boolean(column_name) %}
    case 
        when lower(trim(cast({{ column_name }} as varchar))) in ('yes', 'true', '1', 't', 'y') then true
        when lower(trim(cast({{ column_name }} as varchar))) in ('no', 'false', '0', 'f', 'n', '', 'none', 'null', 'na', 'n/a') then false
        when {{ column_name }} is null then false
        else false
    end
{% endmacro %}

