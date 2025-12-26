{% macro calculate_interest_rate(balance_column, hasloan_column) %}
    -- Base interest rate based on balance tiers
    case 
        when {{ balance_column }} < 10000 then {{ var('tier_1_rate') }}
        when {{ balance_column }} between 10000 and 20000 then {{ var('tier_2_rate') }}
        else {{ var('tier_3_rate') }}
    end
    -- Add loan bonus if customer has a loan
    + case when {{ hasloan_column }} then {{ var('loan_bonus_rate') }} else 0 end
{% endmacro %}


{% macro calculate_annual_interest(balance_column, interest_rate_column) %}
    round({{ balance_column }} * {{ interest_rate_column }}, 2)
{% endmacro %}


{% macro calculate_new_balance(balance_column, annual_interest_column) %}
    round({{ balance_column }} + {{ annual_interest_column }}, 2)
{% endmacro %}


