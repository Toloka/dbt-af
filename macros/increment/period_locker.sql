{%- macro period_locker(
    cooldown_size=5, error_message="Period is locked", force_flag=False
) -%}

    {% if not force_flag %}
        select
            case
                when
                    cast('{{var("end_dttm")}}' as timestamp)
                    + make_dt_interval({{ cooldown_size }})
                    < cast('{{run_started_at}}' as timestamp)
                then raise_error('{{error_message}}')
            end
    {% endif %}
{% endmacro %}
