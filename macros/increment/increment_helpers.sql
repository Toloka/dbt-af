{%- macro between_dttm(field, overlap_size=0, direction="left", overlap_flg=True) -%}

    {{ field }}
    between dateadd(
        minute,
        {%- if (
            direction == "left"
            or direction == "both"
            or direction == "shift"
        ) and overlap_flg -%} -{{ overlap_size }},
        {%- else -%} 0,
        {%- endif -%}
        timestamp '{{var("start_dttm")}}'
    ) and dateadd(
        minute,
        {%- if (direction == "right" or direction == "both") and overlap_flg -%}
            {{ overlap_size }},
            {%- elif (direction == "shift") and overlap_flg -%} -{{ overlap_size }},
        {%- else -%} 0,
        {%- endif -%}
        timestamp '{{var("end_dttm")}}'
    )
{% endmacro %}


{%- macro between_dt(field, overlap_size=0, direction="left", overlap_flg=True) -%}

    {{ field }}
    between cast(
        dateadd(
            minute,
            {%- if (
                direction == "left"
                or direction == "both"
                or direction == "shift"
            ) and overlap_flg -%} -{{ overlap_size }},
            {%- else -%} 0,
            {%- endif -%}
            timestamp '{{var("start_dttm")}}'
        ) as date
    ) and cast(
        dateadd(
            minute,
            {%- if (direction == "right" or direction == "both") and overlap_flg -%}
                {{ overlap_size }},
                {%- elif (direction == "shift") and overlap_flg -%} -{{ overlap_size }},
            {%- else -%} -1,
            {%- endif -%}
            timestamp '{{var("end_dttm")}}'
        ) as date
    )
{% endmacro %}


{%- macro increment_dttm_condition(
    dttm_field=None, dt_field=None, overlap_size=0, direction="left"
) -%}

    {% if dttm_field %}

        {{
            between_dttm(
                field=dttm_field,
                overlap_size=overlap_size,
                direction=direction,
                overlap_flg=var("overlap"),
            )
        }}

    {% endif %}

    {% if dttm_field and dt_field %} and {% endif %}

    {% if dt_field %}
        {{
            between_dt(
                field=dt_field,
                overlap_size=overlap_size,
                direction=direction,
                overlap_flg=var("overlap"),
            )
        }}
    {%- endif -%}

{%- endmacro -%}


{%- macro increment_universal_dttm_condition(
    dttm_field=None,
    dt_field=None,
    business_overlap_size=0,
    tech_overlap_size=0,
    direction="left"
) -%}

    {% set overlap = business_overlap_size %}

    {% if var("overlap") %} {% set overlap = overlap + tech_overlap_size %} {% endif %}

    {% if dttm_field %}

        {{
            between_dttm(
                field=dttm_field,
                overlap_size=overlap,
                direction=direction,
                overlap_flg=True,
            )
        }}

    {% endif %}

    {% if dttm_field and dt_field %} and {% endif %}

    {% if dt_field %}
        {{
            between_dt(
                field=dt_field,
                overlap_size=overlap,
                direction=direction,
                overlap_flg=True,
            )
        }}
    {%- endif -%}

{%- endmacro -%}


{%- macro increment_business_dttm_condition(
    dttm_field=None, dt_field=None, overlap_size=0, direction="left"
) -%}

    {% if dttm_field %}

        {{
            between_dttm(
                field=dttm_field,
                overlap_size=overlap_size,
                direction=direction,
                overlap_flg=True,
            )
        }}

    {% endif %}

    {% if dttm_field and dt_field %} and {% endif %}

    {% if dt_field %}
        {{
            between_dt(
                field=dt_field,
                overlap_size=overlap_size,
                direction=direction,
                overlap_flg=True,
            )
        }}
    {%- endif -%}

{%- endmacro -%}

{%- macro increment_dttm_condition_with_window(
    dttm_field=None, dt_field=None, window_size_hours=0
) -%}

    {{
        increment_business_dttm_condition(
            dttm_field=dttm_field,
            dt_field=dt_field,
            overlap_size=window_size_hours * 60,
            direction="left",
        )
    }}

{%- endmacro -%}

{%- macro explode_on_timerange(window_size_minutes=0) -%}
    {% if window_size_minutes|int % 60 == 0 %}
        explode(
            sequence(
                timestampadd(
                    minute, -{{ window_size_minutes }},
                    timestamp '{{var("start_dttm")}}'
                ),
                timestamp '{{var("end_dttm")}}',
                interval '1 hour'
            )
        )
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `window_size_minutes`. You need to provide a multiple of 60. Got: " ~ window_size_minutes) }}
    {% endif %}
{%- endmacro -%}

{%- macro filter_exploded_by_window(filter_field=None, exploded_field=None, window_size_minutes=0) -%}
    {% if window_size_minutes|int % 60 == 0 %}
        {{ filter_field }} between timestampadd(
            minute, -{{ window_size_minutes }}, {{ exploded_field }}
        ) and timestampadd(microsecond, -1, {{ exploded_field }})
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `window_size_minutes`. You need to provide a multiple of 60. Got: " ~ window_size_minutes) }}
    {% endif %}
{%- endmacro -%}
