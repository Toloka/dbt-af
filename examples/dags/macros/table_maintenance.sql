{% macro set_ttl_on_table(
    model_name,
    key,
    expiration_timeout,
    additional_predicate,
    force_predicate
) %}

    {% if execute %}
        {% set model_node = (
            graph.nodes.values()
            | selectattr("name", "equalto", model_name)
            | list
        )[0] %}
        {{ log(model_node) }}
        {% set relation = adapter.get_relation(
            database=model_node.database,
            schema=model_node.schema,
            identifier=model_node.alias,
        ) %}

        {{ log(generate_alias_name(node=model_node)) }}

        {% if relation %}
            {% set ttl_query %}
            delete from {{ relation }}
            where
                1=1
                {% if not force_predicate %}
                    and {{ key }} < date_add(DAY, -{{expiration_timeout}}, current_timestamp())
                    {% if additional_predicate %}
                    and {{ additional_predicate }}
                    {% endif %}
                {% else %}
                    and {{ force_predicate }}
                {% endif %}
            ;
            {% endset %}
            {% do run_query(ttl_query) %}
        {% endif %}

    {% endif %}

{% endmacro %}


{% macro vacuum_table(model_name) %}

    {% if execute %}
        {% set model_node = (
            graph.nodes.values()
            | selectattr("name", "equalto", model_name)
            | list
        )[0] %}
        {{ log(model_node) }}
        {% set relation = adapter.get_relation(
            database=model_node.database,
            schema=model_node.schema,
            identifier=model_node.alias,
        ) %}

        {{ log(generate_alias_name(node=model_node)) }}

        {% if relation %}
            {% set vacuum_query %}
            VACUUM {{ relation }};
            {% endset %}
            {% do run_query(vacuum_query) %}
        {% endif %}

    {% endif %}

{% endmacro %}


{% macro optimize_table(model_name) %}

    {% if execute %}
        {% set model_node = (
            graph.nodes.values()
            | selectattr("name", "equalto", model_name)
            | list
        )[0] %}
        {% set relation = adapter.get_relation(
            database=model_node.database,
            schema=model_node.schema,
            identifier=model_node.alias,
        ) %}

        {{ log(model_node.config.partition_by) }}

        {% if relation %}
            {% set optimize_query %}
            OPTIMIZE {{ relation }}

        {%if model_node.config.partition_by %}
            where 1=1
            {% for column_name in model_node.config.partition_by %}
                {%if '_dt' in column_name or 'date' in column_name %}
                and    {{increment_dttm_condition(column_name, None, 0, None)}}
                {% endif %}
            {% endfor %}
            ;
        {% endif %}

            {% endset %}
            {% do run_query(optimize_query) %}
        {% endif %}

    {% endif %}

{% endmacro %}
