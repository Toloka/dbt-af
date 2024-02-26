{% macro dedublicate_table(model_name) %}

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

        {% if relation and model_node.config.unique_key %}

            {% set keys_list = (
                model_node.config.unique_key
                if model_node.config.unique_key is string
                else ", ".join(model_node.config.unique_key)
            ) %}

            {% set condition_part %}
        {%if model_node.config.partition_by %}
            where
            {% for column_name in model_node.config.partition_by if '_dt' in column_name or 'date' in column_name  %}
                {{increment_dttm_condition(column_name, None, 0, None)}}
            {% if not loop.last %}
            and
            {% endif %}
            {% endfor %}
        {% endif %}
            {% endset %}

            {% set count_dubles %}
    select count(1) from (
        select {{keys_list}}, count(*)
            from {{relation}}
            {{condition_part}}
            group by {{keys_list}}
            having count(*)>1
    );
            {% endset %}

            {% set results = run_query(count_dubles) %}
            {% set dubles_count = results[0][0] %}

            {% if dubles_count != 0 %}

                {% set relation_dedup -%}
        `{{model_node.database}}`.`{{model_node.schema}}`.`{{model_node.alias}}__dedublication_tmp`
                {%- endset %}

                {% set delete_query_s1 %}
            create table {{relation_dedup}} as
            select row_number() over (
                    partition by {{keys_list}}
                    order by etl_updated_dttm desc
                    ) as rn
                    , *
            from {{relation}}
            {{condition_part}};
                {% endset %}

                {% set delete_query_s2 %}
            delete from {{relation}}
            {{condition_part}};
                {% endset %}

                {% set delete_query_s3 %}
            insert into {{relation}}
            select * except (rn) from {{relation_dedup}}
            where rn=1;
                {% endset %}

                {% set delete_query_s4 %}
            drop table {{relation_dedup}};
                {% endset %}

                {% set result_s1 = run_query(delete_query_s1) %}
                {% set result_s2 = run_query(delete_query_s2) %}
                {% set result_s3 = run_query(delete_query_s3) %}
                {% set result_s4 = run_query(delete_query_s4) %}

            {% endif %}

        {% endif %}

    {% endif %}

{% endmacro %}
