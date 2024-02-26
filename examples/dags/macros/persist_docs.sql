{% macro persist_docs_table(model_name) %}

    {% if execute %}

        {{ log(graph.groups) }}

        {% set model_node_list = (
            graph.nodes.values()
            | selectattr("name", "equalto", model_name)
            | selectattr("resource_type", "equalto", "model")
            | list
        ) %}

        {% if model_node_list | length > 0 %}

            {% set model_node = model_node_list[0] %}
            {% set relation = adapter.get_relation(
                database=model_node.database,
                schema=model_node.schema,
                identifier=model_node.alias,
            ) %}

            {% if relation and relation.type == "table" %}

                {% set get_description %}
        describe detail {{ relation }} ;
                {% endset %}

                {% set results = run_query(get_description) %}
                {% set relation_desc = results[0]["description"] %}

                {% set table_comment = model_node["description"] %}

                {% if table_comment | trim == "" %}
                    {% set node_list = (
                        graph.nodes.values()
                        | selectattr("name", "equalto", model_name)
                        | list
                    ) %}
                    {% if node_list | length > 0 %}
                        table_comment = node_list[0].block_contents
                    {% endif %}

                {% endif %}

                {% set escaped_table_comment = (
                    table_comment | replace("'", "\\'") | truncate(250)
                ) %}

                {% if not escaped_table_comment | string() == relation_desc | string() %}

                    {% set comment_query %}
                COMMENT ON TABLE {{ relation }} IS '{{ escaped_table_comment }}';
                    {% endset %}
                    {% do run_query(comment_query) %}
                {% endif %}

                {%- set columns = adapter.get_columns_in_relation(relation) -%}

                {% set exist_column_names = [] %}
                {% for clmn in columns %}
                    {{ exist_column_names.append(clmn.name) }}
                {% endfor %}

                {% set describe_column %}
                    select column_name, comment from `system`.`information_schema`.`columns`
                    where concat('`',table_catalog,'`.`',table_schema,'`.`',table_name,'`') = '{{ relation }}' 
                {% endset %}

                {% set clmn_results = run_query(describe_column) %}

                {% set clmn_comment_dict = {} %}
                {% for result_row in clmn_results %}
                    {% do clmn_comment_dict.update({result_row[0]: result_row[1]}) %}
                {% endfor %}

                {% for column_name in model_node.columns if column_name in exist_column_names %}

                    {% set comment = model_node.columns[column_name]["description"] %}
                    {% if comment | trim == "" %}
                        {% set node_list = (
                            graph.nodes.values()
                            | selectattr("name", "equalto", column_name)
                            | list
                        ) %}
                        {% if node_list | length > 0 %}
        comment = node_list[0].block_contents
    {% endif %}

{% endif %}

{% set escaped_comment = comment | replace("'", "\\'") | truncate(250) %}

{% set column_description=clmn_comment_dict[column_name] %}


{% if not escaped_comment|string() == column_description|string() and escaped_comment != '' %}
 {{ log("escaped_comment: '" ~ escaped_comment ~ "'" ) }}
 {{ log("column_description: '" ~ column_description ~ "'" ) }}

{% set comment_query %}
            alter table {{ relation }} change column
                {{ adapter.quote(column_name) if model_node.columns[column_name]['quote'] else column_name }}
                    comment '{{ escaped_comment }}';
{% endset %}
{% do run_query(comment_query) %}
{% endif %}
{% endfor %}

{% endif %}

{% endif %}

{% endif %}

{% endmacro %}

{% macro persist_docs_full() %}

{% for model_node in graph.nodes.values() %}

{% do persist_docs_table(model_node["name"]) %}

{% endfor %}

{% endmacro %}
