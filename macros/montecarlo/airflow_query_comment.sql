{% macro airflow_query_comment(node) %}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(
        app="dbt",
        dbt_version=dbt_version,
        profile_name=target.get("profile_name"),
        target_name=target.get("target_name"),
        task_id=env_var("AIRFLOW_CTX_TASK_ID", "local_run"),
        dag_id=env_var("AIRFLOW_CTX_DAG_ID", ""),
    ) -%}
    {%- if node is not none -%}
        {%- do comment_dict.update(
            file=node.original_file_path,
            node_id=node.unique_id,
            node_name=node.name,
            resource_type=node.resource_type,
            package_name=node.package_name,
            relation={
                "database": node.database,
                "schema": node.schema,
                "identifier": node.identifier,
            },
        ) -%}
    {% else %} {%- do comment_dict.update(node_id="internal") -%}
    {%- endif -%}
    {% do return(tojson(comment_dict)) %}
{% endmacro %}
