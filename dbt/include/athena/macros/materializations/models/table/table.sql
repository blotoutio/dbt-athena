{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set table_type = config.get('table_type', default='hive') | lower -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- cleanup
  {% if not var('table_zero_downtime', false) and 'table_zero_downtime' not in config.get('tags') %}
    {%- if old_relation is not none -%}
      {{ drop_relation(old_relation) }}
    {%- endif -%}
  {%- endif -%}

  -- build model
  {% if var('table_zero_downtime', false) or 'table_zero_downtime' in config.get('tags') %}
    {%- set current_ts = (modules.datetime.datetime.utcnow() - modules.datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000 -%}
    {%- set ctas_id_str = "{0}_{1}".format(identifier, current_ts) -%}
    {%- set ctas_id = ctas_id_str[0:ctas_id_str.index('.')] -%}
    {%- set ctas_relation = '"{0}"."{1}"."{2}"'.format(database, schema, ctas_id)  -%}
    {% call statement('main') -%}
      {{ create_table_as(False, ctas_relation, sql) }}
    {% endcall -%}
    {% call statement('main') -%}
        {%- set view_relation = api.Relation.create(identifier='view_'~identifier,
                                                        schema=schema,
                                                        database=database,
                                                        type='table') -%}
        {{ create_view_as(view_relation, "SELECT * FROM " ~ ctas_relation) }}
    {% endcall -%}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

    {% if table_type != 'iceberg' %}
      {{ set_table_classification(target_relation) }}
    {% endif %}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
