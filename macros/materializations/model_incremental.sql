{% macro csv_to_pairs(source_alias, target_alias, csv, separator = ',') %}
  {# covert comma sep string to paris (for updates) like "A.NAME = B.NAME,"... #}
    {% set cp = [] %}
    {% for c in csv.split(', ') %}
        {% do cp.append('{}.{}={}.{}'.format(source_alias, c, target_alias, c)) %}

    {% endfor%}
    {%- set col_pairs = cp | join(separator) -%}
    {{ return(col_pairs) }}
{% endmacro %}

{% macro get_hash_na() %}
    md5_binary('x0')
{% endmacro %}
{% macro get_hash_missing() %}
    md5_binary('x1')
{% endmacro %}

{% macro get_hash_key(csv) %}
    {% set new_csv = [] %}
   
    {% for c in csv.split(',') -%}
        {%- do new_csv.append("coalesce(upper(cast({} as text)),'x0')".format(c)) -%}
    {% endfor%}

    {%- set md5 = 'md5_binary(' + new_csv | join("||'-'||") + ')'-%}

    {{ return(md5) }}
{% endmacro %}
{% macro get_hash_checksum(csv )%}
  {#build hash for checksum, build csv first using model_get_quoted_csv #}
    {% set new_csv = [] %}
   
    {% for c in csv.split(',') -%}
        {% if "DW_CHECKSUM" not in c %}
          {%- do new_csv.append("coalesce(cast({} as text),'x0')".format(c)) -%}
        {%endif%}
    {% endfor%}

    {%- set md5 = 'md5_binary(' + new_csv | join("||'-'||") + ')'-%}

    {{ return(md5) }}

{% endmacro %}

{% macro model_get_quoted_csv(column_names, ignore_dw_identifers=True,ignore_list=[]) %}
  {#covert columns to comma sep string #}
    {# BY DEFAULT THE DW COLUMNS ARE IGNORED #}
    {% set quoted = [] %}

    {% if ignore_dw_identifers %}
      {% set ignore_list = ['"DW_ACTIVE_IND"', '"DW_DELETED_IND"', '"DW_EFFECTIVE_FROM_TS"', 
                          '"DW_EFFECTIVE_TO_TS"', '"DW_CREATE_TS"', '"DW_UPDATE_TS"', '"DW_CURRENT_IND"', '"CHECKSUM"', '"DW_SOURCE_UPDATE_TS"'] %}
    {% else %}
       {% set ignore_list = ignore_list %}
    {% endif %}
    
    {% for col in column_names -%}
        {% if adapter.quote(col) not in ignore_list %}
            {%- do quoted.append(adapter.quote(col)) -%}
        {% endif %}

    {%- endfor %}
    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}

{% macro model_add_alias_to_csv(alias, csv) %}
  {#add alias to column string, like "B."  #}
    {% set new_csv = [] %}
   
    {% for c in csv.split(', ') -%}
        {%- do new_csv.append('{}.{}'.format(alias, c)) -%}
        
    {% endfor%}

    {%- set dest_cols_csv = new_csv | join(', ') -%}

    {{ return(dest_cols_csv) }}

{% endmacro %}


{% macro model_simple_merge(target, source, unique_key, dest_columns, predicates, base_name) %}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    
      {%- set dest_cols_csv = model_get_quoted_csv(dest_columns | map(attribute="name"),ignore_dw_identifers=False, ignore_list=['"DW_CREATE_TS"', '"DW_UPDATE_TS"']) -%}
     {% set alias_dest_cols_csv = model_add_alias_to_csv(alias='B', csv=dest_cols_csv) %}
    {% set pairs = csv_to_pairs('T', 'S', dest_cols_csv) %}
    
    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as T
      using 
      (        
        select {{ dest_cols_csv }}
        from {{source}}
      ) S 
        on  {{csv_to_pairs("S", "T" , unique_key, ' and ') }}
        when matched
          then update set
              {{ pairs }}
              , T.DW_UPDATE_TS = CURRENT_TIMESTAMP()
        when not matched 
          then insert ({{ dest_cols_csv }} 
              , T.DW_CREATE_TS
              , T.DW_UPDATE_TS
              )
          values  ({{ dest_cols_csv }} 
              , CURRENT_TIMESTAMP() 
              , CURRENT_TIMESTAMP() 
              )
    ;
    

{% endmacro %}

{% macro model_dbt_snowflake_validate_get_incremental_strategy(strategy) %}
  {#-- Find and validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'type_one', 'type_two, type_one_full_load, simple_merge, snapshot_fact, transaction_fact'
  {%- endset %}
  {% if strategy not in ['type_one', 'type_two', 'type_one_full_load', 'simple_merge', 'snapshot_fact', 'transaction_fact'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro model_dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, base_name, date_key) %}
  {% if strategy == 'type_one' %}
    {% do return( model_type_one_merge(target_relation, tmp_relation, unique_key, dest_columns, predicates=none, base_name = base_name)) %}
  {% elif strategy == 'type_two' %}
    {% do return(model_type_two_merge(target_relation, tmp_relation, unique_key, dest_columns, predicates=none, base_name = base_name)) %}
  {% elif strategy == 'simple_merge' %}
   {% do return(model_simple_merge(target_relation, tmp_relation, unique_key, dest_columns, predicates=none, base_name = base_name)) %}
  {% elif strategy == 'transaction_fact' %}
    {% do return(model_transaction_fact(target_relation, tmp_relation, unique_key, dest_columns, predicates=none, base_name = base_name, date_key=date_key)) %}
  {% elif strategy == 'type_one_full_load' %}
    {% do return(type_one_full_load_merge(target_relation, tmp_relation, unique_key, dest_columns, predicates=none, op_name=op_name)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% materialization model_incremental, adapter='snowflake' -%}
  {% set schema  = config.get('schema') %}
  {% set table_name   = config.get('table_name') %} 
  {% set unique_key   = config.get('unique_key') %}

  {% set incremental_strategy = config.get('incremental_strategy') %}


  {%- set target_relation = api.Relation.create(
        identifier=adapter.quote(this.identifier.upper()), 
        schema=schema, database=this.database,
        type=this.type) -%}
  
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% set strategy = model_dbt_snowflake_validate_get_incremental_strategy(incremental_strategy) -%}

  {% set full_refresh_mode = (should_full_refresh()) %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}


  {% set sql2 %}
     with "SOURCE" as 
     (
          {{sql}}   
     )
      select *
      from "SOURCE"
  {% endset %}



  {% do run_query(create_table_as(True, tmp_relation, sql2)) %}
  {% do adapter.expand_target_column_types(
        from_relation=tmp_relation,
        to_relation=target_relation) %}
  {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}


  {% set build_sql = model_dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, base_name, date_key) %}


  {%- call statement('main') -%}
  {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  

  {% set target_relation = target_relation.incorporate(type='table') %}
  {#% do persist_docs(target_relation, model) %#}

{#   {% do unset_query_tag(original_query_tag) %} #}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
