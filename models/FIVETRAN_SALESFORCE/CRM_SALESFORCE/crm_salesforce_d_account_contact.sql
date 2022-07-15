
{% set schema = 'CRM_SALESFORCE' %}
{% set table_name = 'D_ACCOUNT_CONTACT' %}
{% set unique_key ='ACCOUNT_CONTACT_ID' %} {# source natural key, comma seperated #}

{% set incremental_strategy ='simple_merge' %}
{# 'type_one', 'type_two', 'simple_merge',  'transaction_fact' #}


{{ config(
    materialized='model_incremental',
    incremental_strategy=incremental_strategy,
    unique_key= unique_key,
    schema=schema,
    table_name=table_name,
    alias=table_name
    ) 
}}


SELECT 
    C.ID as ACCOUNT_CONTACT_ID,
    C.NAME as CONTACT_NAME,
    A.NAME as ACCOUNT_NAME,
    A.TYPE as ACOUNT_TYPE,
    A.ACCOUNT_NUMBER,
    C.MOBILE_PHONE as CONTACT_MOBILE,
    C.EMAIL as CONTACT_EMAIL
FROM 
    CRM_SALESFORCE.ACCOUNT A
    JOIN CRM_SALESFORCE.CONTACT C
    ON A.ID = C.ACCOUNT_ID