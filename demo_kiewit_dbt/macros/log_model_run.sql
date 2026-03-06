{% macro log_model_run() %}

    insert into SANDBOX.METRICS.DBT_MODEL_RUNS (
        INVOCATION_ID,
        MODEL_NAME,
        STATUS,
        ROWS_AFTER,
        RUN_START_AT,
        LOGGED_AT
    )
    select
        '{{ invocation_id }}'::string,
        '{{ this }}'::string,
        'SUCCESS'::string,
        (select count(*) from {{ this }}),
        {{ run_started_at }},
        CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP())
    ;

{% endmacro %}