import os
import json
import time
import requests
import snowflake.connector
from dotenv import load_dotenv

from dagster import (
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    asset,
    Output,
    ScheduleDefinition,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    DefaultSensorStatus,
    in_process_executor,
)

# ─────────────────────────────────────────────
# LOAD ENV VARS
# ─────────────────────────────────────────────
load_dotenv()

DBT_SERVICE_TOKEN = os.getenv("DBT_CLOUD_SERVICE_TOKEN")
DBT_ACCOUNT_ID    = os.getenv("DBT_CLOUD_ACCOUNT_ID")
DBT_JOB_ID        = os.getenv("DBT_CLOUD_JOB_ID")
DBT_HOST          = os.getenv("DBT_CLOUD_HOST", "https://cloud.getdbt.com")
DBT_BASE_URL      = f"{DBT_HOST}/api/v2"


# ─────────────────────────────────────────────
# dbt CLOUD API
# ─────────────────────────────────────────────
def trigger_dbt_cloud_job(context: AssetExecutionContext, cause: str):
    headers = {
        "Authorization": f"Token {DBT_SERVICE_TOKEN}",
        "Content-Type":  "application/json",
    }
    trigger_url = f"{DBT_BASE_URL}/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    context.log.info(f"Triggering dbt Cloud Job ID: {DBT_JOB_ID}")

    resp = requests.post(trigger_url, headers=headers, json={"cause": cause})
    resp.raise_for_status()

    run_id = resp.json()["data"]["id"]
    context.log.info(f"✓ dbt Cloud run triggered — Run ID: {run_id}")
    context.log.info(f"Track: {DBT_HOST}/deploy/{DBT_ACCOUNT_ID}/runs/{run_id}")

    # Poll until done
    poll_url = f"{DBT_BASE_URL}/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
    while True:
        time.sleep(15)
        poll   = requests.get(poll_url, headers=headers)
        poll.raise_for_status()
        data   = poll.json()["data"]
        status = data["status"]
        label  = data.get("status_humanized", str(status))
        context.log.info(f"  dbt Cloud status: {label}")

        if status == 10:
            context.log.info(f"✓ dbt Cloud Job SUCCESS — Run ID: {run_id}")
            return run_id
        if status in (20, 30):
            raise Exception(
                f"dbt Cloud Job FAILED — {label} | "
                f"{DBT_HOST}/deploy/{DBT_ACCOUNT_ID}/runs/{run_id}"
            )


# ─────────────────────────────────────────────
# SNOWFLAKE LOGGING
# ─────────────────────────────────────────────
def write_run_to_snowflake(
    context: RunStatusSensorContext,
    status: str,
    error_msg: dict = None
):
    try:
        conn = snowflake.connector.connect(
            user      = os.getenv("SNOWFLAKE_USER"),
            password  = os.getenv("SNOWFLAKE_PASSWORD"),
            account   = os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
            database  = "CUSTOMERS_DB",
            schema    = "SANDBOX",
        )
        cursor = conn.cursor()
        stats  = context.instance.get_run_stats(context.dagster_run.run_id)

        cursor.execute("""
            INSERT INTO DAGSTER_JOB_RUNS
              (RUN_ID, JOB_NAME, STATUS, START_TIME, END_TIME, ERROR_MESSAGE)
            VALUES (%s, %s, %s,
                    TO_TIMESTAMP_NTZ(%s),
                    TO_TIMESTAMP_NTZ(%s),
                    PARSE_JSON(%s))
        """, (
            context.dagster_run.run_id,
            context.dagster_run.job_name,
            status,
            stats.start_time,
            stats.end_time,
            json.dumps(error_msg) if error_msg else "{}",
        ))
        conn.commit()
        context.log.info(f"✓ Logged {status} → CUSTOMERS_DB.SANDBOX.DAGSTER_JOB_RUNS")

    except Exception as e:
        context.log.error(f"Snowflake log failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn'   in locals(): conn.close()


# ─────────────────────────────────────────────
# ASSET 1 — SEED + ALL dbt MODELS
# group: raw | tag: seed
# ─────────────────────────────────────────────
@asset(
    name="customer_seed",
    group_name="raw",
    compute_kind="dbt",
    tags={"layer": "seed"},
    description="Triggers dbt Cloud Job → runs all 6 dbt commands",
)
def customer_seed(context: AssetExecutionContext):
    context.log.info("=== ASSET 1: customer_seed ===")
    context.log.info("dbt Cloud will run:")
    context.log.info("  1. dbt seed  --select customer --full-refresh")
    context.log.info("  2. dbt run   --select lz_customers")
    context.log.info("  3. dbt run   --select stg_customers")
    context.log.info("  4. dbt test  --select stg_customers")
    context.log.info("  5. dbt run   --select dbo_customers")
    context.log.info("  6. dbt test  --select dbo_customers")
    run_id = trigger_dbt_cloud_job(
        context,
        cause="Dagster Local → customer_pipeline_job"
    )
    return Output(
        value=run_id,
        metadata={"dbt_cloud_run_id": run_id}
    )


# ─────────────────────────────────────────────
# ASSET 2 — BRONZE CHECKPOINT
# group: raw | tag: bronze
# ─────────────────────────────────────────────
@asset(
    name="lz_customers",
    group_name="raw",
    deps=[customer_seed],
    compute_kind="dbt",
    tags={"layer": "bronze"},
    description="Bronze checkpoint → CUSTOMERS_DB.LZ.LZ_CUSTOMERS",
)
def lz_customers(context: AssetExecutionContext):
    context.log.info("=== ASSET 2: lz_customers ===")
    context.log.info("✓ CUSTOMERS_DB.LZ.LZ_CUSTOMERS confirmed by dbt Cloud")
    return Output(value=True)


# ─────────────────────────────────────────────
# ASSET 3 — SILVER CHECKPOINT
# group: staging | tag: silver
# ─────────────────────────────────────────────
@asset(
    name="stg_customers",
    group_name="staging",
    deps=[lz_customers],
    compute_kind="dbt",
    tags={"layer": "silver"},
    description="Silver checkpoint → CUSTOMERS_DB.STAGING.STG_CUSTOMERS",
)
def stg_customers(context: AssetExecutionContext):
    context.log.info("=== ASSET 3: stg_customers ===")
    context.log.info("✓ CUSTOMERS_DB.STAGING.STG_CUSTOMERS confirmed by dbt Cloud")
    return Output(value=True)


# ─────────────────────────────────────────────
# ASSET 4 — GOLD CHECKPOINT
# group: mart | tag: gold
# ─────────────────────────────────────────────
@asset(
    name="dbo_customers",
    group_name="mart",
    deps=[stg_customers],
    compute_kind="dbt",
    tags={"layer": "gold"},
    description="Gold checkpoint → CUSTOMERS_DB.DBO.DBO_CUSTOMERS",
)
def dbo_customers(context: AssetExecutionContext):
    context.log.info("=== ASSET 4: dbo_customers ===")
    context.log.info("✓ CUSTOMERS_DB.DBO.DBO_CUSTOMERS confirmed by dbt Cloud")
    return Output(value=True)


# ─────────────────────────────────────────────
# JOB
# ─────────────────────────────────────────────
customer_pipeline_job = define_asset_job(
    name="customer_pipeline_job",
    selection=[
        "customer_seed",
        "lz_customers",
        "stg_customers",
        "dbo_customers",
    ],
    description="Dagster Local → dbt Cloud → Snowflake",
    executor_def=in_process_executor,
    tags={
        "pipeline": "customer",
        "team": "data-engineering"
    },
)

# ─────────────────────────────────────────────
# SCHEDULE
# ─────────────────────────────────────────────
daily_schedule = ScheduleDefinition(
    name="customer_pipeline_daily_6am",
    job=customer_pipeline_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)

# ─────────────────────────────────────────────
# SENSORS
# ─────────────────────────────────────────────
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    error_data = None
    if context.failure_event and context.failure_event.step_failure_data:
        error_data = {
            "error_message": context.failure_event.step_failure_data.error.message
        }
    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)


# ─────────────────────────────────────────────
# DEFINITIONS
# ─────────────────────────────────────────────
defs = Definitions(
    assets=[
        customer_seed,
        lz_customers,
        stg_customers,
        dbo_customers,
    ],
    jobs=[customer_pipeline_job],
    schedules=[daily_schedule],
    sensors=[
        log_success_to_snowflake,
        log_failure_to_snowflake,
    ],
)