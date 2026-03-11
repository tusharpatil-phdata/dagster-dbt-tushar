import os
import json
import random
import uuid
import snowflake.connector
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    AssetSelection,
    in_process_executor,
    DefaultSensorStatus,
    asset,
    AssetKey,
    Output,
    RunRequest,
    RetryPolicy,
)
from dagster_dbt import (
    dbt_cloud_resource,
    load_assets_from_dbt_cloud_job,
)

# ══════════════════════════════════════════════════════════════
# 1. LOAD SECRETS
# ══════════════════════════════════════════════════════════════
load_dotenv()

# ══════════════════════════════════════════════════════════════
# 2. INGEST FROM ADLS + THRESHOLD CHECK
#    Runs BEFORE dbt — if thresholds breached, dbt won't run
# ══════════════════════════════════════════════════════════════
@asset(
    key=AssetKey("ingest_daily_data"),
    group_name="ingestion",
    compute_kind="snowflake",
    description="Ingest data from ADLS into SOURCE + check thresholds before dbt runs",
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def ingest_daily_data(context):
    """
    Step 1: Load data from ADLS Gen2 into SOURCE tables (TRUNCATE + COPY)
    Step 2: Read Snowflake streams + thresholds from config table
    Step 3: If breached -> FAIL (dbt won't run)
             If OK -> PASS (dbt runs next via sensor)
    """
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="DAGSTER_DBT_KIEWIT_DB",
            schema="SOURCE",
        )
        cursor = conn.cursor()

        # ══════════════════════════════════════════════
        # STEP 1: INGEST FROM ADLS (TRUNCATE + COPY)
        # ADLS stage + file format are already configured:
        #   - STAGE  : SOURCE.ADLS_RAW_STAGE
        #   - FORMAT : SOURCE.CSV_FORMAT
        # This step ensures SOURCE tables match latest CSVs in ADLS.
        # ══════════════════════════════════════════════
        context.log.info("=" * 60)
        context.log.info("  STEP 1: LOAD FROM ADLS INTO SOURCE")
        context.log.info("=" * 60)

        # 1a) Truncate all SOURCE tables
        truncate_cmds = [
            "TRUNCATE TABLE CUSTOMER",
            "TRUNCATE TABLE ORDER_DETAIL",
            "TRUNCATE TABLE ORDER_ITEM",
            "TRUNCATE TABLE PRODUCT",
            "TRUNCATE TABLE STORE",
            "TRUNCATE TABLE SUPPLY",
        ]
        for sql in truncate_cmds:
            context.log.info(f"  Running: {sql}")
            cursor.execute(sql)

        # 1b) COPY from ADLS stage into each SOURCE table
        copy_cmds = [
            """
            COPY INTO CUSTOMER
            FROM @SOURCE.ADLS_RAW_STAGE/customer.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
            """
            COPY INTO ORDER_DETAIL
            FROM @SOURCE.ADLS_RAW_STAGE/order_detail.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
            """
            COPY INTO ORDER_ITEM
            FROM @SOURCE.ADLS_RAW_STAGE/order_item.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
            """
            COPY INTO PRODUCT
            FROM @SOURCE.ADLS_RAW_STAGE/product.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
            """
            COPY INTO STORE
            FROM @SOURCE.ADLS_RAW_STAGE/store.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
            """
            COPY INTO SUPPLY
            FROM @SOURCE.ADLS_RAW_STAGE/supply.csv
            FILE_FORMAT = (FORMAT_NAME = SOURCE.CSV_FORMAT)
            """,
        ]
        for sql in copy_cmds:
            clean_sql = sql.strip()
            context.log.info(f"  Running COPY:\n{clean_sql}")
            cursor.execute(clean_sql)

        conn.commit()
        context.log.info("  SOURCE tables successfully loaded from ADLS")

        # ══════════════════════════════════════════════
        # STEP 2: READ STREAMS + CHECK THRESHOLDS
        # Thresholds stored in METRICS.THRESHOLD_CONFIG (Snowflake table)
        # Streams combined in SOURCE.ALL_STREAMS_SUMMARY (one query)
        # ══════════════════════════════════════════════
        context.log.info("")
        context.log.info("=" * 60)
        context.log.info("  STEP 2: THRESHOLD CHECK (before dbt runs)")
        context.log.info("=" * 60)

        # Read per-table thresholds from config table
        cursor.execute(
            "SELECT TABLE_NAME, MAX_INSERT_PCT, MAX_UPDATE_PCT, MAX_DELETE_PCT "
            "FROM METRICS.THRESHOLD_CONFIG"
        )
        threshold_rows = cursor.fetchall()
        thresholds = {}
        for t_name, m_ins, m_upd, m_del in threshold_rows:
            thresholds[t_name] = {"insert": m_ins, "update": m_upd, "delete": m_del}

        context.log.info("  Thresholds loaded from METRICS.THRESHOLD_CONFIG")
        context.log.info("-" * 60)

        alerts = []

        # One query to read all stream changes
        cursor.execute(
            "SELECT TABLE_NAME, INSERTS, UPDATES, DELETES FROM SOURCE.ALL_STREAMS_SUMMARY"
        )
        stream_rows = cursor.fetchall()

        for table_name, inserts, updates, deletes in stream_rows:
            inserts = int(inserts or 0)
            updates = int(updates or 0)
            deletes = int(deletes or 0)
            total = inserts + updates + deletes

            if total == 0:
                context.log.info(f"  SOURCE.{table_name}: No changes")
                continue

            # Get current row count for percentage calculation
            cursor.execute(f"SELECT COUNT(*) FROM SOURCE.{table_name}")
            current_rows = cursor.fetchone()[0] or 1

            # Get thresholds for this table (default 50/30/10 if not in config)
            t = thresholds.get(table_name, {"insert": 50, "update": 30, "delete": 10})

            ins_pct = round(inserts / current_rows * 100, 1)
            upd_pct = round(updates / current_rows * 100, 1)
            del_pct = round(deletes / current_rows * 100, 1)

            context.log.info(
                f"  SOURCE.{table_name}: {inserts:,} inserts ({ins_pct}%) | "
                f"{updates:,} updates ({upd_pct}%) | "
                f"{deletes:,} deletes ({del_pct}%) "
                f"[limits: {t['insert']}%/{t['update']}%/{t['delete']}%]"
            )

            # Check insert threshold
            if ins_pct > t["insert"]:
                msg = (
                    f"INSERT BREACH: SOURCE.{table_name} | {inserts:,} rows "
                    f"({ins_pct}%) exceeds {t['insert']}% limit"
                )
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

            # Check update threshold
            if upd_pct > t["update"]:
                msg = (
                    f"UPDATE BREACH: SOURCE.{table_name} | {updates:,} rows "
                    f"({upd_pct}%) exceeds {t['update']}% limit"
                )
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

            # Check delete threshold
            if del_pct > t["delete"]:
                msg = (
                    f"DELETE BREACH: SOURCE.{table_name} | {deletes:,} rows "
                    f"({del_pct}%) exceeds {t['delete']}% limit"
                )
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

        # ══════════════════════════════════════════════
        # STEP 3: PASS OR FAIL — gates the dbt run
        # ══════════════════════════════════════════════
        context.log.info("-" * 60)
        if alerts:
            context.log.error(f"  {len(alerts)} THRESHOLD BREACH(ES) — dbt will NOT run:")
            for a in alerts:
                context.log.error(f"    - {a}")
            context.log.info("=" * 60)
            raise Exception(
                f"Threshold breached! {len(alerts)} alert(s). "
                f"Fix SOURCE data before running dbt. Details: {'; '.join(alerts)}"
            )
        else:
            context.log.info("  ALL THRESHOLDS PASSED — dbt will run next")
            context.log.info("=" * 60)

        return Output(None)

    except snowflake.connector.errors.ProgrammingError as e:
        context.log.error(f"Snowflake error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# ══════════════════════════════════════════════════════════════
# 3. CONFIGURE dbt CLOUD CONNECTION
# ══════════════════════════════════════════════════════════════
dbt_cloud_connection = dbt_cloud_resource.configured(
    {
        "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
        "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID")),
        "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST"),
    }
)

# ══════════════════════════════════════════════════════════════
# 4. AUTO-DISCOVER dbt MODELS FROM dbt CLOUD JOB
# ══════════════════════════════════════════════════════════════
customer_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID")),
)

# ══════════════════════════════════════════════════════════════
# 5a. SNOWFLAKE AUDIT LOGGING (IST timestamps)
# ══════════════════════════════════════════════════════════════
def write_run_to_snowflake(
    context: RunStatusSensorContext,
    status: str,
    error_msg: dict = None,
):
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="DAGSTER_DBT_KIEWIT_DB",
            schema="METRICS",
        )
        cursor = conn.cursor()
        run_id = context.dagster_run.run_id
        job_name = context.dagster_run.job_name
        stats = context.instance.get_run_stats(run_id)
        error_json = json.dumps(error_msg) if error_msg else None

        cursor.execute(
            """
            INSERT INTO DAGSTER_JOB_RUNS
              (RUN_ID, JOB_NAME, STATUS,
               START_TIME, END_TIME, ERROR_MESSAGE, LOGGED_AT)
            VALUES (%s, %s, %s,
                    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TO_TIMESTAMP_NTZ(%s)),
                    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TO_TIMESTAMP_NTZ(%s)),
                    %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
            """,
            (run_id, job_name, status,
             stats.start_time, stats.end_time, error_json),
        )
        conn.commit()
        context.log.info("  Logged run to METRICS.DAGSTER_JOB_RUNS")
    except Exception as e:
        context.log.error(f"  Snowflake log failed: {e}")
    finally:
        if conn:
            conn.close()

# ══════════════════════════════════════════════════════════════
# 5b. FETCH dbt CLOUD RUN DETAILS + LOG TO SNOWFLAKE
# ══════════════════════════════════════════════════════════════
def fetch_dbt_run_results(context: RunStatusSensorContext):
    """Fetch per-model results from dbt Cloud and log to Snowflake."""
    host = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token = os.getenv("DBT_CLOUD_API_TOKEN")
    job_id = os.getenv("DBT_JOB_ID")

    if not (host and account_id and token and job_id):
        context.log.warning("  dbt Cloud env vars missing, skipping fetch_dbt_run_results")
        return

    headers = {"Authorization": f"Token {token}"}

    # Get latest run for this job
    runs_url = (
        f"{host}/api/v2/accounts/{account_id}/runs/"
        f"?job_definition_id={job_id}&order_by=-id&limit=1"
    )
    run_resp = requests.get(runs_url, headers=headers, timeout=20)
    run_resp.raise_for_status()
    latest_run = run_resp.json()["data"][0]
    dbt_run_id = latest_run["id"]

    # Fetch run results artifact
    artifact_url = (
        f"{host}/api/v2/accounts/{account_id}/runs/{dbt_run_id}/artifacts/run_results.json"
    )
    art_resp = requests.get(artifact_url, headers=headers, timeout=20)
    art_resp.raise_for_status()
    results = art_resp.json()["results"]

    # Log each model to Dagster UI
    for r in results:
        node = r["unique_id"]
        status = r["status"]
        exec_time = r["execution_time"]
        rows = r.get("adapter_response", {}).get("rows_affected", "N/A")
        context.log.info(f"  dbt: {node} | {status} | {rows} rows | {exec_time:.1f}s")

    passed = sum(1 for r in results if r["status"] in ("success", "pass"))
    failed = sum(1 for r in results if r["status"] == "error")
    context.log.info(
        f"  dbt Summary: {passed} passed, {failed} failed out of {len(results)} total"
    )

    # Write per-model results to Snowflake
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="DAGSTER_DBT_KIEWIT_DB",
            schema="METRICS",
        )
        cursor = conn.cursor()
        dagster_run_id = context.dagster_run.run_id

        for r in results:
            rows_val = r.get("adapter_response", {}).get("rows_affected")
            cursor.execute(
                """
                INSERT INTO DBT_MODEL_RUNS
                  (DAGSTER_RUN_ID, DBT_CLOUD_RUN_ID, MODEL_NAME,
                   STATUS, ROWS_AFFECTED, EXECUTION_TIME, LOGGED_AT)
                VALUES (%s, %s, %s, %s, %s, %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
                """,
                (dagster_run_id, dbt_run_id, r["unique_id"],
                 r["status"], rows_val, round(r["execution_time"], 2)),
            )

        conn.commit()
        context.log.info(f"  Logged {len(results)} model results to METRICS.DBT_MODEL_RUNS")
    except Exception as e:
        context.log.error(f"  Failed to log model results: {e}")
    finally:
        if conn:
            conn.close()

# ══════════════════════════════════════════════════════════════
# 5c. TRIGGER dbt RETRY JOB ON FAILURE
# ══════════════════════════════════════════════════════════════
def trigger_dbt_retry(context: RunStatusSensorContext):
    """Trigger the dbt retry job to rerun only failed models."""
    host = os.getenv("DBT_CLOUD_HOST")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    token = os.getenv("DBT_CLOUD_API_TOKEN")
    retry_job_id = os.getenv("DBT_RETRY_JOB_ID")

    if not retry_job_id:
        context.log.warning("  DBT_RETRY_JOB_ID not set, skipping retry")
        return

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    url = f"{host}/api/v2/accounts/{account_id}/jobs/{retry_job_id}/run/"
    body = {"cause": "Auto-retry triggered by Dagster on failure"}

    response = requests.post(url, headers=headers, json=body, timeout=20)
    response.raise_for_status()
    run_id = response.json()["data"]["id"]
    context.log.info(f"  Retry job triggered! dbt Cloud Run ID: {run_id}")
    context.log.info("  Only failed models from the last run will be re-executed.")

# ══════════════════════════════════════════════════════════════
# 5d. LOG RECORD COUNTS TO SNOWFLAKE (ALL 24 TABLES)
# ══════════════════════════════════════════════════════════════
def log_record_counts(context: RunStatusSensorContext):
    """Query row counts per layer, log with before/after, apply thresholds."""
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="DAGSTER_DBT_KIEWIT_DB",
        )
        cursor = conn.cursor()
        dagster_run_id = context.dagster_run.run_id

        # Percentage thresholds for row count changes
        MAX_INSERT_PCT = 50
        MAX_DELETE_PCT = 10
        MIN_ROWS = 1

        # All 24 tables across 4 layers
        tables = [
            ("SOURCE", "CUSTOMER"),
            ("LZ", "RAW_CUSTOMER"),
            ("STAGING", "STG_CUSTOMER"),
            ("DBO", "DIM_CUSTOMER"),

            ("SOURCE", "ORDER_DETAIL"),
            ("LZ", "RAW_ORDER_DETAIL"),
            ("STAGING", "STG_ORDER_DETAIL"),
            ("DBO", "FCT_ORDER_DETAIL"),

            ("SOURCE", "ORDER_ITEM"),
            ("LZ", "RAW_ORDER_ITEM"),
            ("STAGING", "STG_ORDER_ITEM"),
            ("DBO", "FCT_ORDER_ITEM"),

            ("SOURCE", "PRODUCT"),
            ("LZ", "RAW_PRODUCT"),
            ("STAGING", "STG_PRODUCT"),
            ("DBO", "DIM_PRODUCT"),

            ("SOURCE", "STORE"),
            ("LZ", "RAW_STORE"),
            ("STAGING", "STG_STORE"),
            ("DBO", "DIM_STORE"),

            ("SOURCE", "SUPPLY"),
            ("LZ", "RAW_SUPPLY"),
            ("STAGING", "STG_SUPPLY"),
            ("DBO", "DIM_SUPPLY"),
        ]

        alerts = []

        context.log.info("=" * 60)
        context.log.info("  RECORD COUNTS (all layers) + SOURCE THRESHOLDS")
        context.log.info("=" * 60)

        # Read per-table thresholds from config for SOURCE tables
        cursor.execute(
            "SELECT TABLE_NAME, MAX_INSERT_PCT, MAX_UPDATE_PCT, MAX_DELETE_PCT "
            "FROM METRICS.THRESHOLD_CONFIG"
        )
        threshold_rows = cursor.fetchall()
        thresholds = {}
        for t_name, m_ins, m_upd, m_del in threshold_rows:
            thresholds[t_name] = {"insert": m_ins, "update": m_upd, "delete": m_del}

        context.log.info("  SOURCE thresholds (from METRICS.THRESHOLD_CONFIG):")
        for t_name, limits in thresholds.items():
            context.log.info(
                f"    {t_name}: Insert>{limits['insert']}% | "
                f"Update>{limits['update']}% | Delete>{limits['delete']}%"
            )
        context.log.info("-" * 60)

        for schema, table in tables:
            # Current row count
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            rows_after = cursor.fetchone()[0]

            # Previous run row count
            cursor.execute(
                """
                SELECT ROWS_AFTER FROM METRICS.LAYER_ROW_COUNTS
                WHERE SCHEMA_NAME = %s AND TABLE_NAME = %s
                ORDER BY LOGGED_AT DESC LIMIT 1
                """,
                (schema, table),
            )
            prev = cursor.fetchone()
            rows_before = prev[0] if prev else 0
            rows_added = rows_after - rows_before

            # Determine change type
            if rows_before == 0 and rows_after > 0:
                inserted = rows_after
                deleted = 0
                change_type = "INITIAL LOAD"
            elif rows_added > 0:
                inserted = rows_added
                deleted = 0
                change_type = f"+{inserted:,} inserted"
            elif rows_added == 0:
                inserted = 0
                deleted = 0
                change_type = f"no change ({rows_after:,} rows)"
            else:
                inserted = 0
                deleted = abs(rows_added)
                change_type = f"-{deleted:,} DELETED"

            # Save to Snowflake
            cursor.execute(
                """
                INSERT INTO METRICS.LAYER_ROW_COUNTS
                  (DAGSTER_RUN_ID, SCHEMA_NAME, TABLE_NAME,
                   ROWS_BEFORE, ROWS_AFTER, ROWS_ADDED, LOGGED_AT)
                VALUES (%s, %s, %s, %s, %s, %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
                """,
                (dagster_run_id, schema, table,
                 rows_before, rows_after, rows_added),
            )

            context.log.info(
                f"  {schema}.{table}: {rows_before:,} -> {rows_after:,} ({change_type})"
            )

            # Threshold checks ONLY for SOURCE layer (using config table values)
            if rows_before > 0 and schema == "SOURCE":
                t = thresholds.get(table, {"insert": MAX_INSERT_PCT, "delete": MAX_DELETE_PCT})
                t_insert = t.get("insert", MAX_INSERT_PCT)
                t_delete = t.get("delete", MAX_DELETE_PCT)

                max_insert = int(rows_before * t_insert / 100) or 1
                if inserted > max_insert:
                    pct = round(inserted / rows_before * 100)
                    msg = f"INSERT: SOURCE.{table} | +{inserted:,} rows ({pct}%) exceeds {t_insert}% limit"
                    context.log.warning(f"    >> {msg}")
                    alerts.append(msg)

                max_delete = int(rows_before * t_delete / 100) or 1
                if deleted > max_delete:
                    pct = round(deleted / rows_before * 100)
                    msg = f"DELETE: SOURCE.{table} | -{deleted:,} rows ({pct}%) exceeds {t_delete}% limit"
                    context.log.error(f"    >> {msg}")
                    alerts.append(msg)

            # Empty table check
            if rows_after < MIN_ROWS and rows_before > 0:
                msg = f"EMPTY: {schema}.{table} has {rows_after} rows after pipeline run!"
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

        conn.commit()

        # Summary
        context.log.info("-" * 60)
        if alerts:
            context.log.warning(f"  {len(alerts)} THRESHOLD ALERT(S):")
            for a in alerts:
                context.log.warning(f"    - {a}")
        else:
            context.log.info("  ALL THRESHOLDS OK")
        context.log.info("  Saved to METRICS.LAYER_ROW_COUNTS")
        context.log.info("=" * 60)

    except Exception as e:
        context.log.error(f"Record count failed: {e}")
    finally:
        if conn:
            conn.close()

# ══════════════════════════════════════════════════════════════
# 5e. LOG SOURCE TABLE COUNTS AFTER INGESTION
# ══════════════════════════════════════════════════════════════
def log_source_counts(context: RunStatusSensorContext):
    """Log row counts for SOURCE tables only — runs after ingestion."""
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="DAGSTER_DBT_KIEWIT_DB",
        )
        cursor = conn.cursor()
        dagster_run_id = context.dagster_run.run_id

        source_tables = [
            ("SOURCE", "CUSTOMER"),
            ("SOURCE", "ORDER_DETAIL"),
            ("SOURCE", "ORDER_ITEM"),
            ("SOURCE", "PRODUCT"),
            ("SOURCE", "STORE"),
            ("SOURCE", "SUPPLY"),
        ]

        context.log.info("=" * 60)
        context.log.info("  SOURCE LAYER ROW COUNTS (after ingestion)")
        context.log.info("=" * 60)

        for schema, table in source_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            rows_after = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT ROWS_AFTER FROM METRICS.LAYER_ROW_COUNTS
                WHERE SCHEMA_NAME = %s AND TABLE_NAME = %s
                ORDER BY LOGGED_AT DESC LIMIT 1
                """,
                (schema, table),
            )
            prev = cursor.fetchone()
            rows_before = prev[0] if prev else 0
            rows_added = rows_after - rows_before

            if rows_before == 0 and rows_after > 0:
                change_type = "INITIAL LOAD"
            elif rows_added > 0:
                change_type = f"+{rows_added:,} new rows"
            elif rows_added == 0:
                change_type = f"no change ({rows_after:,} rows)"
            else:
                change_type = f"-{abs(rows_added):,} rows removed"

            cursor.execute(
                """
                INSERT INTO METRICS.LAYER_ROW_COUNTS
                  (DAGSTER_RUN_ID, SCHEMA_NAME, TABLE_NAME,
                   ROWS_BEFORE, ROWS_AFTER, ROWS_ADDED, LOGGED_AT)
                VALUES (%s, %s, %s, %s, %s, %s,
                    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', CURRENT_TIMESTAMP()))
                """,
                (dagster_run_id, schema, table,
                 rows_before, rows_after, rows_added),
            )

            context.log.info(f"  {schema}.{table}: {rows_before:,} -> {rows_after:,} ({change_type})")

        conn.commit()
        context.log.info("-" * 60)
        context.log.info("  SOURCE counts saved to METRICS.LAYER_ROW_COUNTS")
        context.log.info("=" * 60)

    except Exception as e:
        context.log.error(f"Source count failed: {e}")
    finally:
        if conn:
            conn.close()

# ══════════════════════════════════════════════════════════════
# 6. SENSORS
# ══════════════════════════════════════════════════════════════
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    """
    Fires after every successful Dagster run.
    - After ingestion: logs job status + SOURCE row counts only
    - After dbt: logs job status + all 24 table row counts
    """
    write_run_to_snowflake(context, status="SUCCESS")

    if context.dagster_run.job_name == "run_ingestion_and_threshold_check":
        # After ingestion — log only SOURCE table counts
        try:
            log_source_counts(context)
        except Exception as e:
            context.log.warning(f"  Could not log source counts: {e}")
    elif context.dagster_run.job_name == "trigger_dbt_cloud_job":
        # After dbt — log all 24 table counts
        try:
            log_record_counts(context)
        except Exception as e:
            context.log.warning(f"  Could not log record counts: {e}")

@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    """
    Fires after every failed Dagster run.
    - After ingestion failure: logs error only (no retry needed)
    - After dbt failure: logs error + triggers retry job
    """
    error_data = None
    if context.failure_event and context.failure_event.step_failure_data:
        error_data = {
            "error_message": context.failure_event.step_failure_data.error.message
        }
    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)

    # Only trigger retry if dbt job failed, not if ingestion failed
    if context.dagster_run.job_name == "trigger_dbt_cloud_job":
        try:
            trigger_dbt_retry(context)
        except Exception as e:
            context.log.warning(f"  Could not trigger retry job: {e}")
    else:
        context.log.info("  Ingestion failed — dbt will not run, no retry needed")

# ══════════════════════════════════════════════════════════════
# 7. JOBS
# ══════════════════════════════════════════════════════════════

# Job that runs ONLY ingestion + threshold check
ingestion_job = define_asset_job(
    name="run_ingestion_and_threshold_check",
    selection=AssetSelection.keys(AssetKey("ingest_daily_data")),
    executor_def=in_process_executor,
)

# Job that runs ONLY dbt Cloud models
dbt_job = define_asset_job(
    name="trigger_dbt_cloud_job",
    selection=AssetSelection.all() - AssetSelection.keys(AssetKey("ingest_daily_data")),
    executor_def=in_process_executor,
)

# ══════════════════════════════════════════════════════════════
# 8. SENSOR: Run dbt ONLY after ingestion succeeds
# ══════════════════════════════════════════════════════════════
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
    monitored_jobs=[ingestion_job],
    request_job=dbt_job,
    name="trigger_dbt_after_ingestion",
)
def trigger_dbt_after_ingestion(context: RunStatusSensorContext):
    """
    Only triggers dbt Cloud job AFTER ingestion + threshold check passes.
    If ingestion fails (threshold breached), this sensor never fires,
    and dbt never runs — bad data never reaches Gold layer.
    """
    context.log.info("  Ingestion succeeded + thresholds passed -> triggering dbt Cloud job")
    return RunRequest()

# ══════════════════════════════════════════════════════════════
# 9. SCHEDULE
# ══════════════════════════════════════════════════════════════
daily_schedule = ScheduleDefinition(
    job=ingestion_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)

# ══════════════════════════════════════════════════════════════
# 10. REGISTER EVERYTHING
# ══════════════════════════════════════════════════════════════
defs = Definitions(
    assets=[ingest_daily_data, customer_dbt_assets],
    jobs=[ingestion_job, dbt_job],
    schedules=[daily_schedule],
    sensors=[
        log_success_to_snowflake,
        log_failure_to_snowflake,
        trigger_dbt_after_ingestion,
    ],
)