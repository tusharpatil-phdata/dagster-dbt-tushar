import os
import json
import random
import snowflake.connector
import requests
import uuid
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
)
from dagster_dbt import (
    dbt_cloud_resource,
    load_assets_from_dbt_cloud_job,
)


# 1. LOAD SECRETS
load_dotenv()


# ══════════════════════════════════════════════════════════════
# 2. SIMULATE DAILY INGESTION + THRESHOLD CHECK
#    Runs BEFORE dbt — if thresholds breached, dbt won't run
# ══════════════════════════════════════════════════════════════
@asset(
    key=AssetKey("ingest_daily_data"),
    group_name="ingestion",
    compute_kind="snowflake",
    description="Ingest daily data into SOURCE + check thresholds before dbt runs",
)
def ingest_daily_data(context):
    """
    Step 1: Insert/Update/Delete rows in SOURCE (simulating daily ingestion)
    Step 2: Read Snowflake streams + thresholds from config table
    Step 3: If breached → FAIL (dbt won't run)
             If OK → PASS (dbt runs next)
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
        # STEP 1: INGEST DATA (simulate daily load)
        # All IDs are UUIDs except PRODUCT (SKU) and SUPPLY (SUP-xxx)
        # ══════════════════════════════════════════════
        context.log.info("=" * 60)
        context.log.info("  STEP 1: DAILY DATA INGESTION")
        context.log.info("=" * 60)

        import uuid

        first_names = ["John", "Jane", "Mike", "Sara", "Alex", "Emma", "Tom", "Lisa", "Ryan", "Kate"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson"]

        # Get existing IDs for references
        cursor.execute("SELECT ID FROM SOURCE.CUSTOMER ORDER BY RANDOM() LIMIT 10")
        existing_customers = [r[0] for r in cursor.fetchall()]

        cursor.execute("SELECT ID FROM SOURCE.STORE ORDER BY RANDOM() LIMIT 1")
        random_store = cursor.fetchone()[0]

        cursor.execute("SELECT SKU FROM SOURCE.PRODUCT")
        all_skus = [r[0] for r in cursor.fetchall()]

        # ── CUSTOMER: insert + update + delete ──
        new_custs = random.randint(3, 5)
        for i in range(new_custs):
            cid = str(uuid.uuid4())
            name = f"{random.choice(first_names)} {random.choice(last_names)}"
            cursor.execute("INSERT INTO SOURCE.CUSTOMER (ID, NAME) VALUES (%s, %s)", (cid, name))

        upd_custs = random.randint(2, 3)
        cursor.execute(f"SELECT ID FROM SOURCE.CUSTOMER ORDER BY RANDOM() LIMIT {upd_custs}")
        for row in cursor.fetchall():
            new_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            cursor.execute("UPDATE SOURCE.CUSTOMER SET NAME = %s WHERE ID = %s", (new_name, row[0]))

        cursor.execute("DELETE FROM SOURCE.CUSTOMER WHERE ID = (SELECT ID FROM SOURCE.CUSTOMER ORDER BY RANDOM() LIMIT 1)")
        context.log.info(f"  CUSTOMER: +{new_custs} inserts | {upd_custs} updates | -1 deletes")

        # ── ORDER_DETAIL: insert + update + delete ──
        new_orders = random.randint(5, 10)
        new_order_ids = []
        for i in range(new_orders):
            oid = str(uuid.uuid4())
            new_order_ids.append(oid)
            cust = random.choice(existing_customers)
            order_date = datetime.now() - timedelta(days=random.randint(0, 2))
            subtotal = round(random.uniform(5, 100), 2)
            tax = round(subtotal * 0.08, 2)
            total = round(subtotal + tax, 2)
            cursor.execute(
                """INSERT INTO SOURCE.ORDER_DETAIL
                   (ID, CUSTOMER, ORDERED_AT, STORE_ID, SUBTOTAL, TAX_PAID, ORDER_TOTAL)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (oid, cust, order_date.isoformat(), random_store,
                 str(subtotal), str(tax), str(total)))

        upd_orders = random.randint(3, 5)
        cursor.execute(f"SELECT ID FROM SOURCE.ORDER_DETAIL ORDER BY RANDOM() LIMIT {upd_orders}")
        for row in cursor.fetchall():
            new_total = round(random.uniform(10, 200), 2)
            cursor.execute("UPDATE SOURCE.ORDER_DETAIL SET ORDER_TOTAL = %s WHERE ID = %s",
                (str(new_total), row[0]))

        cursor.execute("DELETE FROM SOURCE.ORDER_DETAIL WHERE ID IN (SELECT ID FROM SOURCE.ORDER_DETAIL ORDER BY RANDOM() LIMIT 2)")
        context.log.info(f"  ORDER_DETAIL: +{new_orders} inserts | {upd_orders} updates | -2 deletes")

        # ── ORDER_ITEM: insert + delete ──
        new_items = 0
        for oid in new_order_ids:
            for j in range(random.randint(2, 3)):
                iid = str(uuid.uuid4())
                sku = random.choice(all_skus)
                cursor.execute("INSERT INTO SOURCE.ORDER_ITEM (ID, ORDER_ID, SKU) VALUES (%s, %s, %s)",
                    (iid, oid, sku))
                new_items += 1

        del_items = random.randint(2, 4)
        cursor.execute(f"DELETE FROM SOURCE.ORDER_ITEM WHERE ID IN (SELECT ID FROM SOURCE.ORDER_ITEM ORDER BY RANDOM() LIMIT {del_items})")
        context.log.info(f"  ORDER_ITEM: +{new_items} inserts | 0 updates | -{del_items} deletes")

        # ── PRODUCT + STORE: master data, no daily changes ──
        context.log.info("  PRODUCT: no changes (master data)")
        context.log.info("  STORE: no changes (master data)")

        # ── SUPPLY: insert + update + delete ──
        # Supply IDs are like SUP-001, SUP-002, etc.
        cursor.execute("SELECT MAX(CAST(REPLACE(ID, 'SUP-', '') AS INTEGER)) FROM SOURCE.SUPPLY WHERE ID LIKE 'SUP-%'")
        max_sup_result = cursor.fetchone()[0]
        max_sup_num = int(max_sup_result) if max_sup_result else 0

        new_supplies = random.randint(1, 2)
        for i in range(new_supplies):
            sid = f"SUP-{str(max_sup_num + i + 1).zfill(3)}"
            sku = random.choice(all_skus)
            cost = round(random.uniform(1, 50), 2)
            cursor.execute(
                "INSERT INTO SOURCE.SUPPLY (ID, NAME, COST, PERISHABLE, SKU) VALUES (%s, %s, %s, %s, %s)",
                (sid, f"supply item {sid}", str(cost), random.choice(["true", "false"]), sku))

        cursor.execute("SELECT ID FROM SOURCE.SUPPLY ORDER BY RANDOM() LIMIT 1")
        sup_row = cursor.fetchone()
        if sup_row:
            new_cost = round(random.uniform(1, 50), 2)
            cursor.execute("UPDATE SOURCE.SUPPLY SET COST = %s WHERE ID = %s", (str(new_cost), sup_row[0]))

        cursor.execute("DELETE FROM SOURCE.SUPPLY WHERE ID = (SELECT ID FROM SOURCE.SUPPLY ORDER BY RANDOM() LIMIT 1)")
        context.log.info(f"  SUPPLY: +{new_supplies} inserts | 1 updates | -1 deletes")

        conn.commit()

        # ══════════════════════════════════════════════
        # STEP 2: READ STREAMS + CHECK THRESHOLDS
        # Thresholds are stored in METRICS.THRESHOLD_CONFIG
        # Streams are combined in SOURCE.ALL_STREAMS_SUMMARY
        # ══════════════════════════════════════════════
        context.log.info("")
        context.log.info("=" * 60)
        context.log.info("  STEP 2: THRESHOLD CHECK (before dbt runs)")
        context.log.info("=" * 60)

        # Read per-table thresholds from config table (no hardcoded values)
        cursor.execute("SELECT TABLE_NAME, MAX_INSERT_PCT, MAX_UPDATE_PCT, MAX_DELETE_PCT FROM METRICS.THRESHOLD_CONFIG")
        threshold_rows = cursor.fetchall()
        thresholds = {}
        for t_name, m_ins, m_upd, m_del in threshold_rows:
            thresholds[t_name] = {"insert": m_ins, "update": m_upd, "delete": m_del}

        context.log.info("  Thresholds loaded from METRICS.THRESHOLD_CONFIG")
        context.log.info("-" * 60)

        alerts = []

        # One query to read all stream changes
        cursor.execute("SELECT TABLE_NAME, INSERTS, UPDATES, DELETES FROM SOURCE.ALL_STREAMS_SUMMARY")
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
                msg = f"INSERT BREACH: SOURCE.{table_name} | {inserts:,} rows ({ins_pct}%) exceeds {t['insert']}% limit"
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

            # Check update threshold
            if upd_pct > t["update"]:
                msg = f"UPDATE BREACH: SOURCE.{table_name} | {updates:,} rows ({upd_pct}%) exceeds {t['update']}% limit"
                context.log.error(f"    >> {msg}")
                alerts.append(msg)

            # Check delete threshold
            if del_pct > t["delete"]:
                msg = f"DELETE BREACH: SOURCE.{table_name} | {deletes:,} rows ({del_pct}%) exceeds {t['delete']}% limit"
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
#    Values come from .env file (local) or Dagster+ Environment Variables (cloud)
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
#    Reads manifest from dbt Cloud, creates Dagster assets automatically
#    No manual @asset definitions needed
# ══════════════════════════════════════════════════════════════
customer_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID")),
)


# ══════════════════════════════════════════════════════════════
# 5a. SNOWFLAKE AUDIT LOGGING (IST timestamps)
#     Logs every Dagster run (success/failure) to METRICS.DAGSTER_JOB_RUNS
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
#     Calls dbt Cloud API to get per-model status, rows, execution time
#     Saves to METRICS.DBT_MODEL_RUNS
#     NOTE: Not called in sensor to avoid timeout. Call manually if needed.
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
#     Calls dbt Cloud API to trigger "Retry Failed Models" job
#     Only reruns failed models + their downstream dependencies
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
#     After every successful run, counts rows in all layers
#     Compares with previous run (no hardcoded values)
#     Checks percentage-based thresholds
#     Saves to METRICS.LAYER_ROW_COUNTS
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
        context.log.info("  RECORD COUNTS + THRESHOLD CHECKS")
        context.log.info("=" * 60)
        context.log.info(f"  Thresholds: Insert>{MAX_INSERT_PCT}% | Delete>{MAX_DELETE_PCT}% | MinRows>{MIN_ROWS}")
        context.log.info("-" * 60)

        for schema, table in tables:
            # Current row count
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            rows_after = cursor.fetchone()[0]

            # Previous run row count (from METRICS.LAYER_ROW_COUNTS)
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

            # Threshold checks (based on previous run, not hardcoded)
            if rows_before > 0:
                max_insert = int(rows_before * MAX_INSERT_PCT / 100) or 1
                if inserted > max_insert:
                    pct = round(inserted / rows_before * 100)
                    msg = f"INSERT: {schema}.{table} | +{inserted:,} rows ({pct}% growth) exceeds {MAX_INSERT_PCT}% limit"
                    context.log.warning(f"    >> {msg}")
                    alerts.append(msg)

                max_delete = int(rows_before * MAX_DELETE_PCT / 100) or 1
                if deleted > max_delete:
                    pct = round(deleted / rows_before * 100)
                    msg = f"DELETE: {schema}.{table} | -{deleted:,} rows ({pct}% loss) exceeds {MAX_DELETE_PCT}% limit"
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
# 6. SENSORS (auto-start)
#    Success: logs to DAGSTER_JOB_RUNS + row counts for all 24 tables
#    Failure: logs to DAGSTER_JOB_RUNS + triggers retry job
# ══════════════════════════════════════════════════════════════
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING,
)
def log_success_to_snowflake(context: RunStatusSensorContext):
    """
    Fires after every successful Dagster run.
    1. Logs job status to METRICS.DAGSTER_JOB_RUNS
    2. Logs record counts for ALL 24 tables across SOURCE/LZ/STAGING/DBO
    NOTE: fetch_dbt_run_results not called here to avoid sensor timeout.
    """
    write_run_to_snowflake(context, status="SUCCESS")
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
    1. Logs job status to METRICS.DAGSTER_JOB_RUNS with error message
    2. Triggers dbt retry job (only reruns failed models)
    """
    error_data = None
    if context.failure_event and context.failure_event.step_failure_data:
        error_data = {
            "error_message": context.failure_event.step_failure_data.error.message
        }
    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)
    try:
        trigger_dbt_retry(context)
    except Exception as e:
        context.log.warning(f"  Could not trigger retry job: {e}")


# ══════════════════════════════════════════════════════════════
# 7. JOB + SCHEDULE
#    Job runs all assets (ingestion + dbt models)
#    Schedule: daily at 6 AM UTC
# ══════════════════════════════════════════════════════════════
run_customer_pipeline = define_asset_job(
    name="trigger_customer_dbt_cloud_job",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
)

daily_schedule = ScheduleDefinition(
    job=run_customer_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
)


# ══════════════════════════════════════════════════════════════
# 8. REGISTER EVERYTHING
#    ingest_daily_data runs FIRST (ingestion + threshold gate)
#    customer_dbt_assets runs NEXT (dbt Cloud job)
#    sensors fire AFTER (logging + retry)
# ══════════════════════════════════════════════════════════════
defs = Definitions(
    assets=[ingest_daily_data, customer_dbt_assets],
    jobs=[run_customer_pipeline],
    schedules=[daily_schedule],
    sensors=[log_success_to_snowflake, log_failure_to_snowflake],
)