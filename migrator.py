import sys
import csv
import uuid
import os
import subprocess
from io import StringIO
import teradatasql
from azure.storage.blob import BlobServiceClient

# --- Local Module Imports ---
import configg
import snowflake_operations_1 as sf_ops


def list_teradata_databases():
    """Fetch all databases from Teradata."""
    try:
        with teradatasql.connect(host=configg.TERADATA_HOST, user=configg.TERADATA_USER, password=configg.TERADATA_PASS) as conn:
            cur = conn.cursor()
            cur.execute("SELECT DatabaseName FROM DBC.DatabasesV;")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[ERROR] Could not connect to Teradata or list databases: {e}", file=sys.stderr)
        return []


def list_teradata_tables(database):
    """Fetch all tables from a given Teradata database."""
    try:
        with teradatasql.connect(host=configg.TERADATA_HOST, user=configg.TERADATA_USER, password=configg.TERADATA_PASS) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT TableName FROM DBC.TablesV WHERE DatabaseName='{database}';")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[ERROR] Could not connect to Teradata or list tables for {database}: {e}", file=sys.stderr)
        return []

def get_teradata_columns(database_name, table_name):
    """Fetches the column names in order from the Teradata source."""
    try:
        with teradatasql.connect(host=configg.TERADATA_HOST, user=configg.TERADATA_USER, password=configg.TERADATA_PASS) as conn:
            cur = conn.cursor()
            query = f"SELECT ColumnName FROM DBC.ColumnsV WHERE DatabaseName='{database_name}' AND TableName='{table_name}' ORDER BY ColumnId;"
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[ERROR] Could not get column list for {database_name}.{table_name}: {e}", file=sys.stderr)
        return []

def get_teradata_query_details(database_name, table_name, where_clause="", tracking_col=None):
    """Gets the row count and new max watermark for a given query from Teradata."""
    count_query = f"SELECT COUNT(*) FROM {database_name}.{table_name} {where_clause};"
    watermark_query = f"SELECT MAX({tracking_col}) FROM {database_name}.{table_name} {where_clause};" if tracking_col else None
    
    try:
        with teradatasql.connect(host=configg.TERADATA_HOST, user=configg.TERADATA_USER, password=configg.TERADATA_PASS) as conn:
            cur = conn.cursor()
            cur.execute(count_query)
            row_count = cur.fetchone()[0]
            
            new_watermark = None
            if watermark_query:
                cur.execute(watermark_query)
                result = cur.fetchone()
                if result:
                    new_watermark = result[0]
            
            return row_count, new_watermark
    except Exception as e:
        print(f"[ERROR] Could not get query details for {database_name}.{table_name}: {e}", file=sys.stderr)
        return 0, None

def run_teradata_to_azure_tpt(table_name, database_name, log_func, migration_details, last_watermark=None):
    """
    Exports Teradata data using TPT to a local file and uploads it to Azure.
    Returns a tuple: (success_bool, new_max_watermark, total_rows_extracted).
    """
    db_name_lower = database_name.lower()
    table_name_lower = table_name.lower()
    
    run_uuid = str(uuid.uuid4())
    local_filename = f"{db_name_lower}_{table_name_lower}_{run_uuid}.csv"
    tpt_script_filename = f"tpt_job_{run_uuid}.tpt"
    tpt_log_filename = f"tpt_log_{run_uuid}.log"
    azure_blob_name = f"{db_name_lower}/{table_name_lower}.csv"

    log_func(f"[TPT] Starting export for table '{table_name}'")
    
    where_clause = ""
    tracking_col = None
    new_max_watermark = last_watermark

    if migration_details['type'] == 'Delta Load (Incremental)' and last_watermark:
        tracking_col = migration_details['tracking_column']
        where_clause = f" WHERE {tracking_col} > '{last_watermark}'"
    
    log_func("  [TD] Getting row count and new watermark...")
    rows_to_export, potential_new_watermark = get_teradata_query_details(database_name, table_name, where_clause, tracking_col)
    
    if rows_to_export == 0:
        log_func("[SUCCESS] No new rows found to export. Task is complete.")
        return True, last_watermark, 0
        
    log_func(f"  [TD] Found {rows_to_export} rows to export.")
    if potential_new_watermark:
        new_max_watermark = potential_new_watermark

    full_table_name = f"{database_name}.{table_name}"
    tpt_script_content = f"""
    DEFINE JOB EXPORT_{table_name_lower}
    (
        DEFINE SCHEMA S_{table_name_lower} FROM TABLE '{full_table_name}';

        DEFINE OPERATOR EXPORT_OPERATOR
        TYPE EXPORT
        SCHEMA S_{table_name_lower}
        ATTRIBUTES
        (
            VARCHAR TdpId          = '{configg.TERADATA_HOST}',
            VARCHAR UserName       = '{configg.TERADATA_USER}',
            VARCHAR UserPassword   = '{configg.TERADATA_PASS}',
            VARCHAR SelectStmt     = 'SELECT * FROM {full_table_name}{where_clause};'
        );

        DEFINE OPERATOR FILE_WRITER
        TYPE DATACONNECTOR CONSUMER
        SCHEMA *
        ATTRIBUTES
        (
            VARCHAR FileName       = '{local_filename}',
            VARCHAR Format         = 'Delimited',
            VARCHAR OpenMode       = 'Write',
            VARCHAR TextDelimiter  = ',',
            VARCHAR IndicatorMode  = 'N'
        );

        APPLY TO OPERATOR (FILE_WRITER)
        SELECT * FROM OPERATOR (EXPORT_OPERATOR);
    );
    """

    tpt_succeeded = False
    try:
        with open(tpt_script_filename, 'w', encoding='utf-8') as f:
            f.write(tpt_script_content)

        log_func(f"  [TPT] Executing TPT job. Log will be in '{tpt_log_filename}'")
        tpt_command = ['tbuild', '-f', tpt_script_filename, '-l', tpt_log_filename]
        
        result = subprocess.run(tpt_command, capture_output=True, text=True)

        if result.returncode > 8:
            raise subprocess.CalledProcessError(
                returncode=result.returncode, cmd=result.args, output=result.stdout, stderr=result.stderr
            )
        
        log_func(f"  [TPT] TPT job finished with exit code: {result.returncode} (0=Success).")
        
        if not os.path.exists(local_filename):
            log_func(f"[ERROR] TPT exited with code {result.returncode} but did NOT create the output file.")
            raise FileNotFoundError(f"TPT failed to create output file: {local_filename}")
        
        tpt_succeeded = True
        log_func(f"  [AZ] Uploading '{local_filename}' to blob '{azure_blob_name}'...")
        blob_service_client = BlobServiceClient.from_connection_string(configg.AZURE_CONN_STR)
        blob_client = blob_service_client.get_blob_client(container=configg.AZURE_CONTAINER, blob=azure_blob_name)
        
        with open(local_filename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        log_func(f"[SUCCESS] Finished exporting {rows_to_export} rows to {azure_blob_name}")
        return True, new_max_watermark, rows_to_export

    except Exception as e:
        log_func(f"[ERROR] An unexpected error occurred during the TPT process: {e}")
        return False, None, 0
    finally:
        log_func("  [SYS] Cleaning up local temporary files...")
        if not tpt_succeeded and os.path.exists(tpt_log_filename):
            log_func(f"  [DEBUG] TPT failed. Log file '{tpt_log_filename}' has been kept for inspection.")
        else:
            for f in [local_filename, tpt_script_filename, tpt_log_filename]:
                if os.path.exists(f):
                    try: os.remove(f)
                    except OSError as e: log_func(f"  [WARN] Could not remove temporary file {f}: {e}")

def migrate_table(table_name, database_name, sf_cursor, log_func, migration_details):
    log_func(f"[DEBUG] Current Working Directory is: {os.getcwd()}")
    log_func("\n" + "=" * 70)
    log_func(f"             Processing Table: {table_name.upper()}")
    log_func(f"             Mode: {migration_details['type']}")
    log_func("=" * 70)

    result = {"success": False, "rows_processed": 0, "watermark_start": None, "watermark_end": None}
    migration_type = migration_details['type']

    if migration_type == 'Full Load (Replaces table)':
        success, _, rows_processed = run_teradata_to_azure_tpt(table_name, database_name, log_func, migration_details)
        result["rows_processed"] = rows_processed
        if not success:
            return result
        
        if rows_processed == 0:
            log_func("[SUCCESS] Table is empty. No Snowflake objects will be created.")
            result["success"] = True
            return result

        log_func("  [TD] Getting source table schema...")
        column_names = get_teradata_columns(database_name, table_name)
        if not column_names:
            log_func(f"[ERROR] Could not retrieve column names for '{table_name}'. Aborting Snowflake operations.")
            return result

        pipe_name = sf_ops.create_table_and_pipe(
            sf_cursor, table_name, configg.SNOWFLAKE_DATABASE, 
            configg.SNOWFLAKE_SCHEMA, configg.SNOWFLAKE_STAGE_NAME, 
            configg.SNOWFLAKE_FILE_FORMAT_NAME, database_name, column_names, log_func
        )
        if not pipe_name:
            return result
        
        # --- THIS IS THE MODIFIED LINE ---
        # We now pass 'rows_processed' to the verification function for the fallback check.
        pipe_success = sf_ops.refresh_and_verify_pipe(
            sf_cursor, pipe_name, table_name, configg.SNOWFLAKE_DATABASE, 
            configg.SNOWFLAKE_SCHEMA, database_name, rows_processed, log_func
        )
        if not pipe_success:
            log_func(f"[ERROR] Data ingestion via Snowpipe for '{table_name}' failed or timed out.")
            return result

        result["success"] = True
        return result

    elif migration_type == 'Delta Load (Incremental)':
        last_watermark = sf_ops.get_last_watermark(sf_cursor, table_name, log_func)
        log_func(f"[DELTA] Current watermark for '{table_name}' is: {last_watermark}")
        result["watermark_start"] = last_watermark

        success, new_watermark, rows_processed = run_teradata_to_azure_tpt(table_name, database_name, log_func, migration_details, last_watermark)
        result["rows_processed"] = rows_processed
        if not success:
            return result
        
        if rows_processed == 0:
            log_func("[SUCCESS] No new data to process. Table is up-to-date.")
            result["success"] = True
            result["watermark_end"] = last_watermark
            return result
        
        result["watermark_end"] = new_watermark

        merge_success, _ = sf_ops.load_and_merge_delta(sf_cursor, table_name, database_name, log_func, migration_details)
        if not merge_success:
            return result
        
        sf_ops.update_watermark(sf_cursor, table_name, new_watermark, log_func)
        result["success"] = True
        return result
    
    return result