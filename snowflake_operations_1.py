import snowflake.connector
from azure.storage.blob import BlobServiceClient
import sys
import datetime
import time

def get_snowflake_type(teradata_type_code):
    """Maps Teradata data type codes to their Snowflake equivalents."""
    clean_type_code = teradata_type_code.strip()
    mapping = { 'CF': 'VARCHAR', 'CV': 'VARCHAR', 'CO': 'VARCHAR', 'CG': 'VARCHAR', 'VG': 'VARCHAR', 'I1': 'TINYINT', 'I2': 'SMALLINT', 'I': 'INTEGER', 'I8': 'BIGINT', 'D': 'DECIMAL', 'N': 'NUMBER', 'F': 'FLOAT', 'DA': 'DATE', 'AT': 'TIME', 'TZ': 'TIME', 'TS': 'TIMESTAMP_NTZ', 'SZ': 'TIMESTAMP_TZ', 'YR': 'VARCHAR', 'YM': 'VARCHAR', 'MO': 'VARCHAR', 'DY': 'VARCHAR', 'DH': 'VARCHAR', 'DM': 'VARCHAR', 'DS': 'VARCHAR', 'HR': 'VARCHAR', 'HM': 'VARCHAR', 'HS': 'VARCHAR', 'MI': 'VARCHAR', 'MS': 'VARCHAR', 'SC': 'VARCHAR', 'BF': 'BINARY', 'BV': 'BINARY', 'BO': 'BINARY', 'BC': 'BINARY', 'GS': 'GEOGRAPHY', 'MB': 'GEOMETRY', 'JN': 'VARIANT', 'XM': 'VARCHAR', 'AV': 'VARIANT', 'PD': 'VARCHAR', 'PT': 'VARCHAR', 'PZ': 'VARCHAR', 'PS': 'VARCHAR', 'PM': 'VARCHAR', 'A1': 'VARIANT', 'AN': 'VARIANT'}
    return mapping.get(clean_type_code, 'STRING')

def create_table_and_pipe(config, sf_cursor, table_name, database_name, teradata_columns, log_func):
    """Creates a Snowflake table and a pipe for FULL LOADS."""
    db_name = config['SNOWFLAKE']['DATABASE']
    sf_schema = config['SNOWFLAKE']['SCHEMA']
    stage_name = config['SNOWFLAKE']['STAGE_NAME']
    file_format_name = config['SNOWFLAKE']['FILE_FORMAT_NAME']

    table_name_upper = table_name.upper()
    db_name_upper = db_name.upper()
    schema_upper = sf_schema.upper()
    table_fqn = f'"{db_name_upper}"."{schema_upper}"."{table_name_upper}"'
    pipe_name = f'"{table_name.lower()}_pipe"'
    pipe_fqn = f'"{db_name_upper}"."{schema_upper}".{pipe_name}'

    log_func(f"  [SF] Source columns and types found: {teradata_columns}")

    col_defs_list = []
    for col_name, td_type in teradata_columns:
        sf_type = get_snowflake_type(td_type)
        col_defs_list.append(f'"{col_name.upper()}" {sf_type}')
    col_defs = ", ".join(col_defs_list)
    create_table_sql = f"CREATE OR REPLACE TABLE {table_fqn} ({col_defs});"
    log_func(f"  [DEBUG] Generated CREATE TABLE SQL: {create_table_sql}")

    stage_path = f"@{stage_name}/{database_name.lower()}"
    create_pipe_sql = f"""
    CREATE OR REPLACE PIPE {pipe_fqn}
    AUTO_INGEST = FALSE AS
    COPY INTO {table_fqn}
    FROM {stage_path}
    FILE_FORMAT = (FORMAT_NAME = '{file_format_name}', SKIP_HEADER = 0)
    PATTERN = '.*{table_name.lower()}.csv';
    """
    try:
        log_func(f"  [SF] Executing CREATE TABLE for {table_fqn}...")
        sf_cursor.execute(create_table_sql)
        log_func(f"  [SF] Executing CREATE PIPE for {pipe_fqn}...")
        sf_cursor.execute(create_pipe_sql)
        return pipe_fqn
    except snowflake.connector.errors.ProgrammingError as e:
        log_func(f"  [SF ERROR] Could not create objects for {table_fqn}: {e}")
        return None

def refresh_and_verify_pipe(config, sf_cursor, pipe_name_fqn, table_name, teradata_db, expected_rows, log_func, timeout_seconds=600):
    """Refreshes a Snowpipe and verifies completion."""
    sf_db = config['SNOWFLAKE']['DATABASE']
    sf_schema = config['SNOWFLAKE']['SCHEMA']

    log_func("  [SF] Triggering pipe refresh...")
    start_time_utc_for_query = datetime.datetime.now(datetime.timezone.utc)
    try:
        sf_cursor.execute(f"ALTER PIPE {pipe_name_fqn} REFRESH;")
        log_func("  [SF] Load command issued. Now verifying completion...")
    except snowflake.connector.errors.ProgrammingError as e:
        log_func(f"  [SF ERROR] Could not refresh pipe {pipe_name_fqn}: {e}")
        return False

    start_time_for_timeout = time.time()
    table_fqn_for_sql = f'"{sf_db.upper()}"."{sf_schema.upper()}"."{table_name.upper()}"'
    table_fqn_for_history_func = f"{sf_db.upper()}.{sf_schema.upper()}.{table_name.upper()}"
    blob_path_for_history = f"{teradata_db.lower()}/{table_name.lower()}.csv"
    fallback_check_triggered = False

    while time.time() - start_time_for_timeout < timeout_seconds:
        time.sleep(20)
        if not fallback_check_triggered and (time.time() - start_time_for_timeout) > 15:
            log_func("    [SF] copy_history is slow to update. Triggering row count fallback check.")
            fallback_check_triggered = True
            try:
                sf_cursor.execute(f'SELECT COUNT(*) FROM {table_fqn_for_sql};')
                current_rows = sf_cursor.fetchone()[0]
                log_func(f"    [SF Fallback] Expected rows: {expected_rows}, Found in table: {current_rows}")
                if current_rows >= expected_rows:
                    log_func(f"  [SF SUCCESS] Fallback check PASSED. Row count matches.")
                    return True
            except Exception as e:
                log_func(f"    [SF WARN] Fallback check failed: {e}")

        try:
            history_query = f"""
            SELECT STATUS, ROW_COUNT, FIRST_ERROR_MESSAGE
            FROM table(information_schema.copy_history(TABLE_NAME=>'{table_fqn_for_history_func}', START_TIME=>'{start_time_utc_for_query.isoformat()}'::TIMESTAMP_LTZ))
            WHERE file_name LIKE '%{blob_path_for_history}' ORDER BY last_load_time DESC LIMIT 1;
            """
            sf_cursor.execute(history_query)
            result = sf_cursor.fetchone()
            if result:
                status, row_count, error_msg = result
                log_func(f"    [SF] Load status from history: {status}, Rows Loaded: {row_count}")
                if status == 'LOADED':
                    log_func(f"  [SF SUCCESS] copy_history confirmed LOADED.")
                    return True
                elif status == 'LOAD_FAILED':
                    log_func(f"  [SF ERROR] copy_history reported LOAD_FAILED. Reason: {error_msg}")
                    return False
            else:
                log_func("    [SF] No load history found for the file yet. Waiting...")
        except Exception as e:
            log_func(f"  [SF WARN] Could not check copy history, will retry... Error: {e}")
    log_func(f"  [SF ERROR] Pipe refresh verification timed out after {timeout_seconds} seconds.")
    return False

def get_last_watermark(sf_cursor, table_name, log_func):
    """Fetches the last successful watermark value from the control table."""
    try:
        query = "SELECT LAST_WATERMARK_VALUE FROM MIGRATION_CONTROL.WATERMARKS WHERE TABLE_NAME = %s"
        sf_cursor.execute(query, (table_name.upper(),))
        result = sf_cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        log_func(f"  [SF ERROR] Could not retrieve watermark for {table_name}: {e}")
        return None

def update_watermark(sf_cursor, table_name, new_watermark, log_func):
    """Updates (or inserts) the watermark value for a table."""
    log_func(f"  [SF] Updating watermark for '{table_name}' to '{new_watermark}'")
    try:
        watermark_str = new_watermark.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(new_watermark, datetime.datetime) else str(new_watermark)
        query = """
        MERGE INTO MIGRATION_CONTROL.WATERMARKS w
        USING (SELECT %s as name, %s as val) v
        ON w.TABLE_NAME = v.name
        WHEN MATCHED THEN UPDATE SET w.LAST_WATERMARK_VALUE = v.val, w.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (TABLE_NAME, LAST_WATERMARK_VALUE) VALUES (v.name, v.val);
        """
        sf_cursor.execute(query, (table_name.upper(), watermark_str))
        log_func(f"  [SF] Watermark updated successfully.")
    except Exception as e:
        log_func(f"  [SF ERROR] Failed to update watermark for {table_name}: {e}")

def load_and_merge_delta(config, sf_cursor, table_name, teradata_db_name, log_func, migration_details):
    """Loads delta data into a transient table and merges it."""
    sf_db = config['SNOWFLAKE']['DATABASE']
    sf_schema = config['SNOWFLAKE']['SCHEMA']
    stage_name = config['SNOWFLAKE']['STAGE_NAME']
    file_format_name = config['SNOWFLAKE']['FILE_FORMAT_NAME']

    table_name_upper = table_name.upper()
    pk_col = migration_details['primary_key_column']

    target_table_fqn = f'"{sf_db}"."{sf_schema}"."{table_name_upper}"'
    temp_table_fqn = f'"{sf_db}"."{sf_schema}"."TEMP_DELTA_{table_name_upper}"'

    try:
        log_func(f"  [SF] Creating transient staging table: {temp_table_fqn}")
        sf_cursor.execute(f"CREATE OR REPLACE TRANSIENT TABLE {temp_table_fqn} LIKE {target_table_fqn};")

        stage_path = f"@{stage_name}/{teradata_db_name.lower()}/{table_name.lower()}.csv"
        copy_sql = f"COPY INTO {temp_table_fqn} FROM '{stage_path}' FILE_FORMAT = (FORMAT_NAME = '{file_format_name}', SKIP_HEADER = 0);"
        log_func(f"  [SF] Copying delta data from stage into transient table...")
        sf_cursor.execute(copy_sql)
        rows_copied = sf_cursor.rowcount
        log_func(f"  [SF] Copied {rows_copied} rows into transient table.")

        log_func(f"  [SF] Merging data into target table: {target_table_fqn}")

        sf_cursor.execute(f"DESC TABLE {target_table_fqn}")
        columns = [row[0] for row in sf_cursor.fetchall()]
        pk_col_upper = pk_col.upper()

        update_set_clause = ", ".join([f'target."{col}" = source."{col}"' for col in columns if col.upper() != pk_col_upper])
        insert_cols_clause = ", ".join([f'"{col}"' for col in columns])
        insert_values_clause = ", ".join([f'source."{col}"' for col in columns])
        if not update_set_clause:
            update_set_clause = f'target."{pk_col}" = source."{pk_col}"'

        merge_sql = f"""
        MERGE INTO {target_table_fqn} AS target
        USING {temp_table_fqn} AS source
        ON target."{pk_col.upper()}" = source."{pk_col.upper()}"
        WHEN MATCHED THEN UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols_clause}) VALUES ({insert_values_clause});
        """
        sf_cursor.execute(merge_sql)
        rows_merged = sf_cursor.rowcount
        log_func(f"  [SF] Merge complete. {rows_merged} rows affected.")
        return True, rows_copied
    except Exception as e:
        log_func(f"  [SF ERROR] Failed during delta merge process for {table_name}: {e}")
        return False, 0
    finally:
        log_func(f"  [SF] Dropping transient table: {temp_table_fqn}")
        sf_cursor.execute(f"DROP TABLE IF EXISTS {temp_table_fqn};")




# (Keep all other functions in this file as they are)

def start_audit_log(sf_cursor, job_id, table_name, migration_type, watermark_start):
    """Creates a new row in the audit table with status 'IN_PROGRESS' and returns the AUDIT_ID."""
    try:
        # --- MODIFICATION: Removed WATERMARK_START from the INSERT statement ---
        insert_sql = """
        INSERT INTO MIGRATION_CONTROL.MIGRATION_AUDIT_LOG 
            (JOB_ID, TABLE_NAME, MIGRATION_TYPE, START_TIME, STATUS)
        VALUES 
            (%s, %s, %s, %s, %s)
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        # --- MODIFICATION: Removed watermark_start from the parameters ---
        params = (job_id, table_name.upper(), migration_type, start_time, 'IN_PROGRESS')
        
        sf_cursor.execute(insert_sql, params)
        
        get_id_sql = """
            SELECT AUDIT_ID 
            FROM MIGRATION_CONTROL.MIGRATION_AUDIT_LOG
            WHERE JOB_ID = %s 
              AND TABLE_NAME = %s 
              AND STATUS = 'IN_PROGRESS'
            ORDER BY START_TIME DESC
            LIMIT 1;
        """
        get_id_params = (job_id, table_name.upper())
        result = sf_cursor.execute(get_id_sql, get_id_params).fetchone()

        if result:
            return result[0]
        else:
            raise Exception("Could not retrieve AUDIT_ID after insertion.")

    except Exception as e:
        print(f"[AUDIT ERROR] Failed to start audit log for {table_name}: {e}", file=sys.stderr)
        return None
    
def finish_audit_log(sf_cursor, audit_id, status, rows_processed, watermark_end, error_message=None):
    """Updates an existing audit log row with the final outcome of the migration."""
    if audit_id is None:
        print(f"[AUDIT ERROR] Cannot finish audit log because audit_id is None.", file=sys.stderr)
        return
    try:
        # --- MODIFICATION: Removed WATERMARK_END from the UPDATE statement ---
        sql = """
        UPDATE MIGRATION_CONTROL.MIGRATION_AUDIT_LOG
        SET END_TIME = %s, STATUS = %s, ROWS_PROCESSED = %s, ERROR_MESSAGE = %s, LAST_UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE AUDIT_ID = %s
        """
        end_time = datetime.datetime.now(datetime.timezone.utc)
        error_msg_safe = str(error_message)[:1000] if error_message else None
        
        # --- MODIFICATION: Removed watermark_end_safe from the parameters ---
        params = (end_time, status, rows_processed, error_msg_safe, audit_id)
        
        sf_cursor.execute(sql, params)
    except Exception as e:
        print(f"[AUDIT ERROR] Failed to finish audit log for ID {audit_id}: {e}", file=sys.stderr)



        
# def start_audit_log(sf_cursor, job_id, table_name, migration_type, watermark_start):
#     """Creates a new row in the audit table with status 'IN_PROGRESS' and returns the AUDIT_ID."""
#     try:
#         insert_sql = "INSERT INTO MIGRATION_CONTROL.MIGRATION_AUDIT_LOG (JOB_ID, TABLE_NAME, MIGRATION_TYPE, START_TIME, STATUS, WATERMARK_START) VALUES (%s, %s, %s, %s, %s, %s)"
#         start_time = datetime.datetime.now(datetime.timezone.utc)
#         params = (job_id, table_name.upper(), migration_type, start_time, 'IN_PROGRESS', watermark_start)
#         sf_cursor.execute(insert_sql, params)
#         get_id_sql = "SELECT AUDIT_ID FROM MIGRATION_CONTROL.MIGRATION_AUDIT_LOG WHERE JOB_ID = %s AND TABLE_NAME = %s AND STATUS = 'IN_PROGRESS' ORDER BY START_TIME DESC LIMIT 1;"
#         get_id_params = (job_id, table_name.upper())
#         result = sf_cursor.execute(get_id_sql, get_id_params).fetchone()
#         if result: return result[0]
#         else: raise Exception("Could not retrieve AUDIT_ID after insertion.")
#     except Exception as e:
#         print(f"[AUDIT ERROR] Failed to start audit log for {table_name}: {e}", file=sys.stderr)
#         return None

# def finish_audit_log(sf_cursor, audit_id, status, rows_processed, watermark_end, error_message=None):
#     """Updates an existing audit log row with the final outcome of the migration."""
#     if audit_id is None:
#         print(f"[AUDIT ERROR] Cannot finish audit log because audit_id is None.", file=sys.stderr)
#         return
#     try:
#         sql = "UPDATE MIGRATION_CONTROL.MIGRATION_AUDIT_LOG SET END_TIME = %s, STATUS = %s, ROWS_PROCESSED = %s, WATERMARK_END = %s, ERROR_MESSAGE = %s, LAST_UPDATED_AT = CURRENT_TIMESTAMP() WHERE AUDIT_ID = %s"
#         end_time = datetime.datetime.now(datetime.timezone.uc)
#         error_msg_safe = str(error_message)[:1000] if error_message else None
#         watermark_end_safe = None
#         if watermark_end is not None:
#             watermark_end_safe = watermark_end.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(watermark_end, datetime.datetime) else str(watermark_end)
#         params = (end_time, status, rows_processed, watermark_end_safe, error_msg_safe, audit_id)
#         sf_cursor.execute(sql, params)
#     except Exception as e:
#         print(f"[AUDIT ERROR] Failed to finish audit log for ID {audit_id}: {e}", file=sys.stderr)