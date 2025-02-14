import logging
import pandas as pd
from sqlalchemy import text

def setup_logging():
    """Setup logging for merge operations."""
    logging.basicConfig(filename="merge_process.log", level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

def quote_table_name(schema_name, table_name):
    """Ensure schema and table names are properly quoted for case sensitivity."""
    return f'"{schema_name}"."{table_name}"'

def fetch_column_names(engine, schema_name, table_name):
    """Fetch column names dynamically and ensure case-insensitive matching."""
    query = text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    """)
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)

    # Create a mapping of lowercase column names to actual column names
    column_mapping = {col.lower(): col for col in df["column_name"].tolist()}
    logging.info(f"Columns fetched for {table_name}: {column_mapping}")

    return column_mapping

def fetch_quarter_details(engine):
    """Ensure quarter_detail table exists and fetch quarter details."""
    query = text('SELECT "Update_type", "Status", "EFFECTIVE_START_DATE", "EFFECTIVE_END_DATE", "VERSION_ID" FROM "table_registry"."quarter_detail"')

    with engine.begin() as conn:
        df = pd.read_sql(query, conn)

    df.columns = df.columns.str.strip()  # Remove extra spaces if any

    return df.set_index("Update_type")

def ensure_target_table_exists(engine, schema_name, target_table, source_table):
    """Ensure that the target table exists and matches the source table's structure."""
    create_table_query = text(f"""
    DO $$ 
    BEGIN 
        IF NOT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{schema_name}' AND table_name = '{target_table}'
        ) THEN 
            CREATE TABLE {quote_table_name(schema_name, target_table)} AS 
            SELECT * FROM {quote_table_name(schema_name, source_table)} WHERE 1=0;
        END IF;
    END $$;
    """)

    with engine.begin() as conn:
        conn.execute(create_table_query)

    logging.info(f"Ensured {target_table} exists with the same structure as {source_table}")

def run_merge_process(engine, schema_name, source_table, target_table, business_keys, exclude_fields):
    """Perform the merge process using dynamic column selection."""

    logging.info(f"Starting merge process for {target_table} in schema {schema_name}")

    try:
        # Step 1: Fetch all column names dynamically
        source_columns = fetch_column_names(engine, schema_name, source_table)
        target_columns = fetch_column_names(engine, schema_name, target_table)

        # Match columns dynamically using lowercase mapping
        mapped_columns = [target_columns.get(col.lower(), col) for col in source_columns.keys()]

        logging.info(f"Mapped columns: {mapped_columns}")

        # Step 2: Archive target table
        archive_query = text(f'ALTER TABLE {quote_table_name(schema_name, target_table)} RENAME TO "{target_table}_bu"')
        create_backup_query = text(f'''
            CREATE TABLE {quote_table_name(schema_name, target_table)} AS 
            TABLE {quote_table_name(schema_name, target_table + "_bu")}
        ''')

        with engine.begin() as conn:
            conn.execute(archive_query)  # Rename the table
            conn.execute(create_backup_query)  # Create a new copy with the same structure and data

        logging.info(f"Archived {target_table} as {target_table}_bu and created a new table with the same structure and data")

        # Step 3: Ensure target table exists before inserting
        ensure_target_table_exists(engine, schema_name, target_table, source_table)

        # Step 4: Fetch quarter details
        quarter_details = fetch_quarter_details(engine)

        # Step 5: Insert new records
        insert_new_query = text(f"""
        INSERT INTO {quote_table_name(schema_name, target_table)} ({", ".join(mapped_columns)}, status, effective_start_date, effective_end_date, version_id)
        SELECT {", ".join(source_columns.values())}, 
            {quarter_details.loc['New', 'Status']}, 
            '{quarter_details.loc['New', 'EFFECTIVE_START_DATE']}', 
            '{quarter_details.loc['New', 'EFFECTIVE_END_DATE']}', 
            '{quarter_details.loc['New', 'VERSION_ID']}'
        FROM {quote_table_name(schema_name, source_table)} s
        LEFT JOIN {quote_table_name(schema_name, target_table)} t
        ON {" AND ".join([f"s.{col} = t.{col}" for col in business_keys])}
        WHERE t.{business_keys[0]} IS NULL;
        """)
        with engine.begin() as conn:
            conn.execute(insert_new_query)
        logging.info("Inserted new records into target table")

        # Step 6: Term-date deleted records
        term_delete_query = text(f"""
        UPDATE {quote_table_name(schema_name, target_table)} t
        SET status = {quarter_details.loc['Delete', 'Status']}, 
            effective_end_date = '{quarter_details.loc['Delete', 'EFFECTIVE_END_DATE']}'
        FROM {quote_table_name(schema_name, target_table)} t1
        LEFT JOIN {quote_table_name(schema_name, source_table)} s
        ON {" AND ".join([f"t1.{col} = s.{col}" for col in business_keys])}
        WHERE t.{business_keys[0]} = t1.{business_keys[0]} 
        AND t.status != 0 AND s.{business_keys[0]} IS NULL;
        """)
        with engine.begin() as conn:
            conn.execute(term_delete_query)
        logging.info("Updated term-date records for deleted entries")

        # Step 7: Insert change-based new records
        insert_change_new_query = text(f"""
        INSERT INTO {quote_table_name(schema_name, target_table)} ({", ".join(mapped_columns)}, status, effective_start_date, effective_end_date, version_id)
        SELECT {", ".join(source_columns.values())}, 
            {quarter_details.loc['Change-Based New', 'Status']}, 
            '{quarter_details.loc['Change-Based New', 'EFFECTIVE_START_DATE']}', 
            '{quarter_details.loc['Change-Based New', 'EFFECTIVE_END_DATE']}', 
            '{quarter_details.loc['Change-Based New', 'VERSION_ID']}'
        FROM {quote_table_name(schema_name, source_table)} s
        JOIN {quote_table_name(schema_name, target_table)} t
        ON {" AND ".join([f"s.{col} = t.{col}" for col in business_keys])}
        WHERE t.status != 0
        AND ({ " OR ".join([f"s.{col} <> t.{col}" for col in mapped_columns]) });
        """)
        with engine.begin() as conn:
            conn.execute(insert_change_new_query)
        logging.info("Inserted change-based new records")

        logging.info("Merge process completed successfully!")
        return "Merge process completed successfully!"

    except Exception as e:
        logging.error(f"Error during merge: {str(e)}")
        return f"Merge failed: {str(e)}"
