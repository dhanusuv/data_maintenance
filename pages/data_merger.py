import streamlit as st
import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from merge_process import run_merge_process  # Import merge function

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

# Get database credentials
DB_HOST = config.get("database", "host")
DB_NAME = config.get("database", "name")
DB_USER = config.get("database", "user")
DB_PASSWORD = config.get("database", "password")
DB_PORT = config.get("database", "port")
SCHEMA_NAME = config.get("database", "schema")

# Create SQLAlchemy engine
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Function to get all schemas
def get_schemas():
    query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
    df = pd.read_sql(query, engine)
    return df["schema_name"].tolist()

# Function to get tables for a selected schema
def get_tables(schema_name):
    query = text(f'SELECT table_name FROM information_schema.tables WHERE table_schema = :schema_name')
    df = pd.read_sql(query, engine, params={"schema_name": schema_name})
    return df["table_name"].tolist()

# Function to store user selections in session state
def store_user_selections(schema, source_table, target_table):
    st.session_state.setdefault("selected_schema", schema)
    st.session_state.setdefault("source_table", source_table)
    st.session_state.setdefault("target_table", target_table)

# Function to get column names for selected table
def get_column_names(schema_name, table_name):
    query = text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :schema_name AND table_name = :table_name
    """)
    df = pd.read_sql(query, engine, params={"schema_name": schema_name, "table_name": table_name})
    return df["column_name"].tolist()

# Function to fetch table data
def fetch_table_data(schema_name, table_name, columns, fetch_all=False):
    """Fetch table data dynamically from the database."""
    if fetch_all:
        column_str = "*"  # Fetch all columns
    else:
        column_str = ', '.join([f'"{col}"' for col in columns])

    query = text(f'SELECT {column_str} FROM "{schema_name}"."{table_name}"')

    with engine.begin() as conn:
        df = pd.read_sql(query, conn)

    return df

# Function to find new records
def find_new_records(df_source, df_target, business_keys):
    merged_df = df_source.merge(
        df_target,
        on=business_keys,
        how="left",
        indicator=True
    )
    new_records = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])
    return new_records

# Function to find deleted (termed) records
def find_deleted_records(df_source, df_target, business_keys):
    status_col = next((col for col in df_target.columns if col.lower() == "status"), None)

    if status_col:
        df_target_active = df_target[df_target[status_col] != "0"].copy()
    else:
        df_target_active = df_target.copy()

    merged_df = df_target_active.merge(
        df_source[business_keys],
        on=business_keys,
        how="left",
        indicator=True
    )

    deleted_records = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])
    return deleted_records

# Function to find changed records
def find_changed_records(df_source, df_target, business_keys, exclude_fields):
    df_source.columns = df_source.columns.str.lower()
    df_target.columns = df_target.columns.str.lower()

    business_keys = [col.lower() for col in business_keys]
    exclude_fields = [col.lower() for col in exclude_fields]

    # Identify STATUS column in target and filter active records
    status_col = next((col for col in df_target.columns if col.lower() == "status"), None)
    if status_col:
        df_target = df_target[df_target[status_col] != "0"].copy()

    # Get all fields excluding business keys and user-selected exclusions
    compare_fields = [col for col in df_source.columns if col not in business_keys + exclude_fields]
    compare_fields = [col for col in compare_fields if col in df_target.columns]

    # Perform INNER JOIN to match records based on business keys
    merged_df = df_source.merge(
        df_target[business_keys + compare_fields],
        on=business_keys,
        how="inner",
        suffixes=("", "_target")
    )

    # Identify rows where at least one field has changed
    conditions = []
    for field in compare_fields:
        target_col = f"{field}_target"
        conditions.append(merged_df[field] != merged_df[target_col])

    changed_records = merged_df[pd.concat(conditions, axis=1).any(axis=1)].copy()

    # Drop target columns to keep only the source columns
    columns_to_keep = [col for col in changed_records.columns if "_target" not in col]
    changed_records = changed_records[columns_to_keep]

    return changed_records

# Streamlit App
def main():
    st.title("Data Merger - Identify New, Deleted & Changed Records")

    # Maintain selections across pages
    selected_schema = st.session_state.get("selected_schema", None)
    source_table = st.session_state.get("source_table", None)
    target_table = st.session_state.get("target_table", None)

    schemas = get_schemas()
    selected_schema = st.selectbox("Select Schema:", schemas, index=0)

    if selected_schema:
        tables = get_tables(selected_schema)

        source_table = st.selectbox("Select Source Table:", tables, index=0)
        target_table = st.selectbox("Select Target Table:", tables, index=0)

        if source_table and target_table:
            source_columns = get_column_names(selected_schema, source_table)
            target_columns = get_column_names(selected_schema, target_table)

            business_keys = st.multiselect("Select Business Key(s) for Matching", source_columns)

            default_exclude = ["STATUS", "EFFECTIVE_START_DATE", "EFFECTIVE_END_DATE", "VERSION_ID"]
            valid_defaults = [col for col in default_exclude if col in target_columns or col.lower() in [c.lower() for c in target_columns]]

            exclude_fields = st.multiselect(
                "Select Fields to Exclude from Comparison", target_columns, default=valid_defaults
            )

            if st.button("Find New Records"):
                df_source = fetch_table_data(selected_schema, source_table, business_keys, fetch_all=True)
                df_target = fetch_table_data(selected_schema, target_table, business_keys, fetch_all=False)

                new_records = find_new_records(df_source, df_target, business_keys)

                if not new_records.empty:
                    st.write(f"### New Records Found: {len(new_records)}")
                    st.dataframe(new_records)
                else:
                    st.success("No new records found.")

            if st.button("Find Deleted Records"):
                df_target = fetch_table_data(selected_schema, target_table, business_keys, fetch_all=True)
                df_source = fetch_table_data(selected_schema, source_table, business_keys, fetch_all=False)

                deleted_records = find_deleted_records(df_source, df_target, business_keys)

                if not deleted_records.empty:
                    st.write(f"### Deleted (Termed) Records Found: {len(deleted_records)}")
                    st.dataframe(deleted_records)
                else:
                    st.success("No deleted records found.")

            if st.button("Find Changed Records"):
                df_source = fetch_table_data(selected_schema, source_table, target_columns, fetch_all=True)
                df_target = fetch_table_data(selected_schema, target_table, target_columns, fetch_all=True)

                changed_records = find_changed_records(df_source, df_target, business_keys, exclude_fields)

                if not changed_records.empty:
                    st.write(f"### Changed Records Found: {len(changed_records)}")
                    st.dataframe(changed_records)
                else:
                    st.success("No changed records found.")

            # 🟢 Merge Button (Green Color)
            merge_button = st.button("Merge", key="merge_button")

            if merge_button:
                st.write("### Merging Data... Please wait.")
                result = run_merge_process(engine, selected_schema, source_table, target_table, business_keys, exclude_fields)

                if "completed" in result.lower():
                    st.success(result)  # Display success message
                else:
                    st.error(result)  # Display error message

    st.page_link("home.py", label="⬅ Go Back to Home Page")

if __name__ == "__main__":
    main()
