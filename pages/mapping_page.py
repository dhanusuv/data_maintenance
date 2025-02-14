import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import configparser

import toml

secrets = toml.load(".streamlit/secrets.toml")
db_config = secrets["database"]

# Get database credentials
DB_HOST = db_config["host"]
DB_NAME = db_config["name"]
DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]
DB_PORT = db_config["port"]
SCHEMA_NAME = db_config["schema"]
TABLE_NAME = db_config["table"]

# Create SQLAlchemy engine
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

import streamlit as st

# Retrieve selections from session state
selected_schema = st.session_state.get("selected_schema")
source_table = st.session_state.get("source_table")
target_table = st.session_state.get("target_table")

# Ensure selections exist before proceeding
if not selected_schema or not source_table or not target_table:
    st.error("⚠️ Please select schema and tables in Data Merger before proceeding.")
    st.stop()

st.title(f"Column Mapping for {source_table} → {target_table} in {selected_schema}")

# Now continue with mapping logic (auto-matching, manual override, saving mappings)

# Function to get all schemas
def get_schemas():
    """Fetch all schema names from PostgreSQL."""
    query = text("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    """)
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
    return df["schema_name"].tolist()

# Update schema selection in Streamlit UI
schemas = get_schemas()  # Fetch dynamically
selected_schema = st.selectbox("Select Schema:", schemas)

# Function to get tables
def get_tables(schema_name):
    query = text(f'SELECT table_name FROM information_schema.tables WHERE table_schema = :schema_name')
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"schema_name": schema_name})
    return df["table_name"].tolist()

# Function to get column names
def get_column_names(schema_name, table_name):
    query = text(f'''
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :schema_name AND table_name = :table_name
    ''')
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"schema_name": schema_name, "table_name": table_name})
    return df["column_name"].tolist()

# Function to save column mappings
def save_column_mapping(schema_name, source_table, target_table, mapping_df):
    """Save column mappings to the database."""
    with engine.begin() as conn:
        for _, row in mapping_df.iterrows():
            query = text(f'''
                INSERT INTO table_registry.column_mapping (schema_name, source_table, target_table, source_column, target_column)
                VALUES (:schema_name, :source_table, :target_table, :source_column, :target_column)
                ON CONFLICT (schema_name, source_table, target_table, source_column)
                DO UPDATE SET target_column = EXCLUDED.target_column;
            ''')
            conn.execute(query, {
                "schema_name": schema_name,
                "source_table": source_table,
                "target_table": target_table,
                "source_column": row["source_column"],
                "target_column": row["target_column"]
            })

# Function to load saved mappings
def load_saved_mappings(schema_name, source_table, target_table):
    query = text(f'''
        SELECT source_column, target_column FROM table_registry.column_mapping
        WHERE schema_name = :schema_name AND source_table = :source_table AND target_table = :target_table
    ''')
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={
            "schema_name": schema_name,
            "source_table": source_table,
            "target_table": target_table
        })
    return df

# Streamlit UI
def main():
    st.title("Column Mapping for Merging")

    schemas = [SCHEMA_NAME]
    selected_schema = st.selectbox("Select Schema:", schemas, index=0)

    if selected_schema:
        tables = get_tables(selected_schema)

        source_table = st.selectbox("Select Source Table:", tables, index=0)
        target_table = st.selectbox("Select Target Table:", tables, index=0)

        if source_table and target_table:
            source_columns = get_column_names(selected_schema, source_table)
            target_columns = get_column_names(selected_schema, target_table)

            # Auto-match columns with same names
            auto_mapping = {col: col for col in source_columns if col in target_columns}
            saved_mappings = load_saved_mappings(selected_schema, source_table, target_table)

            # Convert saved mappings to a dictionary for quick lookup
            saved_mapping_dict = dict(zip(saved_mappings["source_column"], saved_mappings["target_column"])) if not saved_mappings.empty else {}

            # Apply saved mappings first, then auto-matching
            final_mapping = {col: saved_mapping_dict.get(col, auto_mapping.get(col, "")) for col in source_columns}

            st.subheader("Column Mapping")
            mapping_data = pd.DataFrame({"source_column": list(final_mapping.keys()), "target_column": list(final_mapping.values())})

            edited_mapping = st.data_editor(mapping_data, num_rows="dynamic")

            if st.button("Save Mapping"):
                save_column_mapping(selected_schema, source_table, target_table, edited_mapping)
                st.success("Column mapping saved successfully!")

            st.page_link("data_merger.py", label="➡ Proceed to Merging")

if __name__ == "__main__":
    main()
