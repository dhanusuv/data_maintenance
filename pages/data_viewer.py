import streamlit as st
import pandas as pd
import configparser
from sqlalchemy import create_engine, text

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

# Function to get column names for selected table
def get_column_names(schema_name, table_name):
    query = text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :schema_name AND table_name = :table_name
    """)
    df = pd.read_sql(query, engine, params={"schema_name": schema_name, "table_name": table_name})
    return [f'"{col}"' for col in df["column_name"].tolist()]  # Ensure double quotes for case sensitivity

# Function to fetch data from the selected table
def fetch_data(schema_name, table_name):
    columns = get_column_names(schema_name, table_name)
    column_str = ", ".join(columns)  # Join column names with quotes
    query = text(f'SELECT {column_str} FROM "{schema_name}"."{table_name}" LIMIT 100')
    df = pd.read_sql(query, engine)
    return df

# Function to generate summary for target tables
def generate_summary(schema_name, table_name):
    columns = get_column_names(schema_name, table_name)

    # Check if required columns exist
    required_cols = ['"STATUS"', '"EFFECTIVE_START_DATE"', '"EFFECTIVE_END_DATE"', '"VERSION_ID"']
    missing_cols = [col for col in required_cols if col not in columns]

    if missing_cols:
        return pd.DataFrame({"Error": [f"Missing required columns: {', '.join(missing_cols)}"]})

    query = text(f"""
        SELECT
            "STATUS",
            "EFFECTIVE_START_DATE",
            "EFFECTIVE_END_DATE",
            "VERSION_ID",
            COUNT(*) AS count_of_records
        FROM "{schema_name}"."{table_name}"
        GROUP BY "STATUS", "EFFECTIVE_START_DATE", "EFFECTIVE_END_DATE", "VERSION_ID"
        ORDER BY "VERSION_ID"
    """)
    df = pd.read_sql(query, engine)
    return df

# Streamlit App
def main():
    st.title("PostgreSQL Data Viewer")

    # Sidebar - Schema selection
    st.sidebar.header("Schema & Table Selection")

    schemas = get_schemas()
    selected_schema = st.sidebar.selectbox("Select Schema:", schemas, index=0)

    if selected_schema:
        tables = get_tables(selected_schema)
        selected_table = st.sidebar.selectbox("Select Table:", tables, index=0)

        # Fetch and display table data
        if st.sidebar.button("Fetch Data"):
            try:
                df = fetch_data(selected_schema, selected_table)
                st.write(f"Displaying data from `{selected_schema}.{selected_table}`:")
                st.dataframe(df)  # Display table data
            except Exception as e:
                st.error(f"Error fetching data: {e}")

        # Enable "Generate Summary" for tables containing "target"
        if "target" in selected_table.lower():
            st.subheader("Summary Report for Target Table")
            if st.button("Generate Summary"):
                try:
                    summary_df = generate_summary(selected_schema, selected_table)
                    st.dataframe(summary_df)
                except Exception as e:
                    st.error(f"Error generating summary: {e}")

    # Navigation Button
    st.page_link("home.py", label="⬅ Go Back to Home Page")

if __name__ == "__main__":
    main()
