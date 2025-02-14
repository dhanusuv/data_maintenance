import streamlit as st
import pandas as pd
import configparser

import toml
from sqlalchemy import create_engine, text


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

# Function to insert form data into DB
def insert_data(form_data):
    query = text(f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} 
        (dataset_type, publisher_type, publisher_name, table_category_name, lob, update_frequency, 
         source_file_link, is_update_available, table_name, present_status, process_guideline_doc, 
         usage_rule_count, update_priority, version_id, version_effective_date, monitoring_start_date, 
         modified_date, created_date, table_driven_savings, column_count, row_count, created_by, 
         modified_by, notified_to_product_team, uploaded_file, data_processing_guideline, 
         domain_process_doc, table_owner)
        VALUES (:dataset_type, :publisher_type, :publisher_name, :table_category_name, :lob, :update_frequency,
                :source_file_link, :is_update_available, :table_name, :present_status, :process_guideline_doc,
                :usage_rule_count, :update_priority, :version_id, :version_effective_date, :monitoring_start_date,
                :modified_date, :created_date, :table_driven_savings, :column_count, :row_count, :created_by,
                :modified_by, :notified_to_product_team, :uploaded_file, :data_processing_guideline,
                :domain_process_doc, :table_owner)
    """)
    with engine.begin() as conn:
        conn.execute(query, form_data)

# Function to fetch all records
def fetch_data():
    query = f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}"
    return pd.read_sql(query, engine)

# Home Page
st.title("Dataset Metadata Submission")

# Form Input
with st.form("metadata_form"):
    st.subheader("Dataset Details")

    form_data = {
        "dataset_type": st.text_input("Dataset Type"),
        "publisher_type": st.text_input("Publisher Type"),
        "publisher_name": st.text_input("Publisher Name"),
        "table_category_name": st.text_input("Table Category Name"),
        "lob": st.text_input("Line of Business (LOB)"),
        "update_frequency": st.selectbox("Update Frequency", ["Monthly", "Quarterly", "Yearly"]),
        "source_file_link": st.text_input("Source File Link"),
        "is_update_available": st.radio("Is update available for 2023 Q4?", ["Yes", "No"]) == "Yes",
        "table_name": st.text_input("Table Name (CREOL Link)"),
        "present_status": st.selectbox("Present Status", ["Active", "Inactive", "Under Review"]),
        "process_guideline_doc": st.text_input("Process Guideline Document"),
        "usage_rule_count": st.number_input("Usage Rule Count", min_value=0, step=1),
        "update_priority": st.selectbox("Update Priority", ["High", "Medium", "Low"]),
        "version_id": st.text_input("Version ID"),
        "version_effective_date": st.date_input("Version Effective Date"),
        "monitoring_start_date": st.date_input("Monitoring Start Date"),
        "modified_date": st.date_input("Modified Date"),
        "created_date": st.date_input("Created Date"),
        "table_driven_savings": st.number_input("Table-Driven Savings", min_value=0, step=1),
        "column_count": st.number_input("Column Count", min_value=0, step=1),
        "row_count": st.number_input("Row Count", min_value=0, step=1),
        "created_by": st.text_input("Created By"),
        "modified_by": st.text_input("Modified By"),
        "notified_to_product_team": st.radio("Notified to Product Team?", ["Yes", "No"]) == "Yes",
        "uploaded_file": st.text_input("Uploaded File"),
        "data_processing_guideline": st.text_area("Data Processing Guideline"),
        "domain_process_doc": st.text_input("Domain Process Document"),
        "table_owner": st.text_input("Table Owner"),
    }

    submitted = st.form_submit_button("Submit")
    if submitted:
        insert_data(form_data)
        st.success("Form submitted successfully! Refresh to see updated data.")

# Display stored data
st.subheader("Submitted Data")
df = fetch_data()

# Add Edit/Delete/View Buttons
for index, row in df.iterrows():
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.write(row["table_name"])
    with col2:
        if st.button("Edit", key=f"edit_{index}"):
            st.write("Edit feature not implemented yet")  # TODO: Implement Edit Feature
    with col3:
        if st.button("Delete", key=f"delete_{index}"):
            st.write("Delete feature not implemented yet")  # TODO: Implement Delete Feature
    with col4:
        if st.button("View", key=f"view_{index}"):
            st.write("View feature not implemented yet")  # TODO: Implement View Feature
    with col5:
        st.page_link("pages/data_viewer.py", label="View Table Data")

