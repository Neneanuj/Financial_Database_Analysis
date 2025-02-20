import streamlit as st
import requests
import pandas as pd
from snowflake.connector import connect


# Page configuration
st.set_page_config(layout="wide", page_title="SEC Financial Data Explorer")

# Initialize session state
if "query_response" not in st.session_state:
    st.session_state["query_response"] = {}

# Snowflake connection configuration
def get_snowflake_connection():
    return connect(
        user='your_username',
        password='your_password',
        account='your_account',
        warehouse='your_warehouse',
        database='your_database',
        schema='your_schema'
    )

# Helper function for data retrieval
def fetch_data(pipeline, period, company=None, statement_type=None):
    try:
        conn = get_snowflake_connection()
        
        if pipeline == "Raw Pipeline":
            query = """
            SELECT sub.cik, sub.name, sub.form, sub.period, 
                   num.tag, num.value, num.uom
            FROM sub 
            JOIN num ON sub.adsh = num.adsh
            WHERE sub.period = %s
            """
            if company:
                query += f" AND LOWER(sub.name) LIKE LOWER('%{company}%')"
                
        elif pipeline == "JSON Pipeline":
            query = """
            SELECT company_name, cik, filing_date, 
                   financial_data, period
            FROM financial_statements_json
            WHERE period = %s
            """
            if company:
                query += f" AND LOWER(company_name) LIKE LOWER('%{company}%')"
                
        else:  # Normalized Pipeline
            table_mapping = {
                "Balance Sheet": "balance_sheet_fact",
                "Income Statement": "income_statement_fact",
                "Cash Flow": "cash_flow_fact"
            }
            table_name = table_mapping[statement_type]
            
            query = f"""
            SELECT company_name, cik, filing_date, 
                   metric_name, value, period
            FROM {table_name}
            WHERE period = %s
            """
            if company:
                query += f" AND LOWER(company_name) LIKE LOWER('%{company}%')"
        
        df = pd.read_sql(query, conn, params=[period])
        conn.close()
        return {"status": "success", "data": df}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Sidebar navigation
with st.sidebar:
    st.title("SEC Financial Data Explorer")
    pipeline = st.radio("Select Pipeline:", 
                       ["Raw Pipeline", 
                        "JSON Pipeline", 
                        "Normalized Pipeline"])
    
    period = st.text_input("Enter Period (YYYYQ1-4):",
                          placeholder="Example: 2023Q4")
    
    if pipeline == "Normalized Pipeline":
        statement_type = st.selectbox("Select Statement Type:",
                                    ["Balance Sheet",
                                     "Income Statement",
                                     "Cash Flow"])
    else:
        statement_type = None

# Main content area
st.title("Financial Data Analysis")

# Company search
company_search = st.text_input("Search Company:", 
                              placeholder="Enter company name")

if period:
    if st.button("Fetch Data", use_container_width=True):
        with st.spinner("Fetching data..."):
            response = fetch_data(pipeline, period, company_search, statement_type)
            st.session_state.query_response = response

    # Display results
    if "status" in st.session_state.query_response:
        if st.session_state.query_response["status"] == "success":
            df = st.session_state.query_response["data"]
            
            if df.empty:
                st.warning("No data found for the selected criteria.")
            else:
                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Companies", df['cik'].nunique())
                with col2:
                    st.metric("Period", period)
                with col3:
                    st.metric("Pipeline", pipeline)
                
                # Visualization for Normalized Pipeline
                if pipeline == "Normalized Pipeline":
                    st.subheader(f"{statement_type} Analysis")
                    fig = px.bar(
                        df.groupby('company_name')['value'].sum().nlargest(10).reset_index(),
                        x='company_name',
                        y='value',
                        title=f"Top 10 Companies by {statement_type} Value"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                st.subheader("Financial Data")
                st.dataframe(df, use_container_width=True)
                
                # Export option
                if st.download_button(
                    "Download Data as CSV",
                    df.to_csv(index=False).encode('utf-8'),
                    f"financial_data_{period}.csv",
                    "text/csv",
                    use_container_width=True
                ):
                    st.success("Download started!")
        else:
            st.error(f"Error: {st.session_state.query_response.get('message', 'Unknown error')}")

    # Clear results button
    if st.session_state.get("query_response") and st.button("Clear Results", use_container_width=True):
        st.session_state.query_response = {}
        st.experimental_rerun()
else:
    st.info("Please enter a period to view data.")

# Footer
st.markdown("---")
st.markdown("SEC Financial Data Explorer - v1.0")
