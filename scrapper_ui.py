import streamlit as st
import pandas as pd
import os
from prod_assistant.etl.data_ingestion import DataIngestion

# Configuration
DATA_FILE = "data/product_reviews.csv"

st.set_page_config(page_title="Data Manager", page_icon="⚙️")
st.title("⚙️ Knowledge Base Admin")
st.markdown("Use this dashboard to inspect your Amazon data and load it into the AI memory.")

# --- Status Check ---
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE)
    row_count = len(df)
    st.success(f"✅ Data File Found: {row_count} Products loaded.")
    
    with st.expander("📊 Preview Data"):
        st.dataframe(df.head(10))
        
    st.divider()
    
    st.subheader("🚀 Ingest to Vector Database")
    st.markdown(
        """
        This will generate embeddings and store them in AstraDB.
        **Note:** Depending on the data size, this may take 1-2 minutes.
        """
    )
    
    if st.button("Start Ingestion"):
        with st.spinner("Processing... Do not close this tab."):
            try:
                ingestion = DataIngestion()
                ingestion.run_pipeline()
                st.balloons()
                st.success("✅ Ingestion Complete! Your AI is now ready to chat.")
            except Exception as e:
                st.error("❌ Ingestion Failed")
                st.exception(e)

else:
    st.warning("⚠️ 'data/product_reviews.csv' not found.")
    st.info(
        """
        **How to fix:**
        Run the streaming converter script in your terminal to fetch data from Hugging Face:
        `uv run python prod_assistant/etl/convert_amazon_data.py`
        """
    )