import os
import pandas as pd
from dotenv import load_dotenv
from typing import List
from langchain_core.documents import Document
from langchain_astradb import AstraDBVectorStore
from prod_assistant.utils.model_loader import ModelLoader
from prod_assistant.utils.config_loader import load_config

class DataIngestion:
    """
    Class to handle data transformation and ingestion into AstraDB vector store.
    """

    def __init__(self):
        """
        Initialize environment variables, embedding model, and set CSV file path.
        """
        print("Initializing DataIngestion pipeline...")
        
        # 1. LOAD ENV VARS FIRST (Critical Fix)
        self._load_env_variables()
        
        # 2. THEN Load Models (Now they can see the GOOGLE_API_KEY)
        self.model_loader = ModelLoader()
        
        self.csv_path = self._get_csv_path()
        self.product_data = self._load_csv()
        self.config = load_config()

    def _load_env_variables(self):
        """
        Load and validate required environment variables.
        """
        # Explicitly reload to be safe
        load_dotenv(override=True)
        
        # We only check for critical DB keys here. 
        # ModelLoader checks for GOOGLE_API_KEY separately.
        required_vars = ["ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_KEYSPACE"]
        
        missing_vars = [var for var in required_vars if os.getenv(var) is None]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables: {missing_vars}")
        
        self.db_api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        self.db_application_token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        self.db_keyspace = os.getenv("ASTRA_DB_KEYSPACE")

    def _get_csv_path(self):
        """
        Get path to the CSV file located inside 'data' folder.
        """
        current_dir = os.getcwd()
        csv_path = os.path.join(current_dir, 'data', 'product_reviews.csv')

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}. Please run the converter script first.")

        return csv_path

    def _load_csv(self):
        """
        Load product data from CSV.
        """
        df = pd.read_csv(self.csv_path)
        # Relaxed column check to match what our Amazon Converter outputs
        expected_columns = {'product_id', 'product_title', 'top_reviews'}

        if not expected_columns.issubset(set(df.columns)):
            raise ValueError(f"CSV must contain at least: {expected_columns}")

        return df

    def transform_data(self):
        """
        Transform product data into list of LangChain Document objects.
        """
        documents = []
        for _, row in self.product_data.iterrows():
            # Helper to safely get metadata values and handle NaNs (prevent JSON errors)
            def get_safe_value(val, default):
                return default if pd.isna(val) else val

            rating = get_safe_value(row.get("rating"), 0.0)
            total_reviews = get_safe_value(row.get("total_reviews"), 0)
            price = get_safe_value(row.get("price"), "N/A")
            
            metadata = {
                "product_id": str(row["product_id"]),
                "product_title": str(row.get("product_title", "Unknown")),
                "rating": rating,
                "total_reviews": total_reviews,
                "price": str(price)
            }
            
            # The 'page_content' is what gets embedded. We use the reviews.
            content = get_safe_value(row.get("top_reviews"), "")
            
            doc = Document(page_content=str(content), metadata=metadata)
            documents.append(doc)

        print(f"Transformed {len(documents)} documents.")
        return documents

    def store_in_vector_db(self, documents: List[Document]):
        """
        Store documents into AstraDB vector store.
        """
        # Ensure we have a collection name, default to 'product_reviews' if config is missing
        collection_name = self.config.get("astra_db", {}).get("collection_name", "product_reviews")
        
        print(f"Connecting to AstraDB Collection: {collection_name}")
        vstore = AstraDBVectorStore(
            embedding=self.model_loader.load_embeddings(),
            collection_name=collection_name,
            api_endpoint=self.db_api_endpoint,
            token=self.db_application_token,
            namespace=self.db_keyspace,
        )

        inserted_ids = vstore.add_documents(documents)
        print(f"Successfully inserted {len(inserted_ids)} documents into AstraDB.")
        return vstore, inserted_ids

    def run_pipeline(self):
        """
        Run the full data ingestion pipeline.
        """
        documents = self.transform_data()
        if documents:
            vstore, _ = self.store_in_vector_db(documents)
            print("Pipeline finished successfully.")
        else:
            print("No documents to ingest.")

if __name__ == "__main__":
    ingestion = DataIngestion()
    ingestion.run_pipeline()