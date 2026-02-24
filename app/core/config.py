import os
from dotenv import load_dotenv


class Settings:
    """
    Application configuration loaded from environment variables.
    
    Pull values from .env file or set as environment variables.
    """
    
    def __init__(self) -> None:
        load_dotenv()

        self.PAGEINDEX_API_KEY = os.getenv("PAGEINDEX_API_KEY")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        self.OPENAI_API_BASE_URL = os.getenv("OPENAI_API_BASE_URL")
        self.MODEL_NAME = os.getenv("MODEL_NAME")

        # PDF Configuration
        self.PDF_INPUT_DIR = os.getenv("PDF_INPUT_DIR", "./data")
        self.PDF_OUTPUT_DIR = os.getenv("PDF_OUTPUT_DIR", "./output")

        # Neo4j
        self.NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
        self.NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "testpassword")

        # Embeddings (Ollama)
        self.EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", "http://localhost:11434")
        self.EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "embeddinggemma")
        self.EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "768"))
        self.EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))


settings = Settings()