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


settings = Settings()