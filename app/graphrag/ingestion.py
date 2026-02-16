import fitz, pdfplumber
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict

@dataclass
class Region:
    """Represents a region on a PDF page."""
    type: str
    bbox: tuple[float, float, float, float]
    order: int
    content: Optional[any] = None
    metadata: Optional[Dict] = None

class PDFIngestor():
    """
    PDF Ingestion and Parsing Module
    Detects regions (text, tables, images) and extracts structured content
    """
    def __init__(self, pdf_path: str) -> None:
        self.pymupdf_doc = fitz.open(pdf_path)
        self.pdfplumber_doc = pdfplumber.open(pdf_path)
        self.parsed_pages = []

    def process_document(self):
        for page_num in range(len(self.pymupdf_doc)):
            parsed_page = self.process_page(page_num)
            self.parsed_pages.append(parsed_page)
    
    def process_page(self, page_num: int):
        # Text and Images
        blocks = self.pymupdf_doc[page_num].get_text("dict")

        for block in blocks:
            
            if block['type'] == 0: # Text
                pass
    
            if block['type'] == 1: # Image
                pass