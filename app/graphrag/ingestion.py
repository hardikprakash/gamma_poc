import pymupdf4llm
import fitz
from typing import List, Dict


class PDFIngestor:
    """
    Simple PDF Ingestion Module using pymupdf4llm
    
    Extracts content as LLM-optimized markdown with proper table parsing.
    """
    
    def __init__(self, pdf_path: str, document_id: str = None) -> None:
        self.pdf_path = pdf_path
        self.document_id = document_id or pdf_path.split('/')[-1].replace('.pdf', '')
        self.doc = fitz.open(pdf_path)
        self.parsed_pages = []
    
    def process_document(self) -> List[Dict]:
        """Process entire document and return LLM-optimized markdown for each page"""
        print(f"Processing document: {self.document_id}")
        print(f"Total pages: {len(self.doc)}\n")
        
        for page_num in range(len(self.doc)):
            print(f"Processing page {page_num + 1}/{len(self.doc)}...", end="\r")
            parsed_page = self.process_page(page_num)
            self.parsed_pages.append(parsed_page)
        
        print(f"Processing page {len(self.doc)}/{len(self.doc)}... ✓")
        print(f"\n✓ Document processing complete!")
        
        return self.parsed_pages
    
    def process_page(self, page_num: int) -> Dict:
        """
        Extract LLM-optimized markdown content from a single page.
        
        Returns dict with:
        - page_num: Page number (0-indexed)
        - markdown: LLM-optimized markdown with proper table parsing
        """
        # Extract markdown for this specific page using pymupdf4llm
        markdown = pymupdf4llm.to_markdown(self.pdf_path, pages=range(page_num, page_num + 1))
        
        return {
            'document_id': self.document_id,
            'page_num': page_num,
            'markdown': markdown
        }
    
    def get_parsed_pages(self) -> List[Dict]:
        """Return all parsed pages"""
        return self.parsed_pages
    
    def close(self):
        """Clean up resources"""
        self.doc.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
