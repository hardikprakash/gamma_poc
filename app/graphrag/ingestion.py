import pymupdf.layout
import pymupdf4llm
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class PDFIngestor:
    """
    Simple PDF Ingestion Module using pymupdf4llm
    
    Extracts content as LLM-optimized markdown with proper table parsing.
    Returns structured data with pages and blocks.
    """
    
    def __init__(self, pdf_path: str, document_id: str, document_name: str, document_type: str = None) -> None:
        self.pdf_path = pdf_path
        self.document_id = document_id
        self.document_name = document_name
        self.document_type = document_type
        self.processed_page_chunks = []
    
    def process_page_chunk(self, page_chunk: Dict) -> Dict:
        """Process a single page chunk and extract blocks"""
        page_blocks = page_chunk["page_boxes"]
        page = {"page_number": None, "blocks": []}

        page_number = page_chunk["metadata"]["page_number"]
        page["page_number"] = page_number

        for page_block in page_blocks:
            block = {}
            
            block_content_start = page_block["pos"][0]
            block_content_end = page_block["pos"][1]

            block_type = page_block["class"]
            block_order = page_block["index"]
            
            block_content = page_chunk["text"][block_content_start:block_content_end]

            block.update({
                "block_type": block_type,
                "block_order": block_order,
                "content": block_content
            })

            page["blocks"].append(block)

        return page

    def process_document(self) -> Dict:
        """Process entire document and return LLM-optimized markdown"""
        
        logger.info(f"Processing document: {self.document_name}")
        
        # Extract page chunks with metadata
        page_chunks = pymupdf4llm.to_markdown(self.pdf_path, page_chunks=True)
        
        logger.info(f"Total pages: {page_chunks[0]['metadata']['page_count']}")
        
        # Extract document metadata from first page chunk
        document_metadata = {
            "name": self.document_name,
            "id": self.document_id,
            "metadata": {
                "page_count": page_chunks[0]["metadata"]["page_count"],
                "format": page_chunks[0]["metadata"].get("format", ""),
                "file_path": self.pdf_path
            }
        }

        # Process each page chunk
        for idx, page_chunk in enumerate(page_chunks):
            logger.debug(f"Processing page {idx + 1}/{len(page_chunks)}")
            processed_page_chunk = self.process_page_chunk(page_chunk)
            self.processed_page_chunks.append(processed_page_chunk)
        
        logger.info(f"Document processing complete! Processed {len(page_chunks)} pages")
        
        # Build final document structure
        processed_document = {
            **document_metadata,
            "pages": self.processed_page_chunks
        }
        
        return processed_document
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # No cleanup needed since we're not opening file handles
