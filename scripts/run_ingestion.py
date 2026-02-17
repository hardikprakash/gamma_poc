import sys
import os
import logging

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# Configure logging to see progress
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from app.graphrag.ingestion import PDFIngestor
import json

# Test with a sample PDF
pdf_path = "./data/infosys_sample.pdf"

with PDFIngestor(
    pdf_path=pdf_path, 
    document_id="test_doc", 
    document_name="infosys_sample.pdf", 
    document_type="sampled_filing"
) as ingestor:
    # Process document - returns complete document structure
    document = ingestor.process_document()
    
    # Save to JSON
    with open('./output/parsing/parsed_output.json', 'w', encoding='utf-8') as f:
        json.dump(document, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Output saved to ./output/parsing/parsed_output.json")
    
    # Print summary
    print(f"\n{'='*60}")
    print("PARSING SUMMARY")
    print(f"{'='*60}")
    print(f"Document: {document['name']}")
    print(f"Document ID: {document['id']}")
    print(f"Total pages: {document['metadata']['page_count']}")
    
    for page in document['pages']:
        page_num = page['page_number']
        block_count = len(page['blocks'])
        print(f"\nPage {page_num}: {block_count} blocks")
        for block in page['blocks'][:3]:  # Show first 3 blocks
            content_preview = block['content'][:50].replace('\n', ' ')
            print(f"  - {block['block_type']}: {content_preview}...")
