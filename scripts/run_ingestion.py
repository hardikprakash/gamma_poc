import sys
import os

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from app.graphrag.ingestion import PDFIngestor

import json

# Test with a sample PDF
pdf_path = "./data/infosys_sample.pdf"  # Replace with your PDF path

with PDFIngestor(pdf_path, document_id="test_doc") as ingestor:
    # Process document
    parsed_pages = ingestor.process_document()
    
    # Save to JSON
    output = {
        'document_id': ingestor.document_id,
        'total_pages': len(parsed_pages),
        'pages': parsed_pages
    }
    
    with open('./output/parsing/parsed_output.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Output saved to parsed_output.json")
    
    # Print summary
    print(f"\n{'='*60}")
    print("PARSING SUMMARY")
    print(f"{'='*60}")
    print(f"Document: {ingestor.document_id}")
    print(f"Total pages: {len(parsed_pages)}")
    
    for page in parsed_pages:
        page_num = page['page_num'] + 1
        markdown_size = len(page['markdown'])
        print(f"\nPage {page_num}: {markdown_size} characters")

