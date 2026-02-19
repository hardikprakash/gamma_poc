import sys
import os
import json
import logging


# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from app.graphrag.entity_relation_extraction import EntityRelationExtractor

with EntityRelationExtractor(parsed_json_path="./output/parsing/parsed_output.json", filing_year="2026") as extractor:
    result = extractor.process_document()
    with open(r"./output/extraction/extracted_output.json", "w") as file:
        json.dump(result.to_dict(), file, indent=2, ensure_ascii=False)