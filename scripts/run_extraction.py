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

parsed_json_path = "./output/parsing/parsed_infosys_form20f-2025_sample.json"
file_name = os.path.splitext(os.path.basename(parsed_json_path))[0]  # Extract file name without extension

# Remove 'parsed_' prefix if it exists to get the base name
if file_name.startswith('parsed_'):
    base_name = file_name[7:]  # Remove 'parsed_' prefix
else:
    base_name = file_name

with EntityRelationExtractor(parsed_json_path=parsed_json_path, filing_year="2026") as extractor:
    result = extractor.process_document()
    output_path = f"./output/extraction/extracted_{base_name}.json"
    with open(output_path, "w") as file:
        json.dump(result.to_dict(), file, indent=2, ensure_ascii=False)