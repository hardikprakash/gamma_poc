import sys
import os
import json

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from app.graphrag.entity_relation_extraction import EntityRelationExtractor

with EntityRelationExtractor(parsed_json_path="./output/parsing/parsed_output.json") as extractor:
    result = extractor.process_document()
    with open(r"./output/extraction/extracted_output.json", "w") as file:
        json.dump(result.to_dict(), file, indent=2, ensure_ascii=False)