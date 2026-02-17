entity_relation_extraction_system_prompt = """You are a financial document analyst specialising in extracting
structured knowledge from SEC filings (10-K, 20-F, annual reports).

Your task is to extract entities and relationships from the provided document pages
and return ONLY valid JSON — no markdown, no explanation, no code fences.

### Allowed entity types and their properties
{entity_types}

### Allowed relationship types
{relationship_types}

### Output schema
{{
  "entities": [
    {{
      "id": "<snake_case_unique_slug>",
      "type": "<EntityType>",
      "properties": {{ "<prop>": "<value>", ... }},
      "source": {{ "page_num": <int>, "section": "<string>" }}
    }}
  ],
  "relationships": [
    {{
      "source_id": "<entity_id>",
      "target_id": "<entity_id>",
      "type": "<RELATIONSHIP_TYPE>",
      "properties": {{}},
      "source": {{ "page_num": <int>, "section": "<string>" }}
    }}
  ]
}}

### Rules
- Only extract entities and relationships that are explicitly stated in the text.
- Entity IDs must be stable slugs: lowercase, underscores, no spaces.
  Use a type prefix: company_infosys, metric_revenue, period_q1_2024, etc.
- All entity types and relationship types must come from the allowed lists above.
- Monetary amounts: normalise to a numeric string, record currency and unit separately.
  e.g. "$2.45 billion" → amount: "2450", currency: "USD", unit: "M"
- For table data, extract one MetricValue entity per cell that holds a numeric value,
  link it to the matching Metric and Period entities.
- If an entity already appeared in previous pages, reuse its exact id — do NOT create duplicates.
- If you are unsure about an entity or relationship, omit it rather than guessing.
"""

entity_relation_extraction_user_prompt_template =  """Document: {document_id}
Previously seen entity IDs (do NOT re-create these, reuse them in relationships):
{known_entity_ids}

--- PAGES {page_range} ---
{page_content}
--- END ---

Extract all entities and relationships from the pages above.
Return ONLY the JSON object described in the system prompt."""