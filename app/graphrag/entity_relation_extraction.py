import json
from dataclasses import dataclass, field
import logging
from typing import Optional

from app.core.openrouter import client
from app.core.prompts import entity_relation_extraction_system_prompt as SYSTEM_PROMPT_TEMPLATE, entity_relation_extraction_user_prompt_template as USER_PROMPT_TEMPLATE
from app.domain.ontology import ENTITY_TYPES, RELATIONSHIP_TYPES
from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class Entity:
    id: str                        # stable slug, e.g. "company_infosys"
    type: str                      # must be in ENTITY_TYPES
    properties: dict = field(default_factory=dict)
    source: dict = field(default_factory=dict)      # {document_id, page_num, section}


@dataclass
class Relationship:
    source_id: str                 # Entity.id
    target_id: str                 # Entity.id
    type: str                      # must be in RELATIONSHIP_TYPES
    properties: dict = field(default_factory=dict)
    source: dict = field(default_factory=dict)


@dataclass
class ExtractionResult:
    document_id: str
    entities: list[Entity] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)  # {pages, message}

    def to_dict(self) -> dict:
        return {
            "document_id": self.document_id,
            "entities": [
                {**e.__dict__} for e in self.entities
            ],
            "relationships": [
                {**r.__dict__} for r in self.relationships
            ],
            "errors": self.errors,
        }


class EntityRelationExtractor:
    def __init__(self, parsed_json_path: str, filing_year: str, window_size: int = 3, step_size: int = 2):
        """
        Args:
            parsed_json_path:    Path to the parsed JSON produced by the ingestion module.
            window_size: Number of pages per LLM call.
            step_size:   Slide increment. window_size=3, step_size=2 gives
                         [0-2], [2-4], [4-6] ... (1-page overlap between windows).
        """
        
        self.window_size = window_size
        self.step_size = step_size

        with open(parsed_json_path, "r") as f:
            self.document_json = json.load(f)

        self.document_id: str = self.document_json["id"]
        self.pages: list[dict] = self.document_json.get("pages", [])
        self.filing_year = filing_year

        # Format the system prompt once with the ontology
        entity_types_str = json.dumps(ENTITY_TYPES, indent=2)
        relationship_types_str = json.dumps(RELATIONSHIP_TYPES, indent=2)
        self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            entity_types=entity_types_str,
            relationship_types=relationship_types_str,
        )

    def process_document(self) -> ExtractionResult:
        """
        Slide a window over all pages, call the LLM for each window,
        and accumulate a single de-duplicated ExtractionResult.
        """
        result = ExtractionResult(document_id=self.document_id)
        total_pages = len(self.pages)
        windows = self._build_windows(total_pages)

        logger.info(
            "Processing '%s': %d pages, %d windows (size=%d, step=%d)",
            self.document_id, total_pages, len(
                windows), self.window_size, self.step_size,
        )

        for start, end in windows:
            window_pages = self.pages[start:end]
            
            page_nums = [page["page_number"] for page in window_pages]
            page_range = f"{page_nums[0]}-{page_nums[-1]}"

            logger.info("Window pages %s ...", page_range)

            raw_response = self._call_llm(
                window_pages,
                known_entity_ids=[e.id for e in result.entities],
                page_range=page_range,
            )
            if raw_response is None:
                result.errors.append(
                    {"pages": page_range, "message": "LLM call failed"})
                continue

            entities, relationships, err = self._parse_llm_response(
                raw_response)
            if err:
                result.errors.append({"pages": page_range, "message": err})
                continue

            result.entities = self._merge_entities(result.entities, entities)
            result.relationships.extend(relationships)

            logger.info(
                "    +%d entities, +%d relationships (totals: %d, %d)",
                len(entities), len(relationships),
                len(result.entities), len(result.relationships),
            )

        result.relationships = self._deduplicate_relationships(
            result.relationships)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_windows(self, total: int) -> list[tuple[int, int]]:
        windows = []
        start = 0
        while start < total:
            end = min(start + self.window_size, total)
            windows.append((start, end))
            if end == total:
                break
            start += self.step_size
        return windows

    def _call_llm(self, pages: list[dict], known_entity_ids: list[str], page_range: str) -> Optional[str]:
        page_content = self._format_page_content(pages)
        
        user_message = USER_PROMPT_TEMPLATE.format(
            document_id=self.document_id,
            known_entity_ids=json.dumps(known_entity_ids),
            page_range=page_range,
            page_content=page_content,
            filing_year=self.filing_year
        
        )
        try:
            response = client.chat.completions.create(
                model=settings.MODEL_NAME, # type: ignore
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user",   "content": user_message},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            return response.choices[0].message.content
        except Exception as exc:
            logger.error("LLM call failed for pages %s: %s", page_range, exc)
            return None

    def _parse_llm_response(self, raw: str) -> tuple[list[Entity], list[Relationship], Optional[str]]:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            return [], [], f"JSON decode error: {exc} — raw snippet: {raw[:200]}"

        entities: list[Entity] = []
        for e in data.get("entities", []):
            etype = e.get("type", "")
            if etype not in ENTITY_TYPES:
                logger.warning(
                    "Skipping unknown entity type '%s' (id=%s)", etype, e.get("id"))
                continue
            source = {**e.get("source", {}), "document_id": self.document_id}
            entities.append(Entity(
                id=e["id"],
                type=etype,
                properties=e.get("properties", {}),
                source=source,
            ))

        relationships: list[Relationship] = []
        for r in data.get("relationships", []):
            rtype = r.get("type", "")
            if rtype not in RELATIONSHIP_TYPES:
                logger.warning(
                    "Skipping unknown relationship type '%s'", rtype)
                continue
            source = {**r.get("source", {}), "document_id": self.document_id}
            relationships.append(Relationship(
                source_id=r["source_id"],
                target_id=r["target_id"],
                type=rtype,
                properties=r.get("properties", {}),
                source=source,
            ))

        return entities, relationships, None

    @staticmethod
    def _merge_entities(existing: list[Entity], incoming: list[Entity]) -> list[Entity]:
        """Merge by id — incoming properties win on conflict, no duplicates created."""
        index = {e.id: e for e in existing}
        for new in incoming:
            if new.id in index:
                index[new.id].properties.update(new.properties)
            else:
                index[new.id] = new
        return list(index.values())

    @staticmethod
    def _deduplicate_relationships(rels: list[Relationship]) -> list[Relationship]:
        """Remove exact duplicate triples that arise from window overlaps."""
        seen: set[tuple] = set()
        unique: list[Relationship] = []
        for r in rels:
            key = (r.source_id,
                   r.type,
                   r.target_id,
                   r.properties.get("filing_year"),
                )
            if key not in seen:
                seen.add(key)
                unique.append(r)
        return unique

    @staticmethod
    def _format_page_content(pages: list[dict]) -> str:
        page_blocks = []
        for page in pages:
            # Concatenate all block contents from the page
            block_contents = []
            for block in page.get("blocks", []):
                content = block.get("content", "").strip()
                if content:
                    block_contents.append(content)
            
            if block_contents:
                page_text = "\n".join(block_contents)
                page_blocks.append(f"[Page {page['page_number']}]\n{page_text}")
        
        return "\n\n".join(page_blocks) if page_blocks else "(no extractable text)"

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
