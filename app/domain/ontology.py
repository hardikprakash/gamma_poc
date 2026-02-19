ENTITY_TYPES = {
    "Company": ["name", "ticker", "sector", "country", "exchange", "fiscal_year_end"],
    "Segment": ["name", "description", "status", "start_date", "end_date"],
    "Metric": ["name", "unit", "category"],          # e.g. Revenue, Net Income
    "MetricValue": ["amount", "currency", "unit"],   # e.g. 2450, USD, M
    "Period": ["name", "start_date", "end_date", "fiscal_year", "quarter"],
    "Risk": ["name", "category", "severity", "description"],
    "Policy": ["name", "description", "effective_date"],
    "Person": ["name", "role", "title"],
    "Geography": ["name", "region", "country"],
    "Product": ["name", "category", "description"],
    "Event": ["name", "date", "type", "impact"],
    "LegalEntity": ["name", "jurisdiction", "type"],
}

RELATIONSHIP_TYPES = [
    "HAS_SEGMENT",       # Company → Segment
    "REPORTS",           # Company → Metric
    "HAS_VALUE",         # Metric → MetricValue
    "IN_PERIOD",         # MetricValue → Period
    "COMPARED_TO",       # MetricValue → MetricValue
    "INCREASED_BY",      # Metric → MetricValue (delta)
    "DECREASED_BY",      # Metric → MetricValue (delta)
    "RELATED_TO",        # Generic
    "IMPACTS",           # Risk → Segment | Metric
    "MITIGATES",         # Policy → Risk
    "OPERATES_IN",       # Company → Geography
    "EMPLOYED_BY",       # Person → Company
    "OVERSEES",          # Person → Segment
    "HAS_RISK",          # Company → Risk
    "HAS_POLICY",        # Company → Policy
    "SUBSIDIARY_OF",     # Company → Company
    "DISCLOSED_IN",      # any → Section (source tracing)
    "MERGED_INTO",       # Segment → Segment  (segment A folded into segment B)
    "DIVESTED",          # Company → LegalEntity  (subsidiary sold off)
    "ACQUIRED",          # Company → LegalEntity  (bought something new)
    "SUCCEEDED_BY",      # Segment → Segment  (renamed or restructured)
    "COMPONENT_OF",      # Metric → Metric (e.g., LongTermDebt → TotalDebt)
    "ROLL_UP_TO",        # MetricValue → MetricValue (aggregation)
]
