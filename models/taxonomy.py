"""
Fixed taxonomies for semantic categories and canonical financial metrics.
All modules reference these constants — never define them inline.
"""

# ── Semantic Category Taxonomy ──────────────────────────────────────────────
SEMANTIC_CATEGORIES = [
    "financial_results",
    "financial_statements",
    "risk_factors",
    "segment_data",
    "governance",
    "business_overview",
    "legal",
    "market_data",
    "other",
]

SEMANTIC_CATEGORY_DESCRIPTIONS = {
    "financial_results": "MD&A, Operating Results, Revenue Discussion, Management Discussion",
    "financial_statements": "Income Statement, Balance Sheet, Cash Flow Statement, Notes to Financial Statements",
    "risk_factors": "Risk Factors, Key Risks, Principal Risks",
    "segment_data": "Segment Results, Business Segments, Geographic Breakdown",
    "governance": "Executive Compensation, Directors, Board, Corporate Governance",
    "business_overview": "Business Description, Company Overview, Strategy, Products & Services",
    "legal": "Legal Proceedings, Litigation, Regulatory Matters, Contingencies",
    "market_data": "Share Price, Market Information, Dividends, Listing Details",
    "other": "Exhibits, Index, Cover Page, Signatures, anything unclassifiable",
}

# ── Canonical Metric Taxonomy ───────────────────────────────────────────────
# key = canonical name, value = list of known aliases (lowercased)
CANONICAL_METRICS: dict[str, list[str]] = {
    "revenue": ["total revenues", "net revenues", "net sales", "total sales", "turnover", "total net revenues", "revenue", "revenues"],
    "gross_profit": ["gross profit", "gross income", "gross margin dollars"],
    "operating_income": ["operating income", "operating profit", "ebit", "income from operations", "operating earnings"],
    "net_income": ["net income", "profit for the year", "net profit", "net earnings", "profit attributable to shareholders"],
    "ebitda": ["ebitda", "adjusted ebitda", "earnings before interest tax depreciation amortization"],
    "eps_diluted": ["diluted eps", "diluted earnings per share", "eps (diluted)", "diluted net income per share"],
    "eps_basic": ["basic eps", "basic earnings per share"],
    "gross_margin_pct": ["gross margin %", "gross profit margin", "gross margin percentage", "gross margin"],
    "operating_margin_pct": ["operating margin", "operating profit margin", "operating income margin"],
    "net_margin_pct": ["net margin", "net profit margin", "net income margin"],
    "total_assets": ["total assets"],
    "total_liabilities": ["total liabilities"],
    "total_equity": ["shareholders equity", "stockholders equity", "total equity", "net assets", "shareholders' equity", "stockholders' equity"],
    "total_debt": ["total debt", "long-term debt", "total borrowings", "financial debt"],
    "cash_and_equivalents": ["cash and cash equivalents", "cash and short-term investments"],
    "operating_cashflow": ["operating cash flow", "net cash from operating activities", "cash from operations"],
    "capex": ["capital expenditure", "capex", "purchases of property plant and equipment", "pp&e additions"],
    "free_cashflow": ["free cash flow", "fcf"],
    "revenue_growth_pct": ["revenue growth", "year-over-year revenue growth", "yoy growth"],
    "rd_expense": ["research and development", "r&d expense", "r&d costs"],
}

# Reverse lookup: alias → canonical name
_ALIAS_TO_CANONICAL: dict[str, str] = {}
for canonical, aliases in CANONICAL_METRICS.items():
    for alias in aliases:
        _ALIAS_TO_CANONICAL[alias.lower()] = canonical
    _ALIAS_TO_CANONICAL[canonical.lower()] = canonical


def resolve_metric_name(raw_name: str) -> tuple[str, str]:
    """
    Resolve a raw metric name to its canonical form.
    Returns (canonical_name, confidence) where confidence is 'high' | 'medium' | 'low'.
    Uses exact match first, then fuzzy matching via rapidfuzz.
    """
    lowered = raw_name.strip().lower()

    # Exact match
    if lowered in _ALIAS_TO_CANONICAL:
        return _ALIAS_TO_CANONICAL[lowered], "high"

    # Fuzzy match
    from rapidfuzz import fuzz, process

    all_aliases = list(_ALIAS_TO_CANONICAL.keys())
    result = process.extractOne(lowered, all_aliases, scorer=fuzz.token_sort_ratio)
    if result and result[1] >= 80:
        return _ALIAS_TO_CANONICAL[result[0]], "medium"

    # Unknown metric — store raw
    return raw_name, "low"


# Metric categories for classification
METRIC_CATEGORIES = {
    "revenue": "revenue",
    "gross_profit": "profitability",
    "operating_income": "profitability",
    "net_income": "profitability",
    "ebitda": "profitability",
    "eps_diluted": "per_share",
    "eps_basic": "per_share",
    "gross_margin_pct": "profitability",
    "operating_margin_pct": "profitability",
    "net_margin_pct": "profitability",
    "total_assets": "liquidity",
    "total_liabilities": "leverage",
    "total_equity": "leverage",
    "total_debt": "leverage",
    "cash_and_equivalents": "liquidity",
    "operating_cashflow": "cashflow",
    "capex": "cashflow",
    "free_cashflow": "cashflow",
    "revenue_growth_pct": "revenue",
    "rd_expense": "other",
}