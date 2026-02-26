"""
Tests for M4: Fact Extractor — taxonomy and metric normalization tests.
Also tests the validated_llm_call pattern.
"""

import pytest
from models.taxonomy import resolve_metric_name, CANONICAL_METRICS, METRIC_CATEGORIES


class TestMetricResolution:
    """Test canonical metric name resolution."""

    def test_exact_match_revenue(self):
        canon, conf = resolve_metric_name("Total Revenues")
        assert canon == "revenue"
        assert conf == "high"

    def test_exact_match_net_income(self):
        canon, conf = resolve_metric_name("Net Income")
        assert canon == "net_income"
        assert conf == "high"

    def test_exact_match_case_insensitive(self):
        canon, conf = resolve_metric_name("TOTAL REVENUES")
        assert canon == "revenue"

    def test_alias_diluted_eps(self):
        canon, conf = resolve_metric_name("Diluted Earnings Per Share")
        assert canon == "eps_diluted"
        assert conf == "high"

    def test_alias_ebit(self):
        canon, conf = resolve_metric_name("EBIT")
        assert canon == "operating_income"
        assert conf == "high"

    def test_alias_shareholders_equity(self):
        canon, conf = resolve_metric_name("Shareholders Equity")
        assert canon == "total_equity"
        assert conf == "high"

    def test_alias_capex(self):
        canon, conf = resolve_metric_name("Capital Expenditure")
        assert canon == "capex"
        assert conf == "high"

    def test_fuzzy_match_close_typo(self):
        canon, conf = resolve_metric_name("total revenuse")  # typo
        assert conf in ("medium", "high")
        assert canon == "revenue"

    def test_fuzzy_match_operating_cash_flow(self):
        canon, conf = resolve_metric_name("cash flow from ops")
        # May or may not fuzzy match — check doesn't crash
        assert isinstance(canon, str)
        assert conf in ("high", "medium", "low")

    def test_unknown_metric(self):
        canon, conf = resolve_metric_name("zxcvbnm_totally_made_up")
        assert conf == "low"
        assert canon == "zxcvbnm_totally_made_up"

    def test_all_canonical_metrics_resolve_to_themselves(self):
        for canonical in CANONICAL_METRICS:
            c, conf = resolve_metric_name(canonical)
            assert c == canonical, f"{canonical} did not resolve to itself"
            assert conf == "high"

    def test_all_canonical_metrics_have_categories(self):
        for canonical in CANONICAL_METRICS:
            assert canonical in METRIC_CATEGORIES, f"{canonical} missing from METRIC_CATEGORIES"


class TestExtractorModels:
    """Test extractor output models."""

    def test_financial_fact_construction(self):
        from models.fact import FinancialFact
        fact = FinancialFact(
            fact_id="f-001",
            metric_name_raw="Net Revenue",
            metric_name_canonical="revenue",
            metric_category="revenue",
            value=383285.0,
            unit="millions",
            period="FY2023",
            fiscal_year=2023,
            company="Apple Inc.",
            source_chunk_id="AAPL_2023_financial_statements_42_1",
            confidence="high",
        )
        assert fact.value == 383285.0
        assert fact.metric_name_canonical == "revenue"

    def test_financial_fact_validation_rejects_bad_types(self):
        from models.fact import FinancialFact
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            FinancialFact(
                fact_id="f-001",
                metric_name_raw="Revenue",
                metric_name_canonical="revenue",
                metric_category="revenue",
                value="not_a_number",  # should fail
                unit="millions",
                period="FY2023",
                fiscal_year=2023,
                company="Test",
                source_chunk_id="t_1",
            )

    def test_entity_construction(self):
        from models.fact import Entity
        e = Entity(
            entity_id="e-001",
            name="Apple Inc.",
            canonical_name="Apple",
            entity_type="company",
        )
        assert e.entity_type == "company"

    def test_risk_factor_construction(self):
        from models.fact import RiskFactor
        r = RiskFactor(
            risk_id="r-001",
            title="Supply Chain Disruption",
            summary="Risk of disruption due to geopolitical tensions.",
            risk_category="operational",
            company="Apple",
            fiscal_year=2023,
            source_chunk_id="AAPL_2023_risk_factors_15_1",
        )
        assert r.risk_category == "operational"
