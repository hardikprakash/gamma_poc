#!/usr/bin/env python3
"""
Evaluation runner — measures system quality against DOC4 criteria.

Usage:
    python eval/eval.py --test-set eval/test_set.json --output eval/results.json --verbose
"""

from __future__ import annotations
import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

logger = logging.getLogger("eval")

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")


# ── Metric Implementations ──────────────────────────────────────────────────

def numerical_accuracy(response: dict, test_case: dict) -> float:
    """
    For each ground_truth_fact, check if the response answer contains the value
    within tolerance: 0.5% for values > 100, exact for values <= 100.
    """
    ground_truth = test_case.get("ground_truth_facts", [])
    if not ground_truth:
        return 1.0  # No facts to check

    answer = response.get("answer", "")
    # Extract all numbers from answer
    numbers_in_answer = [float(n.replace(",", "")) for n in re.findall(r'[\d,]+\.?\d*', answer)]

    matched = 0
    for fact in ground_truth:
        expected = fact.get("value", 0)
        tolerance = abs(expected * 0.005) if abs(expected) > 100 else 0.01

        for num in numbers_in_answer:
            if abs(num - expected) <= tolerance:
                matched += 1
                break

    return matched / len(ground_truth) if ground_truth else 1.0


def citation_precision(response: dict, test_case: dict) -> float:
    """
    Check if cited sources match expected citation sources.
    Simplified version — checks company + year + section overlap.
    """
    citations = response.get("resolved_citations", [])
    expected_sources = test_case.get("expected_citation_sources", [])

    if not citations:
        return 0.0 if expected_sources else 1.0

    if not expected_sources:
        return 1.0

    matched = 0
    for citation in citations:
        cite_str = f"{citation.get('company', '')} {citation.get('fiscal_year', '')} {citation.get('section_path', '')}".lower()
        for expected in expected_sources:
            if any(word.lower() in cite_str for word in expected.split()):
                matched += 1
                break

    return matched / len(citations) if citations else 0.0


def hallucination_rate(response: dict, test_case: dict) -> float:
    """
    Extract all numeric claims from answer using regex.
    Check if they exist in the ground truth or cited facts.
    """
    answer = response.get("answer", "")
    numbers = re.findall(r'[\d,]+\.?\d*', answer)
    if not numbers:
        return 0.0

    # Collect all known values
    known_values = set()
    for fact in test_case.get("ground_truth_facts", []):
        known_values.add(fact.get("value", 0))
    for fact in response.get("facts_used", []):
        if isinstance(fact, dict):
            known_values.add(fact.get("value", 0))

    # Also include any values from resolved citations
    for citation in response.get("resolved_citations", []):
        preview = citation.get("content_preview", "")
        for n in re.findall(r'[\d,]+\.?\d*', preview):
            try:
                known_values.add(float(n.replace(",", "")))
            except ValueError:
                pass

    unverified = 0
    total = 0
    for n_str in numbers:
        try:
            n = float(n_str.replace(",", ""))
            if n < 1 or n == int(n) and n < 100:
                continue  # Skip small integers likely not financial data
            total += 1
            verified = False
            for kv in known_values:
                tolerance = abs(kv * 0.005) if abs(kv) > 100 else 0.01
                if abs(n - kv) <= tolerance:
                    verified = True
                    break
            if not verified:
                unverified += 1
        except ValueError:
            pass

    return unverified / total if total > 0 else 0.0


def unanswerable_declaration(response: dict, test_case: dict) -> bool:
    """
    Pass = unanswerable_sub_questions is non-empty OR answer contains declaration phrases.
    Fail = answer contains a specific numeric value.
    """
    if test_case.get("answerable", True):
        return True  # Not applicable

    unanswerable = response.get("unanswerable_sub_questions", [])
    answer = response.get("answer", "").lower()

    declaration_phrases = [
        "could not find", "not available", "not in", "no information",
        "cannot answer", "not present", "outside", "not in the corpus",
        "not in corpus", "no data", "unable to find", "not included",
    ]

    has_declaration = (
        bool(unanswerable)
        or any(phrase in answer for phrase in declaration_phrases)
    )

    # Check for hallucinated numbers
    numbers = re.findall(r'\$?[\d,]+\.?\d*\s*(?:million|billion|%)', answer)
    has_specific_number = bool(numbers)

    return has_declaration and not has_specific_number


def conflict_detection(response: dict, planted_conflicts: list[dict]) -> float:
    """Check if planted conflicts are detected."""
    if not planted_conflicts:
        return 1.0

    conflicts = response.get("conflicts_detected", [])
    detected = 0
    for planted in planted_conflicts:
        metric = planted.get("metric", "")
        for conflict_text in conflicts:
            if metric.lower() in conflict_text.lower():
                detected += 1
                break

    return detected / len(planted_conflicts) if planted_conflicts else 1.0


def retrieval_recall_facts(response: dict, test_case: dict) -> float:
    """% of ground truth facts that appear in retrieved context."""
    ground_truth = test_case.get("ground_truth_facts", [])
    if not ground_truth:
        return 1.0

    answer = response.get("answer", "")
    facts_used = response.get("facts_used", [])
    citations = response.get("resolved_citations", [])

    # Build set of all values mentioned in response
    all_text = answer
    for c in citations:
        all_text += " " + c.get("content_preview", "")

    matched = 0
    for fact in ground_truth:
        expected = fact.get("value", 0)
        # Check if value appears (within tolerance)
        numbers = [float(n.replace(",", "")) for n in re.findall(r'[\d,]+\.?\d*', all_text)]
        tolerance = abs(expected * 0.005) if abs(expected) > 100 else 0.01
        if any(abs(n - expected) <= tolerance for n in numbers):
            matched += 1

    return matched / len(ground_truth) if ground_truth else 1.0


# ── Main Eval Loop ──────────────────────────────────────────────────────────

async def run_eval(test_set_path: str, output_path: str, verbose: bool = False):
    """Run evaluation against all test cases."""

    with open(test_set_path) as f:
        test_cases = json.load(f)

    logger.info(f"Loaded {len(test_cases)} test cases from {test_set_path}")

    results = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_test_cases": len(test_cases),
        "summary": {},
        "by_category": {},
        "failures": [],
        "latencies_ms": [],
    }

    category_scores: dict[str, list[dict]] = {}

    async with httpx.AsyncClient(timeout=60.0) as client:
        for tc in test_cases:
            tc_id = tc.get("id", "unknown")
            category = tc.get("category", "unknown")
            query = tc.get("query", "")
            answerable = tc.get("answerable", True)

            if verbose:
                logger.info(f"Running {tc_id} [{category}]: {query[:60]}...")

            start = time.time()
            try:
                resp = await client.post(
                    f"{BACKEND_URL}/query",
                    json={"query": query},
                )
                latency_ms = int((time.time() - start) * 1000)
                results["latencies_ms"].append(latency_ms)

                if resp.status_code != 200:
                    logger.warning(f"{tc_id}: HTTP {resp.status_code}")
                    data = {}
                else:
                    data = resp.json()
            except Exception as e:
                logger.error(f"{tc_id}: Request failed: {e}")
                latency_ms = int((time.time() - start) * 1000)
                results["latencies_ms"].append(latency_ms)
                data = {}

            # Compute metrics
            scores = {}
            if answerable:
                scores["numerical_accuracy"] = numerical_accuracy(data, tc)
                scores["citation_precision"] = citation_precision(data, tc)
                scores["hallucination_rate"] = hallucination_rate(data, tc)
                scores["retrieval_recall"] = retrieval_recall_facts(data, tc)
            else:
                scores["unanswerable_declaration"] = 1.0 if unanswerable_declaration(data, tc) else 0.0

            if tc.get("planted_conflicts"):
                scores["conflict_detection"] = conflict_detection(data, tc["planted_conflicts"])

            scores["latency_ms"] = latency_ms

            # Track by category
            if category not in category_scores:
                category_scores[category] = []
            category_scores[category].append(scores)

            # Check for failures
            if answerable and scores.get("numerical_accuracy", 1) < 0.75:
                results["failures"].append({
                    "id": tc_id,
                    "category": category,
                    "query": query,
                    "metric": "numerical_fact_accuracy",
                    "score": scores.get("numerical_accuracy", 0),
                    "detail": "Below critical threshold",
                })

            if not answerable and scores.get("unanswerable_declaration", 1) == 0:
                results["failures"].append({
                    "id": tc_id,
                    "category": category,
                    "query": query,
                    "metric": "unanswerable_declaration",
                    "score": 0.0,
                    "detail": "Failed to declare unanswerable",
                })

    # ── Aggregate Results ────────────────────────────────────────────────────

    all_num_acc = []
    all_cit_prec = []
    all_hall = []
    all_unans = []
    all_recall = []
    all_conflict = []

    for cat, score_list in category_scores.items():
        cat_summary = {"count": len(score_list)}
        for scores in score_list:
            if "numerical_accuracy" in scores:
                all_num_acc.append(scores["numerical_accuracy"])
            if "citation_precision" in scores:
                all_cit_prec.append(scores["citation_precision"])
            if "hallucination_rate" in scores:
                all_hall.append(scores["hallucination_rate"])
            if "unanswerable_declaration" in scores:
                all_unans.append(scores["unanswerable_declaration"])
            if "retrieval_recall" in scores:
                all_recall.append(scores["retrieval_recall"])
            if "conflict_detection" in scores:
                all_conflict.append(scores["conflict_detection"])

        # Category-level averages
        cat_num = [s.get("numerical_accuracy", 0) for s in score_list if "numerical_accuracy" in s]
        cat_cit = [s.get("citation_precision", 0) for s in score_list if "citation_precision" in s]
        if cat_num:
            cat_summary["numerical_fact_accuracy"] = round(sum(cat_num) / len(cat_num), 3)
        if cat_cit:
            cat_summary["citation_precision"] = round(sum(cat_cit) / len(cat_cit), 3)
        if any("unanswerable_declaration" in s for s in score_list):
            cat_unans = [s["unanswerable_declaration"] for s in score_list if "unanswerable_declaration" in s]
            cat_summary["declaration_rate"] = round(sum(cat_unans) / len(cat_unans), 3)

        results["by_category"][cat] = cat_summary

    # P95 latency
    latencies = sorted(results["latencies_ms"])
    p95_idx = int(len(latencies) * 0.95)
    p95_latency = latencies[p95_idx] if latencies else 0

    results["summary"] = {
        "numerical_fact_accuracy": round(sum(all_num_acc) / len(all_num_acc), 3) if all_num_acc else 0,
        "citation_precision": round(sum(all_cit_prec) / len(all_cit_prec), 3) if all_cit_prec else 0,
        "hallucination_rate": round(sum(all_hall) / len(all_hall), 3) if all_hall else 0,
        "unanswerable_declaration_rate": round(sum(all_unans) / len(all_unans), 3) if all_unans else 0,
        "retrieval_recall_facts": round(sum(all_recall) / len(all_recall), 3) if all_recall else 0,
        "p95_latency_ms": p95_latency,
        "conflict_detection_rate": round(sum(all_conflict) / len(all_conflict), 3) if all_conflict else 0,
        "all_targets_met": False,
    }

    # Check all targets
    s = results["summary"]
    s["all_targets_met"] = (
        s["numerical_fact_accuracy"] >= 0.90
        and s["citation_precision"] >= 0.85
        and s["hallucination_rate"] < 0.05
        and s["unanswerable_declaration_rate"] >= 0.95
        and s["p95_latency_ms"] < 8000
    )

    # Remove raw latencies from output
    del results["latencies_ms"]

    # Write results
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Eval complete. Results written to {output_path}")
    logger.info(f"Summary: {json.dumps(results['summary'], indent=2)}")
    if results["failures"]:
        logger.warning(f"{len(results['failures'])} failures detected.")

    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

    parser = argparse.ArgumentParser(description="Evaluate Graph RAG agent")
    parser.add_argument("--test-set", required=True, help="Path to test_set.json")
    parser.add_argument("--output", default=None, help="Output results JSON path")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.output:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"eval/results_{ts}.json"

    asyncio.run(run_eval(args.test_set, args.output, args.verbose))


if __name__ == "__main__":
    main()
