"""
AegisIngest - Cybersecurity Audit Report Benchmark & Quality Analyzer
Parses audit reports, computes compliance benchmark indices, and scores auditor performance.
"""

import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger("aegis.analyzer")

# Benchmark Baseline Weights by Audit Framework
FRAMEWORK_STANDARDS = {
    "ISO_27001": {"total_controls": 114, "critical_weight": 25, "high_weight": 15, "med_weight": 5, "low_weight": 1},
    "SOC_2_TYPE_II": {"total_controls": 64, "critical_weight": 30, "high_weight": 18, "med_weight": 6, "low_weight": 1},
    "NIST_CSF": {"total_controls": 108, "critical_weight": 25, "high_weight": 15, "med_weight": 5, "low_weight": 1},
    "FEDRAMP_HIGH": {"total_controls": 325, "critical_weight": 40, "high_weight": 20, "med_weight": 8, "low_weight": 2},
    "PCI_DSS_V4": {"total_controls": 250, "critical_weight": 35, "high_weight": 18, "med_weight": 5, "low_weight": 1},
}


class AuditBenchmarkAnalyzer:
    def analyze(self, raw_content: str, audit_type: str, agency_id: str) -> Dict[str, Any]:
        """
        Executes cybersecurity audit report parsing, benchmarking, and quality analysis.
        """
        framework = FRAMEWORK_STANDARDS.get(audit_type, FRAMEWORK_STANDARDS["ISO_27001"])
        
        # Parse document structure
        findings: List[Dict[str, Any]] = []
        controls_evaluated = framework["total_controls"]
        executive_summary = "Automated cybersecurity posture audit analysis."

        try:
            data = json.loads(raw_content)
            findings = data.get("findings", [])
            controls_evaluated = data.get("controls_evaluated", framework["total_controls"])
            executive_summary = data.get("executive_summary", executive_summary)
        except Exception:
            # Fallback text parsing heuristics
            lines = raw_content.splitlines()
            findings = [
                {"id": f"FIND-{i+1}", "severity": "MEDIUM", "category": "General Security", "description": line[:80]}
                for i, line in enumerate(lines[:3]) if line.strip()
            ]

        # Calculate severity counts
        severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for f in findings:
            sev = f.get("severity", "MEDIUM").upper()
            if sev in severity_counts:
                severity_counts[sev] += 1
            else:
                severity_counts["MEDIUM"] += 1

        # Calculate Risk Penalty
        risk_penalty = (
            severity_counts["CRITICAL"] * framework["critical_weight"] +
            severity_counts["HIGH"] * framework["high_weight"] +
            severity_counts["MEDIUM"] * framework["med_weight"] +
            severity_counts["LOW"] * framework["low_weight"]
        )

        # Base Compliance Benchmark Score (0 - 100)
        max_possible_penalty = 150.0
        normalized_penalty = min(100.0, (risk_penalty / max_possible_penalty) * 100.0)
        compliance_score = max(5.0, round(100.0 - normalized_penalty, 2))

        # Auditor Quality & Evidence Sufficiency Index (0 - 100)
        # Evaluates completeness of report documentation
        detail_score = min(100.0, 50.0 + (len(findings) * 10.0) + (len(executive_summary) / 10.0))
        evidence_sufficiency = round(min(100.0, max(20.0, detail_score)), 1)

        # Posture Grade
        if compliance_score >= 90:
            grade = "A+ (Resilient)"
        elif compliance_score >= 80:
            grade = "A (Strong)"
        elif compliance_score >= 70:
            grade = "B (Satisfactory)"
        elif compliance_score >= 60:
            grade = "C (Remediation Required)"
        else:
            grade = "D (Critical Non-Compliance)"

        return {
            "agency_id": agency_id,
            "audit_type": audit_type,
            "compliance_benchmark_score": compliance_score,
            "posture_grade": grade,
            "evidence_sufficiency_score": evidence_sufficiency,
            "total_controls_in_scope": controls_evaluated,
            "findings_summary": {
                "total_findings": len(findings),
                "critical": severity_counts["CRITICAL"],
                "high": severity_counts["HIGH"],
                "medium": severity_counts["MEDIUM"],
                "low": severity_counts["LOW"],
            },
            "findings_details": findings[:10],  # Top findings
            "executive_summary_excerpt": executive_summary[:200],
            "benchmark_status": "COMPLETED",
        }


analyzer = AuditBenchmarkAnalyzer()
