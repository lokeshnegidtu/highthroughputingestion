"""
AegisIngest - Sample Audit Report Generator
Generates structured cybersecurity audit reports (ISO 27001, SOC 2, NIST CSF, FedRAMP, PCI-DSS)
for load tests and validation workflows.
"""

import random
import argparse
from pathlib import Path

FRAMEWORKS = {
    "ISO_27001": {
        "title": "ISO/IEC 27001:2022 Information Security Management System Audit",
        "controls": 114,
        "categories": ["Access Control", "Cryptography", "Physical Security", "Operations Security", "Supplier Relationships"],
    },
    "SOC_2_TYPE_II": {
        "title": "SOC 2 Type II Security, Availability & Confidentiality Report",
        "controls": 64,
        "categories": ["Common Criteria", "Logical Access", "Change Management", "Risk Assessment"],
    },
    "NIST_CSF": {
        "title": "NIST Cybersecurity Framework v2.0 Audit Assessment",
        "controls": 108,
        "categories": ["Identify", "Protect", "Detect", "Respond", "Recover", "Govern"],
    },
    "FEDRAMP_HIGH": {
        "title": "FedRAMP High Baseline Security Assessment Report (SAR)",
        "controls": 325,
        "categories": ["AC - Access Control", "AU - Audit & Accountability", "SC - System & Comms Protection", "SI - System Integrity"],
    },
    "PCI_DSS_V4": {
        "title": "Payment Card Industry Data Security Standard (PCI DSS) v4.0 Report on Compliance",
        "controls": 250,
        "categories": ["Network Security", "Cardholder Data Protection", "Vulnerability Management", "Strong Access Control"],
    },
}

SAMPLE_FINDINGS = [
    {"severity": "CRITICAL", "description": "Unauthenticated API gateway endpoint exposes administrative user directory."},
    {"severity": "HIGH", "description": "MFA enforcement bypassed on internal service mesh ingress proxy."},
    {"severity": "HIGH", "description": "Database backup snapshots stored in unencrypted storage volume."},
    {"severity": "MEDIUM", "description": "Password complexity policy lacks special character requirements."},
    {"severity": "MEDIUM", "description": "Session expiration timeout set to 24 hours instead of recommended 15 minutes."},
    {"severity": "LOW", "description": "Audit log rotation retention period configured for 90 days instead of 180 days."},
    {"severity": "LOW", "description": "Minor formatting discrepancy in third-party vendor risk assessment questionnaires."}
]


def generate_report(agency_id: str, framework_name: str = "ISO_27001") -> dict:
    fw = FRAMEWORKS.get(framework_name, FRAMEWORKS["ISO_27001"])
    num_findings = random.randint(0, 5)
    selected_findings = random.sample(SAMPLE_FINDINGS, min(num_findings, len(SAMPLE_FINDINGS)))

    # Assign IDs and categories
    findings = []
    for idx, f in enumerate(selected_findings):
        findings.append({
            "id": f"FIND-{agency_id[-4:]}-{idx+1:03d}",
            "severity": f["severity"],
            "category": random.choice(fw["categories"]),
            "description": f["description"],
            "remediation_sla_days": 7 if f["severity"] == "CRITICAL" else (30 if f["severity"] == "HIGH" else 90),
        })

    return {
        "agency_id": agency_id,
        "audit_type": framework_name,
        "report_title": fw["title"],
        "report_year": 2026,
        "auditor_org": f"Global Cybersecurity Audit Authority #{random.randint(1, 20)}",
        "controls_evaluated": fw["controls"],
        "executive_summary": (
            f"Independent cybersecurity assessment conducted for {agency_id}. "
            f"Evaluated {fw['controls']} controls across standard domains with {len(findings)} non-conformances identified."
        ),
        "findings": findings,
    }


def report_as_pdf(report: dict) -> bytes:
    """Render a lightweight PDF without requiring an external PDF package."""
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    lines = [
        report["report_title"],
        f"Agency: {report['agency_id']}",
        f"Auditor: {report['auditor_org']}",
        f"Reporting year: {report['report_year']}",
        f"Controls evaluated: {report['controls_evaluated']}",
        "",
        "Executive summary",
        report["executive_summary"],
        "",
        "Findings",
    ]
    lines.extend(f"{finding['severity']}: {finding['description']}" for finding in report["findings"])
    if not report["findings"]:
        lines.append("No findings identified in this sample assessment.")
    content = ["BT", "/F1 15 Tf", "54 748 Td"]
    for number, line in enumerate(lines):
        content.append(f"({escape(line)}) Tj")
        content.extend(["0 -19 Td", "/F1 10 Tf"] if number == 0 else ["0 -15 Td"])
    content.append("ET")
    stream = "\n".join(content).encode("latin-1", "replace")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    pdf, offsets = bytearray(b"%PDF-1.4\n"), [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode() + obj + b"\nendobj\n")
    xref = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
    pdf.extend(b"".join(f"{offset:010} 00000 n \n".encode() for offset in offsets[1:]))
    pdf.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    return bytes(pdf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--out-dir", default="data/sample_reports")
    args = parser.parse_args()

    out_path = Path(args.out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    framework_keys = list(FRAMEWORKS.keys())
    for i in range(args.count):
        agency = f"agency-gov-{i+1:03d}"
        fw = random.choice(framework_keys)
        rep = generate_report(agency, fw)
        file_path = out_path / f"{agency}_{fw}.pdf"
        with open(file_path, "wb") as f:
            f.write(report_as_pdf(rep))

    print(f"Generated {args.count} sample reports in {out_path}")
