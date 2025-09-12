#
# FILE: `StockTrader/.github/scripts/code_quality/static_analysis.py`
#

"""
Static Analysis for Code Quality Workflow

Tooling:
	• black 	- formatter
	• flake8 	- linter
	• mypy 		- static type checker
	• bandit 	- security analyzer

Scope:
	• Blake/Flake8 - source, scripts, tests, utils
	• MyPy/Bandit - source, scripts, utils
	• Artifact Repo - artifacts/reports

Artifacts:
	• black_report.txt
	• flake8_report.txt
	• mypy_report.txt
	• bandit_output.txt
	• bandit_report.json
	• bandit.sarif
	• static_analysis_summary.txt, static_analysis_summary.json
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterable

ARTIFACTS = Path("artifacts/reports")
SRC_ROOTS = [p for p in ("src", "scripts", "utils") if Path(p).exists()]
LINT_ROOTS = SRC_ROOTS + ([p for p in ("tests",) if Path(p).exists()])

BLACK_REPORT = ARTIFACTS / "black_report.txt"
FLAKE8_REPORT = ARTIFACTS / "flake8_report.txt"
MYPY_REPORT = ARTIFACTS / "mypy_report.txt"
BANDIT_TXT = ARTIFACTS / "bandit_console.txt"
BANDIT_JSON = ARTIFACTS / "bandit_report.json"
BANDIT_SARIF = ARTIFACTS / "bandit.sarif"
SUMMARY_TXT = ARTIFACTS / "static_analysis_summary.txt"
SUMMARY_JSON = ARTIFACTS / "static_analysis_summary.json"



def ensure_dirs() -> None:
	ARTIFACTS.mkdir(parents=True, exist_ok=True)


def run(cmd: Iterable[str], label: str, outfile: Path | None=None) -> tuple[bool, str]:
	print(f"\n{'='*60}\nRunning {label}\n{'='*60}")
	try:
		res = subprocess.run(list(cmd), check=True, capture_output=True, text=True)
		print(f"{label}: PASSED")
		if outfile:
			outfile.write_text(f"{label} Results\n{'='*60}\n{res.stdout}")
		return True, res.stdout
	except subprocess.CalledProcessError as e:
		print(f"{label}: FAILED (exit {e.returncode})")
		if e.stdout:
			print("STDOUT:\n", e.stdout)
		if e.stderr:
			print("STDERR:\n", e.stderr)
		if outfile:
			outfile.write_text(
				f"{label} Results(FAILED)\n{'='*60}\n"
				f"Exit Code: {e.returncode}\n\n"
				f"STDOUT:\n{e.stdout or ''}\n\nSTDERR\n{e.stderr or ''}"
			)
		reason = e.stdout or e.stderr or f"{label} failed"
		return False, reason


def run_black () -> tuple[bool, str]:
	if not LINT_ROOTS:
		print("black: no targets found; skipping")
		return True, "no targets"
	return run(["black", "--check", "--diff", *LINT_ROOTS], "black code formatting", BLACK_REPORT)


def run_flake8() -> tuple[bool, str]:
	if not LINT_ROOTS:
		print("Flake8: no targets found; skipping")
		return True, "No targets"

	print(f"\n{'='*60}\nRunning Flake8 Linting\n{'='*60}")
	try:
		res = subprocess.run(
			[
				"flake8",
				*LINT_ROOTS,
				"--max-line-length=120",
				"--ignore=E203,W503",
				"--statistics",
				"--tee",
				f"--output-file={FLAKE8_REPORT}",
			],
			check=True,
			capture_output=True,
			text=True,
		)
		print("Flake8 Lint: PASSED")
		return True, res.stdout
	except subprocess.CalledProcessError as e:
		print(f"Flake8 Lint: FAILED (exit {e.returncode})")
		if e.stdout:
			print("STDOUT:\n", e.stdout)
		if e.stderr:
			print("STDERR:\n", e.stderr)

		issue_count = 0
		if FLAKE8_REPORT.exists():
			lines = FLAKE8_REPORT.read_text(errors="ignore").splitlines()
			issue_count = len([ln for ln in lines if ":" in lnand not ln.startswith("flake8")])

		return False, f"{issue_count} linting issues"



def run_mypy () -> tuple[bool, str]:
	if not SRC_ROOTS:
		print("MyPy: no targets found; skip")
		return True, "No targets"

	mypy_cmd = ["mypy", *SRC_ROOTS]

	if not Path("mypy.ini").exists():
		mypy_cmd+= ["--ignore-missing-imports", "--no-strict-optional", "--show-error-codes"]

	return run(mypy_cmd, "MyPy type checking", MYPY_REPORT)



def run_bandit() -> tuple[bool, str]:
	if not SRC_ROOTS:
		print("Bandit: no targets, skip.")
		return True, "no targets"

	ok_text, _ = run(
		["bandit", "-r", *SRC_ROOTS, "--severity-level", "medium", "-f", "txt"],
		"bandit security scan",
		BANDIT_TXT
	)

	print(f"\n{'='*60}\nRunning Bandit JSON Export\n{'='*60}")
	try:
		res_json = subprocess.run(
			["bandit", "-r", *SRC_ROOTS, "-f", "json", "-o", str(BANDIT_JSON)],
			check=False,
			capture_output=True,
			text=True
		)

		if res_json.returncode not in (0,1):
			print(f"Bandit JSON export exited with code {res_json.returncode}")
			if res_json.stderr:
				print("STDERR\n", res_json.stderr)
	except subprocess.CalledProcessError as e:
		print(f"Bandit JSON export failed (exit {e.returncode})")
		if e.stderr:
			print("STDERR:\n", e.stderr)

	ok_sarif, _ = run(
		["bandit", "-r", *SRC_ROOTS, "-f", "sarif", "-o", str(BANDIT_SARIF)],
		"Bandit SARIF Export",
		None,
	)

	issues_medium_high = 0
	if BANDIT_JSON.exists():
		try:
			data = json.loads(BANDIT_JSON.read_text())
			for issue in data.get("results", []):
				severity = (issue.get("issue_severity") or "").upper()
				if severity in {"MEDIUM", "HIGH"}:
					issues_medium_high += 1
		except json.JSONDecodeError as e:
			print(f"Warning: bad parse json badit: {e}")

	is_pass = ok_text and ok_sarif and issues_medium_high == 0
	msg = f"{issues_medium_high} medium/high issues"

	if not is_pass:
		print("Bandit result: FAIL")
	else:
		print("Bandit result: PASS")

	return is_pass, msg


def write_summary (results: dict[str, dict[str, str|bool]]) -> None:
	pass


def main() -> int:
	pass;
	
























