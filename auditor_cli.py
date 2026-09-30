#!/usr/bin/env python3
"""
auditor_cli.py — lightweight streaming auditor pipeline
Intake → Score → Report (streaming JSONL)
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional


# ---------------------------------------------------------------------------
# Core data
# ---------------------------------------------------------------------------

@dataclass
class Turn:
    id: str
    content: str


@dataclass
class ScoreResult:
    turn_id: str
    score: float
    flags: List[str] = field(default_factory=list)
    max_severity: str = "info"          # info | warning | critical
    raw: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------

class IntakeAdapter(ABC):
    """Produces a stream of Turn objects."""

    @abstractmethod
    def stream(self) -> Iterator[Turn]:
        ...


class BaseScorer:
    """
    Simple rule-based scorer.
    ruleset: list of (keyword, severity, flag_name, score_delta)
    """

    SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}

    def __init__(self, ruleset: Optional[List[tuple]] = None):
        # Default minimal rules so the harness produces non-empty results
        self.ruleset = ruleset or [
            ("warning", "warning", "warn_flag", 0.4),
            ("critical", "critical", "crit_flag", 0.9),
            ("failure", "critical", "fail_flag", 0.85),
            ("odd", "warning", "odd_flag", 0.3),
        ]

    def score(self, turn: Turn) -> ScoreResult:
        content_lower = turn.content.lower()
        score = 0.0
        flags: List[str] = []
        max_sev = "info"

        for keyword, severity, flag, delta in self.ruleset:
            if keyword in content_lower:
                score += delta
                flags.append(flag)
                if self.SEVERITY_ORDER.get(severity, 0) > self.SEVERITY_ORDER.get(max_sev, 0):
                    max_sev = severity

        # Clamp
        score = max(0.0, min(1.0, score))

        return ScoreResult(
            turn_id=turn.id,
            score=round(score, 4),
            flags=flags,
            max_severity=max_sev,
            raw={"content_preview": turn.content[:80]},
        )


class StreamingReporter:
    """Writes one JSON object per line immediately (JSONL)."""

    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "w", encoding="utf-8")
        self._count = 0

    def report(self, result: ScoreResult) -> None:
        obj = {
            "turn_id": result.turn_id,
            "score": result.score,
            "flags": result.flags,
            "max_severity": result.max_severity,
        }
        if result.raw is not None:
            obj["raw"] = result.raw
        self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._fh.flush()
        self._count += 1

    def close(self) -> None:
        if self._fh and not self._fh.closed:
            self._fh.close()

    @property
    def count(self) -> int:
        return self._count


class Auditor:
    """Orchestrates intake → score → report with basic fault tolerance."""

    def __init__(
        self,
        intake: IntakeAdapter,
        scorer: BaseScorer,
        reporter: StreamingReporter,
    ):
        self.intake = intake
        self.scorer = scorer
        self.reporter = reporter
        self.logger = logging.getLogger("auditor")

    def run(self, verbose: bool = False) -> None:
        if verbose:
            logging.basicConfig(level=logging.INFO)

        for turn in self.intake.stream():
            try:
                result = self.scorer.score(turn)
                self.reporter.report(result)
                if verbose:
                    self.logger.info(
                        "scored %s → score=%.3f sev=%s flags=%s",
                        turn.id, result.score, result.max_severity, result.flags,
                    )
            except Exception as exc:
                # Fault tolerance: log and continue
                self.logger.warning(
                    "scorer failed on turn %s: %s — skipping", turn.id, exc
                )
                if verbose:
                    self.logger.debug("exception detail", exc_info=True)
                # Do not write a partial result
                continue


# ---------------------------------------------------------------------------
# Convenience helpers (optional for CLI use)
# ---------------------------------------------------------------------------

class ListIntake(IntakeAdapter):
    """Simple intake from an in-memory list of Turns."""

    def __init__(self, turns: Iterable[Turn]):
        self._turns = list(turns)

    def stream(self) -> Iterator[Turn]:
        yield from self._turns


def main():
    """Minimal CLI entry point for manual runs."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Streaming auditor")
    parser.add_argument("--out", default="audit.jsonl", help="Output JSONL path")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    # Example turns for manual testing
    demo_turns = [
        Turn("t001", "normal message"),
        Turn("t002", "warning: something odd"),
        Turn("t003", "critical failure occurred"),
    ]

    intake = ListIntake(demo_turns)
    scorer = BaseScorer()
    reporter = StreamingReporter(args.out)

    auditor = Auditor(intake=intake, scorer=scorer, reporter=reporter)
    auditor.run(verbose=args.verbose)
    reporter.close()

    print(f"Wrote {reporter.count} results → {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
