#!/usr/bin/env python3
import os
import json
import tempfile
import unittest

from auditor_cli import (
    IntakeAdapter,
    BaseScorer,
    StreamingReporter,
    Auditor,
)


class FakeTurn:
    """Simple turn object for controlled testing."""
    def __init__(self, id, content):
        self.id = id
        self.content = content


class FakeIntake(IntakeAdapter):
    """Override stream() to provide deterministic test turns."""
    def __init__(self, turns):
        self.turns = turns

    def stream(self):
        for t in self.turns:
            yield t


class AuditorHarnessTests(unittest.TestCase):

    def test_end_to_end_streaming(self):
        # Prepare deterministic turns
        turns = [
            FakeTurn("t001", "normal message"),
            FakeTurn("t002", "warning: something odd"),
            FakeTurn("t003", "critical failure occurred"),
        ]

        # Build intake
        intake = FakeIntake(turns)

        # Build scorer with simple keyword rules
        scorer = BaseScorer(ruleset=[
            # These rules already exist in your scorer integration
        ])

        # Temporary output file for streaming reporter
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "audit.jsonl")
            reporter = StreamingReporter(out_path)

            # Run auditor
            auditor = Auditor(intake=intake, scorer=scorer, reporter=reporter)
            auditor.run(verbose=False)

            # Close reporter
            reporter.close()

            # Validate output file exists
            self.assertTrue(os.path.exists(out_path))

            # Validate JSONL structure
            with open(out_path, "r") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 3)

            # Validate each line is valid JSON
            for line in lines:
                obj = json.loads(line)
                self.assertIn("turn_id", obj)
                self.assertIn("score", obj)
                self.assertIn("flags", obj)
                self.assertIn("max_severity", obj)

    def test_fault_tolerance(self):
        """Ensure auditor continues after scorer exceptions."""

        class BadScorer(BaseScorer):
            def score(self, turn):
                raise ValueError("boom")

        turns = [FakeTurn("tX", "content")]

        intake = FakeIntake(turns)

        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "audit.jsonl")
            reporter = StreamingReporter(out_path)

            auditor = Auditor(intake=intake, scorer=BadScorer(), reporter=reporter)
            auditor.run(verbose=True)

            reporter.close()

            # No results should be written
            with open(out_path, "r") as f:
                lines = f.readlines()

            self.assertEqual(len(lines), 0)


if __name__ == "__main__":
    unittest.main()
