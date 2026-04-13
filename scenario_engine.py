"""
scenario_engine.py
------------------
Closes Gap 4: "Business Scenario Support"

Loads a named scenario from scenarios.yaml and applies it to the
generation pipeline — overriding volumes, column values, domain profile,
and date context. No Python code changes needed for new scenarios.

Integration points
------------------
  1. main.py reads --scenario flag and passes it to ScenarioEngine
  2. ScenarioEngine modifies:
       - volume_plan         (anchor_overrides + volume_skews)
       - DomainConfig        (domain switch + column_overrides injected)
       - date context        (reference_date, date_range_years)
  3. DataGenerator already uses DomainConfig.lookup() — scenario overrides
     are injected as highest-priority table_prefix_overrides, so they
     transparently apply without touching DataGenerator code.

Adding a new scenario
---------------------
  1. Add a block to scenarios.yaml
  2. Run: python main.py --scenario your_scenario_name
  Zero Python changes required.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("app")


class ScenarioEngine:
    """
    Loads a scenario from scenarios.yaml and applies it to the pipeline.
    """

    def __init__(
        self,
        scenario_name: str,
        scenarios_path: str | Path = "scenarios.yaml",
        loggers: dict | None = None,
    ):
        self.log = (loggers or {}).get("app", logger)
        self.scenario_name = scenario_name
        self._raw: dict = {}
        self._scenario: dict = {}

        path = Path(scenarios_path)
        if not path.exists():
            self.log.warning("scenarios.yaml not found at %s", path)
            return

        with open(path, encoding="utf-8") as fh:
            self._raw = yaml.safe_load(fh) or {}

        scenarios = self._raw.get("scenarios", {})
        if scenario_name not in scenarios:
            available = list(scenarios.keys())
            raise ValueError(
                f"Scenario '{scenario_name}' not found in {path}. "
                f"Available: {available}"
            )

        self._scenario = scenarios[scenario_name]
        self.log.info(
            "Scenario loaded: '%s' — %s",
            scenario_name,
            self._scenario.get("description", ""),
        )

    # ------------------------------------------------------------------
    # Volume plan modifications
    # ------------------------------------------------------------------

    def apply_volume_overrides(self, volume_plan: dict[str, int]) -> dict[str, int]:
        """
        Apply anchor_overrides and volume_skews to an existing volume plan.
        Returns a new plan dict — original is not mutated.
        """
        if not self._scenario:
            return volume_plan

        result = dict(volume_plan)

        # anchor_overrides: directly set root table counts
        for table, count in self._scenario.get("anchor_overrides", {}).items():
            if table in result:
                old = result[table]
                result[table] = count
                self.log.info(
                    "  Scenario anchor override: %s  %d → %d", table, old, count
                )

        # volume_skews: multiply child table volumes
        for table, factor in self._scenario.get("volume_skews", {}).items():
            if table in result:
                old = result[table]
                result[table] = max(1, int(old * factor))
                self.log.info(
                    "  Scenario volume skew: %s  %d × %.1f = %d",
                    table, old, factor, result[table],
                )

        return result

    # ------------------------------------------------------------------
    # Domain config injection
    # ------------------------------------------------------------------

    def inject_into_domain(self, domain_config) -> None:
        """
        Inject scenario column_overrides into the DomainConfig as the
        highest-priority table_prefix_overrides. This means scenario
        values always win over domain defaults — without modifying
        domains.yaml or DataGenerator code.

        domain_config: a DomainConfig instance from data_generator.py
        """
        if not self._scenario:
            return

        overrides = self._scenario.get("column_overrides", {})
        if not overrides:
            return

        # Inject into the active domain's table_prefix_overrides
        # DomainConfig._active is the live dict — we mutate it in place
        existing = domain_config._active.setdefault("table_prefix_overrides", {})
        for table, col_overrides in overrides.items():
            if table not in existing:
                existing[table] = {}
            for col, values in col_overrides.items():
                existing[table][col] = values
                self.log.debug(
                    "  Scenario column override: %s.%s → %s", table, col, values
                )

        self.log.info(
            "Scenario '%s': injected column overrides for %d table(s).",
            self.scenario_name, len(overrides),
        )

    # ------------------------------------------------------------------
    # Domain profile override
    # ------------------------------------------------------------------

    def get_domain_override(self) -> str | None:
        """Return the domain profile this scenario wants, or None."""
        return self._scenario.get("domain")

    # ------------------------------------------------------------------
    # Date context
    # ------------------------------------------------------------------

    def get_date_context(self) -> dict:
        """
        Return resolved date context dict:
          { "reference_date": date, "date_start": date, "date_end": date }
        Used by DataGenerator for date generation bounds.
        """
        ctx = self._scenario.get("date_context", {})
        if not ctx:
            return {}

        ref_str = ctx.get("reference_date", "today")
        if ref_str == "today":
            ref_date = date.today()
        else:
            ref_date = date.fromisoformat(ref_str)

        years = ctx.get("date_range_years", 5)
        if years == 0:
            # All dates within last 30 days
            date_start = ref_date - timedelta(days=30)
        else:
            date_start = ref_date - timedelta(days=int(years * 365))

        return {
            "reference_date": ref_date,
            "date_start": date_start,
            "date_end": ref_date,
        }

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def is_active(self) -> bool:
        return bool(self._scenario)

    @property
    def description(self) -> str:
        return self._scenario.get("description", "")

    @property
    def name(self) -> str:
        return self.scenario_name

    def summary(self) -> str:
        if not self._scenario:
            return "No scenario active."
        lines = [
            f"Scenario: {self.scenario_name}",
            f"  Description  : {self.description}",
            f"  Domain       : {self._scenario.get('domain', 'default')}",
            f"  Anchor overrides : {list(self._scenario.get('anchor_overrides', {}).keys())}",
            f"  Volume skews     : {self._scenario.get('volume_skews', {})}",
            f"  Column overrides : {list(self._scenario.get('column_overrides', {}).keys())}",
            f"  Date context     : {self._scenario.get('date_context', {})}",
        ]
        return "\n".join(lines)

    @staticmethod
    def list_scenarios(scenarios_path: str | Path = "scenarios.yaml") -> list[dict]:
        """Return a list of available scenarios with name and description."""
        path = Path(scenarios_path)
        if not path.exists():
            return []
        with open(path) as fh:
            raw = yaml.safe_load(fh) or {}
        return [
            {"name": k, "description": v.get("description", "")}
            for k, v in raw.get("scenarios", {}).items()
        ]