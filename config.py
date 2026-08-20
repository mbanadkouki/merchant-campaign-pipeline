"""
Single source of truth for environment-dependent configuration.

Design principle: no other file in this repo should read os.environ
directly for pipeline config, or open a YAML file directly. Everything
imports constants from here. This is what makes promoting the pipeline
from dev -> staging -> prod a config change, not a code change.

Resolution order:
1. PIPELINE_ENV env var (set via .env locally, or set by the job/DAB
   target in a real deployment) selects which config/<env>.yaml to load.
2. That YAML supplies catalog/schema/market/synthetic-data settings.
3. Secrets (tokens, hosts) stay in .env / Databricks Secrets — never in
   the YAML files, since those are meant to be committed to git.
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Loads .env into os.environ if present. No-op in CI/job contexts where
# real env vars are already injected (e.g. by a DAB job's env config).
load_dotenv(override=True)

ENV = os.environ.get("PIPELINE_ENV", "dev")
print(f"Loading config for PIPELINE_ENV={ENV} from config/{ENV}.yaml")
_CONFIG_DIR = Path(__file__).parent / "config"


def _load_yaml(env: str) -> dict:
    path = _CONFIG_DIR / f"{env}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"No config file for PIPELINE_ENV='{env}' at {path}. "
            f"Available: {[p.stem for p in _CONFIG_DIR.glob('*.yaml')]}"
        )
    with open(path) as f:
        return yaml.safe_load(f)


_cfg = _load_yaml(ENV)

# --- Catalog / schema names -------------------------------------------------
CATALOG = _cfg["catalog"]
BRONZE_SCHEMA = f"{CATALOG}.{_cfg['schemas']['bronze']}"
SILVER_SCHEMA = f"{CATALOG}.{_cfg['schemas']['silver']}"
GOLD_SCHEMA = f"{CATALOG}.{_cfg['schemas']['gold']}"

# --- Fully-qualified table names --------------------------------------------
BRONZE_EVENTS_TABLE = f"{BRONZE_SCHEMA}.merchant_campaign_events"
SILVER_EVENTS_TABLE = f"{SILVER_SCHEMA}.merchant_campaign_events"
SILVER_REJECTED_TABLE = f"{SILVER_SCHEMA}.merchant_campaign_events_rejected"

# --- Business config ---------------------------------------------------------
MARKETS = _cfg.get("markets", [])

# --- Synthetic data generation (Day 1 only; prod won't have this key) -------
_synthetic = _cfg.get("synthetic_data", {})
SYNTHETIC_NUM_ROWS = _synthetic.get("num_rows")
SYNTHETIC_NUM_MERCHANTS = _synthetic.get("num_merchants")
SYNTHETIC_NUM_CAMPAIGNS = _synthetic.get("num_campaigns")
SYNTHETIC_DATE_START = _synthetic.get("date_range_start")
SYNTHETIC_DATE_END = _synthetic.get("date_range_end")


def describe() -> str:
    """Small debug helper — call from a notebook cell to sanity-check
    which environment/config actually got loaded before running a job."""
    return (
        f"PIPELINE_ENV={ENV} | catalog={CATALOG} | "
        f"bronze={BRONZE_SCHEMA} silver={SILVER_SCHEMA} gold={GOLD_SCHEMA} | "
        f"markets={MARKETS}"
    )
