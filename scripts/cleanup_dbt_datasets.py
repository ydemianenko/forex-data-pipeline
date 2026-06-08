"""
One-shot cleanup script.

Removes legacy dbt-created datasets that were left behind after profile/schema
changes, so that we can rebuild Silver/Gold cleanly from a single base dataset.

Bronze (forex_analysis.eurusd_raw) is NEVER touched — it is the source of truth
backed by GCS Parquet files.

Usage:
    python scripts/cleanup_dbt_datasets.py              # dry-run (default)
    python scripts/cleanup_dbt_datasets.py --apply      # actually delete
"""
import argparse
import os
import sys

from google.cloud import bigquery
from google.api_core.exceptions import NotFound


PROJECT_ID = "forex-etl-project"
LOCATION = "europe-central2"

DATASETS_TO_DELETE = [
    "analytics_staging",
    "analytics_marts",
    "forex_analysis_staging",
    "forex_analysis_marts",
]

DATASETS_TO_KEEP = [
    "forex_analysis",  # Bronze — externl table over GCS Parquet
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete datasets. Without this flag, runs in dry-run mode.",
    )
    args = parser.parse_args()

    keyfile = os.path.expanduser("~/.gcp/forex-dbt-local.json")
    if not os.path.exists(keyfile):
        print(f"Service account key not found at {keyfile}")
        return 1

    client = bigquery.Client.from_service_account_json(
        keyfile, project=PROJECT_ID, location=LOCATION
    )

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode} ===")
    print(f"Project: {PROJECT_ID} (location: {LOCATION})")
    print(f"Will preserve: {', '.join(DATASETS_TO_KEEP)}")
    print()

    for ds_name in DATASETS_TO_DELETE:
        ds_ref = f"{PROJECT_ID}.{ds_name}"
        try:
            ds = client.get_dataset(ds_ref)
        except NotFound:
            print(f"  [skip]    {ds_name} — does not exist")
            continue

        tables = list(client.list_tables(ds))
        table_names = [t.table_id for t in tables]
        print(f"  [target]  {ds_name} (tables: {', '.join(table_names) or '<empty>'})")

        if args.apply:
            client.delete_dataset(ds_ref, delete_contents=True, not_found_ok=True)
            print(f"  [DELETED] {ds_name}")

    print()
    if not args.apply:
        print("This was a dry run. To actually delete, re-run with --apply")
    else:
        print("Done. Now run dbt to rebuild from scratch:")
        print("  dbt run --select staging.*")
        print("  dbt run --select marts.*")
    return 0


if __name__ == "__main__":
    sys.exit(main())
