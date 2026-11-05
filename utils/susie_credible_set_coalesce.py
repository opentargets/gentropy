"""Rewrite a migrated credible set snapshot with one file per dataset, verified row by row.

Temporary: delete with the migration scripts. Runs locally with gentropy and the GCS connector
(application-default credentials):

    python utils/susie_credible_set_coalesce.py local --source <gs snapshot> --local <dir>
        Reads credible_set, credible_set_variant and study from the source snapshot through the
        gentropy dataset classes (schema validation), writes each with one partition to <dir>,
        reads it back and checks it holds exactly the same rows as the source.
    python utils/susie_credible_set_coalesce.py verify --source <gs snapshot> --target <path>
        Checks a written snapshot (local or gs://) against the source: it loads as the gentropy
        dataset, has one parquet file per dataset, and holds exactly the same rows (counts, and
        rows missing either way with duplicates counted, every column compared).
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_index import StudyIndex

DATASETS = {
    "credible_set": CredibleSet,
    "credible_set_variant": CredibleSetVariant,
    "study": StudyIndex,
}


def session() -> Session:
    """A local gentropy session that reads and writes gs:// with application-default credentials.

    Returns:
        Session: The session.
    """
    return Session(
        add_gcs_connector=True,
        gcs_configuration={
            "auth_type": "APPLICATION_DEFAULT",
            "project_id": "open-targets-genetics-dev",
        },
        write_mode="errorifexists",
        extended_spark_conf={"spark.driver.memory": "8g"},
    )


def load(s: Session, path: str, name: str) -> DataFrame:
    """Load one dataset through its gentropy class.

    Args:
        s (Session): Session.
        path (str): Snapshot root.
        name (str): Dataset folder.

    Returns:
        DataFrame: The dataset, validated against its schema.
    """
    return DATASETS[name].from_parquet(s, f"{path}/{name}").df


def parquet_files(s: Session, path: str) -> list[str]:
    """The parquet files directly under a folder.

    Args:
        s (Session): Session.
        path (str): Folder, local or gs://.

    Returns:
        list[str]: Their names.
    """
    jvm = s.spark.sparkContext._jvm
    conf = s.spark.sparkContext._jsc.hadoopConfiguration()
    folder = jvm.org.apache.hadoop.fs.Path(path)
    return sorted(
        status.getPath().getName()
        for status in folder.getFileSystem(conf).listStatus(folder)
        if status.getPath().getName().endswith(".parquet")
    )


def compare(s: Session, source: str, target: str) -> bool:
    """Check every dataset of the target against the source.

    Args:
        s (Session): Session.
        source (str): Source snapshot.
        target (str): Written snapshot.

    Returns:
        bool: True when every check passes.
    """
    ok = True
    for name in DATASETS:
        a = load(s, source, name)
        b = load(s, target, name)
        files = parquet_files(s, f"{target}/{name}")
        n_a, n_b = a.count(), b.count()
        only_source = a.exceptAll(b).count()
        only_target = b.exceptAll(a).count()
        checks = {
            "loads as the gentropy dataset": True,
            "one parquet file": len(files) == 1,
            "same row count": n_a == n_b,
            "same columns": a.columns == b.columns,
            "rows missing from the target": only_source == 0,
            "rows not in the source": only_target == 0,
        }
        for check, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'} {name}: {check}")
        print(
            f"INFO {name}: source rows {n_a}, target rows {n_b}, "
            f"missing {only_source}, extra {only_target}, files {files}"
        )
        ok = ok and all(checks.values())
    return ok


def main() -> int:
    """Run one step.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=["local", "verify"])
    parser.add_argument("--source", required=True, help="Source snapshot, gs://.")
    parser.add_argument("--local", help="Local directory to write (step local).")
    parser.add_argument("--target", help="Snapshot to verify (step verify).")
    args = parser.parse_args()
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
    s = session()
    if args.step == "local":
        for name in DATASETS:
            df = load(s, args.source, name)
            df.coalesce(1).write.mode("errorifexists").parquet(f"{args.local}/{name}")
            print(f"INFO wrote {name} to {args.local}/{name}")
        target = args.local
    else:
        target = args.target
    ok = compare(s, args.source, target)
    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
