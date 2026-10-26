"""Merge the nested UKB PPP harmonised summary statistics into the top-level folder.

Temporary: delete once the summary statistics are fixed.

One harmonisation run (2025-04-11) wrote part of its output one level too deep:

    gs://ukb_ppp_eur_inputs/harmonised_summary_statistics/studyId=<study>/chromosome=<c>/
    gs://ukb_ppp_eur_inputs/harmonised_summary_statistics/harmonised_summary_statistics/studyId=<study>/chromosome=<c>/

Both folders hold files of the same Spark write: one file per chromosome partition, with the
same write ids. 436 studies are only in the nested folder, and 40 are split between the two, so
neither copy of those is complete; where both copies hold a file, it is identical. The merge
copies every nested file that is not already at the top level, which completes every study.

Steps, each run separately:

    python utils/ukb_ppp_eur_sumstats_unnest.py manifest --manifest <csv>
        Fails unless every object under the root is a partition file, at the top level or one
        level nested, or `_SUCCESS`. Lists both folders and writes one row per nested file: its destination at the top level
        and the action, `copy` (missing there), `identical` (already there, same crc32c) or
        `conflict` (the chromosome is there with different content). Read-only.
    python utils/ukb_ppp_eur_sumstats_unnest.py apply --manifest <csv>
        Copies the `copy` rows, per study with `gcloud storage rsync --no-clobber`, so nothing at
        the top level is overwritten. Refuses to run when the manifest has a conflict.
    python utils/ukb_ppp_eur_sumstats_unnest.py verify --manifest <csv>
        Lists the top level again: every manifest file must be there with its crc32c, and every
        study must have one file per chromosome for all 23 chromosomes. Read-only.
    python utils/ukb_ppp_eur_sumstats_unnest.py cleanup --manifest <csv> --yes
        Runs verify, then deletes the nested folder. Irreversible.

`--manifest` can be a local path or a gs:// URL. Requires gcloud.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import collections
import csv
import io
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

ROOT = "gs://ukb_ppp_eur_inputs/harmonised_summary_statistics"
NESTED = f"{ROOT}/harmonised_summary_statistics"
CHROMOSOMES = 23
FIELDS = [
    "studyId",
    "chromosome",
    "file",
    "source",
    "destination",
    "size",
    "crc32c",
    "action",
]
PARTITION = re.compile(r"studyId=([^/]+)/chromosome=([^/]+)/([^/]+)$")
# Every object under ROOT must have one of these shapes, relative to ROOT.
EXPECTED_SHAPES = (
    re.compile(r"_SUCCESS"),
    re.compile(r"studyId=[^/]+/chromosome=[^/]+/part-[^/]+\.parquet"),
    re.compile(
        r"harmonised_summary_statistics/studyId=[^/]+/chromosome=[^/]+/part-[^/]+\.parquet"
    ),
)


def unexpected_objects() -> list[str]:
    """Objects under ROOT that are neither partition files, one level nested at most, nor _SUCCESS.

    Returns:
        list[str]: Their URLs; empty when the layout is as expected.
    """
    names = gcloud(
        "storage", "objects", "list", f"{ROOT}/**", "--format=value(name)"
    ).split()
    prefix = ROOT.split("/", 3)[3] + "/"
    return [
        f"{ROOT}/{name[len(prefix) :]}"
        for name in names
        if not any(shape.fullmatch(name[len(prefix) :]) for shape in EXPECTED_SHAPES)
    ]


def gcloud(*args: str) -> str:
    """Run gcloud and return its output.

    Args:
        *args (str): gcloud arguments.

    Returns:
        str: Standard output.
    """
    return subprocess.run(
        ["gcloud", *args], check=True, capture_output=True, text=True
    ).stdout


def list_partition_files(prefix_glob: str) -> list[dict[str, str]]:
    """List the partition files under a prefix.

    Args:
        prefix_glob (str): gs:// glob of the objects to list.

    Returns:
        list[dict[str, str]]: `studyId`, `chromosome`, `file`, `url`, `size` and `crc32c` of
            every `studyId=*/chromosome=*/*` object.
    """
    objects = json.loads(
        gcloud(
            "storage",
            "objects",
            "list",
            prefix_glob,
            "--format=json(bucket,name,size,crc32c_hash)",
        )
        or "[]"
    )
    files = []
    for obj in objects:
        match = PARTITION.search(obj["name"])
        if match:
            files.append(
                {
                    "studyId": match.group(1),
                    "chromosome": match.group(2),
                    "file": match.group(3),
                    "url": f"gs://{obj['bucket']}/{obj['name']}",
                    "size": str(obj["size"]),
                    "crc32c": obj["crc32c_hash"],
                }
            )
    return files


def top_level_files() -> list[dict[str, str]]:
    """List the partition files at the top level, without the nested folder.

    Returns:
        list[dict[str, str]]: As `list_partition_files`.
    """
    return list_partition_files(f"{ROOT}/studyId=*/**")


def write_manifest(rows: list[dict[str, str]], path: str) -> None:
    """Write the manifest to a local path or a gs:// URL.

    Args:
        rows (list[dict[str, str]]): Manifest rows.
        path (str): Destination.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    if path.startswith("gs://"):
        subprocess.run(
            ["gcloud", "storage", "cp", "-", path],
            input=buffer.getvalue(),
            text=True,
            check=True,
        )
    else:
        with open(path, "w") as out:
            out.write(buffer.getvalue())


def read_manifest(path: str) -> list[dict[str, str]]:
    """Read the manifest from a local path or a gs:// URL.

    Args:
        path (str): Manifest location.

    Returns:
        list[dict[str, str]]: Manifest rows.
    """
    if path.startswith("gs://"):
        text = gcloud("storage", "cat", path)
    else:
        with open(path) as handle:
            text = handle.read()
    return list(csv.DictReader(io.StringIO(text)))


def manifest(path: str) -> int:
    """Build the manifest from listings of both folders.

    Args:
        path (str): Where to write the manifest.

    Returns:
        int: 0 when the manifest has no conflict, 1 otherwise.
    """
    unexpected = unexpected_objects()
    if unexpected:
        print(
            f"FAILED {len(unexpected)} objects do not fit the expected layout "
            f"(partition files at most one level nested, and _SUCCESS):"
        )
        for url in unexpected[:20]:
            print(f"  {url}")
        return 1
    print("INFO layout: only partition files, at most one level nested, and _SUCCESS")
    nested = list_partition_files(f"{NESTED}/**")
    top = top_level_files()
    at_top = {(f["studyId"], f["chromosome"]): f for f in top}
    rows = []
    for f in sorted(nested, key=lambda f: (f["studyId"], f["chromosome"], f["file"])):
        existing = at_top.get((f["studyId"], f["chromosome"]))
        if existing is None:
            action = "copy"
        elif existing["file"] == f["file"] and existing["crc32c"] == f["crc32c"]:
            action = "identical"
        else:
            action = "conflict"
        rows.append(
            {
                "studyId": f["studyId"],
                "chromosome": f["chromosome"],
                "file": f["file"],
                "source": f["url"],
                "destination": f"{ROOT}/studyId={f['studyId']}/chromosome={f['chromosome']}/{f['file']}",
                "size": f["size"],
                "crc32c": f["crc32c"],
                "action": action,
            }
        )
    write_manifest(rows, path)

    actions = collections.Counter(r["action"] for r in rows)
    nested_studies = {r["studyId"] for r in rows}
    top_studies = {f["studyId"] for f in top}
    chromosomes = collections.defaultdict(set)
    for f in top:
        chromosomes[f["studyId"]].add(f["chromosome"])
    for r in rows:
        chromosomes[r["studyId"]].add(r["chromosome"])
    incomplete = sorted(s for s, c in chromosomes.items() if len(c) < CHROMOSOMES)
    print(f"INFO manifest written to {path}: {len(rows)} nested files")
    print(f"INFO actions: {dict(actions)}")
    print(
        f"INFO nested studies: {len(nested_studies)} "
        f"(only nested: {len(nested_studies - top_studies)}, "
        f"split across both: {len(nested_studies & top_studies)})"
    )
    print(
        f"INFO studies after the merge: {len(chromosomes)}, "
        f"with fewer than {CHROMOSOMES} chromosomes: {len(incomplete)} {incomplete[:10]}"
    )
    return 1 if actions["conflict"] else 0


def apply(path: str, parallel: int) -> int:
    """Copy the nested files missing at the top level, never overwriting.

    Args:
        path (str): Manifest location.
        parallel (int): Studies copied at the same time.

    Returns:
        int: 0 on success, 1 when the manifest has a conflict or a copy fails.
    """
    rows = read_manifest(path)
    if any(r["action"] == "conflict" for r in rows):
        print("FAILED the manifest has conflicts; resolve them before copying")
        return 1
    studies = sorted({r["studyId"] for r in rows if r["action"] == "copy"})
    print(
        f"INFO copying {sum(r['action'] == 'copy' for r in rows)} files of {len(studies)} studies"
    )

    def sync(study: str) -> tuple[str, str]:
        """Copy one study's nested files to the top level.

        Args:
            study (str): Study id.

        Returns:
            tuple[str, str]: The study and an error message, empty on success.
        """
        result = subprocess.run(
            [
                "gcloud",
                "storage",
                "rsync",
                "--recursive",
                "--no-clobber",
                f"{NESTED}/studyId={study}",
                f"{ROOT}/studyId={study}",
            ],
            capture_output=True,
            text=True,
            check=False,  # a failure is reported per study, not raised
        )
        return study, "" if result.returncode == 0 else result.stderr.strip()[-500:]

    with ThreadPoolExecutor(max_workers=parallel) as pool:
        errors = [(s, e) for s, e in pool.map(sync, studies) if e]
    for study, error in errors:
        print(f"FAILED {study}: {error}")
    print("INFO copy finished" if not errors else f"{len(errors)} studies FAILED")
    return 1 if errors else 0


def verify(path: str) -> int:
    """Check the top level holds every manifest file and every study is complete.

    Args:
        path (str): Manifest location.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    rows = read_manifest(path)
    top = top_level_files()
    at_top = {(f["studyId"], f["chromosome"], f["file"]): f for f in top}
    missing = [
        r for r in rows if (r["studyId"], r["chromosome"], r["file"]) not in at_top
    ]
    different = [
        r
        for r in rows
        if (key := (r["studyId"], r["chromosome"], r["file"])) in at_top
        and at_top[key]["crc32c"] != r["crc32c"]
    ]
    files_per_chromosome = collections.Counter(
        (f["studyId"], f["chromosome"]) for f in top
    )
    chromosomes = collections.defaultdict(set)
    for f in top:
        chromosomes[f["studyId"]].add(f["chromosome"])
    incomplete = sorted(s for s, c in chromosomes.items() if len(c) < CHROMOSOMES)
    doubled = sorted({s for (s, _), n in files_per_chromosome.items() if n > 1})
    failures: dict[str, list[Any]] = {
        "manifest files missing at the top level": missing,
        "manifest files with a different crc32c at the top level": different,
        f"studies with fewer than {CHROMOSOMES} chromosomes": incomplete,
        "studies with a chromosome in several files": doubled,
    }
    for name, items in failures.items():
        print(f"{'PASS' if not items else 'FAIL'} {name}: {len(items)}")
        for item in items[:10]:
            print(f"  {item}")
    print(f"INFO top-level studies: {len(chromosomes)}, files: {len(top)}")
    ok = not any(failures.values())
    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


def cleanup(path: str, confirmed: bool) -> int:
    """Delete the nested folder once verify passes.

    Args:
        path (str): Manifest location.
        confirmed (bool): Whether `--yes` was given.

    Returns:
        int: 0 when the folder was deleted, 1 otherwise.
    """
    if not confirmed:
        print(f"FAILED cleanup deletes {NESTED} for good: pass --yes")
        return 1
    if verify(path):
        print(f"FAILED verify did not pass; {NESTED} is kept")
        return 1
    subprocess.run(["gcloud", "storage", "rm", "--recursive", NESTED], check=True)
    print(f"INFO deleted {NESTED}")
    return 0


def main() -> int:
    """Run one step.

    Returns:
        int: The step's exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=["manifest", "apply", "verify", "cleanup"])
    parser.add_argument("--manifest", required=True, help="Local path or gs:// URL.")
    parser.add_argument("--parallel", type=int, default=16)
    parser.add_argument("--yes", action="store_true", help="Confirm cleanup.")
    args = parser.parse_args()
    if args.step == "manifest":
        return manifest(args.manifest)
    if args.step == "apply":
        return apply(args.manifest, args.parallel)
    if args.step == "verify":
        return verify(args.manifest)
    return cleanup(args.manifest, args.yes)


if __name__ == "__main__":
    sys.exit(main())
