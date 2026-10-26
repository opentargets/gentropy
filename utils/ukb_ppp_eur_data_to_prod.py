"""Move everything in gs://ukb_ppp_eur_data under gs://ukb_ppp_eur_data/prod/<snapshot>/.

Temporary: delete once the bucket is reorganised.

Every object keeps its path below the new prefix: `gs://ukb_ppp_eur_data/X` becomes
`gs://ukb_ppp_eur_data/prod/20260526T131650/X`. The snapshot is the creation time of
`credible_set_clean/20250129` (2026-05-26T13:16:50Z), the credible sets the release reads.

The bucket has neither soft delete nor versioning, so the delete in `move` is final. Steps:

    python utils/ukb_ppp_eur_data_to_prod.py manifest --manifest <csv>
        Read-only. Lists every object outside `prod/` with its size and crc32c, and fails when
        the snapshot prefix already holds anything. Folder markers (names ending in `/`) are
        listed as `marker`: they are deleted with the move but not copied.
    python utils/ukb_ppp_eur_data_to_prod.py move --manifest <csv>
        1. Refuses when the objects outside `prod/` no longer match the manifest exactly.
        2. Copies each top-level folder with `gcloud storage rsync --no-clobber`.
        3. Verify: every manifest object is under the snapshot with its size and crc32c.
        4. Only when 1 and 3 hold, and again after re-listing: deletes exactly the manifest's
           objects (`rm -I`), never a folder recursively and never anything under `prod/`.
        5. Verifies the snapshot again, and that none of the manifest's objects is left.
        Stops at the first failing step, before any delete.
    python utils/ukb_ppp_eur_data_to_prod.py verify --manifest <csv>
        Read-only: step 3.

`--manifest` can be a local path or a gs:// URL. Requires gcloud.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import argparse
import collections
import csv
import io
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

ROOT = "gs://ukb_ppp_eur_data"
SNAPSHOT = "prod/20260526T131650"
# Set from --root, for a rehearsal on a test prefix: the bucket and the prefix within it.
root = ROOT
FIELDS = ["name", "size", "crc32c", "kind"]


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


def list_objects(glob: str) -> list[dict[str, str]]:
    """List objects with their size and crc32c.

    Args:
        glob (str): gs:// glob.

    Returns:
        list[dict[str, str]]: `name` (relative to the root), `size` and `crc32c` of every
            object.
    """
    objects = json.loads(
        gcloud(
            "storage",
            "objects",
            "list",
            glob,
            "--format=json(name,size,crc32c_hash)",
        )
        or "[]"
    )
    prefix = (
        root.removeprefix("gs://").split("/", 1)[1] + "/" if root.count("/") > 2 else ""
    )
    return [
        {
            "name": o["name"][len(prefix) :],
            "size": str(o.get("size", 0)),
            "crc32c": o.get("crc32c_hash", ""),
        }
        for o in objects
        if o["name"].startswith(prefix)
    ]


def source_objects() -> list[dict[str, str]]:
    """Every object of the bucket outside `prod/`.

    Returns:
        list[dict[str, str]]: As `list_objects`.
    """
    return [o for o in list_objects(f"{root}/**") if not o["name"].startswith("prod/")]


def snapshot_objects() -> dict[str, dict[str, str]]:
    """Objects under the snapshot, by their name relative to it.

    Returns:
        dict[str, dict[str, str]]: Objects keyed by their path below the snapshot.
    """
    prefix = f"{SNAPSHOT}/"
    return {
        o["name"][len(prefix) :]: o
        for o in list_objects(f"{root}/{SNAPSHOT}/**")
        if o["name"].startswith(prefix)
    }


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


def report(checks: dict[str, list[Any]]) -> bool:
    """Print checks and whether they all pass.

    Args:
        checks (dict[str, list[Any]]): Check name and its failing items.

    Returns:
        bool: True when no check has a failing item.
    """
    for name, items in checks.items():
        print(f"{'PASS' if not items else 'FAIL'} {name}: {len(items)}")
        for item in items[:10]:
            print(f"  {item}")
    return not any(checks.values())


def top_level(name: str) -> str:
    """The top-level folder of an object, or the object itself at the root.

    Args:
        name (str): Object name.

    Returns:
        str: First path element.
    """
    return name.split("/", 1)[0]


def manifest(path: str) -> int:
    """List every object outside `prod/` into the manifest.

    Args:
        path (str): Where to write the manifest.

    Returns:
        int: 0 when written, 1 when the snapshot is not empty.
    """
    existing = snapshot_objects()
    if existing:
        print(f"FAILED {root}/{SNAPSHOT}/ already holds {len(existing)} objects")
        return 1
    rows = [
        {**o, "kind": "marker" if o["name"].endswith("/") else "object"}
        for o in sorted(source_objects(), key=lambda o: o["name"])
    ]
    write_manifest(rows, path)
    print(f"INFO manifest written to {path}: {len(rows)} objects")
    per_folder: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0])
    for r in rows:
        counts = per_folder[top_level(r["name"])]
        counts[0 if r["kind"] == "object" else 1] += 1
        counts[2] += int(r["size"])
    print(f"{'top-level folder':28} {'objects':>9} {'markers':>8} {'GB':>9}")
    for folder, (objects, markers, size) in sorted(per_folder.items()):
        print(f"{folder:28} {objects:9} {markers:8} {size / 1e9:9.2f}")
    total = sum(int(r["size"]) for r in rows)
    print(f"{'total':28} {len(rows):9} {'':8} {total / 1e9:9.2f}")
    return 0


def source_differences(rows: list[dict[str, str]]) -> dict[str, list[str]]:
    """Compare the objects outside `prod/` as they are now with the manifest.

    Args:
        rows (list[dict[str, str]]): Manifest rows.

    Returns:
        dict[str, list[str]]: Objects added, gone and changed since the manifest.
    """
    now = {o["name"]: (o["size"], o["crc32c"]) for o in source_objects()}
    listed = {r["name"]: (r["size"], r["crc32c"]) for r in rows}
    return {
        "objects outside prod/ not in the manifest": sorted(set(now) - set(listed)),
        "manifest objects gone from outside prod/": sorted(set(listed) - set(now)),
        "objects changed since the manifest": sorted(
            n for n in set(now) & set(listed) if now[n] != listed[n]
        ),
    }


def verify(path: str) -> int:
    """Check every manifest object is under the snapshot with its size and crc32c.

    Args:
        path (str): Manifest location.

    Returns:
        int: 0 when every check passes, 1 otherwise.
    """
    rows = [r for r in read_manifest(path) if r["kind"] == "object"]
    copied = snapshot_objects()
    ok = report(
        {
            "manifest objects missing under the snapshot": [
                r["name"] for r in rows if r["name"] not in copied
            ],
            "manifest objects with another size or crc32c under the snapshot": [
                r["name"]
                for r in rows
                if r["name"] in copied
                and (copied[r["name"]]["size"], copied[r["name"]]["crc32c"])
                != (r["size"], r["crc32c"])
            ],
        }
    )
    print(
        f"INFO manifest objects: {len(rows)}, objects under the snapshot: {len(copied)}"
    )
    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


def copy(rows: list[dict[str, str]], parallel: int) -> list[str]:
    """Copy each top-level folder, and each root object, below the snapshot.

    Args:
        rows (list[dict[str, str]]): Manifest rows.
        parallel (int): Folders copied at the same time.

    Returns:
        list[str]: Errors; empty on success.
    """
    names = [r["name"] for r in rows if r["kind"] == "object"]
    folders = sorted({top_level(n) for n in names if "/" in n})
    root_objects = sorted(n for n in names if "/" not in n)

    def run(command: list[str]) -> str:
        """Run one copy command.

        Args:
            command (list[str]): gcloud command.

        Returns:
            str: Error message, empty on success.
        """
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,  # a failure is reported per folder, not raised
        )
        return (
            ""
            if result.returncode == 0
            else f"{command[-2]}: {result.stderr.strip()[-500:]}"
        )

    commands = [
        [
            "gcloud",
            "storage",
            "rsync",
            "--recursive",
            "--no-clobber",
            f"{root}/{folder}",
            f"{root}/{SNAPSHOT}/{folder}",
        ]
        for folder in folders
    ] + [
        [
            "gcloud",
            "storage",
            "cp",
            "--no-clobber",
            f"{root}/{name}",
            f"{root}/{SNAPSHOT}/{name}",
        ]
        for name in root_objects
    ]
    print(f"INFO copying {len(folders)} folders and {len(root_objects)} root objects")
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        return [e for e in pool.map(run, commands) if e]


def move(path: str, parallel: int) -> int:
    """Copy, verify, and only then delete the originals.

    Args:
        path (str): Manifest location.
        parallel (int): Folders copied at the same time.

    Returns:
        int: 0 when the move completed and verified, 1 at the first failing step.
    """
    rows = read_manifest(path)
    print("== 1. the objects outside prod/ match the manifest")
    if not report(source_differences(rows)):
        print("FAILED nothing copied or deleted: the bucket changed since the manifest")
        return 1
    print("== 2. copy (never overwrites)")
    errors = copy(rows, parallel)
    for error in errors:
        print(f"FAILED {error}")
    if errors:
        print("FAILED nothing deleted: copy errors")
        return 1
    print("== 3. verify the copy")
    if verify(path):
        print("FAILED nothing deleted: verify did not pass")
        return 1
    print("== 4. delete the originals")
    if not report(source_differences(rows)):
        print("FAILED nothing deleted: the bucket changed during the copy")
        return 1
    names = [r["name"] for r in rows]
    if any(n.startswith("prod/") for n in names):
        print("FAILED nothing deleted: a manifest object is under prod/")
        return 1
    if verify(path):
        print("FAILED nothing deleted: verify did not pass on the second listing")
        return 1
    subprocess.run(
        ["gcloud", "storage", "rm", "-I"],
        input="".join(f"{root}/{n}\n" for n in names),
        text=True,
        check=True,
    )
    print(f"INFO deleted {len(names)} objects")
    print("== 5. after the delete")
    left = source_objects()
    listed = {r["name"] for r in rows}
    ok = report(
        {
            "manifest objects still outside prod/": [
                o["name"] for o in left if o["name"] in listed
            ]
        }
    )
    return 0 if ok and verify(path) == 0 else 1


def main() -> int:
    """Run one step.

    Returns:
        int: The step's exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=["manifest", "move", "verify"])
    parser.add_argument("--manifest", required=True, help="Local path or gs:// URL.")
    parser.add_argument("--parallel", type=int, default=8)
    parser.add_argument(
        "--root",
        default=ROOT,
        help="Bucket, or a test prefix, to reorganise. Defaults to %(default)s.",
    )
    args = parser.parse_args()
    global root  # noqa: PLW0603 (one-off script: --root sets the module-wide root)
    root = args.root.rstrip("/")
    print(f"INFO root: {root}, snapshot: {root}/{SNAPSHOT}/")
    if args.step == "manifest":
        return manifest(args.manifest)
    if args.step == "move":
        return move(args.manifest, args.parallel)
    return verify(args.manifest)


if __name__ == "__main__":
    sys.exit(main())
