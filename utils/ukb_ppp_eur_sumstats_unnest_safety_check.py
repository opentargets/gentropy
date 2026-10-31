"""Offline check that ukb_ppp_eur_sumstats_unnest.py deletes only after every check passes.

Temporary: delete with the unnest script. gcloud is replaced by fakes, so nothing touches GCS:

    python utils/ukb_ppp_eur_sumstats_unnest_safety_check.py

Each scenario runs `cleanup --yes` (or `apply`) against faked listings and asserts whether
`gcloud storage rm` is called, and with which objects.
"""

# ruff: noqa: T201  (command-line script: prints are its report)
from __future__ import annotations

import contextlib
import io
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import ukb_ppp_eur_sumstats_unnest as unnest  # noqa: E402

ROOT, NESTED = unnest.ROOT, unnest.NESTED
CHROMOSOMES = [str(i) for i in range(1, 23)] + ["X"]


def partition_file(
    root: str, chromosome: str, name: str, crc: str = "A", size: str = "10"
) -> dict[str, str]:
    """A listed partition file of study S.

    Args:
        root (str): Folder holding the study.
        chromosome (str): Chromosome partition.
        name (str): File name.
        crc (str): crc32c.
        size (str): Size in bytes.

    Returns:
        dict[str, str]: The file as `list_partition_files` gives it.
    """
    return {
        "studyId": "S",
        "chromosome": chromosome,
        "file": name,
        "url": f"{root}/studyId=S/chromosome={chromosome}/{name}",
        "size": size,
        "crc32c": crc,
    }


def main() -> int:  # noqa: C901
    """Run the scenarios.

    Returns:
        int: 0 when every scenario behaves as expected, 1 otherwise.
    """
    # Study S: chromosome 1 is only nested, the other 22 are at the top level.
    nested = [partition_file(NESTED, "1", "p1")]
    top_before = [partition_file(ROOT, c, f"p{c}") for c in CHROMOSOMES if c != "1"]
    top_after = [*top_before, partition_file(ROOT, "1", "p1")]
    rows = [
        {
            "studyId": "S",
            "chromosome": "1",
            "file": "p1",
            "source": nested[0]["url"],
            "destination": f"{ROOT}/studyId=S/chromosome=1/p1",
            "size": "10",
            "crc32c": "A",
            "action": "copy",
        }
    ]
    calls: list[tuple[list[str], Any]] = []

    class Done:
        """A successful process."""

        returncode, stdout, stderr = 0, "", ""

    def fake_run(cmd: list[str], *args: Any, **kwargs: Any) -> Done:
        """Record a command instead of running it.

        Args:
            cmd (list[str]): The command.
            *args (Any): Ignored.
            **kwargs (Any): `input` is recorded.

        Returns:
            Done: Success.
        """
        calls.append((cmd, kwargs.get("input")))
        return Done()

    unnest.read_manifest = lambda path: rows
    # The unnest script calls subprocess.run, so every gcloud call goes to the fake.
    setattr(subprocess, "run", fake_run)  # noqa: B010

    def deleted(
        nested_now: list[dict[str, str]], top_now: list[dict[str, str]], yes: bool
    ) -> tuple[int, list[str] | None]:
        """Run cleanup against faked listings.

        Args:
            nested_now (list[dict[str, str]]): Nested files at cleanup time.
            top_now (list[dict[str, str]]): Top-level files at cleanup time.
            yes (bool): Whether `--yes` is given.

        Returns:
            tuple[int, list[str] | None]: Exit code, and the objects passed to rm, or None.
        """
        calls.clear()
        unnest.list_partition_files = lambda prefix_glob: list(nested_now)
        unnest.top_level_files = lambda: list(top_now)
        with contextlib.redirect_stdout(io.StringIO()):
            code = unnest.cleanup("manifest.csv", yes)
        rm = [c for c in calls if c[0][:3] == ["gcloud", "storage", "rm"]]
        return code, (rm[0][1].split() if rm else None)

    def changed_top(**kwargs: str) -> list[dict[str, str]]:
        """Top level after apply, with chromosome 1's copy altered.

        Args:
            **kwargs (str): `crc` or `size` of the altered copy.

        Returns:
            list[dict[str, str]]: The listing.
        """
        return [*top_before, partition_file(ROOT, "1", "p1", **kwargs)]

    scenarios = [
        ("--yes before apply: top level incomplete", nested, top_before, True, None),
        ("dry run after apply", nested, top_after, False, None),
        ("--yes after apply", nested, top_after, True, [nested[0]["url"]]),
        (
            "--yes, a nested file appeared after the manifest",
            [*nested, partition_file(NESTED, "2", "new")],
            top_after,
            True,
            None,
        ),
        (
            "--yes, a nested file changed after the manifest",
            [partition_file(NESTED, "1", "p1", crc="B")],
            top_after,
            True,
            None,
        ),
        (
            "--yes, top-level copy with another crc32c",
            nested,
            changed_top(crc="B"),
            True,
            None,
        ),
        ("--yes, top-level copy truncated", nested, changed_top(size="9"), True, None),
        (
            "--yes, a chromosome in two files",
            nested,
            [*top_after, partition_file(ROOT, "5", "extra")],
            True,
            None,
        ),
    ]
    failures = 0
    for name, nested_now, top_now, yes, expected in scenarios:
        code, removed = deleted(nested_now, top_now, yes)
        ok = removed == expected and (code == 0) == (expected is not None or not yes)
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} {name}: exit {code}, deleted {removed}")

    calls.clear()
    unnest.list_partition_files = lambda prefix_glob: [
        *nested,
        partition_file(NESTED, "2", "new"),
    ]
    with contextlib.redirect_stdout(io.StringIO()):
        code = unnest.apply("manifest.csv", 2)
    copied = any(c[0][2] == "rsync" for c in calls)
    ok = code == 1 and not copied
    failures += not ok
    print(
        f"{'PASS' if ok else 'FAIL'} apply refuses when the nested folder changed: exit {code}"
    )

    print("ALL SCENARIOS PASSED" if not failures else f"{failures} SCENARIOS FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
