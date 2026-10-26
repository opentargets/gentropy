"""Offline check that ukb_ppp_eur_data_to_prod.py deletes only after every check passes.

Temporary: delete with the move script. gcloud is replaced by fakes, so nothing touches GCS:

    python utils/ukb_ppp_eur_data_to_prod_safety_check.py

Each scenario runs `move` against faked listings and asserts whether `gcloud storage rm` is
called, and with which objects.
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
import ukb_ppp_eur_data_to_prod as move_script  # noqa: E402

SNAP = move_script.SNAPSHOT


def obj(name: str, crc: str = "A", size: str = "10") -> dict[str, str]:
    """A listed object.

    Args:
        name (str): Name relative to the root.
        crc (str): crc32c.
        size (str): Size in bytes.

    Returns:
        dict[str, str]: The object as `list_objects` gives it.
    """
    return {"name": name, "size": size, "crc32c": crc}


def main() -> int:  # noqa: C901
    """Run the scenarios.

    Returns:
        int: 0 when every scenario behaves as expected, 1 otherwise.
    """
    source = [obj("a/1"), obj("a/2"), obj("b/x/3"), obj("a/", size="0")]
    rows = [
        {**o, "kind": "marker" if o["name"].endswith("/") else "object"} for o in source
    ]
    copied = [obj(f"{SNAP}/{o['name']}") for o in source if not o["name"].endswith("/")]
    calls: list[tuple[list[str], Any]] = []

    class Process:
        """A finished process."""

        def __init__(self, returncode: int) -> None:
            """Set the exit code.

            Args:
                returncode (int): Exit code.
            """
            self.returncode, self.stdout, self.stderr = returncode, "", "copy failed"

    def fake_run(cmd: list[str], *args: Any, **kwargs: Any) -> Process:
        """Record a command instead of running it; rsync fails when asked to.

        Args:
            cmd (list[str]): The command.
            *args (Any): Ignored.
            **kwargs (Any): `input` is recorded.

        Returns:
            Process: Success, or failure for rsync when `copy_fails` is set.
        """
        calls.append((cmd, kwargs.get("input")))
        return Process(1 if (state["copy_fails"] and "rsync" in cmd) else 0)

    setattr(subprocess, "run", fake_run)  # noqa: B010
    move_script.read_manifest = lambda path: rows
    state: dict[str, Any] = {}

    def run_move(
        before: list[dict[str, str]],
        during: list[dict[str, str]],
        snapshot: list[dict[str, str]],
        copy_fails: bool = False,
    ) -> tuple[int, list[str] | None]:
        """Run move against faked listings.

        Args:
            before (list[dict[str, str]]): Source objects at step 1.
            during (list[dict[str, str]]): Source objects at step 4 and after.
            snapshot (list[dict[str, str]]): Objects under the snapshot after the copy.
            copy_fails (bool): Whether rsync fails.

        Returns:
            tuple[int, list[str] | None]: Exit code, and the objects passed to rm, or None.
        """
        calls.clear()
        state.update(copy_fails=copy_fails, listings=0)

        def list_objects(glob: str) -> list[dict[str, str]]:
            """Fake listing: the source changes after the first look.

            Args:
                glob (str): gs:// glob.

            Returns:
                list[dict[str, str]]: The objects.
            """
            if f"/{SNAP}/" in glob:
                return list(snapshot)
            if any(c[0][:3] == ["gcloud", "storage", "rm"] for c in calls):
                return []  # the fake rm deleted the source
            state["listings"] += 1
            return list(before if state["listings"] == 1 else during)

        move_script.list_objects = list_objects
        with contextlib.redirect_stdout(io.StringIO()):
            code = move_script.move("manifest.csv", 2)
        rm = [c for c in calls if c[0][:3] == ["gcloud", "storage", "rm"]]
        return code, (rm[0][1].split() if rm else None)

    root = move_script.root
    everything = [f"{root}/{o['name']}" for o in source]
    scenarios = [
        ("everything consistent", source, source, copied, False, everything),
        (
            "a source object added before the copy",
            [*source, obj("c/new")],
            [*source, obj("c/new")],
            copied,
            False,
            None,
        ),
        ("a copy fails", source, source, copied, True, None),
        ("a copy is missing", source, source, copied[:-1], False, None),
        (
            "a copy has another crc32c",
            source,
            source,
            [*copied[:-1], obj(copied[-1]["name"], crc="B")],
            False,
            None,
        ),
        (
            "a source object changes during the copy",
            source,
            [*source[:-2], obj("b/x/3", crc="B"), source[-1]],
            copied,
            False,
            None,
        ),
        (
            "a source object added during the copy",
            source,
            [*source, obj("c/new")],
            copied,
            False,
            None,
        ),
    ]
    failures = 0
    for name, before, during, snapshot, copy_fails, expected in scenarios:
        code, removed = run_move(before, during, snapshot, copy_fails)
        ok = removed == expected and ((code == 0) == (expected is not None))
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} {name}: exit {code}, deleted {removed}")

    def run_finish(
        left: list[dict[str, str]], snapshot: list[dict[str, str]]
    ) -> tuple[int, list[str] | None]:
        """Run finish against faked listings.

        Args:
            left (list[dict[str, str]]): Source objects still outside prod/.
            snapshot (list[dict[str, str]]): Objects under the snapshot.

        Returns:
            tuple[int, list[str] | None]: Exit code, and the objects passed to rm, or None.
        """
        calls.clear()
        state.update(copy_fails=False)
        removed: set[str] = set()

        def list_objects(glob: str) -> list[dict[str, str]]:
            """Fake listing: objects passed to rm are gone.

            Args:
                glob (str): gs:// glob.

            Returns:
                list[dict[str, str]]: The objects.
            """
            if f"/{SNAP}/" in glob:
                return list(snapshot)
            for cmd, given in calls:
                if cmd[:3] == ["gcloud", "storage", "rm"] and given:
                    removed.update(u.removeprefix(f"{root}/") for u in given.split())
            return [o for o in left if o["name"] not in removed]

        move_script.list_objects = list_objects
        with contextlib.redirect_stdout(io.StringIO()):
            code = move_script.finish("manifest.csv", 3)
        rm = sorted(
            u
            for c in calls
            if c[0][:3] == ["gcloud", "storage", "rm"] and c[1]
            for u in c[1].split()
        )
        return code, (rm or None)

    half = [source[1], source[3]]
    finish_scenarios = [
        (
            "finish: half deleted",
            half,
            copied,
            sorted(f"{root}/{o['name']}" for o in half),
            0,
        ),
        ("finish: nothing left", [], copied, None, 0),
        (
            "finish: a left object changed",
            [obj("a/2", crc="B"), source[3]],
            copied,
            None,
            1,
        ),
        ("finish: snapshot missing an object", half, copied[:-1], None, 1),
        (
            "finish: an unlisted object is kept",
            [*half, obj("c/new")],
            copied,
            sorted(f"{root}/{o['name']}" for o in half),
            0,  # the unlisted object is reported and kept, not deleted
        ),
    ]
    for name, left, snapshot, expected, expected_code in finish_scenarios:
        code, removed_urls = run_finish(left, snapshot)
        ok = removed_urls == expected and code == expected_code
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} {name}: exit {code}, deleted {removed_urls}")

    print("ALL SCENARIOS PASSED" if not failures else f"{failures} SCENARIOS FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
