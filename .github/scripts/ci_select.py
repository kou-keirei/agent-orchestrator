#!/usr/bin/env python3
"""Conservative CI selection; standard library only. Run tests with unittest.

Only the named test fixtures have a backend-only proof: backend-checks still
builds/vets everything; the general, chat and store jobs race-test every package
outside the SQLite matrix, and lint still checks the backend. They change no
production storage, generated contracts, cloud, CLI or native Windows suites.
Every other backend change, including lease/parent/Chat writer tests, is full.
This is path selection, not permission to reuse a skipped suite as a PASS.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


SUITES = ("backend", "sqlite", "windows", "cloud", "lint", "sqlc", "api", "cli")
GO_JOBS = {
    "backend-checks": "backend", "backend-chat-race": "backend",
    "backend-store-race": "backend", "sqlite-race": "sqlite",
    "windows-workspace": "windows", "windows-journal": "windows",
    "cloud-build-test": "cloud", "lint": "lint", "sqlc-drift": "sqlc",
    "api-drift": "api",
}
BACKEND_MODULE = "github.com/aoagents/agent-orchestrator/backend"
ISOLATED_PACKAGES = {
    "chat": BACKEND_MODULE + "/internal/service/chat",
    "store": BACKEND_MODULE + "/internal/storage/sqlite/store",
    "sqlite": BACKEND_MODULE + "/internal/storage/sqlite",
}
FIXTURES = frozenset({
    "backend/internal/service/chat/provider_event_restore_fixture_test.go",
    "backend/internal/service/chat/provider_event_restore_equivalence_test.go",
})
SHA = re.compile(r"[0-9a-f]{40}")
# These names override even otherwise isolated cloud/docs changes. Unknown
# backend paths already fall back to full, without maintaining an import graph.
SENSITIVE = re.compile(
    r"security|auth|credential|secret|lease|parent|writer|invariant|"
    r"windows|darwin|linux|workflow|ci_select|go\.(?:mod|sum)|go\.work",
    re.IGNORECASE,
)


def plan(paths=None, event="pull_request"):
    selected = dict.fromkeys(SUITES, False)
    reason = "isolated paths"
    if event != "pull_request" or paths is None:
        return dict.fromkeys(SUITES, True) | {"reason": "push or unknown diff"}
    for path in paths:
        if (not path or path.startswith("/") or "\\" in path
                or any(part in ("", ".", "..") for part in path.split("/"))
                or any(ord(char) < 32 for char in path)):
            reason = "uncertain path"
            break
        if SENSITIVE.search(path) or path.startswith(".github/"):
            reason = "shared, security, host or selection change"
            break
        if path in FIXTURES:
            selected["backend"] = selected["lint"] = True
        elif path.startswith("cloud/"):
            selected["cloud"] = True
        elif path == "frontend/src/api/schema.ts":
            selected["backend"] = selected["lint"] = selected["api"] = True
        elif (path.startswith("docs/") and path.endswith(".md")) or path in {
            "README.md", "CHANGELOG.md", "LICENSE",
        }:
            pass
        else:
            reason = "backend production or unclassified change"
            break
    else:
        return selected | {"reason": reason}
    return dict.fromkeys(SUITES, True) | {"reason": reason}


def git(*args):
    return subprocess.check_output(["git", *args], stderr=subprocess.PIPE)


def changed_paths(event, source):
    """Diff the actual PR merge tree against its base, never a truncated API list.

    checkout's default PR merge must have the event base/head as its parents.
    No fetches, network, shell evaluation or branch-name interpolation here.
    --no-renames includes BOTH sides of renames, so moving a source into a
    fixture/docs path cannot hide its deletion. Missing history means full CI.
    """
    base = event["pull_request"]["base"]["sha"]
    head = event["pull_request"]["head"]["sha"]
    if not all(isinstance(sha, str) and SHA.fullmatch(sha)
               for sha in (base, head, source)):
        raise ValueError("invalid event SHA")
    parents = git("rev-list", "--parents", "-n", "1", "HEAD").decode().split()
    if parents != [source, base, head]:
        raise ValueError("checkout is not the event PR merge")
    raw = git("diff", "--name-only", "--no-renames", "-z", base, source, "--")
    if raw and not raw.endswith(b"\0"):
        raise ValueError("incomplete path list")
    return [name.decode("utf-8") for name in raw.split(b"\0")[:-1]]


def check_go(needs):
    """The summary accepts only success for selected jobs, explicit N/A otherwise.

    Missing, failed, cancelled or unexpectedly skipped required jobs fail. A
    skipped matrix result is accepted ONLY when the successful selector proved
    that suite inapplicable. Never present that as successful test execution.
    """
    selector = needs["select"]
    if selector["result"] != "success":
        raise ValueError("selector did not succeed")
    selection = json.loads(selector["outputs"]["plan"])
    if any(type(selection[key]) is not bool for key in SUITES):
        raise ValueError("invalid selection")
    for job, suite in GO_JOBS.items():
        result = needs[job]["result"]
        expected = "success" if selection[suite] else "skipped"
        if result != expected:
            raise ValueError(f"{job}: expected {expected}, got {result}")
        print(f"{job}: {'PASS' if selection[suite] else 'NOT APPLICABLE (not run)'}")


def backend_partition(packages):
    """Partition the complete go list inventory, failing before any tests run.

    The SQLite package stays in its existing four test shards. New packages
    belong to general automatically; exact matches alone isolate chat/store.
    """
    if not packages or any(not package or any(char.isspace() for char in package)
                           or not (package == BACKEND_MODULE or
                                   package.startswith(BACKEND_MODULE + "/"))
                           for package in packages):
        raise ValueError("invalid or empty backend go list inventory")
    if len(packages) != len(set(packages)):
        raise ValueError("duplicate packages in backend go list inventory")
    missing = sorted(set(ISOLATED_PACKAGES.values()) - set(packages))
    if missing:
        raise ValueError(f"missing isolated backend packages: {missing}")
    groups = {name: [package] for name, package in ISOLATED_PACKAGES.items()}
    groups["general"] = [package for package in packages
                         if package not in ISOLATED_PACKAGES.values()]
    if not all(groups.values()):
        raise ValueError("empty backend race group")
    assigned = [package for group in groups.values() for package in group]
    if len(assigned) != len(set(assigned)) or set(assigned) != set(packages):
        raise ValueError("backend race partition is not exhaustive and disjoint")
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--check-go", action="store_true")
    actions.add_argument("--backend-packages", choices=("general", "chat", "store"))
    args = parser.parse_args()
    if args.backend_packages:
        try:
            groups = backend_partition(sys.stdin.read().splitlines())
        except ValueError as err:
            parser.exit(1, f"Backend race partition failed: {err}\n")
        print("Backend race partition: " + ", ".join(
            f"{name}={len(group)}" for name, group in groups.items()), file=sys.stderr)
        print("\n".join(groups[args.backend_packages]))
        return
    if args.check_go:
        check_go(json.loads(os.environ["NEEDS_JSON"]))
        return
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    source = git("rev-parse", "HEAD").decode().strip()
    paths = None
    if event_name == "pull_request":
        try:
            event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
            paths = changed_paths(event, os.environ.get("GITHUB_SHA", ""))
        except (KeyError, TypeError, OSError, ValueError, subprocess.CalledProcessError) as err:
            # The fallback is a full matrix, not a successful empty selection.
            print(f"Unknown diff ({type(err).__name__}); selecting full CI", file=sys.stderr)
    selection = plan(paths, event_name)
    output = json.dumps(selection, sort_keys=True, separators=(",", ":"))
    print(f"source={source} paths={len(paths) if paths is not None else 'unknown'} {output}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        stream.write(f"plan={output}\n")
        for suite in SUITES:
            stream.write(f"{suite}={str(selection[suite]).lower()}\n")


if __name__ == "__main__":
    main()
