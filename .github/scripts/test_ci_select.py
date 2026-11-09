"""Offline selector, Git-range and workflow contracts; no third-party packages."""

from contextlib import redirect_stdout
from io import StringIO
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import ci_select as ci


ROOT = Path(__file__).resolve().parents[2]
FULL = set(ci.SUITES)
FIXTURE = "backend/internal/service/chat/provider_event_restore_equivalence_test.go"
CHAT_FIXTURE = "backend/internal/service/chat/provider_event_restore_fixture_test.go"
REMOVED_REPORTACTOR = "backend/internal/adapters/reportactor/acceptance_sqlite_test.go"


def selected(paths=None, event="pull_request"):
    return {key for key, value in ci.plan(paths, event).items() if value is True}


def needs_for(selection):
    return {
        "select": {"result": "success", "outputs": {"plan": json.dumps(selection)}},
    } | {job: {"result": "success" if selection[suite] else "skipped"}
         for job, suite in ci.GO_JOBS.items()}


class SelectionTests(unittest.TestCase):
    def test_table(self):
        cases = [
            ("unreviewed reportactor fixture", [REMOVED_REPORTACTOR], FULL),
            ("chat fixture", [CHAT_FIXTURE], {"backend", "lint"}),
            ("chat equivalence", ["backend/internal/service/chat/provider_event_restore_equivalence_test.go"], {"backend", "lint"}),
            ("fixtures and docs", [FIXTURE, CHAT_FIXTURE, "docs/README.md"], {"backend", "lint"}),
            ("other backend tests unknown", ["backend/internal/service/project/service_test.go"], FULL),
            ("schema", ["backend/internal/storage/sqlite/migrations/0221_work_item_admission.sql"], FULL),
            ("query", ["backend/internal/storage/sqlite/queries/sessions.sql"], FULL),
            ("sqlc config", ["backend/sqlc.yaml"], FULL),
            ("generated SQL", ["backend/internal/storage/sqlite/gen/models.go"], FULL),
            ("API source", ["backend/internal/httpd/controllers/dto.go"], FULL),
            ("API registry", ["backend/internal/httpd/apispec/specgen/build.go"], FULL),
            ("generated API", ["frontend/src/api/schema.ts"], {"backend", "lint", "api"}),
            ("cloud", ["cloud/internal/billing/catalog.go"], {"cloud"}),
            ("cloud plus fixture", ["cloud/internal/billing/catalog.go", FIXTURE], {"cloud", "backend", "lint"}),
            ("Windows", ["backend/internal/process/process_windows.go"], FULL),
            ("native test", ["backend/internal/adapters/chatdriver/acp/process_windows_test.go"], FULL),
            ("Chat driver", ["backend/internal/adapters/chatdriver/codexappserver/driver.go"], FULL),
            ("Chat writer", ["backend/internal/service/chat/worker_report_actor.go"], FULL),
            ("lease", ["backend/internal/storage/sqlite/store/lease.go"], FULL),
            ("parent test", ["backend/internal/service/chat/work_item_parent_test.go"], FULL),
            ("cloud security", ["cloud/internal/auth/auth.go"], FULL),
            ("shared", ["backend/internal/domain/session.go"], FULL),
            ("backend module", ["backend/go.mod"], FULL),
            ("cloud module", ["cloud/go.sum"], FULL),
            ("workspace module", ["go.work"], FULL),
            ("root package", ["package.json"], FULL),
            ("root lock", ["package-lock.json"], FULL),
            ("workflow", [".github/workflows/go.yml"], FULL),
            ("CLI workflow", [".github/workflows/cli-e2e.yml"], FULL),
            ("selector", [".github/scripts/ci_select.py"], FULL),
            ("selector tests", [".github/scripts/test_ci_select.py"], FULL),
            ("security config", [".github/.gitleaks.toml"], FULL),
            ("CLI tests", ["test/cli/install-check.sh"], FULL),
            ("no relevant change", ["docs/README.md"], set()),
            ("empty known diff", [], set()),
            ("unknown diff", None, FULL),
            ("unknown file", ["new-subsystem/input.data"], FULL),
            ("sensitive docs", ["docs/security.md"], FULL),
            ("fixture and source", [FIXTURE, "backend/internal/adapters/reportactor/acceptance.go"], FULL),
        ]
        for name, paths, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(selected(paths), expected)

    def test_every_allowlisted_fixture(self):
        # Bind exceptions to committed Git source, not merely strings or
        # untracked working files. Future exceptions need actual reviewed source.
        tracked = ci.git("-C", str(ROOT), "ls-tree", "-r", "--name-only", "HEAD",
                         "--", *sorted(ci.FIXTURES)).decode().splitlines()
        self.assertEqual(set(tracked), ci.FIXTURES)
        for path in ci.FIXTURES:
            with self.subTest(path=path):
                self.assertEqual(selected([path]), {"backend", "lint"})
                self.assertEqual(selected([path.replace("_test.go", ".go")]), FULL)

    def test_events_are_full_even_for_fixtures_or_empty_paths(self):
        for event in ("push", "merge_group", "workflow_dispatch", "", "pull_request_target"):
            for paths in ([], [FIXTURE], None):
                with self.subTest(event=event, paths=paths):
                    self.assertEqual(selected(paths, event), FULL)

    def test_malformed_paths(self):
        for path in ("", "/docs/a.md", "docs/../a.md", "docs/./a.md", "cloud//a.go", "docs\\a.md", "docs/a\n.md"):
            with self.subTest(path=path):
                self.assertEqual(selected([path]), FULL)

    def test_order_duplicates_and_full_override(self):
        paths = [FIXTURE, CHAT_FIXTURE, "cloud/main.go", "docs/README.md"]
        for ordered in itertools.permutations(paths):
            self.assertEqual(selected(ordered), {"backend", "lint", "cloud"})
            self.assertEqual(selected([*ordered, *ordered]), selected(ordered))
            self.assertEqual(selected([*ordered, "backend/internal/service/chat/controller.go"]), FULL)

    def test_pr58_broad_source_is_full(self):
        # Representative paths from live PR58 head 23768b2e; the fixture must
        # never narrow the broad parent/lease/writer/source qualification.
        self.assertEqual(selected([
            REMOVED_REPORTACTOR,
            "backend/internal/adapters/reportactor/acceptance.go",
            "backend/internal/service/chat/worker_report_actor.go",
            "backend/internal/domain/work_item_parent.go",
            "backend/internal/storage/sqlite/migrations/0221_work_item_admission.sql",
            "frontend/src/api/schema.ts",
        ]), FULL)


class SummaryTests(unittest.TestCase):
    def check(self, needs):
        with redirect_stdout(StringIO()):
            ci.check_go(needs)

    def test_all_256_selection_combinations(self):
        for bits in itertools.product((False, True), repeat=len(ci.SUITES)):
            selection = dict(zip(ci.SUITES, bits))
            with self.subTest(selection=selection):
                self.check(needs_for(selection))

    def test_each_selected_job_requires_success(self):
        for job in ci.GO_JOBS:
            for result in ("failure", "cancelled", "skipped", "", None):
                with self.subTest(job=job, result=result):
                    needs = needs_for(ci.plan(None))
                    needs[job]["result"] = result
                    with self.assertRaises(ValueError):
                        self.check(needs)
            needs = needs_for(ci.plan(None))
            del needs[job]
            with self.assertRaises(KeyError):
                self.check(needs)

    def test_unselected_success_is_not_pass_evidence(self):
        for job in ci.GO_JOBS:
            for result in ("success", "failure", "cancelled", ""):
                needs = needs_for(ci.plan([]))
                needs[job]["result"] = result
                with self.subTest(job=job, result=result), self.assertRaises(ValueError):
                    self.check(needs)

    def test_failed_or_missing_selector_and_malformed_outputs(self):
        for result in ("failure", "cancelled", "skipped", ""):
            needs = needs_for(ci.plan(None))
            needs["select"]["result"] = result
            with self.subTest(result=result), self.assertRaises(ValueError):
                self.check(needs)
        for raw in ("", "{}", "null", '{"backend":"true"}'):
            needs = needs_for(ci.plan(None))
            needs["select"]["outputs"]["plan"] = raw
            with self.subTest(raw=raw), self.assertRaises((ValueError, KeyError, TypeError)):
                self.check(needs)

    def test_non_boolean_or_missing_selection_never_passes(self):
        for suite in ci.SUITES:
            for value in ("true", 1, None):
                selection = ci.plan(None) | {suite: value}
                with self.subTest(suite=suite, value=value), self.assertRaises(ValueError):
                    self.check(needs_for(selection))
        for job in ("select", *ci.GO_JOBS):
            needs = needs_for(ci.plan([]))
            del needs[job]
            with self.subTest(missing=job), self.assertRaises(KeyError):
                self.check(needs)


class RangeTests(unittest.TestCase):
    def test_bad_or_incomplete_diff_is_unknown(self):
        base, head, source = "a" * 40, "b" * 40, "c" * 40
        event = {"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}
        parents = f"{source} {base} {head}\n".encode()
        for raw in (b"docs/README.md", b"docs/\xff.md\0"):
            with patch.object(ci, "git", side_effect=[parents, raw]), self.assertRaises(ValueError):
                ci.changed_paths(event, source)

    def test_workflow_outputs_match_selected_plan(self):
        for event, paths in (("pull_request", [FIXTURE]), ("push", [])):
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "outputs"
                event_file = Path(directory) / "event.json"
                event_file.write_text("{}")
                with patch.dict(os.environ, {
                    "GITHUB_EVENT_NAME": event, "GITHUB_EVENT_PATH": str(event_file),
                    "GITHUB_OUTPUT": str(output),
                }), patch.object(ci, "git", return_value=b"a" * 40), patch.object(ci, "changed_paths", return_value=paths), patch.object(os.sys, "argv", ["ci_select.py"]), redirect_stdout(StringIO()):
                    ci.main()
                outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
                expected = ci.plan(paths, event)
                self.assertEqual(json.loads(outputs.pop("plan")), expected)
                self.assertEqual(outputs, {suite: str(expected[suite]).lower() for suite in ci.SUITES})

    def test_real_git_merge_rename_deletion_and_source_mismatch(self):
        # Local temporary Git repo only; no AO state, network or global config.
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.check_output([
                    "git", "-C", directory, "-c", "user.name=CI fixture",
                    "-c", "user.email=ci@example.invalid", *args,
                ], stderr=subprocess.PIPE).decode().strip()

            git("init", "-b", "main")
            root = Path(directory)
            (root / "source.go").write_text("source\n")
            git("add", ".")
            git("commit", "-m", "base")
            base = git("rev-parse", "HEAD")
            git("checkout", "-b", "fixture")
            target = root / FIXTURE
            target.parent.mkdir(parents=True)
            (root / "source.go").rename(target)
            git("add", "-A")
            git("commit", "-m", "rename source to fixture")
            head = git("rev-parse", "HEAD")
            git("checkout", "main")
            git("merge", "--no-ff", "fixture", "-m", "merge")
            source = git("rev-parse", "HEAD")
            event = {"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}
            original = ci.git
            with patch.object(ci, "git", side_effect=lambda *args: original("-C", directory, *args)):
                paths = ci.changed_paths(event, source)
                self.assertEqual(set(paths), {"source.go", FIXTURE})
                self.assertEqual(selected(paths), FULL)
                for wrong_source in (head, "0" * 40, "--output=bad"):
                    with self.assertRaises(ValueError):
                        ci.changed_paths(event, wrong_source)
                event["pull_request"]["base"]["sha"] = "a" * 40
                with self.assertRaises(ValueError):
                    ci.changed_paths(event, source)

            # checkout fetch-depth:2 contains exactly the merge's parent trees.
            # Prove selection still works without fetching repository history.
            with tempfile.TemporaryDirectory() as shallow:
                subprocess.check_output([
                    "git", "clone", "--depth=2", "--branch=main", root.as_uri(), shallow,
                ], stderr=subprocess.PIPE)
                event["pull_request"]["base"]["sha"] = base
                with patch.object(ci, "git", side_effect=lambda *args: original("-C", shallow, *args)):
                    self.assertEqual(set(ci.changed_paths(event, source)), {"source.go", FIXTURE})

    def test_unknown_range_main_falls_back_to_full(self):
        for error in (ValueError("range"), TypeError("event"), subprocess.CalledProcessError(1, "git"), KeyError("base"), OSError("event")):
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "outputs"
                event_file = Path(directory) / "event.json"
                event_file.write_text("{}")
                with patch.dict(os.environ, {
                    "GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": str(event_file),
                    "GITHUB_OUTPUT": str(output),
                }), patch.object(ci, "git", return_value=b"a" * 40), patch.object(ci, "changed_paths", side_effect=error), patch.object(os.sys, "argv", ["ci_select.py"]), redirect_stdout(StringIO()):
                    ci.main()
                selection = json.loads(output.read_text().splitlines()[0].removeprefix("plan="))
                self.assertTrue(all(selection[key] for key in ci.SUITES))


class BackendPartitionTests(unittest.TestCase):
    def setUp(self):
        self.general = [ci.BACKEND_MODULE + suffix for suffix in (
            "", "/cmd/ao", "/internal/service/chat/nested",
            "/internal/storage/sqlite/store/nested", "/internal/storage/sqlite/sqlitetest",
        )]
        self.inventory = [*ci.ISOLATED_PACKAGES.values(), *self.general]

    def test_complete_disjoint_partition_keeps_new_and_nested_packages(self):
        for packages in (self.inventory, list(reversed(self.inventory))):
            groups = ci.backend_partition(packages)
            self.assertEqual(set(groups), {"general", "chat", "store", "sqlite"})
            self.assertEqual(set(groups["general"]), set(self.general))
            for group, package in ci.ISOLATED_PACKAGES.items():
                self.assertEqual(groups[group], [package])
            assigned = list(itertools.chain.from_iterable(groups.values()))
            self.assertEqual(sorted(assigned), sorted(packages))
            self.assertEqual(len(assigned), len(set(assigned)))

    def test_empty_duplicate_malformed_or_missing_inventory_fails_closed(self):
        bad = [[], list(ci.ISOLATED_PACKAGES.values()),
               [*self.inventory, self.inventory[0]]]
        bad += [[*self.inventory, value] for value in (
            "", " ", "other/module/package", ci.BACKEND_MODULE + "-foreign/package",
            ci.BACKEND_MODULE + "/package with whitespace",
        )]
        bad += [[package for package in self.inventory if package != missing]
                for missing in ci.ISOLATED_PACKAGES.values()]
        for packages in bad:
            with self.subTest(packages=packages), self.assertRaises(ValueError):
                ci.backend_partition(packages)

    def test_actual_partition_cli_stdout_and_failure_diagnostics(self):
        script = str(ROOT / ".github/scripts/ci_select.py")
        for group in ("general", "chat", "store"):
            result = subprocess.run(
                [sys.executable, "-B", script, "--backend-packages", group],
                input="\n".join(self.inventory) + "\n", text=True, capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.splitlines(), ci.backend_partition(self.inventory)[group])
            self.assertIn("chat=1, store=1, sqlite=1, general=5", result.stderr)
            for inventory in ([], self.inventory[1:], [*self.inventory, ""]):
                result = subprocess.run(
                    [sys.executable, "-B", script, "--backend-packages", group],
                    input="\n".join(inventory) + "\n", text=True, capture_output=True,
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertIn("Backend race partition failed:", result.stderr)


class WorkflowContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.go = (ROOT / ".github/workflows/go.yml").read_text()
        cls.cli = (ROOT / ".github/workflows/cli-e2e.yml").read_text()
        cls.leaks = (ROOT / ".github/workflows/gitleaks.yml").read_text()

    def jobs(self, text):
        sections = re.split(r"^  ([a-z][a-z0-9-]*):\n", text.split("jobs:\n", 1)[1], flags=re.MULTILINE)
        return dict(zip(sections[1::2], sections[2::2]))

    def test_contexts_and_all_go_needs_preserved(self):
        jobs = self.jobs(self.go)
        self.assertEqual(set(jobs), {"select", "build-test", *ci.GO_JOBS})
        gate = jobs["build-test"]
        needs = re.search(r"needs: \[([^\]]+)\]", gate).group(1).split(", ")
        self.assertEqual(set(needs), {"select", *ci.GO_JOBS})
        self.assertIn("if: ${{ always() }}", gate)
        self.assertIn("NEEDS_JSON: ${{ toJSON(needs) }}", gate)
        self.assertIn("ci_select.py --check-go", gate)
        for job, suite in ci.GO_JOBS.items():
            self.assertIn("needs: select", jobs[job])
            self.assertIn(f"if: needs.select.outputs.{suite} == 'true'", jobs[job])
            self.assertIn(f"{suite}: ${{{{ steps.select.outputs.{suite} }}}}", jobs["select"])

    def test_host_and_shard_coverage_retained(self):
        jobs = self.jobs(self.go)
        self.assertIn("shard: [0, 1, 2, 3]", jobs["sqlite-race"])
        self.assertIn("fail-fast: false", jobs["sqlite-race"])
        self.assertIn("assert names and all(shards)", jobs["sqlite-race"])
        self.assertIn("assert executed and not missing and not unexpected", jobs["sqlite-race"])
        self.assertIn("if-no-files-found: error", jobs["sqlite-race"])
        for job in ("windows-workspace", "windows-journal"):
            self.assertIn("runs-on: windows-latest", jobs[job])
        cli_jobs = self.jobs(self.cli)
        self.assertEqual(set(cli_jobs), {"select", "native", "container"})
        for job in ("native", "container"):
            self.assertIn("needs: select", cli_jobs[job])
            self.assertIn("if: ${{ !cancelled() && (needs.select.result != 'success' || needs.select.outputs.cli != 'false') }}", cli_jobs[job])
        self.assertIn("os: [ubuntu-latest, macos-latest, windows-latest]", cli_jobs["native"])
        self.assertIn("go test -tags e2e -v ./internal/cli/...", cli_jobs["native"])
        self.assertIn("docker run --rm --init ao-cli-smoke", cli_jobs["container"])

    def test_backend_race_groups_and_selector_mapping(self):
        jobs = self.jobs(self.go)
        groups = {"backend-checks": "general", "backend-chat-race": "chat",
                  "backend-store-race": "store"}
        for job, group in groups.items():
            body = jobs[job]
            self.assertEqual(ci.GO_JOBS[job], "backend")
            self.assertIn("runs-on: ubuntu-latest", body)
            self.assertIn("go-version-file: backend/go.mod", body)
            self.assertIn("cache: true", body)
            self.assertIn("set -euo pipefail", body)
            self.assertIn("package_list=$(go list ./...)", body)
            self.assertIn(f'--backend-packages {group})', body)
            self.assertIn('mapfile -t packages <<< "$selected"', body)
            commands = re.findall(r'^\s+go test .+$', body, flags=re.MULTILINE)
            self.assertEqual(len(commands), 1)
            command = commands[0]
            self.assertIn('-race ', command)
            self.assertIn('-count=1 ', command)
            self.assertIn('-timeout=30m "${packages[@]}"', command)
            self.assertNotIn('-run', command)
            self.assertNotIn('-skip', command)
            if group != "general":
                self.assertIn('-p=1 -parallel=1 ', command)
                self.assertIn('timeout-minutes: 35', body)
        self.assertIn("go build ./...", jobs["backend-checks"])
        self.assertIn("go vet ./...", jobs["backend-checks"])

    def test_native_go_cache_covers_exact_workspace_sums_without_gating_checks(self):
        jobs = self.jobs(self.go)
        setups = 0
        for job in ci.GO_JOBS:
            body = jobs[job]
            with self.subTest(job=job):
                self.assertEqual(body.count('uses: actions/setup-go@v5'), 1)
                self.assertIn(
                    '          cache: true\n'
                    '          cache-dependency-path: |\n'
                    '            backend/go.sum\n'
                    '            cloud/go.sum\n'
                    '            go.work.sum\n', body)
                self.assertNotIn('cache-hit', body)
                self.assertNotIn('actions/cache', body)
                setups += 1
        self.assertEqual(setups, 10)

    def test_cli_output_validation_and_dependency_failure_truth_table(self):
        jobs = self.jobs(self.cli)
        validation = jobs["select"].split("- name: Validate CLI selection output", 1)[1]
        self.assertIn("CLI_SELECTION: ${{ steps.select.outputs.cli }}", validation)
        code = re.search(r'run: python3 -c "([^"\n]+)"', validation).group(1)
        # Test the actual inline validator, including Actions' case-insensitive
        # string comparison edge. Missing outputs are empty strings in needs.
        outputs = ("", "true", "false", "TRUE", "FALSE", "False", " false",
                   "false ", "false\n", '"false"', "0", "null", "{}", "garbage")
        for output in outputs:
            validated = subprocess.run(
                [sys.executable, "-c", code],
                env=dict(os.environ, CLI_SELECTION=output), capture_output=True,
            ).returncode
            self.assertEqual(validated, 0 if output in ("true", "false") else 1)
            for result, cancelled, job in itertools.product(
                ("success", "failure", "cancelled", "skipped", "", "unknown"),
                (False, True), ("native", "container"),
            ):
                # Invalid output turns an otherwise successful select into a
                # failed job through the validator's nonzero process exit.
                effective_result = "failure" if result == "success" and validated else result
                expected = not cancelled and not (result == "success" and output == "false")
                with self.subTest(output=output, result=result, cancelled=cancelled, job=job):
                    predicate = re.search(r"if: \$\{\{ (.+) \}\}", jobs[job]).group(1)
                    # Evaluate only these source predicate terms, not a general
                    # Actions engine. Native scheduling remains a hosted check.
                    predicate = predicate.replace("!cancelled()", str(not cancelled))
                    predicate = predicate.replace("needs.select.result != 'success'",
                                                  str(effective_result.casefold() != "success"))
                    predicate = predicate.replace("needs.select.outputs.cli != 'false'",
                                                  str(output.casefold() != "false"))
                    self.assertRegex(predicate, r"^[TrueFals ()&|]+$")
                    actual = eval(predicate.replace("&&", " and ").replace("||", " or "),
                                  {"__builtins__": {}}, {})
                    self.assertEqual(actual, expected)

    def test_trigger_permissions_no_masks_and_security_unconditional(self):
        for workflow in (self.go, self.cli):
            triggers = workflow.split("permissions:", 1)[0]
            self.assertIn("pull_request:\n", triggers)
            self.assertNotIn("paths:", triggers)
            self.assertIn("branches: [main]", triggers)
            self.assertIn("contents: read", workflow)
            self.assertNotIn("continue-on-error", workflow)
            self.assertNotIn("self-hosted", workflow)
            self.assertNotIn("pull_request_target", workflow)
            self.assertIn("cancel-in-progress: ${{ github.event_name == 'pull_request' }}", workflow)
            self.assertIn("fetch-depth: 2", self.jobs(workflow)["select"])
        self.assertNotIn("paths:", self.leaks)
        self.assertNotIn("if:", self.leaks)
        self.assertNotIn("needs:", self.leaks)
        self.assertEqual(set(self.jobs(self.leaks)), {"scan"})
        self.assertIn("--redact", self.leaks)


if __name__ == "__main__":
    unittest.main()
