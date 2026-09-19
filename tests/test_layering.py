"""The layering rule, asserted rather than documented.

`core/`, `targets/` and `obs/` must not import a web framework, and `cli.py` must
not import one at module scope. That is what makes "everything except `serve`
runs on a bare standard library" true, and it is exactly the kind of property
that decays the first time someone needs a type from `fastapi` in a hurry.

Checking `sys.modules` after an import was the obvious way to test this and it
is the wrong one: it cannot tell a hard dependency from an optional import that
happened to succeed, so it passes on a bare CI runner and fails on any developer
machine that has the extras installed. Reading the source says what the code
requires, independently of what the machine happens to have.
"""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[1]

WEB_STACK = {"fastapi", "uvicorn", "starlette", "pydantic"}

# Modules that are allowed to import it, and nothing else.
API_LAYER = {"api/server.py"}


def module_level_imports(path: Path) -> set[str]:
    """Root package names imported when the module is imported.

    Bodies of functions are skipped on purpose: an import inside `cmd_serve` is
    paid only by the caller who asked for the server, which is the whole point.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:
                    found.add(node.module.split(".")[0])
            elif isinstance(node, (ast.If, ast.Try)):
                # Conditional and try/except imports still run at import time.
                visit(node.body)
                visit(node.orelse)
                visit(getattr(node, "finalbody", []))
                for handler in getattr(node, "handlers", []):
                    visit(handler.body)
            elif isinstance(node, ast.ClassDef):
                visit(node.body)
            # FunctionDef / AsyncFunctionDef deliberately not descended into.

    visit(tree.body)
    return found


def source_files() -> list[Path]:
    files = [ROOT / "cli.py"]
    for package in ("core", "targets", "obs", "api"):
        files.extend(sorted((ROOT / package).glob("*.py")))
    return files


class LayeringTests(unittest.TestCase):
    def test_only_api_server_imports_the_web_stack(self) -> None:
        offenders: list[str] = []
        for path in source_files():
            relative = path.relative_to(ROOT).as_posix()
            if relative in API_LAYER:
                continue
            leaked = module_level_imports(path) & WEB_STACK
            if leaked:
                offenders.append(f"{relative} imports {sorted(leaked)}")
        self.assertEqual(offenders, [], "web framework leaked out of api/server.py")

    def test_api_server_really_is_the_one_that_has_it(self) -> None:
        # If this ever fails the rule above has become vacuous, which is the
        # failure mode of every "nothing does X" assertion.
        self.assertTrue(module_level_imports(ROOT / "api" / "server.py") & WEB_STACK)

    def test_core_does_not_import_targets(self) -> None:
        # The scheduler holds `dict[str, Target]` handed to it at construction.
        # The moment `core/` names a concrete target, adding a new one stops
        # being free and the contract stops being the seam.
        for path in sorted((ROOT / "core").glob("*.py")):
            self.assertNotIn(
                "targets",
                module_level_imports(path),
                f"{path.name} imports a concrete target",
            )

    def test_the_standard_library_is_enough_for_core(self) -> None:
        allowed = {"api", "core", "targets", "obs"} | set(sys.stdlib_module_names)
        for package in ("core", "obs"):
            for path in sorted((ROOT / package).glob("*.py")):
                outside = module_level_imports(path) - allowed
                # `cryptography` is an optional import in core/auth.py, guarded
                # by try/except with a working stdlib fallback, so it is listed
                # here rather than silently tolerated.
                outside -= {"cryptography"}
                self.assertEqual(
                    outside, set(), f"{path.name} needs third-party {sorted(outside)}"
                )


if __name__ == "__main__":
    unittest.main()
