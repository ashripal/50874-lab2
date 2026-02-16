#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class VersionSpec:
    idx: int
    exe: str  # executable path/name


def _render_cmd_help() -> str:
    return (
        "VERSION EXECUTABLE INTERFACE (REQUIRED)\n"
        "  Each --versionN must be the name/path of an executable program.\n"
        "\n"
        "  The driver will invoke each version as:\n"
        "    <version_exe> --input <input.ndjson> --output <output.ndjson>\n"
        "\n"
        "  Examples:\n"
        "    --version1 ./v1/taxcalc\n"
        "    --version1 python3 --version2 ./v2/taxcalc   (NOT supported: version args must be executables, not interpreters)\n"
        "\n"
        "  Notes:\n"
        "    - If your implementation is a script (e.g., Python), wrap it in an executable shim script,\n"
        "      or add a shebang and chmod +x it.\n"
        "    - The program must write NDJSON output to the file path provided by --output.\n"
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    epilog = (
        "OUTPUT FILES\n"
        "  For output prefix P and K versions provided, this tool writes:\n"
        "    P-1.ndjson  (output from version 1)\n"
        "    P-2.ndjson  (output from version 2, if provided)\n"
        "    P-3.ndjson  (output from version 3, if provided)\n"
        "\n"
        + _render_cmd_help()
    )

    p = argparse.ArgumentParser(
        prog="nversion_driver.py",
        description=(
            "Run 1–3 independent implementations on the same NDJSON input and save each output.\n"
            "This tool performs no analysis; it only executes and records outputs."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=epilog,
    )

    p.add_argument(
        "--version1",
        required=True,
        metavar="EXE",
        help="Executable for implementation 1 (required). See VERSION EXECUTABLE INTERFACE below.",
    )
    p.add_argument(
        "--version2",
        required=False,
        default=None,
        metavar="EXE",
        help="Executable for implementation 2 (optional). See VERSION EXECUTABLE INTERFACE below.",
    )
    p.add_argument(
        "--version3",
        required=False,
        default=None,
        metavar="EXE",
        help="Executable for implementation 3 (optional). See VERSION EXECUTABLE INTERFACE below.",
    )

    p.add_argument(
        "--inputFile",
        required=True,
        metavar="PATH",
        help="Path to input NDJSON file (one JSON object per line).",
    )
    p.add_argument(
        "--outputFilePrefix",
        required=True,
        metavar="PREFIX",
        help="Prefix for output NDJSON files. Outputs are PREFIX-1.ndjson, PREFIX-2.ndjson, PREFIX-3.ndjson.",
    )

    p.add_argument(
        "--timeoutSeconds",
        type=int,
        default=60,
        metavar="S",
        help="Per-version timeout in seconds (default: 60).",
    )
    p.add_argument(
        "--cwd",
        default=None,
        metavar="DIR",
        help="Optional working directory to run all versions from (default: current directory).",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print each subprocess stdout/stderr on success.",
    )

    return p.parse_args(argv)


def _ensure_parent_dir(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _run_one(
    version: VersionSpec,
    input_path: Path,
    output_path: Path,
    timeout_s: int,
    cwd: Optional[str],
    verbose: bool,
) -> None:
    argv = [
        version.exe,
        "--input",
        str(input_path),
        "--output",
        str(output_path),
    ]

    print(f"\n== Running version {version.idx} ==")
    print(f"  Executable: {version.exe}")
    print(f"  Input:      {input_path}")
    print(f"  Output:     {output_path}")
    print(f"  Timeout:    {timeout_s}s")
    if cwd:
        print(f"  CWD:        {cwd}")

    _ensure_parent_dir(output_path)

    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            f"Version {version.idx} failed: executable not found.\n"
            f"  Provided: {version.exe}\n"
            f"  Details: {e}"
        ) from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"Version {version.idx} timed out after {timeout_s}s.\n"
            f"  Provided: {version.exe}"
        ) from e

    if proc.returncode != 0:
        raise RuntimeError(
            f"Version {version.idx} exited with status {proc.returncode}.\n"
            f"  Executable: {version.exe}\n"
            f"\n--- stdout ---\n{proc.stdout}\n"
            f"\n--- stderr ---\n{proc.stderr}\n"
        )

    if not output_path.exists():
        raise RuntimeError(
            f"Version {version.idx} reported success but did not create output file:\n"
            f"  Expected: {output_path}"
        )
    if output_path.stat().st_size == 0:
        raise RuntimeError(
            f"Version {version.idx} produced an empty output file:\n"
            f"  {output_path}"
        )

    if verbose:
        print("\n--- stdout ---")
        print(proc.stdout.rstrip())
        print("\n--- stderr ---")
        print(proc.stderr.rstrip())

    print(f"OK: wrote {output_path} ({output_path.stat().st_size} bytes)")


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    input_path = Path(args.inputFile)
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 2
    if input_path.is_dir():
        print(f"ERROR: inputFile is a directory, expected a file: {input_path}", file=sys.stderr)
        return 2

    # Collect provided versions (1..3, with version1 mandatory).
    versions: List[VersionSpec] = [VersionSpec(1, args.version1)]
    if args.version2:
        versions.append(VersionSpec(2, args.version2))
    if args.version3:
        versions.append(VersionSpec(3, args.version3))

    prefix = args.outputFilePrefix
    outputs = [Path(f"{prefix}-{v.idx}.ndjson") for v in versions]

    for v, outp in zip(versions, outputs):
        try:
            _run_one(
                version=v,
                input_path=input_path,
                output_path=outp,
                timeout_s=args.timeoutSeconds,
                cwd=args.cwd,
                verbose=args.verbose,
            )
        except Exception as e:
            print(f"\nFAILED: {e}", file=sys.stderr)
            print(
                "\nRequired interface for each version executable:\n"
                "  <version_exe> --input <input.ndjson> --output <output.ndjson>\n",
                file=sys.stderr,
            )
            return 1

    print("\nCompleted.")
    print("Outputs:")
    for p in outputs:
        print(f"  {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
