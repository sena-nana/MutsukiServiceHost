#!/usr/bin/env python3
"""Build both deployment fixtures and run the ServiceHost reference matrix."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[3]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("smoke", "reference"), default="reference")
    parser.add_argument("--warmup", type=int)
    parser.add_argument("--samples", type=int)
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=ROOT / "target/mutsuki-benchmarks/service-host-reference.json",
    )
    args = parser.parse_args()
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--package",
            "mutsuki-service-benchmarks",
            "--bins",
        ],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--package",
            "mutsuki-service-abi-fixture",
            "--lib",
        ],
        cwd=ROOT,
        check=True,
    )
    command = [
        str(ROOT / "target/release/mutsuki-service-benchmarks"),
        "--mode",
        args.mode,
        "--output",
        str(args.output),
    ]
    if args.warmup is not None:
        command += ["--warmup", str(args.warmup)]
    if args.samples is not None:
        command += ["--samples", str(args.samples)]
    subprocess.run(command, cwd=ROOT, check=True)
    report = json.loads(args.output.read_text())
    noisy = []
    for case in report["cases"]:
        latency = case["metrics"].get("latency_ns")
        if latency and latency["median"] and latency["mad"] / latency["median"] > 0.05:
            noisy.append(case["case_id"])
    analysis = {
        "classification": (
            "test-implementation-error"
            if not report["correctness"]["passed"]
            else "environmental-noise"
            if noisy
            else "no-obvious-anomaly"
        ),
        "noisy_cases": noisy,
        "rule": "relative MAD above 5% is environmental noise unless correctness also failed",
    }
    args.output.with_suffix(".analysis.json").write_text(
        json.dumps(analysis, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(analysis, indent=2))


if __name__ == "__main__":
    main()
