#!/usr/bin/env python3

import argparse
import datetime
import multiprocessing
import socket
import sys
import typing

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tee stdin to stdout and parse timing records.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="context_durations.csv",
        help="Output file path.",
    )
    parser.add_argument(
        "--with-column",
        action="append",
        default=[],
        dest="with_columns",
        type=str,
    )
    return parser.parse_args()


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def tee_lines() -> typing.List[str]:
    lines = []
    # adapted from https://stackoverflow.com/a/45223675/17332200
    for line in iter(sys.stdin.readline, ""):
        lines.append(line)
        sys.stdout.write(line)
        sys.stdout.flush()
    return lines


def main() -> None:
    args = parse_args()

    lines = tee_lines()
    timing_records = {
        k: v
        for line in lines
        if line.startswith("!!!")
        for k, v in eval(remove_prefix(line.strip(), "!!!")).items()
    }
    timing_results = pl.DataFrame(
        {
            "what": timing_records.keys(),
            "duration (s)": timing_records.values(),
        },
        schema={
            "what": str,
            "duration (s)": float,
        },
    ).with_columns(
        *(eval(col) for col in args.with_columns),
    ).with_columns(
        cpu_count=pl.lit(multiprocessing.cpu_count()),
        date=pl.lit(datetime.datetime.now().isoformat()),
        hostname=pl.lit(socket.gethostname()),
    )

    if not args.output.endswith(".csv"):
        raise ValueError("Output file must be a CSV file.")

    timing_results.write_csv(args.output)


if __name__ == "__main__":
    main()
