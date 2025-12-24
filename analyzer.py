#!/usr/bin/env python3

import argparse
import sys
import colorama

from src.metrics import Metrics

VERSION = "0.0.1"


def init_args():
    """Parse and return the arguments."""

    parser = argparse.ArgumentParser(description="Simple Gmail Analyzer")
    parser.add_argument("--top", type=int, default=10, help="Number of results to show")
    parser.add_argument(
        "--user", type=str, default="me", help="User ID to fetch data for"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Verbose output, helpful for debugging"
    )
    parser.add_argument(
        "--version", action="store_true", help="Display version and exit"
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Gmail search query (e.g., 'label:work after:2023/01/01')",
    )
    parser.add_argument(
        "--inactive", type=int, default=0, help="Show senders inactive for more than X days"
    )
    parser.add_argument(
        "--max-retry-rounds",
        type=int,
        default=5,
        help="Max retry rounds for failed message fetches (0 for unlimited)",
    )
    parser.add_argument(
        "--pull-data",
        action="store_true",
        help="Fetch and cache data, then exit",
    )
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="Force refresh cached data, then exit",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Analyze using cached data only (no API calls)",
    )
    parser.add_argument(
        "--export-csv",
        type=str,
        default=None,
        help="Export message metadata to CSV at the given path",
    )

    args = vars(parser.parse_args())

    return args


if __name__ == "__main__":
    colorama.init()

    args = init_args()

    if args["version"]:
        print("gmail analyzer v{}".format(VERSION))
        sys.exit()

    mode_flags = [
        args["pull_data"],
        args["refresh_data"],
        args["analyze_only"],
    ]
    if sum(1 for flag in mode_flags if flag) > 1:
        print("Error: --pull-data, --refresh-data, and --analyze-only are mutually exclusive.")
        sys.exit(1)

    Metrics(args).start()
