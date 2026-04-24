"""CLI entry point for pipewatch using argparse."""

import argparse
import json
import sys
import time
from pathlib import Path

from pipewatch.dashboard import Dashboard
from pipewatch.exporter import MetricsExporter
from pipewatch.pipeline import PipelineMonitor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline health metrics.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- show command ---
    show_parser = subparsers.add_parser("show", help="Display pipeline dashboard.")
    show_parser.add_argument("--no-color", action="store_true", help="Disable colored output.")
    show_parser.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                             help="Refresh dashboard every N seconds (0 = run once).")

    # --- export command ---
    export_parser = subparsers.add_parser("export", help="Export metrics to file.")
    export_parser.add_argument("--format", choices=["json", "csv"], default="json",
                               dest="fmt", help="Output format (default: json).")
    export_parser.add_argument("--output", required=True, metavar="FILE",
                               help="Destination file path.")

    # --- summary command ---
    subparsers.add_parser("summary", help="Print pipeline summary as JSON.")

    return parser


def cmd_show(monitor: PipelineMonitor, args: argparse.Namespace) -> None:
    use_color = not args.no_color
    dash = Dashboard(monitor, use_color=use_color)
    if args.watch > 0:
        try:
            while True:
                print("\033[2J\033[H", end="")  # clear terminal
                dash.print()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        dash.print()


def cmd_export(monitor: PipelineMonitor, args: argparse.Namespace) -> None:
    exporter = MetricsExporter(monitor)
    output_path = Path(args.output)
    if args.fmt == "json":
        exporter.save_json(output_path)
    else:
        exporter.save_csv(output_path)
    print(f"Exported metrics to {output_path} ({args.fmt}).")


def cmd_summary(monitor: PipelineMonitor) -> None:
    summary = monitor.summary()
    print(json.dumps(summary, indent=2, default=str))


def main(monitor: PipelineMonitor = None, argv=None) -> int:
    """Main CLI entry point. Accepts an optional monitor for testing."""
    if monitor is None:
        monitor = PipelineMonitor(pipeline_name="default")

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "show":
            cmd_show(monitor, args)
        elif args.command == "export":
            cmd_export(monitor, args)
        elif args.command == "summary":
            cmd_summary(monitor)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
