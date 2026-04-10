#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import bootstrap

bootstrap.initialize()

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import benchlib

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()
REPO_ROOT = Path(__file__).resolve().parents[1]


def require_selector(track: Optional[str], scenario: Optional[str]) -> list[str]:
    if bool(track) == bool(scenario):
        raise typer.BadParameter("use exactly one of --track or --scenario")
    return ["--track", track] if track else ["--scenario", scenario]


def render_header(title: str, body: str) -> None:
    console.print(Panel(body, title=title, expand=False))


def collect_records_with_status(description: str, args: list[str]) -> list[dict]:
    records: list[dict] = []
    finished_runs = 0
    with console.status(description) as status:
        for record in benchlib.stream_runner_records(REPO_ROOT, args):
            records.append(record)
            if record.get("type") == "scenario_finished":
                finished_runs += 1
                status.update(f"{description} ({finished_runs} runs finished)")
    return records


def collect_records_with_status_allow_failure(
    description: str, args: list[str]
) -> tuple[list[dict], Optional[benchlib.RunnerError]]:
    records: list[dict] = []
    finished_runs = 0
    error: Optional[benchlib.RunnerError] = None
    with console.status(description) as status:
        try:
            for record in benchlib.stream_runner_records(REPO_ROOT, args):
                records.append(record)
                if record.get("type") == "scenario_finished":
                    finished_runs += 1
                    status.update(f"{description} ({finished_runs} runs finished)")
        except benchlib.RunnerError as exc:
            error = exc
    return records, error


def list_records_to_table(records: list[dict]) -> Table:
    table = Table(title="Available Workloads")
    table.add_column("Suite")
    table.add_column("Scenario")
    table.add_column("Track")
    table.add_column("Role")
    table.add_column("Scorer")
    for record in records:
        if record.get("type") != "workload_listed":
            continue
        table.add_row(
            str(record.get("suite", "")),
            str(record.get("scenario", "")),
            str(record.get("track", "")),
            str(record.get("role", "")),
            str(record.get("scorer", "")),
        )
    return table


def release_records_to_tables(records: list[dict]) -> tuple[Table, Panel]:
    scenarios = [record for record in records if record.get("type") == "scenario_scored"]
    track = next(record for record in records if record.get("type") == "track_scored")

    table = Table(title="Scenario Scores")
    table.add_column("Scenario")
    table.add_column("Scorer")
    table.add_column("Score", justify="right")
    for record in scenarios:
        table.add_row(
            str(record.get("scenario", "")),
            str(record.get("scorer", "")),
            f'{float(record.get("score", 0.0)):.3f}x',
        )

    summary = Panel(
        f"Track: {track.get('track', '')}\n"
        f"Score: {float(track.get('score', 0.0)):.3f}x baseline\n"
        f"Display Score: {float(track.get('display_score', 0.0)):.0f}",
        title="Track Result",
        expand=False,
    )
    return table, summary


def gate_panel(records: list[dict]) -> Optional[Panel]:
    gate_finished = next((record for record in records if record.get("type") == "gate_finished"), None)
    if gate_finished is None:
        return None
    failed = gate_finished.get("failed_scenarios", []) or []
    status = "PASS" if gate_finished.get("passed", False) else "FAIL"
    body = (
        f"Track: {gate_finished.get('track', '')}\n"
        f"Scenarios: {gate_finished.get('scenario_count', 0)}\n"
        f"Status: {status}"
    )
    if failed:
        body += "\nFailed: " + ", ".join(str(item) for item in failed)
    title = "Correctness Gate"
    return Panel(body, title=title, expand=False)


def debug_records_to_table(records: list[dict]) -> Table:
    table = Table(title="Debug Runs")
    table.add_column("Scenario")
    table.add_column("Scheduler")
    table.add_column("Run Kind")
    table.add_column("Elapsed (us)", justify="right")
    table.add_column("Tasks", justify="right")
    for record in records:
        if record.get("type") != "scenario_finished":
            continue
        metrics = record.get("metrics", {})
        table.add_row(
            str(record.get("scenario", "")),
            str(record.get("scheduler", "")),
            str(record.get("run_kind", "")),
            str(metrics.get("elapsed_time_us", "")),
            str(metrics.get("completed_tasks", "")),
        )
    return table


@app.command()
def list(
    suite: str = typer.Option("public", help="Suite to list: public or hidden."),
    track: Optional[str] = typer.Option(None, help="Optional track filter."),
    role: Optional[str] = typer.Option(None, help="Optional role filter: gate or leaderboard."),
) -> None:
    args = ["--list-workloads", "--suite", suite]
    if track:
        args.extend(["--track", track])
    if role:
        args.extend(["--role", role])

    records = collect_records_with_status("Discovering workloads", args)
    console.print(list_records_to_table(records))


@app.command()
def release(
    track: Optional[str] = typer.Option(None, help="Track to run."),
    scenario: Optional[str] = typer.Option(None, help="Scenario to run."),
    suite: str = typer.Option("public", help="Suite to run."),
    repetitions: int = typer.Option(1, min=1, help="Number of repetitions."),
    workers: int = typer.Option(0, min=0, help="Optional worker override."),
) -> None:
    args = ["--mode", "release", "--suite", suite, "--repetitions", str(repetitions)]
    args.extend(require_selector(track, scenario))
    if workers > 0:
        args.extend(["--workers", str(workers)])

    records, error = collect_records_with_status_allow_failure("Running release benchmark", args)
    header = next(record for record in records if record.get("type") == "run_started")
    render_header(
        "Schedlab Release",
        f"Suite: {header.get('suite', '')}\n"
        f"Target: {header.get('track') or scenario or ''}\n"
        f"Repetitions: {header.get('repetitions', '')}",
    )
    panel = gate_panel(records)
    if panel is not None:
        console.print(panel)
    if error is not None:
        raise typer.Exit(code=1)
    scenario_table, summary_panel = release_records_to_tables(records)
    console.print(scenario_table)
    console.print(summary_panel)


@app.command()
def debug(
    track: Optional[str] = typer.Option(None, help="Track to run."),
    scenario: Optional[str] = typer.Option(None, help="Scenario to run."),
    suite: str = typer.Option("public", help="Suite to run."),
    scheduler: str = typer.Option("student", help="Scheduler to use in debug mode."),
    repetitions: int = typer.Option(1, min=1, help="Number of repetitions."),
    workers: int = typer.Option(0, min=0, help="Optional worker override."),
) -> None:
    args = [
        "--mode",
        "debug",
        "--suite",
        suite,
        "--scheduler",
        scheduler,
        "--repetitions",
        str(repetitions),
    ]
    args.extend(require_selector(track, scenario))
    if workers > 0:
        args.extend(["--workers", str(workers)])

    records = collect_records_with_status("Running debug benchmark", args)
    header = next(record for record in records if record.get("type") == "run_started")
    render_header(
        "Schedlab Debug",
        f"Suite: {header.get('suite', '')}\n"
        f"Target: {header.get('track') or scenario or ''}\n"
        f"Repetitions: {header.get('repetitions', '')}",
    )
    console.print(debug_records_to_table(records))


@app.command()
def calibrate(
    suite: str = typer.Option("public", help="Suite to run calibration against."),
    repetitions: int = typer.Option(1, min=1, help="Number of repetitions per track."),
) -> None:
    tracks = ["throughput", "latency", "fairness"]

    for track in tracks:
        discovered = collect_records_with_status(
            f"Discovering {track} workloads",
            ["--list-workloads", "--suite", suite, "--track", track, "--role", "leaderboard"],
        )
        scenarios = [
            str(record.get("scenario", ""))
            for record in discovered
            if record.get("type") == "workload_listed"
        ]
        if not scenarios:
            continue

        summary = Table(title=f"{track.capitalize()} Calibration")
        summary.add_column("Scheduler")
        summary.add_column("Display", justify="right")
        summary.add_column("Score", justify="right")

        scenario_table = Table(title=f"{track.capitalize()} Scenario Matrix")
        scenario_table.add_column("Scheduler")
        for scenario in scenarios:
            scenario_table.add_column(scenario.split("/", 1)[-1], justify="right")

        for scheduler in benchlib.default_calibration_schedulers():
            if scheduler == "baseline":
                summary.add_row("baseline", "1000", "1.000x")
                scenario_table.add_row("baseline", *["1.000x"] * len(scenarios))
                continue

            records = collect_records_with_status(
                f"Calibrating {track} with {scheduler}",
                benchlib.release_args_for_scheduler(
                    suite=suite,
                    track=track,
                    scenario=None,
                    role="leaderboard",
                    repetitions=repetitions,
                    workers=0,
                    scheduler=scheduler,
                ),
            )
            track_record = next(record for record in records if record.get("type") == "track_scored")
            scenario_records = {
                str(record.get("scenario", "")): record
                for record in records
                if record.get("type") == "scenario_scored"
            }

            summary.add_row(
                scheduler,
                f'{float(track_record.get("display_score", 0.0)):.0f}',
                f'{float(track_record.get("score", 0.0)):.3f}x',
            )
            scenario_table.add_row(
                scheduler,
                *[
                    f'{float(scenario_records.get(scenario, {}).get("score", 0.0)):.3f}x'
                    for scenario in scenarios
                ],
            )

        console.print(summary)
        console.print(scenario_table)


@app.command()
def grade(
    suite: str = typer.Option("public", help="Suite to grade against."),
    repetitions: int = typer.Option(3, min=1, help="Number of repetitions per track."),
    workers: int = typer.Option(0, min=0, help="Optional worker override."),
) -> None:
    """Build, run unit tests, then score all three tracks."""
    import subprocess as sp

    all_passed = True

    console.rule("[bold]Step 1: Build")
    build_result = sp.run(
        ["cmake", "--build", "build", "--parallel"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if build_result.returncode != 0:
        console.print("[red bold]BUILD FAILED[/red bold]")
        console.print(build_result.stderr or build_result.stdout)
        raise typer.Exit(code=1)
    console.print("[green]Build OK[/green]")

    console.rule("[bold]Step 2: Unit Tests")
    test_result = sp.run(
        ["ctest", "--test-dir", "build", "--output-on-failure", "--parallel", "8"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    for line in (test_result.stdout + test_result.stderr).splitlines():
        if "tests passed" in line or "FAILED" in line or "Test " in line:
            console.print(line)
    if test_result.returncode != 0:
        console.print("[red bold]UNIT TESTS FAILED[/red bold]")
        all_passed = False
    else:
        console.print("[green]All unit tests passed[/green]")

    console.rule("[bold]Step 3: Scoring")
    tracks = ["throughput", "latency", "fairness"]

    summary_table = Table(title="Grade Summary")
    summary_table.add_column("Track")
    summary_table.add_column("Gate", justify="center")
    summary_table.add_column("Score", justify="right")
    summary_table.add_column("Display Score", justify="right")

    total_display = 0.0
    track_count = 0

    for track in tracks:
        args = [
            "--mode", "release",
            "--suite", suite,
            "--track", track,
            "--repetitions", str(repetitions),
        ]
        if workers > 0:
            args.extend(["--workers", str(workers)])

        records, error = collect_records_with_status_allow_failure(
            f"Scoring {track}", args
        )

        gate_rec = next(
            (r for r in records if r.get("type") == "gate_finished"), None
        )
        track_rec = next(
            (r for r in records if r.get("type") == "track_scored"), None
        )

        gate_status = "[green]PASS[/green]"
        if gate_rec and not gate_rec.get("passed", False):
            gate_status = "[red]FAIL[/red]"
            failed = gate_rec.get("failed_scenarios", [])
            if failed:
                gate_status += f" ({', '.join(str(s) for s in failed)})"
            all_passed = False

        if error is not None:
            gate_status = "[red]FAIL[/red]"
            all_passed = False

        if track_rec:
            score = float(track_rec.get("score", 0.0))
            display = float(track_rec.get("display_score", 0.0))
            summary_table.add_row(
                track,
                gate_status,
                f"{score:.3f}x",
                f"{display:.0f}",
            )
            total_display += display
            track_count += 1

            scenario_recs = [
                r for r in records if r.get("type") == "scenario_scored"
            ]
            if scenario_recs:
                detail = Table(title=f"{track.capitalize()} Scenarios", show_edge=False)
                detail.add_column("Scenario")
                detail.add_column("Score", justify="right")
                for sr in scenario_recs:
                    detail.add_row(
                        str(sr.get("scenario", "")),
                        f'{float(sr.get("score", 0.0)):.3f}x',
                    )
                console.print(detail)
        else:
            summary_table.add_row(track, gate_status, "-", "-")

    console.print()
    console.print(summary_table)

    if track_count > 0:
        avg = total_display / track_count
        console.print(
            Panel(
                f"Average Display Score: [bold]{avg:.0f}[/bold] / 1000\n"
                f"(baseline = 1000 per track)",
                title="Overall",
                expand=False,
            )
        )

    if not all_passed:
        raise typer.Exit(code=1)


def main() -> None:
    try:
        app()
    except benchlib.RunnerError as exc:
        console.print(f"[red]Runner failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    main()
