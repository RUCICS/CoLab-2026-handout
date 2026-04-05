from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence

try:
    from .bench_config import DEFAULT_CALIBRATION_SCHEDULERS
except ImportError:
    from bench_config import DEFAULT_CALIBRATION_SCHEDULERS


class RunnerError(RuntimeError):
    pass


def repo_root_from_file(file_path: str) -> pathlib.Path:
    return pathlib.Path(file_path).resolve().parents[1]


def resolve_runner_path(
    repo_root: pathlib.Path,
    env: Mapping[str, str] | None = None,
) -> pathlib.Path:
    environment = dict(os.environ if env is None else env)
    explicit = environment.get("SCHEDLAB_RUNNER")
    if explicit:
        explicit_path = pathlib.Path(explicit)
        if explicit_path.exists():
            return explicit_path
        raise FileNotFoundError(f"runner binary not found at {explicit_path}")

    stable_runner = repo_root / "out" / "runner"
    if stable_runner.exists():
        return stable_runner

    raise FileNotFoundError("runner binary not found; build it with `make runner`")


def ensure_runner(repo_root: pathlib.Path, env: Mapping[str, str] | None = None) -> pathlib.Path:
    environment = dict(os.environ if env is None else env)
    explicit = environment.get("SCHEDLAB_RUNNER")
    if explicit:
        return resolve_runner_path(repo_root, env=environment)

    result = subprocess.run(
        ["make", "runner"],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip() or "make runner failed"
        raise RunnerError(message)
    return resolve_runner_path(repo_root, env=environment)


def build_runner_command(runner: pathlib.Path, args: Sequence[str]) -> list[str]:
    command = [str(runner), *args]
    if "--jsonl" not in command:
        command.append("--jsonl")
    return command


def default_calibration_schedulers() -> list[str]:
    return list(DEFAULT_CALIBRATION_SCHEDULERS)


def release_args_for_scheduler(
    *,
    suite: str,
    track: str | None,
    scenario: str | None,
    role: str | None,
    repetitions: int,
    workers: int,
    scheduler: str,
) -> list[str]:
    if bool(track) == bool(scenario):
        raise ValueError("use exactly one of track or scenario")

    args = [
        "--mode",
        "release",
        "--engine",
        "sim",
        "--suite",
        suite,
        "--repetitions",
        str(repetitions),
    ]
    if track is not None:
        args.extend(["--track", track])
        if role is not None:
            args.extend(["--role", role])
    else:
        args.extend(["--scenario", scenario])
    if workers > 0:
        args.extend(["--workers", str(workers)])
    if scheduler != "student":
        args.extend(["--candidate-scheduler", scheduler])
    return args


def parse_jsonl_records(lines: Iterable[str]) -> Iterator[dict]:
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL record: {line}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"expected JSON object record, got: {line}")
        yield record


def stream_runner_records(
    repo_root: pathlib.Path,
    args: Sequence[str],
    env: Mapping[str, str] | None = None,
) -> Iterator[dict]:
    runner = ensure_runner(repo_root, env=env)
    command = build_runner_command(runner, args)
    last_os_error: OSError | None = None
    process: subprocess.Popen[str] | None = None
    for _ in range(5):
        try:
            process = subprocess.Popen(
                command,
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            break
        except OSError as exc:
            last_os_error = exc
            if exc.errno != 26:
                raise
            time.sleep(0.1)
    if process is None:
        assert last_os_error is not None
        raise RunnerError(f"runner is being rebuilt; retry shortly ({last_os_error})")
    assert process.stdout is not None
    assert process.stderr is not None
    stdout = process.stdout
    stderr_pipe = process.stderr
    try:
        for record in parse_jsonl_records(stdout):
            yield record
    finally:
        stderr = stderr_pipe.read()
        stdout.close()
        stderr_pipe.close()
        return_code = process.wait()
        if return_code != 0:
            message = stderr.strip() or f"runner exited with status {return_code}"
            raise RunnerError(message)


def collect_runner_records(
    repo_root: pathlib.Path,
    args: Sequence[str],
    env: Mapping[str, str] | None = None,
) -> list[dict]:
    return list(stream_runner_records(repo_root, args, env=env))
