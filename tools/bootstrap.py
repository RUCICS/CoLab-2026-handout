from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
import venv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

VENV_DIR_NAME = ".venv"
REQUIREMENTS_FILE = "requirements.txt"
REQUIRED_IMPORTS = ["rich", "typer"]
REQUIRED_PACKAGES = ["rich", "typer"]

PYPI_MIRRORS = {
    "Official": "https://pypi.org/simple",
    "Tsinghua": "https://pypi.tuna.tsinghua.edu.cn/simple",
    "Aliyun": "https://mirrors.aliyun.com/pypi/simple",
    "Tencent": "https://mirrors.cloud.tencent.com/pypi/simple",
}


def get_venv_paths(root_dir: Path) -> tuple[Path, Path, Path]:
    venv_dir = root_dir / VENV_DIR_NAME
    if sys.platform == "win32":
        return (
            venv_dir,
            venv_dir / "Scripts" / "python.exe",
            venv_dir / "Scripts" / "pip.exe",
        )
    return (
        venv_dir,
        venv_dir / "bin" / "python",
        venv_dir / "bin" / "pip",
    )


def check_venv_integrity(python_executable: Path) -> bool:
    if not python_executable.exists():
        return False

    check_script = "; ".join(f"import {package}" for package in REQUIRED_IMPORTS)
    try:
        subprocess.run(
            [str(python_executable), "-c", check_script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def create_venv_if_missing(venv_dir: Path) -> None:
    if not venv_dir.exists():
        print(f"[Bootstrap] Creating virtual environment at: {venv_dir}...", flush=True)
        venv.create(venv_dir, with_pip=True)


def test_mirror_latency(name: str, url: str, timeout: int = 2) -> tuple[str, str, float]:
    start_time = time.time()
    try:
        request = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status == 200:
                latency = (time.time() - start_time) * 1000.0
                return name, url, latency
    except Exception:
        pass
    return name, url, float("inf")


def get_fastest_mirror() -> str:
    print("[Bootstrap] Selecting the fastest PyPI mirror...", flush=True)
    fastest_url = PYPI_MIRRORS["Official"]
    best_name = "Official"
    min_latency = float("inf")

    with ThreadPoolExecutor(max_workers=len(PYPI_MIRRORS)) as executor:
        futures = [
            executor.submit(test_mirror_latency, name, url)
            for name, url in PYPI_MIRRORS.items()
        ]
        for future in as_completed(futures):
            name, url, latency = future.result()
            if latency < min_latency:
                min_latency = latency
                fastest_url = url
                best_name = name

    if min_latency < float("inf"):
        print(f"[Bootstrap] Selected {best_name} (Latency: {min_latency:.0f}ms)", flush=True)
    else:
        print("[Bootstrap] Warning: all mirror checks failed, falling back to Official", flush=True)
    return fastest_url


def install_dependencies(root_dir: Path, pip_executable: Path) -> None:
    requirements = root_dir / REQUIREMENTS_FILE
    mirror_url = get_fastest_mirror()

    command = [
        str(pip_executable),
        "install",
        "--disable-pip-version-check",
        "-i",
        mirror_url,
    ]
    if "pypi.org" not in mirror_url:
        command.extend(["--trusted-host", urlparse(mirror_url).hostname or ""])

    print("[Bootstrap] Installing dependencies...", flush=True)
    try:
        if requirements.exists():
            subprocess.run(command + ["-r", str(requirements)], check=True)
        else:
            subprocess.run(command + REQUIRED_PACKAGES, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("failed to install Python dependencies") from exc


def restart_in_venv(python_executable: Path) -> None:
    os.environ["SCHEDLAB_BOOTSTRAPPED"] = "1"
    os.execv(str(python_executable), [str(python_executable), *sys.argv])


def initialize() -> None:
    if os.environ.get("SCHEDLAB_SKIP_BOOTSTRAP") == "1":
        return

    try:
        for package in REQUIRED_IMPORTS:
            __import__(package)
        return
    except ImportError:
        pass

    root_dir = Path(__file__).resolve().parents[1]
    venv_dir, venv_python, venv_pip = get_venv_paths(root_dir)
    in_bootstrapped_process = os.environ.get("SCHEDLAB_BOOTSTRAPPED") == "1"

    create_venv_if_missing(venv_dir)

    if not check_venv_integrity(venv_python):
        install_dependencies(root_dir, venv_pip)

    if not in_bootstrapped_process:
        restart_in_venv(venv_python)
