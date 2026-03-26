from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path


def base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def app_path() -> Path:
    bundle_dir = Path(getattr(sys, "_MEIPASS", base_dir()))
    return bundle_dir / "app.py"


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def main() -> int:
    root = base_dir()
    os.chdir(root)

    port = find_free_port()
    url = f"http://127.0.0.1:{port}"

    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path()),
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
        "--server.address=127.0.0.1",
        f"--server.port={port}",
    ]

    process = subprocess.Popen(command, cwd=root)
    time.sleep(2)
    webbrowser.open(url)

    try:
        return int(process.wait())
    except KeyboardInterrupt:
        process.terminate()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
